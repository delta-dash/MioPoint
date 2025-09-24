import asyncio
import logging
import socket
import sys
import signal


# Uvicorn and project-specific imports
from uvicorn import Config, Server
from common import IS_PRODUCTION
from register_admin_ui import start_kivy  # This now returns a Kivy app instance
import logging
from src.log.HelperLog import setup_log_database_async
from src.media.MediaDB import setup_database
from src.messaging.messaging_service import setup_messaging_database
from src.notifications.notification_service import setup_notifications_database_async
from src.reactions.HelperReactions import setup_reactions_database_async
from src.roles.rbac_manager import check_if_admin_exists
from server import app
from src.roles import start_role_db
from src.tags.HelperTags import setup_tags_database_with_loop_async
from src.users.user_manager import setup_user_database



#root_logger.addHandler(handler)

# Silence noisy third-party loggers
logging.getLogger("watchdog").setLevel(logging.INFO)
logging.getLogger("aiosqlite").setLevel(logging.INFO)


# --- Global Shutdown Event ---
shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    """Sets the shutdown event when a signal is received."""
    logger.warning(f"Caught signal {sig}. Initiating graceful shutdown...")
    asyncio.get_running_loop().call_soon_threadsafe(shutdown_event.set)


# --- Logger Setup ---
logger = logging.getLogger("Startup")
logger.setLevel(logging.DEBUG)

# --- Phase 1: Initial Async Check ---
async def run_initial_setup_and_check() -> bool:
    """
    Performs the initial async database setup and checks if an admin exists.
    Returns: True if an admin already exists, False otherwise.
    """
    logger.info("--- Phase 1: Initializing Database and Roles ---")
    await setup_log_database_async()
    await setup_user_database()
    await setup_database()
    await start_role_db()
    await setup_reactions_database_async()
    await setup_tags_database_with_loop_async()
    await setup_notifications_database_async()
    await setup_messaging_database()
    logger.info("Checking for existing admin user...")
    return await check_if_admin_exists()


# --- Manual Async Kivy Driver ---
async def run_kivy_app_async(app):
    """
    Manually drives the Kivy event loop within an asyncio loop.
    This is the replacement for the blocking `app.run()` method.
    """
    from kivy.base import EventLoop
    from kivy.clock import Clock
    from kivy.logger import Logger, LOG_LEVELS
    Logger.setLevel(LOG_LEVELS["warning"])

    await app.async_run()  # This builds the window but doesn't block the program

    # Loop until the Kivy app is intended to be closed (e.g., app.stop() is called)
    while not app.exit_flag:
        EventLoop.idle()

        if app.registration_task and app.registration_task.done():
            try:
                success, message = app.registration_task.result()
                
                # Clear the task so we don't run it again
                app.registration_task = None
                
                # --- CHANGE 2: SCHEDULE THE UI UPDATE INSTEAD OF CALLING IT DIRECTLY ---
                # This is the key fix. We use a lambda to pass the results to the function
                # and schedule it to run on Kivy's main thread on the next frame.
                Clock.schedule_once(lambda dt: app.process_registration_result(success, message))

            except Exception as e:
                logger.error(f"Error retrieving result from admin registration task: {e}", exc_info=True)
                message = "A critical database error occurred."
                # Also schedule the error dialog on the main thread
                Clock.schedule_once(lambda dt: app.process_registration_result(False, message))
                app.registration_task = None

        await asyncio.sleep(1 / 60)

    # Once the loop is broken, ensure Kivy is fully stopped.
    app.stop()


# --- Phase 2: Synchronous UI Launcher (now async) ---
async def launch_admin_registration_ui():
    """Launches the Kivy UI and waits for it to signal completion."""
    logger.warning("No admin user found. Launching admin registration UI.")
    logger.info("The application will pause until the admin user is created and the UI is closed.")
    
    # Create an event that the Kivy app will use to signal it's done.
    registration_complete_event = asyncio.Event()

    try:
        # Pass the event to the Kivy app instance.
        kivy_app_instance = start_kivy(completion_event=registration_complete_event)
        
        # This is the key change. We run the Kivy app and wait for the event
        # to be set simultaneously. The `gather` will complete when BOTH are done.
        # However, since the Kivy app sets the event and then closes itself, this
        # effectively waits until the user finishes registration.
        await asyncio.gather(
            kivy_app_instance.async_run(),
            registration_complete_event.wait()
        )
        
        logger.info("Admin registration UI has signaled completion.")
        
    except Exception as e:
        # This will catch errors if the UI window is closed prematurely.
        if isinstance(e, asyncio.CancelledError):
             logger.warning("Admin registration was cancelled by closing the window.")
        else:
             logger.critical(f"The admin registration UI crashed: {e}", exc_info=True)
        # In either case, we will proceed to the confirmation step.
    finally:
        # Ensure the app is fully stopped to release resources.
        if 'kivy_app_instance' in locals() and kivy_app_instance.root:
            kivy_app_instance.stop()



# --- Phase 3: Final Async Confirmation ---
async def confirm_admin_creation() -> bool:
    """A final async check to confirm the UI successfully created the admin."""
    logger.info("--- Phase 3: Confirming Admin Creation ---")
    if await check_if_admin_exists():
        logger.info("Admin user successfully created. Continuing startup.")
        return True
    else:
        logger.critical("Admin user was NOT created after running the UI. Shutting down.")
        return False

def find_free_port(start_port=8000, max_port=9000):
    """Return a free port in the given range, or raise if none found."""
    for port in range(start_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return port  # Port is free
            except OSError:
                continue
    raise RuntimeError(f"No free ports between {start_port}-{max_port}")

# --- Main Async Orchestrator ---
async def main():
    """The main asynchronous entry point for the entire application."""
    port = 8000#find_free_port()
    # Phase 1
    admin_exists = await run_initial_setup_and_check()

    # Phase 2 & 3
    if not admin_exists:
        await launch_admin_registration_ui()

        if not await confirm_admin_creation():
            # This SystemExit will be caught by the main block below
            raise SystemExit("Admin user creation failed. Cannot start the application.")
    else:
        logger.info("Admin user found. Proceeding with startup.")

    # Phase 4: Start and manage the server
    logger.info("--- Phase 4: Starting FastAPI Server ---")
    config = Config(
        app=app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        reload=False,
        lifespan="on" # Explicitly use lifespan protocol
    )
    server = Server(config)

    # Run the server in a background task
    server_task = asyncio.create_task(server.serve())

    # Wait for the shutdown signal (e.g., from CTRL+C)
    await shutdown_event.wait()

    # Gracefully shut down the server
    logger.info("Shutdown signal received. Telling Uvicorn server to exit.")
    server.should_exit = True

    # Wait for the server task to complete. This allows Uvicorn to finish
    # its shutdown sequence, including closing connections.
    await server_task
    logger.info("Server task has completed. Main application will now exit.")


# --- Main Synchronous Entry Point ---
if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start the entire application with a single, top-level asyncio.run() call.
        asyncio.run(main())
    except (SystemExit, KeyboardInterrupt):
        # The main() function now handles graceful shutdown, so we only expect
        # exceptions here if something went wrong during startup.
        pass # The specific error will have already been logged.
    except Exception as e:
        logger.critical(f"An unhandled exception occurred during startup: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Application has finished.")