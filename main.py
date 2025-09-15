import asyncio
import logging
import sys


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


# --- Phase 4: Main Application Async Task ---
async def start_server():
    """Sets up and runs the Uvicorn server programmatically."""
    logger.info("--- Phase 4: Starting FastAPI Server ---")

    config = Config(
        app=app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        # Set reload to True for development, False for production
        reload=not IS_PRODUCTION
    )
    server = Server(config)

    await server.serve()
    logger.info("Server has been shut down.")


# --- Main Async Orchestrator ---
async def main():
    """The main asynchronous entry point for the entire application."""

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

    # Phase 4
    await start_server()


# --- Main Synchronous Entry Point ---
if __name__ == "__main__":
    try:
        # Start the entire application with a single, top-level asyncio.run() call.
        asyncio.run(main())
    except SystemExit as e:
        logger.error(f"Application startup halted: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unhandled exception occurred during startup: {e}", exc_info=True)
        sys.exit(1)