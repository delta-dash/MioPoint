#server.py
from datetime import datetime, timedelta, timezone
import os
from fastapi import  FastAPI,  HTTPException, Request, status
from cachetools import TTLCache
from contextlib import asynccontextmanager
import logging

from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
import scanner_service
from ConfigMedia import get_config
from common import IS_PRODUCTION
from src.db_utils import AsyncDBContext
from src.users.user_manager import  get_or_create_guest_user
from src.users.auth import AUTH_COOKIE_NAME, create_access_token

# In-memory rate limiter
class InMemoryRateLimiter:
    def __init__(self):
        self.cache = TTLCache(maxsize=10000, ttl=3600)

    async def check_limit(self, key: str, times: int, seconds: int):
        current_time = datetime.now(timezone.utc)
        entry = self.cache.get(key)

        if entry is None:
            self.cache[key] = (1, current_time)
            return

        count, last_request_time = entry
        time_elapsed = (current_time - last_request_time).total_seconds()

        if time_elapsed > seconds:
            self.cache[key] = (1, current_time)
        else:
            if count >= times:
                retry_after = seconds - time_elapsed
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Too many requests. Try again in {round(retry_after, 2)} seconds."
                )
            self.cache[key] = (count + 1, last_request_time)

rate_limiter = InMemoryRateLimiter()

def rate_limiter_dependency(times: int, seconds: int):
    async def dependency(request: Request):
        key = request.client.host
        await rate_limiter.check_limit(key, times, seconds)
    return dependency

config = get_config()
logger = logging.getLogger("Server")
logger.setLevel(logging.DEBUG)


def connect_routes_tools_fastapi(app: FastAPI):
    """
    Includes all the tool routers in the FastAPI application.
    """
    from src.users.routes import admin_router as user_admin_router, auth_router as user_auth_router
    from src.tags.TagRoutes import router as tags_router
    from src.reactions.ReactionRoutes import router as reactions_router
    from src.media.routes import router as media_router
    from src.roles.routes import router as roles_router
    from src.messaging.messaging_router import router as messaging_router
    from src.notifications.router import router as notifications_router
    from src.profile.routes import auth_router as profile_router
    from src.websocket.WebSocketRoutes import router as websocket_router
    from src.settings.config_routes import router as config_router

    app.include_router(user_auth_router)
    app.include_router(user_admin_router)
    app.include_router(tags_router)
    app.include_router(reactions_router)
    app.include_router(media_router)
    app.include_router(roles_router)
    app.include_router(messaging_router)
    app.include_router(notifications_router)
    app.include_router(profile_router)
    app.include_router(websocket_router)
    app.include_router(config_router)
    
    if not IS_PRODUCTION: return
    STATIC_DIR = "static"
    app.mount("/_app", StaticFiles(directory=os.path.join(STATIC_DIR, "_app")), name="app")

    @app.get("/{full_path:path}", response_class=FileResponse)
    async def serve_spa(full_path: str):
        # Define the path to your main Svelte HTML file
        index_file = os.path.join(STATIC_DIR, "200.html")
        
        # Check if the requested path corresponds to a file in the static directory
        # This is a security measure to prevent directory traversal attacks.
        # It also handles requests for files like favicon.png or manifest.json
        requested_path = os.path.join(STATIC_DIR, full_path)
        if os.path.isfile(requested_path):
            return FileResponse(requested_path)

        # If the path doesn't correspond to a file, serve the main Svelte HTML file
        return FileResponse(index_file)

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    import asyncio
    logger.info("Server is starting up...")

    # Initialize state attributes immediately to prevent AttributeError on early requests.
    # We point to the global app_state dict from the scanner_service.
    # It exists on module import, and will be populated by the startup task.
    app.state.scanner_app_state = scanner_service.app_state
    app.state.scanner_managed_state = None # This will be populated later.

    async def startup_task():
        try:
            # This now runs in the background and doesn't block startup
            # startup_scanner_logic returns the app_state, which includes service_handles
            managed_state = await scanner_service.startup_scanner_logic()
            app.state.scanner_managed_state = managed_state
            # No need to set scanner_app_state again, it's the same object.
            logger.info("Background scanner service has started successfully.")
        except Exception as e:
            logger.exception(f"FATAL: Failed to start the background scanner service: {e}")
            app.state.scanner_managed_state = None

    # Create a background task for the scanner startup
    scanner_startup_task = asyncio.create_task(startup_task())

    # Connect routes immediately so the server is responsive
    connect_routes_tools_fastapi(app)
    
    yield

    logger.info("Server is shutting down...")

    # If startup is still running, cancel it instead of waiting.
    if not scanner_startup_task.done():
        logger.info("Scanner is still starting up. Cancelling startup task for shutdown...")
        scanner_startup_task.cancel()
        # Wait for the cancellation to be processed.
        await asyncio.gather(scanner_startup_task, return_exceptions=True)

    scanner_state_to_close = getattr(app.state, 'scanner_managed_state', None)
    if scanner_state_to_close:
        await scanner_service.shutdown_scanner_logic(scanner_state_to_close)
        logger.info("Background scanner service has been shut down.")
    else:
        logger.warning("Scanner state not found or was not initialized, skipping shutdown logic.")

app = FastAPI(lifespan=app_lifespan)

PUBLIC_PATHS = {
    "/api/auth/token",
    "/api/auth/refresh_token",
    "/api/auth/register",
    "/openapi.json",
}



DB_FILE = config.get('DB_FILE', 'media.db')

@app.middleware("http")
async def smart_auth_middleware(request: Request, call_next):
    # Guest provisioning is disabled. The frontend is responsible for
    # redirecting to a login page if no active session is found via API.
    # This middleware is kept as a placeholder for potential future use.
    return await call_next(request)