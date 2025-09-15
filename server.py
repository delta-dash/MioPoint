#server.py
from datetime import datetime, timedelta, timezone
from fastapi import  FastAPI,  HTTPException, Request, status
from cachetools import TTLCache
from contextlib import asynccontextmanager
import logging
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

def config_fastapi_routes(app: FastAPI):
    from src.settings.config_routes import router as config_router
    app.include_router(config_router)

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
    config_fastapi_routes(app)

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    logger.info("Server is starting up...")
    try:
        scanner_state = await scanner_service.startup_scanner_logic()
        app.state.scanner = scanner_state
        logger.info("Background scanner service has been started successfully.")
    except Exception as e:
        logger.exception(f"FATAL: Failed to start the background scanner service: {e}")
        app.state.scanner = None
        
    connect_routes_tools_fastapi(app) # No await
    yield

    logger.info("Server is shutting down...")
    scanner_state_to_close = app.state.scanner
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
    if request.scope["type"] == "websocket":
        return await call_next(request)
    
    path = request.url.path
    is_likely_page_load = not path.startswith('/api/') and not '.' in path.split('/')[-1]

    if not is_likely_page_load:
        return await call_next(request)

    token = request.cookies.get(AUTH_COOKIE_NAME)
    if token:
        return await call_next(request)

    logger.info(f"New visitor on page '{path}'. Provisioning guest session.")
    response = await call_next(request)

    async with AsyncDBContext() as conn:
        guest_result = await get_or_create_guest_user(request.client.host, conn)
        if guest_result:
            guest_data, _ = guest_result
            expires = timedelta(days=90)
            guest_token = create_access_token(data={"sub": str(guest_data['id'])}, expires_delta=expires)
            response.set_cookie(
                key=AUTH_COOKIE_NAME, value=guest_token,
                httponly=True, samesite="lax", secure=IS_PRODUCTION,
                max_age=int(expires.total_seconds())
            )
    return response