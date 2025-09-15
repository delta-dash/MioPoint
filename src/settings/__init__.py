from fastapi import APIRouter
import logging
from src.settings.config_routes import router

logger = logging.getLogger("ConfigRoutes")
logger.setLevel(logging.DEBUG)

async def config_fastapi_routes(app: APIRouter):
    app.include_router(router, prefix="/api", tags=["Config"])
    logger.info("Config routes initialized.")
