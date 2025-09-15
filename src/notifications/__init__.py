
from fastapi import FastAPI

from src.notifications.notification_service import setup_notifications_database_async
from src.notifications.router import router

async def notification_fastapi_routes(app:FastAPI):
    await setup_notifications_database_async()
    app.include_router(router)