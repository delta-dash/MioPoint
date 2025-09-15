

from fastapi import FastAPI

from src.messaging.messaging_router import router
from src.messaging.messaging_service import setup_messaging_database


async def chat_fastapi_routes(app:FastAPI):
    await setup_messaging_database()
    app.include_router(router)
    