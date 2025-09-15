from fastapi import FastAPI
from Searcher import get_searcher
from src.media.MediaDB import setup_database
from src.media.routes import router


async def start_db():
    await setup_database()
    await get_searcher()


async def media_fastapi_routes(app:FastAPI):
    await start_db()
    app.include_router(router)
