

from fastapi import FastAPI
from src.tags.HelperTags import setup_tags_database_with_loop_async
from src.tags.TagRoutes import router

async def tags_fastapi_routes(app:FastAPI):
    await setup_tags_database_with_loop_async()
    app.include_router(router)
