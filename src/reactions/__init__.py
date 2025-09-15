

from fastapi import FastAPI

from src.reactions.HelperReactions import setup_reactions_database_async
from src.reactions.ReactionRoutes import router


async def reaction_fastapi_routes(app:FastAPI):
    await setup_reactions_database_async()
    app.include_router(router)
    