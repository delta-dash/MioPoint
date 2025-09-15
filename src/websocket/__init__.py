

from fastapi import FastAPI

from src.websocket.WebSocketRoutes import router



async def websocket_fastapi_routes(app:FastAPI):
    app.include_router(router)
    