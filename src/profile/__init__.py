
from fastapi import FastAPI

from src.profile.routes import auth_router

def profile_fastapi_routes(app:FastAPI):
    app.include_router(auth_router)

    