

from fastapi import FastAPI

from ConfigMedia import get_config
from src.users.user_manager import setup_user_database
from src.users.routes import admin_router, auth_router
CONFIG = get_config()



async def user_fastapi_routes(app:FastAPI):
    
    await setup_user_database()
    #setup_initial_roles_and_permissions()
    app.include_router(auth_router)
    app.include_router(admin_router)
    