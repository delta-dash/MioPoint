

from fastapi import FastAPI
from src.db_utils import AsyncDBContext
from src.roles.rbac_manager import initialize_protected_role_ids, setup_initial_roles_and_permissions, setup_roles_database
from src.roles.routes import router

async def start_role_db():
    async with AsyncDBContext() as db:
        await setup_roles_database(db)
        await setup_initial_roles_and_permissions(db)
        await initialize_protected_role_ids(db)

async def role_fastapi_routes(app:FastAPI):
    await start_role_db()
    app.include_router(router)
    