
from src.log.HelperLog import setup_log_database_async

async def log_initialize():
    await setup_log_database_async()