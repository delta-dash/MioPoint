from typing import List, Dict, Any, Optional
import aiosqlite
from src.db_utils import AsyncDBContext, execute_db_query

async def search_users_by_username(username_query: str, conn: Optional[aiosqlite.Connection] = None) -> List[Dict[str, Any]]:
    """Searches for users by username."""
    query = "SELECT id, username FROM users WHERE username LIKE ? AND is_guest = 0 AND is_active = 1 LIMIT 10"
    params = (f"%{username_query}%",)
    async with AsyncDBContext(conn) as db:
        rows = await execute_db_query(db, query, params, fetch_all=True)
        return [dict(row) for row in rows] if rows else []
