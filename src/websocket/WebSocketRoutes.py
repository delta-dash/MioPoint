# WebSocketRoutes.py (Full, Updated Code)

import asyncio
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import jwt

# --- Local Imports ---
import logging

# --- Authentication and Model Imports ---
from src.models import UserProfile
from src.users.auth import AUTH_COOKIE_NAME, authenticate_user_from_token, TokenRevokedError

# --- WebSocket Infrastructure Imports ---
from src.websocket.HelperWebsocket import get_websocket_manager
from src.websocket.dispatcher import get_websocket_dispatcher

# --- Service Function Imports for Cleanup Logic ---
from src.messaging.messaging_service import delete_watch_party_thread, get_thread_details

# --- This import is critical! It runs the code in the handler files,
# which registers their functions with the dispatcher instance.
from src.websocket import handlers 
from utils import logging_handler

logger = logging.getLogger("WebSocket")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging_handler)
router = APIRouter(prefix="/api/ws", tags=["Websocket"])

# Get singleton instances
manager = get_websocket_manager()
dispatcher = get_websocket_dispatcher()

@router.websocket("/connect")
async def websocket_endpoint(websocket: WebSocket):
    """
    Handles the initial WebSocket connection, authentication, and user registration
    with the connection manager.
    """
    logger.info(f"WebSocket connection attempt from {websocket.client.host}:{websocket.client.port}")
    try:
        # 1. Authenticate the user via the auth cookie
        token = websocket.cookies.get(AUTH_COOKIE_NAME)
        if not token:
            await websocket.close(code=4001, reason="Authentication token missing.")
            return

        user_profile: UserProfile | None = None
        try:
            user_profile = await authenticate_user_from_token(token)
        except (jwt.PyJWTError, TokenRevokedError, ValueError) as e:
            await websocket.close(code=4003, reason="Invalid or expired token.")
            return

        # 2. Accept the WebSocket connection
        await websocket.accept()

        # 3. Check for guest/inactive users
        if user_profile.is_guest or not user_profile.is_active:
            reason = "Guest users cannot connect." if user_profile.is_guest else "Your account is inactive."
            await manager.send_json_message({"type": "error", "payload": reason}, websocket)
            await websocket.close()
            return
            
        # 4. Connect the user to the manager and send a success confirmation
        user_data_dict = user_profile.model_dump()
        await manager.connect(websocket, user_data_dict)
        await websocket.send_json({
            "type": "authenticated",
            "payload": {"user_id": user_profile.id, "username": user_profile.username}
        })
        
        # 5. Hand off to the message handling loop
        await handle_websocket_messages(websocket)

    except WebSocketDisconnect:
        # This catches disconnects that happen *during* the initial handshake.
        logger.info(f"WebSocket connection closed during handshake: {websocket.client.host}:{websocket.client.port}")
    except Exception as e:
        logger.error(f"Unexpected error during WebSocket connection: {e}", exc_info=True)
        # Attempt to gracefully close the connection if it's still open
        if websocket.client_state != "DISCONNECTED":
            await websocket.close(code=1011, reason="Internal server error.")


async def handle_websocket_messages(websocket: WebSocket):
    """
    Handles all subsequent messages from an authenticated WebSocket connection
    by dispatching them to the appropriate registered handler. Also manages
    cleanup logic on disconnect.
    """
    authenticated_user_data = manager.get_user_for_websocket(websocket)
    if not authenticated_user_data:
        # This is a safeguard; should not happen if connect() succeeded.
        await manager.disconnect(websocket)
        return

    username = authenticated_user_data.get("username", "Unknown User")
    logger.info(f"Starting message handling for authenticated user: {username}")

    try:
        # Main message loop
        while True:
            data = await websocket.receive_json()
            logger.info(f"Received from {username}: {data}")
            # Dispatch the message to a handler function based on its 'type'
            await dispatcher.dispatch(manager, websocket, data)
    
    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {username} ({websocket.client.host}:{websocket.client.port})")
        # Create a synthetic event for the dispatcher to handle all cleanup logic,
        # such as notifying party members and disconnecting from the manager.
        await dispatcher.dispatch(manager, websocket, {"type": "client_disconnected", "payload": {}})

    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON received from {username}. Closing connection.")
        await manager.send_personal_message("Invalid JSON received. Closing connection.", websocket)
        # Dispatch to the centralized disconnect handler
        await dispatcher.dispatch(manager, websocket, {"type": "client_disconnected", "payload": {}})

    except Exception as e:
        logger.error(f"Error handling WebSocket messages for {username}: {e}", exc_info=True)
        # Avoid sending detailed errors to the client for security.
        await manager.send_personal_message("An internal server error occurred.", websocket)
