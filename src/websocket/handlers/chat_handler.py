# tools/websocket/handlers/chat_handler.py

import asyncio
from fastapi import WebSocket
from typing import Dict, Any

from src.messaging.messaging_service import ban_user_from_thread, create_post, transfer_thread_ownership
from src.websocket.dispatcher import get_websocket_dispatcher
from src.websocket.HelperWebsocket import WebSocketConnectionManager, get_websocket_manager

dispatcher = get_websocket_dispatcher()
manager = get_websocket_manager()


@dispatcher.on("send_message_to_thread")
async def handle_send_message_to_thread(websocket: WebSocket, payload: dict, **kwargs):
    """
    Handles incoming chat messages for a specific thread.
    """
    content = payload.get("content")
    thread_id = payload.get("thread_id")
    user_data = manager.get_user_for_websocket(websocket)

    if not all([content, thread_id, user_data]):
        await manager.send_json_message({"type": "error", "payload": "Invalid chat message request."}, websocket)
        return

    # More secure check: is the user a member of the room they claim to post in?
    if not manager.is_in_room(websocket, thread_id):
        await manager.send_json_message({"type": "error", "payload": f"Not subscribed to thread {thread_id}."}, websocket)
        return

    sender_id = user_data.get("id")

    # The create_post service function already handles permissions
    new_post = await create_post(
        sender_id=sender_id,
        thread_id=thread_id,
        content=content
    )

    if new_post:
        await manager.broadcast_to_room(thread_id, {"type": "new_chat_message", "payload": new_post})
    else:
        await manager.send_json_message({"type": "error", "payload": "Failed to send message."}, websocket)


# --- NEW: Specific Handlers for Thread Administration ---

@dispatcher.on("transfer_ownership_in_thread")
async def handle_transfer_ownership_in_thread(websocket: WebSocket, payload: dict, **kwargs):
    """Handles ownership transfer specifically for a general thread."""
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = payload.get("thread_id")
    new_owner_id = payload.get("new_owner_id")

    if not all([user_data, thread_id, new_owner_id]):
        await manager.send_json_message({"type": "error", "payload": "Invalid ownership transfer request."}, websocket)
        return

    acting_user_id = user_data.get("id")
    success = await transfer_thread_ownership(
        thread_id=thread_id,
        current_owner_id=acting_user_id,
        new_owner_id=new_owner_id
    )

    if success:
        # On success, the service function logs the event.
        # We broadcast an update to all members to refresh their UI.
        await manager.broadcast_to_room(thread_id, {"type": "thread_members_updated"})


@dispatcher.on("ban_user_from_thread")
async def handle_ban_user_from_thread(websocket: WebSocket, payload: dict, **kwargs):
    """Handles banning a user specifically from a general thread."""
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = payload.get("thread_id")
    banned_user_id = payload.get("banned_user_id")

    if not all([user_data, thread_id, banned_user_id]):
        await manager.send_json_message({"type": "error", "payload": "Invalid ban request."}, websocket)
        return

    acting_user_id = user_data.get("id")
    success = await ban_user_from_thread(
        thread_id=thread_id,
        banning_user_id=acting_user_id,
        banned_user_id=banned_user_id
    )

    if success:
        # Notify the room that members have changed and potentially that a user was banned.
        await manager.broadcast_to_room(thread_id, {
            "type": "user_banned",
            "payload": {"thread_id": thread_id, "banned_user_id": banned_user_id}
        })


@dispatcher.on("player_state_update")
async def handle_player_state_update(websocket: WebSocket, payload: Dict[str, Any], **kwargs):
    """
    Broadcasts a media player state update (e.g., play, pause, seek) to others in the party.
    """
    current_thread_id = manager.get_chat_room_for_websocket(websocket)
    user_data = manager.get_user_for_websocket(websocket)
    
    if current_thread_id and user_data:
        broadcast_message = {
            "type": "player_state_update",
            "payload": payload,
            "sender": {"id": user_data.get("id"), "username": user_data.get("username")}
        }
        # Exclude the sender to prevent video playback loops on their own client.
        await manager.broadcast_to_room(current_thread_id, broadcast_message, sender_websocket=websocket)

# A simple ping handler can live here or in its own file 
@dispatcher.on("ping")
async def handle_ping(websocket: WebSocket, **kwargs):
    await manager.send_personal_message("pong", websocket)

@dispatcher.on("subscribe_to_thread_updates")
async def handle_subscribe_to_thread_updates(websocket: WebSocket, payload: dict, **kwargs):
    """
    Subscribes a client to a specific chat thread's updates.
    """
    thread_id = payload.get("thread_id")
    if thread_id:
        await manager.join_room(websocket, thread_id)

@dispatcher.on("unsubscribe_from_thread_updates")
async def handle_unsubscribe_from_thread_updates(websocket: WebSocket, payload: dict, **kwargs):
    """
    Unsubscribes a client from a specific chat thread's updates.
    """
    thread_id = payload.get("thread_id")
    if thread_id:
        await manager.leave_room(websocket, thread_id)
