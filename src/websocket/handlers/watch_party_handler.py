# tools/websocket/handlers/watch_party_handler.py

from fastapi import WebSocket
from typing import Dict, Any

from src.messaging.messaging_service import (
    create_post,
    delete_watch_party_thread,
    get_thread_details,
    join_watch_party_thread,
    leave_thread,
    list_watch_parties_for_file,
    transfer_thread_ownership,
    ban_user_from_thread,
    _get_user_thread_role_logic
)
from src.db_utils import AsyncDBContext
from src.websocket.dispatcher import get_websocket_dispatcher
from src.websocket.HelperWebsocket import get_websocket_manager

dispatcher = get_websocket_dispatcher()
manager = get_websocket_manager()

# --- Watch Party Lifecycle Handlers (Unchanged) ---
@dispatcher.on("join_watch_party")
async def handle_join_watch_party(websocket: WebSocket, payload: dict, **kwargs):
    """
    Handles a request from a client to join a watch party using an invite code.
    """
    invite_code = payload.get("invite_code")
    user_data = manager.get_user_for_websocket(websocket)

    # Basic validation
    if not invite_code or not user_data:
        await manager.send_json_message({
            "type": "error", 
            "payload": "Missing invite code or user not authenticated."
        }, websocket)
        return

    user_id = user_data.get("id")
    if not user_id: return

    # Call the service to handle database logic for joining the party
    thread_id = await join_watch_party_thread(user_id=user_id, invite_code=invite_code)

    if thread_id:
        # --- SUCCESS! The user has joined the party in the database. ---

        # 1. Join the WebSocket to the specific chat room for this party.
        await manager.join_room(websocket, thread_id)

        # 2. Send a success message back ONLY to the user who just joined.
        await manager.send_json_message({
            "type": "joined_watch_party_success",
            "payload": {
                "thread_id": thread_id,
                "invite_code": invite_code
            }
        }, websocket)

        # 3. Notify everyone else *already in the party room* that a new user has joined.
        join_notification = {
            "type": "user_joined_thread",
            "payload": {
                "thread_id": thread_id,
                "user": {
                    "id": user_id,
                    "username": user_data.get("username", "A new user"),
                    "role": "member"
                }
            }
        }
        await manager.broadcast_to_room(thread_id, join_notification, sender_websocket=websocket)

        # 4. --- TARGETED BROADCAST ---
        #    Notify everyone viewing the file page that the list of parties has changed.
        thread_details = await get_thread_details(thread_id)
        if thread_details and thread_details.get('content_id'):
            file_id = thread_details['content_id']
            observation_room = f"file-viewers:{file_id}"
            update_message = {"type": "party_list_updated", "payload": {"fileId": file_id}}
            await manager.broadcast_to_room(observation_room, update_message)

    else:
        # --- FAILURE. The invite code was invalid or another error occurred. ---
        await manager.send_json_message({
            "type": "join_watch_party_failed",
            "payload": "Failed to join watch party. The code may be invalid or expired."
        }, websocket)


@dispatcher.on("leave_watch_party")
async def handle_leave_watch_party(websocket: WebSocket, **kwargs):
    """
    Handles a user leaving their current watch party.
    This now correctly updates the database and deletes the thread if it becomes empty.
    """
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_chat_room_for_websocket(websocket)

    if not user_data or not thread_id:
        await manager.send_json_message({"type": "error", "payload": "Not in a party."}, websocket)
        return

    user_id = user_data.get("id")
    if not user_id:
        return

    # --- DATABASE AND STATE UPDATE ---
    # 1. Get file_id for final broadcast BEFORE any data is deleted.
    file_id = None
    thread_details = await get_thread_details(thread_id)
    if thread_details:
        file_id = thread_details.get('content_id')

    # 2. Remove the user from the thread in the DATABASE.
    await leave_thread(user_id=user_id, thread_id=thread_id)

    # 3. Remove the WebSocket from the party's chat room in the MANAGER.
    await manager.leave_room(websocket, thread_id)
    
    # 4. Tell the user's client that they have successfully left.
    await manager.send_json_message({"type": "watch_party_left"}, websocket)

    # --- NOTIFICATIONS AND CLEANUP ---
    # 5. Notify everyone else *remaining in the party room* that the user has left.
    leave_notification = {
        "type": "user_left_thread",
        "payload": {"user_id": user_id, "thread_id": thread_id}
    }
    await manager.broadcast_to_room(thread_id, leave_notification)

    # 6. Check if the party is now empty (both in DB and WebSocket manager).
    #    The `list_watch_parties_for_file` function is the source of truth for member count.
    if file_id:
        parties = await list_watch_parties_for_file(file_id)
        current_party = next((p for p in parties if p['id'] == thread_id), None)
        
        # If the party no longer exists or has 0 members, delete it.
        if not current_party or current_party['member_count'] == 0:
            await delete_watch_party_thread(thread_id=thread_id)

        # 7. Notify everyone viewing the file page that the list of parties has changed.
        observation_room = f"file-viewers:{file_id}"
        update_message = {"type": "party_list_updated", "payload": {"fileId": file_id}}
        await manager.broadcast_to_room(observation_room, update_message)

@dispatcher.on("player_state_update")
async def handle_player_state_update(websocket: WebSocket, payload: Dict[str, Any], **kwargs):
    """
    Receives and broadcasts player state updates only for watch parties.
    """
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_chat_room_for_websocket(websocket) # Safe here

    if not user_data or not thread_id:
        return

    user_id = user_data.get("id")
    async with AsyncDBContext() as db:
        user_role = await _get_user_thread_role_logic(db, user_id, thread_id)
        if user_role == 'owner':
            message = {"type": "player_state_update", "payload": payload}
            await manager.broadcast_to_room(thread_id, message, sender_websocket=websocket)

@dispatcher.on("send_message_to_party")
async def handle_send_message_to_party(websocket: WebSocket, payload: dict, **kwargs):
    """
    Handles sending a chat message within a watch party.
    The thread_id is inferred from the user's current room.
    """
    content = payload.get("content")
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_chat_room_for_websocket(websocket) # Safe here

    if not all([content, user_data, thread_id]):
        return

    new_post = await create_post(
        sender_id=user_data.get("id"),
        thread_id=thread_id,
        content=content
    )

    if new_post:
        await manager.broadcast_to_room(thread_id, {"type": "new_chat_message", "payload": new_post})


@dispatcher.on("transfer_ownership_in_party")
async def handle_transfer_ownership_in_party(websocket: WebSocket, payload: Dict[str, Any], **kwargs):
    """Handles ownership transfer specifically within a watch party."""
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_chat_room_for_websocket(websocket) # Safe here

    if not user_data or not thread_id:
        return

    current_owner_id = user_data.get("id")
    new_owner_id = payload.get("new_owner_id")

    success = await transfer_thread_ownership(thread_id, current_owner_id, new_owner_id)
    if success:
        await manager.broadcast_to_room(thread_id, {"type": "thread_members_updated"})


@dispatcher.on("ban_user_from_party")
async def handle_ban_user_from_party(websocket: WebSocket, payload: Dict[str, Any], **kwargs):
    """Handles banning a user specifically from a watch party."""
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_chat_room_for_websocket(websocket) # Safe here

    if not user_data or not thread_id:
        return

    banning_user_id = user_data.get("id")
    banned_user_id = payload.get("banned_user_id")

    success = await ban_user_from_thread(thread_id, banning_user_id, banned_user_id)
    if success:
        await manager.broadcast_to_room(thread_id, {
            "type": "user_banned",
            "payload": {"thread_id": thread_id, "banned_user_id": banned_user_id}
        })