# tools/websocket/handlers/watch_party_handler.py

import asyncio
from fastapi import WebSocket
from src.media.HelperMedia import get_instance_details, get_an_instance_for_content
from fastapi.encoders import jsonable_encoder
from typing import Dict, Any

from src.messaging.messaging_service import (
    create_post,
    delete_watch_party_thread,
    get_thread_details,
    get_user_active_watch_party,
    join_watch_party_thread,
    leave_thread,
    list_watch_parties_for_file,
    transfer_thread_ownership,
	update_watch_party_file,
	ban_user_from_thread,
    _get_user_thread_role_logic
)
from src.db_utils import AsyncDBContext
from src.websocket.dispatcher import get_websocket_dispatcher
from src.websocket.HelperWebsocket import WebSocketConnectionManager, get_websocket_manager

dispatcher = get_websocket_dispatcher()
manager = get_websocket_manager()

async def _perform_full_party_leave(user_id: int, thread_id: int, manager: WebSocketConnectionManager):
    """
    Helper function to fully remove a user from a party in the database,
    update WebSocket manager state, and notify all relevant clients.
    This is the core logic for both explicit "leave" actions and final disconnects.
    """
    # --- PRE-LEAVE DATA GATHERING ---
    # Get content_id for the final broadcast to instance-viewers BEFORE any data is deleted.
    thread_details = await get_thread_details(thread_id)
    content_id = thread_details.get('content_id') if thread_details else None

    # --- DATABASE UPDATE ---
    # Atomically remove the user from the thread in the DATABASE and get the new owner, if any.
    success, new_owner_id = await leave_thread(user_id=user_id, thread_id=thread_id)
    if not success:
        # This could happen if the user was already removed (e.g., by another tab, or banned).
        # The desired state (user not in party) is already achieved, so we can stop.
        return

    # --- MANAGER STATE CLEANUP & BROADCASTS ---
    # 1. Clean up ALL of the user's websocket connections from the manager's room state.
    await manager.remove_user_from_room(user_id, thread_id)

    # 2. Explicitly tell ALL of the user's connections they have left the party.
    await manager.send_personal_message_to_user(user_id, {"type": "watch_party_left"})

    # 3. Notify everyone else *remaining in the party room* that the user has left.
    leave_notification = {
        "type": "user_left_thread",
        "payload": {"user_id": user_id, "thread_id": thread_id}
    }
    await manager.broadcast_to_room(thread_id, leave_notification)

    # 4. If ownership was transferred, notify the new owner directly.
    if new_owner_id:
        ownership_notification = {
            "type": "ownership_transferred",
            "payload": {"thread_id": thread_id}
        }
        await manager.send_personal_message_to_user(new_owner_id, ownership_notification)

    # 5. Check if the party is now empty and, if so, delete it.
    if content_id:
        parties = await list_watch_parties_for_file(content_id)
        is_party_empty = not any(p['id'] == thread_id and p['member_count'] > 0 for p in parties)
        
        if is_party_empty:
            await delete_watch_party_thread(thread_id=thread_id)
        
        # 6. Notify viewers of the file page that the party list has changed.
        async with AsyncDBContext() as db:
            instance_id_for_broadcast = await get_an_instance_for_content(content_id, user_id, db)
            if instance_id_for_broadcast:
                updated_parties = await list_watch_parties_for_file(content_id, db)
                observation_room = f"instance-viewers:{instance_id_for_broadcast}"
                update_message = {"type": "party_list_updated", "payload": {"fileId": instance_id_for_broadcast, "parties": jsonable_encoder(updated_parties)}}
                await manager.broadcast_to_room(observation_room, update_message)

async def _schedule_delayed_leave(user_id: int, thread_id: int, manager: WebSocketConnectionManager):
    """
    A coroutine that waits for a grace period and then performs a full party leave.
    This task is stored in the manager and can be cancelled if the user reconnects.
    """
    LEAVE_GRACE_PERIOD_SECONDS = 10
    try:
        await asyncio.sleep(LEAVE_GRACE_PERIOD_SECONDS)
        # If we got here, the task was not cancelled by a reconnection.
        #logger.info(f"User {user_id} did not reconnect in {LEAVE_GRACE_PERIOD_SECONDS}s. Performing full party leave from thread {thread_id}.")
        await _perform_full_party_leave(user_id, thread_id, manager)
    #except asyncio.CancelledError:
        # This is the expected outcome if the user reconnects in time.
        #logger.info(f"Delayed leave for user {user_id} from thread {thread_id} was cancelled due to reconnection.")
    finally:
        # Ensure the task is removed from the manager's tracking dict.
        if user_id in manager.pending_leave_tasks:
            del manager.pending_leave_tasks[user_id]

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

    # The service function now handles leaving any old party and returns details.
    join_result = await join_watch_party_thread(user_id=user_id, invite_code=invite_code)

    if not join_result:
        # --- FAILURE. The invite code was invalid or another error occurred. ---
        await manager.send_json_message({
            "type": "join_watch_party_failed",
            "payload": "Failed to join watch party. The code may be invalid or expired."
        }, websocket)
        return

    # --- DB JOIN SUCCESSFUL ---
    # Now that the user is confirmed in the party in the DB, we can update the websocket manager state and notify clients.
    thread_id = join_result.get("thread_id")
    left_party_details = join_result.get("left_party")

    # --- 1. HANDLE OLD PARTY (if any) ---
    if left_party_details:
        old_thread_id = left_party_details["thread_id"]
        old_content_id = left_party_details["content_id"]
        new_owner_id_for_old_party = left_party_details["new_owner_id"]

        # a. Leave the old websocket room for this specific connection.
        await manager.leave_room(websocket, old_thread_id)

        # b. Notify remaining members of the OLD party that the user has left.
        leave_notification = {
            "type": "user_left_thread",
            "payload": {"user_id": user_id, "thread_id": old_thread_id}
        }
        await manager.broadcast_to_room(old_thread_id, leave_notification)

        # c. If ownership was transferred in the OLD party, notify the new owner.
        if new_owner_id_for_old_party:
            ownership_notification = {
                "type": "ownership_transferred",
                "payload": {"thread_id": old_thread_id}
            }
            await manager.send_personal_message_to_user(new_owner_id_for_old_party, ownership_notification)

        # d. Notify viewers of the OLD party's file that the party list has changed.
        async with AsyncDBContext() as db:
            old_instance_id = await get_an_instance_for_content(old_content_id, user_id, db)
            if old_instance_id:
                old_parties = await list_watch_parties_for_file(old_content_id, db)
                old_observation_room = f"instance-viewers:{old_instance_id}"
                update_message = {
                    "type": "party_list_updated",
                    "payload": {
                        "fileId": old_instance_id, "parties": jsonable_encoder(old_parties)
                    }
                }
                await manager.broadcast_to_room(old_observation_room, update_message)

        # e. Explicitly tell ALL of the user's connections they have left the old party.
        user_left_message = {
            "type": "watch_party_left",
            "payload": {"reason": "joined_another_party", "thread_id": old_thread_id}
        }
        await manager.send_personal_message_to_user(user_id, user_left_message)

    # --- 2. JOIN THE NEW PARTY ---
    # This is now guaranteed to happen if join_result was successful.
    await manager.join_room(websocket, thread_id)

    # --- 3. GATHER DETAILS FOR NOTIFICATIONS ---
    thread_details = await get_thread_details(thread_id)
    content_id = thread_details.get('content_id') if thread_details else None

    instance_id_for_nav = None
    if content_id:
        async with AsyncDBContext() as db:
            instance_id_for_nav = await get_an_instance_for_content(content_id, user_id, db)

    if not instance_id_for_nav:
        # This is a non-fatal error. The user is in the party but can't navigate.
        await manager.send_json_message({"type": "join_watch_party_failed", "payload": "Could not find a viewable file for this party."}, websocket)
        # Do not return, proceed to send success message so they can at least chat.

    async with AsyncDBContext() as db:
        role = await _get_user_thread_role_logic(db, user_id, thread_id)
    is_owner = (role == 'owner')

    # --- 4. NOTIFY THE JOINING USER ---
    await manager.send_json_message({
        "type": "joined_watch_party_success",
        "payload": {"thread_id": thread_id, "file_id": instance_id_for_nav, "is_owner": is_owner}
    }, websocket)

    # --- 5. NOTIFY OTHERS IN THE PARTY ---
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

    # --- 6. NOTIFY FILE VIEWERS ---
    if instance_id_for_nav:
        new_parties = await list_watch_parties_for_file(content_id)
        observation_room = f"instance-viewers:{instance_id_for_nav}"
        update_message = {
            "type": "party_list_updated",
            "payload": {
                "fileId": instance_id_for_nav, "parties": jsonable_encoder(new_parties)
            }
        }
        await manager.broadcast_to_room(observation_room, update_message)

@dispatcher.on("get_my_party_status")
async def handle_get_my_party_status(websocket: WebSocket, **kwargs):
    """
    Client is asking for its current party status, usually upon a new connection/tab.
    If the user is in a party, this will send a 'joined_watch_party_success' message
    to sync the client's state.
    """
    user_data = manager.get_user_for_websocket(websocket)
    if not user_data:
        return # Not authenticated yet

    user_id = user_data.get("id")
    if not user_id:
        return

    active_party = await get_user_active_watch_party(user_id)
    if active_party:
        thread_id = active_party.get("thread_id")
        content_id = active_party.get("content_id")
        role = active_party.get("role")
        is_owner = (role == 'owner')

        # We need a navigable instance_id for the frontend to link to.
        instance_id_for_nav = None
        if content_id:
            async with AsyncDBContext() as db:
                instance_id_for_nav = await get_an_instance_for_content(content_id, user_id, db)

        if instance_id_for_nav:
            # IMPORTANT: Join this specific websocket to the party's chat room
            # so it receives future updates for this party.
            await manager.join_room(websocket, thread_id)

            # Send a DIFFERENT message type so the client can distinguish
            # this passive status update from an active "join" action. This
            # prevents auto-navigation on new tabs.
            await manager.send_json_message({
                "type": "active_party_info",
                "payload": {
                    "thread_id": thread_id,
                    "file_id": instance_id_for_nav,
                    "is_owner": is_owner
                }
            }, websocket)

@dispatcher.on("leave_watch_party")
async def handle_leave_watch_party(websocket: WebSocket, **kwargs):
    """
    Handles a user explicitly leaving their current watch party. This triggers
    a full removal from the party in the database.
    """
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_party_room_for_websocket(websocket)

    if not user_data or not thread_id:
        await manager.send_json_message({"type": "error", "payload": "Not in a party."}, websocket)
        return

    user_id = user_data.get("id")
    if not user_id:
        return

    # Delegate all the complex logic to the shared helper function.
    await _perform_full_party_leave(user_id, thread_id, manager)


@dispatcher.on("change_party_file")
async def handle_change_party_file(websocket: WebSocket, payload: dict, **kwargs):
    """
    Handles a request from a party owner to change the file being watched.
    The owner's client is expected to navigate on its own; this just tells other members.
    """
    user_data = manager.get_user_for_websocket(websocket)
    # Get the thread_id from the manager, which is more secure and reliable than trusting the client payload.
    # This makes it consistent with other in-party actions like 'player_state_update'.
    thread_id = manager.get_party_room_for_websocket(websocket)
    print(thread_id)

    new_instance_id = payload.get("newFileId")
    print(new_instance_id)
    if not all([user_data, thread_id, new_instance_id]):
        return  # Or send an error message

    user_id = user_data.get("id")
    if not user_id:
        return

    # 1. Verify the user is the owner of the party.
    async with AsyncDBContext() as db:
        role = await _get_user_thread_role_logic(db, user_id, thread_id)

    if role != 'owner':
        # Silently fail or send an error message for non-owners.
        return

    # Get details of the old file to notify its viewers later.
    old_thread_details = await get_thread_details(thread_id)
    old_content_id = old_thread_details.get('content_id') if old_thread_details else None

    # 2. Get the content_id from the instance_id sent by the client.
    instance_details = await get_instance_details(new_instance_id, user_id=user_id)
    if not instance_details:
        # The file doesn't exist or user can't access it.
        # You could optionally send an error back to the owner here.
        return
    new_content_id = instance_details['content_id']


    # 3. Update the content_id in the database for the thread.
    success = await update_watch_party_file(thread_id=thread_id, new_file_id=new_content_id, acting_user_id=user_id)

    if success:
        # 4. Broadcast the change to ALL members of the party, including the owner.
        #    This ensures the owner's client state is also updated authoritatively after navigating.
        notification = {
            "type": "party_file_changed",
            "payload": {"newFileId": new_instance_id}
        }
        await manager.broadcast_to_room(thread_id, notification)

        # 5. Broadcast party list updates to relevant file viewers.
        #    a) Notify viewers of the NEW file that a party is now associated with it.
        new_parties = await list_watch_parties_for_file(new_content_id)
        new_observation_room = f"instance-viewers:{new_instance_id}"
        new_update_message = {"type": "party_list_updated", "payload": {"fileId": new_instance_id, "parties": jsonable_encoder(new_parties)}}
        await manager.broadcast_to_room(new_observation_room, new_update_message)

        #    b) Notify viewers of the OLD file that a party is no longer associated with it.
        if old_content_id and old_content_id != new_content_id:
            async with AsyncDBContext() as db:
                # We need an instance_id to broadcast to. We'll find one for the old content.
                old_instance_id = await get_an_instance_for_content(old_content_id, user_id, db)
                if old_instance_id:
                    old_parties = await list_watch_parties_for_file(old_content_id, db)
                    old_observation_room = f"instance-viewers:{old_instance_id}"
                    old_update_message = {"type": "party_list_updated", "payload": {"fileId": old_instance_id, "parties": jsonable_encoder(old_parties)}}
                    await manager.broadcast_to_room(old_observation_room, old_update_message)


@dispatcher.on("player_state_update")
async def handle_player_state_update(websocket: WebSocket, payload: Dict[str, Any], **kwargs):
    """
    Receives and broadcasts player state updates only for watch parties.
    """
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_party_room_for_websocket(websocket) # Safe here

    if not user_data or not thread_id:
        return

    user_id = user_data.get("id")
    async with AsyncDBContext() as db:
        user_role = await _get_user_thread_role_logic(db, user_id, thread_id)
        if user_role == 'owner':
            message = {"type": "player_state_update", "payload": payload}
            await manager.broadcast_to_room(thread_id, message, sender_websocket=websocket)

@dispatcher.on("client_disconnected")
async def handle_client_disconnected(websocket: WebSocket, payload: dict, **kwargs):
    """
    Handles cleanup when a WebSocket connection is lost.

    If it's the user's last connection, schedules a delayed task to remove them
    from the party. This gives them a grace period to reconnect (e.g., on page refresh).
    """
    # 1. Get user and room info BEFORE disconnecting them from the manager.
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_party_room_for_websocket(websocket)

    # 2. Now, perform the actual cleanup in the connection manager.
    # This removes the websocket from all lists and rooms.
    await manager.disconnect(websocket)

    # 3. If the user was in a party, decide what to do next.
    if thread_id and user_data:
        user_id = user_data.get('id')
        if not user_id:
            return

        # Check if the user has any OTHER active connections.
        # This check happens *after* manager.disconnect(), so if the list is
        # empty, we know this was the user's last connection.
        if not manager.active_connections.get(user_id):
            # This was the last connection. Schedule a delayed, cancellable leave.
            #logger.info(f"User {user_id}'s last connection closed. Scheduling delayed leave from party {thread_id}.")
            leave_task = asyncio.create_task(_schedule_delayed_leave(user_id, thread_id, manager))
            manager.pending_leave_tasks[user_id] = leave_task
        else:
            # User has other connections open. Just notify the party that this
            # one went offline, but don't remove them from the party in the DB.
            notification = {
                "type": "user_left_thread",
                "payload": {"user_id": user_id, "thread_id": thread_id}
            }
            await manager.broadcast_to_room(thread_id, notification)

@dispatcher.on("send_message_to_party")
async def handle_send_message_to_party(websocket: WebSocket, payload: dict, **kwargs):
    """
    Handles sending a chat message within a watch party.
    The thread_id is inferred from the user's current room.
    """
    content = payload.get("content")
    user_data = manager.get_user_for_websocket(websocket)
    thread_id = manager.get_party_room_for_websocket(websocket) # Safe here

    if not all([content, user_data, thread_id]):
        return

    new_post = await create_post(
        sender_id=user_data.get("id"),
        thread_id=thread_id,
        content=content
    )

    if new_post:
        await manager.broadcast_to_room(thread_id, {"type": "new_chat_message", "payload": new_post})