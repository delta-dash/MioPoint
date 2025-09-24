# tools/websocket/handlers/file_observation_handler.py

from fastapi import WebSocket
from src.websocket.dispatcher import get_websocket_dispatcher
from src.websocket.HelperWebsocket import get_websocket_manager

dispatcher = get_websocket_dispatcher()
manager = get_websocket_manager()

@dispatcher.on("subscribe_to_file_updates")
async def handle_subscribe_to_file(websocket: WebSocket, payload: dict, **kwargs):
    file_id = payload.get("fileId")
    if not file_id:
        return

    # Create a unique, descriptive room name
    room_name = f"instance-viewers:{file_id}"
    
    # The manager's generic join_room works perfectly here.
    # It automatically handles leaving the previous room (whether it was another
    # file-viewer room or a party room).
    await manager.join_room(websocket, room_name)

@dispatcher.on("unsubscribe_from_file_updates")
async def handle_unsubscribe_from_file(websocket: WebSocket, payload: dict, **kwargs):
    file_id = payload.get("fileId")
    if not file_id:
        return

    room_name = f"instance-viewers:{file_id}"
    current_room = manager.get_rooms_for_websocket(websocket)

    # Only leave the room if they are actually in the one they claim to be leaving.
    if current_room == room_name:
        await manager.leave_room(websocket)