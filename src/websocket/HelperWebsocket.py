# tools/websocket/HelperWebsocket.py (Full, Corrected Code)

import asyncio
import json
from typing import Dict, Any, List, Optional, Union, Set # CHANGED: Added Set

from fastapi import WebSocket
import logging

logger = logging.getLogger("Server")
logger.setLevel(logging.DEBUG)

class WebSocketConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[WebSocket]] = {}
        self.user_data_map: Dict[WebSocket, Dict[str, Any]] = {}
        self.pending_leave_tasks: Dict[int, asyncio.Task] = {}
        
        # --- Party & Observation Room Management ---
        # A party room ID is the integer thread_id.
        self.party_rooms: Dict[int, List[WebSocket]] = {}
        # An observation room ID is a string like 'instance-viewers:123'.
        self.observation_rooms: Dict[str, List[WebSocket]] = {}
        # Maps a WebSocket to a SET of room_ids it's in.
        self.socket_to_rooms: Dict[WebSocket, Set[Union[int, str]]] = {}
        
    def is_in_room(self, websocket: WebSocket, room_id: Union[int, str]) -> bool:
        """Checks if a specific websocket is in a specific room."""
        user_rooms = self.socket_to_rooms.get(websocket)
        if user_rooms is None:
            return False
        return room_id in user_rooms
    
    def get_users_in_room(self, room_id: Union[int, str]) -> List[dict]:
        users = []
        sockets_in_room = self.party_rooms.get(room_id, []) if isinstance(room_id, int) else self.observation_rooms.get(room_id, [])

        for websocket in sockets_in_room:
            user_data = self.user_data_map.get(websocket)
            if user_data:
                users.append({
                    "id": user_data.get("id"),
                    "username": user_data.get("username")
                })
        return users

    async def connect(self, websocket: WebSocket, user_data: Dict[str, Any]):
        user_id = user_data.get("id")
        if not user_id:
            logger.error("Attempted to connect WebSocket without a user_id.")
            return

        # --- NEW: Cancel any pending leave task for this user ---
        # This handles the case where a user reconnects (e.g. page refresh)
        # before the grace period for leaving a party has expired.
        if user_id in self.pending_leave_tasks:
            pending_task = self.pending_leave_tasks.pop(user_id)
            pending_task.cancel()
            logger.info(f"User '{user_data.get('username')}' reconnected. Cancelling pending party leave.")
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)
        self.user_data_map[websocket] = user_data
        logger.info(f"Manager registered connection for user '{user_data.get('username')}' (ID: {user_id}).")

    async def disconnect(self, websocket: WebSocket):
        # CHANGED: Must leave all rooms on disconnect
        rooms_to_leave = list(self.socket_to_rooms.get(websocket, set()))
        for room_id in rooms_to_leave:
            self.leave_room_sync(websocket, room_id)

        user_data = self.user_data_map.pop(websocket, None)
        user_id = user_data.get("id") if user_data else None

        if user_id in self.active_connections and websocket in self.active_connections[user_id]:
            self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
        
        username = user_data.get('username') if user_data else 'Unknown'
        logger.info(f"Manager disconnected user '{username}'.")
    
    def get_user_for_websocket(self, websocket: WebSocket) -> Optional[Dict[str, Any]]:
        return self.user_data_map.get(websocket)
    
    # --- Room Management Methods ---

    async def join_room(self, websocket: WebSocket, room_id: Union[int, str]):
        # 1. Add to the appropriate room dictionary based on type.
        if isinstance(room_id, int):
            if room_id not in self.party_rooms:
                self.party_rooms[room_id] = []
            if websocket not in self.party_rooms[room_id]:
                self.party_rooms[room_id].append(websocket)
        else:
            if room_id not in self.observation_rooms:
                self.observation_rooms[room_id] = []
            if websocket not in self.observation_rooms[room_id]:
                self.observation_rooms[room_id].append(websocket)

        # 2. Add to the socket's personal room set
        if websocket not in self.socket_to_rooms:
            self.socket_to_rooms[websocket] = set()
        self.socket_to_rooms[websocket].add(room_id)
        
        user_data = self.get_user_for_websocket(websocket)
        username = user_data.get('username') if user_data else 'Unknown'
        logger.info(f"User '{username}' joined room '{room_id}'.")

    async def leave_room(self, websocket: WebSocket, room_id: Union[int, str]):
        self.leave_room_sync(websocket, room_id)

    def leave_room_sync(self, websocket: WebSocket, room_id: Union[int, str]):
        # 1. Remove from the appropriate room dictionary.
        if isinstance(room_id, int):
            if room_id in self.party_rooms:
                if websocket in self.party_rooms[room_id]:
                    self.party_rooms[room_id].remove(websocket)
                if not self.party_rooms[room_id]:
                    del self.party_rooms[room_id]
        else:
            if room_id in self.observation_rooms:
                if websocket in self.observation_rooms[room_id]:
                    self.observation_rooms[room_id].remove(websocket)
                if not self.observation_rooms[room_id]:
                    del self.observation_rooms[room_id]

        # 2. Remove from the socket's personal room set
        if websocket in self.socket_to_rooms and room_id in self.socket_to_rooms[websocket]:
            self.socket_to_rooms[websocket].remove(room_id)
            if not self.socket_to_rooms[websocket]:
                del self.socket_to_rooms[websocket]
        
        user_data = self.get_user_for_websocket(websocket)
        username = user_data.get('username') if user_data else 'Unknown'
        logger.info(f"User '{username}' left room '{room_id}'.")

    async def remove_user_from_room(self, user_id: int, room_id: Union[int, str]):
        """Removes all of a user's connections from a specific room."""
        connections = self.active_connections.get(user_id, [])
        if not connections:
            return

        # Use a copy of the list to iterate while modifying the original state via leave_room_sync
        for websocket in list(connections):
            self.leave_room_sync(websocket, room_id)

        # Get user data from any connection for logging
        if connections:
            user_data = self.get_user_for_websocket(connections[0])
            username = user_data.get('username') if user_data else 'Unknown'
            logger.info(f"Removed all connections for user '{username}' (ID: {user_id}) from room '{room_id}'.")

    async def broadcast_to_room(self, room_id: Union[int, str], message: dict, sender_websocket: Optional[WebSocket] = None):
        if isinstance(room_id, int):
            sockets_in_room = self.party_rooms.get(room_id)
        else:
            sockets_in_room = self.observation_rooms.get(room_id)

        if not sockets_in_room:
            return
        message_str = json.dumps(message)
        send_tasks = [
            connection.send_text(message_str)
            for connection in sockets_in_room
            if connection != sender_websocket
        ]
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)

    # NEW HELPER METHODS
    def get_party_room_for_websocket(self, websocket: WebSocket) -> Optional[int]:
        """Finds the integer-based party room for a given WebSocket."""
        user_rooms = self.socket_to_rooms.get(websocket, set())
        for room_id in user_rooms:
            if isinstance(room_id, int):
                return room_id
        return None

    def get_rooms_for_websocket(self, websocket: WebSocket) -> Set[Union[int, str]]:
        """Gets the set of all rooms for a WebSocket."""
        return self.socket_to_rooms.get(websocket, set())
    
    async def send_personal_message_to_user(self, user_id: int, message: dict):
        """Sends a JSON message to all active connections for a specific user."""
        if user_id is None:
            logger.error(f"Attempted to send personal message to a user with ID None.")
            return

        connections = self.active_connections.get(user_id)
        if not connections:
            # This is not an error, the user might just be offline.
            return

        message_str = json.dumps(message)
        send_tasks = [conn.send_text(message_str) for conn in connections]
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)

    # ... (send_json_message, send_personal_message, and singleton logic are unchanged) ...
    async def send_json_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.warning(f"Failed to send JSON to websocket, may disconnect: {e}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.warning(f"Failed to send text to websocket, may disconnect: {e}")

_manager_instance: Optional[WebSocketConnectionManager] = None
def get_websocket_manager() -> WebSocketConnectionManager:
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = WebSocketConnectionManager()
    return _manager_instance