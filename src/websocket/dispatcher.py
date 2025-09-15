# tools/websocket/dispatcher.py

from typing import Dict, Callable, Any
from fastapi import WebSocket
import inspect

from .HelperWebsocket import WebSocketConnectionManager
import logging

logger = logging.getLogger("WS_Dispatcher")
logger.setLevel(logging.DEBUG)

class WebSocketDispatcher:
    """
    Manages and dispatches incoming WebSocket messages to registered handlers.
    Uses a decorator-based approach for registering event handlers.
    """
    def __init__(self):
        # Maps an event_type (str) to an async handler function
        self.handlers: Dict[str, Callable[..., Any]] = {}

    def on(self, event_type: str):
        """
        A decorator to register a function as a handler for a specific event type.
        
        Usage:
            @dispatcher.on("my_event_type")
            async def handle_my_event(manager, websocket, payload):
                ...
        """
        def decorator(func: Callable[..., Any]):
            if not inspect.iscoroutinefunction(func):
                raise TypeError("WebSocket event handler must be an async function.")
            
            if event_type in self.handlers:
                logger.warning(f"Overwriting handler for event type '{event_type}'.")
                
            self.handlers[event_type] = func
            logger.debug(f"Registered handler for event '{event_type}': {func.__name__}")
            return func
        return decorator

    async def dispatch(self, manager: WebSocketConnectionManager, websocket: WebSocket, data: dict):
        """
        Looks up the handler for the message type and executes it.
        """
        event_type = data.get("type")
        payload = data.get("payload", {})

        handler = self.handlers.get(event_type)

        if handler:
            logger.info(f"Dispatching event '{event_type}' to handler '{handler.__name__}'.")
            try:
                # The handler function will be called with these arguments
                await handler(manager=manager, websocket=websocket, payload=payload)
            except Exception as e:
                logger.error(f"Error in handler for event '{event_type}': {e}", exc_info=True)
                await manager.send_json_message({
                    "type": "error", 
                    "payload": f"An error occurred while processing event '{event_type}'."
                }, websocket)
        else:
            logger.warning(f"No handler found for event type '{event_type}'.")
            await manager.send_json_message({
                "type": "error",
                "payload": f"Unknown message type: '{event_type}'"
            }, websocket)

# --- Singleton Pattern for the Dispatcher ---
_dispatcher_instance = None

def get_websocket_dispatcher() -> WebSocketDispatcher:
    global _dispatcher_instance
    if _dispatcher_instance is None:
        _dispatcher_instance = WebSocketDispatcher()
    return _dispatcher_instance