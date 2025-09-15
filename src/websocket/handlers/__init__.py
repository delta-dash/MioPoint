# tools/websocket/handlers/__init__.py

# This file ensures that when the 'handlers' package is imported,
# all handler modules within it are also imported. This is crucial
# because the act of importing them runs the @dispatcher.on(...) decorators,
# which registers the event handlers with the central dispatcher.

from . import chat_handler
from . import watch_party_handler
from . import file_observation_handler
# If you add a new handler file, like 'notifications_handler.py',
# you would add it here:
# from . import notifications_handler