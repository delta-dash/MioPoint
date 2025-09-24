# auth_config.py
"""
Manages JWT (JSON Web Token) configuration and secret key storage.

This module provides a secure and automated way to handle the application's
secret key. The key is stored in the operating system's native credential
store (keyring) for enhanced security.

Features:
- On first import, it automatically generates a cryptographically secure
  secret key if one does not already exist in the keyring.
- Retrieves the existing key on subsequent imports.
- Provides a command-line utility to manually view or overwrite the key.

To manage the key manually, run this file from your terminal:
  python auth_config.py
"""

import keyring
import secrets
import sys
import logging
from keyring.errors import NoKeyringError

# ==============================================================================
# --- CORE CONFIGURATION ---
# ==============================================================================

# Define a unique service name for your application's secrets.
# This isolates it from other applications using the keyring.
SERVICE_NAME = "Miopoint"

# Define a specific "username" (i.e., key name) for the JWT secret.
# This helps avoid conflicts if you store other secrets for the same service.
JWT_SECRET_USERNAME = "jwt_secret_key"

# JWT algorithm and token expiration settings.
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
# Set a very long refresh token lifespan (e.g., 100 years).
REFRESH_TOKEN_EXPIRE_DAYS: int = 36500

# Get a logger for this module
logger = logging.getLogger(__name__)

# ==============================================================================
# --- KEYRING HELPER FUNCTIONS ---
# ==============================================================================

def store_jwt_secret(secret_key: str):
    """
    Stores the JWT secret string securely in the system's keyring.

    Args:
        secret_key: The secret string to store.

    Raises:
        SystemExit: If the keyring backend is not available or fails.
    """
    try:
        keyring.set_password(SERVICE_NAME, JWT_SECRET_USERNAME, secret_key)
        logger.info(f"Secret key successfully stored in system keyring for service '{SERVICE_NAME}'.")
    except Exception as e:
        logger.critical(
            "Could not store secret in keyring. "
            "Your system may not have a supported backend (e.g., dbus-launch). "
            f"Original error: {e}",
            exc_info=True
        )
        sys.exit(1)


def retrieve_jwt_secret() -> str | None:
    """
    Retrieves the JWT secret string from the system's keyring.

    Returns:
        The secret key as a string if found, otherwise None.

    Raises:
        SystemExit: If the keyring backend is not available or fails.
    """
    try:
        return keyring.get_password(SERVICE_NAME, JWT_SECRET_USERNAME)
    except (NoKeyringError, Exception) as e:
        logger.critical(
            "Could not retrieve secret from keyring. "
            "Your system may not have a supported backend. "
            f"Original error: {e}",
            exc_info=True
        )
        sys.exit(1)


# ==============================================================================
# --- INITIALIZATION LOGIC ---
# ==============================================================================

def _initialize_secret() -> str:
    """
    Ensures a secret key exists, creating one if necessary.

    This is the main logic that runs when the module is imported. It attempts
    to retrieve the key and, if it fails, generates, stores, and returns a
    new one.
    """
    secret = retrieve_jwt_secret()

    if secret:
        logger.info(f"✅ JWT secret key found and loaded from system keyring for '{SERVICE_NAME}'.")
        return secret
    else:
        logger.warning(f"ℹ️ No secret key found for '{SERVICE_NAME}'. Generating a new one...")
        new_secret = secrets.token_hex(32)
        store_jwt_secret(new_secret)
        logger.info("🔑 New secret key has been generated and securely stored.")
        return new_secret

# --- LOAD OR CREATE THE SECRET KEY ON MODULE IMPORT ---
SECRET_KEY = _initialize_secret()


# ==============================================================================
# --- COMMAND-LINE UTILITY ---
# ==============================================================================

def _main_cli():
    """Provides a command-line interface for managing the secret key."""
    print("--- JWT Secret Key Management Utility ---")

    existing_key = retrieve_jwt_secret()
    if existing_key:
        print(f"ℹ️ A secret key already exists in the keyring for '{SERVICE_NAME}'.")
        # For security, you might want to avoid printing the key.
        # If you need to see it, uncomment the line below.
        # print(f"   Existing Key: {existing_key}")

        overwrite = input("\nDo you want to overwrite it with a new one? (yes/no): ").lower().strip()
        if overwrite not in ('yes', 'y'):
            print("Operation cancelled. The existing key was not changed.")
            sys.exit(0)
    else:
        print("ℹ️ No existing key was found. A new key will be generated.")

    # Generate and store a new key
    new_key = secrets.token_hex(32)
    print("\nWe have generated a new, secure secret key.")
    print("It will be stored in your operating system's native credential store.")

    confirm = input("Do you want to store this new key now? (yes/no): ").lower().strip()
    if confirm in ('yes', 'y'):
        store_jwt_secret(new_key)
    else:
        print("Operation cancelled. No key was stored.")

if __name__ == '__main__':
    _main_cli()