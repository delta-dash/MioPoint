# utilsEncryption.py
# --- IMPORTS ---
import base64
import hashlib
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

# --- LOCAL IMPORTS ---
import logging
from ConfigMedia import get_config
from src.db_utils import AsyncDBContext, execute_db_query


# --- CONFIGURATION ---
config = get_config()
logger = logging.getLogger("UtilsEncryption")
logger.setLevel(logging.DEBUG)


# --- UPDATED DATABASE FUNCTION ---
async def is_hash_in_db(file_hash: str) -> bool:
    """
    Asynchronously checks if a file hash exists in the database using the new db_utils.
    """
    db_file = config.get("DB_FILE")
    if not db_file or not os.path.exists(db_file):
        logger.warning(f"Database file not found at path: {db_file}. Assuming hash does not exist.")
        return False

    try:
        # Use the AsyncDBContext to automatically manage the connection lifecycle.
        # It will open, commit (on success) or rollback (on error), and close the connection.
        async with AsyncDBContext() as conn:
            query = "SELECT 1 FROM files WHERE file_hash = ? LIMIT 1"

            # Use the dedicated executor function for clean query handling.
            result = await execute_db_query(
                conn=conn,
                query=query,
                params=(file_hash,),
                fetch_one=True
            )
            # The function returns the row if found, or None.
            # `is not None` correctly converts this to a boolean.
            return result is not None
    except Exception as e:
        # Error logging is already handled in db_utils, but we can add more context here.
        logger.error(f"Failed to check hash in DB for hash starting with '{file_hash[:10]}...': {e}")
        # Return False to prevent crashes; the file will be treated as non-existent in the DB.
        return False



def calculate_file_hash(filepath: str) -> str:
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def _derive_key(password: bytes, salt: bytes) -> bytes:
    """Derives a cryptographic key from a password and salt."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(password))

def encrypt_file(source_path, dest_path, password):
    """Encrypts a file and saves it to a destination path."""
    salt = os.urandom(16)
    key = _derive_key(password, salt)
    with open(source_path, "rb") as f:
        file_data = f.read()
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(file_data)
    with open(dest_path, "wb") as f:
        f.write(salt)
        f.write(encrypted_data)

def decrypt_file_content(encrypted_path, password):
    """Decrypts a file and returns its content as bytes."""
    try:
        with open(encrypted_path, "rb") as f:
            salt = f.read(16)
            encrypted_data = f.read()
        key = _derive_key(password, salt)
        fernet = Fernet(key)
        return fernet.decrypt(encrypted_data)
    except Exception as e:
        logger.exception(f"Decryption failed for {encrypted_path}: {e}")
        return None