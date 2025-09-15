# EncryptionManager.py
import os
import base64
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

# --- Configuration ---
# It's crucial that these parameters remain consistent for decryption.
SALT_SIZE = 16  # bytes
ITERATIONS = 390_000 # Recommended value by OWASP as of 2021 for PBKDF2-HMAC-SHA256

def _derive_key(password: str, salt: bytes) -> bytes:
    """Derives a secure encryption key from a password and salt."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=ITERATIONS,
        backend=default_backend()
    )
    # Fernet keys must be 32 url-safe base64-encoded bytes.
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    return key

def encrypt(input_path: str, output_path: str, password: str):
    """
    Encrypts a file using a user-provided password.

    The salt is prepended to the encrypted file for later use in decryption.
    """
    # 1. Generate a cryptographically secure random salt
    salt = os.urandom(SALT_SIZE)
    
    # 2. Derive a key from the password and salt
    key = _derive_key(password, salt)
    
    # 3. Read the file content
    with open(input_path, 'rb') as f:
        data = f.read()
        
    # 4. Encrypt the data
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(data)
    
    # 5. Write the salt + encrypted data to the output file
    with open(output_path, 'wb') as f:
        f.write(salt)
        f.write(encrypted_data)

def decrypt(input_path: str, output_path: str, password: str) -> bool:
    """
    Decrypts a file using a user-provided password.

    Reads the salt from the beginning of the file to re-derive the key.
    Returns True on success, False on failure (e.g., wrong password).
    """
    try:
        # 1. Read the salt and encrypted data from the input file
        with open(input_path, 'rb') as f:
            salt = f.read(SALT_SIZE)
            encrypted_data = f.read()
            
        # 2. Derive the key using the same password and the extracted salt
        key = _derive_key(password, salt)
        
        # 3. Decrypt the data
        fernet = Fernet(key)
        decrypted_data = fernet.decrypt(encrypted_data)
        
        # 4. Write the decrypted data to the output file
        with open(output_path, 'wb') as f:
            f.write(decrypted_data)
            
        return True
    except (InvalidToken, ValueError, IndexError):
        # InvalidToken: Incorrect password or corrupted data
        # ValueError/IndexError: File is too small or malformed
        return False

def decrypt_to_memory(input_path: str, password: str) -> Optional[bytes]:
    """
    Decrypts a file to an in-memory bytes object.

    Useful for streaming or temporary access without writing to disk.
    Returns the decrypted data as bytes, or None on failure.
    """
    try:
        with open(input_path, 'rb') as f:
            salt = f.read(SALT_SIZE)
            encrypted_data = f.read()
            
        key = _derive_key(password, salt)
        fernet = Fernet(key)
        return fernet.decrypt(encrypted_data)
    except (InvalidToken, ValueError, IndexError, FileNotFoundError):
        return None
    
def re_encrypt_file(file_path: str, old_password: str, new_password: str) -> bool:
    """
    Safely re-encrypts a single file using a temporary file.
    Returns True on success, False on failure.
    """
    if not os.path.exists(file_path):
        raise ValueError(f"File not found during re-encryption: {file_path}")

    temp_path = file_path + ".tmp"
    
    try:
        # Step 1: Decrypt to a temporary file
        if not decrypt(input_path=file_path, output_path=temp_path, password=old_password):
            ValueError(f"DECRYPTION FAILED for {file_path}. The old password may be incorrect or the file is corrupt.")
            
        # Step 2: Encrypt from the temporary file back to the original path
        encrypt(input_path=temp_path, output_path=file_path, password=new_password)
        
        return True

    finally:
        # Step 3: Clean up the temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)