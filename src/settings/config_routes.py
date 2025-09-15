import json
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

from ConfigMedia import get_config
from src.models import UserProfile
from src.roles.permissions import Permission
from src.roles.rbac_manager import require_permission_from_profile
from src.users.auth_dependency import get_current_active_user
import logging

logger = logging.getLogger("ConfigRoutes")
logger.setLevel(logging.DEBUG)
config_manager = get_config()
router = APIRouter()

@router.get("/config/schema", response_model=List[Dict[str, Any]])
@require_permission_from_profile(Permission.ADMIN_CONFIG_VIEW)
async def get_config_schema(current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrieves the raw configuration schema from the config file.
    Requires admin privileges.
    """
    logger.info(f"User {current_user.username} requested config schema.")
    try:
        with open(config_manager.paths[0], 'r') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Failed to read or parse config file: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve configuration schema.")

@router.get("/config", response_model=Dict[str, Any])
@require_permission_from_profile(Permission.ADMIN_CONFIG_VIEW)
async def get_current_config(current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrierves the current configuration settings.
    Requires admin privileges.
    """
    logger.info(f"User {current_user.username} requested config.")
    return config_manager.data

@router.put("/config")
@require_permission_from_profile(Permission.ADMIN_CONFIG_EDIT)
async def update_configuration(updates: Dict[str, Any], current_user: UserProfile = Depends(get_current_active_user)):
    """
    Updates configuration settings.
    Requires admin privileges.
    """
    logger.info(f"User {current_user.username} attempting to update config with: {updates}")
    
    if not updates:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No configuration updates provided.")

    try:
        config_manager.update_config(updates)
        config_manager.reload()
        logger.info(f"Configuration updated successfully by {current_user.username}.")
        return {"message": "Configuration updated successfully", "new_config": config_manager.data}
    except Exception as e:
        logger.error(f"Failed to update configuration: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to update configuration: {e}")
