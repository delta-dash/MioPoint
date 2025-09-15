# router_notifications.py (New File)

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from datetime import datetime  # Import the class 'datetime' from the module 'datetime'

from src.models import UserProfile
from src.users.auth_dependency import get_current_active_user
from src.notifications.notification_service import (
    get_unread_notifications_for_user,
    get_unread_notification_count,
    mark_notification_as_read,
    mark_all_notifications_as_read
)

router = APIRouter(
    prefix="/api/notifications",
    tags=["Notifications"],
)

# --- Pydantic Models ---
class NotificationResponse(BaseModel):
    id: int
    actor_id: Optional[int]
    event_type: str
    is_read: bool
    context_data: dict
    created_at: datetime

class UnreadCountResponse(BaseModel):
    count: int

# --- API Routes ---

@router.get("/", response_model=List[NotificationResponse])
async def get_my_notifications(
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Retrieves the current user's most recent unread notifications."""
    notifications = await get_unread_notifications_for_user(user_id=current_user.id)
    return notifications

@router.get("/unread-count", response_model=UnreadCountResponse)
async def get_my_unread_count(
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Gets the count of unread notifications for a badge."""
    count = await get_unread_notification_count(user_id=current_user.id)
    return UnreadCountResponse(count=count)

@router.post("/{notification_id}/mark-read", status_code=status.HTTP_204_NO_CONTENT)
async def mark_one_as_read(
    notification_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Marks a single notification as read."""
    success = await mark_notification_as_read(notification_id=notification_id, user_id=current_user.id)
    if not success:
        raise HTTPException(status_code=404, detail="Notification not found or you do not own it.")
    return

@router.post("/mark-all-read", status_code=status.HTTP_204_NO_CONTENT)
async def mark_all_as_read(
    current_user: UserProfile = Depends(get_current_active_user)
):
    """Marks all of the user's notifications as read."""
    await mark_all_notifications_as_read(user_id=current_user.id)
    return