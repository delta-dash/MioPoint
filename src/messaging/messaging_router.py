#messaging router.py
"""
API Router for the Chat / Messaging System

This router exposes endpoints for all chat-related functionalities, acting as the
HTTP interface for the business logic defined in `messaging_service.py`.

It has been refactored to align with the new data model:
- 'Conversations' are now 'Threads'.
- 'Messages' are now 'Posts' (which can be nested).
- 'Participants' are now 'Thread Members'.

All core logic, including permission checks, database operations, logging, and
notifications, is handled by the messaging_service.
"""
from datetime import datetime
from typing import Dict, List, Optional, Set, Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

# --- INTEGRATION: Import from the new messaging_service ---
from src.media.HelperMedia import get_instance_details
from src.messaging.messaging_service import (
    create_direct_message_thread,
    create_group_thread,
    create_post,
    create_watch_party_thread,
    delete_post,
    get_posts_for_thread,
    get_public_threads,
    get_thread_details,
    get_thread_members,
    get_user_threads,
    join_thread,
    join_watch_party_thread,
    leave_thread,
    list_watch_parties_for_file,
    update_thread_name,
)
from src.models import UserProfile
from src.roles.permissions import Permission
from src.users.auth_dependency import get_current_active_user
import logging
from src.websocket.HelperWebsocket import get_websocket_manager

logger = logging.getLogger("ROUTER_CHAT")
logger.setLevel(logging.DEBUG)
manager = get_websocket_manager()

# ==============================================================================
# --- Pydantic Models for API Data Validation (Refactored) ---
# ==============================================================================
# These models are updated to match the 'threads' and 'posts' terminology and structure.

class ThreadResponse(BaseModel):
    """Response model for a user's thread/conversation."""
    id: int
    type: str
    name: Optional[str] = None
    picture_url: Optional[str] = None
    role: str

class PublicThreadResponse(BaseModel):
    """Response model for a public thread."""
    id: int
    type: str
    name: Optional[str] = None
    picture_url: Optional[str] = None

class ThreadCreateRequest(BaseModel):
    """Request model for creating a new thread."""
    # Using a Set automatically handles duplicate user IDs.
    participant_ids: Set[int]
    type: str = Field("group", description="Type of thread, e.g., 'group', 'direct_message', or 'public_group'.")
    name: Optional[str] = None

class ThreadCreateResponse(BaseModel):
    """Response model after successfully creating a thread."""
    thread_id: int
    message: str = "Thread created successfully."

class ThreadNameUpdateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)

class PostCreateRequest(BaseModel):
    """Request model for creating a new post (message or reply)."""
    content: str = Field(..., min_length=1, max_length=4000)
    parent_id: Optional[int] = Field(None, description="The ID of the post this is a reply to.")

class PostResponse(BaseModel):
    """Response model for a single post, supporting nested replies."""
    id: int
    content: str
    created_at: datetime
    sender_id: int
    sender_username: str
    parent_id: Optional[int]
    # This enables the nested structure for replies
    replies: List['PostResponse'] = []

# This is crucial for Pydantic to handle the recursive 'replies' field.
PostResponse.update_forward_refs()

class CreateWatchPartyRequest(BaseModel):
    """Request model for creating a new watch party."""
    name: str = Field(..., min_length=3, max_length=100)

class WatchPartyResponse(BaseModel):
    """Response model for a listed watch party."""
    id: int
    name: str
    invite_code: str
    created_at: datetime
    member_count: int

class JoinWatchPartyResponse(BaseModel):
    thread_id: int
    status: str = "Successfully joined watch party."


# ==============================================================================
# --- API Router ---
# ==============================================================================

router = APIRouter(
    prefix="/api/chat",
    tags=["Chat & Messaging"],
)

# ==============================================================================
# --- Thread & Post Management Routes ---
# ==============================================================================

@router.get("/threads", response_model=List[ThreadResponse])
async def get_my_threads(current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrieves a list of all threads (DMs, groups, etc.) the current user is a member of.
    """
    try:
        threads = await get_user_threads(user_id=current_user.id)
        return threads
    except Exception as e:
        logger.error(f"Failed to get threads for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve threads.")

@router.post("/threads", response_model=ThreadCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_new_thread(
    thread_data: ThreadCreateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Creates a new thread. Can be a 'group', 'public_group', or a 'direct_message'.
    For DMs, `participant_ids` must contain exactly one other user.
    """
    thread_id = None
    try:
        if thread_data.type == 'direct_message':
            if len(thread_data.participant_ids) != 1:
                raise HTTPException(status_code=400, detail="Direct messages must have exactly one other participant.")
            
            recipient_id = thread_data.participant_ids.pop()
            thread_id = await create_direct_message_thread(
                creator_id=current_user.id,
                recipient_id=recipient_id
            )
        
        elif thread_data.type in ['group', 'public_group']:
            if not thread_data.name:
                raise HTTPException(status_code=400, detail="Group threads must have a name.")
            
            is_public = thread_data.type == 'public_group'
            new_thread_dict = await create_group_thread(
                creator_id=current_user.id,
                name=thread_data.name,
                participant_ids=thread_data.participant_ids,
                is_public=is_public
            )
            if new_thread_dict:
                thread_id = new_thread_dict.get('id')
            else:
                thread_id = None
        else:
            raise HTTPException(status_code=400, detail=f"Invalid thread type: '{thread_data.type}'.")

        if thread_id is None:
            # This can happen if the user lacks permissions or another error occurs in the service
            raise HTTPException(status_code=400, detail="Could not create thread. Check permissions or participant IDs.")
        
        return ThreadCreateResponse(thread_id=thread_id)
    except Exception as e:
        logger.error(f"Error creating thread for user {current_user.id}: {e}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail="An internal error occurred.")

@router.get("/public_threads", response_model=List[PublicThreadResponse])
async def list_public_threads(current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrieves a list of all public group threads.
    """
    try:
        threads = await get_public_threads()
        return threads
    except Exception as e:
        logger.error(f"Failed to get public threads: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve public threads.")

@router.post("/threads/{thread_id}/join", status_code=status.HTTP_204_NO_CONTENT)
async def join_public_thread_route(
    thread_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Allows a user to join a public thread.
    """
    success = await join_thread(user_id=current_user.id, thread_id=thread_id)
    if not success:
        raise HTTPException(status_code=403, detail="Could not join thread. It might not be public or you may be banned.")
    return

@router.post("/threads/{thread_id}/leave", status_code=status.HTTP_204_NO_CONTENT)
async def leave_thread_route(
    thread_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Allows a user to leave a thread.
    """
    success, _ = await leave_thread(user_id=current_user.id, thread_id=thread_id)
    if not success:
        raise HTTPException(status_code=400, detail="Could not leave thread.")
    return

@router.get("/threads/{thread_id}/posts", response_model=List[PostResponse])
async def get_posts_in_thread_route(
    thread_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Fetches all posts for a given thread, structured in a nested tree for replies.
    Access is granted if the user is a member or if the thread is public (e.g., watch party).
    """
    posts_data = await get_posts_for_thread(thread_id=thread_id, viewing_user_id=current_user.id)
    
    if posts_data is None:
        # The service function returns None if the user lacks access to a non-public thread.
        raise HTTPException(status_code=403, detail="You do not have permission to view this thread.")
        
    return posts_data

@router.get("/threads/{thread_id}/members", response_model=List[Dict[str, Any]])
async def get_thread_members_route(
    thread_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Retrieves a list of all members for a given thread.
    Access is granted if the user is a member or if the thread is public.
    """
    # First, check if the user has access to the thread at all.
    # We can reuse the get_posts_for_thread logic for the access check part.
    posts_data = await get_posts_for_thread(thread_id=thread_id, viewing_user_id=current_user.id)
    if posts_data is None:
        raise HTTPException(status_code=403, detail="You do not have permission to view this thread's members.")

    members = await get_thread_members(thread_id=thread_id)
    return members

@router.get("/threads/{thread_id}/details", response_model=Dict[str, Any])
async def get_thread_details_route(
    thread_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Retrieves details for a given thread.
    Access is granted if the user is a member or if the thread is public.
    """
    # Check for access
    posts_data = await get_posts_for_thread(thread_id=thread_id, viewing_user_id=current_user.id)
    if posts_data is None:
        raise HTTPException(status_code=403, detail="You do not have permission to view this thread's details.")

    details = await get_thread_details(thread_id=thread_id)
    if not details:
        raise HTTPException(status_code=404, detail="Thread not found.")

    return details

@router.put("/threads/{thread_id}/name", status_code=status.HTTP_204_NO_CONTENT)
async def update_thread_name_route(
    thread_id: int,
    request: ThreadNameUpdateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Updates the name of a thread.
    Requires the user to be an owner or moderator of the thread.
    """
    success = await update_thread_name(
        thread_id=thread_id,
        name=request.name,
        user_id=current_user.id
    )
    if not success:
        raise HTTPException(status_code=403, detail="You do not have permission to change this thread's name.")
    
    # Also broadcast an update to all members of the thread
    update_message = {"type": "thread_updated", "payload": {"thread_id": thread_id, "name": request.name}}
    await manager.broadcast_to_room(thread_id, update_message)

    return

@router.post("/threads/{thread_id}/posts", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def create_post_in_thread_route(
    thread_id: int,
    post_data: PostCreateRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Creates a new post (a top-level message or a reply to another post) in a thread.
    The user must be a member of the thread and have POST_CREATE permission.
    """
    new_post = await create_post(
        sender_id=current_user.id,
        thread_id=thread_id,
        content=post_data.content,
        parent_id=post_data.parent_id
    )
    if new_post is None:
        # Can be due to lack of permissions, not being a member, or invalid parent_id
        raise HTTPException(status_code=403, detail="Failed to create post. You may not be a member or lack permissions.")
    
    return new_post

@router.delete("/posts/{post_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_a_post_route(
    post_id: int,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Deletes a post. Requires ownership of the post (POST_DELETE_OWN) or moderation
    privileges in the thread or globally (POST_DELETE_ANY).
    """
    success = await delete_post(deleting_user_id=current_user.id, post_id=post_id)
    if not success:
        # Service function handles all permission logic and returns False on failure.
        raise HTTPException(status_code=403, detail="Post not found or you do not have permission to delete it.")
    
    # On success, FastAPI returns a 204 response with no body.
    return

# ==============================================================================
# --- Watch Party Routes ---
# ==============================================================================

@router.get("/watch-parties/for-instance/{instance_id}", response_model=List[WatchPartyResponse])
async def list_watch_parties_for_instance_route(instance_id: int, current_user: UserProfile = Depends(get_current_active_user)):
    """
    Retrieves a list of all active watch parties for a specific file instance.
    """
    # A watch party is tied to the content, not the specific instance.
    # First, get the content_id for the given instance_id.
    instance_details = await get_instance_details(instance_id, user_id=current_user.id)
    if not instance_details:
        # If the user can't see the instance, they can't see its watch parties.
        # Return an empty list, as this is a GET request.
        return []
    content_id = instance_details['content_id']
    return await list_watch_parties_for_file(file_id=content_id)

@router.post("/watch-parties/for-instance/{instance_id}", response_model=WatchPartyResponse, status_code=status.HTTP_201_CREATED)
async def create_watch_party_for_instance_route(
    instance_id: int,
    request: CreateWatchPartyRequest,
    current_user: UserProfile = Depends(get_current_active_user)
):
    """
    Creates a new watch party chat thread for a specific file instance and broadcasts
    an update to all clients viewing that instance's page.
    """
    try:
        # A watch party is tied to the content, not the specific instance.
        # First, get the content_id for the given instance_id.
        instance_details = await get_instance_details(instance_id, user_id=current_user.id)
        if not instance_details:
            raise HTTPException(status_code=404, detail="File instance not found or access denied.")
        content_id = instance_details['content_id']

        # 1. Create the party in the database
        new_party_thread = await create_watch_party_thread(
            creator_id=current_user.id,
            file_id=content_id, # Pass the correct content_id to the service
            name=request.name
        )
        if not new_party_thread:
            raise HTTPException(status_code=403, detail="Failed to create watch party. You may lack the required permissions.")
        
        # --- WEBSOCKET STATE UPDATE ---
        # Now that the party is created, join the creator's websockets to the room
        # and notify them that they are in the party.
        thread_id = new_party_thread['id']
        user_id = current_user.id
        
        # Find all active websockets for the creator
        user_websockets = manager.active_connections.get(user_id, [])
        for ws in user_websockets:
            await manager.join_room(ws, thread_id)

        # Send a success message to all of the creator's connections so their UI updates.
        join_success_message = {
            "type": "joined_watch_party_success",
            "payload": {
                "thread_id": thread_id,
                "file_id": instance_id, # The instance_id from the route
                "is_owner": True
            }
        }
        await manager.send_personal_message_to_user(user_id, join_success_message)

        # --- BROADCAST UPDATE ---
        # Fetch the complete, updated list of parties for this content.
        all_parties_for_content = await list_watch_parties_for_file(file_id=content_id)

        observation_room = f"instance-viewers:{instance_id}"
        # Send the full list directly to clients, avoiding a refetch.
        update_message = {
            "type": "party_list_updated",
            "payload": {
                "fileId": instance_id,
                "parties": jsonable_encoder(all_parties_for_content)
            }
        }
        await manager.broadcast_to_room(observation_room, update_message)

        # 3. Return the response to the creator (this is unchanged)
        # We need to manually construct the response because create_watch_party_thread returns a dict
        # and the initial member_count is 1.
        return WatchPartyResponse(
            id=new_party_thread['id'],
            name=new_party_thread['name'],
            invite_code=new_party_thread['invite_code'],
            created_at=new_party_thread['created_at'],
            member_count=1  # The creator is the first member
        )
    except Exception as e:
        logger.error(f"Error creating watch party for instance {instance_id} by user {current_user.id}: {e}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail="An internal error occurred.")


@router.post("/watch-parties/join/{invite_code}", response_model=JoinWatchPartyResponse)
async def join_watch_party_route(invite_code: str, current_user: UserProfile = Depends(get_current_active_user)):
    """
    Allows a user to join an existing watch party using an invite code.
    If the user is in another party, they will be moved.
    """
    join_result = await join_watch_party_thread(user_id=current_user.id, invite_code=invite_code)
    if not join_result:
        raise HTTPException(status_code=404, detail="Invalid or expired invite code.")
    
    # The HTTP endpoint doesn't handle websocket notifications for the old party
    # that the user might have left. This is a limitation of using HTTP for this
    # action. The primary path is via WebSocket, which handles all notifications.
    # This endpoint ensures the DB state is correct.
    thread_id = join_result.get("thread_id")
    if not thread_id:
        raise HTTPException(status_code=500, detail="Failed to join party.")

    return JoinWatchPartyResponse(thread_id=thread_id)
