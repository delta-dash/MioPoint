from enum import Enum


class Permission(Enum):
    """Defines all available permissions as constants."""
    FILE_ADD = 'file:add'
    FILE_VIEW = 'file:view'
    FILE_EDIT = 'file:edit'
    FILE_DELETE = 'file:delete'
    FILE_SEARCH = 'file:search'
    FILE_REVERSE_SEARCH = 'file:reverse_search'
    TAG_EDIT = 'tag:edit'
    USER_MANAGE_ROLES = 'user:manage_roles'
    PERMISSION_GRANT = 'permission:grant'
    ROLE_MANAGE_HIERARCHY = 'role:manage_hierarchy'
    REACTION_ADD = 'reaction:add'
    REACTION_MANAGE = 'reaction:manage'
    CHAT_CREATE = 'chat:conversation:create'
    CHAT_SEND_MESSAGE = 'chat:message:send'
    CHAT_CONVERSATION_VIEW_PUBLIC = 'chat:conversation:view_public'
    CHAT_VIEW_OWN = 'chat:conversation:own'
    USER_VIEW_PUBLIC_PROFILE = 'user:view_public_profile'
    USER_VIEW_ANY = 'user:view_any' 
    USER_BAN = 'user:ban'
    
    THREAD_PARTICIPATE = "thread:participate"
    THREAD_CREATE_DM = "thread:create:dm"
    THREAD_CREATE_GROUP = "thread:create:group"
    THREAD_CREATE_PUBLIC = "thread:create:public"
    THREAD_CREATE_WATCH_PARTY = "thread:create:watch_party"
    THREAD_EDIT = "thread:edit"
    THREAD_DELETE = "thread:delete"
    THREAD_MANAGE_MEMBERS = "thread:manage_members"

    # --- Post (Message/Comment) Management Permissions ---
    POST_CREATE = "post:create"
    POST_EDIT_OWN = "post:edit:own"
    POST_DELETE_OWN = "post:delete:own"

    # --- High-Level Admin Permissions ---
    ADMIN_CONFIG_VIEW = 'admin:config:view'
    ADMIN_CONFIG_EDIT = 'admin:config:edit'
    THREAD_VIEW_ANY = "thread:view:any"
    POST_EDIT_ANY = "post:edit:any"
    POST_DELETE_ANY = "post:delete:any"
    def __str__(self) -> str:
        return self.value