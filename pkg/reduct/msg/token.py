"""Message types for the Token API"""

from datetime import datetime

from pydantic import BaseModel, Field


class Permissions(BaseModel):
    """Token permission"""

    full_access: bool = False
    """full access to manage buckets and tokens"""

    read: list[str] = Field(default_factory=list)
    """list of buckets with read access"""

    write: list[str] = Field(default_factory=list)
    """list of buckets with write access"""


class Token(BaseModel):
    """Token for authentication"""

    name: str
    """name of token"""

    created_at: datetime
    """creation time of token"""

    is_provisioned: bool = False
    """token is provisioned and can't be deleted or changed"""

    expires_at: datetime | None = None
    """absolute expiration time"""

    ttl: int | None = None
    """inactivity timeout in seconds"""

    last_access: datetime | None = None
    """latest token usage timestamp"""

    is_expired: bool = False
    """token is currently expired and unusable"""

    ip_allowlist: list[str] = Field(default_factory=list)
    """list of allowed client IPs or CIDRs"""


class FullTokenInfo(Token):
    """Full information about token with permissions"""

    permissions: Permissions
    """permissions of token"""


class TokenList(BaseModel):
    """List of tokens"""

    tokens: list[Token]
    """list of tokens"""


class TokenCreateRequest(BaseModel):
    """Request payload for creating a token"""

    permissions: Permissions
    """permissions of token"""

    expires_at: datetime | None = None
    """absolute expiration time"""

    ttl: int | None = None
    """inactivity timeout in seconds"""

    ip_allowlist: list[str] = Field(default_factory=list)
    """list of allowed client IPs or CIDRs"""


class TokenCreateResponse(BaseModel):
    """Response from creating a token"""

    value: str
    """token for authentication"""

    created_at: datetime | None = None
    """timestamp when a token value was created or rotated"""
