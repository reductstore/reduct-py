"""Message types for the Token API"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class Permissions(BaseModel):
    """Token permission"""

    full_access: bool
    """full access to manage buckets and tokens"""

    read: Optional[List[str]]
    """list of buckets with read access"""

    write: Optional[List[str]]
    """list of buckets with write access"""


class Token(BaseModel):
    """Token for authentication"""

    name: str
    """name of token"""

    created_at: datetime
    """creation time of token"""

    is_provisioned: bool = False
    """token is provisioned and can't be deleted or changed"""


class FullTokenInfo(Token):
    """Full information about token with permissions"""

    permissions: Permissions
    """permissions of token"""


class TokenList(BaseModel):
    """List of tokens"""

    tokens: List[Token]
    """list of tokens"""


class TokenCreateResponse(BaseModel):
    """Response from creating a token"""

    value: str
    """token for authentication"""
