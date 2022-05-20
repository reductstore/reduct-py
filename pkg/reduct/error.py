"""Reduct Errors"""
from pydantic import BaseModel


class ServerError(BaseModel):
    """sent from the server"""

    detail: str


class ReductError(Exception):
    """General exception for all HTTP errors"""

    def __init__(self, code: int, message: str):
        self._code = code
        self._detail = message
        self.message = ServerError.parse_raw(message) if message else ""
        super().__init__(self.message)

    @property
    def status_code(self):
        """Return HTTP status code"""
        return self._code
