"""Reduct Errors"""
from pydantic import BaseModel, ValidationError


class ServerError(BaseModel):
    """sent from the server"""

    detail: str


class ReductError(Exception):
    """General exception for all HTTP errors"""

    def __init__(self, code: int, message: str):
        self._code = code

        try:
            self.message = ServerError.parse_raw(message).detail if message else ""
        except ValidationError:
            self.message = (
                "Could not parse error response from server, is it definitely Reduct?"
            )

        super().__init__(f"Status {self._code}: {self.message}")

    @property
    def status_code(self):
        """Return HTTP status code"""
        return self._code
