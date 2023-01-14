"""Reduct Errors"""


class ReductError(Exception):
    """General exception for all HTTP errors"""

    def __init__(self, code: int, message: str):
        self._code = code
        self._message = message
        super().__init__(f"Status {self._code}: {self.message}")

    @property
    def status_code(self):
        """Return HTTP status code"""
        return self._code

    @property
    def message(self):
        """Return error message"""
        return self._message
