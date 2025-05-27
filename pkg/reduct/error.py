"""Reduct Errors"""


class ReductError(Exception):
    """General exception for all HTTP errors"""

    def __init__(self, code: int, message: str):
        self._code = code
        self._message = message
        super().__init__(f"Status {self._code}: {self.message}")

    @staticmethod
    def from_header(header: str) -> "ReductError":
        """Create ReductError from HTTP header
        with status code and message (batched write
        )"""
        status_code, message = header.split(",", 1)
        return ReductError(int(status_code), message)

    @property
    def status_code(self):
        """Return HTTP status code"""
        return self._code

    @property
    def message(self):
        """Return error message"""
        return self._message

    def __eq__(self, other: "ReductError"):
        return self._code == other._code and self._message == other._message
