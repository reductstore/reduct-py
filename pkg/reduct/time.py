"""Helper functions for time-related operations."""

from __future__ import annotations

from datetime import datetime
from typing import TypeAlias

TIME_PRECISION = 1_000_000

# Python 3.10+ Type Alias
TimestampLike: TypeAlias = int | datetime | float | str
"""Flexible timestamp input type accepting multiple formats"""


def unix_timestamp_from_any(timestamp: TimestampLike) -> int:
    """Convert timestamp to UNIX timestamp in microseconds
    Args:
        timestamp (int | datetime | float | str): int (UNIX timestamp in microseconds),
            datetime, float (UNIX timestamp in seconds), str (ISO 8601 string)
    Returns:
        int: UNIX timestamp in microseconds
    """
    if isinstance(timestamp, datetime):
        return int(timestamp.timestamp() * TIME_PRECISION)
    if isinstance(timestamp, str):
        return int(
            datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
            * TIME_PRECISION
        )
    if isinstance(timestamp, float):
        return int(timestamp * TIME_PRECISION)
    return int(timestamp)


def unix_timestamp_to_datetime(timestamp: int) -> datetime:
    """Convert UNIX timestamp to datetime
    Args:
        timestamp (int): UNIX timestamp in microseconds
    Returns:
        datetime: timestamp as datetime
    """
    return datetime.fromtimestamp(timestamp / TIME_PRECISION)


def unix_timestamp_to_iso(timestamp: int) -> str:
    """Convert UNIX timestamp to ISO 8601 string
    Args:
        timestamp (int): UNIX timestamp in microseconds
    Returns:
        str: timestamp as ISO 8601 string
    """
    return unix_timestamp_to_datetime(timestamp).isoformat()


def unix_timestamp_to_py_timestamp(timestamp: int) -> float:
    """Convert UNIX timestamp to Python timestamp
    Args:
        timestamp (int): UNIX timestamp in microseconds
    Returns:
        float: timestamp as Python timestamp
    """
    return timestamp / TIME_PRECISION
