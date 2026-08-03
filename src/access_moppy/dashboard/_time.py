"""Time formatting helpers for dashboard presentation."""

from __future__ import annotations

from datetime import datetime, timezone


def format_local_time(value: str | None) -> str:
    """Format a tracker timestamp in the user's local timezone."""
    if not value:
        return "—"
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return value
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
