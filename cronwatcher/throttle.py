"""Per-job alert throttling with cooldown windows."""

import time
from typing import Optional

# In-memory store: job_name -> last alert timestamp
_cooldowns: dict[str, float] = {}

DEFAULT_COOLDOWN = 300  # 5 minutes


def set_cooldown(job_name: str, ts: Optional[float] = None) -> None:
    """Record that an alert was sent for job_name at ts (default: now)."""
    _cooldowns[job_name] = ts if ts is not None else time.time()


def get_last_alert(job_name: str) -> Optional[float]:
    """Return timestamp of last alert for job, or None."""
    return _cooldowns.get(job_name)


def is_throttled(job_name: str, cooldown: int = DEFAULT_COOLDOWN) -> bool:
    """Return True if an alert was sent within the cooldown window."""
    last = _cooldowns.get(job_name)
    if last is None:
        return False
    return (time.time() - last) < cooldown


def clear(job_name: Optional[str] = None) -> None:
    """Clear throttle state for a job or all jobs."""
    if job_name is None:
        _cooldowns.clear()
    else:
        _cooldowns.pop(job_name, None)


def time_until_unthrottled(job_name: str, cooldown: int = DEFAULT_COOLDOWN) -> Optional[int]:
    """Return seconds remaining in cooldown, or None if not throttled."""
    last = _cooldowns.get(job_name)
    if last is None:
        return None
    remaining = cooldown - (time.time() - last)
    return max(0, int(remaining)) if remaining > 0 else None
