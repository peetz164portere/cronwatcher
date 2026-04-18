"""Rate limiting for webhook/alert delivery."""

import time
from collections import deque
from typing import Deque, Dict

# job_name -> deque of timestamps (seconds)
_buckets: Dict[str, Deque[float]] = {}


def _get_bucket(job_name: str) -> Deque[float]:
    if job_name not in _buckets:
        _buckets[job_name] = deque()
    return _buckets[job_name]


def is_rate_limited(job_name: str, max_alerts: int, window_seconds: int) -> bool:
    """Return True if job has exceeded max_alerts within window_seconds."""
    if max_alerts <= 0 or window_seconds <= 0:
        return False
    now = time.time()
    bucket = _get_bucket(job_name)
    cutoff = now - window_seconds
    while bucket and bucket[0] < cutoff:
        bucket.popleft()
    return len(bucket) >= max_alerts


def record_alert_sent(job_name: str) -> None:
    """Record that an alert was sent for job_name right now."""
    _get_bucket(job_name).append(time.time())


def reset(job_name: str = None) -> None:
    """Clear rate limit state. Pass job_name to clear one job, or None for all."""
    global _buckets
    if job_name is None:
        _buckets.clear()
    else:
        _buckets.pop(job_name, None)


def get_alert_count(job_name: str, window_seconds: int) -> int:
    """Return number of alerts sent for job within the window."""
    now = time.time()
    bucket = _get_bucket(job_name)
    cutoff = now - window_seconds
    return sum(1 for t in bucket if t >= cutoff)
