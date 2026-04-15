"""Retry logic for webhook notifications with exponential backoff."""

import time
import logging
from typing import Callable, Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_MAX_DELAY = 30.0  # seconds


def with_retry(
    fn: Callable[[], Any],
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: float = DEFAULT_MAX_DELAY,
    exceptions: tuple = (Exception,),
    label: Optional[str] = None,
) -> Any:
    """Call fn with exponential backoff on failure.

    Returns the result of fn on success.
    Raises the last exception if all retries are exhausted.
    """
    label = label or getattr(fn, "__name__", "operation")
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 2):  # +1 for the initial attempt
        try:
            result = fn()
            if attempt > 1:
                logger.info("[retry] %s succeeded on attempt %d", label, attempt)
            return result
        except exceptions as exc:  # type: ignore[misc]
            last_exc = exc
            if attempt > max_retries:
                logger.error(
                    "[retry] %s failed after %d attempt(s): %s",
                    label,
                    attempt,
                    exc,
                )
                break
            delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
            logger.warning(
                "[retry] %s attempt %d failed (%s). Retrying in %.1fs…",
                label,
                attempt,
                exc,
                delay,
            )
            time.sleep(delay)

    raise last_exc  # type: ignore[misc]


def get_retry_config(config: dict) -> dict:
    """Extract retry settings from a config dict, applying defaults."""
    retry_cfg = config.get("retry", {})
    return {
        "max_retries": int(retry_cfg.get("max_retries", DEFAULT_MAX_RETRIES)),
        "base_delay": float(retry_cfg.get("base_delay", DEFAULT_BASE_DELAY)),
        "backoff_factor": float(retry_cfg.get("backoff_factor", DEFAULT_BACKOFF_FACTOR)),
        "max_delay": float(retry_cfg.get("max_delay", DEFAULT_MAX_DELAY)),
    }
