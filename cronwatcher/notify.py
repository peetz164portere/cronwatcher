"""Orchestrates failure notifications, suppression checks, and retry logic."""

import logging
from typing import Optional

from cronwatcher.alerts import should_suppress_alert, record_alert, init_alert_log
from cronwatcher.webhook import notify_failure
from cronwatcher.config import load_config, should_alert
from cronwatcher.retry import with_retry, get_retry_config

logger = logging.getLogger(__name__)


def maybe_notify(
    job_name: str,
    exit_code: int,
    duration: float,
    output: Optional[str] = None,
    db_path: Optional[str] = None,
) -> bool:
    """Send a webhook alert for a failed job if conditions are met.

    Returns True if a notification was sent, False otherwise.
    """
    config = load_config()

    if not should_alert(config, exit_code):
        logger.debug("[notify] alert suppressed by config for job=%s exit_code=%d", job_name, exit_code)
        return False

    webhook_url: Optional[str] = config.get("webhook_url")
    if not webhook_url:
        logger.warning("[notify] no webhook_url configured, skipping notification")
        return False

    init_alert_log(db_path=db_path)

    if should_suppress_alert(job_name, config, db_path=db_path):
        logger.info("[notify] alert suppressed by cooldown for job=%s", job_name)
        return False

    retry_cfg = get_retry_config(config)

    try:
        with_retry(
            fn=lambda: notify_failure(
                webhook_url=webhook_url,
                job_name=job_name,
                exit_code=exit_code,
                duration=duration,
                output=output,
            ),
            label=f"webhook:{job_name}",
            exceptions=(Exception,),
            **retry_cfg,
        )
    except Exception as exc:
        logger.error("[notify] failed to send webhook for job=%s: %s", job_name, exc)
        return False

    record_alert(job_name, db_path=db_path)
    logger.info("[notify] alert sent for job=%s", job_name)
    return True
