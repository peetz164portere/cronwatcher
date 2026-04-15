"""High-level notification orchestration combining alerts + webhook."""

from __future__ import annotations

from typing import Any

from cronwatcher.alerts import init_alert_log, record_alert, should_suppress_alert
from cronwatcher.config import load_config, should_alert
from cronwatcher.webhook import notify_failure


def maybe_notify(
    db_path: str,
    job_name: str,
    run_id: int,
    exit_code: int,
    duration: float,
    output: str = "",
    config_path: str | None = None,
) -> dict[str, Any]:
    """Evaluate alert rules and send a webhook notification if warranted.

    Returns a dict describing what action was taken.
    """
    init_alert_log(db_path)

    if exit_code == 0:
        return {"action": "skipped", "reason": "exit_code_ok"}

    config = load_config(config_path) if config_path else {}

    if not should_alert(config, exit_code):
        return {"action": "skipped", "reason": "alert_rule_suppressed"}

    cooldown = int(config.get("alert_cooldown_seconds", 3600))
    if should_suppress_alert(db_path, job_name, cooldown=cooldown):
        return {"action": "skipped", "reason": "cooldown_active"}

    webhook_url: str | None = config.get("webhook_url")
    if not webhook_url:
        return {"action": "skipped", "reason": "no_webhook_configured"}

    notify_failure(
        webhook_url=webhook_url,
        job_name=job_name,
        exit_code=exit_code,
        duration=duration,
        output=output,
    )

    record_alert(db_path, job_name, run_id)
    return {"action": "notified", "webhook_url": webhook_url}
