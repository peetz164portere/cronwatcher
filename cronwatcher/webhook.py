import json
import urllib.request
import urllib.error
from datetime import datetime


def build_payload(job_name: str, run_id: int, exit_code: int, started_at: str, finished_at: str, duration_seconds: float) -> dict:
    """Build a webhook payload dict for a failed cron job."""
    return {
        "event": "cron_failure",
        "job_name": job_name,
        "run_id": run_id,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def send_webhook(url: str, payload: dict, timeout: int = 10) -> bool:
    """
    POST payload as JSON to the given URL.
    Returns True on success (2xx), False otherwise.
    """
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "cronwatcher/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        print(f"[cronwatcher] Webhook HTTP error: {e.code} {e.reason}")
        return False
    except urllib.error.URLError as e:
        print(f"[cronwatcher] Webhook URL error: {e.reason}")
        return False
    except Exception as e:
        print(f"[cronwatcher] Webhook unexpected error: {e}")
        return False


def notify_failure(url: str, job_name: str, run_id: int, exit_code: int, started_at: str, finished_at: str, duration_seconds: float) -> bool:
    """Convenience wrapper: build payload and send webhook."""
    if not url:
        return False
    payload = build_payload(job_name, run_id, exit_code, started_at, finished_at, duration_seconds)
    return send_webhook(url, payload)
