"""Formatting utilities for displaying cron job history in the CLI."""

from datetime import datetime
from typing import List, Optional


STATUS_SYMBOLS = {
    "success": "✓",
    "failure": "✗",
    "running": "⟳",
    None: "?",
}

STATUS_COLORS = {
    "success": "\033[32m",  # green
    "failure": "\033[31m",  # red
    "running": "\033[33m",  # yellow
    None: "\033[0m",
}

RESET = "\033[0m"


def _colorize(text: str, color_code: str, use_color: bool = True) -> str:
    if not use_color:
        return text
    return f"{color_code}{text}{RESET}"


def format_duration(seconds: Optional[float]) -> str:
    """Return a human-readable duration string."""
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m"


def format_timestamp(ts: Optional[str]) -> str:
    """Format an ISO timestamp into something readable."""
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return ts


def format_row(record: dict, use_color: bool = True) -> str:
    """Format a single history record as a table row string."""
    status = record.get("status")
    symbol = STATUS_SYMBOLS.get(status, "?")
    color = STATUS_COLORS.get(status, RESET)

    job_name = record.get("job_name", "unknown")[:24].ljust(24)
    started = format_timestamp(record.get("started_at"))
    duration = format_duration(record.get("duration"))
    status_str = (status or "unknown").ljust(8)
    exit_code = str(record.get("exit_code") if record.get("exit_code") is not None else "—")

    row = f"{symbol} {job_name}  {started}  {status_str}  dur={duration}  exit={exit_code}"
    return _colorize(row, color, use_color)


def format_history_table(records: List[dict], use_color: bool = True) -> str:
    """Render a list of history records as a formatted table."""
    if not records:
        return "No history found."

    header = "  {:<24}  {:<19}  {:<8}  {:<12}  {}".format(
        "JOB", "STARTED", "STATUS", "DURATION", "EXIT"
    )
    separator = "-" * len(header)
    rows = [format_row(r, use_color) for r in records]
    return "\n".join([header, separator] + rows)
