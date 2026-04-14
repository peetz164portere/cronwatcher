import os
import json
from pathlib import Path

DEFAULT_DB_PATH = str(Path.home() / ".cronwatcher" / "history.db")
DEFAULT_CONFIG_PATH = str(Path.home() / ".cronwatcher" / "config.json")


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Load config from a JSON file. Falls back to defaults if the file
    doesn't exist or is malformed.
    """
    defaults = {
        "db_path": DEFAULT_DB_PATH,
        "webhook_url": "",
        "webhook_timeout": 10,
        "alert_on_exit_codes": [],  # empty = alert on any non-zero
    }

    path = Path(config_path)
    if not path.exists():
        return defaults

    try:
        with open(path, "r") as fh:
            data = json.load(fh)
        # Merge with defaults so missing keys are filled in
        return {**defaults, **data}
    except (json.JSONDecodeError, OSError) as e:
        print(f"[cronwatcher] Warning: could not read config ({e}), using defaults.")
        return defaults


def save_config(config: dict, config_path: str = DEFAULT_CONFIG_PATH) -> None:
    """Persist config dict to JSON file, creating parent dirs as needed."""
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        json.dump(config, fh, indent=2)


def should_alert(exit_code: int, alert_on_exit_codes: list) -> bool:
    """Return True if this exit code should trigger an alert."""
    if exit_code == 0:
        return False
    if not alert_on_exit_codes:
        return True  # alert on any non-zero
    return exit_code in alert_on_exit_codes
