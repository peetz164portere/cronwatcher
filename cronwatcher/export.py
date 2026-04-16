"""Export cron job history to CSV or JSON formats."""
import csv
import json
import io
from typing import List, Dict, Any, Literal


def rows_to_dicts(rows) -> List[Dict[str, Any]]:
    """Convert sqlite3 Row objects to plain dicts."""
    return [dict(row) for row in rows]


def export_csv(rows) -> str:
    """Serialize history rows to CSV string."""
    data = rows_to_dicts(rows)
    if not data:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def export_json(rows, indent: int = 2) -> str:
    """Serialize history rows to JSON string."""
    data = rows_to_dicts(rows)
    return json.dumps(data, indent=indent, default=str)


def export_history(
    rows,
    fmt: Literal["csv", "json"] = "json",
    indent: int = 2,
) -> str:
    """Export history in the requested format.

    Args:
        rows: Iterable of sqlite3 Row or dict-like objects.
        fmt: Output format, either 'csv' or 'json'.
        indent: JSON indent level (ignored for CSV).

    Returns:
        Formatted string.
    """
    if fmt == "csv":
        return export_csv(rows)
    elif fmt == "json":
        return export_json(rows, indent=indent)
    else:
        raise ValueError(f"Unsupported export format: {fmt!r}")
