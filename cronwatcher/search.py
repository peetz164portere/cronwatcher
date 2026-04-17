"""Search and filter cron job history."""
from datetime import datetime
from typing import Optional


def search_history(conn, job_name: Optional[str] = None, status: Optional[str] = None,
                   since: Optional[datetime] = None, until: Optional[datetime] = None,
                   limit: int = 100):
    """Search history with optional filters."""
    query = "SELECT id, job_name, started_at, finished_at, exit_code, output FROM runs WHERE 1=1"
    params = []

    if job_name:
        query += " AND job_name LIKE ?"
        params.append(f"%{job_name}%")

    if status == "success":
        query += " AND exit_code = 0"
    elif status == "failure":
        query += " AND exit_code != 0 AND exit_code IS NOT NULL"
    elif status == "running":
        query += " AND finished_at IS NULL"

    if since:
        query += " AND started_at >= ?"
        params.append(since.isoformat())

    if until:
        query += " AND started_at <= ?"
        params.append(until.isoformat())

    query += " ORDER BY started_at DESC LIMIT ?"
    params.append(limit)

    cur = conn.execute(query, params)
    return cur.fetchall()


def count_by_status(conn, job_name: Optional[str] = None):
    """Return dict with counts per status for a job or all jobs."""
    base = "FROM runs"
    params = []
    if job_name:
        base += " WHERE job_name = ?"
        params.append(job_name)

    def _count(condition):
        cur = conn.execute(f"SELECT COUNT(*) {base}" + (" AND " if job_name else " WHERE ") + condition, params)
        return cur.fetchone()[0]

    total_cur = conn.execute(f"SELECT COUNT(*) {base}", params)
    total = total_cur.fetchone()[0]

    success_params = params + []
    fail_params = params + []
    running_params = params + []

    join = " AND " if job_name else " WHERE "
    s = conn.execute(f"SELECT COUNT(*) {base}{join}exit_code = 0", success_params).fetchone()[0]
    f = conn.execute(f"SELECT COUNT(*) {base}{join}exit_code != 0 AND exit_code IS NOT NULL", fail_params).fetchone()[0]
    r = conn.execute(f"SELECT COUNT(*) {base}{join}finished_at IS NULL", running_params).fetchone()[0]

    return {"total": total, "success": s, "failure": f, "running": r}
