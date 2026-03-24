"""
Persistent cron schedules (SQLite). Env: SCRIPTOPS_SCHEDULES_DB (default ./data/schedules.db).
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.utils.logger import setup_logger

logger = setup_logger(__name__)

_lock = threading.RLock()

_DEFAULT_SEED: List[Dict[str, Any]] = [
    {
        "schedule_id": "sc_001",
        "script_id": "gen_sales",
        "script_name": "generate_sales_report.py",
        "cron_expr": "0 8 * * 1-5",
        "human_readable": "Weekdays at 08:00",
        "server": "prod-01",
        "enabled": 1,
        "notify_on": "failure",
        "params": {},
        "description": "Daily sales report, Mon–Fri",
        "last_run": "2024-06-10T08:00:01Z",
        "next_run": "2024-06-11T08:00:00Z",
        "last_status": "success",
        "created_by": "system",
        "created_at": "2024-01-10T09:00:00Z",
    },
    {
        "schedule_id": "sc_002",
        "script_id": "db_backup",
        "script_name": "db_backup.sh",
        "cron_expr": "0 2 * * *",
        "human_readable": "Daily at 02:00 AM",
        "server": "db-01",
        "enabled": 1,
        "notify_on": "always",
        "params": {},
        "description": "Nightly PostgreSQL backup to S3",
        "last_run": "2024-06-11T02:00:02Z",
        "next_run": "2024-06-12T02:00:00Z",
        "last_status": "success",
        "created_by": "system",
        "created_at": "2024-01-10T09:00:00Z",
    },
    {
        "schedule_id": "sc_003",
        "script_id": "cleanup",
        "script_name": "cleanup_logs.sh",
        "cron_expr": "0 0 * * 0",
        "human_readable": "Sundays at midnight",
        "server": "prod-01",
        "enabled": 1,
        "notify_on": "failure",
        "params": {},
        "description": "Weekly log rotation",
        "last_run": "2024-06-09T00:00:01Z",
        "next_run": "2024-06-16T00:00:00Z",
        "last_status": "success",
        "created_by": "system",
        "created_at": "2024-01-15T09:00:00Z",
    },
    {
        "schedule_id": "sc_004",
        "script_id": "sched_sales",
        "script_name": "scheduled_sales_push.py",
        "cron_expr": "30 23 * * *",
        "human_readable": "Daily at 23:30",
        "server": "prod-01",
        "enabled": 1,
        "notify_on": "always",
        "params": {},
        "description": "Nightly data warehouse push",
        "last_run": "2024-06-10T23:30:01Z",
        "next_run": "2024-06-11T23:30:00Z",
        "last_status": "success",
        "created_by": "system",
        "created_at": "2024-02-01T09:00:00Z",
    },
    {
        "schedule_id": "sc_005",
        "script_id": "health",
        "script_name": "health_check.sh",
        "cron_expr": "*/5 * * * *",
        "human_readable": "Every 5 minutes",
        "server": "prod-01",
        "enabled": 1,
        "notify_on": "failure",
        "params": {},
        "description": "Endpoint health monitoring",
        "last_run": "2024-06-11T11:45:02Z",
        "next_run": "2024-06-11T11:50:00Z",
        "last_status": "success",
        "created_by": "system",
        "created_at": "2024-01-10T09:00:00Z",
    },
    {
        "schedule_id": "sc_006",
        "script_id": "weekly_digest",
        "script_name": "weekly_digest.py",
        "cron_expr": "0 7 * * 1",
        "human_readable": "Mondays at 07:00",
        "server": "prod-01",
        "enabled": 1,
        "notify_on": "failure",
        "params": {},
        "description": "Monday morning management digest",
        "last_run": "2024-06-10T07:00:01Z",
        "next_run": "2024-06-17T07:00:00Z",
        "last_status": "success",
        "created_by": "system",
        "created_at": "2024-01-10T09:00:00Z",
    },
]


def _db_path() -> Path:
    env = os.environ.get("SCRIPTOPS_SCHEDULES_DB")
    if env:
        return Path(env).resolve()
    # Default: scriptops-api/data/schedules.db (this file is app/services/schedule_store.py)
    base = Path(__file__).resolve().parent.parent.parent
    d = base / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d / "schedules.db"


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schedules (
                    schedule_id TEXT PRIMARY KEY,
                    script_id TEXT NOT NULL,
                    script_name TEXT,
                    cron_expr TEXT NOT NULL,
                    human_readable TEXT,
                    server TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    notify_on TEXT NOT NULL DEFAULT 'failure',
                    params_json TEXT NOT NULL DEFAULT '{}',
                    description TEXT,
                    last_run TEXT,
                    next_run TEXT,
                    last_status TEXT,
                    created_by TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
            cur = conn.execute("SELECT COUNT(*) AS c FROM schedules")
            row = cur.fetchone()
            if row and row["c"] == 0:
                for s in _DEFAULT_SEED:
                    conn.execute(
                        """
                        INSERT INTO schedules (
                            schedule_id, script_id, script_name, cron_expr, human_readable,
                            server, enabled, notify_on, params_json, description,
                            last_run, next_run, last_status, created_by, created_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            s["schedule_id"],
                            s["script_id"],
                            s["script_name"],
                            s["cron_expr"],
                            s["human_readable"],
                            s["server"],
                            int(s.get("enabled", 1)),
                            s.get("notify_on", "failure"),
                            json.dumps(s.get("params") or {}),
                            s.get("description"),
                            s.get("last_run"),
                            s.get("next_run"),
                            s.get("last_status"),
                            s.get("created_by", "system"),
                            s.get("created_at", datetime.now(timezone.utc).isoformat()),
                        ),
                    )
                conn.commit()
                logger.info("Seeded %d default schedule(s)", len(_DEFAULT_SEED))
        finally:
            conn.close()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    try:
        d["params"] = json.loads(d.pop("params_json") or "{}")
    except json.JSONDecodeError:
        d["params"] = {}
        d.pop("params_json", None)
    d["enabled"] = bool(d.get("enabled", 0))
    return d


def all_schedules() -> Dict[str, Dict[str, Any]]:
    init_db()
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute("SELECT * FROM schedules ORDER BY schedule_id")
            out: Dict[str, Dict[str, Any]] = {}
            for row in cur.fetchall():
                sd = _row_to_dict(row)
                out[sd["schedule_id"]] = sd
            return out
        finally:
            conn.close()


def get_schedule(schedule_id: str) -> Optional[Dict[str, Any]]:
    alls = all_schedules()
    return alls.get(schedule_id)


def upsert_schedule(row: Dict[str, Any]) -> Dict[str, Any]:
    init_db()
    params_json = json.dumps(row.get("params") or {})
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO schedules (
                    schedule_id, script_id, script_name, cron_expr, human_readable,
                    server, enabled, notify_on, params_json, description,
                    last_run, next_run, last_status, created_by, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                    script_id=excluded.script_id,
                    script_name=excluded.script_name,
                    cron_expr=excluded.cron_expr,
                    human_readable=excluded.human_readable,
                    server=excluded.server,
                    enabled=excluded.enabled,
                    notify_on=excluded.notify_on,
                    params_json=excluded.params_json,
                    description=excluded.description,
                    last_run=excluded.last_run,
                    next_run=excluded.next_run,
                    last_status=excluded.last_status
                """,
                (
                    row["schedule_id"],
                    row["script_id"],
                    row.get("script_name"),
                    row["cron_expr"],
                    row.get("human_readable"),
                    row["server"],
                    1 if row.get("enabled", True) else 0,
                    row.get("notify_on", "failure"),
                    params_json,
                    row.get("description"),
                    row.get("last_run"),
                    row.get("next_run"),
                    row.get("last_status"),
                    row.get("created_by", "system"),
                    row.get("created_at", datetime.now(timezone.utc).isoformat()),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    return get_schedule(row["schedule_id"]) or row


def delete_schedule(schedule_id: str) -> bool:
    init_db()
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute("DELETE FROM schedules WHERE schedule_id = ?", (schedule_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
