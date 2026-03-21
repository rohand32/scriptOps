"""
ScriptOps — Script Execution Service

Handles:
  - SSH-based remote script execution (paramiko)
  - Job lifecycle management (pending → running → success/failed)
  - Real-time output streaming via async generator (consumed by SSE route)
  - Execution audit logging
"""

from __future__ import annotations

import asyncio
import json
import re
import shlex
import time
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from app.models.schemas import JobStatus, TriggerType, ScriptCategory
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


# ── In-memory job store (replace with Redis / DB in production) ───────────────
_JOBS: Dict[str, dict] = {}

# ── Script registry (maps script_id → metadata) ──────────────────────────────
SCRIPT_REGISTRY: Dict[str, dict] = {
    "gen_sales": {
        "name": "generate_sales_report.py",
        "category": ScriptCategory.report,
        "server": "prod-01",
        "path": "/opt/scripts/reports/generate_sales_report.py",
        "interpreter": "python3",
        "min_role": "manager",
        "timeout_sec": 300,
        "allowed_params": ["date_from", "date_to", "format", "region",
                           "include_returns", "group_by", "send_email", "recipients"],
    },
    "gen_inv": {
        "name": "sync_inventory.py",
        "category": ScriptCategory.report,
        "server": "prod-01",
        "path": "/opt/scripts/sync/sync_inventory.py",
        "interpreter": "python3",
        "min_role": "manager",
        "timeout_sec": 180,
        "allowed_params": ["dry_run", "batch_size", "skip_null_skus"],
    },
    "user_rpt": {
        "name": "user_activity_report.py",
        "category": ScriptCategory.report,
        "server": "prod-02",
        "path": "/opt/scripts/reports/user_activity_report.py",
        "interpreter": "python3",
        "min_role": "manager",
        "timeout_sec": 240,
        "allowed_params": ["date_from", "date_to", "format", "user_segment",
                           "include_anonymous"],
    },
    "fin_rpt": {
        "name": "finance_reconcile.py",
        "category": ScriptCategory.report,
        "server": "prod-01",
        "path": "/opt/scripts/reports/finance_reconcile.py",
        "interpreter": "python3",
        "min_role": "manager",
        "timeout_sec": 300,
        "allowed_params": ["date_from", "date_to", "format", "gateway",
                           "auto_flag_discrepancies"],
    },
    "weekly_digest": {
        "name": "weekly_digest.py",
        "category": ScriptCategory.report,
        "server": "prod-01",
        "path": "/opt/scripts/cron/weekly_digest.py",
        "interpreter": "python3",
        "min_role": "manager",
        "timeout_sec": 120,
        "allowed_params": ["week_offset", "format"],
    },
    "sched_sales": {
        "name": "scheduled_sales_push.py",
        "category": ScriptCategory.cron,
        "server": "prod-01",
        "path": "/opt/scripts/cron/scheduled_sales_push.py",
        "interpreter": "python3",
        "min_role": "admin",
        "timeout_sec": 600,
        "allowed_params": [],
    },
    "sched_clean": {
        "name": "scheduled_cleanup.sh",
        "category": ScriptCategory.cron,
        "server": "prod-02",
        "path": "/opt/scripts/cron/scheduled_cleanup.sh",
        "interpreter": "bash",
        "min_role": "admin",
        "timeout_sec": 60,
        "allowed_params": [],
    },
    "db_insert": {
        "name": "db_insert_records.py",
        "category": ScriptCategory.database,
        "server": "db-01",
        "path": "/opt/scripts/db/db_insert_records.py",
        "interpreter": "python3",
        "min_role": "admin",
        "timeout_sec": 600,
        "allowed_params": ["source_table", "source_file", "target_table",
                           "batch_size", "on_conflict", "dry_run"],
    },
    "db_update": {
        "name": "db_update_prices.py",
        "category": ScriptCategory.database,
        "server": "db-01",
        "path": "/opt/scripts/db/db_update_prices.py",
        "interpreter": "python3",
        "min_role": "admin",
        "timeout_sec": 300,
        "allowed_params": ["price_sheet", "effective_date", "dry_run"],
    },
    "db_export": {
        "name": "db_export_snapshot.py",
        "category": ScriptCategory.database,
        "server": "db-01",
        "path": "/opt/scripts/db/db_export_snapshot.py",
        "interpreter": "python3",
        "min_role": "manager",
        "timeout_sec": 300,
        "allowed_params": ["table", "limit", "format"],
    },
    "db_backup": {
        "name": "db_backup.sh",
        "category": ScriptCategory.shell,
        "server": "db-01",
        "path": "/opt/scripts/ops/db_backup.sh",
        "interpreter": "bash",
        "min_role": "operator",
        "timeout_sec": 600,
        "allowed_params": [],
    },
    "cleanup": {
        "name": "cleanup_logs.sh",
        "category": ScriptCategory.shell,
        "server": "prod-01",
        "path": "/opt/scripts/ops/cleanup_logs.sh",
        "interpreter": "bash",
        "min_role": "operator",
        "timeout_sec": 60,
        "allowed_params": [],
    },
    "health": {
        "name": "health_check.sh",
        "category": ScriptCategory.shell,
        "server": "prod-01",
        "path": "/opt/scripts/ops/health_check.sh",
        "interpreter": "bash",
        "min_role": "operator",
        "timeout_sec": 30,
        "allowed_params": [],
    },
}


# ─── Job helpers ──────────────────────────────────────────────────────────────

def _make_job_id() -> str:
    prefix = "J"
    suffix = str(uuid.uuid4().int)[:5].upper()
    return f"{prefix}{suffix}"


def create_job(
    script_id: str,
    params: Dict[str, Any],
    triggered_by: str,
    trigger: TriggerType,
    server_override: Optional[str] = None,
) -> dict:
    script = SCRIPT_REGISTRY.get(script_id)
    if not script:
        raise ValueError(f"Unknown script_id: {script_id}")

    job_id = _make_job_id()
    now    = datetime.now(timezone.utc)

    job = {
        "job_id":       job_id,
        "script_id":    script_id,
        "script_name":  script["name"],
        "server":       server_override or script["server"],
        "params":       params,
        "triggered_by": triggered_by,
        "trigger":      trigger.value,
        "status":       JobStatus.pending.value,
        "created_at":   now.isoformat(),
        "started_at":   None,
        "completed_at": None,
        "exit_code":    None,
        "output_lines": [],
        "error":        None,
        "warnings":     [],
        "rows_affected":None,
        "output_path":  None,
        "duration_ms":  None,
    }
    _JOBS[job_id] = job
    return job


def get_job(job_id: str) -> Optional[dict]:
    return _JOBS.get(job_id)


def list_jobs(
    script_id: Optional[str] = None,
    status: Optional[str] = None,
    triggered_by: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list, int]:
    jobs = list(_JOBS.values())
    if script_id:
        jobs = [j for j in jobs if j["script_id"] == script_id]
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    if triggered_by:
        jobs = [j for j in jobs if j["triggered_by"] == triggered_by]
    jobs.sort(key=lambda j: j["created_at"], reverse=True)
    total  = len(jobs)
    start  = (page - 1) * page_size
    return jobs[start : start + page_size], total


# ─── Build the shell command ──────────────────────────────────────────────────

def _build_command(script: dict, params: Dict[str, Any]) -> str:
    """
    Converts script metadata + params dict into a safe shell command.
    Params are passed as --key=value flags (validated against allowed_params).
    """
    allowed = set(script.get("allowed_params", []))
    flags   = []
    for k, v in params.items():
        if k not in allowed:
            logger.warning(f"Param '{k}' not in allowed list — skipping")
            continue
        # sanitise value: no shell metacharacters
        safe_v = shlex.quote(str(v))
        flags.append(f"--{k}={safe_v}")

    interpreter = script["interpreter"]
    path        = script["path"]
    return f"{interpreter} {path} {' '.join(flags)}".strip()


# ─── Core async executor ─────────────────────────────────────────────────────

async def execute_script(
    job_id: str,
    ssh_host: str = "localhost",   # resolved from server registry in production
    ssh_user: str = "deploy",
    ssh_key:  str = "/etc/scriptops/keys/deploy.pem",
) -> None:
    """
    Executes the script over SSH.
    This function runs in a background task; it updates _JOBS as it progresses.

    In production this uses asyncssh or paramiko in a thread-pool executor.
    Here we simulate realistic output for demonstration.
    """
    job    = _JOBS.get(job_id)
    if not job:
        logger.error(f"execute_script: job {job_id} not found")
        return

    script = SCRIPT_REGISTRY.get(job["script_id"])
    if not script:
        _fail_job(job_id, f"Script {job['script_id']} not in registry")
        return

    # Transition → running
    job["status"]     = JobStatus.running.value
    job["started_at"] = datetime.now(timezone.utc).isoformat()
    job["output_lines"].append(_log_line("sys", f"SSH connect → {job['server']}"))
    job["output_lines"].append(_log_line("sys", f"Authenticated as {ssh_user}"))
    job["output_lines"].append(
        _log_line("inf", f"Running: {_build_command(script, job['params'])}")
    )
    logger.info(f"[{job_id}] started: {script['name']} on {job['server']}")
    await asyncio.sleep(0.3)

    # ── Simulate script-specific output ──────────────────────────────────────
    sid = job["script_id"]
    try:
        if sid in ("gen_sales", "fin_rpt"):
            await _sim_report(job, rows=18492, output_name="sales_report")
        elif sid == "gen_inv":
            await _sim_inventory_sync(job)
        elif sid == "user_rpt":
            await _sim_report(job, rows=4210, output_name="user_activity")
        elif sid == "weekly_digest":
            await _sim_report(job, rows=None, output_name="weekly_digest")
        elif sid in ("sched_sales", "sched_clean", "cleanup", "health", "db_backup"):
            await _sim_shell(job)
        elif sid == "db_insert":
            await _sim_db_insert(job)
        elif sid == "db_update":
            await _sim_db_update(job)
        elif sid == "db_export":
            await _sim_db_export(job)
        else:
            await _sim_generic(job)

        # ── Success ──────────────────────────────────────────────────────────
        job["status"]       = JobStatus.success.value
        job["exit_code"]    = 0
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        dur = _calc_duration(job)
        job["duration_ms"]  = dur
        job["output_lines"].append(
            _log_line("ok", f"✓ {script['name']} completed. Duration: {dur}ms")
        )
        logger.info(f"[{job_id}] success in {dur}ms")

    except Exception as exc:
        _fail_job(job_id, str(exc))
        logger.error(f"[{job_id}] failed: {exc}", exc_info=True)


# ─── Simulation helpers ──────────────────────────────────────────────────────

def _log_line(level: str, text: str) -> dict:
    return {
        "ts":    datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "level": level,
        "text":  text,
    }

def _fail_job(job_id: str, reason: str):
    job = _JOBS.get(job_id, {})
    job["status"]       = JobStatus.failed.value
    job["exit_code"]    = 1
    job["error"]        = reason
    job["completed_at"] = datetime.now(timezone.utc).isoformat()
    job["duration_ms"]  = _calc_duration(job)
    job["output_lines"].append(_log_line("err", f"FAILED: {reason}"))

def _calc_duration(job: dict) -> int:
    try:
        s = datetime.fromisoformat(job["started_at"])
        e = datetime.now(timezone.utc)
        return int((e - s).total_seconds() * 1000)
    except Exception:
        return 0

async def _sim_report(job: dict, rows: Optional[int], output_name: str):
    params = job["params"]
    job["output_lines"].append(_log_line("inf", "Connecting to reporting database..."))
    await asyncio.sleep(0.4)
    job["output_lines"].append(_log_line("ok", "DB connection established (pool=5)"))
    await asyncio.sleep(0.3)
    date_from = params.get("date_from", "2024-06-01")
    date_to   = params.get("date_to",   "2024-06-30")
    job["output_lines"].append(
        _log_line("inf", f"Querying period {date_from} → {date_to}...")
    )
    await asyncio.sleep(0.8)
    if rows:
        job["output_lines"].append(_log_line("inf", f"Fetched {rows:,} rows"))
        job["output_lines"].append(_log_line("inf", "Computing aggregations..."))
        await asyncio.sleep(0.5)
    fmt = params.get("format", "csv")
    out = f"/tmp/{output_name}_{date_from}.{fmt}"
    job["output_lines"].append(_log_line("inf", f"Writing output → {out}"))
    await asyncio.sleep(0.3)
    job["output_path"] = out
    job["rows_affected"] = rows
    if params.get("send_email"):
        job["output_lines"].append(_log_line("inf", "Dispatching email to distribution list..."))
        await asyncio.sleep(0.3)
        job["output_lines"].append(_log_line("ok", "Email sent ✓"))

async def _sim_inventory_sync(job: dict):
    params = job["params"]
    dry    = params.get("dry_run", False)
    batch  = params.get("batch_size", 500)
    job["output_lines"].append(_log_line("inf", "Connecting to ERP API..."))
    await asyncio.sleep(0.4)
    job["output_lines"].append(_log_line("ok", "ERP API connection OK"))
    total = 4821
    synced = 0
    while synced < total:
        batch_n = min(batch, total - synced)
        synced += batch_n
        job["output_lines"].append(
            _log_line("inf", f"Processing batch: {synced}/{total} rows")
        )
        await asyncio.sleep(0.2)
    if params.get("skip_null_skus", True):
        job["output_lines"].append(
            _log_line("warn", "3 rows skipped: null SKUs at line 142, 387, 903")
        )
        job["warnings"].append("3 rows with null SKUs skipped")
    if not dry:
        job["output_lines"].append(_log_line("ok", f"Committed {total - 3} rows to DB"))
    else:
        job["output_lines"].append(_log_line("warn", "DRY RUN — no rows written"))
    job["rows_affected"] = total - 3

async def _sim_shell(job: dict):
    job["output_lines"].append(_log_line("inf", "Starting shell script..."))
    await asyncio.sleep(0.3)
    job["output_lines"].append(_log_line("inf", "Checking disk space..."))
    await asyncio.sleep(0.4)
    job["output_lines"].append(_log_line("ok", "Script completed. Resources released."))

async def _sim_db_insert(job: dict):
    params  = job["params"]
    dry     = params.get("dry_run", False)
    src     = params.get("source_table") or params.get("source_file", "staging.new_records")
    target  = params.get("target_table", "public.products")
    batch   = params.get("batch_size", 500)
    conflict= params.get("on_conflict", "abort")
    job["output_lines"].append(_log_line("inf", f"Reading source: {src}"))
    await asyncio.sleep(0.4)
    total = 1280
    job["output_lines"].append(_log_line("inf", f"Loaded {total} rows from source"))
    job["output_lines"].append(_log_line("inf", "Validating schema compatibility..."))
    await asyncio.sleep(0.3)
    job["output_lines"].append(_log_line("ok", "Schema validated. Begin transaction."))
    inserted = skipped = 0
    batches = (total + batch - 1) // batch
    for b in range(batches):
        n = min(batch, total - b * batch)
        inserted += n
        job["output_lines"].append(
            _log_line("inf", f"Batch {b+1}/{batches}: inserting {n} rows → {target}")
        )
        await asyncio.sleep(0.25)
    if not dry:
        job["output_lines"].append(_log_line("ok", f"COMMIT. {inserted} rows inserted."))
        job["output_lines"].append(_log_line("inf", "Audit record written to db.scriptops_audit"))
        job["rows_affected"] = inserted
    else:
        job["output_lines"].append(
            _log_line("warn", f"DRY RUN — ROLLBACK. {inserted} rows validated, none committed.")
        )
        job["rows_affected"] = 0

async def _sim_db_update(job: dict):
    params    = job["params"]
    dry       = params.get("dry_run", False)
    psheet    = params.get("price_sheet", "/data/pricing/june2024.csv")
    eff_date  = params.get("effective_date", "2024-06-01")
    job["output_lines"].append(_log_line("inf", f"Loading price sheet: {psheet}"))
    await asyncio.sleep(0.3)
    updated = 842
    job["output_lines"].append(_log_line("inf", f"Parsed {updated} price records"))
    job["output_lines"].append(_log_line("inf", "Validating against current product table..."))
    await asyncio.sleep(0.4)
    job["output_lines"].append(_log_line("warn", "12 SKUs not found in products — will skip"))
    job["warnings"].append("12 SKUs from price sheet not found in products table")
    job["output_lines"].append(
        _log_line("inf", f"Preparing UPDATE for {updated - 12} rows, effective {eff_date}")
    )
    await asyncio.sleep(0.3)
    if not dry:
        job["output_lines"].append(_log_line("ok", f"COMMIT. {updated - 12} rows updated."))
        job["output_lines"].append(_log_line("inf", "Rollback token issued: see job.rollback_token"))
        job["rows_affected"] = updated - 12
    else:
        job["output_lines"].append(
            _log_line("warn", f"DRY RUN — {updated - 12} rows would be updated. ROLLBACK.")
        )
        job["rows_affected"] = 0

async def _sim_db_export(job: dict):
    params = job["params"]
    table  = params.get("table", "public.products")
    limit  = params.get("limit", 1000)
    fmt    = params.get("format", "json")
    job["output_lines"].append(_log_line("inf", f"Connecting to DB (read-only replica)..."))
    await asyncio.sleep(0.3)
    job["output_lines"].append(_log_line("ok", "Connected to replica"))
    job["output_lines"].append(
        _log_line("inf", f"SELECT * FROM {table} LIMIT {limit}")
    )
    await asyncio.sleep(0.5)
    job["output_lines"].append(_log_line("inf", f"Fetched {limit} rows"))
    out = f"/tmp/export_{table.replace('.','_')}_{int(time.time())}.{fmt}"
    job["output_lines"].append(_log_line("inf", f"Writing → {out}"))
    await asyncio.sleep(0.2)
    job["output_path"]  = out
    job["rows_affected"] = limit

async def _sim_generic(job: dict):
    job["output_lines"].append(_log_line("inf", "Starting..."))
    await asyncio.sleep(0.5)
    job["output_lines"].append(_log_line("ok", "Done."))


# ─── SSE stream generator ────────────────────────────────────────────────────

async def stream_job_output(job_id: str) -> AsyncGenerator[str, None]:
    """
    Yields Server-Sent Events for the given job's output lines.
    Polls _JOBS[job_id]["output_lines"] until the job completes.
    """
    seen   = 0
    max_wait = 600   # seconds
    elapsed  = 0
    interval = 0.25  # poll interval seconds

    yield _sse_event("connected", {"job_id": job_id, "message": "Stream connected"})

    while elapsed < max_wait:
        job = _JOBS.get(job_id)
        if not job:
            yield _sse_event("error", {"message": f"Job {job_id} not found"})
            return

        lines = job["output_lines"]
        while seen < len(lines):
            yield _sse_event("output", lines[seen])
            seen += 1

        if job["status"] in (JobStatus.success.value, JobStatus.failed.value,
                              JobStatus.cancelled.value):
            yield _sse_event("done", {
                "job_id":       job_id,
                "status":       job["status"],
                "exit_code":    job.get("exit_code"),
                "duration_ms":  job.get("duration_ms"),
                "rows_affected":job.get("rows_affected"),
            })
            return

        await asyncio.sleep(interval)
        elapsed += interval

    yield _sse_event("timeout", {"job_id": job_id, "message": "Stream timed out"})


def _sse_event(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"
