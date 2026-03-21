"""
ScriptOps — Cron / Scheduler API Routes

Manual trigger, schedule management, and history for cron jobs.
Role: Admin (trigger + manage), Manager (trigger only + view)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Path

from app.middleware.auth import require_role, get_current_user
from app.models.schemas import (
    Role, TokenUser, JobRef,
    CronTriggerRequest, ScheduleCreateRequest,
    ScheduleUpdateRequest, ScheduleResponse, JobStatus, NotifyOn,
)
from app.services.executor import (
    create_job, execute_script, get_job,
    list_jobs, SCRIPT_REGISTRY, TriggerType,
)
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()

# ── In-memory schedule store ──────────────────────────────────────────────────
_SCHEDULES: Dict[str, dict] = {
    "sc_001": {
        "schedule_id": "sc_001", "script_id": "gen_sales",
        "script_name": "generate_sales_report.py",
        "cron_expr": "0 8 * * 1-5", "human_readable": "Weekdays at 08:00",
        "server": "prod-01", "enabled": True, "notify_on": "failure",
        "params": {}, "description": "Daily sales report, Mon–Fri",
        "last_run": "2024-06-10T08:00:01Z", "next_run": "2024-06-11T08:00:00Z",
        "last_status": "success", "created_by": "Arjun Desai",
        "created_at": "2024-01-10T09:00:00Z",
    },
    "sc_002": {
        "schedule_id": "sc_002", "script_id": "db_backup",
        "script_name": "db_backup.sh",
        "cron_expr": "0 2 * * *", "human_readable": "Daily at 02:00 AM",
        "server": "db-01", "enabled": True, "notify_on": "always",
        "params": {}, "description": "Nightly PostgreSQL backup to S3",
        "last_run": "2024-06-11T02:00:02Z", "next_run": "2024-06-12T02:00:00Z",
        "last_status": "success", "created_by": "Arjun Desai",
        "created_at": "2024-01-10T09:00:00Z",
    },
    "sc_003": {
        "schedule_id": "sc_003", "script_id": "cleanup",
        "script_name": "cleanup_logs.sh",
        "cron_expr": "0 0 * * 0", "human_readable": "Sundays at midnight",
        "server": "prod-01", "enabled": True, "notify_on": "failure",
        "params": {}, "description": "Weekly log rotation",
        "last_run": "2024-06-09T00:00:01Z", "next_run": "2024-06-16T00:00:00Z",
        "last_status": "success", "created_by": "Arjun Desai",
        "created_at": "2024-01-15T09:00:00Z",
    },
    "sc_004": {
        "schedule_id": "sc_004", "script_id": "sched_sales",
        "script_name": "scheduled_sales_push.py",
        "cron_expr": "30 23 * * *", "human_readable": "Daily at 23:30",
        "server": "prod-01", "enabled": True, "notify_on": "always",
        "params": {}, "description": "Nightly data warehouse push",
        "last_run": "2024-06-10T23:30:01Z", "next_run": "2024-06-11T23:30:00Z",
        "last_status": "success", "created_by": "Arjun Desai",
        "created_at": "2024-02-01T09:00:00Z",
    },
    "sc_005": {
        "schedule_id": "sc_005", "script_id": "health",
        "script_name": "health_check.sh",
        "cron_expr": "*/5 * * * *", "human_readable": "Every 5 minutes",
        "server": "prod-01", "enabled": True, "notify_on": "failure",
        "params": {}, "description": "Endpoint health monitoring",
        "last_run": "2024-06-11T11:45:02Z", "next_run": "2024-06-11T11:50:00Z",
        "last_status": "success", "created_by": "Arjun Desai",
        "created_at": "2024-01-10T09:00:00Z",
    },
    "sc_006": {
        "schedule_id": "sc_006", "script_id": "weekly_digest",
        "script_name": "weekly_digest.py",
        "cron_expr": "0 7 * * 1", "human_readable": "Mondays at 07:00",
        "server": "prod-01", "enabled": True, "notify_on": "failure",
        "params": {}, "description": "Monday morning management digest",
        "last_run": "2024-06-10T07:00:01Z", "next_run": "2024-06-17T07:00:00Z",
        "last_status": "success", "created_by": "Arjun Desai",
        "created_at": "2024-01-10T09:00:00Z",
    },
}


def _human_cron(expr: str) -> str:
    """Best-effort human-readable description of a cron expression."""
    try:
        min_, hr, dom, mon, dow = expr.strip().split()
        parts = []
        if dow == "*" and dom == "*":
            parts.append("Every day")
        elif dom == "*":
            dow_map = {"0":"Sunday","1":"Monday","2":"Tuesday","3":"Wednesday",
                       "4":"Thursday","5":"Friday","6":"Saturday",
                       "1-5":"weekdays","1-7":"every day"}
            parts.append(f"Every {dow_map.get(dow, dow)}")
        else:
            parts.append(f"Day {dom} of month")

        if min_ == "*" and hr == "*":
            parts.append("every minute")
        elif min_.startswith("*/"):
            parts.append(f"every {min_[2:]} minutes")
        else:
            parts.append(f"at {hr.zfill(2)}:{min_.zfill(2)}")

        return ", ".join(parts)
    except Exception:
        return expr


# ─── List all schedules ───────────────────────────────────────────────────────

@router.get(
    "/schedules",
    summary="List All Schedules",
    description="Returns all registered cron schedules. **Role: Manager+**",
)
async def list_schedules(
    enabled: Optional[bool] = Query(None),
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    items = list(_SCHEDULES.values())
    if enabled is not None:
        items = [s for s in items if s["enabled"] == enabled]
    return {"items": items, "total": len(items)}


# ─── Get schedule ─────────────────────────────────────────────────────────────

@router.get(
    "/schedules/{schedule_id}",
    summary="Get Schedule Details",
)
async def get_schedule(
    schedule_id: str = Path(..., example="sc_001"),
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    sc = _SCHEDULES.get(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})
    return sc


# ─── Create schedule ──────────────────────────────────────────────────────────

@router.post(
    "/schedules",
    summary="Create Schedule",
    description="Register a new cron schedule. **Role: Admin only**",
    status_code=201,
)
async def create_schedule(
    req: ScheduleCreateRequest,
    user: TokenUser = Depends(require_role(Role.admin)),
):
    script = SCRIPT_REGISTRY.get(req.script_id)
    if not script:
        raise HTTPException(400, detail={"error":"unknown_script",
                                          "message":f"script_id '{req.script_id}' not registered"})
    sc_id = f"sc_{uuid.uuid4().hex[:6]}"
    now   = datetime.now(timezone.utc).isoformat()
    sc    = {
        "schedule_id":   sc_id,
        "script_id":     req.script_id,
        "script_name":   script["name"],
        "cron_expr":     req.cron_expr,
        "human_readable":_human_cron(req.cron_expr),
        "server":        req.server,
        "enabled":       req.enabled,
        "notify_on":     req.notify_on.value,
        "params":        req.params,
        "description":   req.description,
        "last_run":      None,
        "next_run":      None,
        "last_status":   None,
        "created_by":    user.name,
        "created_at":    now,
    }
    _SCHEDULES[sc_id] = sc
    logger.info(f"Schedule {sc_id} created by {user.name}: {req.cron_expr} → {script['name']}")
    return sc


# ─── Update schedule ──────────────────────────────────────────────────────────

@router.patch(
    "/schedules/{schedule_id}",
    summary="Update Schedule",
    description="Modify a schedule (enable/disable, change cron, etc.). **Role: Admin only**",
)
async def update_schedule(
    req: ScheduleUpdateRequest,
    schedule_id: str = Path(...),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    sc = _SCHEDULES.get(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})

    updates = req.dict(exclude_none=True)
    if "cron_expr" in updates:
        updates["human_readable"] = _human_cron(updates["cron_expr"])
    if "notify_on" in updates:
        updates["notify_on"] = updates["notify_on"].value

    sc.update(updates)
    logger.info(f"Schedule {schedule_id} updated by {user.name}: {list(updates.keys())}")
    return sc


# ─── Delete schedule ──────────────────────────────────────────────────────────

@router.delete(
    "/schedules/{schedule_id}",
    summary="Delete Schedule",
    description="Permanently remove a schedule. **Role: Admin only**",
    status_code=204,
)
async def delete_schedule(
    schedule_id: str = Path(...),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    if schedule_id not in _SCHEDULES:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})
    del _SCHEDULES[schedule_id]
    logger.info(f"Schedule {schedule_id} deleted by {user.name}")
    return None


# ─── Toggle schedule on/off ───────────────────────────────────────────────────

@router.post(
    "/schedules/{schedule_id}/toggle",
    summary="Enable / Disable Schedule",
    description="Toggle a schedule on or off. **Role: Admin or Manager**",
)
async def toggle_schedule(
    schedule_id: str = Path(...),
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    sc = _SCHEDULES.get(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})
    sc["enabled"] = not sc["enabled"]
    state = "enabled" if sc["enabled"] else "disabled"
    logger.info(f"Schedule {schedule_id} {state} by {user.name}")
    return {"schedule_id": schedule_id, "enabled": sc["enabled"],
            "message": f"Schedule {state} successfully."}


# ─── Manual trigger ───────────────────────────────────────────────────────────

@router.post(
    "/schedules/{schedule_id}/trigger",
    summary="Manually Trigger a Scheduled Job",
    description="""
Execute a scheduled job immediately, without waiting for its cron time.

**Role: Admin or Manager**

The job runs with the schedule's default params unless overridden in the request body.
A `reason` field is strongly recommended for audit purposes.
""",
    response_model=JobRef,
    status_code=202,
)
async def trigger_schedule(
    req: CronTriggerRequest,
    bg: BackgroundTasks,
    schedule_id: str = Path(..., example="sc_001"),
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    sc = _SCHEDULES.get(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})

    # Merge default params with override params
    params = {**sc.get("params", {}), **req.params}
    server = req.server or sc["server"]
    job    = create_job(
        sc["script_id"], params,
        triggered_by=user.name,
        trigger=TriggerType.manual,
        server_override=server,
    )
    bg.add_task(execute_script, job["job_id"])

    reason_note = f" Reason: {req.reason}" if req.reason else ""
    logger.info(
        f"[{job['job_id']}] Manual trigger: {sc['script_name']} "
        f"by {user.name}.{reason_note}"
    )
    return {
        "job_id":     job["job_id"],
        "status":     job["status"],
        "message":    (
            f"Manually triggered '{sc['script_name']}'. "
            f"Scheduled: {sc['cron_expr']} ({sc['human_readable']})."
            + (f" Reason: {req.reason}" if req.reason else "")
        ),
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }


# ─── Trigger by script_id directly ───────────────────────────────────────────

@router.post(
    "/run/{script_id}",
    summary="Trigger Cron Script by Script ID",
    description="""
Directly trigger any registered cron script by its ID (e.g. `sched_sales`, `sched_clean`).

**Role: Admin only** (cron scripts are admin-gated)

Params in the body override the defaults for this run only.
""",
    response_model=JobRef,
    status_code=202,
)
async def run_cron_script(
    req: CronTriggerRequest,
    bg: BackgroundTasks,
    script_id: str = Path(..., example="sched_sales"),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    script = SCRIPT_REGISTRY.get(script_id)
    if not script:
        raise HTTPException(
            400,
            detail={"error":"unknown_script","message":f"script_id '{script_id}' not registered"},
        )
    if script.get("min_role") != "admin":
        raise HTTPException(
            403,
            detail={"error":"not_a_cron_script",
                    "message":f"Script '{script_id}' is not a cron-category script"},
        )

    job = create_job(script_id, req.params, user.name, TriggerType.manual,
                     server_override=req.server)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] cron script {script_id} triggered by {user.name}")
    return {
        "job_id":     job["job_id"],
        "status":     job["status"],
        "message":    f"Cron script '{script['name']}' triggered manually.",
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }


# ─── Schedule run history ─────────────────────────────────────────────────────

@router.get(
    "/schedules/{schedule_id}/history",
    summary="Schedule Run History",
    description="Last N runs for a specific schedule. **Role: Manager+**",
)
async def schedule_history(
    schedule_id: str = Path(...),
    limit: int = Query(20, ge=1, le=100),
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    sc = _SCHEDULES.get(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":"Schedule not found"})
    jobs, total = list_jobs(script_id=sc["script_id"], page=1, page_size=limit)
    return {"schedule_id": schedule_id, "script_name": sc["script_name"],
            "items": jobs, "total": total}
