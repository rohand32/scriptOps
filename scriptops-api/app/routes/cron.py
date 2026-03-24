"""
ScriptOps — Cron / Scheduler API Routes

Manual trigger, schedule management, and history for cron jobs.
Role: Admin (trigger + manage), Manager (trigger only + view)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Any

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
from app.services import schedule_store
from app.services.scheduler_worker import get_scheduler, resync_jobs
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()


def _resync_scheduler() -> None:
    sched = get_scheduler()
    if sched is not None:
        resync_jobs(sched)


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
    items = list(schedule_store.all_schedules().values())
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
    sc = schedule_store.get_schedule(schedule_id)
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
    schedule_store.upsert_schedule(sc)
    _resync_scheduler()
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
    sc = schedule_store.get_schedule(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})

    updates = req.dict(exclude_none=True)
    if "cron_expr" in updates:
        updates["human_readable"] = _human_cron(updates["cron_expr"])
    if "notify_on" in updates:
        v = updates["notify_on"]
        updates["notify_on"] = v.value if isinstance(v, Enum) else v

    sc.update(updates)
    schedule_store.upsert_schedule(sc)
    _resync_scheduler()
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
    if not schedule_store.delete_schedule(schedule_id):
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})
    _resync_scheduler()
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
    sc = schedule_store.get_schedule(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":f"Schedule {schedule_id} not found"})
    sc["enabled"] = not sc["enabled"]
    schedule_store.upsert_schedule(sc)
    _resync_scheduler()
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
    sc = schedule_store.get_schedule(schedule_id)
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
        job_meta={
            "notify_on": req.notify_on.value,
            "schedule_id": schedule_id,
        },
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
    sc = schedule_store.get_schedule(schedule_id)
    if not sc:
        raise HTTPException(404, detail={"error":"not_found","message":"Schedule not found"})
    jobs, total = list_jobs(script_id=sc["script_id"], page=1, page_size=limit)
    return {"schedule_id": schedule_id, "script_name": sc["script_name"],
            "items": jobs, "total": total}
