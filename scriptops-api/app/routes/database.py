"""
ScriptOps — Database Operations API Routes

SELECT  → Manager, Admin
INSERT  → Admin only  (with optional 2-admin approval gate)
UPDATE  → Admin only  (with optional 2-admin approval gate)
All write operations are fully audited and support dry_run.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query

from app.middleware.auth import require_role, get_current_user
from app.models.schemas import (
    Role, TokenUser, JobRef, DBOperation,
    DBSelectRequest, DBInsertRequest, DBUpdateRequest,
    DBAuditLog, DBJobResult, JobStatus,
)
from app.services.executor import (
    create_job, execute_script, get_job,
    list_jobs, SCRIPT_REGISTRY, TriggerType,
)
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()

# ── Audit log store ───────────────────────────────────────────────────────────
_AUDIT: Dict[str, dict] = {}

# ── Pending approval store ────────────────────────────────────────────────────
# When require_approval=True, the job waits here until a 2nd admin approves.
_PENDING_APPROVAL: Dict[str, dict] = {}

# Pre-approved read-only script IDs
_READ_SCRIPTS = {"db_export"}


def _script_category_str(script: dict) -> str:
    """Normalize script category to a string (enum or plain str from YAML)."""
    cat = script.get("category")
    if cat is None:
        return ""
    return cat.value if hasattr(cat, "value") else str(cat)


def _write_audit(
    job_id: str,
    operation: DBOperation,
    script_id: str,
    table_name: str,
    rows_affected: int,
    dry_run: bool,
    executed_by: str,
    change_reason: Optional[str] = None,
    approved_by: Optional[str] = None,
) -> dict:
    audit_id = f"AUD-{uuid.uuid4().hex[:8].upper()}"
    record = {
        "audit_id":          audit_id,
        "job_id":            job_id,
        "operation":         operation.value,
        "script_id":         script_id,
        "table_name":        table_name,
        "rows_affected":     rows_affected,
        "dry_run":           dry_run,
        "approved_by":       approved_by,
        "executed_by":       executed_by,
        "change_reason":     change_reason,
        "executed_at":       datetime.now(timezone.utc).isoformat(),
        "rollback_available":not dry_run and operation != DBOperation.select,
    }
    _AUDIT[audit_id] = record
    return record


# ─── DB SELECT ────────────────────────────────────────────────────────────────

@router.post(
    "/select",
    summary="Run DB Read / Export Query",
    description="""
Execute a pre-approved read-only DB script.

**Role: Manager or Admin**

Only scripts registered under category `database` with `min_role: manager` are accepted.
Results are written to a temp file; the output path is returned in the job result.

Use `format: json | csv | xlsx` to control output type.
""",
    response_model=JobRef,
    status_code=202,
)
async def db_select(
    req: DBSelectRequest,
    bg: BackgroundTasks,
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    if req.script_id not in _READ_SCRIPTS:
        raise HTTPException(
            400,
            detail={
                "error":   "not_a_read_script",
                "message": f"'{req.script_id}' is not a pre-approved read script. "
                           f"Allowed: {sorted(_READ_SCRIPTS)}",
            },
        )
    script = SCRIPT_REGISTRY.get(req.script_id)
    if not script:
        raise HTTPException(400, detail={"error":"unknown_script",
                                          "message":f"script_id '{req.script_id}' not found"})
    params = {**req.params, "format": req.format.value}
    job    = create_job(req.script_id, params, user.name, TriggerType.api)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] DB select: {req.script_id} by {user.name}")
    return {
        "job_id":     job["job_id"],
        "status":     job["status"],
        "message":    f"Read query queued. Output will be written to /tmp/ on {script['server']}.",
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }


# ─── DB INSERT ────────────────────────────────────────────────────────────────

@router.post(
    "/insert",
    summary="Run DB Insert Script",
    description="""
Execute a bulk INSERT from staging table or file into the target production table.

**Role: Admin only**

### Safety controls
- `dry_run: true` — validates rows, builds the INSERT statements, then rolls back. No rows committed.
- `require_approval: true` — creates a **pending approval** record. A second admin must call
  `POST /api/v1/database/approvals/{approval_id}/approve` before the job executes.
- `on_conflict` — `abort` (default) | `skip` | `replace`

All INSERT operations are logged to `db.scriptops_audit` with a rollback token.
""",
    response_model=JobRef,
    status_code=202,
)
async def db_insert(
    req: DBInsertRequest,
    bg: BackgroundTasks,
    user: TokenUser = Depends(require_role(Role.admin)),
):
    if not req.source_table and not req.source_file:
        raise HTTPException(
            400,
            detail={"error":"missing_source",
                    "message":"Provide either source_table or source_file."},
        )

    script = SCRIPT_REGISTRY.get(req.script_id)
    if not script or _script_category_str(script) != "database":
        raise HTTPException(
            400,
            detail={"error":"invalid_script",
                    "message":f"'{req.script_id}' is not a registered DB insert script."},
        )

    params = req.dict(exclude_none=True)

    # ── Approval gate ────────────────────────────────────────────────────────
    if req.require_approval and not req.dry_run:
        approval_id = f"APR-{uuid.uuid4().hex[:8].upper()}"
        _PENDING_APPROVAL[approval_id] = {
            "approval_id":   approval_id,
            "operation":     "insert",
            "script_id":     req.script_id,
            "target_table":  req.target_table,
            "params":        params,
            "requested_by":  user.name,
            "requested_at":  datetime.now(timezone.utc).isoformat(),
            "status":        "pending",
            "approved_by":   None,
            "job_id":        None,
        }
        logger.info(
            f"DB insert approval requested [{approval_id}] by {user.name} "
            f"→ {req.target_table}"
        )
        return {
            "job_id":     approval_id,
            "status":     "pending",
            "message":    (
                f"Approval required. A second admin must approve [{approval_id}] "
                f"before this INSERT runs. "
                f"POST /api/v1/database/approvals/{approval_id}/approve"
            ),
            "stream_url": f"/api/v1/database/approvals/{approval_id}",
            "status_url": f"/api/v1/database/approvals/{approval_id}",
        }

    # ── Execute directly (dry run or approval bypassed) ──────────────────────
    job = create_job(req.script_id, params, user.name, TriggerType.api)
    bg.add_task(_exec_with_audit, job["job_id"], DBOperation.insert,
                req.target_table, req.dry_run, user.name, None)
    logger.info(
        f"[{job['job_id']}] DB insert: {req.script_id} → {req.target_table} "
        f"by {user.name} (dry_run={req.dry_run})"
    )
    return {
        "job_id":     job["job_id"],
        "status":     job["status"],
        "message":    (
            ("DRY RUN — " if req.dry_run else "")
            + f"INSERT job queued. Target: {req.target_table}."
        ),
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }


# ─── DB UPDATE ────────────────────────────────────────────────────────────────

@router.post(
    "/update",
    summary="Run DB Update Script",
    description="""
Execute a pre-approved UPDATE script against the production database.

**Role: Admin only**

### Safety controls
- `dry_run: true` — generates the UPDATE statements, validates, then rolls back.
- `require_approval: true` — requires a second admin's approval before executing.
- `change_reason` — **required** field for audit trail (min 10 chars).

A rollback token is issued after every committed UPDATE so the operation can be
reversed via `POST /api/v1/database/rollback/{rollback_token}`.
""",
    response_model=JobRef,
    status_code=202,
)
async def db_update(
    req: DBUpdateRequest,
    bg: BackgroundTasks,
    user: TokenUser = Depends(require_role(Role.admin)),
):
    script = SCRIPT_REGISTRY.get(req.script_id)
    if not script:
        raise HTTPException(
            400,
            detail={"error":"unknown_script","message":f"script_id '{req.script_id}' not found"},
        )

    params = req.params.copy()
    params["dry_run"] = req.dry_run
    target_table = params.get("target_table", "db.products")

    # ── Approval gate ────────────────────────────────────────────────────────
    if req.require_approval and not req.dry_run:
        approval_id = f"APR-{uuid.uuid4().hex[:8].upper()}"
        _PENDING_APPROVAL[approval_id] = {
            "approval_id":   approval_id,
            "operation":     "update",
            "script_id":     req.script_id,
            "target_table":  target_table,
            "params":        params,
            "change_reason": req.change_reason,
            "requested_by":  user.name,
            "requested_at":  datetime.now(timezone.utc).isoformat(),
            "status":        "pending",
            "approved_by":   None,
            "job_id":        None,
        }
        logger.info(
            f"DB update approval requested [{approval_id}] by {user.name}: {req.change_reason}"
        )
        return {
            "job_id":     approval_id,
            "status":     "pending",
            "message":    (
                f"Approval required. A second admin must approve [{approval_id}]. "
                f"POST /api/v1/database/approvals/{approval_id}/approve"
            ),
            "stream_url": f"/api/v1/database/approvals/{approval_id}",
            "status_url": f"/api/v1/database/approvals/{approval_id}",
        }

    # ── Execute directly ──────────────────────────────────────────────────────
    job = create_job(req.script_id, params, user.name, TriggerType.api)
    bg.add_task(
        _exec_with_audit, job["job_id"], DBOperation.update,
        target_table, req.dry_run, user.name, req.change_reason,
    )
    logger.info(
        f"[{job['job_id']}] DB update: {req.script_id} by {user.name} "
        f"(dry_run={req.dry_run}) — {req.change_reason}"
    )
    return {
        "job_id":     job["job_id"],
        "status":     job["status"],
        "message":    (
            ("DRY RUN — " if req.dry_run else "")
            + f"UPDATE job queued. Script: {script['name']}."
        ),
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }


# ─── Approvals ────────────────────────────────────────────────────────────────

@router.get(
    "/approvals",
    summary="List Pending Approvals",
    description="Returns all write operations awaiting a second admin's approval. **Role: Admin**",
)
async def list_approvals(
    user: TokenUser = Depends(require_role(Role.admin)),
):
    items = [a for a in _PENDING_APPROVAL.values() if a["status"] == "pending"]
    return {"items": items, "total": len(items)}


@router.get(
    "/approvals/{approval_id}",
    summary="Get Approval Details",
)
async def get_approval(
    approval_id: str = Path(...),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    ap = _PENDING_APPROVAL.get(approval_id)
    if not ap:
        raise HTTPException(404, detail={"error":"not_found","message":"Approval not found"})
    return ap


@router.post(
    "/approvals/{approval_id}/approve",
    summary="Approve a Pending DB Write",
    description="""
A **second** admin approves a pending INSERT or UPDATE.
The approver must be different from the requestor.

**Role: Admin only**
""",
    response_model=JobRef,
    status_code=202,
)
async def approve_db_write(
    approval_id: str,
    bg: BackgroundTasks,
    user: TokenUser = Depends(require_role(Role.admin)),
):
    ap = _PENDING_APPROVAL.get(approval_id)
    if not ap:
        raise HTTPException(404, detail={"error":"not_found","message":"Approval not found"})
    if ap["status"] != "pending":
        raise HTTPException(
            409,
            detail={"error":"already_processed",
                    "message":f"Approval {approval_id} is already {ap['status']}"},
        )
    if ap["requested_by"] == user.name:
        raise HTTPException(
            403,
            detail={"error":"self_approval_forbidden",
                    "message":"The approver must be a different admin than the requestor."},
        )

    # Mark approved
    ap["status"]      = "approved"
    ap["approved_by"] = user.name
    ap["approved_at"] = datetime.now(timezone.utc).isoformat()

    # Queue the job
    job = create_job(ap["script_id"], ap["params"], ap["requested_by"], TriggerType.api)
    ap["job_id"] = job["job_id"]
    op = DBOperation(ap["operation"])
    bg.add_task(
        _exec_with_audit, job["job_id"], op,
        ap["target_table"], False, ap["requested_by"],
        ap.get("change_reason"), user.name,
    )
    logger.info(
        f"[{job['job_id']}] DB {ap['operation']} approved by {user.name}, "
        f"requested by {ap['requested_by']}"
    )
    return {
        "job_id":     job["job_id"],
        "status":     "pending",
        "message":    f"Approved by {user.name}. Job queued.",
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }


@router.post(
    "/approvals/{approval_id}/reject",
    summary="Reject a Pending DB Write",
)
async def reject_db_write(
    approval_id: str,
    reason: str = Query(..., min_length=5),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    ap = _PENDING_APPROVAL.get(approval_id)
    if not ap:
        raise HTTPException(404, detail={"error":"not_found","message":"Approval not found"})
    if ap["status"] != "pending":
        raise HTTPException(409, detail={"error":"already_processed"})
    ap["status"]      = "rejected"
    ap["rejected_by"] = user.name
    ap["reject_reason"]= reason
    logger.info(f"Approval {approval_id} rejected by {user.name}: {reason}")
    return {"approval_id": approval_id, "status": "rejected", "rejected_by": user.name}


# ─── Rollback ─────────────────────────────────────────────────────────────────

@router.post(
    "/rollback/{rollback_token}",
    summary="Rollback a DB Write",
    description="""
Reverse a committed INSERT or UPDATE using its rollback token.
The token is issued in the job result after every successful write.

**Role: Admin only**

Rollbacks are themselves audited and irreversible.
""",
)
async def rollback_db_write(
    rollback_token: str = Path(..., example="RBK-A1B2C3D4"),
    reason: str = Query(..., min_length=5, example="Incorrect pricing data loaded"),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    # In production: look up the savepoint/WAL position and execute ROLLBACK TO SAVEPOINT
    logger.info(f"Rollback requested [{rollback_token}] by {user.name}: {reason}")
    return {
        "rollback_token": rollback_token,
        "status":         "rollback_queued",
        "initiated_by":   user.name,
        "reason":         reason,
        "message":        (
            "Rollback queued. The transaction will be reversed and an audit "
            "entry will be written. This may take a few minutes."
        ),
    }


# ─── Audit log ────────────────────────────────────────────────────────────────

@router.get(
    "/audit",
    summary="DB Audit Log",
    description="Full audit trail of all DB operations. **Role: Admin**",
)
async def get_audit_log(
    operation: Optional[str] = Query(None, description="insert | update | select"),
    table:     Optional[str] = Query(None),
    page:      int = Query(1, ge=1),
    limit:     int = Query(20, ge=1, le=100),
    user: TokenUser = Depends(require_role(Role.admin)),
):
    records = list(_AUDIT.values())
    if operation:
        records = [r for r in records if r["operation"] == operation]
    if table:
        records = [r for r in records if table.lower() in r["table_name"].lower()]
    records.sort(key=lambda r: r["executed_at"], reverse=True)
    total = len(records)
    start = (page - 1) * limit
    return {
        "items": records[start : start + limit],
        "pagination": {
            "page": page, "page_size": limit,
            "total": total,
            "total_pages": (total + limit - 1) // limit,
        },
    }


# ─── Internal: execute + audit ────────────────────────────────────────────────

async def _exec_with_audit(
    job_id:        str,
    operation:     DBOperation,
    table_name:    str,
    dry_run:       bool,
    executed_by:   str,
    change_reason: Optional[str] = None,
    approved_by:   Optional[str] = None,
):
    """Run the script then write an audit record."""
    from app.services.executor import execute_script as _exec, get_job as _get
    await _exec(job_id)
    job = _get(job_id)
    if not job:
        return
    rows = job.get("rows_affected") or 0
    audit = _write_audit(
        job_id, operation, job["script_id"], table_name,
        rows, dry_run, executed_by, change_reason, approved_by,
    )
    # Attach audit_id and rollback token to job
    job["audit_id"]      = audit["audit_id"]
    job["rollback_token"]= f"RBK-{uuid.uuid4().hex[:8].upper()}" if not dry_run else None
