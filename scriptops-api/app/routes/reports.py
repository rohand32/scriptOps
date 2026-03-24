"""
ScriptOps — Reports API Routes
Role: Manager, Admin
"""

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional

from app.middleware.auth import require_role, get_current_user
from app.models.schemas import (
    Role, TokenUser, JobRef, JobStatus,
    SalesReportRequest, InventorySyncRequest,
    UserActivityRequest, FinanceReconcileRequest,
    WeeklyDigestRequest, ReportResult,
)
from app.services.executor import (
    create_job, execute_script, get_job,
    list_jobs, SCRIPT_REGISTRY, TriggerType,
)
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()

# Shared dependency: manager or admin
_mgr_or_admin = Depends(require_role(Role.manager, Role.admin))


def _job_ref(job: dict) -> dict:
    jid = job["job_id"]
    return {
        "job_id":     jid,
        "status":     job["status"],
        "message":    f"Job {jid} queued. Use stream_url for live output.",
        "stream_url": f"/api/v1/executions/{jid}/stream",
        "status_url": f"/api/v1/executions/{jid}",
    }


# ─── Sales Report ─────────────────────────────────────────────────────────────

@router.post(
    "/sales",
    summary="Generate Sales Report",
    description="""
Runs `generate_sales_report.py` on prod-01.

**Role required:** Manager or Admin

**Output:** CSV / JSON / XLSX / PDF deposited to `/tmp/` on the server.
Set `send_email: true` to dispatch to the distribution list.

Use `stream_url` in the response to follow real-time output via SSE.
""",
    response_model=JobRef,
    status_code=202,
)
async def run_sales_report(
    req: SalesReportRequest,
    bg: BackgroundTasks,
    user: TokenUser = _mgr_or_admin,
):
    params = req.dict(exclude_none=True)
    job    = create_job("gen_sales", params, user.name, TriggerType.api)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] sales report queued by {user.name}")
    return _job_ref(job)


# ─── Inventory Sync ───────────────────────────────────────────────────────────

@router.post(
    "/inventory-sync",
    summary="Sync Inventory from ERP",
    description="""
Runs `sync_inventory.py` on prod-01.

**Role required:** Manager or Admin

Set `dry_run: true` to validate rows without writing to the database.
`batch_size` controls how many ERP records are processed per chunk.
""",
    response_model=JobRef,
    status_code=202,
)
async def run_inventory_sync(
    req: InventorySyncRequest,
    bg: BackgroundTasks,
    user: TokenUser = _mgr_or_admin,
):
    params = req.dict(exclude_none=True)
    job    = create_job("gen_inv", params, user.name, TriggerType.api,
                        server_override=req.server)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] inventory sync queued by {user.name}")
    return _job_ref(job)


# ─── User Activity Report ─────────────────────────────────────────────────────

@router.post(
    "/user-activity",
    summary="Generate User Activity Report",
    description="""
Runs `user_activity_report.py` on prod-02.

**Role required:** Manager or Admin

Filter by user_segment (e.g. `premium`, `trial`) and date range.
""",
    response_model=JobRef,
    status_code=202,
)
async def run_user_activity(
    req: UserActivityRequest,
    bg: BackgroundTasks,
    user: TokenUser = _mgr_or_admin,
):
    params = req.dict(exclude_none=True)
    job    = create_job("user_rpt", params, user.name, TriggerType.api)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] user activity report queued by {user.name}")
    return _job_ref(job)


# ─── Finance Reconciliation ───────────────────────────────────────────────────

@router.post(
    "/finance-reconcile",
    summary="Run Finance Reconciliation",
    description="""
Runs `finance_reconcile.py` on prod-01.

**Role required:** Manager or Admin

Reconciles transactions across all payment gateways (Razorpay, Stripe, etc.).
Set `auto_flag_discrepancies: true` to mark unmatched rows in the output.
""",
    response_model=JobRef,
    status_code=202,
)
async def run_finance_reconcile(
    req: FinanceReconcileRequest,
    bg: BackgroundTasks,
    user: TokenUser = _mgr_or_admin,
):
    params = req.dict(exclude_none=True)
    job    = create_job("fin_rpt", params, user.name, TriggerType.api)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] finance reconcile queued by {user.name}")
    return _job_ref(job)


# ─── Weekly Digest ────────────────────────────────────────────────────────────

@router.post(
    "/weekly-digest",
    summary="Generate Weekly Digest",
    description="""
Runs `weekly_digest.py` on prod-01.

**Role required:** Manager or Admin

`week_offset=0` = current week, `week_offset=1` = last week, etc.
Generates a PDF summary emailed to all managers.
""",
    response_model=JobRef,
    status_code=202,
)
async def run_weekly_digest(
    req: WeeklyDigestRequest,
    bg: BackgroundTasks,
    user: TokenUser = _mgr_or_admin,
):
    params = req.dict(exclude_none=True)
    job    = create_job("weekly_digest", params, user.name, TriggerType.api)
    bg.add_task(execute_script, job["job_id"])
    logger.info(f"[{job['job_id']}] weekly digest queued by {user.name}")
    return _job_ref(job)


# ─── List recent report jobs ──────────────────────────────────────────────────

@router.get(
    "/history",
    summary="Report Execution History",
    description="Returns history of report jobs. Manager+ can see their own; Admin sees all.",
)
async def report_history(
    status: Optional[str] = Query(None, description="Filter by status"),
    page:   int = Query(1,  ge=1),
    limit:  int = Query(20, ge=1, le=100),
    user: TokenUser = Depends(require_role(Role.manager, Role.admin)),
):
    report_script_ids = {"gen_sales","gen_inv","user_rpt","fin_rpt","weekly_digest"}
    by_user = user.name if user.role != Role.admin else None
    jobs, total = list_jobs(
        status=status,
        triggered_by=by_user,
        script_ids=report_script_ids,
        page=page,
        page_size=limit,
    )
    return {
        "items": jobs,
        "pagination": {
            "page": page, "page_size": limit,
            "total": total,
            "total_pages": (total + limit - 1) // limit,
        },
    }
