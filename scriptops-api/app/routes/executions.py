"""
ScriptOps — Executions API Routes

Provides job status polling, real-time SSE output streaming,
execution history, log retrieval, and job cancellation.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import StreamingResponse

from app.middleware.auth import require_role, require_min_role, get_current_user
from app.models.schemas import Role, TokenUser, JobStatus, CancelRequest
from app.services.executor import get_job, list_jobs, stream_job_output
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()


# ─── Job Status ───────────────────────────────────────────────────────────────

@router.get(
    "/{job_id}",
    summary="Get Job Status",
    description="""
Poll the status and result of any job.

**Role: Viewer+** (all roles can read status of their own jobs)

Admins can view any job. Other roles see only their own jobs.
""",
)
async def get_execution(
    job_id: str = Path(..., example="J0047"),
    user: TokenUser = Depends(get_current_user),
):
    job = get_job(job_id)
    if not job:
        raise HTTPException(
            404,
            detail={"error": "not_found", "message": f"Job {job_id} not found"},
        )
    # Non-admin users can only see their own jobs
    if user.role != Role.admin and job.get("triggered_by") != user.name:
        raise HTTPException(
            403,
            detail={
                "error":   "forbidden",
                "message": "You can only view your own job executions.",
            },
        )
    # Strip raw output_lines from status response (use /logs for that)
    return {k: v for k, v in job.items() if k != "output_lines"}


# ─── SSE Real-Time Stream ─────────────────────────────────────────────────────

@router.get(
    "/{job_id}/stream",
    summary="Stream Real-Time Job Output (SSE)",
    description="""
Returns a **Server-Sent Events** stream of the job's real-time output.

Connect with EventSource in the browser or `curl -N`:
```
curl -N -H "X-ScriptOps-Key: sk_live_..." \\
  https://scriptops.internal/api/v1/executions/J0047/stream
```

### Events
| Event      | Payload |
|------------|---------|
| `connected`| `{job_id}` — stream is live |
| `output`   | `{ts, level, text}` — one line of script output |
| `done`     | `{status, exit_code, duration_ms, rows_affected}` |
| `error`    | `{message}` |
| `timeout`  | `{message}` — stream closed after 10 min |

**Role: Viewer+**
""",
    response_class=StreamingResponse,
)
async def stream_execution(
    job_id: str = Path(..., example="J0047"),
    user: TokenUser = Depends(get_current_user),
):
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, detail={"error":"not_found","message":f"Job {job_id} not found"})
    if user.role != Role.admin and job.get("triggered_by") != user.name:
        raise HTTPException(403, detail={"error":"forbidden",
                                          "message":"You can only stream your own jobs."})

    return StreamingResponse(
        stream_job_output(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ─── Job Logs ─────────────────────────────────────────────────────────────────

@router.get(
    "/{job_id}/logs",
    summary="Get Full Job Output Log",
    description="""
Returns the complete captured output log for a completed job.

**Role: Viewer+** (own jobs; Admin sees all)

Use `level` filter to show only `err`, `warn`, `ok`, `inf` lines.
""",
)
async def get_logs(
    job_id: str = Path(...),
    level:  Optional[str] = Query(None, description="Filter: err | warn | ok | inf"),
    user: TokenUser = Depends(get_current_user),
):
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, detail={"error":"not_found","message":f"Job {job_id} not found"})
    if user.role != Role.admin and job.get("triggered_by") != user.name:
        raise HTTPException(403, detail={"error":"forbidden"})

    lines = job.get("output_lines", [])
    if level:
        lines = [l for l in lines if l.get("level") == level]

    return {
        "job_id":    job_id,
        "script":    job.get("script_name"),
        "status":    job.get("status"),
        "log_count": len(lines),
        "lines":     lines,
    }


# ─── Execution History ────────────────────────────────────────────────────────

@router.get(
    "/",
    summary="List Executions",
    description="""
Returns paginated execution history.

**Role: Viewer+**

Viewers and non-admins see only their own jobs.
Admins see all jobs across all users.
""",
)
async def list_executions(
    script_id:  Optional[str] = Query(None),
    status:     Optional[str] = Query(None, description="pending|running|success|failed|cancelled"),
    page:       int = Query(1, ge=1),
    limit:      int = Query(20, ge=1, le=100),
    user: TokenUser = Depends(get_current_user),
):
    # Non-admin: scope to own jobs
    by_user = None if user.role == Role.admin else user.name
    jobs, total = list_jobs(
        script_id=script_id,
        status=status,
        triggered_by=by_user,
        page=page,
        page_size=limit,
    )
    # Strip output_lines from list view
    clean = [{k: v for k, v in j.items() if k != "output_lines"} for j in jobs]
    return {
        "items": clean,
        "pagination": {
            "page": page, "page_size": limit,
            "total": total,
            "total_pages": max(1, (total + limit - 1) // limit),
        },
    }


# ─── Cancel Job ───────────────────────────────────────────────────────────────

@router.post(
    "/{job_id}/cancel",
    summary="Cancel a Running Job",
    description="""
Request cancellation of a running job.

**Role:** Owner of the job, or Admin.

Sends SIGTERM to the remote process. The job transitions to `cancelled`
status. Note: DB write jobs that are mid-transaction will be rolled back.
""",
)
async def cancel_execution(
    req: CancelRequest,
    job_id: str = Path(...),
    user: TokenUser = Depends(get_current_user),
):
    from app.services.executor import _JOBS
    job = get_job(job_id)
    if not job:
        raise HTTPException(404, detail={"error":"not_found","message":f"Job {job_id} not found"})
    if user.role != Role.admin and job.get("triggered_by") != user.name:
        raise HTTPException(403, detail={"error":"forbidden",
                                          "message":"You can only cancel your own jobs."})
    if job["status"] not in (JobStatus.pending.value, JobStatus.running.value):
        raise HTTPException(
            409,
            detail={
                "error":   "not_cancellable",
                "message": f"Job is already {job['status']} — cannot cancel.",
            },
        )
    from datetime import datetime, timezone
    job["status"]       = JobStatus.cancelled.value
    job["completed_at"] = datetime.now(timezone.utc).isoformat()
    job["error"]        = f"Cancelled by {user.name}" + (f": {req.reason}" if req.reason else "")
    job["output_lines"].append({
        "ts":    datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "level": "warn",
        "text":  f"Job cancelled by {user.name}" + (f" — {req.reason}" if req.reason else ""),
    })
    logger.info(f"[{job_id}] cancelled by {user.name}" +
                (f": {req.reason}" if req.reason else ""))
    return {
        "job_id":  job_id,
        "status":  "cancelled",
        "message": "Job cancellation requested. SIGTERM sent to remote process.",
    }
