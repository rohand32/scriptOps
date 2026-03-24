"""
List registered scripts and run by script_id with validated params.
"""
from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path

from app.middleware.auth import get_current_user
from app.models.schemas import Role, TokenUser, JobRef, ScriptRunRequest, TriggerType
from app.services.executor import SCRIPT_REGISTRY, create_job, execute_script
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()


def can_run_script(user: TokenUser, script_id: str, script: dict) -> bool:
    cat = script["category"]
    cv = cat.value if hasattr(cat, "value") else str(cat)
    if cv == "report":
        return user.role in (Role.manager, Role.admin)
    if cv == "shell":
        return user.role in (Role.operator, Role.admin)
    if cv == "database":
        if script_id in ("db_insert", "db_update"):
            return user.role == Role.admin
        return user.role in (Role.manager, Role.admin)
    if cv == "cron":
        return user.role == Role.admin
    return False


@router.get("/", summary="List registered scripts")
async def list_scripts(user: TokenUser = Depends(get_current_user)):
    items = []
    for sid, meta in SCRIPT_REGISTRY.items():
        cat = meta["category"]
        items.append(
            {
                "script_id": sid,
                "name": meta["name"],
                "category": cat.value if hasattr(cat, "value") else str(cat),
                "server": meta["server"],
                "path": meta["path"],
                "interpreter": meta["interpreter"],
                "min_role": meta["min_role"],
                "allowed_params": meta.get("allowed_params", []),
            }
        )
    return {"items": items, "total": len(items)}


@router.post(
    "/{script_id}/run",
    summary="Run a script by ID",
    response_model=JobRef,
    status_code=202,
)
async def run_script(
    req: ScriptRunRequest,
    bg: BackgroundTasks,
    script_id: str = Path(..., example="gen_sales"),
    user: TokenUser = Depends(get_current_user),
):
    script = SCRIPT_REGISTRY.get(script_id)
    if not script:
        raise HTTPException(
            400,
            detail={"error": "unknown_script", "message": f"script_id '{script_id}' not registered"},
        )
    if not can_run_script(user, script_id, script):
        raise HTTPException(
            403,
            detail={
                "error": "insufficient_permissions",
                "message": "Your role cannot run this script category.",
            },
        )

    job = create_job(
        script_id,
        req.params or {},
        user.name,
        TriggerType.api,
        server_override=req.server,
    )
    bg.add_task(execute_script, job["job_id"])
    logger.info("[%s] script run queued: %s by %s", job["job_id"], script_id, user.name)
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "message": f"Job queued for {script['name']}.",
        "stream_url": f"/api/v1/executions/{job['job_id']}/stream",
        "status_url": f"/api/v1/executions/{job['job_id']}",
    }
