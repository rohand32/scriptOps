"""
List registered scripts and run by script_id with validated params.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path

from app.middleware.auth import get_current_user
from app.models.schemas import (
    Role,
    TokenUser,
    JobRef,
    ScriptRunRequest,
    ScriptRunBatchRequest,
    ScriptRunBatchResponse,
    ScriptRunBatchItem,
    TriggerType,
)
from app.services.config_loader import get_server
from app.services.executor import SCRIPT_REGISTRY, create_job, execute_script
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
router = APIRouter()


def _validate_script_params(script: dict, params: Dict[str, Any]) -> None:
    allowed = set(script.get("allowed_params") or [])
    if not allowed:
        return
    unknown = set(params.keys()) - allowed
    if unknown:
        raise HTTPException(
            422,
            detail={
                "error": "invalid_params",
                "message": f"Params not in allowed_params for this script: {sorted(unknown)}",
                "allowed_params": sorted(allowed),
            },
        )


def _assert_server_registered(server_id: str) -> None:
    sid = (server_id or "").strip()
    if not sid:
        raise HTTPException(422, detail={"error": "invalid_server", "message": "server id is required"})
    if get_server(sid) is None:
        raise HTTPException(
            400,
            detail={
                "error": "unknown_server",
                "message": (
                    f"Server '{sid}' is not defined in the server registry "
                    "(servers.yaml / SCRIPTOPS_SERVERS_FILE)."
                ),
            },
        )


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

    params = req.params or {}
    _validate_script_params(script, params)
    if req.server:
        _assert_server_registered(req.server)

    job = create_job(
        script_id,
        params,
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


@router.post(
    "/{script_id}/run-batch",
    summary="Run script on multiple servers (per-target params)",
    response_model=ScriptRunBatchResponse,
    status_code=202,
    description="""
Queue one job per target. Each target specifies a **server** id (from `servers.yaml`) and its own **params** payload.

Use this from the dashboard or automation to fan out the same script across hosts (e.g. prod-01 vs prod-02) with different arguments per host.

Returns a `job_id` and SSE `stream_url` for **each** target (poll or stream separately).
""",
)
async def run_script_batch(
    req: ScriptRunBatchRequest,
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

    items: list[ScriptRunBatchItem] = []
    n = len(req.targets)
    for i, tgt in enumerate(req.targets):
        params = dict(tgt.params or {})
        _validate_script_params(script, params)
        _assert_server_registered(tgt.server)
        job = create_job(
            script_id,
            params,
            user.name,
            TriggerType.api,
            server_override=tgt.server.strip(),
            job_meta={"batch": True, "batch_index": i, "batch_total": n, "batch_server": tgt.server},
        )
        bg.add_task(execute_script, job["job_id"])
        jid = job["job_id"]
        items.append(
            ScriptRunBatchItem(
                server=tgt.server.strip(),
                job_id=jid,
                status=job["status"],
                message=f"Queued {script['name']} on {tgt.server.strip()}.",
                stream_url=f"/api/v1/executions/{jid}/stream",
                status_url=f"/api/v1/executions/{jid}",
            )
        )
        logger.info("[%s] batch[%s/%s] %s on %s by %s", jid, i + 1, n, script_id, tgt.server, user.name)

    return ScriptRunBatchResponse(script_id=script_id, total=len(items), items=items)
