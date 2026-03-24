"""
ScriptOps — Auth Routes (API key management)
"""

from fastapi import APIRouter, Depends, HTTPException
from app.middleware.auth import require_role, get_current_user
from app.models.schemas import Role, TokenUser, APIKeyCreate, APIKeyResponse
from app.utils.logger import setup_logger
from datetime import datetime, timezone
import uuid, hashlib

logger = setup_logger(__name__)
router = APIRouter()

_KEYS = {}   # key_id -> record

@router.get("/me", summary="Current User Info")
async def whoami(user: TokenUser = Depends(get_current_user)):
    return user

@router.post("/keys", summary="Create API Key", response_model=APIKeyResponse, status_code=201)
async def create_key(
    req: APIKeyCreate,
    user: TokenUser = Depends(require_role(Role.admin)),
):
    raw  = f"sk_live_{uuid.uuid4().hex}"
    kid  = f"key_{uuid.uuid4().hex[:8]}"
    record = {
        "key_id": kid, "name": req.name, "key": raw,
        "scope": req.scope, "created_at": datetime.now(timezone.utc),
        "created_by": user.name,
    }
    _KEYS[kid] = record
    logger.info(f"API key '{req.name}' created by {user.name} with scope={req.scope}")
    return record

@router.delete("/keys/{key_id}", summary="Revoke API Key", status_code=204)
async def revoke_key(key_id: str, user: TokenUser = Depends(require_role(Role.admin))):
    if key_id not in _KEYS:
        raise HTTPException(404, detail={"error":"not_found"})
    del _KEYS[key_id]
    logger.info(f"API key {key_id} revoked by {user.name}")
    return None

@router.get("/keys", summary="List API Keys")
async def list_keys(user: TokenUser = Depends(require_role(Role.admin))):
    safe = [{k: v for k, v in rec.items() if k != "key"} for rec in _KEYS.values()]
    return {"items": safe, "total": len(safe)}
