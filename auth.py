"""
ScriptOps — Authentication & Authorization Middleware
"""
from __future__ import annotations
import hashlib, hmac
from typing import Optional
from fastapi import Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from app.models.schemas import TokenUser, Role
from app.utils.logger import setup_logger

logger = setup_logger(__name__)
PUBLIC_PATHS = {"/", "/health", "/docs", "/redoc", "/openapi.json"}
ROLE_RANK = {Role.viewer: 0, Role.operator: 1, Role.manager: 2, Role.admin: 3}

def _hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()

_KEY_STORE: dict[str, dict] = {
    _hash("sk_live_admin_demo"):    {"user_id":"usr_001","name":"Arjun Desai",  "email":"arjun@corp.internal", "role":"admin",    "key_name":"Admin Demo Key"},
    _hash("sk_live_manager_demo"):  {"user_id":"usr_002","name":"Priya Mehta",  "email":"priya@corp.internal", "role":"manager",  "key_name":"Manager Demo Key"},
    _hash("sk_live_operator_demo"): {"user_id":"usr_003","name":"Rahul Khanna", "email":"rahul@corp.internal", "role":"operator", "key_name":"Operator Demo Key"},
    _hash("sk_live_viewer_demo"):   {"user_id":"usr_004","name":"Sneha Joshi",  "email":"sneha@corp.internal", "role":"viewer",   "key_name":"Viewer Demo Key"},
}

def _lookup_key(raw_key: str) -> Optional[TokenUser]:
    target = _hash(raw_key)
    found = None
    for h, u in _KEY_STORE.items():
        if hmac.compare_digest(h, target):
            found = u
    return TokenUser(**found) if found else None

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in PUBLIC_PATHS or request.method == "OPTIONS":
            return await call_next(request)
        raw_key = (request.headers.get("X-ScriptOps-Key") or request.headers.get("Authorization","").removeprefix("Bearer ")).strip()
        if not raw_key:
            return JSONResponse(status_code=401, content={"error":"missing_api_key","message":"Provide your API key via X-ScriptOps-Key header."})
        user = _lookup_key(raw_key)
        if not user:
            return JSONResponse(status_code=401, content={"error":"invalid_api_key","message":"API key not recognised or has been revoked."})
        request.state.user = user
        return await call_next(request)

def get_current_user(request: Request) -> TokenUser:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

def require_role(*allowed_roles: Role):
    def dependency(user: TokenUser = Depends(get_current_user)) -> TokenUser:
        if user.role not in allowed_roles:
            raise HTTPException(status_code=403, detail={"error":"insufficient_permissions","message":f"Requires: {' or '.join(r.value for r in allowed_roles)}. Yours: {user.role.value}","your_role":user.role,"required_roles":[r.value for r in allowed_roles]})
        return user
    return dependency

def require_min_role(min_role: Role):
    def dependency(user: TokenUser = Depends(get_current_user)) -> TokenUser:
        if ROLE_RANK[user.role] < ROLE_RANK[min_role]:
            raise HTTPException(status_code=403, detail={"error":"insufficient_permissions","message":f"Min role: {min_role.value}. Yours: {user.role.value}"})
        return user
    return dependency
