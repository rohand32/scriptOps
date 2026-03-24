"""
ScriptOps Internal Automation API
FastAPI application entry point
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import time

from app.routes import reports, cron, database, executions, auth, scripts
from app.middleware.auth import AuthMiddleware
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


def _cors_origins() -> list[str]:
    raw = os.environ.get(
        "SCRIPTOPS_CORS_ORIGINS",
        "https://scriptops.internal,http://localhost:3000,http://localhost:8080,http://127.0.0.1:5500,http://127.0.0.1:8080",
    )
    return [o.strip() for o in raw.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.services.schedule_store import init_db
    from app.services.scheduler_worker import shutdown_scheduler, start_scheduler

    init_db()
    await start_scheduler()
    yield
    await shutdown_scheduler()


app = FastAPI(
    title="ScriptOps Internal API",
    description="""
## ScriptOps — Internal Automation Platform API

Provides authenticated endpoints to:
- **Execute report generation scripts** (Python, role: Manager+)
- **Trigger cron jobs manually** (role: Admin/Manager)
- **Run database operations** (INSERT/UPDATE: Admin only, SELECT: Manager+)
- **Stream real-time execution output** via SSE
- **View execution history and logs**
- **List scripts and run by script_id** (`/api/v1/scripts`)

### Authentication
All endpoints require an `X-ScriptOps-Key` header with a valid API key.
Keys are scoped to a role; actions are enforced against that role.

### Role Hierarchy
- `viewer`   — read-only access (logs, status)
- `operator` — shell/server scripts only
- `manager`  — reports + DB read + scheduling
- `admin`    — full access including DB INSERT/UPDATE
""",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── AUTH MIDDLEWARE ────────────────────────────────────────────────────────────
app.add_middleware(AuthMiddleware)

# ── REQUEST TIMING ────────────────────────────────────────────────────────────
@app.middleware("http")
async def add_timing(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = round((time.perf_counter() - start) * 1000, 2)
    response.headers["X-Response-Time"] = f"{duration}ms"
    return response

# ── ROUTES ────────────────────────────────────────────────────────────────────
app.include_router(auth.router,       prefix="/api/v1/auth",       tags=["Auth"])
app.include_router(scripts.router,   prefix="/api/v1/scripts",    tags=["Scripts"])
app.include_router(reports.router,    prefix="/api/v1/reports",    tags=["Reports"])
app.include_router(cron.router,       prefix="/api/v1/cron",       tags=["Cron Jobs"])
app.include_router(database.router,   prefix="/api/v1/database",   tags=["Database"])
app.include_router(executions.router, prefix="/api/v1/executions", tags=["Executions"])

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "version": "2.0.0", "service": "scriptops-api"}

@app.get("/", tags=["System"])
async def root():
    return {
        "service": "ScriptOps API",
        "version": "2.0.0",
        "docs": "/docs",
        "health": "/health",
    }

# ── GLOBAL ERROR HANDLER ──────────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    logger.error(f"Unhandled error on {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "message": "An unexpected error occurred."},
    )
