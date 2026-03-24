"""
ScriptOps — Pydantic models / request-response schemas
"""

from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator, EmailStr
import uuid


# ── ENUMS ─────────────────────────────────────────────────────────────────────

class Role(str, Enum):
    viewer   = "viewer"
    operator = "operator"
    manager  = "manager"
    admin    = "admin"

class JobStatus(str, Enum):
    pending  = "pending"
    running  = "running"
    success  = "success"
    failed   = "failed"
    cancelled= "cancelled"

class TriggerType(str, Enum):
    manual    = "manual"
    cron      = "cron"
    api       = "api"
    webhook   = "webhook"

class ScriptType(str, Enum):
    python = "python"
    shell  = "shell"

class ScriptCategory(str, Enum):
    report   = "report"
    database = "database"
    shell    = "shell"
    cron     = "cron"

class DBOperation(str, Enum):
    select = "select"
    insert = "insert"
    update = "update"

class OutputFormat(str, Enum):
    csv   = "csv"
    json  = "json"
    xlsx  = "xlsx"
    pdf   = "pdf"

class NotifyOn(str, Enum):
    always  = "always"
    failure = "failure"
    never   = "never"


# ── AUTH ──────────────────────────────────────────────────────────────────────

class TokenUser(BaseModel):
    user_id:   str
    name:      str
    email:     str
    role:      Role
    key_name:  str

class APIKeyCreate(BaseModel):
    name:  str = Field(..., min_length=2, max_length=80, example="ETL Pipeline")
    scope: Role = Field(Role.operator, example="operator")

class APIKeyResponse(BaseModel):
    key_id:     str
    name:       str
    key:        str = Field(..., description="Full key — shown once only")
    scope:      Role
    created_at: datetime
    created_by: str


# ── COMMON ────────────────────────────────────────────────────────────────────

class JobRef(BaseModel):
    job_id:     str = Field(example="J0047")
    status:     JobStatus
    message:    str
    stream_url: str = Field(example="/api/v1/executions/J0047/stream")
    status_url: str = Field(example="/api/v1/executions/J0047")

class Pagination(BaseModel):
    page:       int = Field(1, ge=1)
    page_size:  int = Field(20, ge=1, le=100)
    total:      int
    total_pages:int

class ErrorResponse(BaseModel):
    error:   str
    message: str
    detail:  Optional[Any] = None


# ── REPORTS ───────────────────────────────────────────────────────────────────

class ReportParams(BaseModel):
    """Common parameters for all report scripts."""
    date_from:  Optional[str] = Field(None,    example="2024-06-01",
                                       description="ISO date, inclusive start")
    date_to:    Optional[str] = Field(None,    example="2024-06-30",
                                       description="ISO date, inclusive end")
    format:     OutputFormat  = Field(OutputFormat.csv, example="csv")
    send_email: bool          = Field(False,   description="Email output to distribution list")
    recipients: List[EmailStr]= Field([],      description="Override recipient list")

    @validator("date_from", "date_to", pre=True, always=True)
    def validate_date(cls, v):
        if v:
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise ValueError("Date must be ISO format YYYY-MM-DD")
        return v

class SalesReportRequest(ReportParams):
    region:         Optional[str]        = Field(None,  example="south_asia")
    include_returns:bool                 = Field(True)
    group_by:       Literal["day","week","month"] = Field("month")

class InventorySyncRequest(BaseModel):
    server:         str          = Field("prod-01", example="prod-01")
    dry_run:        bool         = Field(False, description="Validate without writing")
    batch_size:     int          = Field(500,  ge=1, le=5000)
    skip_null_skus: bool         = Field(True)

class UserActivityRequest(ReportParams):
    user_segment: Optional[str] = Field(None, example="premium")
    include_anonymous: bool     = Field(False)

class FinanceReconcileRequest(ReportParams):
    gateway:      Optional[str] = Field(None, example="razorpay")
    auto_flag_discrepancies: bool = Field(True)

class WeeklyDigestRequest(BaseModel):
    week_offset: int  = Field(0, ge=0, le=12,
                              description="0 = current week, 1 = last week, ...")
    format:      OutputFormat = Field(OutputFormat.pdf)

class ReportResult(BaseModel):
    job_id:       str
    script:       str
    status:       JobStatus
    output_path:  Optional[str] = None
    output_url:   Optional[str] = None
    row_count:    Optional[int] = None
    file_size_kb: Optional[int] = None
    warnings:     List[str]     = []
    duration_ms:  Optional[int] = None
    completed_at: Optional[datetime] = None


# ── CRON / SCHEDULER ─────────────────────────────────────────────────────────

class CronTriggerRequest(BaseModel):
    """Manually trigger a scheduled job without waiting for its cron time."""
    params:      Dict[str, Any] = Field({},
                     description="Override parameters for this run only")
    server:      Optional[str] = Field(None,
                     description="Override the default server for this run")
    notify_on:   NotifyOn      = Field(NotifyOn.failure)
    reason:      Optional[str] = Field(None, max_length=200,
                     example="Month-end close — triggering ahead of schedule")

class ScheduleCreateRequest(BaseModel):
    script_id:   str        = Field(...,  example="gen_sales")
    cron_expr:   str        = Field(...,  example="0 8 * * 1-5",
                                description="Standard 5-field cron expression")
    server:      str        = Field("prod-01", example="prod-01")
    params:      Dict[str, Any] = Field({})
    enabled:     bool       = Field(True)
    notify_on:   NotifyOn   = Field(NotifyOn.failure)
    description: Optional[str] = Field(None, max_length=300)

    @validator("cron_expr")
    def validate_cron(cls, v):
        parts = v.strip().split()
        if len(parts) != 5:
            raise ValueError("cron_expr must have exactly 5 fields: min hr dom mon dow")
        return v

class ScheduleUpdateRequest(BaseModel):
    cron_expr:   Optional[str]          = None
    server:      Optional[str]          = None
    params:      Optional[Dict[str,Any]]= None
    enabled:     Optional[bool]         = None
    notify_on:   Optional[NotifyOn]     = None
    description: Optional[str]          = None

class ScheduleResponse(BaseModel):
    schedule_id: str
    script_id:   str
    script_name: str
    cron_expr:   str
    human_readable: str
    server:      str
    enabled:     bool
    notify_on:   NotifyOn
    last_run:    Optional[datetime] = None
    next_run:    Optional[datetime] = None
    last_status: Optional[JobStatus]= None
    created_by:  str
    created_at:  datetime


# ── DATABASE ─────────────────────────────────────────────────────────────────

class DBSelectRequest(BaseModel):
    script_id: str = Field(..., example="db_export_snapshot",
                            description="Pre-approved read-only script ID")
    params:    Dict[str, Any] = Field({}, example={"table": "products", "limit": 1000})
    format:    OutputFormat   = Field(OutputFormat.json)

class DBInsertRequest(BaseModel):
    """
    Bulk insert from a pre-approved staging table or file path.
    Requires Admin role.
    """
    script_id:      str  = Field(..., example="db_insert_records",
                                  description="Must be a registered DB insert script")
    source_table:   Optional[str] = Field(None, example="staging.new_products",
                                          description="Staging table to read from")
    source_file:    Optional[str] = Field(None, example="/data/staging/products.csv",
                                          description="CSV/JSON file path on the server")
    target_table:   str  = Field(..., example="public.products")
    batch_size:     int  = Field(500, ge=1, le=10000)
    on_conflict:    Literal["abort","skip","replace"] = Field("abort")
    dry_run:        bool = Field(False, description="Validate rows without committing")
    require_approval: bool = Field(True,
                                   description="If True, creates a pending job awaiting 2nd admin approval")

    @validator("source_table", "source_file", always=True)
    def source_required(cls, v, values):
        if not v and not values.get("source_file") and not values.get("source_table"):
            pass  # checked together at model level
        return v

class DBUpdateRequest(BaseModel):
    """
    Execute a pre-approved UPDATE script. Requires Admin role.
    All updates are transactional and logged in the audit table.
    """
    script_id:      str  = Field(..., example="db_update_prices")
    params:         Dict[str, Any] = Field({},
                        example={"price_sheet": "/data/pricing/june2024.csv",
                                 "effective_date": "2024-06-01"})
    dry_run:        bool = Field(False)
    require_approval: bool = Field(True)
    change_reason:  str  = Field(..., min_length=10, max_length=500,
                                  example="Q2 pricing update approved by CFO on 2024-05-28")

class DBAuditLog(BaseModel):
    audit_id:       str
    job_id:         str
    operation:      DBOperation
    script_id:      str
    table_name:     str
    rows_affected:  int
    dry_run:        bool
    approved_by:    Optional[str] = None
    executed_by:    str
    change_reason:  Optional[str] = None
    executed_at:    datetime
    rollback_available: bool

class DBJobResult(BaseModel):
    job_id:         str
    status:         JobStatus
    operation:      DBOperation
    script_id:      str
    dry_run:        bool
    rows_affected:  Optional[int]   = None
    rows_skipped:   Optional[int]   = None
    errors:         List[str]       = []
    warnings:       List[str]       = []
    audit_id:       Optional[str]   = None
    rollback_token: Optional[str]   = None
    duration_ms:    Optional[int]   = None
    completed_at:   Optional[datetime] = None


# ── EXECUTIONS ───────────────────────────────────────────────────────────────

class ExecutionListItem(BaseModel):
    job_id:       str
    script_id:    str
    script_name:  str
    server:       str
    trigger:      TriggerType
    triggered_by: str
    status:       JobStatus
    duration_ms:  Optional[int] = None
    created_at:   datetime
    completed_at: Optional[datetime] = None

class ExecutionDetail(ExecutionListItem):
    params:       Dict[str, Any]
    exit_code:    Optional[int]      = None
    output_path:  Optional[str]      = None
    log_lines:    int                = 0
    error:        Optional[str]      = None
    warnings:     List[str]          = []

class ExecutionListResponse(BaseModel):
    items:      List[ExecutionListItem]
    pagination: Pagination

class CancelRequest(BaseModel):
    reason: Optional[str] = Field(None, max_length=300)


# ── SCRIPTS (generic list / run) ─────────────────────────────────────────────

class ScriptRunRequest(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict, description="Script parameters (validated against allowed_params)")
    server: Optional[str] = Field(None, description="Override default server id from registry")
