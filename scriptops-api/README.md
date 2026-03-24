# ScriptOps Internal API

**FastAPI backend for the ScriptOps Internal Automation Platform.**  
Authenticated REST + SSE endpoints to execute report scripts, trigger cron jobs manually, and run database operations — with role-based access control enforced on every route.

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Interactive docs
open http://localhost:8000/docs
```

---

## Authentication

Every request (except `/health`, `/docs`) requires an API key:

```
X-ScriptOps-Key: sk_live_<your_key>
```

**Demo keys (development only):**

| Key | Role |
|-----|------|
| `sk_live_admin_demo`    | Admin    |
| `sk_live_manager_demo`  | Manager  |
| `sk_live_operator_demo` | Operator |
| `sk_live_viewer_demo`   | Viewer   |

---

## Role Matrix

| Action | Admin | Manager | Operator | Viewer |
|--------|:-----:|:-------:|:--------:|:------:|
| View scripts & logs | ✓ | ✓ | ✓ | ✓ |
| Run report scripts | ✓ | ✓ | — | — |
| Run shell / server scripts | ✓ | — | ✓ | — |
| DB SELECT / export | ✓ | ✓ | — | — |
| DB INSERT | ✓ only | — | — | — |
| DB UPDATE | ✓ only | — | — | — |
| Trigger cron manually | ✓ | ✓ | — | — |
| Create / edit schedules | ✓ | — | — | — |
| Manage users & API keys | ✓ | — | — | — |

---

## API Reference

### Reports  `POST /api/v1/reports/...`  *(Manager+)*

All report endpoints return `202 Accepted` with a `JobRef`:
```json
{
  "job_id":     "J1A2B3",
  "status":     "pending",
  "message":    "Job queued.",
  "stream_url": "/api/v1/executions/J1A2B3/stream",
  "status_url": "/api/v1/executions/J1A2B3"
}
```

#### `POST /api/v1/reports/sales`
Runs `generate_sales_report.py` on `prod-01`.

```json
{
  "date_from":       "2024-06-01",
  "date_to":         "2024-06-30",
  "format":          "csv",
  "region":          "south_asia",
  "include_returns": true,
  "group_by":        "month",
  "send_email":      false,
  "recipients":      []
}
```

#### `POST /api/v1/reports/inventory-sync`
Runs `sync_inventory.py` on `prod-01`.

```json
{
  "server":         "prod-01",
  "dry_run":        false,
  "batch_size":     500,
  "skip_null_skus": true
}
```

#### `POST /api/v1/reports/user-activity`
Runs `user_activity_report.py` on `prod-02`.

```json
{
  "date_from":          "2024-06-01",
  "date_to":            "2024-06-30",
  "format":             "csv",
  "user_segment":       "premium",
  "include_anonymous":  false
}
```

#### `POST /api/v1/reports/finance-reconcile`
Runs `finance_reconcile.py` on `prod-01`.

```json
{
  "date_from":                  "2024-06-01",
  "date_to":                    "2024-06-30",
  "gateway":                    "razorpay",
  "auto_flag_discrepancies":    true
}
```

#### `POST /api/v1/reports/weekly-digest`
Runs `weekly_digest.py` on `prod-01`.

```json
{
  "week_offset": 0,
  "format":      "pdf"
}
```

#### `GET /api/v1/reports/history`
Returns history of report jobs for the calling user (Admin sees all).

---

### Cron / Scheduler  `GET|POST /api/v1/cron/...`

#### `GET /api/v1/cron/schedules` *(Manager+)*
List all registered schedules.

```
GET /api/v1/cron/schedules?enabled=true
```

#### `POST /api/v1/cron/schedules` *(Admin)*
Create a new schedule.

```json
{
  "script_id":   "gen_sales",
  "cron_expr":   "0 8 * * 1-5",
  "server":      "prod-01",
  "enabled":     true,
  "notify_on":   "failure",
  "description": "Daily sales report, Mon–Fri"
}
```

#### `PATCH /api/v1/cron/schedules/{schedule_id}` *(Admin)*
Update any field of an existing schedule.

```json
{
  "cron_expr": "0 9 * * 1-5",
  "enabled":   true
}
```

#### `POST /api/v1/cron/schedules/{schedule_id}/toggle` *(Manager+)*
Enable or disable a schedule with a single call.

#### `POST /api/v1/cron/schedules/{schedule_id}/trigger` *(Manager+)*
**Manually trigger** a scheduled job immediately.

```json
{
  "params": { "month": "2024-06" },
  "server": "prod-01",
  "reason": "Month-end close — triggering ahead of schedule"
}
```

Response includes `stream_url` for live output via SSE.

#### `POST /api/v1/cron/run/{script_id}` *(Admin)*
Directly trigger any cron-category script by ID (`sched_sales`, `sched_clean`, `weekly_digest`).

#### `GET /api/v1/cron/schedules/{schedule_id}/history`
Last N runs for a specific schedule.

#### `DELETE /api/v1/cron/schedules/{schedule_id}` *(Admin)*
Permanently delete a schedule.

---

### Database  `POST /api/v1/database/...`

#### `POST /api/v1/database/select` *(Manager+)*
Run a pre-approved read-only export script.

```json
{
  "script_id": "db_export",
  "params":    { "table": "public.products", "limit": 1000 },
  "format":    "json"
}
```

#### `POST /api/v1/database/insert` *(Admin only)*
Bulk insert from staging table or file.

```json
{
  "script_id":        "db_insert_records",
  "source_table":     "staging.new_products",
  "target_table":     "public.products",
  "batch_size":       500,
  "on_conflict":      "abort",
  "dry_run":          false,
  "require_approval": true
}
```

> If `require_approval: true` and `dry_run: false`, a **pending approval** record is created.  
> A *second* admin must call `POST /api/v1/database/approvals/{id}/approve`.

#### `POST /api/v1/database/update` *(Admin only)*
Execute a pre-approved UPDATE script.

```json
{
  "script_id":        "db_update_prices",
  "params":           { "price_sheet": "/data/pricing/june2024.csv", "effective_date": "2024-06-01" },
  "dry_run":          false,
  "require_approval": true,
  "change_reason":    "Q2 pricing update approved by CFO on 2024-05-28"
}
```

After a successful committed UPDATE, a `rollback_token` is returned in the job result.

#### `GET /api/v1/database/approvals` *(Admin)*
List all pending write approvals.

#### `POST /api/v1/database/approvals/{approval_id}/approve` *(Admin — different from requestor)*
Approve a pending INSERT or UPDATE. Self-approval is blocked (403).

#### `POST /api/v1/database/approvals/{approval_id}/reject` *(Admin)*
Reject a pending write with a mandatory reason.

#### `POST /api/v1/database/rollback/{rollback_token}` *(Admin)*
Reverse a committed INSERT or UPDATE.

```
POST /api/v1/database/rollback/RBK-A1B2C3D4?reason=Incorrect+pricing+data
```

#### `GET /api/v1/database/audit` *(Admin)*
Full audit trail of all DB operations with pagination.

```
GET /api/v1/database/audit?operation=insert&page=1&limit=20
```

---

### Executions  `GET|POST /api/v1/executions/...`

#### `GET /api/v1/executions/` *(All roles)*
Paginated execution history. Non-admins see only their own jobs.

```
GET /api/v1/executions/?status=failed&limit=20&page=1
```

#### `GET /api/v1/executions/{job_id}` *(All roles)*
Full job status including `rows_affected`, `output_path`, `warnings`, `audit_id`.

#### `GET /api/v1/executions/{job_id}/stream` *(All roles — SSE)*
Real-time output stream. Connect with `EventSource` or `curl -N`:

```bash
curl -N \
  -H "X-ScriptOps-Key: sk_live_manager_demo" \
  http://localhost:8000/api/v1/executions/J1A2B3/stream
```

SSE events:

| Event | Payload |
|-------|---------|
| `connected` | `{ job_id }` |
| `output` | `{ ts, level, text }` — one line of script stdout/stderr |
| `done` | `{ status, exit_code, duration_ms, rows_affected }` |
| `error` | `{ message }` |
| `timeout` | stream closed after 10 min |

#### `GET /api/v1/executions/{job_id}/logs`
Full captured output log. Filter by `?level=err` or `?level=warn`.

#### `POST /api/v1/executions/{job_id}/cancel`
Cancel a running or pending job.

```json
{ "reason": "Triggered by mistake" }
```

---

### Auth  `/api/v1/auth/...`

#### `GET /api/v1/auth/me`
Returns the identity and role of the current API key.

#### `POST /api/v1/auth/keys` *(Admin)*
Generate a new API key with a scoped role.

```json
{ "name": "ETL Pipeline", "scope": "operator" }
```

Response contains the full key — **shown once only**.

#### `DELETE /api/v1/auth/keys/{key_id}` *(Admin)*
Revoke an API key immediately.

---

## Error Responses

All errors follow a consistent shape:

```json
{
  "error":   "insufficient_permissions",
  "message": "Requires role: admin. Your role: manager",
  "your_role":      "manager",
  "required_roles": ["admin"]
}
```

| Status | Error key |
|--------|-----------|
| 401 | `missing_api_key` / `invalid_api_key` |
| 403 | `insufficient_permissions` / `self_approval_forbidden` |
| 404 | `not_found` |
| 409 | `not_cancellable` / `already_processed` |
| 422 | Pydantic validation error |
| 500 | `internal_server_error` |

---

## Project Structure

```
scriptops-api/
├── app/
│   ├── main.py                  # FastAPI app, middleware wiring, router mounts
│   ├── models/
│   │   └── schemas.py           # All Pydantic request/response models + enums
│   ├── middleware/
│   │   └── auth.py              # API key validation, role dependency factories
│   ├── services/
│   │   └── executor.py          # Script registry, job store, SSH runner, SSE generator
│   ├── routes/
│   │   ├── auth.py              # Key management
│   │   ├── reports.py           # 5 report endpoints
│   │   ├── cron.py              # Schedule CRUD + manual trigger
│   │   ├── database.py          # SELECT / INSERT / UPDATE + approval flow + audit
│   │   └── executions.py        # Status polling, SSE stream, logs, cancel
│   └── utils/
│       └── logger.py            # Structured logging setup
├── tests/
│   └── test_api.py              # 57 unit tests (stdlib unittest, no deps)
├── requirements.txt
└── README.md
```

---

## Running Tests

```bash
# No external dependencies needed
python3 -m unittest tests.test_api -v

# Or directly
python3 tests/test_api.py
```

**57 tests, 10 test classes:**

| Class | What it covers |
|-------|---------------|
| `TestApiKeyHashing` | SHA-256 hashing, constant-time comparison |
| `TestRoleHierarchy` | All 4 roles × all action types |
| `TestCronHumanReadable` | Cron expression → English parser |
| `TestJobLifecycle` | pending → running → success/failed/cancelled |
| `TestApprovalFlow` | Two-admin gate, self-approval block, state machine |
| `TestAuditLog` | Audit record creation, dry-run logic, rollback flags |
| `TestCommandBuilding` | CLI arg construction, injection prevention |
| `TestSSE` | SSE message format, JSON validity, Unicode |
| `TestSchemaValidation` | Date formats, cron fields, batch bounds |
| `TestRollbackToken` | Token format, uniqueness, dry-run suppression |

---

## Production Checklist

- Replace `_KEY_STORE` dict with database-backed key store (PostgreSQL + bcrypt)
- Replace `_JOBS` / `_SCHEDULES` dicts with Redis or a proper job queue (Celery, ARQ)
- Implement real SSH execution via `asyncssh` in `executor.py`
- Add rate limiting (e.g. `slowapi`) on execution endpoints
- Enable HTTPS / TLS termination at the reverse proxy
- Set `CORS` `allow_origins` to your actual internal domain only
- Wire up real email notifications via SMTP/SES on job completion
- Add Prometheus metrics middleware (`/metrics`)
- Deploy with `gunicorn -k uvicorn.workers.UvicornWorker app.main:app`
