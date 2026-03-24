# ScriptOps deployment notes

This document covers hosting the **dashboard** (static HTML) and the **API** (FastAPI) on different origins, browser limitations for **SSE** with API keys, **CORS**, and **secrets**.

## UI host vs API host

- Serve **`scriptops-dashboard.html`** from any static host (object storage + CDN, Nginx `root`, GitHub Pages, etc.). It is a single file that calls the API using a configurable base URL and `X-ScriptOps-Key`.
- Run the API from **`scriptops-api/`** with Uvicorn/Gunicorn behind a reverse proxy (TLS termination at the proxy is recommended).

Set the dashboard’s **API base URL** at login (for example `https://api.scriptops.internal`). Do not commit real URLs or keys into the repository.

## CORS

The API enables `CORSMiddleware` with origins from **`SCRIPTOPS_CORS_ORIGINS`** (comma-separated). Include the **exact** origin of the dashboard, including scheme and port, for example:

```bash
export SCRIPTOPS_CORS_ORIGINS="https://dashboard.scriptops.internal,http://localhost:5500"
```

The default in code includes common local dev ports; production should list only trusted UI origins.

## SSE and the `X-ScriptOps-Key` header

`EventSource` in browsers **cannot** set custom headers, so you cannot send `X-ScriptOps-Key` on a native `EventSource` request.

**Options:**

1. **Fetch + ReadableStream (what the dashboard uses for live runs)**  
   After `POST /api/v1/scripts/{id}/run`, the UI uses `fetch()` to `GET /api/v1/executions/{job_id}/stream` with `X-ScriptOps-Key` and reads the SSE body in JavaScript. This works cross-origin when CORS allows the UI origin.

2. **Same-origin reverse proxy**  
   Put the UI and API behind one host (or path-prefix the API) and configure the proxy to inject the API key from an **HttpOnly cookie** or server-side session into `X-ScriptOps-Key` for upstream requests. The browser then calls same-origin URLs without custom headers on `EventSource` if you choose to switch to it.

3. **Short-lived query token (only with TLS and tight scope)**  
   Issue a narrow token valid only for `GET .../stream` and validate it in the API. Prefer (1) or (2) unless you have a strong operational reason.

Ensure buffering is disabled for SSE at the proxy (`X-Accel-Buffering: no` for Nginx; similar for other proxies) so lines flush immediately.

## Configuration and secrets

| Concern | Mechanism |
|--------|-----------|
| Server list / script registry | YAML under `scriptops-api/config/` or paths via `SCRIPTOPS_CONFIG_DIR`, `SCRIPTOPS_SERVERS_FILE`, `SCRIPTOPS_SCRIPTS_FILE` |
| SSH private keys | Paths in YAML (e.g. `ssh_key_path`); store keys outside the repo and restrict file permissions on the API host |
| API keys | In-memory demo store in development; production should use a real store and rotation |
| Schedule DB | `SCRIPTOPS_SCHEDULES_DB` or default SQLite under `scriptops-api/data/` |
| Outbound notifications | `SCRIPTOPS_NOTIFY_WEBHOOK_URL` (optional) |

## Rotating secrets

- **API keys**: Revoke and reissue via `DELETE` / `POST` on `/api/v1/auth/keys` (see OpenAPI). Update clients and dashboards; old keys should fail fast after revocation.
- **SSH keys**: Replace key files on disk (or update paths in server config), then reload/restart the API if you add key hot-reload; otherwise restart after rotation.
- **Webhook URLs**: Rotate `SCRIPTOPS_NOTIFY_WEBHOOK_URL` in the deployment environment and redeploy.

## Suggested production command

From `scriptops-api/`:

```bash
gunicorn -k uvicorn.workers.UvicornWorker app.main:app -b 0.0.0.0:8000 --workers 2
```

Tune workers and timeouts for long-running script streams.
