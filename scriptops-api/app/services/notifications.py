"""
Optional post-job notifications (webhook). Configure via SCRIPTOPS_NOTIFY_WEBHOOK_URL.
Schedules pass notify_on via job['meta'].
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx

from app.models.schemas import JobStatus
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


async def maybe_notify_job(job: Dict[str, Any]) -> None:
    meta: Dict[str, Any] = job.get("meta") or {}
    notify_on = meta.get("notify_on", "never")
    if notify_on == "never":
        return

    status = job.get("status")
    success = status == JobStatus.success.value
    if notify_on == "failure" and success:
        return
    if notify_on not in ("always", "failure"):
        return

    url = os.environ.get("SCRIPTOPS_NOTIFY_WEBHOOK_URL")
    if not url:
        logger.debug("No SCRIPTOPS_NOTIFY_WEBHOOK_URL — skipping notification")
        return

    payload = {
        "job_id": job.get("job_id"),
        "script_id": job.get("script_id"),
        "script_name": job.get("script_name"),
        "status": status,
        "server": job.get("server"),
        "trigger": job.get("trigger"),
        "schedule_id": meta.get("schedule_id"),
        "error": job.get("error"),
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
            r.raise_for_status()
        logger.info("Notification sent for job %s", job.get("job_id"))
    except Exception as exc:
        logger.warning("Webhook notification failed: %s", exc)
