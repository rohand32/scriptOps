"""
APScheduler: fire enabled cron schedules by calling the same executor as manual runs.
"""
from __future__ import annotations

from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.models.schemas import TriggerType
from app.services.executor import create_job, execute_script
from app.services.schedule_store import all_schedules, init_db
from app.utils.logger import setup_logger

logger = setup_logger(__name__)

_scheduler: Optional[AsyncIOScheduler] = None


def _cron_to_trigger(expr: str) -> CronTrigger:
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError("cron_expr must have 5 fields: min hr dom mon dow")
    minute, hour, day, month, day_of_week = parts
    return CronTrigger(
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
    )


async def _run_scheduled(schedule_id: str) -> None:
    from app.services.schedule_store import get_schedule

    sc = get_schedule(schedule_id)
    if not sc or not sc.get("enabled"):
        return
    params = sc.get("params") or {}
    job = create_job(
        sc["script_id"],
        params,
        triggered_by="scheduler",
        trigger=TriggerType.cron,
        server_override=sc.get("server"),
        job_meta={
            "notify_on": sc.get("notify_on", "failure"),
            "schedule_id": schedule_id,
        },
    )
    await execute_script(job["job_id"])
    logger.info("Scheduled run finished: %s job=%s", schedule_id, job["job_id"])


def resync_jobs(sched: AsyncIOScheduler) -> None:
    init_db()
    for job in list(sched.get_jobs()):
        if job.id and str(job.id).startswith("so_"):
            sched.remove_job(job.id)

    schedules = all_schedules()
    for sid, sc in schedules.items():
        if not sc.get("enabled"):
            continue
        try:
            trig = _cron_to_trigger(sc["cron_expr"])
        except Exception as exc:
            logger.error("Invalid cron for %s: %s", sid, exc)
            continue
        job_id = f"so_{sid}"
        sched.add_job(
            _run_scheduled,
            trigger=trig,
            args=[sid],
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logger.debug("Registered schedule job %s (%s)", job_id, sc.get("cron_expr"))


async def start_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    _scheduler = AsyncIOScheduler()
    resync_jobs(_scheduler)
    _scheduler.start()
    logger.info("APScheduler started with cron schedules")
    return _scheduler


async def shutdown_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("APScheduler stopped")


def get_scheduler() -> Optional[AsyncIOScheduler]:
    return _scheduler
