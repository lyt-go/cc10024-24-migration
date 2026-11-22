"""运行存储迁移接口：状态、回填、切换、回滚、兼容读取与事件对照。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from app import run_store
from app.services.run_history import RunHistoryService

router = APIRouter(prefix="/api/run-store", tags=["运行存储迁移"])


@router.get("/status")
def status() -> dict[str, Any]:
    return run_store.migration_status()


@router.post("/migrate/start")
def start() -> dict[str, Any]:
    return run_store.start_migration()


@router.post("/migrate/backfill")
def backfill(
    limit: int = Query(default=10, ge=1, le=5000),
    interrupt_at_journal: str | None = Query(default=None),
) -> dict[str, Any]:
    return run_store.backfill_step(
        limit, interrupt_at_journal=interrupt_at_journal)


@router.post("/migrate/interrupt")
def interrupt(reason: str = Query(default="operator")) -> dict[str, Any]:
    return run_store.interrupt_migration(reason)


@router.post("/migrate/resume")
def resume() -> dict[str, Any]:
    """中断/失败后重新启动：游标不动，从断点继续。"""
    return run_store.resume_migration()


@router.post("/migrate/finish")
def finish() -> dict[str, Any]:
    return run_store.finish_migration()


@router.post("/migrate/rollback")
def rollback() -> dict[str, Any]:
    return run_store.rollback_migration()


@router.get("/events")
def events() -> dict[str, Any]:
    """兼容读取：新旧部署都能用同一份可读事件，且同一事件只出现一次。"""
    return run_store.visible_events()


@router.get("/recent-runs")
def recent_runs(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
    return {"items": RunHistoryService().get_recent_runs(limit)}


@router.get("/ledger")
def ledger() -> dict[str, Any]:
    """运行历史事件对照：新录入事件与既有 JSONL 来源位置并列。"""
    return run_store.run_history_ledger()


@router.get("/verify")
def verify() -> dict[str, Any]:
    return run_store.verify_migration()


@router.get("/compare")
def compare() -> dict[str, Any]:
    """迁移前 / 进行中 / 完成后的可读事件与各来源计数。"""
    return run_store.compare()
