"""运行存储迁移集成测试：JSONL → SQLite 的兼容读取、事件对照与中断重启。

场景覆盖：
- 迁移前 / 进行中 / 完成后各阶段的可读事件与来源计数对照
- 同一个事件在 JSONL 与 SQLite 各有一份时只读一次
- 存量行缺失列不补默认值（source 计为 unknown）
- 批次确认登记真实回执并绑定唯一确认来源
- 回填一部分后触发批次完成、打断迁移、重新启动后从游标续跑
- 失败与回滚期间查询不重复、来源计数一致
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

DATA_ROOT = os.path.normpath(os.path.join(backend_dir, "..", "data"))
SQLITE_PATH = os.path.join(DATA_ROOT, "run_store.sqlite3")


def _reset():
    """清空运行数据与迁移库，得到一次干净的新部署基线。"""
    for sub in ("runs", "snapshots", "reports", "exports"):
        directory = os.path.join(DATA_ROOT, sub)
        if os.path.isdir(directory):
            shutil.rmtree(directory)
    for suffix in ("", "-wal", "-shm"):
        path = SQLITE_PATH + suffix
        if os.path.exists(path):
            os.remove(path)


def _seed_legacy_logs():
    """模拟旧部署留下的 JSONL：只有业务字段，没有迁移库。"""
    from app import persistence as persist

    lines = {
        "batch_queue.jsonl": [
            {"module": "batch_queue", "entity": "采集批次", "entry_id": 1,
             "status": "排队中", "version": "v1", "action": "create",
             "timestamp": "2026-10-01T08:00:00", "abnormal": False},
            {"module": "batch_queue", "entity": "采集批次", "entry_id": 1,
             "status": "已完成", "version": "v2", "action": "确认完成",
             "timestamp": "2026-10-01T09:00:00", "abnormal": False},
        ],
        "error_recover.jsonl": [
            {"module": "error_recover", "entity": "恢复任务", "entry_id": 1,
             "status": "待处理", "version": "v1", "action": "create",
             "timestamp": "2026-10-01T08:10:00", "abnormal": False},
        ],
        "run_history.jsonl": [
            # 旧格式：缺少 source_module / source_id，必须原样保留为 NULL
            {"module": "run_history", "entity": "运行记录", "entry_id": 1,
             "status": "已完成", "version": "", "action": "auto_create_from_batch",
             "timestamp": "2026-10-01T09:00:00", "abnormal": False},
            {"module": "run_history", "entity": "运行记录", "entry_id": 2,
             "status": "运行中", "version": "", "action": "auto_create_from_recovery",
             "timestamp": "2026-10-01T09:30:00", "abnormal": False},
        ],
    }
    for filename, records in lines.items():
        for record in records:
            persist.append_jsonl("runs", filename, record)


def _new_batch_confirm(code: str, plan: str, tasks: int):
    from app.services.batch_queue import BatchQueueService

    service = BatchQueueService()
    entry, missing = service.create_entry({
        "批次编号": code, "关联计划": plan, "任务数量": tasks,
    })
    assert entry is not None, missing
    result, message = service.run_action(entry["id"], "确认完成")
    assert result is not None, message
    return result


def _new_recovery(task_code: str, plan: str):
    from app.services.error_recover import ErrorRecoverService

    service = ErrorRecoverService()
    entry, missing = service.create_entry({
        "任务编号": task_code, "关联计划": plan, "失败步骤": "data_extract",
    })
    assert entry is not None, missing
    result, message = service.run_action(entry["id"], "确认恢复")
    assert result is not None, message
    return result


def test_stage_counts_and_dedup():
    """迁移前/中/后：可读事件数不重复，来源计数可解释。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    before = run_store.visible_events()
    assert before["status"] == "idle"
    assert before["read_mode"] == "jsonl_only"
    assert before["duplicates_suppressed"] == 0
    jsonl_total = before["count"]
    assert jsonl_total == 5
    # 旧格式缺少来源列 → 计入 (unknown)，不臆造来源
    assert before["source_counts"][run_store.UNKNOWN_SOURCE] == 5

    run_store.start_migration()
    step = run_store.backfill_step(3)
    assert step["appended"] == 3
    mid = run_store.visible_events()
    assert mid["status"] == run_store.STATUS_BACKFILLING
    assert mid["read_mode"] == "hybrid"
    # 库里的 3 条与 JSONL 的 3 条是同一批事件，只算一次
    assert mid["count"] == jsonl_total
    assert mid["duplicates_suppressed"] == 3
    assert mid["store_counts"]["sqlite"] == 3
    assert mid["store_counts"]["jsonl"] == jsonl_total - 3

    while True:
        result = run_store.backfill_step(50)
        if not result["remaining"]:
            break
    assert run_store.migration_status()["status"] == run_store.STATUS_SWITCH_PENDING
    pending = run_store.visible_events()
    assert pending["count"] == jsonl_total
    assert pending["duplicates_suppressed"] == jsonl_total

    final = run_store.finish_migration()
    assert final["status"] == run_store.STATUS_COMPLETED
    assert final["verification"]["ok"] is True
    done = run_store.visible_events()
    assert done["read_mode"] == "sqlite_primary"
    assert done["count"] == jsonl_total
    assert done["source_counts"] == before["source_counts"]


def test_legacy_missing_columns_stay_null():
    """存量列缺失不填默认值：source_module/source_id 在库内必须是 NULL。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    run_store.start_migration()
    while run_store.backfill_step(50)["remaining"]:
        pass
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT source_module, source_id, origin, legacy_origin FROM run_events"
        ).fetchall()
        assert rows, "回填后应存在事件"
        for row in rows:
            assert row["origin"] == "jsonl_backfill"
            assert row["legacy_origin"], "存量事件必须带来源位置"
            assert row["source_module"] is None
            assert row["source_id"] is None
    finally:
        conn.close()


def test_batch_receipts_and_unique_confirm_source():
    """批次确认登记真实回执；迁移中到达的事件可追到唯一确认来源。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    run_store.start_migration()
    run_store.backfill_step(2)  # 只回填一部分旧历史

    _new_batch_confirm("BATCH-LIVE-1", "PLAN-LIVE-1", 9)
    _new_recovery("RECOVER-LIVE-1", "PLAN-LIVE-1")

    status = run_store.migration_status()
    # live 侧包含批次/恢复写出的全部运行日志事件，均未被回填重复插入
    assert status["counts"]["live"] > 0
    assert status["counts"]["legacy_backfilled"] == 2
    assert status["counts"]["batch_receipts"] == 1
    assert status["duplicate_event_keys"] == 0

    ledger = run_store.run_history_ledger()
    live_confirms = [
        e for e in ledger["entries"]
        if e["origin"] == "live" and e["action"].startswith("auto_create_from_")
    ]
    assert len(live_confirms) == 2
    live_rows = [
        e for e in ledger["entries"]
        if e["origin"] == "live" and e["module"] == "run_history"
    ]
    assert len(live_rows) == 2
    for row in live_rows:
        assert row["confirm_ref"], "迁移中到达的事件必须有唯一确认来源"
        # 双写：JSONL 与 SQLite 都能找到同一条事件
        assert row["in_jsonl"] and row["in_sqlite"]
    batch_rows = [r for r in live_rows if r["action"] == "auto_create_from_batch"]
    recovery_rows = [r for r in live_rows if r["action"] == "auto_create_from_recovery"]
    assert batch_rows and batch_rows[0]["batch_receipt"] is True
    assert recovery_rows and recovery_rows[0]["batch_receipt"] is False

    receipt = ledger["batch_receipts"][0]
    assert receipt["batch_code"] == "BATCH-LIVE-1"
    assert receipt["task_count"] == 9
    assert receipt["run_event_uid"]
    # 回执与事件一一对应
    assert len({r["event_uid"] for r in ledger["batch_receipts"]}) == len(
        ledger["batch_receipts"])


def test_ledger_links_new_events_to_existing_positions():
    """运行历史事件对照：新录入事件与已有 JSONL 来源位置同时可查。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    view_before = run_store.visible_events()
    run_store.start_migration()
    while run_store.backfill_step(50)["remaining"]:
        pass
    _new_batch_confirm("BATCH-LEDGER", "PLAN-LEDGER", 3)
    run_store.finish_migration()

    ledger = run_store.run_history_ledger()
    # 一次批次确认新增 3 条日志事件：batch_queue 的 create/确认完成 + run_history 自动记录
    assert ledger["total"] == view_before["count"] + 3
    # 存量事件都有 JSONL 来源位置
    legacy = [e for e in ledger["entries"] if e["origin"] == "jsonl_backfill"]
    assert legacy
    for row in legacy:
        assert row["legacy_position"] and "#L" in row["legacy_position"]
        assert row["in_jsonl"] and row["in_sqlite"]
    # 新录入事件能追到唯一确认来源
    live = [e for e in ledger["entries"] if e["origin"] == "live"]
    assert live
    live_confirms = [
        e for e in live if e["action"].startswith("auto_create_from_")
    ]
    assert live_confirms and all(row["confirm_ref"] for row in live_confirms)
    # 没有事件被读成两份
    assert ledger["duplicates_suppressed"] >= 0
    assert len({e["event_key"] for e in ledger["entries"]}) == ledger["total"]


def test_interrupt_after_partial_backfill_then_restart():
    """回填一部分 → 触发批次完成 → 打断迁移 → 重新启动续跑。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    run_store.start_migration()
    first = run_store.backfill_step(2)
    assert first["appended"] == 2
    cursor_before = first["cursor"]

    _new_batch_confirm("BATCH-RESTART", "PLAN-RESTART", 4)
    assert run_store.migration_status()["counts"]["live"] > 0

    run_store.interrupt_migration("simulated restart")
    interrupted = run_store.migration_status()
    assert interrupted["status"] == run_store.STATUS_INTERRUPTED
    assert interrupted["cursor"] == cursor_before

    # 用独立进程重启，模拟服务重新拉起后读取持久化游标
    script = (
        "import sys, json; sys.path.insert(0, %r);"
        "from app import run_store;"
        "s = run_store.migration_status();"
        "print(json.dumps({'status': s['status'], 'cursor': s['cursor']}))"
        % backend_dir
    )
    output = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True,
        cwd=backend_dir,
    )
    reloaded = json.loads(output.stdout.strip().splitlines()[-1])
    assert reloaded["status"] == run_store.STATUS_INTERRUPTED
    assert reloaded["cursor"] == cursor_before

    resumed = run_store.resume_migration()
    assert resumed["status"] == run_store.STATUS_BACKFILLING
    assert resumed["cursor"] == cursor_before
    assert resumed["attempts"] >= 2

    while True:
        result = run_store.backfill_step(50)
        if not result["remaining"]:
            break
    # 回填不得把已在库的 live 事件再插一遍
    assert run_store.migration_status()["duplicate_event_keys"] == 0
    final = run_store.finish_migration()
    assert final["status"] == run_store.STATUS_COMPLETED
    assert final["verification"]["ok"] is True

    ledger = run_store.run_history_ledger()
    keys = [e["event_key"] for e in ledger["entries"]]
    assert len(keys) == len(set(keys))
    live = [e for e in ledger["entries"]
            if e["origin"] == "live" and e["module"] == "run_history"]
    assert len(live) == 1  # 一次批次确认 → 一条运行历史事件
    assert all(row["confirm_ref"] for row in live)


def test_failure_and_rollback_do_not_duplicate():
    """失败恢复与回滚期间查询不重复，回滚后回到旧部署可读视图。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    baseline = run_store.visible_events()
    run_store.start_migration()
    run_store.backfill_step(3)
    _new_batch_confirm("BATCH-FAIL", "PLAN-FAIL", 2)

    run_store.fail_migration("disk full")
    failed = run_store.visible_events()
    assert failed["status"] == run_store.STATUS_FAILED
    # 一次批次确认新增 3 条日志事件：批次 create + 确认完成 + 运行历史
    assert failed["count"] == baseline["count"] + 3
    # 回填的 3 条 + 双写的 3 条都能在两边找到，被抑制掉
    assert failed["duplicates_suppressed"] == 6
    # 同一事件在两种存储中都存在，但只读出一份
    assert len({e["_event_key"] for e in failed["events"]}) == failed["count"]

    resumed = run_store.resume_migration()
    assert resumed["status"] == run_store.STATUS_BACKFILLING
    assert resumed["last_error"] is None

    rolled = run_store.rollback_migration()
    assert rolled["status"] == run_store.STATUS_ROLLED_BACK
    assert rolled["read_mode"] == "jsonl_only"
    after = run_store.visible_events()
    # 回滚后只读 JSONL，SQLite 数据保留但不再参与可读视图
    assert after["store_counts"]["sqlite"] == 0
    assert after["duplicates_suppressed"] == 0
    assert after["count"] == len(run_store._jsonl_events())
    assert after["count"] == baseline["count"] + 3

    # 回滚后重新迁移仍能收口，且不产生重复键
    run_store.start_migration()
    while run_store.backfill_step(50)["remaining"]:
        pass
    final = run_store.finish_migration()
    assert final["verification"]["ok"] is True


def test_compare_reports_three_phases():
    """对照迁移前、进行中、完成后的可读事件与各来源计数。"""
    from app import run_store

    _reset()
    _seed_legacy_logs()
    run_store.start_migration()
    run_store.backfill_step(2)
    run_store.interrupt_migration("pause")
    _new_batch_confirm("BATCH-CMP", "PLAN-CMP", 1)
    run_store.resume_migration()
    while run_store.backfill_step(50)["remaining"]:
        pass
    run_store.finish_migration()

    comparison = run_store.compare()
    phases = {snap["phase"] for snap in comparison["snapshots"]}
    assert {"before", "in_progress", "completed"} <= phases
    before = comparison["before"]
    in_progress = comparison["in_progress"]
    completed = comparison["completed"]
    assert before["visible_events"] == 5
    assert in_progress["visible_events"] >= before["visible_events"]
    assert completed["visible_events"] >= in_progress["visible_events"]
    assert before["by_store"]["sqlite"] == 0
    assert completed["by_store"]["sqlite"] == completed["visible_events"]
    assert sum(before["by_source"].values()) == before["visible_events"]
    assert sum(completed["by_source"].values()) == completed["visible_events"]


if __name__ == "__main__":
    tests = [
        test_stage_counts_and_dedup,
        test_legacy_missing_columns_stay_null,
        test_batch_receipts_and_unique_confirm_source,
        test_ledger_links_new_events_to_existing_positions,
        test_interrupt_after_partial_backfill_then_restart,
        test_failure_and_rollback_do_not_duplicate,
        test_compare_reports_three_phases,
    ]
    for test in tests:
        test()
        print(f"PASS: {test.__name__}")
    print(f"\nALL {len(tests)} MIGRATION TESTS PASSED")
