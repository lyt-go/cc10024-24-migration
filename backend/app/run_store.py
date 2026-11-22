"""运行存储迁移：把 JSONL 运行日志逐步迁入 SQLite，并提供新旧部署兼容读取。

设计要点
--------
1. 存量日志按「日志文件 + 物理行号」记录来源位置，迁移游标落库，
   允许回填一部分后中断、重启后从游标继续。
2. 迁移期间新产生的运行事件双写：JSONL 保持旧部署可读，SQLite 侧以
   ``event_uid`` 记录唯一确认来源，同一个事件不会在库里出现两份。
3. 存量行缺少的列（例如老的 ``batch_queue.jsonl`` 没有 source_module/source_id）
   一律写 NULL，不臆造默认值；字段缺失本身就是要保留的事实。
4. 兼容读取在 JSONL 只读 / SQLite 只读 / 混合三种模式下都按 event_key 去重，
   失败恢复与回滚期间查询读到的同一个事件只会出现一次。
5. ``migration_snapshots`` 在迁移前、进行中、完成后各冻结一份可读事件与
   各来源计数，供 ``compare()`` 对照。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections import Counter
from typing import Any

from app import persistence as persist

# 迁移状态机
STATUS_IDLE = "idle"
STATUS_BACKFILLING = "backfilling"
STATUS_SWITCH_PENDING = "switch_pending"
STATUS_INTERRUPTED = "interrupted"
STATUS_FAILED = "failed"
STATUS_COMPLETED = "completed"
STATUS_ROLLED_BACK = "rolled_back"

_IN_PROGRESS = (
    STATUS_BACKFILLING,
    STATUS_SWITCH_PENDING,
    STATUS_INTERRUPTED,
    STATUS_FAILED,
)
# 这些状态下查询需要把 SQLite 侧并进来
_DB_VISIBLE = _IN_PROGRESS + (STATUS_COMPLETED,)

RUN_HISTORY_JOURNAL = "run_history.jsonl"
UNKNOWN_SOURCE = "(unknown)"
DB_FILENAME = "run_store.sqlite3"

_LOCK = threading.RLock()


def _default_db_path() -> str:
    override = os.environ.get("RUN_STORE_DB")
    if override:
        return override
    return persist.data_path("", DB_FILENAME)


def event_key(record: dict[str, Any]) -> str:
    """兼容读取的去重键：来源优先，其次是条目+版本+动作。

    JSONL 与 SQLite 两边对同一条事件算出的键必须一致，因此这里只依赖
    事件本身的业务字段，不依赖库内自增主键。
    """
    source_module = record.get("source_module")
    source_id = record.get("source_id")
    if source_module is not None and source_id is not None:
        subject = f"src:{source_module}:{source_id}"
    else:
        subject = f"entry:{record.get('module')}:{record.get('entry_id')}"
    return "|".join(
        str(part)
        for part in (
            subject,
            record.get("module"),
            record.get("action"),
            record.get("version") or "",
            record.get("timestamp") or "",
        )
    )


def _now_utc() -> str:
    return persist.now_iso()


# ---------------------------------------------------------------------------
# 数据库结构与连接
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS migration_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    status          TEXT NOT NULL,
    read_mode       TEXT NOT NULL,          -- jsonl_only / hybrid / sqlite_primary
    cursor_file     TEXT,
    cursor_line     INTEGER NOT NULL DEFAULT 0,
    backfilled      INTEGER NOT NULL DEFAULT 0,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    started_at      TEXT,
    updated_at      TEXT,
    completed_at    TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
    event_uid       TEXT PRIMARY KEY,
    event_key       TEXT NOT NULL,
    module          TEXT NOT NULL,
    entity          TEXT,
    entry_id        INTEGER,
    status          TEXT,
    version         TEXT,
    action          TEXT,
    timestamp       TEXT,
    abnormal        INTEGER,
    source_module   TEXT,                   -- 存量行缺失时为 NULL
    source_id       INTEGER,                -- 存量行缺失时为 NULL
    journal         TEXT,                   -- 事件所属 JSONL 日志名
    origin          TEXT NOT NULL,          -- jsonl_backfill / live
    in_batch        INTEGER NOT NULL DEFAULT 0,
    confirm_ref     TEXT,                   -- 唯一确认来源（批次确认 / 恢复确认）
    legacy_origin   TEXT,                   -- 迁移来源位置 file#line
    backfilled_at   TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_events_key ON run_events(event_key);
CREATE INDEX IF NOT EXISTS idx_run_events_module ON run_events(module);

CREATE TABLE IF NOT EXISTS run_history_rows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uid       TEXT NOT NULL UNIQUE,   -- 一个运行记录只对应一个确认事件
    record_no       TEXT,
    plan            TEXT,
    start_time      TEXT,
    end_time        TEXT,
    collected_rows  TEXT,
    failed_rows     TEXT,
    exec_log        TEXT,
    abnormal_summary TEXT,
    status          TEXT,
    version         TEXT,
    updated_at      TEXT,
    event_key       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_receipts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        INTEGER NOT NULL,
    batch_code      TEXT,
    plan            TEXT,
    task_count      INTEGER,
    completed_count INTEGER,
    failed_count    INTEGER,
    status          TEXT,
    confirmed_at    TEXT,
    event_uid       TEXT NOT NULL UNIQUE,   -- 接住批次确认的采集回执
    run_event_uid   TEXT,
    run_row_id      INTEGER
);

CREATE TABLE IF NOT EXISTS migration_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    phase           TEXT NOT NULL,           -- before / in_progress / completed
    status          TEXT,
    read_mode       TEXT,
    taken_at        TEXT NOT NULL,
    visible_events  INTEGER NOT NULL,
    source_counts   TEXT NOT NULL            -- JSON: {source: count}
);

CREATE TABLE IF NOT EXISTS migration_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT,
    appended        INTEGER NOT NULL DEFAULT 0,
    skipped         INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);
"""


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or _default_db_path()
    conn = sqlite3.connect(path, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA)
    return conn


def _state_row(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM migration_state WHERE id = 1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO migration_state (id, status, read_mode, updated_at) "
            "VALUES (1, ?, 'jsonl_only', ?)",
            (STATUS_IDLE, _now_utc()),
        )
        row = conn.execute("SELECT * FROM migration_state WHERE id = 1").fetchone()
    return row


def _set_state(conn: sqlite3.Connection, **fields: Any) -> None:
    fields["updated_at"] = _now_utc()
    assignments = ", ".join(f"{key} = ?" for key in fields)
    conn.execute(
        f"UPDATE migration_state SET {assignments} WHERE id = 1",
        (*fields.values(),),
    )


def _read_mode_for(status: str) -> str:
    if status == STATUS_COMPLETED:
        return "sqlite_primary"
    if status in _IN_PROGRESS:
        return "hybrid"
    return "jsonl_only"


# ---------------------------------------------------------------------------
# 运行事件读取（JSONL 物理行号 → 来源位置）
# ---------------------------------------------------------------------------

def _runs_dir() -> str:
    return os.path.dirname(persist.data_path("runs", RUN_HISTORY_JOURNAL))


def _journals() -> list[str]:
    directory = _runs_dir()
    if not os.path.isdir(directory):
        return []
    return sorted(
        name for name in os.listdir(directory)
        if name.endswith(".jsonl") and os.path.isfile(os.path.join(directory, name))
    )


def _journal_lines(journal: str) -> list[tuple[int, dict[str, Any] | None, str | None]]:
    """返回 [(物理行号, 记录或 None, 解析错误)]，行号从 1 开始，空行也占号。"""
    path = os.path.join(_runs_dir(), journal)
    out: list[tuple[int, dict[str, Any] | None, str | None]] = []
    if not os.path.isfile(path):
        return out
    with open(path, "r", encoding="utf-8") as handle:
        for lineno, raw in enumerate(handle, start=1):
            text = raw.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                out.append((lineno, None, str(exc)))
                continue
            if isinstance(record, dict):
                out.append((lineno, record, None))
    return out


def _jsonl_events(journals: list[str] | None = None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for journal in journals or _journals():
        for lineno, record, error in _journal_lines(journal):
            if record is None:
                continue
            event = dict(record)
            event["journal"] = journal
            event["legacy_origin"] = f"{journal}#L{lineno}"
            event["_store"] = "jsonl"
            event["_event_key"] = event_key(event)
            event["confirm_ref"] = event.get("confirm_ref")
            events.append(event)
    return events


# ---------------------------------------------------------------------------
# 迁移引擎
# ---------------------------------------------------------------------------

def _event_uid_for(record: dict[str, Any]) -> str:
    import hashlib
    digest = hashlib.sha1(event_key(record).encode("utf-8")).hexdigest()[:20]
    return f"live:{digest}"


def _legacy_event_uid(journal: str, lineno: int) -> str:
    return f"legacy:{journal}#L{lineno}"


def _insert_event(
    conn: sqlite3.Connection,
    record: dict[str, Any],
    *,
    journal: str,
    origin: str,
    event_uid: str,
    legacy_origin: str | None,
    confirm_ref: str | None,
    in_batch: bool = False,
) -> str:
    """插入一条运行事件；event_key 已存在时复用原行，保证唯一来源。

    存量行里缺失的列按 NULL 写入，不替换成空串或 0。
    """
    existing = conn.execute(
        "SELECT event_uid FROM run_events WHERE event_key = ? LIMIT 1",
        (event_key(record),),
    ).fetchone()
    now = _now_utc()
    if existing is not None:
        # 同一事件已在库中（例如 live 先到、回填后到）：只补来源位置，不新增行。
        conn.execute(
            "UPDATE run_events SET legacy_origin = COALESCE(legacy_origin, ?), "
            "journal = COALESCE(journal, ?), "
            "confirm_ref = COALESCE(confirm_ref, ?) WHERE event_uid = ?",
            (legacy_origin, journal, confirm_ref, existing["event_uid"]),
        )
        return existing["event_uid"]
    conn.execute(
        "INSERT OR IGNORE INTO run_events ("
        "event_uid, event_key, module, entity, entry_id, status, version, action, "
        "timestamp, abnormal, source_module, source_id, journal, origin, in_batch, "
        "confirm_ref, legacy_origin, backfilled_at, created_at"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            event_uid,
            event_key(record),
            record.get("module"),
            record.get("entity"),
            record.get("entry_id"),
            record.get("status"),
            record.get("version"),
            record.get("action"),
            record.get("timestamp"),
            None if record.get("abnormal") is None else int(bool(record.get("abnormal"))),
            record.get("source_module"),
            record.get("source_id"),
            journal,
            origin,
            1 if in_batch else 0,
            confirm_ref,
            legacy_origin,
            now if origin == "jsonl_backfill" else None,
            now,
        ),
    )
    return event_uid


TARGET_JOURNALS = ("batch_queue.jsonl", "error_recover.jsonl", RUN_HISTORY_JOURNAL)


def _target_journals() -> list[str]:
    available = set(_journals())
    return [name for name in TARGET_JOURNALS if name in available]


def _normalize_db_event(row: sqlite3.Row) -> dict[str, Any]:
    event = {
        "module": row["module"],
        "entity": row["entity"],
        "entry_id": row["entry_id"],
        "status": row["status"],
        "version": row["version"],
        "action": row["action"],
        "timestamp": row["timestamp"],
        "abnormal": None if row["abnormal"] is None else bool(row["abnormal"]),
        "source_module": row["source_module"],
        "source_id": row["source_id"],
        "journal": row["journal"],
        "legacy_origin": row["legacy_origin"],
        "confirm_ref": row["confirm_ref"],
        "origin": row["origin"],
        "in_batch": bool(row["in_batch"]),
        "_store": "sqlite",
        "_event_uid": row["event_uid"],
    }
    event["_event_key"] = row["event_key"] or event_key(event)
    return event


def _merge_events(
    jsonl_events: list[dict[str, Any]],
    db_events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """按 event_key 合并两边事件，SQLite 侧胜出但保留 JSONL 来源位置。"""
    merged: dict[str, dict[str, Any]] = {}
    dupes = 0
    for event in jsonl_events:
        key = event["_event_key"]
        if key in merged:
            dupes += 1
            continue
        merged[key] = event
    for event in db_events:
        key = event["_event_key"]
        if key in merged:
            dupes += 1
            previous = merged[key]
            event = dict(event)
            event["legacy_origin"] = event.get("legacy_origin") or previous.get("legacy_origin")
            event["jsonl_origin"] = previous.get("legacy_origin")
            event["_store"] = "sqlite"
        merged[key] = event
    ordered = sorted(
        merged.values(),
        key=lambda item: (str(item.get("timestamp") or ""), item["_event_key"]),
    )
    return ordered, dupes


def _source_counts(events: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for event in events:
        source = event.get("source_module")
        counter[UNKNOWN_SOURCE if source is None else str(source)] += 1
    return dict(sorted(counter.items()))


def _journal_counts(events: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for event in events:
        counter[str(event.get("journal") or UNKNOWN_SOURCE)] += 1
    return dict(sorted(counter.items()))


def db_enabled(db_path: str | None = None) -> bool:
    return os.path.exists(db_path or _default_db_path())


def visible_events(db_path: str | None = None) -> dict[str, Any]:
    """兼容读取：无论迁移进行到哪一步，同一事件只出现一次。"""
    with _LOCK:
        path = db_path or _default_db_path()
        jsonl_events = _jsonl_events(_target_journals())
        state = None
        db_events: list[dict[str, Any]] = []
        if db_enabled(path):
            conn = _connect(path)
            try:
                state = dict(_state_row(conn))
                if state["status"] in _DB_VISIBLE:
                    db_events = [
                        _normalize_db_event(row)
                        for row in conn.execute("SELECT * FROM run_events ORDER BY rowid")
                    ]
            finally:
                conn.close()
        merged, dupes = _merge_events(jsonl_events, db_events)
        status = state["status"] if state else STATUS_IDLE
        return {
            "status": status,
            "read_mode": state["read_mode"] if state else "jsonl_only",
            "events": merged,
            "count": len(merged),
            "duplicates_suppressed": dupes,
            "jsonl_count": len(jsonl_events),
            "sqlite_count": len(db_events),
            "source_counts": _source_counts(merged),
            "journal_counts": _journal_counts(merged),
            "store_counts": {
                "jsonl": sum(1 for e in merged if e.get("_store") == "jsonl"),
                "sqlite": sum(1 for e in merged if e.get("_store") == "sqlite"),
            },
        }


def _take_snapshot(conn: sqlite3.Connection, phase: str) -> dict[str, Any]:
    view = visible_events(_db_path_of(conn))
    conn.execute(
        "INSERT INTO migration_snapshots "
        "(phase, status, read_mode, taken_at, visible_events, source_counts) "
        "VALUES (?,?,?,?,?,?)",
        (
            phase,
            view["status"],
            view["read_mode"],
            _now_utc(),
            view["count"],
            json.dumps(
                {
                    "by_source": view["source_counts"],
                    "by_journal": view["journal_counts"],
                    "by_store": view["store_counts"],
                    "duplicates_suppressed": view["duplicates_suppressed"],
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        ),
    )
    return view


def _db_path_of(conn: sqlite3.Connection) -> str:
    for row in conn.execute("PRAGMA database_list"):
        if row["name"] == "main":
            return row["file"]
    return _default_db_path()


# ---------------------------------------------------------------------------
# 迁移控制
# ---------------------------------------------------------------------------

def start_migration(db_path: str | None = None) -> dict[str, Any]:
    """开始迁移：冻结迁移前快照，清空游标进入回填阶段。"""
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] == STATUS_COMPLETED:
                return migration_status(db_path=path, conn=conn)
            _take_snapshot(conn, "before")
            _set_state(
                conn,
                status=STATUS_BACKFILLING,
                read_mode="hybrid",
                cursor_file=None,
                cursor_line=0,
                last_error=None,
                started_at=state["started_at"] or _now_utc(),
                completed_at=None,
                attempts=state["attempts"] + 1,
            )
            conn.execute(
                "INSERT INTO migration_attempts (started_at, status) VALUES (?, ?)",
                (_now_utc(), STATUS_BACKFILLING),
            )
            return migration_status(db_path=path, conn=conn)
        finally:
            conn.close()


def backfill_step(
    limit: int = 10,
    *,
    db_path: str | None = None,
    interrupt_at_journal: str | None = None,
) -> dict[str, Any]:
    """从游标往后回填至多 limit 条事件，返回是否还有剩余。

    ``interrupt_at_journal`` 用于复现「迁移被日志生产者打断」：
    游标推进到该日志后停止，状态置为 interrupted，游标保留。
    """
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] in (STATUS_IDLE, STATUS_ROLLED_BACK):
                start_migration(path)
                state = _state_row(conn)
            elif state["status"] == STATUS_COMPLETED:
                # 已完成的不再回填；避免快照里出现第二个 before。
                return {
                    "appended": 0, "skipped": 0, "remaining": False,
                    "interrupted": False, "status": STATUS_COMPLETED,
                    "cursor": {"file": None, "line": 0},
                    "backfilled_total": state["backfilled"],
                }
            journals = _target_journals()
            cursor_file = state["cursor_file"]
            cursor_line = int(state["cursor_line"] or 0)
            if cursor_file is None:
                cursor_file = journals[0] if journals else None
                cursor_line = 0

            appended = 0
            skipped = 0
            interrupted = False
            done = False

            while not done:
                if cursor_file is None:
                    done = True
                    break
                if interrupt_at_journal and cursor_file == interrupt_at_journal:
                    interrupted = True
                    break
                if cursor_file not in journals:
                    cursor_file, cursor_line, done = _advance_journal(
                        journals, cursor_file)
                    continue

                lines = _journal_lines(cursor_file)
                pending = [
                    (lineno, record)
                    for lineno, record, _error in lines
                    if lineno > cursor_line and record is not None
                ]
                if appended >= limit:
                    break
                for lineno, record in pending:
                    if appended >= limit:
                        break
                    uid = _legacy_event_uid(cursor_file, lineno)
                    before = conn.total_changes
                    _insert_event(
                        conn,
                        record,
                        journal=cursor_file,
                        origin="jsonl_backfill",
                        event_uid=uid,
                        legacy_origin=f"{cursor_file}#L{lineno}",
                        confirm_ref=record.get("confirm_ref"),
                        in_batch=_is_batch_confirm(record),
                    )
                    if conn.total_changes > before:
                        appended += 1
                    else:
                        skipped += 1
                    if cursor_file == RUN_HISTORY_JOURNAL:
                        _ensure_receipt_from_legacy(conn, record, uid)
                    cursor_line = lineno

                if appended >= limit:
                    break
                cursor_file, cursor_line, done = _advance_journal(
                    journals, cursor_file)

            total_written = int(state["backfilled"] or 0) + appended
            new_status = (
                STATUS_INTERRUPTED if interrupted
                else STATUS_SWITCH_PENDING if done
                else STATUS_BACKFILLING
            )
            _set_state(
                conn,
                status=new_status,
                read_mode="hybrid",
                cursor_file=cursor_file,
                cursor_line=cursor_line if cursor_file else 0,
                backfilled=total_written,
            )
            if interrupted:
                _take_snapshot(conn, "in_progress")
            if done:
                conn.execute(
                    "UPDATE migration_attempts SET finished_at = ?, status = ?, "
                    "appended = appended + ?, skipped = skipped + ? "
                    "WHERE id = (SELECT MAX(id) FROM migration_attempts)",
                    (_now_utc(), new_status, appended, skipped),
                )
            return {
                "appended": appended,
                "skipped": skipped,
                "remaining": not done,
                "interrupted": interrupted,
                "status": new_status,
                "cursor": {"file": cursor_file, "line": cursor_line},
                "backfilled_total": total_written,
            }
        finally:
            conn.close()


def _advance_journal(
    journals: list[str], cursor_file: str
) -> tuple[str | None, int, bool]:
    """返回下一份日志与起始行；已到末尾时返回 (None, 0, True)。"""
    if cursor_file not in journals:
        return (journals[0] if journals else None), 0, not journals
    index = journals.index(cursor_file) + 1
    if index >= len(journals):
        return None, 0, True
    return journals[index], 0, False


def _is_batch_confirm(record: dict[str, Any]) -> bool:
    return (
        record.get("module") == "run_history"
        and str(record.get("action") or "").startswith("auto_create_from_batch")
    )


def _ensure_receipt_from_legacy(
    conn: sqlite3.Connection, record: dict[str, Any], event_uid: str
) -> None:
    """回填到的存量批次确认，若带得出批次来源就登记回执，否则留空不臆造。"""
    if not _is_batch_confirm(record):
        return
    source_module = record.get("source_module")
    source_id = record.get("source_id")
    if source_module != "batch_queue" or source_id is None:
        # 存量日志缺少来源列：回执无法定位到具体批次，保持缺失。
        return
    conn.execute(
        "INSERT OR IGNORE INTO batch_receipts "
        "(batch_id, batch_code, plan, task_count, completed_count, failed_count, "
        "status, confirmed_at, event_uid, run_event_uid, run_row_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            int(source_id),
            None,
            None,
            None,
            None,
            None,
            record.get("status"),
            record.get("timestamp"),
            event_uid,
            event_uid,
            None,
        ),
    )


def register_batch_receipt(
    *,
    batch: dict[str, Any],
    run_row: dict[str, Any],
    event: dict[str, Any],
    db_path: str | None = None,
) -> str:
    """登记一次真实的批次确认回执，并把运行事件绑定到唯一确认来源。

    返回 confirm_ref（同时作为 run event 的 confirm_ref 与回执唯一键的
    业务前缀）。重复调用同一次确认不会产生第二份回执。
    """
    with _LOCK:
        path = db_path or _default_db_path()
        if not db_enabled(path):
            return ""
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] not in _DB_VISIBLE:
                return ""
            uid = _event_uid_for(event)
            record = dict(event)
            record["_event_key"] = event_key(record)
            confirm_ref = f"batch-confirm:{record['_event_key']}"
            _insert_event(
                conn,
                record,
                journal=RUN_HISTORY_JOURNAL,
                origin="live",
                event_uid=uid,
                legacy_origin=None,
                confirm_ref=confirm_ref,
                in_batch=True,
            )
            row_id = None
            if run_row:
                conn.execute(
                    "INSERT OR IGNORE INTO run_history_rows "
                    "(event_uid, record_no, plan, start_time, end_time, collected_rows, "
                    "failed_rows, exec_log, abnormal_summary, status, version, "
                    "updated_at, event_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        uid,
                        run_row.get("记录编号"),
                        run_row.get("关联计划"),
                        run_row.get("开始时间"),
                        run_row.get("结束时间"),
                        run_row.get("采集行数"),
                        run_row.get("失败行数"),
                        run_row.get("执行日志"),
                        run_row.get("异常摘要"),
                        run_row.get("status"),
                        run_row.get("version"),
                        run_row.get("updated_at"),
                        record["_event_key"],
                    ),
                )
                found = conn.execute(
                    "SELECT id FROM run_history_rows WHERE event_uid = ?", (uid,)
                ).fetchone()
                row_id = found["id"] if found else None
            conn.execute(
                "INSERT OR IGNORE INTO batch_receipts "
                "(batch_id, batch_code, plan, task_count, completed_count, failed_count, "
                "status, confirmed_at, event_uid, run_event_uid, run_row_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    int(batch.get("id") or 0),
                    batch.get("批次编号"),
                    batch.get("关联计划"),
                    batch.get("任务数量"),
                    batch.get("完成数"),
                    batch.get("失败数"),
                    batch.get("status"),
                    record.get("timestamp"),
                    uid,
                    uid,
                    row_id,
                ),
            )
            return confirm_ref
        finally:
            conn.close()


def register_recovery_event(
    *,
    run_row: dict[str, Any],
    event: dict[str, Any],
    db_path: str | None = None,
) -> str:
    """登记恢复确认产生的运行事件（不写批次回执）。"""
    with _LOCK:
        path = db_path or _default_db_path()
        if not db_enabled(path):
            return ""
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] not in _DB_VISIBLE:
                return ""
            uid = _event_uid_for(event)
            record = dict(event)
            record["_event_key"] = event_key(record)
            confirm_ref = f"recovery-confirm:{record['_event_key']}"
            _insert_event(
                conn,
                record,
                journal=RUN_HISTORY_JOURNAL,
                origin="live",
                event_uid=uid,
                legacy_origin=None,
                confirm_ref=confirm_ref,
            )
            if run_row:
                conn.execute(
                    "INSERT OR IGNORE INTO run_history_rows "
                    "(event_uid, record_no, plan, start_time, end_time, collected_rows, "
                    "failed_rows, exec_log, abnormal_summary, status, version, "
                    "updated_at, event_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        uid,
                        run_row.get("记录编号"),
                        run_row.get("关联计划"),
                        run_row.get("开始时间"),
                        run_row.get("结束时间"),
                        run_row.get("采集行数"),
                        run_row.get("失败行数"),
                        run_row.get("执行日志"),
                        run_row.get("异常摘要"),
                        run_row.get("status"),
                        run_row.get("version"),
                        run_row.get("updated_at"),
                        record["_event_key"],
                    ),
                )
            return confirm_ref
        finally:
            conn.close()


def record_live_event(
    event: dict[str, Any],
    *,
    db_path: str | None = None,
    journal: str | None = None,
) -> str | None:
    """迁移期间/完成后到达的事件写入 SQLite，以 event_uid 保证唯一。"""
    with _LOCK:
        path = db_path or _default_db_path()
        if not db_enabled(path):
            return None
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] not in _DB_VISIBLE:
                return None
            uid = _event_uid_for(event)
            _insert_event(
                conn,
                event,
                journal=journal or event.get("journal") or f"{event.get('module')}.jsonl",
                origin="live",
                event_uid=uid,
                legacy_origin=None,
                confirm_ref=event.get("confirm_ref"),
                in_batch=bool(event.get("in_batch")),
            )
            return uid
        finally:
            conn.close()


def finish_migration(db_path: str | None = None, *, verify: bool = True) -> dict[str, Any]:
    """收口切换：补齐回填缺口 → 对齐校验 → 冻结完成后快照 → 标记完成。"""
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] not in _IN_PROGRESS:
                return migration_status(db_path=path, conn=conn)
            # 切换前把游标之后的剩余事件补齐（幂等，event_key 去重）
            while True:
                result = backfill_step(1000, db_path=path)
                if not result["remaining"]:
                    break
            _take_snapshot(conn, "completed")
            _set_state(
                conn,
                status=STATUS_COMPLETED,
                read_mode="sqlite_primary",
                cursor_file=None,
                cursor_line=0,
                completed_at=_now_utc(),
            )
            conn.execute(
                "UPDATE migration_attempts SET finished_at = ?, status = ? "
                "WHERE id = (SELECT MAX(id) FROM migration_attempts)",
                (_now_utc(), STATUS_COMPLETED),
            )
            status = migration_status(db_path=path, conn=conn)
            if verify:
                status["verification"] = verify_migration(db_path=path)
            return status
        finally:
            conn.close()


def interrupt_migration(
    reason: str = "signal", db_path: str | None = None
) -> dict[str, Any]:
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            _state_row(conn)
            _take_snapshot(conn, "in_progress")
            _set_state(
                conn,
                status=STATUS_INTERRUPTED,
                read_mode="hybrid",
                last_error=reason,
            )
            return migration_status(db_path=path, conn=conn)
        finally:
            conn.close()


def fail_migration(error: str, db_path: str | None = None) -> dict[str, Any]:
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            _state_row(conn)
            _set_state(
                conn,
                status=STATUS_FAILED,
                read_mode="hybrid",
                last_error=error,
            )
            conn.execute(
                "UPDATE migration_attempts SET finished_at = ?, status = ?, error = ? "
                "WHERE id = (SELECT MAX(id) FROM migration_attempts) AND finished_at IS NULL",
                (_now_utc(), STATUS_FAILED, error),
            )
            return migration_status(db_path=path, conn=conn)
        finally:
            conn.close()


def resume_migration(db_path: str | None = None) -> dict[str, Any]:
    """从中断/失败处继续：游标保持不动，只把状态改回回填中。"""
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            state = _state_row(conn)
            if state["status"] not in (STATUS_INTERRUPTED, STATUS_FAILED):
                return migration_status(db_path=path, conn=conn)
            _set_state(
                conn,
                status=STATUS_BACKFILLING,
                read_mode="hybrid",
                last_error=None,
                attempts=state["attempts"] + 1,
            )
            conn.execute(
                "INSERT INTO migration_attempts (started_at, status) VALUES (?, ?)",
                (_now_utc(), STATUS_BACKFILLING),
            )
            return migration_status(db_path=path, conn=conn)
        finally:
            conn.close()


def rollback_migration(db_path: str | None = None) -> dict[str, Any]:
    """回滚到只读 JSONL：库内数据保留，但不再参与可读事件。"""
    with _LOCK:
        path = db_path or _default_db_path()
        conn = _connect(path)
        try:
            state = _state_row(conn)
            _set_state(
                conn,
                status=STATUS_ROLLED_BACK,
                read_mode="jsonl_only",
                last_error=None,
                cursor_file=None,
                cursor_line=0,
                completed_at=None,
            )
            conn.execute(
                "UPDATE migration_attempts SET finished_at = ?, status = ? "
                "WHERE id = (SELECT MAX(id) FROM migration_attempts) AND finished_at IS NULL",
                (_now_utc(), STATUS_ROLLED_BACK),
            )
            status = migration_status(db_path=path, conn=conn)
            status["rolled_back_from"] = state["status"]
            return status
        finally:
            conn.close()


def migration_status(
    db_path: str | None = None, *, conn: sqlite3.Connection | None = None
) -> dict[str, Any]:
    with _LOCK:
        path = db_path or _default_db_path()
        owned = conn is None
        if conn is None:
            conn = _connect(path)
        try:
            state = dict(_state_row(conn))
            counts = {
                "run_events": conn.execute(
                    "SELECT COUNT(*) AS n FROM run_events").fetchone()["n"],
                "legacy_backfilled": conn.execute(
                    "SELECT COUNT(*) AS n FROM run_events WHERE origin = 'jsonl_backfill'"
                ).fetchone()["n"],
                "live": conn.execute(
                    "SELECT COUNT(*) AS n FROM run_events WHERE origin = 'live'"
                ).fetchone()["n"],
                "run_history_rows": conn.execute(
                    "SELECT COUNT(*) AS n FROM run_history_rows").fetchone()["n"],
                "batch_receipts": conn.execute(
                    "SELECT COUNT(*) AS n FROM batch_receipts").fetchone()["n"],
            }
            duplicates = conn.execute(
                "SELECT COUNT(*) AS n FROM ("
                "SELECT event_key FROM run_events GROUP BY event_key HAVING COUNT(*) > 1)"
            ).fetchone()["n"]
            return {
                "status": state["status"],
                "read_mode": state["read_mode"],
                "cursor": {
                    "file": state["cursor_file"],
                    "line": state["cursor_line"],
                },
                "backfilled": state["backfilled"],
                "attempts": state["attempts"],
                "last_error": state["last_error"],
                "started_at": state["started_at"],
                "updated_at": state["updated_at"],
                "completed_at": state["completed_at"],
                "counts": counts,
                "duplicate_event_keys": duplicates,
            }
        finally:
            if owned:
                conn.close()


# ---------------------------------------------------------------------------
# 运行历史事件对照
# ---------------------------------------------------------------------------

def _snapshot_payloads(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM migration_snapshots ORDER BY id"
    ).fetchall()
    payloads = []
    for row in rows:
        counts = json.loads(row["source_counts"])
        payloads.append({
            "phase": row["phase"],
            "status": row["status"],
            "read_mode": row["read_mode"],
            "taken_at": row["taken_at"],
            "visible_events": row["visible_events"],
            "by_source": counts.get("by_source", {}),
            "by_journal": counts.get("by_journal", {}),
            "by_store": counts.get("by_store", {}),
            "duplicates_suppressed": counts.get("duplicates_suppressed", 0),
        })
    return payloads


def run_history_ledger(db_path: str | None = None) -> dict[str, Any]:
    """运行历史事件对照：把库内新录入事件与 JSONL 既有位置并列。

    查询方可以同时看到：事件身份、来源文件与物理行、迁移来源还是在线录入、
    唯一确认来源、以及是否登记了批次回执。
    """
    with _LOCK:
        path = db_path or _default_db_path()
        jsonl_by_key: dict[str, dict[str, Any]] = {}
        for event in _jsonl_events():
            jsonl_by_key.setdefault(event["_event_key"], event)
        view = visible_events(path)
        receipts: dict[str, dict[str, Any]] = {}
        db_rows: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []
        if db_enabled(path):
            conn = _connect(path)
            try:
                for row in conn.execute(
                    "SELECT event_uid, in_batch, confirm_ref, origin, legacy_origin, "
                    "journal FROM run_events"
                ):
                    receipts[row["event_uid"]] = dict(row)
                for row in conn.execute(
                    "SELECT * FROM batch_receipts ORDER BY id"
                ):
                    db_rows.append(dict(row))
                snapshots = _snapshot_payloads(conn)
            finally:
                conn.close()

        entries = []
        for event in view["events"]:
            key = event["_event_key"]
            source = jsonl_by_key.get(key)
            event_uid = event.get("_event_uid") or (
                _legacy_event_uid(event["journal"], 0)
                if source is None else _legacy_event_uid(
                    source["journal"], 0)
            )
            registry = receipts.get(event_uid, {})
            origin = event.get("origin") or (
                "jsonl_backfill" if event.get("_store") == "sqlite" else "jsonl_only")
            entries.append({
                "event_key": key,
                "event_uid": event_uid,
                "module": event.get("module"),
                "action": event.get("action"),
                "status": event.get("status"),
                "timestamp": event.get("timestamp"),
                "source_module": event.get("source_module"),
                "source_id": event.get("source_id"),
                "origin": origin,
                "store": event.get("_store"),
                "legacy_position": (source or {}).get("legacy_origin"),
                "sqlite_position": event.get("legacy_origin")
                if event.get("_store") == "sqlite" else None,
                "confirm_ref": event.get("confirm_ref"),
                "batch_receipt": bool(registry.get("in_batch")),
                "in_jsonl": source is not None,
                "in_sqlite": event.get("_store") == "sqlite",
            })

        return {
            "status": view["status"],
            "read_mode": view["read_mode"],
            "total": len(entries),
            "entries": entries,
            "source_counts": view["source_counts"],
            "journal_counts": view["journal_counts"],
            "store_counts": view["store_counts"],
            "duplicates_suppressed": view["duplicates_suppressed"],
            "batch_receipts": db_rows,
            "snapshots": snapshots,
        }


def verify_migration(db_path: str | None = None) -> dict[str, Any]:
    """对齐校验：库里每个事件键都能在 JSONL 或唯一确认来源上找到出处。"""
    with _LOCK:
        path = db_path or _default_db_path()
        view = visible_events(path)
        conn = _connect(path)
        try:
            missing_origin = []
            duplicate_keys = []
            for row in conn.execute(
                "SELECT event_uid, event_key, legacy_origin, confirm_ref, origin "
                "FROM run_events"
            ):
                if not row["legacy_origin"] and not row["confirm_ref"]:
                    missing_origin.append(row["event_uid"])
            for row in conn.execute(
                "SELECT event_key, COUNT(*) AS n FROM run_events "
                "GROUP BY event_key HAVING n > 1"
            ):
                duplicate_keys.append(row["event_key"])
            # 在线登记的批次确认必须都有回执；存量行缺少来源列时无法定位批次，
            # 这类事件只能作为「未挂接」信息保留，不能臆造回执。
            receipt_gaps = conn.execute(
                "SELECT COUNT(*) AS n FROM run_events e "
                "WHERE e.in_batch = 1 AND e.origin = 'live' AND NOT EXISTS ("
                "SELECT 1 FROM batch_receipts b WHERE b.run_event_uid = e.event_uid)"
            ).fetchone()["n"]
            legacy_unlinked = conn.execute(
                "SELECT COUNT(*) AS n FROM run_events e "
                "WHERE e.in_batch = 1 AND e.origin = 'jsonl_backfill' AND NOT EXISTS ("
                "SELECT 1 FROM batch_receipts b WHERE b.run_event_uid = e.event_uid)"
            ).fetchone()["n"]
        finally:
            conn.close()
        jsonl_keys = {event["_event_key"] for event in _jsonl_events()}
        return {
            "status": view["status"],
            "visible_events": view["count"],
            "jsonl_events": len(jsonl_keys),
            "sqlite_events": view["sqlite_count"],
            "duplicates_suppressed": view["duplicates_suppressed"],
            "duplicate_event_keys": duplicate_keys,
            "events_without_origin": missing_origin,
            "batch_events_without_receipt": receipt_gaps,
            "legacy_batch_events_without_receipt": legacy_unlinked,
            "ok": not duplicate_keys and not missing_origin and receipt_gaps == 0,
        }


def compare(db_path: str | None = None) -> dict[str, Any]:
    """迁移前 / 进行中 / 完成后的可读事件与来源计数对照。"""
    with _LOCK:
        path = db_path or _default_db_path()
        if not db_enabled(path):
            return {"snapshots": [], "current": visible_events(path)}
        conn = _connect(path)
        try:
            snapshots = _snapshot_payloads(conn)
        finally:
            conn.close()
        current = visible_events(path)
        by_phase = {snap["phase"]: snap for snap in snapshots}
        return {
            "snapshots": snapshots,
            "phases": sorted(by_phase),
            "before": by_phase.get("before"),
            "in_progress": by_phase.get("in_progress"),
            "completed": by_phase.get("completed"),
            "current": {
                "status": current["status"],
                "read_mode": current["read_mode"],
                "visible_events": current["count"],
                "source_counts": current["source_counts"],
                "journal_counts": current["journal_counts"],
                "store_counts": current["store_counts"],
                "duplicates_suppressed": current["duplicates_suppressed"],
            },
        }
