"""共享文件持久化：跨模块数据产物读写。
data/runs/     → 运行历史JSONL
data/snapshots/ → 页面快照JSON
data/reports/   → 质量报告JSON
data/exports/   → 导出文件
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_ROOT = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "data"))


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def data_path(sub: str, filename: str) -> str:
    p = os.path.join(_DATA_ROOT, sub, filename)
    _ensure_dir(os.path.dirname(p))
    return p


def write_json(sub: str, filename: str, data: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    return path


def read_json(sub: str, filename: str) -> dict[str, Any] | None:
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def append_jsonl(sub: str, filename: str, record: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    # 以下划线开头的是写库控制标记，不落进 JSONL，保证旧部署读到的字段不变。
    persisted = {k: v for k, v in record.items() if not k.startswith("_")}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(persisted, ensure_ascii=False, default=str) + "\n")
    if sub == "runs" and not record.get("_dual_written"):
        # 运行存储迁移期间双写：JSONL 保证旧部署可读，SQLite 侧按事件身份去重。
        # 已经通过 run_store 登记过业务事件（回执/恢复）的记录不再重复写库。
        try:
            from app import run_store
            run_store.record_live_event(record, journal=filename)
        except Exception:  # 迁移层故障不能影响主流程写日志
            pass
    return path


def read_jsonl(sub: str, filename: str) -> list[dict[str, Any]]:
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def list_files(sub: str, suffix: str = "") -> list[str]:
    path = os.path.join(_DATA_ROOT, sub)
    if not os.path.isdir(path):
        return []
    names = sorted(os.listdir(path))
    if suffix:
        names = [n for n in names if n.endswith(suffix)]
    return names


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")