# 网页行为采集与内容清洗平台

面向网页行为录制、脚本化采集、内容清洗与结构化输出的数据采集与加工平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/migration.py 运行存储迁移/回填/对照接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   ├── app/run_store.py      JSONL → SQLite 运行存储迁移与兼容读取
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 行为脚本 | `behavior_script` | 行为脚本 | 脚本编号、目标站点、录制步骤 |
| 元素定位 | `element_selector` | 元素锚点 | 锚点编号、关联脚本、选择器类型 |
| 数据抽取 | `data_extract` | 抽取模板 | 模板编号、关联脚本、抽取字段 |
| 清洗管道 | `clean_pipeline` | 清洗工序 | 工序编号、输入源、清洗规则 |
| 转换规则 | `transform_rule` | 转换规则 | 规则编号、关联工序、源字段 |
| 质量防护 | `quality_guard` | 质量规则 | 规则编号、检核对象、检核维度 |
| 脚本调度 | `script_schedule` | 调度计划 | 计划编号、关联脚本、触发条件 |
| 登录凭证 | `login_mgr` | 登录凭证 | 凭证编号、目标站点、认证方式 |
| 页面快照 | `page_snapshot` | 页面快照 | 快照编号、采集来源、页面版本 |
| 错误恢复 | `error_recover` | 恢复任务 | 任务编号、关联计划、失败步骤 |
| 页面去重 | `page_dedup` | 去重规则 | 规则编号、去重维度、特征字段 |
| 字段归一化 | `field_normalize` | 归一规则 | 规则编号、关联字段、归一类型 |
| 批次队列 | `batch_queue` | 采集批次 | 批次编号、关联计划、任务数量 |
| 画布配置 | `canvas_config` | 画布配置 | 配置编号、关联站点、分辨率 |
| 导出通道 | `export_channel` | 导出配置 | 通道编号、输出格式、目标路径 |
| 运行历史 | `run_history` | 运行记录 | 记录编号、关联计划、开始时间 |
| Schema校验 | `schema_check` | Schema规则 | 规则编号、目标数据、字段名 |
| 内容缓存 | `content_cache` | 缓存配置 | 配置编号、缓存类型、过期策略 |

## 运行存储迁移（JSONL → SQLite）

`data/runs/` 下的 JSONL 运行日志可以逐步迁入 `data/run_store.sqlite3`，
迁移期间新旧两种部署都能读取，且同一个运行事件只算一次。

迁移状态：`idle → backfilling → switch_pending → completed`，可停在
`interrupted` / `failed`，也可 `rolled_back` 回到只读 JSONL。

| 接口 | 用途 |
| --- | --- |
| `GET /api/run-store/status` | 迁移阶段、游标、各类计数、重复事件键 |
| `POST /api/run-store/migrate/start` | 冻结「迁移前」快照并开始回填 |
| `POST /api/run-store/migrate/backfill?limit=N` | 从游标续跑 N 条，可 `interrupt_at_journal` 复现被打断 |
| `POST /api/run-store/migrate/interrupt` / `resume` | 打断 / 重新启动（游标保留） |
| `POST /api/run-store/migrate/finish` | 补齐缺口 → 冻结完成后快照 → 校验 |
| `POST /api/run-store/migrate/rollback` | 回到旧部署可读视图（库内数据保留） |
| `GET /api/run-store/events` | 兼容读取：新旧存储合并、按事件身份去重 |
| `GET /api/run-store/ledger` | 运行历史事件对照：新录入事件 + 既有 JSONL 来源位置 |
| `GET /api/run-store/compare` | 迁移前 / 进行中 / 完成后的可读事件与来源计数 |
| `GET /api/run-store/verify` | 对齐校验：来源位置、重复键、批次回执缺口 |

要点：

- 存量事件按「日志文件 + 物理行号」记录来源位置，游标落库，重启后从断点续跑。
- 存量行缺少的列（如老日志没有 `source_module` / `source_id`）写 `NULL`，
  不补默认值；来源计数里体现为 `(unknown)`。
- 迁移期间新产生的批次/恢复事件双写 JSONL 与 SQLite，以 `confirm_ref`
  绑定唯一确认来源；批次确认另外登记 `batch_receipts` 采集回执。
- 兼容读取在 `hybrid` 与 `sqlite_primary` 下都按事件键去重，失败恢复
  和回滚期间不会把同一事件读成两份。

测试：

```bash
cd backend
.venv/bin/python tests/test_cross_module.py        # 原有跨模块流程
.venv/bin/python tests/test_run_store_migration.py # 迁移 / 兼容读取 / 对照
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
