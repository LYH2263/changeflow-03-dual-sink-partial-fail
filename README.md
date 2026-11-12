# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 双 Sink 扇出与部分失败

每条 apply 事件同时写主 Sink（`sink_records`）与旁路 Sink（`sink_shadow_records`），
两侧都记录对应 seq 与实体键。旁路失败时按 **应用台 → 双 Sink 部分失败策略** 里可保存的
互斥策略处理：

| 策略 | 旁路失败时 | 游标 |
| --- | --- | --- |
| `main_commit_isolate_shadow`（默认） | 主 Sink 提交，旁路失败进隔离区（`shadow_sink_failed`） | 前进，继续后续事件 |
| `rollback_both` | 本条两侧都回滚 | 不前进到该条并停止 |

同一页可填「旁路故障注入 seq」（逗号分隔，留空=不注入）来演示部分失败。

一次 Apply 的 `report.dual_write` 是计数唯一来源（写入 `dual_write_attempts`），
其中 `attempts = both_success + partial_failures + rollbacks`，并与主 Sink、旁路 Sink、
隔离区、游标页互相解释；明细可通过 `run #N` 的「本次双写明细」或
`GET /api/runs/{id}/dual-write` 查看，刷新后仍在。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链与「双 Sink 扇出 / 部分失败取舍」。缺口等待/跳过、乱序缓冲、事务整批、毒丸选择性跳过等仍在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
