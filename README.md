# oura-data-saver

定时从 [Oura Ring API v2](https://cloud.ouraring.com/v2/docs) 拉取所有数据并保存到 PostgreSQL。

## 要求

- Python ≥ 3.11
- PostgreSQL ≥ 18（使用 [virtual generated columns](https://www.postgresql.org/docs/18/ddl-generated-columns.html)）
- Oura Personal Access Token（[获取方式](https://cloud.ouraring.com/personal-access-tokens)）

## 安装

```bash
pip install .
```

## 配置

通过环境变量配置：

| 变量 | 必填 | 说明 | 默认值 |
|---|---|---|---|
| `OURA_ACCESS_TOKEN` | ✅ | Oura API token | — |
| `DATABASE_URL` | ✅ | PostgreSQL 连接串，如 `postgresql://user:pass@localhost:5432/oura` | — |
| `OURA_FULL_SYNC_START_DATE` | ❌ | 首次全量同步起始日期 | `2020-01-01` |
| `OURA_OVERLAP_DAYS` | ❌ | 每次增量同步回溯天数，用于捕获 Oura 回溯更新的数据 | `30` |

## 运行

```bash
export OURA_ACCESS_TOKEN="your-token"
export DATABASE_URL="postgresql://user:pass@localhost:5432/oura"
oura-data-saver
```

程序为一次性运行（run-once），适合配合 systemd timer 或 cron 定时调度。

## systemd timer 示例

```ini
# /etc/systemd/system/oura-data-saver.service
[Unit]
Description=Oura Ring data sync

[Service]
Type=oneshot
Environment=OURA_ACCESS_TOKEN=your-token
Environment=DATABASE_URL=postgresql://user:pass@localhost:5432/oura
ExecStart=oura-data-saver
```

```ini
# /etc/systemd/system/oura-data-saver.timer
[Unit]
Description=Run oura-data-saver every 6 hours

[Timer]
OnCalendar=*-*-* 00/6:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
systemctl daemon-reload
systemctl enable --now oura-data-saver.timer
```

## 同步的数据

### 文档类端点（按日期增量）

| 表名 | API 端点 | 说明 |
|---|---|---|
| `personal_info` | `/v2/usercollection/personal_info` | 个人信息 |
| `daily_activity` | `/v2/usercollection/daily_activity` | 每日活动 |
| `daily_sleep` | `/v2/usercollection/daily_sleep` | 每日睡眠评分 |
| `daily_readiness` | `/v2/usercollection/daily_readiness` | 每日准备度 |
| `daily_spo2` | `/v2/usercollection/daily_spo2` | 每日血氧 |
| `daily_stress` | `/v2/usercollection/daily_stress` | 每日压力 |
| `daily_resilience` | `/v2/usercollection/daily_resilience` | 每日恢复力 |
| `daily_cardiovascular_age` | `/v2/usercollection/daily_cardiovascular_age` | 每日心血管年龄 |
| `sleep` | `/v2/usercollection/sleep` | 睡眠详情 |
| `sleep_time` | `/v2/usercollection/sleep_time` | 建议睡眠时间 |
| `workout` | `/v2/usercollection/workout` | 运动记录 |
| `session` | `/v2/usercollection/session` | 冥想/呼吸练习 |
| `tag` | `/v2/usercollection/tag` | 标签 |
| `enhanced_tag` | `/v2/usercollection/enhanced_tag` | 增强标签 |
| `rest_mode_period` | `/v2/usercollection/rest_mode_period` | 休息模式 |
| `vo2_max` | `/v2/usercollection/vO2_max` | 最大摄氧量 |
| `ring_configuration` | `/v2/usercollection/ring_configuration` | 戒指配置（全量） |

### 时间序列端点

| 表名 | API 端点 | 说明 |
|---|---|---|
| `heartrate` | `/v2/usercollection/heartrate` | 逐条心率 |
| `ring_battery_level` | `/v2/usercollection/ring_battery_level` | 戒指电量 |
| `interbeat_interval` | `/v2/usercollection/interbeat_interval` | 心跳间期 (IBI) |

## 存储设计

每张表仅有两个物理存储列：

- `data` — 完整的 API 原始 JSON（JSONB）
- `synced_at` — 本地同步时间

所有其他字段（如 `day`、`score`、`bpm` 等）均为 PostgreSQL 18 的 **virtual generated column**，从 `data` 中实时计算，零额外磁盘开销。可直接用 SQL 查询：

```sql
SELECT day, score, steps FROM daily_activity WHERE day >= '2025-01-01';
SELECT ts, bpm, source FROM heartrate WHERE ts >= now() - interval '1 day';
```

## 增量同步策略

Oura API 仅支持按记录日期过滤（`start_date` / `end_date`），不支持按 `updated_at` 过滤。数据可能被 Oura 回溯更新（如重新计算睡眠评分）。

因此每次同步时会从 `last_sync_date - OVERLAP_DAYS` 开始重新拉取，通过 upsert 保证幂等，确保捕获回溯更新。

## 依赖

仅使用纯 Python 库，无 C 扩展依赖：

- [httpx](https://github.com/encode/httpx) — HTTP 客户端
- [pg8000](https://github.com/tlocke/pg8000) — PostgreSQL 驱动
