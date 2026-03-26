# YTPanel Agent API

`ytpanel` 提供一套独立的 Agent API，供本机 AI agent 直接读取仪表盘、频道、任务、设置和队列数据，并通过统一的异步动作接口执行变更。

## Design Goals

- 只服务本机 agent，不复用页面接口语义
- 默认返回 `summary` 和 `raw` 两层数据
- 所有有副作用操作统一入队，返回 `job_id`
- 所有 agent 动作都写入审计记录

服务边界：

- 仅绑定 `127.0.0.1`
- 路由层也会再次校验 loopback 请求
- 第一版不加 token

## Base URL

默认后端：

```text
http://127.0.0.1:3457/api/agent
```

## Response Shapes

读取接口：

```json
{
  "ok": true,
  "date": "2026-03-26",
  "summary": {},
  "raw": {}
}
```

动作接口：

```json
{
  "ok": true,
  "accepted": true,
  "action": "create_task",
  "target": {
    "type": "task",
    "id": null
  },
  "job_id": "uuid",
  "queued_at": "2026-03-26T09:00:00.000Z"
}
```

错误接口：

```json
{
  "ok": false,
  "error": {
    "code": "INVALID_INPUT",
    "message": "title is required",
    "retryable": false,
    "details": {}
  }
}
```

## Read Endpoints

### `GET /context`

一次返回 agent 建立上下文所需的核心信息：

- dashboard summary
- queue counts
- API quota
- scheduler next run
- recent failures
- attention items

示例：

```bash
curl "http://127.0.0.1:3457/api/agent/context?date=2026-03-26"
```

### `GET /dashboard`

查询指定日期仪表盘。

参数：

- `date=YYYY-MM-DD`
- `view=summary|raw|both`

`summary` 关键字段：

- `channel_total`
- `due_today_total`
- `updated_today_total`
- `task_total`
- `completed_task_total`
- `progress_percent`
- `attention_items`

`attention_items` 是面向 agent 的可排序提醒对象：

- `kind`
- `severity`
- `score`
- `title`
- `message`
- `recommended_action`
- `retryable`
- `target`
- `metadata`

### `GET /channels`

返回频道列表。

参数：

- `date=YYYY-MM-DD`
- `view=summary|raw|both`

`summary` 每项包含：

- `channel_id`
- `title`
- `workflow_status`
- `today_status`
- `update_cadence`
- `publish_days`
- `update_frequency_summary`
- `last_sync_age_hours`
- `latest_video`
- `growth`
- `risks`

### `GET /channels/:id`

返回单频道 agent 视图。

参数：

- `date=YYYY-MM-DD`
- `view=summary|raw|both`
- `include=videos,analytics,tasks`

### `GET /tasks`

返回指定日期任务。

参数：

- `date=YYYY-MM-DD`
- `status=todo|in_progress|done|delayed`
- `view=summary|raw|both`

### `GET /jobs`

返回队列状态和近 100 条 job。

参数：

- `status=queued,running,failed`
- `view=summary|raw|both`

### `GET /jobs/:id`

返回单个 job 的摘要、原始 job 记录、审计记录、日志。

### `GET /jobs/:id/result`

返回适合 agent 轮询的 job 完成结果：

- `status`
- `progress`
- `logs`
- `result.summary`
- `result.raw`
- `error`

### `GET /settings`

返回 agent 关心的有效配置：

- API quota
- scheduler state
- sync/download concurrency
- `has_api_key`

## Action Endpoint

### `POST /actions/:action`

所有有副作用操作统一从这里进。

支持动作：

- `sync_channel`
- `sync_all_due_channels`
- `create_task`
- `update_task`
- `delete_task`
- `update_task_status`
- `update_channel_schedule`
- `mark_channel_updated`
- `unmark_channel_updated`
- `update_channel_workflow_status`
- `download_channel_metadata`
- `refresh_dashboard_cache`

统一请求体：

```json
{
  "target_type": "task",
  "target_id": null,
  "input": {},
  "request_context": {
    "source": "agent",
    "agent_name": "local-agent",
    "reason": "why the action is being triggered"
  }
}
```

## Action Examples

### 创建任务

```json
{
  "target_type": "task",
  "target_id": null,
  "input": {
    "title": "优化旧视频封面",
    "task_name": "制作封面",
    "channel_id": "UCnqiHrso5OdqMGttbqDslcw",
    "due_date": "2026-03-27",
    "priority": "high",
    "status": "todo",
    "planned_start_time": "13:30",
    "planned_end_time": "15:00",
    "notes": "先改 CTR 最低的一条"
  },
  "request_context": {
    "source": "agent",
    "agent_name": "planner-agent",
    "reason": "convert dashboard insight into an action item"
  }
}
```

### 更新任务状态

```json
{
  "target_type": "task",
  "target_id": "task-uuid",
  "input": {
    "status": "in_progress"
  },
  "request_context": {
    "source": "agent",
    "agent_name": "ops-agent",
    "reason": "task started"
  }
}
```

### 更新频道更新频率

```json
{
  "target_type": "channel",
  "target_id": "UCnqiHrso5OdqMGttbqDslcw",
  "input": {
    "sync_policy": {
      "cadence": "custom",
      "publish_days": [1, 3, 5],
      "target_publish_time": null
    }
  },
  "request_context": {
    "source": "agent",
    "agent_name": "schedule-agent",
    "reason": "align posting cadence with strategy"
  }
}
```

## Polling Pattern

1. 调用 `POST /actions/:action`
2. 拿到 `job_id`
3. 轮询 `GET /jobs/:id`
4. 结束后读取 `GET /jobs/:id/result`

示例：

```bash
curl -X POST "http://127.0.0.1:3457/api/agent/actions/create_task" \
  -H "content-type: application/json" \
  -d @payload.json

curl "http://127.0.0.1:3457/api/agent/jobs/<job_id>"
curl "http://127.0.0.1:3457/api/agent/jobs/<job_id>/result"
```

## Audit

所有 agent 动作都会写入 `agent_actions`：

- `action_id`
- `agent_name`
- `action`
- `target_type`
- `target_id`
- `payload_json`
- `job_id`
- `status`
- `created_at`
- `finished_at`

job 详情接口会把对应的 audit 一起返回。

## Notes

- Agent API 不依赖前端页面状态。
- `summary` 适合决策，`raw` 适合深挖和二次计算。
- `refresh_dashboard_cache` 当前是兼容性动作，占位返回成功；仪表盘本身仍是请求时计算。
