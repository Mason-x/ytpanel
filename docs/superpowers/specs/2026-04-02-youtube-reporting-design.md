# YouTube Reporting 多 Owner 隔离接入设计

## 背景

当前项目已经具备以下能力：

- 基于 `YouTube Data API v3` 的频道基础元数据同步
- 基于 `yt-dlp` / Cookie 的公开视频与资产抓取
- React 设置页、频道页、任务队列与每日调度
- SQLite 持久化与现有同步日志

本次新增需求不是替换现有能力，而是在现有系统上增加一套独立的 `YouTube Reporting API` 报表子系统，用于获取视频级报表数据，并提供多 Owner 的 OAuth 与网络隔离能力。

用户已明确以下产品边界：

- `YouTube Data API v3` 与 `YouTube Reporting API` 口径不同，必须分层存储与展示。
- 频道不要求强制绑定 Owner。
- 只有在设置页把频道绑定到某个 Owner 并开启 `Reporting API` 后，系统才开始为该频道拉取报表。
- OAuth 不做浏览器授权流，只支持手动录入 `refresh_token`。
- 历史数据不自动回补，只从“启用当天”开始进入派生日报。
- 频道详情页新增独立的“报表”页签，不和现有“数据洞察”混写。

## 目标

- 为每个 Owner 提供独立的 `client_id`、`client_secret`、`refresh_token`、`proxy_url`。
- 为每个 Owner 提供完全隔离的 token 刷新、报表列表查询与报表下载网络通道。
- 在设置页提供完整的 Owner 管理后台：
  - 新增、编辑、删除 Owner
  - 绑定频道
  - 开启/关闭频道的 Reporting 接入
  - 代理有效性检测
  - 本地运行用度展示
  - 请求日志排查
- 在后端实现 `Reporting API` 原始报表落盘、索引、幂等状态机与派生日报。
- 在频道页新增“报表”页签，展示以下视频级指标：
  - 展现量
  - 点击率
  - 平均观看时长
  - 平均观看百分比
  - 流量来源占比

## 非目标

- 不替换现有 `YouTube Data API v3` 同步链路。
- 不把报表指标回写到现有 `videos`、`video_daily` 等公开元数据字段。
- 不实现浏览器 OAuth 授权流程。
- 不做启用前历史报表的自动业务回补。
- 不把 Reporting 指标混入现有“数据洞察”页签。
- 不引入跨 Owner 共享的 HTTP session、token transport 或连接池。

## 外部依赖与约束

- `YouTube Data API v3` 与 `YouTube Reporting API` 可以在同一系统并行使用，但认证方式与数据口径不同。
- `Reporting API` 依赖 OAuth 2.0，不使用现有全局 API key。
- `Reporting API` 返回的是异步批量报表，适合原始文件落盘与后续派生。
- 官方文档说明 backfill 场景下可能重新投递修订报表，因此本地派生必须支持覆盖重算。

参考文档：

- `https://developers.google.com/youtube/v3/getting-started`
- `https://developers.google.com/youtube/reporting`
- `https://developers.google.com/youtube/reporting/v1/reports/channel_reports`
- `https://developers.google.com/youtube/reporting/v1/reports`

## 方案总览

采用“结构化后台方案”：

- 把 Owner、频道绑定、请求日志、报表状态、原始报表索引、派生日报全部设计为独立表。
- `Data API` 与 `Reporting API` 使用不同的数据面。
- `Reporting API` 的每一次网络调用都从 Owner 配置构建新的独立 transport/client。
- 设置页新增完整 `Reporting Owners` 后台。
- 频道页新增独立“报表”页签。

这样可以满足多 Owner 会话隔离、代理隔离、日志审计与可扩展性需求，避免把复杂状态挤进当前 `settings` 键值对。

## 架构边界

### 1. 现有系统与 Reporting 子系统并存

- 现有 `YouTube Data API v3` 继续负责频道基础元数据、公开视频列表、现有频道页内容。
- 新 `Reporting API` 子系统只负责报表抓取、落盘、导入与派生。
- 两者各自拥有独立的存储表、同步入口和前端展示区域。

### 2. Owner 是一等实体

每个 Owner 拥有：

- 独立 `client_id`
- 独立 `client_secret`
- 独立 `refresh_token`
- 独立 `proxy_url`
- 独立 token 刷新状态
- 独立请求日志
- 独立本地运行指标

### 3. 频道绑定是可选行为

- 频道默认不绑定 Owner。
- 绑定后，一个频道最多属于一个 Owner。
- 只有绑定且启用 Reporting 的频道才会纳入报表同步。

### 4. 网络隔离是硬约束

对同一个 Owner 的以下请求：

- token refresh
- Reporting job 列表查询
- report 列表查询
- report 下载
- 代理连通性检测

全部在请求发生时现建独立 HTTP 客户端，不跨 Owner 复用 session、连接池或 transport。

### 5. 数据存储采用“两层模型”

- 原始层：按 YouTube 下发的报表周期原样落盘，并存储索引元数据。
- 派生层：将所需报表按日聚合成频道页消费用的 `video_reporting_daily`。

## 数据模型设计

### `reporting_owners`

Owner 主表。

字段建议：

- `owner_id TEXT PRIMARY KEY`
- `name TEXT NOT NULL`
- `client_id TEXT NOT NULL`
- `client_secret TEXT NOT NULL`
- `refresh_token TEXT NOT NULL`
- `proxy_url TEXT`
- `enabled INTEGER NOT NULL DEFAULT 1`
- `reporting_enabled INTEGER NOT NULL DEFAULT 1`
- `started_at TEXT`
- `last_token_refresh_at TEXT`
- `last_sync_at TEXT`
- `last_error TEXT`
- `created_at TEXT NOT NULL DEFAULT (datetime('now'))`
- `updated_at TEXT NOT NULL DEFAULT (datetime('now'))`

说明：

- `started_at` 表示 Owner 维度启用 Reporting 的业务起始日。
- 凭证字段在返回前端时必须做掩码处理，和现有设置页敏感配置保持一致。

### `reporting_owner_channel_bindings`

频道与 Owner 绑定表。

字段建议：

- `id TEXT PRIMARY KEY`
- `owner_id TEXT NOT NULL`
- `channel_id TEXT NOT NULL`
- `enabled INTEGER NOT NULL DEFAULT 1`
- `reporting_enabled INTEGER NOT NULL DEFAULT 1`
- `started_at TEXT NOT NULL`
- `created_at TEXT NOT NULL DEFAULT (datetime('now'))`
- `updated_at TEXT NOT NULL DEFAULT (datetime('now'))`

索引与约束：

- `UNIQUE(channel_id)`，确保一个频道最多绑定一个 Owner。
- `INDEX(owner_id, reporting_enabled)`

说明：

- `started_at` 是频道 Reporting 生效边界。
- 派生日报只会写入 `started_at` 当天及之后的数据。

### `reporting_request_logs`

网络与同步请求审计表。

字段建议：

- `id INTEGER PRIMARY KEY AUTOINCREMENT`
- `owner_id TEXT`
- `channel_id TEXT`
- `request_kind TEXT NOT NULL`
- `request_url TEXT`
- `proxy_url_snapshot TEXT`
- `status_code INTEGER`
- `success INTEGER NOT NULL DEFAULT 0`
- `error_code TEXT`
- `error_message TEXT`
- `started_at TEXT NOT NULL`
- `finished_at TEXT`
- `duration_ms INTEGER`
- `response_meta_json TEXT NOT NULL DEFAULT '{}'`

`request_kind` 约定值：

- `proxy_probe`
- `token_refresh`
- `reporting_jobs_list`
- `reporting_reports_list`
- `report_download`

### `reporting_job_state`

本地幂等状态机。

字段建议：

- `id TEXT PRIMARY KEY`
- `owner_id TEXT NOT NULL`
- `channel_id TEXT NOT NULL`
- `report_type_id TEXT NOT NULL`
- `remote_job_id TEXT`
- `remote_report_id TEXT`
- `report_start_date TEXT`
- `report_end_date TEXT`
- `discovered_at TEXT`
- `downloaded_at TEXT`
- `imported_at TEXT`
- `derived_at TEXT`
- `status TEXT NOT NULL`
- `raw_file_path TEXT`
- `checksum TEXT`
- `error_message TEXT`
- `updated_at TEXT NOT NULL DEFAULT (datetime('now'))`

`status` 建议值：

- `discovered`
- `downloaded`
- `imported`
- `derived`
- `import_failed`
- `derive_failed`

唯一性建议：

- `UNIQUE(owner_id, remote_report_id)`

### `reporting_raw_reports`

原始报表文件索引表。

字段建议：

- `id TEXT PRIMARY KEY`
- `owner_id TEXT NOT NULL`
- `channel_id TEXT NOT NULL`
- `report_type_id TEXT NOT NULL`
- `remote_job_id TEXT`
- `remote_report_id TEXT NOT NULL`
- `start_date TEXT`
- `end_date TEXT`
- `file_path TEXT NOT NULL`
- `file_size INTEGER`
- `checksum TEXT`
- `downloaded_at TEXT NOT NULL`
- `imported_at TEXT`

文件落盘建议路径：

- `data/reporting/<owner_id>/<channel_id>/<report_type_id>/<start_date>_<end_date>_<remote_report_id>.csv`

### `video_reporting_daily`

频道页消费的派生日报事实表。

字段建议：

- `date TEXT NOT NULL`
- `channel_id TEXT NOT NULL`
- `video_id TEXT NOT NULL`
- `owner_id TEXT NOT NULL`
- `impressions INTEGER`
- `impressions_ctr REAL`
- `avg_view_duration_seconds REAL`
- `avg_view_percentage REAL`
- `traffic_source_share_json TEXT NOT NULL DEFAULT '{}'`
- `source_report_ids_json TEXT NOT NULL DEFAULT '[]'`
- `computed_at TEXT NOT NULL`

主键建议：

- `PRIMARY KEY(date, channel_id, video_id)`

说明：

- 该表不存公开视频元数据，只存报表派生指标。
- `traffic_source_share_json` 保存按来源聚合后的占比，例如：
  - `{"YT_SEARCH":0.31,"SUGGESTED_VIDEO":0.28,"BROWSE_FEATURES":0.17}`

### `video_reporting_latest`

可选的加速缓存表，用于频道页快速展示最近一天或最近区间汇总结果。该表不是事实源，任何时候都可根据 `video_reporting_daily` 重算。

## 指标映射

本次只接入以下五类视频级指标：

1. 展现量
2. 点击率
3. 平均观看时长
4. 平均观看百分比
5. 流量来源占比

映射策略：

- `展现量`、`点击率`
  - 来自 `channel_reach_basic_a1`
- `平均观看时长`、`平均观看百分比`
  - 来自 `channel_basic_a3`
- `流量来源占比`
  - 来自 `channel_traffic_source_a3`
  - 按 `views` 聚合为日级来源占比 JSON

实现时应把报表解析逻辑写成独立 mapper，避免把报表列名解析散落在路由层或调度层。

## 启用与历史边界规则

### 1. 业务生效边界

用户要求：

- 频道开启 Reporting 后，只从“启用当天”开始拉业务数据。

实现约束：

- 原始报表允许正常下载和索引。
- 派生写入 `video_reporting_daily` 时，过滤掉 `started_at` 之前的日期。

### 2. backfill 处理

YouTube 可能在后续补发或修订历史报表。

本地策略：

- 原始层允许保留这些文件。
- 若修订日期在 `started_at` 之后，则允许覆盖重算派生日报。
- 若修订日期早于 `started_at`，仍不进入派生事实表。

## 后端服务设计

### 1. Owner 配置服务

新增服务模块，例如：

- `server/src/services/reportingOwners.ts`

职责：

- Owner CRUD
- 敏感字段掩码与保存
- 频道绑定/解绑
- 启用/停用 Reporting
- 代理配置规范化

### 2. Reporting OAuth/HTTP 客户端工厂

新增服务模块，例如：

- `server/src/services/youtubeReportingClient.ts`

职责：

- 从 `reporting_owners` 读取 Owner 凭证
- 每次请求构造新的 HTTP client / transport
- 使用 Owner 固定 `proxy_url`
- 执行 refresh token 流程
- 返回带 access token 的请求能力

实现约束：

- 不共享全局 session
- 不缓存跨 Owner transport
- token refresh 也必须走 Owner 代理

项目当前后端是 Node/TypeScript 而不是 Python/FastAPI，因此这里不应实现为 `httpx`。对应做法是：

- Node 原生 `fetch` / `undici` + `ProxyAgent`
- 如需 SOCKS，使用现有项目已在用的 `socks-proxy-agent`
- 若后续引入 `googleapis` 或 `google-auth-library`，则需要为其 transport 层注入 Owner 专属代理，但同样不能跨 Owner 共享

### 3. 代理检测服务

新增服务模块，例如：

- `server/src/services/reportingProxyProbe.ts`

职责：

- 基于 Owner 的 `proxy_url` 执行真实出口连通性检测
- 返回：
  - `proxy_mode`
  - `egress_ip`
  - `google_oauth_ok`
  - `reporting_api_ok`
  - `message`

检测顺序：

1. 探测出口 IP
2. 请求 Google OAuth/token 相关连通性
3. 请求 Reporting API 基础连通性

所有探测结果写入 `reporting_request_logs`

### 4. Reporting 同步服务

新增服务模块，例如：

- `server/src/services/youtubeReportingSync.ts`

职责：

- 扫描启用 Owner 与启用绑定
- 查询远端 reporting jobs / reports
- 下载新增 CSV
- 写入原始文件索引
- 触发导入与派生
- 更新 `reporting_job_state`

### 5. 报表导入与派生服务

新增服务模块，例如：

- `server/src/services/youtubeReportingImport.ts`
- `server/src/services/youtubeReportingDerive.ts`

职责：

- 解析不同报表类型的 CSV
- 对齐视频 ID 与日期
- 聚合为 `video_reporting_daily`
- 构建 `traffic_source_share_json`

## 请求隔离策略

每个 Owner 的每次请求必须遵守：

- 从数据库读取当前 Owner 配置
- 规范化 `proxy_url`
- 现建 client / agent / transport
- 发请求
- 记录日志
- 请求结束后释放对象引用

不允许：

- 共享全局 OAuth client
- 共享跨 Owner token transport
- 共享跨 Owner session / keep-alive pool

这样会带来一定性能损失，但能满足“会话与网络全隔离”的核心需求。

## 幂等与状态机

### 1. 报表发现去重

使用以下维度判重：

- `owner_id`
- `remote_report_id`

若同一 `remote_report_id` 已存在：

- 已完成下载与导入：跳过下载
- 已下载但派生失败：只重跑导入/派生
- checksum 不一致：标记为修订版本，覆盖导入并重算派生

### 2. 原始层与派生层分离

- 原始文件下载成功后立即写索引
- 导入失败不删除原始文件
- 派生失败不回滚原始索引

### 3. 派生覆盖写

同一 `(date, channel_id, video_id)` 采用 upsert 方式写入，以支持：

- 重复执行同步
- 修订报表覆盖
- 单独重跑派生

## 设置页设计

在当前设置页新增完整区块：`Reporting Owners`

### 1. Owner 列表

展示字段：

- Owner 名称
- 启用状态
- 代理状态
- 绑定频道数
- 最近同步时间
- 最近 token 刷新时间
- 最近错误
- 近 24 小时请求成功率

### 2. Owner 表单

支持新增、编辑、删除：

- `name`
- `client_id`
- `client_secret`
- `refresh_token`
- `proxy_url`
- `enabled`

敏感字段展示策略：

- 加载时返回 masked placeholder
- 聚焦或点击后转为空输入
- 留空保持原值，输入新值覆盖

### 3. 频道绑定面板

每个 Owner 下展示：

- 已绑定频道列表
- 绑定/解绑操作
- 每个频道的 Reporting 开关
- `started_at`
- 手动触发该频道报表同步

### 4. 代理检测按钮

每个 Owner 支持点击检测并实时展示：

- 出口 IP
- 代理模式
- Google OAuth 连通性
- Reporting API 连通性
- 最近检测消息

### 5. 本地用度面板

用户已明确“用度”采用本地运行指标而不是 Google 侧剩余额度。

展示内容：

- 近 24 小时请求数
- 成功率
- 最近错误
- 最近 token 刷新
- 最近下载文件数
- 最近同步耗时

### 6. 请求日志面板

支持按以下条件筛选：

- Owner
- 频道
- 请求类型
- 成功/失败
- 时间范围

每条日志展示：

- 请求类型
- URL
- 状态码
- 耗时
- 代理快照
- 错误信息
- 开始/结束时间

## 频道页设计

在现有 `视频列表 / 数据洞察` 右侧新增第三个页签：`报表`

### 空态

若频道未绑定 Owner 或未开启 Reporting：

- 展示明确空态
- 文案说明“该频道尚未启用 Reporting API”
- 可提示去设置页完成绑定与开启

### 已启用态

默认展示近 28 天视图。

建议内容：

- 顶部 KPI 卡片
  - 最新日报展现量
  - 最新日报 CTR
  - 最新日报平均观看时长
  - 最新日报平均观看百分比
- 流量来源占比卡片
  - 最近 1 日或近 28 天汇总
- 每日趋势表
  - 按日展示四个核心指标
- 视频明细表
  - 视频级 5 项指标
- 报表状态信息
  - 当前 Owner
  - 最近导入时间
  - 覆盖日期范围

### 与现有数据洞察的关系

- `数据洞察` 保持当前公开数据逻辑不变
- `报表` 只展示 Reporting 口径
- 所有标题、提示文案都要显式区分“公开视频元数据”与“YouTube Reporting 报表指标”

## API 设计

### 设置页相关

建议新增：

- `GET /api/reporting/owners`
- `POST /api/reporting/owners`
- `PATCH /api/reporting/owners/:ownerId`
- `DELETE /api/reporting/owners/:ownerId`
- `POST /api/reporting/owners/:ownerId/proxy-test`
- `GET /api/reporting/owners/:ownerId/logs`
- `GET /api/reporting/owners/:ownerId/usage`
- `POST /api/reporting/owners/:ownerId/bindings`
- `PATCH /api/reporting/bindings/:bindingId`
- `DELETE /api/reporting/bindings/:bindingId`
- `POST /api/reporting/bindings/:bindingId/sync`

### 频道页相关

建议新增：

- `GET /api/channels/:id/reporting/summary`
- `GET /api/channels/:id/reporting/daily`
- `GET /api/channels/:id/reporting/videos`
- `POST /api/channels/:id/reporting/sync`

返回结构应显式包含：

- `enabled`
- `owner`
- `started_at`
- `latest_imported_at`

便于频道页决定展示空态、错误态和状态提示。

## 调度设计

### 1. 每日自动调度

复用现有 `dailySyncScheduler` 机制，在每日调度中增加 Reporting 分支：

- 扫描 `enabled owner`
- 扫描 `enabled binding`
- 为每个绑定频道入队 reporting sync job

### 2. 手动触发

支持两类手动触发：

- 设置页按 Owner / 按绑定频道触发
- 频道页“报表”页签直接触发当前频道同步

### 3. 与现有同步并存

- `daily_sync` 与 `sync_channel` 不互相替代
- Reporting job 可以独立为新任务类型，例如 `sync_reporting_channel`
- 失败不应阻塞现有公开视频同步链路

## 错误处理

### 1. token refresh 失败

- 只标记当前 Owner 降级
- 记录错误日志
- 不影响其他 Owner

### 2. 代理异常

区分以下失败类型：

- 代理不可连通
- 代理连通但 Google OAuth 失败
- 代理连通但 Reporting API 失败

这样设置页能直接提供可排查信息。

### 3. 导入失败

- 原始文件保留
- 状态机置为 `import_failed`
- 支持后续单独重跑导入/派生

### 4. 派生失败

- 原始文件保留
- 状态机置为 `derive_failed`
- 支持后续重跑派生

## 测试策略

### 后端单元测试

- Owner 配置校验
- 敏感字段掩码/覆盖保存
- 频道绑定唯一性约束
- `started_at` 日期过滤
- 报表去重逻辑
- backfill 覆盖重算
- 流量来源占比聚合
- 请求日志写入

### 后端集成测试

- Owner CRUD
- 绑定/解绑与开关
- 代理检测接口
- 手动同步接口
- 频道报表接口返回结构

### 前端测试

- 设置页 Owner 管理表单
- Owner 列表与日志区块渲染
- 频道绑定开关与手动触发按钮
- 频道页“报表”页签空态/加载态/错误态
- KPI 与视频明细渲染

### 回归测试重点

- 不破坏现有设置页 API key / Cookie 保存
- 不破坏现有频道页“数据洞察”
- 不影响现有 `daily_sync`、`sync_channel`

## 风险与控制

- 风险：Owner 凭证与日志混用现有 `settings` 表会导致维护困难
  - 控制：使用结构化表，不把 Owner 存成 JSON blob
- 风险：跨 Owner 复用 transport 导致网络隔离失效
  - 控制：把“每次请求现建 client”写成服务层硬约束
- 风险：YouTube 回补或修订报表导致派生脏数据
  - 控制：采用 upsert 与 checksum 检测，支持覆盖重算
- 风险：启用前历史数据被误写入派生表
  - 控制：在派生服务统一过滤 `started_at`
- 风险：前端把公开元数据与报表口径混淆
  - 控制：单独页签、单独接口、单独文案

## 验收标准

- 设置页可以新增、编辑、删除 Owner
- 设置页可以为 Owner 配置独立 `client_id`、`client_secret`、`refresh_token`、`proxy_url`
- 设置页可以真实检测代理有效性并展示出口 IP
- 设置页可以绑定频道并为频道开启/关闭 Reporting
- 每个 Owner 都能查看本地运行用度与请求日志
- Reporting 请求不共享 session，不跨 Owner 复用 transport
- 原始报表能按周期落盘并建立索引
- 派生日报只写入启用当天及之后的数据
- 频道页新增“报表”页签并展示 5 个视频级指标
- 现有 `YouTube Data API v3` 数据链路保持可用

## 落地建议

实施顺序建议：

1. 数据库迁移与类型定义
2. Owner CRUD、绑定与代理检测接口
3. Reporting OAuth/client 工厂与请求日志
4. 原始报表下载与索引
5. 派生日报与频道报表接口
6. 设置页后台 UI
7. 频道页“报表”页签
8. 调度、手动触发与回归测试
