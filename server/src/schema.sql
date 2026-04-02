-- ViralRadar SQLite Schema

CREATE TABLE IF NOT EXISTS channels (
  channel_id TEXT PRIMARY KEY,
  platform TEXT NOT NULL DEFAULT 'youtube',
  title TEXT NOT NULL,
  handle TEXT,
  source_url TEXT,
  avatar_url TEXT,
  country TEXT,
  language TEXT,
  tags_json TEXT NOT NULL DEFAULT '[]',
  favorite INTEGER NOT NULL DEFAULT 0,
  priority TEXT NOT NULL DEFAULT 'normal',
  sync_policy_json TEXT NOT NULL DEFAULT '{}',
  last_sync_at TEXT,
  new_video_badge_count INTEGER NOT NULL DEFAULT 0,
  new_video_badge_at TEXT,
  monitor_status TEXT NOT NULL DEFAULT 'ok',
  monitor_reason TEXT,
  monitor_checked_at TEXT,
  api_last_sync_at TEXT,
  subscriber_count INTEGER,
  video_count INTEGER,
  view_count INTEGER,
  channel_view_increase_7d INTEGER,
  channel_view_growth_series_7d_json TEXT NOT NULL DEFAULT '[]',
  channel_growth_computed_at TEXT,
  workflow_status TEXT NOT NULL DEFAULT 'in_progress',
  positioning TEXT,
  notes TEXT,
  manual_updated_at TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_channels_title ON channels(title);
CREATE INDEX IF NOT EXISTS idx_channels_handle ON channels(handle);
CREATE INDEX IF NOT EXISTS idx_channels_platform ON channels(platform);

CREATE TABLE IF NOT EXISTS channel_daily (
  date TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  subscriber_count INTEGER,
  view_count_total INTEGER,
  video_count INTEGER,
  PRIMARY KEY (date, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_daily_channel_date ON channel_daily(channel_id, date);

CREATE TABLE IF NOT EXISTS videos (
  video_id TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL,
  platform TEXT NOT NULL DEFAULT 'youtube',
  title TEXT NOT NULL,
  description TEXT,
  uploader TEXT,
  webpage_url TEXT,
  published_at TEXT,
  duration_sec INTEGER,
  view_count INTEGER,
  like_count INTEGER,
  comment_count INTEGER,
  collect_count INTEGER,
  share_count INTEGER,
  content_type TEXT NOT NULL DEFAULT 'long',
  content_type_source TEXT,
  availability_status TEXT NOT NULL DEFAULT 'available',
  unavailable_reason TEXT,
  unavailable_at TEXT,
  tags_json TEXT NOT NULL DEFAULT '[]',
  favorite INTEGER NOT NULL DEFAULT 0,
  download_status TEXT NOT NULL DEFAULT 'none',
  local_meta_path TEXT,
  local_thumb_path TEXT,
  local_subtitle_paths TEXT NOT NULL DEFAULT '[]',
  local_video_path TEXT,
  file_hash TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_videos_channel_published ON videos(channel_id, published_at);
CREATE INDEX IF NOT EXISTS idx_videos_platform ON videos(platform);
CREATE INDEX IF NOT EXISTS idx_videos_title ON videos(title);
CREATE INDEX IF NOT EXISTS idx_videos_content_type ON videos(content_type);
CREATE INDEX IF NOT EXISTS idx_videos_availability ON videos(availability_status);
CREATE INDEX IF NOT EXISTS idx_videos_download_status ON videos(download_status);

CREATE TABLE IF NOT EXISTS video_daily (
  date TEXT NOT NULL,
  video_id TEXT NOT NULL,
  view_count INTEGER,
  like_count INTEGER,
  comment_count INTEGER,
  PRIMARY KEY (date, video_id)
);

CREATE INDEX IF NOT EXISTS idx_video_daily_video_date ON video_daily(video_id, date);

CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'queued',
  progress INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  started_at TEXT,
  finished_at TEXT,
  error_code TEXT,
  error_message TEXT,
  parent_job_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);

CREATE TABLE IF NOT EXISTS job_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  level TEXT NOT NULL DEFAULT 'info',
  message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_ts ON job_events(job_id, ts);

CREATE TABLE IF NOT EXISTS availability_log (
  date TEXT NOT NULL,
  video_id TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  raw_message TEXT,
  PRIMARY KEY (date, video_id)
);

CREATE INDEX IF NOT EXISTS idx_availability_video_date ON availability_log(video_id, date);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dashboard_tasks (
  task_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  task_name TEXT,
  channel_id TEXT,
  due_date TEXT NOT NULL,
  priority TEXT NOT NULL DEFAULT 'medium',
  status TEXT NOT NULL DEFAULT 'todo',
  estimate_minutes INTEGER,
  planned_start_time TEXT,
  planned_end_time TEXT,
  notes TEXT,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dashboard_tasks_due_date ON dashboard_tasks(due_date);
CREATE INDEX IF NOT EXISTS idx_dashboard_tasks_status ON dashboard_tasks(status);
CREATE INDEX IF NOT EXISTS idx_dashboard_tasks_channel ON dashboard_tasks(channel_id);

CREATE TABLE IF NOT EXISTS tool_job_results (
  job_id TEXT PRIMARY KEY,
  result_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tool_job_results_created_at ON tool_job_results(created_at);

CREATE TABLE IF NOT EXISTS agent_actions (
  action_id TEXT PRIMARY KEY,
  agent_name TEXT NOT NULL,
  action TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}',
  job_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_actions_job_id ON agent_actions(job_id);
CREATE INDEX IF NOT EXISTS idx_agent_actions_created_at ON agent_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_actions_status ON agent_actions(status);

CREATE TABLE IF NOT EXISTS research_channels (
  channel_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  handle TEXT,
  avatar_url TEXT,
  subscriber_count INTEGER,
  video_count INTEGER,
  view_count INTEGER,
  first_video_published_at TEXT,
  tags_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_research_channels_title ON research_channels(title);
CREATE INDEX IF NOT EXISTS idx_research_channels_handle ON research_channels(handle);

CREATE TABLE IF NOT EXISTS research_channel_daily (
  date TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  subscriber_count INTEGER,
  view_count INTEGER,
  PRIMARY KEY (date, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_research_channel_daily_channel_date
  ON research_channel_daily(channel_id, date);

CREATE TABLE IF NOT EXISTS hit_videos (
  video_id TEXT PRIMARY KEY,
  channel_id TEXT,
  channel_title TEXT,
  platform TEXT NOT NULL DEFAULT 'Other',
  title TEXT NOT NULL,
  description TEXT,
  webpage_url TEXT,
  published_at TEXT,
  duration_sec INTEGER,
  view_count INTEGER,
  like_count INTEGER,
  comment_count INTEGER,
  categories_json TEXT NOT NULL DEFAULT '[]',
  tags_json TEXT NOT NULL DEFAULT '[]',
  local_meta_path TEXT,
  local_thumb_path TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_hit_videos_title ON hit_videos(title);
CREATE INDEX IF NOT EXISTS idx_hit_videos_channel ON hit_videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_hit_videos_published ON hit_videos(published_at);

CREATE TABLE IF NOT EXISTS hit_video_daily (
  date TEXT NOT NULL,
  video_id TEXT NOT NULL,
  view_count INTEGER,
  like_count INTEGER,
  comment_count INTEGER,
  collect_count INTEGER,
  share_count INTEGER,
  PRIMARY KEY (date, video_id)
);

CREATE INDEX IF NOT EXISTS idx_hit_video_daily_video_date
  ON hit_video_daily(video_id, date);

CREATE TABLE IF NOT EXISTS channel_invalid_archive (
  channel_id TEXT PRIMARY KEY,
  title TEXT,
  handle TEXT,
  first_invalid_at TEXT NOT NULL,
  last_invalid_at TEXT NOT NULL,
  first_reason TEXT,
  last_reason TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  resolved_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_channel_invalid_archive_status ON channel_invalid_archive(status);
CREATE INDEX IF NOT EXISTS idx_channel_invalid_archive_first_at ON channel_invalid_archive(first_invalid_at);

CREATE TABLE IF NOT EXISTS channel_invalid_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id TEXT NOT NULL,
  title TEXT,
  handle TEXT,
  detected_at TEXT NOT NULL DEFAULT (datetime('now')),
  reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_channel_invalid_events_channel_at
  ON channel_invalid_events(channel_id, detected_at);

CREATE TABLE IF NOT EXISTS video_unavailable_archive (
  video_id TEXT PRIMARY KEY,
  channel_id TEXT,
  title TEXT,
  webpage_url TEXT,
  first_unavailable_at TEXT NOT NULL,
  last_unavailable_at TEXT NOT NULL,
  first_reason TEXT,
  last_reason TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  resolved_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_video_unavailable_archive_status ON video_unavailable_archive(status);
CREATE INDEX IF NOT EXISTS idx_video_unavailable_archive_first_at ON video_unavailable_archive(first_unavailable_at);

CREATE TABLE IF NOT EXISTS video_unavailable_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  video_id TEXT NOT NULL,
  channel_id TEXT,
  title TEXT,
  detected_at TEXT NOT NULL DEFAULT (datetime('now')),
  reason TEXT,
  raw_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_video_unavailable_events_video_at
  ON video_unavailable_events(video_id, detected_at);

CREATE TABLE IF NOT EXISTS reporting_owners (
  owner_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  client_id TEXT NOT NULL,
  client_secret TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  proxy_url TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  reporting_enabled INTEGER NOT NULL DEFAULT 1,
  started_at TEXT,
  last_token_refresh_at TEXT,
  last_sync_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_reporting_owners_enabled
  ON reporting_owners(enabled, reporting_enabled);

CREATE TABLE IF NOT EXISTS reporting_owner_channel_bindings (
  id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  reporting_enabled INTEGER NOT NULL DEFAULT 1,
  started_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY(owner_id) REFERENCES reporting_owners(owner_id) ON DELETE CASCADE,
  FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE,
  UNIQUE(channel_id)
);

CREATE INDEX IF NOT EXISTS idx_reporting_owner_channel_bindings_owner_enabled
  ON reporting_owner_channel_bindings(owner_id, reporting_enabled);

CREATE TABLE IF NOT EXISTS reporting_request_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id TEXT,
  channel_id TEXT,
  request_kind TEXT NOT NULL,
  request_url TEXT,
  proxy_url_snapshot TEXT,
  status_code INTEGER,
  success INTEGER NOT NULL DEFAULT 0,
  error_code TEXT,
  error_message TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  duration_ms INTEGER,
  response_meta_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY(owner_id) REFERENCES reporting_owners(owner_id) ON DELETE SET NULL,
  FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_reporting_request_logs_owner_started
  ON reporting_request_logs(owner_id, started_at DESC);

CREATE TABLE IF NOT EXISTS reporting_job_state (
  id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  report_type_id TEXT NOT NULL,
  remote_job_id TEXT,
  remote_report_id TEXT,
  report_start_date TEXT,
  report_end_date TEXT,
  discovered_at TEXT,
  downloaded_at TEXT,
  imported_at TEXT,
  derived_at TEXT,
  status TEXT NOT NULL,
  raw_file_path TEXT,
  checksum TEXT,
  error_message TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY(owner_id) REFERENCES reporting_owners(owner_id) ON DELETE CASCADE,
  FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE,
  UNIQUE(owner_id, remote_report_id)
);

CREATE INDEX IF NOT EXISTS idx_reporting_job_state_owner_report
  ON reporting_job_state(owner_id, remote_report_id);

CREATE TABLE IF NOT EXISTS reporting_raw_reports (
  id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  report_type_id TEXT NOT NULL,
  remote_job_id TEXT,
  remote_report_id TEXT NOT NULL,
  start_date TEXT,
  end_date TEXT,
  file_path TEXT NOT NULL,
  file_size INTEGER,
  checksum TEXT,
  downloaded_at TEXT NOT NULL,
  imported_at TEXT,
  FOREIGN KEY(owner_id) REFERENCES reporting_owners(owner_id) ON DELETE CASCADE,
  FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_reporting_raw_reports_owner_channel_dates
  ON reporting_raw_reports(owner_id, channel_id, start_date, end_date);

CREATE TABLE IF NOT EXISTS video_reporting_daily (
  date TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  video_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  impressions INTEGER,
  impressions_ctr REAL,
  avg_view_duration_seconds REAL,
  avg_view_percentage REAL,
  traffic_source_share_json TEXT NOT NULL DEFAULT '{}',
  source_report_ids_json TEXT NOT NULL DEFAULT '[]',
  computed_at TEXT NOT NULL,
  PRIMARY KEY (date, channel_id, video_id),
  FOREIGN KEY(channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE,
  FOREIGN KEY(owner_id) REFERENCES reporting_owners(owner_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_video_reporting_daily_channel_date
  ON video_reporting_daily(channel_id, date);
