export interface AppSettingsResponse {
  youtube_api_key?: string
  youtube_api_key_masked_preview?: string
  youtube_api_keys?: string
  youtube_api_key_proxies?: string
  download_root?: string
  daily_sync_time?: string
  sync_job_concurrency?: string
  download_job_concurrency?: string
}

export interface AppSettings {
  youtubeApiKey: string
}

export interface YoutubeApiUsage {
  enabled?: boolean
  has_api_key?: boolean
  date?: string
  key_count: number
  per_key_daily_limit: number
  used_units: number
  daily_limit: number
  remaining_units: number
  total_calls?: number
  warning_threshold_percent?: number
  warning_threshold_units?: number
  auto_rotate_enabled?: boolean
}

export interface ApiChannel {
  channel_id: string
  platform?: string | null
  title: string
  handle?: string | null
  source_url?: string | null
  avatar_url?: string | null
  tags_json?: string | null
  subscriber_count?: number | null
  video_count?: number | null
  view_count?: number | null
  channel_view_increase_7d?: number | null
  channel_view_growth_series_7d_json?: string | null
  last_sync_at?: string | null
  latest_short_published_at?: string | null
  latest_long_published_at?: string | null
  workflow_status?: 'in_progress' | 'blocked' | 'paused' | null
  positioning?: string | null
  notes?: string | null
  manual_updated_at?: string | null
  sync_policy_json?: string | null
  created_at?: string | null
}

export interface YoutubeChannelStats {
  subscriberCount: number | null
  viewCount: number | null
  videoCount: number | null
}

export interface YoutubeVideo {
  id: string
  title: string
  description: string
  publishedAt: string
  publishedAtMs: number
  thumbnailUrl: string
  duration: string
  durationSeconds: number
  viewCount: number | null
  likeCount: number | null
  commentCount: number | null
  isShort: boolean
}

export interface YoutubeChannelSnapshot {
  id: string
  title: string
  handle: string
  description: string
  customUrl: string
  thumbnailUrl: string
  publishedAt: string
  uploadsPlaylistId: string
  stats: YoutubeChannelStats
  recentVideos: YoutubeVideo[]
}

export interface StoredChannel {
  id: string
  sourceInput: string
  createdAt: string
  updatedAt: string
  tags: string[]
  snapshot: YoutubeChannelSnapshot
}

export interface ApiVideoGrowthPoint {
  date: string
  view_count?: number | null
}

export interface ApiVideo {
  video_id: string
  channel_id: string
  platform?: string | null
  title: string
  description?: string | null
  webpage_url?: string | null
  published_at?: string | null
  duration_sec?: number | null
  view_count?: number | null
  like_count?: number | null
  comment_count?: number | null
  collect_count?: number | null
  share_count?: number | null
  content_type?: string | null
  content_type_source?: string | null
  availability_status?: string | null
  download_status?: string | null
  local_meta_path?: string | null
  local_thumb_path?: string | null
  local_thumb_url?: string | null
  local_video_path?: string | null
  latest_views?: number | null
  latest_likes?: number | null
  latest_comments?: number | null
  views_change_7d?: number | null
  views_change_28d?: number | null
  daily_view_increase?: number | null
  growth_series_7d?: ApiVideoGrowthPoint[]
  channel_title?: string | null
  channel_tags_json?: string | null
}

export interface ApiJob {
  job_id: string
  type: string
  payload_json?: string | null
  status: string
  progress: number
  created_at: string
  started_at?: string | null
  finished_at?: string | null
  error_code?: string | null
  error_message?: string | null
}

export interface AnalyticsKpi {
  views: number | null
  subs: number | null
  uploads: number
  latest_snapshot_date?: string | null
  earliest_snapshot_date?: string | null
}

export interface AnalyticsDailyRow {
  date: string
  subs_total: number | null
  subs_change: number | null
  views_total: number | null
  views_change: number | null
}

export interface DashboardTask {
  task_id: string
  title: string
  task_name?: string | null
  channel_id?: string | null
  channel_title?: string | null
  channel_avatar_url?: string | null
  due_date: string
  priority: 'high' | 'medium' | 'low'
  status: 'todo' | 'in_progress' | 'done' | 'delayed'
  estimate_minutes?: number | null
  planned_start_time?: string | null
  planned_end_time?: string | null
  notes?: string | null
  sort_order?: number | null
  created_at?: string
  updated_at?: string
}

export interface DashboardReminder {
  level: 'warning' | 'info' | 'success'
  title: string
  detail: string
}

export interface DashboardChannelOverview {
  channel_id: string
  title: string
  avatar_url?: string | null
  workflow_status: 'in_progress' | 'blocked' | 'paused'
  latest_video_title?: string | null
  latest_video_published_at?: string | null
  latest_video_thumbnail_url?: string | null
  last_sync_at?: string | null
  today_status: 'updated' | 'due' | 'optional'
  target_publish_time?: string | null
  update_cadence?: string | null
  publish_days?: number[] | null
  subscriber_count?: number | null
  video_count?: number | null
}

export interface DashboardSummary {
  date: string
  overview: {
    channel_total: number
    active_channel_total: number
    due_today_total: number
    updated_today_total: number
    task_total: number
    completed_task_total: number
    progress_percent: number
  }
  top_tasks: DashboardTask[]
  tasks: DashboardTask[]
  monitoring: {
    running_jobs: number
    queued_jobs: number
    failed_jobs: number
    reminders: DashboardReminder[]
    recent_failed_syncs: Array<{
      job_id: string
      type: string
      created_at?: string | null
      error_message?: string | null
    }>
  }
  channel_overview: DashboardChannelOverview[]
}
