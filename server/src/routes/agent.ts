import { Router, Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { getAllSettings, getDb, getSetting } from '../db.js';
import { getYoutubeApiUsageStatus } from '../services/youtubeApi.js';
import { getJobQueue } from '../services/jobQueue.js';

const router = Router();
const SUPPORTED_AGENT_ACTIONS = new Set([
  'sync_channel',
  'sync_all_due_channels',
  'create_task',
  'update_task',
  'delete_task',
  'update_task_status',
  'update_channel_schedule',
  'mark_channel_updated',
  'unmark_channel_updated',
  'update_channel_workflow_status',
  'download_channel_metadata',
  'refresh_dashboard_cache',
]);

type ViewMode = 'summary' | 'raw' | 'both';

function okPayload(date: string | null, summary: unknown, raw: unknown, view: ViewMode = 'both') {
  const payload: Record<string, unknown> = { ok: true };
  if (date) payload.date = date;
  if (view === 'summary' || view === 'both') payload.summary = summary;
  if (view === 'raw' || view === 'both') payload.raw = raw;
  return payload;
}

function fail(res: Response, status: number, code: string, message: string, retryable = false, details: Record<string, unknown> = {}) {
  res.status(status).json({
    ok: false,
    error: {
      code,
      message,
      retryable,
      details,
    },
  });
}

function parseViewMode(input: unknown): ViewMode {
  const value = String(input || '').trim().toLowerCase();
  return value === 'summary' || value === 'raw' ? value : 'both';
}

function isLoopbackAddress(raw: string | undefined | null): boolean {
  const value = String(raw || '').trim().toLowerCase();
  return value === '127.0.0.1'
    || value === '::1'
    || value === '::ffff:127.0.0.1'
    || value === 'localhost';
}

router.use((req: Request, res: Response, next) => {
  const remoteAddress = req.socket.remoteAddress;
  const forwardedFor = Array.isArray(req.headers['x-forwarded-for'])
    ? req.headers['x-forwarded-for'][0]
    : String(req.headers['x-forwarded-for'] || '').split(',')[0]?.trim();
  const ip = String(req.ip || '').trim();
  if (isLoopbackAddress(remoteAddress) || isLoopbackAddress(forwardedFor) || isLoopbackAddress(ip)) {
    next();
    return;
  }
  fail(res, 403, 'LOCALHOST_ONLY', 'Agent API is only available from localhost');
});

function todayInShanghai(): string {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date());
}

function normalizeDateParam(input: unknown, fallback = todayInShanghai()): string {
  const value = String(input || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : fallback;
}

function dateOnly(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const text = input.trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(parsed);
}

function normalizeClockTime(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const text = input.trim();
  if (!text || !/^\d{2}:\d{2}$/.test(text)) return null;
  return text;
}

function parseSyncPolicy(raw: unknown): { cadence: string; publish_days: number[]; target_publish_time: string | null } {
  const fallback = { cadence: 'manual', publish_days: [] as number[], target_publish_time: null as string | null };
  if (typeof raw !== 'string' || !raw.trim()) return fallback;
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const publishDaysSource = Array.isArray(parsed?.publish_days)
      ? parsed.publish_days
      : (Array.isArray(parsed?.days) ? parsed.days : []);
    const publish_days = [1, 2, 3, 4, 5, 6, 0].filter((day) =>
      publishDaysSource.map((item) => Number(item)).filter((item) => Number.isInteger(item) && item >= 0 && item <= 6).includes(day),
    );
    const target_publish_time = normalizeClockTime(parsed?.target_publish_time ?? parsed?.time);
    return {
      cadence: String(parsed?.cadence || parsed?.frequency || 'manual').trim() || 'manual',
      publish_days,
      target_publish_time,
    };
  } catch {
    return fallback;
  }
}

function isChannelDueToday(syncPolicyJson: unknown, workflowStatus: unknown, dateText: string): boolean {
  if (String(workflowStatus || '').trim() === 'paused') return false;
  const policy = parseSyncPolicy(syncPolicyJson);
  const weekday = new Date(`${dateText}T12:00:00Z`).getUTCDay();
  if (policy.cadence === 'daily') return true;
  if (policy.cadence === 'weekdays') return weekday >= 1 && weekday <= 5;
  if (policy.cadence === 'weekly' || policy.cadence === 'custom') return policy.publish_days.includes(weekday);
  return false;
}

function isChannelUpdatedToday(row: any, dateText: string): boolean {
  const latestVideoDate = dateOnly(row?.latest_video_published_at);
  const latestLongDate = dateOnly(row?.latest_long_published_at);
  const manualDate = dateOnly(row?.manual_updated_at);
  return latestVideoDate === dateText || latestLongDate === dateText || manualDate === dateText;
}

function resolveExistingPath(maybePath: unknown): string | null {
  if (typeof maybePath !== 'string') return null;
  const value = maybePath.trim();
  if (!value) return null;
  if (fs.existsSync(value)) return value;
  const resolved = path.resolve(value);
  return fs.existsSync(resolved) ? resolved : null;
}

function resolveAssetsRootPath(): string {
  const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');
  return path.resolve(downloadRoot, 'assets');
}

function localPathToAssetsUrl(localPath: string | null, assetsRoot: string): string | null {
  if (!localPath) return null;
  const absLocal = path.resolve(localPath);
  const relativePath = path.relative(assetsRoot, absLocal);
  if (!relativePath || relativePath.startsWith('..') || path.isAbsolute(relativePath)) return null;
  const baseUrl = `/assets/${relativePath.split(path.sep).join('/')}`;
  try {
    const stat = fs.statSync(absLocal);
    return `${baseUrl}?v=${Math.floor(stat.mtimeMs)}`;
  } catch {
    return baseUrl;
  }
}

function youtubeThumbUrl(videoId: unknown): string | null {
  const value = String(videoId || '').trim();
  return /^[A-Za-z0-9_-]{11}$/.test(value) ? `https://i.ytimg.com/vi/${value}/mqdefault.jpg` : null;
}

function hoursSince(input: unknown): number | null {
  if (typeof input !== 'string' || !input.trim()) return null;
  const parsed = new Date(input);
  if (Number.isNaN(parsed.getTime())) return null;
  return Math.max(0, Math.round(((Date.now() - parsed.getTime()) / 3600000) * 10) / 10);
}

function daysSince(input: unknown, todayDate: string): number | null {
  const target = dateOnly(input);
  if (!target) return null;
  const today = new Date(`${todayDate}T00:00:00+08:00`);
  const value = new Date(`${target}T00:00:00+08:00`);
  if (Number.isNaN(today.getTime()) || Number.isNaN(value.getTime())) return null;
  return Math.max(0, Math.floor((today.getTime() - value.getTime()) / 86400000));
}

function getTimeZoneParts(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date);
  const map: Record<string, string> = {};
  for (const part of parts) {
    if (part.type !== 'literal') map[part.type] = part.value;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
    second: Number(map.second),
  };
}

function getTimeZoneOffsetMs(date: Date, timeZone: string) {
  const parts = getTimeZoneParts(date, timeZone);
  const zonedUtcMs = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  return zonedUtcMs - date.getTime();
}

function zonedTimeToUtcDate(timeZone: string, year: number, month: number, day: number, hour = 0, minute = 0, second = 0) {
  const utcGuessMs = Date.UTC(year, month - 1, day, hour, minute, second);
  let candidate = new Date(utcGuessMs - getTimeZoneOffsetMs(new Date(utcGuessMs), timeZone));
  candidate = new Date(utcGuessMs - getTimeZoneOffsetMs(candidate, timeZone));
  return candidate;
}

function getResetCountdown() {
  const now = new Date();
  const zoneNow = getTimeZoneParts(now, 'America/Los_Angeles');
  const nextDateRef = new Date(Date.UTC(zoneNow.year, zoneNow.month - 1, zoneNow.day + 1));
  const nextMidnightUtc = zonedTimeToUtcDate(
    'America/Los_Angeles',
    nextDateRef.getUTCFullYear(),
    nextDateRef.getUTCMonth() + 1,
    nextDateRef.getUTCDate(),
    0,
    0,
    0,
  );
  const diffMs = Math.max(0, nextMidnightUtc.getTime() - now.getTime());
  const totalSeconds = Math.floor(diffMs / 1000);
  return `${String(Math.floor(totalSeconds / 3600)).padStart(2, '0')}:${String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0')}:${String(totalSeconds % 60).padStart(2, '0')}`;
}

function getSchedulerState() {
  const time = String(getSetting('daily_sync_time') || '03:00').trim() || '03:00';
  const [hourText, minuteText] = /^\d{2}:\d{2}$/.test(time) ? time.split(':') : ['03', '00'];
  const now = new Date();
  const zoneNow = getTimeZoneParts(now, 'Asia/Shanghai');
  const next = new Date(Date.UTC(zoneNow.year, zoneNow.month - 1, zoneNow.day, Number(hourText), Number(minuteText), 0));
  const nowZoned = new Date(Date.UTC(zoneNow.year, zoneNow.month - 1, zoneNow.day, zoneNow.hour, zoneNow.minute, zoneNow.second));
  if (next.getTime() <= nowZoned.getTime()) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  const nextRunLocal = `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, '0')}-${String(next.getUTCDate()).padStart(2, '0')} ${String(next.getUTCHours()).padStart(2, '0')}:${String(next.getUTCMinutes()).padStart(2, '0')}:00`;
  return {
    daily_sync_time: time,
    next_run_local: `${nextRunLocal} Asia/Shanghai`,
  };
}

type AgentAttentionItem = {
  kind: string;
  severity: 'critical' | 'high' | 'medium' | 'low';
  score: number;
  title: string;
  message: string;
  recommended_action: string;
  retryable: boolean;
  target: {
    type: 'channel' | 'task' | 'job' | 'system';
    id: string | null;
  };
  metadata?: Record<string, unknown>;
};

function sortAttentionItems(items: AgentAttentionItem[]) {
  return items
    .slice()
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return a.title.localeCompare(b.title);
    });
}

function buildAttentionItems(
  normalizedChannels: any[],
  tasks: any[],
  recentFailures: any[],
  queueCounts: { queued_count?: number; running_count?: number; failed_count?: number } | null | undefined,
  date: string,
) {
  const items: AgentAttentionItem[] = [];
  const today = todayInShanghai();
  const queuedJobs = Number(queueCounts?.queued_count || 0);
  const runningJobs = Number(queueCounts?.running_count || 0);
  const failedJobs = Number(queueCounts?.failed_count || 0);

  if (failedJobs > 0) {
    items.push({
      kind: 'queue_failure',
      severity: 'high',
      score: 95,
      title: 'Queue failures detected',
      message: `${failedJobs} failed job(s) currently recorded in the queue state.`,
      recommended_action: 'Inspect /api/agent/jobs?status=failed and retry or fix failing jobs.',
      retryable: true,
      target: { type: 'system', id: null },
      metadata: { failed_jobs: failedJobs },
    });
  }

  if (queuedJobs > 0 || runningJobs > 0) {
    items.push({
      kind: 'queue_backlog',
      severity: queuedJobs >= 5 ? 'high' : 'medium',
      score: queuedJobs >= 5 ? 78 : 58,
      title: 'Queue backlog present',
      message: `${queuedJobs} queued job(s), ${runningJobs} running job(s).`,
      recommended_action: 'Check queue throughput and make sure sync/download concurrency matches workload.',
      retryable: false,
      target: { type: 'system', id: null },
      metadata: { queued_jobs: queuedJobs, running_jobs: runningJobs },
    });
  }

  for (const failure of recentFailures.slice(0, 5)) {
    items.push({
      kind: 'job_failure',
      severity: 'high',
      score: 88,
      title: String(failure.type || 'job_failure'),
      message: String(failure.error_message || 'Job failed'),
      recommended_action: 'Inspect this failed job result/logs and retry if the error is transient.',
      retryable: true,
      target: { type: 'job', id: String(failure.job_id || '') || null },
      metadata: {
        created_at: failure.created_at || null,
        finished_at: failure.finished_at || null,
      },
    });
  }

  for (const channel of normalizedChannels) {
    if (channel.workflow_status === 'blocked') {
      items.push({
        kind: 'workflow_blocked',
        severity: 'high',
        score: 85,
        title: channel.title,
        message: 'Channel workflow is blocked.',
        recommended_action: 'Review workflow blockers and update channel workflow_status after resolution.',
        retryable: false,
        target: { type: 'channel', id: channel.channel_id },
        metadata: { workflow_status: channel.workflow_status },
      });
    }

    if (channel.today_status === 'due') {
      items.push({
        kind: 'channel_due_unupdated',
        severity: 'medium',
        score: 68,
        title: channel.title,
        message: `Channel is due on ${date} but has not been marked updated.`,
        recommended_action: 'Sync the channel or mark it updated if content was already published.',
        retryable: true,
        target: { type: 'channel', id: channel.channel_id },
        metadata: {
          date,
          update_cadence: channel.update_cadence,
          publish_days: channel.publish_days,
        },
      });
    }

    if (channel.risks.includes('stale_channel')) {
      items.push({
        kind: 'stale_channel',
        severity: 'medium',
        score: 62,
        title: channel.title,
        message: 'Latest video is older than 7 days.',
        recommended_action: 'Review the content calendar and decide whether the channel should publish or pause.',
        retryable: false,
        target: { type: 'channel', id: channel.channel_id },
        metadata: {
          latest_video_published_at: channel.latest_video?.published_at || null,
          stale_days: daysSince(channel.latest_video?.published_at, date),
        },
      });
    }

    if (channel.risks.includes('sync_stale')) {
      items.push({
        kind: 'sync_stale',
        severity: 'medium',
        score: 57,
        title: channel.title,
        message: 'Channel has not been synced in the last 24 hours.',
        recommended_action: 'Run a channel sync to refresh metadata and latest videos.',
        retryable: true,
        target: { type: 'channel', id: channel.channel_id },
        metadata: { last_sync_age_hours: channel.last_sync_age_hours },
      });
    }
  }

  for (const task of tasks) {
    const dueDate = String(task.due_date || '').trim();
    if (task.status !== 'done' && dueDate && dueDate < today) {
      items.push({
        kind: 'overdue_task',
        severity: 'medium',
        score: 64,
        title: String(task.title || 'task'),
        message: `Task is overdue from ${dueDate}.`,
        recommended_action: 'Reschedule, complete, or mark the task delayed.',
        retryable: false,
        target: { type: 'task', id: String(task.task_id || '') || null },
        metadata: {
          due_date: dueDate,
          priority: task.priority || null,
          status: task.status || null,
        },
      });
    }
  }

  return sortAttentionItems(items);
}

function buildDashboardModel(date: string) {
  const db = getDb();
  const assetsRoot = resolveAssetsRootPath();
  const channels = db.prepare(`
    SELECT
      c.*,
      latest_video.title AS latest_video_title,
      latest_video.published_at AS latest_video_published_at,
      latest_video.video_id AS latest_video_id,
      latest_video.local_thumb_path AS latest_video_local_thumb_path
    FROM channels c
    LEFT JOIN videos latest_video
      ON latest_video.video_id = (
        SELECT v2.video_id
        FROM videos v2
        WHERE v2.channel_id = c.channel_id
        ORDER BY COALESCE(v2.published_at, v2.created_at) DESC
        LIMIT 1
      )
    WHERE lower(COALESCE(c.platform, 'youtube')) = 'youtube'
    ORDER BY c.title COLLATE NOCASE ASC
  `).all() as any[];

  const tasks = db.prepare(`
    SELECT dt.*, c.title AS channel_title, c.avatar_url AS channel_avatar_url
    FROM dashboard_tasks dt
    LEFT JOIN channels c ON c.channel_id = dt.channel_id
    WHERE dt.due_date = ?
    ORDER BY
      CASE dt.priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END ASC,
      CASE dt.status WHEN 'in_progress' THEN 0 WHEN 'todo' THEN 1 WHEN 'delayed' THEN 2 ELSE 3 END ASC,
      dt.sort_order ASC,
      dt.created_at ASC
  `).all(date) as any[];

  const recentFailures = db.prepare(`
    SELECT job_id, type, status, created_at, finished_at, error_message
    FROM jobs
    WHERE status = 'failed'
    ORDER BY created_at DESC
    LIMIT 10
  `).all() as any[];

  const queueCounts = db.prepare(`
    SELECT
      SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_count,
      SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
    FROM jobs
  `).get() as any;

  const normalizedChannels = channels.map((row) => {
    const syncPolicy = parseSyncPolicy(row.sync_policy_json);
    const updatedToday = isChannelUpdatedToday(row, date);
    const dueToday = isChannelDueToday(row.sync_policy_json, row.workflow_status, date);
    const latestThumbPath = resolveExistingPath(row.latest_video_local_thumb_path);
    const latestVideoThumbnailUrl = localPathToAssetsUrl(latestThumbPath, assetsRoot) || youtubeThumbUrl(row.latest_video_id);
    const risks: string[] = [];
    const staleDays = daysSince(row.latest_video_published_at, date);
    const syncAgeHours = hoursSince(row.last_sync_at);
    if (staleDays != null && staleDays >= 7) risks.push('stale_channel');
    if (syncAgeHours != null && syncAgeHours >= 24) risks.push('sync_stale');
    if (row.workflow_status === 'blocked') risks.push('workflow_blocked');
    return {
      channel_id: String(row.channel_id || '').trim(),
      title: String(row.title || '').trim(),
      avatar_url: typeof row.avatar_url === 'string' ? row.avatar_url : null,
      workflow_status: String(row.workflow_status || 'in_progress').trim(),
      today_status: updatedToday ? 'updated' : (dueToday ? 'due' : 'optional'),
      update_cadence: syncPolicy.cadence,
      publish_days: syncPolicy.publish_days,
      target_publish_time: syncPolicy.target_publish_time,
      last_sync_at: typeof row.last_sync_at === 'string' ? row.last_sync_at : null,
      last_sync_age_hours: syncAgeHours,
      latest_video: {
        video_id: row.latest_video_id || null,
        title: row.latest_video_title || null,
        published_at: row.latest_video_published_at || null,
        thumbnail_url: latestVideoThumbnailUrl,
      },
      growth: {
        channel_view_increase_7d: row.channel_view_increase_7d ?? null,
      },
      risks,
      subscriber_count: row.subscriber_count ?? null,
      video_count: row.video_count ?? null,
      raw: row,
    };
  });

  const taskSummary = tasks.map((task) => ({
    task_id: task.task_id,
    title: task.title,
    task_name: task.task_name || null,
    priority: task.priority,
    status: task.status,
    planned_window: task.planned_start_time && task.planned_end_time ? `${task.planned_start_time}-${task.planned_end_time}` : null,
    channel_ref: task.channel_id ? { channel_id: task.channel_id, title: task.channel_title || null } : null,
    overdue_like_signal: task.status !== 'done' && task.due_date < todayInShanghai(),
  }));

  const dueTodayTotal = normalizedChannels.filter((item) => item.today_status === 'due').length;
  const updatedTodayTotal = normalizedChannels.filter((item) => item.today_status === 'updated').length;
  const completedTaskTotal = tasks.filter((task) => task.status === 'done').length;
  const attentionItems = buildAttentionItems(normalizedChannels, tasks, recentFailures, queueCounts, date);

  return {
    summary: {
      date,
      channel_total: normalizedChannels.length,
      due_today_total: dueTodayTotal,
      updated_today_total: updatedTodayTotal,
      task_total: tasks.length,
      completed_task_total: completedTaskTotal,
      progress_percent: tasks.length > 0 ? Math.round((completedTaskTotal / tasks.length) * 100) : 0,
      attention_items: attentionItems,
    },
    raw: {
      date,
      channels: normalizedChannels,
      tasks,
      queue: {
        queued_jobs: Number(queueCounts?.queued_count || 0),
        running_jobs: Number(queueCounts?.running_count || 0),
        failed_jobs: Number(queueCounts?.failed_count || 0),
      },
      attention_items: attentionItems,
      recent_failures: recentFailures,
      task_summary: taskSummary,
    },
  };
}

function buildSettingsModel() {
  const settings = getAllSettings();
  const usage = getYoutubeApiUsageStatus();
  const scheduler = getSchedulerState();
  return {
    summary: {
      api_quota: {
        used_units: usage.used_units,
        daily_limit: usage.daily_limit,
        remaining_units: usage.remaining_units,
        reset_countdown: getResetCountdown(),
      },
      scheduler,
      concurrency: {
        sync_job_concurrency: Number.parseInt(String(settings.sync_job_concurrency || '2'), 10) || 2,
        download_job_concurrency: Number.parseInt(String(settings.download_job_concurrency || '2'), 10) || 2,
      },
      has_api_key: Boolean(String(settings.youtube_api_key || '').trim() || String(settings.youtube_api_keys || '').trim()),
    },
    raw: {
      youtube_api_key_masked_preview: String(settings.youtube_api_key || '').trim() ? `${String(settings.youtube_api_key).trim().slice(0, 3)}***${String(settings.youtube_api_key).trim().slice(-4)}` : '',
      daily_sync_time: scheduler.daily_sync_time,
      scheduler,
      sync_job_concurrency: settings.sync_job_concurrency || '2',
      download_job_concurrency: settings.download_job_concurrency || '2',
      usage,
    },
  };
}

function buildJobsModel(statusFilter: string[] = []) {
  const db = getDb();
  let where = 'WHERE 1=1';
  const params: any[] = [];
  if (statusFilter.length > 0) {
    const placeholders = statusFilter.map(() => '?').join(', ');
    where += ` AND status IN (${placeholders})`;
    params.push(...statusFilter);
  }
  const rows = db.prepare(`
    SELECT *
    FROM jobs
    ${where}
    ORDER BY created_at DESC
    LIMIT 100
  `).all(...params) as any[];
  const queueCounts = db.prepare(`
    SELECT
      SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_count,
      SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
    FROM jobs
  `).get() as any;
  return {
    summary: {
      queue_counts: {
        queued_jobs: Number(queueCounts?.queued_count || 0),
        running_jobs: Number(queueCounts?.running_count || 0),
        failed_jobs: Number(queueCounts?.failed_count || 0),
      },
      recent_failures: rows.filter((row) => row.status === 'failed').slice(0, 10).map((row) => ({
        job_id: row.job_id,
        type: row.type,
        error_message: row.error_message || null,
      })),
    },
    raw: {
      jobs: rows,
    },
  };
}

function buildTaskModel(date: string, status: string | null) {
  const db = getDb();
  let where = 'WHERE dt.due_date = ?';
  const params: any[] = [date];
  if (status) {
    where += ' AND dt.status = ?';
    params.push(status);
  }
  const rows = db.prepare(`
    SELECT dt.*, c.title AS channel_title, c.avatar_url AS channel_avatar_url
    FROM dashboard_tasks dt
    LEFT JOIN channels c ON c.channel_id = dt.channel_id
    ${where}
    ORDER BY
      CASE dt.priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END ASC,
      CASE dt.status WHEN 'in_progress' THEN 0 WHEN 'todo' THEN 1 WHEN 'delayed' THEN 2 ELSE 3 END ASC,
      dt.sort_order ASC,
      dt.created_at ASC
  `).all(...params) as any[];
  return {
    summary: rows.map((task) => ({
      task_id: task.task_id,
      title: task.title,
      task_name: task.task_name || null,
      priority: task.priority,
      status: task.status,
      planned_window: task.planned_start_time && task.planned_end_time ? `${task.planned_start_time}-${task.planned_end_time}` : null,
      channel_ref: task.channel_id ? { channel_id: task.channel_id, title: task.channel_title || null } : null,
      overdue_like_signal: task.status !== 'done' && task.due_date < todayInShanghai(),
    })),
    raw: rows,
  };
}

router.get('/context', (req: Request, res: Response) => {
  const date = normalizeDateParam(req.query.date);
  const dashboard = buildDashboardModel(date);
  const jobs = buildJobsModel();
  const settings = buildSettingsModel();
  res.json(okPayload(date, {
    dashboard: dashboard.summary,
    queue_counts: jobs.summary.queue_counts,
    api_quota: settings.summary.api_quota,
    scheduler_next_run: settings.summary.scheduler.next_run_local,
    recent_failures: jobs.summary.recent_failures,
    attention_items: dashboard.summary.attention_items,
  }, {
    dashboard: dashboard.raw,
    jobs: jobs.raw,
    settings: settings.raw,
  }));
});

router.get('/dashboard', (req: Request, res: Response) => {
  const date = normalizeDateParam(req.query.date);
  const view = parseViewMode(req.query.view);
  const model = buildDashboardModel(date);
  res.json(okPayload(date, model.summary, model.raw, view));
});

router.get('/channels', (req: Request, res: Response) => {
  const date = normalizeDateParam(req.query.date);
  const view = parseViewMode(req.query.view);
  const dashboard = buildDashboardModel(date);
  const summary = dashboard.raw.channels.map((channel: any) => ({
    channel_id: channel.channel_id,
    title: channel.title,
    workflow_status: channel.workflow_status,
    today_status: channel.today_status,
    update_cadence: channel.update_cadence,
    publish_days: channel.publish_days,
    update_frequency_summary: channel.publish_days?.length ? channel.publish_days.join(',') : channel.update_cadence,
    last_sync_age_hours: channel.last_sync_age_hours,
    latest_video: channel.latest_video,
    growth: channel.growth,
    risks: channel.risks,
  }));
  const raw = dashboard.raw.channels.map((channel: any) => channel.raw);
  res.json(okPayload(date, summary, raw, view));
});

router.get('/channels/:id', (req: Request, res: Response) => {
  const db = getDb();
  const date = normalizeDateParam(req.query.date);
  const include = String(req.query.include || '').split(',').map((item) => item.trim()).filter(Boolean);
  const view = parseViewMode(req.query.view);
  const channel = db.prepare('SELECT * FROM channels WHERE channel_id = ?').get(req.params.id) as any;
  if (!channel) {
    fail(res, 404, 'NOT_FOUND', 'Channel not found');
    return;
  }
  const dashboard = buildDashboardModel(date);
  const channelSummary = dashboard.raw.channels.find((item: any) => item.channel_id === req.params.id);
  const raw: Record<string, unknown> = { channel };
  if (include.includes('videos')) {
    raw.videos = db.prepare(`
      SELECT *
      FROM videos
      WHERE channel_id = ?
      ORDER BY COALESCE(published_at, created_at) DESC
    `).all(req.params.id);
  }
  if (include.includes('analytics')) {
    raw.analytics = {
      channel_daily: db.prepare(`
        SELECT *
        FROM channel_daily
        WHERE channel_id = ?
        ORDER BY date DESC
        LIMIT 90
      `).all(req.params.id),
      video_daily: db.prepare(`
        SELECT vd.*
        FROM video_daily vd
        JOIN videos v ON v.video_id = vd.video_id
        WHERE v.channel_id = ?
        ORDER BY vd.date DESC
        LIMIT 500
      `).all(req.params.id),
    };
  }
  if (include.includes('tasks')) {
    raw.tasks = db.prepare(`
      SELECT *
      FROM dashboard_tasks
      WHERE channel_id = ?
      ORDER BY due_date DESC, updated_at DESC
    `).all(req.params.id);
  }
  res.json(okPayload(date, {
    channel_id: channelSummary?.channel_id || req.params.id,
    title: channelSummary?.title || channel.title,
    workflow_status: channelSummary?.workflow_status || channel.workflow_status,
    today_status: channelSummary?.today_status || 'optional',
    update_frequency_summary: channelSummary?.publish_days?.length ? channelSummary.publish_days.join(',') : (channelSummary?.update_cadence || 'manual'),
    last_sync_age_hours: channelSummary?.last_sync_age_hours ?? hoursSince(channel.last_sync_at),
    latest_video: channelSummary?.latest_video || null,
    growth: channelSummary?.growth || { channel_view_increase_7d: channel.channel_view_increase_7d ?? null },
    risks: channelSummary?.risks || [],
  }, raw, view));
});

router.get('/tasks', (req: Request, res: Response) => {
  const date = normalizeDateParam(req.query.date);
  const status = String(req.query.status || '').trim() || null;
  const view = parseViewMode(req.query.view);
  const model = buildTaskModel(date, status);
  res.json(okPayload(date, model.summary, model.raw, view));
});

router.get('/jobs', (req: Request, res: Response) => {
  const view = parseViewMode(req.query.view);
  const statusList = String(req.query.status || '').split(',').map((item) => item.trim()).filter(Boolean);
  const model = buildJobsModel(statusList);
  res.json(okPayload(null, model.summary, model.raw, view));
});

router.get('/jobs/:id', (req: Request, res: Response) => {
  const db = getDb();
  const job = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(req.params.id) as any;
  if (!job) {
    fail(res, 404, 'NOT_FOUND', 'Job not found');
    return;
  }
  const audit = db.prepare('SELECT * FROM agent_actions WHERE job_id = ?').get(req.params.id) as any;
  const events = db.prepare('SELECT ts, level, message FROM job_events WHERE job_id = ? ORDER BY ts ASC').all(req.params.id) as any[];
  res.json({
    ok: true,
    summary: {
      job_id: job.job_id,
      type: job.type,
      status: job.status,
      progress: job.progress,
      action: audit?.action || null,
      target_type: audit?.target_type || null,
      target_id: audit?.target_id || null,
    },
    raw: {
      job,
      audit,
      logs: events,
    },
  });
});

router.get('/jobs/:id/result', (req: Request, res: Response) => {
  const db = getDb();
  const job = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(req.params.id) as any;
  if (!job) {
    fail(res, 404, 'NOT_FOUND', 'Job not found');
    return;
  }
  const events = db.prepare('SELECT ts, level, message FROM job_events WHERE job_id = ? ORDER BY ts ASC').all(req.params.id) as any[];
  const stored = db.prepare('SELECT result_json FROM tool_job_results WHERE job_id = ?').get(req.params.id) as any;
  let parsedResult: any = null;
  if (stored?.result_json) {
    try {
      parsedResult = JSON.parse(stored.result_json);
    } catch {
      parsedResult = null;
    }
  }
  res.json({
    ok: true,
    job_id: job.job_id,
    status: job.status,
    progress: Number(job.progress || 0),
    logs: events,
    result: {
      summary: parsedResult?.result?.summary ?? parsedResult?.summary ?? null,
      raw: parsedResult?.result?.raw ?? parsedResult?.raw ?? null,
    },
    error: job.status === 'failed'
      ? {
          code: String(job.error_code || parsedResult?.error?.code || 'JOB_FAILED'),
          message: String(job.error_message || parsedResult?.error?.message || 'Job failed'),
          retryable: Boolean(parsedResult?.error?.retryable),
          details: parsedResult?.error?.details || {},
        }
      : null,
  });
});

router.get('/settings', (_req: Request, res: Response) => {
  const model = buildSettingsModel();
  res.json(okPayload(null, model.summary, model.raw));
});

router.post('/actions/:action', (req: Request, res: Response) => {
  const db = getDb();
  const action = String(req.params.action || '').trim();
  const body = req.body && typeof req.body === 'object' ? req.body as Record<string, unknown> : {};
  const requestContext = body.request_context && typeof body.request_context === 'object' && !Array.isArray(body.request_context)
    ? body.request_context as Record<string, unknown>
    : {};
  const agentName = String(requestContext.agent_name || 'local-agent').trim() || 'local-agent';
  const targetType = String(body.target_type || '').trim() || 'system';
  const targetId = body.target_id == null ? null : String(body.target_id || '').trim() || null;
  const input = body.input && typeof body.input === 'object' && !Array.isArray(body.input)
    ? body.input
    : {};
  if (!action) {
    fail(res, 400, 'INVALID_INPUT', 'action is required');
    return;
  }
  if (!SUPPORTED_AGENT_ACTIONS.has(action)) {
    fail(res, 400, 'UNSUPPORTED_ACTION', `Unsupported agent action: ${action}`);
    return;
  }

  const actionId = uuidv4();
  const jobId = uuidv4();
  const payload = {
    action,
    target_type: targetType,
    target_id: targetId,
    input,
    request_context: {
      source: 'agent',
      agent_name: agentName,
      reason: String(requestContext.reason || '').trim() || null,
    },
  };

  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'agent_action', ?, 'queued')
  `).run(jobId, JSON.stringify(payload));

  db.prepare(`
    INSERT INTO agent_actions (action_id, agent_name, action, target_type, target_id, payload_json, job_id, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
  `).run(actionId, agentName, action, targetType, targetId, JSON.stringify(payload), jobId);

  try {
    getJobQueue().processNext();
  } catch {}

  res.status(202).json({
    ok: true,
    accepted: true,
    action,
    target: {
      type: targetType,
      id: targetId,
    },
    job_id: jobId,
    queued_at: new Date().toISOString(),
  });
});

export default router;
