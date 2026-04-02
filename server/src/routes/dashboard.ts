import { Router, Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { getDb, getSetting } from '../db.js';
import { buildDueChannelAutoTasks } from './dashboardAutoTasks.js';

const router = Router();

type WorkflowStatus = 'in_progress' | 'blocked' | 'paused';
type TaskStatus = 'todo' | 'in_progress' | 'done' | 'delayed';
type TaskPriority = 'high' | 'medium' | 'low';
type TodayStatus = 'updated' | 'due' | 'optional';

const VALID_TASK_STATUSES = new Set<TaskStatus>(['todo', 'in_progress', 'done', 'delayed']);
const VALID_TASK_PRIORITIES = new Set<TaskPriority>(['high', 'medium', 'low']);

function normalizeClockTime(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const text = input.trim();
  if (!text) return null;
  if (!/^\d{2}:\d{2}$/.test(text)) return null;
  const [hours, minutes] = text.split(':').map((value) => Number.parseInt(value, 10));
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) return null;
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return null;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;
}

function resolveExistingPath(maybePath: unknown): string | null {
  if (typeof maybePath !== 'string') return null;
  const value = maybePath.trim();
  if (!value) return null;
  if (fs.existsSync(value)) return value;
  const resolved = path.resolve(value);
  if (fs.existsSync(resolved)) return resolved;
  return null;
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
  const normalized = relativePath.split(path.sep).join('/');
  return `/assets/${normalized}`;
}

function youtubeThumbUrl(videoId: unknown): string | null {
  const value = String(videoId || '').trim();
  if (!/^[A-Za-z0-9_-]{11}$/.test(value)) return null;
  return `https://i.ytimg.com/vi/${value}/mqdefault.jpg`;
}

function todayInShanghai(): string {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return formatter.format(new Date());
}

function toUtcSqliteDateTime(date: Date): string {
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

function getShanghaiDateUtcBounds(date: string): { startUtc: string; endUtc: string } {
  const start = new Date(`${date}T00:00:00+08:00`);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
  return {
    startUtc: toUtcSqliteDateTime(start),
    endUtc: toUtcSqliteDateTime(end),
  };
}

function hourInShanghai(): number {
  const formatter = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Shanghai',
    hour: '2-digit',
    hour12: false,
  });
  return Number.parseInt(formatter.format(new Date()), 10) || 0;
}

function dateOnly(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const text = input.trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return formatter.format(parsed);
}

function parseSyncPolicy(raw: unknown): {
  cadence: string;
  publish_days: number[];
  target_publish_time: string | null;
} {
  const fallback = { cadence: 'manual', publish_days: [] as number[], target_publish_time: null as string | null };
  if (typeof raw !== 'string' || !raw.trim()) return fallback;

  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const cadence = String(parsed?.cadence || parsed?.frequency || 'manual').trim() || 'manual';
    const publishDaysRaw = Array.isArray(parsed?.publish_days)
      ? parsed.publish_days
      : Array.isArray(parsed?.days)
        ? parsed.days
        : [];
    const publish_days = publishDaysRaw
      .map((item) => Number(item))
      .filter((item) => Number.isInteger(item) && item >= 0 && item <= 6);
    const targetPublishTime = typeof parsed?.target_publish_time === 'string'
      ? parsed.target_publish_time.trim()
      : (typeof parsed?.time === 'string' ? parsed.time.trim() : '');
    return {
      cadence,
      publish_days,
      target_publish_time: targetPublishTime || null,
    };
  } catch {
    return fallback;
  }
}

function isChannelDueToday(syncPolicyJson: unknown, date: string, workflowStatus: unknown): boolean {
  if (String(workflowStatus || '').trim() === 'paused') return false;
  const { cadence, publish_days } = parseSyncPolicy(syncPolicyJson);
  const parsed = new Date(`${date}T12:00:00Z`);
  const weekday = parsed.getUTCDay();

  if (cadence === 'daily') return true;
  if (cadence === 'weekdays') return weekday >= 1 && weekday <= 5;
  if (cadence === 'weekly' || cadence === 'custom') return publish_days.includes(weekday);
  return false;
}

function isChannelUpdatedToday(row: any, date: string): boolean {
  const latestAny = dateOnly(row?.latest_video_published_at);
  const latestLong = dateOnly(row?.latest_long_published_at);
  const manual = dateOnly(row?.manual_updated_at);
  return latestAny === date || latestLong === date || manual === date;
}

function daysSince(input: unknown, todayDate: string): number | null {
  const target = dateOnly(input);
  if (!target) return null;
  const today = new Date(`${todayDate}T00:00:00+08:00`);
  const value = new Date(`${target}T00:00:00+08:00`);
  if (Number.isNaN(today.getTime()) || Number.isNaN(value.getTime())) return null;
  return Math.max(0, Math.floor((today.getTime() - value.getTime()) / 86400000));
}

function priorityRank(priority: string): number {
  if (priority === 'high') return 0;
  if (priority === 'medium') return 1;
  return 2;
}

function normalizeTaskPriority(input: unknown): TaskPriority | null {
  const value = String(input || '').trim();
  if (VALID_TASK_PRIORITIES.has(value as TaskPriority)) return value as TaskPriority;
  return null;
}

function normalizeTaskStatus(input: unknown): TaskStatus | null {
  const value = String(input || '').trim();
  if (VALID_TASK_STATUSES.has(value as TaskStatus)) return value as TaskStatus;
  return null;
}

function normalizeTaskDueDate(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const value = input.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null;
}

router.get('/', (req: Request, res: Response) => {
  const db = getDb();
  const date = typeof req.query.date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(req.query.date)
    ? req.query.date
    : todayInShanghai();
  const { startUtc, endUtc } = getShanghaiDateUtcBounds(date);
  const assetsRoot = resolveAssetsRootPath();

  const channelRows = db.prepare(`
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

  const recentFailedSyncs = db.prepare(`
    SELECT job_id, type, status, created_at, error_message, payload_json
    FROM jobs
    WHERE type IN ('sync_channel', 'daily_sync')
      AND status = 'failed'
      AND created_at >= ?
      AND created_at < ?
    ORDER BY created_at DESC
    LIMIT 5
  `).all(startUtc, endUtc) as any[];

  const activeJobCounts = db.prepare(`
    SELECT
      SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
      SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_count,
      SUM(CASE WHEN status = 'failed' AND created_at >= ? AND created_at < ? THEN 1 ELSE 0 END) AS failed_count
    FROM jobs
  `).get(startUtc, endUtc) as any;

  const normalizedChannels = channelRows.map((row) => {
    const syncPolicy = parseSyncPolicy(row?.sync_policy_json);
    const dueToday = isChannelDueToday(row?.sync_policy_json, date, row?.workflow_status);
    const updatedToday = isChannelUpdatedToday(row, date);
    const todayStatus: TodayStatus = updatedToday ? 'updated' : (dueToday ? 'due' : 'optional');
    const latestLocalThumbPath = resolveExistingPath(row?.latest_video_local_thumb_path);
    const latestVideoThumbnailUrl = localPathToAssetsUrl(latestLocalThumbPath, assetsRoot) || youtubeThumbUrl(row?.latest_video_id);
    return {
      channel_id: String(row?.channel_id || '').trim(),
      title: String(row?.title || '').trim(),
      avatar_url: typeof row?.avatar_url === 'string' ? row.avatar_url : null,
      workflow_status: String(row?.workflow_status || 'in_progress').trim() as WorkflowStatus,
      positioning: typeof row?.positioning === 'string' ? row.positioning : null,
      notes: typeof row?.notes === 'string' ? row.notes : null,
      latest_video_title: typeof row?.latest_video_title === 'string' ? row.latest_video_title : null,
      latest_video_published_at: typeof row?.latest_video_published_at === 'string' ? row.latest_video_published_at : null,
      latest_video_id: typeof row?.latest_video_id === 'string' ? row.latest_video_id : null,
      latest_video_thumbnail_url: latestVideoThumbnailUrl,
      last_sync_at: typeof row?.last_sync_at === 'string' ? row.last_sync_at : null,
      subscriber_count: row?.subscriber_count ?? null,
      video_count: row?.video_count ?? null,
      today_status: todayStatus,
      due_today: dueToday,
      updated_today: updatedToday,
      target_publish_time: syncPolicy.target_publish_time,
      update_cadence: syncPolicy.cadence,
      publish_days: syncPolicy.publish_days,
    };
  });

  const totalChannels = normalizedChannels.length;
  const activeChannels = normalizedChannels.filter((item) => item.workflow_status === 'in_progress').length;
  const dueTodayCount = normalizedChannels.filter((item) => item.due_today).length;
  const updatedTodayCount = normalizedChannels.filter((item) => item.updated_today).length;
  const existingTaskRows = db.prepare(`
    SELECT *
    FROM dashboard_tasks
    WHERE due_date = ?
  `).all(date) as any[];
  const autoTasks = buildDueChannelAutoTasks(normalizedChannels, existingTaskRows, date);
  for (const task of autoTasks) {
    db.prepare(`
      INSERT INTO dashboard_tasks (
        task_id, title, task_name, channel_id, due_date, priority, status, estimate_minutes, planned_start_time, planned_end_time, notes, sort_order, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
    `).run(
      uuidv4(),
      task.title,
      task.task_name,
      task.channel_id,
      task.due_date,
      task.priority,
      task.status,
      null,
      task.planned_start_time,
      task.planned_end_time,
      task.notes,
      task.sort_order,
    );
  }

  const taskRows = db.prepare(`
    SELECT *
    FROM dashboard_tasks
    WHERE due_date = ?
    ORDER BY
      CASE priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END ASC,
      CASE status WHEN 'in_progress' THEN 0 WHEN 'todo' THEN 1 WHEN 'delayed' THEN 2 ELSE 3 END ASC,
      sort_order ASC,
      created_at ASC
  `).all(date) as any[];
  const totalTasks = taskRows.length;
  const completedTasks = taskRows.filter((item) => item.status === 'done').length;
  const progressPercent = totalTasks > 0 ? Math.round((completedTasks / totalTasks) * 100) : 0;

  const staleChannels = normalizedChannels
    .map((item) => ({
      ...item,
      stale_days: daysSince(item.latest_video_published_at, date),
    }))
    .filter((item) => item.workflow_status !== 'paused' && item.stale_days != null && item.stale_days >= 7)
    .sort((a, b) => Number(b.stale_days || 0) - Number(a.stale_days || 0))
    .slice(0, 3);

  const reminders: Array<{ level: 'warning' | 'info' | 'success'; title: string; detail: string }> = [];
  if (dueTodayCount > updatedTodayCount) {
    reminders.push({
      level: 'warning',
      title: '今日仍有频道未完成更新',
      detail: `还有 ${dueTodayCount - updatedTodayCount} 个应更频道未更。`,
    });
  }
  if (staleChannels.length > 0) {
    reminders.push({
      level: 'warning',
      title: '存在较久未更新频道',
      detail: staleChannels.map((item) => `${item.title} ${item.stale_days} 天未更新`).join('；'),
    });
  }
  if (recentFailedSyncs.length > 0) {
    reminders.push({
      level: 'warning',
      title: '最近存在同步失败',
      detail: `最近 ${recentFailedSyncs.length} 次同步失败，建议优先处理异常频道。`,
    });
  }
  if (totalTasks > 0 && hourInShanghai() >= 15 && progressPercent < 50) {
    reminders.push({
      level: 'info',
      title: '今日任务完成率偏低',
      detail: `当前完成率 ${progressPercent}%，建议先收口高优任务。`,
    });
  }
  if (reminders.length === 0) {
    reminders.push({
      level: 'success',
      title: '今日节奏正常',
      detail: '目前没有明显风险提醒，可以继续按计划推进。',
    });
  }

  const topTasks = [...taskRows]
    .sort((a, b) => {
      const priorityGap = priorityRank(String(a?.priority || 'medium')) - priorityRank(String(b?.priority || 'medium'));
      if (priorityGap !== 0) return priorityGap;
      const donePenaltyA = String(a?.status || '') === 'done' ? 1 : 0;
      const donePenaltyB = String(b?.status || '') === 'done' ? 1 : 0;
      if (donePenaltyA !== donePenaltyB) return donePenaltyA - donePenaltyB;
      return String(a?.created_at || '').localeCompare(String(b?.created_at || ''));
    })
    .slice(0, 3);

  const taskChannelMap = new Map(
    normalizedChannels.map((item) => [
      item.channel_id,
      { title: item.title, avatar_url: item.avatar_url },
    ]),
  );
  const tasks = taskRows.map((row) => ({
    task_id: row.task_id,
    title: row.title,
    task_name: typeof row.task_name === 'string' ? row.task_name : null,
    channel_id: row.channel_id,
    channel_title: row.channel_id ? (taskChannelMap.get(String(row.channel_id))?.title || null) : null,
    channel_avatar_url: row.channel_id ? (taskChannelMap.get(String(row.channel_id))?.avatar_url || null) : null,
    due_date: row.due_date,
    priority: row.priority,
    status: row.status,
    estimate_minutes: row.estimate_minutes,
    planned_start_time: typeof row.planned_start_time === 'string' ? row.planned_start_time : null,
    planned_end_time: typeof row.planned_end_time === 'string' ? row.planned_end_time : null,
    notes: row.notes,
    sort_order: row.sort_order,
    created_at: row.created_at,
    updated_at: row.updated_at,
  }));

  res.json({
    date,
    overview: {
      channel_total: totalChannels,
      active_channel_total: activeChannels,
      due_today_total: dueTodayCount,
      updated_today_total: updatedTodayCount,
      task_total: totalTasks,
      completed_task_total: completedTasks,
      progress_percent: progressPercent,
    },
    top_tasks: topTasks.map((row) => ({
      task_id: row.task_id,
      title: row.title,
      task_name: typeof row.task_name === 'string' ? row.task_name : null,
      channel_id: row.channel_id,
      channel_title: row.channel_id ? (taskChannelMap.get(String(row.channel_id))?.title || null) : null,
      channel_avatar_url: row.channel_id ? (taskChannelMap.get(String(row.channel_id))?.avatar_url || null) : null,
      priority: row.priority,
      status: row.status,
      estimate_minutes: row.estimate_minutes,
      planned_start_time: typeof row.planned_start_time === 'string' ? row.planned_start_time : null,
      planned_end_time: typeof row.planned_end_time === 'string' ? row.planned_end_time : null,
      notes: row.notes,
    })),
    tasks,
    monitoring: {
      running_jobs: Number(activeJobCounts?.running_count || 0),
      queued_jobs: Number(activeJobCounts?.queued_count || 0),
      failed_jobs: Number(activeJobCounts?.failed_count || 0),
      reminders,
      recent_failed_syncs: recentFailedSyncs.map((row) => ({
        job_id: row.job_id,
        type: row.type,
        created_at: row.created_at,
        error_message: row.error_message,
      })),
    },
    channel_overview: normalizedChannels.map((row) => ({
      channel_id: row.channel_id,
      title: row.title,
      avatar_url: row.avatar_url,
      workflow_status: row.workflow_status,
      latest_video_title: row.latest_video_title,
      latest_video_published_at: row.latest_video_published_at,
      latest_video_thumbnail_url: row.latest_video_thumbnail_url,
      last_sync_at: row.last_sync_at,
      today_status: row.today_status,
      target_publish_time: row.target_publish_time,
      update_cadence: row.update_cadence,
      publish_days: row.publish_days,
      subscriber_count: row.subscriber_count,
      video_count: row.video_count,
    })),
  });
});

router.get('/tasks', (req: Request, res: Response) => {
  const db = getDb();
  const date = typeof req.query.date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(req.query.date)
    ? req.query.date
    : todayInShanghai();

  const rows = db.prepare(`
    SELECT *
    FROM dashboard_tasks
    WHERE due_date = ?
    ORDER BY
      CASE priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END ASC,
      CASE status WHEN 'in_progress' THEN 0 WHEN 'todo' THEN 1 WHEN 'delayed' THEN 2 ELSE 3 END ASC,
      sort_order ASC,
      created_at ASC
  `).all(date);

  res.json({ data: rows, total: rows.length, page: 1, limit: rows.length });
});

router.post('/tasks', (req: Request, res: Response) => {
  const db = getDb();
  const { title, task_name, channel_id, due_date, priority, status, estimate_minutes, planned_start_time, planned_end_time, notes, sort_order } = req.body || {};
  const normalizedTitle = String(title || '').trim();
  const normalizedTaskName = String(task_name || '').trim();
  const normalizedDueDate = typeof due_date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(due_date) ? due_date : todayInShanghai();
  const normalizedPriority: TaskPriority = priority === 'high' || priority === 'low' ? priority : 'medium';
  const normalizedStatus: TaskStatus = ['todo', 'in_progress', 'done', 'delayed'].includes(String(status || ''))
    ? String(status) as TaskStatus
    : 'todo';
  const normalizedStartTime = normalizeClockTime(planned_start_time);
  const normalizedEndTime = normalizeClockTime(planned_end_time);

  if (!normalizedTitle) {
    res.status(400).json({ error: 'title is required' });
    return;
  }

  if ((normalizedStartTime && !normalizedEndTime) || (!normalizedStartTime && normalizedEndTime)) {
    res.status(400).json({ error: 'planned_start_time and planned_end_time must be provided together' });
    return;
  }
  if (normalizedStartTime && normalizedEndTime && normalizedEndTime <= normalizedStartTime) {
    res.status(400).json({ error: 'planned_end_time must be later than planned_start_time' });
    return;
  }

  const taskId = uuidv4();
  db.prepare(`
    INSERT INTO dashboard_tasks (
      task_id, title, task_name, channel_id, due_date, priority, status, estimate_minutes, planned_start_time, planned_end_time, notes, sort_order, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
  `).run(
    taskId,
    normalizedTitle,
    normalizedTaskName || null,
    channel_id ? String(channel_id).trim() : null,
    normalizedDueDate,
    normalizedPriority,
    normalizedStatus,
    estimate_minutes == null || estimate_minutes === '' ? null : Math.max(0, Number(estimate_minutes) || 0),
    normalizedStartTime,
    normalizedEndTime,
    notes ? String(notes) : null,
    Number.isFinite(Number(sort_order)) ? Number(sort_order) : 0,
  );

  const task = db.prepare('SELECT * FROM dashboard_tasks WHERE task_id = ?').get(taskId);
  res.status(201).json(task);
});

router.patch('/tasks/:id', (req: Request, res: Response) => {
  const db = getDb();
  const existing = db.prepare('SELECT * FROM dashboard_tasks WHERE task_id = ?').get(req.params.id) as any;
  if (!existing) {
    res.status(404).json({ error: 'Task not found' });
    return;
  }

  const updates: string[] = [];
  const params: any[] = [];
  const body = req.body || {};

  const assign = (field: string, value: unknown) => {
    updates.push(`${field} = ?`);
    params.push(value);
  };

  const normalizedTitle = body.title !== undefined ? String(body.title || '').trim() : String(existing.title || '').trim();
  if (!normalizedTitle) {
    res.status(400).json({ error: 'title is required' });
    return;
  }

  const normalizedTaskName = body.task_name !== undefined
    ? String(body.task_name || '').trim() || null
    : (typeof existing.task_name === 'string' && existing.task_name.trim() ? existing.task_name.trim() : null);
  const normalizedChannelId = body.channel_id !== undefined
    ? (body.channel_id ? String(body.channel_id).trim() : null)
    : (existing.channel_id ? String(existing.channel_id).trim() : null);

  let normalizedDueDate = typeof existing.due_date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(existing.due_date)
    ? existing.due_date
    : todayInShanghai();
  if (body.due_date !== undefined) {
    const nextDueDate = body.due_date ? normalizeTaskDueDate(body.due_date) : todayInShanghai();
    if (!nextDueDate) {
      res.status(400).json({ error: 'due_date must be in YYYY-MM-DD format' });
      return;
    }
    normalizedDueDate = nextDueDate;
  }

  let normalizedPriority = normalizeTaskPriority(existing.priority) || 'medium';
  if (body.priority !== undefined) {
    const nextPriority = normalizeTaskPriority(body.priority);
    if (!nextPriority) {
      res.status(400).json({ error: 'priority must be one of high, medium, low' });
      return;
    }
    normalizedPriority = nextPriority;
  }

  let normalizedStatus = normalizeTaskStatus(existing.status) || 'todo';
  if (body.status !== undefined) {
    const nextStatus = normalizeTaskStatus(body.status);
    if (!nextStatus) {
      res.status(400).json({ error: 'status must be one of todo, in_progress, done, delayed' });
      return;
    }
    normalizedStatus = nextStatus;
  }

  const hasStartPatch = Object.prototype.hasOwnProperty.call(body, 'planned_start_time');
  const hasEndPatch = Object.prototype.hasOwnProperty.call(body, 'planned_end_time');
  const rawStartText = hasStartPatch ? String(body.planned_start_time ?? '').trim() : '';
  const rawEndText = hasEndPatch ? String(body.planned_end_time ?? '').trim() : '';
  const parsedStart = hasStartPatch ? normalizeClockTime(body.planned_start_time) : null;
  const parsedEnd = hasEndPatch ? normalizeClockTime(body.planned_end_time) : null;

  if (hasStartPatch && rawStartText && !parsedStart) {
    res.status(400).json({ error: 'planned_start_time must be in HH:MM format' });
    return;
  }
  if (hasEndPatch && rawEndText && !parsedEnd) {
    res.status(400).json({ error: 'planned_end_time must be in HH:MM format' });
    return;
  }

  let normalizedStartTime = normalizeClockTime(existing.planned_start_time);
  let normalizedEndTime = normalizeClockTime(existing.planned_end_time);
  if (hasStartPatch) normalizedStartTime = parsedStart;
  if (hasEndPatch) normalizedEndTime = parsedEnd;
  if (hasStartPatch && !rawStartText && !hasEndPatch) normalizedEndTime = null;
  if (hasEndPatch && !rawEndText && !hasStartPatch) normalizedStartTime = null;

  if ((normalizedStartTime && !normalizedEndTime) || (!normalizedStartTime && normalizedEndTime)) {
    res.status(400).json({ error: 'planned_start_time and planned_end_time must be provided together' });
    return;
  }
  if (normalizedStartTime && normalizedEndTime && normalizedEndTime <= normalizedStartTime) {
    res.status(400).json({ error: 'planned_end_time must be later than planned_start_time' });
    return;
  }

  const normalizedNotes = body.notes !== undefined
    ? (body.notes ? String(body.notes) : null)
    : (existing.notes ?? null);
  const normalizedEstimateMinutes = body.estimate_minutes !== undefined
    ? (body.estimate_minutes == null || body.estimate_minutes === '' ? null : Math.max(0, Number(body.estimate_minutes) || 0))
    : existing.estimate_minutes;
  const normalizedSortOrder = body.sort_order !== undefined
    ? (Number.isFinite(Number(body.sort_order)) ? Number(body.sort_order) : 0)
    : existing.sort_order;

  if (body.title !== undefined) assign('title', normalizedTitle);
  if (body.task_name !== undefined) assign('task_name', normalizedTaskName);
  if (body.channel_id !== undefined) assign('channel_id', normalizedChannelId);
  if (body.due_date !== undefined) assign('due_date', normalizedDueDate);
  if (body.priority !== undefined) assign('priority', normalizedPriority);
  if (body.status !== undefined) assign('status', normalizedStatus);
  if (body.estimate_minutes !== undefined) assign('estimate_minutes', body.estimate_minutes == null || body.estimate_minutes === '' ? null : Math.max(0, Number(body.estimate_minutes) || 0));
  if (hasStartPatch || hasEndPatch) {
    assign('planned_start_time', normalizedStartTime);
    assign('planned_end_time', normalizedEndTime);
  }
  if (body.notes !== undefined) assign('notes', normalizedNotes);
  if (body.sort_order !== undefined) assign('sort_order', normalizedSortOrder);

  if (updates.length === 0) {
    res.status(400).json({ error: 'No fields to update' });
    return;
  }

  updates.push("updated_at = datetime('now')");
  params.push(req.params.id);
  const result = db.prepare(`UPDATE dashboard_tasks SET ${updates.join(', ')} WHERE task_id = ?`).run(...params);
  if (result.changes === 0) {
    res.status(404).json({ error: 'Task not found' });
    return;
  }

  const task = db.prepare('SELECT * FROM dashboard_tasks WHERE task_id = ?').get(req.params.id);
  res.json(task);
});

router.delete('/tasks/:id', (req: Request, res: Response) => {
  const db = getDb();
  const result = db.prepare('DELETE FROM dashboard_tasks WHERE task_id = ?').run(req.params.id);
  if (result.changes === 0) {
    res.status(404).json({ error: 'Task not found' });
    return;
  }
  res.json({ success: true });
});

export default router;
