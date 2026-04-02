import { Router, Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { getDb, getSetting } from '../db.js';
import { v4 as uuidv4 } from 'uuid';
import { enqueueDailySyncJob } from '../services/dailySyncScheduler.js';

const router = Router();

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
  const baseUrl = `/assets/${normalized}`;
  try {
    const stat = fs.statSync(absLocal);
    return `${baseUrl}?v=${Math.floor(stat.mtimeMs)}`;
  } catch {
    return baseUrl;
  }
}

function parseYoutubeVideoId(input: unknown): string | null {
  const value = String(input || '').trim();
  if (!value) return null;
  if (/^[A-Za-z0-9_-]{11}$/.test(value)) return value;
  if (value.startsWith('youtube__')) {
    const stripped = value.slice('youtube__'.length);
    if (/^[A-Za-z0-9_-]{11}$/.test(stripped)) return stripped;
  }

  let normalized = value;
  if (!/^https?:\/\//i.test(normalized) && normalized.startsWith('www.')) normalized = `https://${normalized}`;
  if (!/^https?:\/\//i.test(normalized)) return null;

  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.replace(/^www\./i, '').toLowerCase();
    if (host.endsWith('youtube.com')) {
      if (parsed.pathname === '/watch') {
        const id = parsed.searchParams.get('v') || '';
        return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
      }
      if (parsed.pathname.startsWith('/shorts/')) {
        const id = parsed.pathname.split('/')[2] || '';
        return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
      }
      if (parsed.pathname.startsWith('/live/')) {
        const id = parsed.pathname.split('/')[2] || '';
        return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
      }
      return null;
    }
    if (host === 'youtu.be') {
      const id = parsed.pathname.replace(/^\/+/, '').split('/')[0] || '';
      return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
    }
    return null;
  } catch {
    return null;
  }
}

function toStringArray(input: unknown): string[] {
  if (!Array.isArray(input)) return [];
  return Array.from(
    new Set(
      input
        .map((item) => String(item || '').trim())
        .filter(Boolean),
    ),
  );
}

// POST /api/sync/daily
router.post('/daily', async (_req: Request, res: Response) => {
  const result = enqueueDailySyncJob('manual');
  res.json({ ...result, message: 'Daily sync job created' });
});

// POST /api/sync/availability-check
router.post('/availability-check', async (_req: Request, res: Response) => {
  const db = getDb();
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'availability_check', '{}', 'queued')
  `).run(jobId);

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  res.json({ job_id: jobId, status: 'queued', message: 'Availability check job created' });
});

// POST /api/sync/metadata-repair
router.post('/metadata-repair', async (req: Request, res: Response) => {
  const db = getDb();
  const payload =
    req.body && typeof req.body === 'object'
      ? req.body
      : {};
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'metadata_repair', ?, 'queued')
  `).run(jobId, JSON.stringify(payload || {}));

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  res.json({ job_id: jobId, status: 'queued', message: 'Metadata repair job created' });
});

// GET /api/sync/history
router.get('/history', (req: Request, res: Response) => {
  const db = getDb();
  const { limit = '20' } = req.query;
  const limitNum = Math.min(100, parseInt(limit as string, 10) || 20);

  const rows = db.prepare(`
    SELECT job_id, type, status, created_at as started_at, finished_at,
      payload_json, error_message
    FROM jobs
    WHERE type IN ('daily_sync', 'availability_check', 'metadata_repair', 'sync_channel')
    ORDER BY created_at DESC
    LIMIT ?
  `).all(limitNum);

  res.json({ data: rows });
});

// GET /api/sync/unavailable
router.get('/unavailable', (req: Request, res: Response) => {
  const db = getDb();
  const { reason, page = '1', limit = '50' } = req.query;
  const pageNum = Math.max(1, parseInt(page as string, 10));
  const limitNum = Math.min(200, parseInt(limit as string, 10) || 50);
  const offset = (pageNum - 1) * limitNum;

  let where = "WHERE v.availability_status = 'unavailable'";
  const params: any[] = [];

  if (reason) {
    where += ' AND v.unavailable_reason = ?';
    params.push(reason);
  }

  const total = (db.prepare(`SELECT COUNT(*) as count FROM videos v ${where}`).get(...params) as any).count;
  const rows = db.prepare(`
    SELECT v.video_id, v.title, v.channel_id, v.unavailable_reason, v.unavailable_at,
      c.title as channel_title
    FROM videos v
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    ${where}
    ORDER BY v.unavailable_at DESC
    LIMIT ? OFFSET ?
  `).all(...params, limitNum, offset);

  res.json({ data: rows, total, page: pageNum, limit: limitNum });
});

// GET /api/sync/deleted-channels
router.get('/deleted-channels', (req: Request, res: Response) => {
  const db = getDb();
  const { status = 'all', page = '1', limit = '200' } = req.query;
  const pageNum = Math.max(1, parseInt(page as string, 10));
  const limitNum = Math.min(500, parseInt(limit as string, 10) || 200);
  const offset = (pageNum - 1) * limitNum;

  let where = 'WHERE 1=1';
  const params: any[] = [];
  if (status === 'active' || status === 'resolved') {
    where += ' AND a.status = ?';
    params.push(status);
  }

  const total = Number((db.prepare(`
    SELECT COUNT(*) AS count
    FROM channel_invalid_archive a
    ${where}
  `).get(...params) as any)?.count || 0);

  const rows = db.prepare(`
    SELECT
      a.channel_id,
      a.title,
      a.handle,
      a.first_invalid_at,
      a.last_invalid_at,
      a.first_reason,
      a.last_reason,
      a.status,
      a.resolved_at,
      c.avatar_url,
      c.monitor_status,
      c.monitor_reason,
      c.last_sync_at
    FROM channel_invalid_archive a
    LEFT JOIN channels c ON c.channel_id = a.channel_id
    ${where}
    ORDER BY datetime(a.first_invalid_at) DESC
    LIMIT ? OFFSET ?
  `).all(...params, limitNum, offset);

  res.json({ data: rows, total, page: pageNum, limit: limitNum });
});

// POST /api/sync/deleted-channels/delete
router.post('/deleted-channels/delete', (req: Request, res: Response) => {
  const db = getDb();
  const channelIds = toStringArray(req.body?.channel_ids);
  if (channelIds.length === 0) {
    res.status(400).json({ error: 'channel_ids is required' });
    return;
  }

  const placeholders = channelIds.map(() => '?').join(', ');
  const deletedArchive = db.prepare(`
    DELETE FROM channel_invalid_archive
    WHERE channel_id IN (${placeholders})
  `).run(...channelIds).changes || 0;

  res.json({
    ok: true,
    deleted_archive: Number(deletedArchive),
    deleted_events: 0,
  });
});

// GET /api/sync/deleted-videos
router.get('/deleted-videos', (req: Request, res: Response) => {
  const db = getDb();
  const { status = 'all', reason, page = '1', limit = '500' } = req.query;
  const pageNum = Math.max(1, parseInt(page as string, 10));
  const limitNum = Math.min(1000, parseInt(limit as string, 10) || 500);
  const offset = (pageNum - 1) * limitNum;

  let where = 'WHERE 1=1';
  const params: any[] = [];
  if (status === 'active' || status === 'resolved') {
    where += ' AND va.status = ?';
    params.push(status);
  }
  if (reason) {
    where += ' AND va.last_reason = ?';
    params.push(String(reason));
  }

  const total = Number((db.prepare(`
    SELECT COUNT(*) AS count
    FROM video_unavailable_archive va
    ${where}
  `).get(...params) as any)?.count || 0);

  const rows = db.prepare(`
    SELECT
      va.video_id,
      va.channel_id,
      va.title,
      va.webpage_url,
      va.first_unavailable_at,
      va.last_unavailable_at,
      va.first_reason,
      va.last_reason,
      va.status,
      va.resolved_at,
      c.title AS channel_title,
      v.local_thumb_path,
      v.platform
    FROM video_unavailable_archive va
    LEFT JOIN channels c ON c.channel_id = va.channel_id
    LEFT JOIN videos v ON v.video_id = va.video_id
    ${where}
    ORDER BY datetime(va.first_unavailable_at) DESC
    LIMIT ? OFFSET ?
  `).all(...params, limitNum, offset);
  const assetsRoot = resolveAssetsRootPath();
  const normalizedRows = (Array.isArray(rows) ? rows : []).map((row: any) => {
    const localThumbPath = resolveExistingPath(row?.local_thumb_path);
    const localThumbUrl = localPathToAssetsUrl(localThumbPath, assetsRoot);
    let thumbnailUrl = localThumbUrl;
    if (!thumbnailUrl) {
      const youtubeId = parseYoutubeVideoId(row?.video_id) || parseYoutubeVideoId(row?.webpage_url);
      if (youtubeId) {
        thumbnailUrl = `https://i.ytimg.com/vi/${youtubeId}/mqdefault.jpg`;
      }
    }
    return {
      ...row,
      thumbnail_url: thumbnailUrl,
      local_thumb_path: localThumbPath,
    };
  });

  res.json({ data: normalizedRows, total, page: pageNum, limit: limitNum });
});

// POST /api/sync/deleted-videos/delete
router.post('/deleted-videos/delete', (req: Request, res: Response) => {
  const db = getDb();
  const videoIds = toStringArray(req.body?.video_ids);
  if (videoIds.length === 0) {
    res.status(400).json({ error: 'video_ids is required' });
    return;
  }

  const placeholders = videoIds.map(() => '?').join(', ');
  const deletedArchive = db.prepare(`
    DELETE FROM video_unavailable_archive
    WHERE video_id IN (${placeholders})
  `).run(...videoIds).changes || 0;

  res.json({
    ok: true,
    deleted_archive: Number(deletedArchive),
    deleted_events: 0,
  });
});

export default router;
