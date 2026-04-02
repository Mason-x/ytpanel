import { Router, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import fs from 'fs';
import path from 'path';
import { getDb, getSetting } from '../db.js';

const router = Router();

interface ParsedHitInput {
  input: string;
  normalized_key: string;
}

interface HitGrowthPoint {
  date: string;
  view_count: number | null;
}

interface HitGrowthData {
  daily_view_increase: number | null;
  growth_series_7d: HitGrowthPoint[];
}

interface AutoCollectYoutubeHitStats {
  ok: true;
  scanned_youtube: number;
  qualified: number;
  inserted: number;
  updated: number;
  long_videos: number;
  shorts: number;
  skipped_invalid_id: number;
  skipped_threshold: number;
  deduped: number;
  thresholds: {
    long_video_views_gt: number;
    shorts_views_gt: number;
  };
  applied_tags: {
    base: string;
    long_video: string;
    shorts: string;
  };
  snapshot_date: string;
}

interface AutoCollectSourceRow {
  video_id: string;
  channel_id?: string | null;
  channel_title?: string | null;
  title?: string | null;
  description?: string | null;
  webpage_url?: string | null;
  published_at?: string | null;
  duration_sec?: number | null;
  content_type?: string | null;
  categories_json?: string | null;
  local_meta_path?: string | null;
  local_thumb_path?: string | null;
  latest_view_count?: number | null;
  latest_like_count?: number | null;
  latest_comment_count?: number | null;
}

function toNullableInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.trunc(num);
}

function toStringArray(input: unknown): string[] {
  if (Array.isArray(input)) {
    return input.map((item) => String(item || '').trim()).filter(Boolean);
  }
  if (typeof input === 'string') {
    return input.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
  }
  return [];
}

function normalizeTagList(input: unknown): string[] {
  const source = Array.isArray(input)
    ? input
    : (typeof input === 'string' ? input.split(/[,\n]+/) : []);

  const seen = new Set<string>();
  const tags: string[] = [];
  for (const item of source) {
    const value = String(item || '').trim().replace(/^#+/, '');
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    tags.push(value);
  }
  return tags;
}

function parseTagsJson(raw: unknown): string[] {
  if (Array.isArray(raw)) return normalizeTagList(raw);
  if (typeof raw !== 'string') return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return normalizeTagList(parsed);
  } catch {
    return [];
  }
}

function mergeUniqueTags(...parts: unknown[]): string[] {
  const merged: string[] = [];
  const seen = new Set<string>();
  for (const part of parts) {
    const tags = normalizeTagList(Array.isArray(part) ? part : parseTagsJson(part));
    for (const tag of tags) {
      const key = tag.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push(tag);
    }
  }
  return merged;
}

function normalizeHitInput(raw: string): string | null {
  const value = String(raw || '').trim();
  if (!value) return null;

  // Keep YouTube id compatibility.
  if (/^[A-Za-z0-9_-]{11}$/.test(value)) {
    return `https://www.youtube.com/watch?v=${value}`;
  }

  let normalized = value;
  if (!/^https?:\/\//i.test(normalized) && /^www\./i.test(normalized)) {
    normalized = `https://${normalized}`;
  }
  if (!/^https?:\/\//i.test(normalized)) return null;

  try {
    const url = new URL(normalized);
    if (!url.hostname) return null;
    return url.toString();
  } catch {
    return null;
  }
}

function parseHitVideoInput(input: string): ParsedHitInput | null {
  const normalized = normalizeHitInput(input);
  if (!normalized) return null;
  return {
    input: normalized,
    normalized_key: normalized.toLowerCase(),
  };
}

function inferPlatformFromUrl(rawUrl: unknown): string {
  const value = String(rawUrl || '').trim();
  if (!value) return 'Other';
  try {
    const host = new URL(value).hostname.replace(/^www\./i, '').toLowerCase();
    if (host.includes('youtube.com') || host.includes('youtu.be')) return 'YouTube';
    if (host.includes('tiktok.com')) return 'TikTok';
    if (host.includes('bilibili.com') || host.includes('b23.tv')) return '哔哩哔哩';
    if (host.includes('douyin.com')) return '抖音';
    if (host.includes('twitter.com') || host.includes('x.com')) return 'X';
    if (host.includes('xiaohongshu.com') || host.includes('xhslink.com')) return '小红书';
    if (host.includes('instagram.com')) return 'Instagram';
    return 'Other';
  } catch {
    return 'Other';
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
  } catch {
    return null;
  }
  return null;
}

function getStartDateBeforeDays(days: number): string {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  start.setUTCDate(start.getUTCDate() - Math.max(0, days - 1));
  return start.toISOString().slice(0, 10);
}

function buildRecentDateRange(days: number): string[] {
  const safeDays = Math.max(1, Math.trunc(days));
  const startDate = getStartDateBeforeDays(safeDays);
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  if (Number.isNaN(cursor.getTime())) return [startDate];
  const out: string[] = [];
  for (let i = 0; i < safeDays; i += 1) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

function isInsidePath(rootPath: string, targetPath: string): boolean {
  const relative = path.relative(path.resolve(rootPath), path.resolve(targetPath));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function resolveAssetsRootPath(): string {
  const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');
  return path.resolve(downloadRoot, 'assets');
}

function localPathToAssetsUrl(localPath: string | null, assetsRoot: string): string | null {
  if (!localPath) return null;
  const absLocal = path.resolve(localPath);
  if (!fs.existsSync(absLocal)) return null;
  if (!isInsidePath(assetsRoot, absLocal)) return null;
  const relativePath = path.relative(assetsRoot, absLocal).split(path.sep).join('/');
  const baseUrl = `/assets/${relativePath}`;
  try {
    const stat = fs.statSync(absLocal);
    return `${baseUrl}?v=${Math.floor(stat.mtimeMs)}`;
  } catch {
    return baseUrl;
  }
}

function buildHitGrowthMap(
  db: ReturnType<typeof getDb>,
  rows: any[],
): Map<string, HitGrowthData> {
  const growthByVideo = new Map<string, HitGrowthData>();
  if (!Array.isArray(rows) || rows.length === 0) return growthByVideo;

  const videoIds = rows
    .map((row) => String(row?.video_id || '').trim())
    .filter(Boolean);
  if (videoIds.length === 0) return growthByVideo;

  const placeholders = videoIds.map(() => '?').join(', ');
  const dateRange = buildRecentDateRange(7);
  const startDate = dateRange[0];
  const endDate = dateRange[dateRange.length - 1];
  const dailyRows = db.prepare(`
    SELECT video_id, date, view_count
    FROM hit_video_daily
    WHERE video_id IN (${placeholders}) AND date >= ? AND date <= ?
    ORDER BY video_id ASC, date ASC
  `).all(...videoIds, startDate, endDate) as any[];

  const previousRows = db.prepare(`
    SELECT hd.video_id as video_id, hd.view_count as view_count
    FROM hit_video_daily hd
    INNER JOIN (
      SELECT video_id, MAX(date) as max_date
      FROM hit_video_daily
      WHERE video_id IN (${placeholders}) AND date < ?
      GROUP BY video_id
    ) prev
      ON prev.video_id = hd.video_id
     AND prev.max_date = hd.date
  `).all(...videoIds, startDate) as any[];

  const grouped = new Map<string, Map<string, number | null>>();
  for (const item of dailyRows) {
    const videoId = String(item?.video_id || '').trim();
    if (!videoId) continue;
    const date = String(item?.date || '').trim();
    if (!date) continue;
    const dateMap = grouped.get(videoId) || new Map<string, number | null>();
    dateMap.set(date, toNullableInt(item?.view_count));
    grouped.set(videoId, dateMap);
  }

  const previousByVideo = new Map<string, number | null>();
  for (const item of previousRows) {
    const videoId = String(item?.video_id || '').trim();
    if (!videoId) continue;
    previousByVideo.set(videoId, toNullableInt(item?.view_count));
  }

  for (const row of rows) {
    const videoId = String(row?.video_id || '').trim();
    if (!videoId) continue;

    const dateMap = grouped.get(videoId) || new Map<string, number | null>();
    let carry = previousByVideo.get(videoId) ?? null;
    let points: HitGrowthPoint[] = dateRange.map((date) => {
      const incoming = dateMap.has(date) ? (dateMap.get(date) ?? null) : null;
      if (incoming != null) carry = incoming;
      return {
        date,
        view_count: carry,
      };
    });

    if (!points.some((item) => item.view_count != null)) {
      const fallbackView = toNullableInt(row?.view_count);
      if (fallbackView != null) {
        points = dateRange.map((date) => ({ date, view_count: fallbackView }));
      }
    }

    const validPoints = points.filter((item) => item.view_count != null);
    const first = validPoints.length > 0 ? validPoints[0] : null;
    const latest = validPoints.length > 0 ? validPoints[validPoints.length - 1] : null;
    const dailyViewIncrease = (
      first && latest && first.view_count != null && latest.view_count != null
    )
      ? (latest.view_count - first.view_count)
      : null;

    growthByVideo.set(videoId, {
      daily_view_increase: dailyViewIncrease,
      growth_series_7d: points,
    });
  }

  return growthByVideo;
}

export function autoCollectYoutubeHitVideos(db: ReturnType<typeof getDb> = getDb()): AutoCollectYoutubeHitStats {
  const LONG_VIEW_THRESHOLD = 1_000_000;
  const SHORT_VIEW_THRESHOLD = 10_000_000;
  const today = new Date().toISOString().slice(0, 10);

  const sourceRows = db.prepare(`
    SELECT
      v.video_id,
      v.channel_id,
      c.title AS channel_title,
      v.title,
      v.description,
      v.webpage_url,
      v.published_at,
      v.duration_sec,
      v.content_type,
      NULL AS categories_json,
      v.local_meta_path,
      v.local_thumb_path,
      COALESCE(vd_latest.view_count, v.view_count) AS latest_view_count,
      COALESCE(vd_latest.like_count, v.like_count) AS latest_like_count,
      COALESCE(vd_latest.comment_count, v.comment_count) AS latest_comment_count
    FROM videos v
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    LEFT JOIN (
      SELECT vd.video_id, vd.view_count, vd.like_count, vd.comment_count
      FROM video_daily vd
      INNER JOIN (
        SELECT video_id, MAX(date) AS max_date
        FROM video_daily
        GROUP BY video_id
      ) latest
        ON latest.video_id = vd.video_id
       AND latest.max_date = vd.date
    ) vd_latest ON vd_latest.video_id = v.video_id
    WHERE lower(COALESCE(v.platform, 'youtube')) = 'youtube'
      AND COALESCE(vd_latest.view_count, v.view_count, 0) > ?
    ORDER BY COALESCE(vd_latest.view_count, v.view_count, 0) DESC, v.published_at DESC
  `).all(LONG_VIEW_THRESHOLD) as AutoCollectSourceRow[];

  const existingRows = db.prepare('SELECT video_id, tags_json FROM hit_videos').all() as Array<{ video_id: string; tags_json?: string | null }>;
  const existingTagMap = new Map<string, string[]>();
  const existingIdSet = new Set<string>();
  for (const row of existingRows) {
    const id = String(row?.video_id || '').trim();
    if (!id) continue;
    existingIdSet.add(id);
    existingTagMap.set(id, parseTagsJson(row?.tags_json));
  }

  const upsertHit = db.prepare(`
    INSERT INTO hit_videos (
      video_id, channel_id, channel_title, platform, title, description, webpage_url, published_at, duration_sec,
      view_count, like_count, comment_count, categories_json, tags_json, local_meta_path, local_thumb_path,
      created_at, updated_at
    )
    VALUES (?, ?, ?, 'YouTube', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
    ON CONFLICT(video_id) DO UPDATE SET
      channel_id = excluded.channel_id,
      channel_title = COALESCE(excluded.channel_title, hit_videos.channel_title),
      platform = 'YouTube',
      title = excluded.title,
      description = excluded.description,
      webpage_url = excluded.webpage_url,
      published_at = excluded.published_at,
      duration_sec = excluded.duration_sec,
      view_count = excluded.view_count,
      like_count = excluded.like_count,
      comment_count = excluded.comment_count,
      categories_json = excluded.categories_json,
      tags_json = excluded.tags_json,
      local_meta_path = COALESCE(excluded.local_meta_path, hit_videos.local_meta_path),
      local_thumb_path = COALESCE(excluded.local_thumb_path, hit_videos.local_thumb_path),
      updated_at = datetime('now')
  `);

  const upsertDaily = db.prepare(`
    INSERT OR REPLACE INTO hit_video_daily (date, video_id, view_count, like_count, comment_count)
    VALUES (?, ?, ?, ?, ?)
  `);

  const stats: AutoCollectYoutubeHitStats = {
    ok: true,
    scanned_youtube: 0,
    qualified: 0,
    inserted: 0,
    updated: 0,
    long_videos: 0,
    shorts: 0,
    skipped_invalid_id: 0,
    skipped_threshold: 0,
    deduped: 0,
    thresholds: {
      long_video_views_gt: LONG_VIEW_THRESHOLD,
      shorts_views_gt: SHORT_VIEW_THRESHOLD,
    },
    applied_tags: {
      base: '自动归集',
      long_video: '视频',
      shorts: 'shorts',
    },
    snapshot_date: today,
  };

  const seenHitIds = new Set<string>();
  const runTx = db.transaction((rows: AutoCollectSourceRow[]) => {
    for (const row of rows) {
      stats.scanned_youtube += 1;

      const rawVideoId = String(row?.video_id || '').trim();
      const webpageUrl = String(row?.webpage_url || '').trim();
      const hitVideoId = parseYoutubeVideoId(rawVideoId) || parseYoutubeVideoId(webpageUrl) || rawVideoId;
      if (!hitVideoId) {
        stats.skipped_invalid_id += 1;
        continue;
      }
      if (seenHitIds.has(hitVideoId)) {
        stats.deduped += 1;
        continue;
      }
      seenHitIds.add(hitVideoId);

      const latestViews = toNullableInt(row?.latest_view_count) ?? 0;
      const latestLikes = toNullableInt(row?.latest_like_count);
      const latestComments = toNullableInt(row?.latest_comment_count);
      const contentType = String(row?.content_type || '').trim().toLowerCase();
      const lowerUrl = webpageUrl.toLowerCase();
      const isShort = contentType === 'short' || lowerUrl.includes('/shorts/');
      const threshold = isShort ? SHORT_VIEW_THRESHOLD : LONG_VIEW_THRESHOLD;
      if (!(latestViews > threshold)) {
        stats.skipped_threshold += 1;
        continue;
      }

      const autoTags = [stats.applied_tags.base, isShort ? stats.applied_tags.shorts : stats.applied_tags.long_video];
      const mergedTags = mergeUniqueTags(existingTagMap.get(hitVideoId) || [], autoTags);
      const categories = parseTagsJson(row?.categories_json);
      const normalizedTitle = String(row?.title || '').trim() || hitVideoId;
      const channelTitle = String(row?.channel_title || '').trim() || null;
      const channelId = String(row?.channel_id || '').trim() || null;
      const description = String(row?.description || '').trim() || null;
      const publishedAt = String(row?.published_at || '').trim() || null;
      const durationSec = toNullableInt(row?.duration_sec);
      const localMetaPath = String(row?.local_meta_path || '').trim() || null;
      const localThumbPath = String(row?.local_thumb_path || '').trim() || null;

      upsertHit.run(
        hitVideoId,
        channelId,
        channelTitle,
        normalizedTitle,
        description,
        webpageUrl || null,
        publishedAt,
        durationSec,
        latestViews,
        latestLikes,
        latestComments,
        JSON.stringify(categories),
        JSON.stringify(mergedTags),
        localMetaPath,
        localThumbPath,
      );
      upsertDaily.run(today, hitVideoId, latestViews, latestLikes, latestComments);

      stats.qualified += 1;
      if (isShort) stats.shorts += 1;
      else stats.long_videos += 1;

      if (existingIdSet.has(hitVideoId)) stats.updated += 1;
      else {
        stats.inserted += 1;
        existingIdSet.add(hitVideoId);
      }
      existingTagMap.set(hitVideoId, mergedTags);
    }
  });

  runTx(sourceRows);
  return stats;
}

// GET /api/hits/videos
router.get('/videos', (req: Request, res: Response) => {
  const db = getDb();
  const assetsRoot = resolveAssetsRootPath();
  const search = String(req.query.search || '').trim();
  const tag = String(req.query.tag || '').trim();
  const platform = String(req.query.platform || '').trim();

  let where = 'WHERE 1=1';
  const params: any[] = [];
  if (search) {
    where += ' AND (title LIKE ? OR channel_title LIKE ? OR video_id LIKE ?)';
    params.push(`%${search}%`, `%${search}%`, `%${search}%`);
  }
  if (tag) {
    where += ' AND tags_json LIKE ?';
    params.push(`%"${tag}"%`);
  }
  if (platform && platform.toLowerCase() !== 'all') {
    where += " AND lower(COALESCE(platform, '')) = lower(?)";
    params.push(platform);
  }

  const rows = db.prepare(`
    SELECT *
    FROM hit_videos
    ${where}
    ORDER BY view_count DESC NULLS LAST, published_at DESC, created_at DESC
  `).all(...params) as any[];

  const growthMap = buildHitGrowthMap(db, rows);
  const data = rows.map((row) => {
    const localThumbPath = typeof row.local_thumb_path === 'string' ? row.local_thumb_path.trim() : '';
    const localThumbUrl = localThumbPath ? localPathToAssetsUrl(localThumbPath, assetsRoot) : null;
    const storedPlatform = String(row?.platform || '').trim();
    const inferredPlatform = inferPlatformFromUrl(row?.webpage_url);
    const platformName = (!storedPlatform || /^other$/i.test(storedPlatform))
      ? inferredPlatform
      : storedPlatform;
    return {
      ...row,
      platform: platformName,
      tags: parseTagsJson(row.tags_json),
      categories: parseTagsJson(row.categories_json),
      local_thumb_url: localThumbUrl,
      ...(growthMap.get(String(row?.video_id || '').trim()) || {
        daily_view_increase: null,
        growth_series_7d: [],
      }),
    };
  });
  res.json({ data, total: data.length });
});

// POST /api/hits/videos/bulk-add
router.post('/videos/bulk-add', async (req: Request, res: Response) => {
  const rawInputs = toStringArray(req.body?.inputs);
  const defaultTags = normalizeTagList(req.body?.tags);

  if (rawInputs.length === 0) {
    res.status(400).json({ error: '请至少提供一条视频链接或视频ID' });
    return;
  }

  const parsed: ParsedHitInput[] = [];
  const dedupe = new Set<string>();
  let invalidCount = 0;
  for (const raw of rawInputs) {
    const item = parseHitVideoInput(raw);
    if (!item) {
      invalidCount += 1;
      continue;
    }
    if (dedupe.has(item.normalized_key)) continue;
    dedupe.add(item.normalized_key);
    parsed.push(item);
  }

  if (parsed.length === 0) {
    res.status(400).json({ error: '未识别到有效链接，请输入 yt-dlp 支持的平台视频链接（也兼容 YouTube 11位视频ID）' });
    return;
  }

  const db = getDb();
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'hit_bulk_add', ?, 'queued')
  `).run(jobId, JSON.stringify({ videos: parsed, tags: defaultTags }));

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  res.json({
    job_id: jobId,
    status: 'queued',
    total: parsed.length,
    invalid_count: invalidCount,
  });
});

// POST /api/hits/videos/auto-collect-youtube
router.post('/videos/auto-collect-youtube', (req: Request, res: Response) => {
  res.json(autoCollectYoutubeHitVideos(getDb()));
});

// PATCH /api/hits/videos/:id
router.patch('/videos/:id', (req: Request, res: Response) => {
  const db = getDb();
  const videoId = String(req.params.id || '').trim();
  if (!videoId) {
    res.status(400).json({ error: 'video_id is required' });
    return;
  }

  const existing = db.prepare('SELECT video_id FROM hit_videos WHERE video_id = ?').get(videoId) as any;
  if (!existing) {
    res.status(404).json({ error: 'Video not found' });
    return;
  }

  const tags = normalizeTagList(req.body?.tags);
  db.prepare(`
    UPDATE hit_videos
    SET tags_json = ?, updated_at = datetime('now')
    WHERE video_id = ?
  `).run(JSON.stringify(tags), videoId);

  const row = db.prepare('SELECT * FROM hit_videos WHERE video_id = ?').get(videoId) as any;
  const storedPlatform = String(row?.platform || '').trim();
  const inferredPlatform = inferPlatformFromUrl(row?.webpage_url);
  res.json({
    ...row,
    platform: (!storedPlatform || /^other$/i.test(storedPlatform)) ? inferredPlatform : storedPlatform,
    tags: parseTagsJson(row?.tags_json),
    categories: parseTagsJson(row?.categories_json),
  });
});

// DELETE /api/hits/videos/:id
router.delete('/videos/:id', (req: Request, res: Response) => {
  const db = getDb();
  const videoId = String(req.params.id || '').trim();
  if (!videoId) {
    res.status(400).json({ error: 'video_id is required' });
    return;
  }

  const existing = db.prepare('SELECT video_id, title FROM hit_videos WHERE video_id = ?').get(videoId) as any;
  if (!existing) {
    res.status(404).json({ error: 'Video not found' });
    return;
  }

  const deleteDaily = db.prepare('DELETE FROM hit_video_daily WHERE video_id = ?').run(videoId);
  const deleteVideo = db.prepare('DELETE FROM hit_videos WHERE video_id = ?').run(videoId);

  res.json({
    deleted: true,
    video_id: videoId,
    title: String(existing?.title || ''),
    deleted_daily_rows: Number(deleteDaily?.changes || 0),
    deleted_video_rows: Number(deleteVideo?.changes || 0),
  });
});

export default router;
