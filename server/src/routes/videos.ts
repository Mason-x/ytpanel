import { Router, Request, Response } from 'express';
import { getDb, getSetting } from '../db.js';
import fs from 'fs';
import path from 'path';
import { resolveVideoAvailabilityFilter } from './videoAvailabilityFilter.js';

const router = Router();
const WEEKDAY_LABELS = ['\u5468\u65e5', '\u5468\u4e00', '\u5468\u4e8c', '\u5468\u4e09', '\u5468\u56db', '\u5468\u4e94', '\u5468\u516d'];

interface VideoMetaSummary {
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  collect_count: number | null;
  share_count: number | null;
  categories: string[];
  tag_labels: string[];
  timestamp: number | null;
  thumb_url: string | null;
  description: string | null;
}

interface VideoGrowthPoint {
  date: string;
  view_count: number | null;
}

interface VideoGrowthData {
  daily_view_increase: number | null;
  growth_series_7d: VideoGrowthPoint[];
}

const metaSummaryCache = new Map<string, { mtimeMs: number; summary: VideoMetaSummary }>();

function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
}

function toNullableInt(value: unknown): number | null {
  const num = toNullableNumber(value);
  if (num == null) return null;
  return Math.trunc(num);
}

function parseUploadDateToTimestamp(uploadDate: unknown): number | null {
  if (typeof uploadDate !== 'string') return null;
  const raw = uploadDate.trim();
  if (!/^\d{8}$/.test(raw)) return null;
  const yyyy = raw.slice(0, 4);
  const mm = raw.slice(4, 6);
  const dd = raw.slice(6, 8);
  const date = new Date(`${yyyy}-${mm}-${dd}T00:00:00`);
  if (Number.isNaN(date.getTime())) return null;
  return Math.floor(date.getTime() / 1000);
}

function parsePublishedAtToTimestamp(publishedAt: unknown): number | null {
  if (typeof publishedAt !== 'string') return null;
  const raw = publishedAt.trim();
  if (!raw) return null;
  const withTime = raw.includes('T') ? raw : `${raw}T00:00:00`;
  const date = new Date(withTime);
  if (Number.isNaN(date.getTime())) return null;
  return Math.floor(date.getTime() / 1000);
}

function normalizeEpochSeconds(value: unknown): number | null {
  const num = toNullableNumber(value);
  if (num == null) return null;
  if (num > 1e12) return Math.floor(num / 1000);
  return Math.floor(num);
}

function timestampToWeekday(timestamp: number | null): string | null {
  if (timestamp == null) return null;
  const date = new Date(timestamp * 1000);
  if (Number.isNaN(date.getTime())) return null;
  return WEEKDAY_LABELS[date.getDay()] || null;
}

function extractHashtagsFromText(value: unknown): string[] {
  if (typeof value !== 'string') return [];
  const text = value.trim();
  if (!text) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  const regex = /(?:^|\s)#([^\s#]{1,50})/g;
  let match: RegExpExecArray | null = null;
  while ((match = regex.exec(text)) !== null) {
    const token = String(match[1] || '').trim();
    if (!token) continue;
    const normalized = token.toLowerCase();
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(`#${token}`);
  }
  return out;
}

function mergeTagLikeValues(primary: unknown[], secondary: unknown[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const feed = (raw: unknown) => {
    if (typeof raw !== 'string') return;
    const text = raw.trim();
    if (!text) return;
    const key = text.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(text);
  };
  for (const item of primary) feed(item);
  for (const item of secondary) feed(item);
  return out;
}

function readUrlFromUnknown(value: unknown): string | null {
  if (typeof value === 'string') {
    const text = value.trim();
    if (/^https?:\/\//i.test(text)) return text;
    if (text.startsWith('//')) return `https:${text}`;
    return null;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = readUrlFromUnknown(item);
      if (found) return found;
    }
    return null;
  }
  if (value && typeof value === 'object') {
    const anyValue = value as any;
    const direct = readUrlFromUnknown(anyValue.url);
    if (direct) return direct;
    const fromList = readUrlFromUnknown(anyValue.url_list);
    if (fromList) return fromList;
    const fromInfoList = readUrlFromUnknown(anyValue.info_list);
    if (fromInfoList) return fromInfoList;
    const fromDefaultSet = readUrlFromUnknown(anyValue.url_default_set);
    if (fromDefaultSet) return fromDefaultSet;
    const fromOrigin = readUrlFromUnknown(anyValue.origin);
    if (fromOrigin) return fromOrigin;
  }
  return null;
}

function extractTextExtraTags(raw: any): string[] {
  if (!Array.isArray(raw?.text_extra)) return [];
  return raw.text_extra
    .map((item: any) => String(item?.hashtag_name || item?.hashtag_name_rich || item?.hashtag_name_span || '').trim())
    .filter(Boolean)
    .map((item: string) => `#${item}`);
}

function extractVideoTagNames(raw: any): string[] {
  if (!Array.isArray(raw?.video_tag)) return [];
  return raw.video_tag
    .map((item: any) => String(item?.tag_name || item?.name || '').trim())
    .filter(Boolean);
}

function extractCommonTagNames(raw: any): string[] {
  const out: string[] = [];
  const pushFrom = (value: unknown) => {
    if (!value) return;
    if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === 'string') {
          const text = item.trim();
          if (text) out.push(text);
          continue;
        }
        if (item && typeof item === 'object') {
          const row = item as any;
          const text = String(row?.name || row?.tag || row?.tag_name || '').trim();
          if (text) out.push(text);
        }
      }
      return;
    }
    if (typeof value === 'string') {
      const text = value.trim();
      if (!text) return;
      if (text.includes(',')) {
        for (const part of text.split(',')) {
          const token = part.trim();
          if (token) out.push(token);
        }
      } else {
        out.push(text);
      }
    }
  };

  pushFrom(raw?.tags);
  pushFrom(raw?.keywords);
  pushFrom(raw?.tag);
  pushFrom(raw?.raw?.tags);
  pushFrom(raw?.raw?.keywords);
  pushFrom(raw?.raw?.tag);
  return out;
}

function extractMetaThumbUrl(raw: any): string | null {
  return (
    readUrlFromUnknown(raw?.video_cover)
    || readUrlFromUnknown(raw?.thumbnail)
    || readUrlFromUnknown(raw?.thumbnails)
    || readUrlFromUnknown(raw?.static_cover)
    || readUrlFromUnknown(raw?.dynamic_cover)
    || readUrlFromUnknown(raw?.origin_cover)
    || readUrlFromUnknown(raw?.video?.cover)
    || readUrlFromUnknown(raw?.video?.dynamic_cover)
    || readUrlFromUnknown(raw?.video?.origin_cover)
    || readUrlFromUnknown(raw?.cover)
    || readUrlFromUnknown(raw?.images)
    || readUrlFromUnknown(raw?.image_infos)
    || readUrlFromUnknown(raw?.image_list)
    || readUrlFromUnknown(raw?.note_card?.cover)
    || readUrlFromUnknown(raw?.note_card?.image_list)
    || readUrlFromUnknown(raw?.raw?.thumbnail)
    || readUrlFromUnknown(raw?.raw?.cover)
    || readUrlFromUnknown(raw?.raw?.images)
    || readUrlFromUnknown(raw?.raw?.image_infos)
    || readUrlFromUnknown(raw?.raw?.image_list)
    || readUrlFromUnknown(raw?.raw?.video?.cover)
    || readUrlFromUnknown(raw?.raw?.video?.dynamic_cover)
    || readUrlFromUnknown(raw?.raw?.video?.origin_cover)
    || null
  );
}

function readVideoMetaSummary(localMetaPath: unknown): VideoMetaSummary | null {
  const resolvedMetaPath = resolveExistingPath(localMetaPath);
  if (!resolvedMetaPath) return null;

  try {
    const stat = fs.statSync(resolvedMetaPath);
    const cached = metaSummaryCache.get(resolvedMetaPath);
    if (cached && cached.mtimeMs === stat.mtimeMs) {
      return cached.summary;
    }

    const raw = JSON.parse(fs.readFileSync(resolvedMetaPath, 'utf8')) as any;
    const categories = (
      Array.isArray(raw?.categories)
        ? raw.categories
        : (typeof raw?.category === 'string' ? [raw.category] : [])
    )
      .filter((item: unknown) => typeof item === 'string' && item.trim())
      .map((item: string) => item.trim());
    const tagLabels = mergeTagLikeValues(
      [
        ...extractCommonTagNames(raw),
        ...extractVideoTagNames(raw),
        ...extractTextExtraTags(raw),
      ],
      extractHashtagsFromText(raw?.description ?? raw?.desc ?? raw?.title),
    );
    const timestamp = normalizeEpochSeconds(raw?.timestamp)
      ?? normalizeEpochSeconds(raw?.release_timestamp)
      ?? normalizeEpochSeconds(raw?.create_time)
      ?? normalizeEpochSeconds(raw?.video?.create_time)
      ?? parsePublishedAtToTimestamp(raw?.upload_time)
      ?? parseUploadDateToTimestamp(raw?.upload_date)
      ?? null;
    const viewCount = toNullableInt(
      raw?.view_count
      ?? raw?.play_count
      ?? raw?.statistics?.play_count
      ?? raw?.stat?.view
      ?? raw?.note_card?.play_count
      ?? raw?.note_card?.interact_info?.view_count,
    );
    const likeCount = toNullableInt(
      raw?.like_count
      ?? raw?.liked_count
      ?? raw?.statistics?.digg_count
      ?? raw?.stat?.like
      ?? raw?.interact_info?.liked_count
      ?? raw?.note_card?.interact_info?.liked_count,
    );
    const commentCount = toNullableInt(
      raw?.comment_count
      ?? raw?.statistics?.comment_count
      ?? raw?.stat?.reply
      ?? raw?.interact_info?.comment_count
      ?? raw?.note_card?.interact_info?.comment_count,
    );
    const collectCount = toNullableInt(
      raw?.collect_count
      ?? raw?.collected_count
      ?? raw?.statistics?.collect_count
      ?? raw?.stat?.collect
      ?? raw?.interact_info?.collect_count
      ?? raw?.interact_info?.collected_count
      ?? raw?.note_card?.interact_info?.collect_count
      ?? raw?.note_card?.interact_info?.collected_count,
    );
    const shareCount = toNullableInt(
      raw?.share_count
      ?? raw?.statistics?.share_count
      ?? raw?.stat?.share
      ?? raw?.interact_info?.share_count
      ?? raw?.note_card?.interact_info?.share_count,
    );
    const normalizedViewCount = (
      viewCount === 0
      && [likeCount, commentCount, collectCount, shareCount].some((value) => value != null && value > 0)
    )
      ? null
      : viewCount;
    const thumbUrl = extractMetaThumbUrl(raw);
    const descriptionRaw = typeof raw?.description === 'string'
      ? raw.description
      : (typeof raw?.desc === 'string' ? raw.desc : null);
    const description = typeof descriptionRaw === 'string' && descriptionRaw.trim()
      ? descriptionRaw.trim()
      : null;

    const summary: VideoMetaSummary = {
      view_count: normalizedViewCount,
      like_count: likeCount,
      comment_count: commentCount,
      collect_count: collectCount,
      share_count: shareCount,
      categories,
      tag_labels: tagLabels,
      timestamp,
      thumb_url: thumbUrl,
      description,
    };

    metaSummaryCache.set(resolvedMetaPath, { mtimeMs: stat.mtimeMs, summary });
    return summary;
  } catch {
    metaSummaryCache.delete(resolvedMetaPath);
    return null;
  }
}

function normalizeDownloadStatus(raw: unknown): string {
  if (typeof raw !== 'string') return 'none';
  const parts = raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
  if (parts.length === 0) return 'none';
  return Array.from(new Set(parts)).join(',');
}

function removeStatusToken(raw: unknown, token: string): string {
  const normalized = normalizeDownloadStatus(raw);
  if (normalized === 'none') return 'none';
  const parts = normalized
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item && item !== token);
  if (parts.length === 0) return 'none';
  return Array.from(new Set(parts)).join(',');
}

function ensureStatusToken(raw: unknown, token: string): string {
  const normalized = normalizeDownloadStatus(raw);
  if (normalized === 'none') return token;
  const parts = normalized.split(',').map((item) => item.trim()).filter(Boolean);
  if (!parts.includes(token)) parts.push(token);
  return Array.from(new Set(parts)).join(',');
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

function buildVideoGrowthMap(
  db: ReturnType<typeof getDb>,
  rows: any[],
): Map<string, VideoGrowthData> {
  const growthByVideo = new Map<string, VideoGrowthData>();
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
    FROM video_daily
    WHERE video_id IN (${placeholders}) AND date >= ? AND date <= ?
    ORDER BY video_id ASC, date ASC
  `).all(...videoIds, startDate, endDate) as any[];

  const previousRows = db.prepare(`
    SELECT vd.video_id as video_id, vd.view_count as view_count
    FROM video_daily vd
    INNER JOIN (
      SELECT video_id, MAX(date) as max_date
      FROM video_daily
      WHERE video_id IN (${placeholders}) AND date < ?
      GROUP BY video_id
    ) prev
      ON prev.video_id = vd.video_id
     AND prev.max_date = vd.date
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
    let points: VideoGrowthPoint[] = dateRange.map((date) => {
      const incoming = dateMap.has(date) ? (dateMap.get(date) ?? null) : null;
      if (incoming != null) carry = incoming;
      return {
        date,
        view_count: carry,
      };
    });

    if (!points.some((item) => item.view_count != null)) {
      const fallbackView = toNullableInt(row?.latest_views) ?? toNullableInt(row?.view_count);
      if (fallbackView != null) {
        points = dateRange.map((date) => ({ date, view_count: fallbackView }));
      }
    }

    const validPoints = points.filter((item) => item.view_count != null);
    const first = validPoints.length > 0 ? validPoints[0] : null;
    const latest = validPoints.length > 0 ? validPoints[validPoints.length - 1] : null;
    const publishedAtText = String(row?.published_at || '').trim();
    const publishedDate = publishedAtText.includes('T')
      ? publishedAtText.slice(0, 10)
      : publishedAtText.slice(0, 10);
    const publishedInWindow = Boolean(
      /^\d{4}-\d{2}-\d{2}$/.test(publishedDate)
      && publishedDate >= startDate
      && publishedDate <= endDate
    );

    const dailyViewIncrease = (() => {
      if (!latest || latest.view_count == null) return null;
      if (publishedInWindow) {
        // Videos published within the window count their full current views as 7-day growth.
        return Math.max(0, latest.view_count);
      }
      if (!first || first.view_count == null) return null;
      return Math.max(0, latest.view_count - first.view_count);
    })();

    growthByVideo.set(videoId, {
      daily_view_increase: dailyViewIncrease,
      growth_series_7d: points,
    });
  }

  return growthByVideo;
}

function localPathToAssetsUrl(localPath: string | null, assetsRoot: string): string | null {
  if (!localPath) return null;
  const absLocal = path.resolve(localPath);
  const relativePath = path.relative(assetsRoot, absLocal);
  if (!relativePath || relativePath.startsWith('..') || path.isAbsolute(relativePath)) return null;
  const normalized = relativePath.split(path.sep).join('/');
  return `/assets/${normalized}`;
}

function reconcileVideoLocalState(db: ReturnType<typeof getDb>, rawVideo: any, assetsRoot: string): any {
  const video = { ...rawVideo };
  const existingVideoPath = resolveExistingPath(video.local_video_path);
  const existingMetaPath = resolveExistingPath(video.local_meta_path);
  const existingThumbPath = resolveExistingPath(video.local_thumb_path);
  let nextLocalVideoPath = existingVideoPath;
  let nextLocalMetaPath = existingMetaPath;
  let nextLocalThumbPath = existingThumbPath;
  let nextDownloadStatus = normalizeDownloadStatus(video.download_status);

  if (existingVideoPath) {
    nextDownloadStatus = ensureStatusToken(nextDownloadStatus, 'video');
  } else {
    nextLocalVideoPath = null;
    nextDownloadStatus = removeStatusToken(nextDownloadStatus, 'video');
  }

  if (existingMetaPath) {
    nextDownloadStatus = ensureStatusToken(nextDownloadStatus, 'meta');
  } else {
    nextLocalMetaPath = null;
    nextDownloadStatus = removeStatusToken(nextDownloadStatus, 'meta');
  }

  if (
    nextLocalVideoPath !== video.local_video_path ||
    nextLocalMetaPath !== video.local_meta_path ||
    nextLocalThumbPath !== video.local_thumb_path ||
    nextDownloadStatus !== video.download_status
  ) {
    db.prepare(`
      UPDATE videos
      SET local_video_path = ?, local_meta_path = ?, local_thumb_path = ?, download_status = ?
      WHERE video_id = ?
    `).run(nextLocalVideoPath, nextLocalMetaPath, nextLocalThumbPath, nextDownloadStatus, video.video_id);
  }

  video.local_video_path = nextLocalVideoPath;
  video.local_meta_path = nextLocalMetaPath;
  video.local_thumb_path = nextLocalThumbPath;
  video.local_thumb_url = localPathToAssetsUrl(nextLocalThumbPath, assetsRoot);
  video.download_status = nextDownloadStatus;
  const meta = readVideoMetaSummary(nextLocalMetaPath);
  const fallbackTimestamp = parsePublishedAtToTimestamp(video.published_at);
  const mergedTimestamp = meta?.timestamp ?? fallbackTimestamp;

  video.view_count = meta?.view_count ?? toNullableInt(video.latest_views) ?? toNullableInt(video.view_count);
  video.like_count = meta?.like_count ?? toNullableInt(video.latest_likes) ?? toNullableInt(video.like_count);
  video.comment_count = meta?.comment_count ?? toNullableInt(video.latest_comments) ?? toNullableInt(video.comment_count);
  video.collect_count = meta?.collect_count ?? toNullableInt(video.latest_collects) ?? toNullableInt(video.collect_count);
  video.share_count = meta?.share_count ?? toNullableInt(video.latest_shares) ?? toNullableInt(video.share_count);
  const dbDescription = typeof video.description === 'string' ? video.description.trim() : '';
  const metaDescription = typeof meta?.description === 'string' ? meta.description.trim() : '';
  if (metaDescription && metaDescription.length > dbDescription.length) {
    video.description = metaDescription;
    db.prepare(`
      UPDATE videos
      SET description = ?
      WHERE video_id = ?
        AND (description IS NULL OR length(trim(description)) < ?)
    `).run(metaDescription, video.video_id, metaDescription.length);
  }
  const fallbackDescriptionTags = mergeTagLikeValues(
    extractHashtagsFromText(video.description),
    extractHashtagsFromText(video.title),
  );
  const isYoutube = String(video?.platform || '').trim().toLowerCase() === 'youtube';
  if (isYoutube) {
    video.categories = meta?.categories && meta.categories.length > 0 ? meta.categories : [];
  } else {
    const metaTags = Array.isArray(meta?.tag_labels) ? meta!.tag_labels : [];
    video.categories = metaTags.length > 0
      ? metaTags
      : fallbackDescriptionTags;
  }
  video.timestamp = mergedTimestamp;
  video.upload_weekday = timestampToWeekday(mergedTimestamp);
  video.meta_thumb_url = meta?.thumb_url || null;
  return video;
}

// GET /api/videos
router.get('/', (req: Request, res: Response) => {
  const db = getDb();
  const assetsRoot = resolveAssetsRootPath();
  const { channel_id, platform, tag, channel_tag, type, availability, download, q, sort, page = '1', limit = '48', favorite, recent_days } = req.query;
  const effectiveAvailability = resolveVideoAvailabilityFilter(availability, sort);
  const pageNum = Math.max(1, parseInt(page as string, 10) || 1);
  const limitNum = Math.min(200, parseInt(limit as string, 10) || 48);
  const offset = (pageNum - 1) * limitNum;

  let where = 'WHERE 1=1';
  const params: any[] = [];
  where += `
    AND NOT (
      v.title = 'Untitled'
      AND v.duration_sec IS NULL
      AND v.view_count IS NULL
      AND v.like_count IS NULL
      AND (v.local_meta_path IS NULL OR trim(v.local_meta_path) = '')
      AND (v.local_thumb_path IS NULL OR trim(v.local_thumb_path) = '')
    )
  `;

  if (channel_id) {
    where += ' AND v.channel_id = ?';
    params.push(channel_id);
  }
  if (platform) {
    where += " AND lower(COALESCE(c.platform, 'youtube')) = ?";
    params.push(String(platform).toLowerCase());
  }
  const normalizedType = typeof type === 'string' ? type.trim().toLowerCase() : '';
  const normalizedPlatform = typeof platform === 'string' ? platform.trim().toLowerCase() : '';
  if (normalizedType === 'album') {
    if (normalizedPlatform === 'douyin') {
      where += `
        AND lower(COALESCE(c.platform, 'youtube')) = 'douyin'
        AND (
          lower(COALESCE(v.content_type, '')) = 'album'
          OR lower(COALESCE(v.content_type, '')) = 'live_photo'
          OR (
            lower(COALESCE(v.content_type, '')) = 'note'
            AND lower(COALESCE(v.content_type_source, '')) NOT LIKE 'douyin_live_photo%'
          )
          OR lower(COALESCE(v.content_type_source, '')) LIKE 'douyin_album%'
          OR lower(COALESCE(v.content_type_source, '')) LIKE 'douyin_note%'
        )
      `;
    } else if (normalizedPlatform === 'xiaohongshu') {
      where += `
        AND lower(COALESCE(c.platform, 'youtube')) = 'xiaohongshu'
        AND (
          lower(COALESCE(v.content_type, '')) = 'album'
          OR lower(COALESCE(v.content_type, '')) = 'note'
          OR lower(COALESCE(v.content_type_source, '')) LIKE 'xhs_content_type_album%'
        )
      `;
    } else {
      where += " AND lower(COALESCE(v.content_type, '')) = 'album'";
    }
  } else if (normalizedType === 'short' && normalizedPlatform === 'youtube') {
    where += `
      AND lower(COALESCE(c.platform, 'youtube')) = 'youtube'
      AND (
        lower(COALESCE(v.content_type, '')) = 'short'
        OR lower(COALESCE(v.webpage_url, '')) LIKE '%/shorts/%'
      )
    `;
  } else if (normalizedType === 'long' && normalizedPlatform === 'youtube') {
    where += `
      AND lower(COALESCE(c.platform, 'youtube')) = 'youtube'
      AND lower(COALESCE(v.content_type, '')) = 'long'
      AND lower(COALESCE(v.webpage_url, '')) NOT LIKE '%/shorts/%'
    `;
  } else if (normalizedType === 'live' && normalizedPlatform === 'youtube') {
    where += `
      AND lower(COALESCE(c.platform, 'youtube')) = 'youtube'
      AND (
        lower(COALESCE(v.content_type, '')) = 'live'
        OR lower(COALESCE(v.content_type_source, '')) LIKE 'streams_feed%'
        OR lower(COALESCE(v.content_type_source, '')) LIKE 'youtube_live_status%'
      )
    `;
  } else if (normalizedType === 'long' && String(platform || '').trim().toLowerCase() === 'douyin') {
    where += " AND lower(COALESCE(c.platform, 'youtube')) = 'douyin' AND lower(COALESCE(v.content_type, '')) = 'long'";
  } else if (normalizedType === 'short' && String(platform || '').trim().toLowerCase() === 'douyin') {
    where += " AND lower(COALESCE(c.platform, 'youtube')) = 'douyin' AND lower(COALESCE(v.content_type, '')) = 'short'";
  } else if (normalizedType === 'live_photo') {
    where += `
      AND lower(COALESCE(c.platform, 'youtube')) = 'douyin'
      AND (
        lower(COALESCE(v.content_type, '')) = 'live_photo'
        OR lower(COALESCE(v.content_type_source, '')) LIKE 'douyin_live_photo%'
      )
    `;
  } else if (normalizedType === 'short') {
    where += `
      AND (
        lower(COALESCE(v.content_type, '')) = 'short'
        OR (
          lower(COALESCE(c.platform, 'youtube')) = 'youtube'
          AND lower(COALESCE(v.webpage_url, '')) LIKE '%/shorts/%'
        )
      )
    `;
  } else if (normalizedType === 'long') {
    where += `
      AND lower(COALESCE(v.content_type, '')) = 'long'
      AND NOT (
        lower(COALESCE(c.platform, 'youtube')) = 'youtube'
        AND lower(COALESCE(v.webpage_url, '')) LIKE '%/shorts/%'
      )
    `;
  } else if (normalizedType === 'live') {
    where += " AND lower(COALESCE(v.content_type, '')) = 'live'";
  } else if (normalizedType === 'note') {
    where += ' AND lower(COALESCE(v.content_type, \'\')) = ?';
    params.push(normalizedType);
  }
  if (effectiveAvailability) {
    where += ' AND v.availability_status = ?';
    params.push(effectiveAvailability);
  }
  if (download && download !== 'any') {
    where += ' AND v.download_status = ?';
    params.push(download);
  }
  if (q) {
    where += ' AND v.title LIKE ?';
    params.push(`%${q}%`);
  }
  if (tag) {
    where += ' AND v.tags_json LIKE ?';
    params.push(`%"${tag}"%`);
  }
  if (channel_tag) {
    where += ' AND c.tags_json LIKE ?';
    params.push(`%"${channel_tag}"%`);
  }
  if (favorite === '1') {
    where += ' AND v.favorite = 1';
  }
  if (recent_days !== undefined) {
    const recentDaysNum = Math.max(1, parseInt(String(recent_days), 10) || 0);
    if (recentDaysNum > 0) {
      const cutoffDate = new Date(Date.now() - (recentDaysNum - 1) * 86400000).toISOString().slice(0, 10);
      where += ' AND date(COALESCE(v.published_at, v.created_at)) >= date(?)';
      params.push(cutoffDate);
    }
  }

  // Sorting
  let orderBy = 'date(COALESCE(v.published_at, v.created_at)) DESC';
  switch (sort) {
    case 'most_viewed':
      orderBy = 'latest_views DESC NULLS LAST, date(COALESCE(v.published_at, v.created_at)) DESC';
      break;
    case 'views_7d':
      orderBy = `
        CASE WHEN views_change_7d IS NULL THEN 1 ELSE 0 END ASC,
        CAST(views_change_7d AS REAL) DESC,
        CAST(COALESCE(vd_latest.view_count, v.view_count) AS REAL) DESC,
        date(COALESCE(v.published_at, v.created_at)) DESC
      `;
      break;
    case 'views_28d':
      orderBy = `
        CASE WHEN views_change_28d IS NULL THEN 1 ELSE 0 END ASC,
        CAST(views_change_28d AS REAL) DESC,
        CAST(COALESCE(vd_latest.view_count, v.view_count) AS REAL) DESC,
        date(COALESCE(v.published_at, v.created_at)) DESC
      `;
      break;
    case 'unavailable_recent':
      orderBy = 'v.unavailable_at DESC NULLS LAST, v.published_at DESC';
      break;
    case 'most_recent':
    default:
      orderBy = 'date(COALESCE(v.published_at, v.created_at)) DESC';
      break;
  }

  const day7ago = new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10);
  const day28ago = new Date(Date.now() - 27 * 86400000).toISOString().slice(0, 10);

  const query = `
    SELECT v.*,
      c.title as channel_title,
      c.tags_json as channel_tags_json,
      COALESCE(vd_latest.view_count, v.view_count) as latest_views,
      COALESCE(vd_latest.like_count, v.like_count) as latest_likes,
      vd_latest.comment_count as latest_comments,
      COALESCE(vd_latest.collect_count, v.collect_count) as latest_collects,
      COALESCE(vd_latest.share_count, v.share_count) as latest_shares,
      (COALESCE(vd_latest.view_count, v.view_count) - vd_7d.view_count) as views_change_7d,
      (COALESCE(vd_latest.view_count, v.view_count) - vd_28d.view_count) as views_change_28d
    FROM videos v
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    LEFT JOIN (
      SELECT video_id, view_count, like_count, comment_count, collect_count, share_count FROM video_daily
      WHERE date = (SELECT MAX(date) FROM video_daily vd2 WHERE vd2.video_id = video_daily.video_id)
    ) vd_latest ON vd_latest.video_id = v.video_id
    LEFT JOIN (
      SELECT vd.video_id, vd.view_count
      FROM video_daily vd
      INNER JOIN (
        SELECT video_id, MIN(date) as base_date
        FROM video_daily
        WHERE date >= ?
        GROUP BY video_id
      ) base ON base.video_id = vd.video_id AND base.base_date = vd.date
    ) vd_7d ON vd_7d.video_id = v.video_id
    LEFT JOIN (
      SELECT vd.video_id, vd.view_count
      FROM video_daily vd
      INNER JOIN (
        SELECT video_id, MIN(date) as base_date
        FROM video_daily
        WHERE date >= ?
        GROUP BY video_id
      ) base ON base.video_id = vd.video_id AND base.base_date = vd.date
    ) vd_28d ON vd_28d.video_id = v.video_id
    ${where}
    ORDER BY ${orderBy}
    LIMIT ? OFFSET ?
  `;

  const countQuery = `SELECT COUNT(*) as count FROM videos v LEFT JOIN channels c ON c.channel_id = v.channel_id ${where}`;
  const total = (db.prepare(countQuery).get(...params) as any).count;
  const rows = db.prepare(query).all(day7ago, day28ago, ...params, limitNum, offset) as any[];
  const growthMap = buildVideoGrowthMap(db, rows);
  const normalizedRows = rows.map((row) => ({
    ...reconcileVideoLocalState(db, row, assetsRoot),
    ...(growthMap.get(String(row?.video_id || '').trim()) || {
      daily_view_increase: null,
      growth_series_7d: [],
    }),
  }));

  res.json({ data: normalizedRows, total, page: pageNum, limit: limitNum });
});

// GET /api/videos/:id
router.get('/:id', (req: Request, res: Response) => {
  const db = getDb();
  const assetsRoot = resolveAssetsRootPath();
  const video = db.prepare(`
    SELECT v.*,
      vd.view_count as latest_views,
      vd.like_count as latest_likes,
      vd.comment_count as latest_comments,
      vd.collect_count as latest_collects,
      vd.share_count as latest_shares,
      c.title as channel_title,
      c.handle as channel_handle,
      c.avatar_url as channel_avatar
    FROM videos v
    LEFT JOIN (
      SELECT video_id, view_count, like_count, comment_count, collect_count, share_count FROM video_daily
      WHERE date = (SELECT MAX(date) FROM video_daily vd2 WHERE vd2.video_id = video_daily.video_id)
    ) vd ON vd.video_id = v.video_id
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    WHERE v.video_id = ?
  `).get(req.params.id);

  if (!video) {
    res.status(404).json({ error: 'Video not found' });
    return;
  }
  const normalizedVideo = reconcileVideoLocalState(db, video, assetsRoot);
  const growth = buildVideoGrowthMap(db, [video]).get(String((video as any)?.video_id || '').trim());
  if (growth) {
    Object.assign(normalizedVideo, growth);
  } else {
    normalizedVideo.daily_view_increase = null;
    normalizedVideo.growth_series_7d = [];
  }
  res.json(normalizedVideo);
});

// PATCH /api/videos/:id
router.patch('/:id', (req: Request, res: Response) => {
  const db = getDb();
  const { tags, favorite } = req.body;
  const updates: string[] = [];
  const params: any[] = [];

  if (tags !== undefined) {
    updates.push('tags_json = ?');
    params.push(JSON.stringify(tags));
  }
  if (favorite !== undefined) {
    updates.push('favorite = ?');
    params.push(favorite ? 1 : 0);
  }

  if (updates.length === 0) {
    res.status(400).json({ error: 'No fields to update' });
    return;
  }

  params.push(req.params.id);
  const result = db.prepare(`UPDATE videos SET ${updates.join(', ')} WHERE video_id = ?`).run(...params);
  if (result.changes === 0) {
    res.status(404).json({ error: 'Video not found' });
    return;
  }

  const video = db.prepare('SELECT * FROM videos WHERE video_id = ?').get(req.params.id);
  res.json(video);
});

export default router;
