import { getDb, getSetting } from '../db.js';
import { v4 as uuidv4 } from 'uuid';
import * as ytdlp from './ytdlp.js';
import { fetchChannelSnapshotFromApi, fetchResearchChannelSnapshotFromApi } from './youtubeApi.js';
import { downloadSimilarChannelMetaBatch, downloadSimilarContentMetaBatch } from './toolsSimilar.js';
import {
  fetchTikTokDownloaderAccountFeed,
  fetchTikTokDownloaderVideoDetail,
  resolveTikTokDownloaderAccountId,
} from './tiktokDownloader.js';
import { fetchXhsSpiderAccountFeed } from './xhsSpider.js';
import { fetchDouyinChannelCardStatsByPlaywright } from './douyinPlaywright.js';
import { getPlaywrightHeadlessEnabled, getPlaywrightSessionEnabled } from './playwrightSession.js';
import { writeChannelViewGrowthCache } from './channelMetrics.js';
import { autoCollectYoutubeHitVideos } from '../routes/hits.js';
import { hasUsableYoutubeCookiePoolItems, isYoutubeCookiePoolEnabled } from './youtubeCookiePool.js';
import { syncReportingBinding } from './youtubeReportingSync.js';
import path from 'path';
import fs from 'fs';

type JobHandler = (job: any, logEvent: (level: string, message: string) => void, updateProgress: (progress: number) => void) => Promise<void>;

function isApiChannelUnavailableReason(reason?: string): boolean {
  if (!reason) return false;
  const lower = reason.toLowerCase();
  return (
    lower.includes('channel_not_found') ||
    lower === 'notfound' ||
    lower.includes('youtube_api_http_404') ||
    (lower.includes('not found') && lower.includes('channel'))
  );
}

function updateChannelMonitorState(
  db: ReturnType<typeof getDb>,
  channelId: string,
  status: 'ok' | 'invalid',
  reason: string | null = null
): void {
  const channel = db.prepare(`
    SELECT channel_id, title, handle, monitor_status
    FROM channels
    WHERE channel_id = ?
  `).get(channelId) as any;
  if (!channel) return;

  const previousStatus = String(channel.monitor_status || '').toLowerCase();
  const normalizedReason = reason || null;

  db.prepare(`
    UPDATE channels
    SET monitor_status = ?,
        monitor_reason = ?,
        monitor_checked_at = datetime('now'),
        last_sync_at = datetime('now')
    WHERE channel_id = ?
  `).run(status, normalizedReason, channelId);

  if (status === 'invalid') {
    if (previousStatus === 'invalid') {
      return;
    }

    const hasEverInvalidRecord = Boolean(
      (db.prepare('SELECT 1 as v FROM channel_invalid_events WHERE channel_id = ? LIMIT 1').get(channelId) as any)?.v
      || (db.prepare('SELECT 1 as v FROM channel_invalid_archive WHERE channel_id = ? LIMIT 1').get(channelId) as any)?.v,
    );
    if (hasEverInvalidRecord) {
      return;
    }

    db.prepare(`
      INSERT OR IGNORE INTO channel_invalid_archive (
        channel_id, title, handle, first_invalid_at, last_invalid_at, first_reason, last_reason, status, resolved_at
      )
      VALUES (?, ?, ?, datetime('now'), datetime('now'), ?, ?, 'active', NULL)
    `).run(channelId, channel.title || null, channel.handle || null, normalizedReason, normalizedReason);

    db.prepare(`
      INSERT INTO channel_invalid_events (channel_id, title, handle, detected_at, reason)
      VALUES (?, ?, ?, datetime('now'), ?)
    `).run(channelId, channel.title || null, channel.handle || null, normalizedReason);
    return;
  }

  // Keep deleted archive immutable once recorded.
}

function markVideoUnavailableArchive(
  db: ReturnType<typeof getDb>,
  videoId: string,
  reason: string,
  rawMessage: string | null = null,
): void {
  const video = db.prepare(`
    SELECT video_id, channel_id, title, webpage_url, availability_status
    FROM videos
    WHERE video_id = ?
  `).get(videoId) as any;
  if (!video) return;

  const hasEverUnavailableRecord = Boolean(
    (db.prepare('SELECT 1 as v FROM video_unavailable_events WHERE video_id = ? LIMIT 1').get(videoId) as any)?.v
    || (db.prepare('SELECT 1 as v FROM video_unavailable_archive WHERE video_id = ? LIMIT 1').get(videoId) as any)?.v,
  );
  if (hasEverUnavailableRecord) {
    return;
  }

  db.prepare(`
    INSERT OR IGNORE INTO video_unavailable_archive (
      video_id, channel_id, title, webpage_url, first_unavailable_at, last_unavailable_at, first_reason, last_reason, status, resolved_at
    )
    VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), ?, ?, 'active', NULL)
  `).run(
    videoId,
    video.channel_id || null,
    video.title || null,
    video.webpage_url || null,
    reason || null,
    reason || null,
  );

  db.prepare(`
    INSERT INTO video_unavailable_events (video_id, channel_id, title, detected_at, reason, raw_message)
    VALUES (?, ?, ?, datetime('now'), ?, ?)
  `).run(videoId, video.channel_id || null, video.title || null, reason || null, rawMessage);
}

function markVideoUnavailable(
  db: ReturnType<typeof getDb>,
  videoId: string,
  reason: string,
  rawMessage: string | null = null,
): void {
  db.prepare(`
    UPDATE videos
    SET availability_status = 'unavailable',
        unavailable_reason = ?,
        unavailable_at = COALESCE(unavailable_at, datetime('now'))
    WHERE video_id = ?
  `).run(reason, videoId);

  markVideoUnavailableArchive(db, videoId, reason, rawMessage);
}

function markVideoAvailableArchiveResolved(
  db: ReturnType<typeof getDb>,
  videoId: string,
): void {
  void db;
  void videoId;
  // Keep deleted archive immutable once recorded.
}

const DEFINITIVE_UNAVAILABLE_REASONS = new Set<string>([
  'removed_by_uploader',
  'private',
  'region_restricted',
  'video_unavailable',
  'channel_not_found',
]);

const TRANSIENT_UNAVAILABLE_REASONS = new Set<string>([
  'login_required',
  'age_restricted',
  'network_error',
  'http_403',
  'unknown',
  'js_runtime_missing',
  'format_not_available',
  'ffmpeg_error',
]);

function normalizeAvailabilityReason(reason: unknown): string {
  return String(reason || '').trim().toLowerCase();
}

function containsAny(text: string, patterns: string[]): boolean {
  return patterns.some((pattern) => text.includes(pattern));
}

function resolveUnavailableMarkReason(
  reason: unknown,
  rawMessage?: string | null,
): string | null {
  const normalized = normalizeAvailabilityReason(reason);
  const raw = String(rawMessage || '').trim().toLowerCase();

  if (TRANSIENT_UNAVAILABLE_REASONS.has(normalized)) return null;
  if (DEFINITIVE_UNAVAILABLE_REASONS.has(normalized)) return normalized;

  if (!raw) return null;

  const transientHints = [
    'unexpected_eof_while_reading',
    'ssl',
    'winerror 10054',
    'connection reset',
    'connection aborted',
    'timed out',
    'timeout',
    'network',
    'proxy',
    'remote end closed',
    'temporarily unavailable',
    'name resolution',
    'dns',
    'too many requests',
    'http error 429',
    'http error 502',
    'http error 503',
    'sign in',
    'login',
    'confirm your age',
    'cookies',
    'forbidden',
    'http error 403',
  ];
  if (containsAny(raw, transientHints)) return null;

  if (containsAny(raw, ['private video', 'is private'])) return 'private';
  if (containsAny(raw, ['not available in your country', 'geo'])) return 'region_restricted';
  if (containsAny(raw, ['has been removed', 'removed by the uploader', 'deleted'])) return 'removed_by_uploader';
  if (containsAny(raw, ['this video is unavailable', 'video unavailable'])) return 'video_unavailable';
  if (containsAny(raw, ['channel not found', 'this channel does not exist', 'the channel does not exist'])) {
    return 'channel_not_found';
  }

  return null;
}

const DEFAULT_CHANNEL_API_REFRESH_HOURS = 24;
type ChannelPlatform = 'youtube' | 'bilibili' | 'tiktok' | 'douyin' | 'xiaohongshu';

const SUPPORTED_CHANNEL_PLATFORMS = new Set<ChannelPlatform>([
  'youtube',
  'bilibili',
  'tiktok',
  'douyin',
  'xiaohongshu',
]);

function isSupportedRawChannelPlatform(raw: unknown): boolean {
  const value = String(raw || '').trim().toLowerCase();
  if (!value) return true;
  return SUPPORTED_CHANNEL_PLATFORMS.has(value as ChannelPlatform);
}

function normalizeChannelPlatform(raw: unknown): ChannelPlatform {
  const value = String(raw || '').trim().toLowerCase();
  if (SUPPORTED_CHANNEL_PLATFORMS.has(value as ChannelPlatform)) return value as ChannelPlatform;
  return 'youtube';
}

function getChannelApiRefreshHours(): number {
  const raw = parseInt(getSetting('youtube_api_channel_refresh_hours') || `${DEFAULT_CHANNEL_API_REFRESH_HOURS}`, 10);
  if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_CHANNEL_API_REFRESH_HOURS;
  return Math.min(raw, 24 * 30);
}

function toEpochFromSqliteTimestamp(value: unknown): number | null {
  if (typeof value !== 'string' || !value.trim()) return null;
  const normalized = value.trim().replace(' ', 'T');
  const withTimezone = /[zZ]|[+-]\d{2}:\d{2}$/.test(normalized) ? normalized : `${normalized}Z`;
  const epoch = Date.parse(withTimezone);
  if (!Number.isFinite(epoch)) return null;
  return epoch;
}

function hasChannelMetadataGap(channel: any): boolean {
  const numericMissing = channel.subscriber_count == null || channel.video_count == null || channel.view_count == null;
  const avatarMissing = !String(channel.avatar_url || '').trim();
  const countryMissing = !String(channel.country || '').trim();
  return numericMissing || avatarMissing || countryMissing;
}

function getRecentVideoFetchLimit(): number {
  const raw = parseInt(getSetting('recent_video_fetch_limit') || '50', 10);
  if (!Number.isFinite(raw) || raw <= 0) return 50;
  return Math.max(1, Math.min(500, raw));
}

function hasSuspiciousYoutubeVideoCount(channel: any): boolean {
  if (normalizeChannelPlatform(channel?.platform) !== 'youtube') return false;
  const videoCount = toNullableInt(channel?.video_count);
  if (videoCount == null || videoCount <= 0) return false;
  const fetchLimit = getRecentVideoFetchLimit();
  // A common failure mode: fallback metadata/aggregation writes the capped fetch count
  // instead of channel total video count (e.g. exactly 200 when fetch limit is 200).
  if (videoCount === fetchLimit) return true;
  if (videoCount === 200) return true;
  return false;
}

function getYoutubeCookiePoolSwitchOffWarning(): string | null {
  try {
    const poolEnabled = isYoutubeCookiePoolEnabled();
    const hasUsableItems = hasUsableYoutubeCookiePoolItems();
    if (!poolEnabled && hasUsableItems) {
      return 'CRITICAL: 检测到 YouTube Cookie 池存在可用项，但池调度开关为关闭状态；当前任务可能不会按池分发。请到设置页立即开启“Cookie 池调度”。';
    }
  } catch {
    // keep queue robust
  }
  return null;
}

function normalizeVideoTarget(raw: string): string | null {
  const value = String(raw || '').trim();
  if (!value) return null;
  if (/^[A-Za-z0-9_-]{11}$/.test(value)) {
    return `https://www.youtube.com/watch?v=${value}`;
  }

  let normalized = value;
  if (!/^https?:\/\//i.test(normalized) && /^www\./i.test(normalized)) {
    normalized = `https://${normalized}`;
  }
  if (!/^https?:\/\//i.test(normalized)) return null;

  try {
    const parsed = new URL(normalized);
    if (!parsed.hostname) return null;
    return parsed.toString();
  } catch {
    return null;
  }
}

function sanitizePathSegment(raw: string, fallback: string): string {
  const cleaned = String(raw || '')
    .trim()
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/\.+$/g, '')
    .replace(/^_+|_+$/g, '');
  return cleaned || fallback;
}

function sanitizeIdentifierSegment(raw: string, fallback: string): string {
  const cleaned = String(raw || '')
    .trim()
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/\.+$/g, '');
  return cleaned || fallback;
}

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

function normalizeEpochSeconds(value: unknown): number | null {
  const raw = toNullableNumber(value);
  if (raw == null) return null;
  if (raw > 1e12) return Math.floor(raw / 1000);
  return Math.floor(raw);
}

function epochToDateText(epochSec: number | null): string | null {
  if (epochSec == null) return null;
  const date = new Date(epochSec * 1000);
  if (Number.isNaN(date.getTime())) return null;
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function epochToIsoDateTimeText(epochSec: number | null): string | null {
  if (epochSec == null) return null;
  const date = new Date(epochSec * 1000);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function resolveEntryPublishedAt(entry: any): string | null {
  const uploadDate = String(entry?.upload_date || '').trim();
  if (/^\d{8}$/.test(uploadDate)) {
    return `${uploadDate.slice(0, 4)}-${uploadDate.slice(4, 6)}-${uploadDate.slice(6, 8)}`;
  }
  const ts = normalizeEpochSeconds(entry?.timestamp ?? entry?.release_timestamp ?? entry?.create_time);
  return epochToIsoDateTimeText(ts) ?? epochToDateText(ts);
}

function resolveEntryStats(entry: any): {
  viewCount: number | null;
  likeCount: number | null;
  commentCount: number | null;
  collectCount: number | null;
  shareCount: number | null;
} {
  const likeCount = toNullableInt(
    entry?.like_count
    ?? entry?.statistics?.digg_count
    ?? entry?.statistics?.diggCount
    ?? entry?.stats?.diggCount
    ?? entry?.statsV2?.diggCount
    ?? entry?.stat?.like
    ?? entry?.interact_info?.liked_count,
  );
  const commentCount = toNullableInt(
    entry?.comment_count
    ?? entry?.statistics?.comment_count
    ?? entry?.statistics?.commentCount
    ?? entry?.stats?.commentCount
    ?? entry?.statsV2?.commentCount
    ?? entry?.stat?.reply
    ?? entry?.interact_info?.comment_count,
  );
  const collectCount = toNullableInt(
    entry?.collect_count
    ?? entry?.statistics?.collect_count
    ?? entry?.statistics?.collectCount
    ?? entry?.stats?.collectCount
    ?? entry?.statsV2?.collectCount
    ?? entry?.stat?.collect
    ?? entry?.interact_info?.collect_count,
  );
  const shareCount = toNullableInt(
    entry?.share_count
    ?? entry?.statistics?.share_count
    ?? entry?.statistics?.shareCount
    ?? entry?.stats?.shareCount
    ?? entry?.statsV2?.shareCount
    ?? entry?.stat?.share
    ?? entry?.interact_info?.share_count,
  );
  let viewCount = toNullableInt(
    entry?.view_count
    ?? entry?.play_count
    ?? entry?.statistics?.play_count
    ?? entry?.statistics?.playCount
    ?? entry?.stats?.playCount
    ?? entry?.statsV2?.playCount
    ?? entry?.stat?.view,
  );
  if (
    viewCount === 0
    && [likeCount, commentCount, collectCount, shareCount].some((value) => value != null && value > 0)
  ) {
    viewCount = null;
  }
  return {
    viewCount,
    likeCount,
    commentCount,
    collectCount,
    shareCount,
  };
}

function resolveEntryTitle(entry: any): string {
  return String(entry?.title || entry?.desc || entry?.description || 'Untitled').trim() || 'Untitled';
}

function hasLowVersionPlaceholderText(value: unknown): boolean {
  const text = String(value || '').trim();
  if (!text) return false;
  return (
    text.includes('版本过低')
    || text.includes('升级后可展示全部信息')
  );
}

function hasLowVersionPlaceholderMeta(meta: any): boolean {
  if (!meta || typeof meta !== 'object') return false;
  return (
    hasLowVersionPlaceholderText(meta?.title)
    || hasLowVersionPlaceholderText(meta?.desc)
    || hasLowVersionPlaceholderText(meta?.description)
    || hasLowVersionPlaceholderText(meta?.preview_title)
  );
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

function readLocalVideoMeta(localMetaPath: unknown): any | null {
  const resolved = resolveExistingPath(localMetaPath);
  if (!resolved) return null;
  try {
    return JSON.parse(fs.readFileSync(resolved, 'utf8'));
  } catch {
    return null;
  }
}

function hasRichTikTokDownloaderRawPayload(raw: any): boolean {
  if (!raw || typeof raw !== 'object') return false;
  const keyCount = Object.keys(raw).length;
  if (keyCount >= 40) return true;
  const hasStats = [raw?.statistics, raw?.stats, raw?.statsV2, raw?.stat, raw?.interact_info]
    .some((value) => value && typeof value === 'object' && Object.keys(value).length > 0);
  const hasAuthor = raw?.author && typeof raw.author === 'object' && Object.keys(raw.author).length > 0;
  const hasVideo = raw?.video && typeof raw.video === 'object' && (
    raw.video?.play_addr
    || (Array.isArray(raw.video?.bit_rate) && raw.video.bit_rate.length > 0)
    || toNullableInt(raw.video?.width) != null
    || toNullableInt(raw.video?.height) != null
  );
  const hasImages = (
    (Array.isArray(raw?.images) && raw.images.length > 0)
    || (Array.isArray(raw?.image_infos) && raw.image_infos.length > 0)
    || (Array.isArray(raw?.image_list) && raw.image_list.length > 0)
    || (Array.isArray(raw?.imagePost?.images) && raw.imagePost.images.length > 0)
  );
  return hasStats || hasAuthor || hasVideo || hasImages || toNullableInt(raw?.aweme_type) != null;
}

function isSparseTikTokDownloaderMeta(meta: any): boolean {
  if (!meta || typeof meta !== 'object') return true;
  const raw = meta?.raw && typeof meta.raw === 'object' ? meta.raw : null;
  const richRaw = hasRichTikTokDownloaderRawPayload(raw);
  try {
    const parsed = ytdlp.parseVideoMeta(meta);
    const hasStats = [
      parsed.view_count,
      parsed.like_count,
      parsed.comment_count,
      parsed.collect_count,
      parsed.share_count,
    ].some((value) => value != null);
    const visualKind = classifyDouyinVisualKind(raw || meta || {});
    const isAlbumLike = (
      visualKind !== 'video'
      || String(parsed.content_type || '').toLowerCase() === 'album'
      || String(parsed.content_type || '').toLowerCase() === 'note'
    );
    const missingDuration = !isAlbumLike && parsed.duration_sec == null;
    return (
      !richRaw
      || parsed.title === 'Untitled'
      || missingDuration
      || !hasStats
    );
  } catch {
    return true;
  }
}

function isUnknownChannelTitle(value: unknown): boolean {
  const rawText = String(value || '').trim();
  const text = rawText.toLowerCase();
  if (text === '' || text === 'unknown') return true;
  // Bilibili UID-like placeholder title (for example: "4401694")
  if (/^\d{4,}$/.test(rawText)) return true;
  return false;
}

function isDouyinFreshCookieError(errorText: unknown): boolean {
  const lower = String(errorText || '').toLowerCase();
  if (!lower) return false;
  return (
    lower.includes('fresh cookies') ||
    lower.includes('[douyin]') && lower.includes('failed to parse json') ||
    lower.includes('failed to parse json')
  );
}

function buildSyntheticDouyinMetaFromVideo(video: any): any {
  const webpage = String(video?.webpage_url || '').trim();
  const rawId = String(video?.video_id || '').trim();
  const fallbackId = rawId.includes('__') ? rawId.split('__').slice(1).join('__') : rawId;
  const uploader = String(video?.uploader || '').trim();
  const channelId = String(video?.channel_id || '').trim();
  const ts = (() => {
    const raw = String(video?.published_at || '').trim();
    if (!raw) return null;
    const parsed = Date.parse(raw.includes('T') ? raw : `${raw}T00:00:00Z`);
    if (!Number.isFinite(parsed)) return null;
    return Math.floor(parsed / 1000);
  })();
  const uploadDate = (() => {
    const raw = String(video?.published_at || '').trim();
    if (!raw) return null;
    const normalized = raw.slice(0, 10).replace(/-/g, '');
    return /^\d{8}$/.test(normalized) ? normalized : null;
  })();

  return {
    extractor: 'douyin:webapi',
    extractor_key: 'Douyin',
    id: fallbackId || rawId,
    aweme_id: fallbackId || rawId,
    title: String(video?.title || 'Untitled').trim() || 'Untitled',
    description: String(video?.description || '').trim(),
    webpage_url: webpage || (fallbackId ? `https://www.douyin.com/video/${fallbackId}` : null),
    uploader: uploader || null,
    channel: uploader || null,
    channel_id: channelId || null,
    uploader_id: channelId || null,
    duration: toNullableInt(video?.duration_sec),
    timestamp: ts,
    upload_date: uploadDate,
    view_count: toNullableInt(video?.view_count),
    like_count: toNullableInt(video?.like_count),
    comment_count: toNullableInt(video?.comment_count),
    collect_count: toNullableInt(video?.collect_count),
    share_count: toNullableInt(video?.share_count),
  };
}

function resolvePlatformCookieHeader(platform: ChannelPlatform): string | null {
  if (platform === 'tiktok') return resolveTiktokCookieHeader();
  if (platform === 'douyin') return resolveDouyinCookieHeader();
  if (platform === 'xiaohongshu') return resolveXiaohongshuCookieHeader();
  return null;
}

function stripPlatformVideoPrefix(platform: ChannelPlatform, videoId: string): string {
  const raw = String(videoId || '').trim();
  const prefix = `${platform}__`;
  if (raw.toLowerCase().startsWith(prefix.toLowerCase())) {
    return raw.slice(prefix.length);
  }
  return raw;
}

function buildTikTokDownloaderDetailInput(platform: ChannelPlatform, rawVideoId: string, webpageUrl: string | null): string {
  const idValue = String(rawVideoId || '').trim();
  if (webpageUrl && /^https?:\/\//i.test(webpageUrl)) return webpageUrl;
  if (platform === 'tiktok') {
    return idValue ? `https://www.tiktok.com/video/${idValue}` : '';
  }
  if (platform === 'douyin') {
    return idValue ? `https://www.douyin.com/video/${idValue}` : '';
  }
  return idValue;
}

function buildTikTokDownloaderVideoMeta(item: any, platform: ChannelPlatform): any {
  const raw = item?.raw && typeof item.raw === 'object' ? item.raw : null;
  const rawId = String(item?.id || raw?.id || raw?.aweme_id || '').trim();
  const douyinKind = platform === 'douyin'
    ? (resolveDouyinKindFromContentType(item?.content_type) || classifyDouyinVisualKind(raw || item || {}))
    : 'video';
  const douyinContentType: VideoContentType | undefined = platform === 'douyin'
    ? resolveDouyinVideoContentType(raw || item || {})
    : undefined;
  const title = String(item?.title || raw?.title || raw?.desc || 'Untitled').trim() || 'Untitled';
  const description = String(item?.description || raw?.description || raw?.desc || '').trim();
  const webpageUrl = String(item?.webpage_url || raw?.webpage_url || raw?.share_url || '').trim()
    || (platform === 'douyin' && douyinKind !== 'video'
      ? `https://www.douyin.com/note/${rawId}`
      : (buildFallbackVideoUrl(platform, rawId) || ''));
  const uploader = String(
    item?.uploader
    || raw?.uploader
    || raw?.channel
    || raw?.nickname
    || raw?.author?.nickname
    || raw?.user?.nickname
    || '',
  ).trim() || null;
  const channelId = String(
    item?.channel_id
    || raw?.channel_id
    || raw?.sec_uid
    || raw?.author?.secUid
    || raw?.author?.sec_uid
    || raw?.author?.uid
    || raw?.uid
    || '',
  ).trim() || null;
  const uploaderId = String(
    item?.unique_id
    || item?.uploader_id
    || raw?.unique_id
    || raw?.uploader_id
    || raw?.author?.uniqueId
    || raw?.author?.unique_id
    || raw?.author?.uid
    || raw?.uid
    || '',
  ).trim() || null;
  const timestamp = toNullableInt(
    item?.timestamp
    ?? raw?.create_timestamp
    ?? raw?.timestamp
    ?? raw?.create_time
    ?? raw?.createTime
    ?? raw?.release_timestamp,
  );
  const uploadDate = (() => {
    if (timestamp == null) return null;
    const iso = epochToDateText(timestamp);
    return iso ? iso.replace(/-/g, '') : null;
  })();
  let durationSec = toNullableInt(item?.duration_sec ?? raw?.duration ?? raw?.video?.duration ?? raw?.duration_ms);
  if (platform === 'douyin' && durationSec != null && durationSec > 1000) {
    durationSec = Math.max(1, Math.round(durationSec / 1000));
  }
  const thumb = extractEntryThumbnailUrl(item) || extractEntryThumbnailUrl(raw);
  const tags = (
    Array.isArray(item?.tags)
    ? item.tags.map((tag: any) => String(tag || '').trim()).filter(Boolean)
    : extractEntryTags(raw || {})
  );
  const textExtra = tags.map((tag: string) => ({ hashtag_name: tag }));
  const viewCount = toNullableInt(
    item?.view_count
    ?? raw?.play_count
    ?? raw?.view_count
    ?? raw?.statistics?.play_count
    ?? raw?.statistics?.playCount
    ?? raw?.stats?.playCount
    ?? raw?.statsV2?.playCount
    ?? raw?.stat?.view,
  );
  const likeCount = toNullableInt(
    item?.like_count
    ?? raw?.digg_count
    ?? raw?.like_count
    ?? raw?.statistics?.digg_count
    ?? raw?.statistics?.diggCount
    ?? raw?.stats?.diggCount
    ?? raw?.statsV2?.diggCount
    ?? raw?.stat?.like
    ?? raw?.interact_info?.liked_count,
  );
  const commentCount = toNullableInt(
    item?.comment_count
    ?? raw?.comment_count
    ?? raw?.statistics?.comment_count
    ?? raw?.statistics?.commentCount
    ?? raw?.stats?.commentCount
    ?? raw?.statsV2?.commentCount
    ?? raw?.stat?.reply
    ?? raw?.interact_info?.comment_count,
  );
  const collectCount = toNullableInt(
    item?.collect_count
    ?? raw?.collect_count
    ?? raw?.statistics?.collect_count
    ?? raw?.statistics?.collectCount
    ?? raw?.stats?.collectCount
    ?? raw?.statsV2?.collectCount
    ?? raw?.stat?.collect
    ?? raw?.interact_info?.collect_count,
  );
  const shareCount = toNullableInt(
    item?.share_count
    ?? raw?.share_count
    ?? raw?.statistics?.share_count
    ?? raw?.statistics?.shareCount
    ?? raw?.stats?.shareCount
    ?? raw?.statsV2?.shareCount
    ?? raw?.stat?.share
    ?? raw?.interact_info?.share_count,
  );

  const extractorBase = platform === 'tiktok' ? 'tiktok' : 'douyin';
  const extractorKey = platform === 'tiktok' ? 'TikTok' : 'Douyin';

  return {
    extractor: extractorBase,
    extractor_key: extractorKey,
    id: rawId,
    aweme_id: platform === 'douyin' ? rawId : undefined,
    title,
    desc: description || title,
    description: description || null,
    webpage_url: webpageUrl || null,
    share_url: webpageUrl || null,
    uploader,
    channel: uploader,
    channel_id: channelId,
    uploader_id: uploaderId,
    timestamp,
    create_time: timestamp,
    upload_date: uploadDate,
    duration: durationSec,
    view_count: viewCount,
    like_count: likeCount,
    comment_count: commentCount,
    collect_count: collectCount,
    share_count: shareCount,
    thumbnail: thumb,
    static_cover: thumb ? { url_list: [thumb] } : undefined,
    dynamic_cover: thumb ? { url_list: [thumb] } : undefined,
    origin_cover: thumb ? { url_list: [thumb] } : undefined,
    tag: tags,
    text_extra: textExtra,
    content_type: douyinContentType,
    aweme_type: raw?.aweme_type,
    statistics: raw?.statistics,
    stats: raw?.stats,
    statsV2: raw?.statsV2,
    stat: raw?.stat,
    interact_info: raw?.interact_info,
    images: raw?.images,
    image_infos: raw?.image_infos,
    image_list: raw?.image_list,
    imagePost: raw?.imagePost,
    image_post_info: raw?.image_post_info,
    video: raw?.video,
    cover: raw?.cover,
    raw: raw || undefined,
  };
}

function readUrlFromUnknownLocal(value: unknown): string | null {
  if (typeof value === 'string') {
    const text = value.trim();
    if (/^https?:\/\//i.test(text)) return text;
    if (text.startsWith('//')) return `https:${text}`;
    return null;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = readUrlFromUnknownLocal(item);
      if (found) return found;
    }
    return null;
  }
  if (value && typeof value === 'object') {
    const anyValue = value as any;
    const direct = readUrlFromUnknownLocal(anyValue.url);
    if (direct) return direct;
    const listUrl = readUrlFromUnknownLocal(anyValue.url_list);
    if (listUrl) return listUrl;
    const infoListUrl = readUrlFromUnknownLocal(anyValue.info_list);
    if (infoListUrl) return infoListUrl;
    const urlDefaultSet = readUrlFromUnknownLocal(anyValue.url_default_set);
    if (urlDefaultSet) return urlDefaultSet;
    const originUrl = readUrlFromUnknownLocal(anyValue.origin);
    if (originUrl) return originUrl;
    const playAddrUrl = readUrlFromUnknownLocal(anyValue.play_addr);
    if (playAddrUrl) return playAddrUrl;
    const masterUrl = readUrlFromUnknownLocal(anyValue.master_url);
    if (masterUrl) return masterUrl;
    const srcUrl = readUrlFromUnknownLocal(anyValue.src);
    if (srcUrl) return srcUrl;
  }
  return null;
}

function collectUrlsFromUnknownLocal(value: unknown, out: string[], seen: Set<string>): void {
  if (value == null) return;
  if (typeof value === 'string') {
    const text = value.trim();
    if (!text) return;
    const normalized = text.startsWith('//') ? `https:${text}` : text;
    if (!/^https?:\/\//i.test(normalized)) return;
    if (seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      collectUrlsFromUnknownLocal(item, out, seen);
    }
    return;
  }
  if (value && typeof value === 'object') {
    const row = value as any;
    collectUrlsFromUnknownLocal(row.url, out, seen);
    collectUrlsFromUnknownLocal(row.url_list, out, seen);
    collectUrlsFromUnknownLocal(row.info_list, out, seen);
    collectUrlsFromUnknownLocal(row.url_default_set, out, seen);
    collectUrlsFromUnknownLocal(row.origin, out, seen);
    collectUrlsFromUnknownLocal(row.src, out, seen);
  }
}

function extractXhsImageUrlList(entry: any): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const candidates = [
    entry?.image_list,
    entry?.images_list,
    entry?.images,
    entry?.raw?.image_list,
    entry?.raw?.images_list,
    entry?.raw?.images,
    entry?.raw?.note_card?.image_list,
    entry?.raw?.note_card?.images_list,
    entry?.raw?.note_card?.images,
    entry?.raw?.note_card?.cover,
  ];
  for (const candidate of candidates) {
    collectUrlsFromUnknownLocal(candidate, out, seen);
  }
  return out;
}

function formatTimestampToLocalText(timestampSec: number | null): string | null {
  if (timestampSec == null) return null;
  const date = new Date(timestampSec * 1000);
  if (Number.isNaN(date.getTime())) return null;
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function buildXiaohongshuVideoMeta(entry: any, fallbackWebUrl: string | null): any {
  const raw = entry?.raw && typeof entry.raw === 'object'
    ? entry.raw
    : (entry && typeof entry === 'object' ? entry : {});
  const noteId = String(entry?.id || raw?.id || raw?.note_id || raw?.note_card?.note_id || '').trim();
  const timestamp = toNullableInt(entry?.timestamp ?? raw?.timestamp ?? raw?.time ?? raw?.note_card?.time ?? raw?.create_time);
  const publishedAtIso = epochToIsoDateTimeText(timestamp) || null;
  const uploadTime = formatTimestampToLocalText(timestamp)
    || String(raw?.upload_time || raw?.publish_time || '').trim()
    || null;
  const noteUrl = String(entry?.webpage_url || raw?.url || raw?.share_url || fallbackWebUrl || '').trim() || null;
  const noteTypeRaw = String(raw?.note_card?.type || raw?.type || '').trim().toLowerCase();
  const contentTypeRaw = String(entry?.content_type || raw?.content_type || '').trim().toLowerCase();
  const noteType = noteTypeRaw || (contentTypeRaw === 'album' ? 'normal' : 'video');
  const noteTypeLabel = (contentTypeRaw === 'album' || noteType === 'normal') ? '图集' : '视频';

  const userId = String(
    entry?.channel_id
    || entry?.uploader_id
    || raw?.note_card?.user?.user_id
    || raw?.user?.user_id
    || raw?.author?.user_id
    || '',
  ).trim() || null;
  const nickname = String(
    entry?.uploader
    || raw?.note_card?.user?.nickname
    || raw?.user?.nickname
    || raw?.author?.nickname
    || '',
  ).trim() || null;
  const avatar = readUrlFromUnknownLocal(
    raw?.note_card?.user?.avatar
    || raw?.user?.avatar
    || raw?.author?.avatar
    || raw?.note_card?.user?.images
    || raw?.note_card?.user?.imageb,
  ) || null;
  const homeUrl = userId ? `https://www.xiaohongshu.com/user/profile/${userId}` : null;
  const title = String(entry?.title || raw?.title || raw?.display_title || raw?.note_card?.title || raw?.desc || 'Untitled').trim() || 'Untitled';
  const description = String(entry?.description || raw?.description || raw?.desc || raw?.note_card?.desc || '').trim();

  const likeCount = toNullableInt(entry?.like_count ?? raw?.like_count ?? raw?.liked_count ?? raw?.note_card?.interact_info?.liked_count);
  const collectCount = toNullableInt(entry?.collect_count ?? raw?.collect_count ?? raw?.collected_count ?? raw?.note_card?.interact_info?.collected_count);
  const commentCount = toNullableInt(entry?.comment_count ?? raw?.comment_count ?? raw?.note_card?.interact_info?.comment_count);
  const shareCount = toNullableInt(entry?.share_count ?? raw?.share_count ?? raw?.note_card?.interact_info?.share_count);
  const viewCount = toNullableInt(entry?.view_count ?? raw?.view_count ?? raw?.play_count ?? raw?.note_card?.play_count);

  const imageList = extractXhsImageUrlList(entry);
  const thumb = extractEntryThumbnailUrl(entry) || imageList[0] || null;
  const originVideoKey = String(raw?.note_card?.video?.consumer?.origin_video_key || raw?.video?.consumer?.origin_video_key || '').trim();
  const videoAddr = readUrlFromUnknownLocal(
    raw?.note_card?.video?.media?.master_url
    || raw?.video?.media?.master_url
    || raw?.note_card?.video?.media?.h264
    || raw?.video?.media?.h264
    || raw?.note_card?.video?.media?.stream
    || raw?.video?.media?.stream,
  ) || (originVideoKey ? `https://sns-video-bd.xhscdn.com/${originVideoKey}` : null);

  const tags = Array.isArray(entry?.tags)
    ? entry.tags.map((tag: any) => String(tag || '').trim()).filter(Boolean)
    : extractEntryTags(raw || {});

  return {
    extractor: 'xiaohongshu:spider_xhs',
    extractor_key: 'Xiaohongshu',
    id: noteId || null,
    note_id: noteId || null,
    note_url: noteUrl,
    note_type: noteType,
    note_type_label: noteTypeLabel,
    user_id: userId,
    home_url: homeUrl,
    nickname,
    avatar,
    title,
    desc: description || title,
    description: description || null,
    liked_count: likeCount,
    collected_count: collectCount,
    comment_count: commentCount,
    share_count: shareCount,
    view_count: viewCount,
    video_cover: thumb,
    video_addr: videoAddr,
    image_list: imageList,
    tags,
    tag: tags,
    upload_time: uploadTime,
    published_at: publishedAtIso,
    timestamp,
    duration_sec: toNullableInt(entry?.duration_sec ?? raw?.duration ?? raw?.note_card?.video?.duration),
    webpage_url: noteUrl,
    share_url: noteUrl,
    uploader: nickname,
    channel: nickname,
    channel_id: userId,
    uploader_id: userId,
    thumbnail: thumb,
    like_count: likeCount,
    collect_count: collectCount,
    interact_info: {
      liked_count: likeCount,
      collected_count: collectCount,
      comment_count: commentCount,
      share_count: shareCount,
      view_count: viewCount,
    },
    ip_location: String(raw?.note_card?.ip_location || raw?.ip_location || '').trim() || null,
    content_type: String(entry?.content_type || raw?.content_type || '').trim().toLowerCase() || undefined,
    cover: raw?.note_card?.cover || raw?.cover,
    video: raw?.note_card?.video || raw?.video,
    note_card: raw?.note_card,
    raw,
  };
}

function extractEntryThumbnailUrl(entry: any): string | null {
  return (
    readUrlFromUnknownLocal(entry?.thumbnail)
    || readUrlFromUnknownLocal(entry?.thumbnails)
    || readUrlFromUnknownLocal(entry?.cover)
    || readUrlFromUnknownLocal(entry?.static_cover)
    || readUrlFromUnknownLocal(entry?.dynamic_cover)
    || readUrlFromUnknownLocal(entry?.origin_cover)
    || readUrlFromUnknownLocal(entry?.video?.cover)
    || readUrlFromUnknownLocal(entry?.video?.dynamic_cover)
    || readUrlFromUnknownLocal(entry?.video?.origin_cover)
    || readUrlFromUnknownLocal(entry?.images)
    || readUrlFromUnknownLocal(entry?.image_infos)
    || readUrlFromUnknownLocal(entry?.image_list)
    || readUrlFromUnknownLocal(entry?.imagePost?.images)
    || readUrlFromUnknownLocal(entry?.image_post_info?.images)
    || readUrlFromUnknownLocal(entry?.raw?.thumbnail)
    || readUrlFromUnknownLocal(entry?.raw?.cover)
    || readUrlFromUnknownLocal(entry?.raw?.images)
    || readUrlFromUnknownLocal(entry?.raw?.image_infos)
    || readUrlFromUnknownLocal(entry?.raw?.image_list)
    || readUrlFromUnknownLocal(entry?.raw?.imagePost?.images)
    || readUrlFromUnknownLocal(entry?.raw?.image_post_info?.images)
    || readUrlFromUnknownLocal(entry?.raw?.video?.cover)
    || readUrlFromUnknownLocal(entry?.raw?.video?.dynamic_cover)
    || readUrlFromUnknownLocal(entry?.raw?.video?.origin_cover)
    || null
  );
}

function normalizeTikTokDownloaderFeedEntry(platform: ChannelPlatform, item: any): any {
  const rawId = String(item?.id || '').trim();
  const raw = item?.raw && typeof item.raw === 'object' ? item.raw : null;
  const douyinKind = platform === 'douyin'
    ? (resolveDouyinKindFromContentType(item?.content_type) || classifyDouyinVisualKind(raw || item || {}))
    : 'video';
  const fallbackUrl = (
    platform === 'douyin' && douyinKind !== 'video'
      ? `https://www.douyin.com/note/${rawId}`
      : (buildFallbackVideoUrl(platform, rawId) || '')
  );
  const webpageUrl = String(item?.webpage_url || raw?.webpage_url || raw?.share_url || '').trim() || fallbackUrl;
  const title = String(item?.title || raw?.title || raw?.desc || 'Untitled').trim() || 'Untitled';
  const description = String(item?.description || raw?.description || raw?.desc || '').trim() || null;
  const thumb = extractEntryThumbnailUrl(item) || extractEntryThumbnailUrl(raw);
  const tags = Array.isArray(item?.tags)
    ? item.tags.map((tag: any) => String(tag || '').trim()).filter(Boolean)
    : extractEntryTags(raw || {});
  let durationSec = toNullableInt(item?.duration_sec ?? raw?.duration ?? raw?.video?.duration ?? raw?.duration_ms);
  if (platform === 'douyin' && durationSec != null && durationSec > 1000) {
    durationSec = Math.max(1, Math.round(durationSec / 1000));
  }

  return {
    id: rawId,
    title,
    desc: description || title,
    description,
    url: webpageUrl,
    webpage_url: webpageUrl,
    uploader: String(item?.uploader || '').trim() || null,
    channel: String(item?.uploader || '').trim() || null,
    channel_id: String(item?.channel_id || raw?.sec_uid || raw?.author?.sec_uid || raw?.author?.secUid || '').trim() || null,
    uploader_id: String(item?.unique_id || item?.uploader_id || raw?.uid || raw?.author?.uid || raw?.author?.unique_id || raw?.author?.uniqueId || '').trim() || null,
    timestamp: toNullableInt(item?.timestamp ?? raw?.create_timestamp ?? raw?.timestamp ?? raw?.create_time ?? raw?.createTime ?? raw?.release_timestamp),
    create_time: toNullableInt(item?.timestamp ?? raw?.create_timestamp ?? raw?.timestamp ?? raw?.create_time ?? raw?.createTime ?? raw?.release_timestamp),
    duration: durationSec,
    view_count: toNullableInt(item?.view_count ?? raw?.play_count ?? raw?.view_count ?? raw?.statistics?.play_count ?? raw?.statistics?.playCount ?? raw?.stats?.playCount ?? raw?.statsV2?.playCount ?? raw?.stat?.view),
    like_count: toNullableInt(item?.like_count ?? raw?.digg_count ?? raw?.like_count ?? raw?.statistics?.digg_count ?? raw?.statistics?.diggCount ?? raw?.stats?.diggCount ?? raw?.statsV2?.diggCount ?? raw?.stat?.like ?? raw?.interact_info?.liked_count),
    comment_count: toNullableInt(item?.comment_count ?? raw?.comment_count ?? raw?.statistics?.comment_count ?? raw?.statistics?.commentCount ?? raw?.stats?.commentCount ?? raw?.statsV2?.commentCount ?? raw?.stat?.reply ?? raw?.interact_info?.comment_count),
    collect_count: toNullableInt(item?.collect_count ?? raw?.collect_count ?? raw?.statistics?.collect_count ?? raw?.statistics?.collectCount ?? raw?.stats?.collectCount ?? raw?.statsV2?.collectCount ?? raw?.stat?.collect ?? raw?.interact_info?.collect_count),
    share_count: toNullableInt(item?.share_count ?? raw?.share_count ?? raw?.statistics?.share_count ?? raw?.statistics?.shareCount ?? raw?.stats?.shareCount ?? raw?.statsV2?.shareCount ?? raw?.stat?.share ?? raw?.interact_info?.share_count),
    thumbnail: thumb,
    content_type: platform === 'douyin'
      ? resolveDouyinVideoContentType(raw || item || {})
      : 'short',
    static_cover: thumb ? { url_list: [thumb] } : undefined,
    dynamic_cover: thumb ? { url_list: [thumb] } : undefined,
    tag: tags,
    text_extra: tags.map((tag: string) => ({ hashtag_name: tag })),
    images: raw?.images,
    image_infos: raw?.image_infos,
    image_list: raw?.image_list,
    imagePost: raw?.imagePost,
    image_post_info: raw?.image_post_info,
    aweme_type: raw?.aweme_type,
    statistics: raw?.statistics,
    stats: raw?.stats,
    statsV2: raw?.statsV2,
    stat: raw?.stat,
    interact_info: raw?.interact_info,
    raw: raw || undefined,
  };
}

function extractEntryTags(entry: any): string[] {
  const tags: string[] = [];
  const push = (value: unknown) => {
    const text = String(value || '').trim().replace(/^#+/, '');
    if (!text) return;
    if (tags.some((item) => item.toLowerCase() === text.toLowerCase())) return;
    tags.push(text);
  };

  if (Array.isArray(entry?.tag)) {
    for (const item of entry.tag) push(item);
  } else if (typeof entry?.tag === 'string') {
    for (const item of entry.tag.split(/[\s,，]+/)) push(item);
  }

  if (Array.isArray(entry?.text_extra)) {
    for (const item of entry.text_extra) {
      push(item?.hashtag_name || item?.hashtagName || item?.tag_name || item?.tagName);
    }
  }
  return tags;
}

function persistPlatformFeedMeta(
  channelId: string,
  storageVideoId: string,
  platform: ChannelPlatform,
  entry: any,
  fallbackWebUrl: string | null,
): string | null {
  if (platform !== 'tiktok' && platform !== 'douyin' && platform !== 'xiaohongshu') return null;
  const rawVideoId = String(entry?.id || '').trim();
  if (!rawVideoId) return null;

  let meta: any;
  if (platform === 'xiaohongshu') {
    meta = buildXiaohongshuVideoMeta(entry, resolveEntryWebpageUrl(entry) || fallbackWebUrl || null);
  } else {
    const normalized = {
      id: rawVideoId,
      title: String(entry?.title || entry?.desc || 'Untitled').trim() || 'Untitled',
      description: String(entry?.description || entry?.desc || '').trim() || null,
      uploader: String(entry?.uploader || entry?.channel || '').trim() || null,
      channel_id: String(entry?.channel_id || '').trim() || null,
      uploader_id: String(entry?.uploader_id || '').trim() || null,
      unique_id: String(entry?.unique_id || entry?.uploader_id || '').trim() || null,
      timestamp: toNullableInt(entry?.timestamp ?? entry?.create_time),
      duration_sec: toNullableInt(entry?.duration),
      view_count: toNullableInt(entry?.view_count),
      like_count: toNullableInt(entry?.like_count),
      comment_count: toNullableInt(entry?.comment_count),
      collect_count: toNullableInt(entry?.collect_count),
      share_count: toNullableInt(entry?.share_count),
      webpage_url: resolveEntryWebpageUrl(entry) || fallbackWebUrl || null,
      thumbnail: extractEntryThumbnailUrl(entry),
      tags: extractEntryTags(entry),
      images: entry?.images,
      image_infos: entry?.image_infos,
      image_list: entry?.image_list,
      imagePost: entry?.imagePost,
      image_post_info: entry?.image_post_info,
      aweme_type: entry?.aweme_type,
      video: entry?.video,
      statistics: entry?.statistics,
      stats: entry?.stats,
      statsV2: entry?.statsV2,
      stat: entry?.stat,
      interact_info: entry?.interact_info,
      raw: (entry?.raw && typeof entry.raw === 'object')
        ? entry.raw
        : (entry && typeof entry === 'object' ? entry : undefined),
    };
    meta = buildTikTokDownloaderVideoMeta(normalized, platform);
  }

  const root = getSetting('download_root') || path.join(process.cwd(), 'downloads');
  const dir = path.join(root, 'assets', 'meta', channelId, storageVideoId);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const metaPath = path.join(dir, `${storageVideoId}.info.json`);
  try {
    if (platform === 'tiktok' || platform === 'douyin') {
      const existingMeta = readLocalVideoMeta(metaPath);
      if (existingMeta) {
        const existingSparse = isSparseTikTokDownloaderMeta(existingMeta);
        const incomingSparse = isSparseTikTokDownloaderMeta(meta);
        if (!existingSparse && incomingSparse) {
          return metaPath;
        }
      }
    }
    fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), 'utf8');
    return metaPath;
  } catch {
    return null;
  }
}

function resolveBilibiliUid(channel: any, channelId: string): string | null {
  const fromChannelId = stripStoredChannelPrefix(channelId, 'bilibili');
  if (/^\d+$/.test(fromChannelId)) return fromChannelId;

  const fromHandle = String(channel?.handle || '').trim().replace(/^@+/, '');
  if (/^\d+$/.test(fromHandle)) return fromHandle;

  const sourceUrl = String(channel?.source_url || '').trim();
  if (!sourceUrl) return null;
  try {
    const parsed = new URL(sourceUrl);
    const parts = parsed.pathname.split('/').filter(Boolean);
    if (parsed.hostname.replace(/^www\./, '').toLowerCase() === 'space.bilibili.com' && /^\d+$/.test(parts[0] || '')) {
      return parts[0];
    }
    if ((parts[0] || '').toLowerCase() === 'space' && /^\d+$/.test(parts[1] || '')) {
      return parts[1];
    }
  } catch {}
  return null;
}

async function fetchBilibiliRelationStat(
  uid: string,
  abortSignal?: AbortSignal,
): Promise<{ follower: number | null; following: number | null; reason?: string }> {
  if (!/^\d+$/.test(uid)) {
    return { follower: null, following: null, reason: 'invalid_uid' };
  }

  const url = `https://api.bilibili.com/x/relation/stat?vmid=${encodeURIComponent(uid)}`;
  const payloadResult = await fetchBilibiliApiJsonWithRetry(url, abortSignal);
  if (!payloadResult.payload) {
    return { follower: null, following: null, reason: payloadResult.reason || 'request_failed' };
  }
  const payload = payloadResult.payload;
  const follower = toNullableInt(payload?.data?.follower);
  const following = toNullableInt(payload?.data?.following);
  return { follower, following };
}

function isRetryableBilibiliFailure(httpStatus: number | null, apiCode: number | null): boolean {
  if (httpStatus != null && [403, 408, 412, 425, 429, 500, 502, 503, 504].includes(httpStatus)) {
    return true;
  }
  if (apiCode != null && [-412, -352, -799, -509].includes(apiCode)) {
    return true;
  }
  return false;
}

function waitWithAbort(ms: number, abortSignal?: AbortSignal): Promise<boolean> {
  return new Promise((resolve) => {
    if (!abortSignal) {
      setTimeout(() => resolve(true), ms);
      return;
    }
    if (abortSignal.aborted) {
      resolve(false);
      return;
    }

    const timer = setTimeout(() => {
      abortSignal.removeEventListener('abort', onAbort);
      resolve(true);
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      abortSignal.removeEventListener('abort', onAbort);
      resolve(false);
    };
    abortSignal.addEventListener('abort', onAbort, { once: true });
  });
}

async function fetchBilibiliApiJsonWithRetry(
  url: string,
  abortSignal?: AbortSignal,
  maxAttempts = 4,
): Promise<{ payload: any | null; reason?: string; attempts: number }> {
  let lastReason = 'request_failed';
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'accept': 'application/json',
          'user-agent': 'ytmonitor/1.0',
        },
        signal: abortSignal,
      });
      if (!response.ok) {
        const reason = `http_${response.status}`;
        const canRetry = isRetryableBilibiliFailure(response.status, null);
        lastReason = reason;
        if (!canRetry || attempt >= maxAttempts) {
          return { payload: null, reason, attempts: attempt };
        }
      } else {
        const payload = await response.json() as any;
        const code = Number(payload?.code || 0);
        if (code === 0) {
          return { payload, attempts: attempt };
        }
        const reason = `api_code_${String(payload?.code ?? 'unknown')}`;
        const canRetry = isRetryableBilibiliFailure(null, code);
        lastReason = reason;
        if (!canRetry || attempt >= maxAttempts) {
          return { payload: null, reason, attempts: attempt };
        }
      }
    } catch (err: any) {
      const reason = err?.name === 'AbortError' ? 'aborted' : 'request_failed';
      lastReason = reason;
      if (reason === 'aborted' || attempt >= maxAttempts) {
        return { payload: null, reason, attempts: attempt };
      }
    }

    const jitter = Math.floor(Math.random() * 200);
    const delayMs = 450 * attempt + jitter;
    const waited = await waitWithAbort(delayMs, abortSignal);
    if (!waited) {
      return { payload: null, reason: 'aborted', attempts: attempt };
    }
  }
  return { payload: null, reason: lastReason, attempts: maxAttempts };
}

async function fetchBilibiliAccountInfo(
  uid: string,
  abortSignal?: AbortSignal,
): Promise<{ title: string | null; avatarUrl: string | null; follower: number | null; reason?: string; source?: string }> {
  if (!/^\d+$/.test(uid)) {
    return { title: null, avatarUrl: null, follower: null, reason: 'invalid_uid' };
  }

  const accUrl = `https://api.bilibili.com/x/space/acc/info?mid=${encodeURIComponent(uid)}`;
  const accPayloadResult = await fetchBilibiliApiJsonWithRetry(accUrl, abortSignal);
  if (accPayloadResult.payload) {
    const payload = accPayloadResult.payload;
    const title = String(payload?.data?.name || '').trim() || null;
    const avatarUrl = String(payload?.data?.face || '').trim() || null;
    const follower = toNullableInt(payload?.data?.follower);
    if (title || avatarUrl || follower != null) {
      return { title, avatarUrl, follower, source: 'acc_info' };
    }
  }

  // Fallback: this endpoint is usually less strict and still provides name/face/follower.
  const cardUrl = `https://api.bilibili.com/x/web-interface/card?mid=${encodeURIComponent(uid)}`;
  const cardPayloadResult = await fetchBilibiliApiJsonWithRetry(cardUrl, abortSignal);
  if (!cardPayloadResult.payload) {
    return {
      title: null,
      avatarUrl: null,
      follower: null,
      reason: [accPayloadResult.reason, cardPayloadResult.reason].filter(Boolean).join('|') || 'request_failed',
    };
  }
  const cardPayload = cardPayloadResult.payload;
  const title = String(cardPayload?.data?.card?.name || '').trim() || null;
  const avatarUrl = String(cardPayload?.data?.card?.face || '').trim() || null;
  const follower = toNullableInt(cardPayload?.data?.follower ?? cardPayload?.data?.card?.fans);
  return { title, avatarUrl, follower, source: 'web_card' };
}

function normalizeCookieHeaderValue(text: string): string {
  return text
    .split(';')
    .map((item) => item.trim())
    .filter((item) => item && item.includes('='))
    .join('; ');
}

function parseCookieHeaderFromNetscapeText(text: string, hostHint: string): string | null {
  const pairs: string[] = [];
  const lines = text.split(/\r?\n/);
  for (const lineRaw of lines) {
    const line = lineRaw.trim();
    if (!line || line.startsWith('#')) continue;
    const cols = line.split('\t');
    if (cols.length < 7) continue;
    const domain = String(cols[0] || '').trim().toLowerCase();
    if (domain && !domain.includes(hostHint)) continue;
    const name = String(cols[5] || '').trim();
    const value = String(cols[6] || '').trim();
    if (!name || !value) continue;
    pairs.push(`${name}=${value}`);
  }
  if (pairs.length === 0) return null;
  return pairs.join('; ');
}

function parseCookieHeaderFromJsonText(text: string, hostHint = 'tiktok.com'): string | null {
  try {
    const parsed = JSON.parse(text);
    const pairs: string[] = [];
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        const obj = item as any;
        const name = String(obj?.name || '').trim();
        const value = String(obj?.value || '').trim();
        const domain = String(obj?.domain || '').trim().toLowerCase();
        if (!name || !value) continue;
        if (domain && !domain.includes(hostHint.toLowerCase())) continue;
        pairs.push(`${name}=${value}`);
      }
    } else if (parsed && typeof parsed === 'object') {
      for (const [key, val] of Object.entries(parsed as Record<string, unknown>)) {
        const name = String(key || '').trim();
        const value = String(val ?? '').trim();
        if (!name || !value) continue;
        pairs.push(`${name}=${value}`);
      }
    }
    if (pairs.length === 0) return null;
    return pairs.join('; ');
  } catch {
    return null;
  }
}

function resolveCookieHeaderFromSetting(settingKey: string, hostHint: string): string | null {
  const settingValue = String(getSetting(settingKey) || '').trim();
  if (!settingValue) return null;

  let source = settingValue;
  try {
    const maybePath = path.resolve(settingValue);
    if (fs.existsSync(maybePath)) {
      source = fs.readFileSync(maybePath, 'utf8');
    }
  } catch {}

  const raw = String(source || '').trim();
  if (!raw) return null;

  if (raw.includes('Netscape HTTP Cookie File') || raw.includes('\tTRUE\t') || raw.includes('\tFALSE\t')) {
    return parseCookieHeaderFromNetscapeText(raw, hostHint);
  }

  if (raw.startsWith('{') || raw.startsWith('[')) {
    const fromJson = parseCookieHeaderFromJsonText(raw, hostHint);
    if (fromJson) return fromJson;
  }

  const cookieLine = raw.split(/\r?\n/).find((line) => /^cookie\s*:/i.test(line.trim()));
  if (cookieLine) {
    const value = cookieLine.replace(/^cookie\s*:/i, '').trim();
    return normalizeCookieHeaderValue(value) || null;
  }

  return normalizeCookieHeaderValue(raw) || null;
}

function resolveTiktokCookieHeader(): string | null {
  return resolveCookieHeaderFromSetting('yt_dlp_cookie_file_tiktok', 'tiktok.com');
}

function resolveDouyinCookieHeader(): string | null {
  return resolveCookieHeaderFromSetting('yt_dlp_cookie_file_douyin', 'douyin.com');
}

function resolveXiaohongshuCookieHeader(): string | null {
  return resolveCookieHeaderFromSetting('yt_dlp_cookie_file_xiaohongshu', 'xiaohongshu.com');
}

function extractScriptJsonById(html: string, scriptId: string): any | null {
  const escapedId = scriptId.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
  const regex = new RegExp(`<script[^>]*id=["']${escapedId}["'][^>]*>([\\s\\S]*?)<\\/script>`, 'i');
  const match = html.match(regex);
  if (!match || !match[1]) return null;
  const content = match[1].trim();
  if (!content) return null;
  try {
    return JSON.parse(content);
  } catch {
    return null;
  }
}

function extractWindowAssignedJson(html: string, keyName: string): any | null {
  const escaped = keyName.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
  const patterns = [
    new RegExp(`window\\[['"]${escaped}['"]\\]\\s*=\\s*(\\{[\\s\\S]*?\\})\\s*;`, 'i'),
    new RegExp(`window\\.${escaped}\\s*=\\s*(\\{[\\s\\S]*?\\})\\s*;`, 'i'),
  ];
  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (!match || !match[1]) continue;
    try {
      return JSON.parse(match[1]);
    } catch {}
  }
  return null;
}

function normalizeUrlProtocol(raw: string): string | null {
  const value = String(raw || '').trim();
  if (!value) return null;
  if (value.startsWith('//')) return `https:${value}`;
  if (/^https?:\/\//i.test(value)) return value;
  return null;
}

function pickTiktokProfileCandidate(root: any): { followerCount: number | null; avatarUrl: string | null; title: string | null } {
  let best = { score: -1, followerCount: null as number | null, avatarUrl: null as string | null, title: null as string | null };
  const stack: any[] = [root];
  let inspected = 0;
  const maxInspect = 50_000;

  while (stack.length > 0 && inspected < maxInspect) {
    const node = stack.pop();
    inspected += 1;
    if (!node || typeof node !== 'object') continue;

    const followerCount = toNullableInt(
      (node as any)?.stats?.followerCount
      ?? (node as any)?.followerCount
      ?? (node as any)?.statsV2?.followerCount,
    );
    const avatarUrl = normalizeUrlProtocol(
      String(
        (node as any)?.user?.avatarLarger
        || (node as any)?.user?.avatarMedium
        || (node as any)?.user?.avatarThumb
        || (node as any)?.avatarLarger
        || (node as any)?.avatarMedium
        || (node as any)?.avatarThumb
        || (node as any)?.author?.avatarLarger
        || '',
      ),
    );
    const title = String(
      (node as any)?.user?.nickname
      || (node as any)?.nickname
      || (node as any)?.author?.nickname
      || (node as any)?.user?.uniqueId
      || (node as any)?.uniqueId
      || '',
    ).trim() || null;

    let score = 0;
    if (followerCount != null) score += 5;
    if (avatarUrl) score += 3;
    if (title) score += 2;
    if (score > best.score) {
      best = { score, followerCount, avatarUrl, title };
    }

    if (Array.isArray(node)) {
      for (const item of node) stack.push(item);
    } else {
      for (const value of Object.values(node)) {
        if (value && typeof value === 'object') stack.push(value);
      }
    }
  }

  return { followerCount: best.followerCount, avatarUrl: best.avatarUrl, title: best.title };
}

function parseTiktokProfileFromHtml(html: string): { followerCount: number | null; avatarUrl: string | null; title: string | null; source?: string } {
  const states: Array<{ key: string; data: any | null }> = [
    { key: '__UNIVERSAL_DATA_FOR_REHYDRATION__', data: extractScriptJsonById(html, '__UNIVERSAL_DATA_FOR_REHYDRATION__') },
    { key: 'SIGI_STATE', data: extractScriptJsonById(html, 'SIGI_STATE') },
    { key: '__NEXT_DATA__', data: extractScriptJsonById(html, '__NEXT_DATA__') },
    { key: 'window.SIGI_STATE', data: extractWindowAssignedJson(html, 'SIGI_STATE') },
  ];

  let best = { followerCount: null as number | null, avatarUrl: null as string | null, title: null as string | null, source: '' };
  let bestScore = -1;
  for (const state of states) {
    if (!state.data) continue;
    const picked = pickTiktokProfileCandidate(state.data);
    let score = 0;
    if (picked.followerCount != null) score += 5;
    if (picked.avatarUrl) score += 3;
    if (picked.title) score += 2;
    if (score > bestScore) {
      bestScore = score;
      best = { ...picked, source: state.key };
    }
  }

  return {
    followerCount: best.followerCount,
    avatarUrl: best.avatarUrl,
    title: best.title,
    source: best.source || undefined,
  };
}

function resolveTiktokProfileUrl(channel: any, channelUrl: string | null): string | null {
  const candidates = [String(channel?.source_url || ''), String(channelUrl || ''), String(channel?.handle || '')];
  for (const raw of candidates) {
    const value = String(raw || '').trim();
    if (!value) continue;
    const asUrl = /^https?:\/\//i.test(value) ? value : (value.startsWith('@') ? `https://www.tiktok.com/${value}` : '');
    if (!asUrl) continue;
    try {
      const parsed = new URL(asUrl);
      const host = parsed.hostname.replace(/^www\./, '').toLowerCase();
      if (!host.includes('tiktok.com')) continue;
      const pathname = parsed.pathname || '';
      if (!pathname.includes('@')) continue;
      return `https://www.tiktok.com${pathname}`.replace(/\/+$/, '');
    } catch {}
  }
  return null;
}

function isRetryableTiktokProfileStatus(status: number): boolean {
  return [403, 408, 425, 429, 500, 502, 503, 504].includes(status);
}

function isLikelyTiktokBlockPage(html: string): boolean {
  const lower = html.toLowerCase();
  return (
    lower.includes('captcha') ||
    lower.includes('verify') ||
    lower.includes('access denied') ||
    lower.includes('too many requests')
  );
}

async function fetchTiktokProfileSnapshot(
  profileUrl: string,
  abortSignal?: AbortSignal,
): Promise<{ followerCount: number | null; avatarUrl: string | null; title: string | null; reason?: string; source?: string }> {
  const cookieHeader = resolveTiktokCookieHeader();
  const maxAttempts = 4;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const headers: Record<string, string> = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'referer': 'https://www.tiktok.com/',
      };
      if (cookieHeader) headers.cookie = cookieHeader;

      const response = await fetch(profileUrl, {
        method: 'GET',
        headers,
        signal: abortSignal,
        redirect: 'follow',
      });

      if (!response.ok) {
        if (isRetryableTiktokProfileStatus(response.status) && attempt < maxAttempts) {
          const jitter = Math.floor(Math.random() * 200);
          const waited = await waitWithAbort(450 * attempt + jitter, abortSignal);
          if (!waited) return { followerCount: null, avatarUrl: null, title: null, reason: 'aborted' };
          continue;
        }
        return { followerCount: null, avatarUrl: null, title: null, reason: `http_${response.status}` };
      }

      const html = await response.text();
      const parsed = parseTiktokProfileFromHtml(html);
      if (parsed.followerCount != null || parsed.avatarUrl || parsed.title) {
        return parsed;
      }

      const blocked = isLikelyTiktokBlockPage(html);
      if (blocked && attempt < maxAttempts) {
        const jitter = Math.floor(Math.random() * 200);
        const waited = await waitWithAbort(500 * attempt + jitter, abortSignal);
        if (!waited) return { followerCount: null, avatarUrl: null, title: null, reason: 'aborted' };
        continue;
      }

      return { followerCount: null, avatarUrl: null, title: null, reason: blocked ? 'blocked_page' : 'state_not_found' };
    } catch (err: any) {
      const reason = err?.name === 'AbortError' ? 'aborted' : 'request_failed';
      if (reason === 'aborted' || attempt >= maxAttempts) {
        return { followerCount: null, avatarUrl: null, title: null, reason };
      }
      const jitter = Math.floor(Math.random() * 200);
      const waited = await waitWithAbort(450 * attempt + jitter, abortSignal);
      if (!waited) return { followerCount: null, avatarUrl: null, title: null, reason: 'aborted' };
    }
  }

  return { followerCount: null, avatarUrl: null, title: null, reason: 'state_not_found' };
}

function isRetryableDouyinHttpStatus(status: number): boolean {
  return [403, 408, 425, 429, 500, 502, 503, 504].includes(status);
}

async function fetchDouyinApiJsonWithRetry(
  url: string,
  referer: string,
  abortSignal?: AbortSignal,
  maxAttempts = 4,
): Promise<{ payload: any | null; reason?: string; attempts: number }> {
  const cookieHeader = resolveDouyinCookieHeader();
  let lastReason = 'request_failed';

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const headers: Record<string, string> = {
        'accept': 'application/json,text/plain,*/*',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'referer': referer,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
      };
      if (cookieHeader) headers.cookie = cookieHeader;

      const response = await fetch(url, {
        method: 'GET',
        headers,
        signal: abortSignal,
      });

      if (!response.ok) {
        const reason = `http_${response.status}`;
        lastReason = reason;
        if (!isRetryableDouyinHttpStatus(response.status) || attempt >= maxAttempts) {
          return { payload: null, reason, attempts: attempt };
        }
      } else {
        const text = await response.text();
        if (!text || !text.trim()) {
          lastReason = 'empty_body';
          if (attempt >= maxAttempts) {
            return { payload: null, reason: lastReason, attempts: attempt };
          }
        } else {
          try {
            const payload = JSON.parse(text);
            const statusCode = Number(payload?.status_code ?? payload?.statusCode ?? 0);
            if (statusCode === 0) {
              return { payload, attempts: attempt };
            }
            const reason = `api_status_${String(statusCode)}`;
            lastReason = reason;
            if (attempt >= maxAttempts) {
              return { payload: null, reason, attempts: attempt };
            }
          } catch {
            lastReason = 'invalid_json';
            if (attempt >= maxAttempts) {
              return { payload: null, reason: lastReason, attempts: attempt };
            }
          }
        }
      }
    } catch (err: any) {
      const reason = err?.name === 'AbortError' ? 'aborted' : 'request_failed';
      lastReason = reason;
      if (reason === 'aborted' || attempt >= maxAttempts) {
        return { payload: null, reason, attempts: attempt };
      }
    }

    const jitter = Math.floor(Math.random() * 220);
    const waited = await waitWithAbort(500 * attempt + jitter, abortSignal);
    if (!waited) {
      return { payload: null, reason: 'aborted', attempts: attempt };
    }
  }

  return { payload: null, reason: lastReason, attempts: maxAttempts };
}

function normalizeDouyinSecUid(value: unknown): string | null {
  const text = String(value || '').trim();
  if (!text) return null;
  if (!/^MS4wLj/i.test(text)) return null;
  return text;
}

function resolveDouyinSecUidFromChannel(channel: any, channelId: string, channelUrl: string): string | null {
  const idFromStorage = normalizeDouyinSecUid(stripStoredChannelPrefix(channelId, 'douyin'));
  if (idFromStorage) return idFromStorage;

  const handleValue = normalizeDouyinSecUid(String(channel?.handle || '').trim().replace(/^@+/, ''));
  if (handleValue) return handleValue;

  const candidates = [String(channel?.source_url || '').trim(), String(channelUrl || '').trim()];
  for (const raw of candidates) {
    if (!raw) continue;
    try {
      const parsed = new URL(raw);
      const parts = parsed.pathname.split('/').filter(Boolean);
      if ((parts[0] || '').toLowerCase() !== 'user') continue;
      let decoded = String(parts[1] || '');
      try {
        decoded = decodeURIComponent(decoded);
      } catch {}
      const candidate = normalizeDouyinSecUid(decoded);
      if (candidate) return candidate;
    } catch {}
  }

  return null;
}

async function fetchDouyinUserSnapshot(
  secUid: string,
  abortSignal?: AbortSignal,
): Promise<{
  title: string | null;
  avatarUrl: string | null;
  followerCount: number | null;
  uniqueId: string | null;
  uid: string | null;
  awemeCount: number | null;
  mixIds: string[];
  reason?: string;
}> {
  const referer = `https://www.douyin.com/user/${secUid}`;
  const endpoint = `https://www.iesdouyin.com/web/api/v2/user/info/?sec_uid=${encodeURIComponent(secUid)}`;
  const result = await fetchDouyinApiJsonWithRetry(endpoint, referer, abortSignal);
  if (!result.payload) {
    return {
      title: null,
      avatarUrl: null,
      followerCount: null,
      uniqueId: null,
      uid: null,
      awemeCount: null,
      mixIds: [],
      reason: result.reason || 'request_failed',
    };
  }

  const userInfo = result.payload?.user_info || {};
  const title = String(userInfo?.nickname || '').trim() || null;
  const avatarUrl = String(
    userInfo?.avatar_thumb?.url_list?.[0]
    || userInfo?.avatar_medium?.url_list?.[0]
    || userInfo?.avatar_larger?.url_list?.[0]
    || '',
  ).trim() || null;
  const followerCount = toNullableInt(
    userInfo?.mplatform_followers_count
    ?? userInfo?.follower_count
    ?? userInfo?.followers_count,
  );
  const uniqueId = String(userInfo?.unique_id || '').trim() || null;
  const uid = String(userInfo?.uid || '').trim() || null;
  const awemeCount = toNullableInt(userInfo?.aweme_count);
  const mixIdList = (Array.isArray(userInfo?.mix_info) ? userInfo.mix_info : [])
    .map((item: any) => String(item?.mix_id || '').trim())
    .filter((value: string) => Boolean(value));
  const mixIds = Array.from(new Set<string>(mixIdList));

  return {
    title,
    avatarUrl,
    followerCount,
    uniqueId,
    uid,
    awemeCount,
    mixIds,
  };
}

async function fetchDouyinMixEntries(
  secUid: string,
  mixId: string,
  maxEntries: number,
  abortSignal?: AbortSignal,
): Promise<{ items: any[]; reason?: string }> {
  const referer = `https://www.douyin.com/user/${secUid}`;
  const collected: any[] = [];
  let cursor = 0;
  let loops = 0;

  while (loops < 40 && collected.length < maxEntries) {
    loops += 1;
    const endpoint = `https://www.douyin.com/aweme/v1/web/mix/aweme/?mix_id=${encodeURIComponent(mixId)}&count=20&cursor=${cursor}&device_platform=webapp&aid=6383`;
    const result = await fetchDouyinApiJsonWithRetry(endpoint, referer, abortSignal);
    if (!result.payload) {
      return { items: collected, reason: result.reason || 'request_failed' };
    }
    const awemeList = Array.isArray(result.payload?.aweme_list) ? result.payload.aweme_list : [];
    if (awemeList.length === 0) {
      return { items: collected };
    }
    for (const item of awemeList) {
      collected.push(item);
      if (collected.length >= maxEntries) break;
    }

    const hasMore = Number(result.payload?.has_more || 0) === 1;
    const nextCursor = toNullableInt(result.payload?.cursor);
    if (!hasMore || nextCursor == null || nextCursor === cursor) {
      break;
    }
    cursor = nextCursor;
  }

  return { items: collected };
}

function stripStoredChannelPrefix(channelId: string, platform: ChannelPlatform): string {
  const raw = String(channelId || '').trim();
  const prefix = `${platform}__`;
  if (raw.toLowerCase().startsWith(prefix.toLowerCase())) {
    return raw.slice(prefix.length);
  }
  return raw;
}

function composePlatformVideoId(platform: ChannelPlatform, rawVideoId: string): string {
  const base = sanitizeIdentifierSegment(rawVideoId, 'video');
  if (platform === 'youtube' && /^[A-Za-z0-9_-]{11}$/.test(base)) return base;
  return `${platform}__${base}`;
}

function normalizeChannelSourceUrlByPlatform(platform: ChannelPlatform, sourceUrl: string): string {
  if (platform !== 'bilibili') return sourceUrl;
  try {
    const parsed = new URL(sourceUrl);
    const host = parsed.hostname.replace(/^www\./, '').toLowerCase();
    if (!host.endsWith('bilibili.com')) return sourceUrl;
    const parts = parsed.pathname.split('/').filter(Boolean);
    let uid = '';
    if (host === 'space.bilibili.com' && parts[0]) {
      uid = parts[0];
    } else if (parts[0] === 'space' && parts[1]) {
      uid = parts[1];
    }
    if (!uid) return sourceUrl;
    return `https://space.bilibili.com/${uid}/video`;
  } catch {
    return sourceUrl;
  }
}

function buildChannelUrlByPlatform(channel: any, platform: ChannelPlatform): string | null {
  const sourceUrl = normalizeVideoTarget(String(channel?.source_url || ''));
  if (sourceUrl) return normalizeChannelSourceUrlByPlatform(platform, sourceUrl);

  const channelId = String(channel?.channel_id || '').trim();
  const handleRaw = String(channel?.handle || '').trim().replace(/^@+/, '');
  const bareId = stripStoredChannelPrefix(channelId, platform);
  const idValue = sanitizePathSegment(bareId, '');
  const handleValue = sanitizePathSegment(handleRaw, '');

  switch (platform) {
    case 'youtube':
      if (channelId.startsWith('UC')) return `https://www.youtube.com/channel/${channelId}`;
      if (handleValue) return `https://www.youtube.com/@${handleValue}`;
      if (idValue) return `https://www.youtube.com/@${idValue}`;
      return null;
    case 'bilibili':
      if (!idValue) return null;
      return `https://space.bilibili.com/${idValue}/video`;
    case 'tiktok':
      if (handleValue) return `https://www.tiktok.com/@${handleValue}`;
      if (idValue) return `https://www.tiktok.com/@${idValue}`;
      return null;
    case 'douyin':
      if (!idValue) return null;
      return `https://www.douyin.com/user/${idValue}`;
    case 'xiaohongshu':
      if (!idValue) return null;
      return `https://www.xiaohongshu.com/user/profile/${idValue}`;
    default:
      return null;
  }
}

interface ChannelFeedTarget {
  url: string;
  feedKey: 'videos' | 'shorts' | 'streams' | 'main';
  inferredType: VideoContentType;
  typeSource: string;
}

type VideoContentType = 'long' | 'short' | 'live' | 'note' | 'album';
type DouyinVisualKind = 'video' | 'album' | 'live_photo';

function normalizeParsedContentTypeForStorage(
  platform: ChannelPlatform,
  rawType: unknown,
): { type: VideoContentType; source: string } | null {
  const value = String(rawType || '').trim().toLowerCase();
  if (!value) return null;
  if (value === 'album') return { type: 'album', source: 'meta' };
  if (value === 'note') {
    if (platform === 'douyin' || platform === 'xiaohongshu') {
      return { type: 'album', source: 'meta_note_as_album' };
    }
    return null;
  }
  if (value === 'short') return { type: 'short', source: 'meta' };
  if (value === 'live') return { type: 'live', source: 'meta' };
  if (value === 'long') return { type: 'long', source: 'meta' };
  if (value === 'video') {
    if (platform === 'tiktok' || platform === 'xiaohongshu') {
      return { type: 'short', source: 'meta_video_as_short' };
    }
    return { type: 'long', source: 'meta_video_as_long' };
  }
  return null;
}

function buildChannelFeedTargets(platform: ChannelPlatform, channelUrl: string): ChannelFeedTarget[] {
  if (platform === 'youtube') {
    return [
      { url: `${channelUrl}/videos`, feedKey: 'videos', inferredType: 'long', typeSource: 'videos_feed' },
      { url: `${channelUrl}/shorts`, feedKey: 'shorts', inferredType: 'short', typeSource: 'shorts_feed' },
      { url: `${channelUrl}/streams`, feedKey: 'streams', inferredType: 'live', typeSource: 'streams_feed' },
    ];
  }
  if (platform === 'douyin') {
    return [
      { url: channelUrl, feedKey: 'main', inferredType: 'long', typeSource: 'platform_default_video' },
    ];
  }
  if (platform === 'tiktok' || platform === 'xiaohongshu') {
    return [
      { url: channelUrl, feedKey: 'main', inferredType: 'short', typeSource: 'platform_default_short' },
    ];
  }
  return [
    { url: channelUrl, feedKey: 'main', inferredType: 'long', typeSource: 'main_feed' },
  ];
}

function buildFallbackVideoUrl(platform: ChannelPlatform, rawVideoId: string): string | null {
  if (!rawVideoId) return null;
  switch (platform) {
    case 'youtube':
      return `https://www.youtube.com/watch?v=${rawVideoId}`;
    case 'bilibili':
      if (/^BV[0-9A-Za-z]+$/i.test(rawVideoId)) {
        return `https://www.bilibili.com/video/${rawVideoId}`;
      }
      return null;
    case 'tiktok':
      return `https://www.tiktok.com/video/${rawVideoId}`;
    case 'douyin':
      return `https://www.douyin.com/video/${rawVideoId}`;
    case 'xiaohongshu':
      return `https://www.xiaohongshu.com/explore/${rawVideoId}`;
    default:
      return null;
  }
}

function toPositiveInt(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.trunc(parsed);
}

function collectImageCandidates(value: unknown): any[] {
  if (Array.isArray(value)) return value.filter((item) => item && typeof item === 'object');
  if (!value || typeof value !== 'object') return [];
  const row = value as any;
  if (Array.isArray(row?.images)) return row.images.filter((item: any) => item && typeof item === 'object');
  return [];
}

function extractEntryImages(entry: any): any[] {
  const candidates = [
    entry?.images,
    entry?.image_infos,
    entry?.image_list,
    entry?.note_card?.image_list,
    entry?.note_card?.images,
    entry?.imagePost?.images,
    entry?.image_post_info?.images,
    entry?.raw?.images,
    entry?.raw?.image_infos,
    entry?.raw?.image_list,
    entry?.raw?.note_card?.image_list,
    entry?.raw?.note_card?.images,
    entry?.raw?.imagePost?.images,
    entry?.raw?.image_post_info?.images,
  ];
  const images: any[] = [];
  for (const candidate of candidates) {
    images.push(...collectImageCandidates(candidate));
  }
  return images;
}

function imageItemHasVideoField(value: unknown): boolean {
  if (!value || typeof value !== 'object') return false;
  const row = value as any;
  return row.video != null;
}

function resolveDouyinKindFromContentType(rawType: unknown): DouyinVisualKind | null {
  const value = String(rawType || '').trim().toLowerCase();
  if (!value) return null;
  if (value === 'live_photo') return 'live_photo';
  if (value === 'album' || value === 'note') return 'album';
  if (value === 'short' || value === 'long' || value === 'video') return 'video';
  return null;
}

function classifyDouyinVisualKind(entry: any): DouyinVisualKind {
  const fromType = resolveDouyinKindFromContentType(entry?.content_type);
  if (fromType) return fromType;

  const images = extractEntryImages(entry);
  if (images.length > 0) {
    return images.some(imageItemHasVideoField) ? 'live_photo' : 'album';
  }

  const awemeType = Number(entry?.aweme_type ?? entry?.raw?.aweme_type);
  if (Number.isFinite(awemeType) && [2, 68, 150].includes(awemeType)) return 'album';

  const urls = [
    entry?.url,
    entry?.webpage_url,
    entry?.share_url,
    entry?.original_url,
  ];
  for (const candidate of urls) {
    const text = String(candidate || '').trim().toLowerCase();
    if (!text) continue;
    if (text.includes('/note/') || text.includes('/slides/')) return 'album';
  }

  return 'video';
}

function readResolutionFromNode(value: unknown): { width: number; height: number } | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as any;
  const width = toPositiveInt(row?.width);
  const height = toPositiveInt(row?.height);
  if (width == null || height == null) return null;
  return { width, height };
}

function extractEntryResolution(entry: any): { width: number; height: number } | null {
  const candidates: Array<{ width: number; height: number }> = [];
  const feed = (value: unknown) => {
    const parsed = readResolutionFromNode(value);
    if (parsed) candidates.push(parsed);
  };

  const nodeCandidates = [
    entry,
    entry?.video,
    entry?.play_addr,
    entry?.video?.play_addr,
    entry?.raw,
    entry?.raw?.video,
    entry?.raw?.play_addr,
    entry?.raw?.video?.play_addr,
  ];
  for (const node of nodeCandidates) feed(node);

  const bitRateLists = [
    entry?.bit_rate,
    entry?.video?.bit_rate,
    entry?.raw?.bit_rate,
    entry?.raw?.video?.bit_rate,
  ];
  for (const list of bitRateLists) {
    if (!Array.isArray(list)) continue;
    for (const item of list) {
      feed(item);
      feed(item?.play_addr);
    }
  }

  if (Array.isArray(entry?.formats)) {
    for (const format of entry.formats) {
      feed(format);
    }
  }

  if (candidates.length === 0) return null;
  candidates.sort((a, b) => (b.width * b.height) - (a.width * a.height));
  return candidates[0];
}

function resolveEntryOrientation(entry: any): 'portrait' | 'landscape' | 'square' | null {
  const resolution = extractEntryResolution(entry);
  if (!resolution) return null;
  if (resolution.width > resolution.height) return 'landscape';
  if (resolution.width < resolution.height) return 'portrait';
  return 'square';
}

function resolveDouyinVideoContentType(entry: any): VideoContentType {
  const visualKind = classifyDouyinVisualKind(entry);
  if (visualKind !== 'video') return 'album';

  const rawType = String(entry?.content_type || entry?.raw?.content_type || '').trim().toLowerCase();
  if (rawType === 'short') return 'short';

  const orientation = resolveEntryOrientation(entry);
  if (orientation === 'portrait') return 'short';
  if (orientation === 'landscape' || orientation === 'square') return 'long';

  let durationSec = toNullableInt(
    entry?.duration
    ?? entry?.duration_sec
    ?? entry?.raw?.duration
    ?? entry?.raw?.video?.duration
    ?? entry?.raw?.duration_ms,
  );
  if (durationSec != null && durationSec > 1000) {
    durationSec = Math.max(1, Math.round(durationSec / 1000));
  }
  if (durationSec != null) return durationSec >= 60 ? 'long' : 'short';

  return 'long';
}

function shouldRefetchTikTokDownloaderDetailMeta(meta: any, platform: ChannelPlatform): boolean {
  if (platform !== 'tiktok' && platform !== 'douyin') return false;
  return isSparseTikTokDownloaderMeta(meta);
}

function isDouyinNoteEntry(entry: any): boolean {
  return classifyDouyinVisualKind(entry) !== 'video';
}

function resolveEntryWebpageUrl(entry: any): string | null {
  const primary = String(entry?.url || '').trim();
  if (/^https?:\/\//i.test(primary)) return primary;
  const page = String(entry?.webpage_url || '').trim();
  if (/^https?:\/\//i.test(page)) return page;
  return null;
}

function isLikelyVideoEntry(platform: ChannelPlatform, entry: any): boolean {
  const duration = Number(entry?.duration || 0);
  if (Number.isFinite(duration) && duration > 0) return true;
  const url = String(entry?.url || entry?.webpage_url || '').toLowerCase();
  if (!url) return platform === 'youtube';

  switch (platform) {
    case 'youtube':
      return true;
    case 'bilibili':
      return url.includes('/video/');
    case 'tiktok':
      return url.includes('/video/') || url.includes('/photo/');
    case 'douyin':
      return url.includes('/video/') || url.includes('/note/') || url.includes('/slides/') || isDouyinNoteEntry(entry);
    case 'xiaohongshu':
      return url.includes('/video/') || url.includes('/explore/');
    default:
      return true;
  }
}

function inferEntryContentType(
  platform: ChannelPlatform,
  target: ChannelFeedTarget,
  entry: any,
  resolvedWebUrl: string | null = null,
): { type: VideoContentType; source: string } {
  const rawLiveStatus = String(entry?.live_status || entry?.raw?.live_status || '').trim().toLowerCase();
  const hasYoutubeLiveSignal = (
    rawLiveStatus === 'is_live'
    || rawLiveStatus === 'was_live'
    || rawLiveStatus === 'is_upcoming'
    || rawLiveStatus === 'post_live'
    || entry?.is_live === true
    || entry?.was_live === true
  );
  const urlCandidates = [
    entry?.url,
    entry?.webpage_url,
    entry?.share_url,
    entry?.original_url,
    resolvedWebUrl,
  ];
  const entryUrl = urlCandidates
    .map((value) => String(value || '').trim().toLowerCase())
    .find(Boolean) || '';
  const hasShortUrl = urlCandidates
    .some((value) => String(value || '').trim().toLowerCase().includes('/shorts/'));
  if (platform === 'douyin') {
    const visualKind = classifyDouyinVisualKind(entry);
    if (visualKind === 'live_photo') {
      return { type: 'album', source: 'douyin_album_live_photo_meta' };
    }
    if (visualKind === 'album') {
      return {
        type: 'album',
        source: entryUrl.includes('/note/') || entryUrl.includes('/slides/')
          ? 'douyin_album_url'
          : 'douyin_album_meta',
      };
    }
    const rawType = String(entry?.content_type || entry?.raw?.content_type || '').trim().toLowerCase();
    if (rawType === 'short') return { type: 'short', source: 'douyin_video_raw_short' };
    const orientation = resolveEntryOrientation(entry);
    if (orientation === 'landscape') return { type: 'long', source: 'douyin_video_landscape' };
    if (orientation === 'portrait') return { type: 'short', source: 'douyin_video_portrait' };
    if (orientation === 'square') return { type: 'long', source: 'douyin_video_square' };
    const duration = toNullableInt(entry?.duration ?? entry?.duration_sec ?? entry?.raw?.duration ?? entry?.raw?.video?.duration ?? entry?.raw?.duration_ms);
    if (duration != null) {
      const durationSec = duration > 1000 ? Math.max(1, Math.round(duration / 1000)) : duration;
      if (durationSec < 60) return { type: 'short', source: 'douyin_video_duration_short' };
      return { type: 'long', source: 'douyin_video_duration_long' };
    }
    return { type: 'long', source: 'douyin_video_meta' };
  }
  if (platform === 'xiaohongshu') {
    const rawType = String(entry?.content_type || entry?.raw?.content_type || '').trim().toLowerCase();
    const noteCardType = String(entry?.note_card?.type || entry?.raw?.note_card?.type || '').trim().toLowerCase();
    if (noteCardType === 'normal') {
      return { type: 'album', source: 'xhs_note_card_normal' };
    }
    if (rawType === 'album' || rawType === 'note' || rawType === 'normal' || rawType === 'image') {
      return { type: 'album', source: 'xhs_raw_album' };
    }
    if (rawType === 'short' || rawType === 'long' || rawType === 'video') {
      return { type: 'short', source: 'xhs_raw_short' };
    }
    const hasVideoPayload = Boolean(
      entry?.video
      || entry?.raw?.video
      || entry?.note_card?.video
      || entry?.raw?.note_card?.video,
    );
    if (extractEntryImages(entry).length > 0 && !hasVideoPayload) {
      return { type: 'album', source: 'xhs_images' };
    }
    // Any non-album XHS note is treated as short video.
    if (hasVideoPayload) return { type: 'short', source: 'xhs_video_payload' };
    const duration = toNullableInt(entry?.duration ?? entry?.duration_sec ?? entry?.raw?.duration ?? entry?.raw?.video?.duration ?? entry?.raw?.duration_ms);
    if (duration != null) return { type: 'short', source: 'xhs_video_duration' };
    const orientation = resolveEntryOrientation(entry);
    if (orientation) return { type: 'short', source: 'xhs_video_orientation' };
    return { type: 'short', source: 'xhs_video_default' };
  }
  if (target.feedKey === 'shorts') {
    return { type: 'short', source: target.typeSource };
  }
  if (target.feedKey === 'streams') {
    return { type: 'live', source: target.typeSource };
  }
  if (platform === 'youtube' && hasYoutubeLiveSignal) {
    return { type: 'live', source: rawLiveStatus ? `youtube_live_status_${rawLiveStatus}` : 'youtube_live_status' };
  }
  if (platform === 'youtube' && hasShortUrl) {
    return { type: 'short', source: 'url' };
  }
  return { type: target.inferredType, source: target.typeSource };
}

function shouldTrackDeletedByPlatform(platform: ChannelPlatform): boolean {
  return platform === 'youtube';
}

function normalizePlatformFromInfo(info: any, webpageUrl: string | null): { key: string; label: string } {
  const rawExtractor = String(info?.extractor_key || info?.extractor || '').trim().toLowerCase();
  const source = `${rawExtractor} ${String(webpageUrl || '').toLowerCase()}`;

  if (source.includes('youtube') || source.includes('youtu.be')) return { key: 'youtube', label: 'YouTube' };
  if (source.includes('tiktok')) return { key: 'tiktok', label: 'TikTok' };
  if (source.includes('bilibili') || source.includes('b23.tv')) return { key: 'bilibili', label: '\u54d4\u54e9\u54d4\u54e9' };
  if (source.includes('douyin')) return { key: 'douyin', label: '\u6296\u97f3' };
  if (source.includes('twitter') || source.includes('x.com')) return { key: 'x', label: 'X' };
  if (source.includes('xiaohongshu') || source.includes('xhs')) return { key: 'xiaohongshu', label: '\u5c0f\u7ea2\u4e66' };
  if (source.includes('instagram')) return { key: 'instagram', label: 'Instagram' };
  return { key: 'other', label: 'Other' };
}

function composeHitVideoId(platformKey: string, rawVideoId: string): string {
  const base = sanitizeIdentifierSegment(rawVideoId, 'video');
  if (platformKey === 'youtube' && /^[A-Za-z0-9_-]{11}$/.test(base)) return base;
  return `${platformKey}__${base}`;
}

const YOUTUBE_HIT_THRESHOLD_LONG_7D = 500_000;
const YOUTUBE_HIT_THRESHOLD_SHORT_7D = 10_000_000;

function normalizeTagList(tags: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const tag of tags) {
    const value = String(tag || '').trim().replace(/^#+/, '');
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }
  return out;
}

function parseTagsJson(raw: unknown): string[] {
  if (typeof raw !== 'string' || !raw.trim()) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return normalizeTagList(parsed.map((item) => String(item || '')));
  } catch {
    return [];
  }
}

function normalizeStringList(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of raw) {
    const value = String(item || '').trim();
    if (!value) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

function normalizeAgentClockTime(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const text = input.trim();
  if (!text) return null;
  if (!/^\d{2}:\d{2}$/.test(text)) return null;
  const [hours, minutes] = text.split(':').map((value) => Number.parseInt(value, 10));
  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) return null;
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return null;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;
}

function normalizeAgentDate(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const text = input.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

function parseAgentSyncPolicy(raw: unknown): { cadence: string; publish_days: number[]; target_publish_time: string | null } {
  const fallback = { cadence: 'manual', publish_days: [] as number[], target_publish_time: null as string | null };
  if (typeof raw !== 'string' || !raw.trim()) return fallback;

  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const cadence = String(parsed?.cadence || parsed?.frequency || 'manual').trim() || 'manual';
    const sourceDays = Array.isArray(parsed?.publish_days)
      ? parsed.publish_days
      : (Array.isArray(parsed?.days) ? parsed.days : []);
    const publish_days = Array.from(
      new Set(
        sourceDays
          .map((item) => Number(item))
          .filter((item) => Number.isInteger(item) && item >= 0 && item <= 6),
      ),
    );
    const targetPublishTime = normalizeAgentClockTime(parsed?.target_publish_time ?? parsed?.time);
    return {
      cadence,
      publish_days,
      target_publish_time: targetPublishTime,
    };
  } catch {
    return fallback;
  }
}

function isAgentChannelDueOnDate(syncPolicyJson: unknown, workflowStatus: unknown, dateText: string): boolean {
  if (String(workflowStatus || '').trim() === 'paused') return false;
  const { cadence, publish_days } = parseAgentSyncPolicy(syncPolicyJson);
  const parsedDate = new Date(`${dateText}T12:00:00Z`);
  const weekday = parsedDate.getUTCDay();
  if (cadence === 'daily') return true;
  if (cadence === 'weekdays') return weekday >= 1 && weekday <= 5;
  if (cadence === 'weekly' || cadence === 'custom') return publish_days.includes(weekday);
  return false;
}

function syncAgentAuditStatus(jobId: string, status: string, finished = false): void {
  try {
    const db = getDb();
    if (finished) {
      db.prepare(`
        UPDATE agent_actions
        SET status = ?, finished_at = datetime('now')
        WHERE job_id = ?
      `).run(status, jobId);
      return;
    }
    db.prepare(`
      UPDATE agent_actions
      SET status = ?
      WHERE job_id = ?
    `).run(status, jobId);
  } catch {}
}

function extractMetaCategories(meta: any): string[] {
  if (!meta || typeof meta !== 'object') return [];
  const values = Array.isArray(meta?.categories)
    ? meta.categories
    : (typeof meta?.category === 'string' ? [meta.category] : []);
  return values
    .map((item: unknown) => String(item || '').trim())
    .filter(Boolean);
}

function shouldFetchChannelSnapshotByApi(channel: any): { shouldFetch: boolean; reason: string } {
  if (normalizeChannelPlatform(channel?.platform) !== 'youtube') {
    return { shouldFetch: false, reason: 'platform_not_youtube' };
  }
  if (getSetting('channel_api_enabled') === 'false') {
    return { shouldFetch: false, reason: 'channel_api_disabled' };
  }

  const hasAnyApiKey = Boolean(
    String(getSetting('youtube_api_key') || '').trim() ||
    String(getSetting('youtube_api_keys') || '').trim()
  );
  if (!hasAnyApiKey) {
    return { shouldFetch: false, reason: 'youtube_api_key_missing' };
  }

  if (hasChannelMetadataGap(channel)) {
    return { shouldFetch: true, reason: 'channel_metadata_missing' };
  }
  if (hasSuspiciousYoutubeVideoCount(channel)) {
    return { shouldFetch: true, reason: 'channel_video_count_suspected_capped' };
  }

  const lastApiSyncEpoch = toEpochFromSqliteTimestamp(channel.api_last_sync_at);
  if (lastApiSyncEpoch == null) {
    return { shouldFetch: true, reason: 'channel_api_never_synced' };
  }

  const refreshHours = getChannelApiRefreshHours();
  const due = Date.now() - lastApiSyncEpoch >= refreshHours * 60 * 60 * 1000;
  if (due) {
    return { shouldFetch: true, reason: `channel_api_snapshot_stale_${refreshHours}h` };
  }

  return { shouldFetch: false, reason: 'channel_api_snapshot_fresh' };
}

class JobQueue {
  private running = new Map<string, { cancel: () => void; type: string; lane: 'sync' | 'download' | 'other' }>();
  private processing = false;

  getRunningCount(): number {
    return this.running.size;
  }

  getMaxConcurrency(): number {
    return Math.max(1, parseInt(getSetting('download_job_concurrency') || getSetting('max_concurrency') || '2', 10) || 2);
  }

  private getJobLane(type: string): 'sync' | 'download' | 'other' {
    if (['sync_channel', 'sync_reporting_channel', 'daily_sync', 'availability_check', 'metadata_repair'].includes(type)) return 'sync';
    if (
      [
        'download_meta',
        'download_thumb',
        'download_subs',
        'download_video',
        'download_all',
        'tool_download_meta',
        'tool_download_meta_content',
      ].includes(type)
    ) return 'download';
    return 'other';
  }

  private getLaneLimit(lane: 'sync' | 'download' | 'other'): number {
    if (lane === 'sync') {
      return Math.max(1, parseInt(getSetting('sync_job_concurrency') || getSetting('max_concurrency') || '2', 10) || 2);
    }
    if (lane === 'download') {
      return Math.max(1, parseInt(getSetting('download_job_concurrency') || getSetting('max_concurrency') || '2', 10) || 2);
    }
    return 1;
  }

  private getRunningCountForLane(lane: 'sync' | 'download' | 'other'): number {
    let total = 0;
    for (const handle of this.running.values()) {
      if (handle.lane === lane) total += 1;
    }
    return total;
  }

  private canStartJobType(type: string): boolean {
    const lane = this.getJobLane(type);
    return this.getRunningCountForLane(lane) < this.getLaneLimit(lane);
  }

  async processNext(): Promise<void> {
    if (this.processing) return;
    this.processing = true;

    try {
      while (true) {
        const db = getDb();
        const queuedJobs = db.prepare(
          `
          SELECT *
          FROM jobs
          WHERE status = 'queued'
          ORDER BY
            CASE type
              WHEN 'sync_channel' THEN 0
              WHEN 'sync_reporting_channel' THEN 1
              WHEN 'daily_sync' THEN 2
              ELSE 3
            END ASC,
            created_at ASC
          LIMIT 50
          `
        ).all() as any[];

        const nextJob = queuedJobs.find((item) => this.canStartJobType(String(item?.type || '').trim()));

        if (!nextJob) break;

        // Mark as running
        db.prepare("UPDATE jobs SET status = 'running', started_at = datetime('now') WHERE job_id = ?")
          .run(nextJob.job_id);
        syncAgentAuditStatus(String(nextJob.job_id || '').trim(), 'running');

        this.executeJob(nextJob);
      }
    } finally {
      this.processing = false;
    }
  }

  private async executeJob(job: any): Promise<void> {
    const db = getDb();
    let cancelled = false;
    const abortController = new AbortController();

    const cancel = () => {
      cancelled = true;
      try {
        abortController.abort();
      } catch {}
    };
    const lane = this.getJobLane(String(job.type || '').trim());
    this.running.set(job.job_id, { cancel, type: String(job.type || '').trim(), lane });

    const logEvent = (level: string, message: string) => {
      try {
        db.prepare(
          "INSERT INTO job_events (job_id, ts, level, message) VALUES (?, datetime('now'), ?, ?)"
        ).run(job.job_id, level, message);
      } catch {}
    };

    const updateProgress = (progress: number) => {
      try {
        db.prepare('UPDATE jobs SET progress = ? WHERE job_id = ?').run(Math.min(100, progress), job.job_id);
      } catch {}
    };

    try {
      logEvent('info', `Started job: ${job.type}`);
      const poolWarning = getYoutubeCookiePoolSwitchOffWarning();
      if (poolWarning) {
        logEvent('error', poolWarning);
      }
      const payload = JSON.parse(job.payload_json || '{}');
      const handler = this.getHandler(job.type);

      if (!handler) {
        throw new Error(`Unknown job type: ${job.type}`);
      }

      await handler(
        {
          ...job,
          payload,
          cancelled: () => cancelled,
          abortSignal: abortController.signal,
        },
        logEvent,
        updateProgress,
      );

      if (cancelled) {
        db.prepare("UPDATE jobs SET status = 'canceled', finished_at = datetime('now') WHERE job_id = ?")
          .run(job.job_id);
        syncAgentAuditStatus(String(job.job_id || '').trim(), 'canceled', true);
        logEvent('info', 'Job cancelled');
      } else {
        db.prepare("UPDATE jobs SET status = 'done', progress = 100, finished_at = datetime('now') WHERE job_id = ?")
          .run(job.job_id);
        syncAgentAuditStatus(String(job.job_id || '').trim(), 'done', true);
        logEvent('info', 'Job completed successfully');
      }
    } catch (err: any) {
      if (cancelled) {
        db.prepare("UPDATE jobs SET status = 'canceled', finished_at = datetime('now') WHERE job_id = ?")
          .run(job.job_id);
        syncAgentAuditStatus(String(job.job_id || '').trim(), 'canceled', true);
        logEvent('info', 'Job cancelled');
      } else {
        db.prepare("UPDATE jobs SET status = 'failed', error_message = ?, finished_at = datetime('now') WHERE job_id = ?")
          .run(err.message || 'Unknown error', job.job_id);
        syncAgentAuditStatus(String(job.job_id || '').trim(), 'failed', true);
        logEvent('error', `Job failed: ${err.message}`);
      }
    } finally {
      this.running.delete(job.job_id);
      // Process next job
      setTimeout(() => this.processNext(), 100);
    }
  }

  cancelJob(jobId: string): boolean {
    const handle = this.running.get(jobId);
    if (!handle) return false;
    handle.cancel();
    return true;
  }

  private getHandler(type: string): JobHandler | null {
    switch (type) {
      case 'sync_channel': return this.handleSyncChannel.bind(this);
      case 'sync_reporting_channel': return this.handleSyncReportingChannel.bind(this);
      case 'channel_meta_retry_audit': return this.handleChannelMetaRetryAudit.bind(this);
      case 'daily_sync': return this.handleDailySync.bind(this);
      case 'availability_check': return this.handleAvailabilityCheck.bind(this);
      case 'metadata_repair': return this.handleMetadataRepair.bind(this);
      case 'download_meta': return this.handleDownloadMeta.bind(this);
      case 'download_thumb': return this.handleDownloadThumb.bind(this);
      case 'download_subs': return this.handleDownloadSubs.bind(this);
      case 'download_video': return this.handleDownloadVideo.bind(this);
      case 'download_all': return this.handleDownloadAll.bind(this);
      case 'tool_download_meta': return this.handleToolDownloadMeta.bind(this);
      case 'tool_download_meta_content': return this.handleToolDownloadMetaContent.bind(this);
      case 'agent_action': return this.handleAgentAction.bind(this);
      case 'research_bulk_add': return this.handleResearchBulkAdd.bind(this);
      case 'hit_bulk_add': return this.handleHitBulkAdd.bind(this);
      default: return null;
    }
  }

  private saveStructuredJobResult(jobId: string, payload: unknown): void {
    const db = getDb();
    db.prepare(`
      INSERT OR REPLACE INTO tool_job_results (job_id, result_json, created_at)
      VALUES (?, ?, datetime('now'))
    `).run(jobId, JSON.stringify(payload ?? null));
  }

  private async handleAgentAction(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const payload = job.payload && typeof job.payload === 'object' ? job.payload as Record<string, unknown> : {};
    const action = String(payload.action || '').trim();
    const targetType = String(payload.target_type || '').trim();
    const targetId = payload.target_id == null ? null : String(payload.target_id || '').trim() || null;
    const input = payload.input && typeof payload.input === 'object' && !Array.isArray(payload.input)
      ? payload.input as Record<string, unknown>
      : {};

    const fail = (message: string, code = 'INVALID_INPUT', retryable = false, details?: Record<string, unknown>): never => {
      const error = new Error(message) as Error & { code?: string; retryable?: boolean; details?: Record<string, unknown> };
      error.code = code;
      error.retryable = retryable;
      error.details = details;
      throw error;
    };

    const normalizeTaskStatus = (value: unknown): 'todo' | 'in_progress' | 'done' | 'delayed' => {
      const normalized = String(value || '').trim();
      if (normalized === 'todo' || normalized === 'in_progress' || normalized === 'done' || normalized === 'delayed') {
        return normalized;
      }
      return fail('status must be one of todo, in_progress, done, delayed');
    };

    const normalizeTaskPriority = (value: unknown): 'high' | 'medium' | 'low' => {
      const normalized = String(value || '').trim();
      if (normalized === 'high' || normalized === 'medium' || normalized === 'low') {
        return normalized;
      }
      return fail('priority must be one of high, medium, low');
    };

    const normalizeDueDate = (value: unknown, fallback: string): string => {
      if (value == null || value === '') return fallback;
      const normalized = normalizeAgentDate(value);
      if (!normalized) fail('due_date must be in YYYY-MM-DD format');
      return normalized ?? fallback;
    };

    const nowIso = new Date().toISOString();
    const today = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date());

    let resultSummary: Record<string, unknown> | null = null;
    let resultRaw: Record<string, unknown> | null = null;

    try {
      logEvent('info', `Agent action started: ${action}${targetId ? ` (${targetId})` : ''}`);
      updateProgress(2);

      switch (action) {
        case 'sync_channel': {
          if (targetType !== 'channel' || !targetId) fail('sync_channel requires target_type=channel and target_id');
          const channel = db.prepare('SELECT channel_id, platform FROM channels WHERE channel_id = ?').get(targetId) as any;
          if (!channel) fail('Channel not found', 'NOT_FOUND');
          await this.handleSyncChannel(
            {
              ...job,
              payload: {
                channel_id: targetId,
                platform: channel.platform,
                meta_retry_audit: true,
              },
            },
            logEvent,
            updateProgress,
          );
          resultSummary = { synced_channel_id: targetId };
          resultRaw = { channel_id: targetId, platform: channel.platform };
          break;
        }

        case 'sync_all_due_channels': {
          const selectedDate = normalizeDueDate(input.date, today);
          const channels = db.prepare(`
            SELECT channel_id, title, platform, sync_policy_json, workflow_status
            FROM channels
            WHERE lower(COALESCE(platform, 'youtube')) IN ('youtube', 'bilibili', 'tiktok', 'douyin', 'xiaohongshu')
            ORDER BY priority DESC, last_sync_at ASC
          `).all() as any[];
          const dueChannels = channels.filter((channel) => isAgentChannelDueOnDate(channel.sync_policy_json, channel.workflow_status, selectedDate));
          const synced: string[] = [];
          const failed: Array<{ channel_id: string; title: string; message: string }> = [];

          if (dueChannels.length === 0) {
            resultSummary = { date: selectedDate, due_channels: 0, synced_channels: 0, failed_channels: 0 };
            resultRaw = { date: selectedDate, synced: [], failed: [] };
            updateProgress(100);
            break;
          }

          for (let i = 0; i < dueChannels.length; i++) {
            const channel = dueChannels[i];
            try {
              await this.handleSyncChannel(
                {
                  ...job,
                  payload: {
                    channel_id: channel.channel_id,
                    platform: channel.platform,
                    meta_retry_audit: true,
                  },
                },
                logEvent,
                () => {},
              );
              synced.push(String(channel.channel_id || '').trim());
            } catch (err: any) {
              failed.push({
                channel_id: String(channel.channel_id || '').trim(),
                title: String(channel.title || '').trim(),
                message: String(err?.message || 'unknown error'),
              });
              logEvent('error', `Agent due-sync failed for ${String(channel.title || channel.channel_id || 'unknown')}: ${String(err?.message || 'unknown error')}`);
            }
            const pct = 5 + Math.floor(((i + 1) / dueChannels.length) * 95);
            updateProgress(Math.min(99, pct));
          }

          resultSummary = {
            date: selectedDate,
            due_channels: dueChannels.length,
            synced_channels: synced.length,
            failed_channels: failed.length,
          };
          resultRaw = {
            date: selectedDate,
            synced,
            failed,
          };
          break;
        }

        case 'create_task': {
          const title = String(input.title || '').trim();
          const taskName = String(input.task_name || '').trim();
          if (!title) fail('title is required');
          if (!taskName) fail('task_name is required');
          const dueDate = normalizeDueDate(input.due_date, today);
          const priority = normalizeTaskPriority(input.priority || 'medium');
          const status = normalizeTaskStatus(input.status || 'todo');
          const startTime = normalizeAgentClockTime(input.planned_start_time);
          const endTime = normalizeAgentClockTime(input.planned_end_time);
          if ((startTime && !endTime) || (!startTime && endTime)) fail('planned_start_time and planned_end_time must be provided together');
          if (startTime && endTime && endTime <= startTime) fail('planned_end_time must be later than planned_start_time');
          const taskId = uuidv4();
          db.prepare(`
            INSERT INTO dashboard_tasks (
              task_id, title, task_name, channel_id, due_date, priority, status, estimate_minutes, planned_start_time, planned_end_time, notes, sort_order, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
          `).run(
            taskId,
            title,
            taskName,
            input.channel_id ? String(input.channel_id).trim() : null,
            dueDate,
            priority,
            status,
            input.estimate_minutes == null || input.estimate_minutes === '' ? null : Math.max(0, Number(input.estimate_minutes) || 0),
            startTime,
            endTime,
            input.notes ? String(input.notes) : null,
            Number.isFinite(Number(input.sort_order)) ? Number(input.sort_order) : 0,
          );
          resultSummary = { task_id: taskId, due_date: dueDate, status };
          resultRaw = db.prepare('SELECT * FROM dashboard_tasks WHERE task_id = ?').get(taskId) as Record<string, unknown>;
          updateProgress(100);
          break;
        }

        case 'update_task':
        case 'update_task_status': {
          if (targetType !== 'task' || !targetId) fail(`${action} requires target_type=task and target_id`);
          const existing = db.prepare('SELECT * FROM dashboard_tasks WHERE task_id = ?').get(targetId) as any;
          if (!existing) fail('Task not found', 'NOT_FOUND');
          const title = input.title !== undefined ? String(input.title || '').trim() : String(existing.title || '').trim();
          const taskName = input.task_name !== undefined ? String(input.task_name || '').trim() : String(existing.task_name || '').trim();
          if (!title) fail('title is required');
          if (action === 'update_task' && !taskName) fail('task_name is required');
          const priority = input.priority !== undefined ? normalizeTaskPriority(input.priority) : (normalizeTaskPriority(existing.priority) || 'medium');
          const status = input.status !== undefined ? normalizeTaskStatus(input.status) : (normalizeTaskStatus(existing.status) || 'todo');
          const dueDate = normalizeDueDate(input.due_date !== undefined ? input.due_date : existing.due_date, today);
          const startTime = input.planned_start_time !== undefined ? normalizeAgentClockTime(input.planned_start_time) : normalizeAgentClockTime(existing.planned_start_time);
          const endTime = input.planned_end_time !== undefined ? normalizeAgentClockTime(input.planned_end_time) : normalizeAgentClockTime(existing.planned_end_time);
          if ((startTime && !endTime) || (!startTime && endTime)) fail('planned_start_time and planned_end_time must be provided together');
          if (startTime && endTime && endTime <= startTime) fail('planned_end_time must be later than planned_start_time');

          db.prepare(`
            UPDATE dashboard_tasks
            SET title = ?, task_name = ?, channel_id = ?, due_date = ?, priority = ?, status = ?, estimate_minutes = ?, planned_start_time = ?, planned_end_time = ?, notes = ?, sort_order = ?, updated_at = datetime('now')
            WHERE task_id = ?
          `).run(
            title,
            taskName || null,
            input.channel_id !== undefined ? (input.channel_id ? String(input.channel_id).trim() : null) : (existing.channel_id || null),
            dueDate,
            priority,
            status,
            input.estimate_minutes !== undefined
              ? (input.estimate_minutes == null || input.estimate_minutes === '' ? null : Math.max(0, Number(input.estimate_minutes) || 0))
              : existing.estimate_minutes,
            startTime,
            endTime,
            input.notes !== undefined ? (input.notes ? String(input.notes) : null) : (existing.notes || null),
            input.sort_order !== undefined ? (Number.isFinite(Number(input.sort_order)) ? Number(input.sort_order) : 0) : (existing.sort_order || 0),
            targetId,
          );
          resultRaw = db.prepare('SELECT * FROM dashboard_tasks WHERE task_id = ?').get(targetId) as Record<string, unknown>;
          resultSummary = { task_id: targetId, status: (resultRaw as any)?.status, due_date: (resultRaw as any)?.due_date };
          updateProgress(100);
          break;
        }

        case 'delete_task': {
          if (targetType !== 'task' || !targetId) fail('delete_task requires target_type=task and target_id');
          const result = db.prepare('DELETE FROM dashboard_tasks WHERE task_id = ?').run(targetId);
          if (result.changes === 0) fail('Task not found', 'NOT_FOUND');
          resultSummary = { deleted: true, task_id: targetId };
          resultRaw = { task_id: targetId };
          updateProgress(100);
          break;
        }

        case 'update_channel_schedule': {
          if (targetType !== 'channel' || !targetId) fail('update_channel_schedule requires target_type=channel and target_id');
          const channel = db.prepare('SELECT channel_id, sync_policy_json FROM channels WHERE channel_id = ?').get(targetId) as any;
          if (!channel) fail('Channel not found', 'NOT_FOUND');
          const scheduleInput = input.sync_policy && typeof input.sync_policy === 'object' && !Array.isArray(input.sync_policy)
            ? input.sync_policy as Record<string, unknown>
            : input;
          if (!Array.isArray(scheduleInput.publish_days)) fail('publish_days must be an array');
          const publishDays = Array.from(new Set((scheduleInput.publish_days as unknown[]).map((item) => Number(item)).filter((item) => Number.isInteger(item) && item >= 0 && item <= 6)));
          const cadence = typeof scheduleInput.cadence === 'string' && scheduleInput.cadence.trim()
            ? String(scheduleInput.cadence).trim()
            : (publishDays.length === 7 ? 'daily' : (publishDays.length === 5 && [1, 2, 3, 4, 5].every((day) => publishDays.includes(day)) ? 'weekdays' : (publishDays.length > 0 ? 'custom' : 'manual')));
          if (!['manual', 'daily', 'weekdays', 'weekly', 'custom'].includes(cadence)) fail('cadence is invalid');
          const targetPublishTimeText = scheduleInput.target_publish_time == null ? '' : String(scheduleInput.target_publish_time).trim();
          const targetPublishTime = targetPublishTimeText ? normalizeAgentClockTime(targetPublishTimeText) : null;
          if (targetPublishTimeText && !targetPublishTime) fail('target_publish_time must be in HH:MM format');
          const nextSyncPolicy = {
            cadence,
            publish_days: [1, 2, 3, 4, 5, 6, 0].filter((day) => publishDays.includes(day)),
            target_publish_time: targetPublishTime,
          };
          db.prepare(`
            UPDATE channels
            SET sync_policy_json = ?
            WHERE channel_id = ?
          `).run(JSON.stringify(nextSyncPolicy), targetId);
          resultSummary = { channel_id: targetId, update_frequency_summary: `${cadence}:${nextSyncPolicy.publish_days.join(',')}` };
          resultRaw = { channel_id: targetId, sync_policy: nextSyncPolicy };
          updateProgress(100);
          break;
        }

        case 'mark_channel_updated':
        case 'unmark_channel_updated': {
          if (targetType !== 'channel' || !targetId) fail(`${action} requires target_type=channel and target_id`);
          const existingChannel = db.prepare('SELECT channel_id FROM channels WHERE channel_id = ?').get(targetId) as any;
          if (!existingChannel) fail('Channel not found', 'NOT_FOUND');
          const manualUpdatedAt = action === 'mark_channel_updated'
            ? (typeof input.at === 'string' && input.at.trim() ? String(input.at).trim() : nowIso)
            : null;
          if (manualUpdatedAt) {
            const parsed = new Date(manualUpdatedAt);
            if (Number.isNaN(parsed.getTime())) fail('at must be a valid ISO datetime');
          }
          db.prepare('UPDATE channels SET manual_updated_at = ? WHERE channel_id = ?').run(manualUpdatedAt, targetId);
          resultSummary = { channel_id: targetId, manual_updated_at: manualUpdatedAt };
          resultRaw = db.prepare('SELECT channel_id, manual_updated_at FROM channels WHERE channel_id = ?').get(targetId) as Record<string, unknown>;
          updateProgress(100);
          break;
        }

        case 'update_channel_workflow_status': {
          if (targetType !== 'channel' || !targetId) fail('update_channel_workflow_status requires target_type=channel and target_id');
          const workflowStatus = String(input.workflow_status || '').trim();
          if (!['in_progress', 'blocked', 'paused'].includes(workflowStatus)) fail('workflow_status is invalid');
          const result = db.prepare('UPDATE channels SET workflow_status = ? WHERE channel_id = ?').run(workflowStatus, targetId);
          if (result.changes === 0) fail('Channel not found', 'NOT_FOUND');
          resultSummary = { channel_id: targetId, workflow_status: workflowStatus };
          resultRaw = db.prepare('SELECT channel_id, workflow_status FROM channels WHERE channel_id = ?').get(targetId) as Record<string, unknown>;
          updateProgress(100);
          break;
        }

        case 'download_channel_metadata': {
          if (targetType !== 'channel' || !targetId) fail('download_channel_metadata requires target_type=channel and target_id');
          const channel = db.prepare('SELECT channel_id FROM channels WHERE channel_id = ?').get(targetId) as any;
          if (!channel) fail('Channel not found', 'NOT_FOUND');
          const onlyMissing = Boolean(input.only_missing);
          const rows = db.prepare(`
            SELECT video_id, local_meta_path, availability_status
            FROM videos
            WHERE channel_id = ?
            ORDER BY COALESCE(published_at, created_at) DESC
          `).all(targetId) as Array<{ video_id: string; local_meta_path: string | null; availability_status: string | null }>;
          const candidates = rows.filter((row) => {
            if (String(row.availability_status || '').trim().toLowerCase() === 'unavailable') return false;
            if (!onlyMissing) return true;
            const localMetaPath = String(row.local_meta_path || '').trim();
            return !localMetaPath || !fs.existsSync(localMetaPath);
          });
          const downloaded: string[] = [];
          const failed: Array<{ video_id: string; message: string }> = [];
          for (let i = 0; i < candidates.length; i++) {
            const item = candidates[i];
            try {
              await this.handleDownloadMeta(
                {
                  ...job,
                  payload: {
                    video_id: item.video_id,
                    force: Boolean(input.force),
                  },
                },
                logEvent,
                () => {},
              );
              downloaded.push(item.video_id);
            } catch (err: any) {
              failed.push({
                video_id: item.video_id,
                message: String(err?.message || 'unknown error'),
              });
              logEvent('error', `Agent metadata download failed for ${item.video_id}: ${String(err?.message || 'unknown error')}`);
            }
            const pct = candidates.length > 0 ? 5 + Math.floor(((i + 1) / candidates.length) * 95) : 100;
            updateProgress(Math.min(99, pct));
          }
          resultSummary = {
            channel_id: targetId,
            requested_videos: candidates.length,
            downloaded_videos: downloaded.length,
            failed_videos: failed.length,
          };
          resultRaw = { channel_id: targetId, downloaded, failed };
          break;
        }

        case 'refresh_dashboard_cache': {
          resultSummary = { refreshed: true, mode: 'noop_request_time_dashboard' };
          resultRaw = { note: 'Dashboard data is computed on request; no materialized cache was refreshed.' };
          updateProgress(100);
          break;
        }

        default:
          fail(`Unsupported agent action: ${action}`, 'UNSUPPORTED_ACTION');
      }

      const output = {
        ok: true,
        action,
        target: {
          type: targetType,
          id: targetId,
        },
        result: {
          summary: resultSummary,
          raw: resultRaw,
        },
      };
      this.saveStructuredJobResult(String(job.job_id || '').trim(), output);
      updateProgress(100);
    } catch (err: any) {
      this.saveStructuredJobResult(String(job.job_id || '').trim(), {
        ok: false,
        action,
        target: {
          type: targetType,
          id: targetId,
        },
        error: {
          code: String(err?.code || 'AGENT_ACTION_FAILED'),
          message: String(err?.message || 'Agent action failed'),
          retryable: Boolean(err?.retryable),
          details: err?.details || {},
        },
      });
      throw err;
    }
  }

  private countPendingMetadataDownloadJobs(videoIds: string[]): number {
    const normalizedVideoIds = normalizeStringList(videoIds);
    if (normalizedVideoIds.length === 0) return 0;
    const db = getDb();
    const placeholders = normalizedVideoIds.map(() => '?').join(', ');
    const row = db.prepare(`
      SELECT COUNT(*) AS c
      FROM jobs
      WHERE type IN ('download_meta', 'download_all')
        AND status IN ('queued', 'running', 'canceling')
        AND json_extract(payload_json, '$.video_id') IN (${placeholders})
    `).get(...normalizedVideoIds) as any;
    return Math.max(0, Math.trunc(Number(row?.c || 0)));
  }

  private loadVideosMissingLocalMetadata(channelId: string, videoIds: string[]): Array<{ video_id: string; title: string }> {
    const normalizedChannelId = String(channelId || '').trim();
    if (!normalizedChannelId) return [];
    const normalizedVideoIds = normalizeStringList(videoIds);
    if (normalizedVideoIds.length === 0) return [];

    const db = getDb();
    const placeholders = normalizedVideoIds.map(() => '?').join(', ');
    const rows = db.prepare(`
      SELECT video_id, title, local_meta_path, availability_status
      FROM videos
      WHERE channel_id = ?
        AND video_id IN (${placeholders})
    `).all(normalizedChannelId, ...normalizedVideoIds) as Array<{
      video_id: string;
      title: string | null;
      local_meta_path: string | null;
      availability_status: string | null;
    }>;

    const missing: Array<{ video_id: string; title: string }> = [];
    for (const row of rows) {
      const videoId = String(row?.video_id || '').trim();
      if (!videoId) continue;
      if (String(row?.availability_status || '').trim().toLowerCase() === 'unavailable') continue;
      const localMetaPath = resolveExistingPath(row?.local_meta_path);
      if (localMetaPath) continue;
      missing.push({
        video_id: videoId,
        title: String(row?.title || '').trim() || videoId,
      });
    }
    return missing;
  }

  private async handleChannelMetaRetryAudit(
    job: any,
    logEvent: (l: string, m: string) => void,
    updateProgress: (p: number) => void,
  ): Promise<void> {
    const db = getDb();
    const payload = job.payload || {};
    const channelId = String(payload.channel_id || '').trim();
    const videoIds = normalizeStringList(payload.video_ids);
    if (!channelId || videoIds.length === 0) {
      logEvent('warn', 'Metadata retry audit skipped: missing channel_id or video_ids');
      updateProgress(100);
      return;
    }

    const maxWaitSecRaw = Math.trunc(Number(payload.max_wait_sec || 1800) || 1800);
    const maxWaitSec = Math.max(60, Math.min(7200, maxWaitSecRaw));
    const pollMsRaw = Math.trunc(Number(payload.poll_interval_ms || 3000) || 3000);
    const pollMs = Math.max(1000, Math.min(15000, pollMsRaw));
    const maxRetryRaw = Math.trunc(Number(payload.max_retry_count || 1) || 1);
    const maxRetryCount = Math.max(1, Math.min(3, maxRetryRaw));

    logEvent('info', `Metadata retry audit started: channel ${channelId}, videos ${videoIds.length}, wait ${maxWaitSec}s`);
    updateProgress(5);

    const waitStartMs = Date.now();
    let waitLoops = 0;
    while (!job.cancelled()) {
      const pending = this.countPendingMetadataDownloadJobs(videoIds);
      if (pending <= 0) break;

      const elapsedSec = Math.floor((Date.now() - waitStartMs) / 1000);
      if (elapsedSec >= maxWaitSec) {
        logEvent('warn', `Metadata retry audit wait timeout (${maxWaitSec}s), continue with current snapshot`);
        break;
      }

      if (waitLoops === 0 || waitLoops % Math.max(1, Math.floor(15000 / pollMs)) === 0) {
        logEvent('info', `Metadata retry audit waiting: pending ${pending} metadata jobs, elapsed ${elapsedSec}s`);
      }
      waitLoops += 1;

      const waited = await waitWithAbort(pollMs, job.abortSignal);
      if (!waited) {
        logEvent('warn', 'Metadata retry audit aborted while waiting');
        updateProgress(100);
        return;
      }
    }

    updateProgress(45);
    const missingVideos = this.loadVideosMissingLocalMetadata(channelId, videoIds);
    if (missingVideos.length === 0) {
      logEvent('info', `Metadata retry audit finished: all ${videoIds.length} videos already have metadata`);
      updateProgress(100);
      return;
    }

    let queued = 0;
    let skipped = 0;
    for (const row of missingVideos) {
      if (job.cancelled()) break;
      const pendingForVideo = this.countPendingMetadataDownloadJobs([row.video_id]);
      if (pendingForVideo > 0) {
        skipped += 1;
        continue;
      }

      const retryPayload = {
        video_id: row.video_id,
        force: true,
        auto_meta_retry: true,
        auto_meta_retry_count: maxRetryCount,
        source: 'channel_meta_retry_audit',
      };
      const retryJobId = uuidv4();
      db.prepare(`
        INSERT INTO jobs (job_id, type, payload_json, status, parent_job_id)
        VALUES (?, 'download_meta', ?, 'queued', ?)
      `).run(retryJobId, JSON.stringify(retryPayload), job.job_id || null);
      queued += 1;

      const progress = 45 + Math.floor((queued / Math.max(1, missingVideos.length)) * 50);
      updateProgress(Math.max(45, Math.min(95, progress)));
    }

    logEvent('info', `Metadata retry audit queued ${queued} retry jobs, skipped ${skipped}, missing ${missingVideos.length}`);
    updateProgress(100);
  }

  private async handleResearchBulkAdd(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const channels = Array.isArray(job.payload?.channels) ? job.payload.channels : [];
    const defaultTags = Array.isArray(job.payload?.tags)
      ? job.payload.tags.map((item: unknown) => String(item || '').trim()).filter(Boolean)
      : [];
    const normalizeTags = (tags: string[]): string[] => {
      const seen = new Set<string>();
      const out: string[] = [];
      for (const tag of tags) {
        const value = String(tag || '').trim().replace(/^#+/, '');
        if (!value) continue;
        const key = value.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(value);
      }
      return out;
    };
    const parseTagsJson = (raw: unknown): string[] => {
      if (typeof raw !== 'string' || !raw.trim()) return [];
      try {
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return [];
        return normalizeTags(parsed.map((item) => String(item || '')));
      } catch {
        return [];
      }
    };

    if (channels.length === 0) {
      throw new Error('No channels provided for research_bulk_add');
    }

    const total = channels.length;
    let successCount = 0;
    let failedCount = 0;
    const normalizedDefaultTags = normalizeTags(defaultTags);

    logEvent('info', `Research bulk add started: ${total} channels`);
    updateProgress(1);

    for (let i = 0; i < total; i += 1) {
      if (job.cancelled()) break;

      const item = (channels[i] && typeof channels[i] === 'object') ? channels[i] : {};
      const channelId = String((item as any).channel_id || '').trim();
      const handle = String((item as any).handle || '').trim();
      const input = String((item as any).input || channelId || handle).trim();
      const lookup = channelId || handle.replace(/^@+/, '');

      if (!lookup) {
        failedCount += 1;
        logEvent('warn', `Skip invalid input: ${input || '(empty)'}`);
        updateProgress(Math.floor(((i + 1) / total) * 100));
        continue;
      }

      logEvent('info', `Fetching channel ${i + 1}/${total}: ${lookup}`);
      const snapshotResult = await fetchResearchChannelSnapshotFromApi(lookup);
      if (!snapshotResult.success || !snapshotResult.data) {
        failedCount += 1;
        logEvent('warn', `API fetch failed (${lookup}): ${snapshotResult.reason || 'unknown'}`);
        updateProgress(Math.floor(((i + 1) / total) * 100));
        continue;
      }

      const snap = snapshotResult.data;
      const existing = db.prepare('SELECT tags_json FROM research_channels WHERE channel_id = ?').get(snap.channel_id) as any;
      const existingTags = parseTagsJson(existing?.tags_json);
      const mergedTags = normalizeTags([...existingTags, ...normalizedDefaultTags]);

      db.prepare(`
        INSERT INTO research_channels (
          channel_id, title, handle, avatar_url, subscriber_count, video_count, view_count, first_video_published_at, tags_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(channel_id) DO UPDATE SET
          title = excluded.title,
          handle = excluded.handle,
          avatar_url = excluded.avatar_url,
          subscriber_count = excluded.subscriber_count,
          video_count = excluded.video_count,
          view_count = excluded.view_count,
          first_video_published_at = excluded.first_video_published_at,
          tags_json = excluded.tags_json,
          updated_at = datetime('now')
      `).run(
        snap.channel_id,
        snap.title || snap.channel_id,
        snap.handle || (handle || null),
        snap.avatar_url || null,
        snap.subscriber_count,
        snap.video_count,
        snap.view_count,
        snap.first_video_published_at,
        JSON.stringify(mergedTags),
      );

      const today = new Date().toISOString().slice(0, 10);
      db.prepare(`
        INSERT OR REPLACE INTO research_channel_daily (date, channel_id, subscriber_count, view_count)
        VALUES (?, ?, ?, ?)
      `).run(
        today,
        snap.channel_id,
        snap.subscriber_count,
        snap.view_count,
      );

      successCount += 1;
      updateProgress(Math.floor(((i + 1) / total) * 100));
    }

    logEvent('info', `Research bulk add finished. Success ${successCount}, failed ${failedCount}`);
    if (successCount <= 0) {
      throw new Error(`No channels were added. Failed ${failedCount}/${total}`);
    }
    updateProgress(100);
  }

  private async handleHitBulkAdd(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const items = Array.isArray(job.payload?.videos) ? job.payload.videos : [];
    const defaultTags = Array.isArray(job.payload?.tags)
      ? normalizeTagList(job.payload.tags.map((item: unknown) => String(item || '')))
      : [];

    if (items.length === 0) {
      throw new Error('No videos provided for hit_bulk_add');
    }

    const total = items.length;
    let successCount = 0;
    let failedCount = 0;
    const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');

    logEvent('info', `Hit library bulk add started: ${total} videos`);
    updateProgress(1);

    for (let i = 0; i < total; i += 1) {
      if (job.cancelled()) break;

      const raw = (items[i] && typeof items[i] === 'object') ? items[i] : {};
      const input = String((raw as any).input || '').trim();
      const target = normalizeVideoTarget(input);
      if (!target) {
        failedCount += 1;
        logEvent('warn', `Skip invalid video input: ${input || '(empty)'}`);
        updateProgress(Math.floor(((i + 1) / total) * 100));
        continue;
      }

      logEvent('info', `Fetching video ${i + 1}/${total}: ${target}`);
      const infoResult = await ytdlp.getVideoInfo(target, { abortSignal: job.abortSignal });
      if (!infoResult.success || !infoResult.data) {
        failedCount += 1;
        logEvent('warn', `yt-dlp fetch failed (${target}): ${infoResult.error || 'unknown'}`);
        updateProgress(Math.floor(((i + 1) / total) * 100));
        continue;
      }

      const parsed = ytdlp.parseVideoMeta(infoResult.data);
      const parsedVideoId = String(parsed.video_id || infoResult.data?.id || '').trim();
      const webpageUrl = String(parsed.webpage_url || target).trim() || target;
      const platform = normalizePlatformFromInfo(infoResult.data, webpageUrl);
      const logicalVideoId = composeHitVideoId(platform.key, parsedVideoId || `video_${Date.now()}_${i + 1}`);

      const rawChannelId = String(parsed.channel_id || infoResult.data?.channel_id || infoResult.data?.uploader_id || '').trim().replace(/^@+/, '');
      const storageChannelId = sanitizePathSegment(rawChannelId || platform.key, `hit_${platform.key}`);
      const storageVideoId = sanitizePathSegment(logicalVideoId, `video_${i + 1}`);

      const channelId = rawChannelId || storageChannelId;
      const channelTitle = String(infoResult.data?.channel || infoResult.data?.uploader || parsed.uploader || '').trim() || null;
      const title = String(parsed.title || logicalVideoId).trim() || logicalVideoId;
      const description = typeof parsed.description === 'string' ? parsed.description : null;
      const categories = Array.isArray(infoResult.data?.categories)
        ? infoResult.data.categories.filter((item: unknown) => typeof item === 'string' && item.trim()).map((item: string) => item.trim())
        : [];

      let metaPath: string | null = null;
      let thumbPath: string | null = null;

      const metaResult = await ytdlp.downloadMeta(storageVideoId, storageChannelId, {
        abortSignal: job.abortSignal,
        sourceUrl: webpageUrl || target,
      });
      if (metaResult.success) {
        const candidate = path.join(downloadRoot, 'assets', 'meta', storageChannelId, storageVideoId, `${storageVideoId}.info.json`);
        if (fs.existsSync(candidate)) metaPath = candidate;
      }
      const thumbResult = await ytdlp.downloadThumb(storageVideoId, storageChannelId, {
        abortSignal: job.abortSignal,
        sourceUrl: webpageUrl || target,
      });
      if (thumbResult.success) {
        const candidate = path.join(downloadRoot, 'assets', 'thumbs', storageChannelId, storageVideoId, `${storageVideoId}.jpg`);
        if (fs.existsSync(candidate)) thumbPath = candidate;
      }

      let latestInfo: any = infoResult.data;
      if (metaPath) {
        try {
          latestInfo = JSON.parse(fs.readFileSync(metaPath, 'utf-8'));
        } catch {
          latestInfo = infoResult.data;
        }
      }
      const latest = ytdlp.parseVideoMeta(latestInfo);
      const viewCount = latest.view_count ?? parsed.view_count ?? null;
      const likeCount = latest.like_count ?? parsed.like_count ?? null;
      const commentCount = latest.comment_count ?? parsed.comment_count ?? null;
      const publishedAt = latest.published_at ?? parsed.published_at ?? null;
      const durationSec = latest.duration_sec ?? parsed.duration_sec ?? null;

      const existing = db.prepare('SELECT tags_json FROM hit_videos WHERE video_id = ?').get(logicalVideoId) as any;
      const existingTags = parseTagsJson(existing?.tags_json);
      const mergedTags = normalizeTagList([...existingTags, ...defaultTags]);

      db.prepare(`
        INSERT INTO hit_videos (
          video_id, channel_id, channel_title, platform, title, description, webpage_url, published_at, duration_sec,
          view_count, like_count, comment_count, categories_json, tags_json, local_meta_path, local_thumb_path,
          created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(video_id) DO UPDATE SET
          channel_id = excluded.channel_id,
          channel_title = excluded.channel_title,
          platform = excluded.platform,
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
          local_meta_path = excluded.local_meta_path,
          local_thumb_path = excluded.local_thumb_path,
          updated_at = datetime('now')
      `).run(
        logicalVideoId,
        channelId,
        channelTitle,
        platform.label,
        title,
        description,
        webpageUrl,
        publishedAt,
        durationSec,
        viewCount,
        likeCount,
        commentCount,
        JSON.stringify(categories),
        JSON.stringify(mergedTags),
        metaPath,
        thumbPath,
      );

      const today = new Date().toISOString().slice(0, 10);
      db.prepare(`
        INSERT OR REPLACE INTO hit_video_daily (date, video_id, view_count, like_count, comment_count)
        VALUES (?, ?, ?, ?, ?)
      `).run(today, logicalVideoId, viewCount, likeCount, commentCount);

      successCount += 1;
      updateProgress(Math.floor(((i + 1) / total) * 100));
    }

    logEvent('info', `Hit library bulk add finished. Success ${successCount}, failed ${failedCount}`);
    if (successCount <= 0) {
      throw new Error(`No videos were added. Failed ${failedCount}/${total}`);
    }
    updateProgress(100);
  }

  private async refreshResearchChannelsDaily(
    job: any,
    logEvent: (l: string, m: string) => void,
    updateProgress: (p: number) => void,
    startProgress: number,
    progressSpan: number,
  ): Promise<void> {
    const db = getDb();
    const rows = db.prepare(`
      SELECT channel_id, handle
      FROM research_channels
      ORDER BY updated_at DESC, channel_id ASC
    `).all() as Array<{ channel_id: string; handle?: string | null }>;

    if (rows.length <= 0) {
      logEvent('info', 'Research daily refresh skipped: no channels');
      updateProgress(startProgress + progressSpan);
      return;
    }

    const total = rows.length;
    let success = 0;
    let failed = 0;
    const today = new Date().toISOString().slice(0, 10);

    const reportProgress = (index: number) => {
      const ratio = total > 0 ? (index / total) : 1;
      const next = Math.min(99, Math.floor(startProgress + ratio * progressSpan));
      updateProgress(next);
    };

    logEvent('info', `Research daily refresh started: ${total} channels`);
    reportProgress(0);

    for (let i = 0; i < total; i += 1) {
      if (job.cancelled()) break;

      const row = rows[i];
      const channelId = String(row?.channel_id || '').trim();
      const handle = String(row?.handle || '').trim().replace(/^@+/, '');
      const lookup = channelId || handle;
      if (!lookup) {
        failed += 1;
        reportProgress(i + 1);
        continue;
      }

      const snapshotResult = await fetchResearchChannelSnapshotFromApi(lookup);
      if (!snapshotResult.success || !snapshotResult.data) {
        failed += 1;
        logEvent('warn', `Research refresh failed (${lookup}): ${snapshotResult.reason || 'unknown'}`);
        reportProgress(i + 1);
        continue;
      }

      const snap = snapshotResult.data;
      db.prepare(`
        UPDATE research_channels
        SET title = COALESCE(?, title),
            handle = COALESCE(?, handle),
            avatar_url = COALESCE(?, avatar_url),
            subscriber_count = COALESCE(?, subscriber_count),
            video_count = COALESCE(?, video_count),
            view_count = COALESCE(?, view_count),
            first_video_published_at = COALESCE(?, first_video_published_at),
            updated_at = datetime('now')
        WHERE channel_id = ?
      `).run(
        snap.title || null,
        snap.handle || null,
        snap.avatar_url || null,
        snap.subscriber_count,
        snap.video_count,
        snap.view_count,
        snap.first_video_published_at,
        channelId,
      );

      db.prepare(`
        INSERT OR REPLACE INTO research_channel_daily (date, channel_id, subscriber_count, view_count)
        VALUES (?, ?, ?, ?)
      `).run(
        today,
        channelId,
        snap.subscriber_count,
        snap.view_count,
      );

      success += 1;
      reportProgress(i + 1);
    }

    logEvent('info', `Research daily refresh finished. Success: ${success}, Failed: ${failed}`);
    updateProgress(startProgress + progressSpan);
  }

  private async refreshHitVideosDaily(
    job: any,
    logEvent: (l: string, m: string) => void,
    updateProgress: (p: number) => void,
    startProgress: number,
    progressSpan: number,
  ): Promise<void> {
    const db = getDb();
    const rows = db.prepare(`
      SELECT video_id, webpage_url
      FROM hit_videos
      ORDER BY updated_at DESC, video_id ASC
    `).all() as Array<{ video_id: string; webpage_url?: string | null }>;

    if (rows.length <= 0) {
      logEvent('info', 'Hit library daily refresh skipped: no videos');
      updateProgress(startProgress + progressSpan);
      return;
    }

    const total = rows.length;
    let success = 0;
    let failed = 0;
    const today = new Date().toISOString().slice(0, 10);

    const reportProgress = (index: number) => {
      const ratio = total > 0 ? (index / total) : 1;
      const next = Math.min(99, Math.floor(startProgress + ratio * progressSpan));
      updateProgress(next);
    };

    logEvent('info', `Hit library daily refresh started: ${total} videos`);
    reportProgress(0);

    for (let i = 0; i < total; i += 1) {
      if (job.cancelled()) break;

      const row = rows[i];
      const videoId = String(row?.video_id || '').trim();
      const target = normalizeVideoTarget(String(row?.webpage_url || '').trim());
      if (!videoId || !target) {
        failed += 1;
        reportProgress(i + 1);
        continue;
      }

      const infoResult = await ytdlp.getVideoInfo(target, { abortSignal: job.abortSignal });
      if (!infoResult.success || !infoResult.data) {
        failed += 1;
        logEvent('warn', `Hit refresh failed (${videoId}): ${infoResult.error || 'unknown'}`);
        reportProgress(i + 1);
        continue;
      }

      const parsed = ytdlp.parseVideoMeta(infoResult.data);
      const parsedWebpageUrl = String(parsed.webpage_url || target).trim() || target;
      const platform = normalizePlatformFromInfo(infoResult.data, parsedWebpageUrl);
      const rawChannelId = String(parsed.channel_id || infoResult.data?.channel_id || infoResult.data?.uploader_id || '').trim().replace(/^@+/, '');
      const channelTitle = String(infoResult.data?.channel || infoResult.data?.uploader || parsed.uploader || '').trim() || null;
      const title = String(parsed.title || '').trim() || null;
      const description = typeof parsed.description === 'string' && parsed.description.trim() ? parsed.description : null;
      const categories = Array.isArray(infoResult.data?.categories)
        ? infoResult.data.categories.filter((item: unknown) => typeof item === 'string' && item.trim()).map((item: string) => item.trim())
        : [];

      db.prepare(`
        UPDATE hit_videos
        SET channel_id = COALESCE(?, channel_id),
            channel_title = COALESCE(?, channel_title),
            platform = COALESCE(?, platform),
            title = COALESCE(?, title),
            description = COALESCE(?, description),
            webpage_url = COALESCE(?, webpage_url),
            published_at = COALESCE(?, published_at),
            duration_sec = COALESCE(?, duration_sec),
            view_count = COALESCE(?, view_count),
            like_count = COALESCE(?, like_count),
            comment_count = COALESCE(?, comment_count),
            categories_json = COALESCE(?, categories_json),
            updated_at = datetime('now')
        WHERE video_id = ?
      `).run(
        rawChannelId || null,
        channelTitle,
        platform.label || null,
        title,
        description,
        parsedWebpageUrl || null,
        parsed.published_at,
        parsed.duration_sec,
        parsed.view_count,
        parsed.like_count,
        parsed.comment_count,
        JSON.stringify(categories),
        videoId,
      );

      db.prepare(`
        INSERT OR REPLACE INTO hit_video_daily (date, video_id, view_count, like_count, comment_count)
        VALUES (?, ?, ?, ?, ?)
      `).run(
        today,
        videoId,
        parsed.view_count,
        parsed.like_count,
        parsed.comment_count,
      );

      success += 1;
      reportProgress(i + 1);
    }

    logEvent('info', `Hit library daily refresh finished. Success: ${success}, Failed: ${failed}`);
    updateProgress(startProgress + progressSpan);
  }

  private async autoAddYoutubeBreakoutsToHitLibrary(
    job: any,
    logEvent: (l: string, m: string) => void,
    updateProgress: (p: number) => void,
    startProgress: number,
    progressSpan: number,
  ): Promise<void> {
    const db = getDb();
    const day7ago = new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10);
    const today = new Date().toISOString().slice(0, 10);

    const candidates = db.prepare(`
      SELECT
        v.video_id,
        v.channel_id,
        c.title as channel_title,
        v.title,
        v.description,
        v.webpage_url,
        v.published_at,
        v.duration_sec,
        v.content_type,
        v.tags_json,
        v.local_meta_path,
        v.local_thumb_path,
        COALESCE(vd_latest.view_count, v.view_count) as latest_views,
        COALESCE(vd_latest.like_count, v.like_count) as latest_likes,
        COALESCE(vd_latest.comment_count, v.comment_count) as latest_comments,
        (COALESCE(vd_latest.view_count, v.view_count) - vd_7d.view_count) as views_change_7d
      FROM videos v
      LEFT JOIN channels c ON c.channel_id = v.channel_id
      LEFT JOIN (
        SELECT video_id, view_count, like_count, comment_count FROM video_daily
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
      WHERE lower(COALESCE(v.platform, c.platform, 'youtube')) = 'youtube'
        AND v.availability_status = 'available'
        AND COALESCE(vd_latest.view_count, v.view_count) IS NOT NULL
        AND vd_7d.view_count IS NOT NULL
    `).all(day7ago) as Array<{
      video_id: string;
      channel_id: string | null;
      channel_title: string | null;
      title: string | null;
      description: string | null;
      webpage_url: string | null;
      published_at: string | null;
      duration_sec: number | null;
      content_type: string | null;
      tags_json: string | null;
      local_meta_path: string | null;
      local_thumb_path: string | null;
      latest_views: number | null;
      latest_likes: number | null;
      latest_comments: number | null;
      views_change_7d: number | null;
    }>;

    if (candidates.length <= 0) {
      logEvent('info', 'YouTube auto-hit skipped: no candidate videos with 7d baseline');
      updateProgress(startProgress + progressSpan);
      return;
    }

    const upsertHit = db.prepare(`
      INSERT INTO hit_videos (
        video_id, channel_id, channel_title, platform, title, description, webpage_url, published_at, duration_sec,
        view_count, like_count, comment_count, categories_json, tags_json, local_meta_path, local_thumb_path,
        created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
      ON CONFLICT(video_id) DO UPDATE SET
        channel_id = excluded.channel_id,
        channel_title = excluded.channel_title,
        platform = excluded.platform,
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

    const existingIds = new Set(
      (db.prepare('SELECT video_id FROM hit_videos').all() as Array<{ video_id: string }>)
        .map((item) => String(item.video_id || '').trim())
        .filter(Boolean),
    );

    let qualified = 0;
    let inserted = 0;
    let updated = 0;
    let longQualified = 0;
    let shortQualified = 0;

    const total = candidates.length;
    const reportProgress = (index: number) => {
      const ratio = total > 0 ? (index / total) : 1;
      const next = Math.min(99, Math.floor(startProgress + ratio * progressSpan));
      updateProgress(next);
    };

    logEvent(
      'info',
      `YouTube auto-hit scan started: ${total} candidates, long>=${YOUTUBE_HIT_THRESHOLD_LONG_7D}, short>=${YOUTUBE_HIT_THRESHOLD_SHORT_7D}`,
    );
    reportProgress(0);

    for (let i = 0; i < total; i += 1) {
      if (job.cancelled()) break;
      const row = candidates[i];

      const rawVideoId = String(row.video_id || '').trim();
      if (!rawVideoId) {
        reportProgress(i + 1);
        continue;
      }
      const viewIncrease7d = toNullableInt(row.views_change_7d);
      if (viewIncrease7d == null || viewIncrease7d <= 0) {
        reportProgress(i + 1);
        continue;
      }

      const url = String(row.webpage_url || '').toLowerCase();
      const contentType = String(row.content_type || '').toLowerCase();
      const isShort = contentType === 'short' || url.includes('/shorts/');
      const threshold = isShort ? YOUTUBE_HIT_THRESHOLD_SHORT_7D : YOUTUBE_HIT_THRESHOLD_LONG_7D;
      if (viewIncrease7d < threshold) {
        reportProgress(i + 1);
        continue;
      }

      const logicalVideoId = composeHitVideoId('youtube', rawVideoId);
      const existing = db.prepare('SELECT tags_json FROM hit_videos WHERE video_id = ?').get(logicalVideoId) as any;
      const existingTags = parseTagsJson(existing?.tags_json);
      const sourceTags = parseTagsJson(row.tags_json);
      const mergedTags = normalizeTagList([...existingTags, ...sourceTags]);
      const localMeta = readLocalVideoMeta(row.local_meta_path);
      const categories = extractMetaCategories(localMeta);

      upsertHit.run(
        logicalVideoId,
        row.channel_id || null,
        row.channel_title || null,
        'YouTube',
        String(row.title || logicalVideoId),
        row.description || null,
        row.webpage_url || null,
        row.published_at || null,
        toNullableInt(row.duration_sec),
        toNullableInt(row.latest_views),
        toNullableInt(row.latest_likes),
        toNullableInt(row.latest_comments),
        JSON.stringify(categories),
        JSON.stringify(mergedTags),
        row.local_meta_path || null,
        row.local_thumb_path || null,
      );
      upsertDaily.run(
        today,
        logicalVideoId,
        toNullableInt(row.latest_views),
        toNullableInt(row.latest_likes),
        toNullableInt(row.latest_comments),
      );

      qualified += 1;
      if (isShort) shortQualified += 1;
      else longQualified += 1;
      if (existingIds.has(logicalVideoId)) {
        updated += 1;
      } else {
        inserted += 1;
        existingIds.add(logicalVideoId);
      }
      reportProgress(i + 1);
    }

    logEvent(
      'info',
      `YouTube auto-hit scan finished: qualified ${qualified} (long ${longQualified}, short ${shortQualified}), inserted ${inserted}, updated ${updated}`,
    );
    updateProgress(startProgress + progressSpan);
  }

  private async handleSyncChannel(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const payload = job.payload;
    const channelId = payload.channel_id;
    const scheduleMetaRetryAudit = Boolean(payload?.meta_retry_audit || payload?.post_add_meta_retry_audit);
    const deferAutoDownload = Boolean(payload?.defer_auto_download);
    const deferredAutoDownloads = Array.isArray(payload?.deferred_auto_downloads)
      ? payload.deferred_auto_downloads as Array<{ video_id: string; with: string[] }>
      : null;
    const channel = db.prepare('SELECT * FROM channels WHERE channel_id = ?').get(channelId) as any;
    if (!channel) throw new Error(`Channel not found: ${channelId}`);
    if (!isSupportedRawChannelPlatform(channel.platform)) {
      throw new Error(`Unsupported channel platform: ${String(channel.platform || 'unknown')}`);
    }

    const platform = normalizeChannelPlatform(channel.platform);
    const trackDeleted = shouldTrackDeletedByPlatform(platform);
    let hasConfirmedChannelAlive = false;
    let channelMarkedInvalid = false;
    let channelInvalidReason: string | null = null;
    let douyinSecUid: string | null = null;
    let douyinPrefetchedEntries: any[] = [];
    let douyinReportedVideoCount: number | null = null;
    let specializedFeedEntries: any[] = [];

    const markChannelHealthy = () => {
      hasConfirmedChannelAlive = true;
      if (trackDeleted) {
        updateChannelMonitorState(db, channelId, 'ok', null);
      }
    };

    const markChannelInvalid = (reason: string) => {
      channelMarkedInvalid = true;
      channelInvalidReason = reason || 'unknown';
      if (trackDeleted) {
        updateChannelMonitorState(db, channelId, 'invalid', reason || 'unknown');
      }
    };

    logEvent('info', `Syncing channel: ${channelId} (${platform})`);
    updateProgress(10);

    const channelUrl = buildChannelUrlByPlatform(channel, platform);
    if (!channelUrl) {
      throw new Error(`Unable to resolve channel URL for ${channelId} (${platform})`);
    }
    logEvent('info', `Using channel URL: ${channelUrl}`);

    // Smart path: call official API only when missing or stale; otherwise prefer local/yt-dlp pipeline.
    const apiDecision = shouldFetchChannelSnapshotByApi(channel);
    let apiSnapshotApplied = false;

    if (apiDecision.shouldFetch) {
      const apiResult = await fetchChannelSnapshotFromApi(channelId);
      if (apiResult.success && apiResult.data) {
        const snapshot = apiResult.data;
        const today = new Date().toISOString().slice(0, 10);

        db.prepare(`
          UPDATE channels
          SET title = ?,
              handle = ?,
              avatar_url = ?,
              country = ?,
              subscriber_count = ?,
              view_count = ?,
              video_count = ?,
              api_last_sync_at = datetime('now'),
              last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(
          snapshot.title || channel.title,
          snapshot.customUrl || channel.handle,
          snapshot.highThumbnailUrl || channel.avatar_url,
          snapshot.country || channel.country,
          snapshot.subscriberCount,
          snapshot.totalViews,
          snapshot.videoCount,
          channelId
        );

        db.prepare(`
          INSERT OR REPLACE INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
          VALUES (?, ?, ?, ?, ?)
        `).run(today, channelId, snapshot.subscriberCount, snapshot.totalViews, snapshot.videoCount);

        logEvent('info', `Channel metadata updated by YouTube API. Subs: ${snapshot.subscriberCount}`);
        markChannelHealthy();
        apiSnapshotApplied = true;
      } else {
        logEvent('warn', `YouTube API unavailable (${apiResult.reason || 'unknown'}), fallback to yt-dlp`);
        if (isApiChannelUnavailableReason(apiResult.reason)) {
          const reason = apiResult.reason || 'youtube_api_channel_not_found';
          markChannelInvalid(reason);
          if (trackDeleted) {
            logEvent('warn', `Channel marked invalid by YouTube API: ${reason}`);
          }
        }
      }
    } else {
      if (platform === 'youtube') {
        logEvent('info', `Skip YouTube API (${apiDecision.reason}), use yt-dlp/local metadata`);
      } else {
        logEvent('info', `Non-YouTube platform, skipping YouTube API path (${apiDecision.reason})`);
      }
    }

    if (!apiSnapshotApplied) {
      const infoResult = await ytdlp.getChannelInfo(channelUrl, { abortSignal: job.abortSignal });

      if (infoResult.success && infoResult.data) {
        const meta = ytdlp.parseChannelMeta(infoResult.data);
        const currentChannelVideoCount = toNullableInt(channel?.video_count);
        const fallbackVideoCountRaw = toNullableInt(meta.video_count);
        const mergedFallbackVideoCount = platform === 'youtube'
          ? (
            fallbackVideoCountRaw == null
              ? currentChannelVideoCount
              : (currentChannelVideoCount == null
                ? fallbackVideoCountRaw
                : Math.max(currentChannelVideoCount, fallbackVideoCountRaw))
          )
          : fallbackVideoCountRaw;
        const safeMetaTitle = isUnknownChannelTitle(meta.title) ? null : meta.title;
        db.prepare(`
          UPDATE channels SET title = COALESCE(?, title), handle = ?, avatar_url = ?, last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(
          safeMetaTitle,
          meta.handle || channel.handle,
          meta.avatar_url || channel.avatar_url,
          channelId
        );

        // Write channel_daily snapshot
        const today = new Date().toISOString().slice(0, 10);
        db.prepare(`
          INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(date, channel_id) DO UPDATE SET
            subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count),
            view_count_total = COALESCE(excluded.view_count_total, channel_daily.view_count_total),
            video_count = COALESCE(excluded.video_count, channel_daily.video_count)
        `).run(today, channelId, meta.subscriber_count, meta.view_count_total, mergedFallbackVideoCount);

        // Also update main channel record with latest stats
        db.prepare(`
          UPDATE channels
          SET subscriber_count = COALESCE(?, subscriber_count),
              view_count = COALESCE(?, view_count),
              video_count = COALESCE(?, video_count),
              last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(meta.subscriber_count, meta.view_count_total, mergedFallbackVideoCount, channelId);
        if (mergedFallbackVideoCount != null) {
          channel.video_count = mergedFallbackVideoCount;
        }

        logEvent('info', `Channel metadata updated via yt-dlp fallback. Subs: ${meta.subscriber_count}`);
        markChannelHealthy();
      } else {
        logEvent('warn', `Failed to fetch channel info by yt-dlp fallback: ${infoResult.error}`);
        const infoErrorText = [infoResult.error, infoResult.log].filter(Boolean).join('\n');
        if (!hasConfirmedChannelAlive && ytdlp.isChannelUnavailableError(infoErrorText, infoResult.errorCode)) {
          const reason = infoResult.errorCode || 'channel_not_found';
          markChannelInvalid(reason);
          if (trackDeleted) {
            logEvent('warn', `Channel marked invalid by yt-dlp metadata check: ${reason}`);
          }
        }
      }
    }

    if (platform === 'bilibili') {
      const biliUid = resolveBilibiliUid(channel, channelId);
      if (biliUid) {
        const accountInfo = await fetchBilibiliAccountInfo(biliUid, job.abortSignal);
        if (accountInfo.avatarUrl || accountInfo.title || accountInfo.follower != null) {
          const shouldApplyTitle = !!accountInfo.title && isUnknownChannelTitle(channel?.title);
          db.prepare(`
            UPDATE channels
            SET title = COALESCE(?, title),
                avatar_url = COALESCE(?, avatar_url),
                subscriber_count = COALESCE(?, subscriber_count),
                last_sync_at = datetime('now')
            WHERE channel_id = ?
          `).run(
            shouldApplyTitle ? accountInfo.title : null,
            accountInfo.avatarUrl,
            accountInfo.follower,
            channelId,
          );
          if (shouldApplyTitle && accountInfo.title) {
            channel.title = accountInfo.title;
          }
          if (accountInfo.avatarUrl) {
            channel.avatar_url = accountInfo.avatarUrl;
          }
          if (accountInfo.follower != null) {
            channel.subscriber_count = accountInfo.follower;
          }
          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
            VALUES (?, ?, ?, NULL, NULL)
            ON CONFLICT(date, channel_id) DO UPDATE SET
              subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count)
          `).run(today, channelId, accountInfo.follower);
          logEvent('info', `Bilibili account API updated (source=${accountInfo.source || 'unknown'})${accountInfo.avatarUrl ? ' avatar' : ''}${shouldApplyTitle ? ' title' : ''}${accountInfo.follower != null ? ' followers' : ''}`.trim());
        } else if (accountInfo.reason) {
          logEvent('warn', `Bilibili account API unavailable: ${accountInfo.reason}`);
        }

        const relation = await fetchBilibiliRelationStat(biliUid, job.abortSignal);
        if (relation.follower != null) {
          db.prepare(`
            UPDATE channels
            SET subscriber_count = ?, last_sync_at = datetime('now')
            WHERE channel_id = ?
          `).run(relation.follower, channelId);
          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
            VALUES (?, ?, ?, NULL, NULL)
            ON CONFLICT(date, channel_id) DO UPDATE SET
              subscriber_count = excluded.subscriber_count
          `).run(today, channelId, relation.follower);
          channel.subscriber_count = relation.follower;
          logEvent('info', `Bilibili relation API updated followers: ${relation.follower}`);
        } else if (relation.reason) {
          logEvent('warn', `Bilibili relation API unavailable: ${relation.reason}`);
        }
      } else {
        logEvent('warn', 'Bilibili relation API skipped: uid unresolved');
      }
    }

    if (platform === 'tiktok') {
      const limit = parseInt(getSetting('recent_video_fetch_limit') || '50', 10);
      const tiktokCookie = resolvePlatformCookieHeader(platform) || '';
      const tiktokSeedInput = String(channel?.source_url || channelUrl || channel?.handle || channelId).trim();
      if (tiktokCookie && tiktokSeedInput) {
        const resolved = await resolveTikTokDownloaderAccountId(
          'tiktok',
          tiktokSeedInput,
          tiktokCookie,
          '',
          job.abortSignal,
        );
        if (resolved.ok && resolved.accountId) {
          const fetchResult = await fetchTikTokDownloaderAccountFeed(
            'tiktok',
            resolved.accountId,
            tiktokCookie,
            {
              abortSignal: job.abortSignal,
              tab: 'post',
              pages: Math.max(1, Math.ceil(limit / 20)),
              count: Math.max(1, Math.min(20, limit)),
            },
          );
          if (fetchResult.ok && fetchResult.entries.length > 0) {
            specializedFeedEntries = fetchResult.entries.map((entry) => normalizeTikTokDownloaderFeedEntry(platform, entry));
            const snap = fetchResult.channel;
            const normalizedHandle = String(snap?.unique_id || '').trim();
            db.prepare(`
              UPDATE channels
              SET title = COALESCE(?, title),
                  handle = COALESCE(?, handle),
                  avatar_url = COALESCE(?, avatar_url),
                  subscriber_count = COALESCE(?, subscriber_count),
                  source_url = COALESCE(?, source_url),
                  last_sync_at = datetime('now')
              WHERE channel_id = ?
            `).run(
              snap?.title || null,
              normalizedHandle ? `@${normalizedHandle}` : null,
              snap?.avatar_url || null,
              snap?.follower_count ?? null,
              normalizedHandle ? `https://www.tiktok.com/@${normalizedHandle}` : channel.source_url,
              channelId,
            );
            if (snap?.title) channel.title = snap.title;
            if (snap?.avatar_url) channel.avatar_url = snap.avatar_url;
            if (snap?.follower_count != null) channel.subscriber_count = snap.follower_count;
            if (normalizedHandle) channel.handle = `@${normalizedHandle}`;
            const today = new Date().toISOString().slice(0, 10);
            db.prepare(`
              INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
              VALUES (?, ?, ?, NULL, NULL)
              ON CONFLICT(date, channel_id) DO UPDATE SET
                subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count)
            `).run(today, channelId, snap?.follower_count ?? null);
            logEvent('info', `TikTokDownloader account feed fetched ${specializedFeedEntries.length} videos`);
            markChannelHealthy();
          } else {
            logEvent('warn', `TikTokDownloader account feed unavailable: ${fetchResult.error || 'empty_entries'}`);
          }
        } else {
          logEvent('warn', `TikTokDownloader account resolve failed: ${resolved.error || 'unknown'}`);
        }
      } else if (!tiktokCookie) {
        logEvent('warn', 'TikTokDownloader skipped: TikTok cookie missing');
      }

      const needsSnapshot = (
        channel?.subscriber_count == null
        || Number(channel.subscriber_count) <= 0
        || !String(channel?.avatar_url || '').trim()
        || isUnknownChannelTitle(channel?.title)
      );
      if (needsSnapshot) {
        const tiktokProfileUrl = resolveTiktokProfileUrl(channel, channelUrl);
        if (tiktokProfileUrl) {
          const snapshot = await fetchTiktokProfileSnapshot(tiktokProfileUrl, job.abortSignal);
          if (snapshot.followerCount != null || snapshot.avatarUrl || snapshot.title) {
            const shouldApplyTitle = !!snapshot.title && isUnknownChannelTitle(channel?.title);
            db.prepare(`
              UPDATE channels
              SET title = COALESCE(?, title),
                  avatar_url = COALESCE(?, avatar_url),
                  subscriber_count = COALESCE(?, subscriber_count),
                  last_sync_at = datetime('now')
              WHERE channel_id = ?
            `).run(
              shouldApplyTitle ? snapshot.title : null,
              snapshot.avatarUrl,
              snapshot.followerCount,
              channelId,
            );

            const today = new Date().toISOString().slice(0, 10);
            db.prepare(`
              INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
              VALUES (?, ?, ?, NULL, NULL)
              ON CONFLICT(date, channel_id) DO UPDATE SET
                subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count)
            `).run(today, channelId, snapshot.followerCount);

            if (shouldApplyTitle && snapshot.title) channel.title = snapshot.title;
            if (snapshot.avatarUrl) channel.avatar_url = snapshot.avatarUrl;
            if (snapshot.followerCount != null) channel.subscriber_count = snapshot.followerCount;
            logEvent(
              'info',
              `TikTok profile HTML snapshot updated${snapshot.source ? ` (source=${snapshot.source})` : ''}${snapshot.avatarUrl ? ' avatar' : ''}${shouldApplyTitle ? ' title' : ''}${snapshot.followerCount != null ? ' followers' : ''}`.trim(),
            );
          } else if (snapshot.reason) {
            logEvent('warn', `TikTok profile snapshot unavailable: ${snapshot.reason}`);
          }
        } else {
          logEvent('warn', 'TikTok profile snapshot skipped: profile url unresolved');
        }
      }
    }

    if (platform === 'xiaohongshu') {
      const limit = parseInt(getSetting('recent_video_fetch_limit') || '50', 10);
      const xhsCookie = resolvePlatformCookieHeader(platform) || '';
      const seedInput = String(
        channel?.source_url
        || channelUrl
        || stripStoredChannelPrefix(channelId, platform)
        || channelId,
      ).trim();

      if (xhsCookie && seedInput) {
        const fetchResult = await fetchXhsSpiderAccountFeed(
          seedInput,
          xhsCookie,
          {
            limit: Math.max(1, Math.min(limit, 500)),
            includeNoteDetail: true,
            abortSignal: job.abortSignal,
          },
        );
        if (fetchResult.ok) {
          specializedFeedEntries = fetchResult.entries.map((entry) => ({
            ...entry,
            raw: entry?.raw && typeof entry.raw === 'object' ? entry.raw : entry,
          }));
          const snap = fetchResult.channel;
          const normalizedHandle = String(snap?.handle || '').trim();
          const normalizedUserId = String(snap?.user_id || '').trim();
          const profileUrl = String(snap?.profile_url || '').trim()
            || (normalizedUserId ? `https://www.xiaohongshu.com/user/profile/${normalizedUserId}` : channelUrl);
          db.prepare(`
            UPDATE channels
            SET title = COALESCE(?, title),
                handle = COALESCE(?, handle),
                avatar_url = COALESCE(?, avatar_url),
                subscriber_count = COALESCE(?, subscriber_count),
                view_count = COALESCE(?, view_count),
                video_count = COALESCE(?, video_count),
                source_url = COALESCE(?, source_url),
                last_sync_at = datetime('now')
            WHERE channel_id = ?
          `).run(
            snap?.title || null,
            normalizedHandle || channel.handle,
            snap?.avatar_url || null,
            snap?.follower_count ?? null,
            snap?.total_view_count ?? null,
            snap?.note_count ?? null,
            profileUrl || channel.source_url,
            channelId,
          );

          if (snap?.title) channel.title = snap.title;
          if (snap?.avatar_url) channel.avatar_url = snap.avatar_url;
          if (snap?.follower_count != null) channel.subscriber_count = snap.follower_count;
          if (snap?.total_view_count != null) channel.view_count = snap.total_view_count;
          if (snap?.note_count != null) channel.video_count = snap.note_count;
          if (normalizedHandle) channel.handle = normalizedHandle;

          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, channel_id) DO UPDATE SET
              subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count),
              view_count_total = COALESCE(excluded.view_count_total, channel_daily.view_count_total),
              video_count = COALESCE(excluded.video_count, channel_daily.video_count)
          `).run(
            today,
            channelId,
            snap?.follower_count ?? null,
            snap?.total_view_count ?? null,
            snap?.note_count ?? null,
          );

          logEvent(
            'info',
            `Spider_XHS account feed fetched ${specializedFeedEntries.length} items`
            + `${snap?.follower_count != null ? `, followers=${snap.follower_count}` : ''}`
            + `${snap?.note_count != null ? `, notes=${snap.note_count}` : ''}`,
          );
          markChannelHealthy();
        } else {
          logEvent('warn', `Spider_XHS account feed unavailable: ${fetchResult.error || 'unknown'}`);
        }
      } else if (!xhsCookie) {
        logEvent('warn', 'Spider_XHS skipped: Xiaohongshu cookie missing');
      } else {
        logEvent('warn', 'Spider_XHS skipped: Xiaohongshu channel input unresolved');
      }
    }

    if (platform === 'douyin') {
      douyinSecUid = resolveDouyinSecUidFromChannel(channel, channelId, channelUrl);
      if (!douyinSecUid) {
        const douyinCookie = resolvePlatformCookieHeader(platform) || '';
        const resolveSeed = String(channel?.source_url || channelUrl || channel?.handle || channelId).trim();
        if (douyinCookie && resolveSeed) {
          const resolved = await resolveTikTokDownloaderAccountId(
            'douyin',
            resolveSeed,
            douyinCookie,
            '',
            job.abortSignal,
          );
          if (resolved.ok && resolved.accountId) {
            douyinSecUid = resolved.accountId;
            db.prepare(`
              UPDATE channels
              SET source_url = COALESCE(?, source_url),
                  last_sync_at = datetime('now')
              WHERE channel_id = ?
            `).run(`https://www.douyin.com/user/${douyinSecUid}`, channelId);
            logEvent('info', `Douyin sec_uid resolved by TikTokDownloader: ${douyinSecUid}`);
          } else {
            logEvent('warn', `Douyin sec_uid resolve failed: ${resolved.error || 'unknown'}`);
          }
        }
      }
      if (!douyinSecUid) {
        logEvent('warn', 'Douyin channel id is not sec_uid and auto-resolve failed');
        throw new Error('抖音同步失败：无法解析 sec_uid。请更新抖音 Cookie 或使用 sec_uid 主页链接重新添加频道。');
      }

      const douyinCookie = resolvePlatformCookieHeader(platform) || '';
      const limit = parseInt(getSetting('recent_video_fetch_limit') || '50', 10);
      if (douyinCookie) {
        const fetchResult = await fetchTikTokDownloaderAccountFeed(
          'douyin',
          douyinSecUid,
          douyinCookie,
          {
            abortSignal: job.abortSignal,
            tab: 'post',
            pages: Math.max(1, Math.ceil(limit / 20)),
            count: Math.max(1, Math.min(20, limit)),
          },
        );
        if (fetchResult.ok && fetchResult.entries.length > 0) {
          specializedFeedEntries = fetchResult.entries.map((entry) => normalizeTikTokDownloaderFeedEntry(platform, entry));
          douyinReportedVideoCount = fetchResult.entries.length;
          const snap = fetchResult.channel;
          const normalizedHandle = String(snap?.unique_id || '').trim();
          db.prepare(`
            UPDATE channels
            SET title = COALESCE(?, title),
                handle = COALESCE(?, handle),
                avatar_url = COALESCE(?, avatar_url),
                subscriber_count = COALESCE(?, subscriber_count),
                source_url = COALESCE(?, source_url),
                last_sync_at = datetime('now')
            WHERE channel_id = ?
          `).run(
            snap?.title || null,
            normalizedHandle ? `@${normalizedHandle}` : channel.handle,
            snap?.avatar_url || null,
            snap?.follower_count ?? null,
            `https://www.douyin.com/user/${douyinSecUid}`,
            channelId,
          );
          if (snap?.title) channel.title = snap.title;
          if (snap?.avatar_url) channel.avatar_url = snap.avatar_url;
          if (snap?.follower_count != null) channel.subscriber_count = snap.follower_count;
          if (normalizedHandle) channel.handle = `@${normalizedHandle}`;
          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
            VALUES (?, ?, ?, NULL, ?)
            ON CONFLICT(date, channel_id) DO UPDATE SET
              subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count),
              video_count = COALESCE(excluded.video_count, channel_daily.video_count)
          `).run(today, channelId, snap?.follower_count ?? null, douyinReportedVideoCount);
          logEvent('info', `TikTokDownloader Douyin feed fetched ${specializedFeedEntries.length} videos`);
          markChannelHealthy();
        } else {
          logEvent('warn', `TikTokDownloader Douyin feed unavailable: ${fetchResult.error || 'empty_entries'}`);
        }
      } else {
        logEvent('warn', 'TikTokDownloader skipped: Douyin cookie missing');
      }

      if (specializedFeedEntries.length > 0) {
        const snapshot = await fetchDouyinUserSnapshot(douyinSecUid, job.abortSignal);
        if (snapshot.awemeCount != null) {
          douyinReportedVideoCount = snapshot.awemeCount;
        }
        if (snapshot.title || snapshot.avatarUrl || snapshot.followerCount != null || snapshot.awemeCount != null) {
          db.prepare(`
            UPDATE channels
            SET title = COALESCE(?, title),
                handle = COALESCE(?, handle),
                avatar_url = COALESCE(?, avatar_url),
                subscriber_count = COALESCE(?, subscriber_count),
                source_url = COALESCE(?, source_url),
                last_sync_at = datetime('now')
            WHERE channel_id = ?
          `).run(
            snapshot.title,
            snapshot.uniqueId ? `@${snapshot.uniqueId}` : channel.handle,
            snapshot.avatarUrl,
            snapshot.followerCount,
            `https://www.douyin.com/user/${douyinSecUid}`,
            channelId,
          );

          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
            VALUES (?, ?, ?, NULL, ?)
            ON CONFLICT(date, channel_id) DO UPDATE SET
              subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count),
              video_count = COALESCE(excluded.video_count, channel_daily.video_count)
          `).run(today, channelId, snapshot.followerCount, snapshot.awemeCount ?? douyinReportedVideoCount);

          if (snapshot.title) channel.title = snapshot.title;
          if (snapshot.avatarUrl) channel.avatar_url = snapshot.avatarUrl;
          if (snapshot.followerCount != null) channel.subscriber_count = snapshot.followerCount;
          if (snapshot.uniqueId) channel.handle = `@${snapshot.uniqueId}`;
          markChannelHealthy();
        } else if (snapshot.reason) {
          logEvent('warn', `Douyin user profile unavailable: ${snapshot.reason}`);
        }

        if (snapshot.mixIds.length > 0 && limit > 0) {
          const unique = new Map<string, any>();
          for (const mixId of snapshot.mixIds) {
            if (unique.size >= limit) break;
            const pulled = await fetchDouyinMixEntries(douyinSecUid, mixId, limit - unique.size, job.abortSignal);
            if (pulled.reason && pulled.items.length === 0) {
              logEvent('warn', `Douyin mix ${mixId} fetch failed: ${pulled.reason}`);
              continue;
            }
            for (const aweme of pulled.items) {
              const awemeId = String(aweme?.aweme_id || '').trim();
              if (!awemeId || unique.has(awemeId)) continue;
              const hasTextMeta = Boolean(String(aweme?.desc || aweme?.title || '').trim());
              const hasDurationMeta = toNullableInt(
                aweme?.duration
                ?? aweme?.video?.duration
                ?? aweme?.duration_ms,
              ) != null;
              const hasStatsMeta = [
                toNullableInt(aweme?.statistics?.digg_count ?? aweme?.statistics?.diggCount ?? aweme?.stats?.diggCount),
                toNullableInt(aweme?.statistics?.comment_count ?? aweme?.statistics?.commentCount ?? aweme?.stats?.commentCount),
                toNullableInt(aweme?.statistics?.collect_count ?? aweme?.statistics?.collectCount ?? aweme?.stats?.collectCount),
                toNullableInt(aweme?.statistics?.share_count ?? aweme?.statistics?.shareCount ?? aweme?.stats?.shareCount),
              ].some((value) => value != null);
              if (!hasTextMeta && !hasDurationMeta && !hasStatsMeta) continue;
              const isNote = isDouyinNoteEntry(aweme);
              const pageUrl = `https://www.douyin.com/${isNote ? 'note' : 'video'}/${awemeId}`;
              unique.set(awemeId, {
                ...aweme,
                id: awemeId,
                url: pageUrl,
                webpage_url: pageUrl,
                channel: String(aweme?.author?.nickname || '').trim() || null,
                uploader: String(aweme?.author?.nickname || '').trim() || null,
                uploader_id: String(aweme?.author?.unique_id || aweme?.author?.sec_uid || '').trim() || null,
                channel_id: String(aweme?.author?.uid || aweme?.author?.sec_uid || '').trim() || null,
                timestamp: normalizeEpochSeconds(aweme?.create_time),
              });
              if (unique.size >= limit) break;
            }
          }
          douyinPrefetchedEntries = Array.from(unique.values());
          if (douyinPrefetchedEntries.length > 0) {
            logEvent('info', `Douyin mix supplemental fetched ${douyinPrefetchedEntries.length} videos`);
            markChannelHealthy();
          }
        }
      }

      if (specializedFeedEntries.length === 0) {
        const snapshot = await fetchDouyinUserSnapshot(douyinSecUid, job.abortSignal);
        douyinReportedVideoCount = snapshot.awemeCount;
        if (snapshot.title || snapshot.avatarUrl || snapshot.followerCount != null || snapshot.awemeCount != null) {
          db.prepare(`
            UPDATE channels
            SET title = COALESCE(?, title),
                handle = COALESCE(?, handle),
                avatar_url = COALESCE(?, avatar_url),
                subscriber_count = COALESCE(?, subscriber_count),
                source_url = COALESCE(?, source_url),
                last_sync_at = datetime('now')
            WHERE channel_id = ?
          `).run(
            snapshot.title,
            snapshot.uniqueId ? `@${snapshot.uniqueId}` : channel.handle,
            snapshot.avatarUrl,
            snapshot.followerCount,
            `https://www.douyin.com/user/${douyinSecUid}`,
            channelId,
          );

          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
            VALUES (?, ?, ?, NULL, ?)
            ON CONFLICT(date, channel_id) DO UPDATE SET
              subscriber_count = COALESCE(excluded.subscriber_count, channel_daily.subscriber_count),
              video_count = COALESCE(excluded.video_count, channel_daily.video_count)
          `).run(today, channelId, snapshot.followerCount, snapshot.awemeCount);

          if (snapshot.title) channel.title = snapshot.title;
          if (snapshot.avatarUrl) channel.avatar_url = snapshot.avatarUrl;
          if (snapshot.followerCount != null) channel.subscriber_count = snapshot.followerCount;
          if (snapshot.uniqueId) channel.handle = `@${snapshot.uniqueId}`;

          logEvent('info', `Douyin user profile updated${snapshot.mixIds.length > 0 ? ` (mixes=${snapshot.mixIds.length})` : ''}`);
          markChannelHealthy();
        } else {
          logEvent('warn', `Douyin user profile unavailable: ${snapshot.reason || 'unknown'}`);
        }

        if (snapshot.mixIds.length > 0 && limit > 0) {
          const unique = new Map<string, any>();
          for (const mixId of snapshot.mixIds) {
            if (unique.size >= limit) break;
            const pulled = await fetchDouyinMixEntries(douyinSecUid, mixId, limit - unique.size, job.abortSignal);
            if (pulled.reason && pulled.items.length === 0) {
              logEvent('warn', `Douyin mix ${mixId} fetch failed: ${pulled.reason}`);
              continue;
            }
            for (const aweme of pulled.items) {
              const awemeId = String(aweme?.aweme_id || '').trim();
              if (!awemeId || unique.has(awemeId)) continue;
              const isNote = isDouyinNoteEntry(aweme);
              const pageUrl = `https://www.douyin.com/${isNote ? 'note' : 'video'}/${awemeId}`;
              unique.set(awemeId, {
                ...aweme,
                id: awemeId,
                url: pageUrl,
                webpage_url: pageUrl,
                channel: String(aweme?.author?.nickname || '').trim() || null,
                uploader: String(aweme?.author?.nickname || '').trim() || null,
                uploader_id: String(aweme?.author?.unique_id || aweme?.author?.sec_uid || '').trim() || null,
                channel_id: String(aweme?.author?.uid || aweme?.author?.sec_uid || '').trim() || null,
                timestamp: normalizeEpochSeconds(aweme?.create_time),
              });
              if (unique.size >= limit) break;
            }
          }
          douyinPrefetchedEntries = Array.from(unique.values());
          if (douyinPrefetchedEntries.length > 0) {
            logEvent('info', `Douyin feeds fetched ${douyinPrefetchedEntries.length} videos from mix endpoints`);
            markChannelHealthy();
          } else {
            logEvent('warn', 'Douyin feeds returned no videos from mix endpoints');
          }
        }
      }
    }

    updateProgress(30);

    // Fetch recent videos from platform feed(s)
    const limit = parseInt(getSetting('recent_video_fetch_limit') || '50', 10);
    const feedTargets = buildChannelFeedTargets(platform, channelUrl);
    let feedResults: Array<{ target: ChannelFeedTarget; result: ytdlp.YtDlpResult }> = [];
    if (platform === 'douyin' && (specializedFeedEntries.length > 0 || douyinPrefetchedEntries.length > 0)) {
      const merged = new Map<string, any>();
      for (const entry of specializedFeedEntries) {
        const id = String(entry?.id || '').trim();
        if (!id || merged.has(id)) continue;
        merged.set(id, entry);
      }
      for (const raw of douyinPrefetchedEntries) {
        const normalized = normalizeTikTokDownloaderFeedEntry(platform, raw);
        const id = String(normalized?.id || '').trim();
        if (!id || merged.has(id)) continue;
        merged.set(id, normalized);
      }
      const mergedEntries = Array.from(merged.values()).slice(0, Math.max(1, limit));
      feedResults = [{
        target: feedTargets[0],
        result: {
          success: true,
          data: { entries: mergedEntries },
        },
      }];
    } else if ((platform === 'tiktok' || platform === 'douyin' || platform === 'xiaohongshu') && specializedFeedEntries.length > 0) {
      feedResults = [{
        target: feedTargets[0],
        result: {
          success: true,
          data: { entries: specializedFeedEntries },
        },
      }];
    } else if (platform === 'douyin' && douyinPrefetchedEntries.length > 0) {
      feedResults = [{
        target: feedTargets[0],
        result: {
          success: true,
          data: { entries: douyinPrefetchedEntries },
        },
      }];
    } else {
      feedResults = await Promise.all(feedTargets.map(async (target) => ({
        target,
        result: await ytdlp.getChannelVideos(target.url, limit, { abortSignal: job.abortSignal }),
      })));
    }

    const newVideoIds: string[] = [];
    const queuedAutoMetaVideoIds: string[] = [];
    const fetchedVideoIds = new Set<string>();
    const successfulFeedTypes = new Set<VideoContentType>();
    const mergedEntries = new Map<string, { entry: any; target: ChannelFeedTarget; rawVideoId: string }>();
    let inferredChannelTitle: string | null = null;
    let inferredChannelHandle: string | null = null;
    let inferredChannelSubscriberCount: number | null = null;

    for (const item of feedResults) {
      const { target, result } = item;
      const feedLabel = target.feedKey === 'main' ? '/main' : `/${target.feedKey}`;
      if (result.success && result.data) {
        const rawEntries = Array.isArray(result.data.entries) ? result.data.entries : [];
        successfulFeedTypes.add(target.inferredType);
        let acceptedCount = 0;
        for (const entry of rawEntries) {
          const rawVideoId = String(entry?.id || '').trim();
          if (!rawVideoId) continue;
          if (!isLikelyVideoEntry(platform, entry)) continue;
          const storageVideoId = composePlatformVideoId(platform, rawVideoId);
          acceptedCount += 1;

          if (target.feedKey === 'shorts' || target.feedKey === 'streams') {
            mergedEntries.set(storageVideoId, { entry, target, rawVideoId });
            continue;
          }
          if (!mergedEntries.has(storageVideoId)) {
            mergedEntries.set(storageVideoId, { entry, target, rawVideoId });
          }
        }
        logEvent('info', `Fetched ${acceptedCount}/${rawEntries.length} entries from ${feedLabel} feed`);
      } else {
        const feedErrorText = [result.error, result.log].filter(Boolean).join('\n');
        const isOptionalYoutubeTabMissing = (
          platform === 'youtube'
          && (target.feedKey === 'shorts' || target.feedKey === 'streams')
          && ytdlp.isYoutubeTabMissingError(feedErrorText)
        );
        if (isOptionalYoutubeTabMissing) {
          logEvent('info', `Skip ${feedLabel} feed: channel does not expose this tab`);
        } else {
          logEvent('warn', `Failed to fetch ${feedLabel} feed: ${result.error}`);
        }
        if (platform === 'youtube' && !isOptionalYoutubeTabMissing) {
          if (!hasConfirmedChannelAlive && ytdlp.isChannelUnavailableError(feedErrorText, result.errorCode)) {
            const reason = result.errorCode || 'channel_not_found';
            markChannelInvalid(reason);
            if (trackDeleted) {
              logEvent('warn', `Channel marked invalid by yt-dlp list check: ${reason}`);
            }
          }
        }
      }
    }

    const entries = Array.from(mergedEntries.values());
    if (platform === 'douyin' && entries.length === 0 && (douyinReportedVideoCount || 0) > 0) {
      throw new Error('抖音同步失败：未拉取到作品列表，请更新抖音 Cookie 或改用 sec_uid 主页链接后重试。');
    }
    if (entries.length > 0) {
      logEvent('info', `Found ${entries.length} unique videos from platform feeds`);
      markChannelHealthy();
    }

    for (let i = 0; i < entries.length; i++) {
      if (job.cancelled()) break;

      const { entry, target, rawVideoId } = entries[i];
      const storageVideoId = composePlatformVideoId(platform, rawVideoId);
      fetchedVideoIds.add(storageVideoId);

      const fallbackWebUrl = buildFallbackVideoUrl(platform, rawVideoId);
      const finalWebUrl = resolveEntryWebpageUrl(entry) || fallbackWebUrl || null;
      const generatedMetaPath = persistPlatformFeedMeta(channelId, storageVideoId, platform, entry, fallbackWebUrl || null);
      const publishedAt = resolveEntryPublishedAt(entry);
      const stats = resolveEntryStats(entry);
      const existing = db.prepare(`
        SELECT video_id, title, description, uploader, published_at, duration_sec,
          view_count, like_count, comment_count, collect_count, share_count,
          local_meta_path, local_thumb_path
        FROM videos
        WHERE video_id = ?
      `).get(storageVideoId) as any;
      const existingLocalMetaPath = resolveExistingPath(existing?.local_meta_path);
      const effectiveLocalMetaPath = existingLocalMetaPath || generatedMetaPath || null;
      let resolvedTitle = resolveEntryTitle(entry);
      let resolvedDescription: string | null = String(entry?.description || entry?.desc || '').trim() || null;
      let resolvedUploader: string | null = String(
        entry?.uploader
        || entry?.channel
        || entry?.author?.nickname
        || entry?.user?.nickname
        || '',
      ).trim() || null;
      let resolvedPublishedAt = publishedAt;
      let resolvedDurationSec = toNullableInt(entry?.duration);
      if (platform === 'douyin' && resolvedDurationSec != null && resolvedDurationSec > 1000) {
        resolvedDurationSec = Math.max(1, Math.round(resolvedDurationSec / 1000));
      }
      let resolvedViewCount = stats.viewCount;
      let resolvedLikeCount = stats.likeCount;
      let resolvedCommentCount = stats.commentCount;
      let resolvedCollectCount = stats.collectCount;
      let resolvedShareCount = stats.shareCount;
      let resolvedContentType: VideoContentType | null = null;
      let resolvedContentTypeSource: string | null = null;
      const localMetaForStats = readLocalVideoMeta(effectiveLocalMetaPath);
      if (localMetaForStats) {
        const localStats = resolveEntryStats(localMetaForStats);
        if (resolvedViewCount == null) resolvedViewCount = localStats.viewCount;
        if (resolvedLikeCount == null) resolvedLikeCount = localStats.likeCount;
        if (resolvedCommentCount == null) resolvedCommentCount = localStats.commentCount;
        if (resolvedCollectCount == null) resolvedCollectCount = localStats.collectCount;
        if (resolvedShareCount == null) resolvedShareCount = localStats.shareCount;
        try {
          const parsedLocalMeta = ytdlp.parseVideoMeta(localMetaForStats);
          const normalized = normalizeParsedContentTypeForStorage(platform, parsedLocalMeta.content_type);
          if (normalized) {
            resolvedContentType = normalized.type;
            resolvedContentTypeSource = String(parsedLocalMeta.content_type_source || normalized.source || 'meta').trim() || 'meta';
          }
        } catch {
          // Ignore local meta parse failures and continue with feed inference.
        }
      }

      // Douyin feed often returns play_count=0 while engagement stats are populated.
      // Treat this as unknown view count instead of an actual zero to avoid false resets.
      if (
        platform === 'douyin'
        && resolvedViewCount === 0
        && [resolvedLikeCount, resolvedCommentCount, resolvedCollectCount, resolvedShareCount]
          .some((value) => value != null && value > 0)
      ) {
        resolvedViewCount = null;
      }
      const hasEntryThumb = Boolean(extractEntryThumbnailUrl(entry));
      const hasExistingThumbPath = Boolean(String(existing?.local_thumb_path || '').trim());
      const hasExistingMetaThumb = Boolean(extractEntryThumbnailUrl(readLocalVideoMeta(effectiveLocalMetaPath)));
      const needsDouyinViewBackfill = (
        platform === 'douyin'
        && resolvedViewCount == null
        && toNullableInt(existing?.view_count) == null
      );
      const needsDouyinThumbBackfill = (
        platform === 'douyin'
        && !hasEntryThumb
        && !hasExistingThumbPath
        && !hasExistingMetaThumb
      );
      const hasLowVersionPlaceholder = (
        hasLowVersionPlaceholderText(resolvedTitle)
        || hasLowVersionPlaceholderText(resolvedDescription)
        || hasLowVersionPlaceholderText(existing?.title)
        || hasLowVersionPlaceholderText(existing?.description)
      );

      const needsMetadataEnrichment = (
        platform !== 'youtube'
        && !existing
        && !localMetaForStats
        && (
          hasLowVersionPlaceholder
          || resolvedTitle === 'Untitled'
          || resolvedPublishedAt == null
          || resolvedDurationSec == null
          || (resolvedViewCount == null && resolvedLikeCount == null && resolvedCommentCount == null)
          || needsDouyinViewBackfill
          || needsDouyinThumbBackfill
        )
      );
      if (needsMetadataEnrichment) {
        let detailInfo: any | null = readLocalVideoMeta(effectiveLocalMetaPath);
        let detailBridgeItem: any | null = null;
        if (
          hasLowVersionPlaceholderMeta(detailInfo)
          || shouldRefetchTikTokDownloaderDetailMeta(detailInfo, platform)
        ) {
          detailInfo = null;
        }
        const supportsTikTokDownloader = platform === 'tiktok' || platform === 'douyin';
        if (!detailInfo && supportsTikTokDownloader) {
          const cookieHeader = resolvePlatformCookieHeader(platform) || '';
          const detailInput = buildTikTokDownloaderDetailInput(platform, rawVideoId, finalWebUrl);
          if (detailInput) {
            const detailResult = await fetchTikTokDownloaderVideoDetail(
              platform,
              detailInput,
              cookieHeader,
              { abortSignal: job.abortSignal },
            );
            if (detailResult.ok && detailResult.item) {
              detailBridgeItem = detailResult.item;
              detailInfo = buildTikTokDownloaderVideoMeta(detailBridgeItem, platform);
            }
          }
        }
        if (!detailInfo && finalWebUrl && !supportsTikTokDownloader) {
          const infoResult = await ytdlp.getVideoInfo(finalWebUrl, { abortSignal: job.abortSignal });
          if (infoResult.success && infoResult.data) {
            detailInfo = infoResult.data;
          }
        }

        if (detailInfo) {
          const parsed = ytdlp.parseVideoMeta(detailInfo);
          resolvedTitle = String(parsed.title || resolvedTitle || 'Untitled').trim() || 'Untitled';
          resolvedDescription = typeof parsed.description === 'string' && parsed.description.trim()
            ? parsed.description.trim()
            : null;
          resolvedUploader = String(parsed.uploader || '').trim() || null;
          resolvedPublishedAt = parsed.published_at ?? resolvedPublishedAt;
          resolvedDurationSec = toNullableInt(parsed.duration_sec ?? resolvedDurationSec);
          resolvedViewCount = toNullableInt(parsed.view_count ?? resolvedViewCount);
          resolvedLikeCount = toNullableInt(parsed.like_count ?? resolvedLikeCount);
          resolvedCommentCount = toNullableInt(parsed.comment_count ?? resolvedCommentCount);
          resolvedCollectCount = toNullableInt(parsed.collect_count ?? resolvedCollectCount);
          resolvedShareCount = toNullableInt(parsed.share_count ?? resolvedShareCount);
          const normalized = normalizeParsedContentTypeForStorage(platform, parsed.content_type);
          if (normalized) {
            resolvedContentType = normalized.type;
            resolvedContentTypeSource = String(parsed.content_type_source || normalized.source || 'meta').trim() || 'meta';
          }

          if (platform === 'tiktok' || platform === 'douyin') {
            const writePath = effectiveLocalMetaPath || generatedMetaPath;
            if (writePath) {
              try {
                const persistMeta = detailBridgeItem
                  ? buildTikTokDownloaderVideoMeta(detailBridgeItem, platform)
                  : detailInfo;
                fs.mkdirSync(path.dirname(writePath), { recursive: true });
                fs.writeFileSync(writePath, JSON.stringify(persistMeta, null, 2), 'utf8');
              } catch {
                // Ignore local meta write failures and continue sync.
              }
            }
          }

          if (resolvedUploader && !inferredChannelTitle) {
            inferredChannelTitle = resolvedUploader;
          }
          const parsedChannelId = String(parsed.channel_id || detailInfo?.uploader_id || '').trim().replace(/^@+/, '');
          if (parsedChannelId && !inferredChannelHandle) {
            inferredChannelHandle = parsedChannelId;
          }
          const parsedSubscriberCount = toNullableInt(
            detailInfo?.channel_follower_count
            ?? detailInfo?.follower_count
            ?? detailInfo?.author?.follower_count
            ?? detailInfo?.raw?.author?.follower_count
            ?? detailInfo?.raw?.author?.fans
            ?? detailInfo?.user?.followers,
          );
          if (parsedSubscriberCount != null) {
            inferredChannelSubscriberCount = Math.max(inferredChannelSubscriberCount ?? 0, parsedSubscriberCount);
          }
        }
      }

      // Some Douyin feeds return placeholder zero stats; avoid overwriting real values with synthetic zeros.
      if (
        platform === 'douyin'
        && resolvedViewCount === 0
        && resolvedLikeCount === 0
        && resolvedCommentCount === 0
        && resolvedCollectCount === 0
        && resolvedShareCount === 0
      ) {
        const existingView = toNullableInt(existing?.view_count);
        const existingLike = toNullableInt(existing?.like_count);
        const existingComment = toNullableInt(existing?.comment_count);
        const existingCollect = toNullableInt(existing?.collect_count);
        const existingShare = toNullableInt(existing?.share_count);
        const hasExistingStats = [existingView, existingLike, existingComment, existingCollect, existingShare]
          .some((value) => value != null && value > 0);
        if (hasExistingStats) {
          resolvedViewCount = existingView;
          resolvedLikeCount = existingLike;
          resolvedCommentCount = existingComment;
          resolvedCollectCount = existingCollect;
          resolvedShareCount = existingShare;
        } else {
          resolvedViewCount = null;
          resolvedLikeCount = null;
          resolvedCommentCount = null;
          resolvedCollectCount = null;
          resolvedShareCount = null;
        }
      }

      const isUnresolvedPlaceholder = (
        resolvedTitle === 'Untitled'
        && resolvedDurationSec == null
        && resolvedViewCount == null
        && resolvedLikeCount == null
        && resolvedCommentCount == null
      );
      if (!existing && platform !== 'youtube' && isUnresolvedPlaceholder) {
        logEvent('warn', `Skipped unresolved placeholder video: ${storageVideoId}`);
        continue;
      }

      const inferred = inferEntryContentType(platform, target, entry, finalWebUrl);
      const inferredType = resolvedContentType ?? inferred.type;
      const inferredTypeSource = resolvedContentTypeSource ?? inferred.source;

      // Check if video exists
      if (!existing) {
        db.prepare(`
          INSERT OR IGNORE INTO videos (
            video_id, channel_id, platform, title, description, uploader, published_at, duration_sec,
            content_type, content_type_source, webpage_url, view_count, like_count, comment_count, collect_count, share_count
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          storageVideoId,
          channelId,
          platform,
          resolvedTitle,
          resolvedDescription,
          resolvedUploader,
          resolvedPublishedAt,
          resolvedDurationSec,
          inferredType,
          inferredTypeSource,
          finalWebUrl,
          resolvedViewCount,
          resolvedLikeCount,
          resolvedCommentCount,
          resolvedCollectCount,
          resolvedShareCount,
        );
        newVideoIds.push(storageVideoId);
        logEvent('info', `New video discovered: ${storageVideoId} - ${resolvedTitle}`);
      }

      db.prepare(`
        UPDATE videos
        SET platform = ?,
            title = COALESCE(?, title),
            description = CASE
              WHEN ? IS NULL OR trim(?) = '' THEN description
              WHEN length(trim(?)) >= length(trim(COALESCE(description, ''))) THEN ?
              ELSE description
            END,
            uploader = COALESCE(?, uploader),
            published_at = COALESCE(?, published_at),
            duration_sec = COALESCE(?, duration_sec),
            webpage_url = COALESCE(?, webpage_url),
            content_type = ?,
            content_type_source = ?,
            availability_status = 'available',
            unavailable_reason = NULL,
            unavailable_at = NULL
        WHERE video_id = ?
      `).run(
        platform,
        resolvedTitle,
        resolvedDescription,
        resolvedDescription,
        resolvedDescription,
        resolvedDescription,
        resolvedUploader,
        resolvedPublishedAt,
        resolvedDurationSec,
        finalWebUrl,
        inferredType,
        inferredTypeSource,
        storageVideoId
      );
      if (generatedMetaPath && !existingLocalMetaPath) {
        db.prepare(`UPDATE videos
          SET local_meta_path = ?,
              download_status = CASE
                WHEN download_status = 'none' THEN 'meta'
                WHEN instr(download_status, 'meta') > 0 THEN download_status
                ELSE download_status || ',meta'
              END
          WHERE video_id = ?`)
          .run(generatedMetaPath, storageVideoId);
      }
      if (trackDeleted) {
        markVideoAvailableArchiveResolved(db, storageVideoId);
      }

      if (
        resolvedViewCount != null
        || resolvedLikeCount != null
        || resolvedCommentCount != null
        || resolvedCollectCount != null
        || resolvedShareCount != null
      ) {
        db.prepare('UPDATE videos SET view_count = ?, like_count = ?, comment_count = ?, collect_count = ?, share_count = ? WHERE video_id = ?')
          .run(resolvedViewCount, resolvedLikeCount, resolvedCommentCount, resolvedCollectCount, resolvedShareCount, storageVideoId);
      }

      if (
        resolvedViewCount != null
        || resolvedLikeCount != null
        || resolvedCommentCount != null
        || resolvedCollectCount != null
        || resolvedShareCount != null
      ) {
        const today = new Date().toISOString().slice(0, 10);
        db.prepare(`
          INSERT OR REPLACE INTO video_daily (date, video_id, view_count, like_count, comment_count, collect_count, share_count)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(
          today,
          storageVideoId,
          resolvedViewCount,
          resolvedLikeCount,
          resolvedCommentCount,
          resolvedCollectCount,
          resolvedShareCount,
        );
      }

      updateProgress(30 + Math.floor(((i + 1) / Math.max(entries.length, 1)) * 60));
    }

    if (platform !== 'youtube') {
      if (isUnknownChannelTitle(channel?.title) && inferredChannelTitle) {
        db.prepare(`
          UPDATE channels
          SET title = ?, last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(inferredChannelTitle, channelId);
        channel.title = inferredChannelTitle;
        logEvent('info', `Channel title inferred from video metadata: ${inferredChannelTitle}`);
      }
      if ((!String(channel?.handle || '').trim()) && inferredChannelHandle) {
        db.prepare(`
          UPDATE channels
          SET handle = ?, last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(inferredChannelHandle, channelId);
        channel.handle = inferredChannelHandle;
      }
      if ((channel?.subscriber_count == null || Number(channel.subscriber_count) <= 0) && inferredChannelSubscriberCount != null) {
        db.prepare(`
          UPDATE channels
          SET subscriber_count = ?, last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(inferredChannelSubscriberCount, channelId);
        channel.subscriber_count = inferredChannelSubscriberCount;
      }

      const stalePlaceholders = db.prepare(`
        SELECT video_id
        FROM videos
        WHERE channel_id = ?
          AND title = 'Untitled'
          AND duration_sec IS NULL
          AND view_count IS NULL
          AND like_count IS NULL
          AND (local_meta_path IS NULL OR trim(local_meta_path) = '')
          AND (local_thumb_path IS NULL OR trim(local_thumb_path) = '')
      `).all(channelId) as Array<{ video_id: string }>;
      let pruned = 0;
      for (const row of stalePlaceholders) {
        if (fetchedVideoIds.has(row.video_id)) continue;
        db.prepare('DELETE FROM video_daily WHERE video_id = ?').run(row.video_id);
        db.prepare('DELETE FROM videos WHERE video_id = ?').run(row.video_id);
        pruned += 1;
      }
      if (pruned > 0) {
        logEvent('info', `Pruned ${pruned} stale placeholder videos`);
      }

      if (platform === 'douyin') {
        const deletedSparse = db.prepare(`
          DELETE FROM videos
          WHERE channel_id = ?
            AND title = 'Untitled'
            AND duration_sec IS NULL
            AND view_count IS NULL
            AND like_count IS NULL
            AND comment_count IS NULL
        `).run(channelId).changes || 0;
        if (deletedSparse > 0) {
          db.prepare(`
            DELETE FROM video_daily
            WHERE video_id NOT IN (SELECT video_id FROM videos)
          `).run();
          logEvent('info', `Pruned ${deletedSparse} sparse Douyin placeholders`);
        }
      }
    }

    if (trackDeleted && channelMarkedInvalid && !hasConfirmedChannelAlive) {
      const availableVideos = db.prepare(`
        SELECT video_id, title
        FROM videos
        WHERE channel_id = ? AND availability_status = 'available'
      `).all(channelId) as Array<{ video_id: string; title: string }>;
      if (availableVideos.length > 0) {
        logEvent('warn', `Channel marked invalid; marking ${availableVideos.length} videos unavailable`);
      }
      const today = new Date().toISOString().slice(0, 10);
      const reason = channelInvalidReason || 'channel_not_found';
      for (const row of availableVideos) {
        if (job.cancelled()) break;
        const rawMessage = `Channel marked invalid during sync (${reason})`;
        markVideoUnavailable(db, row.video_id, reason, rawMessage);
        db.prepare(`
          INSERT OR REPLACE INTO availability_log (date, video_id, status, reason, raw_message)
          VALUES (?, ?, 'unavailable', ?, ?)
        `).run(today, row.video_id, reason, rawMessage);
      }
    } else if (trackDeleted && entries.length > 0 && successfulFeedTypes.size > 0) {
      const hasLongFeed = successfulFeedTypes.has('long');
      const hasShortFeed = successfulFeedTypes.has('short');
      const hasLiveFeed = successfulFeedTypes.has('live');
      let recentKnownVideos: Array<{ video_id: string; title: string }> = [];
      if (hasLongFeed && hasShortFeed && hasLiveFeed) {
        recentKnownVideos = db.prepare(`
          SELECT video_id, title FROM videos
          WHERE channel_id = ? AND availability_status = 'available'
          ORDER BY COALESCE(published_at, created_at) DESC
          LIMIT ?
        `).all(channelId, limit) as Array<{ video_id: string; title: string }>;
      } else {
        const scopeFilters: string[] = [];
        if (hasLongFeed) {
          scopeFilters.push("(content_type IS NULL OR content_type = 'long')");
        }
        if (hasShortFeed) {
          scopeFilters.push("content_type = 'short'");
        }
        if (hasLiveFeed) {
          scopeFilters.push("content_type = 'live'");
        }
        if (scopeFilters.length > 0) {
          recentKnownVideos = db.prepare(`
          SELECT video_id, title FROM videos
          WHERE channel_id = ? AND availability_status = 'available' AND (${scopeFilters.join(' OR ')})
          ORDER BY COALESCE(published_at, created_at) DESC
          LIMIT ?
        `).all(channelId, limit) as Array<{ video_id: string; title: string }>;
        }
      }

      const missingVideos = recentKnownVideos.filter(v => !fetchedVideoIds.has(v.video_id));
      if (missingVideos.length > 0) {
        logEvent('info', `Checking ${missingVideos.length} videos missing from latest channel feed`);
      }

      const today = new Date().toISOString().slice(0, 10);
      const missingScope = hasLongFeed && hasShortFeed && hasLiveFeed
        ? '/videos+/shorts+/streams'
        : [
          hasLongFeed ? '/videos' : null,
          hasShortFeed ? '/shorts' : null,
          hasLiveFeed ? '/streams' : null,
        ].filter(Boolean).join('+');
      for (const missing of missingVideos) {
        if (job.cancelled()) break;

        const missingVideo = db.prepare('SELECT video_id, webpage_url FROM videos WHERE video_id = ?').get(missing.video_id) as any;
        const availability = await ytdlp.checkAvailability(missing.video_id, {
          abortSignal: job.abortSignal,
          sourceUrl: missingVideo?.webpage_url || undefined,
        });
        if (availability.available) {
          const reason = 'missing_from_channel_videos';
          const rawMessage = `Not present in latest channel ${missingScope} feed during sync`;
          db.prepare(`
            INSERT OR REPLACE INTO availability_log (date, video_id, status, reason, raw_message)
            VALUES (?, ?, 'available', ?, ?)
          `).run(today, missing.video_id, reason, rawMessage);
          logEvent('info', `Skipped unavailable mark: ${missing.title || missing.video_id} (${reason})`);
          continue;
        }

        const probeReason = normalizeAvailabilityReason(availability.reason || 'unknown') || 'unknown';
        const rawMessage = availability.rawMessage || null;
        const markReason = resolveUnavailableMarkReason(probeReason, rawMessage);
        if (!markReason) {
          db.prepare(`
            INSERT OR REPLACE INTO availability_log (date, video_id, status, reason, raw_message)
            VALUES (?, ?, 'probe_failed', ?, ?)
          `).run(today, missing.video_id, probeReason, rawMessage);
          logEvent(
            'warn',
            `Skipped unavailable mark (transient probe failure): ${missing.title || missing.video_id} (${probeReason})`,
          );
          continue;
        }
        markVideoUnavailable(db, missing.video_id, markReason, rawMessage);

        db.prepare(`
          INSERT OR REPLACE INTO availability_log (date, video_id, status, reason, raw_message)
          VALUES (?, ?, 'unavailable', ?, ?)
        `).run(today, missing.video_id, markReason, rawMessage);

        logEvent('warn', `Marked unavailable: ${missing.title || missing.video_id} (${markReason})`);
      }
    } else if (entries.length === 0 || successfulFeedTypes.size === 0) {
      logEvent('warn', 'No entries returned from channel feeds, skipped missing-video comparison');
    } else {
      logEvent('info', 'Non-YouTube platform: skipped unavailable/deleted monitoring');
    }

    // Auto-download meta+thumb for new videos based on setting
    if (newVideoIds.length > 0) {
      db.prepare(`
        UPDATE channels
        SET new_video_badge_count = COALESCE(new_video_badge_count, 0) + ?,
            new_video_badge_at = datetime('now')
        WHERE channel_id = ?
      `).run(newVideoIds.length, channelId);
      logEvent('info', `Detected ${newVideoIds.length} new videos, badge updated`);
    }

    const autoDownload = getSetting('auto_download_on_new_video') || 'meta+thumb';
    if (autoDownload !== 'none' && newVideoIds.length > 0) {
      if (platform === 'douyin') {
        logEvent('info', `Skip auto-download for Douyin (${newVideoIds.length} new videos) to avoid cookie-gated failures`);
      } else {
        const withItems: string[] = [];
        if (autoDownload.includes('meta')) withItems.push('meta');
        if (autoDownload.includes('thumb')) withItems.push('thumb');

        if (withItems.length > 0) {
          if (deferAutoDownload && deferredAutoDownloads) {
            logEvent('info', `Deferring auto-download (${withItems.join('+')}) for ${newVideoIds.length} new videos until daily sync completes`);
            for (const vid of newVideoIds) {
              deferredAutoDownloads.push({ video_id: vid, with: [...withItems] });
              if (withItems.includes('meta')) {
                queuedAutoMetaVideoIds.push(vid);
              }
            }
          } else {
            logEvent('info', `Queuing auto-download (${withItems.join('+')}) for ${newVideoIds.length} new videos`);
            for (const vid of newVideoIds) {
              const jobId = uuidv4();
              db.prepare(`
                INSERT INTO jobs (job_id, type, status, payload_json, created_at)
                VALUES (?, 'download_all', 'queued', ?, datetime('now'))
              `).run(jobId, JSON.stringify({ video_id: vid, with: withItems }));
              if (withItems.includes('meta')) {
                queuedAutoMetaVideoIds.push(vid);
              }
            }
          }
        }
      }
    }

    if (platform === 'youtube') {
      const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');
      const recentThumbTargets = db.prepare(`
        SELECT video_id, webpage_url
        FROM videos
        WHERE channel_id = ?
          AND lower(COALESCE(platform, 'youtube')) = 'youtube'
          AND COALESCE(availability_status, 'available') = 'available'
        ORDER BY datetime(COALESCE(published_at, created_at)) DESC
        LIMIT 3
      `).all(channelId) as Array<{ video_id: string; webpage_url: string | null }>;

      let refreshedThumbCount = 0;
      for (const target of recentThumbTargets) {
        if (job.cancelled()) break;
        const videoId = String(target?.video_id || '').trim();
        if (!videoId) continue;
        try {
          const thumbResult = await ytdlp.downloadThumb(videoId, channelId, {
            abortSignal: job.abortSignal,
            sourceUrl: String(target?.webpage_url || '').trim() || undefined,
          });
          if (!thumbResult.success) {
            logEvent('warn', `Thumbnail refresh failed for ${videoId}: ${thumbResult.error || 'unknown error'}`);
            continue;
          }
          const thumbPath = path.join(downloadRoot, 'assets', 'thumbs', channelId, videoId, `${videoId}.jpg`);
          if (fs.existsSync(thumbPath)) {
            db.prepare(`UPDATE videos SET local_thumb_path = ? WHERE video_id = ?`).run(thumbPath, videoId);
            refreshedThumbCount += 1;
          }
        } catch (error: any) {
          logEvent('warn', `Thumbnail refresh failed for ${videoId}: ${error?.message || error}`);
        }
      }
      if (refreshedThumbCount > 0) {
        logEvent('info', `Refreshed thumbnails for ${refreshedThumbCount} recent YouTube videos`);
      }
    }

    if (scheduleMetaRetryAudit && queuedAutoMetaVideoIds.length > 0) {
      const auditJobId = uuidv4();
      db.prepare(`
        INSERT INTO jobs (job_id, type, payload_json, status, parent_job_id)
        VALUES (?, 'channel_meta_retry_audit', ?, 'queued', ?)
      `).run(
        auditJobId,
        JSON.stringify({
          channel_id: channelId,
          video_ids: queuedAutoMetaVideoIds,
          max_wait_sec: 1800,
          poll_interval_ms: 3000,
          max_retry_count: 1,
        }),
        job.job_id || null,
      );
      logEvent('info', `Queued post-sync metadata retry audit for ${queuedAutoMetaVideoIds.length} videos`);
    }

    if (platform === 'douyin') {
      const enabledRaw = String(getSetting('douyin_playwright_view_sync_enabled') || 'true').trim().toLowerCase();
      const playwrightViewSyncEnabled = !['false', '0', 'off', 'no'].includes(enabledRaw);
      if (playwrightViewSyncEnabled) {
        const rawLimit = parseInt(String(getSetting('douyin_playwright_view_sync_limit') || '40'), 10);
        const syncLimit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(rawLimit, 500)) : 40;
        const timeoutRaw = parseInt(String(getSetting('douyin_playwright_timeout_ms') || '22000'), 10);
        const timeoutMs = Number.isFinite(timeoutRaw) ? Math.max(5_000, Math.min(timeoutRaw, 90_000)) : 22_000;
        const delayRaw = parseInt(String(getSetting('douyin_playwright_delay_ms') || '800'), 10);
        const delayMs = Number.isFinite(delayRaw) ? Math.max(300, Math.min(delayRaw, 12_000)) : 800;
        const headless = getPlaywrightHeadlessEnabled('douyin');
        const usePersistentSession = getPlaywrightSessionEnabled();

        const pendingViewRows = db.prepare(`
          SELECT
            video_id,
            webpage_url,
            view_count,
            like_count,
            comment_count,
            collect_count,
            share_count
          FROM videos
          WHERE channel_id = ?
            AND availability_status = 'available'
            AND (content_type IS NULL OR content_type IN ('long', 'short', 'album', 'note'))
            AND (view_count IS NULL OR view_count <= 0)
          ORDER BY COALESCE(published_at, created_at) DESC
          LIMIT ?
        `).all(channelId, syncLimit) as Array<{
          video_id: string;
          webpage_url: string | null;
          view_count: number | null;
          like_count: number | null;
          comment_count: number | null;
          collect_count: number | null;
          share_count: number | null;
        }>;

        if (pendingViewRows.length > 0) {
          const channelScanUrl = String(channel?.source_url || '').trim() || channelUrl;
          logEvent('info', `Douyin Playwright channel scan: pending ${pendingViewRows.length}, max ${syncLimit}`);
          const cookieHeader = resolvePlatformCookieHeader(platform) || '';
          const maxScanAttempts = 3;
          let batch = await fetchDouyinChannelCardStatsByPlaywright(
            channelScanUrl,
            {
              cookieHeader,
              headless,
              usePersistentSession,
              timeoutMs,
              delayMs,
              maxItems: syncLimit,
              abortSignal: job.abortSignal,
            },
          );
          for (let attempt = 2; attempt <= maxScanAttempts; attempt++) {
            if (job.abortSignal?.aborted) break;
            if (batch.scanned > 0) break;
            const waitMs = Math.max(600, Math.min(5000, delayMs + (attempt - 1) * 700));
            logEvent('warn', `Douyin Playwright channel scan empty on attempt ${attempt - 1}, retrying (${attempt}/${maxScanAttempts})`);
            await new Promise((resolve) => setTimeout(resolve, waitMs));
            batch = await fetchDouyinChannelCardStatsByPlaywright(
              channelScanUrl,
              {
                cookieHeader,
                headless,
                usePersistentSession,
                timeoutMs,
                delayMs: waitMs,
                maxItems: syncLimit,
                abortSignal: job.abortSignal,
              },
            );
          }

          if (!batch.ok && batch.error) {
            logEvent('warn', `Douyin Playwright channel scan unavailable: ${batch.error}`);
          }

          logEvent(
            'info',
            `Douyin Playwright channel scan stop: ${batch.stopReason || 'unknown'}, rounds ${batch.rounds || 0}, extracted ${batch.scanned}, session ${batch.sessionMode || 'ephemeral'}`,
          );
          if (batch.stopReason === 'login_required') {
            logEvent('warn', 'Douyin Playwright detected login panel and could not continue collecting cards');
          }

          const currentByVideoId = new Map<string, {
            viewCount: number | null;
            likeCount: number | null;
            commentCount: number | null;
            collectCount: number | null;
            shareCount: number | null;
          }>();
          for (const row of pendingViewRows) {
            currentByVideoId.set(String(row.video_id || '').trim(), {
              viewCount: toNullableInt(row.view_count),
              likeCount: toNullableInt(row.like_count),
              commentCount: toNullableInt(row.comment_count),
              collectCount: toNullableInt(row.collect_count),
              shareCount: toNullableInt(row.share_count),
            });
          }

          const today = new Date().toISOString().slice(0, 10);
          let updatedCount = 0;
          let positiveViewCount = 0;
          let notMatchedCount = 0;
          let failedCount = 0;

          for (const item of batch.results) {
            const storageVideoId = composePlatformVideoId('douyin', item.videoId);
            const current = currentByVideoId.get(storageVideoId);
            if (!current) {
              notMatchedCount += 1;
              continue;
            }

            const incomingView = toNullableInt(item.viewCount);
            if (incomingView == null || incomingView <= 0) {
              failedCount += 1;
              continue;
            }

            const nextView = Math.max(current.viewCount ?? 0, incomingView);
            if (nextView === current.viewCount) continue;

            db.prepare(`
              UPDATE videos
              SET view_count = ?
              WHERE video_id = ?
            `).run(nextView, storageVideoId);

            db.prepare(`
              INSERT OR REPLACE INTO video_daily (date, video_id, view_count, like_count, comment_count, collect_count, share_count)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            `).run(
              today,
              storageVideoId,
              nextView,
              current.likeCount,
              current.commentCount,
              current.collectCount,
              current.shareCount,
            );

            updatedCount += 1;
            positiveViewCount += 1;
          }

          logEvent(
            'info',
            `Douyin Playwright view sync: updated ${updatedCount}/${pendingViewRows.length}, positive views ${positiveViewCount}, extracted ${batch.scanned}, unmatched ${notMatchedCount}, failed ${failedCount}`,
          );
        }
      }
    }

    // Aggregation: ensure channel stats reflect video data
    try {
      const stats = db.prepare(`
        SELECT
          count(*) as count,
          sum(CASE WHEN view_count > 0 THEN view_count ELSE 0 END) as total_views_positive,
          sum(CASE WHEN view_count IS NOT NULL AND view_count > 0 THEN 1 ELSE 0 END) as view_samples
        FROM videos
        WHERE channel_id = ?
      `).get(channelId) as any;
      if (stats) {
        const viewSamples = Number(stats.view_samples || 0);
        const totalViews = viewSamples > 0
          ? Number(stats.total_views_positive || 0)
          : null;
        const totalVideos = Number(stats.count || 0);
        const aggregateVideoCount = (
          platform === 'douyin' && douyinReportedVideoCount != null
            ? Math.max(totalVideos, Number(douyinReportedVideoCount || 0))
            : totalVideos
        );
        const channelSnapshotBeforeUpdate = db.prepare("SELECT subscriber_count, video_count FROM channels WHERE channel_id = ?").get(channelId) as any;
        const currentChannelVideoCount = toNullableInt(channelSnapshotBeforeUpdate?.video_count);
        // Keep channel-level "total video count" from metadata/API when available.
        // Aggregated table count is only used as fallback or lower-bound, not as overwrite.
        const effectiveVideoCount = currentChannelVideoCount != null
          ? Math.max(currentChannelVideoCount, aggregateVideoCount)
          : aggregateVideoCount;

        db.prepare("UPDATE channels SET video_count = ?, view_count = COALESCE(?, view_count) WHERE channel_id = ?")
          .run(effectiveVideoCount, totalViews, channelId);

        const channelSnapshot = db.prepare("SELECT subscriber_count FROM channels WHERE channel_id = ?").get(channelId) as any;
        const today = new Date().toISOString().slice(0, 10);
        db.prepare(`
          INSERT INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(date, channel_id) DO UPDATE SET
            subscriber_count = COALESCE(channel_daily.subscriber_count, excluded.subscriber_count),
            view_count_total = COALESCE(channel_daily.view_count_total, excluded.view_count_total),
            video_count = COALESCE(channel_daily.video_count, excluded.video_count)
        `).run(today, channelId, channelSnapshot?.subscriber_count ?? null, totalViews, effectiveVideoCount);

        logEvent(
          'info',
          `Synched aggregates: channel videos ${effectiveVideoCount} (tracked ${aggregateVideoCount}), ${totalViews == null ? 'N/A' : totalViews} total views`,
        );
      }
    } catch (e: any) {
      logEvent('error', `Failed to update aggregates: ${e.message}`);
    }

    try {
      const growth = writeChannelViewGrowthCache(db, channelId);
      logEvent(
        'info',
        `Cached channel 7d growth: ${growth.channel_view_increase_7d == null ? 'N/A' : growth.channel_view_increase_7d}`,
      );
    } catch (e: any) {
      logEvent('error', `Failed to cache channel growth: ${e.message}`);
    }

    updateProgress(100);
  }

  private async handleDailySync(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const channels = db.prepare(`
      SELECT * FROM channels
      WHERE lower(COALESCE(platform, 'youtube')) IN ('youtube', 'bilibili', 'tiktok', 'douyin', 'xiaohongshu')
      ORDER BY priority DESC, last_sync_at ASC
    `).all() as any[];

    logEvent('info', `Daily sync started for ${channels.length} channels`);
    let okCount = 0;
    let failCount = 0;
    const deferredAutoDownloads: Array<{ video_id: string; with: string[] }> = [];

    for (let i = 0; i < channels.length; i++) {
      if (job.cancelled()) break;

      try {
        logEvent('info', `Syncing channel ${i + 1}/${channels.length}: ${channels[i].title}`);

        // Create a sub-sync for this channel
        const syncJob = {
          payload: {
            channel_id: channels[i].channel_id,
            defer_auto_download: true,
            deferred_auto_downloads: deferredAutoDownloads,
          },
          cancelled: job.cancelled,
        };
        await this.handleSyncChannel(syncJob, logEvent, () => {});
        okCount++;
      } catch (err: any) {
        failCount++;
        logEvent('error', `Failed to sync ${channels[i].title}: ${err.message}`);
      }

      const ratio = channels.length > 0 ? ((i + 1) / channels.length) : 1;
      updateProgress(Math.floor(ratio * 70));
    }

    if (!job.cancelled()) {
      await this.refreshResearchChannelsDaily(job, logEvent, updateProgress, 70, 15);
    }
    if (!job.cancelled()) {
      await this.autoAddYoutubeBreakoutsToHitLibrary(job, logEvent, updateProgress, 85, 7);
    }
    if (!job.cancelled()) {
      try {
        const autoCollect = autoCollectYoutubeHitVideos(db);
        logEvent(
          'info',
          `Hit library auto-collect (absolute views) finished: qualified ${autoCollect.qualified}, inserted ${autoCollect.inserted}, updated ${autoCollect.updated}, long ${autoCollect.long_videos}, shorts ${autoCollect.shorts}`,
        );
      } catch (err: any) {
        logEvent('error', `Hit library auto-collect failed: ${String(err?.message || err || 'unknown')}`);
      }
      updateProgress(92);
    }
    if (!job.cancelled()) {
      await this.refreshHitVideosDaily(job, logEvent, updateProgress, 92, 8);
    }

    if (!job.cancelled() && deferredAutoDownloads.length > 0) {
      const mergedDownloads = new Map<string, Set<string>>();
      for (const item of deferredAutoDownloads) {
        const videoId = String(item?.video_id || '').trim();
        if (!videoId) continue;
        const current = mergedDownloads.get(videoId) || new Set<string>();
        for (const part of Array.isArray(item?.with) ? item.with : []) {
          const label = String(part || '').trim();
          if (label) current.add(label);
        }
        if (current.size > 0) {
          mergedDownloads.set(videoId, current);
        }
      }

      let enqueuedCount = 0;
      for (const [videoId, withSet] of mergedDownloads.entries()) {
        const withItems = Array.from(withSet);
        if (withItems.length === 0) continue;
        const jobId = uuidv4();
        db.prepare(`
          INSERT INTO jobs (job_id, type, status, payload_json, created_at)
          VALUES (?, 'download_all', 'queued', ?, datetime('now'))
        `).run(jobId, JSON.stringify({ video_id: videoId, with: withItems }));
        enqueuedCount += 1;
      }

      logEvent(
        'info',
        `Queued deferred auto-download jobs after daily sync: ${enqueuedCount} videos (${deferredAutoDownloads.length} raw requests)`,
      );
    }

    logEvent('info', `Daily sync complete. Success: ${okCount}, Failed: ${failCount}`);
    updateProgress(100);
  }

  private async handleSyncReportingChannel(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const bindingId = String(job?.payload?.binding_id || '').trim();
    if (!bindingId) {
      throw new Error('binding_id is required');
    }

    logEvent('info', `Starting reporting sync for binding ${bindingId}`);
    updateProgress(10);
    const result = await syncReportingBinding(bindingId);
    updateProgress(90);
    logEvent(
      'info',
      `Reporting sync complete for ${result.channel_id}: downloaded ${result.downloaded_reports} reports, derived ${result.derived_rows} rows`,
    );
    updateProgress(100);
  }

  private async handleAvailabilityCheck(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const refreshDays = parseInt(getSetting('refresh_window_days') || '30', 10);
    const startDate = new Date(Date.now() - refreshDays * 86400000).toISOString().slice(0, 10);

    const videos = db.prepare(`
      SELECT video_id, title FROM videos
      WHERE published_at >= ? AND availability_status = 'available'
      ORDER BY published_at DESC
    `).all(startDate) as any[];

    logEvent('info', `Checking availability for ${videos.length} videos`);
    const today = new Date().toISOString().slice(0, 10);

    for (let i = 0; i < videos.length; i++) {
      if (job.cancelled()) break;

      const result = await ytdlp.checkAvailability(videos[i].video_id, { abortSignal: job.abortSignal });
      const probeReason = normalizeAvailabilityReason(result.reason || 'unknown') || 'unknown';
      const markReason = resolveUnavailableMarkReason(probeReason, result.rawMessage || null);

      db.prepare(`
        INSERT OR REPLACE INTO availability_log (date, video_id, status, reason, raw_message)
        VALUES (?, ?, ?, ?, ?)
      `).run(
        today,
        videos[i].video_id,
        result.available ? 'available' : (markReason ? 'unavailable' : 'probe_failed'),
        result.available ? null : probeReason,
        result.rawMessage || null
      );

      if (!result.available && markReason) {
        markVideoUnavailable(db, videos[i].video_id, markReason, result.rawMessage || null);
        logEvent('warn', `Video unavailable: ${videos[i].title} (${markReason})`);
      } else if (!result.available) {
        logEvent('warn', `Skipped unavailable mark (transient probe failure): ${videos[i].title} (${probeReason})`);
      }

      updateProgress(Math.floor(((i + 1) / videos.length) * 100));
    }

    logEvent('info', 'Availability check complete');
  }

  private async handleMetadataRepair(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const payload = job?.payload || {};
    const maxQueueRaw = Math.trunc(Number(payload.max_queue || 0) || 0);
    const maxQueue = maxQueueRaw > 0 ? Math.min(50000, maxQueueRaw) : 0;
    const maxRetryRaw = Math.trunc(Number(payload.max_retry_count || 2) || 2);
    const maxRetryCount = Math.max(1, Math.min(5, maxRetryRaw));

    const videos = db.prepare(`
      SELECT v.video_id, v.title, v.local_meta_path
      FROM videos v
      INNER JOIN channels c ON c.channel_id = v.channel_id
      WHERE lower(COALESCE(v.availability_status, 'available')) <> 'unavailable'
      ORDER BY datetime(COALESCE(v.published_at, v.created_at)) DESC, v.created_at DESC
    `).all() as Array<{
      video_id: string;
      title: string | null;
      local_meta_path: string | null;
    }>;

    if (videos.length === 0) {
      logEvent('info', 'Metadata repair skipped: no channel videos found');
      updateProgress(100);
      return;
    }

    const activeMetaRows = db.prepare(`
      SELECT DISTINCT json_extract(payload_json, '$.video_id') AS video_id
      FROM jobs
      WHERE type IN ('download_meta', 'download_all')
        AND status IN ('queued', 'running', 'canceling')
        AND json_extract(payload_json, '$.video_id') IS NOT NULL
    `).all() as Array<{ video_id: string | null }>;
    const activeMetaVideoIds = new Set<string>(
      activeMetaRows
        .map((row) => String(row?.video_id || '').trim())
        .filter(Boolean),
    );

    const missingMetaVideos = videos.filter((row) => {
      const videoId = String(row?.video_id || '').trim();
      if (!videoId) return false;
      return !resolveExistingPath(row?.local_meta_path);
    });
    const queueableVideos = missingMetaVideos.filter((row) => {
      const videoId = String(row?.video_id || '').trim();
      if (!videoId) return false;
      return !activeMetaVideoIds.has(videoId);
    });
    const targetVideos = maxQueue > 0 ? queueableVideos.slice(0, maxQueue) : queueableVideos;

    logEvent(
      'info',
      `Metadata repair scan complete: total ${videos.length}, missing ${missingMetaVideos.length}, already queued ${Math.max(0, missingMetaVideos.length - queueableVideos.length)}, to queue ${targetVideos.length}${maxQueue > 0 ? ` (limit ${maxQueue})` : ''}`,
    );

    if (targetVideos.length === 0) {
      updateProgress(100);
      return;
    }

    updateProgress(10);
    let queued = 0;
    for (let i = 0; i < targetVideos.length; i += 1) {
      if (job.cancelled()) break;

      const row = targetVideos[i];
      const videoId = String(row?.video_id || '').trim();
      if (!videoId) continue;

      const childJobId = uuidv4();
      const childPayload = {
        video_id: videoId,
        force: true,
        auto_meta_retry: true,
        auto_meta_retry_count: maxRetryCount,
        source: 'metadata_repair',
      };
      db.prepare(`
        INSERT INTO jobs (job_id, type, payload_json, status, parent_job_id)
        VALUES (?, 'download_meta', ?, 'queued', ?)
      `).run(childJobId, JSON.stringify(childPayload), job.job_id || null);
      queued += 1;

      const ratio = targetVideos.length > 0 ? (i + 1) / targetVideos.length : 1;
      updateProgress(Math.max(10, Math.min(95, 10 + Math.floor(ratio * 85))));
    }

    if (job.cancelled()) {
      logEvent('warn', `Metadata repair cancelled: queued ${queued}/${targetVideos.length}`);
      updateProgress(100);
      return;
    }

    const postponedByLimit = Math.max(0, queueableVideos.length - targetVideos.length);
    logEvent(
      'info',
      `Metadata repair queued ${queued} download jobs${postponedByLimit > 0 ? `, postponed ${postponedByLimit} by limit` : ''}`,
    );
    updateProgress(100);
  }

  private async handleDownloadMeta(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const { video_id } = job.payload;
    const forceRedownload = Boolean(job?.payload?.force);

    const video = db.prepare('SELECT * FROM videos WHERE video_id = ?').get(video_id) as any;
    if (!video) throw new Error(`Video not found: ${video_id}`);

    const platform = normalizeChannelPlatform(video?.platform);
    const root = getSetting('download_root') || path.join(process.cwd(), 'downloads');
    const metaPath = path.join(root, 'assets', 'meta', video.channel_id, video_id, `${video_id}.info.json`);

    logEvent('info', `Downloading metadata for ${video_id}${forceRedownload ? ' (force)' : ''}`);
    if (platform === 'tiktok' || platform === 'douyin') {
      const rawId = stripPlatformVideoPrefix(platform, video_id);
      const cookieHeader = resolvePlatformCookieHeader(platform) || '';
      const detailInput = buildTikTokDownloaderDetailInput(platform, rawId, String(video?.webpage_url || '').trim() || null);
      if (detailInput) {
        const detailResult = await fetchTikTokDownloaderVideoDetail(
          platform,
          detailInput,
          cookieHeader,
          { abortSignal: job.abortSignal },
        );
        if (detailResult.ok && detailResult.item) {
          const normalizedMeta = buildTikTokDownloaderVideoMeta(detailResult.item, platform);
          const dir = path.dirname(metaPath);
          if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
          }
          fs.writeFileSync(metaPath, JSON.stringify(normalizedMeta, null, 2), 'utf8');

          db.prepare(`UPDATE videos SET local_meta_path = ?, download_status = CASE
            WHEN download_status = 'none' THEN 'meta'
            ELSE download_status || ',meta'
          END WHERE video_id = ?`).run(metaPath, video_id);

          const parsed = ytdlp.parseVideoMeta(normalizedMeta);
          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT OR REPLACE INTO video_daily (date, video_id, view_count, like_count, comment_count, collect_count, share_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).run(
            today,
            video_id,
            parsed.view_count,
            parsed.like_count,
            parsed.comment_count,
            parsed.collect_count,
            parsed.share_count,
          );

          db.prepare(`UPDATE videos SET title = ?, description = ?, duration_sec = ?,
            content_type = ?, content_type_source = ?, published_at = ?, webpage_url = COALESCE(?, webpage_url),
            view_count = COALESCE(?, view_count), like_count = COALESCE(?, like_count),
            comment_count = COALESCE(?, comment_count), collect_count = COALESCE(?, collect_count),
            share_count = COALESCE(?, share_count)
            WHERE video_id = ?`)
            .run(
              parsed.title,
              parsed.description,
              parsed.duration_sec,
              parsed.content_type,
              parsed.content_type_source,
              parsed.published_at,
              parsed.webpage_url,
              parsed.view_count,
              parsed.like_count,
              parsed.comment_count,
              parsed.collect_count,
              parsed.share_count,
              video_id,
            );
          logEvent('info', 'Metadata downloaded via TikTokDownloader');
          updateProgress(100);
          return;
        }
        logEvent('warn', `TikTokDownloader metadata fetch failed, fallback to yt-dlp: ${detailResult.error || 'unknown'}`);
      }
    }
    if (platform === 'xiaohongshu') {
      const existingMeta = readLocalVideoMeta(video?.local_meta_path) || readLocalVideoMeta(metaPath);
      const hasXhsCoreStats = Boolean(
        existingMeta
        && (
          existingMeta?.liked_count != null
          || existingMeta?.like_count != null
          || existingMeta?.collected_count != null
          || existingMeta?.collect_count != null
          || existingMeta?.interact_info?.liked_count != null
          || existingMeta?.note_card?.interact_info?.liked_count != null
        )
      );
      if (hasXhsCoreStats && !forceRedownload) {
        const existingPath = resolveExistingPath(video?.local_meta_path);
        const chosenMetaPath = existingPath || metaPath;
        if (!existingPath) {
          const dir = path.dirname(metaPath);
          if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
          }
          fs.writeFileSync(metaPath, JSON.stringify(existingMeta, null, 2), 'utf8');
        }

        db.prepare(`UPDATE videos SET local_meta_path = ?, download_status = CASE
          WHEN download_status = 'none' THEN 'meta'
          WHEN instr(download_status, 'meta') > 0 THEN download_status
          ELSE download_status || ',meta'
        END WHERE video_id = ?`).run(chosenMetaPath, video_id);

        const parsed = ytdlp.parseVideoMeta(existingMeta);
        const today = new Date().toISOString().slice(0, 10);
        db.prepare(`
          INSERT OR REPLACE INTO video_daily (date, video_id, view_count, like_count, comment_count, collect_count, share_count)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(
          today,
          video_id,
          parsed.view_count,
          parsed.like_count,
          parsed.comment_count,
          parsed.collect_count,
          parsed.share_count,
        );

        db.prepare(`UPDATE videos SET title = ?, description = ?, duration_sec = ?,
          content_type = ?, content_type_source = ?, published_at = ?, webpage_url = COALESCE(?, webpage_url),
          view_count = COALESCE(?, view_count), like_count = COALESCE(?, like_count),
          comment_count = COALESCE(?, comment_count), collect_count = COALESCE(?, collect_count),
          share_count = COALESCE(?, share_count)
          WHERE video_id = ?`)
          .run(
            parsed.title,
            parsed.description,
            parsed.duration_sec,
            parsed.content_type,
            parsed.content_type_source,
            parsed.published_at,
            parsed.webpage_url,
            parsed.view_count,
            parsed.like_count,
            parsed.comment_count,
            parsed.collect_count,
            parsed.share_count,
            video_id,
          );

        logEvent('info', 'Metadata reused from Spider_XHS local meta');
        updateProgress(100);
        return;
      }
    }

    const result = await ytdlp.downloadMeta(video_id, video.channel_id, {
      abortSignal: job.abortSignal,
      sourceUrl: video.webpage_url || undefined,
      forceOverwrite: forceRedownload,
    });

    if (result.success) {

      db.prepare(`UPDATE videos SET local_meta_path = ?, download_status = CASE
        WHEN download_status = 'none' THEN 'meta'
        ELSE download_status || ',meta'
      END WHERE video_id = ?`).run(metaPath, video_id);

      // Parse and update video_daily from the info.json
      if (fs.existsSync(metaPath)) {
        try {
          const info = JSON.parse(fs.readFileSync(metaPath, 'utf-8'));
          const meta = ytdlp.parseVideoMeta(info);
          const today = new Date().toISOString().slice(0, 10);

          db.prepare(`
            INSERT OR REPLACE INTO video_daily (date, video_id, view_count, like_count, comment_count, collect_count, share_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).run(
            today,
            video_id,
            meta.view_count,
            meta.like_count,
            meta.comment_count,
            meta.collect_count,
            meta.share_count,
          );

          // Update video title/description if newer
          db.prepare(`UPDATE videos SET title = ?, description = ?, duration_sec = ?,
            content_type = ?, content_type_source = ?, published_at = ?,
            view_count = COALESCE(?, view_count), like_count = COALESCE(?, like_count),
            comment_count = COALESCE(?, comment_count), collect_count = COALESCE(?, collect_count),
            share_count = COALESCE(?, share_count)
            WHERE video_id = ?`)
            .run(meta.title, meta.description, meta.duration_sec,
              meta.content_type, meta.content_type_source, meta.published_at,
              meta.view_count, meta.like_count, meta.comment_count, meta.collect_count, meta.share_count,
              video_id);
        } catch {}
      }

      logEvent('info', 'Metadata downloaded');
    } else {
      const isDouyinVideo = String(video?.platform || '').trim().toLowerCase() === 'douyin'
        || String(video?.video_id || '').toLowerCase().startsWith('douyin__');
      if (isDouyinVideo && isDouyinFreshCookieError(result.error)) {
        try {
          const dir = path.dirname(metaPath);
          if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
          }
          const synthetic = buildSyntheticDouyinMetaFromVideo(video);
          fs.writeFileSync(metaPath, JSON.stringify(synthetic, null, 2), 'utf8');
          db.prepare(`UPDATE videos SET local_meta_path = ?, download_status = CASE
            WHEN download_status = 'none' THEN 'meta'
            ELSE download_status || ',meta'
          END WHERE video_id = ?`).run(metaPath, video_id);

          const today = new Date().toISOString().slice(0, 10);
          db.prepare(`
            INSERT OR REPLACE INTO video_daily (date, video_id, view_count, like_count, comment_count, collect_count, share_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).run(
            today,
            video_id,
            toNullableInt(video?.view_count),
            toNullableInt(video?.like_count),
            null,
            toNullableInt(video?.collect_count),
            toNullableInt(video?.share_count),
          );

          logEvent('warn', 'Douyin metadata blocked by remote; wrote synthetic local metadata instead');
          updateProgress(100);
          return;
        } catch (err: any) {
          throw new Error(`Failed to persist synthetic metadata: ${err?.message || 'unknown'}`);
        }
      }
      throw new Error(`Failed to download metadata: ${result.error}`);
    }
    updateProgress(100);
  }

  private async handleDownloadThumb(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const { video_id } = job.payload;

    const video = db.prepare('SELECT * FROM videos WHERE video_id = ?').get(video_id) as any;
    if (!video) throw new Error(`Video not found: ${video_id}`);

    logEvent('info', `Downloading thumbnail for ${video_id}`);
    const result = await ytdlp.downloadThumb(video_id, video.channel_id, {
      abortSignal: job.abortSignal,
      sourceUrl: video.webpage_url || undefined,
    });

    if (result.success) {
      const root = getSetting('download_root') || path.join(process.cwd(), 'downloads');
      const thumbPath = path.join(root, 'assets', 'thumbs', video.channel_id, video_id, `${video_id}.jpg`);

      db.prepare(`UPDATE videos SET local_thumb_path = ? WHERE video_id = ?`).run(thumbPath, video_id);
      logEvent('info', 'Thumbnail downloaded');
    } else {
      throw new Error(`Failed to download thumbnail: ${result.error}`);
    }
    updateProgress(100);
  }

  private async handleDownloadSubs(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const { video_id } = job.payload;

    const video = db.prepare('SELECT * FROM videos WHERE video_id = ?').get(video_id) as any;
    if (!video) throw new Error(`Video not found: ${video_id}`);

    logEvent('info', `Downloading subtitles for ${video_id}`);
    const result = await ytdlp.downloadSubs(video_id, video.channel_id, {
      abortSignal: job.abortSignal,
      sourceUrl: video.webpage_url || undefined,
    });

    if (result.success) {
      const root = getSetting('download_root') || path.join(process.cwd(), 'downloads');
      const subsDir = path.join(root, 'assets', 'subs', video.channel_id, video_id);

      // Find subtitle files
      const subFiles: string[] = [];
      if (fs.existsSync(subsDir)) {
        const files = fs.readdirSync(subsDir).filter(f => f.endsWith('.vtt'));
        subFiles.push(...files.map(f => path.join(subsDir, f)));
      }

      db.prepare(`UPDATE videos SET local_subtitle_paths = ? WHERE video_id = ?`)
        .run(JSON.stringify(subFiles), video_id);
      logEvent('info', `Subtitles downloaded: ${subFiles.length} files`);
    } else {
      logEvent('warn', `No subtitles available: ${result.error}`);
    }
    updateProgress(100);
  }

  private async handleDownloadVideo(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const { video_id } = job.payload;

    const video = db.prepare('SELECT * FROM videos WHERE video_id = ?').get(video_id) as any;
    if (!video) throw new Error(`Video not found: ${video_id}`);

    logEvent('info', `Downloading video ${video_id}`);
    const result = await ytdlp.downloadVideo(video_id, video.channel_id, (progress, msg) => {
      updateProgress(Math.floor(progress));
      if (Math.floor(progress) % 10 === 0) {
        logEvent('info', `Download progress: ${Math.floor(progress)}%`);
      }
    }, {
      abortSignal: job.abortSignal,
      sourceUrl: video.webpage_url || undefined,
    });

    if (result.success) {
      const root = getSetting('download_root') || path.join(process.cwd(), 'downloads');
      const videoDir = path.join(root, 'assets', 'videos', video.channel_id, video_id);

      // Prefer engine-captured output path; fallback to directory scan.
      let videoPath = result.outputPath || '';
      if (!videoPath || !fs.existsSync(videoPath)) {
        videoPath = '';
        if (fs.existsSync(videoDir)) {
          const files = fs.readdirSync(videoDir).filter(f => /\.(mp4|mkv|webm)$/i.test(f));
          if (files.length > 0) {
            videoPath = path.join(videoDir, files[0]);
          }
        }
      }

      db.prepare(`UPDATE videos SET local_video_path = ?, download_status = CASE
        WHEN download_status = 'none' THEN 'video'
        ELSE download_status || ',video'
      END WHERE video_id = ?`).run(videoPath, video_id);

      logEvent('info', 'Video downloaded');
    } else {
      const hint = result.errorCode === 'js_runtime_missing'
        ? ' Hint: set yt_dlp_js_runtimes=node,deno and ensure node is installed.'
        : result.errorCode === 'http_403'
          ? ' Hint: try fallback format (format_selector_fallback) or provide login cookies.'
          : '';
      throw new Error(`Failed to download video: ${result.error || 'unknown error'}${hint}`);
    }
    updateProgress(100);
  }

  private async handleDownloadAll(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const steps = [
      { fn: this.handleDownloadMeta, label: 'meta' },
      { fn: this.handleDownloadThumb, label: 'thumb' },
      { fn: this.handleDownloadSubs, label: 'subs' },
      { fn: this.handleDownloadVideo, label: 'video' },
    ];

    // Filter based on "with" array in payload if specified
    const withItems = job.payload.with || ['meta', 'thumb', 'subs', 'video'];
    const filteredSteps = steps.filter(s => withItems.includes(s.label));

    for (let i = 0; i < filteredSteps.length; i++) {
      if (job.cancelled()) break;
      const step = filteredSteps[i];
      const stepStartedAt = Date.now();
      logEvent('info', `Step ${i + 1}/${filteredSteps.length}: ${step.label}`);
      await step.fn(job, logEvent, (p) => {
        updateProgress(Math.floor(((i + p / 100) / filteredSteps.length) * 100));
      });
      const elapsedSec = ((Date.now() - stepStartedAt) / 1000).toFixed(1);
      logEvent('info', `Step ${i + 1}/${filteredSteps.length} completed: ${step.label} (${elapsedSec}s)`);
    }
    if (!job.cancelled()) {
      logEvent('info', `All requested steps completed (${filteredSteps.length})`);
    }
  }

  private async handleToolDownloadMeta(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const linksRaw = Array.isArray(job.payload?.links) ? job.payload.links : [];
    const links = linksRaw.map((item: unknown) => String(item || '').trim()).filter(Boolean);
    const rawConcurrency = Math.trunc(Number(job.payload?.concurrency || 0) || 0);
    const requestedConcurrency = rawConcurrency > 0 ? Math.max(1, Math.min(16, rawConcurrency)) : 0;
    const concurrency = requestedConcurrency > 0 ? requestedConcurrency : Math.max(1, Math.min(16, this.getMaxConcurrency()));
    if (links.length === 0) {
      throw new Error('No links provided for tool_download_meta');
    }

    logEvent('info', `Tool metadata batch started: ${links.length} links, concurrency ${concurrency}`);
    updateProgress(1);

    let lastLogAt = -1;
    const result = await downloadSimilarChannelMetaBatch(links, {
      cancelled: job.cancelled,
      abortSignal: job.abortSignal,
      concurrency,
      onProgress: (progress) => {
        const pct = Math.max(1, Math.min(99, Math.round(progress.percent * 10) / 10));
        updateProgress(pct);
        const bucket = Math.floor(pct);
        if (bucket !== lastLogAt) {
          lastLogAt = bucket;
          logEvent('info', `Tool metadata progress ${pct}% (${progress.stage})`);
        }
      },
    });

    db.prepare(`
      INSERT OR REPLACE INTO tool_job_results (job_id, result_json, created_at)
      VALUES (?, ?, datetime('now'))
    `).run(job.job_id, JSON.stringify(result));

    logEvent(
      'info',
      `Tool metadata batch completed. Output ${result.success_count}, failed ${result.failed.length}, dedupe ${result.before_dedupe_count}->${result.after_dedupe_count}, conc probe/meta/thumb ${result.probe_concurrency || '-'}\/${result.metadata_concurrency || '-'}\/${result.thumbnail_concurrency || '-'}`,
    );
    updateProgress(100);
  }

  private async handleToolDownloadMetaContent(job: any, logEvent: (l: string, m: string) => void, updateProgress: (p: number) => void): Promise<void> {
    const db = getDb();
    const linksRaw = Array.isArray(job.payload?.links) ? job.payload.links : [];
    const links = linksRaw.map((item: unknown) => String(item || '').trim()).filter(Boolean);
    const rawConcurrency = Math.trunc(Number(job.payload?.concurrency || 0) || 0);
    const requestedConcurrency = rawConcurrency > 0 ? Math.max(1, Math.min(16, rawConcurrency)) : 0;
    const concurrency = requestedConcurrency > 0 ? requestedConcurrency : Math.max(1, Math.min(16, this.getMaxConcurrency()));
    if (links.length === 0) {
      throw new Error('No links provided for tool_download_meta_content');
    }

    logEvent('info', `Tool similar-content metadata batch started: ${links.length} links, concurrency ${concurrency}`);
    updateProgress(1);

    let lastLogAt = -1;
    const result = await downloadSimilarContentMetaBatch(links, {
      cancelled: job.cancelled,
      abortSignal: job.abortSignal,
      concurrency,
      onProgress: (progress) => {
        const pct = Math.max(1, Math.min(99, Math.round(progress.percent * 10) / 10));
        updateProgress(pct);
        const bucket = Math.floor(pct);
        if (bucket !== lastLogAt) {
          lastLogAt = bucket;
          logEvent('info', `Tool similar-content metadata progress ${pct}% (${progress.stage})`);
        }
      },
    });

    db.prepare(`
      INSERT OR REPLACE INTO tool_job_results (job_id, result_json, created_at)
      VALUES (?, ?, datetime('now'))
    `).run(job.job_id, JSON.stringify(result));

    logEvent(
      'info',
      `Tool similar-content completed. Output ${result.success_count}, failed ${result.failed.length}, hit ${result.filter_before_count || 0}->${result.filter_after_count || 0}, ruleA ${result.filter_rule_a_count || 0}, ruleB ${result.filter_rule_b_count || 0}, conc probe/meta/thumb ${result.probe_concurrency || '-'}\/${result.metadata_concurrency || '-'}\/${result.thumbnail_concurrency || '-'}`,
    );
    updateProgress(100);
  }
}

let instance: JobQueue | null = null;

export function getJobQueue(): JobQueue {
  if (!instance) {
    instance = new JobQueue();
  }
  return instance;
}
