import { Router, Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import { getDb, getSetting } from '../db.js';
import { v4 as uuidv4 } from 'uuid';
import {
  fetchChannelSnapshotFromApi,
  getYoutubeApiUsageStatus,
  toChannelReportRow,
  toChannelReportRowFromDb,
} from '../services/youtubeApi.js';
import { buildChannelViewGrowthData, parseCachedChannelViewGrowth } from '../services/channelMetrics.js';
import { isSuspiciousYoutubeVideoCount } from '../services/channelVideoCount.js';
import * as ytdlp from '../services/ytdlp.js';

const router = Router();

function withFreshChannelGrowth(db: ReturnType<typeof getDb>, channel: any) {
  if (!channel?.channel_id) {
    return {
      ...channel,
      ...parseCachedChannelViewGrowth(channel),
    };
  }

  const fresh = buildChannelViewGrowthData(db, String(channel.channel_id));
  return {
    ...channel,
    channel_view_increase_7d: fresh.channel_view_increase_7d,
    channel_view_growth_series_7d_json: JSON.stringify(fresh.channel_view_growth_series_7d),
  };
}

type ChannelPlatform = 'youtube' | 'bilibili' | 'tiktok' | 'douyin' | 'xiaohongshu';
type WorkflowStatus = 'in_progress' | 'blocked' | 'paused';
const SUPPORTED_PLATFORMS = new Set<ChannelPlatform>([
  'youtube',
  'bilibili',
  'tiktok',
  'douyin',
  'xiaohongshu',
]);
const VALID_WORKFLOW_STATUSES = new Set<WorkflowStatus>(['in_progress', 'blocked', 'paused']);
const VALID_SYNC_CADENCES = new Set(['manual', 'daily', 'weekdays', 'weekly', 'custom']);

function normalizePlatform(raw: unknown): ChannelPlatform {
  const value = String(raw || '').trim().toLowerCase();
  if (SUPPORTED_PLATFORMS.has(value as ChannelPlatform)) return value as ChannelPlatform;
  return 'youtube';
}

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function sanitizeChannelSegment(value: string): string {
  return String(value || '')
    .trim()
    .replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/\.+$/g, '');
}

function normalizeHandleName(value: string): string {
  return String(value || '').trim().replace(/^@+/, '').replace(/\/+$/, '');
}

function normalizeSourceUrl(input: unknown): string | null {
  if (typeof input !== 'string') return null;
  const raw = input.trim();
  if (!raw) return null;
  const candidate = /^https?:\/\//i.test(raw) ? raw : (/^www\./i.test(raw) ? `https://${raw}` : '');
  if (!candidate) return null;
  try {
    const parsed = new URL(candidate);
    return parsed.toString();
  } catch {
    return null;
  }
}

function canonicalizeSourceUrlByPlatform(platform: ChannelPlatform, urlText: string | null): string | null {
  if (!urlText) return null;
  if (platform !== 'bilibili') return urlText;
  const identity = extractChannelIdentityFromUrl(platform, urlText);
  if (!identity) return urlText;
  return `https://space.bilibili.com/${identity}/video`;
}

function extractChannelIdentityFromUrl(platform: ChannelPlatform, urlText: string): string {
  try {
    const parsed = new URL(urlText);
    const host = parsed.hostname.replace(/^www\./, '').toLowerCase();
    const parts = parsed.pathname.split('/').filter(Boolean).map(safeDecode);
    if (platform === 'youtube' && host.endsWith('youtube.com')) {
      if (parts.length >= 2 && parts[0] === 'channel') return parts[1].trim();
      if (parts.length >= 1 && parts[0].startsWith('@')) return normalizeHandleName(parts[0]);
      if (parts.length >= 2 && (parts[0] === 'c' || parts[0] === 'user')) return normalizeHandleName(parts[1]);
    }
    if (platform === 'bilibili' && host.endsWith('bilibili.com') && parts[0] === 'space' && parts[1]) {
      return parts[1].trim();
    }
    if (platform === 'bilibili' && host === 'space.bilibili.com' && parts[0]) {
      return parts[0].trim();
    }
    if (platform === 'tiktok' && host.endsWith('tiktok.com') && parts[0]?.startsWith('@')) {
      return normalizeHandleName(parts[0]);
    }
    if (platform === 'douyin' && host.endsWith('douyin.com') && parts[0] === 'user' && parts[1]) {
      return parts[1].trim();
    }
    if (platform === 'xiaohongshu' && host.endsWith('xiaohongshu.com') && parts[0] === 'user' && parts[1] === 'profile' && parts[2]) {
      return parts[2].trim();
    }
  } catch {
    // ignore parse failure
  }
  return '';
}

function buildStorageChannelId(platform: ChannelPlatform, rawIdentity: string): string {
  const identity = sanitizeChannelSegment(rawIdentity);
  if (!identity) return '';
  if (platform === 'youtube') {
    if (identity.startsWith('UC')) return identity;
    return identity.replace(/^@+/, '');
  }
  return `${platform}__${identity}`;
}

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

function normalizePublishDays(input: unknown): number[] | null {
  if (!Array.isArray(input)) return null;
  const days = input
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item) && item >= 0 && item <= 6);
  const unique = Array.from(new Set(days));
  const order = [1, 2, 3, 4, 5, 6, 0];
  return order.filter((day) => unique.includes(day));
}

function normalizeChannelSyncPolicy(input: unknown): { cadence: string; publish_days: number[]; target_publish_time: string | null } | null {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const raw = input as Record<string, unknown>;
  const cadence = String(raw.cadence || raw.frequency || 'manual').trim() || 'manual';
  if (!VALID_SYNC_CADENCES.has(cadence)) return null;

  const publishDaysSource = raw.publish_days ?? raw.days ?? [];
  const publishDays = normalizePublishDays(publishDaysSource);
  if (publishDays == null) return null;

  const rawTargetPublishTime = raw.target_publish_time ?? raw.time;
  const targetPublishTimeText = typeof rawTargetPublishTime === 'string' ? rawTargetPublishTime.trim() : '';
  const targetPublishTime = targetPublishTimeText ? normalizeClockTime(targetPublishTimeText) : null;
  if (targetPublishTimeText && !targetPublishTime) return null;

  return {
    cadence,
    publish_days: publishDays,
    target_publish_time: targetPublishTime,
  };
}

function normalizeHandle(input: unknown, platform: ChannelPlatform, identity: string): string | null {
  const explicit = normalizeHandleName(typeof input === 'string' ? input : '');
  const candidate = explicit || normalizeHandleName(identity);
  if (!candidate) return null;
  if (platform === 'youtube' || platform === 'tiktok') return `@${candidate}`;
  return candidate;
}

function isLikelyYoutubeChannelId(value: string): boolean {
  return /^UC[\w-]{10,}$/i.test(String(value || '').trim());
}

function buildYoutubeProbeUrl(
  sourceUrl: string | null,
  rawIdentity: string,
  normalizedIdentity: string,
): string | null {
  if (sourceUrl) return sourceUrl;
  const raw = String(rawIdentity || '').trim();
  if (isLikelyYoutubeChannelId(raw)) {
    return `https://www.youtube.com/channel/${raw}`;
  }
  const handleName = normalizeHandleName(raw || normalizedIdentity);
  if (!handleName) return null;
  return `https://www.youtube.com/@${encodeURIComponent(handleName)}`;
}

interface ExportMetaSummary {
  id: string;
  title: string;
  description: string;
  duration: string;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  collect_count: number | null;
  share_count: number | null;
  categories: string;
  timestamp: number | null;
  timestamp_beijing: string;
  upload_weekday: string;
  like_rate: string;
  comment_rate: string;
  like_rate_value: number | null;
  comment_rate_value: number | null;
}

type ChannelHotMode = 'long' | 'short';

interface ChannelHotRankingRow {
  channel_id: string;
  channel_title: string;
  channel_handle: string | null;
  source_url: string | null;
  avatar_url: string | null;
  tags: string[];
  subscriber_count: number | null;
  views_growth: number;
  videos_count: number;
  days: number;
  mode: ChannelHotMode;
  channel_url: string | null;
}

function getStartDateBeforeDays(days: number): string {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  start.setUTCDate(start.getUTCDate() - Math.max(0, days - 1));
  return start.toISOString().slice(0, 10);
}

function resolveYoutubeChannelUrl(channelId: string, handle: string | null, sourceUrl: string | null): string | null {
  const direct = String(sourceUrl || '').trim();
  if (direct) return direct;

  const normalizedHandle = String(handle || '').trim().replace(/^@+/, '');
  if (normalizedHandle) return `https://www.youtube.com/@${encodeURIComponent(normalizedHandle)}`;

  if (/^UC[0-9A-Za-z_-]{20,}$/.test(channelId)) {
    return `https://www.youtube.com/channel/${channelId}`;
  }
  return null;
}

function buildHotChannelGrowthRanking(
  db: ReturnType<typeof getDb>,
  mode: ChannelHotMode,
  days: number,
  limit: number,
): ChannelHotRankingRow[] {
  const safeDays = Math.max(1, Math.trunc(days));
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  const startDate = getStartDateBeforeDays(safeDays);
  const endDate = new Date().toISOString().slice(0, 10);
  const shortExpr = "(lower(COALESCE(v.content_type, '')) = 'short' OR lower(COALESCE(v.webpage_url, '')) LIKE '%/shorts/%')";
  const modeWhere = mode === 'short'
    ? `AND ${shortExpr}`
    : `AND NOT ${shortExpr}`;

  const rows = db.prepare(`
    SELECT
      v.video_id,
      v.channel_id,
      v.published_at,
      COALESCE(vd_latest.view_count, v.view_count) AS latest_view_count,
      vd_base.view_count AS base_view_count_in_range,
      vd_prev.view_count AS base_view_count_prev,
      c.title AS channel_title,
      c.handle AS channel_handle,
      c.source_url AS source_url,
      c.avatar_url AS avatar_url,
      c.tags_json AS tags_json,
      c.subscriber_count AS subscriber_count
    FROM videos v
    INNER JOIN channels c ON c.channel_id = v.channel_id
    LEFT JOIN (
      SELECT vd.video_id, vd.view_count
      FROM video_daily vd
      INNER JOIN (
        SELECT video_id, MAX(date) AS max_date
        FROM video_daily
        GROUP BY video_id
      ) latest
        ON latest.video_id = vd.video_id
       AND latest.max_date = vd.date
    ) vd_latest ON vd_latest.video_id = v.video_id
    LEFT JOIN (
      SELECT vd.video_id, vd.view_count
      FROM video_daily vd
      INNER JOIN (
        SELECT video_id, MIN(date) AS base_date
        FROM video_daily
        WHERE date >= ?
        GROUP BY video_id
      ) base
        ON base.video_id = vd.video_id
       AND base.base_date = vd.date
    ) vd_base ON vd_base.video_id = v.video_id
    LEFT JOIN (
      SELECT vd.video_id, vd.view_count
      FROM video_daily vd
      INNER JOIN (
        SELECT video_id, MAX(date) AS prev_date
        FROM video_daily
        WHERE date < ?
        GROUP BY video_id
      ) prev
        ON prev.video_id = vd.video_id
       AND prev.prev_date = vd.date
    ) vd_prev ON vd_prev.video_id = v.video_id
    WHERE lower(COALESCE(v.platform, 'youtube')) = 'youtube'
      ${modeWhere}
      AND v.channel_id IS NOT NULL
      AND trim(v.channel_id) <> ''
      AND COALESCE(vd_latest.view_count, v.view_count) IS NOT NULL
  `).all(startDate, startDate) as any[];

  const byChannel = new Map<string, ChannelHotRankingRow>();
  for (const row of rows) {
    const channelId = String(row?.channel_id || '').trim();
    if (!channelId) continue;

    const latestViews = toNullableInt(row?.latest_view_count);
    if (latestViews == null) continue;

    // Rule:
    // 1) videos/shorts published within the window contribute full current views.
    // 2) older videos contribute only window growth (latest - baseline).
    const publishedAtText = String(row?.published_at || '').trim();
    const publishedDate = publishedAtText.includes('T')
      ? publishedAtText.slice(0, 10)
      : publishedAtText.slice(0, 10);
    const publishedInWindow = Boolean(
      /^\d{4}-\d{2}-\d{2}$/.test(publishedDate)
      && publishedDate >= startDate
      && publishedDate <= endDate,
    );

    const growth = (() => {
      if (publishedInWindow) return Math.max(0, latestViews);
      const baselineViews = toNullableInt(row?.base_view_count_in_range)
        ?? toNullableInt(row?.base_view_count_prev)
        ?? latestViews;
      return Math.max(0, latestViews - baselineViews);
    })();

    const existing = byChannel.get(channelId);
    if (existing) {
      existing.views_growth += growth;
      existing.videos_count += 1;
      continue;
    }

    const handle = String(row?.channel_handle || '').trim() || null;
    const sourceUrl = String(row?.source_url || '').trim() || null;
    byChannel.set(channelId, {
      channel_id: channelId,
      channel_title: String(row?.channel_title || '').trim() || channelId,
      channel_handle: handle,
      source_url: sourceUrl,
      avatar_url: String(row?.avatar_url || '').trim() || null,
      tags: parseTagsJson(row?.tags_json),
      subscriber_count: toNullableInt(row?.subscriber_count),
      views_growth: growth,
      videos_count: 1,
      days: safeDays,
      mode,
      channel_url: resolveYoutubeChannelUrl(channelId, handle, sourceUrl),
    });
  }

  return Array.from(byChannel.values())
    .sort((a, b) => {
      if (b.views_growth !== a.views_growth) return b.views_growth - a.views_growth;
      const bSubs = b.subscriber_count ?? -1;
      const aSubs = a.subscriber_count ?? -1;
      if (bSubs !== aSubs) return bSubs - aSubs;
      return a.channel_title.localeCompare(b.channel_title, 'zh-Hans-CN');
    })
    .slice(0, safeLimit);
}

function toNullableInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.trunc(parsed);
}

function normalizeEpochSeconds(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  if (parsed > 1e12) return Math.floor(parsed / 1000);
  return Math.floor(parsed);
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

function resolveExistingPath(maybePath: unknown): string | null {
  if (typeof maybePath !== 'string') return null;
  const value = maybePath.trim();
  if (!value) return null;
  if (fs.existsSync(value)) return value;
  const resolved = path.resolve(value);
  if (fs.existsSync(resolved)) return resolved;
  return null;
}

function isInsidePath(rootPath: string, targetPath: string): boolean {
  const relative = path.relative(path.resolve(rootPath), path.resolve(targetPath));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function parsePathArrayJson(input: unknown): string[] {
  if (typeof input !== 'string' || !input.trim()) return [];
  try {
    const parsed = JSON.parse(input);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item: unknown) => String(item || '').trim())
      .filter(Boolean);
  } catch {
    return [];
  }
}

function formatDuration(sec: number | null): string {
  if (sec == null || sec <= 0) return '';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function formatPercent(rate: number | null): string {
  if (rate == null) return 'N/A';
  return `${(rate * 100).toFixed(2)}%`;
}

const DEFAULT_CHANNEL_API_REFRESH_HOURS = 24;

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

function shouldFetchChannelApiForRefresh(channel: any): { shouldFetch: boolean; reason: string } {
  if (normalizePlatform(channel?.platform) !== 'youtube') return { shouldFetch: false, reason: 'platform_not_youtube' };
  if (getSetting('channel_api_enabled') === 'false') return { shouldFetch: false, reason: 'channel_api_disabled' };
  const hasAnyApiKey = Boolean(
    String(getSetting('youtube_api_key') || '').trim() ||
    String(getSetting('youtube_api_keys') || '').trim()
  );
  if (!hasAnyApiKey) return { shouldFetch: false, reason: 'youtube_api_key_missing' };
  if (hasChannelMetadataGap(channel)) return { shouldFetch: true, reason: 'channel_metadata_missing' };
  const db = getDb();
  const unavailableVideoStats = db.prepare(`
    SELECT
      SUM(CASE WHEN lower(COALESCE(availability_status, 'available')) = 'available' THEN 1 ELSE 0 END) AS available_count,
      SUM(CASE WHEN lower(COALESCE(availability_status, 'available')) <> 'available' THEN 1 ELSE 0 END) AS unavailable_count
    FROM videos
    WHERE channel_id = ?
  `).get(String(channel?.channel_id || '').trim()) as any;
  if (isSuspiciousYoutubeVideoCount({
    platform: normalizePlatform(channel?.platform),
    currentVideoCount: toNullableInt(channel?.video_count),
    availableTrackedVideoCount: Number(unavailableVideoStats?.available_count || 0),
    unavailableTrackedVideoCount: Number(unavailableVideoStats?.unavailable_count || 0),
    fetchLimit: getRecentVideoFetchLimit(),
  })) return { shouldFetch: true, reason: 'channel_video_count_suspected_capped' };

  const lastApiSyncEpoch = toEpochFromSqliteTimestamp(channel.api_last_sync_at);
  if (lastApiSyncEpoch == null) return { shouldFetch: true, reason: 'channel_api_never_synced' };

  const refreshHours = getChannelApiRefreshHours();
  const due = Date.now() - lastApiSyncEpoch >= refreshHours * 60 * 60 * 1000;
  if (due) return { shouldFetch: true, reason: `channel_api_snapshot_stale_${refreshHours}h` };
  return { shouldFetch: false, reason: 'channel_api_snapshot_fresh' };
}

function sanitizeFileName(input: string): string {
  const raw = (input || '').trim();
  const normalized = raw.replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_').replace(/\s+/g, ' ');
  const compact = normalized.replace(/\.+$/g, '').trim();
  return compact || 'channel';
}

function escapeCsvValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (!/[",\r\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function readLocalMetaSummary(localMetaPath: unknown): {
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  collect_count: number | null;
  share_count: number | null;
  categories: string[];
  timestamp: number | null;
} | null {
  const metaPath = resolveExistingPath(localMetaPath);
  if (!metaPath) return null;

  try {
    const raw = JSON.parse(fs.readFileSync(metaPath, 'utf8')) as any;
    const baseCategories = Array.isArray(raw?.categories)
      ? raw.categories.filter((item: unknown) => typeof item === 'string' && item.trim()).map((item: string) => item.trim())
      : (Array.isArray(raw?.tags)
        ? raw.tags.filter((item: unknown) => typeof item === 'string' && item.trim()).map((item: string) => item.trim())
        : []);
    const videoTagNames = Array.isArray(raw?.video_tag)
      ? raw.video_tag
        .map((item: any) => String(item?.tag_name || item?.name || '').trim())
        .filter(Boolean)
      : [];
    const textExtraTags = Array.isArray(raw?.text_extra)
      ? raw.text_extra
        .map((item: any) => String(item?.hashtag_name || item?.hashtag_name_rich || item?.hashtag_name_span || '').trim())
        .filter(Boolean)
        .map((item: string) => `#${item}`)
      : [];
    const categories = Array.from(new Set([...baseCategories, ...videoTagNames, ...textExtraTags]));
    const timestamp = normalizeEpochSeconds(raw?.timestamp)
      ?? normalizeEpochSeconds(raw?.release_timestamp)
      ?? normalizeEpochSeconds(raw?.create_time)
      ?? parsePublishedAtToTimestamp(raw?.upload_time)
      ?? parseUploadDateToTimestamp(raw?.upload_date)
      ?? null;

    return {
      view_count: (() => {
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
        if (
          viewCount === 0
          && [likeCount, commentCount, collectCount, shareCount].some((value) => value != null && value > 0)
        ) {
          return null;
        }
        return viewCount;
      })(),
      like_count: toNullableInt(
        raw?.like_count
        ?? raw?.liked_count
        ?? raw?.statistics?.digg_count
        ?? raw?.stat?.like
        ?? raw?.interact_info?.liked_count
        ?? raw?.note_card?.interact_info?.liked_count,
      ),
      comment_count: toNullableInt(
        raw?.comment_count
        ?? raw?.statistics?.comment_count
        ?? raw?.stat?.reply
        ?? raw?.interact_info?.comment_count
        ?? raw?.note_card?.interact_info?.comment_count,
      ),
      collect_count: toNullableInt(
        raw?.collect_count
        ?? raw?.collected_count
        ?? raw?.statistics?.collect_count
        ?? raw?.stat?.collect
        ?? raw?.interact_info?.collect_count
        ?? raw?.interact_info?.collected_count
        ?? raw?.note_card?.interact_info?.collect_count
        ?? raw?.note_card?.interact_info?.collected_count,
      ),
      share_count: toNullableInt(
        raw?.share_count
        ?? raw?.statistics?.share_count
        ?? raw?.stat?.share
        ?? raw?.interact_info?.share_count
        ?? raw?.note_card?.interact_info?.share_count,
      ),
      categories,
      timestamp,
    };
  } catch {
    return null;
  }
}

const beijingDateTimeFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

const beijingWeekdayFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  weekday: 'short',
});

function formatBeijingDateTime(timestamp: number | null): string {
  if (timestamp == null) return 'N/A';
  const date = new Date(timestamp * 1000);
  if (Number.isNaN(date.getTime())) return 'N/A';
  const parts = beijingDateTimeFormatter.formatToParts(date);
  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
}

function formatBeijingWeekday(timestamp: number | null): string {
  if (timestamp == null) return 'N/A';
  const date = new Date(timestamp * 1000);
  if (Number.isNaN(date.getTime())) return 'N/A';
  return beijingWeekdayFormatter.format(date) || 'N/A';
}

const EN_STOP_WORDS = new Set([
  'the', 'a', 'an', 'of', 'to', 'in', 'on', 'for', 'at', 'by', 'and', 'or', 'is', 'are', 'be',
  'with', 'from', 'this', 'that', 'it', 'as', 'your', 'you', 'my', 'our',
]);

function normalizeToken(token: string): string {
  const trimmed = token.trim();
  if (!trimmed) return '';
  if (/^[A-Za-z0-9][A-Za-z0-9'_-]*$/.test(trimmed)) return trimmed.toLowerCase();
  return trimmed;
}

function extractTitleTokens(title: unknown): string[] {
  if (typeof title !== 'string') return [];
  const raw = title.trim();
  if (!raw) return [];
  const matches = raw.match(/[\u4e00-\u9fff]{2,}|[A-Za-z0-9][A-Za-z0-9'_-]{1,}/g) || [];
  return matches
    .map(normalizeToken)
    .filter((token) => token.length >= 2)
    .filter((token) => !EN_STOP_WORDS.has(token));
}

function parseTagsJson(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.filter((item) => typeof item === 'string').map((item) => item.trim()).filter(Boolean);
  }
  if (typeof raw !== 'string') return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((item) => typeof item === 'string').map((item) => item.trim()).filter(Boolean);
  } catch {
    return [];
  }
}

function extractDescriptionTags(video: any): string[] {
  const fromTagsField = parseTagsJson(video?.tags_json);
  const description = typeof video?.description === 'string' ? video.description : '';
  const hashtagMatches: string[] = [];

  for (const match of description.matchAll(/(?:^|\s)#([^\s#]{1,50})/g)) {
    if (match[1]) hashtagMatches.push(match[1]);
  }

  return [...fromTagsField, ...hashtagMatches]
    .map((tag) => normalizeToken(tag.replace(/^#+/, '')))
    .filter((tag) => tag.length >= 2);
}

function topEntries(map: Map<string, number>, limit = 20): Array<{ term: string; count: number }> {
  return Array.from(map.entries())
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0], 'zh-Hans-CN');
    })
    .slice(0, limit)
    .map(([term, count]) => ({ term, count }));
}

function parseLooseDateToMs(raw: unknown): number | null {
  if (typeof raw !== 'string') return null;
  const text = raw.trim();
  if (!text) return null;
  let normalized = text;
  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    normalized = `${normalized}T00:00:00Z`;
  } else if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$/.test(normalized)) {
    normalized = `${normalized.replace(/\s+/, 'T')}Z`;
  } else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/.test(normalized)) {
    normalized = `${normalized}Z`;
  }
  const parsed = Date.parse(normalized);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
}

function formatIsoSecondFromMs(ms: number | null): string | null {
  if (ms == null || !Number.isFinite(ms)) return null;
  const iso = new Date(ms).toISOString();
  return iso.replace(/\.\d{3}Z$/, 'Z');
}

function resolveInsightSortValue(
  row: ExportMetaSummary,
  key: 'view_count' | 'like_count' | 'comment_count' | 'collect_count' | 'share_count' | 'like_rate' | 'comment_rate',
): number | null {
  switch (key) {
    case 'view_count':
      return row.view_count;
    case 'like_count':
      return row.like_count;
    case 'comment_count':
      return row.comment_count;
    case 'collect_count':
      return row.collect_count;
    case 'share_count':
      return row.share_count;
    case 'like_rate':
      return row.like_rate_value;
    case 'comment_rate':
      return row.comment_rate_value;
    default:
      return null;
  }
}

// GET /api/channels
router.get('/', (req: Request, res: Response) => {
  const db = getDb();
  const { tag, q, page = '1', limit = '20', favorite, platform, sort = 'recent' } = req.query;
  const pageNum = Math.max(1, parseInt(page as string, 10) || 1);
  const limitNum = Math.min(100, parseInt(limit as string, 10) || 20);
  const offset = (pageNum - 1) * limitNum;
  const normalizedPlatform = platform ? normalizePlatform(platform) : null;

  let where = 'WHERE 1=1';
  const params: any[] = [];

  if (q) {
    where += ' AND (c.title LIKE ? OR c.handle LIKE ?)';
    params.push(`%${q}%`, `%${q}%`);
  }
  if (tag) {
    where += ' AND c.tags_json LIKE ?';
    params.push(`%"${tag}"%`);
  }
  if (favorite === '1') {
    where += ' AND c.favorite = 1';
  }
  if (normalizedPlatform) {
    where += ' AND lower(COALESCE(c.platform, \'youtube\')) = ?';
    params.push(normalizedPlatform);
  }

  const shortExpr = "(lower(COALESCE(v.content_type, '')) = 'short' OR lower(COALESCE(v.webpage_url, '')) LIKE '%/shorts/%')";
  const channelsWithLatestPublishSql = `
    SELECT
      c.*,
      MAX(CASE WHEN ${shortExpr} THEN COALESCE(v.published_at, v.created_at) ELSE NULL END) AS latest_short_published_at,
      MAX(CASE WHEN NOT (${shortExpr}) THEN COALESCE(v.published_at, v.created_at) ELSE NULL END) AS latest_long_published_at
    FROM channels c
    LEFT JOIN videos v ON v.channel_id = c.channel_id
    ${where}
    GROUP BY c.channel_id
  `;

  if (sort === 'views_7d') {
    const total = (db.prepare(`SELECT COUNT(*) as count FROM channels c ${where}`).get(...params) as any).count;
    const rows = db.prepare(`
      ${channelsWithLatestPublishSql}
      ORDER BY COALESCE(c.channel_view_increase_7d, 0) DESC, c.title COLLATE NOCASE ASC, c.created_at DESC
      LIMIT ? OFFSET ?
    `).all(...params, limitNum, offset);
    res.json({ data: rows.map((row: any) => withFreshChannelGrowth(db, row)), total, page: pageNum, limit: limitNum });
    return;
  }

  const orderBy = sort === 'name'
    ? 'ORDER BY c.title COLLATE NOCASE ASC, c.created_at DESC'
    : 'ORDER BY c.last_sync_at DESC NULLS LAST, c.created_at DESC';

  const total = (db.prepare(`SELECT COUNT(*) as count FROM channels c ${where}`).get(...params) as any).count;
  const rows = db.prepare(
    `${channelsWithLatestPublishSql} ${orderBy} LIMIT ? OFFSET ?`
  ).all(...params, limitNum, offset);

  res.json({ data: rows.map((row: any) => withFreshChannelGrowth(db, row)), total, page: pageNum, limit: limitNum });
});

// GET /api/channels/report
router.get('/report', async (req: Request, res: Response) => {
  const db = getDb();
  const { page = '1', limit = '50', refresh = '0', platform } = req.query;
  const pageNum = Math.max(1, parseInt(page as string, 10) || 1);
  const limitNum = Math.min(200, parseInt(limit as string, 10) || 50);
  const offset = (pageNum - 1) * limitNum;
  const shouldRefresh = refresh === '1' || refresh === 'true';
  const normalizedPlatform = platform ? normalizePlatform(platform) : null;
  const where = normalizedPlatform
    ? `WHERE lower(COALESCE(platform, 'youtube')) = ?`
    : '';
  const whereParams = normalizedPlatform ? [normalizedPlatform] : [];

  const total = (db.prepare(`SELECT COUNT(*) as count FROM channels ${where}`).get(...whereParams) as any).count;
  const channels = db.prepare(
    `SELECT * FROM channels ${where} ORDER BY last_sync_at DESC NULLS LAST, created_at DESC LIMIT ? OFFSET ?`
  ).all(...whereParams, limitNum, offset) as any[];

  const rows: Array<Record<string, string | number>> = [];

  for (const channel of channels) {
    if (shouldRefresh) {
      const apiDecision = shouldFetchChannelApiForRefresh(channel);
      if (!apiDecision.shouldFetch) {
        rows.push(toChannelReportRowFromDb(channel));
        continue;
      }
      const apiResult = await fetchChannelSnapshotFromApi(channel.channel_id);
      if (apiResult.success && apiResult.data) {
        const item = apiResult.data;
        db.prepare(`
          UPDATE channels
          SET title = ?,
              handle = ?,
              avatar_url = ?,
              country = ?,
              subscriber_count = ?,
              video_count = ?,
              view_count = ?,
              api_last_sync_at = datetime('now'),
              last_sync_at = datetime('now')
          WHERE channel_id = ?
        `).run(
          item.title || channel.title,
          item.customUrl || channel.handle,
          item.highThumbnailUrl || channel.avatar_url,
          item.country || channel.country,
          item.subscriberCount,
          item.videoCount,
          item.totalViews,
          channel.channel_id
        );

        const today = new Date().toISOString().slice(0, 10);
        db.prepare(`
          INSERT OR REPLACE INTO channel_daily (date, channel_id, subscriber_count, view_count_total, video_count)
          VALUES (?, ?, ?, ?, ?)
        `).run(today, channel.channel_id, item.subscriberCount, item.totalViews, item.videoCount);

        rows.push(toChannelReportRow(item));
        continue;
      }
    }

    rows.push(toChannelReportRowFromDb(channel));
  }

  res.json({
    data: rows,
    total,
    page: pageNum,
    limit: limitNum,
    refresh: shouldRefresh,
    api_usage: getYoutubeApiUsageStatus(),
  });
});

// GET /api/channels/hot-rankings
router.get('/hot-rankings', (req: Request, res: Response) => {
  const db = getDb();
  const limitNum = Math.max(1, Math.min(50, parseInt(String(req.query.limit || '10'), 10) || 10));
  const longDays = Math.max(1, Math.min(90, parseInt(String(req.query.long_days || '15'), 10) || 15));
  const shortDays = Math.max(1, Math.min(90, parseInt(String(req.query.short_days || '7'), 10) || 7));

  const long = buildHotChannelGrowthRanking(db, 'long', longDays, limitNum);
  const short = buildHotChannelGrowthRanking(db, 'short', shortDays, limitNum);

  res.json({
    long_days: longDays,
    short_days: shortDays,
    limit: limitNum,
    long,
    short,
  });
});

// GET /api/channels/tag-insights
router.get('/tag-insights', (req: Request, res: Response) => {
  const db = getDb();
  const tag = String(req.query.tag || '').trim();
  if (!tag) {
    res.status(400).json({ error: 'tag is required' });
    return;
  }

  const platform = normalizePlatform(req.query.platform);
  const mode = String(req.query.mode || 'long').trim().toLowerCase() === 'short' ? 'short' : 'long';
  const recentDays = Math.max(1, Math.min(90, parseInt(String(req.query.recent_days || '30'), 10) || 30));
  const limitNum = Math.max(1, Math.min(100, parseInt(String(req.query.limit || '30'), 10) || 30));
  const favoriteOnly = String(req.query.favorite || '') === '1';
  const cutoffDate = new Date(Date.now() - (recentDays - 1) * 86400000).toISOString().slice(0, 10);

  const params: any[] = [platform, `%"${tag}"%`, cutoffDate];
  let favoriteWhere = '';
  if (favoriteOnly) {
    favoriteWhere = ' AND c.favorite = 1';
  }

  const shortExpr = "(lower(COALESCE(v.content_type, '')) = 'short' OR lower(COALESCE(v.webpage_url, '')) LIKE '%/shorts/%')";
  const contentWhere = mode === 'short'
    ? `AND (${shortExpr})`
    : `AND NOT (${shortExpr})`;
  const rows = db.prepare(`
    SELECT
      v.title AS title,
      COALESCE(vd_latest.view_count, v.view_count, 0) AS latest_views,
      COALESCE(v.published_at, v.created_at) AS published_at
    FROM videos v
    INNER JOIN channels c ON c.channel_id = v.channel_id
    LEFT JOIN video_daily vd_latest
      ON vd_latest.video_id = v.video_id
     AND vd_latest.date = (
       SELECT MAX(vd2.date) FROM video_daily vd2 WHERE vd2.video_id = v.video_id
     )
    WHERE lower(COALESCE(c.platform, 'youtube')) = ?
      AND c.tags_json LIKE ?
      ${favoriteWhere}
      ${contentWhere}
      AND date(COALESCE(v.published_at, v.created_at)) >= date(?)
      AND NOT (
        v.title = 'Untitled'
        AND v.duration_sec IS NULL
        AND v.view_count IS NULL
        AND v.like_count IS NULL
      )
  `).all(...params) as Array<{ title: string | null; latest_views: number | null; published_at: string | null }>;

  const totalChannelsRow = db.prepare(`
    SELECT COUNT(*) AS count
    FROM channels c
    WHERE lower(COALESCE(c.platform, 'youtube')) = ?
      AND c.tags_json LIKE ?
      ${favoriteWhere}
  `).get(platform, `%"${tag}"%`) as { count?: number } | undefined;
  const totalChannels = Math.max(0, Number(totalChannelsRow?.count || 0));

  const nowMs = Date.now();
  const tokenStats = new Map<string, { score: number; count: number; total_views: number; latest_ms: number | null }>();

  for (const row of rows) {
    const title = String(row?.title || '').trim();
    if (!title) continue;
    const views = Math.max(0, Number(row?.latest_views || 0));
    const publishedMs = parseLooseDateToMs(row?.published_at);
    const daysOld = publishedMs == null
      ? 365
      : Math.max(0.25, (nowMs - publishedMs) / 86400000);
    const recencyBoost = 1 / Math.log2(daysOld + 2);
    const videoScore = Math.log1p(views + 1) * recencyBoost;
    if (!Number.isFinite(videoScore) || videoScore <= 0) continue;

    const tokens = Array.from(new Set(extractTitleTokens(title)));
    for (const token of tokens) {
      const prev = tokenStats.get(token) || { score: 0, count: 0, total_views: 0, latest_ms: null };
      const nextLatest = publishedMs == null
        ? prev.latest_ms
        : (prev.latest_ms == null ? publishedMs : Math.max(prev.latest_ms, publishedMs));
      tokenStats.set(token, {
        score: prev.score + videoScore,
        count: prev.count + 1,
        total_views: prev.total_views + views,
        latest_ms: nextLatest,
      });
    }
  }

  const keywords = Array.from(tokenStats.entries())
    .map(([term, stat]) => ({
      term,
      score: Number(stat.score.toFixed(2)),
      count: stat.count,
      total_views: Math.trunc(stat.total_views),
      latest_published_at: formatIsoSecondFromMs(stat.latest_ms),
    }))
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (b.count !== a.count) return b.count - a.count;
      if (b.total_views !== a.total_views) return b.total_views - a.total_views;
      return a.term.localeCompare(b.term, 'zh-Hans-CN');
    })
    .slice(0, limitNum);

  res.json({
    tag,
    platform,
    mode,
    recent_days: recentDays,
    favorite_only: favoriteOnly,
    total_channels: totalChannels,
    total_videos: rows.length,
    keywords,
    generated_at: new Date().toISOString(),
  });
});

// POST /api/channels
router.post('/', async (req: Request, res: Response) => {
  const db = getDb();
  const { channel_id, title, handle, avatar_url, country, language, tags, platform, source_url, favorite, priority } = req.body;
  const rawPlatform = String(platform || '').trim().toLowerCase();
  if (rawPlatform && !SUPPORTED_PLATFORMS.has(rawPlatform as ChannelPlatform)) {
    res.status(400).json({ error: `Unsupported channel platform: ${rawPlatform}` });
    return;
  }

  const normalizedPlatform = normalizePlatform(platform);
  const normalizedSourceUrl = canonicalizeSourceUrlByPlatform(
    normalizedPlatform,
    normalizeSourceUrl(source_url) || normalizeSourceUrl(channel_id),
  );
  const sourceIdentity = String(channel_id || '').trim();
  const identityFromUrl = normalizedSourceUrl ? extractChannelIdentityFromUrl(normalizedPlatform, normalizedSourceUrl) : '';
  const fallbackIdentitySource = identityFromUrl
    || safeDecode(sourceIdentity).replace(/^@+/, '')
    || String(normalizedSourceUrl || '').trim();
  const normalizedIdentity = sanitizeChannelSegment(fallbackIdentitySource);
  const normalizedChannelId = buildStorageChannelId(normalizedPlatform, normalizedIdentity);
  const normalizedHandle = normalizeHandle(handle, normalizedPlatform, normalizedIdentity);
  const rawTitleInput = String(title || '').trim();
  const defaultIdentityTitle = (
    normalizedPlatform === 'bilibili' && /^\d+$/.test(normalizedIdentity)
      ? 'unknown'
      : (normalizedIdentity || normalizedChannelId || '')
  );
  const normalizedTitle = String(rawTitleInput || defaultIdentityTitle).trim();
  let resolvedChannelId = normalizedChannelId;
  let resolvedHandle = normalizedHandle;
  let resolvedTitle = normalizedTitle;
  let resolvedSourceUrl = normalizedSourceUrl;
  let resolvedAvatarUrl = String(avatar_url || '').trim() || null;

  // YouTube handle/custom-url inputs can create duplicate channel rows.
  // Probe once to resolve canonical UC channel_id before insertion.
  if (normalizedPlatform === 'youtube') {
    const probeUrl = buildYoutubeProbeUrl(normalizedSourceUrl, sourceIdentity, normalizedIdentity);
    if (probeUrl) {
      try {
        const infoResult = await ytdlp.getChannelInfo(probeUrl);
        if (infoResult.success && infoResult.data) {
          const parsedMeta = ytdlp.parseChannelMeta(infoResult.data);
          const parsedChannelId = sanitizeChannelSegment(String(parsedMeta.channel_id || '').trim());
          if (isLikelyYoutubeChannelId(parsedChannelId)) {
            resolvedChannelId = parsedChannelId;
            resolvedSourceUrl = `https://www.youtube.com/channel/${parsedChannelId}`;
          } else if (!resolvedSourceUrl) {
            resolvedSourceUrl = probeUrl;
          }

          const parsedHandleName = normalizeHandleName(String(parsedMeta.handle || '').trim());
          if (parsedHandleName) {
            resolvedHandle = `@${parsedHandleName}`;
          }

          const parsedTitle = String(parsedMeta.title || '').trim();
          if (parsedTitle) {
            resolvedTitle = parsedTitle;
          }

          const parsedAvatar = String(parsedMeta.avatar_url || '').trim();
          if (parsedAvatar) {
            resolvedAvatarUrl = parsedAvatar;
          }
        }
      } catch {
        // Keep fallback identity when canonical probe fails.
      }
    }
  }

  if (!resolvedChannelId || !resolvedTitle) {
    res.status(400).json({ error: 'channel_id and title are required' });
    return;
  }

  const existing = db.prepare('SELECT channel_id FROM channels WHERE channel_id = ?').get(resolvedChannelId);
  if (existing) {
    res.status(409).json({ error: 'Channel already exists', channel_id: resolvedChannelId });
    return;
  }

  if (normalizedPlatform === 'youtube') {
    const resolvedHandleLower = String(resolvedHandle || '').trim().toLowerCase();
    if (resolvedHandleLower) {
      const existingByHandle = db.prepare(`
        SELECT channel_id FROM channels
        WHERE lower(COALESCE(platform, 'youtube')) = 'youtube'
          AND lower(COALESCE(handle, '')) = ?
        LIMIT 1
      `).get(resolvedHandleLower) as { channel_id?: string } | undefined;
      const existingByHandleId = String(existingByHandle?.channel_id || '').trim();
      if (existingByHandleId && existingByHandleId !== resolvedChannelId) {
        res.status(409).json({ error: 'Channel already exists', channel_id: existingByHandleId });
        return;
      }
    }

    const resolvedSourceLower = String(resolvedSourceUrl || '').trim().toLowerCase();
    if (resolvedSourceLower) {
      const existingBySource = db.prepare(`
        SELECT channel_id FROM channels
        WHERE lower(COALESCE(platform, 'youtube')) = 'youtube'
          AND lower(COALESCE(source_url, '')) = ?
        LIMIT 1
      `).get(resolvedSourceLower) as { channel_id?: string } | undefined;
      const existingBySourceId = String(existingBySource?.channel_id || '').trim();
      if (existingBySourceId && existingBySourceId !== resolvedChannelId) {
        res.status(409).json({ error: 'Channel already exists', channel_id: existingBySourceId });
        return;
      }
    }
  }

  const existingBySourceUrl = resolvedSourceUrl
    ? db.prepare(`
      SELECT channel_id FROM channels
      WHERE lower(COALESCE(platform, 'youtube')) = ?
        AND lower(COALESCE(source_url, '')) = ?
      LIMIT 1
    `).get(normalizedPlatform, String(resolvedSourceUrl || '').trim().toLowerCase()) as { channel_id?: string } | undefined
    : null;
  const existingBySourceId = String(existingBySourceUrl?.channel_id || '').trim();
  if (existingBySourceId && existingBySourceId !== resolvedChannelId) {
    res.status(409).json({ error: 'Channel already exists', channel_id: existingBySourceId });
    return;
  }

  const normalizedPriority = String(priority || '').trim() || (favorite ? 'high' : 'normal');

  db.prepare(`
    INSERT INTO channels (channel_id, platform, title, handle, source_url, avatar_url, country, language, tags_json, favorite, priority)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    resolvedChannelId,
    normalizedPlatform,
    resolvedTitle,
    resolvedHandle,
    resolvedSourceUrl,
    resolvedAvatarUrl,
    country || null,
    language || null,
    JSON.stringify(tags || []),
    favorite ? 1 : 0,
    normalizedPriority
  );

  // Auto-trigger sync for the new channel
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'sync_channel', ?, 'queued')
  `).run(jobId, JSON.stringify({
    channel_id: resolvedChannelId,
    platform: normalizedPlatform,
    post_add_meta_retry_audit: true,
  }));

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  const channel = db.prepare('SELECT * FROM channels WHERE channel_id = ?').get(resolvedChannelId);
  res.status(201).json({
    ...(channel as Record<string, unknown>),
    sync_job_id: jobId,
  });
});

// GET /api/channels/:id
router.get('/:id', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  db.prepare(`
    UPDATE channels
    SET new_video_badge_count = 0
    WHERE channel_id = ?
      AND COALESCE(new_video_badge_count, 0) > 0
  `).run(channelId);
  const channel = db.prepare('SELECT * FROM channels WHERE channel_id = ?').get(channelId) as any;
  if (!channel) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }
  res.json(withFreshChannelGrowth(db, channel));
});

// PATCH /api/channels/:id
router.patch('/:id', (req: Request, res: Response) => {
  const db = getDb();
  const {
    tags,
    tags_json,
    favorite,
    priority,
    sync_policy,
    workflow_status,
    positioning,
    notes,
    manual_updated_at,
  } = req.body;
  const updates: string[] = [];
  const params: any[] = [];

  let normalizedTags: any[] | undefined;
  if (tags !== undefined) {
    if (!Array.isArray(tags)) {
      res.status(400).json({ error: 'tags must be an array' });
      return;
    }
    normalizedTags = tags;
  } else if (tags_json !== undefined) {
    if (Array.isArray(tags_json)) {
      normalizedTags = tags_json;
    } else if (typeof tags_json === 'string') {
      try {
        const parsed = JSON.parse(tags_json);
        normalizedTags = Array.isArray(parsed) ? parsed : [];
      } catch {
        res.status(400).json({ error: 'tags_json must be valid JSON array' });
        return;
      }
    }
  }

  if (normalizedTags !== undefined) {
    updates.push('tags_json = ?');
    params.push(JSON.stringify(normalizedTags));
  }
  if (favorite !== undefined) {
    updates.push('favorite = ?');
    params.push(favorite ? 1 : 0);
  }
  if (priority !== undefined) {
    updates.push('priority = ?');
    params.push(priority);
  }
  if (sync_policy !== undefined) {
    const normalizedSyncPolicy = normalizeChannelSyncPolicy(sync_policy);
    if (!normalizedSyncPolicy) {
      res.status(400).json({ error: 'sync_policy is invalid' });
      return;
    }
    updates.push('sync_policy_json = ?');
    params.push(JSON.stringify(normalizedSyncPolicy));
  }
  if (workflow_status !== undefined) {
    const normalizedWorkflowStatus = String(workflow_status || '').trim();
    if (!VALID_WORKFLOW_STATUSES.has(normalizedWorkflowStatus as WorkflowStatus)) {
      res.status(400).json({ error: 'workflow_status is invalid' });
      return;
    }
    updates.push('workflow_status = ?');
    params.push(normalizedWorkflowStatus);
  }
  if (positioning !== undefined) {
    updates.push('positioning = ?');
    params.push(positioning);
  }
  if (notes !== undefined) {
    updates.push('notes = ?');
    params.push(notes);
  }
  if (manual_updated_at !== undefined) {
    const normalizedManualUpdatedAt = manual_updated_at == null || manual_updated_at === ''
      ? null
      : String(manual_updated_at).trim();
    if (normalizedManualUpdatedAt) {
      const parsed = new Date(normalizedManualUpdatedAt);
      if (Number.isNaN(parsed.getTime())) {
        res.status(400).json({ error: 'manual_updated_at is invalid' });
        return;
      }
    }
    updates.push('manual_updated_at = ?');
    params.push(normalizedManualUpdatedAt);
  }

  if (updates.length === 0) {
    res.status(400).json({ error: 'No fields to update' });
    return;
  }

  params.push(req.params.id);
  const result = db.prepare(`UPDATE channels SET ${updates.join(', ')} WHERE channel_id = ?`).run(...params);
  if (result.changes === 0) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }

  const channel = db.prepare('SELECT * FROM channels WHERE channel_id = ?').get(req.params.id);
  res.json(channel);
});

// DELETE /api/channels/:id
router.delete('/:id', async (req: Request, res: Response) => {
  const db = getDb();
  const channelIdParam = req.params.id as unknown;
  const channelId = Array.isArray(channelIdParam)
    ? String(channelIdParam[0] || '').trim()
    : String(channelIdParam || '').trim();
  if (!channelId) {
    res.status(400).json({ error: 'Invalid channel id' });
    return;
  }

  const channel = db.prepare('SELECT channel_id, title FROM channels WHERE channel_id = ?').get(channelId) as any;
  if (!channel) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }

  const videoRows = db.prepare(`
    SELECT video_id, local_meta_path, local_thumb_path, local_subtitle_paths, local_video_path
    FROM videos
    WHERE channel_id = ?
  `).all(channelId) as Array<{
    video_id: string;
    local_meta_path: string | null;
    local_thumb_path: string | null;
    local_subtitle_paths: string | null;
    local_video_path: string | null;
  }>;
  const videoIdList = videoRows.map((v) => v.video_id);

  const relatedJobs = db.prepare(`
    SELECT job_id, status FROM jobs
    WHERE type IN ('sync_channel', 'channel_meta_retry_audit')
      AND json_extract(payload_json, '$.channel_id') = ?
      AND status IN ('queued', 'running')
  `).all(channelId) as Array<{ job_id: string; status: string }>;

  if (videoIdList.length > 0) {
    const placeholders = videoIdList.map(() => '?').join(',');
    const videoJobs = db.prepare(`
      SELECT job_id, status FROM jobs
      WHERE type IN ('download_meta', 'download_thumb', 'download_subs', 'download_video', 'download_all')
        AND json_extract(payload_json, '$.video_id') IN (${placeholders})
        AND status IN ('queued', 'running')
    `).all(...videoIdList) as Array<{ job_id: string; status: string }>;
    relatedJobs.push(...videoJobs);
  }

  const dedupJobs = Array.from(new Map(relatedJobs.map(j => [j.job_id, j])).values());
  const runningJobIds = dedupJobs.filter(j => j.status === 'running').map(j => j.job_id);
  const relatedJobIds = dedupJobs.map(j => j.job_id);

  if (runningJobIds.length > 0) {
    try {
      const { getJobQueue } = await import('../services/jobQueue.js');
      const queue = getJobQueue();
      for (const jobId of runningJobIds) {
        queue.cancelJob(jobId);
      }
    } catch {}
  }

  const downloadRoot = path.resolve(getSetting('download_root') || path.join(process.cwd(), 'downloads'));
  const assetsRoot = path.resolve(downloadRoot, 'assets');
  const channelScopedDirs = [
    path.resolve(path.join(assetsRoot, 'meta', channelId)),
    path.resolve(path.join(assetsRoot, 'thumbs', channelId)),
    path.resolve(path.join(assetsRoot, 'subs', channelId)),
    path.resolve(path.join(assetsRoot, 'videos', channelId)),
  ];
  let deletedChannelDirs = 0;
  let skippedChannelDirs = 0;
  let channelDirDeleteErrors = 0;
  for (const dirPath of channelScopedDirs) {
    if (!isInsidePath(assetsRoot, dirPath)) {
      skippedChannelDirs += 1;
      continue;
    }
    try {
      fs.rmSync(dirPath, { recursive: true, force: true });
      deletedChannelDirs += 1;
    } catch {
      channelDirDeleteErrors += 1;
    }
  }

  const localPathCandidates = new Set<string>();
  const addLocalPath = (rawPath: unknown) => {
    if (typeof rawPath !== 'string') return;
    const value = rawPath.trim();
    if (!value) return;
    localPathCandidates.add(path.resolve(value));
  };
  for (const row of videoRows) {
    addLocalPath(row.local_meta_path);
    addLocalPath(row.local_thumb_path);
    addLocalPath(row.local_video_path);
    for (const subPath of parsePathArrayJson(row.local_subtitle_paths)) {
      addLocalPath(subPath);
    }
  }

  let deletedLocalPaths = 0;
  let missingLocalPaths = 0;
  let skippedOutsideLocalPaths = 0;
  let localPathDeleteErrors = 0;
  for (const candidatePath of localPathCandidates) {
    if (!isInsidePath(downloadRoot, candidatePath)) {
      skippedOutsideLocalPaths += 1;
      continue;
    }
    if (!fs.existsSync(candidatePath)) {
      missingLocalPaths += 1;
      continue;
    }
    try {
      const stat = fs.lstatSync(candidatePath);
      if (stat.isDirectory()) {
        fs.rmSync(candidatePath, { recursive: true, force: true });
      } else {
        fs.unlinkSync(candidatePath);
      }
      deletedLocalPaths += 1;
    } catch {
      localPathDeleteErrors += 1;
    }
  }

  const tx = db.transaction(() => {
    if (relatedJobIds.length > 0) {
      const placeholders = relatedJobIds.map(() => '?').join(',');
      db.prepare(`
        UPDATE jobs
        SET status = 'canceled',
            finished_at = datetime('now'),
            error_message = COALESCE(error_message, 'Canceled because channel was deleted')
        WHERE job_id IN (${placeholders})
      `).run(...relatedJobIds);
    }

    db.prepare('DELETE FROM availability_log WHERE video_id IN (SELECT video_id FROM videos WHERE channel_id = ?)').run(channelId);
    db.prepare('DELETE FROM video_daily WHERE video_id IN (SELECT video_id FROM videos WHERE channel_id = ?)').run(channelId);
    db.prepare('DELETE FROM videos WHERE channel_id = ?').run(channelId);
    db.prepare('DELETE FROM channel_daily WHERE channel_id = ?').run(channelId);
    db.prepare('DELETE FROM channels WHERE channel_id = ?').run(channelId);
  });

  tx();

  res.json({
    channel_id: channelId,
    title: channel.title,
    deleted: true,
    canceled_jobs: relatedJobIds.length,
    deleted_videos: videoIdList.length,
    deleted_channel_dirs: deletedChannelDirs,
    skipped_channel_dirs: skippedChannelDirs,
    channel_dir_delete_errors: channelDirDeleteErrors,
    deleted_local_paths: deletedLocalPaths,
    missing_local_paths: missingLocalPaths,
    skipped_outside_local_paths: skippedOutsideLocalPaths,
    local_path_delete_errors: localPathDeleteErrors,
  });
});

// POST /api/channels/:id/sync 鈥?trigger sync
router.post('/:id/sync', async (req: Request, res: Response) => {
  const db = getDb();
  const channel = db.prepare('SELECT channel_id, platform FROM channels WHERE channel_id = ?').get(req.params.id) as any;
  if (!channel) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }
  const rawPlatform = String(channel.platform || '').trim().toLowerCase();
  if (rawPlatform && !SUPPORTED_PLATFORMS.has(rawPlatform as ChannelPlatform)) {
    res.status(400).json({ error: `Unsupported channel platform: ${rawPlatform}` });
    return;
  }

  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'sync_channel', ?, 'queued')
  `).run(jobId, JSON.stringify({
    channel_id: req.params.id,
    platform: normalizePlatform(channel.platform),
    meta_retry_audit: true,
  }));

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  res.json({ job_id: jobId, status: 'queued' });
});

// POST /api/channels/:id/export-metadata
router.post('/:id/export-metadata', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = req.params.id;
  const {
    target_dir,
    sort_key,
    sort_direction,
  } = req.body as {
    target_dir?: string;
    sort_key?: 'view_count' | 'like_count' | 'comment_count' | 'collect_count' | 'share_count' | 'like_rate' | 'comment_rate';
    sort_direction?: 'asc' | 'desc';
  };

  const channel = db.prepare('SELECT channel_id, title, handle, subscriber_count FROM channels WHERE channel_id = ?').get(channelId) as
    | { channel_id: string; title: string; handle: string | null; subscriber_count: number | null }
    | undefined;
  if (!channel) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }

  const defaultExportRoot = path.resolve(getSetting('download_root') || path.join(process.cwd(), 'downloads'), 'exports');
  const requestedRoot = (typeof target_dir === 'string' && target_dir.trim())
    ? path.resolve(target_dir.trim())
    : defaultExportRoot;

  try {
    fs.mkdirSync(requestedRoot, { recursive: true });
  } catch (err: any) {
    res.status(400).json({ error: `Failed to create export root: ${err?.message || 'unknown error'}` });
    return;
  }

  const now = new Date();
  const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}${String(now.getMinutes()).padStart(2, '0')}${String(now.getSeconds()).padStart(2, '0')}`;
  const fileBaseName = sanitizeFileName(`${channel.title}_${channel.channel_id}_insight_csv_${stamp}`);
  const exportDir = path.join(requestedRoot, fileBaseName);
  try {
    fs.mkdirSync(exportDir, { recursive: true });
  } catch (err: any) {
    res.status(500).json({ error: `Failed to create export folder: ${err?.message || 'unknown error'}` });
    return;
  }

  const rows = db.prepare(`
    SELECT v.*,
      COALESCE(vd_latest.view_count, v.view_count) as latest_views,
      COALESCE(vd_latest.like_count, v.like_count) as latest_likes,
      vd_latest.comment_count as latest_comments,
      COALESCE(vd_latest.collect_count, v.collect_count) as latest_collects,
      COALESCE(vd_latest.share_count, v.share_count) as latest_shares
    FROM videos v
    LEFT JOIN (
      SELECT video_id, view_count, like_count, comment_count, collect_count, share_count FROM video_daily
      WHERE date = (SELECT MAX(date) FROM video_daily vd2 WHERE vd2.video_id = video_daily.video_id)
    ) vd_latest ON vd_latest.video_id = v.video_id
    WHERE v.channel_id = ?
    ORDER BY date(COALESCE(v.published_at, v.created_at)) DESC, v.video_id DESC
  `).all(channelId) as any[];

  const tableRows: ExportMetaSummary[] = [];

  for (const row of rows) {
    const localMeta = readLocalMetaSummary(row.local_meta_path);
    const durationSec = toNullableInt(row.duration_sec);
    const timestamp = localMeta?.timestamp ?? parsePublishedAtToTimestamp(row.published_at);
    const viewCount = localMeta?.view_count ?? toNullableInt(row.latest_views) ?? toNullableInt(row.view_count);
    const likeCount = localMeta?.like_count ?? toNullableInt(row.latest_likes) ?? toNullableInt(row.like_count);
    const commentCount = localMeta?.comment_count ?? toNullableInt(row.latest_comments);
    const collectCount = localMeta?.collect_count ?? toNullableInt(row.latest_collects) ?? toNullableInt(row.collect_count);
    const shareCount = localMeta?.share_count ?? toNullableInt(row.latest_shares) ?? toNullableInt(row.share_count);
    const categories = localMeta?.categories ?? [];
    const likeRate = (viewCount && viewCount > 0 && likeCount != null) ? likeCount / viewCount : null;
    const commentRate = (viewCount && viewCount > 0 && commentCount != null) ? commentCount / viewCount : null;

    const summary: ExportMetaSummary = {
      id: String(row.video_id || ''),
      title: String(row.title || ''),
      description: typeof row.description === 'string' && row.description.trim() ? row.description.trim() : 'N/A',
      duration: formatDuration(durationSec),
      view_count: viewCount,
      like_count: likeCount,
      comment_count: commentCount,
      collect_count: collectCount,
      share_count: shareCount,
      categories: categories.join(' | '),
      timestamp,
      timestamp_beijing: formatBeijingDateTime(timestamp),
      upload_weekday: formatBeijingWeekday(timestamp),
      like_rate: formatPercent(likeRate),
      comment_rate: formatPercent(commentRate),
      like_rate_value: likeRate,
      comment_rate_value: commentRate,
    };
    tableRows.push(summary);
  }

  const sortKey = sort_key && ['view_count', 'like_count', 'comment_count', 'collect_count', 'share_count', 'like_rate', 'comment_rate'].includes(sort_key)
    ? sort_key
    : null;
  const sortDirection = sort_direction === 'asc' ? 'asc' : 'desc';

  const sortedRows = sortKey
    ? [...tableRows].sort((a, b) => {
        const av = resolveInsightSortValue(a, sortKey);
        const bv = resolveInsightSortValue(b, sortKey);
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return sortDirection === 'asc' ? av - bv : bv - av;
      })
    : tableRows;

  const totalVideos = sortedRows.length;
  const totalViews = sortedRows.reduce((acc, item) => acc + (item.view_count ?? 0), 0);
  const avgViews = totalVideos > 0 ? totalViews / totalVideos : 0;
  const timestamps = sortedRows.map((item) => item.timestamp).filter((item): item is number => item != null);
  const firstTs = timestamps.length > 0 ? Math.min(...timestamps) : null;
  const latestTs = timestamps.length > 0 ? Math.max(...timestamps) : null;

  let avgMonthlyUploads = totalVideos;
  if (firstTs != null && latestTs != null) {
    const firstDate = new Date(firstTs * 1000);
    const latestDate = new Date(latestTs * 1000);
    const monthSpan = Math.max(
      1,
      (latestDate.getUTCFullYear() - firstDate.getUTCFullYear()) * 12
        + (latestDate.getUTCMonth() - firstDate.getUTCMonth())
        + 1,
    );
    avgMonthlyUploads = totalVideos / monthSpan;
  }

  const titleTokenFreq = new Map<string, number>();
  const descTagFreq = new Map<string, number>();
  for (const row of rows) {
    for (const token of extractTitleTokens(row?.title)) {
      titleTokenFreq.set(token, (titleTokenFreq.get(token) || 0) + 1);
    }
    for (const tag of extractDescriptionTags(row)) {
      descTagFreq.set(tag, (descTagFreq.get(tag) || 0) + 1);
    }
  }

  const topTitleKeywords = topEntries(titleTokenFreq, 20);
  const topDescriptionTags = topEntries(descTagFreq, 20);
  const toCsv = (rows: Array<Array<string | number>>): string => {
    if (rows.length === 0) return '';
    return `${rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n')}\n`;
  };

  const overviewCsvPath = path.join(exportDir, 'overview.csv');
  const keywordsCsvPath = path.join(exportDir, 'title_keywords_top20.csv');
  const tagsCsvPath = path.join(exportDir, 'description_tags_top20.csv');
  const videosCsvPath = path.join(exportDir, 'video_insight_table.csv');

  const overviewRows: Array<Array<string | number>> = [
    ['指标', '值'],
    ['第一条视频发布时间', formatBeijingDateTime(firstTs)],
    ['总订阅数', channel.subscriber_count ?? 'N/A'],
    ['总视频数', totalVideos],
    ['总观看次数', totalViews],
    ['平均观看次数', Math.round(avgViews)],
    ['平均每月发布数', `${avgMonthlyUploads.toFixed(2)} 条/月`],
  ];

  const keywordRows: Array<Array<string | number>> = [
    ['排名', '词/词组', '频次'],
    ...(topTitleKeywords.length > 0
      ? topTitleKeywords.map((item, index) => [index + 1, item.term, item.count])
      : [['-', '无', 0]]),
  ];

  const tagRows: Array<Array<string | number>> = [
    ['排名', '标签', '频次'],
    ...(topDescriptionTags.length > 0
      ? topDescriptionTags.map((item, index) => [index + 1, item.term, item.count])
      : [['-', '无', 0]]),
  ];

  const videoTableRows: Array<Array<string | number>> = [
    ['视频ID', '标题', '描述', '时长', '播放量', '点赞数', '评论数', '收藏数', '分享数', '分类', '发布时间（北京时间）', '点赞率', '评论率', '上传星期'],
    ...(sortedRows.length > 0
      ? sortedRows.map((row) => [
          row.id,
          row.title,
          row.description,
          row.duration,
          row.view_count ?? 'N/A',
          row.like_count ?? 'N/A',
          row.comment_count ?? 'N/A',
          row.collect_count ?? 'N/A',
          row.share_count ?? 'N/A',
          row.categories || 'N/A',
          row.timestamp_beijing,
          row.like_rate,
          row.comment_rate,
          row.upload_weekday,
        ])
      : [['暂无数据', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-']]),
  ];

  fs.writeFileSync(overviewCsvPath, toCsv(overviewRows), 'utf8');
  fs.writeFileSync(keywordsCsvPath, toCsv(keywordRows), 'utf8');
  fs.writeFileSync(tagsCsvPath, toCsv(tagRows), 'utf8');
  fs.writeFileSync(videosCsvPath, toCsv(videoTableRows), 'utf8');

  res.json({
    success: true,
    channel_id: channel.channel_id,
    format: 'csv',
    export_dir: exportDir,
    total_videos: sortedRows.length,
    files: {
      overview_csv: overviewCsvPath,
      title_keywords_csv: keywordsCsvPath,
      description_tags_csv: tagsCsvPath,
      video_table_csv: videosCsvPath,
    },
  });
});

export default router;

