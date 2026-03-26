import fs from 'fs';
import path from 'path';
import { getSetting } from '../db.js';
import * as ytdlp from './ytdlp.js';

export interface SimilarMetaVideoRow {
  video_id: string;
  title: string;
  description: string | null;
  category: string | null;
  webpage_url: string;
  published_at: string | null;
  duration_sec: number | null;
  latest_views: number | null;
  latest_likes: number | null;
  latest_comments: number | null;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  channel_subscriber_count: number | null;
  channel_video_count: number | null;
  channel_id: string;
  channel_handle: string | null;
  channel_title: string;
  channel_tags_json: string;
  local_meta_path: string | null;
  local_thumb_path: string | null;
  local_thumb_url: string;
  published_days?: number | null;
  avg_daily_views?: number | null;
  freshness_boost?: number | null;
  interaction_rate?: number | null;
  pulse_score?: number | null;
  tier?: 'hot' | 'normal' | null;
  matched_rule_a?: boolean;
  matched_rule_b?: boolean;
}

export interface SimilarMetaBatchResult {
  data: SimilarMetaVideoRow[];
  total: number;
  success_count: number;
  metadata_success_count: number;
  before_dedupe_count: number;
  after_dedupe_count: number;
  failed: Array<{ link: string; error: string }>;
  probe_concurrency?: number;
  metadata_concurrency?: number;
  thumbnail_concurrency?: number;
  filter_mode?: string;
  filter_before_count?: number;
  filter_after_count?: number;
  filter_rule_a_count?: number;
  filter_rule_b_count?: number;
}

export interface SimilarMetaProgress {
  percent: number;
  stage: 'metadata' | 'thumbnail' | 'done';
  current_link: string;
}

interface SimilarMetaBatchOptions {
  onProgress?: (progress: SimilarMetaProgress) => void;
  cancelled?: () => boolean;
  abortSignal?: AbortSignal;
  concurrency?: number;
}

interface CandidateRow {
  order: number;
  link: string;
  videoId: string;
  channelId: string;
  channelHandle: string | null;
  channelTitle: string;
  videoCategory: string | null;
  channelSubscriberCount: number | null;
  channelVideoCount: number | null;
  durationSec: number | null;
  metaPath: string;
  meta: any;
  latestMeta: any;
  publishedDays?: number;
  avgDailyViews?: number;
  freshnessBoost?: number;
  interactionRate?: number;
  pulseScore?: number;
  tier?: 'hot' | 'normal';
  matchedRuleA?: boolean;
  matchedRuleB?: boolean;
}

interface ChannelProbeRow {
  order: number;
  link: string;
  videoId: string;
  channelId: string;
  channelHandleHint: string | null;
  channelTitleHint: string;
}

function extractVideoIdFromUrl(raw: string): string | null {
  const value = String(raw || '').trim();
  if (!value) return null;

  if (/^[A-Za-z0-9_-]{11}$/.test(value)) return value;

  let normalized = value;
  if (!/^https?:\/\//i.test(normalized) && normalized.startsWith('www.')) {
    normalized = `https://${normalized}`;
  }
  if (!/^https?:\/\//i.test(normalized)) return null;

  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.replace(/^www\./i, '').toLowerCase();
    if (host.endsWith('youtube.com')) {
      if (parsed.pathname === '/watch') {
        const id = parsed.searchParams.get('v') || '';
        return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
      }
      if (parsed.pathname.startsWith('/shorts/')) {
        const id = parsed.pathname.split('/')[2] || '';
        return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
      }
      if (parsed.pathname.startsWith('/live/')) {
        const id = parsed.pathname.split('/')[2] || '';
        return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
      }
      return null;
    }
    if (host === 'youtu.be') {
      const id = parsed.pathname.replace(/^\/+/, '').split('/')[0] || '';
      return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
    }
    return null;
  } catch {
    return null;
  }
}

function canonicalVideoUrl(raw: string): string | null {
  const id = extractVideoIdFromUrl(raw);
  if (!id) return null;
  return `https://www.youtube.com/watch?v=${id}`;
}

export function normalizeSimilarVideoLinks(links: string[]): string[] {
  return Array.from(
    new Set(
      (Array.isArray(links) ? links : [])
        .map((item) => canonicalVideoUrl(String(item || '')))
        .filter((item): item is string => Boolean(item)),
    ),
  );
}

function normalizeToolChannelId(rawChannelId: string | null | undefined, videoId: string): string {
  const value = String(rawChannelId || '').trim();
  if (value && /^[-_A-Za-z0-9@.]+$/.test(value)) {
    return value.replace(/^@+/, '');
  }
  return `tool_${videoId}`;
}

function toAssetsUrl(absPath: string, assetsRoot: string): string | null {
  const relative = path.relative(assetsRoot, path.resolve(absPath));
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) return null;
  const encoded = relative.split(path.sep).map((part) => encodeURIComponent(part)).join('/');
  return `/assets/${encoded}`;
}

function throwIfCancelled(cancelled?: () => boolean): void {
  if (cancelled && cancelled()) {
    throw new Error('Cancelled by user');
  }
}

function isCancelledResult(result: { errorCode?: string }): boolean {
  return String(result?.errorCode || '').toLowerCase() === 'cancelled';
}

function toNullableInt(value: unknown): number | null {
  if (value == null) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.trunc(num);
}

function toNullablePositiveInt(value: unknown): number | null {
  const num = toNullableInt(value);
  if (num == null) return null;
  return num > 0 ? num : null;
}

function extractChannelVideoCountFromInfo(info: any): number | null {
  const direct = toNullablePositiveInt(
    info?.channel_count
    ?? info?.channel_video_count
    ?? info?.playlist_count
    ?? info?.entry_count
    ?? info?.aweme_count
    ?? info?.author?.aweme_count
    ?? info?.author?.video_count,
  );
  if (direct != null) return direct;
  try {
    const parsed = ytdlp.parseChannelMeta(info);
    return toNullablePositiveInt(parsed?.video_count);
  } catch {
    return null;
  }
}

function normalizeHandleValue(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const value = raw.trim().replace(/^@+/, '');
  if (!value) return null;
  if (/^UC[0-9A-Za-z_-]{20,}$/i.test(value)) return null;
  if (/[/?#]/.test(value)) return null;
  return value;
}

function extractHandleFromUrl(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const value = raw.trim();
  if (!value) return null;
  const match = value.match(/\/@([^/?#]+)/i);
  if (!match?.[1]) return null;
  return normalizeHandleValue(decodeURIComponent(match[1]));
}

function extractChannelHandle(info: any): string | null {
  return (
    extractHandleFromUrl(info?.channel_url)
    || extractHandleFromUrl(info?.uploader_url)
    || normalizeHandleValue(info?.uploader_id)
    || normalizeHandleValue(info?.channel)
    || null
  );
}

function resolveVideoCategory(info: any): string | null {
  const direct = String(info?.category || '').trim();
  if (direct) return direct;

  if (Array.isArray(info?.categories)) {
    for (const item of info.categories) {
      const value = String(item || '').trim();
      if (value) return value;
    }
  }

  const genre = String(info?.genre || '').trim();
  if (genre) return genre;

  return null;
}

function buildChannelLookupUrl(info: any, channelId: string, channelHandle: string | null): string {
  const fromInfo = String(info?.channel_url || info?.uploader_url || '').trim();
  if (/^https?:\/\//i.test(fromInfo)) return fromInfo;

  const handle = String(channelHandle || '').trim().replace(/^@+/, '');
  if (handle) return `https://www.youtube.com/@${handle}`;

  const cid = String(channelId || '').trim();
  if (cid) return `https://www.youtube.com/channel/${cid}`;

  return '';
}

function getBatchConcurrency(total: number, preferred?: number): number {
  const fromInput = Number.isFinite(Number(preferred)) ? Number(preferred) : null;
  const raw = fromInput != null
    ? fromInput
    : parseInt(getSetting('download_job_concurrency') || getSetting('max_concurrency') || '2', 10);
  const base = Number.isFinite(raw) ? Math.trunc(raw) : 2;
  const normalized = Math.max(1, Math.min(16, base));
  return Math.max(1, Math.min(total, normalized));
}

function getStageConcurrency(total: number, settingKey: string, fallback: number): number {
  const raw = parseInt(getSetting(settingKey) || '', 10);
  const configured = Number.isFinite(raw) ? raw : fallback;
  const normalized = Math.max(1, Math.min(16, Math.trunc(configured)));
  return Math.max(1, Math.min(total, normalized));
}

function getSimilarMetaStepTimeoutMs(): number {
  const raw = parseInt(getSetting('tool_similar_meta_timeout_sec') || '120', 10);
  const sec = Number.isFinite(raw) ? raw : 120;
  return Math.max(15, Math.min(900, sec)) * 1000;
}

interface PulseMetrics {
  publishedDays: number;
  withinRecent3Months: boolean;
  avgDailyViews: number;
  freshnessBoost: number;
  interactionRate: number;
  pulseScore: number;
  matchRuleA: boolean;
  matchRuleB: boolean;
  tier: 'hot' | 'normal';
}

function getSimilarContentPulseScoreThreshold(): number {
  const raw = Number(getSetting('tool_similar_content_pulse_score_threshold') || '800');
  if (!Number.isFinite(raw)) return 800;
  return Math.max(0, Math.min(10_000_000, raw));
}

function resolvePublishedDays(publishedAt: string | null | undefined): number {
  const text = String(publishedAt || '').trim();
  if (!text) return 3650;
  const normalized = text.includes('T') ? text : `${text}T00:00:00Z`;
  const timestamp = Date.parse(normalized);
  if (!Number.isFinite(timestamp)) return 3650;
  const diffMs = Date.now() - timestamp;
  if (!Number.isFinite(diffMs)) return 3650;
  if (diffMs <= 0) return 1;
  return Math.max(1, Math.floor(diffMs / 86_400_000));
}

function parsePublishedTimestampMs(publishedAt: string | null | undefined): number | null {
  const text = String(publishedAt || '').trim();
  if (!text) return null;
  const normalized = text.includes('T') ? text : `${text}T00:00:00Z`;
  const timestamp = Date.parse(normalized);
  if (!Number.isFinite(timestamp)) return null;
  return timestamp;
}

function isWithinRecentMonths(publishedAt: string | null | undefined, months: number): boolean {
  const ts = parsePublishedTimestampMs(publishedAt);
  if (ts == null) return false;
  const now = new Date();
  const cutoff = new Date(now.getTime());
  cutoff.setMonth(cutoff.getMonth() - Math.max(1, Math.trunc(months)));
  return ts >= cutoff.getTime();
}

function computePulseMetrics(meta: {
  published_at: string | null;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
}, scoreThreshold: number): PulseMetrics | null {
  const views = toNullablePositiveInt(meta.view_count);
  if (views == null || views <= 0) return null;
  const likes = Math.max(0, toNullableInt(meta.like_count) || 0);
  const comments = Math.max(0, toNullableInt(meta.comment_count) || 0);
  const publishedDays = resolvePublishedDays(meta.published_at);
  const withinRecent3Months = isWithinRecentMonths(meta.published_at, 3);
  const avgDailyViews = views / Math.max(1, publishedDays);
  const freshnessBoost = 1 / Math.log2(publishedDays + 2);
  const interactionRate = (likes + comments * 2) / views;
  const pulseScore = avgDailyViews * freshnessBoost * (1 + interactionRate);
  const matchRuleA = pulseScore >= scoreThreshold;
  const matchRuleB = avgDailyViews > 3000 || (publishedDays <= 7 && avgDailyViews > 5000);
  return {
    publishedDays,
    withinRecent3Months,
    avgDailyViews,
    freshnessBoost,
    interactionRate,
    pulseScore,
    matchRuleA,
    matchRuleB,
    tier: matchRuleB ? 'hot' : 'normal',
  };
}

async function withAbortTimeout<T>(
  runner: (signal: AbortSignal) => Promise<T>,
  parentSignal: AbortSignal | undefined,
  timeoutMs: number,
  stepName: string,
): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  let timer: NodeJS.Timeout | null = null;
  const onParentAbort = () => {
    try {
      controller.abort();
    } catch {}
  };

  if (parentSignal) {
    if (parentSignal.aborted) {
      onParentAbort();
    } else {
      parentSignal.addEventListener('abort', onParentAbort, { once: true });
    }
  }

  if (timeoutMs > 0) {
    timer = setTimeout(() => {
      timedOut = true;
      onParentAbort();
    }, timeoutMs);
  }

  try {
    return await runner(controller.signal);
  } catch (err: any) {
    if (timedOut) {
      throw new Error(`${stepName} timed out after ${Math.round(timeoutMs / 1000)}s`);
    }
    throw err;
  } finally {
    if (timer) clearTimeout(timer);
    if (parentSignal) {
      parentSignal.removeEventListener('abort', onParentAbort);
    }
  }
}

async function runWithConcurrency(
  total: number,
  concurrency: number,
  worker: (index: number) => Promise<void>,
  cancelled?: () => boolean,
): Promise<void> {
  if (total <= 0) return;

  let cursor = 0;
  const takeNext = (): number | null => {
    if (cursor >= total) return null;
    const index = cursor;
    cursor += 1;
    return index;
  };

  const workers = Array.from({ length: Math.max(1, Math.min(total, concurrency)) }, async () => {
    while (true) {
      throwIfCancelled(cancelled);
      const index = takeNext();
      if (index == null) return;
      await worker(index);
    }
  });

  await Promise.all(workers);
}

export async function downloadSimilarChannelMetaBatch(
  normalizedLinks: string[],
  options: SimilarMetaBatchOptions = {},
): Promise<SimilarMetaBatchResult> {
  const links = normalizeSimilarVideoLinks(normalizedLinks);
  if (links.length === 0) {
    return {
      data: [],
      total: 0,
      success_count: 0,
      metadata_success_count: 0,
      before_dedupe_count: 0,
      after_dedupe_count: 0,
      failed: [],
    };
  }

  const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');
  const assetsRoot = path.resolve(downloadRoot, 'assets');
  const batchConcurrency = getBatchConcurrency(links.length, options.concurrency);
  const probeConcurrency = getStageConcurrency(
    links.length,
    'tool_similar_probe_concurrency',
    Math.max(batchConcurrency, Math.min(16, batchConcurrency * 3)),
  );
  const stepTimeoutMs = getSimilarMetaStepTimeoutMs();

  const failed: Array<{ link: string; error: string }> = [];
  let probeSuccessCount = 0;
  let metadataSuccessCount = 0;

  const channelKeyFrom = (rawChannelId: unknown, rawChannelTitle: unknown): string => {
    const cid = String(rawChannelId || '').trim().replace(/^@+/, '').toLowerCase();
    if (cid) return `id:${cid}`;
    const ctitle = String(rawChannelTitle || '').trim().toLowerCase();
    if (ctitle) return `title:${ctitle}`;
    return '';
  };

  const probeByOrder: Array<ChannelProbeRow | null> = new Array(links.length).fill(null);
  let probeDone = 0;
  await runWithConcurrency(
    links.length,
    probeConcurrency,
    async (i: number) => {
      throwIfCancelled(options.cancelled);
      const link = links[i];
      const videoId = extractVideoIdFromUrl(link);

      if (!videoId) {
        failed.push({ link, error: 'Failed to parse video id' });
      } else {
        try {
          const probeResult = await withAbortTimeout(
            (abortSignal) => ytdlp.getVideoChannelIdentity(videoId, { abortSignal }),
            options.abortSignal,
            stepTimeoutMs,
            'probeChannelId',
          );
          if (isCancelledResult(probeResult)) throw new Error('Cancelled by user');
          if (!probeResult.success || !probeResult.data) {
            throw new Error(probeResult.error || 'Failed to probe channel id');
          }
          const probed = probeResult.data || {};
          const channelId = normalizeToolChannelId(probed?.channel_id || null, videoId);
          const channelTitleHint = String(probed?.channel || probed?.uploader || channelId);
          const channelHandleHint = extractChannelHandle(probed) || null;
          probeSuccessCount += 1;
          probeByOrder[i] = {
            order: i,
            link,
            videoId,
            channelId,
            channelHandleHint,
            channelTitleHint,
          };
        } catch (err: any) {
          const message = String(err?.message || '').toLowerCase();
          if (message.includes('cancelled')) throw err;
          failed.push({
            link,
            error: err?.message || 'Metadata download failed',
          });
        }
      }

      probeDone += 1;
      options.onProgress?.({
        percent: Math.min(35, (probeDone / Math.max(1, links.length)) * 35),
        stage: 'metadata',
        current_link: link,
      });
    },
    options.cancelled,
  );

  const channelCandidateMap = new Map<string, ChannelProbeRow>();
  for (const item of probeByOrder) {
    if (!item) continue;
    const channelKey = channelKeyFrom(item.channelId, item.channelTitleHint) || `video:${item.videoId}`;
    if (!channelCandidateMap.has(channelKey)) {
      channelCandidateMap.set(channelKey, item);
    }
  }

  const dedupedCandidates = Array.from(channelCandidateMap.values()).sort((a, b) => a.order - b.order);
  const metadataByOrder: Array<CandidateRow | null> = new Array(dedupedCandidates.length).fill(null);

  let metadataDone = 0;
  await runWithConcurrency(
    dedupedCandidates.length,
    getStageConcurrency(
      dedupedCandidates.length,
      'tool_similar_metadata_concurrency',
      Math.max(1, Math.min(batchConcurrency, dedupedCandidates.length || 1)),
    ),
    async (i: number) => {
      throwIfCancelled(options.cancelled);
      const item = dedupedCandidates[i];
      try {
        const infoResult = await withAbortTimeout(
          (abortSignal) => ytdlp.getVideoInfo(item.videoId, { abortSignal }),
          options.abortSignal,
          stepTimeoutMs,
          'getVideoInfo',
        );
        if (isCancelledResult(infoResult)) throw new Error('Cancelled by user');
        if (!infoResult.success || !infoResult.data) {
          throw new Error(infoResult.error || 'Failed to fetch video info');
        }

        const meta = ytdlp.parseVideoMeta(infoResult.data);
        const channelId = normalizeToolChannelId(meta.channel_id || item.channelId, item.videoId);
        const metaPath = path.join(downloadRoot, 'assets', 'meta', channelId, item.videoId, `${item.videoId}.info.json`);
        const metaDir = path.dirname(metaPath);
        if (!fs.existsSync(metaDir)) {
          fs.mkdirSync(metaDir, { recursive: true });
        }
        fs.writeFileSync(metaPath, JSON.stringify(infoResult.data, null, 2), 'utf-8');

        const latestInfo: any = infoResult.data;
        const latestMeta = ytdlp.parseVideoMeta(latestInfo);
        const videoCategory = resolveVideoCategory(latestInfo);
        const channelHandle = extractChannelHandle(latestInfo) || item.channelHandleHint;
        const channelTitle = String(latestInfo?.channel || latestInfo?.uploader || item.channelTitleHint || channelId);
        const durationSec = toNullableInt(latestMeta.duration_sec ?? meta.duration_sec);
        let channelSubscriberCount = toNullablePositiveInt(
          latestInfo?.channel_follower_count
          ?? latestInfo?.uploader_follower_count
          ?? latestInfo?.subscriber_count
          ?? infoResult.data?.channel_follower_count
          ?? infoResult.data?.uploader_follower_count
          ?? infoResult.data?.subscriber_count
        );
        let channelVideoCount = extractChannelVideoCountFromInfo(latestInfo);
        if (channelSubscriberCount == null || channelVideoCount == null) {
          const channelLookupUrl = buildChannelLookupUrl(latestInfo, channelId, channelHandle);
          if (channelLookupUrl) {
            try {
              const channelInfoResult = await withAbortTimeout(
                (abortSignal) => ytdlp.getChannelInfo(channelLookupUrl, { abortSignal }),
                options.abortSignal,
                stepTimeoutMs,
                'getChannelInfo',
              );
              if (channelInfoResult.success && channelInfoResult.data) {
                if (channelSubscriberCount == null) {
                  channelSubscriberCount = toNullablePositiveInt(
                    channelInfoResult.data?.channel_follower_count
                    ?? channelInfoResult.data?.uploader_follower_count
                    ?? channelInfoResult.data?.subscriber_count
                  );
                }
                if (channelVideoCount == null) {
                  channelVideoCount = extractChannelVideoCountFromInfo(channelInfoResult.data);
                }
              }
            } catch {
              // Ignore channel-level fallback failures and keep unknown channel counts.
            }
          }
        }

        metadataSuccessCount += 1;
        metadataByOrder[i] = {
          order: item.order,
          link: item.link,
          videoId: item.videoId,
          channelId,
          channelHandle,
          channelTitle,
          videoCategory,
          channelSubscriberCount,
          channelVideoCount,
          durationSec,
          metaPath,
          meta,
          latestMeta,
        };
      } catch (err: any) {
        const message = String(err?.message || '').toLowerCase();
        if (message.includes('cancelled')) throw err;
        failed.push({
          link: item.link,
          error: err?.message || 'Metadata download failed',
        });
      }

      metadataDone += 1;
      options.onProgress?.({
        percent: 35 + (metadataDone / Math.max(1, dedupedCandidates.length)) * 35,
        stage: 'metadata',
        current_link: item.link,
      });
    },
    options.cancelled,
  );

  const candidates = metadataByOrder.filter((item): item is CandidateRow => Boolean(item)).sort((a, b) => a.order - b.order);
  const thumbsTotal = Math.max(1, candidates.length);
  const videosByOrder: Array<{ order: number; row: SimilarMetaVideoRow }> = [];

  let thumbDone = 0;
  await runWithConcurrency(
    candidates.length,
    getStageConcurrency(
      candidates.length,
      'tool_similar_thumbnail_concurrency',
      Math.max(1, Math.min(batchConcurrency, candidates.length || 1)),
    ),
    async (i: number) => {
      throwIfCancelled(options.cancelled);
      const item = candidates[i];
      const thumbPath = path.join(downloadRoot, 'assets', 'thumbs', item.channelId, item.videoId, `${item.videoId}.jpg`);
      let localThumbUrl: string | null = null;

      try {
        const thumbResult = await withAbortTimeout(
          (abortSignal) => ytdlp.downloadThumb(item.videoId, item.channelId, { abortSignal }),
          options.abortSignal,
          stepTimeoutMs,
          'downloadThumb',
        );
        if (isCancelledResult(thumbResult)) throw new Error('Cancelled by user');
        if (!thumbResult.success) {
          failed.push({
            link: item.link,
            error: thumbResult.error || 'Thumbnail download failed',
          });
        }
      } catch (err: any) {
        const message = String(err?.message || '').toLowerCase();
        if (message.includes('cancelled')) throw err;
        failed.push({
          link: item.link,
          error: err?.message || 'Thumbnail download failed',
        });
      }

      if (fs.existsSync(thumbPath)) {
        localThumbUrl = toAssetsUrl(thumbPath, assetsRoot);
      }

      videosByOrder.push({
        order: item.order,
        row: {
          video_id: item.videoId,
          title: item.latestMeta.title || item.meta.title || item.videoId,
          description: item.latestMeta.description || item.meta.description || null,
          category: item.videoCategory,
          webpage_url: item.latestMeta.webpage_url || canonicalVideoUrl(item.link) || item.link,
          published_at: item.latestMeta.published_at,
          duration_sec: item.durationSec,
          latest_views: item.latestMeta.view_count,
          latest_likes: item.latestMeta.like_count,
          latest_comments: item.latestMeta.comment_count,
          view_count: item.latestMeta.view_count,
          like_count: item.latestMeta.like_count,
          comment_count: item.latestMeta.comment_count,
          channel_subscriber_count: item.channelSubscriberCount,
          channel_video_count: item.channelVideoCount,
          channel_id: item.channelId,
          channel_handle: item.channelHandle,
          channel_title: item.channelTitle,
          channel_tags_json: '[]',
          local_meta_path: fs.existsSync(item.metaPath) ? item.metaPath : null,
          local_thumb_path: fs.existsSync(thumbPath) ? thumbPath : null,
          local_thumb_url: localThumbUrl || `https://i.ytimg.com/vi/${item.videoId}/mqdefault.jpg`,
        },
      });

      thumbDone += 1;
      options.onProgress?.({
        percent: 70 + (thumbDone / thumbsTotal) * 30,
        stage: 'thumbnail',
        current_link: item.link,
      });
    },
    options.cancelled,
  );

  const videos = videosByOrder
    .sort((a, b) => a.order - b.order)
    .map((item) => item.row);

  options.onProgress?.({
    percent: 100,
    stage: 'done',
    current_link: '',
  });

  return {
    data: videos,
    total: links.length,
    success_count: videos.length,
    metadata_success_count: metadataSuccessCount,
    before_dedupe_count: probeSuccessCount,
    after_dedupe_count: dedupedCandidates.length,
    failed,
    probe_concurrency: probeConcurrency,
    metadata_concurrency: getStageConcurrency(
      Math.max(1, dedupedCandidates.length),
      'tool_similar_metadata_concurrency',
      Math.max(1, Math.min(batchConcurrency, dedupedCandidates.length || 1)),
    ),
    thumbnail_concurrency: getStageConcurrency(
      Math.max(1, candidates.length),
      'tool_similar_thumbnail_concurrency',
      Math.max(1, Math.min(batchConcurrency, candidates.length || 1)),
    ),
  };
}

export async function downloadSimilarContentMetaBatch(
  normalizedLinks: string[],
  options: SimilarMetaBatchOptions = {},
): Promise<SimilarMetaBatchResult> {
  const links = normalizeSimilarVideoLinks(normalizedLinks);
  if (links.length === 0) {
    return {
      data: [],
      total: 0,
      success_count: 0,
      metadata_success_count: 0,
      before_dedupe_count: 0,
      after_dedupe_count: 0,
      failed: [],
      filter_mode: 'pulse_or_hot',
      filter_before_count: 0,
      filter_after_count: 0,
      filter_rule_a_count: 0,
      filter_rule_b_count: 0,
    };
  }

  const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');
  const assetsRoot = path.resolve(downloadRoot, 'assets');
  const batchConcurrency = getBatchConcurrency(links.length, options.concurrency);
  const probeConcurrency = getStageConcurrency(
    links.length,
    'tool_similar_content_probe_concurrency',
    Math.max(batchConcurrency, Math.min(16, batchConcurrency * 3)),
  );
  const metadataConcurrency = getStageConcurrency(
    links.length,
    'tool_similar_content_metadata_concurrency',
    Math.max(1, Math.min(batchConcurrency, links.length)),
  );
  const stepTimeoutMs = getSimilarMetaStepTimeoutMs();
  const pulseScoreThreshold = getSimilarContentPulseScoreThreshold();

  const failed: Array<{ link: string; error: string }> = [];
  let probeSuccessCount = 0;
  let metadataSuccessCount = 0;
  let filterRuleACount = 0;
  let filterRuleBCount = 0;

  const probeByOrder: Array<ChannelProbeRow | null> = new Array(links.length).fill(null);
  let probeDone = 0;
  await runWithConcurrency(
    links.length,
    probeConcurrency,
    async (i: number) => {
      throwIfCancelled(options.cancelled);
      const link = links[i];
      const videoId = extractVideoIdFromUrl(link);

      if (!videoId) {
        failed.push({ link, error: 'Failed to parse video id' });
      } else {
        try {
          const probeResult = await withAbortTimeout(
            (abortSignal) => ytdlp.getVideoChannelIdentity(videoId, { abortSignal }),
            options.abortSignal,
            stepTimeoutMs,
            'probeChannelId',
          );
          if (isCancelledResult(probeResult)) throw new Error('Cancelled by user');
          if (!probeResult.success || !probeResult.data) {
            throw new Error(probeResult.error || 'Failed to probe channel id');
          }
          const probed = probeResult.data || {};
          const channelId = normalizeToolChannelId(probed?.channel_id || null, videoId);
          const channelTitleHint = String(probed?.channel || probed?.uploader || channelId);
          const channelHandleHint = extractChannelHandle(probed) || null;
          probeSuccessCount += 1;
          probeByOrder[i] = {
            order: i,
            link,
            videoId,
            channelId,
            channelHandleHint,
            channelTitleHint,
          };
        } catch (err: any) {
          const message = String(err?.message || '').toLowerCase();
          if (message.includes('cancelled')) throw err;
          failed.push({
            link,
            error: err?.message || 'Metadata download failed',
          });
        }
      }

      probeDone += 1;
      options.onProgress?.({
        percent: Math.min(35, (probeDone / Math.max(1, links.length)) * 35),
        stage: 'metadata',
        current_link: link,
      });
    },
    options.cancelled,
  );

  const probeCandidates = probeByOrder
    .filter((item): item is ChannelProbeRow => Boolean(item))
    .sort((a, b) => a.order - b.order);
  const metadataByOrder: Array<CandidateRow | null> = new Array(probeCandidates.length).fill(null);

  let metadataDone = 0;
  await runWithConcurrency(
    probeCandidates.length,
    metadataConcurrency,
    async (i: number) => {
      throwIfCancelled(options.cancelled);
      const item = probeCandidates[i];
      try {
        const infoResult = await withAbortTimeout(
          (abortSignal) => ytdlp.getVideoInfo(item.videoId, { abortSignal }),
          options.abortSignal,
          stepTimeoutMs,
          'getVideoInfo',
        );
        if (isCancelledResult(infoResult)) throw new Error('Cancelled by user');
        if (!infoResult.success || !infoResult.data) {
          throw new Error(infoResult.error || 'Failed to fetch video info');
        }

        const latestInfo: any = infoResult.data;
        const latestMeta = ytdlp.parseVideoMeta(latestInfo);
        metadataSuccessCount += 1;

        const pulseMetrics = computePulseMetrics({
          published_at: latestMeta.published_at,
          view_count: latestMeta.view_count,
          like_count: latestMeta.like_count,
          comment_count: latestMeta.comment_count,
        }, pulseScoreThreshold);
        if (!pulseMetrics?.withinRecent3Months) {
          metadataDone += 1;
          options.onProgress?.({
            percent: 35 + (metadataDone / Math.max(1, probeCandidates.length)) * 35,
            stage: 'metadata',
            current_link: item.link,
          });
          return;
        }
        const matchRuleA = Boolean(pulseMetrics?.matchRuleA);
        const matchRuleB = Boolean(pulseMetrics?.matchRuleB);

        if (!matchRuleA && !matchRuleB) {
          metadataDone += 1;
          options.onProgress?.({
            percent: 35 + (metadataDone / Math.max(1, probeCandidates.length)) * 35,
            stage: 'metadata',
            current_link: item.link,
          });
          return;
        }

        if (matchRuleA) filterRuleACount += 1;
        if (matchRuleB) filterRuleBCount += 1;

        const meta = ytdlp.parseVideoMeta(infoResult.data);
        const channelId = normalizeToolChannelId(meta.channel_id || item.channelId, item.videoId);
        const metaPath = path.join(downloadRoot, 'assets', 'meta', channelId, item.videoId, `${item.videoId}.info.json`);
        const metaDir = path.dirname(metaPath);
        if (!fs.existsSync(metaDir)) {
          fs.mkdirSync(metaDir, { recursive: true });
        }
        fs.writeFileSync(metaPath, JSON.stringify(infoResult.data, null, 2), 'utf-8');

        const channelHandle = extractChannelHandle(latestInfo) || item.channelHandleHint;
        const channelTitle = String(latestInfo?.channel || latestInfo?.uploader || item.channelTitleHint || channelId);
        const videoCategory = resolveVideoCategory(latestInfo);
        const durationSec = toNullableInt(latestMeta.duration_sec ?? meta.duration_sec);
        let channelSubscriberCount = toNullablePositiveInt(
          latestInfo?.channel_follower_count
          ?? latestInfo?.uploader_follower_count
          ?? latestInfo?.subscriber_count
          ?? infoResult.data?.channel_follower_count
          ?? infoResult.data?.uploader_follower_count
          ?? infoResult.data?.subscriber_count,
        );
        let channelVideoCount = extractChannelVideoCountFromInfo(latestInfo);
        if (channelSubscriberCount == null || channelVideoCount == null) {
          const channelLookupUrl = buildChannelLookupUrl(latestInfo, channelId, channelHandle);
          if (channelLookupUrl) {
            try {
              const channelInfoResult = await withAbortTimeout(
                (abortSignal) => ytdlp.getChannelInfo(channelLookupUrl, { abortSignal }),
                options.abortSignal,
                stepTimeoutMs,
                'getChannelInfo',
              );
              if (channelInfoResult.success && channelInfoResult.data) {
                if (channelSubscriberCount == null) {
                  channelSubscriberCount = toNullablePositiveInt(
                    channelInfoResult.data?.channel_follower_count
                    ?? channelInfoResult.data?.uploader_follower_count
                    ?? channelInfoResult.data?.subscriber_count
                  );
                }
                if (channelVideoCount == null) {
                  channelVideoCount = extractChannelVideoCountFromInfo(channelInfoResult.data);
                }
              }
            } catch {
              // Ignore channel-level fallback failures and keep unknown channel counts.
            }
          }
        }

        metadataByOrder[i] = {
          order: item.order,
          link: item.link,
          videoId: item.videoId,
          channelId,
          channelHandle,
          channelTitle,
          videoCategory,
          channelSubscriberCount,
          channelVideoCount,
          durationSec,
          metaPath,
          meta,
          latestMeta,
          publishedDays: pulseMetrics?.publishedDays,
          avgDailyViews: pulseMetrics?.avgDailyViews,
          freshnessBoost: pulseMetrics?.freshnessBoost,
          interactionRate: pulseMetrics?.interactionRate,
          pulseScore: pulseMetrics?.pulseScore,
          tier: pulseMetrics?.tier || 'normal',
          matchedRuleA: matchRuleA,
          matchedRuleB: matchRuleB,
        };
      } catch (err: any) {
        const message = String(err?.message || '').toLowerCase();
        if (message.includes('cancelled')) throw err;
        failed.push({
          link: item.link,
          error: err?.message || 'Metadata download failed',
        });
      }

      metadataDone += 1;
      options.onProgress?.({
        percent: 35 + (metadataDone / Math.max(1, probeCandidates.length)) * 35,
        stage: 'metadata',
        current_link: item.link,
      });
    },
    options.cancelled,
  );

  const candidates = metadataByOrder
    .filter((item): item is CandidateRow => Boolean(item))
    .sort((a, b) => a.order - b.order);
  const thumbsTotal = Math.max(1, candidates.length);
  const videosByOrder: Array<{ order: number; row: SimilarMetaVideoRow }> = [];
  const thumbnailConcurrency = getStageConcurrency(
    Math.max(1, candidates.length),
    'tool_similar_content_thumbnail_concurrency',
    Math.max(1, Math.min(batchConcurrency, candidates.length || 1)),
  );

  let thumbDone = 0;
  await runWithConcurrency(
    candidates.length,
    thumbnailConcurrency,
    async (i: number) => {
      throwIfCancelled(options.cancelled);
      const item = candidates[i];
      const thumbPath = path.join(downloadRoot, 'assets', 'thumbs', item.channelId, item.videoId, `${item.videoId}.jpg`);
      let localThumbUrl: string | null = null;

      try {
        const thumbResult = await withAbortTimeout(
          (abortSignal) => ytdlp.downloadThumb(item.videoId, item.channelId, { abortSignal }),
          options.abortSignal,
          stepTimeoutMs,
          'downloadThumb',
        );
        if (isCancelledResult(thumbResult)) throw new Error('Cancelled by user');
        if (!thumbResult.success) {
          failed.push({
            link: item.link,
            error: thumbResult.error || 'Thumbnail download failed',
          });
        }
      } catch (err: any) {
        const message = String(err?.message || '').toLowerCase();
        if (message.includes('cancelled')) throw err;
        failed.push({
          link: item.link,
          error: err?.message || 'Thumbnail download failed',
        });
      }

      if (fs.existsSync(thumbPath)) {
        localThumbUrl = toAssetsUrl(thumbPath, assetsRoot);
      }

      videosByOrder.push({
        order: item.order,
        row: {
          video_id: item.videoId,
          title: item.latestMeta.title || item.meta.title || item.videoId,
          description: item.latestMeta.description || item.meta.description || null,
          category: item.videoCategory,
          webpage_url: item.latestMeta.webpage_url || canonicalVideoUrl(item.link) || item.link,
          published_at: item.latestMeta.published_at,
          duration_sec: item.durationSec,
          latest_views: item.latestMeta.view_count,
          latest_likes: item.latestMeta.like_count,
          latest_comments: item.latestMeta.comment_count,
          view_count: item.latestMeta.view_count,
          like_count: item.latestMeta.like_count,
          comment_count: item.latestMeta.comment_count,
          channel_subscriber_count: item.channelSubscriberCount,
          channel_video_count: item.channelVideoCount,
          channel_id: item.channelId,
          channel_handle: item.channelHandle,
          channel_title: item.channelTitle,
          channel_tags_json: '[]',
          local_meta_path: fs.existsSync(item.metaPath) ? item.metaPath : null,
          local_thumb_path: fs.existsSync(thumbPath) ? thumbPath : null,
          local_thumb_url: localThumbUrl || `https://i.ytimg.com/vi/${item.videoId}/mqdefault.jpg`,
          published_days: item.publishedDays ?? null,
          avg_daily_views: item.avgDailyViews ?? null,
          freshness_boost: item.freshnessBoost ?? null,
          interaction_rate: item.interactionRate ?? null,
          pulse_score: item.pulseScore ?? null,
          tier: item.tier || 'normal',
          matched_rule_a: Boolean(item.matchedRuleA),
          matched_rule_b: Boolean(item.matchedRuleB),
        },
      });

      thumbDone += 1;
      options.onProgress?.({
        percent: 70 + (thumbDone / thumbsTotal) * 30,
        stage: 'thumbnail',
        current_link: item.link,
      });
    },
    options.cancelled,
  );

  const videos = videosByOrder
    .sort((a, b) => a.order - b.order)
    .map((item) => item.row);

  options.onProgress?.({
    percent: 100,
    stage: 'done',
    current_link: '',
  });

  return {
    data: videos,
    total: links.length,
    success_count: videos.length,
    metadata_success_count: metadataSuccessCount,
    before_dedupe_count: probeSuccessCount,
    after_dedupe_count: candidates.length,
    failed,
    filter_mode: 'pulse_or_hot',
    filter_before_count: probeSuccessCount,
    filter_after_count: candidates.length,
    filter_rule_a_count: filterRuleACount,
    filter_rule_b_count: filterRuleBCount,
    probe_concurrency: probeConcurrency,
    metadata_concurrency: metadataConcurrency,
    thumbnail_concurrency: thumbnailConcurrency,
  };
}
