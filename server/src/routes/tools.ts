import { Router, Request, Response } from "express";
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { v4 as uuidv4 } from "uuid";
import { getDb, getSetting } from "../db.js";
import { normalizeSimilarVideoLinks } from "../services/toolsSimilar.js";
import {
  fetchChannelSnapshotFromApi,
  fetchHotVideosByKeyword,
  getYoutubeApiUsageStatus,
} from "../services/youtubeApi.js";
import * as ytdlp from "../services/ytdlp.js";

const router = Router();

interface SimilarLinkRow {
  link: string;
  count: number;
}

interface ToolMetaHydrateRow {
  video_id: string;
  category?: string | null;
  channel_id?: string | null;
  channel_handle?: string | null;
  channel_title?: string | null;
  webpage_url?: string | null;
  local_meta_path?: string | null;
  local_thumb_path?: string | null;
  duration_sec?: number | null;
  channel_subscriber_count?: number | null;
  channel_video_count?: number | null;
  channel_first_video_published_at?: string | null;
}

interface SimilarScriptPayload {
  success: boolean;
  data?: SimilarLinkRow[];
  seed_count?: number;
  related_raw_count?: number;
  error?: string;
}

interface SimilarScrapeOptions {
  browser_api_url: string;
  browser_window_id: string;
  browser_window_ids: string[];
  pre_delay_min: number;
  pre_delay_max: number;
  post_delay_min: number;
  post_delay_max: number;
  retries: number;
  retry_backoff_base: number;
  retry_jitter_max: number;
}

const DEFAULT_SCRAPE_OPTIONS: SimilarScrapeOptions = {
  browser_api_url: "http://127.0.0.1:54345",
  browser_window_id: "2eca47c2be5144088d67d631df96fc89",
  browser_window_ids: ["2eca47c2be5144088d67d631df96fc89"],
  pre_delay_min: 1.0,
  pre_delay_max: 2.2,
  post_delay_min: 2.0,
  post_delay_max: 3.8,
  retries: 3,
  retry_backoff_base: 1.5,
  retry_jitter_max: 0.8,
};

interface HotShortsScrapeOptions {
  browser_api_url: string;
  browser_window_id: string;
  browser_window_ids: string[];
  max_scroll: number;
  scroll_delay_min: number;
  scroll_delay_max: number;
  retries: number;
  retry_backoff_base: number;
  retry_jitter_max: number;
  human_scroll: boolean;
}

interface HotShortsVideoRow {
  id: string;
  title: string;
  channel: string;
  views_text: string;
  views: number;
  publish_text: string;
  duration_text: string;
  duration_sec: number;
  thumbnail: string;
}

const TOOL_JOB_TYPE_SIMILAR_CHANNEL = "tool_download_meta";
const TOOL_JOB_TYPE_SIMILAR_CONTENT = "tool_download_meta_content";

function toStringArray(input: unknown): string[] {
  if (!Array.isArray(input)) return [];
  return input
    .map((item: unknown) => String(item || "").trim())
    .filter((item: string) => Boolean(item));
}

function toNullableInt(value: unknown): number | null {
  if (value == null) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.trunc(num);
}

function toNullablePositiveInt(value: unknown): number | null {
  const num = toNullableInt(value);
  if (num == null) return null;
  return num > 0 ? num : null;
}

function parseToolMetaHydrateRows(input: unknown): ToolMetaHydrateRow[] {
  if (!Array.isArray(input)) return [];
  return input
    .map((item: unknown) => {
      const row =
        item && typeof item === "object"
          ? (item as Record<string, unknown>)
          : {};
      return {
        video_id: String(row.video_id || "").trim(),
        category: String(row.category || "").trim() || null,
        channel_id: String(row.channel_id || "").trim() || null,
        channel_handle: String(row.channel_handle || "").trim() || null,
        channel_title: String(row.channel_title || "").trim() || null,
        webpage_url: String(row.webpage_url || "").trim() || null,
        local_meta_path: String(row.local_meta_path || "").trim() || null,
        local_thumb_path: String(row.local_thumb_path || "").trim() || null,
        duration_sec: toNullableInt(row.duration_sec),
        channel_subscriber_count: toNullablePositiveInt(
          row.channel_subscriber_count,
        ),
        channel_video_count: toNullablePositiveInt(row.channel_video_count),
        channel_first_video_published_at:
          String(row.channel_first_video_published_at || "").trim() || null,
      };
    })
    .filter((row: ToolMetaHydrateRow) => Boolean(row.video_id));
}

function resolveToolMetaFilePath(row: ToolMetaHydrateRow): string | null {
  const fromRow = String(row.local_meta_path || "").trim();
  if (fromRow && fs.existsSync(fromRow)) return fromRow;
  const channelId = String(row.channel_id || "").trim();
  const videoId = String(row.video_id || "").trim();
  if (!channelId || !videoId) return null;
  const root =
    getSetting("download_root") || path.join(process.cwd(), "..", "downloads");
  const fallback = path.join(
    root,
    "assets",
    "meta",
    channelId,
    videoId,
    `${videoId}.info.json`,
  );
  return fs.existsSync(fallback) ? fallback : null;
}

function normalizeHandleValue(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const value = raw.trim().replace(/^@+/, "");
  if (!value) return null;
  if (/^UC[0-9A-Za-z_-]{20,}$/i.test(value)) return null;
  if (/[/?#]/.test(value)) return null;
  return value;
}

function extractHandleFromUrl(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const value = raw.trim();
  if (!value) return null;
  const match = value.match(/\/@([^/?#]+)/i);
  if (!match?.[1]) return null;
  try {
    return normalizeHandleValue(decodeURIComponent(match[1]));
  } catch {
    return normalizeHandleValue(match[1]);
  }
}

function extractChannelHandle(info: any): string | null {
  return (
    extractHandleFromUrl(info?.channel_url) ||
    extractHandleFromUrl(info?.uploader_url) ||
    normalizeHandleValue(info?.uploader_id) ||
    null
  );
}

function resolveVideoCategory(info: any): string | null {
  const direct = String(info?.category || "").trim();
  if (direct) return direct;

  if (Array.isArray(info?.categories)) {
    for (const item of info.categories) {
      const value = String(item || "").trim();
      if (value) return value;
    }
  }

  const genre = String(info?.genre || "").trim();
  if (genre) return genre;

  return null;
}

function hydrateToolMetaRow(row: ToolMetaHydrateRow): ToolMetaHydrateRow {
  const metaPath = resolveToolMetaFilePath(row);
  if (!metaPath) return row;

  try {
    const raw = fs.readFileSync(metaPath, "utf-8");
    const info = JSON.parse(raw);
    const parsed = ytdlp.parseVideoMeta(info);
    const channelHandle = extractChannelHandle(info);
    const channelTitle =
      String(
        info?.channel || info?.uploader || row.channel_title || "",
      ).trim() || null;
    const category =
      resolveVideoCategory(info) || String(row.category || "").trim() || null;
    const subs = toNullablePositiveInt(
      info?.channel_follower_count ??
        info?.uploader_follower_count ??
        info?.subscriber_count,
    );
    const videoCount = extractChannelVideoCountFromInfo(info);
    return {
      ...row,
      category,
      channel_handle: channelHandle,
      channel_title: channelTitle,
      local_meta_path: metaPath,
      duration_sec: toNullableInt(parsed.duration_sec),
      channel_subscriber_count:
        subs ?? toNullablePositiveInt(row.channel_subscriber_count),
      channel_video_count:
        videoCount ?? toNullablePositiveInt(row.channel_video_count),
    };
  } catch {
    return row;
  }
}

function isUcChannelId(value: unknown): boolean {
  return /^UC[0-9A-Za-z_-]{20,}$/.test(String(value || "").trim());
}

function isToolSyntheticChannelId(value: unknown): boolean {
  return /^tool_[A-Za-z0-9_-]+$/i.test(String(value || "").trim());
}

function extractSubscriberCountFromInfo(info: any): number | null {
  const direct = toNullablePositiveInt(
    info?.channel_follower_count ??
      info?.uploader_follower_count ??
      info?.subscriber_count ??
      info?.follower_count ??
      info?.author?.follower_count ??
      info?.user?.followers,
  );
  if (direct != null) return direct;
  try {
    const parsed = ytdlp.parseChannelMeta(info);
    return toNullablePositiveInt(parsed?.subscriber_count);
  } catch {
    return null;
  }
}

function extractChannelVideoCountFromInfo(info: any): number | null {
  const direct = toNullablePositiveInt(
    info?.channel_count ??
      info?.channel_video_count ??
      info?.playlist_count ??
      info?.entry_count ??
      info?.aweme_count ??
      info?.author?.aweme_count ??
      info?.author?.video_count,
  );
  if (direct != null) return direct;
  try {
    const parsed = ytdlp.parseChannelMeta(info);
    return toNullablePositiveInt(parsed?.video_count);
  } catch {
    return null;
  }
}

function normalizePublishedAtText(raw: unknown): string | null {
  if (typeof raw === "string") {
    const text = raw.trim();
    if (!text) return null;
    if (/^\d{8}$/.test(text)) {
      return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    }
    const ts = Date.parse(text.includes("T") ? text : `${text}T00:00:00Z`);
    if (Number.isFinite(ts)) {
      return new Date(ts).toISOString().replace(".000Z", "Z");
    }
  }
  const num = Number(raw);
  if (Number.isFinite(num) && num > 0) {
    const sec = num > 1e12 ? Math.trunc(num / 1000) : Math.trunc(num);
    const ts = sec * 1000;
    if (Number.isFinite(ts) && ts > 0) {
      return new Date(ts).toISOString().replace(".000Z", "Z");
    }
  }
  return null;
}

function extractPublishedAtFromInfo(info: any): string | null {
  if (!info || typeof info !== "object") return null;
  try {
    const parsed = ytdlp.parseVideoMeta(info);
    const fromParsed = String(parsed?.published_at || "").trim();
    if (fromParsed) return fromParsed;
  } catch {
    // ignore and continue raw extraction
  }
  return (
    normalizePublishedAtText(info?.published_at) ||
    normalizePublishedAtText(info?.upload_date) ||
    normalizePublishedAtText(info?.timestamp) ||
    normalizePublishedAtText(info?.release_timestamp) ||
    normalizePublishedAtText(info?.create_time) ||
    null
  );
}

function extractPlaylistFirstEntry(info: any): any | null {
  if (!Array.isArray(info?.entries)) return null;
  for (const entry of info.entries) {
    if (entry && typeof entry === "object") return entry;
  }
  return null;
}

function extractChannelFirstVideoPublishedAtFromInfo(info: any): string | null {
  const entry = extractPlaylistFirstEntry(info);
  if (!entry) return null;
  return extractPublishedAtFromInfo(entry);
}

function resolvePlaylistEntryVideoTarget(info: any): string | null {
  const entry = extractPlaylistFirstEntry(info);
  if (!entry || typeof entry !== "object") return null;
  const webpageUrl = String(
    (entry as any)?.webpage_url || (entry as any)?.url || "",
  ).trim();
  const canonical = canonicalVideoUrl(webpageUrl);
  if (canonical) return canonical;
  const id = String((entry as any)?.id || "").trim();
  if (/^[A-Za-z0-9_-]{6,}$/.test(id)) {
    return `https://www.youtube.com/watch?v=${id}`;
  }
  return null;
}

function resolveChannelLookupUrlFromRow(
  row: ToolMetaHydrateRow,
): string | null {
  const handle = normalizeHandleValue(row.channel_handle);
  if (handle) return `https://www.youtube.com/@${encodeURIComponent(handle)}`;

  const channelId = String(row.channel_id || "").trim();
  if (isUcChannelId(channelId)) {
    return `https://www.youtube.com/channel/${channelId}`;
  }

  return null;
}

function resolveChannelLookupUrlFromInfo(info: any): string | null {
  const fromInfo = String(info?.channel_url || info?.uploader_url || "").trim();
  if (/^https?:\/\//i.test(fromInfo)) return fromInfo;

  const handle = extractChannelHandle(info);
  if (handle) return `https://www.youtube.com/@${encodeURIComponent(handle)}`;

  const channelId = String(info?.channel_id || info?.uploader_id || "").trim();
  if (isUcChannelId(channelId)) {
    return `https://www.youtube.com/channel/${channelId}`;
  }
  return null;
}

function extractChannelApiLookupFromUrl(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const value = raw.trim();
  if (!value) return null;
  try {
    const parsed = new URL(value);
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    if (!host.endsWith("youtube.com")) return null;
    const pathParts = parsed.pathname.split("/").filter(Boolean);
    if (pathParts.length === 0) return null;
    if (pathParts[0].startsWith("@")) {
      return pathParts[0];
    }
    if (
      pathParts[0].toLowerCase() === "channel" &&
      pathParts[1] &&
      isUcChannelId(pathParts[1])
    ) {
      return pathParts[1];
    }
    return null;
  } catch {
    return null;
  }
}

function toFiniteNumber(input: unknown): number | null {
  const value = Number(input);
  if (!Number.isFinite(value)) return null;
  return value;
}

function clampNumber(
  value: number | null,
  min: number,
  max: number,
  fallback: number,
): number {
  if (value == null) return fallback;
  return Math.min(max, Math.max(min, value));
}

function sanitizeString(
  input: unknown,
  fallback: string,
  maxLength = 256,
): string {
  const value = String(input ?? "").trim();
  if (!value) return fallback;
  return value.slice(0, maxLength);
}

function sanitizeWindowIdList(input: unknown, fallback: string[]): string[] {
  const fallbackClean = fallback
    .map((item) =>
      String(item || "")
        .trim()
        .slice(0, 128),
    )
    .filter(Boolean);

  const source = Array.isArray(input)
    ? input
    : typeof input === "string"
      ? input.split(/\r?\n|,/g)
      : [];

  const out: string[] = [];
  for (const item of source) {
    const value = String(item || "")
      .trim()
      .slice(0, 128);
    if (!value) continue;
    if (out.includes(value)) continue;
    out.push(value);
    if (out.length >= 16) break;
  }

  if (out.length > 0) return out;
  if (fallbackClean.length > 0)
    return Array.from(new Set(fallbackClean)).slice(0, 16);
  return [DEFAULT_SCRAPE_OPTIONS.browser_window_id];
}

function resolveToolBatchConcurrency(total: number): number {
  const raw = Number.parseInt(getSetting("download_job_concurrency") || getSetting("max_concurrency") || "2", 10);
  const configured = Number.isFinite(raw) ? raw : 2;
  const normalized = Math.max(1, Math.min(16, configured));
  return Math.max(1, Math.min(Math.max(1, total), normalized));
}

function parseSimilarScrapeOptions(input: unknown): SimilarScrapeOptions {
  const raw =
    input && typeof input === "object"
      ? (input as Record<string, unknown>)
      : {};
  const browserApiUrl = sanitizeString(
    raw.browser_api_url,
    DEFAULT_SCRAPE_OPTIONS.browser_api_url,
    256,
  );
  const legacyWindowId = sanitizeString(raw.browser_window_id, "", 128);
  const browserWindowIds = sanitizeWindowIdList(
    raw.browser_window_ids,
    legacyWindowId
      ? [legacyWindowId]
      : DEFAULT_SCRAPE_OPTIONS.browser_window_ids,
  );
  const browserWindowId =
    browserWindowIds[0] || DEFAULT_SCRAPE_OPTIONS.browser_window_id;

  const preMin = clampNumber(
    toFiniteNumber(raw.pre_delay_min),
    0,
    30,
    DEFAULT_SCRAPE_OPTIONS.pre_delay_min,
  );
  const preMax = clampNumber(
    toFiniteNumber(raw.pre_delay_max),
    preMin,
    60,
    DEFAULT_SCRAPE_OPTIONS.pre_delay_max,
  );
  const postMin = clampNumber(
    toFiniteNumber(raw.post_delay_min),
    0,
    40,
    DEFAULT_SCRAPE_OPTIONS.post_delay_min,
  );
  const postMax = clampNumber(
    toFiniteNumber(raw.post_delay_max),
    postMin,
    80,
    DEFAULT_SCRAPE_OPTIONS.post_delay_max,
  );
  const retries = Math.trunc(
    clampNumber(
      toFiniteNumber(raw.retries),
      1,
      8,
      DEFAULT_SCRAPE_OPTIONS.retries,
    ),
  );
  const retryBackoffBase = clampNumber(
    toFiniteNumber(raw.retry_backoff_base),
    0,
    30,
    DEFAULT_SCRAPE_OPTIONS.retry_backoff_base,
  );
  const retryJitterMax = clampNumber(
    toFiniteNumber(raw.retry_jitter_max),
    0,
    10,
    DEFAULT_SCRAPE_OPTIONS.retry_jitter_max,
  );

  return {
    browser_api_url: browserApiUrl,
    browser_window_id: browserWindowId,
    browser_window_ids: browserWindowIds,
    pre_delay_min: preMin,
    pre_delay_max: preMax,
    post_delay_min: postMin,
    post_delay_max: postMax,
    retries,
    retry_backoff_base: retryBackoffBase,
    retry_jitter_max: retryJitterMax,
  };
}

function scriptShortsPathCandidates(): string[] {
  return [
    path.resolve(process.cwd(), "..", "scrape_youtube_shorts.py"),
    path.resolve(process.cwd(), "scrape_youtube_shorts.py"),
  ];
}

function findScrapeShortsScriptPath(): string | null {
  for (const candidate of scriptShortsPathCandidates()) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function scriptPathCandidates(): string[] {
  return [
    path.resolve(process.cwd(), "..", "scrape_youtube_channels.py"),
    path.resolve(process.cwd(), "scrape_youtube_channels.py"),
  ];
}

function findScrapeScriptPath(): string | null {
  for (const candidate of scriptPathCandidates()) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function parseJsonFromStdout(raw: string): SimilarScriptPayload | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;

  try {
    return JSON.parse(trimmed);
  } catch {
    const lines = trimmed
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      try {
        return JSON.parse(lines[i]);
      } catch {
        // continue
      }
    }
  }
  return null;
}

function runProcess(
  command: string,
  args: string[],
  stdinText: string,
  timeoutMs = 10 * 60 * 1000,
): Promise<{
  code: number | null;
  stdout: string;
  stderr: string;
  spawnError?: any;
}> {
  return new Promise((resolve) => {
    let stdout = "";
    let stderr = "";
    let done = false;
    const proc = spawn(command, args, { shell: false, windowsHide: true });

    const finish = (payload: {
      code: number | null;
      stdout: string;
      stderr: string;
      spawnError?: any;
    }) => {
      if (done) return;
      done = true;
      resolve(payload);
    };

    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // ignore
      }
      finish({
        code: null,
        stdout,
        stderr: `${stderr}\nTimed out after ${timeoutMs}ms`,
      });
    }, timeoutMs);

    proc.on("error", (error) => {
      clearTimeout(timer);
      finish({ code: null, stdout, stderr, spawnError: error });
    });

    proc.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    proc.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    proc.on("close", (code) => {
      clearTimeout(timer);
      finish({ code, stdout, stderr });
    });

    try {
      proc.stdin.write(stdinText);
      proc.stdin.end();
    } catch {
      // ignore
    }
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) =>
    setTimeout(resolve, Math.max(0, Math.trunc(ms))),
  );
}

function randomBetween(min: number, max: number): number {
  const lo = Math.min(min, max);
  const hi = Math.max(min, max);
  return lo + Math.random() * (hi - lo);
}

async function runSimilarLinksScript(
  urls: string[],
  options: SimilarScrapeOptions,
): Promise<{
  rows: SimilarLinkRow[];
  seedCount: number;
  relatedRawCount: number;
  workerCount: number;
  failedWindows: Array<{ window_id: string; error: string }>;
}> {
  const windowIds = sanitizeWindowIdList(options.browser_window_ids, [
    options.browser_window_id || DEFAULT_SCRAPE_OPTIONS.browser_window_id,
  ]);
  const workerCount = Math.max(1, Math.min(windowIds.length, urls.length));
  const buckets: string[][] = Array.from({ length: workerCount }, () => []);
  for (let i = 0; i < urls.length; i += 1) {
    buckets[i % workerCount].push(urls[i]);
  }

  const scriptPath = findScrapeScriptPath();
  if (!scriptPath) {
    throw new Error("scrape_youtube_channels.py not found");
  }
  const runByWindow = async (
    windowId: string,
    seedUrls: string[],
  ): Promise<{
    rows: SimilarLinkRow[];
    seedCount: number;
    relatedRawCount: number;
  }> => {
    const input = JSON.stringify({ urls: seedUrls });
    const optionArgs = [
      "--api-url",
      String(options.browser_api_url),
      "--window-id",
      String(windowId),
      "--pre-delay-min",
      String(options.pre_delay_min),
      "--pre-delay-max",
      String(options.pre_delay_max),
      "--post-delay-min",
      String(options.post_delay_min),
      "--post-delay-max",
      String(options.post_delay_max),
      "--retries",
      String(options.retries),
      "--retry-backoff-base",
      String(options.retry_backoff_base),
      "--retry-jitter-max",
      String(options.retry_jitter_max),
    ];
    const attempts: Array<{ command: string; args: string[] }> = [
      {
        command: "python",
        args: [scriptPath, "--json", "--stdin-json", "--quiet", ...optionArgs],
      },
      {
        command: "py",
        args: [
          "-3",
          scriptPath,
          "--json",
          "--stdin-json",
          "--quiet",
          ...optionArgs,
        ],
      },
    ];
    const timeoutMs = Math.min(
      90 * 60 * 1000,
      Math.max(10 * 60 * 1000, seedUrls.length * 80_000),
    );

    let lastError = "Failed to run scrape script";
    for (const attempt of attempts) {
      const outcome = await runProcess(
        attempt.command,
        attempt.args,
        input,
        timeoutMs,
      );
      if (outcome.spawnError) {
        if (String(outcome.spawnError?.code || "") === "ENOENT") {
          lastError = `${attempt.command} not found`;
          continue;
        }
        lastError = String(outcome.spawnError?.message || "spawn error");
        continue;
      }

      const payload = parseJsonFromStdout(outcome.stdout);
      if (
        outcome.code === 0 &&
        payload?.success &&
        Array.isArray(payload.data)
      ) {
        const rows = payload.data
          .map((item: SimilarLinkRow) => ({
            link: String(item.link || "").trim(),
            count: Math.max(1, Math.trunc(Number(item.count || 0) || 0)),
          }))
          .filter((item: SimilarLinkRow) => item.link);

        return {
          rows,
          seedCount: Math.max(0, Math.trunc(Number(payload.seed_count || 0))),
          relatedRawCount: Math.max(
            0,
            Math.trunc(Number(payload.related_raw_count || 0)),
          ),
        };
      }

      lastError =
        payload?.error ||
        outcome.stderr.trim() ||
        outcome.stdout.trim() ||
        `script exited with code ${outcome.code}`;
    }

    throw new Error(lastError);
  };

  const merged = new Map<string, number>();
  const failedWindows: Array<{ window_id: string; error: string }> = [];
  let seedCount = 0;
  let relatedRawCount = 0;
  let successWorkers = 0;

  await Promise.all(
    buckets.map(async (seedUrls, index) => {
      const windowId = windowIds[index] || options.browser_window_id;
      if (!seedUrls.length) return;
      try {
        const result = await runByWindow(windowId, seedUrls);
        seedCount += result.seedCount;
        relatedRawCount += result.relatedRawCount;
        for (const row of result.rows) {
          const key = String(row.link || "").trim();
          if (!key) continue;
          const count = Math.max(1, Math.trunc(Number(row.count || 0) || 0));
          merged.set(key, (merged.get(key) || 0) + count);
        }
        successWorkers += 1;
      } catch (err: any) {
        failedWindows.push({
          window_id: windowId,
          error: String(err?.message || "unknown error"),
        });
      }
    }),
  );

  if (successWorkers === 0) {
    const summary = failedWindows
      .map((item) => `[${item.window_id}] ${item.error}`)
      .join(" | ");
    throw new Error(summary || "Failed to run scrape script");
  }

  const rows = Array.from(merged.entries())
    .map(([link, count]) => ({ link, count }))
    .sort((a, b) => b.count - a.count || a.link.localeCompare(b.link));

  return {
    rows,
    seedCount: 1,
    relatedRawCount,
    workerCount,
    failedWindows,
  };
}

async function runSimilarContentSingleSeedScript(
  seedUrl: string,
  options: SimilarScrapeOptions,
): Promise<{
  rows: SimilarLinkRow[];
  seedCount: number;
  relatedRawCount: number;
  workerCount: number;
  failedWindows: Array<{ window_id: string; error: string }>;
}> {
  const windowIds = sanitizeWindowIdList(options.browser_window_ids, [
    options.browser_window_id || DEFAULT_SCRAPE_OPTIONS.browser_window_id,
  ]);
  const workerCount = Math.max(1, windowIds.length);
  const scriptPath = findScrapeScriptPath();
  if (!scriptPath) {
    throw new Error("scrape_youtube_channels.py not found");
  }

  const runByWindow = async (
    windowId: string,
  ): Promise<{
    rows: SimilarLinkRow[];
    seedCount: number;
    relatedRawCount: number;
  }> => {
    const seedUrls = [seedUrl];
    const input = JSON.stringify({ urls: seedUrls });
    const optionArgs = [
      "--api-url",
      String(options.browser_api_url),
      "--window-id",
      String(windowId),
      "--pre-delay-min",
      String(options.pre_delay_min),
      "--pre-delay-max",
      String(options.pre_delay_max),
      "--post-delay-min",
      String(options.post_delay_min),
      "--post-delay-max",
      String(options.post_delay_max),
      "--retries",
      String(options.retries),
      "--retry-backoff-base",
      String(options.retry_backoff_base),
      "--retry-jitter-max",
      String(options.retry_jitter_max),
    ];
    const attempts: Array<{ command: string; args: string[] }> = [
      {
        command: "python",
        args: [scriptPath, "--json", "--stdin-json", "--quiet", ...optionArgs],
      },
      {
        command: "py",
        args: [
          "-3",
          scriptPath,
          "--json",
          "--stdin-json",
          "--quiet",
          ...optionArgs,
        ],
      },
    ];
    const timeoutMs = Math.min(
      90 * 60 * 1000,
      Math.max(10 * 60 * 1000, seedUrls.length * 80_000),
    );

    let lastError = "Failed to run scrape script";
    for (const attempt of attempts) {
      const outcome = await runProcess(
        attempt.command,
        attempt.args,
        input,
        timeoutMs,
      );
      if (outcome.spawnError) {
        if (String(outcome.spawnError?.code || "") === "ENOENT") {
          lastError = `${attempt.command} not found`;
          continue;
        }
        lastError = String(outcome.spawnError?.message || "spawn error");
        continue;
      }

      const payload = parseJsonFromStdout(outcome.stdout);
      if (
        outcome.code === 0 &&
        payload?.success &&
        Array.isArray(payload.data)
      ) {
        const rows = payload.data
          .map((item: SimilarLinkRow) => ({
            link: String(item.link || "").trim(),
            count: Math.max(1, Math.trunc(Number(item.count || 0) || 0)),
          }))
          .filter((item: SimilarLinkRow) => item.link);

        return {
          rows,
          seedCount: Math.max(0, Math.trunc(Number(payload.seed_count || 0))),
          relatedRawCount: Math.max(
            0,
            Math.trunc(Number(payload.related_raw_count || 0)),
          ),
        };
      }

      lastError =
        payload?.error ||
        outcome.stderr.trim() ||
        outcome.stdout.trim() ||
        `script exited with code ${outcome.code}`;
    }

    throw new Error(lastError);
  };

  const merged = new Map<string, number>();
  const failedWindows: Array<{ window_id: string; error: string }> = [];
  let seedCount = 0;
  let relatedRawCount = 0;
  let successWorkers = 0;
  const staggerMinMs = Math.max(
    120,
    Math.min(4500, Math.round(options.pre_delay_min * 1000)),
  );
  const staggerMaxMs = Math.max(
    staggerMinMs,
    Math.min(6500, Math.round(options.pre_delay_max * 1000)),
  );

  await Promise.all(
    windowIds.map(async (windowId, index) => {
      if (index > 0) {
        await sleep(randomBetween(staggerMinMs, staggerMaxMs));
      }
      try {
        const result = await runByWindow(windowId);
        seedCount += result.seedCount;
        relatedRawCount += result.relatedRawCount;
        for (const row of result.rows) {
          const key = String(row.link || "").trim();
          if (!key) continue;
          const count = Math.max(1, Math.trunc(Number(row.count || 0) || 0));
          merged.set(key, (merged.get(key) || 0) + count);
        }
        successWorkers += 1;
      } catch (err: any) {
        failedWindows.push({
          window_id: windowId,
          error: String(err?.message || "unknown error"),
        });
      }
    }),
  );

  if (successWorkers === 0) {
    const summary = failedWindows
      .map((item) => `[${item.window_id}] ${item.error}`)
      .join(" | ");
    throw new Error(summary || "Failed to run scrape script");
  }

  const rows = Array.from(merged.entries())
    .map(([link, count]) => ({ link, count }))
    .sort((a, b) => b.count - a.count || a.link.localeCompare(b.link));

  return {
    rows,
    seedCount,
    relatedRawCount,
    workerCount,
    failedWindows,
  };
}

function extractVideoIdFromUrl(raw: string): string | null {
  const value = String(raw || "").trim();
  if (!value) return null;

  if (/^[A-Za-z0-9_-]{11}$/.test(value)) return value;

  let normalized = value;
  if (!/^https?:\/\//i.test(normalized) && normalized.startsWith("www.")) {
    normalized = `https://${normalized}`;
  }
  if (!/^https?:\/\//i.test(normalized)) return null;

  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    if (host.endsWith("youtube.com")) {
      if (parsed.pathname === "/watch") {
        const id = parsed.searchParams.get("v") || "";
        return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
      }
      if (parsed.pathname.startsWith("/shorts/")) {
        const id = parsed.pathname.split("/")[2] || "";
        return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
      }
      if (parsed.pathname.startsWith("/live/")) {
        const id = parsed.pathname.split("/")[2] || "";
        return /^[A-Za-z0-9_-]{6,}$/.test(id) ? id : null;
      }
      return null;
    }
    if (host === "youtu.be") {
      const id = parsed.pathname.replace(/^\/+/, "").split("/")[0] || "";
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

function normalizeToolChannelId(
  rawChannelId: string | null | undefined,
  videoId: string,
): string {
  const value = String(rawChannelId || "").trim();
  if (value && /^[-_A-Za-z0-9@.]+$/.test(value)) {
    return value.replace(/^@+/, "");
  }
  return `tool_${videoId}`;
}

function toAssetsUrl(absPath: string, assetsRoot: string): string | null {
  const relative = path.relative(assetsRoot, path.resolve(absPath));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative))
    return null;
  const encoded = relative
    .split(path.sep)
    .map((part) => encodeURIComponent(part))
    .join("/");
  return `/assets/${encoded}`;
}

function isInsidePath(rootPath: string, targetPath: string): boolean {
  const relative = path.relative(
    path.resolve(rootPath),
    path.resolve(targetPath),
  );
  return (
    relative === "" ||
    (!relative.startsWith("..") && !path.isAbsolute(relative))
  );
}

function removeEmptyParents(fromFilePath: string, stopRoot: string): void {
  let current = path.dirname(path.resolve(fromFilePath));
  const root = path.resolve(stopRoot);
  while (current !== root && isInsidePath(root, current)) {
    try {
      const entries = fs.readdirSync(current);
      if (entries.length > 0) break;
      fs.rmdirSync(current);
      current = path.dirname(current);
    } catch {
      break;
    }
  }
}

function parseToolResultRows(rawResult: unknown): ToolMetaHydrateRow[] {
  const parsed =
    rawResult && typeof rawResult === "object"
      ? (rawResult as Record<string, unknown>)
      : {};
  return parseToolMetaHydrateRows(parsed.data);
}

function resolveToolMetaFallback(
  downloadRoot: string,
  row: ToolMetaHydrateRow,
): string | null {
  const videoId = String(row.video_id || "").trim();
  const channelId = String(row.channel_id || "").trim();
  if (!videoId || !channelId) return null;
  return path.join(
    downloadRoot,
    "assets",
    "meta",
    channelId,
    videoId,
    `${videoId}.info.json`,
  );
}

function resolveToolThumbFallback(
  downloadRoot: string,
  row: ToolMetaHydrateRow,
): string | null {
  const videoId = String(row.video_id || "").trim();
  const channelId = String(row.channel_id || "").trim();
  if (!videoId || !channelId) return null;
  return path.join(
    downloadRoot,
    "assets",
    "thumbs",
    channelId,
    videoId,
    `${videoId}.jpg`,
  );
}

async function handleSimilarScrapeRoute(
  req: Request,
  res: Response,
): Promise<void> {
  const urls: string[] = toStringArray(req.body?.urls);
  const options: SimilarScrapeOptions = parseSimilarScrapeOptions(
    req.body?.options,
  );
  const normalized: string[] = Array.from(
    new Set(
      urls
        .map((item: string) => canonicalVideoUrl(item))
        .filter((item: string | null): item is string => Boolean(item)),
    ),
  );

  if (normalized.length === 0) {
    res.status(400).json({ error: "请提供至少一个有效的 YouTube 视频链接" });
    return;
  }

  try {
    const result = await runSimilarLinksScript(normalized, options);
    res.json({
      data: result.rows,
      seed_count: result.seedCount,
      related_raw_count: result.relatedRawCount,
      worker_count: result.workerCount,
      failed_windows: result.failedWindows,
      options,
    });
  } catch (err: any) {
    res.status(500).json({ error: err?.message || "执行脚本失败" });
  }
}

async function handleSimilarContentScrapeRoute(
  req: Request,
  res: Response,
): Promise<void> {
  const urls: string[] = toStringArray(req.body?.urls);
  const options: SimilarScrapeOptions = parseSimilarScrapeOptions(
    req.body?.options,
  );
  const normalized: string[] = Array.from(
    new Set(
      urls
        .map((item: string) => canonicalVideoUrl(item))
        .filter((item: string | null): item is string => Boolean(item)),
    ),
  );

  if (normalized.length !== 1) {
    res
      .status(400)
      .json({ error: "爆款选题抓取仅支持输入 1 条有效的 YouTube 视频链接" });
    return;
  }

  try {
    const result = await runSimilarContentSingleSeedScript(
      normalized[0],
      options,
    );
    res.json({
      data: result.rows,
      seed_count: result.seedCount,
      related_raw_count: result.relatedRawCount,
      worker_count: result.workerCount,
      failed_windows: result.failedWindows,
      seed_url: normalized[0],
      options,
    });
  } catch (err: any) {
    res.status(500).json({ error: err?.message || "执行脚本失败" });
  }
}

function mapHotVideosApiReasonToMessage(reason: string): string {
  const key = String(reason || "")
    .trim()
    .toLowerCase();
  if (!key) return "热点视频探测失败";
  if (key === "query_required") return "请先输入关键词";
  if (key === "channel_api_disabled")
    return "YouTube API 功能未启用，请在设置中开启";
  if (key === "youtube_api_key_missing")
    return "未配置 YouTube API Key，请先在设置页填写";
  if (key === "youtube_api_hot_budget_insufficient")
    return "当前没有任一单独 API Key 可覆盖本次请求配额，请降低结果数量后重试";
  if (
    key === "youtube_api_daily_limit_reached" ||
    key === "youtube_api_quota_exhausted"
  )
    return "YouTube API 当日配额已用尽";
  if (key === "youtube_api_network_error")
    return "请求 YouTube API 失败，请检查网络连接";
  if (key.startsWith("youtube_api_http_"))
    return `YouTube API 调用失败（${key.replace("youtube_api_http_", "HTTP ")})`;
  return `热点视频探测失败：${reason}`;
}

async function handleHotVideosSearchRoute(
  req: Request,
  res: Response,
): Promise<void> {
  const query = String(req.body?.query || "").trim();
  const typeFilterRaw = String(req.body?.type_filter || "")
    .trim()
    .toLowerCase();
  const typeFilter =
    typeFilterRaw === "shorts" ||
    typeFilterRaw === "video" ||
    typeFilterRaw === "all"
      ? (typeFilterRaw as "shorts" | "video" | "all")
      : "all";
  const timeFilterRaw = String(req.body?.time_filter || "")
    .trim()
    .toLowerCase();
  const timeFilter =
    timeFilterRaw === "week" ||
    timeFilterRaw === "month" ||
    timeFilterRaw === "6month"
      ? (timeFilterRaw as "week" | "month" | "6month")
      : "any";
  const durationFilterRaw = String(req.body?.duration_filter || "")
    .trim()
    .toLowerCase();
  const durationFilter =
    durationFilterRaw === "short" ||
    durationFilterRaw === "medium" ||
    durationFilterRaw === "long"
      ? (durationFilterRaw as "short" | "medium" | "long")
      : "any";
  const maxResults = Math.max(
    1,
    Math.min(500, Math.trunc(Number(req.body?.max_results || 50) || 50)),
  );
  const sortRaw = String(req.body?.order || "").trim();
  const order =
    sortRaw === "date" || sortRaw === "viewCount" || sortRaw === "relevance"
      ? (sortRaw as "date" | "viewCount" | "relevance")
      : "relevance";

  if (!query) {
    res.status(400).json({ error: "请先输入关键词" });
    return;
  }

  const result = await fetchHotVideosByKeyword(query, {
    typeFilter,
    timeFilter,
    durationFilter,
    maxResults,
    order,
  });
  if (!result.success) {
    res.status(400).json({
      error: mapHotVideosApiReasonToMessage(
        String(result.reason || "unknown_error"),
      ),
    });
    return;
  }

  res.json({
    data: Array.isArray(result.items) ? result.items : [],
    api_usage: getYoutubeApiUsageStatus(),
  });
}

async function handleCreateToolDownloadJobRoute(
  req: Request,
  res: Response,
  jobType: string,
): Promise<void> {
  const links: string[] = toStringArray(req.body?.links);
  const normalizedLinks: string[] = normalizeSimilarVideoLinks(links);

  if (normalizedLinks.length === 0) {
    res.status(400).json({ error: "请提供至少一个有效链接用于下载元数据" });
    return;
  }

  const db = getDb();
  const jobId = uuidv4();
  const concurrency = resolveToolBatchConcurrency(normalizedLinks.length);
  db.prepare(
    `
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, ?, ?, 'queued')
  `,
  ).run(
    jobId,
    jobType,
    JSON.stringify({ links: normalizedLinks, concurrency }),
  );

  try {
    const { getJobQueue } = await import("../services/jobQueue.js");
    getJobQueue().processNext();
  } catch {}

  res.json({
    job_id: jobId,
    status: "queued",
    total: normalizedLinks.length,
    concurrency,
  });
}

function handleToolDownloadJobResultRoute(req: Request, res: Response): void {
  const db = getDb();
  const jobId = String(req.params.jobId || "").trim();
  if (!jobId) {
    res.status(400).json({ error: "jobId is required" });
    return;
  }

  const row = db
    .prepare("SELECT result_json FROM tool_job_results WHERE job_id = ?")
    .get(jobId) as { result_json: string } | undefined;
  if (row?.result_json) {
    try {
      res.json(JSON.parse(row.result_json));
      return;
    } catch {
      res.status(500).json({ error: "Result payload is corrupted" });
      return;
    }
  }

  const job = db
    .prepare("SELECT status, error_message FROM jobs WHERE job_id = ?")
    .get(jobId) as any;
  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }

  if (
    job.status === "queued" ||
    job.status === "running" ||
    job.status === "canceling"
  ) {
    res
      .status(409)
      .json({ error: "Job is not finished yet", status: job.status });
    return;
  }

  if (job.status === "failed") {
    res
      .status(500)
      .json({ error: job.error_message || "Job failed", status: job.status });
    return;
  }

  if (job.status === "canceled") {
    res.status(499).json({ error: "Job canceled", status: job.status });
    return;
  }

  res
    .status(404)
    .json({ error: "Job finished but result is missing", status: job.status });
}

function handleToolRehydrateRoute(req: Request, res: Response): void {
  const rows = parseToolMetaHydrateRows(req.body?.videos);
  if (rows.length === 0) {
    res.json({ data: [] });
    return;
  }

  const data = rows.map((row) => hydrateToolMetaRow(row));
  res.json({ data });
}

async function handleToolRefreshSubsRoute(
  req: Request,
  res: Response,
): Promise<void> {
  const rows = parseToolMetaHydrateRows(req.body?.videos);
  const includeFirstVideo = req.body?.include_first_video !== false;
  if (rows.length === 0) {
    res.json({
      data: [],
      refreshed: 0,
      unchanged: 0,
      unresolved: 0,
      failed: 0,
    });
    return;
  }

  type ToolSubsSnapshot = {
    channel_id: string | null;
    channel_handle: string | null;
    channel_title: string | null;
    channel_url: string | null;
    subscriber_count: number | null;
    video_count: number | null;
    first_video_published_at: string | null;
    error: string | null;
  };

  const channelCache = new Map<string, ToolSubsSnapshot>();
  const videoCache = new Map<string, ToolSubsSnapshot>();

  const fetchChannelSnapshot = async (
    channelUrl: string,
  ): Promise<ToolSubsSnapshot> => {
    const key = String(channelUrl || "").trim();
    const cacheKey = key.toLowerCase();
    if (!key) {
      return {
        channel_id: null,
        channel_handle: null,
        channel_title: null,
        channel_url: null,
        subscriber_count: null,
        video_count: null,
        first_video_published_at: null,
        error: "empty channel url",
      };
    }
    if (channelCache.has(cacheKey)) {
      return channelCache.get(cacheKey)!;
    }

    let snapshot: ToolSubsSnapshot = {
      channel_id: null,
      channel_handle: null,
      channel_title: null,
      channel_url: key,
      subscriber_count: null,
      video_count: null,
      first_video_published_at: null,
      error: null,
    };
    try {
      const apiLookup = extractChannelApiLookupFromUrl(key);
      if (apiLookup) {
        try {
          const apiResult = await fetchChannelSnapshotFromApi(apiLookup);
          if (apiResult.success && apiResult.data) {
            const apiData = apiResult.data;
            const apiHandle = normalizeHandleValue(apiData.customUrl) || null;
            const apiChannelId = String(apiData.channelId || "").trim();
            snapshot = {
              channel_id: apiChannelId || null,
              channel_handle: apiHandle,
              channel_title: String(apiData.title || "").trim() || null,
              channel_url: apiHandle
                ? `https://www.youtube.com/@${encodeURIComponent(apiHandle)}`
                : isUcChannelId(apiChannelId)
                  ? `https://www.youtube.com/channel/${apiChannelId}`
                  : key,
              subscriber_count: toNullablePositiveInt(apiData.subscriberCount),
              video_count: toNullablePositiveInt(apiData.videoCount),
              first_video_published_at: null,
              error: null,
            };
          }
        } catch {
          // fall through to yt-dlp path
        }
      }

      if (
        snapshot.subscriber_count == null ||
        snapshot.video_count == null ||
        !snapshot.channel_title
      ) {
        const result = await ytdlp.getChannelInfo(key);
        if (!result.success || !result.data) {
          snapshot = {
            ...snapshot,
            error: String(result.error || "Failed to fetch channel info"),
          };
        } else {
          const info = result.data;
          let parsedMeta: ReturnType<typeof ytdlp.parseChannelMeta> | null =
            null;
          try {
            parsedMeta = ytdlp.parseChannelMeta(info);
          } catch {
            parsedMeta = null;
          }
          const parsedChannelId = String(parsedMeta?.channel_id || "").trim();
          const parsedTitle = String(parsedMeta?.title || "").trim();
          snapshot = {
            channel_id:
              String(
                info?.channel_id ||
                  parsedChannelId ||
                  snapshot.channel_id ||
                  "",
              ).trim() || null,
            channel_handle:
              extractChannelHandle(info) ||
              normalizeHandleValue(parsedMeta?.handle) ||
              snapshot.channel_handle ||
              null,
            channel_title:
              String(
                info?.channel ||
                  info?.uploader ||
                  parsedTitle ||
                  snapshot.channel_title ||
                  "",
              ).trim() || null,
            channel_url:
              resolveChannelLookupUrlFromInfo(info) ||
              snapshot.channel_url ||
              key,
            subscriber_count:
              extractSubscriberCountFromInfo(info) ?? snapshot.subscriber_count,
            video_count:
              extractChannelVideoCountFromInfo(info) ?? snapshot.video_count,
            first_video_published_at: snapshot.first_video_published_at,
            error: null,
          };
        }
      }

      if (snapshot.error) {
        snapshot = {
          ...snapshot,
          error: String(snapshot.error || "Failed to fetch channel info"),
        };
      } else if (includeFirstVideo) {
        try {
          const oldestResult = await ytdlp.getChannelOldestVideo(key);
          if (oldestResult.success && oldestResult.data) {
            let firstPublishedAt = extractChannelFirstVideoPublishedAtFromInfo(
              oldestResult.data,
            );
            if (!firstPublishedAt) {
              const oldestVideoTarget = resolvePlaylistEntryVideoTarget(
                oldestResult.data,
              );
              if (oldestVideoTarget) {
                const oldestVideoInfo =
                  await ytdlp.getVideoInfo(oldestVideoTarget);
                if (oldestVideoInfo.success && oldestVideoInfo.data) {
                  firstPublishedAt = extractPublishedAtFromInfo(
                    oldestVideoInfo.data,
                  );
                }
              }
            }
            if (firstPublishedAt) {
              snapshot.first_video_published_at = firstPublishedAt;
            }
          }
        } catch {
          // keep refresh robust when oldest-video lookup fails
        }
      }
    } catch (err: any) {
      snapshot = {
        ...snapshot,
        error: String(err?.message || "Failed to fetch channel info"),
      };
    }
    channelCache.set(cacheKey, snapshot);
    return snapshot;
  };

  const fetchVideoSnapshot = async (
    videoTarget: string,
  ): Promise<ToolSubsSnapshot> => {
    const key = String(videoTarget || "").trim();
    const cacheKey = key.toLowerCase();
    if (!key) {
      return {
        channel_id: null,
        channel_handle: null,
        channel_title: null,
        channel_url: null,
        subscriber_count: null,
        video_count: null,
        first_video_published_at: null,
        error: "empty video target",
      };
    }
    if (videoCache.has(cacheKey)) {
      return videoCache.get(cacheKey)!;
    }

    let snapshot: ToolSubsSnapshot = {
      channel_id: null,
      channel_handle: null,
      channel_title: null,
      channel_url: null,
      subscriber_count: null,
      video_count: null,
      first_video_published_at: null,
      error: null,
    };
    try {
      const result = await ytdlp.getVideoInfo(key);
      if (!result.success || !result.data) {
        snapshot = {
          ...snapshot,
          error: String(result.error || "Failed to fetch video info"),
        };
      } else {
        const info = result.data;
        const parsed = ytdlp.parseVideoMeta(info);
        const channelId = String(
          info?.channel_id || info?.uploader_id || parsed?.channel_id || "",
        ).trim();
        snapshot = {
          channel_id: channelId || null,
          channel_handle: extractChannelHandle(info),
          channel_title:
            String(info?.channel || info?.uploader || "").trim() || null,
          channel_url: resolveChannelLookupUrlFromInfo(info),
          subscriber_count: extractSubscriberCountFromInfo(info),
          video_count: extractChannelVideoCountFromInfo(info),
          first_video_published_at: null,
          error: null,
        };
      }
    } catch (err: any) {
      snapshot = {
        ...snapshot,
        error: String(err?.message || "Failed to fetch video info"),
      };
    }
    videoCache.set(cacheKey, snapshot);
    return snapshot;
  };

  const applySnapshot = (
    row: ToolMetaHydrateRow,
    snapshot: ToolSubsSnapshot,
  ): ToolMetaHydrateRow => {
    let next = { ...row };
    const subs = toNullablePositiveInt(snapshot.subscriber_count);
    if (subs != null) {
      next.channel_subscriber_count = subs;
    }
    const videoCount = toNullablePositiveInt(snapshot.video_count);
    if (videoCount != null) {
      next.channel_video_count = videoCount;
    }
    const firstPublishedAt = String(
      snapshot.first_video_published_at || "",
    ).trim();
    if (firstPublishedAt) {
      next.channel_first_video_published_at = firstPublishedAt;
    }

    const snapHandle = normalizeHandleValue(snapshot.channel_handle);
    if (snapHandle && !normalizeHandleValue(next.channel_handle)) {
      next.channel_handle = snapHandle;
    }

    const snapTitle = String(snapshot.channel_title || "").trim();
    const currentTitle = String(next.channel_title || "").trim();
    if (snapTitle && (!currentTitle || currentTitle === next.channel_id)) {
      next.channel_title = snapTitle;
    }

    const snapChannelId = String(snapshot.channel_id || "").trim();
    const currentChannelId = String(next.channel_id || "").trim();
    if (
      snapChannelId &&
      (!currentChannelId ||
        isToolSyntheticChannelId(currentChannelId) ||
        (!isUcChannelId(currentChannelId) && isUcChannelId(snapChannelId)))
    ) {
      next.channel_id = snapChannelId;
    }

    return next;
  };

  let refreshed = 0;
  let unchanged = 0;
  let unresolved = 0;
  let failed = 0;
  let subscriberUpdates = 0;
  let videoCountUpdates = 0;
  let firstVideoPublishedAtUpdates = 0;

  const data: ToolMetaHydrateRow[] = [];
  for (const rawRow of rows) {
    const hydratedRow = hydrateToolMetaRow(rawRow);
    const previousSubs = toNullablePositiveInt(rawRow.channel_subscriber_count);
    const previousVideoCount = toNullablePositiveInt(
      rawRow.channel_video_count,
    );
    const previousFirstVideoPublishedAt =
      String(rawRow.channel_first_video_published_at || "").trim() || null;
    let nextRow = { ...hydratedRow };
    let rowHadError = false;
    const needsChannelSnapshot = (row: ToolMetaHydrateRow) =>
      toNullablePositiveInt(row.channel_subscriber_count) == null ||
      toNullablePositiveInt(row.channel_video_count) == null ||
      (includeFirstVideo &&
        !String(row.channel_first_video_published_at || "").trim());

    const rowChannelUrl = resolveChannelLookupUrlFromRow(nextRow);
    if (rowChannelUrl && needsChannelSnapshot(nextRow)) {
      const snapshot = await fetchChannelSnapshot(rowChannelUrl);
      nextRow = applySnapshot(nextRow, snapshot);
      if (snapshot.error) rowHadError = true;
    }

    const needsVideoFallback = (row: ToolMetaHydrateRow) =>
      toNullablePositiveInt(row.channel_subscriber_count) == null;

    if (needsVideoFallback(nextRow)) {
      const fallbackVideoTarget = canonicalVideoUrl(
        String(nextRow.webpage_url || "").trim() || nextRow.video_id,
      );
      if (fallbackVideoTarget) {
        const videoSnapshot = await fetchVideoSnapshot(fallbackVideoTarget);
        nextRow = applySnapshot(nextRow, videoSnapshot);
        if (videoSnapshot.error) rowHadError = true;

        const channelUrlFromVideo = String(
          videoSnapshot.channel_url || "",
        ).trim();
        if (needsVideoFallback(nextRow) && channelUrlFromVideo) {
          const channelSnapshot =
            await fetchChannelSnapshot(channelUrlFromVideo);
          nextRow = applySnapshot(nextRow, channelSnapshot);
          if (channelSnapshot.error) rowHadError = true;
        }
      }
    }

    const finalSubs = toNullablePositiveInt(nextRow.channel_subscriber_count);
    const finalVideoCount = toNullablePositiveInt(nextRow.channel_video_count);
    const finalFirstVideoPublishedAt =
      String(nextRow.channel_first_video_published_at || "").trim() || null;

    const subsChanged = finalSubs != null && finalSubs !== previousSubs;
    const videoCountChanged =
      finalVideoCount != null && finalVideoCount !== previousVideoCount;
    const firstVideoChanged =
      Boolean(finalFirstVideoPublishedAt) &&
      finalFirstVideoPublishedAt !== previousFirstVideoPublishedAt;
    if (subsChanged) subscriberUpdates += 1;
    if (videoCountChanged) videoCountUpdates += 1;
    if (firstVideoChanged) firstVideoPublishedAtUpdates += 1;

    const rowUpdated = subsChanged || videoCountChanged || firstVideoChanged;
    if (rowUpdated) refreshed += 1;
    else unchanged += 1;

    if (
      finalSubs == null ||
      finalVideoCount == null ||
      (includeFirstVideo && !finalFirstVideoPublishedAt)
    )
      unresolved += 1;
    if (rowHadError && finalSubs == null) {
      failed += 1;
    }
    data.push(nextRow);
  }

  res.json({
    data,
    refreshed,
    unchanged,
    unresolved,
    failed,
    subscriber_updates: subscriberUpdates,
    video_count_updates: videoCountUpdates,
    first_video_published_at_updates: firstVideoPublishedAtUpdates,
  });
}

function handleToolClearRoute(
  req: Request,
  res: Response,
  jobType: string,
): void {
  const db = getDb();
  const running = db
    .prepare(
      `
    SELECT COUNT(*) AS c
    FROM jobs
    WHERE type = ?
      AND status IN ('queued', 'running', 'canceling')
  `,
    )
    .get(jobType) as { c?: number } | undefined;
  if (Number(running?.c || 0) > 0) {
    res.status(409).json({
      error: "Tool download job is running; cancel or wait before clearing.",
    });
    return;
  }

  const downloadRoot = path.resolve(
    getSetting("download_root") || path.join(process.cwd(), "downloads"),
  );
  const metaRoot = path.resolve(downloadRoot, "assets", "meta");
  const thumbRoot = path.resolve(downloadRoot, "assets", "thumbs");
  const rowsFromRequest = parseToolMetaHydrateRows(req.body?.videos);

  const rowsFromResultTable: ToolMetaHydrateRow[] = [];
  const resultRows = db
    .prepare(
      `
    SELECT r.result_json
    FROM tool_job_results r
    INNER JOIN jobs j ON j.job_id = r.job_id
    WHERE j.type = ?
  `,
    )
    .all(jobType) as Array<{ result_json: string }>;
  for (const item of resultRows) {
    try {
      const parsed = JSON.parse(String(item.result_json || "{}"));
      rowsFromResultTable.push(...parseToolResultRows(parsed));
    } catch {
      // ignore broken rows
    }
  }

  const candidateFiles = new Set<string>();
  const addCandidate = (rawPath: unknown) => {
    if (typeof rawPath !== "string") return;
    const value = rawPath.trim();
    if (!value) return;
    candidateFiles.add(path.resolve(value));
  };
  for (const row of [...rowsFromRequest, ...rowsFromResultTable]) {
    addCandidate(row.local_meta_path);
    addCandidate(row.local_thumb_path);
    addCandidate(resolveToolMetaFallback(downloadRoot, row));
    addCandidate(resolveToolThumbFallback(downloadRoot, row));
  }

  const referencedRows = db
    .prepare(
      `
    SELECT local_meta_path, local_thumb_path
    FROM videos
    WHERE local_meta_path IS NOT NULL
       OR local_thumb_path IS NOT NULL
  `,
    )
    .all() as Array<{
    local_meta_path: string | null;
    local_thumb_path: string | null;
  }>;
  const protectedPaths = new Set<string>();
  for (const row of referencedRows) {
    if (row.local_meta_path)
      protectedPaths.add(path.resolve(row.local_meta_path));
    if (row.local_thumb_path)
      protectedPaths.add(path.resolve(row.local_thumb_path));
  }

  let deletedFiles = 0;
  let missingFiles = 0;
  let skippedProtectedFiles = 0;
  let skippedOutsideFiles = 0;
  let deleteFileErrors = 0;

  const deleteCandidateFile = (absPath: string) => {
    const inMeta = isInsidePath(metaRoot, absPath);
    const inThumb = isInsidePath(thumbRoot, absPath);
    if (!inMeta && !inThumb) {
      skippedOutsideFiles += 1;
      return;
    }
    if (protectedPaths.has(absPath)) {
      skippedProtectedFiles += 1;
      return;
    }
    if (!fs.existsSync(absPath)) {
      missingFiles += 1;
      return;
    }
    try {
      fs.unlinkSync(absPath);
      deletedFiles += 1;
      if (inMeta) removeEmptyParents(absPath, metaRoot);
      if (inThumb) removeEmptyParents(absPath, thumbRoot);
    } catch {
      deleteFileErrors += 1;
    }
  };

  for (const filePath of candidateFiles) {
    deleteCandidateFile(filePath);
  }

  const listToolDirs = (rootPath: string): string[] => {
    if (!fs.existsSync(rootPath)) return [];
    try {
      return fs
        .readdirSync(rootPath, { withFileTypes: true })
        .filter(
          (entry) => entry.isDirectory() && entry.name.startsWith("tool_"),
        )
        .map((entry) => path.join(rootPath, entry.name));
    } catch {
      return [];
    }
  };

  let deletedDirs = 0;
  let skippedProtectedDirs = 0;
  let deleteDirErrors = 0;
  const protectedList = Array.from(protectedPaths);
  for (const dirPath of [
    ...listToolDirs(metaRoot),
    ...listToolDirs(thumbRoot),
  ]) {
    const hasProtected = protectedList.some((item) =>
      isInsidePath(dirPath, item),
    );
    if (hasProtected) {
      skippedProtectedDirs += 1;
      continue;
    }
    try {
      fs.rmSync(dirPath, { recursive: true, force: true });
      deletedDirs += 1;
    } catch {
      deleteDirErrors += 1;
    }
  }

  const beforeJobs = Number(
    (
      db
        .prepare(`SELECT COUNT(*) AS c FROM jobs WHERE type = ?`)
        .get(jobType) as { c?: number } | undefined
    )?.c || 0,
  );
  const beforeResults = Number(
    (
      db
        .prepare(
          `
    SELECT COUNT(*) AS c
    FROM tool_job_results
    WHERE job_id IN (SELECT job_id FROM jobs WHERE type = ?)
  `,
        )
        .get(jobType) as { c?: number } | undefined
    )?.c || 0,
  );
  const beforeEvents = Number(
    (
      db
        .prepare(
          `
    SELECT COUNT(*) AS c
    FROM job_events
    WHERE job_id IN (SELECT job_id FROM jobs WHERE type = ?)
  `,
        )
        .get(jobType) as { c?: number } | undefined
    )?.c || 0,
  );

  const clearRowsTxn = db.transaction(() => {
    db.prepare(
      `
      DELETE FROM job_events
      WHERE job_id IN (SELECT job_id FROM jobs WHERE type = ?)
    `,
    ).run(jobType);
    db.prepare(
      `
      DELETE FROM tool_job_results
      WHERE job_id IN (SELECT job_id FROM jobs WHERE type = ?)
    `,
    ).run(jobType);
    db.prepare(`DELETE FROM jobs WHERE type = ?`).run(jobType);
  });
  clearRowsTxn();

  res.json({
    success: true,
    deleted_files: deletedFiles,
    missing_files: missingFiles,
    skipped_protected_files: skippedProtectedFiles,
    skipped_outside_files: skippedOutsideFiles,
    file_delete_errors: deleteFileErrors,
    deleted_dirs: deletedDirs,
    skipped_protected_dirs: skippedProtectedDirs,
    dir_delete_errors: deleteDirErrors,
    deleted_jobs: beforeJobs,
    deleted_results: beforeResults,
    deleted_events: beforeEvents,
  });
}

// POST /api/tools/similar-channels/scrape
router.post("/similar-channels/scrape", async (req: Request, res: Response) => {
  await handleSimilarScrapeRoute(req, res);
});

// POST /api/tools/similar-channels/download-meta
router.post(
  "/similar-channels/download-meta",
  async (req: Request, res: Response) => {
    await handleCreateToolDownloadJobRoute(
      req,
      res,
      TOOL_JOB_TYPE_SIMILAR_CHANNEL,
    );
  },
);

// GET /api/tools/similar-channels/download-meta/:jobId/result
router.get(
  "/similar-channels/download-meta/:jobId/result",
  (req: Request, res: Response) => {
    handleToolDownloadJobResultRoute(req, res);
  },
);

// POST /api/tools/similar-channels/rehydrate-meta
router.post(
  "/similar-channels/rehydrate-meta",
  (req: Request, res: Response) => {
    handleToolRehydrateRoute(req, res);
  },
);

// POST /api/tools/similar-channels/refresh-subs
router.post(
  "/similar-channels/refresh-subs",
  async (req: Request, res: Response) => {
    await handleToolRefreshSubsRoute(req, res);
  },
);

// POST /api/tools/similar-channels/clear
router.post("/similar-channels/clear", (req: Request, res: Response) => {
  handleToolClearRoute(req, res, TOOL_JOB_TYPE_SIMILAR_CHANNEL);
});

// POST /api/tools/similar-content/scrape
router.post("/similar-content/scrape", async (req: Request, res: Response) => {
  await handleSimilarContentScrapeRoute(req, res);
});

// POST /api/tools/hot-videos/search
router.post("/hot-videos/search", async (req: Request, res: Response) => {
  await handleHotVideosSearchRoute(req, res);
});

// POST /api/tools/similar-content/download-meta
router.post(
  "/similar-content/download-meta",
  async (req: Request, res: Response) => {
    await handleCreateToolDownloadJobRoute(
      req,
      res,
      TOOL_JOB_TYPE_SIMILAR_CONTENT,
    );
  },
);

// GET /api/tools/similar-content/download-meta/:jobId/result
router.get(
  "/similar-content/download-meta/:jobId/result",
  (req: Request, res: Response) => {
    handleToolDownloadJobResultRoute(req, res);
  },
);

// POST /api/tools/similar-content/rehydrate-meta
router.post(
  "/similar-content/rehydrate-meta",
  (req: Request, res: Response) => {
    handleToolRehydrateRoute(req, res);
  },
);

// POST /api/tools/similar-content/refresh-subs
router.post(
  "/similar-content/refresh-subs",
  async (req: Request, res: Response) => {
    await handleToolRefreshSubsRoute(req, res);
  },
);

// POST /api/tools/similar-content/clear
router.post("/similar-content/clear", (req: Request, res: Response) => {
  handleToolClearRoute(req, res, TOOL_JOB_TYPE_SIMILAR_CONTENT);
});

// POST /api/tools/hot-shorts/scrape
router.post("/hot-shorts/scrape", async (req: Request, res: Response) => {
  await handleHotShortsScrapeRoute(req, res);
});

export default router;

async function runHotShortsScript(
  keyword: string,
  options: HotShortsScrapeOptions,
): Promise<{
  success: boolean;
  data: HotShortsVideoRow[];
  error?: string;
}> {
  const windowIds = sanitizeWindowIdList(options.browser_window_ids, [
    options.browser_window_id || DEFAULT_SCRAPE_OPTIONS.browser_window_id,
  ]);
  const windowId = String(
    options.browser_window_id ||
      windowIds[0] ||
      DEFAULT_SCRAPE_OPTIONS.browser_window_id,
  ).trim();

  const scriptPath = findScrapeShortsScriptPath();
  if (!scriptPath) {
    throw new Error("scrape_youtube_shorts.py not found");
  }

  const input = JSON.stringify({
    keyword,
    max_scroll: options.max_scroll,
    scroll_delay_min: options.scroll_delay_min,
    scroll_delay_max: options.scroll_delay_max,
    retries: options.retries,
    retry_backoff_base: options.retry_backoff_base,
    retry_jitter_max: options.retry_jitter_max,
    human_scroll: options.human_scroll,
  });
  const optionArgs = [
    "--api-url",
    String(options.browser_api_url),
    "--window-id",
    String(windowId),
  ];
  const attempts: Array<{ command: string; args: string[] }> = [
    {
      command: "python",
      args: [scriptPath, "--json", "--stdin-json", "--quiet", ...optionArgs],
    },
    {
      command: "py",
      args: [
        "-3",
        scriptPath,
        "--json",
        "--stdin-json",
        "--quiet",
        ...optionArgs,
      ],
    },
  ];

  const timeoutMs = Math.min(
    8 * 60 * 1000,
    Math.max(
      90 * 1000,
      options.retries * 45_000 +
        options.max_scroll * 18_000 +
        Math.trunc(options.scroll_delay_max * options.max_scroll * 1000),
    ),
  );

  let lastError = "Failed to run scrape script";

  for (const attempt of attempts) {
    const outcome = await runProcess(
      attempt.command,
      attempt.args,
      input,
      timeoutMs,
    );

    if (outcome.spawnError) {
      if (String(outcome.spawnError?.code || "") === "ENOENT") {
        lastError = `${attempt.command} not found`;
        continue;
      }
      lastError = String(outcome.spawnError?.message || "spawn error");
      continue;
    }

    const payload = parseJsonFromStdout(outcome.stdout) as any;
    if (outcome.code === 0 && payload?.success && Array.isArray(payload.data)) {
      return {
        success: true,
        data: payload.data,
      };
    }

    lastError =
      payload?.error ||
      outcome.stderr.trim() ||
      outcome.stdout.trim() ||
      `script exited with code ${outcome.code}`;
  }

  throw new Error(lastError);
}

async function handleHotShortsScrapeRoute(req: Request, res: Response) {
  try {
    const { keyword, max_scroll, options } = req.body;
    if (!keyword) {
      res.status(400).json({ error: "Missing keyword" });
      return;
    }

    const rawOptions = options && typeof options === "object" ? options : {};
    const browserWindowId = sanitizeString(
      rawOptions.browser_window_id,
      "",
      128,
    );
    const browserWindowIds = sanitizeWindowIdList(
      rawOptions.browser_window_ids,
      browserWindowId
        ? [browserWindowId]
        : [DEFAULT_SCRAPE_OPTIONS.browser_window_id],
    );
    const scrapeOptions: HotShortsScrapeOptions = {
      browser_api_url: sanitizeString(
        rawOptions.browser_api_url,
        DEFAULT_SCRAPE_OPTIONS.browser_api_url,
      ),
      browser_window_id:
        browserWindowId ||
        browserWindowIds[0] ||
        DEFAULT_SCRAPE_OPTIONS.browser_window_id,
      browser_window_ids: browserWindowIds,
      max_scroll: Math.max(1, Math.min(20, Number(max_scroll) || 3)),
      scroll_delay_min: clampNumber(
        toFiniteNumber(rawOptions.scroll_delay_min),
        0,
        20,
        1.6,
      ),
      scroll_delay_max: 0,
      retries: Math.trunc(
        clampNumber(toFiniteNumber(rawOptions.retries), 1, 5, 2),
      ),
      retry_backoff_base: clampNumber(
        toFiniteNumber(rawOptions.retry_backoff_base),
        0,
        20,
        1.2,
      ),
      retry_jitter_max: clampNumber(
        toFiniteNumber(rawOptions.retry_jitter_max),
        0,
        10,
        1.0,
      ),
      human_scroll: rawOptions.human_scroll !== false,
    };
    scrapeOptions.scroll_delay_max = clampNumber(
      toFiniteNumber(rawOptions.scroll_delay_max),
      scrapeOptions.scroll_delay_min,
      30,
      3.4,
    );

    const result = await runHotShortsScript(String(keyword), scrapeOptions);
    res.json(result);
  } catch (err: any) {
    console.error("Hot Shorts Scrape Error:", err);
    res
      .status(500)
      .json({ error: String(err?.message || "Internal Server Error") });
  }
}
