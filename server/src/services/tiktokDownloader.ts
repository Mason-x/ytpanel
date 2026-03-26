import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { getSetting } from '../db.js';

export type TikTokDownloaderPlatform = 'tiktok' | 'douyin';

interface BridgeBasePayload {
  tool_root: string;
  platform: TikTokDownloaderPlatform;
  cookie: string;
  proxy?: string;
}

interface BridgeValidateCookiePayload extends BridgeBasePayload {
  action: 'validate_cookie';
}

interface BridgeResolveUserPayload extends BridgeBasePayload {
  action: 'resolve_user';
  input: string;
}

interface BridgeAccountPayload extends BridgeBasePayload {
  action: 'account';
  account_input: string;
  tab?: string;
  pages?: number;
  cursor?: number;
  count?: number;
}

interface BridgeDetailPayload extends BridgeBasePayload {
  action: 'detail';
  detail_input: string;
}

type BridgePayload =
  | BridgeValidateCookiePayload
  | BridgeResolveUserPayload
  | BridgeAccountPayload
  | BridgeDetailPayload;

type BridgeRequest =
  | Omit<BridgeValidateCookiePayload, 'tool_root'>
  | Omit<BridgeResolveUserPayload, 'tool_root'>
  | Omit<BridgeAccountPayload, 'tool_root'>
  | Omit<BridgeDetailPayload, 'tool_root'>;

interface BridgeResult {
  ok: boolean;
  action?: string;
  error?: string;
  stderr?: string;
  traceback?: string;
  [key: string]: unknown;
}

export interface TikTokDownloaderNormalizedItem {
  id: string;
  title: string;
  description: string | null;
  uploader: string | null;
  channel_id: string | null;
  uploader_id: string | null;
  unique_id: string | null;
  sec_uid: string | null;
  uid: string | null;
  published_at: string | null;
  timestamp: number | null;
  duration_sec: number | null;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  webpage_url: string | null;
  thumbnail: string | null;
  tags: string[];
  content_type?: 'long' | 'short' | 'note' | 'album' | 'live_photo' | null;
  raw?: any;
}

export interface TikTokDownloaderChannelSnapshot {
  title: string | null;
  avatar_url: string | null;
  follower_count: number | null;
  uid: string | null;
  sec_uid: string | null;
  unique_id: string | null;
}

export interface TikTokDownloaderAccountResult {
  ok: boolean;
  accountId: string | null;
  channel: TikTokDownloaderChannelSnapshot | null;
  entries: TikTokDownloaderNormalizedItem[];
  error?: string;
  stderr?: string;
}

export interface TikTokDownloaderDetailResult {
  ok: boolean;
  item: TikTokDownloaderNormalizedItem | null;
  error?: string;
  stderr?: string;
}

export interface TikTokDownloaderCookieValidationResult {
  ok: boolean;
  valid: boolean;
  message: string;
  userId?: string | null;
  nickname?: string | null;
  error?: string;
  stderr?: string;
}

const DEFAULT_TIMEOUT_SEC = 120;

function toNullableInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.trunc(parsed);
}

function toNormalizedItems(value: unknown): TikTokDownloaderNormalizedItem[] {
  if (!Array.isArray(value)) return [];
  const rows: TikTokDownloaderNormalizedItem[] = [];
  for (const item of value) {
    if (!item || typeof item !== 'object') continue;
    const row = item as Record<string, unknown>;
    const id = String(row.id || '').trim();
    if (!id) continue;
    rows.push({
      id,
      title: String(row.title || 'Untitled').trim() || 'Untitled',
      description: String(row.description || '').trim() || null,
      uploader: String(row.uploader || '').trim() || null,
      channel_id: String(row.channel_id || '').trim() || null,
      uploader_id: String(row.uploader_id || '').trim() || null,
      unique_id: String(row.unique_id || '').trim() || null,
      sec_uid: String(row.sec_uid || '').trim() || null,
      uid: String(row.uid || '').trim() || null,
      published_at: String(row.published_at || '').trim() || null,
      timestamp: toNullableInt(row.timestamp),
      duration_sec: toNullableInt(row.duration_sec),
      view_count: toNullableInt(row.view_count),
      like_count: toNullableInt(row.like_count),
      comment_count: toNullableInt(row.comment_count),
      webpage_url: String(row.webpage_url || '').trim() || null,
      thumbnail: String(row.thumbnail || '').trim() || null,
      tags: Array.isArray(row.tags)
        ? row.tags.map((tag) => String(tag || '').trim()).filter(Boolean)
        : [],
      content_type: ['long', 'short', 'note', 'album', 'live_photo'].includes(String(row.content_type || '').toLowerCase())
        ? (String(row.content_type || '').toLowerCase() as 'long' | 'short' | 'note' | 'album' | 'live_photo')
        : null,
      raw: row.raw ?? null,
    });
  }
  return rows;
}

function toChannelSnapshot(value: unknown): TikTokDownloaderChannelSnapshot | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as Record<string, unknown>;
  return {
    title: String(row.title || '').trim() || null,
    avatar_url: String(row.avatar_url || '').trim() || null,
    follower_count: toNullableInt(row.follower_count),
    uid: String(row.uid || '').trim() || null,
    sec_uid: String(row.sec_uid || '').trim() || null,
    unique_id: String(row.unique_id || '').trim() || null,
  };
}

function parseJsonFromStdout(stdout: string): BridgeResult {
  const trimmed = stdout.trim();
  if (!trimmed) {
    return { ok: false, error: 'TikTokDownloader bridge returned empty stdout' };
  }

  try {
    return JSON.parse(trimmed) as BridgeResult;
  } catch {
    const lines = trimmed
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i--) {
      try {
        return JSON.parse(lines[i]) as BridgeResult;
      } catch {
        // Continue.
      }
    }
    return { ok: false, error: `TikTokDownloader bridge returned non-JSON stdout: ${trimmed.slice(-500)}` };
  }
}

function shouldRetryWithoutCookie(error: string): boolean {
  const text = String(error || '').trim().toLowerCase();
  if (!text) return false;
  return (
    text === 'empty_detail'
    || text === 'empty_entries'
    || text === 'resolve_empty_detail_id'
    || text === 'resolve_empty_account_id'
    || text.includes('failed to parse json')
    || text.includes('fresh cookies')
  );
}

function mergeStderr(primary?: string, fallback?: string): string | undefined {
  const a = String(primary || '').trim();
  const b = String(fallback || '').trim();
  if (a && b) return `${a}\n--- retry-without-cookie ---\n${b}`.slice(-8000);
  return a || b || undefined;
}

function getBridgeScriptPath(): string {
  return resolvePathSettingWithFallback('tiktok_downloader_bridge_script', [
    path.join(process.cwd(), 'scripts', 'tiktokdownloader_bridge.py'),
    path.join(process.cwd(), 'server', 'scripts', 'tiktokdownloader_bridge.py'),
  ]);
}

function getDownloaderRoot(): string {
  return resolvePathSettingWithFallback('tiktok_downloader_root', [
    path.join(process.cwd(), 'server', 'vendor', 'tiktokdownloader'),
    path.join(process.cwd(), 'vendor', 'tiktokdownloader'),
    path.join(process.cwd(), '..', 'TikTokDownloader'),
    path.join(process.cwd(), 'TikTokDownloader'),
  ]);
}

function resolvePathSettingWithFallback(settingKey: string, fallbackCandidates: string[]): string {
  const configured = String(getSetting(settingKey) || '').trim();
  const normalizedCandidates = fallbackCandidates.map((candidate) => path.resolve(candidate));

  if (configured) {
    const resolvedConfigured = path.resolve(configured);
    if (fs.existsSync(resolvedConfigured)) return resolvedConfigured;
    for (const candidate of normalizedCandidates) {
      if (fs.existsSync(candidate)) return candidate;
    }
    return resolvedConfigured;
  }

  for (const candidate of normalizedCandidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return normalizedCandidates[0] || path.resolve(process.cwd());
}

function getPythonBin(): string {
  const configured = String(getSetting('tiktok_downloader_python') || '').trim();
  return configured || 'python';
}

function getCondaEnvName(): string {
  return String(getSetting('tiktok_downloader_conda_env') || '').trim();
}

function resolveCondaEnvPython(condaEnv: string): string | null {
  if (!condaEnv) return null;
  const exe = process.platform === 'win32' ? 'python.exe' : path.join('bin', 'python');
  const candidates = [
    process.env.CONDA_PREFIX ? path.join(process.env.CONDA_PREFIX, 'envs', condaEnv, exe) : '',
    process.env.USERPROFILE ? path.join(process.env.USERPROFILE, '.conda', 'envs', condaEnv, exe) : '',
    process.env.HOME ? path.join(process.env.HOME, '.conda', 'envs', condaEnv, exe) : '',
  ].map((item) => String(item || '').trim()).filter(Boolean);
  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate)) return candidate;
    } catch {
      // ignore
    }
  }
  return null;
}

function getTimeoutMs(): number {
  const raw = Number(getSetting('tiktok_downloader_timeout_sec') || `${DEFAULT_TIMEOUT_SEC}`);
  if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_TIMEOUT_SEC * 1000;
  return Math.max(10, Math.min(raw, 600)) * 1000;
}

function isEnabledBySetting(): boolean {
  const raw = String(getSetting('tiktok_downloader_enabled') || 'true').trim().toLowerCase();
  return raw !== 'false' && raw !== '0' && raw !== 'off';
}

export function isTikTokDownloaderAvailable(): { ok: boolean; reason?: string } {
  if (!isEnabledBySetting()) {
    return { ok: false, reason: 'tiktok_downloader_disabled' };
  }
  const toolRoot = getDownloaderRoot();
  if (!fs.existsSync(toolRoot)) {
    return { ok: false, reason: `tiktok_downloader_root_not_found: ${toolRoot}` };
  }
  const script = getBridgeScriptPath();
  if (!fs.existsSync(script)) {
    return { ok: false, reason: `tiktok_downloader_bridge_missing: ${script}` };
  }
  return { ok: true };
}

async function runBridge(
  payload: BridgeRequest,
  abortSignal?: AbortSignal,
): Promise<BridgeResult> {
  const available = isTikTokDownloaderAvailable();
  if (!available.ok) {
    return { ok: false, error: available.reason || 'tiktok_downloader_unavailable' };
  }

  const toolRoot = getDownloaderRoot();
  const scriptPath = getBridgeScriptPath();
  const python = getPythonBin();
  const condaEnv = getCondaEnvName();
  const condaEnvPython = resolveCondaEnvPython(condaEnv);
  const timeoutMs = getTimeoutMs();
  const fullPayload: BridgePayload = { ...payload, tool_root: toolRoot } as BridgePayload;

  return new Promise((resolve) => {
    const command = condaEnv
      ? (condaEnvPython || 'conda')
      : python;
    const commandArgs = condaEnv
      ? (condaEnvPython ? [scriptPath] : ['run', '-n', condaEnv, python, scriptPath])
      : [scriptPath];

    const child = spawn(command, commandArgs, {
      cwd: toolRoot,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: {
        ...process.env,
        PYTHONIOENCODING: 'utf-8',
        PYTHONUTF8: '1',
        TTD_BRIDGE_PAYLOAD: JSON.stringify(fullPayload),
      },
    });

    let stdout = '';
    let stderr = '';
    let settled = false;
    let timer: NodeJS.Timeout | null = null;

    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      if (abortSignal) {
        abortSignal.removeEventListener('abort', onAbort);
      }
    };

    const finalize = (result: BridgeResult) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(result);
    };

    const onAbort = () => {
      if (settled) return;
      try {
        child.kill('SIGTERM');
      } catch {
        // Ignore.
      }
      finalize({ ok: false, error: 'aborted', stderr });
    };

    if (abortSignal) {
      if (abortSignal.aborted) {
        onAbort();
        return;
      }
      abortSignal.addEventListener('abort', onAbort, { once: true });
    }

    timer = setTimeout(() => {
      if (settled) return;
      try {
        child.kill('SIGKILL');
      } catch {
        // Ignore.
      }
      finalize({ ok: false, error: `tiktok_downloader_timeout_${timeoutMs}ms`, stderr });
    }, timeoutMs);

    child.stdout.on('data', (chunk: Buffer | string) => {
      stdout += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk: Buffer | string) => {
      stderr += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
    });

    child.on('error', (err) => {
      finalize({ ok: false, error: `spawn_error: ${err.message}`, stderr });
    });

    child.on('close', (code) => {
      const parsed = parseJsonFromStdout(stdout);
      if (!parsed.ok && !parsed.error && code !== 0) {
        parsed.error = `bridge_exit_${String(code)}`;
      }
      if (stderr && !parsed.stderr) {
        parsed.stderr = stderr.trim().slice(-4000);
      }
      finalize(parsed);
    });

    try {
      child.stdin.write(JSON.stringify(fullPayload), 'utf8');
      child.stdin.end();
    } catch (err: any) {
      finalize({ ok: false, error: `bridge_stdin_failed: ${err?.message || 'unknown'}` });
    }
  });
}

export async function resolveTikTokDownloaderAccountId(
  platform: TikTokDownloaderPlatform,
  input: string,
  cookie: string,
  proxy = '',
  abortSignal?: AbortSignal,
): Promise<{ ok: boolean; accountId: string | null; error?: string; stderr?: string }> {
  const requestPayload = {
    action: 'resolve_user',
    platform,
    input,
    cookie,
    proxy,
  } as const;
  const result = await runBridge(requestPayload, abortSignal);

  if (!result.ok && cookie && shouldRetryWithoutCookie(String(result.error || ''))) {
    const retry = await runBridge({ ...requestPayload, cookie: '' }, abortSignal);
    if (retry.ok) {
      const accountId = String(retry.account_id || '').trim() || null;
      if (accountId) {
        return { ok: true, accountId };
      }
    }
    return {
      ok: false,
      accountId: null,
      error: String(retry.error || result.error || 'resolve_failed'),
      stderr: mergeStderr(String(result.stderr || ''), String(retry.stderr || '')),
    };
  }

  if (!result.ok) {
    return { ok: false, accountId: null, error: String(result.error || 'resolve_failed'), stderr: String(result.stderr || '') || undefined };
  }

  const accountId = String(result.account_id || '').trim() || null;
  if (!accountId) {
    return { ok: false, accountId: null, error: 'resolve_empty_account_id', stderr: String(result.stderr || '') || undefined };
  }
  return { ok: true, accountId };
}

export async function fetchTikTokDownloaderAccountFeed(
  platform: TikTokDownloaderPlatform,
  accountInput: string,
  cookie: string,
  options: { proxy?: string; tab?: string; pages?: number; cursor?: number; count?: number; abortSignal?: AbortSignal } = {},
): Promise<TikTokDownloaderAccountResult> {
  const requestPayload = {
    action: 'account',
    platform,
    account_input: accountInput,
    cookie,
    proxy: options.proxy || '',
    tab: options.tab || 'post',
    pages: options.pages,
    cursor: options.cursor,
    count: options.count,
  } as const;
  const result = await runBridge(requestPayload, options.abortSignal);

  if (!result.ok && cookie && shouldRetryWithoutCookie(String(result.error || ''))) {
    const retry = await runBridge({ ...requestPayload, cookie: '' }, options.abortSignal);
    if (retry.ok) {
      const accountId = String(retry.resolved_account_id || '').trim() || null;
      return {
        ok: true,
        accountId,
        channel: toChannelSnapshot(retry.channel),
        entries: toNormalizedItems(retry.entries),
        stderr: mergeStderr(String(result.stderr || ''), String(retry.stderr || '')),
      };
    }
    return {
      ok: false,
      accountId: null,
      channel: null,
      entries: [],
      error: String(retry.error || result.error || 'account_failed'),
      stderr: mergeStderr(String(result.stderr || ''), String(retry.stderr || '')),
    };
  }

  if (!result.ok) {
    return {
      ok: false,
      accountId: null,
      channel: null,
      entries: [],
      error: String(result.error || 'account_failed'),
      stderr: String(result.stderr || '') || undefined,
    };
  }

  const accountId = String(result.resolved_account_id || '').trim() || null;
  return {
    ok: true,
    accountId,
    channel: toChannelSnapshot(result.channel),
    entries: toNormalizedItems(result.entries),
    stderr: String(result.stderr || '') || undefined,
  };
}

export async function fetchTikTokDownloaderVideoDetail(
  platform: TikTokDownloaderPlatform,
  detailInput: string,
  cookie: string,
  options: { proxy?: string; abortSignal?: AbortSignal } = {},
): Promise<TikTokDownloaderDetailResult> {
  const requestPayload = {
    action: 'detail',
    platform,
    detail_input: detailInput,
    cookie,
    proxy: options.proxy || '',
  } as const;
  const result = await runBridge(requestPayload, options.abortSignal);

  if (!result.ok && cookie && shouldRetryWithoutCookie(String(result.error || ''))) {
    const retry = await runBridge({ ...requestPayload, cookie: '' }, options.abortSignal);
    if (retry.ok) {
      const items = toNormalizedItems(retry.item ? [retry.item] : []);
      return {
        ok: true,
        item: items[0] || null,
        stderr: mergeStderr(String(result.stderr || ''), String(retry.stderr || '')),
      };
    }
    return {
      ok: false,
      item: null,
      error: String(retry.error || result.error || 'detail_failed'),
      stderr: mergeStderr(String(result.stderr || ''), String(retry.stderr || '')),
    };
  }

  if (!result.ok) {
    return {
      ok: false,
      item: null,
      error: String(result.error || 'detail_failed'),
      stderr: String(result.stderr || '') || undefined,
    };
  }

  const items = toNormalizedItems(result.item ? [result.item] : []);
  return {
    ok: true,
    item: items[0] || null,
    stderr: String(result.stderr || '') || undefined,
  };
}

export async function validateTikTokDownloaderCookie(
  platform: TikTokDownloaderPlatform,
  cookie: string,
  options: { proxy?: string; abortSignal?: AbortSignal } = {},
): Promise<TikTokDownloaderCookieValidationResult> {
  const result = await runBridge({
    action: 'validate_cookie',
    platform,
    cookie,
    proxy: options.proxy || '',
  }, options.abortSignal);

  if (!result.ok) {
    return {
      ok: false,
      valid: false,
      message: String(result.error || 'tiktok_downloader_cookie_validate_failed'),
      error: String(result.error || 'tiktok_downloader_cookie_validate_failed'),
      stderr: String(result.stderr || '') || undefined,
    };
  }

  const valid = String(result.valid || '').toLowerCase() === 'true' || result.valid === true;
  return {
    ok: true,
    valid,
    message: String(result.message || (valid ? 'ok' : 'invalid')),
    userId: String(result.user_id || '').trim() || null,
    nickname: String(result.nickname || '').trim() || null,
    stderr: String(result.stderr || '') || undefined,
  };
}
