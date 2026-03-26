import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { getSetting } from '../db.js';

interface XhsBridgePayload {
  action: 'account' | 'validate_cookie';
  tool_root: string;
  user_input?: string;
  cookie: string;
  limit?: number;
  include_note_detail?: boolean;
}

interface XhsBridgeResult {
  ok: boolean;
  action?: string;
  error?: string;
  stderr?: string;
  traceback?: string;
  [key: string]: unknown;
}

export interface XhsSpiderNormalizedItem {
  id: string;
  title: string;
  description: string | null;
  uploader: string | null;
  channel_id: string | null;
  uploader_id: string | null;
  published_at: string | null;
  timestamp: number | null;
  duration_sec: number | null;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  collect_count: number | null;
  share_count: number | null;
  webpage_url: string | null;
  thumbnail: string | null;
  tags: string[];
  content_type: 'long' | 'short' | 'album' | null;
  raw?: any;
}

export interface XhsSpiderChannelSnapshot {
  title: string | null;
  avatar_url: string | null;
  follower_count: number | null;
  note_count: number | null;
  total_view_count: number | null;
  user_id: string | null;
  handle: string | null;
  profile_url: string | null;
}

export interface XhsSpiderAccountResult {
  ok: boolean;
  userId: string | null;
  channel: XhsSpiderChannelSnapshot | null;
  entries: XhsSpiderNormalizedItem[];
  error?: string;
  stderr?: string;
}

export interface XhsSpiderCookieValidationResult {
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

function parseJsonFromStdout(stdout: string): XhsBridgeResult {
  const trimmed = stdout.trim();
  if (!trimmed) return { ok: false, error: 'xhs_bridge_empty_stdout' };
  try {
    return JSON.parse(trimmed) as XhsBridgeResult;
  } catch {
    const lines = trimmed
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      try {
        return JSON.parse(lines[i]) as XhsBridgeResult;
      } catch {
        // Continue.
      }
    }
  }
  return { ok: false, error: `xhs_bridge_non_json_stdout:${trimmed.slice(-500)}` };
}

function getBridgeScriptPath(): string {
  return resolvePathSettingWithFallback('xhs_spider_bridge_script', [
    path.join(process.cwd(), 'scripts', 'spider_xhs_bridge.py'),
    path.join(process.cwd(), 'server', 'scripts', 'spider_xhs_bridge.py'),
  ]);
}

function getSpiderRoot(): string {
  return resolvePathSettingWithFallback('xhs_spider_root', [
    path.join(process.cwd(), 'server', 'vendor', 'spider_xhs'),
    path.join(process.cwd(), 'vendor', 'spider_xhs'),
    path.join(process.cwd(), '..', 'Spider_XHS'),
    path.join(process.cwd(), 'Spider_XHS'),
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
  const configured = String(getSetting('xhs_spider_python') || '').trim();
  return configured || 'python';
}

function getCondaEnvName(): string {
  return String(getSetting('xhs_spider_conda_env') || '').trim();
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
  const raw = Number(getSetting('xhs_spider_timeout_sec') || `${DEFAULT_TIMEOUT_SEC}`);
  if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_TIMEOUT_SEC * 1000;
  return Math.max(10, Math.min(raw, 600)) * 1000;
}

function isEnabledBySetting(): boolean {
  const raw = String(getSetting('xhs_spider_enabled') || 'true').trim().toLowerCase();
  return raw !== 'false' && raw !== '0' && raw !== 'off';
}

export function isXhsSpiderAvailable(): { ok: boolean; reason?: string } {
  if (!isEnabledBySetting()) {
    return { ok: false, reason: 'xhs_spider_disabled' };
  }
  const root = getSpiderRoot();
  if (!fs.existsSync(root)) {
    return { ok: false, reason: `xhs_spider_root_not_found:${root}` };
  }
  const script = getBridgeScriptPath();
  if (!fs.existsSync(script)) {
    return { ok: false, reason: `xhs_spider_bridge_missing:${script}` };
  }
  return { ok: true };
}

async function runBridge(payload: Omit<XhsBridgePayload, 'tool_root'>, abortSignal?: AbortSignal): Promise<XhsBridgeResult> {
  const available = isXhsSpiderAvailable();
  if (!available.ok) return { ok: false, error: available.reason || 'xhs_spider_unavailable' };

  const toolRoot = getSpiderRoot();
  const scriptPath = getBridgeScriptPath();
  const python = getPythonBin();
  const condaEnv = getCondaEnvName();
  const condaEnvPython = resolveCondaEnvPython(condaEnv);
  const timeoutMs = getTimeoutMs();
  const fullPayload: XhsBridgePayload = { ...payload, tool_root: toolRoot };

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
        XHS_BRIDGE_PAYLOAD: JSON.stringify(fullPayload),
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

    const finalize = (result: XhsBridgeResult) => {
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
      finalize({ ok: false, error: `xhs_spider_timeout_${timeoutMs}ms`, stderr });
    }, timeoutMs);

    child.stdout.on('data', (chunk: Buffer | string) => {
      stdout += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk: Buffer | string) => {
      stderr += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
    });

    child.on('error', (err) => {
      finalize({ ok: false, error: `spawn_error:${err.message}`, stderr });
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
      finalize({ ok: false, error: `bridge_stdin_failed:${err?.message || 'unknown'}` });
    }
  });
}

function toNormalizedItems(value: unknown): XhsSpiderNormalizedItem[] {
  if (!Array.isArray(value)) return [];
  const rows: XhsSpiderNormalizedItem[] = [];
  for (const item of value) {
    if (!item || typeof item !== 'object') continue;
    const row = item as Record<string, unknown>;
    const id = String(row.id || '').trim();
    if (!id) continue;
    const rawType = String(row.content_type || '').trim().toLowerCase();
    const contentType = (rawType === 'long' || rawType === 'short' || rawType === 'album') ? rawType : null;
    rows.push({
      id,
      title: String(row.title || 'Untitled').trim() || 'Untitled',
      description: String(row.description || '').trim() || null,
      uploader: String(row.uploader || '').trim() || null,
      channel_id: String(row.channel_id || '').trim() || null,
      uploader_id: String(row.uploader_id || '').trim() || null,
      published_at: String(row.published_at || '').trim() || null,
      timestamp: toNullableInt(row.timestamp),
      duration_sec: toNullableInt(row.duration_sec),
      view_count: toNullableInt(row.view_count),
      like_count: toNullableInt(row.like_count),
      comment_count: toNullableInt(row.comment_count),
      collect_count: toNullableInt(row.collect_count),
      share_count: toNullableInt(row.share_count),
      webpage_url: String(row.webpage_url || '').trim() || null,
      thumbnail: String(row.thumbnail || '').trim() || null,
      tags: Array.isArray(row.tags)
        ? row.tags.map((tag) => String(tag || '').trim()).filter(Boolean)
        : [],
      content_type: contentType as 'long' | 'short' | 'album' | null,
      raw: row.raw ?? null,
    });
  }
  return rows;
}

function toChannelSnapshot(value: unknown): XhsSpiderChannelSnapshot | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as Record<string, unknown>;
  return {
    title: String(row.title || '').trim() || null,
    avatar_url: String(row.avatar_url || '').trim() || null,
    follower_count: toNullableInt(row.follower_count),
    note_count: toNullableInt(row.note_count),
    total_view_count: toNullableInt(row.total_view_count),
    user_id: String(row.user_id || '').trim() || null,
    handle: String(row.handle || '').trim() || null,
    profile_url: String(row.profile_url || '').trim() || null,
  };
}

export async function fetchXhsSpiderAccountFeed(
  userInput: string,
  cookie: string,
  options: { limit?: number; includeNoteDetail?: boolean; abortSignal?: AbortSignal } = {},
): Promise<XhsSpiderAccountResult> {
  const limit = Number.isFinite(Number(options.limit))
    ? Math.max(1, Math.min(Number(options.limit), 500))
    : 50;
  const result = await runBridge({
    action: 'account',
    user_input: userInput,
    cookie,
    limit,
    include_note_detail: options.includeNoteDetail ?? true,
  }, options.abortSignal);

  if (!result.ok) {
    return {
      ok: false,
      userId: null,
      channel: null,
      entries: [],
      error: String(result.error || 'xhs_account_failed'),
      stderr: String(result.stderr || '') || undefined,
    };
  }

  return {
    ok: true,
    userId: String(result.resolved_user_id || '').trim() || null,
    channel: toChannelSnapshot(result.channel),
    entries: toNormalizedItems(result.entries),
    stderr: String(result.stderr || '') || undefined,
  };
}

export async function validateXhsSpiderCookie(
  cookie: string,
  options: { abortSignal?: AbortSignal } = {},
): Promise<XhsSpiderCookieValidationResult> {
  const result = await runBridge({
    action: 'validate_cookie',
    cookie,
  }, options.abortSignal);

  if (!result.ok) {
    return {
      ok: false,
      valid: false,
      message: String(result.error || 'xhs_cookie_validate_failed'),
      error: String(result.error || 'xhs_cookie_validate_failed'),
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
