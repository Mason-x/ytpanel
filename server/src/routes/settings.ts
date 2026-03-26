import { Router, Request, Response } from 'express';
import { spawnSync } from 'child_process';
import https from 'https';
import fs from 'fs';
import path from 'path';
import { ProxyAgent } from 'undici';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { getAllSettings, getDb, getSetting, setSetting } from '../db.js';
import { scheduleDailySyncFromSettings } from '../services/dailySyncScheduler.js';
import { getYoutubeApiUsageStatus, testYoutubeApiBindingConnectivity } from '../services/youtubeApi.js';
import { isXhsSpiderAvailable, validateXhsSpiderCookie } from '../services/xhsSpider.js';
import { isTikTokDownloaderAvailable, validateTikTokDownloaderCookie } from '../services/tiktokDownloader.js';
import {
  getYoutubeCookiePoolItems,
  getYoutubeCookiePoolState,
  normalizeYoutubeCookieProxy,
  saveYoutubeCookiePoolState,
  updateYoutubeCookiePoolItem,
  type YoutubeCookiePoolItem,
} from '../services/youtubeCookiePool.js';
import {
  PLAYWRIGHT_SESSION_PLATFORMS,
  clearPlaywrightSession,
  getDefaultLoginUrlByPlatform,
  getPlaywrightSessionInfo,
  getPlaywrightSessionUserDataDir,
  normalizePlaywrightSessionPlatform,
  writePlaywrightSessionMeta,
  type PlaywrightSessionPlatform,
} from '../services/playwrightSession.js';

const router = Router();

type SessionRunStatus = 'running' | 'done' | 'failed';
type SessionRunRecord = {
  platform: PlaywrightSessionPlatform;
  status: SessionRunStatus;
  started_at: string;
  finished_at: string | null;
  target_url: string;
  timeout_sec: number;
  error: string | null;
  context: any | null;
  timer: NodeJS.Timeout | null;
};

const sessionRuns = new Map<PlaywrightSessionPlatform, SessionRunRecord>();

type BitBrowserPlatform = 'youtube';

type CookieValidatePlatform = 'youtube' | 'bilibili' | 'tiktok' | 'douyin' | 'xiaohongshu';
const COOKIE_VALIDATE_PLATFORMS = new Set<CookieValidatePlatform>([
  'youtube',
  'bilibili',
  'tiktok',
  'douyin',
  'xiaohongshu',
]);
const COOKIE_SETTING_KEY_BY_PLATFORM: Record<CookieValidatePlatform, string> = {
  youtube: 'yt_dlp_cookie_file_youtube',
  bilibili: 'yt_dlp_cookie_file_bilibili',
  tiktok: 'yt_dlp_cookie_file_tiktok',
  douyin: 'yt_dlp_cookie_file_douyin',
  xiaohongshu: 'yt_dlp_cookie_file_xiaohongshu',
};
const COOKIE_HOST_HINT_BY_PLATFORM: Record<CookieValidatePlatform, string> = {
  youtube: 'youtube.com',
  bilibili: 'bilibili.com',
  tiktok: 'tiktok.com',
  douyin: 'douyin.com',
  xiaohongshu: 'xiaohongshu.com',
};

const MASKED_SETTING_PREFIX = '__VR_MASKED_SETTING__:';
const MASKED_COOKIE_POOL_PREFIX = '__VR_MASKED_COOKIE_POOL__:';
const YOUTUBE_API_BINDING_MASK_KEY = 'youtube_api_binding';
const MASKED_COOKIE_PLACEHOLDER = '[已保存，留空保持原值；输入新值可覆盖]';
const MASKED_API_KEY_PLACEHOLDER = '[已保存 API Key，留空保持原值；输入新值可覆盖]';
const SENSITIVE_SINGLE_SETTING_KEYS = new Set<string>([
  'yt_dlp_cookie_file',
  'yt_dlp_cookie_file_youtube',
  'yt_dlp_cookie_file_bilibili',
  'yt_dlp_cookie_file_tiktok',
  'yt_dlp_cookie_file_douyin',
  'yt_dlp_cookie_file_xiaohongshu',
  'yt_dlp_youtube_po_token',
]);

type JsonRequestResult = {
  networkError: boolean;
  parseError: boolean;
  status: number;
  payload: any;
};

function parseKeyList(raw: string): string[] {
  const text = String(raw || '').trim();
  if (!text) return [];
  return text.split(/[\r\n,;]+/).map((item) => item.trim()).filter(Boolean);
}

function parseProxyList(raw: string): string[] {
  const source = String(raw || '');
  if (!source.trim()) return [];
  const normalized = source.replace(/\r/g, '');
  const lines = normalized.split('\n').map((item) => item.trim());
  if (lines.length <= 1) {
    return source.split(/[,;]+/).map((item) => item.trim());
  }
  return lines;
}

function maskApiKeyPreview(value: string): string {
  const trimmed = String(value || '').trim();
  if (!trimmed) return '';
  if (trimmed.length <= 7) return `${trimmed.slice(0, 2)}***${trimmed.slice(-2)}`;
  return `${trimmed.slice(0, 3)}***${trimmed.slice(-4)}`;
}

function createMaskedSettingPlaceholder(key: string, index = 0): string {
  const normalizedKey = String(key || '').trim();
  const normalizedIndex = Number.isFinite(index) ? Math.max(0, Math.trunc(index)) : 0;
  if (!normalizedKey) return '';
  return `${MASKED_SETTING_PREFIX}${normalizedKey}:${normalizedIndex}`;
}

function parseMaskedSettingPlaceholder(value: unknown): { key: string; index: number } | null {
  const text = String(value || '').trim();
  if (!text.startsWith(MASKED_SETTING_PREFIX)) return null;
  const body = text.slice(MASKED_SETTING_PREFIX.length);
  const splitIndex = body.lastIndexOf(':');
  if (splitIndex <= 0) return null;
  const key = body.slice(0, splitIndex).trim();
  const index = Number(body.slice(splitIndex + 1));
  if (!key || !Number.isFinite(index) || index < 0) return null;
  return { key, index: Math.trunc(index) };
}

function createMaskedCookiePoolPlaceholder(id: string): string {
  const normalizedId = String(id || '').trim();
  if (!normalizedId) return '';
  return `${MASKED_COOKIE_POOL_PREFIX}${normalizedId}`;
}

function parseMaskedCookiePoolPlaceholder(value: unknown): string | null {
  const text = String(value || '').trim();
  if (!text.startsWith(MASKED_COOKIE_POOL_PREFIX)) return null;
  const id = text.slice(MASKED_COOKIE_POOL_PREFIX.length).trim();
  return id || null;
}

function sanitizeSettingsForClient(settings: Record<string, string>): Record<string, string> {
  const next = { ...settings };
  for (const key of SENSITIVE_SINGLE_SETTING_KEYS) {
    const current = String(next[key] || '').trim();
    next[key] = current ? createMaskedSettingPlaceholder(key, 0) : '';
  }

  const primaryApiKey = String(settings.youtube_api_key || '').trim();
  const extraApiKeys = parseKeyList(String(settings.youtube_api_keys || ''));
  next.youtube_api_key = primaryApiKey ? createMaskedSettingPlaceholder(YOUTUBE_API_BINDING_MASK_KEY, 0) : '';
  next.youtube_api_key_masked_preview = primaryApiKey ? maskApiKeyPreview(primaryApiKey) : '';
  next.youtube_api_keys = extraApiKeys
    .map((_item, index) => createMaskedSettingPlaceholder(YOUTUBE_API_BINDING_MASK_KEY, index + 1))
    .join('\n');

  next.youtube_cookie_pool_json = '[]';
  return next;
}

function sanitizeYoutubeCookiePoolItem(item: YoutubeCookiePoolItem): YoutubeCookiePoolItem {
  return {
    ...item,
    cookie_header: String(item.cookie_header || '').trim()
      ? createMaskedCookiePoolPlaceholder(item.id)
      : '',
  };
}

function sanitizeYoutubeCookiePoolState(state: { enabled: boolean; items: YoutubeCookiePoolItem[] }): { enabled: boolean; items: YoutubeCookiePoolItem[] } {
  return {
    enabled: !!state.enabled,
    items: Array.isArray(state.items) ? state.items.map((item) => sanitizeYoutubeCookiePoolItem(item)) : [],
  };
}

function resolveSingleSensitiveSettingValue(
  settingKey: string,
  value: unknown,
  currentSettings: Record<string, string>,
): string {
  const text = String(value ?? '').trim();
  if (!text) return '';
  if (text === MASKED_COOKIE_PLACEHOLDER || text === MASKED_API_KEY_PLACEHOLDER) {
    return String(currentSettings[settingKey] || '');
  }
  const masked = parseMaskedSettingPlaceholder(text);
  if (masked && masked.key === settingKey && masked.index === 0) {
    return String(currentSettings[settingKey] || '');
  }
  return String(value ?? '');
}

function getCurrentYoutubeApiBindings(settings: Record<string, string>): Array<{ apiKey: string; proxy: string }> {
  const primary = String(settings.youtube_api_key || '').trim();
  const extras = parseKeyList(String(settings.youtube_api_keys || ''));
  const proxies = parseProxyList(String(settings.youtube_api_key_proxies || ''));
  const keys = [primary, ...extras].filter(Boolean);
  const rows: Array<{ apiKey: string; proxy: string }> = [];
  for (let i = 0; i < keys.length; i += 1) {
    rows.push({
      apiKey: String(keys[i] || '').trim(),
      proxy: String(proxies[i] || '').trim(),
    });
  }
  return rows;
}

function resolveYoutubeApiBindingRows(
  updates: Record<string, unknown>,
  currentSettings: Record<string, string>,
): Array<{ apiKey: string; proxy: string }> {
  const currentRows = getCurrentYoutubeApiBindings(currentSettings);
  const fallbackPrimary = currentRows[0]
    ? createMaskedSettingPlaceholder(YOUTUBE_API_BINDING_MASK_KEY, 0)
    : '';
  const fallbackExtras = currentRows.slice(1)
    .map((_row, index) => createMaskedSettingPlaceholder(YOUTUBE_API_BINDING_MASK_KEY, index + 1))
    .join('\n');
  const fallbackProxies = currentRows.map((row) => row.proxy).join('\n');

  const primaryRaw = Object.prototype.hasOwnProperty.call(updates, 'youtube_api_key')
    ? String(updates.youtube_api_key ?? '')
    : fallbackPrimary;
  const extrasRaw = Object.prototype.hasOwnProperty.call(updates, 'youtube_api_keys')
    ? String(updates.youtube_api_keys ?? '')
    : fallbackExtras;
  const proxiesRaw = Object.prototype.hasOwnProperty.call(updates, 'youtube_api_key_proxies')
    ? String(updates.youtube_api_key_proxies ?? '')
    : fallbackProxies;

  const keyInputs = [String(primaryRaw || '').trim(), ...parseKeyList(extrasRaw)].filter(Boolean);
  const proxyInputs = parseProxyList(proxiesRaw);

  return keyInputs.map((input, index) => {
    const masked = parseMaskedSettingPlaceholder(input);
    const resolvedKey = masked && masked.key === YOUTUBE_API_BINDING_MASK_KEY
      ? String(currentRows[masked.index]?.apiKey || '')
      : String(input || '').trim();
    return {
      apiKey: resolvedKey,
      proxy: String(proxyInputs[index] || '').trim(),
    };
  }).filter((row) => row.apiKey);
}

function resolveYoutubeCookiePoolItemsForSave(items: unknown[]): YoutubeCookiePoolItem[] {
  const currentById = new Map(
    getYoutubeCookiePoolItems().map((item) => [item.id, item] as const),
  );

  return items.map((entry) => {
    if (!entry || typeof entry !== 'object') return entry as YoutubeCookiePoolItem;
    const row = { ...(entry as Record<string, unknown>) };
    const itemId = String(row.id || '').trim();
    const cookieHeader = String(row.cookie_header || '');
    const maskedPoolId = parseMaskedCookiePoolPlaceholder(cookieHeader);
    if (maskedPoolId && maskedPoolId === itemId) {
      row.cookie_header = String(currentById.get(itemId)?.cookie_header || '');
    }
    return row as unknown as YoutubeCookiePoolItem;
  });
}

function isSocksProxy(proxyUrl: string): boolean {
  const lower = String(proxyUrl || '').trim().toLowerCase();
  return lower.startsWith('socks5://') || lower.startsWith('socks://') || lower.startsWith('socks4://');
}

function formatProxyDisplay(proxyUrl: string): string {
  const normalized = normalizeYoutubeCookieProxy(proxyUrl);
  if (!normalized) return 'direct';
  try {
    const parsed = new URL(normalized);
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return 'direct';
  }
}

function getProxyMode(proxyUrl: string): 'direct' | 'http' | 'https' | 'socks5' {
  const normalized = normalizeYoutubeCookieProxy(proxyUrl).toLowerCase();
  if (!normalized) return 'direct';
  if (normalized.startsWith('https://')) return 'https';
  if (normalized.startsWith('http://')) return 'http';
  if (isSocksProxy(normalized)) return 'socks5';
  return 'direct';
}

async function executeJsonRequest(url: URL, proxyUrl: string): Promise<JsonRequestResult> {
  const normalizedProxy = normalizeYoutubeCookieProxy(proxyUrl);

  if (isSocksProxy(normalizedProxy)) {
    let socksAgent: SocksProxyAgent | null = null;
    try {
      socksAgent = new SocksProxyAgent(normalizedProxy);
    } catch {
      return { networkError: true, parseError: false, status: 0, payload: null };
    }
    return await new Promise((resolve) => {
      const req = https.request(url, { method: 'GET', agent: socksAgent, timeout: 25_000 }, (resp) => {
        const chunks: string[] = [];
        resp.setEncoding('utf8');
        resp.on('data', (chunk) => chunks.push(String(chunk)));
        resp.on('end', () => {
          const status = Number(resp.statusCode || 0);
          const text = chunks.join('');
          try {
            const payload = text ? JSON.parse(text) : {};
            resolve({ networkError: false, parseError: false, status, payload });
          } catch {
            resolve({ networkError: false, parseError: true, status, payload: null });
          }
        });
      });
      req.on('error', () => resolve({ networkError: true, parseError: false, status: 0, payload: null }));
      req.on('timeout', () => req.destroy(new Error('request_timeout')));
      req.end();
    });
  }

  const options: any = {};
  let proxyAgent: ProxyAgent | null = null;
  if (normalizedProxy) {
    try {
      proxyAgent = new ProxyAgent(normalizedProxy);
      options.dispatcher = proxyAgent;
    } catch {
      return { networkError: true, parseError: false, status: 0, payload: null };
    }
  }
  try {
    const response = await fetch(url.toString(), options);
    try {
      const payload = await response.json();
      return { networkError: false, parseError: false, status: Number(response.status || 0), payload };
    } catch {
      return { networkError: false, parseError: true, status: Number(response.status || 0), payload: null };
    }
  } catch {
    return { networkError: true, parseError: false, status: 0, payload: null };
  } finally {
    if (proxyAgent) {
      await proxyAgent.close().catch(() => null);
    }
  }
}

function parseCookieValidatePlatform(value: unknown): CookieValidatePlatform | null {
  const raw = String(value || '').trim().toLowerCase();
  if (COOKIE_VALIDATE_PLATFORMS.has(raw as CookieValidatePlatform)) return raw as CookieValidatePlatform;
  return null;
}

function normalizeCookieHeaderValue(text: string): string {
  return String(text || '')
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
    if (domain && !domain.includes(hostHint.toLowerCase())) continue;
    const name = String(cols[5] || '').trim();
    const value = String(cols[6] || '').trim();
    if (!name || !value) continue;
    pairs.push(`${name}=${value}`);
  }
  if (pairs.length === 0) return null;
  return pairs.join('; ');
}

function parseCookieHeaderFromJsonText(text: string, hostHint: string): string | null {
  try {
    const parsed = JSON.parse(text);
    const pairs: string[] = [];
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        const row = item as any;
        const name = String(row?.name || '').trim();
        const value = String(row?.value || '').trim();
        const domain = String(row?.domain || '').trim().toLowerCase();
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

function getWorkspacePathCandidates(): string[] {
  const rawCandidates = [process.cwd(), path.resolve(process.cwd(), '..')];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const candidate of rawCandidates) {
    const resolved = path.resolve(candidate);
    const key = resolved.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(resolved);
  }
  return out;
}

function tryResolveMovedWorkspacePath(inputPath: string): string | null {
  const resolvedInput = path.resolve(inputPath);
  if (!path.isAbsolute(resolvedInput) || fs.existsSync(resolvedInput)) {
    return fs.existsSync(resolvedInput) ? resolvedInput : null;
  }

  const parts = resolvedInput.split(/[\\/]+/).filter(Boolean);
  if (parts.length < 2) return null;

  const anchorNames = new Set([
    'server',
    'client',
    'scripts',
    'data',
    'downloads',
    'json',
    'spider_xhs',
    'tiktokdownloader',
  ]);

  for (const workspaceRoot of getWorkspacePathCandidates()) {
    for (let i = 0; i < parts.length; i += 1) {
      const segment = String(parts[i] || '').trim().toLowerCase();
      if (!anchorNames.has(segment)) continue;
      const candidate = path.join(workspaceRoot, ...parts.slice(i));
      if (fs.existsSync(candidate)) return candidate;
    }

    const baseName = path.basename(resolvedInput);
    if (baseName) {
      for (const relDir of ['data', path.join('server', 'data'), '.', 'server']) {
        const candidate = path.join(workspaceRoot, relDir, baseName);
        if (fs.existsSync(candidate)) return candidate;
      }
    }
  }

  return null;
}

function resolveCookieHeaderFromInput(cookieInput: string, hostHint: string): string | null {
  const settingValue = String(cookieInput || '').trim();
  if (!settingValue) return null;

  let source = settingValue;
  try {
    const maybePath = path.resolve(settingValue);
    if (fs.existsSync(maybePath)) {
      source = fs.readFileSync(maybePath, 'utf8');
    } else {
      const relocatedPath = tryResolveMovedWorkspacePath(settingValue);
      if (relocatedPath) {
        source = fs.readFileSync(relocatedPath, 'utf8');
      } else if (path.isAbsolute(settingValue)) {
        // A stale absolute path should not be treated as raw cookie text.
        return null;
      }
    }
  } catch {
    // ignore read failure and fallback to raw text
  }

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
    return normalizeCookieHeaderValue(cookieLine.replace(/^cookie\s*:/i, '').trim()) || null;
  }
  return normalizeCookieHeaderValue(raw) || null;
}

function getCookieDomainForPlatform(platform: CookieValidatePlatform): string {
  switch (platform) {
    case 'youtube':
      return '.youtube.com';
    case 'bilibili':
      return '.bilibili.com';
    case 'tiktok':
      return '.tiktok.com';
    case 'douyin':
      return '.douyin.com';
    case 'xiaohongshu':
      return '.xiaohongshu.com';
    default:
      return '.youtube.com';
  }
}

function parseCookieEntries(header: string): Array<{ name: string; value: string }> {
  const out: Array<{ name: string; value: string }> = [];
  for (const chunk of String(header || '').split(';')) {
    const part = chunk.trim();
    if (!part) continue;
    const idx = part.indexOf('=');
    if (idx <= 0) continue;
    const name = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (!name || !value) continue;
    out.push({ name, value });
  }
  return out;
}

function writeTempCookieFileForValidation(platform: CookieValidatePlatform, cookieHeader: string): string | null {
  const entries = parseCookieEntries(cookieHeader);
  if (entries.length === 0) return null;

  const domain = getCookieDomainForPlatform(platform);
  const lines = entries.map((item) => {
    const name = item.name.replace(/[\r\n\t]/g, ' ').trim();
    const value = item.value.replace(/[\r\n\t]/g, ' ').trim();
    return `${domain}\tTRUE\t/\tTRUE\t2147483647\t${name}\t${value}`;
  });

  const content = `# Netscape HTTP Cookie File\n\n${lines.join('\n')}\n`;
  const dataDir = path.join(process.cwd(), 'data');
  fs.mkdirSync(dataDir, { recursive: true });
  const filePath = path.join(dataDir, `validate.cookies.${platform}.${Date.now()}.${Math.random().toString(36).slice(2, 8)}.txt`);
  fs.writeFileSync(filePath, content, 'utf8');
  return filePath;
}

async function validateYtdlpCookieOnline(
  platform: 'youtube' | 'bilibili',
  cookieHeader: string,
  proxyUrl = '',
): Promise<{ valid: boolean; message: string }> {
  const probeUrl = platform === 'youtube'
    ? 'https://www.youtube.com/playlist?list=WL'
    : 'https://www.bilibili.com/watchlater/';
  const probeLabel = platform === 'youtube' ? 'YouTube' : 'Bilibili';

  let cookieFilePath: string | null = null;
  try {
    cookieFilePath = writeTempCookieFileForValidation(platform, cookieHeader);
    if (!cookieFilePath) {
      return { valid: false, message: `${probeLabel} Cookie 无法解析为引擎可用格式` };
    }

    const args = [
      '--skip-download',
      '--dump-single-json',
      '--no-warnings',
      '--encoding', 'utf-8',
      '--cookies', cookieFilePath,
    ];
    if (getSetting('yt_dlp_disable_plugins') !== 'false') {
      args.push('--no-plugin-dirs');
    }
    if (platform === 'youtube') {
      args.push('--flat-playlist', '--ignore-no-formats-error');
    }
    args.push(probeUrl);
    const normalizedProxy = normalizeYoutubeCookieProxy(proxyUrl);
    if (normalizedProxy) {
      args.splice(args.length - 1, 0, '--proxy', normalizedProxy);
    }

    const result = spawnSync('yt-dlp', args, {
      encoding: 'utf8',
      windowsHide: true,
      timeout: 45_000,
      maxBuffer: 8 * 1024 * 1024,
    });

    if (result.error) {
      const errorText = String(result.error?.message || 'unknown');
      if (errorText.toLowerCase().includes('enoent')) {
        return { valid: false, message: 'yt-dlp 未安装或不可执行' };
      }
      return { valid: false, message: `${probeLabel} 引擎校验异常: ${errorText}` };
    }

    const status = Number(result.status ?? -1);
    const stdout = String(result.stdout || '').trim();
    const stderr = String(result.stderr || '').trim();
    const text = `${stderr}\n${stdout}`.toLowerCase();

    if (status === 0) {
      return { valid: true, message: `${probeLabel} Cookie 有效（yt-dlp 引擎校验通过）` };
    }

    if (
      text.includes('need to login')
      || text.includes('session expired')
      || text.includes('sign in')
      || text.includes('playlist does not exist')
      || text.includes('未登录')
    ) {
      return { valid: false, message: `${probeLabel} Cookie 无效或已过期（引擎返回未登录）` };
    }

    const tail = (stderr || stdout || `exit_${status}`).split(/\r?\n/).slice(-2).join(' ').trim();
    return { valid: false, message: `${probeLabel} 引擎校验失败: ${tail || `exit_${status}`}` };
  } catch (err: any) {
    return { valid: false, message: `${probeLabel} 引擎校验异常: ${String(err?.message || err || 'unknown')}` };
  } finally {
    if (cookieFilePath) {
      try {
        fs.unlinkSync(cookieFilePath);
      } catch {
        // ignore
      }
    }
  }
}

async function validatePlatformCookie(
  platform: CookieValidatePlatform,
  cookieHeader: string,
  proxyUrl = '',
): Promise<{ valid: boolean; message: string }> {
  switch (platform) {
    case 'youtube':
      return validateYtdlpCookieOnline('youtube', cookieHeader, proxyUrl);
    case 'bilibili':
      return validateYtdlpCookieOnline('bilibili', cookieHeader, proxyUrl);
    case 'tiktok':
    case 'douyin': {
      const available = isTikTokDownloaderAvailable();
      if (!available.ok) {
        return { valid: false, message: `TikTokDownloader 在线校验不可用（${available.reason || 'unknown'}）` };
      }
      const result = await validateTikTokDownloaderCookie(platform, cookieHeader);
      if (!result.ok) {
        return { valid: false, message: `TikTokDownloader 在线校验失败：${result.message}` };
      }
      if (!result.valid) {
        return { valid: false, message: `${platform === 'douyin' ? '抖音' : 'TikTok'} Cookie 无效：${result.message}` };
      }
      const who = String(result.nickname || result.userId || '').trim();
      const label = platform === 'douyin' ? '抖音' : 'TikTok';
      return { valid: true, message: who ? `${label} Cookie 有效（${who}）` : `${label} Cookie 有效` };
    }
    case 'xiaohongshu': {
      const available = isXhsSpiderAvailable();
      if (!available.ok) {
        return { valid: false, message: `小红书在线校验不可用（Spider_XHS 未启用：${available.reason || 'unknown'}）` };
      }
      const result = await validateXhsSpiderCookie(cookieHeader);
      if (!result.ok) {
        return { valid: false, message: `小红书在线校验失败：${result.message}` };
      }
      if (!result.valid) {
        return { valid: false, message: `小红书 Cookie 无效：${result.message}` };
      }
      const who = String(result.nickname || result.userId || '').trim();
      return { valid: true, message: who ? `小红书 Cookie 有效（${who}）` : '小红书 Cookie 有效' };
    }
    default:
      return { valid: false, message: 'Unsupported platform' };
  }
}

type PlaywrightExtractCookieResult = {
  ok: boolean;
  cookieHeader: string;
  cookieCount: number;
  message: string;
};

function buildCookieHeaderFromPlaywrightCookies(
  rows: Array<{ name?: string; value?: string; domain?: string }>,
  hostHint: string,
): { cookieHeader: string; cookieCount: number } {
  const pairs: string[] = [];
  const seen = new Set<string>();
  const lowerHostHint = String(hostHint || '').trim().toLowerCase();

  for (const row of rows) {
    const name = String(row?.name || '').trim();
    const value = String(row?.value || '').trim();
    const domain = String(row?.domain || '').trim().toLowerCase();
    if (!name || !value) continue;
    if (lowerHostHint && domain && !domain.includes(lowerHostHint)) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    pairs.push(`${name}=${value}`);
  }

  return {
    cookieHeader: pairs.join('; '),
    cookieCount: pairs.length,
  };
}

async function extractCookieFromPlaywrightSession(
  platform: PlaywrightSessionPlatform,
  hostHint: string,
): Promise<PlaywrightExtractCookieResult> {
  const sessionInfo = getPlaywrightSessionInfo(platform);
  if (!sessionInfo.exists) {
    return {
      ok: false,
      cookieHeader: '',
      cookieCount: 0,
      message: 'Playwright 登录会话不存在，请先点击“打开”完成登录',
    };
  }

  let context: any = null;
  let playwrightMod: any = null;
  try {
    playwrightMod = await import('playwright');
  } catch {
    return {
      ok: false,
      cookieHeader: '',
      cookieCount: 0,
      message: 'Playwright 未安装，无法导出会话 Cookie',
    };
  }

  try {
    const userDataDir = getPlaywrightSessionUserDataDir(platform);
    context = await playwrightMod.chromium.launchPersistentContext(userDataDir, {
      headless: true,
      args: ['--disable-blink-features=AutomationControlled'],
      viewport: { width: 1200, height: 800 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
      locale: 'zh-CN',
    });

    const targetUrl = getDefaultLoginUrlByPlatform(platform);
    let cookies = await context.cookies([targetUrl]).catch(() => []);
    if (!Array.isArray(cookies)) cookies = [];

    if (cookies.length === 0) {
      cookies = await context.cookies().catch(() => []);
      if (!Array.isArray(cookies)) cookies = [];
    }

    const built = buildCookieHeaderFromPlaywrightCookies(cookies, hostHint);
    if (!built.cookieHeader) {
      return {
        ok: false,
        cookieHeader: '',
        cookieCount: 0,
        message: '会话中未找到可导出的平台 Cookie，请确认该平台已登录',
      };
    }

    return {
      ok: true,
      cookieHeader: built.cookieHeader,
      cookieCount: built.cookieCount,
      message: '已从 Playwright 登录会话导出 Cookie',
    };
  } catch (err: any) {
    return {
      ok: false,
      cookieHeader: '',
      cookieCount: 0,
      message: `导出失败: ${String(err?.message || err || 'unknown')}`,
    };
  } finally {
    if (context) {
      await context.close().catch(() => null);
    }
  }
}

function parsePlatform(input: unknown): PlaywrightSessionPlatform | null {
  return normalizePlaywrightSessionPlatform(input);
}

function rejectYoutubePlaywrightCookieFlow(res: Response): boolean {
  res.status(400).json({ error: 'YouTube 请使用 BitBrowser 登录并回填 Cookie' });
  return false;
}

function readSessionTimeoutSec(): number {
  const raw = parseInt(String(getSetting('playwright_session_login_timeout_sec') || '300'), 10);
  return Number.isFinite(raw) ? Math.max(30, Math.min(raw, 3600)) : 300;
}

function serializeSessionStatus(platform: PlaywrightSessionPlatform) {
  const persisted = getPlaywrightSessionInfo(platform);
  const running = sessionRuns.get(platform) || null;
  return {
    ...persisted,
    running: running?.status === 'running',
    run_status: running?.status || null,
    started_at: running?.started_at || null,
    finished_at: running?.finished_at || null,
    target_url: running?.target_url || null,
    error: running?.error || null,
  };
}

function parseBitBrowserPlatform(input: unknown): BitBrowserPlatform | null {
  const raw = String(input || '').trim().toLowerCase();
  if (raw === 'youtube') return 'youtube';
  return null;
}

function normalizeBitBrowserApiUrl(input: unknown): string {
  const raw = String(input || '').trim();
  const fallback = 'http://127.0.0.1:54345';
  const value = raw || fallback;
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return fallback;
    return parsed.toString().replace(/\/+$/, '');
  } catch {
    return fallback;
  }
}

function normalizeBitBrowserWindowId(input: unknown): string {
  return String(input || '').trim().slice(0, 128);
}

function isBitBrowserApiSuccess(payload: any): boolean {
  if (!payload || typeof payload !== 'object') return false;
  const successRaw = payload.success;
  if (successRaw === true || successRaw === 'true' || successRaw === 1 || successRaw === '1') return true;
  const statusRaw = payload.status;
  if (statusRaw === true || statusRaw === 'true' || statusRaw === 1 || statusRaw === 200) return true;
  const code = String(payload.code || '').trim().toLowerCase();
  if (code === '0' || code === 'success' || code === 'ok' || code === '200') return true;
  const msg = String(payload.msg || payload.message || '').trim().toLowerCase();
  if (msg === 'success' || msg === 'ok') return true;
  return false;
}

function isBitBrowserAlreadyClosed(payload: any): boolean {
  const text = String(
    payload?.msg
    || payload?.message
    || payload?.error
    || payload?.data?.msg
    || '',
  ).toLowerCase();
  if (!text) return false;
  return (
    text.includes('not found')
    || text.includes('no such')
    || text.includes('already closed')
    || text.includes('已关闭')
    || text.includes('不存在')
    || text.includes('关闭')
  );
}

async function postBitBrowserJson(
  apiUrl: string,
  endpoint: string,
  payload: Record<string, any>,
): Promise<any | null> {
  const target = `${normalizeBitBrowserApiUrl(apiUrl)}${endpoint}`;
  try {
    const response = await fetch(target, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload || {}),
    });
    return await response.json().catch(() => null);
  } catch {
    return null;
  }
}

async function openBitBrowserWindow(apiUrl: string, windowId: string): Promise<{ ok: boolean; ws: string; error: string }> {
  const data = await postBitBrowserJson(apiUrl, '/browser/open', { id: windowId });
  if (!isBitBrowserApiSuccess(data) || !data?.data) {
    return { ok: false, ws: '', error: 'BitBrowser 打开窗口失败，请检查 API 地址和窗口ID' };
  }
  const ws = String(data.data?.ws || '').trim();
  if (!ws) {
    return { ok: false, ws: '', error: 'BitBrowser 未返回调试 ws 地址' };
  }
  return { ok: true, ws, error: '' };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function resolveBitBrowserContext(browser: any, maxWaitMs = 8_000): Promise<any | null> {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const contexts = Array.isArray(browser?.contexts?.()) ? browser.contexts() : [];
    if (contexts.length > 0) return contexts[0];
    await sleep(250);
  }
  try {
    const page = await browser.newPage();
    return page?.context?.() || null;
  } catch {
    return null;
  }
}

async function closeBitBrowserWindow(apiUrl: string, windowId: string): Promise<boolean> {
  const id = String(windowId || '').trim();
  if (!id) return false;
  const checks: Array<{ endpoint: string; payload: Record<string, any> }> = [
    { endpoint: '/browser/close', payload: { id } },
    { endpoint: '/browser/close', payload: { ids: [id] } },
    { endpoint: '/browser/close', payload: { idList: [id] } },
    { endpoint: '/browser/close', payload: { browserId: id } },
    { endpoint: '/browser/closeBrowser', payload: { id } },
    { endpoint: '/browser/closeBrowser', payload: { ids: [id] } },
    { endpoint: '/browser/closeBrowser', payload: { idList: [id] } },
    { endpoint: '/browser/stop', payload: { id } },
    { endpoint: '/browser/stop', payload: { ids: [id] } },
    { endpoint: '/browser/stop', payload: { idList: [id] } },
  ];
  for (let round = 0; round < 3; round += 1) {
    for (const item of checks) {
      const data = await postBitBrowserJson(apiUrl, item.endpoint, item.payload);
      if (isBitBrowserApiSuccess(data) || isBitBrowserAlreadyClosed(data)) return true;
    }
    await sleep(250);
  }
  return false;
}

type BitBrowserExtractCookieResult = {
  ok: boolean;
  cookieHeader: string;
  cookieCount: number;
  message: string;
};

async function extractCookieFromBitBrowserSession(
  apiUrl: string,
  windowId: string,
  hostHint: string,
): Promise<BitBrowserExtractCookieResult> {
  const opened = await openBitBrowserWindow(apiUrl, windowId);
  if (!opened.ok) {
    return {
      ok: false,
      cookieHeader: '',
      cookieCount: 0,
      message: opened.error,
    };
  }

  let browser: any = null;
  try {
    const playwrightMod = await import('playwright');
    browser = await playwrightMod.chromium.connectOverCDP(opened.ws);
    const context = await resolveBitBrowserContext(browser);
    if (!context) {
      return {
        ok: false,
        cookieHeader: '',
        cookieCount: 0,
        message: 'BitBrowser CDP 连接成功，但上下文未就绪，请稍后重试',
      };
    }
    const targetUrl = `https://${hostHint}`;
    let cookies = await context.cookies([targetUrl]).catch(() => []);
    if (!Array.isArray(cookies) || cookies.length === 0) {
      cookies = await context.cookies().catch(() => []);
    }
    const built = buildCookieHeaderFromPlaywrightCookies(cookies || [], hostHint);
    if (!built.cookieHeader) {
      return {
        ok: false,
        cookieHeader: '',
        cookieCount: 0,
        message: '未读取到有效 Cookie，请先在 BitBrowser 中完成登录',
      };
    }
    return {
      ok: true,
      cookieHeader: built.cookieHeader,
      cookieCount: built.cookieCount,
      message: '已从 BitBrowser 会话导出 Cookie',
    };
  } catch (err: any) {
    return {
      ok: false,
      cookieHeader: '',
      cookieCount: 0,
      message: `导出失败: ${String(err?.message || err || 'unknown')}`,
    };
  } finally {
    if (browser) {
      await browser.close().catch(() => null);
    }
  }
}

function findYoutubeCookiePoolItem(itemId: unknown): YoutubeCookiePoolItem | null {
  const id = String(itemId || '').trim();
  if (!id) return null;
  const items = getYoutubeCookiePoolItems();
  return items.find((item) => item.id === id) || null;
}

async function probeProxyEgressIp(proxyUrl: string): Promise<{
  ok: boolean;
  proxy: string;
  proxy_mode: 'direct' | 'http' | 'https' | 'socks5';
  egress_ip: string;
  message: string;
}> {
  const normalizedProxy = normalizeYoutubeCookieProxy(proxyUrl);
  const proxyDisplay = formatProxyDisplay(normalizedProxy);
  const proxyMode = getProxyMode(normalizedProxy);
  const call = await executeJsonRequest(new URL('https://api.ipify.org?format=json'), normalizedProxy);
  if (call.networkError || call.parseError || !(call.status >= 200 && call.status < 300)) {
    return {
      ok: false,
      proxy: proxyDisplay,
      proxy_mode: proxyMode,
      egress_ip: '',
      message: `代理连通失败（${proxyDisplay}）`,
    };
  }
  const egressIp = String(call.payload?.ip || '').trim();
  if (!egressIp) {
    return {
      ok: false,
      proxy: proxyDisplay,
      proxy_mode: proxyMode,
      egress_ip: '',
      message: `代理已连接，但未获取到出口IP（${proxyDisplay}）`,
    };
  }
  return {
    ok: true,
    proxy: proxyDisplay,
    proxy_mode: proxyMode,
    egress_ip: egressIp,
    message: `代理连通成功（${proxyDisplay}），出口IP ${egressIp}`,
  };
}

async function validateYoutubeCookiePoolItem(item: YoutubeCookiePoolItem): Promise<{
  valid: boolean;
  message: string;
  egress_ip: string;
  proxy: string;
  proxy_mode: 'direct' | 'http' | 'https' | 'socks5';
}> {
  const cookieHeader = String(item.cookie_header || '').trim();
  if (!cookieHeader) {
    return {
      valid: false,
      message: 'Cookie 为空，无法校验',
      egress_ip: '',
      proxy: formatProxyDisplay(item.proxy),
      proxy_mode: getProxyMode(item.proxy),
    };
  }

  const ipProbe = await probeProxyEgressIp(item.proxy);
  if (!ipProbe.ok) {
    return {
      valid: false,
      message: ipProbe.message,
      egress_ip: '',
      proxy: ipProbe.proxy,
      proxy_mode: ipProbe.proxy_mode,
    };
  }

  const yt = await validateYtdlpCookieOnline('youtube', cookieHeader, item.proxy);
  return {
    valid: yt.valid,
    message: yt.message,
    egress_ip: ipProbe.egress_ip,
    proxy: ipProbe.proxy,
    proxy_mode: ipProbe.proxy_mode,
  };
}

async function validateAndPersistYoutubeCookiePoolItem(
  item: YoutubeCookiePoolItem,
  checkedAt: string,
): Promise<{
  id: string;
  valid: boolean;
  message: string;
  checked_at: string;
  egress_ip: string;
  proxy: string;
  proxy_mode: 'direct' | 'http' | 'https' | 'socks5';
  item: YoutubeCookiePoolItem;
}> {
  const result = await validateYoutubeCookiePoolItem(item);
  const updated = updateYoutubeCookiePoolItem(item.id, (row) => ({
    ...row,
    last_checked_at: checkedAt,
    last_check_ok: !!result.valid,
    last_check_message: String(result.message || '').trim(),
    last_egress_ip: String(result.egress_ip || '').trim(),
  }));
  const runtimeItem = getYoutubeCookiePoolState().items.find((row) => row.id === item.id);
  return {
    id: item.id,
    valid: !!result.valid,
    message: result.message,
    checked_at: checkedAt,
    egress_ip: result.egress_ip,
    proxy: result.proxy,
    proxy_mode: result.proxy_mode,
    item: runtimeItem || updated || item,
  };
}

function clearFilesKeepDirs(
  rootDir: string,
  counters: { deleted_files: number; preserved_dirs: number; delete_errors: number },
): void {
  if (!fs.existsSync(rootDir)) return;
  let entries: fs.Dirent[] = [];
  try {
    entries = fs.readdirSync(rootDir, { withFileTypes: true });
  } catch {
    counters.delete_errors += 1;
    return;
  }

  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      counters.preserved_dirs += 1;
      clearFilesKeepDirs(fullPath, counters);
      continue;
    }
    try {
      fs.unlinkSync(fullPath);
      counters.deleted_files += 1;
    } catch {
      counters.delete_errors += 1;
    }
  }
}

// GET /api/settings
router.get('/', (_req: Request, res: Response) => {
  const settings = sanitizeSettingsForClient(getAllSettings());
  res.json(settings);
});

// GET /api/settings/youtube-api-usage
router.get('/youtube-api-usage', (_req: Request, res: Response) => {
  res.json(getYoutubeApiUsageStatus());
});

// GET /api/settings/ytdlp-exec-audit
router.get('/ytdlp-exec-audit', (req: Request, res: Response) => {
  const rawLimit = Number(req.query.limit || 100);
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(500, Math.trunc(rawLimit))) : 100;
  const platform = String(req.query.platform || '').trim().toLowerCase();
  const db = getDb();
  let rows: any[] = [];
  if (platform) {
    rows = db.prepare(`
      SELECT *
      FROM ytdlp_exec_audit
      WHERE lower(COALESCE(cookie_platform, '')) = ?
      ORDER BY id DESC
      LIMIT ?
    `).all(platform, limit) as any[];
  } else {
    rows = db.prepare(`
      SELECT *
      FROM ytdlp_exec_audit
      ORDER BY id DESC
      LIMIT ?
    `).all(limit) as any[];
  }
  res.json({ ok: true, data: rows });
});

// POST /api/settings/youtube-api-binding/test
router.post('/youtube-api-binding/test', async (req: Request, res: Response) => {
  const currentRows = getCurrentYoutubeApiBindings(getAllSettings());
  const apiKeyInput = String(req.body?.api_key || '').trim();
  const masked = parseMaskedSettingPlaceholder(apiKeyInput);
  const apiKey = masked && masked.key === YOUTUBE_API_BINDING_MASK_KEY
    ? String(currentRows[masked.index]?.apiKey || '').trim()
    : apiKeyInput;
  const proxy = String(req.body?.proxy || '').trim();
  const result = await testYoutubeApiBindingConnectivity(apiKey, proxy);
  res.json(result);
});

// GET /api/settings/youtube-cookie-pool
router.get('/youtube-cookie-pool', (_req: Request, res: Response) => {
  const state = sanitizeYoutubeCookiePoolState(getYoutubeCookiePoolState());
  res.json({ ok: true, ...state });
});

// POST /api/settings/youtube-cookie-pool
router.post('/youtube-cookie-pool', (req: Request, res: Response) => {
  const state = saveYoutubeCookiePoolState({
    enabled: req.body?.enabled,
    items: Array.isArray(req.body?.items)
      ? resolveYoutubeCookiePoolItemsForSave(req.body.items as unknown[])
      : req.body?.items,
  });
  res.json({ ok: true, ...sanitizeYoutubeCookiePoolState(state) });
});

// POST /api/settings/youtube-cookie-pool/validate
router.post('/youtube-cookie-pool/validate', async (req: Request, res: Response) => {
  const item = findYoutubeCookiePoolItem(req.body?.id);
  if (!item) {
    res.status(404).json({ error: 'Cookie 项不存在' });
    return;
  }

  const checkedAt = new Date().toISOString();
  const result = await validateAndPersistYoutubeCookiePoolItem(item, checkedAt);

  res.json({
    ok: true,
    valid: result.valid,
    message: result.message,
    checked_at: result.checked_at,
    egress_ip: result.egress_ip,
    proxy: result.proxy,
    proxy_mode: result.proxy_mode,
    item: sanitizeYoutubeCookiePoolItem(result.item),
  });
});

// POST /api/settings/youtube-cookie-pool/validate-all
router.post('/youtube-cookie-pool/validate-all', async (_req: Request, res: Response) => {
  const rows = getYoutubeCookiePoolItems();
  const results: Array<{
    id: string;
    valid: boolean;
    message: string;
    checked_at: string;
    egress_ip: string;
    proxy: string;
    proxy_mode: 'direct' | 'http' | 'https' | 'socks5';
    item: YoutubeCookiePoolItem;
  }> = [];

  for (const row of rows) {
    const checkedAt = new Date().toISOString();
    const result = await validateAndPersistYoutubeCookiePoolItem(row, checkedAt);
    results.push(result);
  }

  const success = results.filter((row) => row.valid).length;
  const failed = results.length - success;
  const latestState = getYoutubeCookiePoolState();
  res.json({
    ok: true,
    total: results.length,
    success,
    failed,
    checked_at: new Date().toISOString(),
    message: results.length > 0
      ? `批量校验完成：成功 ${success}，失败 ${failed}`
      : 'Cookie 池为空，无可校验项',
    results: results.map((row) => ({
      ...row,
      item: sanitizeYoutubeCookiePoolItem(row.item),
    })),
    enabled: latestState.enabled,
    items: sanitizeYoutubeCookiePoolState(latestState).items,
  });
});

// POST /api/settings/youtube-cookie-pool/bitbrowser/open
router.post('/youtube-cookie-pool/bitbrowser/open', async (req: Request, res: Response) => {
  const item = findYoutubeCookiePoolItem(req.body?.id);
  if (!item) {
    res.status(404).json({ error: 'Cookie 项不存在' });
    return;
  }
  const apiUrl = normalizeBitBrowserApiUrl(item.browser_api_url);
  const windowId = normalizeBitBrowserWindowId(item.window_id);
  if (!windowId) {
    res.status(400).json({ error: '请先为该 Cookie 配置窗口ID' });
    return;
  }

  const opened = await openBitBrowserWindow(apiUrl, windowId);
  if (!opened.ok) {
    res.status(500).json({ error: opened.error });
    return;
  }

  let browser: any = null;
  try {
    const playwrightMod = await import('playwright');
    browser = await playwrightMod.chromium.connectOverCDP(opened.ws);
    const context = await resolveBitBrowserContext(browser);
    if (!context) {
      res.status(500).json({ error: 'BitBrowser context not ready after CDP connection; please retry in 2-3 seconds' });
      return;
    }
    const firstPage = context.pages()[0] || await context.newPage();
    const targetUrl = String(req.body?.url || getDefaultLoginUrlByPlatform('youtube')).trim();
    await firstPage.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45_000 }).catch(() => null);
    res.json({
      ok: true,
      id: item.id,
      message: '已打开 BitBrowser YouTube 登录页，请在窗口内完成登录',
      checked_at: new Date().toISOString(),
    });
  } catch (err: any) {
    res.status(500).json({ error: String(err?.message || err || 'bitbrowser_open_failed') });
  } finally {
    if (browser) {
      await browser.close().catch(() => null);
    }
  }
});

// POST /api/settings/youtube-cookie-pool/bitbrowser/close
router.post('/youtube-cookie-pool/bitbrowser/close', async (req: Request, res: Response) => {
  const item = findYoutubeCookiePoolItem(req.body?.id);
  if (!item) {
    res.status(404).json({ error: 'Cookie 项不存在' });
    return;
  }
  const apiUrl = normalizeBitBrowserApiUrl(item.browser_api_url);
  const windowId = normalizeBitBrowserWindowId(item.window_id);
  if (!windowId) {
    res.status(400).json({ error: '请先为该 Cookie 配置窗口ID' });
    return;
  }
  const closed = await closeBitBrowserWindow(apiUrl, windowId);
  res.json({
    ok: closed,
    id: item.id,
    message: closed ? '已关闭 BitBrowser 窗口' : '关闭窗口失败，请检查窗口状态',
    checked_at: new Date().toISOString(),
  });
});

// POST /api/settings/youtube-cookie-pool/bitbrowser/refill
router.post('/youtube-cookie-pool/bitbrowser/refill', async (req: Request, res: Response) => {
  const item = findYoutubeCookiePoolItem(req.body?.id);
  if (!item) {
    res.status(404).json({ error: 'Cookie 项不存在' });
    return;
  }
  const apiUrl = normalizeBitBrowserApiUrl(item.browser_api_url);
  const windowId = normalizeBitBrowserWindowId(item.window_id);
  if (!windowId) {
    res.status(400).json({ error: '请先为该 Cookie 配置窗口ID' });
    return;
  }

  const closeWindow = String(req.body?.close_window ?? 'false').trim().toLowerCase() === 'true';
  const extracted = await extractCookieFromBitBrowserSession(apiUrl, windowId, COOKIE_HOST_HINT_BY_PLATFORM.youtube);
  if (!extracted.ok) {
    let closedOnFail = false;
    if (closeWindow) {
      closedOnFail = await closeBitBrowserWindow(apiUrl, windowId);
    }
    res.json({
      ok: true,
      id: item.id,
      valid: false,
      message: extracted.message,
      checked_at: new Date().toISOString(),
      cookie_length: 0,
      cookie_count: 0,
      closed: closedOnFail,
    });
    return;
  }

  const now = new Date().toISOString();
  const updated = updateYoutubeCookiePoolItem(item.id, (row) => ({
    ...row,
    cookie_header: extracted.cookieHeader,
    updated_at: now,
  }));
  const runtimeItem = getYoutubeCookiePoolState().items.find((row) => row.id === item.id);

  let closed = false;
  if (closeWindow) {
    closed = await closeBitBrowserWindow(apiUrl, windowId);
  }

  res.json({
    ok: true,
    id: item.id,
    valid: true,
    message: closeWindow && !closed ? `${extracted.message}，但关闭窗口失败` : extracted.message,
    checked_at: now,
    cookie_header: extracted.cookieHeader,
    cookie_length: extracted.cookieHeader.length,
    cookie_count: extracted.cookieCount,
    closed,
    item: sanitizeYoutubeCookiePoolItem(runtimeItem || updated || item),
  });
});

// POST /api/settings/bitbrowser-session/open
router.post('/bitbrowser-session/open', async (req: Request, res: Response) => {
  const platform = parseBitBrowserPlatform(req.body?.platform || 'youtube');
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  const apiUrl = normalizeBitBrowserApiUrl(req.body?.browser_api_url);
  const windowId = normalizeBitBrowserWindowId(req.body?.window_id);
  if (!windowId) {
    res.status(400).json({ error: 'window_id is required' });
    return;
  }

  const opened = await openBitBrowserWindow(apiUrl, windowId);
  if (!opened.ok) {
    res.status(500).json({ error: opened.error });
    return;
  }

  let browser: any = null;
  try {
    const playwrightMod = await import('playwright');
    browser = await playwrightMod.chromium.connectOverCDP(opened.ws);
    const context = await resolveBitBrowserContext(browser);
    if (!context) {
      res.status(500).json({ error: 'BitBrowser context not ready after CDP connection; please retry in 2-3 seconds' });
      return;
    }
    const firstPage = context.pages()[0] || await context.newPage();
    const targetUrl = String(req.body?.url || getDefaultLoginUrlByPlatform('youtube')).trim();
    await firstPage.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45_000 }).catch(() => null);
    res.json({
      ok: true,
      platform,
      browser_api_url: apiUrl,
      window_id: windowId,
      message: '已打开 BitBrowser YouTube 登录页，请在窗口内完成登录',
      checked_at: new Date().toISOString(),
    });
  } catch (err: any) {
    res.status(500).json({ error: String(err?.message || err || 'bitbrowser_open_failed') });
  } finally {
    if (browser) {
      await browser.close().catch(() => null);
    }
  }
});

// POST /api/settings/bitbrowser-session/close
router.post('/bitbrowser-session/close', async (req: Request, res: Response) => {
  const platform = parseBitBrowserPlatform(req.body?.platform || 'youtube');
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  const apiUrl = normalizeBitBrowserApiUrl(req.body?.browser_api_url);
  const windowId = normalizeBitBrowserWindowId(req.body?.window_id);
  if (!windowId) {
    res.status(400).json({ error: 'window_id is required' });
    return;
  }
  const closed = await closeBitBrowserWindow(apiUrl, windowId);
  res.json({
    ok: closed,
    platform,
    browser_api_url: apiUrl,
    window_id: windowId,
    message: closed ? '已关闭 BitBrowser 窗口' : '关闭窗口失败，请检查窗口状态',
    checked_at: new Date().toISOString(),
  });
});

// POST /api/settings/bitbrowser-session/export-cookie
router.post('/bitbrowser-session/export-cookie', async (req: Request, res: Response) => {
  const platform = parseBitBrowserPlatform(req.body?.platform || 'youtube');
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  const apiUrl = normalizeBitBrowserApiUrl(req.body?.browser_api_url);
  const windowId = normalizeBitBrowserWindowId(req.body?.window_id);
  if (!windowId) {
    res.status(400).json({ error: 'window_id is required' });
    return;
  }
  const writeSetting = String(req.body?.write_setting ?? 'true').trim().toLowerCase() === 'true';
  const closeWindow = String(req.body?.close_window ?? 'false').trim().toLowerCase() === 'true';
  const hostHint = COOKIE_HOST_HINT_BY_PLATFORM.youtube;
  const settingKey = COOKIE_SETTING_KEY_BY_PLATFORM.youtube;

  const extracted = await extractCookieFromBitBrowserSession(apiUrl, windowId, hostHint);
  if (!extracted.ok) {
    res.json({
      ok: true,
      platform,
      valid: false,
      message: extracted.message,
      checked_at: new Date().toISOString(),
      setting_key: settingKey,
      cookie_length: 0,
      cookie_count: 0,
      written: false,
      closed: false,
    });
    return;
  }

  if (writeSetting) {
    setSetting(settingKey, extracted.cookieHeader);
  }

  let closed = false;
  if (closeWindow) {
    closed = await closeBitBrowserWindow(apiUrl, windowId);
  }

  res.json({
    ok: true,
    platform,
    valid: true,
    message: closeWindow && !closed ? `${extracted.message}，但关闭窗口失败` : extracted.message,
    checked_at: new Date().toISOString(),
    setting_key: settingKey,
    cookie_header: extracted.cookieHeader,
    cookie_length: extracted.cookieHeader.length,
    cookie_count: extracted.cookieCount,
    written: writeSetting,
    closed,
  });
});

// GET /api/settings/playwright-session/status
router.get('/playwright-session/status', (req: Request, res: Response) => {
  const platform = parsePlatform(req.query.platform);
  if (platform) {
    res.json(serializeSessionStatus(platform));
    return;
  }
  const data = PLAYWRIGHT_SESSION_PLATFORMS.map((item) => serializeSessionStatus(item));
  res.json({ data });
});

// POST /api/settings/playwright-session/open
router.post('/playwright-session/open', async (req: Request, res: Response) => {
  const platform = parsePlatform(req.body?.platform);
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  if (platform === 'youtube') {
    rejectYoutubePlaywrightCookieFlow(res);
    return;
  }

  const existing = sessionRuns.get(platform);
  if (existing?.status === 'running') {
    res.json({ ok: true, running: true, status: serializeSessionStatus(platform) });
    return;
  }

  const targetUrl = String(req.body?.url || getDefaultLoginUrlByPlatform(platform)).trim();
  const timeoutSec = Number.isFinite(Number(req.body?.timeout_sec))
    ? Math.max(30, Math.min(Number(req.body?.timeout_sec), 3600))
    : readSessionTimeoutSec();

  const run: SessionRunRecord = {
    platform,
    status: 'running',
    started_at: new Date().toISOString(),
    finished_at: null,
    target_url: targetUrl,
    timeout_sec: timeoutSec,
    error: null,
    context: null,
    timer: null,
  };
  sessionRuns.set(platform, run);

  try {
    const playwrightMod = await import('playwright');
    const userDataDir = getPlaywrightSessionUserDataDir(platform);
    const context = await playwrightMod.chromium.launchPersistentContext(userDataDir, {
      headless: false,
      args: ['--disable-blink-features=AutomationControlled'],
      viewport: { width: 1360, height: 900 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
      extraHTTPHeaders: {
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
      },
      locale: 'zh-CN',
    });
    run.context = context;

    const finalize = (status: SessionRunStatus, error: string | null = null) => {
      if (run.status !== 'running') return;
      run.status = status;
      run.error = error;
      run.finished_at = new Date().toISOString();
      if (run.timer) {
        clearTimeout(run.timer);
        run.timer = null;
      }
      if (status === 'done') {
        writePlaywrightSessionMeta(platform, {
          target_url: targetUrl,
          saved_by: 'manual_login',
        });
      }
      run.context = null;
    };

    context.on('close', () => finalize('done', null));

    const firstPage = context.pages()[0] || await context.newPage();
    await firstPage.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45_000 }).catch(() => null);

    run.timer = setTimeout(() => {
      if (run.context) {
        run.context.close().catch(() => null);
      }
    }, timeoutSec * 1000);

    res.json({
      ok: true,
      running: true,
      message: `Browser opened for ${platform}. Complete login and close the browser window to save session.`,
      status: serializeSessionStatus(platform),
    });
  } catch (err: any) {
    run.status = 'failed';
    run.finished_at = new Date().toISOString();
    run.error = String(err?.message || err || 'playwright_session_open_failed');
    if (run.timer) {
      clearTimeout(run.timer);
      run.timer = null;
    }
    run.context = null;
    res.status(500).json({ error: run.error });
  }
});

// POST /api/settings/playwright-session/close
router.post('/playwright-session/close', async (req: Request, res: Response) => {
  const platform = parsePlatform(req.body?.platform);
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  if (platform === 'youtube') {
    rejectYoutubePlaywrightCookieFlow(res);
    return;
  }
  const run = sessionRuns.get(platform);
  if (!run || run.status !== 'running' || !run.context) {
    res.json({ ok: true, running: false, status: serializeSessionStatus(platform) });
    return;
  }
  await run.context.close().catch(() => null);
  res.json({ ok: true, running: false, status: serializeSessionStatus(platform) });
});

// POST /api/settings/playwright-session/clear
router.post('/playwright-session/clear', (req: Request, res: Response) => {
  const platform = parsePlatform(req.body?.platform);
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  if (platform === 'youtube') {
    rejectYoutubePlaywrightCookieFlow(res);
    return;
  }

  const run = sessionRuns.get(platform);
  if (run?.status === 'running') {
    res.status(409).json({ error: 'Session browser is still running. Close it first.' });
    return;
  }

  clearPlaywrightSession(platform);
  writePlaywrightSessionMeta(platform, { cleared_at: new Date().toISOString(), cleared: true });
  res.json({ ok: true, status: serializeSessionStatus(platform) });
});

// PATCH /api/settings
router.patch('/', (req: Request, res: Response) => {
  const updates = req.body;
  if (!updates || typeof updates !== 'object') {
    res.status(400).json({ error: 'Request body must be an object of key-value pairs' });
    return;
  }

  // Cookie pool state is managed by /api/settings/youtube-cookie-pool only.
  // Avoid accidental override from broad settings PATCH payloads.
  const blockedKeys = new Set<string>([
    'youtube_cookie_pool_enabled',
    'youtube_cookie_pool_json',
  ]);

  const currentSettings = getAllSettings();
  const hasYoutubeApiBindingUpdates = (
    Object.prototype.hasOwnProperty.call(updates, 'youtube_api_key')
    || Object.prototype.hasOwnProperty.call(updates, 'youtube_api_keys')
    || Object.prototype.hasOwnProperty.call(updates, 'youtube_api_key_proxies')
  );

  if (hasYoutubeApiBindingUpdates) {
    const resolvedRows = resolveYoutubeApiBindingRows(updates as Record<string, unknown>, currentSettings);
    setSetting('youtube_api_key', resolvedRows[0]?.apiKey || '');
    setSetting('youtube_api_keys', resolvedRows.slice(1).map((row) => row.apiKey).join('\n'));
    setSetting('youtube_api_key_proxies', resolvedRows.map((row) => row.proxy).join('\n'));
    blockedKeys.add('youtube_api_key');
    blockedKeys.add('youtube_api_keys');
    blockedKeys.add('youtube_api_key_proxies');
  }

  for (const [key, value] of Object.entries(updates)) {
    if (blockedKeys.has(String(key || '').trim())) continue;
    if (SENSITIVE_SINGLE_SETTING_KEYS.has(String(key || '').trim())) {
      setSetting(key, resolveSingleSensitiveSettingValue(key, value, currentSettings));
      continue;
    }
    setSetting(key, String(value));
  }

  if (Object.prototype.hasOwnProperty.call(updates, 'daily_sync_time')) {
    scheduleDailySyncFromSettings();
  }

  const settings = sanitizeSettingsForClient(getAllSettings());
  res.json(settings);
});

// POST /api/settings/validate-cookie
router.post('/validate-cookie', async (req: Request, res: Response) => {
  const platform = parseCookieValidatePlatform(req.body?.platform);
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }

  const settingKey = COOKIE_SETTING_KEY_BY_PLATFORM[platform];
  const hostHint = COOKIE_HOST_HINT_BY_PLATFORM[platform];
  const cookieInputRaw = String(
    req.body?.cookie_input
    ?? req.body?.cookie
    ?? getSetting(settingKey)
    ?? '',
  ).trim();
  const resolvedCookieInput = resolveSingleSensitiveSettingValue(settingKey, cookieInputRaw, getAllSettings());

  if (!resolvedCookieInput) {
    res.json({
      ok: true,
      platform,
      valid: false,
      message: 'Cookie 为空',
      checked_at: new Date().toISOString(),
    });
    return;
  }

  const cookieHeader = resolveCookieHeaderFromInput(resolvedCookieInput, hostHint);
  if (!cookieHeader) {
    res.json({
      ok: true,
      platform,
      valid: false,
      message: 'Cookie 格式无法解析（请使用 Header/Netscape/JSON/文件路径）',
      checked_at: new Date().toISOString(),
    });
    return;
  }

  const proxyUrl = String(req.body?.proxy || '').trim();
  const result = await validatePlatformCookie(platform, cookieHeader, proxyUrl);
  res.json({
    ok: true,
    platform,
    valid: result.valid,
    message: result.message,
    checked_at: new Date().toISOString(),
    cookie_length: cookieHeader.length,
    setting_key: settingKey,
  });
});

// POST /api/settings/playwright-session/export-cookie
router.post('/playwright-session/export-cookie', async (req: Request, res: Response) => {
  const platform = parseCookieValidatePlatform(req.body?.platform);
  if (!platform) {
    res.status(400).json({ error: 'Invalid platform' });
    return;
  }
  if (platform === 'youtube') {
    rejectYoutubePlaywrightCookieFlow(res);
    return;
  }

  const sessionPlatform = normalizePlaywrightSessionPlatform(platform);
  if (!sessionPlatform) {
    res.status(400).json({ error: 'Invalid playwright session platform' });
    return;
  }

  const hostHint = COOKIE_HOST_HINT_BY_PLATFORM[platform];
  const settingKey = COOKIE_SETTING_KEY_BY_PLATFORM[platform];
  const writeSetting = String(req.body?.write_setting ?? 'false').trim().toLowerCase() === 'true';
  const result = await extractCookieFromPlaywrightSession(sessionPlatform, hostHint);

  if (!result.ok) {
    res.json({
      ok: true,
      platform,
      valid: false,
      message: result.message,
      checked_at: new Date().toISOString(),
      setting_key: settingKey,
      cookie_length: 0,
      cookie_count: 0,
    });
    return;
  }

  if (writeSetting) {
    setSetting(settingKey, result.cookieHeader);
  }

  res.json({
    ok: true,
    platform,
    valid: true,
    message: result.message,
    checked_at: new Date().toISOString(),
    setting_key: settingKey,
    cookie_header: result.cookieHeader,
    cookie_length: result.cookieHeader.length,
    cookie_count: result.cookieCount,
    written: writeSetting,
  });
});

// POST /api/settings/clear-local-data
router.post('/clear-local-data', (_req: Request, res: Response) => {
  const db = getDb();
  const activeJobs = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM jobs
    WHERE status IN ('queued', 'running', 'canceling')
  `).get() as any)?.c || 0);
  if (activeJobs > 0) {
    res.status(409).json({ error: 'Active jobs exist. Cancel or wait for completion before cleanup.' });
    return;
  }

  const downloadRoot = path.resolve(getSetting('download_root') || path.join(process.cwd(), 'downloads'));
  const assetsDir = path.join(downloadRoot, 'assets');
  const exportsDir = path.join(downloadRoot, 'exports');

  const ensureDirs = [
    assetsDir,
    path.join(assetsDir, 'meta'),
    path.join(assetsDir, 'thumbs'),
    path.join(assetsDir, 'subs'),
    path.join(assetsDir, 'videos'),
    exportsDir,
  ];

  let createdDirs = 0;
  for (const dirPath of ensureDirs) {
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
      createdDirs += 1;
    }
  }

  const counters = { deleted_files: 0, preserved_dirs: 0, delete_errors: 0 };
  clearFilesKeepDirs(assetsDir, counters);
  clearFilesKeepDirs(exportsDir, counters);

  const resetResult = db.prepare(`
    UPDATE videos
    SET local_meta_path = NULL,
        local_thumb_path = NULL,
        local_subtitle_paths = '[]',
        local_video_path = NULL,
        download_status = 'none'
  `).run();

  // Tool cache payloads are local persisted cache records as well.
  const deletedToolResults = db.prepare(`DELETE FROM tool_job_results`).run().changes;

  res.json({
    success: true,
    download_root: downloadRoot,
    deleted_files: counters.deleted_files,
    preserved_dirs: counters.preserved_dirs,
    delete_errors: counters.delete_errors,
    created_dirs: createdDirs,
    reset_videos: Number(resetResult.changes || 0),
    deleted_tool_results: Number(deletedToolResults || 0),
  });
});

export default router;




