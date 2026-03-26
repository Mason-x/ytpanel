import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getSetting } from '../db.js';

export type PlaywrightSessionPlatform = 'douyin' | 'tiktok' | 'xiaohongshu' | 'bilibili' | 'youtube';
export type PlaywrightSessionMode = 'persistent' | 'ephemeral';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SESSION_ROOT = path.join(__dirname, '..', '..', 'data', 'playwright_sessions');
const FALSE_SET = new Set(['false', '0', 'off', 'no']);

export const PLAYWRIGHT_SESSION_PLATFORMS: PlaywrightSessionPlatform[] = [
  'douyin',
  'tiktok',
  'xiaohongshu',
  'bilibili',
  'youtube',
];

function normalizeBool(value: string | undefined, fallback: boolean): boolean {
  if (value == null) return fallback;
  const raw = String(value).trim().toLowerCase();
  if (!raw) return fallback;
  return !FALSE_SET.has(raw);
}

function ensureDir(dir: string): void {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function resolveHostForPlatform(platform: PlaywrightSessionPlatform): string {
  switch (platform) {
    case 'douyin': return '.douyin.com';
    case 'tiktok': return '.tiktok.com';
    case 'xiaohongshu': return '.xiaohongshu.com';
    case 'bilibili': return '.bilibili.com';
    case 'youtube': return '.youtube.com';
    default: return '.douyin.com';
  }
}

export function normalizePlaywrightSessionPlatform(input: unknown): PlaywrightSessionPlatform | null {
  const raw = String(input || '').trim().toLowerCase();
  return PLAYWRIGHT_SESSION_PLATFORMS.includes(raw as PlaywrightSessionPlatform)
    ? (raw as PlaywrightSessionPlatform)
    : null;
}

export function getDefaultLoginUrlByPlatform(platform: PlaywrightSessionPlatform): string {
  switch (platform) {
    case 'douyin': return 'https://www.douyin.com/';
    case 'tiktok': return 'https://www.tiktok.com/';
    case 'xiaohongshu': return 'https://www.xiaohongshu.com/';
    case 'bilibili': return 'https://www.bilibili.com/';
    case 'youtube': return 'https://www.youtube.com/';
    default: return 'https://www.douyin.com/';
  }
}

export function getPlaywrightSessionUserDataDir(platform: PlaywrightSessionPlatform): string {
  ensureDir(SESSION_ROOT);
  const dir = path.join(SESSION_ROOT, platform);
  ensureDir(dir);
  return dir;
}

export function getPlaywrightSessionMetaPath(platform: PlaywrightSessionPlatform): string {
  return path.join(getPlaywrightSessionUserDataDir(platform), 'session-meta.json');
}

export function writePlaywrightSessionMeta(
  platform: PlaywrightSessionPlatform,
  extra: Record<string, unknown> = {},
): void {
  const metaPath = getPlaywrightSessionMetaPath(platform);
  const payload = {
    platform,
    updated_at: new Date().toISOString(),
    ...extra,
  };
  try {
    fs.writeFileSync(metaPath, JSON.stringify(payload, null, 2), 'utf8');
  } catch {
    // Ignore metadata write failure.
  }
}

export function readPlaywrightSessionMeta(platform: PlaywrightSessionPlatform): Record<string, unknown> | null {
  const metaPath = getPlaywrightSessionMetaPath(platform);
  if (!fs.existsSync(metaPath)) return null;
  try {
    const raw = fs.readFileSync(metaPath, 'utf8');
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

export function clearPlaywrightSession(platform: PlaywrightSessionPlatform): void {
  const dir = getPlaywrightSessionUserDataDir(platform);
  if (!fs.existsSync(dir)) return;
  for (const entry of fs.readdirSync(dir)) {
    const fullPath = path.join(dir, entry);
    try {
      fs.rmSync(fullPath, { recursive: true, force: true });
    } catch {
      // Ignore cleanup failures.
    }
  }
}

export function getPlaywrightSessionInfo(platform: PlaywrightSessionPlatform): {
  platform: PlaywrightSessionPlatform;
  exists: boolean;
  user_data_dir: string;
  file_count: number;
  updated_at: string | null;
} {
  const dir = getPlaywrightSessionUserDataDir(platform);
  const entries = fs.existsSync(dir) ? fs.readdirSync(dir) : [];
  const filtered = entries.filter((name) => name !== 'session-meta.json');
  const meta = readPlaywrightSessionMeta(platform);
  return {
    platform,
    exists: filtered.length > 0,
    user_data_dir: dir,
    file_count: filtered.length,
    updated_at: String(meta?.updated_at || '').trim() || null,
  };
}

export function getPlaywrightHeadlessEnabled(platform?: PlaywrightSessionPlatform): boolean {
  const globalRaw = getSetting('playwright_headless');
  if (globalRaw != null && String(globalRaw).trim() !== '') {
    return normalizeBool(globalRaw, true);
  }
  if (platform === 'douyin') {
    return normalizeBool(getSetting('douyin_playwright_headless'), true);
  }
  return true;
}

export function getPlaywrightSessionEnabled(): boolean {
  return normalizeBool(getSetting('playwright_session_enabled'), true);
}

export interface LaunchContextOptions {
  platform: PlaywrightSessionPlatform;
  headless: boolean;
  viewport: { width: number; height: number };
  userAgent: string;
  extraHTTPHeaders?: Record<string, string>;
  usePersistentSession?: boolean;
}

export async function launchPlaywrightContextWithSession(
  playwrightMod: any,
  options: LaunchContextOptions,
): Promise<{ context: any; browser: any | null; sessionMode: PlaywrightSessionMode }> {
  const usePersistent = options.usePersistentSession !== false && getPlaywrightSessionEnabled();
  const args = ['--disable-blink-features=AutomationControlled'];

  if (usePersistent) {
    const userDataDir = getPlaywrightSessionUserDataDir(options.platform);
    const context = await playwrightMod.chromium.launchPersistentContext(userDataDir, {
      headless: options.headless,
      args,
      viewport: options.viewport,
      userAgent: options.userAgent,
      extraHTTPHeaders: options.extraHTTPHeaders || {},
      locale: 'zh-CN',
    });
    return { context, browser: null, sessionMode: 'persistent' };
  }

  const browser = await playwrightMod.chromium.launch({
    headless: options.headless,
    args,
  });
  const context = await browser.newContext({
    viewport: options.viewport,
    userAgent: options.userAgent,
    extraHTTPHeaders: options.extraHTTPHeaders || {},
    locale: 'zh-CN',
  });
  return { context, browser, sessionMode: 'ephemeral' };
}

export function cookieHeaderToContextCookies(
  cookieHeader: string,
  platform: PlaywrightSessionPlatform,
): Array<{ name: string; value: string; domain: string; path: string; secure: boolean; httpOnly: boolean; sameSite: 'Lax' }> {
  const raw = String(cookieHeader || '').trim();
  if (!raw) return [];
  const domain = resolveHostForPlatform(platform);
  const result: Array<{ name: string; value: string; domain: string; path: string; secure: boolean; httpOnly: boolean; sameSite: 'Lax' }> = [];
  for (const part of raw.split(';')) {
    const token = String(part || '').trim();
    if (!token) continue;
    const idx = token.indexOf('=');
    if (idx <= 0) continue;
    const name = token.slice(0, idx).trim();
    const value = token.slice(idx + 1).trim();
    if (!name || !value) continue;
    result.push({
      name,
      value,
      domain,
      path: '/',
      secure: true,
      httpOnly: false,
      sameSite: 'Lax',
    });
  }
  return result;
}
