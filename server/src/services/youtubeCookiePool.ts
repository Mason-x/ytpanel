import { randomUUID } from 'crypto';
import { getSetting, setSetting } from '../db.js';

export type YoutubeCookiePoolItem = {
  id: string;
  name: string;
  enabled: boolean;
  cookie_header: string;
  proxy: string;
  browser_api_url: string;
  window_id: string;
  updated_at: string;
  last_checked_at: string;
  last_check_ok: boolean;
  last_check_message: string;
  last_egress_ip: string;
  runtime_samples?: number;
  runtime_failures?: number;
  runtime_failure_rate?: number;
  runtime_fused?: boolean;
  runtime_fused_until?: string;
  runtime_proxy_eof_streak?: number;
  runtime_proxy_fused?: boolean;
  runtime_proxy_fused_until?: string;
};

export type YoutubeCookiePoolBinding = {
  id: string;
  name: string;
  cookie_header: string;
  proxy: string;
};

const COOKIE_POOL_JSON_KEY = 'youtube_cookie_pool_json';
const COOKIE_POOL_ENABLED_KEY = 'youtube_cookie_pool_enabled';
const DEFAULT_BROWSER_API_URL = 'http://127.0.0.1:54345';

let roundRobinCursor = 0;
let roundRobinSignature = '';

const FUSE_WINDOW_SIZE = 20;
const FUSE_MIN_SAMPLES = 8;
const FUSE_FAILURE_RATE_THRESHOLD = 0.6;
const FUSE_COOLDOWN_MS = 10 * 60 * 1000;
const PROXY_EOF_STREAK_THRESHOLD = 3;
const PROXY_EOF_FUSE_COOLDOWN_MS = 15 * 60 * 1000;

type RuntimeState = {
  outcomes: number[]; // 1 fail, 0 success
  fused_until_ms: number;
};

type RuntimeProxyState = {
  eof_streak: number;
  fused_until_ms: number;
};

const runtimeStateById = new Map<string, RuntimeState>();
const runtimeStateByProxy = new Map<string, RuntimeProxyState>();

function normalizeBoolean(value: unknown, fallback = false): boolean {
  const lower = String(value ?? '').trim().toLowerCase();
  if (!lower) return fallback;
  return lower === 'true' || lower === '1' || lower === 'yes' || lower === 'on';
}

function nowMs(): number {
  return Date.now();
}

function ensureRuntimeState(id: string): RuntimeState {
  const key = String(id || '').trim();
  const existing = runtimeStateById.get(key);
  if (existing) return existing;
  const created: RuntimeState = { outcomes: [], fused_until_ms: 0 };
  runtimeStateById.set(key, created);
  return created;
}

function normalizeProxyKey(proxy: unknown): string {
  const normalized = normalizeYoutubeCookieProxy(proxy);
  if (!normalized) return '';
  return normalized.toLowerCase();
}

function ensureRuntimeProxyState(proxy: string): RuntimeProxyState {
  const key = normalizeProxyKey(proxy);
  if (!key) return { eof_streak: 0, fused_until_ms: 0 };
  const existing = runtimeStateByProxy.get(key);
  if (existing) return existing;
  const created: RuntimeProxyState = { eof_streak: 0, fused_until_ms: 0 };
  runtimeStateByProxy.set(key, created);
  return created;
}

function computeRuntimeProxyStats(proxy: string): {
  eofStreak: number;
  fused: boolean;
  fusedUntilMs: number;
} {
  const key = normalizeProxyKey(proxy);
  if (!key) return { eofStreak: 0, fused: false, fusedUntilMs: 0 };
  const state = ensureRuntimeProxyState(key);
  const now = nowMs();
  if (state.fused_until_ms > 0 && state.fused_until_ms <= now) {
    state.fused_until_ms = 0;
  }
  return {
    eofStreak: state.eof_streak,
    fused: state.fused_until_ms > now,
    fusedUntilMs: state.fused_until_ms,
  };
}

function isProxyTemporarilyFused(proxy: string): boolean {
  return computeRuntimeProxyStats(proxy).fused;
}

function isEofOrConnectionResetError(value: unknown): boolean {
  const lower = String(value || '').toLowerCase();
  if (!lower) return false;
  return (
    lower.includes('unexpected_eof_while_reading')
    || lower.includes('eof occurred in violation of protocol')
    || lower.includes('winerror 10054')
    || lower.includes('connection reset by peer')
    || lower.includes('远程主机强迫关闭了一个现有的连接')
  );
}

function normalizeOutcomes(outcomes: number[]): number[] {
  const cleaned = outcomes.filter((item) => item === 0 || item === 1);
  if (cleaned.length <= FUSE_WINDOW_SIZE) return cleaned;
  return cleaned.slice(cleaned.length - FUSE_WINDOW_SIZE);
}

function computeRuntimeStats(id: string): {
  samples: number;
  failures: number;
  failureRate: number;
  fused: boolean;
  fusedUntilMs: number;
} {
  const state = ensureRuntimeState(id);
  state.outcomes = normalizeOutcomes(state.outcomes);
  const samples = state.outcomes.length;
  const failures = state.outcomes.reduce((acc, cur) => acc + (cur === 1 ? 1 : 0), 0);
  const failureRate = samples > 0 ? failures / samples : 0;
  const now = nowMs();
  if (state.fused_until_ms > 0 && state.fused_until_ms <= now) {
    state.fused_until_ms = 0;
  }
  return {
    samples,
    failures,
    failureRate,
    fused: state.fused_until_ms > now,
    fusedUntilMs: state.fused_until_ms,
  };
}

function isTemporarilyFused(id: string): boolean {
  return computeRuntimeStats(id).fused;
}

export function normalizeYoutubeCookieProxy(value: unknown): string {
  const text = String(value || '').trim();
  if (!text) return '';
  const lower = text.toLowerCase();
  if (lower.startsWith('http://') || lower.startsWith('https://') || lower.startsWith('socks5://')) {
    return text;
  }
  return '';
}

function createItemId(): string {
  try {
    return randomUUID().replace(/-/g, '');
  } catch {
    return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;
  }
}

function normalizeCookieText(value: unknown): string {
  return String(value || '').trim();
}

function toIso(value: unknown): string {
  const text = String(value || '').trim();
  if (!text) return '';
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return '';
  return date.toISOString();
}

function normalizeItem(input: unknown, index = 0): YoutubeCookiePoolItem | null {
  if (!input || typeof input !== 'object') return null;
  const row = input as Record<string, unknown>;
  const now = new Date().toISOString();
  const id = String(row.id || '').trim() || createItemId();
  const name = String(row.name || '').trim() || `Cookie ${index + 1}`;
  const enabled = normalizeBoolean(row.enabled, true);
  const cookieHeader = normalizeCookieText(row.cookie_header);
  const proxy = normalizeYoutubeCookieProxy(row.proxy);
  const browserApiUrl = String(row.browser_api_url || '').trim() || DEFAULT_BROWSER_API_URL;
  const windowId = String(row.window_id || '').trim().slice(0, 128);
  const updatedAt = toIso(row.updated_at) || now;
  const lastCheckedAt = toIso(row.last_checked_at);
  const lastCheckOk = normalizeBoolean(row.last_check_ok, false);
  const lastCheckMessage = String(row.last_check_message || '').trim();
  const lastEgressIp = String(row.last_egress_ip || '').trim();
  return {
    id,
    name,
    enabled,
    cookie_header: cookieHeader,
    proxy,
    browser_api_url: browserApiUrl,
    window_id: windowId,
    updated_at: updatedAt,
    last_checked_at: lastCheckedAt,
    last_check_ok: lastCheckOk,
    last_check_message: lastCheckMessage,
    last_egress_ip: lastEgressIp,
  };
}

function parseItems(raw: string): YoutubeCookiePoolItem[] {
  const text = String(raw || '').trim();
  if (!text) return [];
  try {
    const parsed = JSON.parse(text);
    if (!Array.isArray(parsed)) return [];
    const out: YoutubeCookiePoolItem[] = [];
    const seen = new Set<string>();
    for (let i = 0; i < parsed.length; i += 1) {
      const normalized = normalizeItem(parsed[i], i);
      if (!normalized) continue;
      if (seen.has(normalized.id)) continue;
      seen.add(normalized.id);
      out.push(normalized);
    }
    return out;
  } catch {
    return [];
  }
}

function saveItems(items: YoutubeCookiePoolItem[]): YoutubeCookiePoolItem[] {
  setSetting(COOKIE_POOL_JSON_KEY, JSON.stringify(items));
  return items;
}

export function isYoutubeCookiePoolEnabled(): boolean {
  return normalizeBoolean(getSetting(COOKIE_POOL_ENABLED_KEY), false);
}

export function setYoutubeCookiePoolEnabled(enabled: boolean): void {
  setSetting(COOKIE_POOL_ENABLED_KEY, enabled ? 'true' : 'false');
}

export function getYoutubeCookiePoolItems(): YoutubeCookiePoolItem[] {
  return parseItems(String(getSetting(COOKIE_POOL_JSON_KEY) || '[]'));
}

export function hasUsableYoutubeCookiePoolItems(): boolean {
  const items = getYoutubeCookiePoolItems();
  return items.some((item) => item.enabled && String(item.cookie_header || '').trim());
}

export function saveYoutubeCookiePoolItems(items: unknown[]): YoutubeCookiePoolItem[] {
  const next: YoutubeCookiePoolItem[] = [];
  const seen = new Set<string>();
  const proxyKeys = new Set<string>();
  for (let i = 0; i < items.length; i += 1) {
    const normalized = normalizeItem(items[i], i);
    if (!normalized) continue;
    if (seen.has(normalized.id)) continue;
    seen.add(normalized.id);
    const proxyKey = normalizeProxyKey(normalized.proxy);
    if (proxyKey) proxyKeys.add(proxyKey);
    next.push(normalized);
  }
  for (const key of Array.from(runtimeStateById.keys())) {
    if (!seen.has(key)) runtimeStateById.delete(key);
  }
  for (const key of Array.from(runtimeStateByProxy.keys())) {
    if (!proxyKeys.has(key)) runtimeStateByProxy.delete(key);
  }
  return saveItems(next);
}

export function getYoutubeCookiePoolState(): { enabled: boolean; items: YoutubeCookiePoolItem[] } {
  const items = getYoutubeCookiePoolItems().map((item) => {
    const stats = computeRuntimeStats(item.id);
    const proxyStats = computeRuntimeProxyStats(item.proxy);
    return {
      ...item,
      runtime_samples: stats.samples,
      runtime_failures: stats.failures,
      runtime_failure_rate: stats.failureRate,
      runtime_fused: stats.fused || proxyStats.fused,
      runtime_fused_until: (() => {
        const until = Math.max(stats.fusedUntilMs, proxyStats.fusedUntilMs);
        return until > 0 ? new Date(until).toISOString() : '';
      })(),
      runtime_proxy_eof_streak: proxyStats.eofStreak,
      runtime_proxy_fused: proxyStats.fused,
      runtime_proxy_fused_until: proxyStats.fusedUntilMs > 0 ? new Date(proxyStats.fusedUntilMs).toISOString() : '',
    };
  });
  return {
    enabled: isYoutubeCookiePoolEnabled(),
    items,
  };
}

export function saveYoutubeCookiePoolState(payload: {
  enabled?: unknown;
  items?: unknown;
}): { enabled: boolean; items: YoutubeCookiePoolItem[] } {
  const enabled = payload && payload.enabled != null
    ? normalizeBoolean(payload.enabled, false)
    : isYoutubeCookiePoolEnabled();
  setYoutubeCookiePoolEnabled(enabled);
  const list = Array.isArray(payload?.items) ? payload.items : getYoutubeCookiePoolItems();
  saveYoutubeCookiePoolItems(list as unknown[]);
  return getYoutubeCookiePoolState();
}

export function updateYoutubeCookiePoolItem(
  id: string,
  updater: (item: YoutubeCookiePoolItem) => YoutubeCookiePoolItem,
): YoutubeCookiePoolItem | null {
  const targetId = String(id || '').trim();
  if (!targetId) return null;
  const items = getYoutubeCookiePoolItems();
  let matched = false;
  const next = items.map((item, index) => {
    if (item.id !== targetId) return item;
    matched = true;
    const updated = normalizeItem(updater(item), index);
    if (!updated) return item;
    return updated;
  });
  if (!matched) return null;
  saveItems(next);
  return next.find((item) => item.id === targetId) || null;
}

export function recordYoutubeCookiePoolExecutionResult(
  id: string,
  success: boolean,
  options?: { proxy?: string; errorText?: string | null },
): void {
  const targetId = String(id || '').trim();
  if (!targetId) return;
  const state = ensureRuntimeState(targetId);
  state.outcomes.push(success ? 0 : 1);
  state.outcomes = normalizeOutcomes(state.outcomes);
  const stats = computeRuntimeStats(targetId);
  if (
    !success
    && stats.samples >= FUSE_MIN_SAMPLES
    && stats.failureRate >= FUSE_FAILURE_RATE_THRESHOLD
    && !stats.fused
  ) {
    state.fused_until_ms = nowMs() + FUSE_COOLDOWN_MS;
  }

  const proxyKey = normalizeProxyKey(options?.proxy || '');
  if (!proxyKey) return;
  const proxyState = ensureRuntimeProxyState(proxyKey);
  computeRuntimeProxyStats(proxyKey);

  if (success) {
    proxyState.eof_streak = 0;
    return;
  }

  const isEofLike = isEofOrConnectionResetError(options?.errorText || '');
  if (!isEofLike) {
    proxyState.eof_streak = 0;
    return;
  }

  proxyState.eof_streak += 1;
  if (proxyState.eof_streak >= PROXY_EOF_STREAK_THRESHOLD) {
    proxyState.fused_until_ms = nowMs() + PROXY_EOF_FUSE_COOLDOWN_MS;
    proxyState.eof_streak = 0;
  }
}

function buildRoundRobinSignature(items: YoutubeCookiePoolItem[]): string {
  return items.map((item) => item.id).join('|');
}

export function reserveYoutubeCookieBindingForYtdlp(): YoutubeCookiePoolBinding | null {
  if (!isYoutubeCookiePoolEnabled()) return null;
  const candidates = getYoutubeCookiePoolItems().filter(
    (item) => item.enabled
      && String(item.cookie_header || '').trim()
      && !isTemporarilyFused(item.id)
      && !isProxyTemporarilyFused(item.proxy),
  );
  if (candidates.length === 0) return null;

  const signature = buildRoundRobinSignature(candidates);
  if (signature !== roundRobinSignature) {
    roundRobinSignature = signature;
    roundRobinCursor = 0;
  }
  if (roundRobinCursor < 0 || roundRobinCursor >= candidates.length) {
    roundRobinCursor = 0;
  }
  const picked = candidates[roundRobinCursor];
  roundRobinCursor = (roundRobinCursor + 1) % candidates.length;

  return {
    id: picked.id,
    name: picked.name,
    cookie_header: picked.cookie_header,
    proxy: picked.proxy,
  };
}
