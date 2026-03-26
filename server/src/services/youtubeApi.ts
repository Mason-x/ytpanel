import { getSetting, setSetting } from "../db.js";

import https from "https";
import { ProxyAgent } from "undici";
import { SocksProxyAgent } from "socks-proxy-agent";

interface YoutubeApiUsageState {
  date: string;
  total_units: number;
  total_calls: number;
  per_method_units: Record<string, number>;
  per_method_calls: Record<string, number>;
  per_key_units: Record<string, number>;
  per_key_calls: Record<string, number>;
  active_key_index: number;
}

interface YoutubeApiKeyConfig {
  key: string;
  proxyUrl: string;
}

const YOUTUBE_API_METHOD_UNIT_COST: Record<string, number> = {
  "channels.list": 1,
  "playlistItems.list": 1,
  "search.list": 100,
  "videos.list": 1,
};

const DEFAULT_DAILY_UNITS_LIMIT = 10000;
const DEFAULT_WARNING_THRESHOLD_PERCENT = 80;
const YOUTUBE_QUOTA_TIMEZONE = "America/Los_Angeles";
const HOT_VIDEO_CHANNEL_STATS_CACHE_TTL_MS = 30 * 60 * 1000;
const HOT_VIDEO_CHANNEL_STATS_CACHE_MAX_SIZE = 5000;
const hotVideoChannelStatsCache = new Map<
  string,
  HotVideoChannelStatsCacheEntry
>();

export interface ChannelApiSnapshot {
  channelId: string;
  title: string;
  description: string;
  customUrl: string;
  createdDate: string;
  totalViews: number | null;
  subscriberCount: number | null;
  videoCount: number | null;
  averageViews: number;
  averageSubscribersPerVideo: number;
  highThumbnailUrl: string;
  country: string;
  fetchedAt: string;
}

export interface ChannelApiResult {
  success: boolean;
  data?: ChannelApiSnapshot;
  reason?: string;
}

export interface ResearchChannelApiSnapshot {
  channel_id: string;
  title: string;
  handle: string | null;
  avatar_url: string;
  subscriber_count: number | null;
  video_count: number | null;
  view_count: number | null;
  first_video_published_at: string | null;
}

export interface ResearchChannelApiResult {
  success: boolean;
  data?: ResearchChannelApiSnapshot;
  reason?: string;
}

export interface HotVideosApiResult {
  success: boolean;
  items?: any[];
  reason?: string;
}

type HotVideoChannelStats = {
  subscriberCount: number | null;
  videoCount: number | null;
  channelHandle: string | null;
};

type HotVideoChannelStatsCacheEntry = HotVideoChannelStats & {
  expiresAtMs: number;
  updatedAtMs: number;
};

export interface YoutubeApiBindingConnectivityResult {
  ok: boolean;
  proxy: string;
  proxy_mode: "direct" | "http" | "https" | "socks5";
  egress_ip: string;
  youtube_ok: boolean;
  youtube_status: number;
  youtube_reason: string;
  message: string;
}

function dateInTimeZone(timeZone: string): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const map: Record<string, string> = {};
  for (const part of parts) {
    if (part.type !== "literal") map[part.type] = part.value;
  }
  return `${map.year}-${map.month}-${map.day}`;
}

function todayDate(): string {
  return dateInTimeZone(YOUTUBE_QUOTA_TIMEZONE);
}

function todayUtcDate(): string {
  return dateInTimeZone("UTC");
}

function formatNowLocal(): string {
  const now = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

function toNullableInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.trunc(parsed);
}

function parsePositiveIntSetting(
  raw: string | undefined,
  fallback: number,
): number {
  const parsed = parseInt(raw || "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

function getDailyLimit(): number {
  return parsePositiveIntSetting(
    getSetting("youtube_api_daily_units_limit"),
    DEFAULT_DAILY_UNITS_LIMIT,
  );
}

function getWarningThresholdPercent(): number {
  const parsed = parsePositiveIntSetting(
    getSetting("youtube_api_warning_threshold_percent"),
    DEFAULT_WARNING_THRESHOLD_PERCENT,
  );
  return Math.max(1, Math.min(100, parsed));
}

function isApiEnabled(): boolean {
  return getSetting("channel_api_enabled") !== "false";
}

function isApiAutoRotateEnabled(): boolean {
  return getSetting("youtube_api_auto_rotate_key") !== "false";
}

function normalizeApiKey(raw: unknown): string {
  if (typeof raw !== "string") return "";
  return raw.trim();
}

function normalizeProxyUrl(raw: unknown): string {
  const text = typeof raw === "string" ? raw.trim() : "";
  if (!text) return "";
  try {
    const parsed = new URL(text);
    if (
      parsed.protocol !== "http:" &&
      parsed.protocol !== "https:" &&
      parsed.protocol !== "socks5:" &&
      parsed.protocol !== "socks:" &&
      parsed.protocol !== "socks4:"
    )
      return "";
    return parsed.toString();
  } catch {
    return "";
  }
}

function getPrimaryApiKey(): string {
  return normalizeApiKey(getSetting("youtube_api_key"));
}

function parseExtraApiKeys(raw: string): string[] {
  if (!raw.trim()) return [];

  const trimmed = raw.trim();
  if (
    (trimmed.startsWith("[") && trimmed.endsWith("]")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed.map(normalizeApiKey).filter(Boolean);
      }
      if (typeof parsed === "string") {
        return parsed
          .split(/[\r\n,;]+/)
          .map(normalizeApiKey)
          .filter(Boolean);
      }
    } catch {
      // Fall through to split mode.
    }
  }

  return trimmed
    .split(/[\r\n,;]+/)
    .map(normalizeApiKey)
    .filter(Boolean);
}

function parseApiKeyProxyList(raw: string): string[] {
  const source = String(raw || "");
  if (!source.trim()) return [];

  const trimmed = source.trim();
  if (
    (trimmed.startsWith("[") && trimmed.endsWith("]")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => String(item || "").trim());
      }
      if (typeof parsed === "string") {
        return parsed
          .split(/[\r\n,;]+/)
          .map((item) => item.trim())
          .filter(Boolean);
      }
    } catch {
      // Fall through to split mode.
    }
  }

  const normalized = source.replace(/\r/g, "");
  const lines = normalized.split("\n").map((item) => item.trim());
  if (lines.length <= 1) {
    return trimmed.split(/[,;]+/).map((item) => item.trim());
  }
  return lines;
}

function getApiKeyConfigs(): YoutubeApiKeyConfig[] {
  const primary = getPrimaryApiKey();
  const extraRaw = getSetting("youtube_api_keys") || "";
  const proxyRaw = getSetting("youtube_api_key_proxies") || "";
  const rawKeys = [primary, ...parseExtraApiKeys(extraRaw)].filter(Boolean);
  const rawProxies = parseApiKeyProxyList(proxyRaw);

  const seen = new Set<string>();
  const configs: YoutubeApiKeyConfig[] = [];
  for (let i = 0; i < rawKeys.length; i += 1) {
    const key = normalizeApiKey(rawKeys[i]);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    configs.push({
      key,
      proxyUrl: normalizeProxyUrl(rawProxies[i] || ""),
    });
  }
  return configs;
}

function getApiKeys(): string[] {
  return getApiKeyConfigs().map((item) => item.key);
}

function fnv1aHash(input: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash =
      (hash +
        ((hash << 1) +
          (hash << 4) +
          (hash << 7) +
          (hash << 8) +
          (hash << 24))) >>>
      0;
  }
  return hash.toString(16).padStart(8, "0");
}

function buildKeyId(apiKey: string): string {
  return `k_${fnv1aHash(apiKey)}`;
}

function maskApiKey(apiKey: string): string {
  if (!apiKey) return "";
  const head = apiKey.slice(0, 3);
  const tail = apiKey.slice(-4);
  return `${head}***${tail}`;
}

function formatProxyDisplay(proxyUrl: string): string {
  if (!proxyUrl) return "direct";
  try {
    const parsed = new URL(proxyUrl);
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return "direct";
  }
}

const PROXY_AGENT_CACHE = new Map<string, ProxyAgent>();
const SOCKS_AGENT_CACHE = new Map<string, SocksProxyAgent>();

function getProxyAgent(proxyUrl: string): ProxyAgent | null {
  const normalized = normalizeProxyUrl(proxyUrl);
  if (!normalized) return null;
  const cached = PROXY_AGENT_CACHE.get(normalized);
  if (cached) return cached;
  try {
    const created = new ProxyAgent(normalized);
    PROXY_AGENT_CACHE.set(normalized, created);
    return created;
  } catch {
    return null;
  }
}

function isSocksProxy(proxyUrl: string): boolean {
  const normalized = normalizeProxyUrl(proxyUrl).toLowerCase();
  return (
    normalized.startsWith("socks5://") ||
    normalized.startsWith("socks://") ||
    normalized.startsWith("socks4://")
  );
}

function getProxyMode(
  proxyUrl: string,
): "direct" | "http" | "https" | "socks5" {
  const normalized = normalizeProxyUrl(proxyUrl).toLowerCase();
  if (!normalized) return "direct";
  if (normalized.startsWith("https://")) return "https";
  if (normalized.startsWith("http://")) return "http";
  if (
    normalized.startsWith("socks5://") ||
    normalized.startsWith("socks://") ||
    normalized.startsWith("socks4://")
  )
    return "socks5";
  return "direct";
}

function getSocksProxyAgent(proxyUrl: string): SocksProxyAgent | null {
  const normalized = normalizeProxyUrl(proxyUrl);
  if (!normalized || !isSocksProxy(normalized)) return null;
  const cached = SOCKS_AGENT_CACHE.get(normalized);
  if (cached) return cached;
  try {
    const created = new SocksProxyAgent(normalized);
    SOCKS_AGENT_CACHE.set(normalized, created);
    return created;
  } catch {
    return null;
  }
}

async function executeJsonRequest(
  url: URL,
  proxyUrl: string,
): Promise<{
  networkError: boolean;
  parseError: boolean;
  status: number;
  payload: any;
}> {
  const normalizedProxy = normalizeProxyUrl(proxyUrl);

  if (isSocksProxy(normalizedProxy)) {
    const socksAgent = getSocksProxyAgent(normalizedProxy);
    if (!socksAgent) {
      return {
        networkError: true,
        parseError: false,
        status: 0,
        payload: null,
      };
    }
    return await new Promise((resolve) => {
      const req = https.request(
        url,
        { method: "GET", agent: socksAgent, timeout: 25_000 },
        (res) => {
          const chunks: string[] = [];
          res.setEncoding("utf8");
          res.on("data", (chunk) => chunks.push(String(chunk)));
          res.on("end", () => {
            const status = Number(res.statusCode || 0);
            const text = chunks.join("");
            try {
              const payload = text ? JSON.parse(text) : {};
              resolve({
                networkError: false,
                parseError: false,
                status,
                payload,
              });
            } catch {
              resolve({
                networkError: false,
                parseError: true,
                status,
                payload: null,
              });
            }
          });
        },
      );
      req.on("error", () =>
        resolve({
          networkError: true,
          parseError: false,
          status: 0,
          payload: null,
        }),
      );
      req.on("timeout", () => req.destroy(new Error("request_timeout")));
      req.end();
    });
  }

  const fetchOptions: any = {};
  const proxyAgent = getProxyAgent(normalizedProxy);
  if (proxyAgent) {
    fetchOptions.dispatcher = proxyAgent;
  }
  try {
    const response = await fetch(url.toString(), fetchOptions);
    try {
      const payload = await response.json();
      return {
        networkError: false,
        parseError: false,
        status: Number(response.status || 0),
        payload,
      };
    } catch {
      return {
        networkError: false,
        parseError: true,
        status: Number(response.status || 0),
        payload: null,
      };
    }
  } catch {
    return { networkError: true, parseError: false, status: 0, payload: null };
  }
}

export async function testYoutubeApiBindingConnectivity(
  apiKey: string,
  proxyUrl: string,
): Promise<YoutubeApiBindingConnectivityResult> {
  const normalizedKey = normalizeApiKey(apiKey);
  const normalizedProxy = normalizeProxyUrl(proxyUrl);
  const proxyMode = getProxyMode(normalizedProxy);
  const proxyDisplay = formatProxyDisplay(normalizedProxy);

  if (!normalizedKey) {
    return {
      ok: false,
      proxy: proxyDisplay,
      proxy_mode: proxyMode,
      egress_ip: "",
      youtube_ok: false,
      youtube_status: 0,
      youtube_reason: "api_key_missing",
      message: "API Key 不能为空",
    };
  }
  if (String(proxyUrl || "").trim() && !normalizedProxy) {
    return {
      ok: false,
      proxy: "invalid",
      proxy_mode: "direct",
      egress_ip: "",
      youtube_ok: false,
      youtube_status: 0,
      youtube_reason: "proxy_invalid",
      message: "代理地址无效，仅支持 http/https/socks5",
    };
  }

  const ipCall = await executeJsonRequest(
    new URL("https://api.ipify.org?format=json"),
    normalizedProxy,
  );
  if (
    ipCall.networkError ||
    ipCall.parseError ||
    !(ipCall.status >= 200 && ipCall.status < 300)
  ) {
    return {
      ok: false,
      proxy: proxyDisplay,
      proxy_mode: proxyMode,
      egress_ip: "",
      youtube_ok: false,
      youtube_status: 0,
      youtube_reason: ipCall.networkError
        ? "proxy_network_error"
        : "proxy_response_invalid",
      message: `代理连通失败（${proxyDisplay}）`,
    };
  }
  const egressIp = String(ipCall.payload?.ip || "").trim();
  if (!egressIp) {
    return {
      ok: false,
      proxy: proxyDisplay,
      proxy_mode: proxyMode,
      egress_ip: "",
      youtube_ok: false,
      youtube_status: 0,
      youtube_reason: "proxy_ip_missing",
      message: `代理已连接，但未获取到出口IP（${proxyDisplay}）`,
    };
  }

  const ytUrl = new URL("https://www.googleapis.com/youtube/v3/videos");
  ytUrl.searchParams.set("part", "id");
  ytUrl.searchParams.set("id", "dQw4w9WgXcQ");
  ytUrl.searchParams.set("maxResults", "1");
  ytUrl.searchParams.set("key", normalizedKey);

  const ytCall = await executeJsonRequest(ytUrl, normalizedProxy);
  const youtubeOk =
    !ytCall.networkError &&
    !ytCall.parseError &&
    ytCall.status >= 200 &&
    ytCall.status < 300;
  const youtubeReason = youtubeOk
    ? ""
    : String(
        ytCall.payload?.error?.errors?.[0]?.reason ||
          ytCall.payload?.error?.message ||
          (ytCall.networkError
            ? "youtube_network_error"
            : `youtube_http_${ytCall.status}`),
      );

  if (!youtubeOk) {
    return {
      ok: false,
      proxy: proxyDisplay,
      proxy_mode: proxyMode,
      egress_ip: egressIp,
      youtube_ok: false,
      youtube_status: Number(ytCall.status || 0),
      youtube_reason: youtubeReason,
      message: `出口IP ${egressIp}，YouTube API 调用失败：${youtubeReason}`,
    };
  }

  return {
    ok: true,
    proxy: proxyDisplay,
    proxy_mode: proxyMode,
    egress_ip: egressIp,
    youtube_ok: true,
    youtube_status: Number(ytCall.status || 200),
    youtube_reason: "",
    message: `连通成功，出口IP ${egressIp}，YouTube API 可用`,
  };
}

function createEmptyUsageState(): YoutubeApiUsageState {
  return {
    date: todayDate(),
    total_units: 0,
    total_calls: 0,
    per_method_units: {},
    per_method_calls: {},
    per_key_units: {},
    per_key_calls: {},
    active_key_index: 0,
  };
}

function migrateLegacyUsageState(parsed: any): YoutubeApiUsageState {
  const migrated = createEmptyUsageState();
  if (!parsed || typeof parsed !== "object") return migrated;

  if (typeof parsed.total_units === "number")
    migrated.total_units = Math.max(0, Math.trunc(parsed.total_units));
  if (typeof parsed.total_calls === "number")
    migrated.total_calls = Math.max(0, Math.trunc(parsed.total_calls));

  if (typeof parsed.units === "number" && migrated.total_units === 0) {
    migrated.total_units = Math.max(0, Math.trunc(parsed.units));
    migrated.total_calls = Math.max(migrated.total_calls, migrated.total_units);
  }

  if (parsed.per_method_units && typeof parsed.per_method_units === "object") {
    for (const [key, value] of Object.entries(parsed.per_method_units)) {
      if (typeof value === "number")
        migrated.per_method_units[key] = Math.max(0, Math.trunc(value));
    }
  }
  if (parsed.per_method_calls && typeof parsed.per_method_calls === "object") {
    for (const [key, value] of Object.entries(parsed.per_method_calls)) {
      if (typeof value === "number")
        migrated.per_method_calls[key] = Math.max(0, Math.trunc(value));
    }
  }
  if (parsed.per_key_units && typeof parsed.per_key_units === "object") {
    for (const [key, value] of Object.entries(parsed.per_key_units)) {
      if (typeof value === "number")
        migrated.per_key_units[key] = Math.max(0, Math.trunc(value));
    }
  }
  if (parsed.per_key_calls && typeof parsed.per_key_calls === "object") {
    for (const [key, value] of Object.entries(parsed.per_key_calls)) {
      if (typeof value === "number")
        migrated.per_key_calls[key] = Math.max(0, Math.trunc(value));
    }
  }

  if (
    typeof parsed.active_key_index === "number" &&
    Number.isFinite(parsed.active_key_index)
  ) {
    migrated.active_key_index = Math.max(
      0,
      Math.trunc(parsed.active_key_index),
    );
  }
  return migrated;
}

function readUsageState(): YoutubeApiUsageState {
  const raw = getSetting("youtube_api_usage_json");
  if (!raw) return createEmptyUsageState();

  try {
    const parsed = JSON.parse(raw) as any;
    const migrated = migrateLegacyUsageState(parsed);
    const today = todayDate();
    if (typeof parsed?.date === "string" && parsed.date === today) {
      migrated.date = today;
      return migrated;
    }

    // Backward compatibility: previous versions used UTC date for daily reset.
    // When UTC day and PT day differ, preserve counters by remapping the stamp once.
    const utcToday = todayUtcDate();
    if (
      typeof parsed?.date === "string" &&
      parsed.date === utcToday &&
      utcToday !== today
    ) {
      migrated.date = today;
      writeUsageState(migrated);
      return migrated;
    }

    return createEmptyUsageState();
  } catch {
    return createEmptyUsageState();
  }
}

function writeUsageState(state: YoutubeApiUsageState): void {
  setSetting("youtube_api_usage_json", JSON.stringify(state));
}

function getKeyUsedUnits(state: YoutubeApiUsageState, apiKey: string): number {
  const keyId = buildKeyId(apiKey);
  const used = state.per_key_units[keyId];
  return typeof used === "number" ? Math.max(0, Math.trunc(used)) : 0;
}

function getMethodUnitCost(method: string): number {
  const cost = YOUTUBE_API_METHOD_UNIT_COST[method];
  if (typeof cost === "number" && cost > 0) return Math.trunc(cost);
  return 1;
}

function findAvailableKeyIndex(
  keys: string[],
  state: YoutubeApiUsageState,
  dailyLimit: number,
  requiredUnits: number,
  startIndex: number,
): number {
  if (keys.length === 0) return -1;
  const normalizedStart =
    ((startIndex % keys.length) + keys.length) % keys.length;
  for (let offset = 0; offset < keys.length; offset += 1) {
    const index = (normalizedStart + offset) % keys.length;
    const key = keys[index];
    const used = getKeyUsedUnits(state, key);
    if (used + requiredUnits <= dailyLimit) return index;
  }
  return -1;
}

function getApiKeyRemainingUnits(
  state: YoutubeApiUsageState,
  apiKey: string,
  dailyLimit: number,
): number {
  const used = getKeyUsedUnits(state, apiKey);
  return Math.max(0, dailyLimit - used);
}

function pickApiKeyForBudget(
  requiredUnits: number,
  preferredApiKey = "",
): {
  ok: boolean;
  reason?: string;
  apiKey?: string;
  keyIndex?: number;
  remainingUnits?: number;
} {
  if (!isApiEnabled()) {
    return { ok: false, reason: "channel_api_disabled" };
  }

  const keyConfigs = getApiKeyConfigs();
  if (keyConfigs.length === 0) {
    return { ok: false, reason: "youtube_api_key_missing" };
  }

  const normalizedRequiredUnits = Math.max(
    1,
    Math.trunc(Number(requiredUnits) || 0),
  );
  const dailyLimit = getDailyLimit();
  const state = readUsageState();
  const preferredKey = String(preferredApiKey || "").trim();
  const prioritizedIndexes: number[] = [];
  const seenIndexes = new Set<number>();

  if (preferredKey) {
    const preferredIndex = keyConfigs.findIndex(
      (cfg) => cfg.key === preferredKey,
    );
    if (preferredIndex >= 0) {
      prioritizedIndexes.push(preferredIndex);
      seenIndexes.add(preferredIndex);
    }
  }

  const startIndex = state.active_key_index || 0;
  for (let offset = 0; offset < keyConfigs.length; offset += 1) {
    const idx =
      (((startIndex + offset) % keyConfigs.length) + keyConfigs.length) %
      keyConfigs.length;
    if (seenIndexes.has(idx)) continue;
    prioritizedIndexes.push(idx);
    seenIndexes.add(idx);
  }

  for (const idx of prioritizedIndexes) {
    const cfg = keyConfigs[idx];
    const remainingUnits = getApiKeyRemainingUnits(state, cfg.key, dailyLimit);
    if (remainingUnits >= normalizedRequiredUnits) {
      return {
        ok: true,
        apiKey: cfg.key,
        keyIndex: idx,
        remainingUnits,
      };
    }
  }

  return { ok: false, reason: "youtube_api_hot_budget_insufficient" };
}

function reserveApiUnits(
  method: string,
  preferredApiKey = "",
  strictPreferred = false,
): {
  ok: boolean;
  reason?: string;
  apiKey?: string;
  keyIndex?: number;
  proxyUrl?: string;
} {
  if (!isApiEnabled()) {
    return { ok: false, reason: "channel_api_disabled" };
  }

  const keyConfigs = getApiKeyConfigs();
  const keys = keyConfigs.map((item) => item.key);
  if (keys.length === 0) {
    return { ok: false, reason: "youtube_api_key_missing" };
  }

  const dailyLimit = getDailyLimit();
  const methodCost = getMethodUnitCost(method);
  const state = readUsageState();
  const startIndex = state.active_key_index || 0;
  const preferredKey = String(preferredApiKey || "").trim();
  let selectedIndex = -1;
  if (preferredKey) {
    const preferredIndex = keyConfigs.findIndex(
      (cfg) => cfg.key === preferredKey,
    );
    if (preferredIndex < 0 && strictPreferred) {
      return { ok: false, reason: "youtube_api_key_missing" };
    }
    if (preferredIndex >= 0) {
      const preferredUsed = getKeyUsedUnits(state, preferredKey);
      if (preferredUsed + methodCost <= dailyLimit) {
        selectedIndex = preferredIndex;
      } else if (strictPreferred) {
        return { ok: false, reason: "youtube_api_daily_limit_reached" };
      }
    }
  }
  if (selectedIndex < 0 && !strictPreferred) {
    selectedIndex = findAvailableKeyIndex(
      keys,
      state,
      dailyLimit,
      methodCost,
      startIndex,
    );
  }

  if (selectedIndex < 0) {
    return { ok: false, reason: "youtube_api_daily_limit_reached" };
  }

  const selectedConfig = keyConfigs[selectedIndex];
  const selectedKey = selectedConfig?.key || keys[selectedIndex];
  const selectedKeyId = buildKeyId(selectedKey);

  state.total_units += methodCost;
  state.total_calls += 1;
  state.per_method_units[method] =
    (state.per_method_units[method] || 0) + methodCost;
  state.per_method_calls[method] = (state.per_method_calls[method] || 0) + 1;
  state.per_key_units[selectedKeyId] =
    (state.per_key_units[selectedKeyId] || 0) + methodCost;
  state.per_key_calls[selectedKeyId] =
    (state.per_key_calls[selectedKeyId] || 0) + 1;
  state.active_key_index = selectedIndex;

  const warningUnits = Math.floor(
    dailyLimit * (getWarningThresholdPercent() / 100),
  );
  if (isApiAutoRotateEnabled() && keys.length > 1) {
    const selectedUsed = getKeyUsedUnits(state, selectedKey);
    if (selectedUsed >= warningUnits) {
      const nextIndex = findAvailableKeyIndex(
        keys,
        state,
        dailyLimit,
        methodCost,
        selectedIndex + 1,
      );
      if (nextIndex >= 0 && nextIndex !== selectedIndex) {
        state.active_key_index = nextIndex;
      }
    }
  }

  writeUsageState(state);
  return {
    ok: true,
    apiKey: selectedKey,
    keyIndex: selectedIndex,
    proxyUrl: normalizeProxyUrl(selectedConfig?.proxyUrl || ""),
  };
}

function markApiKeyExhausted(apiKey: string, keyIndex: number): void {
  const state = readUsageState();
  const keys = getApiKeys();
  const dailyLimit = getDailyLimit();
  const keyId = buildKeyId(apiKey);
  state.per_key_units[keyId] = Math.max(
    state.per_key_units[keyId] || 0,
    dailyLimit,
  );

  if (keys.length > 1) {
    const nextIndex = findAvailableKeyIndex(
      keys,
      state,
      dailyLimit,
      1,
      keyIndex + 1,
    );
    if (nextIndex >= 0) state.active_key_index = nextIndex;
  }

  writeUsageState(state);
}

function isQuotaError(payload: any): boolean {
  const reason = String(
    payload?.error?.errors?.[0]?.reason || "",
  ).toLowerCase();
  const message = String(payload?.error?.message || "").toLowerCase();
  return (
    reason.includes("quotaexceeded") ||
    reason.includes("dailylimitexceeded") ||
    reason.includes("ratelimitexceeded") ||
    reason.includes("userratelimitexceeded") ||
    message.includes("quota") ||
    message.includes("daily limit")
  );
}

async function executeApiCallWithQuota(
  method: string,
  buildUrl: (apiKey: string) => URL,
  preferredApiKey = "",
  strictPreferred = false,
): Promise<{
  ok: boolean;
  payload?: any;
  reason?: string;
  apiKey?: string;
  keyIndex?: number;
  proxyUrl?: string;
}> {
  const normalizedPreferredKey = String(preferredApiKey || "").trim();
  const maxAttempts =
    strictPreferred && normalizedPreferredKey
      ? 1
      : Math.max(1, getApiKeys().length);
  let lastReason = "youtube_api_unknown_error";

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const reservation = reserveApiUnits(
      method,
      strictPreferred
        ? normalizedPreferredKey
        : attempt === 0
          ? normalizedPreferredKey
          : "",
      strictPreferred,
    );
    if (
      !reservation.ok ||
      !reservation.apiKey ||
      reservation.keyIndex == null
    ) {
      return {
        ok: false,
        reason: reservation.reason || "youtube_api_unavailable",
      };
    }

    const call = await executeJsonRequest(
      buildUrl(reservation.apiKey),
      String(reservation.proxyUrl || ""),
    );
    if (call.networkError) {
      lastReason = "youtube_api_network_error";
      if (strictPreferred) {
        return { ok: false, reason: lastReason };
      }
      continue;
    }
    if (call.parseError) {
      lastReason = "youtube_api_invalid_json";
      if (strictPreferred) {
        return { ok: false, reason: lastReason };
      }
      continue;
    }
    const payload = call.payload;

    if (!(call.status >= 200 && call.status < 300)) {
      if (isQuotaError(payload)) {
        markApiKeyExhausted(reservation.apiKey, reservation.keyIndex);
        lastReason = "youtube_api_quota_exhausted";
        if (strictPreferred) {
          return { ok: false, reason: lastReason };
        }
        continue;
      }
      const reason =
        payload?.error?.errors?.[0]?.reason ||
        payload?.error?.message ||
        `youtube_api_http_${call.status}`;
      return { ok: false, reason };
    }

    return {
      ok: true,
      payload,
      apiKey: reservation.apiKey,
      keyIndex: reservation.keyIndex,
      proxyUrl: String(reservation.proxyUrl || ""),
    };
  }

  return { ok: false, reason: lastReason };
}

function normalizeCustomUrl(customUrl: string): string {
  if (!customUrl) return "";
  if (customUrl.startsWith("@")) return customUrl;
  return `@${customUrl}`;
}

function normalizeHotVideoTypeFilter(raw: unknown): "all" | "video" | "shorts" {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (value === "shorts") return "shorts";
  if (value === "video") return "video";
  return "all";
}

function normalizeHotVideoTimeFilter(
  raw: unknown,
): "any" | "week" | "month" | "6month" {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (value === "week") return "week";
  if (value === "month") return "month";
  if (value === "6month") return "6month";
  return "any";
}

function normalizeHotVideoDurationFilter(
  raw: unknown,
): "any" | "short" | "medium" | "long" {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (value === "short") return "short";
  if (value === "medium") return "medium";
  if (value === "long") return "long";
  return "any";
}

function resolvePublishedAfterByTimeFilter(
  timeFilter: "any" | "week" | "month" | "6month",
): string | null {
  if (timeFilter === "any") return null;
  const days = timeFilter === "week" ? 7 : timeFilter === "month" ? 30 : 180;
  const ts = Date.now() - days * 24 * 60 * 60 * 1000;
  return new Date(ts).toISOString();
}

function parseYoutubeIsoDurationSeconds(raw: unknown): number {
  const text = String(raw || "").trim();
  const match = text.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!match) return 0;
  const hours = Number.parseInt(match[1] || "0", 10) || 0;
  const minutes = Number.parseInt(match[2] || "0", 10) || 0;
  const seconds = Number.parseInt(match[3] || "0", 10) || 0;
  return hours * 3600 + minutes * 60 + seconds;
}

function isHotVideoTypeMatched(
  item: any,
  typeFilter: "all" | "video" | "shorts",
): boolean {
  if (typeFilter === "all") return true;
  const durationSec = parseYoutubeIsoDurationSeconds(
    item?.contentDetails?.duration,
  );
  if (typeFilter === "shorts") return durationSec > 0 && durationSec <= 60;
  // If duration is missing, keep the row for video mode instead of dropping a potentially valid long video.
  if (durationSec <= 0) return true;
  return durationSec > 60;
}

function resolveHotVideoSearchMaxPages(
  typeFilter: "all" | "video" | "shorts",
  maxResults: number,
): number {
  const basePages = Math.max(1, Math.ceil(maxResults / 50));
  if (typeFilter === "all") return basePages;
  if (typeFilter === "video") {
    return Math.min(10, Math.max(basePages * 4, basePages + 2));
  }
  return Math.min(10, Math.max(basePages * 5, basePages + 3));
}

function estimateHotVideosRequestUnits(
  typeFilter: "all" | "video" | "shorts",
  maxResults: number,
): number {
  const boundedMaxResults = Math.max(
    1,
    Math.min(500, Math.trunc(Number(maxResults) || 50)),
  );
  const expectedSearchPages = resolveHotVideoSearchMaxPages(
    typeFilter,
    boundedMaxResults,
  );
  const searchUnits = expectedSearchPages * getMethodUnitCost("search.list");
  const detailUnits = expectedSearchPages * getMethodUnitCost("videos.list");
  const channelBatches = Math.max(1, Math.ceil(boundedMaxResults / 50));
  const channelUnits = channelBatches * getMethodUnitCost("channels.list");
  return searchUnits + detailUnits + channelUnits;
}

function getHotVideoChannelStatsCacheTtlMs(): number {
  const minutesRaw = parseInt(
    String(getSetting("hot_videos_channel_stats_cache_minutes") || ""),
    10,
  );
  if (!Number.isFinite(minutesRaw) || minutesRaw <= 0)
    return HOT_VIDEO_CHANNEL_STATS_CACHE_TTL_MS;
  const minutes = Math.max(1, Math.min(24 * 60, minutesRaw));
  return minutes * 60 * 1000;
}

function pruneHotVideoChannelStatsCache(nowMs: number): void {
  for (const [channelId, entry] of hotVideoChannelStatsCache) {
    if (entry.expiresAtMs <= nowMs) {
      hotVideoChannelStatsCache.delete(channelId);
    }
  }
  if (hotVideoChannelStatsCache.size <= HOT_VIDEO_CHANNEL_STATS_CACHE_MAX_SIZE)
    return;

  const items = Array.from(hotVideoChannelStatsCache.entries()).sort(
    (a, b) => a[1].updatedAtMs - b[1].updatedAtMs,
  );
  const overflow =
    hotVideoChannelStatsCache.size - HOT_VIDEO_CHANNEL_STATS_CACHE_MAX_SIZE;
  for (let i = 0; i < overflow; i += 1) {
    const channelId = items[i]?.[0];
    if (channelId) hotVideoChannelStatsCache.delete(channelId);
  }
}

function readHotVideoChannelStatsCache(
  channelId: string,
  nowMs: number,
): HotVideoChannelStats | null {
  const key = String(channelId || "").trim();
  if (!key) return null;
  const entry = hotVideoChannelStatsCache.get(key);
  if (!entry) return null;
  if (entry.expiresAtMs <= nowMs) {
    hotVideoChannelStatsCache.delete(key);
    return null;
  }
  return {
    subscriberCount: entry.subscriberCount,
    videoCount: entry.videoCount,
    channelHandle: entry.channelHandle,
  };
}

function writeHotVideoChannelStatsCache(
  channelId: string,
  stats: HotVideoChannelStats,
  nowMs: number,
): void {
  const key = String(channelId || "").trim();
  if (!key) return;
  const ttlMs = getHotVideoChannelStatsCacheTtlMs();
  hotVideoChannelStatsCache.set(key, {
    subscriberCount: stats.subscriberCount,
    videoCount: stats.videoCount,
    channelHandle: stats.channelHandle,
    updatedAtMs: nowMs,
    expiresAtMs: nowMs + ttlMs,
  });
}

async function attachChannelStatisticsToHotVideos(
  items: any[],
  lockedApiKey = "",
): Promise<any[]> {
  if (!Array.isArray(items) || items.length === 0) return [];

  const nowMs = Date.now();
  pruneHotVideoChannelStatsCache(nowMs);

  const channelIds = Array.from(
    new Set(
      items
        .map((item) => String(item?.snippet?.channelId || "").trim())
        .filter(Boolean),
    ),
  );
  if (channelIds.length === 0) return items;

  const channelStatsById = new Map<string, HotVideoChannelStats>();
  const missingChannelIds: string[] = [];
  for (const channelId of channelIds) {
    const cached = readHotVideoChannelStatsCache(channelId, nowMs);
    if (cached) {
      channelStatsById.set(channelId, cached);
    } else {
      missingChannelIds.push(channelId);
    }
  }

  for (let idx = 0; idx < missingChannelIds.length; idx += 50) {
    const batchIds = missingChannelIds.slice(idx, idx + 50);
    const channelCall = await executeApiCallWithQuota(
      "channels.list",
      (apiKey) => {
        const url = new URL("https://www.googleapis.com/youtube/v3/channels");
        url.searchParams.set("part", "snippet,statistics");
        url.searchParams.set("id", batchIds.join(","));
        url.searchParams.set(
          "maxResults",
          String(Math.min(50, batchIds.length)),
        );
        url.searchParams.set("key", apiKey);
        return url;
      },
      lockedApiKey,
      Boolean(String(lockedApiKey || "").trim()),
    );

    if (!channelCall.ok) {
      break;
    }

    const channels = Array.isArray(channelCall.payload?.items)
      ? channelCall.payload.items
      : [];
    const foundIds = new Set<string>();
    for (const channel of channels) {
      const channelId = String(channel?.id || "").trim();
      if (!channelId) continue;
      const stats: HotVideoChannelStats = {
        subscriberCount: toNullableInt(channel?.statistics?.subscriberCount),
        videoCount: toNullableInt(channel?.statistics?.videoCount),
        channelHandle: normalizeCustomUrl(
          String(channel?.snippet?.customUrl || "").trim(),
        ),
      };
      foundIds.add(channelId);
      channelStatsById.set(channelId, stats);
      writeHotVideoChannelStatsCache(channelId, stats, nowMs);
    }

    for (const channelId of batchIds) {
      if (foundIds.has(channelId)) continue;
      const emptyStats: HotVideoChannelStats = {
        subscriberCount: null,
        videoCount: null,
        channelHandle: null,
      };
      channelStatsById.set(channelId, emptyStats);
      writeHotVideoChannelStatsCache(channelId, emptyStats, nowMs);
    }
  }

  return items.map((item) => {
    const channelId = String(item?.snippet?.channelId || "").trim();
    const stats = channelStatsById.get(channelId) || {
      subscriberCount: null,
      videoCount: null,
      channelHandle: null,
    };
    return {
      ...item,
      channel_subscriber_count: stats.subscriberCount,
      channel_video_count: stats.videoCount,
      channel_handle: stats.channelHandle,
      channel_statistics: {
        subscriberCount: stats.subscriberCount,
        videoCount: stats.videoCount,
      },
    };
  });
}

export async function fetchHotVideosByKeyword(
  query: string,
  options: {
    maxResults?: number;
    typeFilter?: "all" | "video" | "shorts";
    timeFilter?: "any" | "week" | "month" | "6month";
    durationFilter?: "any" | "short" | "medium" | "long";
    order?: "relevance" | "date" | "viewCount";
  } = {},
): Promise<HotVideosApiResult> {
  const keyword = String(query || "").trim();
  if (!keyword) return { success: false, reason: "query_required" };

  const maxResults = Math.max(
    1,
    Math.min(500, Math.trunc(Number(options.maxResults || 50) || 50)),
  );
  const typeFilter = normalizeHotVideoTypeFilter(options.typeFilter || "all");
  const timeFilter = normalizeHotVideoTimeFilter(options.timeFilter || "any");
  const durationFilter = normalizeHotVideoDurationFilter(
    options.durationFilter || "any",
  );
  const publishedAfter = resolvePublishedAfterByTimeFilter(timeFilter);
  const sortOrder = (() => {
    const rawOrder = String(options.order || "relevance").trim();
    if (rawOrder === "date" || rawOrder === "viewCount") return rawOrder;
    return "relevance";
  })();
  const expectedUnits = estimateHotVideosRequestUnits(typeFilter, maxResults);
  const lockSelection = pickApiKeyForBudget(expectedUnits, getPrimaryApiKey());
  if (!lockSelection.ok || !lockSelection.apiKey) {
    return {
      success: false,
      reason: lockSelection.reason || "youtube_api_hot_budget_insufficient",
    };
  }
  const lockedApiKey = lockSelection.apiKey;

  const collectedItems: any[] = [];
  const seenVideoIds = new Set<string>();
  let pageToken = "";
  const maxPages = resolveHotVideoSearchMaxPages(typeFilter, maxResults);

  for (
    let page = 0;
    page < maxPages && collectedItems.length < maxResults;
    page += 1
  ) {
    const pageSize =
      typeFilter === "all"
        ? Math.min(50, maxResults - collectedItems.length)
        : 50;
    const searchCall = await executeApiCallWithQuota(
      "search.list",
      (apiKey) => {
        const url = new URL("https://www.googleapis.com/youtube/v3/search");
        url.searchParams.set("part", "snippet");
        url.searchParams.set("type", "video");
        url.searchParams.set("maxResults", String(pageSize));
        url.searchParams.set("q", keyword);
        url.searchParams.set("order", sortOrder);
        if (publishedAfter) {
          url.searchParams.set("publishedAfter", publishedAfter);
        }
        const apiDurationFilter =
          typeFilter === "shorts" ? "short" : durationFilter;
        if (apiDurationFilter !== "any") {
          url.searchParams.set("videoDuration", apiDurationFilter);
        }
        if (pageToken) {
          url.searchParams.set("pageToken", pageToken);
        }
        url.searchParams.set("key", apiKey);
        return url;
      },
      lockedApiKey,
      true,
    );
    if (!searchCall.ok) {
      return {
        success: false,
        reason: searchCall.reason || "youtube_api_unavailable",
      };
    }

    const pageVideoIds = Array.isArray(searchCall.payload?.items)
      ? searchCall.payload.items
          .map((item: any) => String(item?.id?.videoId || "").trim())
          .filter(Boolean)
      : [];
    const dedupedPageIds: string[] = [];
    for (const id of pageVideoIds) {
      if (seenVideoIds.has(id)) continue;
      seenVideoIds.add(id);
      dedupedPageIds.push(id);
    }

    if (dedupedPageIds.length > 0) {
      const detailsById = new Map<string, any>();
      for (let idx = 0; idx < dedupedPageIds.length; idx += 50) {
        const batchIds = dedupedPageIds.slice(idx, idx + 50);
        const detailCall = await executeApiCallWithQuota(
          "videos.list",
          (apiKey) => {
            const url = new URL("https://www.googleapis.com/youtube/v3/videos");
            url.searchParams.set("part", "snippet,statistics,contentDetails");
            url.searchParams.set("id", batchIds.join(","));
            url.searchParams.set("key", apiKey);
            return url;
          },
          lockedApiKey,
          true,
        );
        if (!detailCall.ok) {
          return {
            success: false,
            reason: detailCall.reason || "youtube_api_unavailable",
          };
        }

        const items = Array.isArray(detailCall.payload?.items)
          ? detailCall.payload.items
          : [];
        for (const item of items) {
          const videoId = String(item?.id || "").trim();
          if (!videoId) continue;
          detailsById.set(videoId, item);
        }
      }

      for (const id of dedupedPageIds) {
        const item = detailsById.get(id);
        if (!item) continue;
        if (!isHotVideoTypeMatched(item, typeFilter)) continue;
        collectedItems.push(item);
        if (collectedItems.length >= maxResults) break;
      }
    }

    const nextPageToken = String(
      searchCall.payload?.nextPageToken || "",
    ).trim();
    if (!nextPageToken) break;
    pageToken = nextPageToken;
  }

  if (collectedItems.length === 0) {
    return { success: true, items: [] };
  }

  const sliced = collectedItems.slice(0, maxResults);
  const enriched = await attachChannelStatisticsToHotVideos(
    sliced,
    lockedApiKey,
  );
  return { success: true, items: enriched };
}

export function getYoutubeApiUsageStatus(): {
  enabled: boolean;
  has_api_key: boolean;
  date: string;
  key_count: number;
  per_key_daily_limit: number;
  used_units: number;
  daily_limit: number;
  remaining_units: number;
  total_calls: number;
  warning_threshold_percent: number;
  warning_threshold_units: number;
  auto_rotate_enabled: boolean;
  keys: Array<{
    index: number;
    key_masked: string;
    proxy: string;
    used_units: number;
    remaining_units: number;
    calls: number;
    warning: boolean;
    exhausted: boolean;
    active: boolean;
  }>;
  method_usage: Record<string, { units: number; calls: number }>;
} {
  const state = readUsageState();
  const perKeyDailyLimit = getDailyLimit();
  const keyConfigs = getApiKeyConfigs();
  const keys = keyConfigs.map((item) => item.key);
  const keyCount = keys.length;
  const totalDailyLimit = Math.max(0, perKeyDailyLimit * keyCount);
  const warningPercent = getWarningThresholdPercent();
  const warningUnits = Math.floor(totalDailyLimit * (warningPercent / 100));
  const perKeyWarningUnits = Math.floor(
    perKeyDailyLimit * (warningPercent / 100),
  );
  const remaining = Math.max(0, totalDailyLimit - state.total_units);

  const keyStatuses = keyConfigs.map((item, index) => {
    const key = item.key;
    const keyId = buildKeyId(key);
    const usedUnits = Math.max(0, Math.trunc(state.per_key_units[keyId] || 0));
    const calls = Math.max(0, Math.trunc(state.per_key_calls[keyId] || 0));
    return {
      index,
      key_masked: maskApiKey(key),
      proxy: formatProxyDisplay(item.proxyUrl),
      used_units: usedUnits,
      remaining_units: Math.max(0, perKeyDailyLimit - usedUnits),
      calls,
      warning: usedUnits >= perKeyWarningUnits,
      exhausted: usedUnits >= perKeyDailyLimit,
      active: index === (state.active_key_index || 0),
    };
  });

  const methodUsage: Record<string, { units: number; calls: number }> = {};
  const methodKeys = new Set([
    ...Object.keys(state.per_method_units || {}),
    ...Object.keys(state.per_method_calls || {}),
  ]);
  for (const method of methodKeys) {
    methodUsage[method] = {
      units: Math.max(0, Math.trunc(state.per_method_units[method] || 0)),
      calls: Math.max(0, Math.trunc(state.per_method_calls[method] || 0)),
    };
  }

  return {
    enabled: isApiEnabled(),
    has_api_key: keys.length > 0,
    date: state.date,
    key_count: keyCount,
    per_key_daily_limit: perKeyDailyLimit,
    used_units: state.total_units,
    daily_limit: totalDailyLimit,
    remaining_units: remaining,
    total_calls: state.total_calls,
    warning_threshold_percent: warningPercent,
    warning_threshold_units: warningUnits,
    auto_rotate_enabled: isApiAutoRotateEnabled(),
    keys: keyStatuses,
    method_usage: methodUsage,
  };
}

export async function fetchChannelSnapshotFromApi(
  channelId: string,
): Promise<ChannelApiResult> {
  const method = "channels.list";
  const maxAttempts = Math.max(1, getApiKeys().length);
  let lastReason = "youtube_api_unknown_error";

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const reservation = reserveApiUnits(method);
    if (
      !reservation.ok ||
      !reservation.apiKey ||
      reservation.keyIndex == null
    ) {
      return {
        success: false,
        reason: reservation.reason || "youtube_api_unavailable",
      };
    }

    const url = new URL("https://www.googleapis.com/youtube/v3/channels");
    url.searchParams.set("part", "snippet,statistics");
    if (channelId.startsWith("UC")) {
      url.searchParams.set("id", channelId);
    } else {
      const handle = channelId.replace(/^@+/, "").trim();
      if (!handle) return { success: false, reason: "channel_id_invalid" };
      // channels.list supports forHandle; value may include or omit @.
      url.searchParams.set("forHandle", handle);
    }
    url.searchParams.set("maxResults", "1");
    url.searchParams.set("key", reservation.apiKey);

    const call = await executeJsonRequest(
      url,
      String(reservation.proxyUrl || ""),
    );
    if (call.networkError) {
      lastReason = "youtube_api_network_error";
      continue;
    }
    if (call.parseError) {
      lastReason = "youtube_api_invalid_json";
      continue;
    }
    const payload = call.payload;

    if (!(call.status >= 200 && call.status < 300)) {
      if (isQuotaError(payload)) {
        markApiKeyExhausted(reservation.apiKey, reservation.keyIndex);
        lastReason = "youtube_api_quota_exhausted";
        continue;
      }
      const reason =
        payload?.error?.errors?.[0]?.reason ||
        payload?.error?.message ||
        `youtube_api_http_${call.status}`;
      return { success: false, reason };
    }

    const item = payload?.items?.[0];
    if (!item) {
      return { success: false, reason: "youtube_api_channel_not_found" };
    }

    const snippet = item.snippet || {};
    const statistics = item.statistics || {};

    const totalViews = toNullableInt(statistics.viewCount);
    const subscriberCount = toNullableInt(statistics.subscriberCount);
    const videoCount = toNullableInt(statistics.videoCount);

    const averageViews =
      totalViews != null && videoCount != null && videoCount > 0
        ? Number((totalViews / videoCount).toFixed(2))
        : 0;
    const averageSubscribersPerVideo =
      subscriberCount != null && videoCount != null && videoCount > 0
        ? Number((subscriberCount / videoCount).toFixed(2))
        : 0;

    const publishedAt =
      typeof snippet.publishedAt === "string"
        ? snippet.publishedAt.split("T")[0] || ""
        : "";

    const snapshot: ChannelApiSnapshot = {
      channelId: item.id || channelId,
      title: snippet.title || "",
      description: snippet.description || "",
      customUrl: normalizeCustomUrl(snippet.customUrl || ""),
      createdDate: publishedAt,
      totalViews,
      subscriberCount,
      videoCount,
      averageViews,
      averageSubscribersPerVideo,
      highThumbnailUrl: snippet.thumbnails?.high?.url || "",
      country: snippet.country || "",
      fetchedAt: formatNowLocal(),
    };

    return { success: true, data: snapshot };
  }

  return { success: false, reason: lastReason };
}

function normalizeResearchChannelInput(
  channelInput: string,
): { lookup: string; handleHint: string | null } | null {
  const raw = String(channelInput || "").trim();
  if (!raw) return null;

  if (raw.startsWith("UC")) {
    return { lookup: raw, handleHint: null };
  }

  const decoded = (() => {
    try {
      return decodeURIComponent(raw);
    } catch {
      return raw;
    }
  })();

  const handleName = decoded.replace(/^@+/, "").trim();
  if (!handleName) return null;
  return { lookup: handleName, handleHint: `@${handleName}` };
}

async function fetchFirstVideoPublishedAtFromUploads(
  uploadsPlaylistId: string,
): Promise<string | null> {
  const playlistId = String(uploadsPlaylistId || "").trim();
  if (!playlistId) return null;

  let pageToken = "";
  let oldestPublishedAt: string | null = null;
  const MAX_PAGES = 200; // hard cap to avoid over-consuming quota for huge channels

  for (let page = 0; page < MAX_PAGES; page += 1) {
    const call = await executeApiCallWithQuota(
      "playlistItems.list",
      (apiKey) => {
        const url = new URL(
          "https://www.googleapis.com/youtube/v3/playlistItems",
        );
        url.searchParams.set("part", "snippet");
        url.searchParams.set("playlistId", playlistId);
        url.searchParams.set("maxResults", "50");
        if (pageToken) url.searchParams.set("pageToken", pageToken);
        url.searchParams.set("key", apiKey);
        return url;
      },
    );
    if (!call.ok) {
      return oldestPublishedAt;
    }

    const items = Array.isArray(call.payload?.items) ? call.payload.items : [];
    if (items.length > 0) {
      const lastItem = items[items.length - 1];
      const publishedAt =
        typeof lastItem?.snippet?.publishedAt === "string"
          ? lastItem.snippet.publishedAt
          : null;
      if (publishedAt) oldestPublishedAt = publishedAt;
    }

    const nextPageToken = String(call.payload?.nextPageToken || "").trim();
    if (!nextPageToken) break;
    pageToken = nextPageToken;
  }

  return oldestPublishedAt;
}

export async function fetchResearchChannelSnapshotFromApi(
  channelInput: string,
): Promise<ResearchChannelApiResult> {
  const normalized = normalizeResearchChannelInput(channelInput);
  if (!normalized) {
    return { success: false, reason: "channel_id_invalid" };
  }

  const call = await executeApiCallWithQuota("channels.list", (apiKey) => {
    const url = new URL("https://www.googleapis.com/youtube/v3/channels");
    url.searchParams.set("part", "snippet,statistics,contentDetails");
    if (normalized.lookup.startsWith("UC")) {
      url.searchParams.set("id", normalized.lookup);
    } else {
      url.searchParams.set("forHandle", normalized.lookup);
    }
    url.searchParams.set("maxResults", "1");
    url.searchParams.set("key", apiKey);
    return url;
  });

  if (!call.ok) {
    return { success: false, reason: call.reason || "youtube_api_unavailable" };
  }

  const item = call.payload?.items?.[0];
  if (!item) {
    return { success: false, reason: "youtube_api_channel_not_found" };
  }

  const snippet = item.snippet || {};
  const statistics = item.statistics || {};
  const contentDetails = item.contentDetails || {};
  const uploadsPlaylistId = String(
    contentDetails?.relatedPlaylists?.uploads || "",
  ).trim();
  const firstVideoPublishedAt = uploadsPlaylistId
    ? await fetchFirstVideoPublishedAtFromUploads(uploadsPlaylistId)
    : null;
  const customUrl = normalizeCustomUrl(String(snippet.customUrl || "").trim());

  const snapshot: ResearchChannelApiSnapshot = {
    channel_id: String(item.id || normalized.lookup),
    title: String(snippet.title || ""),
    handle: customUrl || normalized.handleHint,
    avatar_url: String(
      snippet.thumbnails?.high?.url || snippet.thumbnails?.default?.url || "",
    ),
    subscriber_count: toNullableInt(statistics.subscriberCount),
    video_count: toNullableInt(statistics.videoCount),
    view_count: toNullableInt(statistics.viewCount),
    first_video_published_at: firstVideoPublishedAt,
  };

  return { success: true, data: snapshot };
}

export function toChannelReportRow(
  snapshot: ChannelApiSnapshot,
): Record<string, string | number> {
  return {
    频道ID: snapshot.channelId,
    频道标题: snapshot.title,
    频道简介: snapshot.description,
    频道Handle: snapshot.customUrl,
    创建日期: snapshot.createdDate,
    总观看次数: snapshot.totalViews ?? 0,
    订阅者数: snapshot.subscriberCount ?? 0,
    视频总数: snapshot.videoCount ?? 0,
    平均观看: snapshot.averageViews,
    "平均订阅/视频": snapshot.averageSubscribersPerVideo,
    头像URL: snapshot.highThumbnailUrl,
    国家: snapshot.country,
    抓取时间: snapshot.fetchedAt,
  };
}

export function toChannelReportRowFromDb(
  channel: any,
): Record<string, string | number> {
  const viewCount = channel.view_count != null ? Number(channel.view_count) : 0;
  const subscriberCount =
    channel.subscriber_count != null ? Number(channel.subscriber_count) : 0;
  const videoCount =
    channel.video_count != null ? Number(channel.video_count) : 0;
  const avgViews =
    videoCount > 0 ? Number((viewCount / videoCount).toFixed(2)) : 0;
  const avgSubscribers =
    videoCount > 0 ? Number((subscriberCount / videoCount).toFixed(2)) : 0;

  return {
    频道ID: channel.channel_id || "",
    频道标题: channel.title || "",
    频道简介: "",
    频道Handle: channel.handle || "",
    创建日期:
      typeof channel.created_at === "string"
        ? channel.created_at.split(" ")[0]
        : "",
    总观看次数: viewCount,
    订阅者数: subscriberCount,
    视频总数: videoCount,
    平均观看: avgViews,
    "平均订阅/视频": avgSubscribers,
    头像URL: channel.avatar_url || "",
    国家: channel.country || "",
    抓取时间: formatNowLocal(),
  };
}
