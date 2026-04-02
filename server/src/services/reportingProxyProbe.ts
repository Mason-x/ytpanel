import https from 'https';
import { ProxyAgent } from 'undici';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { getDb } from '../db.js';
import { normalizeReportingOwnerProxyUrl, type ReportingOwnerRow } from './reportingOwners.js';

export type ReportingProxyMode = 'direct' | 'http' | 'https' | 'socks5';

type ProbeCallResult = {
  ok: boolean;
  status_code: number;
  payload: any;
};

type ReportingProxyProbeOptions = {
  ipProbe?: (proxyUrl: string) => Promise<ProbeCallResult>;
  oauthProbe?: (proxyUrl: string) => Promise<ProbeCallResult>;
  reportingProbe?: (proxyUrl: string) => Promise<ProbeCallResult>;
};

type ReportingRequestLogInput = {
  owner_id?: string | null;
  channel_id?: string | null;
  request_kind: string;
  request_url?: string | null;
  proxy_url_snapshot?: string | null;
  status_code?: number | null;
  success?: boolean;
  error_code?: string | null;
  error_message?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  response_meta_json?: string | null;
};

function nowSql(): string {
  return new Date().toISOString().replace('T', ' ').replace('Z', '');
}

function isSocksProxy(proxyUrl: string): boolean {
  const lowered = String(proxyUrl || '').trim().toLowerCase();
  return lowered.startsWith('socks5://') || lowered.startsWith('socks://') || lowered.startsWith('socks4://');
}

function formatProxyDisplay(proxyUrl: string): string {
  const normalized = normalizeReportingOwnerProxyUrl(proxyUrl);
  if (!normalized) return 'direct';
  try {
    const parsed = new URL(normalized);
    return `${parsed.protocol}//${parsed.host}`;
  } catch {
    return 'direct';
  }
}

export function getProxyMode(proxyUrl: string): ReportingProxyMode {
  const normalized = normalizeReportingOwnerProxyUrl(proxyUrl).toLowerCase();
  if (!normalized) return 'direct';
  if (normalized.startsWith('https://')) return 'https';
  if (normalized.startsWith('http://')) return 'http';
  if (isSocksProxy(normalized)) return 'socks5';
  return 'direct';
}

export function insertReportingRequestLog(input: ReportingRequestLogInput): number {
  const result = getDb().prepare(`
    INSERT INTO reporting_request_logs (
      owner_id, channel_id, request_kind, request_url, proxy_url_snapshot, status_code,
      success, error_code, error_message, started_at, finished_at, duration_ms, response_meta_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    input.owner_id || null,
    input.channel_id || null,
    String(input.request_kind || '').trim(),
    input.request_url || null,
    input.proxy_url_snapshot || null,
    input.status_code ?? null,
    input.success ? 1 : 0,
    input.error_code || null,
    input.error_message || null,
    input.started_at || nowSql(),
    input.finished_at || null,
    input.duration_ms ?? null,
    input.response_meta_json || '{}',
  );
  return Number(result.lastInsertRowid || 0);
}

async function executeJsonRequest(url: URL, proxyUrl: string): Promise<ProbeCallResult> {
  const normalizedProxy = normalizeReportingOwnerProxyUrl(proxyUrl);

  if (isSocksProxy(normalizedProxy)) {
    let socksAgent: SocksProxyAgent | null = null;
    try {
      socksAgent = new SocksProxyAgent(normalizedProxy);
    } catch {
      return { ok: false, status_code: 0, payload: null };
    }
    return await new Promise((resolve) => {
      const request = https.request(url, { method: 'GET', agent: socksAgent, timeout: 25_000 }, (response) => {
        const chunks: string[] = [];
        response.setEncoding('utf8');
        response.on('data', (chunk) => chunks.push(String(chunk)));
        response.on('end', () => {
          const statusCode = Number(response.statusCode || 0);
          const text = chunks.join('');
          try {
            resolve({
              ok: statusCode > 0,
              status_code: statusCode,
              payload: text ? JSON.parse(text) : {},
            });
          } catch {
            resolve({
              ok: statusCode > 0,
              status_code: statusCode,
              payload: { raw: text },
            });
          }
        });
      });
      request.on('error', () => resolve({ ok: false, status_code: 0, payload: null }));
      request.on('timeout', () => request.destroy(new Error('request_timeout')));
      request.end();
    });
  }

  let proxyAgent: ProxyAgent | null = null;
  const fetchOptions: RequestInit & { dispatcher?: ProxyAgent } = {};
  if (normalizedProxy) {
    try {
      proxyAgent = new ProxyAgent(normalizedProxy);
      fetchOptions.dispatcher = proxyAgent;
    } catch {
      return { ok: false, status_code: 0, payload: null };
    }
  }

  try {
    const response = await fetch(url.toString(), fetchOptions);
    const statusCode = Number(response.status || 0);
    try {
      return {
        ok: statusCode > 0,
        status_code: statusCode,
        payload: await response.json(),
      };
    } catch {
      return {
        ok: statusCode > 0,
        status_code: statusCode,
        payload: { raw: await response.text().catch(() => '') },
      };
    }
  } catch {
    return { ok: false, status_code: 0, payload: null };
  } finally {
    if (proxyAgent) {
      await proxyAgent.close().catch(() => null);
    }
  }
}

export async function probeReportingProxy(
  owner: Pick<ReportingOwnerRow, 'owner_id' | 'proxy_url'>,
  options: ReportingProxyProbeOptions = {},
): Promise<{
  ok: boolean;
  proxy: string;
  proxy_mode: ReportingProxyMode;
  egress_ip: string;
  google_oauth_ok: boolean;
  reporting_api_ok: boolean;
  message: string;
}> {
  const normalizedProxy = normalizeReportingOwnerProxyUrl(owner.proxy_url);
  const proxy = formatProxyDisplay(normalizedProxy);
  const proxyMode = getProxyMode(normalizedProxy);
  const ownerId = String(owner.owner_id || '').trim() || null;

  const ipProbe = options.ipProbe || ((proxyUrl: string) =>
    executeJsonRequest(new URL('https://api.ipify.org?format=json'), proxyUrl));
  const oauthProbe = options.oauthProbe || ((proxyUrl: string) =>
    executeJsonRequest(new URL('https://oauth2.googleapis.com/tokeninfo?access_token=invalid'), proxyUrl));
  const reportingProbe = options.reportingProbe || ((proxyUrl: string) =>
    executeJsonRequest(new URL('https://youtubereporting.googleapis.com/$discovery/rest?version=v1'), proxyUrl));

  const ipStartedAt = nowSql();
  const ipResult = await ipProbe(normalizedProxy);
  const ipFinishedAt = nowSql();
  insertReportingRequestLog({
    owner_id: ownerId,
    request_kind: 'proxy_probe',
    request_url: 'https://api.ipify.org?format=json',
    proxy_url_snapshot: normalizedProxy || null,
    status_code: ipResult.status_code,
    success: ipResult.ok,
    started_at: ipStartedAt,
    finished_at: ipFinishedAt,
    response_meta_json: JSON.stringify(ipResult.payload || {}),
    error_message: ipResult.ok ? null : 'proxy_probe_failed',
  });

  const egressIp = String(ipResult.payload?.ip || '').trim();
  if (!ipResult.ok || !egressIp) {
    return {
      ok: false,
      proxy,
      proxy_mode: proxyMode,
      egress_ip: '',
      google_oauth_ok: false,
      reporting_api_ok: false,
      message: egressIp ? `代理连通失败（${proxy}）` : `代理已连接，但未获取到出口IP（${proxy}）`,
    };
  }

  const oauthStartedAt = nowSql();
  const oauthResult = await oauthProbe(normalizedProxy);
  const oauthFinishedAt = nowSql();
  insertReportingRequestLog({
    owner_id: ownerId,
    request_kind: 'token_refresh',
    request_url: 'https://oauth2.googleapis.com/tokeninfo?access_token=invalid',
    proxy_url_snapshot: normalizedProxy || null,
    status_code: oauthResult.status_code,
    success: oauthResult.ok,
    started_at: oauthStartedAt,
    finished_at: oauthFinishedAt,
    response_meta_json: JSON.stringify(oauthResult.payload || {}),
    error_message: oauthResult.ok ? null : 'google_oauth_probe_failed',
  });

  const reportingStartedAt = nowSql();
  const reportingResult = await reportingProbe(normalizedProxy);
  const reportingFinishedAt = nowSql();
  insertReportingRequestLog({
    owner_id: ownerId,
    request_kind: 'reporting_reports_list',
    request_url: 'https://youtubereporting.googleapis.com/$discovery/rest?version=v1',
    proxy_url_snapshot: normalizedProxy || null,
    status_code: reportingResult.status_code,
    success: reportingResult.ok,
    started_at: reportingStartedAt,
    finished_at: reportingFinishedAt,
    response_meta_json: JSON.stringify(reportingResult.payload || {}),
    error_message: reportingResult.ok ? null : 'reporting_api_probe_failed',
  });

  const googleOauthOk = oauthResult.ok;
  const reportingApiOk = reportingResult.ok;
  return {
    ok: googleOauthOk && reportingApiOk,
    proxy,
    proxy_mode: proxyMode,
    egress_ip: egressIp,
    google_oauth_ok: googleOauthOk,
    reporting_api_ok: reportingApiOk,
    message: googleOauthOk && reportingApiOk
      ? `代理连通成功（${proxy}），出口IP ${egressIp}`
      : `代理连通成功，但 Google 接口探测未完全通过（${proxy}）`,
  };
}
