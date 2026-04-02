import https from 'https';
import { ProxyAgent } from 'undici';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { normalizeReportingOwnerProxyUrl, type ReportingOwnerRow } from './reportingOwners.js';

type ReportingClientRequestOptions = {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
};

type ReportingClientDependencies = {
  tokenExchange?: (owner: ReportingOwnerRow) => Promise<{
    access_token: string;
    expires_in?: number | null;
    token_type?: string | null;
  }>;
};

function isSocksProxy(proxyUrl: string): boolean {
  const lowered = String(proxyUrl || '').trim().toLowerCase();
  return lowered.startsWith('socks5://') || lowered.startsWith('socks://') || lowered.startsWith('socks4://');
}

async function requestText(
  url: string,
  proxyUrl: string,
  options: ReportingClientRequestOptions,
): Promise<{ status: number; body: string }> {
  const normalizedProxy = normalizeReportingOwnerProxyUrl(proxyUrl);

  if (isSocksProxy(normalizedProxy)) {
    const socksAgent = new SocksProxyAgent(normalizedProxy);
    return await new Promise((resolve, reject) => {
      const request = https.request(url, {
        method: options.method || 'GET',
        headers: options.headers,
        agent: socksAgent,
        timeout: 30_000,
      }, (response) => {
        const chunks: string[] = [];
        response.setEncoding('utf8');
        response.on('data', (chunk) => chunks.push(String(chunk)));
        response.on('end', () => resolve({
          status: Number(response.statusCode || 0),
          body: chunks.join(''),
        }));
      });
      request.on('error', reject);
      request.on('timeout', () => request.destroy(new Error('request_timeout')));
      if (options.body) request.write(options.body);
      request.end();
    });
  }

  let proxyAgent: ProxyAgent | null = null;
  const fetchOptions: RequestInit & { dispatcher?: ProxyAgent } = {
    method: options.method || 'GET',
    headers: options.headers,
    body: options.body,
  };
  if (normalizedProxy) {
    proxyAgent = new ProxyAgent(normalizedProxy);
    fetchOptions.dispatcher = proxyAgent;
  }
  try {
    const response = await fetch(url, fetchOptions);
    return {
      status: Number(response.status || 0),
      body: await response.text(),
    };
  } finally {
    if (proxyAgent) {
      await proxyAgent.close().catch(() => null);
    }
  }
}

export async function refreshReportingAccessToken(
  owner: ReportingOwnerRow,
  dependencies: ReportingClientDependencies = {},
): Promise<{
  access_token: string;
  expires_in?: number | null;
  token_type: string;
}> {
  if (dependencies.tokenExchange) {
    const token = await dependencies.tokenExchange(owner);
    return {
      access_token: String(token.access_token || '').trim(),
      expires_in: token.expires_in ?? null,
      token_type: String(token.token_type || 'Bearer'),
    };
  }

  const body = new URLSearchParams({
    client_id: String(owner.client_id || '').trim(),
    client_secret: String(owner.client_secret || '').trim(),
    refresh_token: String(owner.refresh_token || '').trim(),
    grant_type: 'refresh_token',
  }).toString();

  const response = await requestText(
    'https://oauth2.googleapis.com/token',
    String(owner.proxy_url || '').trim(),
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
    },
  );

  let payload: any = null;
  try {
    payload = response.body ? JSON.parse(response.body) : {};
  } catch {
    payload = {};
  }

  if (!(response.status >= 200 && response.status < 300) || !String(payload?.access_token || '').trim()) {
    throw new Error(`reporting token refresh failed (${response.status || 0})`);
  }

  return {
    access_token: String(payload.access_token || '').trim(),
    expires_in: Number.isFinite(Number(payload?.expires_in)) ? Number(payload.expires_in) : null,
    token_type: String(payload?.token_type || 'Bearer'),
  };
}

export async function reportingAuthorizedJsonRequest(
  owner: ReportingOwnerRow,
  url: string,
  options: ReportingClientRequestOptions = {},
  dependencies: ReportingClientDependencies = {},
): Promise<{ status: number; payload: any }> {
  const token = await refreshReportingAccessToken(owner, dependencies);
  const response = await requestText(
    url,
    String(owner.proxy_url || '').trim(),
    {
      ...options,
      headers: {
        ...(options.headers || {}),
        Authorization: `${token.token_type} ${token.access_token}`,
      },
    },
  );
  try {
    return {
      status: response.status,
      payload: response.body ? JSON.parse(response.body) : {},
    };
  } catch {
    return {
      status: response.status,
      payload: { raw: response.body },
    };
  }
}
