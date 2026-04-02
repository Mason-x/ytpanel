import { v4 as uuidv4 } from 'uuid';
import { getDb } from '../db.js';

const REPORTING_OWNER_MASKED_FIELD_NAMES = new Set([
  'client_secret',
  'refresh_token',
]);

export const REPORTING_OWNER_SECRET_MASK_PREFIX = '__YT_REPORTING_OWNER_MASKED__:';

export type ReportingOwnerSecretFieldName = 'client_secret' | 'refresh_token';

export type ReportingOwnerRow = {
  owner_id: string;
  name: string;
  client_id: string;
  client_secret?: string | null;
  refresh_token?: string | null;
  proxy_url?: string | null;
  enabled?: number | boolean | null;
  reporting_enabled?: number | boolean | null;
  started_at?: string | null;
  last_token_refresh_at?: string | null;
  last_sync_at?: string | null;
  last_error?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ReportingOwnerBindingRow = {
  id: string;
  owner_id: string;
  channel_id: string;
  enabled?: number | boolean | null;
  reporting_enabled?: number | boolean | null;
  started_at: string;
  created_at?: string | null;
  updated_at?: string | null;
};

type CreateReportingOwnerInput = {
  name: string;
  client_id: string;
  client_secret: string;
  refresh_token: string;
  proxy_url?: string | null;
  enabled?: boolean;
  reporting_enabled?: boolean;
  started_at?: string | null;
};

type UpdateReportingOwnerInput = Partial<CreateReportingOwnerInput>;

type CreateReportingBindingInput = {
  channel_id: string;
  enabled?: boolean;
  reporting_enabled?: boolean;
  started_at: string;
};

type UpdateReportingBindingInput = Partial<CreateReportingBindingInput>;

function toBooleanFlag(value: unknown, fallback = true): number {
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (typeof value === 'number') return value ? 1 : 0;
  if (typeof value === 'string') {
    const lowered = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(lowered)) return 1;
    if (['0', 'false', 'no', 'off'].includes(lowered)) return 0;
  }
  return fallback ? 1 : 0;
}

function normalizeRequiredText(value: unknown, fieldName: string): string {
  const normalized = String(value || '').trim();
  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }
  return normalized;
}

export function normalizeReportingOwnerProxyUrl(value: unknown): string {
  const text = String(value || '').trim();
  if (!text) return '';
  let parsed: URL;
  try {
    parsed = new URL(text);
  } catch {
    throw new Error('proxy_url is invalid');
  }
  const protocol = parsed.protocol.toLowerCase();
  if (!['http:', 'https:', 'socks:', 'socks4:', 'socks5:'].includes(protocol)) {
    throw new Error('proxy_url is invalid');
  }
  if (protocol === 'socks5:') {
    return `socks5://${parsed.username ? `${parsed.username}${parsed.password ? `:${parsed.password}` : ''}@` : ''}${parsed.host}`;
  }
  if (protocol === 'socks:' || protocol === 'socks4:') {
    return `${protocol}//${parsed.username ? `${parsed.username}${parsed.password ? `:${parsed.password}` : ''}@` : ''}${parsed.host}`;
  }
  return parsed.toString();
}

function normalizeDateOnly(value: unknown, fieldName: string, required = false): string | null {
  const text = String(value || '').trim();
  if (!text) {
    if (required) throw new Error(`${fieldName} is required`);
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw new Error(`${fieldName} is invalid`);
  }
  const parsed = new Date(`${text}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`${fieldName} is invalid`);
  }
  return text;
}

export function createMaskedReportingOwnerSecretPlaceholder(
  ownerId: string,
  fieldName: ReportingOwnerSecretFieldName,
): string {
  const normalizedOwnerId = String(ownerId || '').trim();
  const normalizedFieldName = String(fieldName || '').trim();
  if (!normalizedOwnerId || !REPORTING_OWNER_MASKED_FIELD_NAMES.has(normalizedFieldName)) return '';
  return `${REPORTING_OWNER_SECRET_MASK_PREFIX}${normalizedOwnerId}:${normalizedFieldName}`;
}

function parseMaskedReportingOwnerSecretPlaceholder(
  value: unknown,
): { ownerId: string; fieldName: ReportingOwnerSecretFieldName } | null {
  const text = String(value || '').trim();
  if (!text.startsWith(REPORTING_OWNER_SECRET_MASK_PREFIX)) return null;
  const body = text.slice(REPORTING_OWNER_SECRET_MASK_PREFIX.length);
  const splitIndex = body.lastIndexOf(':');
  if (splitIndex <= 0) return null;
  const ownerId = body.slice(0, splitIndex).trim();
  const fieldName = body.slice(splitIndex + 1).trim();
  if (!ownerId || !REPORTING_OWNER_MASKED_FIELD_NAMES.has(fieldName)) return null;
  return {
    ownerId,
    fieldName: fieldName as ReportingOwnerSecretFieldName,
  };
}

function mapOwnerRow(row: ReportingOwnerRow): ReportingOwnerRow {
  return {
    ...row,
    owner_id: String(row.owner_id || '').trim(),
    name: String(row.name || '').trim(),
    client_id: String(row.client_id || '').trim(),
    client_secret: String(row.client_secret || '').trim(),
    refresh_token: String(row.refresh_token || '').trim(),
    proxy_url: String(row.proxy_url || '').trim(),
    enabled: toBooleanFlag(row.enabled, true),
    reporting_enabled: toBooleanFlag(row.reporting_enabled, true),
    started_at: row.started_at ? String(row.started_at).trim() : null,
    last_token_refresh_at: row.last_token_refresh_at ? String(row.last_token_refresh_at).trim() : null,
    last_sync_at: row.last_sync_at ? String(row.last_sync_at).trim() : null,
    last_error: row.last_error ? String(row.last_error).trim() : null,
    created_at: row.created_at ? String(row.created_at).trim() : null,
    updated_at: row.updated_at ? String(row.updated_at).trim() : null,
  };
}

function mapBindingRow(row: ReportingOwnerBindingRow): ReportingOwnerBindingRow {
  return {
    ...row,
    id: String(row.id || '').trim(),
    owner_id: String(row.owner_id || '').trim(),
    channel_id: String(row.channel_id || '').trim(),
    enabled: toBooleanFlag(row.enabled, true),
    reporting_enabled: toBooleanFlag(row.reporting_enabled, true),
    started_at: String(row.started_at || '').trim(),
    created_at: row.created_at ? String(row.created_at).trim() : null,
    updated_at: row.updated_at ? String(row.updated_at).trim() : null,
  };
}

function getReportingOwnerRow(ownerId: string): ReportingOwnerRow | null {
  const row = getDb().prepare(`
    SELECT owner_id, name, client_id, client_secret, refresh_token, proxy_url,
      enabled, reporting_enabled, started_at, last_token_refresh_at, last_sync_at,
      last_error, created_at, updated_at
    FROM reporting_owners
    WHERE owner_id = ?
  `).get(ownerId) as ReportingOwnerRow | undefined;
  return row ? mapOwnerRow(row) : null;
}

function getReportingBindingRow(bindingId: string): ReportingOwnerBindingRow | null {
  const row = getDb().prepare(`
    SELECT id, owner_id, channel_id, enabled, reporting_enabled, started_at, created_at, updated_at
    FROM reporting_owner_channel_bindings
    WHERE id = ?
  `).get(bindingId) as ReportingOwnerBindingRow | undefined;
  return row ? mapBindingRow(row) : null;
}

function requireExistingChannel(channelId: string): void {
  const row = getDb().prepare('SELECT channel_id FROM channels WHERE channel_id = ?').get(channelId) as { channel_id?: string } | undefined;
  if (!String(row?.channel_id || '').trim()) {
    throw new Error('channel_id not found');
  }
}

function resolveOwnerSecretValue(
  ownerId: string,
  fieldName: ReportingOwnerSecretFieldName,
  nextValue: unknown,
  currentValue: string,
): string {
  const text = String(nextValue ?? '').trim();
  const masked = parseMaskedReportingOwnerSecretPlaceholder(text);
  if (masked && masked.ownerId === ownerId && masked.fieldName === fieldName) {
    return currentValue;
  }
  const normalized = String(nextValue ?? '').trim();
  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }
  return normalized;
}

export function sanitizeReportingOwnerForClient(owner: ReportingOwnerRow): ReportingOwnerRow {
  const next = mapOwnerRow(owner);
  next.client_secret = next.client_secret
    ? createMaskedReportingOwnerSecretPlaceholder(next.owner_id, 'client_secret')
    : '';
  next.refresh_token = next.refresh_token
    ? createMaskedReportingOwnerSecretPlaceholder(next.owner_id, 'refresh_token')
    : '';
  return next;
}

export function listReportingOwners(): ReportingOwnerRow[] {
  const rows = getDb().prepare(`
    SELECT owner_id, name, client_id, client_secret, refresh_token, proxy_url,
      enabled, reporting_enabled, started_at, last_token_refresh_at, last_sync_at,
      last_error, created_at, updated_at
    FROM reporting_owners
    ORDER BY datetime(created_at) DESC, owner_id DESC
  `).all() as ReportingOwnerRow[];
  return rows.map((row) => sanitizeReportingOwnerForClient(row));
}

export function createReportingOwner(input: CreateReportingOwnerInput): ReportingOwnerRow {
  const ownerId = uuidv4();
  const name = normalizeRequiredText(input.name, 'name');
  const clientId = normalizeRequiredText(input.client_id, 'client_id');
  const clientSecret = normalizeRequiredText(input.client_secret, 'client_secret');
  const refreshToken = normalizeRequiredText(input.refresh_token, 'refresh_token');
  const proxyUrl = normalizeReportingOwnerProxyUrl(input.proxy_url);
  const startedAt = normalizeDateOnly(input.started_at, 'started_at');
  const enabled = toBooleanFlag(input.enabled, true);
  const reportingEnabled = toBooleanFlag(input.reporting_enabled, true);

  getDb().prepare(`
    INSERT INTO reporting_owners (
      owner_id, name, client_id, client_secret, refresh_token, proxy_url,
      enabled, reporting_enabled, started_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    ownerId,
    name,
    clientId,
    clientSecret,
    refreshToken,
    proxyUrl || null,
    enabled,
    reportingEnabled,
    startedAt,
  );

  const row = getReportingOwnerRow(ownerId);
  if (!row) throw new Error('failed to create reporting owner');
  return sanitizeReportingOwnerForClient(row);
}

export function updateReportingOwner(ownerId: string, input: UpdateReportingOwnerInput): ReportingOwnerRow {
  const existing = getReportingOwnerRow(ownerId);
  if (!existing) {
    throw new Error('reporting owner not found');
  }

  const name = Object.prototype.hasOwnProperty.call(input, 'name')
    ? normalizeRequiredText(input.name, 'name')
    : existing.name;
  const clientId = Object.prototype.hasOwnProperty.call(input, 'client_id')
    ? normalizeRequiredText(input.client_id, 'client_id')
    : existing.client_id;
  const clientSecret = Object.prototype.hasOwnProperty.call(input, 'client_secret')
    ? resolveOwnerSecretValue(ownerId, 'client_secret', input.client_secret, String(existing.client_secret || ''))
    : String(existing.client_secret || '');
  const refreshToken = Object.prototype.hasOwnProperty.call(input, 'refresh_token')
    ? resolveOwnerSecretValue(ownerId, 'refresh_token', input.refresh_token, String(existing.refresh_token || ''))
    : String(existing.refresh_token || '');
  const proxyUrl = Object.prototype.hasOwnProperty.call(input, 'proxy_url')
    ? normalizeReportingOwnerProxyUrl(input.proxy_url)
    : String(existing.proxy_url || '');
  const startedAt = Object.prototype.hasOwnProperty.call(input, 'started_at')
    ? normalizeDateOnly(input.started_at, 'started_at')
    : (existing.started_at || null);
  const enabled = Object.prototype.hasOwnProperty.call(input, 'enabled')
    ? toBooleanFlag(input.enabled, true)
    : toBooleanFlag(existing.enabled, true);
  const reportingEnabled = Object.prototype.hasOwnProperty.call(input, 'reporting_enabled')
    ? toBooleanFlag(input.reporting_enabled, true)
    : toBooleanFlag(existing.reporting_enabled, true);

  getDb().prepare(`
    UPDATE reporting_owners
    SET name = ?, client_id = ?, client_secret = ?, refresh_token = ?, proxy_url = ?,
        enabled = ?, reporting_enabled = ?, started_at = ?, updated_at = datetime('now')
    WHERE owner_id = ?
  `).run(
    name,
    clientId,
    clientSecret,
    refreshToken,
    proxyUrl || null,
    enabled,
    reportingEnabled,
    startedAt,
    ownerId,
  );

  const row = getReportingOwnerRow(ownerId);
  if (!row) throw new Error('reporting owner not found');
  return sanitizeReportingOwnerForClient(row);
}

export function deleteReportingOwner(ownerId: string): void {
  const result = getDb().prepare('DELETE FROM reporting_owners WHERE owner_id = ?').run(ownerId);
  if (!result.changes) {
    throw new Error('reporting owner not found');
  }
}

export function listReportingBindings(ownerId: string): ReportingOwnerBindingRow[] {
  return (getDb().prepare(`
    SELECT id, owner_id, channel_id, enabled, reporting_enabled, started_at, created_at, updated_at
    FROM reporting_owner_channel_bindings
    WHERE owner_id = ?
    ORDER BY datetime(created_at) DESC, id DESC
  `).all(ownerId) as ReportingOwnerBindingRow[]).map((row) => mapBindingRow(row));
}

export function createReportingBinding(
  ownerId: string,
  input: CreateReportingBindingInput,
): ReportingOwnerBindingRow {
  const owner = getReportingOwnerRow(ownerId);
  if (!owner) {
    throw new Error('reporting owner not found');
  }

  const channelId = normalizeRequiredText(input.channel_id, 'channel_id');
  requireExistingChannel(channelId);
  const existingBinding = getDb().prepare(`
    SELECT id, owner_id
    FROM reporting_owner_channel_bindings
    WHERE channel_id = ?
  `).get(channelId) as { id?: string; owner_id?: string } | undefined;
  if (String(existingBinding?.id || '').trim()) {
    throw new Error('channel is already bound to another owner');
  }

  const bindingId = uuidv4();
  const startedAt = normalizeDateOnly(input.started_at, 'started_at', true) as string;
  const enabled = toBooleanFlag(input.enabled, true);
  const reportingEnabled = toBooleanFlag(input.reporting_enabled, true);

  getDb().prepare(`
    INSERT INTO reporting_owner_channel_bindings (
      id, owner_id, channel_id, enabled, reporting_enabled, started_at
    )
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(bindingId, owner.owner_id, channelId, enabled, reportingEnabled, startedAt);

  const row = getReportingBindingRow(bindingId);
  if (!row) throw new Error('failed to create reporting binding');
  return row;
}

export function updateReportingBinding(
  bindingId: string,
  input: UpdateReportingBindingInput,
): ReportingOwnerBindingRow {
  const existing = getReportingBindingRow(bindingId);
  if (!existing) {
    throw new Error('reporting binding not found');
  }

  const channelId = Object.prototype.hasOwnProperty.call(input, 'channel_id')
    ? normalizeRequiredText(input.channel_id, 'channel_id')
    : existing.channel_id;
  if (channelId !== existing.channel_id) {
    requireExistingChannel(channelId);
    const boundRow = getDb().prepare(`
      SELECT id
      FROM reporting_owner_channel_bindings
      WHERE channel_id = ? AND id <> ?
    `).get(channelId, bindingId) as { id?: string } | undefined;
    if (String(boundRow?.id || '').trim()) {
      throw new Error('channel is already bound to another owner');
    }
  }

  const startedAt = Object.prototype.hasOwnProperty.call(input, 'started_at')
    ? normalizeDateOnly(input.started_at, 'started_at', true)
    : existing.started_at;
  const enabled = Object.prototype.hasOwnProperty.call(input, 'enabled')
    ? toBooleanFlag(input.enabled, true)
    : toBooleanFlag(existing.enabled, true);
  const reportingEnabled = Object.prototype.hasOwnProperty.call(input, 'reporting_enabled')
    ? toBooleanFlag(input.reporting_enabled, true)
    : toBooleanFlag(existing.reporting_enabled, true);

  getDb().prepare(`
    UPDATE reporting_owner_channel_bindings
    SET channel_id = ?, enabled = ?, reporting_enabled = ?, started_at = ?, updated_at = datetime('now')
    WHERE id = ?
  `).run(channelId, enabled, reportingEnabled, startedAt, bindingId);

  const row = getReportingBindingRow(bindingId);
  if (!row) throw new Error('reporting binding not found');
  return row;
}

export function deleteReportingBinding(bindingId: string): void {
  const result = getDb().prepare('DELETE FROM reporting_owner_channel_bindings WHERE id = ?').run(bindingId);
  if (!result.changes) {
    throw new Error('reporting binding not found');
  }
}
