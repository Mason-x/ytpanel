const REPORTING_OWNER_MASKED_FIELD_NAMES = new Set([
  'client_secret',
  'refresh_token',
]);

export const REPORTING_OWNER_SECRET_MASK_PREFIX = '__YT_REPORTING_OWNER_MASKED__:';

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

export function createMaskedReportingOwnerSecretPlaceholder(
  ownerId: string,
  fieldName: 'client_secret' | 'refresh_token',
): string {
  const normalizedOwnerId = String(ownerId || '').trim();
  const normalizedFieldName = String(fieldName || '').trim();
  if (!normalizedOwnerId || !REPORTING_OWNER_MASKED_FIELD_NAMES.has(normalizedFieldName)) return '';
  return `${REPORTING_OWNER_SECRET_MASK_PREFIX}${normalizedOwnerId}:${normalizedFieldName}`;
}

export function sanitizeReportingOwnerForClient(owner: ReportingOwnerRow): ReportingOwnerRow {
  const next: ReportingOwnerRow = {
    ...owner,
    owner_id: String(owner.owner_id || '').trim(),
    name: String(owner.name || '').trim(),
    client_id: String(owner.client_id || '').trim(),
  };

  if (String(owner.client_secret || '').trim()) {
    next.client_secret = createMaskedReportingOwnerSecretPlaceholder(next.owner_id, 'client_secret');
  } else {
    next.client_secret = '';
  }

  if (String(owner.refresh_token || '').trim()) {
    next.refresh_token = createMaskedReportingOwnerSecretPlaceholder(next.owner_id, 'refresh_token');
  } else {
    next.refresh_token = '';
  }

  return next;
}
