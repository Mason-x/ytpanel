import test from 'node:test';
import assert from 'node:assert/strict';
import {
  REPORTING_OWNER_SECRET_MASK_PREFIX,
  createMaskedReportingOwnerSecretPlaceholder,
  sanitizeReportingOwnerForClient,
} from '../reportingOwners.js';

test('reporting owner rows expose masked placeholders for sensitive credentials', () => {
  const owner = sanitizeReportingOwnerForClient({
    owner_id: 'owner-1',
    name: 'Owner One',
    client_id: 'client-id-1',
    client_secret: 'client-secret-1',
    refresh_token: 'refresh-token-1',
    proxy_url: 'http://127.0.0.1:8080',
    enabled: 1,
    reporting_enabled: 1,
    started_at: '2026-04-02',
    last_token_refresh_at: null,
    last_sync_at: null,
    last_error: null,
    created_at: '2026-04-02 00:00:00',
    updated_at: '2026-04-02 00:00:00',
  });

  assert.equal(owner.owner_id, 'owner-1');
  assert.equal(owner.client_id, 'client-id-1');
  assert.match(String(owner.client_secret || ''), new RegExp(`^${REPORTING_OWNER_SECRET_MASK_PREFIX}`));
  assert.match(String(owner.refresh_token || ''), new RegExp(`^${REPORTING_OWNER_SECRET_MASK_PREFIX}`));
  assert.equal(
    owner.client_secret,
    createMaskedReportingOwnerSecretPlaceholder('owner-1', 'client_secret'),
  );
  assert.equal(
    owner.refresh_token,
    createMaskedReportingOwnerSecretPlaceholder('owner-1', 'refresh_token'),
  );
  assert.equal(owner.started_at, '2026-04-02');
  assert.equal(owner.reporting_enabled, 1);
});
