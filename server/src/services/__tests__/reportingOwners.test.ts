import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { initDb } from '../../db.js';
import {
  REPORTING_OWNER_SECRET_MASK_PREFIX,
  createReportingBinding,
  createReportingOwner,
  createMaskedReportingOwnerSecretPlaceholder,
  listReportingBindings,
  listReportingOwners,
  updateReportingOwner,
  sanitizeReportingOwnerForClient,
} from '../reportingOwners.js';

function createTempDbPath(): string {
  return path.join(
    os.tmpdir(),
    `ytpanel-reporting-owners-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.db`,
  );
}

function initTestDb() {
  const dbPath = createTempDbPath();
  const db = initDb(dbPath);
  db.prepare(`
    INSERT INTO channels (channel_id, platform, title, tags_json, favorite, priority)
    VALUES (?, 'youtube', ?, '[]', 0, 'normal')
  `).run('UC_TEST_CHANNEL_1', 'Test Channel 1');
  db.prepare(`
    INSERT INTO channels (channel_id, platform, title, tags_json, favorite, priority)
    VALUES (?, 'youtube', ?, '[]', 0, 'normal')
  `).run('UC_TEST_CHANNEL_2', 'Test Channel 2');
  return {
    dbPath,
    cleanup() {
      for (const suffix of ['', '-wal', '-shm']) {
        const filePath = `${dbPath}${suffix}`;
        if (fs.existsSync(filePath)) fs.rmSync(filePath, { force: true });
      }
    },
  };
}

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

test('createReportingOwner persists owner and listReportingOwners returns sanitized values', () => {
  const fixture = initTestDb();
  try {
    const created = createReportingOwner({
      name: 'Owner One',
      client_id: 'client-id-1',
      client_secret: 'client-secret-1',
      refresh_token: 'refresh-token-1',
      proxy_url: 'http://127.0.0.1:8080',
      enabled: true,
      reporting_enabled: true,
      started_at: '2026-04-02',
    });

    assert.equal(created.name, 'Owner One');
    assert.equal(created.client_id, 'client-id-1');
    assert.equal(created.proxy_url, 'http://127.0.0.1:8080/');

    const owners = listReportingOwners();
    assert.equal(owners.length, 1);
    assert.equal(owners[0]?.owner_id, created.owner_id);
    assert.equal(
      owners[0]?.client_secret,
      createMaskedReportingOwnerSecretPlaceholder(created.owner_id, 'client_secret'),
    );
    assert.equal(
      owners[0]?.refresh_token,
      createMaskedReportingOwnerSecretPlaceholder(created.owner_id, 'refresh_token'),
    );
  } finally {
    fixture.cleanup();
  }
});

test('updateReportingOwner keeps masked secrets when patch leaves them untouched', () => {
  const fixture = initTestDb();
  try {
    const created = createReportingOwner({
      name: 'Owner One',
      client_id: 'client-id-1',
      client_secret: 'client-secret-1',
      refresh_token: 'refresh-token-1',
      proxy_url: '',
      enabled: true,
      reporting_enabled: true,
      started_at: '2026-04-02',
    });

    const updated = updateReportingOwner(created.owner_id, {
      name: 'Owner One Updated',
      client_id: 'client-id-2',
      client_secret: createMaskedReportingOwnerSecretPlaceholder(created.owner_id, 'client_secret'),
      refresh_token: createMaskedReportingOwnerSecretPlaceholder(created.owner_id, 'refresh_token'),
      proxy_url: 'socks5://127.0.0.1:1080',
    });

    assert.equal(updated.name, 'Owner One Updated');
    assert.equal(updated.client_id, 'client-id-2');
    assert.equal(updated.proxy_url, 'socks5://127.0.0.1:1080');

    const owners = listReportingOwners();
    assert.equal(owners[0]?.owner_id, created.owner_id);
    assert.equal(
      owners[0]?.client_secret,
      createMaskedReportingOwnerSecretPlaceholder(created.owner_id, 'client_secret'),
    );
    assert.equal(
      owners[0]?.refresh_token,
      createMaskedReportingOwnerSecretPlaceholder(created.owner_id, 'refresh_token'),
    );
  } finally {
    fixture.cleanup();
  }
});

test('createReportingBinding rejects a second owner for the same channel', () => {
  const fixture = initTestDb();
  try {
    const ownerOne = createReportingOwner({
      name: 'Owner One',
      client_id: 'client-id-1',
      client_secret: 'client-secret-1',
      refresh_token: 'refresh-token-1',
      started_at: '2026-04-02',
    });
    const ownerTwo = createReportingOwner({
      name: 'Owner Two',
      client_id: 'client-id-2',
      client_secret: 'client-secret-2',
      refresh_token: 'refresh-token-2',
      started_at: '2026-04-02',
    });

    const binding = createReportingBinding(ownerOne.owner_id, {
      channel_id: 'UC_TEST_CHANNEL_1',
      started_at: '2026-04-02',
      enabled: true,
      reporting_enabled: true,
    });

    assert.equal(binding.channel_id, 'UC_TEST_CHANNEL_1');
    assert.equal(listReportingBindings(ownerOne.owner_id).length, 1);

    assert.throws(() => {
      createReportingBinding(ownerTwo.owner_id, {
        channel_id: 'UC_TEST_CHANNEL_1',
        started_at: '2026-04-02',
        enabled: true,
        reporting_enabled: true,
      });
    }, /already bound/i);
  } finally {
    fixture.cleanup();
  }
});
