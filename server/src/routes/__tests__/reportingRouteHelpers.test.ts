import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { initDb, getDb } from '../../db.js';
import { createReportingBinding, createReportingOwner } from '../../services/reportingOwners.js';
import {
  buildReportingOwnerUsageSummary,
  serializeReportingOwnerResponse,
} from '../reporting.js';
import {
  enqueueDailyReportingSyncs,
  enqueueReportingSyncForBinding,
} from '../../services/youtubeReportingSync.js';

function createTempDbPath(): string {
  return path.join(
    os.tmpdir(),
    `ytpanel-reporting-sync-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.db`,
  );
}

function initTestDb() {
  const dbPath = createTempDbPath();
  const db = initDb(dbPath);
  db.prepare(`
    INSERT INTO channels (channel_id, platform, title, tags_json, favorite, priority)
    VALUES ('UC_SYNC_1', 'youtube', 'Sync Channel 1', '[]', 0, 'normal')
  `).run();
  db.prepare(`
    INSERT INTO channels (channel_id, platform, title, tags_json, favorite, priority)
    VALUES ('UC_SYNC_2', 'youtube', 'Sync Channel 2', '[]', 0, 'normal')
  `).run();
  return {
    db,
    cleanup() {
      for (const suffix of ['', '-wal', '-shm']) {
        const filePath = `${dbPath}${suffix}`;
        if (fs.existsSync(filePath)) fs.rmSync(filePath, { force: true });
      }
    },
  };
}

test('enqueueReportingSyncForBinding creates a sync_reporting_channel job', () => {
  const fixture = initTestDb();
  try {
    const owner = createReportingOwner({
      name: 'Owner One',
      client_id: 'client-id-1',
      client_secret: 'client-secret-1',
      refresh_token: 'refresh-token-1',
      started_at: '2026-04-02',
    });
    const binding = createReportingBinding(owner.owner_id, {
      channel_id: 'UC_SYNC_1',
      started_at: '2026-04-02',
      enabled: true,
      reporting_enabled: true,
    });

    const queued = enqueueReportingSyncForBinding(binding.id, 'manual');
    assert.equal(queued.status, 'queued');

    const row = getDb().prepare(`
      SELECT type, status, payload_json
      FROM jobs
      WHERE job_id = ?
    `).get(queued.job_id) as any;

    assert.equal(row.type, 'sync_reporting_channel');
    assert.match(String(row.status || ''), /^(queued|running|done|failed)$/);
    assert.match(String(row.payload_json || ''), /"binding_id":/);
  } finally {
    fixture.cleanup();
  }
});

test('enqueueDailyReportingSyncs only queues enabled owner bindings', () => {
  const fixture = initTestDb();
  try {
    const enabledOwner = createReportingOwner({
      name: 'Owner Enabled',
      client_id: 'client-id-1',
      client_secret: 'client-secret-1',
      refresh_token: 'refresh-token-1',
      started_at: '2026-04-02',
      enabled: true,
      reporting_enabled: true,
    });
    const disabledOwner = createReportingOwner({
      name: 'Owner Disabled',
      client_id: 'client-id-2',
      client_secret: 'client-secret-2',
      refresh_token: 'refresh-token-2',
      started_at: '2026-04-02',
      enabled: false,
      reporting_enabled: false,
    });

    createReportingBinding(enabledOwner.owner_id, {
      channel_id: 'UC_SYNC_1',
      started_at: '2026-04-02',
      enabled: true,
      reporting_enabled: true,
    });
    createReportingBinding(disabledOwner.owner_id, {
      channel_id: 'UC_SYNC_2',
      started_at: '2026-04-02',
      enabled: true,
      reporting_enabled: true,
    });

    const result = enqueueDailyReportingSyncs('cron');
    assert.equal(result.queued_count, 1);

    const rows = getDb().prepare(`
      SELECT type, payload_json
      FROM jobs
      WHERE type = 'sync_reporting_channel'
      ORDER BY created_at ASC
    `).all() as any[];

    assert.equal(rows.length, 1);
    assert.match(String(rows[0]?.payload_json || ''), /UC_SYNC_1/);
  } finally {
    fixture.cleanup();
  }
});

test('serializeReportingOwnerResponse includes bindings and usage payloads', () => {
  const response = serializeReportingOwnerResponse(
    {
      owner_id: 'owner-1',
      name: 'Owner One',
      client_id: 'client-id-1',
      client_secret: '__YT_REPORTING_OWNER_MASKED__:owner-1:client_secret',
      refresh_token: '__YT_REPORTING_OWNER_MASKED__:owner-1:refresh_token',
      proxy_url: 'http://127.0.0.1:8080/',
      enabled: 1,
      reporting_enabled: 1,
      started_at: '2026-04-02',
      last_token_refresh_at: null,
      last_sync_at: null,
      last_error: null,
      created_at: '2026-04-02 00:00:00',
      updated_at: '2026-04-02 00:00:00',
    },
    [
      {
        id: 'binding-1',
        owner_id: 'owner-1',
        channel_id: 'UC_SYNC_1',
        enabled: 1,
        reporting_enabled: 1,
        started_at: '2026-04-02',
        created_at: '2026-04-02 00:00:00',
        updated_at: '2026-04-02 00:00:00',
      },
    ],
    {
      owner_id: 'owner-1',
      request_count_24h: 4,
      success_count_24h: 3,
      failure_count_24h: 1,
      success_rate_24h: 0.75,
      download_count_24h: 2,
      last_token_refresh_at: null,
      last_sync_at: null,
      last_error: null,
      avg_duration_ms_24h: 200,
    },
  );

  assert.equal(response.owner_id, 'owner-1');
  assert.equal(response.bindings.length, 1);
  assert.equal(response.usage?.request_count_24h, 4);
});

test('buildReportingOwnerUsageSummary computes success rate from log counts', () => {
  const usage = buildReportingOwnerUsageSummary({
    owner_id: 'owner-1',
    request_count_24h: 5,
    success_count_24h: 4,
    failure_count_24h: 1,
    download_count_24h: 2,
    avg_duration_ms_24h: 180,
    last_token_refresh_at: null,
    last_sync_at: null,
    last_error: null,
  });

  assert.equal(usage.owner_id, 'owner-1');
  assert.equal(usage.success_rate_24h, 0.8);
  assert.equal(usage.download_count_24h, 2);
});
