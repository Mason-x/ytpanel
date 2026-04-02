import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { initDb, getDb } from '../../db.js';
import { createReportingBinding, createReportingOwner } from '../../services/reportingOwners.js';
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
