import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { initDb, getDb } from '../../db.js';
import {
  getProxyMode,
  insertReportingRequestLog,
  probeReportingProxy,
} from '../reportingProxyProbe.js';

function createTempDbPath(): string {
  return path.join(
    os.tmpdir(),
    `ytpanel-reporting-probe-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.db`,
  );
}

function initTestDb() {
  const dbPath = createTempDbPath();
  const db = initDb(dbPath);
  db.prepare(`
    INSERT INTO channels (channel_id, platform, title, tags_json, favorite, priority)
    VALUES ('UC_TEST_CHANNEL', 'youtube', 'Test Channel', '[]', 0, 'normal')
  `).run();
  db.prepare(`
    INSERT INTO reporting_owners (
      owner_id, name, client_id, client_secret, refresh_token, proxy_url, enabled, reporting_enabled, started_at
    )
    VALUES ('owner-1', 'Owner One', 'client-id-1', 'client-secret-1', 'refresh-token-1', NULL, 1, 1, '2026-04-02')
  `).run();
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

test('getProxyMode resolves direct http https and socks variants', () => {
  assert.equal(getProxyMode(''), 'direct');
  assert.equal(getProxyMode('http://127.0.0.1:8080'), 'http');
  assert.equal(getProxyMode('https://127.0.0.1:8443'), 'https');
  assert.equal(getProxyMode('socks5://127.0.0.1:1080'), 'socks5');
  assert.equal(getProxyMode('socks://127.0.0.1:1080'), 'socks5');
});

test('insertReportingRequestLog stores a reusable request log row', () => {
  const fixture = initTestDb();
  try {
    const logId = insertReportingRequestLog({
      owner_id: 'owner-1',
      channel_id: 'UC_TEST_CHANNEL',
      request_kind: 'proxy_probe',
      request_url: 'https://api.ipify.org?format=json',
      proxy_url_snapshot: 'http://127.0.0.1:8080',
      status_code: 200,
      success: true,
      error_code: null,
      error_message: null,
      started_at: '2026-04-02 00:00:00',
      finished_at: '2026-04-02 00:00:01',
      duration_ms: 1000,
      response_meta_json: '{"ip":"1.2.3.4"}',
    });

    const row = getDb().prepare(`
      SELECT request_kind, status_code, success, response_meta_json
      FROM reporting_request_logs
      WHERE id = ?
    `).get(logId) as any;

    assert.equal(row.request_kind, 'proxy_probe');
    assert.equal(row.status_code, 200);
    assert.equal(row.success, 1);
    assert.equal(row.response_meta_json, '{"ip":"1.2.3.4"}');
  } finally {
    fixture.cleanup();
  }
});

test('probeReportingProxy returns egress ip and probe booleans with stubbed checks', async () => {
  const fixture = initTestDb();
  try {
    const result = await probeReportingProxy(
      {
        owner_id: 'owner-1',
        proxy_url: 'socks5://127.0.0.1:1080',
      },
      {
        ipProbe: async () => ({
          ok: true,
          status_code: 200,
          payload: { ip: '1.2.3.4' },
        }),
        oauthProbe: async () => ({
          ok: true,
          status_code: 200,
          payload: { ok: true },
        }),
        reportingProbe: async () => ({
          ok: true,
          status_code: 200,
          payload: { kind: 'youtubeReporting#reportTypeList' },
        }),
      },
    );

    assert.equal(result.ok, true);
    assert.equal(result.proxy_mode, 'socks5');
    assert.equal(result.egress_ip, '1.2.3.4');
    assert.equal(result.google_oauth_ok, true);
    assert.equal(result.reporting_api_ok, true);

    const rows = getDb().prepare(`
      SELECT request_kind, status_code
      FROM reporting_request_logs
      WHERE owner_id = 'owner-1'
      ORDER BY id ASC
    `).all() as Array<{ request_kind: string; status_code: number }>;

    assert.deepEqual(
      rows.map((row) => row.request_kind),
      ['proxy_probe', 'oauth_probe', 'reporting_api_probe'],
    );
    assert.deepEqual(
      rows.map((row) => row.status_code),
      [200, 200, 200],
    );
  } finally {
    fixture.cleanup();
  }
});
