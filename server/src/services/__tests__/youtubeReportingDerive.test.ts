import test from 'node:test';
import assert from 'node:assert/strict';
import { deriveVideoReportingDaily } from '../youtubeReportingDerive.js';

test('deriveVideoReportingDaily skips rows before started_at', () => {
  const rows = deriveVideoReportingDaily({
    ownerId: 'owner-1',
    channelId: 'UC_TEST_CHANNEL',
    startedAt: '2026-04-02',
    reachRows: [
      { date: '2026-04-01', video_id: 'vid-1', impressions: 100, impressions_ctr: 0.1 },
    ],
    basicRows: [],
    trafficRows: [],
  });

  assert.equal(rows.length, 0);
});

test('deriveVideoReportingDaily merges metrics from multiple report families', () => {
  const rows = deriveVideoReportingDaily({
    ownerId: 'owner-1',
    channelId: 'UC_TEST_CHANNEL',
    startedAt: '2026-04-02',
    reachRows: [
      { date: '2026-04-02', video_id: 'vid-1', impressions: 100, impressions_ctr: 0.1 },
    ],
    basicRows: [
      { date: '2026-04-02', video_id: 'vid-1', avg_view_duration_seconds: 45, avg_view_percentage: 0.5 },
    ],
    trafficRows: [
      { date: '2026-04-02', video_id: 'vid-1', traffic_source_type: 'YT_SEARCH', views: 60 },
      { date: '2026-04-02', video_id: 'vid-1', traffic_source_type: 'SUGGESTED_VIDEO', views: 40 },
    ],
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0]?.date, '2026-04-02');
  assert.equal(rows[0]?.video_id, 'vid-1');
  assert.equal(rows[0]?.impressions, 100);
  assert.equal(rows[0]?.impressions_ctr, 0.1);
  assert.equal(rows[0]?.avg_view_duration_seconds, 45);
  assert.equal(rows[0]?.avg_view_percentage, 0.5);
  assert.equal(
    rows[0]?.traffic_source_share_json,
    JSON.stringify({
      YT_SEARCH: 0.6,
      SUGGESTED_VIDEO: 0.4,
    }),
  );
});
