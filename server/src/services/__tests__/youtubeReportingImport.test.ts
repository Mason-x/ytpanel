import test from 'node:test';
import assert from 'node:assert/strict';
import { parseYoutubeReportingCsv } from '../youtubeReportingImport.js';

test('parseYoutubeReportingCsv reads reach report rows', () => {
  const rows = parseYoutubeReportingCsv({
    reportTypeId: 'channel_reach_basic_a1',
    csvText: [
      'date,channel_id,video_id,video_thumbnail_impressions,video_thumbnail_impressions_ctr',
      '20260402,UC1,vid-1,100,0.12',
      '20260402,UC1,vid-2,50,0.05',
    ].join('\n'),
  });

  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0], {
    date: '2026-04-02',
    video_id: 'vid-1',
    impressions: 100,
    impressions_ctr: 0.12,
  });
});

test('parseYoutubeReportingCsv reads basic report rows', () => {
  const rows = parseYoutubeReportingCsv({
    reportTypeId: 'channel_basic_a3',
    csvText: [
      'date,channel_id,video_id,average_view_duration_seconds,average_view_duration_percentage',
      '20260402,UC1,vid-1,35.5,0.42',
    ].join('\n'),
  });

  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    date: '2026-04-02',
    video_id: 'vid-1',
    avg_view_duration_seconds: 35.5,
    avg_view_percentage: 0.42,
  });
});

test('parseYoutubeReportingCsv reads traffic source rows', () => {
  const rows = parseYoutubeReportingCsv({
    reportTypeId: 'channel_traffic_source_a3',
    csvText: [
      'date,channel_id,video_id,traffic_source_type,views',
      '20260402,UC1,vid-1,YT_SEARCH,70',
      '20260402,UC1,vid-1,SUGGESTED_VIDEO,30',
    ].join('\n'),
  });

  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0], {
    date: '2026-04-02',
    video_id: 'vid-1',
    traffic_source_type: 'YT_SEARCH',
    views: 70,
  });
});
