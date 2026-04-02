import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import ReportingPanel from '../ReportingPanel'

test('reporting panel renders disabled empty state', () => {
  const html = renderToStaticMarkup(
    <ReportingPanel
      enabled={false}
      ownerName={null}
      startedAt={null}
      latestImportedAt={null}
      summary={null}
      dailyRows={[]}
      videos={[]}
      loading={false}
      onSync={() => {}}
    />,
  )

  assert.match(html, /尚未启用 Reporting API/)
})

test('reporting panel renders reporting kpis and traffic source section', () => {
  const html = renderToStaticMarkup(
    <ReportingPanel
      enabled
      ownerName="Owner One"
      startedAt="2026-04-02"
      latestImportedAt="2026-04-03 10:00:00"
      summary={{
        enabled: true,
        owner_id: 'owner-1',
        owner_name: 'Owner One',
        started_at: '2026-04-02',
        latest_imported_at: '2026-04-03 10:00:00',
        latest_date: '2026-04-03',
        impressions: 1000,
        impressions_ctr: 0.12,
        avg_view_duration_seconds: 45,
        avg_view_percentage: 0.5,
        traffic_source_share_json: JSON.stringify({ YT_SEARCH: 0.6, SUGGESTED_VIDEO: 0.4 }),
      }}
      dailyRows={[{
        date: '2026-04-03',
        impressions: 1000,
        impressions_ctr: 0.12,
        avg_view_duration_seconds: 45,
        avg_view_percentage: 0.5,
        traffic_source_share_json: JSON.stringify({ YT_SEARCH: 0.6 }),
      }]}
      videos={[{
        date: '2026-04-03',
        video_id: 'vid-1',
        channel_id: 'UC1',
        owner_id: 'owner-1',
        title: 'Video One',
        impressions: 1000,
        impressions_ctr: 0.12,
        avg_view_duration_seconds: 45,
        avg_view_percentage: 0.5,
        traffic_source_share_json: JSON.stringify({ YT_SEARCH: 0.6 }),
        computed_at: '2026-04-03 10:00:00',
      }]}
      loading={false}
      onSync={() => {}}
    />,
  )

  assert.match(html, /展现量/)
  assert.match(html, /平均观看时长/)
  assert.match(html, /流量来源占比/)
  assert.match(html, /Video One/)
})
