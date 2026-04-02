import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import ReportingOwnersPanel from '../ReportingOwnersPanel'
import type { ApiChannel, ReportingOwner } from '../../../types'

const channels: ApiChannel[] = [
  {
    channel_id: 'UC1',
    title: 'Channel One',
  },
]

test('reporting owners panel renders empty state', () => {
  const html = renderToStaticMarkup(
    <ReportingOwnersPanel
      owners={[]}
      channels={channels}
      selectedOwnerId=""
      ownerLogs={[]}
      loading={false}
      saving={false}
      onSelectOwner={() => {}}
      onCreateOwner={() => {}}
      onUpdateOwner={() => {}}
      onDeleteOwner={() => {}}
      onProbeOwner={() => {}}
      onCreateBinding={() => {}}
      onUpdateBinding={() => {}}
      onDeleteBinding={() => {}}
      onSyncBinding={() => {}}
      onLoadOwnerLogs={() => {}}
    />,
  )

  assert.match(html, /Reporting Owners/)
  assert.match(html, /尚未配置 Owner/)
})

test('reporting owners panel renders owner summary cards', () => {
  const owners: Array<ReportingOwner & { bindings?: any[]; usage?: any }> = [
    {
      owner_id: 'owner-1',
      name: 'Owner One',
      client_id: 'client-id-1',
      client_secret: '__YT_REPORTING_OWNER_MASKED__:owner-1:client_secret',
      refresh_token: '__YT_REPORTING_OWNER_MASKED__:owner-1:refresh_token',
      proxy_url: 'http://127.0.0.1:8080/',
      enabled: true,
      reporting_enabled: true,
      started_at: '2026-04-02',
      bindings: [{ id: 'binding-1', owner_id: 'owner-1', channel_id: 'UC1', enabled: true, reporting_enabled: true, started_at: '2026-04-02' }],
      usage: { owner_id: 'owner-1', request_count_24h: 4, success_count_24h: 3, failure_count_24h: 1, success_rate_24h: 0.75, download_count_24h: 2 },
    },
  ]

  const html = renderToStaticMarkup(
    <ReportingOwnersPanel
      owners={owners}
      channels={channels}
      selectedOwnerId="owner-1"
      ownerLogs={[]}
      loading={false}
      saving={false}
      onSelectOwner={() => {}}
      onCreateOwner={() => {}}
      onUpdateOwner={() => {}}
      onDeleteOwner={() => {}}
      onProbeOwner={() => {}}
      onCreateBinding={() => {}}
      onUpdateBinding={() => {}}
      onDeleteBinding={() => {}}
      onSyncBinding={() => {}}
      onLoadOwnerLogs={() => {}}
    />,
  )

  assert.match(html, /Owner One/)
  assert.match(html, /请求成功率/)
  assert.match(html, /UC1/)
})
