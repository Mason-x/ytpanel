import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import OwnerModal from '../OwnerModal'
import type { ApiChannel, ReportingOwner, ReportingOwnerBinding } from '../../../types'

const channels: ApiChannel[] = [
  { channel_id: 'UC1', title: 'Channel One' },
  { channel_id: 'UC2', title: 'Channel Two' },
]

const owner: ReportingOwner = {
  owner_id: 'owner-1',
  name: 'Owner One',
  client_id: 'client-id-1',
  client_secret: '__YT_REPORTING_OWNER_MASKED__:owner-1:client_secret',
  refresh_token: '__YT_REPORTING_OWNER_MASKED__:owner-1:refresh_token',
  proxy_url: 'socks5://127.0.0.1:1080',
  enabled: true,
  reporting_enabled: true,
}

const bindings: ReportingOwnerBinding[] = [
  {
    id: 'binding-1',
    owner_id: 'owner-1',
    channel_id: 'UC1',
    enabled: true,
    reporting_enabled: true,
    started_at: '2026-04-02',
  },
]

test('owner modal renders owner fields and binding management', () => {
  const html = renderToStaticMarkup(
    <OwnerModal
      open
      mode="edit"
      owner={owner}
      bindings={bindings}
      channels={channels}
      saving={false}
      onClose={() => {}}
      onSubmit={async () => {}}
    />,
  )

  assert.match(html, /编辑 Owner/)
  assert.match(html, /Owner 名称/)
  assert.match(html, /频道绑定/)
  assert.match(html, /Channel One/)
  assert.match(html, /绑定频道/)
})
