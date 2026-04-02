import test from 'node:test'
import assert from 'node:assert/strict'
import {
  buildReportingOwnerPayload,
  deriveReportingOwnerFormState,
} from '../reportingSettingsForm'

test('deriveReportingOwnerFormState exposes masked secret state', () => {
  const state = deriveReportingOwnerFormState({
    owner_id: 'owner-1',
    name: 'Owner One',
    client_id: 'client-id-1',
    client_secret: '__YT_REPORTING_OWNER_MASKED__:owner-1:client_secret',
    refresh_token: '__YT_REPORTING_OWNER_MASKED__:owner-1:refresh_token',
    proxy_url: 'http://127.0.0.1:8080/',
    enabled: true,
    reporting_enabled: true,
  })

  assert.equal(state.showMaskedClientSecret, true)
  assert.equal(state.showMaskedRefreshToken, true)
  assert.equal(Object.prototype.hasOwnProperty.call(state, 'startedAt'), false)
})

test('buildReportingOwnerPayload preserves masked secrets until edited', () => {
  const payload = buildReportingOwnerPayload({
    ownerId: 'owner-1',
    name: 'Owner One',
    clientId: 'client-id-1',
    clientSecret: '__YT_REPORTING_OWNER_MASKED__:owner-1:client_secret',
    refreshToken: '__YT_REPORTING_OWNER_MASKED__:owner-1:refresh_token',
    proxyUrl: 'socks5://127.0.0.1:1080',
    enabled: true,
    reportingEnabled: true,
    showMaskedClientSecret: true,
    showMaskedRefreshToken: true,
  })

  assert.equal(payload.client_secret, '__YT_REPORTING_OWNER_MASKED__:owner-1:client_secret')
  assert.equal(payload.refresh_token, '__YT_REPORTING_OWNER_MASKED__:owner-1:refresh_token')
  assert.equal(payload.proxy_url, 'socks5://127.0.0.1:1080')
  assert.equal(Object.prototype.hasOwnProperty.call(payload, 'started_at'), false)
})
