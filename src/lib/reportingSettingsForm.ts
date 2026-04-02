import type { ReportingOwner } from '../types'

export type ReportingOwnerFormState = {
  ownerId: string
  name: string
  clientId: string
  clientSecret: string
  refreshToken: string
  proxyUrl: string
  enabled: boolean
  reportingEnabled: boolean
  startedAt: string
  showMaskedClientSecret: boolean
  showMaskedRefreshToken: boolean
}

export type ReportingOwnerPayloadInput = ReportingOwnerFormState

export function deriveReportingOwnerFormState(owner?: Partial<ReportingOwner> | null): ReportingOwnerFormState {
  const clientSecret = String(owner?.client_secret || '').trim()
  const refreshToken = String(owner?.refresh_token || '').trim()
  return {
    ownerId: String(owner?.owner_id || '').trim(),
    name: String(owner?.name || '').trim(),
    clientId: String(owner?.client_id || '').trim(),
    clientSecret,
    refreshToken,
    proxyUrl: String(owner?.proxy_url || '').trim(),
    enabled: owner?.enabled !== false,
    reportingEnabled: owner?.reporting_enabled !== false,
    startedAt: String(owner?.started_at || '').trim(),
    showMaskedClientSecret: clientSecret.startsWith('__YT_REPORTING_OWNER_MASKED__:'),
    showMaskedRefreshToken: refreshToken.startsWith('__YT_REPORTING_OWNER_MASKED__:'),
  }
}

export function buildReportingOwnerPayload(input: ReportingOwnerPayloadInput) {
  return {
    name: String(input.name || '').trim(),
    client_id: String(input.clientId || '').trim(),
    client_secret: input.showMaskedClientSecret ? String(input.clientSecret || '').trim() : String(input.clientSecret || '').trim(),
    refresh_token: input.showMaskedRefreshToken ? String(input.refreshToken || '').trim() : String(input.refreshToken || '').trim(),
    proxy_url: String(input.proxyUrl || '').trim(),
    enabled: !!input.enabled,
    reporting_enabled: !!input.reportingEnabled,
    started_at: String(input.startedAt || '').trim() || null,
  }
}
