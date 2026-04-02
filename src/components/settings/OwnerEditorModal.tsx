import { useEffect, useState } from 'react'
import type { ReportingOwner } from '../../types'
import {
  buildReportingOwnerPayload,
  deriveReportingOwnerFormState,
  type ReportingOwnerFormState,
} from '../../lib/reportingSettingsForm'

type Props = {
  owner?: ReportingOwner | null
  saving: boolean
  onSubmit: (payload: Record<string, unknown>) => Promise<void> | void
}

function createEmptyState(): ReportingOwnerFormState {
  return deriveReportingOwnerFormState({
    owner_id: '',
    name: '',
    client_id: '',
    client_secret: '',
    refresh_token: '',
    proxy_url: '',
    enabled: true,
    reporting_enabled: true,
    started_at: '',
  })
}

export default function OwnerEditorModal({ owner, saving, onSubmit }: Props) {
  const [form, setForm] = useState<ReportingOwnerFormState>(createEmptyState())

  useEffect(() => {
    setForm(owner ? deriveReportingOwnerFormState(owner) : createEmptyState())
  }, [owner])

  return (
    <div className="settings-form" style={{ gap: 14 }}>
      <div className="settings-inline-grid">
        <div className="form-group">
          <label className="form-label">Owner 名称</label>
          <input className="input" value={form.name} onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))} placeholder="Owner A" />
        </div>
        <div className="form-group">
          <label className="form-label">启用日期</label>
          <input className="input" type="date" value={form.startedAt} onChange={(event) => setForm((current) => ({ ...current, startedAt: event.target.value }))} />
        </div>
      </div>

      <div className="settings-inline-grid">
        <div className="form-group">
          <label className="form-label">Client ID</label>
          <input className="input" value={form.clientId} onChange={(event) => setForm((current) => ({ ...current, clientId: event.target.value }))} placeholder="google-client-id" />
        </div>
        <div className="form-group">
          <label className="form-label">代理 URL</label>
          <input className="input" value={form.proxyUrl} onChange={(event) => setForm((current) => ({ ...current, proxyUrl: event.target.value }))} placeholder="http://127.0.0.1:8080" />
        </div>
      </div>

      <div className="settings-inline-grid">
        <div className="form-group">
          <label className="form-label">Client Secret</label>
          <input
            className="input"
            type="password"
            value={form.clientSecret}
            onFocus={() => form.showMaskedClientSecret && setForm((current) => ({ ...current, clientSecret: '', showMaskedClientSecret: false }))}
            onChange={(event) => setForm((current) => ({ ...current, clientSecret: event.target.value, showMaskedClientSecret: false }))}
            placeholder="client-secret"
          />
        </div>
        <div className="form-group">
          <label className="form-label">Refresh Token</label>
          <input
            className="input"
            type="password"
            value={form.refreshToken}
            onFocus={() => form.showMaskedRefreshToken && setForm((current) => ({ ...current, refreshToken: '', showMaskedRefreshToken: false }))}
            onChange={(event) => setForm((current) => ({ ...current, refreshToken: event.target.value, showMaskedRefreshToken: false }))}
            placeholder="refresh-token"
          />
        </div>
      </div>

      <div style={{ display: 'flex', gap: 18, alignItems: 'center', flexWrap: 'wrap' }}>
        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <input type="checkbox" checked={form.enabled} onChange={(event) => setForm((current) => ({ ...current, enabled: event.target.checked }))} />
          <span>Owner 启用</span>
        </label>
        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <input type="checkbox" checked={form.reportingEnabled} onChange={(event) => setForm((current) => ({ ...current, reportingEnabled: event.target.checked }))} />
          <span>Reporting 启用</span>
        </label>
        <button
          type="button"
          className="btn btn-primary"
          disabled={saving}
          onClick={() => void onSubmit(buildReportingOwnerPayload(form))}
        >
          {saving ? '保存中...' : owner?.owner_id ? '保存 Owner' : '新增 Owner'}
        </button>
      </div>
    </div>
  )
}
