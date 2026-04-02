import { useEffect, useMemo, useState } from 'react'
import type { ApiChannel, ReportingOwner, ReportingOwnerBinding } from '../../types'
import {
  buildReportingOwnerPayload,
  deriveReportingOwnerFormState,
  type ReportingOwnerFormState,
} from '../../lib/reportingSettingsForm'

type BindingDraft = {
  temp_id: string
  id?: string
  channel_id: string
  enabled: boolean
  reporting_enabled: boolean
}

type SubmitPayload = {
  mode: 'create' | 'edit'
  ownerId?: string
  ownerPayload: Record<string, unknown>
  createBindings: Array<Record<string, unknown>>
  updateBindings: Array<{ id: string; payload: Record<string, unknown> }>
  deleteBindingIds: string[]
}

type Props = {
  open: boolean
  mode: 'create' | 'edit'
  owner?: ReportingOwner | null
  bindings: ReportingOwnerBinding[]
  channels: ApiChannel[]
  saving: boolean
  onClose: () => void
  onSubmit: (payload: SubmitPayload) => Promise<void> | void
}

function createEmptyFormState(): ReportingOwnerFormState {
  return deriveReportingOwnerFormState({
    owner_id: '',
    name: '',
    client_id: '',
    client_secret: '',
    refresh_token: '',
    proxy_url: '',
    enabled: true,
    reporting_enabled: true,
  })
}

function toBindingDraft(binding: ReportingOwnerBinding): BindingDraft {
  return {
    temp_id: binding.id,
    id: binding.id,
    channel_id: binding.channel_id,
    enabled: !!binding.enabled,
    reporting_enabled: !!binding.reporting_enabled,
  }
}

export default function OwnerModal({
  open,
  mode,
  owner,
  bindings,
  channels,
  saving,
  onClose,
  onSubmit,
}: Props) {
  const [form, setForm] = useState<ReportingOwnerFormState>(createEmptyFormState())
  const [bindingDrafts, setBindingDrafts] = useState<BindingDraft[]>([])
  const [channelIdToAdd, setChannelIdToAdd] = useState('')

  useEffect(() => {
    if (!open) return
    setForm(owner ? deriveReportingOwnerFormState(owner) : createEmptyFormState())
    setBindingDrafts((bindings || []).map(toBindingDraft))
    setChannelIdToAdd('')
  }, [open, owner, bindings])

  const boundChannelIds = useMemo(
    () => new Set(bindingDrafts.map((binding) => binding.channel_id)),
    [bindingDrafts],
  )

  if (!open) return null

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-owner" onClick={(event) => event.stopPropagation()}>
        <div className="modal-title">{mode === 'create' ? '新增 Owner' : '编辑 Owner'}</div>

        <div className="settings-form" style={{ gap: 16 }}>
          <div className="settings-inline-grid">
            <div className="form-group">
              <label className="form-label">Owner 名称</label>
              <input className="input" value={form.name} onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))} placeholder="Owner A" autoFocus />
            </div>
          </div>

          <div className="settings-inline-grid">
            <div className="form-group">
              <label className="form-label">Client ID</label>
              <input className="input" value={form.clientId} onChange={(event) => setForm((current) => ({ ...current, clientId: event.target.value }))} placeholder="google-client-id" />
            </div>
            <div className="form-group">
              <label className="form-label">代理 URL</label>
              <input className="input" value={form.proxyUrl} onChange={(event) => setForm((current) => ({ ...current, proxyUrl: event.target.value }))} placeholder="http://127.0.0.1:8080 或 socks5://127.0.0.1:1080" />
              <span className="form-help">支持 `http`、`https`、`socks`、`socks5` 代理。</span>
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
          </div>

          <div className="card-flat" style={{ padding: 14 }}>
            <div className="panel-header">
              <div>
                <h3>频道绑定</h3>
                <p>可以不绑定频道直接保存，也可以在这里一并配置绑定。</p>
              </div>
            </div>

            <div className="settings-inline-grid">
              <div className="form-group">
                <label className="form-label">选择频道</label>
                <select className="input" value={channelIdToAdd} onChange={(event) => setChannelIdToAdd(event.target.value)}>
                  <option value="">选择频道</option>
                  {channels.map((channel) => (
                    <option key={channel.channel_id} value={channel.channel_id} disabled={boundChannelIds.has(channel.channel_id)}>
                      {channel.title} ({channel.channel_id})
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div style={{ marginTop: 10, marginBottom: 14 }}>
              <button
                type="button"
                className="btn btn-secondary"
                disabled={!channelIdToAdd}
                onClick={() => {
                  if (!channelIdToAdd) return
                  setBindingDrafts((current) => [
                    ...current,
                    {
                      temp_id: `draft-${channelIdToAdd}-${Date.now()}`,
                      channel_id: channelIdToAdd,
                      enabled: true,
                      reporting_enabled: true,
                    },
                  ])
                  setChannelIdToAdd('')
                }}
              >
                绑定频道
              </button>
            </div>

            {bindingDrafts.length === 0 ? (
              <div className="empty-state">当前还没有绑定频道。</div>
            ) : (
              <div style={{ display: 'grid', gap: 10 }}>
                {bindingDrafts.map((binding) => (
                  <div key={binding.temp_id} className="card-flat" style={{ padding: 14 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
                      <div style={{ fontWeight: 700 }}>{binding.channel_id}</div>
                      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                        <button
                          type="button"
                          className="btn btn-secondary btn-sm"
                          onClick={() => setBindingDrafts((current) => current.map((item) => item.temp_id === binding.temp_id ? { ...item, enabled: !item.enabled } : item))}
                        >
                          {binding.enabled ? '停用绑定' : '启用绑定'}
                        </button>
                        <button
                          type="button"
                          className="btn btn-secondary btn-sm"
                          onClick={() => setBindingDrafts((current) => current.map((item) => item.temp_id === binding.temp_id ? { ...item, reporting_enabled: !item.reporting_enabled } : item))}
                        >
                          {binding.reporting_enabled ? '关闭 Reporting' : '开启 Reporting'}
                        </button>
                        <button
                          type="button"
                          className="btn btn-ghost btn-sm"
                          onClick={() => setBindingDrafts((current) => current.filter((item) => item.temp_id !== binding.temp_id))}
                        >
                          解绑
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              取消
            </button>
            <button
              type="button"
              className="btn btn-primary"
              disabled={saving}
              onClick={() => {
                const ownerPayload = buildReportingOwnerPayload(form)
                const initialBindings = bindings || []
                const currentExisting = bindingDrafts.filter((binding) => binding.id)
                const createBindings = bindingDrafts
                  .filter((binding) => !binding.id)
                  .map((binding) => ({
                    channel_id: binding.channel_id,
                    enabled: binding.enabled,
                    reporting_enabled: binding.reporting_enabled,
                  }))
                const updateBindings = currentExisting
                  .filter((binding) => {
                    const initial = initialBindings.find((item) => item.id === binding.id)
                    if (!initial) return false
                    return initial.enabled !== binding.enabled || initial.reporting_enabled !== binding.reporting_enabled
                  })
                  .map((binding) => ({
                    id: String(binding.id),
                    payload: {
                      enabled: binding.enabled,
                      reporting_enabled: binding.reporting_enabled,
                    },
                  }))
                const deleteBindingIds = initialBindings
                  .filter((binding) => !bindingDrafts.some((item) => item.id === binding.id))
                  .map((binding) => binding.id)

                void onSubmit({
                  mode,
                  ownerId: owner?.owner_id,
                  ownerPayload,
                  createBindings,
                  updateBindings,
                  deleteBindingIds,
                })
              }}
            >
              {saving ? '保存中...' : mode === 'create' ? '保存 Owner' : '保存修改'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
