import { useMemo, useState } from 'react'
import type { ApiChannel, ReportingOwnerBinding } from '../../types'

type Props = {
  ownerId: string
  bindings: ReportingOwnerBinding[]
  channels: ApiChannel[]
  saving: boolean
  onCreateBinding: (ownerId: string, payload: Record<string, unknown>) => Promise<void> | void
  onUpdateBinding: (bindingId: string, payload: Record<string, unknown>) => Promise<void> | void
  onDeleteBinding: (bindingId: string) => Promise<void> | void
  onSyncBinding: (bindingId: string) => Promise<void> | void
}

export default function OwnerBindingsPanel({
  ownerId,
  bindings,
  channels,
  saving,
  onCreateBinding,
  onUpdateBinding,
  onDeleteBinding,
  onSyncBinding,
}: Props) {
  const [channelId, setChannelId] = useState('')
  const [startedAt, setStartedAt] = useState(new Date().toISOString().slice(0, 10))

  const boundChannelIds = useMemo(() => new Set(bindings.map((binding) => binding.channel_id)), [bindings])

  return (
    <div className="settings-form" style={{ gap: 14 }}>
      <div className="panel-header">
        <div>
          <h3>频道绑定</h3>
          <p>为当前 Owner 绑定频道，并控制 Reporting API 是否生效。</p>
        </div>
      </div>

      <div className="settings-inline-grid">
        <div className="form-group">
          <label className="form-label">选择频道</label>
          <select className="input" value={channelId} onChange={(event) => setChannelId(event.target.value)}>
            <option value="">选择频道</option>
            {channels.map((channel) => (
              <option key={channel.channel_id} value={channel.channel_id} disabled={boundChannelIds.has(channel.channel_id)}>
                {channel.title} ({channel.channel_id})
              </option>
            ))}
          </select>
        </div>
        <div className="form-group">
          <label className="form-label">启用日期</label>
          <input className="input" type="date" value={startedAt} onChange={(event) => setStartedAt(event.target.value)} />
        </div>
      </div>

      <div>
        <button
          type="button"
          className="btn btn-secondary"
          disabled={saving || !channelId}
          onClick={() => void onCreateBinding(ownerId, {
            channel_id: channelId,
            started_at: startedAt,
            enabled: true,
            reporting_enabled: true,
          })}
        >
          绑定频道
        </button>
      </div>

      {bindings.length === 0 ? (
        <div className="empty-state">当前 Owner 暂无频道绑定。</div>
      ) : (
        <div style={{ display: 'grid', gap: 10 }}>
          {bindings.map((binding) => (
            <div key={binding.id} className="card-flat" style={{ padding: 14 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
                <div>
                  <div style={{ fontWeight: 700 }}>{binding.channel_id}</div>
                  <div className="form-help">{`启用日期：${binding.started_at}`}</div>
                </div>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                  <button
                    type="button"
                    className="btn btn-secondary btn-sm"
                    onClick={() => void onUpdateBinding(binding.id, { enabled: !binding.enabled })}
                  >
                    {binding.enabled ? '停用绑定' : '启用绑定'}
                  </button>
                  <button
                    type="button"
                    className="btn btn-secondary btn-sm"
                    onClick={() => void onUpdateBinding(binding.id, { reporting_enabled: !binding.reporting_enabled })}
                  >
                    {binding.reporting_enabled ? '关闭 Reporting' : '开启 Reporting'}
                  </button>
                  <button type="button" className="btn btn-secondary btn-sm" onClick={() => void onSyncBinding(binding.id)}>同步报表</button>
                  <button type="button" className="btn btn-ghost btn-sm" onClick={() => void onDeleteBinding(binding.id)}>解绑</button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
