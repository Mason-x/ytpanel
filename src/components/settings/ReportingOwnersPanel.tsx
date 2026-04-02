import type { ApiChannel, ReportingOwner, ReportingOwnerBinding, ReportingOwnerUsage, ReportingRequestLog } from '../../types'
import OwnerBindingsPanel from './OwnerBindingsPanel'
import OwnerEditorModal from './OwnerEditorModal'
import OwnerRequestLogPanel from './OwnerRequestLogPanel'

type ReportingOwnerWithMeta = ReportingOwner & {
  bindings?: ReportingOwnerBinding[]
  usage?: ReportingOwnerUsage | null
}

type Props = {
  owners: ReportingOwnerWithMeta[]
  channels: ApiChannel[]
  selectedOwnerId: string
  ownerLogs: ReportingRequestLog[]
  probeMessages?: Record<string, string>
  loading: boolean
  saving: boolean
  onSelectOwner: (ownerId: string) => void
  onCreateOwner: (payload: Record<string, unknown>) => Promise<void> | void
  onUpdateOwner: (ownerId: string, payload: Record<string, unknown>) => Promise<void> | void
  onDeleteOwner: (ownerId: string) => Promise<void> | void
  onProbeOwner: (ownerId: string) => Promise<void> | void
  onCreateBinding: (ownerId: string, payload: Record<string, unknown>) => Promise<void> | void
  onUpdateBinding: (bindingId: string, payload: Record<string, unknown>) => Promise<void> | void
  onDeleteBinding: (bindingId: string) => Promise<void> | void
  onSyncBinding: (bindingId: string) => Promise<void> | void
  onLoadOwnerLogs: (ownerId: string) => Promise<void> | void
}

export default function ReportingOwnersPanel({
  owners,
  channels,
  selectedOwnerId,
  ownerLogs,
  probeMessages,
  loading,
  saving,
  onSelectOwner,
  onCreateOwner,
  onUpdateOwner,
  onDeleteOwner,
  onProbeOwner,
  onCreateBinding,
  onUpdateBinding,
  onDeleteBinding,
  onSyncBinding,
  onLoadOwnerLogs,
}: Props) {
  const selectedOwner = owners.find((owner) => owner.owner_id === selectedOwnerId) || owners[0] || null

  return (
    <div className="card-flat">
      <div className="panel-header">
        <div>
          <h3>Reporting Owners</h3>
          <p>维护每个 Owner 独立的 OAuth2 凭证、代理、频道绑定、请求日志和本地用度。</p>
        </div>
      </div>

      {loading ? (
        <div className="empty-state">Reporting Owner 加载中...</div>
      ) : owners.length === 0 ? (
        <div className="settings-form" style={{ gap: 18 }}>
          <div className="empty-state">尚未配置 Owner。</div>
          <OwnerEditorModal saving={saving} onSubmit={onCreateOwner} />
        </div>
      ) : (
        <div className="settings-form" style={{ gap: 20 }}>
          <div style={{ display: 'grid', gap: 12 }}>
            {owners.map((owner) => (
              <button
                key={owner.owner_id}
                type="button"
                className="card-flat"
                onClick={() => {
                  onSelectOwner(owner.owner_id)
                  void onLoadOwnerLogs(owner.owner_id)
                }}
                style={{
                  textAlign: 'left',
                  padding: 14,
                  border: owner.owner_id === selectedOwner?.owner_id ? '1px solid var(--accent-primary)' : '1px solid var(--border-subtle)',
                  background: 'var(--bg-tertiary)',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
                  <div>
                    <div style={{ fontWeight: 700 }}>{owner.name}</div>
                    <div className="form-help">{owner.proxy_url || 'direct'}</div>
                  </div>
                  <div className="form-help">{`绑定 ${owner.bindings?.length || 0} 个频道`}</div>
                </div>
                <div style={{ marginTop: 10, display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                  <span className="form-help">{`请求成功率 ${Math.round((owner.usage?.success_rate_24h || 0) * 100)}%`}</span>
                  <span className="form-help">{`24h 请求 ${owner.usage?.request_count_24h || 0}`}</span>
                  <span className="form-help">{`24h 下载 ${owner.usage?.download_count_24h || 0}`}</span>
                </div>
              </button>
            ))}
          </div>

          {selectedOwner ? (
            <>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
                <div className="form-help">{probeMessages?.[selectedOwner.owner_id] || selectedOwner.last_error || ''}</div>
                <div style={{ display: 'flex', gap: 8 }}>
                  <button type="button" className="btn btn-secondary" onClick={() => void onProbeOwner(selectedOwner.owner_id)}>检测代理</button>
                  <button type="button" className="btn btn-ghost" onClick={() => void onDeleteOwner(selectedOwner.owner_id)}>删除 Owner</button>
                </div>
              </div>

              <OwnerEditorModal
                owner={selectedOwner}
                saving={saving}
                onSubmit={(payload) => onUpdateOwner(selectedOwner.owner_id, payload)}
              />

              <OwnerBindingsPanel
                ownerId={selectedOwner.owner_id}
                bindings={selectedOwner.bindings || []}
                channels={channels}
                saving={saving}
                onCreateBinding={onCreateBinding}
                onUpdateBinding={onUpdateBinding}
                onDeleteBinding={onDeleteBinding}
                onSyncBinding={onSyncBinding}
              />

              <OwnerRequestLogPanel logs={ownerLogs} />

              <div className="card-flat" style={{ padding: 14 }}>
                <div style={{ fontWeight: 700, marginBottom: 8 }}>新增 Owner</div>
                <OwnerEditorModal saving={saving} onSubmit={onCreateOwner} />
              </div>
            </>
          ) : null}
        </div>
      )}
    </div>
  )
}
