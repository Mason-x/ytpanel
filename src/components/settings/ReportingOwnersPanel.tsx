import { useState } from 'react'
import type { ApiChannel, ReportingOwner, ReportingOwnerBinding, ReportingOwnerProbeResult, ReportingOwnerUsage, ReportingRequestLog } from '../../types'
import OwnerModal from './OwnerModal'
import OwnerRequestLogPanel from './OwnerRequestLogPanel'

type ReportingOwnerWithMeta = ReportingOwner & {
  bindings?: ReportingOwnerBinding[]
  usage?: ReportingOwnerUsage | null
}

type OwnerModalSubmitPayload = {
  mode: 'create' | 'edit'
  ownerId?: string
  ownerPayload: Record<string, unknown>
  createBindings: Array<Record<string, unknown>>
  updateBindings: Array<{ id: string; payload: Record<string, unknown> }>
  deleteBindingIds: string[]
}

type Props = {
  owners: ReportingOwnerWithMeta[]
  channels: ApiChannel[]
  selectedOwnerId: string
  ownerLogs: ReportingRequestLog[]
  probeResults?: Record<string, ReportingOwnerProbeResult>
  loading: boolean
  saving: boolean
  onSelectOwner: (ownerId: string) => void
  onSaveOwnerModal: (payload: OwnerModalSubmitPayload) => Promise<void> | void
  onDeleteOwner: (ownerId: string) => Promise<void> | void
  onProbeOwner: (ownerId: string) => Promise<void> | void
  onLoadOwnerLogs: (ownerId: string) => Promise<void> | void
}

export default function ReportingOwnersPanel({
  owners,
  channels,
  selectedOwnerId,
  ownerLogs,
  probeResults,
  loading,
  saving,
  onSelectOwner,
  onSaveOwnerModal,
  onDeleteOwner,
  onProbeOwner,
  onLoadOwnerLogs,
}: Props) {
  const [modalMode, setModalMode] = useState<'create' | 'edit' | null>(null)
  const selectedOwner = owners.find((owner) => owner.owner_id === selectedOwnerId) || owners[0] || null
  const selectedProbeResult = selectedOwner ? probeResults?.[selectedOwner.owner_id] || null : null

  const proxyStatus = (() => {
    if (selectedProbeResult) {
      const parts = [
        selectedProbeResult.ok ? '代理检测正常' : '代理检测失败',
        selectedProbeResult.proxy_mode,
        selectedProbeResult.egress_ip ? `出口 ${selectedProbeResult.egress_ip}` : '出口未返回',
        selectedProbeResult.message,
      ].filter(Boolean)
      return {
        tone: selectedProbeResult.ok ? 'success' : 'error',
        message: parts.join(' · '),
      }
    }

    return {
      tone: 'idle',
      message: selectedOwner?.last_error || '点击“检测代理”查看出口 IP 与 Google 连接状态。',
    }
  })()

  return (
    <div className="card-flat">
      <div className="panel-header">
        <div>
          <h3>Reporting Owners</h3>
          <p>维护每个 Owner 独立的 OAuth2 凭证、代理、频道绑定、请求日志和本地用度。</p>
        </div>
        <button type="button" className="btn btn-primary" onClick={() => setModalMode('create')}>
          新增 Owner
        </button>
      </div>

      {loading ? (
        <div className="empty-state">Reporting Owner 加载中...</div>
      ) : owners.length === 0 ? (
        <div className="empty-state">尚未配置 Owner。</div>
      ) : (
        <div className="settings-form" style={{ gap: 20 }}>
          <div style={{ display: 'grid', gap: 12 }}>
            {owners.map((owner) => (
              <div
                key={owner.owner_id}
                className="card-flat"
                style={{
                  padding: 14,
                  border: owner.owner_id === selectedOwner?.owner_id ? '1px solid var(--accent-primary)' : '1px solid var(--border-subtle)',
                  background: 'var(--bg-tertiary)',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
                  <button
                    type="button"
                    onClick={() => {
                      onSelectOwner(owner.owner_id)
                      void onLoadOwnerLogs(owner.owner_id)
                    }}
                    style={{ background: 'transparent', textAlign: 'left', padding: 0, flex: 1, minWidth: 0 }}
                  >
                    <div className="reporting-owner-title">{owner.name}</div>
                    <div className="reporting-owner-meta">{owner.proxy_url || 'direct'}</div>
                  </button>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'flex-start' }}>
                    <div className="reporting-owner-meta">{`绑定 ${owner.bindings?.length || 0} 个频道`}</div>
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => {
                        onSelectOwner(owner.owner_id)
                        void onLoadOwnerLogs(owner.owner_id)
                        setModalMode('edit')
                      }}
                    >
                      编辑
                    </button>
                  </div>
                </div>
                <div className="reporting-owner-stats">
                  <span className="reporting-owner-stat">{`请求成功率 ${Math.round((owner.usage?.success_rate_24h || 0) * 100)}%`}</span>
                  <span className="reporting-owner-stat">{`24h 请求 ${owner.usage?.request_count_24h || 0}`}</span>
                  <span className="reporting-owner-stat">{`24h 下载 ${owner.usage?.download_count_24h || 0}`}</span>
                </div>
              </div>
            ))}
          </div>

          {selectedOwner ? (
            <>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
                <div className={`reporting-probe-status reporting-probe-status-${proxyStatus.tone}`}>{proxyStatus.message}</div>
                <div style={{ display: 'flex', gap: 8 }}>
                  <button type="button" className="btn btn-secondary" onClick={() => void onProbeOwner(selectedOwner.owner_id)}>检测代理</button>
                  <button type="button" className="btn btn-ghost" onClick={() => void onDeleteOwner(selectedOwner.owner_id)}>删除 Owner</button>
                </div>
              </div>

              <OwnerRequestLogPanel logs={ownerLogs} />
            </>
          ) : null}
        </div>
      )}

      <OwnerModal
        open={modalMode === 'create' || (modalMode === 'edit' && !!selectedOwner)}
        mode={modalMode === 'edit' ? 'edit' : 'create'}
        owner={modalMode === 'edit' ? selectedOwner : null}
        bindings={modalMode === 'edit' ? (selectedOwner?.bindings || []) : []}
        channels={channels}
        saving={saving}
        onClose={() => setModalMode(null)}
        onSubmit={async (payload) => {
          await onSaveOwnerModal(payload)
          setModalMode(null)
        }}
      />
    </div>
  )
}
