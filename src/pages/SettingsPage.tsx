import { useEffect, useState } from 'react'
import { api } from '../lib/api'
import { buildSettingsPayload, clampConcurrency, deriveSettingsFormState } from '../lib/settingsForm'
import ReportingOwnersPanel from '../components/settings/ReportingOwnersPanel'
import type {
  ApiChannel,
  AppSettingsResponse,
  ReportingOwner,
  ReportingOwnerBinding,
  ReportingOwnerProbeResult,
  ReportingOwnerUsage,
  ReportingRequestLog,
  YoutubeApiUsage,
} from '../types'

function maskApiKey(value: string) {
  const trimmed = value.trim()
  if (!trimmed) return ''
  if (trimmed.length <= 7) return `${trimmed.slice(0, 2)}***${trimmed.slice(-2)}`
  return `${trimmed.slice(0, 3)}***${trimmed.slice(-4)}`
}

const MASKED_COOKIE_TEXT = '[已保存 YouTube Cookie，点击或聚焦后重新输入以覆盖]'

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

export default function SettingsPage() {
  const [apiKey, setApiKey] = useState('')
  const [maskedApiKey, setMaskedApiKey] = useState('')
  const [hasSavedKey, setHasSavedKey] = useState(false)
  const [showMaskedValue, setShowMaskedValue] = useState(false)
  const [youtubeCookie, setYoutubeCookie] = useState('')
  const [maskedYoutubeCookie, setMaskedYoutubeCookie] = useState('')
  const [hasSavedYoutubeCookie, setHasSavedYoutubeCookie] = useState(false)
  const [showMaskedYoutubeCookie, setShowMaskedYoutubeCookie] = useState(false)
  const [dailySyncTime, setDailySyncTime] = useState('03:00')
  const [syncConcurrency, setSyncConcurrency] = useState('2')
  const [downloadConcurrency, setDownloadConcurrency] = useState('2')
  const [savedAt, setSavedAt] = useState('')
  const [usage, setUsage] = useState<YoutubeApiUsage | null>(null)
  const [reportingOwners, setReportingOwners] = useState<ReportingOwnerWithMeta[]>([])
  const [channels, setChannels] = useState<ApiChannel[]>([])
  const [selectedOwnerId, setSelectedOwnerId] = useState('')
  const [ownerLogs, setOwnerLogs] = useState<ReportingRequestLog[]>([])
  const [probeResults, setProbeResults] = useState<Record<string, ReportingOwnerProbeResult>>({})
  const [reportingLoading, setReportingLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [errorText, setErrorText] = useState('')

  useEffect(() => {
    let cancelled = false

    async function loadBase() {
      try {
        const [settings, usageData, reportingOwnersRes, channelsRes] = await Promise.all([
          api.getSettings(),
          api.getYoutubeApiUsage().catch(() => null),
          api.getReportingOwners().catch(() => ({ data: [] })),
          api.getChannels({ platform: 'youtube', limit: 1000, sort: 'recent' }).catch(() => ({ data: [] })),
        ])

        if (cancelled) return

        const nextState = deriveSettingsFormState(settings)

        setHasSavedKey(nextState.hasSavedKey)
        setShowMaskedValue(nextState.showMaskedValue)
        setMaskedApiKey(nextState.maskedApiKey || (nextState.hasSavedKey ? maskApiKey(String(settings.youtube_api_key || '').trim()) : ''))
        setHasSavedYoutubeCookie(nextState.hasSavedCookie)
        setShowMaskedYoutubeCookie(nextState.showMaskedCookieValue)
        setMaskedYoutubeCookie(nextState.maskedCookieValue)
        setDailySyncTime(nextState.dailySyncTime)
        setSyncConcurrency(nextState.syncConcurrency)
        setDownloadConcurrency(nextState.downloadConcurrency)
        setUsage(usageData)
        setReportingOwners(reportingOwnersRes.data || [])
        setChannels(channelsRes.data || [])
        setSelectedOwnerId((current) => current || reportingOwnersRes.data?.[0]?.owner_id || '')
        setReportingLoading(false)
      } catch (error) {
        if (cancelled) return
        setErrorText(error instanceof Error ? error.message : '设置加载失败')
        setReportingLoading(false)
      }
    }

    void loadBase()

    return () => {
      cancelled = true
    }
  }, [])

  const handleRevealInput = () => {
    if (!showMaskedValue) return
    setShowMaskedValue(false)
    setApiKey('')
    setErrorText('')
  }

  const handleRevealYoutubeCookieInput = () => {
    if (!showMaskedYoutubeCookie) return
    setShowMaskedYoutubeCookie(false)
    setYoutubeCookie('')
    setErrorText('')
  }

  const handleSave = async () => {
    const nextSyncTime = String(dailySyncTime || '').trim()
    const nextSyncConcurrency = clampConcurrency(syncConcurrency)
    const nextDownloadConcurrency = clampConcurrency(downloadConcurrency)

    if (!showMaskedValue && !apiKey.trim() && !hasSavedKey) {
      setErrorText('请输入 YouTube API Key')
      return
    }

    if (!/^\d{2}:\d{2}$/.test(nextSyncTime)) {
      setErrorText('每日同步时间格式应为 HH:MM')
      return
    }

    setSaving(true)
    setErrorText('')

    const payload: Partial<AppSettingsResponse> = buildSettingsPayload({
      hasSavedKey,
      showMaskedValue,
      apiKey,
      dailySyncTime: nextSyncTime,
      syncConcurrency: nextSyncConcurrency,
      downloadConcurrency: nextDownloadConcurrency,
      hasSavedCookie: hasSavedYoutubeCookie,
      showMaskedCookieValue: showMaskedYoutubeCookie,
      cookieValue: youtubeCookie,
    })

    try {
      const settings = await api.updateSettings(payload)
      const nextState = deriveSettingsFormState(settings)

      setHasSavedKey(nextState.hasSavedKey)
      setShowMaskedValue(nextState.showMaskedValue)
      setMaskedApiKey(nextState.maskedApiKey || (nextState.hasSavedKey ? maskApiKey(String(settings.youtube_api_key || '').trim()) : ''))
      setApiKey('')
      setHasSavedYoutubeCookie(nextState.hasSavedCookie)
      setShowMaskedYoutubeCookie(nextState.showMaskedCookieValue)
      setMaskedYoutubeCookie(nextState.maskedCookieValue)
      setYoutubeCookie('')
      setDailySyncTime(String(settings.daily_sync_time || nextSyncTime))
      setSyncConcurrency(clampConcurrency(String(settings.sync_job_concurrency || nextSyncConcurrency)))
      setDownloadConcurrency(clampConcurrency(String(settings.download_job_concurrency || nextDownloadConcurrency)))
      setSavedAt(new Date().toLocaleString('zh-CN', { hour12: false }))
      setUsage(await api.getYoutubeApiUsage().catch(() => usage))
      window.dispatchEvent(new Event('ytpanel-settings-changed'))
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '设置保存失败')
    } finally {
      setSaving(false)
    }
  }

  const reloadReportingOwners = async (nextOwnerId = '') => {
    const owners = await api.getReportingOwners()
    setReportingOwners(owners.data || [])
    setSelectedOwnerId(nextOwnerId || owners.data?.[0]?.owner_id || '')
  }

  const handleLoadOwnerLogs = async (ownerId: string) => {
    const response = await api.getReportingOwnerLogs(ownerId, { limit: 100 })
    setOwnerLogs(response.data || [])
  }

  const handleDeleteOwner = async (ownerId: string) => {
    setSaving(true)
    setErrorText('')
    try {
      await api.deleteReportingOwner(ownerId)
      await reloadReportingOwners('')
      setOwnerLogs([])
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '删除 Owner 失败')
    } finally {
      setSaving(false)
    }
  }

  const handleProbeOwner = async (ownerId: string) => {
    setSaving(true)
    setErrorText('')
    try {
      const result = await api.testReportingOwnerProxy(ownerId)
      setProbeResults((current) => ({
        ...current,
        [ownerId]: result as ReportingOwnerProbeResult,
      }))
      await handleLoadOwnerLogs(ownerId)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '代理检测失败')
    } finally {
      setSaving(false)
    }
  }

  const handleSyncBinding = async (bindingId: string) => {
    setSaving(true)
    setErrorText('')
    try {
      await api.syncReportingBinding(bindingId)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '同步报表失败')
    } finally {
      setSaving(false)
    }
  }

  const handleSaveOwnerModal = async (payload: OwnerModalSubmitPayload) => {
    setSaving(true)
    setErrorText('')
    try {
      let ownerId = String(payload.ownerId || '').trim()
      if (payload.mode === 'create') {
        const created = await api.createReportingOwner(payload.ownerPayload)
        ownerId = created.owner_id
      } else {
        if (!ownerId) throw new Error('缺少 Owner ID')
        await api.updateReportingOwner(ownerId, payload.ownerPayload)
      }

      for (const bindingId of payload.deleteBindingIds) {
        await api.deleteReportingBinding(bindingId)
      }
      for (const item of payload.updateBindings) {
        await api.updateReportingBinding(item.id, item.payload)
      }
      for (const item of payload.createBindings) {
        await api.createReportingBinding(ownerId, item)
      }

      await reloadReportingOwners(ownerId)
      await handleLoadOwnerLogs(ownerId)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '保存 Owner 失败')
      throw error
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="settings-page">
      <div className="page-header settings-header">
        <div>
          <div className="page-title">设置</div>
          <div className="settings-header-meta">
            管理 API 凭证、每日同步策略和任务并发，决定顶部状态栏与队列的执行行为。
          </div>
        </div>
      </div>

      <div className="settings-layout">
        <section className="settings-main">
          <div className="card-flat">
            <div className="panel-header">
              <div>
                <h3>YouTube API</h3>
                <p>配置频道同步、数据洞察和顶部 API 配额状态使用的 YouTube API Key。</p>
              </div>
            </div>

            <div className="settings-form">
              <div className="form-group">
                <label className="form-label">YouTube API Key</label>
                <input
                  className="input"
                  type={showMaskedValue ? 'text' : 'password'}
                  readOnly={showMaskedValue}
                  value={showMaskedValue ? maskedApiKey : apiKey}
                  onFocus={handleRevealInput}
                  onClick={handleRevealInput}
                  onChange={(event) => {
                    setShowMaskedValue(false)
                    setApiKey(event.target.value)
                    setErrorText('')
                  }}
                  placeholder="AIza..."
                />
                <span className="form-help">建议使用只开放 YouTube Data API v3 的受限 Key。</span>
              </div>

              <div className="form-group">
                <label className="form-label">yt-dlp YouTube Cookie</label>
                <textarea
                  className="input"
                  rows={6}
                  value={showMaskedYoutubeCookie ? MASKED_COOKIE_TEXT : youtubeCookie}
                  onFocus={handleRevealYoutubeCookieInput}
                  onClick={handleRevealYoutubeCookieInput}
                  onChange={(event) => {
                    setShowMaskedYoutubeCookie(false)
                    setYoutubeCookie(event.target.value)
                    setErrorText('')
                  }}
                  placeholder={'支持直接粘贴 Cookie Header、Netscape Cookie、JSON 或本地文件路径'}
                  readOnly={showMaskedYoutubeCookie}
                />
                <span className="form-help">频道元数据、封面和部分受限内容拉取依赖这里的 YouTube Cookie。留空表示清空已保存配置。</span>
              </div>
            </div>
          </div>

          <div className="card-flat">
            <div className="panel-header">
              <div>
                <h3>同步与并发</h3>
                <p>这些配置会直接影响自动每日同步、手动同步和下载任务的排队与执行速度。</p>
              </div>
            </div>

            <div className="settings-form">
              <div className="settings-inline-grid">
                <div className="form-group">
                  <label className="form-label">每日同步时间</label>
                  <input
                    className="input"
                    type="time"
                    step="60"
                    value={dailySyncTime}
                    onChange={(event) => setDailySyncTime(event.target.value)}
                  />
                  <span className="form-help">到点后会自动入队一次 `daily_sync` 任务。</span>
                </div>

                <div className="form-group">
                  <label className="form-label">同步任务并发</label>
                  <input
                    className="input"
                    type="number"
                    min="1"
                    max="16"
                    value={syncConcurrency}
                    onChange={(event) => setSyncConcurrency(clampConcurrency(event.target.value))}
                  />
                  <span className="form-help">控制 `daily_sync`、单频道同步等同步类任务同时运行数量。</span>
                </div>

                <div className="form-group">
                  <label className="form-label">下载任务并发</label>
                  <input
                    className="input"
                    type="number"
                    min="1"
                    max="16"
                    value={downloadConcurrency}
                    onChange={(event) => setDownloadConcurrency(clampConcurrency(event.target.value))}
                  />
                  <span className="form-help">控制元数据、缩略图、字幕和视频下载类任务的同时执行数量。</span>
                </div>
              </div>

              {errorText && <div className="tools-similar-issues">{errorText}</div>}

              <div className="settings-actions">
                <button type="button" className="btn btn-primary" onClick={handleSave} disabled={saving}>
                  {saving ? '保存中...' : '保存设置'}
                </button>
                {savedAt && <span className="form-help">{`已保存：${savedAt}`}</span>}
              </div>
            </div>
          </div>

          <ReportingOwnersPanel
            owners={reportingOwners}
            channels={channels}
            selectedOwnerId={selectedOwnerId}
            ownerLogs={ownerLogs}
            probeResults={probeResults}
            loading={reportingLoading}
            saving={saving}
            onSelectOwner={(ownerId) => {
              setSelectedOwnerId(ownerId)
            }}
            onSaveOwnerModal={handleSaveOwnerModal}
            onDeleteOwner={handleDeleteOwner}
            onProbeOwner={handleProbeOwner}
            onLoadOwnerLogs={handleLoadOwnerLogs}
          />
        </section>

        <aside className="settings-side">
          <div className="card-flat">
            <div className="settings-side-title">状态栏说明</div>
            <ul className="settings-tip-list">
              <li>{hasSavedKey ? '已检测到后端保存的 API Key。' : '当前还没有保存 API Key。'}</li>
              <li>{hasSavedYoutubeCookie ? '已检测到后端保存的 YouTube Cookie。' : '当前还没有保存 YouTube Cookie。'}</li>
              <li>{usage ? `顶部 API 配额会显示为 ${usage.used_units} / ${usage.daily_limit}。` : '顶部 API 配额状态暂未获取。'}</li>
              <li>{`每日同步会在每天 ${dailySyncTime || '03:00'} 自动入队。`}</li>
              <li>{`同步任务并发当前为 ${syncConcurrency}。`}</li>
              <li>{`下载任务并发当前为 ${downloadConcurrency}。`}</li>
              <li>导航栏右侧的“执行每日同步”会立即入队一次每日同步任务。</li>
              <li>频道页的数据拉取、元数据下载和洞察分析都依赖这里的 API、Cookie 与并发配置。</li>
            </ul>
          </div>
        </aside>
      </div>
    </div>
  )
}
