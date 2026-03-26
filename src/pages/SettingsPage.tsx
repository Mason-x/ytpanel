import { useEffect, useState } from 'react'
import { api } from '../lib/api'
import type { AppSettingsResponse, YoutubeApiUsage } from '../types'

function maskApiKey(value: string) {
  const trimmed = value.trim()
  if (!trimmed) return ''
  if (trimmed.length <= 7) return `${trimmed.slice(0, 2)}***${trimmed.slice(-2)}`
  return `${trimmed.slice(0, 3)}***${trimmed.slice(-4)}`
}

function clampConcurrency(value: string) {
  const next = Math.trunc(Number(value) || 0)
  if (!Number.isFinite(next) || next < 1) return '1'
  if (next > 16) return '16'
  return String(next)
}

export default function SettingsPage() {
  const [apiKey, setApiKey] = useState('')
  const [maskedApiKey, setMaskedApiKey] = useState('')
  const [hasSavedKey, setHasSavedKey] = useState(false)
  const [showMaskedValue, setShowMaskedValue] = useState(false)
  const [dailySyncTime, setDailySyncTime] = useState('03:00')
  const [syncConcurrency, setSyncConcurrency] = useState('2')
  const [downloadConcurrency, setDownloadConcurrency] = useState('2')
  const [savedAt, setSavedAt] = useState('')
  const [usage, setUsage] = useState<YoutubeApiUsage | null>(null)
  const [saving, setSaving] = useState(false)
  const [errorText, setErrorText] = useState('')

  useEffect(() => {
    let cancelled = false

    async function load() {
      try {
        const [settings, usageData] = await Promise.all([
          api.getSettings(),
          api.getYoutubeApiUsage().catch(() => null),
        ])

        if (cancelled) return

        const savedToken = String(settings.youtube_api_key || '').trim()
        const hasKey = Boolean(savedToken)

        setHasSavedKey(hasKey)
        setShowMaskedValue(hasKey)
        setMaskedApiKey(String(settings.youtube_api_key_masked_preview || '').trim() || (hasKey ? maskApiKey(savedToken) : ''))
        setDailySyncTime(String(settings.daily_sync_time || '03:00').trim() || '03:00')
        setSyncConcurrency(clampConcurrency(String(settings.sync_job_concurrency || '2')))
        setDownloadConcurrency(clampConcurrency(String(settings.download_job_concurrency || '2')))
        setUsage(usageData)
      } catch (error) {
        if (cancelled) return
        setErrorText(error instanceof Error ? error.message : '设置加载失败')
      }
    }

    void load()

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

  const handleSave = async () => {
    const nextApiKey = apiKey.trim()
    const nextSyncTime = String(dailySyncTime || '').trim()
    const nextSyncConcurrency = clampConcurrency(syncConcurrency)
    const nextDownloadConcurrency = clampConcurrency(downloadConcurrency)

    if (!showMaskedValue && !nextApiKey && !hasSavedKey) {
      setErrorText('请输入 YouTube API Key')
      return
    }

    if (!/^\d{2}:\d{2}$/.test(nextSyncTime)) {
      setErrorText('每日同步时间格式应为 HH:MM')
      return
    }

    setSaving(true)
    setErrorText('')

    const payload: Partial<AppSettingsResponse> = {
      daily_sync_time: nextSyncTime,
      sync_job_concurrency: nextSyncConcurrency,
      download_job_concurrency: nextDownloadConcurrency,
    }
    if (!showMaskedValue || nextApiKey) {
      payload.youtube_api_key = nextApiKey
    }

    try {
      const settings = await api.updateSettings(payload)
      const savedToken = String(settings.youtube_api_key || '').trim()
      const hasKey = Boolean(savedToken)

      setHasSavedKey(hasKey)
      setShowMaskedValue(hasKey)
      setMaskedApiKey(String(settings.youtube_api_key_masked_preview || '').trim() || (hasKey ? maskApiKey(savedToken) : ''))
      setApiKey('')
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
        </section>

        <aside className="settings-side">
          <div className="card-flat">
            <div className="settings-side-title">状态栏说明</div>
            <ul className="settings-tip-list">
              <li>{hasSavedKey ? '已检测到后端保存的 API Key。' : '当前还没有保存 API Key。'}</li>
              <li>{usage ? `顶部 API 配额会显示为 ${usage.used_units} / ${usage.daily_limit}。` : '顶部 API 配额状态暂未获取。'}</li>
              <li>{`每日同步会在每天 ${dailySyncTime || '03:00'} 自动入队。`}</li>
              <li>{`同步任务并发当前为 ${syncConcurrency}。`}</li>
              <li>{`下载任务并发当前为 ${downloadConcurrency}。`}</li>
              <li>导航栏右侧的“执行每日同步”会立即入队一次每日同步任务。</li>
              <li>频道页的数据拉取、元数据下载和洞察分析都依赖这里的 API 与并发配置。</li>
            </ul>
          </div>
        </aside>
      </div>
    </div>
  )
}
