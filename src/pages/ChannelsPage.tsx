import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import AddChannelModal from '../components/channels/AddChannelModal'
import ChannelSidebar from '../components/channels/ChannelSidebar'
import { ChannelGrowthChart } from '../components/channels/GrowthCharts'
import ReportingPanel from '../components/channels/ReportingPanel'
import TagEditorModal from '../components/channels/TagEditorModal'
import VideoCard from '../components/channels/VideoCard'
import { api } from '../lib/api'
import {
  extractDescriptionTags,
  extractTitleTokens,
  formatBeijingDateTime,
  formatDateTime,
  formatNum,
  formatSigned,
  hasDownloadToken,
  normalizeTagList,
  normalizeTagValue,
  parseJobPayload,
  parseTagInputText,
  parseTagsJson,
  relTime,
  resolveChannelHandle,
  resolveChannelUrl,
  resolveTimestampSeconds,
  resolveVideoUrl,
  toNullableNumber,
  topEntries,
  type VideoType,
} from '../lib/channelHelpers'
import type {
  AnalyticsDailyRow,
  AnalyticsKpi,
  ApiChannel,
  ApiJob,
  ApiVideo,
  ChannelReportingDailyRow,
  ChannelReportingSummary,
  ChannelReportingVideoRow,
} from '../types'

const ALL_TAG = '__all__'

type TabKey = 'videos' | 'analytics' | 'reports'
type VideoSort = 'most_recent' | 'most_viewed' | 'views_7d'

function downloadJson(filename: string, payload: unknown) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.click()
  URL.revokeObjectURL(url)
}

export default function ChannelsPage() {
  const { channelId } = useParams()
  const navigate = useNavigate()
  const decodedChannelId = channelId ? decodeURIComponent(channelId) : ''

  const [channels, setChannels] = useState<ApiChannel[]>([])
  const [channelsLoading, setChannelsLoading] = useState(true)
  const [tagSearch, setTagSearch] = useState('')
  const [sidebarSearch, setSidebarSearch] = useState('')
  const [tagFilter, setTagFilter] = useState(ALL_TAG)
  const [activeTab, setActiveTab] = useState<TabKey>('videos')
  const [videoType, setVideoType] = useState<VideoType>('long')
  const [videoSort, setVideoSort] = useState<VideoSort>('most_recent')
  const [videos, setVideos] = useState<ApiVideo[]>([])
  const [videosLoading, setVideosLoading] = useState(false)
  const [analyticsKpi, setAnalyticsKpi] = useState<AnalyticsKpi | null>(null)
  const [analyticsDailyRows, setAnalyticsDailyRows] = useState<AnalyticsDailyRow[]>([])
  const [analyticsVideos, setAnalyticsVideos] = useState<ApiVideo[]>([])
  const [analyticsLoading, setAnalyticsLoading] = useState(false)
  const [reportingSummary, setReportingSummary] = useState<ChannelReportingSummary | null>(null)
  const [reportingDailyRows, setReportingDailyRows] = useState<ChannelReportingDailyRow[]>([])
  const [reportingVideos, setReportingVideos] = useState<ChannelReportingVideoRow[]>([])
  const [reportingLoading, setReportingLoading] = useState(false)
  const [selectedVideoIds, setSelectedVideoIds] = useState<string[]>([])
  const [expandedDescriptionVideoId, setExpandedDescriptionVideoId] = useState('')
  const [copyingSelectedVideoLinks, setCopyingSelectedVideoLinks] = useState(false)
  const [copiedSelectedVideoLinks, setCopiedSelectedVideoLinks] = useState(false)
  const [showInsightDescription, setShowInsightDescription] = useState(false)
  const [jobs, setJobs] = useState<ApiJob[]>([])
  const [errorText, setErrorText] = useState('')
  const [managingChannels, setManagingChannels] = useState(false)
  const [refreshingChannelId, setRefreshingChannelId] = useState('')
  const [showAddModal, setShowAddModal] = useState(false)
  const [addInput, setAddInput] = useState('')
  const [addError, setAddError] = useState('')
  const [adding, setAdding] = useState(false)
  const [showTagEdit, setShowTagEdit] = useState(false)
  const [tagEditChannelId, setTagEditChannelId] = useState('')
  const [tagDraft, setTagDraft] = useState<string[]>([])
  const [tagDraftInput, setTagDraftInput] = useState('')
  const [tagSuggestionFocusIndex, setTagSuggestionFocusIndex] = useState(-1)
  const [tagEditError, setTagEditError] = useState('')
  const [deletingTag, setDeletingTag] = useState('')
  const [exportingChannelLinks, setExportingChannelLinks] = useState(false)
  const [exportedChannelLinks, setExportedChannelLinks] = useState(false)
  const [exportingInsight, setExportingInsight] = useState(false)

  const loadChannels = async (silent = false) => {
    if (!silent) setChannelsLoading(true)
    try {
      const response = await api.getChannels({ platform: 'youtube', limit: 1000, sort: 'recent' })
      setChannels(response.data)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '频道加载失败')
    } finally {
      if (!silent) setChannelsLoading(false)
    }
  }

  const loadVideos = async (channelRef: string, silent = false) => {
    if (!silent) setVideosLoading(true)
    try {
      const response = await api.getVideos({
        channel_id: channelRef,
        platform: 'youtube',
        type: videoType,
        sort: videoSort,
        limit: 120,
      })
      setVideos(response.data)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '视频加载失败')
    } finally {
      if (!silent) setVideosLoading(false)
    }
  }

  const loadAnalytics = async (channelRef: string, silent = false) => {
    if (!silent) setAnalyticsLoading(true)
    try {
      const [kpi, daily, videosRes] = await Promise.all([
        api.getKpi(channelRef, '28d'),
        api.getDailyTable(channelRef, { range: '28d', limit: 28 }),
        api.getVideos({ channel_id: channelRef, platform: 'youtube', limit: 300, sort: 'most_viewed' }),
      ])
      setAnalyticsKpi(kpi)
      setAnalyticsDailyRows(daily.data)
      setAnalyticsVideos(videosRes.data)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '数据洞察加载失败')
    } finally {
      if (!silent) setAnalyticsLoading(false)
    }
  }

  const loadReporting = async (channelRef: string, silent = false) => {
    if (!silent) setReportingLoading(true)
    try {
      const [summary, daily, videosRes] = await Promise.all([
        api.getChannelReportingSummary(channelRef),
        api.getChannelReportingDaily(channelRef, { range: '28d' }),
        api.getChannelReportingVideos(channelRef, { range: '28d', limit: 100 }),
      ])
      setReportingSummary(summary)
      setReportingDailyRows(daily.data)
      setReportingVideos(videosRes.data)
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '报表数据加载失败')
    } finally {
      if (!silent) setReportingLoading(false)
    }
  }

  useEffect(() => {
    void loadChannels()
  }, [])

  const tagStats = useMemo(() => {
    const freq = new Map<string, number>()
    channels.forEach((item) => {
      parseTagsJson(item.tags_json).forEach((tag) => freq.set(tag, (freq.get(tag) || 0) + 1))
    })
    return Array.from(freq.entries())
      .map(([tag, count]) => ({ tag, count }))
      .sort((a, b) => a.tag.localeCompare(b.tag, 'zh-Hans-CN'))
  }, [channels])

  const visibleTagOptions = useMemo(() => {
    const keyword = tagSearch.trim().toLowerCase()
    const base = [{ value: ALL_TAG, label: '全部', count: channels.length }, ...tagStats.map((item) => ({ value: item.tag, label: item.tag, count: item.count }))]
    return keyword ? base.filter((item) => item.value === ALL_TAG || item.label.toLowerCase().includes(keyword)) : base
  }, [channels.length, tagSearch, tagStats])

  const filteredChannels = useMemo(
    () =>
      channels.filter((item) => {
        const keyword = sidebarSearch.trim().toLowerCase()
        const matchesSearch =
          !keyword
          || item.title.toLowerCase().includes(keyword)
          || String(item.handle || '').toLowerCase().includes(keyword)
        return matchesSearch && (tagFilter === ALL_TAG || parseTagsJson(item.tags_json).includes(tagFilter))
      }),
    [channels, sidebarSearch, tagFilter],
  )

  const channel = useMemo(() => {
    if (decodedChannelId) return channels.find((item) => item.channel_id === decodedChannelId) || null
    return filteredChannels[0] || channels[0] || null
  }, [channels, decodedChannelId, filteredChannels])

  useEffect(() => {
    setExpandedDescriptionVideoId('')
  }, [channel?.channel_id, videoSort, videoType])

  const tagInputSuggestions = useMemo(() => {
    const parts = String(tagDraftInput || '').split(/[,\uFF0C\n]+/)
    const keyword = normalizeTagValue(parts[parts.length - 1] || '').toLowerCase()
    if (!keyword) return []
    const selected = new Set(tagDraft.map((item) => item.toLowerCase()))
    return tagStats.filter((item) => !selected.has(item.tag.toLowerCase()) && item.tag.toLowerCase().includes(keyword)).slice(0, 8)
  }, [tagDraft, tagDraftInput, tagStats])

  const analyticsInsight = useMemo(() => {
    const totalSubscribers = toNullableNumber(channel?.subscriber_count)
    const totalVideos = analyticsVideos.length
    if (totalVideos === 0) {
      return {
        firstVideoTime: 'N/A',
        totalSubscribers,
        totalVideos: 0,
        totalViews: 0,
        avgViews: 0,
        avgMonthlyUploads: 0,
        topTitleKeywords: [] as Array<{ term: string; count: number }>,
        topDescriptionTags: [] as Array<{ term: string; count: number }>,
      }
    }

    let totalViews = 0
    const timestamps: number[] = []
    const titleTokenFreq = new Map<string, number>()
    const descTagFreq = new Map<string, number>()

    for (const video of analyticsVideos) {
      const viewCount = toNullableNumber(video.view_count) ?? toNullableNumber(video.latest_views)
      if (viewCount != null) totalViews += Math.trunc(viewCount)
      const ts = resolveTimestampSeconds(undefined, video.published_at)
      if (ts != null) timestamps.push(ts)
      for (const token of extractTitleTokens(video.title || '')) titleTokenFreq.set(token, (titleTokenFreq.get(token) || 0) + 1)
      for (const tag of extractDescriptionTags(video)) descTagFreq.set(tag, (descTagFreq.get(tag) || 0) + 1)
    }

    const firstTs = timestamps.length > 0 ? Math.min(...timestamps) : null
    const latestTs = timestamps.length > 0 ? Math.max(...timestamps) : null
    const avgViews = totalVideos > 0 ? totalViews / totalVideos : 0
    const avgMonthlyUploads = firstTs != null && latestTs != null
      ? totalVideos / Math.max(1, (new Date(latestTs * 1000).getUTCFullYear() - new Date(firstTs * 1000).getUTCFullYear()) * 12 + (new Date(latestTs * 1000).getUTCMonth() - new Date(firstTs * 1000).getUTCMonth()) + 1)
      : totalVideos

    return {
      firstVideoTime: formatBeijingDateTime(firstTs),
      totalSubscribers,
      totalVideos,
      totalViews,
      avgViews,
      avgMonthlyUploads,
      topTitleKeywords: topEntries(titleTokenFreq, 20),
      topDescriptionTags: topEntries(descTagFreq, 20),
    }
  }, [analyticsVideos, channel?.subscriber_count])

  const activeSyncChannelIds = useMemo(() => {
    const next = new Set<string>()
    jobs.forEach((job) => {
      if (job.type !== 'sync_channel' || !['queued', 'running', 'canceling'].includes(job.status)) return
      const id = String(parseJobPayload(job.payload_json).channel_id || '').trim()
      if (id) next.add(id)
    })
    return next
  }, [jobs])

  const progressState = useMemo(() => {
    const metaProgressById: Record<string, number> = {}
    jobs.forEach((job) => {
      if (!['queued', 'running', 'canceling'].includes(job.status)) return
      const videoId = String(parseJobPayload(job.payload_json).video_id || '').trim()
      if (!videoId) return
      const progress = Math.max(2, Math.min(100, Number(job.progress || 0)))
      if (job.type === 'download_meta' || job.type === 'download_all') metaProgressById[videoId] = Math.max(metaProgressById[videoId] || 0, progress)
    })
    return { metaProgressById }
  }, [jobs])

  const visibleSelectableVideoIds = useMemo(() => videos.map((video) => video.video_id), [videos])
  const selectedVisibleCount = useMemo(() => visibleSelectableVideoIds.filter((id) => selectedVideoIds.includes(id)).length, [selectedVideoIds, visibleSelectableVideoIds])
  const allVisibleSelected = visibleSelectableVideoIds.length > 0 && selectedVisibleCount === visibleSelectableVideoIds.length
  const growthDomain = useMemo(() => {
    const values = videos
      .map((video) => toNullableNumber(video.daily_view_increase))
      .filter((value): value is number => value != null)
    if (values.length === 0) return null

    const sorted = [...values].sort((a, b) => a - b)
    const quantile = (q: number) => {
      if (sorted.length === 1) return sorted[0]
      const index = (sorted.length - 1) * q
      const lo = Math.floor(index)
      const hi = Math.ceil(index)
      if (lo === hi) return sorted[lo]
      const t = index - lo
      return sorted[lo] * (1 - t) + sorted[hi] * t
    }

    const q1 = quantile(0.25)
    const q3 = quantile(0.75)
    const iqr = Math.max(1, q3 - q1)
    const min = Math.min(0, q1 - iqr * 1.2)
    const max = Math.max(0, q3 + iqr * 2.4)
    return max <= min ? { min: min - 1, max: max + 1 } : { min, max }
  }, [videos])

  useEffect(() => {
    if (channel && (!decodedChannelId || decodedChannelId !== channel.channel_id)) {
      navigate(`/channels/${encodeURIComponent(channel.channel_id)}`, { replace: true })
    }
  }, [channel, decodedChannelId, navigate])

  useEffect(() => {
    if (channel && activeTab === 'videos') {
      setSelectedVideoIds([])
      void loadVideos(channel.channel_id)
    }
  }, [activeTab, channel?.channel_id, videoSort, videoType])

  useEffect(() => {
    if (channel && activeTab === 'analytics') void loadAnalytics(channel.channel_id)
  }, [activeTab, channel?.channel_id])

  useEffect(() => {
    if (channel && activeTab === 'reports') void loadReporting(channel.channel_id)
  }, [activeTab, channel?.channel_id])

  useEffect(() => {
    if (tagInputSuggestions.length === 0) {
      if (tagSuggestionFocusIndex !== -1) setTagSuggestionFocusIndex(-1)
      return
    }
    if (tagSuggestionFocusIndex >= tagInputSuggestions.length) setTagSuggestionFocusIndex(0)
  }, [tagInputSuggestions, tagSuggestionFocusIndex])

  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      try {
        const response = await api.getJobs({ limit: 80 })
        if (cancelled) return
        setJobs(response.data)
        if (!response.data.some((job) => ['queued', 'running', 'canceling'].includes(job.status))) return
        await loadChannels(true)
        if (channel?.channel_id && activeTab === 'videos') await loadVideos(channel.channel_id, true)
        if (channel?.channel_id && activeTab === 'analytics') await loadAnalytics(channel.channel_id, true)
        if (channel?.channel_id && activeTab === 'reports') await loadReporting(channel.channel_id, true)
      } catch {
        // ignore polling failures
      }
    }
    void poll()
    const timer = window.setInterval(() => void poll(), 2000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [activeTab, channel?.channel_id, videoSort, videoType])

  const submitAddChannel = async () => {
    if (!addInput.trim()) {
      setAddError('请输入 YouTube 频道链接、@Handle 或 Channel ID')
      return
    }
    setAdding(true)
    setAddError('')
    try {
      const created = await api.addChannel({ channel_id: addInput.trim(), title: addInput.trim(), source_url: addInput.trim(), platform: 'youtube' })
      await loadChannels(true)
      setShowAddModal(false)
      setAddInput('')
      navigate(`/channels/${encodeURIComponent(created.channel_id)}`, { replace: true })
    } catch (error) {
      const typedError = error as Error & { data?: { channel_id?: string } }
      const existing = String(typedError?.data?.channel_id || '').trim()
      if (existing) {
        navigate(`/channels/${encodeURIComponent(existing)}`, { replace: true })
        setShowAddModal(false)
        setAddInput('')
      } else {
        setAddError(typedError.message)
      }
    } finally {
      setAdding(false)
    }
  }

  const handleDeleteChannel = async (targetId: string) => {
    try {
      await api.deleteChannel(targetId)
      const next = channels.filter((item) => item.channel_id !== targetId)
      setChannels(next)
      if (channel?.channel_id === targetId) navigate(next[0] ? `/channels/${encodeURIComponent(next[0].channel_id)}` : '/channels', { replace: true })
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '删除频道失败')
    }
  }

  const handleSaveTags = async () => {
    if (!tagEditChannelId) return
    try {
      await api.updateChannel(tagEditChannelId, { tags: normalizeTagList([...tagDraft, ...parseTagInputText(tagDraftInput)]) })
      await loadChannels(true)
      setShowTagEdit(false)
      setTagEditError('')
    } catch (error) {
      setTagEditError(error instanceof Error ? error.message : '保存标签失败')
    }
  }

  const handleToggleVideoSelected = (videoId: string) => {
    setSelectedVideoIds((prev) => (prev.includes(videoId) ? prev.filter((item) => item !== videoId) : [...prev, videoId]))
  }

  const handleToggleSelectAllVisible = () => {
    setSelectedVideoIds((prev) => {
      if (allVisibleSelected) return prev.filter((id) => !visibleSelectableVideoIds.includes(id))
      const next = new Set(prev)
      visibleSelectableVideoIds.forEach((id) => next.add(id))
      return Array.from(next)
    })
  }

  const handleCopyVideoLink = async (video: ApiVideo) => {
    try {
      await navigator.clipboard.writeText(resolveVideoUrl(video))
    } catch {
      setErrorText('视频链接复制失败')
    }
  }

  const handleCopySelectedVideoLinks = async () => {
    const links = videos.filter((video) => selectedVideoIds.includes(video.video_id)).map((video) => resolveVideoUrl(video)).join('\n')
    if (!links) return
    setCopyingSelectedVideoLinks(true)
    try {
      await navigator.clipboard.writeText(links)
      setCopiedSelectedVideoLinks(true)
      window.setTimeout(() => setCopiedSelectedVideoLinks(false), 1600)
    } catch {
      setErrorText('复制选中链接失败')
    } finally {
      setCopyingSelectedVideoLinks(false)
    }
  }

  const handleExportChannelLinks = async () => {
    setExportingChannelLinks(true)
    try {
      await navigator.clipboard.writeText(channels.map((item) => resolveChannelUrl(item)).join('\n'))
      setExportedChannelLinks(true)
      window.setTimeout(() => setExportedChannelLinks(false), 1600)
    } catch {
      setErrorText('频道链接复制失败')
    } finally {
      setExportingChannelLinks(false)
    }
  }

  const handleExportInsight = async () => {
    if (!channel) return
    setExportingInsight(true)
    try {
      downloadJson(`${channel.channel_id}-insight.json`, { channel, analyticsKpi, analyticsDailyRows, analyticsInsight, analyticsVideos })
    } finally {
      setExportingInsight(false)
    }
  }

  const emptySidebar = (
    <ChannelSidebar
      channels={channels}
      filteredChannels={filteredChannels}
      channelId=""
      channelsLoading={channelsLoading}
      tagSearch={tagSearch}
      sidebarSearch={sidebarSearch}
      tagFilter={tagFilter}
      tagOptions={visibleTagOptions}
      managingChannels={managingChannels}
      exportingChannelLinks={exportingChannelLinks}
      exportedChannelLinks={exportedChannelLinks}
      onTagSearchChange={setTagSearch}
      onSidebarSearchChange={setSidebarSearch}
      onTagFilterChange={setTagFilter}
      onSelectChannel={(id) => navigate(`/channels/${encodeURIComponent(id)}`)}
      onToggleManage={() => setManagingChannels((prev) => !prev)}
      onDeleteChannel={(id) => void handleDeleteChannel(id)}
      onOpenAdd={() => {
        setAddError('')
        setShowAddModal(true)
      }}
      onExportLinks={() => void handleExportChannelLinks()}
    />
  )

  if (!channel && !channelsLoading) {
    return (
      <div className="channel-page-shell">
        {emptySidebar}
        <div className="card-flat" style={{ flex: 1, minHeight: 360, display: 'grid', placeItems: 'center' }}>
          <div className="empty-state">
            <div className="empty-state-title">没有可展示的频道</div>
            去左侧添加一个 YouTube 频道。          </div>
        </div>
        <AddChannelModal open={showAddModal} value={addInput} errorText={addError} adding={adding} onChange={setAddInput} onClose={() => setShowAddModal(false)} onSubmit={() => void submitAddChannel()} />
      </div>
    )
  }

  return (
    <div className="channel-page-shell">
      <ChannelSidebar
        channels={channels}
        filteredChannels={filteredChannels}
        channelId={channel?.channel_id || ''}
        channelsLoading={channelsLoading}
        tagSearch={tagSearch}
        sidebarSearch={sidebarSearch}
        tagFilter={tagFilter}
        tagOptions={visibleTagOptions}
        managingChannels={managingChannels}
        exportingChannelLinks={exportingChannelLinks}
        exportedChannelLinks={exportedChannelLinks}
        onTagSearchChange={setTagSearch}
        onSidebarSearchChange={setSidebarSearch}
        onTagFilterChange={setTagFilter}
        onSelectChannel={(id) => navigate(`/channels/${encodeURIComponent(id)}`)}
        onToggleManage={() => setManagingChannels((prev) => !prev)}
        onDeleteChannel={(id) => void handleDeleteChannel(id)}
        onOpenAdd={() => {
          setAddError('')
          setShowAddModal(true)
        }}
        onExportLinks={() => void handleExportChannelLinks()}
      />

      <div className="channel-main-column">
        {channel ? (
          <>
            <div className="channel-header" style={{ marginBottom: 0, paddingBottom: 20, flexShrink: 0 }}>
              <div className="channel-header-layout">
                <div className="channel-header-primary">
                  <div className="channel-header-avatar" style={{ width: 80, height: 80 }}>
                    {channel.avatar_url ? <img src={channel.avatar_url} alt="" loading="lazy" referrerPolicy="no-referrer" /> : null}
                  </div>
                  <div className="channel-header-info">
                    <div className="channel-header-top">
                      <h1 className="channel-header-title" style={{ fontSize: '1.8rem', marginBottom: 0 }}>{channel.title}</h1>
                      <button className="btn btn-primary btn-sm channel-detail-action-btn" onClick={() => window.open(resolveChannelUrl(channel), '_blank', 'noopener,noreferrer')}>访问 YouTube</button>
                      <button className="btn btn-secondary btn-sm channel-detail-action-btn" onClick={() => { setRefreshingChannelId(channel.channel_id); void api.syncChannel(channel.channel_id).then(() => loadChannels(true)).catch((error: Error) => setErrorText(error.message)).finally(() => setRefreshingChannelId('')) }} disabled={refreshingChannelId === channel.channel_id}>
                        {refreshingChannelId === channel.channel_id || activeSyncChannelIds.has(channel.channel_id) ? '执行同步中…' : '执行同步'}
                      </button>
                    </div>
                    <div className="channel-header-handle">{resolveChannelHandle(channel.handle) || channel.channel_id}</div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr auto 1fr', alignItems: 'center', gap: 12, columnGap: 16, marginTop: 8 }}>
                      <div style={{ color: 'var(--text-secondary)', fontSize: '0.96rem', minWidth: 0 }}>
                        {`${formatNum(channel.subscriber_count)} 订阅 · ${formatNum(channel.video_count)} 个视频 · ${formatNum(channel.view_count)} 次播放 · ${relTime(channel.last_sync_at)}`}
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minWidth: 340 }}>
                        <ChannelGrowthChart channel={channel} />
                      </div>
                      <div aria-hidden="true" />
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 14, marginTop: 12, flexWrap: 'wrap' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0, flexWrap: 'wrap' }}>
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          {parseTagsJson(channel.tags_json).length > 0 ? parseTagsJson(channel.tags_json).map((tag) => <span key={tag} style={{ fontSize: '0.8rem', fontWeight: 700, background: 'var(--bg-tertiary)', padding: '2px 8px', borderRadius: 4, color: '#fff' }}>#{tag}</span>) : <span style={{ fontSize: '0.75rem', color: 'var(--text-tertiary)', fontStyle: 'italic' }}>无标签</span>}
                        </div>
                        <button type="button" onClick={() => { setTagEditChannelId(channel.channel_id); setTagDraft(parseTagsJson(channel.tags_json)); setTagDraftInput(''); setTagSuggestionFocusIndex(-1); setTagEditError(''); setShowTagEdit(true) }} style={{ background: 'none', border: 'none', color: 'var(--accent-primary)', fontSize: '0.75rem', cursor: 'pointer', padding: 0 }}>编辑标签</button>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                        <div className="channel-tab-group">
                          <button className={`channel-tab-btn ${activeTab === 'videos' ? 'active' : ''}`} onClick={() => setActiveTab('videos')}>视频列表</button>
                          <button className={`channel-tab-btn ${activeTab === 'analytics' ? 'active' : ''}`} onClick={() => setActiveTab('analytics')}>数据洞察</button>
                          <button className={`channel-tab-btn ${activeTab === 'reports' ? 'active' : ''}`} onClick={() => setActiveTab('reports')}>报表</button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="channel-content-scroll">
              {errorText && <div className="tools-similar-issues" style={{ marginBottom: 12 }}>{errorText}</div>}
              {activeTab === 'videos' ? (
                <div className="card-flat" style={{ border: 'none', background: 'transparent', padding: 0 }}>
                  <div className="video-list-toolbar">
                    <div className="video-list-switch-row">
                      <span className="video-list-toolbar-label">内容类型:</span>
                      <div className="toggle-group">
                        <button className={`toggle-btn ${videoType === 'long' ? 'active' : ''}`} onClick={() => setVideoType('long')}>视频</button>
                        <button className={`toggle-btn ${videoType === 'short' ? 'active' : ''}`} onClick={() => setVideoType('short')}>Shorts</button>
                        <button className={`toggle-btn ${videoType === 'live' ? 'active' : ''}`} onClick={() => setVideoType('live')}>直播</button>
                      </div>
                      <div className="video-list-toolbar-spacer" />
                      <button className="btn btn-secondary btn-sm video-list-select-toggle-btn" onClick={handleToggleSelectAllVisible} disabled={visibleSelectableVideoIds.length === 0}>{allVisibleSelected ? '取消全选' : '全选'}</button>
                      <button className="btn btn-secondary btn-sm video-list-copy-links-btn" onClick={() => void handleCopySelectedVideoLinks()} disabled={copyingSelectedVideoLinks || selectedVisibleCount === 0}>
                        {copyingSelectedVideoLinks ? '复制中…' : copiedSelectedVideoLinks ? `已复制 ${selectedVisibleCount} 条` : `复制选中链接（${selectedVisibleCount}）`}
                      </button>
                    </div>
                    <div className="video-list-sort-row">
                      <span className="video-list-toolbar-label">排序方式:</span>
                      <button className={`video-sort-btn ${videoSort === 'most_recent' ? 'active' : ''}`} onClick={() => setVideoSort('most_recent')}>最新发布</button>
                      <button className={`video-sort-btn ${videoSort === 'most_viewed' ? 'active' : ''}`} onClick={() => setVideoSort('most_viewed')}>最多播放</button>
                      <button className={`video-sort-btn ${videoSort === 'views_7d' ? 'active' : ''}`} onClick={() => setVideoSort('views_7d')}>近7日增长</button>
                      <div className="video-list-toolbar-spacer" />
                      <span className="video-list-toolbar-count">{`共 ${videos.length} 条`}</span>
                    </div>
                  </div>

                  {videosLoading ? (
                    <div className="empty-state"><div className="empty-state-title">视频加载中</div>正在拉取频道内容。</div>
                  ) : videos.length === 0 ? (
                    <div className="empty-state"><div className="empty-state-title">暂无内容</div>当前筛选条件下没有视频。</div>
                  ) : (
                    <div className={`video-grid ${videoType === 'short' ? 'video-grid-shorts' : ''}`}>
                      {videos.map((video) => (
                        <VideoCard
                          key={video.video_id}
                          video={video}
                          selected={selectedVideoIds.includes(video.video_id)}
                          descriptionExpanded={expandedDescriptionVideoId === video.video_id}
                          metaDownloading={(progressState.metaProgressById[video.video_id] || 0) > 0 && (progressState.metaProgressById[video.video_id] || 0) < 100}
                          metaProgress={progressState.metaProgressById[video.video_id] || 0}
                          metaDownloaded={hasDownloadToken(video.download_status, 'meta')}
                          onMetaDownload={() => void api.createJob('download_meta', { video_id: video.video_id, force: hasDownloadToken(video.download_status, 'meta') }).catch((error: Error) => setErrorText(error.message))}
                          onCopyLink={() => void handleCopyVideoLink(video)}
                          onToggleDescription={() => setExpandedDescriptionVideoId((current) => current === video.video_id ? '' : video.video_id)}
                          onToggleSelect={() => handleToggleVideoSelected(video.video_id)}
                          highlightGrowthSort={videoSort === 'views_7d'}
                          forceLandscape={videoType !== 'short'}
                          growthDomain={growthDomain}
                        />
                      ))}
                    </div>
                  )}
                </div>
              ) : activeTab === 'analytics' ? (
                <div style={{ paddingTop: 20, paddingBottom: 50 }}>
                  <div className="card-flat" style={{ marginBottom: 16, border: '1px solid var(--border-subtle)', background: 'linear-gradient(180deg, rgba(99,102,241,0.08) 0%, rgba(99,102,241,0) 100%), var(--bg-card)', padding: 16 }}>
                    <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12, marginBottom: 14 }}>
                      <h3 style={{ margin: 0, fontSize: '1rem', color: 'var(--text-primary)' }}>频道数据洞察</h3>
                      <button className="btn btn-secondary btn-sm" onClick={() => void handleExportInsight()} disabled={exportingInsight || analyticsLoading || !channel?.channel_id} style={{ minWidth: 98 }}>{exportingInsight ? '导出中…' : '一键导出'}</button>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 10, marginBottom: 14 }}>
                      <div style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-subtle)', borderRadius: 8, padding: '10px 12px' }}><div style={{ color: 'var(--text-tertiary)', fontSize: '0.78rem' }}>第一条视频发布时间</div><div style={{ color: 'var(--text-primary)', marginTop: 4, fontSize: '0.92rem' }}>{analyticsInsight.firstVideoTime}</div></div>
                      <div style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-subtle)', borderRadius: 8, padding: '10px 12px' }}><div style={{ color: 'var(--text-tertiary)', fontSize: '0.78rem' }}>总视频数</div><div style={{ color: 'var(--text-primary)', marginTop: 4, fontSize: '1rem', fontWeight: 700 }}>{formatNum(analyticsInsight.totalVideos)}</div></div>
                      <div style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-subtle)', borderRadius: 8, padding: '10px 12px' }}><div style={{ color: 'var(--text-tertiary)', fontSize: '0.78rem' }}>总订阅数</div><div style={{ color: 'var(--text-primary)', marginTop: 4, fontSize: '1rem', fontWeight: 700 }}>{formatNum(analyticsInsight.totalSubscribers)}</div></div>
                      <div style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-subtle)', borderRadius: 8, padding: '10px 12px' }}><div style={{ color: 'var(--text-tertiary)', fontSize: '0.78rem' }}>总观看数</div><div style={{ color: 'var(--text-primary)', marginTop: 4, fontSize: '1rem', fontWeight: 700 }}>{formatNum(analyticsInsight.totalViews)}</div></div>
                      <div style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-subtle)', borderRadius: 8, padding: '10px 12px' }}><div style={{ color: 'var(--text-tertiary)', fontSize: '0.78rem' }}>平均观看数</div><div style={{ color: 'var(--text-primary)', marginTop: 4, fontSize: '1rem', fontWeight: 700 }}>{formatNum(Math.round(analyticsInsight.avgViews))}</div></div>
                      <div style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-subtle)', borderRadius: 8, padding: '10px 12px' }}><div style={{ color: 'var(--text-tertiary)', fontSize: '0.78rem' }}>平均月更</div><div style={{ color: 'var(--text-primary)', marginTop: 4, fontSize: '1rem', fontWeight: 700 }}>{`${analyticsInsight.avgMonthlyUploads.toFixed(2)} 条/月`}</div></div>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: 12 }}>
                      <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 8, background: 'var(--bg-card)' }}>
                        <div style={{ padding: '10px 12px', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-primary)', fontWeight: 600 }}>标题关键词 Top 20</div>
                        <div style={{ maxHeight: 220, overflowY: 'auto' }}>{analyticsInsight.topTitleKeywords.length === 0 ? <div style={{ padding: 12, color: 'var(--text-secondary)' }}>暂无可用关键词</div> : analyticsInsight.topTitleKeywords.map((item, index) => <div key={`kw-${item.term}`} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 12px', borderBottom: '1px solid var(--border-subtle)', fontSize: '0.9rem' }}><span style={{ color: 'var(--text-primary)' }}>{`${index + 1}. ${item.term}`}</span><span style={{ color: 'var(--text-secondary)' }}>{`${item.count} 次`}</span></div>)}</div>
                      </div>
                      <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 8, background: 'var(--bg-card)' }}>
                        <div style={{ padding: '10px 12px', borderBottom: '1px solid var(--border-subtle)', color: 'var(--text-primary)', fontWeight: 600 }}>描述中 tags Top 20</div>
                        <div style={{ maxHeight: 220, overflowY: 'auto' }}>{analyticsInsight.topDescriptionTags.length === 0 ? <div style={{ padding: 12, color: 'var(--text-secondary)' }}>暂无可用标签</div> : analyticsInsight.topDescriptionTags.map((item, index) => <div key={`tag-${item.term}`} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 12px', borderBottom: '1px solid var(--border-subtle)', fontSize: '0.9rem' }}><span style={{ color: 'var(--text-primary)' }}>{`${index + 1}. ${item.term}`}</span><span style={{ color: 'var(--text-secondary)' }}>{`${item.count} 次`}</span></div>)}</div>
                      </div>
                    </div>
                  </div>

                  <div className="card-flat" style={{ marginBottom: 16 }}>
                    <div className="kpi-grid">
                      <div className="kpi-card"><div className="kpi-label">近28日播放增长</div><div className="kpi-value">{formatSigned(analyticsKpi?.views)}</div></div>
                      <div className="kpi-card"><div className="kpi-label">近28日订阅增长</div><div className="kpi-value">{formatSigned(analyticsKpi?.subs)}</div></div>
                      <div className="kpi-card"><div className="kpi-label">近28日上新</div><div className="kpi-value">{formatNum(analyticsKpi?.uploads ?? 0)}</div></div>
                    </div>
                  </div>

                  <div className="card-flat" style={{ marginBottom: 16 }}>
                    {analyticsLoading ? <div className="empty-state">数据洞察加载中…</div> : (
                      <div className="table-container">
                        <table>
                          <thead><tr><th>日期</th><th>总播放</th><th>日增播放</th><th>总订阅</th><th>日增订阅</th></tr></thead>
                          <tbody>{analyticsDailyRows.map((row) => <tr key={row.date}><td>{row.date}</td><td>{formatNum(row.views_total)}</td><td>{formatSigned(row.views_change)}</td><td>{formatNum(row.subs_total)}</td><td>{formatSigned(row.subs_change)}</td></tr>)}</tbody>
                        </table>
                      </div>
                    )}
                  </div>

                  <div className="card-flat">
                    <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
                      <button type="button" className="btn btn-secondary btn-sm" onClick={() => setShowInsightDescription((prev) => !prev)}>
                        {showInsightDescription ? '隐藏描述' : '显示描述'}
                      </button>
                    </div>
                    <div className="table-container">
                      <table>
                        <thead>
                          <tr>
                            <th>标题</th>
                            {showInsightDescription && <th>描述</th>}
                            <th>发布时间</th>
                            <th>播放</th>
                            <th>点赞</th>
                            <th>评论</th>
                          </tr>
                        </thead>
                        <tbody>
                          {analyticsVideos.slice(0, 40).map((video) => (
                            <tr key={video.video_id}>
                              <td>{video.title}</td>
                              {showInsightDescription && <td style={{ maxWidth: 420, whiteSpace: 'pre-wrap' }}>{video.description || ''}</td>}
                              <td>{formatDateTime(video.published_at)}</td>
                              <td>{formatNum(video.latest_views ?? video.view_count)}</td>
                              <td>{formatNum(video.like_count ?? video.latest_likes)}</td>
                              <td>{formatNum(video.comment_count ?? video.latest_comments)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ paddingTop: 20, paddingBottom: 50 }}>
                  <ReportingPanel
                    enabled={!!reportingSummary?.enabled}
                    ownerName={reportingSummary?.owner_name || null}
                    startedAt={reportingSummary?.started_at || null}
                    latestImportedAt={reportingSummary?.latest_imported_at || null}
                    summary={reportingSummary}
                    dailyRows={reportingDailyRows}
                    videos={reportingVideos}
                    loading={reportingLoading}
                    onSync={() => {
                      if (!channel?.channel_id) return
                      void api.syncChannelReporting(channel.channel_id)
                        .then(() => loadReporting(channel.channel_id, true))
                        .catch((error: Error) => setErrorText(error.message))
                    }}
                  />
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="card-flat" style={{ flex: 1, display: 'grid', placeItems: 'center' }}>
            <div className="empty-state">频道加载中…</div>
          </div>
        )}
      </div>

      <AddChannelModal
        open={showAddModal}
        value={addInput}
        errorText={addError}
        adding={adding}
        onChange={setAddInput}
        onClose={() => setShowAddModal(false)}
        onSubmit={() => void submitAddChannel()}
      />
      <TagEditorModal
        open={showTagEdit}
        tagDraft={tagDraft}
        tagDraftInput={tagDraftInput}
        allTagStats={tagStats}
        tagInputSuggestions={tagInputSuggestions}
        tagSuggestionFocusIndex={tagSuggestionFocusIndex}
        tagEditError={tagEditError}
        deletingTag={deletingTag}
        onClose={() => setShowTagEdit(false)}
        onInputChange={(value) => {
          setTagDraftInput(value)
          setTagSuggestionFocusIndex(-1)
          setTagEditError('')
        }}
        onInputKeyDown={(event) => {
          if (event.key === 'ArrowDown') {
            event.preventDefault()
            setTagSuggestionFocusIndex((prev) => tagInputSuggestions.length === 0 ? -1 : prev < tagInputSuggestions.length - 1 ? prev + 1 : 0)
            return
          }
          if (event.key === 'ArrowUp') {
            event.preventDefault()
            setTagSuggestionFocusIndex((prev) => tagInputSuggestions.length === 0 ? -1 : prev > 0 ? prev - 1 : tagInputSuggestions.length - 1)
            return
          }
          if (event.key === 'Enter') {
            event.preventDefault()
            if (tagInputSuggestions.length > 0) {
              const safeIndex = tagSuggestionFocusIndex >= 0 ? tagSuggestionFocusIndex : 0
              const picked = tagInputSuggestions[safeIndex]?.tag
              if (picked) {
                setTagDraft((prev) => normalizeTagList([...prev, picked]))
                setTagDraftInput('')
                setTagSuggestionFocusIndex(-1)
                return
              }
            }
            const tokens = parseTagInputText(tagDraftInput)
            if (tokens.length === 0) return
            setTagDraft((prev) => normalizeTagList([...prev, ...tokens]))
            setTagDraftInput('')
            setTagSuggestionFocusIndex(-1)
            return
          }
          if (event.key === 'Escape') setTagSuggestionFocusIndex(-1)
        }}
        onAddTag={() => {
          const tokens = parseTagInputText(tagDraftInput)
          if (tokens.length === 0) return
          setTagDraft((prev) => normalizeTagList([...prev, ...tokens]))
          setTagDraftInput('')
          setTagSuggestionFocusIndex(-1)
        }}
        onRemoveDraftTag={(tag) => setTagDraft((prev) => prev.filter((item) => item !== tag))}
        onToggleDraftTag={(tag) => setTagDraft((prev) => prev.includes(tag) ? prev.filter((item) => item !== tag) : normalizeTagList([...prev, tag]))}
        onSelectSuggestion={(tag) => {
          const normalized = normalizeTagValue(tag)
          if (!normalized) return
          setTagDraft((prev) => normalizeTagList([...prev, normalized]))
          setTagDraftInput('')
          setTagSuggestionFocusIndex(-1)
        }}
        onDeleteExistingTag={(tag) => {
          const affected = channels.filter((item) => parseTagsJson(item.tags_json).includes(tag))
          if (affected.length === 0 || !window.confirm(`确认删除标签「${tag}」吗？\n将从 ${affected.length} 个频道中移除。`)) return
          setDeletingTag(tag)
          void Promise.all(affected.map((item) => api.updateChannel(item.channel_id, { tags: parseTagsJson(item.tags_json).filter((currentTag) => currentTag !== tag) })))
            .then(async () => {
              await loadChannels(true)
              setTagDraft((prev) => prev.filter((currentTag) => currentTag !== tag))
            })
            .catch((error: Error) => setTagEditError(error.message))
            .finally(() => setDeletingTag(''))
        }}
        onSave={() => void handleSaveTags()}
      />
    </div>
  )
}
