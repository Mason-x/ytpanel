import type {
  ChannelReportingDailyRow,
  ChannelReportingSummary,
  ChannelReportingVideoRow,
} from '../../types'

type Props = {
  enabled: boolean
  ownerName: string | null
  startedAt: string | null
  latestImportedAt: string | null
  summary: ChannelReportingSummary | null
  dailyRows: ChannelReportingDailyRow[]
  videos: ChannelReportingVideoRow[]
  loading: boolean
  onSync: () => void
}

function formatNumber(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return 'N/A'
  return new Intl.NumberFormat('zh-CN').format(value)
}

function formatPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return 'N/A'
  return `${(value * 100).toFixed(2)}%`
}

function formatDuration(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return 'N/A'
  if (value < 60) return `${value.toFixed(1)} 秒`
  const minutes = Math.floor(value / 60)
  const seconds = Math.round(value % 60)
  return `${minutes} 分 ${seconds} 秒`
}

function renderTrafficShares(trafficSourceShareJson?: string | null) {
  let parsed: Record<string, number> = {}
  try {
    parsed = JSON.parse(String(trafficSourceShareJson || '{}'))
  } catch {
    parsed = {}
  }
  const entries = Object.entries(parsed)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)

  if (entries.length === 0) return <div className="empty-state">暂无流量来源数据</div>

  return (
    <div style={{ display: 'grid', gap: 8 }}>
      {entries.map(([source, value]) => (
        <div key={source} style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
          <span>{source}</span>
          <strong>{formatPercent(value)}</strong>
        </div>
      ))}
    </div>
  )
}

export default function ReportingPanel({
  enabled,
  ownerName,
  startedAt,
  latestImportedAt,
  summary,
  dailyRows,
  videos,
  loading,
  onSync,
}: Props) {
  if (!enabled) {
    return (
      <div className="card-flat">
        <div className="empty-state">该频道尚未启用 Reporting API。</div>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="card-flat">
        <div className="empty-state">报表加载中...</div>
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div className="card-flat">
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontWeight: 700 }}>YouTube Reporting 报表指标</div>
            <div className="form-help">{`Owner: ${ownerName || 'N/A'} · 启用日期: ${startedAt || 'N/A'} · 最近导入: ${latestImportedAt || 'N/A'}`}</div>
          </div>
          <button type="button" className="btn btn-secondary btn-sm" onClick={onSync}>同步报表</button>
        </div>
      </div>

      <div className="card-flat">
        <div className="kpi-grid">
          <div className="kpi-card"><div className="kpi-label">展现量</div><div className="kpi-value">{formatNumber(summary?.impressions)}</div></div>
          <div className="kpi-card"><div className="kpi-label">点击率</div><div className="kpi-value">{formatPercent(summary?.impressions_ctr)}</div></div>
          <div className="kpi-card"><div className="kpi-label">平均观看时长</div><div className="kpi-value">{formatDuration(summary?.avg_view_duration_seconds)}</div></div>
          <div className="kpi-card"><div className="kpi-label">平均观看百分比</div><div className="kpi-value">{formatPercent(summary?.avg_view_percentage)}</div></div>
        </div>
      </div>

      <div className="card-flat">
        <div className="panel-header">
          <div>
            <h3>流量来源占比</h3>
            <p>按最新报表汇总的视频来源结构。</p>
          </div>
        </div>
        {renderTrafficShares(summary?.traffic_source_share_json)}
      </div>

      <div className="card-flat">
        <div className="panel-header">
          <div>
            <h3>每日趋势</h3>
            <p>近 28 天报表指标的按日聚合。</p>
          </div>
        </div>
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>日期</th>
                <th>展现量</th>
                <th>点击率</th>
                <th>平均观看时长</th>
                <th>平均观看百分比</th>
              </tr>
            </thead>
            <tbody>
              {dailyRows.length === 0 ? (
                <tr><td colSpan={5}>暂无日报数据</td></tr>
              ) : dailyRows.map((row) => (
                <tr key={row.date}>
                  <td>{row.date}</td>
                  <td>{formatNumber(row.impressions)}</td>
                  <td>{formatPercent(row.impressions_ctr)}</td>
                  <td>{formatDuration(row.avg_view_duration_seconds)}</td>
                  <td>{formatPercent(row.avg_view_percentage)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card-flat">
        <div className="panel-header">
          <div>
            <h3>视频明细</h3>
            <p>当前频道按视频拆分的 Reporting 指标。</p>
          </div>
        </div>
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>日期</th>
                <th>视频</th>
                <th>展现量</th>
                <th>点击率</th>
                <th>平均观看时长</th>
                <th>平均观看百分比</th>
              </tr>
            </thead>
            <tbody>
              {videos.length === 0 ? (
                <tr><td colSpan={6}>暂无视频报表数据</td></tr>
              ) : videos.map((video) => (
                <tr key={`${video.date}-${video.video_id}`}>
                  <td>{video.date}</td>
                  <td>{video.title || video.video_id}</td>
                  <td>{formatNumber(video.impressions)}</td>
                  <td>{formatPercent(video.impressions_ctr)}</td>
                  <td>{formatDuration(video.avg_view_duration_seconds)}</td>
                  <td>{formatPercent(video.avg_view_percentage)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
