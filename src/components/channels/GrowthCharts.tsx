import { useMemo } from 'react'
import type { ApiChannel, ApiVideoGrowthPoint } from '../../types'
import { buildSparkPath, formatSigned } from '../../lib/channelHelpers'

export type VideoGrowthDomain = {
  min: number
  max: number
}

function buildPathWithDomain(
  values: number[],
  width: number,
  height: number,
  padding: number,
  domain?: VideoGrowthDomain | null,
) {
  if (values.length === 0) return ''
  if (!domain || domain.max <= domain.min) {
    return buildSparkPath(values, width, height, padding)
  }

  const min = domain.min
  const max = domain.max
  const range = Math.max(1, max - min)
  const innerWidth = Math.max(1, width - padding * 2)
  const innerHeight = Math.max(1, height - padding * 2)

  return values
    .map((value, index) => {
      const x = padding + (index / Math.max(1, values.length - 1)) * innerWidth
      const clipped = Math.max(min, Math.min(max, value))
      const y = height - padding - ((clipped - min) / range) * innerHeight
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
    })
    .join(' ')
}

function computeVideoDailyGrowth(points?: ApiVideoGrowthPoint[]) {
  const list = Array.isArray(points) ? points : []
  let prev: number | null = null

  return list
    .map((item) => {
      const raw = item?.view_count
      const current = raw == null ? null : Number(raw)
      if (current == null || !Number.isFinite(current)) return null
      if (prev == null) {
        prev = current
        return 0
      }
      const diff = Math.max(0, current - prev)
      prev = current
      return diff
    })
    .filter((item): item is number => item != null)
}

export function MiniGrowthChart({
  points,
  width = 132,
  height = 28,
  stroke = '#ff4d6d',
  domain,
}: {
  points?: ApiVideoGrowthPoint[]
  width?: number
  height?: number
  stroke?: string
  domain?: VideoGrowthDomain | null
}) {
  const values = computeVideoDailyGrowth(points)

  if (values.length < 2) {
    return <span style={{ fontSize: '0.72rem', color: 'var(--text-tertiary)' }}>样本不足</span>
  }

  const path = buildPathWithDomain(values, width, height, 3, domain)
  if (!path) {
    return <span style={{ fontSize: '0.72rem', color: 'var(--text-tertiary)' }}>暂无趋势</span>
  }

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label="近7日播放增长趋势">
      <line x1="3" y1={height - 3} x2={width - 3} y2={height - 3} stroke="rgba(255,255,255,0.1)" strokeWidth="1" />
      <path d={path} fill="none" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

export function ChannelGrowthChart({ channel }: { channel: ApiChannel }) {
  const values = useMemo(() => {
    const raw = String(channel.channel_view_growth_series_7d_json || '').trim()
    if (!raw) return []

    try {
      const parsed = JSON.parse(raw) as Array<{ view_count?: number | null }>
      return parsed
        .map((item) => Number(item?.view_count))
        .filter((item) => Number.isFinite(item)) as number[]
    } catch {
      return []
    }
  }, [channel.channel_view_growth_series_7d_json])

  const path = values.length >= 2 ? buildSparkPath(values, 220, 38, 4) : ''

  return (
    <div className="channel-growth-card">
      <div className="channel-growth-copy">
        <div className="channel-growth-label">近 7 日新增播放</div>
        <div className="channel-growth-value">{formatSigned(channel.channel_view_increase_7d ?? 0)}</div>
      </div>
      <div className="channel-growth-visual channel-growth-visual-compact">
        {path ? (
          <svg width="220" height="38" viewBox="0 0 220 38" role="img" aria-label="频道近7日播放增长趋势">
            <line x1="4" y1="34" x2="216" y2="34" stroke="rgba(255,255,255,0.08)" strokeWidth="1" />
            <path d={path} fill="none" stroke="#ff5a70" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        ) : (
          <div className="channel-growth-empty">样本不足</div>
        )}
      </div>
    </div>
  )
}
