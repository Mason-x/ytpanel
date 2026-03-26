import type { ApiChannel, ApiVideo } from '../types'

export type VideoType = 'long' | 'short' | 'live'

const beijingDateTimeFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})

export function formatNum(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(Number(n))) return 'N/A'

  const value = Number(n)
  const abs = Math.abs(value)
  const compact = (input: number) => {
    const fixed = input.toFixed(1)
    return fixed.endsWith('.0') ? fixed.slice(0, -2) : fixed
  }

  if (abs >= 1e12) return `${compact(value / 1e12)}万亿`
  if (abs >= 1e8) return `${compact(value / 1e8)}亿`
  if (abs >= 1e4) return `${compact(value / 1e4)}万`
  return `${Math.round(value)}`
}

export function formatSigned(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(Number(n))) return 'N/A'
  const value = Number(n)
  return `${value > 0 ? '+' : ''}${formatNum(value)}`
}

export function formatDateTime(value?: string | null): string {
  const timestampSec = resolveTimestampSeconds(undefined, value)
  return formatBeijingDateTime(timestampSec)
}

export function formatBeijingDateTime(timestampSec: number | null): string {
  if (timestampSec == null) return 'N/A'
  const date = new Date(timestampSec * 1000)
  if (Number.isNaN(date.getTime())) return 'N/A'

  const parts = beijingDateTimeFormatter.formatToParts(date)
  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]))
  return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`
}

export function relTime(value?: string | null): string {
  const timestampSec = resolveTimestampSeconds(undefined, value)
  if (timestampSec == null) return 'N/A'

  const diffMs = Date.now() - (timestampSec * 1000)
  const mins = Math.floor(diffMs / 60000)
  if (mins < 1) return '刚刚'
  if (mins < 60) return `${mins}分钟前`

  const hours = Math.floor(mins / 60)
  if (hours < 24) return `${hours}小时前`

  const days = Math.floor(hours / 24)
  if (days < 30) return `${days}天前`
  if (days < 365) return `${Math.floor(days / 30)}个月前`
  return `${Math.floor(days / 365)}年前`
}

export function formatDuration(sec: number | null | undefined): string {
  if (sec == null || sec <= 0) return ''
  const total = Math.max(0, Math.round(sec))
  const hours = Math.floor(total / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  const seconds = total % 60

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`
  }

  return `${minutes}:${String(seconds).padStart(2, '0')}`
}

export function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

export function normalizeTagValue(raw: unknown): string {
  if (typeof raw !== 'string') return ''
  return raw.trim().replace(/^#+/, '')
}

export function normalizeTagList(values: unknown[]): string[] {
  const next: string[] = []
  const seen = new Set<string>()

  for (const raw of values) {
    const normalized = normalizeTagValue(raw)
    if (!normalized) continue

    const key = normalized.toLowerCase()
    if (seen.has(key)) continue

    seen.add(key)
    next.push(normalized)
  }

  return next
}

export function parseTagInputText(raw: string): string[] {
  if (!raw.trim()) return []
  return normalizeTagList(raw.split(/[,\uFF0C\n]+/))
}

export function parseTagsJson(raw: unknown): string[] {
  if (Array.isArray(raw)) return normalizeTagList(raw)
  if (typeof raw !== 'string') return []

  try {
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? normalizeTagList(parsed) : []
  } catch {
    return []
  }
}

export function resolveChannelHandle(handle?: string | null): string {
  const normalized = String(handle || '').trim()
  if (!normalized) return ''
  return normalized.startsWith('@') ? normalized : `@${normalized}`
}

export function resolveChannelUrl(channel: ApiChannel): string {
  const sourceUrl = String(channel.source_url || '').trim()
  if (/^https?:\/\//i.test(sourceUrl)) return sourceUrl

  const handle = String(channel.handle || '').trim().replace(/^@/, '')
  if (handle) return `https://www.youtube.com/@${encodeURIComponent(handle)}`

  return `https://www.youtube.com/channel/${channel.channel_id}`
}

export function resolveVideoUrl(video: ApiVideo): string {
  const pageUrl = String(video.webpage_url || '').trim()
  if (/^https?:\/\//i.test(pageUrl)) return pageUrl
  return `https://www.youtube.com/watch?v=${video.video_id}`
}

export function resolveVideoThumb(video: ApiVideo): string {
  const localThumb = String(video.local_thumb_url || '').trim()
  if (localThumb) return localThumb
  return `https://i.ytimg.com/vi/${video.video_id}/hqdefault.jpg`
}

export function viewVelocity(video: ApiVideo): number {
  const views = Number(video.latest_views ?? video.view_count ?? 0)
  if (!Number.isFinite(views) || views <= 0) return 0

  const publishedMs = Date.parse(String(video.published_at || ''))
  const days = Number.isFinite(publishedMs)
    ? Math.max(0.25, (Date.now() - publishedMs) / 86400000)
    : 365

  return views / days
}

export function extractTitleTokens(title: string): string[] {
  const stopWords = new Set(['the', 'a', 'an', 'to', 'of', 'and', 'or', 'for', 'with', 'this', 'that'])
  const matches = title.match(/[\u4e00-\u9fff]{2,}|[A-Za-z0-9][A-Za-z0-9'_-]{1,}/g) || []

  return matches
    .map((token) => token.trim().toLowerCase())
    .filter((token) => token.length >= 2)
    .filter((token) => !stopWords.has(token))
}

export function extractDescriptionTags(video: Pick<ApiVideo, 'description'> | { description?: string | null }): string[] {
  const text = typeof video.description === 'string' ? video.description : ''
  const matches = text.match(/#[\p{L}\p{N}_-]+/gu) || []
  return normalizeTagList(matches.map((item) => item.replace(/^#/, '')))
}

export function topEntries(freq: Map<string, number>, limit = 12): Array<{ term: string; count: number }> {
  return Array.from(freq.entries())
    .map(([term, count]) => ({ term, count }))
    .sort((a, b) => (b.count - a.count) || a.term.localeCompare(b.term, 'zh-Hans-CN'))
    .slice(0, limit)
}

export function downloadStatusTokens(raw: unknown): Set<string> {
  return new Set(
    String(raw || '')
      .toLowerCase()
      .split(/[+,| ]+/)
      .map((item) => item.trim())
      .filter(Boolean),
  )
}

export function hasDownloadToken(raw: unknown, token: string): boolean {
  return downloadStatusTokens(raw).has(token.toLowerCase())
}

export function parseJobPayload(raw: unknown): Record<string, unknown> {
  if (typeof raw !== 'string' || !raw.trim()) return {}

  try {
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {}
  } catch {
    return {}
  }
}

export function getVideoTypeLabel(type: VideoType): string {
  if (type === 'short') return 'Shorts'
  if (type === 'live') return '直播'
  return '视频'
}

export function resolveTimestampSeconds(timestamp: unknown, publishedAt: unknown): number | null {
  const ts = toNullableNumber(timestamp)
  if (ts != null) return Math.trunc(ts)

  if (typeof publishedAt !== 'string') return null
  const raw = publishedAt.trim()
  if (!raw) return null

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const [year, month, day] = raw.split('-').map((item) => parseInt(item, 10))
    if (!year || !month || !day) return null
    const utcMs = Date.UTC(year, month - 1, day, 0, 0, 0) - (8 * 3600 * 1000)
    return Math.floor(utcMs / 1000)
  }

  const parsedMs = Date.parse(raw)
  if (!Number.isFinite(parsedMs)) return null
  return Math.floor(parsedMs / 1000)
}

export function buildSparkPath(values: number[], width: number, height: number, padding = 4): string {
  if (values.length === 0) return ''

  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = Math.max(1, max - min)
  const innerWidth = Math.max(1, width - padding * 2)
  const innerHeight = Math.max(1, height - padding * 2)

  return values
    .map((value, index) => {
      const x = padding + (index / Math.max(1, values.length - 1)) * innerWidth
      const y = height - padding - ((value - min) / range) * innerHeight
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
    })
    .join(' ')
}
