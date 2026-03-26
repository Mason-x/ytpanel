import type { YoutubeChannelSnapshot, YoutubeVideo } from '../types'

const API_ROOT = 'https://www.googleapis.com/youtube/v3'

type YoutubeChannelsResponse = {
  items?: Array<any>
}

type YoutubePlaylistItemsResponse = {
  items?: Array<any>
}

type YoutubeVideosResponse = {
  items?: Array<any>
}

type YoutubeSearchResponse = {
  items?: Array<any>
}

function buildApiUrl(path: string, params: Record<string, string>): string {
  const search = new URLSearchParams(params)
  return `${API_ROOT}${path}?${search.toString()}`
}

async function youtubeGet<T>(path: string, params: Record<string, string>): Promise<T> {
  const res = await fetch(buildApiUrl(path, params))
  const data = await res.json().catch(() => ({}))

  if (!res.ok) {
    const message = String(data?.error?.message || res.statusText || 'YouTube API request failed')
    throw new Error(message)
  }

  return data as T
}

function parseChannelIdFromInput(input: string): string {
  const trimmed = input.trim()
  const ucMatch = trimmed.match(/(UC[a-zA-Z0-9_-]{20,})/)
  return ucMatch?.[1] || ''
}

function parseHandleFromInput(input: string): string {
  const trimmed = input.trim()
  if (!trimmed) return ''

  const directHandle = trimmed.match(/@([a-zA-Z0-9._-]{3,})/)
  if (directHandle?.[1]) return directHandle[1]

  try {
    const url = new URL(trimmed)
    const parts = url.pathname.split('/').filter(Boolean)
    const handlePart = parts.find((part) => part.startsWith('@'))
    if (handlePart) return handlePart.slice(1)
  } catch {
    // Not a URL.
  }

  return ''
}

function normalizeSearchText(input: string): string {
  return input
    .trim()
    .replace(/^https?:\/\/(www\.)?youtube\.com\//i, '')
    .replace(/^https?:\/\/youtu\.be\//i, '')
    .replace(/^channel\//i, '')
    .replace(/^@/, '')
    .trim()
}

function parseCount(value: unknown): number | null {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function pickBestThumbnail(raw: any): string {
  return String(
    raw?.maxres?.url
      || raw?.standard?.url
      || raw?.high?.url
      || raw?.medium?.url
      || raw?.default?.url
      || '',
  ).trim()
}

function parseDurationToClock(raw: string): string {
  const text = String(raw || '').trim()
  if (!text) return ''

  const match = text.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/)
  if (!match) return ''

  const hours = Number(match[1] || 0)
  const minutes = Number(match[2] || 0)
  const seconds = Number(match[3] || 0)

  if (hours > 0) return `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`
  return `${minutes}:${String(seconds).padStart(2, '0')}`
}

function parseDurationToSeconds(raw: string): number {
  const text = String(raw || '').trim()
  if (!text) return 0

  const match = text.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/)
  if (!match) return 0

  const hours = Number(match[1] || 0)
  const minutes = Number(match[2] || 0)
  const seconds = Number(match[3] || 0)
  return (hours * 3600) + (minutes * 60) + seconds
}

async function getChannelById(apiKey: string, id: string): Promise<any | null> {
  if (!id) return null
  const data = await youtubeGet<YoutubeChannelsResponse>('/channels', {
    key: apiKey,
    part: 'snippet,statistics,contentDetails',
    id,
  })
  return data.items?.[0] || null
}

async function getChannelByHandle(apiKey: string, handle: string): Promise<any | null> {
  if (!handle) return null
  const data = await youtubeGet<YoutubeChannelsResponse>('/channels', {
    key: apiKey,
    part: 'snippet,statistics,contentDetails',
    forHandle: handle,
  })
  return data.items?.[0] || null
}

async function searchChannel(apiKey: string, query: string): Promise<any | null> {
  if (!query) return null
  const search = await youtubeGet<YoutubeSearchResponse>('/search', {
    key: apiKey,
    part: 'snippet',
    q: query,
    type: 'channel',
    maxResults: '1',
  })
  const channelId = String(search.items?.[0]?.snippet?.channelId || search.items?.[0]?.id?.channelId || '').trim()
  if (!channelId) return null
  return getChannelById(apiKey, channelId)
}

async function resolveChannel(apiKey: string, input: string): Promise<any> {
  const channelId = parseChannelIdFromInput(input)
  if (channelId) {
    const byId = await getChannelById(apiKey, channelId)
    if (byId) return byId
  }

  const handle = parseHandleFromInput(input)
  if (handle) {
    const byHandle = await getChannelByHandle(apiKey, handle)
    if (byHandle) return byHandle
  }

  const query = normalizeSearchText(input)
  if (query) {
    const bySearch = await searchChannel(apiKey, query)
    if (bySearch) return bySearch
  }

  throw new Error('未找到匹配的 YouTube 频道，请检查频道链接、@handle 或频道 ID。')
}

async function getRecentVideos(apiKey: string, uploadsPlaylistId: string): Promise<YoutubeVideo[]> {
  if (!uploadsPlaylistId) return []

  const videoIds: string[] = []
  let nextPageToken = ''

  while (videoIds.length < 50) {
    const playlistData = await youtubeGet<YoutubePlaylistItemsResponse & { nextPageToken?: string }>('/playlistItems', {
      key: apiKey,
      part: 'snippet,contentDetails',
      playlistId: uploadsPlaylistId,
      maxResults: '50',
      ...(nextPageToken ? { pageToken: nextPageToken } : {}),
    })

    const baseItems = Array.isArray(playlistData.items) ? playlistData.items : []
    for (const item of baseItems) {
      const videoId = String(item?.contentDetails?.videoId || item?.snippet?.resourceId?.videoId || '').trim()
      if (!videoId || videoIds.includes(videoId)) continue
      videoIds.push(videoId)
      if (videoIds.length >= 50) break
    }

    nextPageToken = String(playlistData.nextPageToken || '').trim()
    if (!nextPageToken) break
  }

  if (videoIds.length === 0) return []

  const videosData = await youtubeGet<YoutubeVideosResponse>('/videos', {
    key: apiKey,
    part: 'snippet,statistics,contentDetails',
    id: videoIds.join(','),
    maxResults: String(videoIds.length),
  })

  const detailsById = new Map<string, any>()
  for (const item of videosData.items || []) {
    const id = String(item?.id || '').trim()
    if (!id) continue
    detailsById.set(id, item)
  }

  return videoIds.map((id) => {
    const detail = detailsById.get(id)
    const snippet = detail?.snippet
    const statistics = detail?.statistics
    const durationRaw = String(detail?.contentDetails?.duration || '')
    const durationSeconds = parseDurationToSeconds(durationRaw)
    const publishedAt = String(snippet?.publishedAt || '')
    const publishedAtMs = Date.parse(publishedAt)

    return {
      id,
      title: String(snippet?.title || 'Untitled video'),
      description: String(snippet?.description || ''),
      publishedAt,
      publishedAtMs: Number.isFinite(publishedAtMs) ? publishedAtMs : 0,
      thumbnailUrl: pickBestThumbnail(snippet?.thumbnails),
      duration: parseDurationToClock(durationRaw),
      durationSeconds,
      viewCount: parseCount(statistics?.viewCount),
      likeCount: parseCount(statistics?.likeCount),
      commentCount: parseCount(statistics?.commentCount),
      isShort: durationSeconds > 0 && durationSeconds <= 180,
    }
  })
}

export async function fetchChannelSnapshot(apiKey: string, input: string): Promise<YoutubeChannelSnapshot> {
  const trimmedKey = String(apiKey || '').trim()
  if (!trimmedKey) {
    throw new Error('请先在设置页保存 YouTube API Key。')
  }

  const channel = await resolveChannel(trimmedKey, input)
  const snippet = channel?.snippet || {}
  const stats = channel?.statistics || {}
  const uploadsPlaylistId = String(channel?.contentDetails?.relatedPlaylists?.uploads || '').trim()

  return {
    id: String(channel?.id || ''),
    title: String(snippet?.title || ''),
    handle: String(snippet?.customUrl || ''),
    description: String(snippet?.description || ''),
    customUrl: String(snippet?.customUrl || ''),
    thumbnailUrl: pickBestThumbnail(snippet?.thumbnails),
    publishedAt: String(snippet?.publishedAt || ''),
    uploadsPlaylistId,
    stats: {
      subscriberCount: parseCount(stats?.subscriberCount),
      viewCount: parseCount(stats?.viewCount),
      videoCount: parseCount(stats?.videoCount),
    },
    recentVideos: await getRecentVideos(trimmedKey, uploadsPlaylistId),
  }
}
