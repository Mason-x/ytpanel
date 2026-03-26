import type { AppSettings, StoredChannel } from '../types'

const SETTINGS_KEY = 'ytpanel:settings'
const CHANNELS_KEY = 'ytpanel:channels'

const DEFAULT_SETTINGS: AppSettings = {
  youtubeApiKey: '',
}

function safeJsonParse<T>(raw: string | null, fallback: T): T {
  if (!raw) return fallback
  try {
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}

export function loadSettings(): AppSettings {
  if (typeof window === 'undefined') return DEFAULT_SETTINGS
  const parsed = safeJsonParse<Partial<AppSettings>>(window.localStorage.getItem(SETTINGS_KEY), {})
  return {
    ...DEFAULT_SETTINGS,
    youtubeApiKey: String(parsed.youtubeApiKey || '').trim(),
  }
}

export function saveSettings(settings: AppSettings): void {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(SETTINGS_KEY, JSON.stringify({
    youtubeApiKey: String(settings.youtubeApiKey || '').trim(),
  }))
}

export function loadChannels(): StoredChannel[] {
  if (typeof window === 'undefined') return []
  const parsed = safeJsonParse<StoredChannel[]>(window.localStorage.getItem(CHANNELS_KEY), [])
  if (!Array.isArray(parsed)) return []
  return parsed.map((item) => ({
    ...item,
    tags: Array.isArray(item?.tags)
      ? item.tags.filter((tag): tag is string => typeof tag === 'string').map((tag) => tag.trim()).filter(Boolean)
      : [],
  }))
}

export function saveChannels(channels: StoredChannel[]): void {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(CHANNELS_KEY, JSON.stringify(channels))
}
