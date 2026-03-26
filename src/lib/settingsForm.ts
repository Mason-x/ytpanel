import type { AppSettingsResponse } from '../types'

export type SettingsFormState = {
  hasSavedKey: boolean
  showMaskedValue: boolean
  maskedApiKey: string
  dailySyncTime: string
  syncConcurrency: string
  downloadConcurrency: string
  hasSavedCookie: boolean
  showMaskedCookieValue: boolean
  maskedCookieValue: string
}

export type SettingsPayloadInput = {
  hasSavedKey: boolean
  showMaskedValue: boolean
  apiKey: string
  dailySyncTime: string
  syncConcurrency: string
  downloadConcurrency: string
  hasSavedCookie: boolean
  showMaskedCookieValue: boolean
  cookieValue: string
}

export function clampConcurrency(value: string) {
  const next = Math.trunc(Number(value) || 0)
  if (!Number.isFinite(next) || next < 1) return '1'
  if (next > 16) return '16'
  return String(next)
}

export function deriveSettingsFormState(settings: Partial<AppSettingsResponse>): SettingsFormState {
  const savedToken = String(settings.youtube_api_key || '').trim()
  const hasKey = Boolean(savedToken)
  const savedCookie = String(settings.yt_dlp_cookie_file_youtube || '').trim()
  const hasSavedCookie = Boolean(savedCookie)

  return {
    hasSavedKey: hasKey,
    showMaskedValue: hasKey,
    maskedApiKey: String(settings.youtube_api_key_masked_preview || '').trim(),
    dailySyncTime: String(settings.daily_sync_time || '03:00').trim() || '03:00',
    syncConcurrency: clampConcurrency(String(settings.sync_job_concurrency || '2')),
    downloadConcurrency: clampConcurrency(String(settings.download_job_concurrency || '2')),
    hasSavedCookie,
    showMaskedCookieValue: hasSavedCookie,
    maskedCookieValue: savedCookie,
  }
}

export function buildSettingsPayload(input: SettingsPayloadInput): Partial<AppSettingsResponse> {
  const nextApiKey = input.apiKey.trim()
  const nextCookieValue = input.cookieValue.trim()
  const payload: Partial<AppSettingsResponse> = {
    daily_sync_time: String(input.dailySyncTime || '').trim(),
    sync_job_concurrency: clampConcurrency(input.syncConcurrency),
    download_job_concurrency: clampConcurrency(input.downloadConcurrency),
  }

  if (!input.showMaskedValue || nextApiKey) {
    payload.youtube_api_key = nextApiKey
  }
  if (!input.showMaskedCookieValue || nextCookieValue) {
    payload.yt_dlp_cookie_file_youtube = nextCookieValue
  }

  return payload
}
