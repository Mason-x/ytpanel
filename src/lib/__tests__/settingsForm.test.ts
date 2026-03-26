import test from 'node:test'
import assert from 'node:assert/strict'
import { deriveSettingsFormState, buildSettingsPayload } from '../settingsForm'

test('deriveSettingsFormState reads yt-dlp youtube cookie setting', () => {
  const state = deriveSettingsFormState({
    youtube_api_key: '__VR_MASKED_SETTING__:youtube_api_binding:0',
    youtube_api_key_masked_preview: 'AIza***abcd',
    yt_dlp_cookie_file_youtube: '__VR_MASKED_SETTING__:yt_dlp_cookie_file_youtube:0',
    daily_sync_time: '04:30',
    sync_job_concurrency: '3',
    download_job_concurrency: '4',
  })

  assert.equal(state.hasSavedCookie, true)
  assert.equal(state.showMaskedCookieValue, true)
  assert.equal(state.maskedCookieValue, '__VR_MASKED_SETTING__:yt_dlp_cookie_file_youtube:0')
})

test('buildSettingsPayload includes youtube yt-dlp cookie when edited', () => {
  const payload = buildSettingsPayload({
    hasSavedKey: true,
    showMaskedValue: true,
    apiKey: '',
    dailySyncTime: '03:00',
    syncConcurrency: '2',
    downloadConcurrency: '2',
    hasSavedCookie: true,
    showMaskedCookieValue: false,
    cookieValue: 'SID=abc; HSID=def',
  })

  assert.equal(payload.yt_dlp_cookie_file_youtube, 'SID=abc; HSID=def')
})
