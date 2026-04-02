import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import VideoCard from '../VideoCard'
import type { ApiVideo } from '../../../types'

const baseVideo: ApiVideo = {
  video_id: 'abc123xyz89',
  channel_id: 'UC123',
  title: 'Sample Video',
  description: '第一行描述\n第二行描述',
  webpage_url: 'https://www.youtube.com/watch?v=abc123xyz89',
  published_at: '2026-04-01T08:00:00Z',
  duration_sec: 360,
  view_count: 1000,
  like_count: 120,
  comment_count: 8,
  content_type: 'long',
}

test('video card renders description toggle button', () => {
  const html = renderToStaticMarkup(
    <VideoCard
      video={baseVideo}
      selected={false}
      metaDownloading={false}
      metaProgress={0}
      metaDownloaded={false}
      onMetaDownload={() => {}}
      onCopyLink={() => {}}
      onToggleSelect={() => {}}
      onToggleDescription={() => {}}
      descriptionExpanded={false}
      highlightGrowthSort={false}
    />,
  )

  assert.match(html, /展开描述/)
  assert.doesNotMatch(html, /video-description-popover/)
})

test('video card renders full description popover when expanded', () => {
  const html = renderToStaticMarkup(
    <VideoCard
      video={baseVideo}
      selected={false}
      metaDownloading={false}
      metaProgress={0}
      metaDownloaded={false}
      onMetaDownload={() => {}}
      onCopyLink={() => {}}
      onToggleSelect={() => {}}
      onToggleDescription={() => {}}
      descriptionExpanded
      highlightGrowthSort={false}
    />,
  )

  assert.match(html, /收起描述/)
  assert.match(html, /video-description-popover/)
  assert.match(html, /video-description-popover-overlay/)
  assert.match(html, /role="dialog"/)
  assert.match(html, /第一行描述/)
  assert.match(html, /第二行描述/)
})

test('video card disables description toggle when description is empty', () => {
  const html = renderToStaticMarkup(
    <VideoCard
      video={{ ...baseVideo, description: '' }}
      selected={false}
      metaDownloading={false}
      metaProgress={0}
      metaDownloaded={false}
      onMetaDownload={() => {}}
      onCopyLink={() => {}}
      onToggleSelect={() => {}}
      onToggleDescription={() => {}}
      descriptionExpanded={false}
      highlightGrowthSort={false}
    />,
  )

  assert.match(html, /展开描述/)
  assert.match(html, /disabled/)
})
