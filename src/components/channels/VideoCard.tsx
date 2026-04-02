import type { ApiVideo } from '../../types'
import {
  formatDateTime,
  formatDuration,
  formatNum,
  formatSigned,
  relTime,
  resolveVideoThumb,
  resolveVideoUrl,
  viewVelocity,
} from '../../lib/channelHelpers'
import { MiniGrowthChart, type VideoGrowthDomain } from './GrowthCharts'

export default function VideoCard({
  video,
  selected,
  descriptionExpanded,
  metaDownloading,
  metaProgress,
  metaDownloaded,
  onMetaDownload,
  onCopyLink,
  onToggleDescription,
  onToggleSelect,
  highlightGrowthSort,
  forceLandscape = false,
  growthDomain,
}: {
  video: ApiVideo
  selected: boolean
  descriptionExpanded: boolean
  metaDownloading: boolean
  metaProgress: number
  metaDownloaded: boolean
  onMetaDownload: () => void
  onCopyLink: () => void
  onToggleDescription: () => void
  onToggleSelect: () => void
  highlightGrowthSort: boolean
  forceLandscape?: boolean
  growthDomain?: VideoGrowthDomain | null
}) {
  const unavailable = String(video.availability_status || 'available') !== 'available'
  const viewCount = Number(video.latest_views ?? video.view_count ?? 0)
  const likeCount = Number(video.like_count ?? video.latest_likes ?? 0)
  const commentCount = Number(video.comment_count ?? video.latest_comments ?? 0)
  const publishedTime = String(video.published_at || '')
  const contentType = String(video.content_type || '').toLowerCase()
  const isShort = contentType === 'short'
  const isLive = contentType === 'live'
  const useVerticalThumb = isShort && !forceLandscape
  const descriptionText = String(video.description || '').trim()
  const hasDescription = descriptionText.length > 0
  const displayedGrowth =
    video.views_change_7d != null && Number(video.views_change_7d) > 0
      ? Number(video.views_change_7d)
      : Number(video.daily_view_increase ?? video.views_change_7d ?? 0)

  const typeClass = isLive ? 'is-live' : isShort ? 'is-short' : 'is-long'
  const metaButtonText = metaDownloading
    ? `元数据 ${metaProgress}%`
    : metaDownloaded
      ? '元数据已下载'
      : '下载元数据'

  return (
    <article
      className={[
        'video-card',
        `video-card-${typeClass}`,
        descriptionExpanded ? 'video-card-description-open' : '',
        selected ? 'video-card-selected' : '',
        unavailable ? 'video-card-unavailable' : '',
      ]
        .filter(Boolean)
        .join(' ')}
      onClick={onToggleSelect}
    >
      {descriptionExpanded ? (
        <div
          className="video-description-popover-overlay"
          role="dialog"
          aria-label="视频描述"
          onClick={(event) => event.stopPropagation()}
        >
          <div className="video-description-popover">
            <div className="video-description-popover-header">
              <strong>视频描述</strong>
              <button
                type="button"
                className="video-description-popover-close"
                onClick={(event) => {
                  event.stopPropagation()
                  onToggleDescription()
                }}
                aria-label="收起描述"
              >
                ×
              </button>
            </div>
            <div className="video-description-popover-body">{descriptionText}</div>
          </div>
        </div>
      ) : null}

      <a
        className={`video-thumb ${useVerticalThumb ? 'video-thumb-vertical' : ''}`}
        href={resolveVideoUrl(video)}
        target="_blank"
        rel="noopener noreferrer"
        onClick={(event) => event.stopPropagation()}
      >
        <img src={resolveVideoThumb(video)} alt="" loading="lazy" referrerPolicy="no-referrer" />
        {!isLive && formatDuration(video.duration_sec) && (
          <span className="video-duration">{formatDuration(video.duration_sec)}</span>
        )}
        {selected && <div className="video-card-selected-badge">已选</div>}
        {unavailable && (
          <div className="video-card-mask">
            <div className="video-card-mask-title">内容不可用</div>
            <div className="video-card-mask-reason">
              {String(video.availability_status || '').trim() || '当前资源暂时无法访问'}
            </div>
          </div>
        )}
      </a>

      <div className="video-info">
        <a
          className="video-title"
          href={resolveVideoUrl(video)}
          target="_blank"
          rel="noopener noreferrer"
          title={video.title}
          onClick={(event) => event.stopPropagation()}
        >
          {video.title}
        </a>

        <div className="video-meta">
          <span className="video-view-count">{`${formatNum(viewCount)} 次观看`}</span>
          <span className="video-meta-time">{relTime(publishedTime)}</span>
        </div>

        <div className="video-growth-row">
          <MiniGrowthChart points={video.growth_series_7d} domain={growthDomain} />
          <div className="video-growth-copy">
            <div className="video-growth-label">近 7 日增长</div>
            <div className="video-growth-value">{formatSigned(displayedGrowth)}</div>
          </div>
        </div>

        <div className="video-insight-grid">
          <div className="video-insight-chip">
            <span className="video-insight-label">点赞</span>
            <span className="video-insight-value">{formatNum(likeCount)}</span>
          </div>
          <div className="video-insight-chip">
            <span className="video-insight-label">评论</span>
            <span className="video-insight-value">{formatNum(commentCount)}</span>
          </div>
        </div>

        <div className="video-insight-line">
          <strong>{formatDateTime(publishedTime)}</strong>
          <span className="video-insight-dot">·</span>
          <span>{highlightGrowthSort ? `日均 ${formatNum(Math.round(viewVelocity(video)))}` : 'YouTube API'}</span>
        </div>

        <div className="video-card-actions" data-card-action="1">
          <button
            type="button"
            className="video-card-action-btn secondary"
            onClick={(event) => {
              event.stopPropagation()
              onToggleDescription()
            }}
            disabled={!hasDescription}
            title={hasDescription ? (descriptionExpanded ? '收起描述' : '展开描述') : '暂无描述'}
          >
            {descriptionExpanded ? '收起描述' : '展开描述'}
          </button>
          <button
            type="button"
            className={`video-card-action-btn ${metaDownloaded ? 'is-downloaded' : ''}`}
            onClick={(event) => {
              event.stopPropagation()
              onMetaDownload()
            }}
            disabled={metaDownloading}
            title={metaButtonText}
          >
            {metaButtonText}
          </button>
          <button
            type="button"
            className="video-card-action-btn secondary"
            onClick={(event) => {
              event.stopPropagation()
              onCopyLink()
            }}
          >
            复制链接
          </button>
        </div>
      </div>
    </article>
  )
}
