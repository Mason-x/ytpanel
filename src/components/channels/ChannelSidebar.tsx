import type { ApiChannel } from '../../types'
import { formatNum, parseTagsJson } from '../../lib/channelHelpers'

export default function ChannelSidebar({
  channels,
  filteredChannels,
  channelId,
  channelsLoading,
  tagSearch,
  sidebarSearch,
  tagFilter,
  tagOptions,
  managingChannels,
  exportingChannelLinks,
  exportedChannelLinks,
  onTagSearchChange,
  onSidebarSearchChange,
  onTagFilterChange,
  onSelectChannel,
  onToggleManage,
  onDeleteChannel,
  onOpenAdd,
  onExportLinks,
}: {
  channels: ApiChannel[]
  filteredChannels: ApiChannel[]
  channelId: string
  channelsLoading: boolean
  tagSearch: string
  sidebarSearch: string
  tagFilter: string
  tagOptions: Array<{ value: string; label: string; count: number }>
  managingChannels: boolean
  exportingChannelLinks: boolean
  exportedChannelLinks: boolean
  onTagSearchChange: (value: string) => void
  onSidebarSearchChange: (value: string) => void
  onTagFilterChange: (value: string) => void
  onSelectChannel: (channelId: string) => void
  onToggleManage: () => void
  onDeleteChannel: (channelId: string) => void
  onOpenAdd: () => void
  onExportLinks: () => void
}) {
  return (
    <aside className="sidebar sidebar-youtube">
      <div className="sidebar-dual-layout">
        <div className="sidebar-tag-panel">
          <div className="sidebar-tag-search-wrap">
            <input
              className="input sidebar-tag-search"
              value={tagSearch}
              onChange={(event) => onTagSearchChange(event.target.value)}
              placeholder="搜索标签..."
              style={{ background: 'var(--bg-primary)', border: 'none' }}
            />
          </div>

          <div className="sidebar-tag-list">
            {tagOptions.map((item) => (
              <span
                key={item.value}
                onClick={() => onTagFilterChange(item.value)}
                className={`sidebar-tag-chip sidebar-tag-chip-row ${tagFilter === item.value ? 'active' : ''}`}
                title={`${item.label} (${item.count})`}
              >
                <span className="sidebar-tag-chip-text">{item.label}</span>
                <span className="sidebar-tag-chip-count">{`(${item.count})`}</span>
              </span>
            ))}
          </div>
        </div>

        <div className="sidebar-channel-panel">
          <div className="sidebar-channel-search-box">
            <div style={{ display: 'flex', gap: 8 }}>
              <input
                className="input sidebar-channel-search"
                placeholder="搜索频道..."
                value={sidebarSearch}
                onChange={(event) => onSidebarSearchChange(event.target.value)}
                style={{ background: 'var(--bg-primary)', border: 'none', flex: 1 }}
              />
              <button className="btn btn-secondary sidebar-add-btn" onClick={onOpenAdd} title="添加频道">
                +
              </button>
            </div>
          </div>

          <div className="sidebar-channel-list">
            {filteredChannels.length > 0 ? (
              filteredChannels.map((item) => (
                <div
                  key={item.channel_id}
                  className={`sidebar-item ${item.channel_id === channelId ? 'active' : ''}`}
                  onClick={() => onSelectChannel(item.channel_id)}
                >
                  <div className="sidebar-item-avatar">
                    {item.avatar_url ? (
                      <img src={item.avatar_url} alt="" loading="lazy" referrerPolicy="no-referrer" />
                    ) : null}
                  </div>

                  <div className="sidebar-item-info">
                    <div className="sidebar-item-title" style={{ fontSize: '0.98rem', fontWeight: 600 }}>
                      {item.title}
                    </div>
                    <div className="sidebar-item-subtitle" style={{ fontSize: '0.78rem', marginTop: 2 }}>
                      {formatNum(item.subscriber_count)} 订阅 · {formatNum(item.video_count)} 视频
                    </div>
                    {parseTagsJson(item.tags_json).length > 0 && (
                      <div className="sidebar-status-text">
                        {parseTagsJson(item.tags_json)
                          .slice(0, 2)
                          .map((tag) => `#${tag}`)
                          .join('  ')}
                      </div>
                    )}
                  </div>

                  {managingChannels && (
                    <button
                      type="button"
                      className="channel-manage-delete-btn"
                      onClick={(event) => {
                        event.stopPropagation()
                        onDeleteChannel(item.channel_id)
                      }}
                      title="删除频道"
                    >
                      -
                    </button>
                  )}
                </div>
              ))
            ) : (
              <div className="empty-state" style={{ padding: '32px 16px' }}>
                {channelsLoading ? '频道加载中...' : '请先添加一个 YouTube 频道。'}
              </div>
            )}
          </div>

          <div className="sidebar-footer-count">
            <div>{`共 ${filteredChannels.length}${filteredChannels.length !== channels.length ? ` / ${channels.length}` : ''} 个频道`}</div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, width: '100%' }}>
              <button className={`sidebar-manage-btn ${managingChannels ? 'active' : ''}`} onClick={onToggleManage}>
                {managingChannels ? '完成管理' : '管理频道'}
              </button>
              <button className="sidebar-manage-btn" onClick={onExportLinks} disabled={exportingChannelLinks || channels.length === 0}>
                {exportingChannelLinks ? '导出中...' : exportedChannelLinks ? '已复制' : '一键导出'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </aside>
  )
}
