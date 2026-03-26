import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import ChannelSidebar from '../ChannelSidebar'

test('sidebar channel rows do not render channel tags', () => {
  const html = renderToStaticMarkup(
    <ChannelSidebar
      channels={[
        {
          channel_id: 'UC123',
          title: 'Alpha',
          subscriber_count: 1200,
          video_count: 48,
          tags_json: JSON.stringify(['news', 'growth']),
        },
      ]}
      filteredChannels={[
        {
          channel_id: 'UC123',
          title: 'Alpha',
          subscriber_count: 1200,
          video_count: 48,
          tags_json: JSON.stringify(['news', 'growth']),
        },
      ]}
      channelId="UC123"
      channelsLoading={false}
      tagSearch=""
      sidebarSearch=""
      tagFilter="all"
      tagOptions={[{ value: 'all', label: '全部', count: 1 }]}
      managingChannels={false}
      exportingChannelLinks={false}
      exportedChannelLinks={false}
      onTagSearchChange={() => {}}
      onSidebarSearchChange={() => {}}
      onTagFilterChange={() => {}}
      onSelectChannel={() => {}}
      onToggleManage={() => {}}
      onDeleteChannel={() => {}}
      onOpenAdd={() => {}}
      onExportLinks={() => {}}
    />,
  )

  assert.match(html, /1200.*订阅.*48.*视频/)
  assert.doesNotMatch(html, /#news|#growth/)
})
