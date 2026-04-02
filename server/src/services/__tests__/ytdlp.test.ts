import test from 'node:test';
import assert from 'node:assert/strict';
import { parseChannelMeta } from '../ytdlp';

test('parseChannelMeta prefers author avatar over youtube video thumbnails', () => {
  const meta = parseChannelMeta({
    extractor_key: 'YoutubeTab',
    thumbnails: [
      {
        id: '0',
        url: 'https://i.ytimg.com/vi/abc123/hqdefault.jpg',
      },
    ],
    author: {
      avatar_thumb: {
        url_list: ['https://yt3.googleusercontent.com/channel-avatar=s88'],
      },
    },
    title: 'Sample Channel',
    channel_id: 'UC123',
  });

  assert.equal(meta.avatar_url, 'https://yt3.googleusercontent.com/channel-avatar=s88');
});

test('parseChannelMeta does not treat youtube video thumbnails as channel avatar fallback', () => {
  const meta = parseChannelMeta({
    extractor_key: 'YoutubeTab',
    thumbnails: [
      {
        id: '0',
        url: 'https://i.ytimg.com/vi/legacyVideo/hqdefault.jpg',
      },
    ],
    title: 'Sample Channel',
    channel_id: 'UC123',
  });

  assert.equal(meta.avatar_url, null);
});
