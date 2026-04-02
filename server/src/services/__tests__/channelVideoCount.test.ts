import test from 'node:test';
import assert from 'node:assert/strict';
import {
  computeEffectiveChannelVideoCount,
  isSuspiciousYoutubeVideoCount,
} from '../channelVideoCount.js';

test('isSuspiciousYoutubeVideoCount flags stale counts inflated by unavailable videos', () => {
  assert.equal(
    isSuspiciousYoutubeVideoCount({
      platform: 'youtube',
      currentVideoCount: 5,
      availableTrackedVideoCount: 2,
      unavailableTrackedVideoCount: 3,
      fetchLimit: 50,
    }),
    true,
  );
});

test('computeEffectiveChannelVideoCount drops to available tracked total for youtube when unavailable rows inflate current count', () => {
  assert.equal(
    computeEffectiveChannelVideoCount({
      platform: 'youtube',
      currentVideoCount: 5,
      trackedVideoCount: 5,
      availableTrackedVideoCount: 2,
      unavailableTrackedVideoCount: 3,
      reportedVideoCount: null,
    }),
    2,
  );
});

test('computeEffectiveChannelVideoCount keeps lower-bound protection when no unavailable rows exist', () => {
  assert.equal(
    computeEffectiveChannelVideoCount({
      platform: 'youtube',
      currentVideoCount: 5,
      trackedVideoCount: 2,
      availableTrackedVideoCount: 2,
      unavailableTrackedVideoCount: 0,
      reportedVideoCount: null,
    }),
    5,
  );
});
