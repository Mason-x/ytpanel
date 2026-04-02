type SupportedPlatform = 'youtube' | 'douyin' | 'bilibili' | 'tiktok' | 'xiaohongshu' | string;

export function isSuspiciousYoutubeVideoCount(input: {
  platform: SupportedPlatform;
  currentVideoCount: number | null;
  availableTrackedVideoCount: number;
  unavailableTrackedVideoCount: number;
  fetchLimit: number;
}): boolean {
  if (String(input.platform || '').trim().toLowerCase() !== 'youtube') return false;
  const videoCount = input.currentVideoCount;
  if (videoCount == null || videoCount <= 0) return false;
  if (videoCount === input.fetchLimit) return true;
  if (videoCount === 200) return true;
  if (input.unavailableTrackedVideoCount > 0 && videoCount > input.availableTrackedVideoCount) return true;
  return false;
}

export function computeEffectiveChannelVideoCount(input: {
  platform: SupportedPlatform;
  currentVideoCount: number | null;
  trackedVideoCount: number;
  availableTrackedVideoCount: number;
  unavailableTrackedVideoCount: number;
  reportedVideoCount: number | null;
}): number {
  const platform = String(input.platform || '').trim().toLowerCase();
  const aggregateVideoCount = (
    platform === 'douyin' && input.reportedVideoCount != null
      ? Math.max(input.trackedVideoCount, Number(input.reportedVideoCount || 0))
      : platform === 'youtube'
        ? input.availableTrackedVideoCount
        : input.trackedVideoCount
  );

  if (platform === 'youtube' && input.unavailableTrackedVideoCount > 0) {
    if (input.currentVideoCount == null) return aggregateVideoCount;
    return Math.min(input.currentVideoCount, aggregateVideoCount);
  }

  return input.currentVideoCount != null
    ? Math.max(input.currentVideoCount, aggregateVideoCount)
    : aggregateVideoCount;
}
