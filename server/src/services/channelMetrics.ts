import type Database from 'better-sqlite3';

interface ChannelGrowthPoint {
  date: string;
  view_count: number | null;
}

export interface ChannelViewGrowthData {
  channel_view_increase_7d: number | null;
  channel_view_growth_series_7d: ChannelGrowthPoint[];
}

function toNullableInt(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.trunc(parsed);
}

function getStartDateBeforeDays(days: number): string {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  start.setUTCDate(start.getUTCDate() - Math.max(0, days - 1));
  return start.toISOString().slice(0, 10);
}

function buildRecentDateRange(days: number): string[] {
  const safeDays = Math.max(1, Math.trunc(days));
  const startDate = getStartDateBeforeDays(safeDays);
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  if (Number.isNaN(cursor.getTime())) return [startDate];
  const out: string[] = [];
  for (let i = 0; i < safeDays; i += 1) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

export function buildChannelViewGrowthData(
  db: Database.Database,
  channelId: string,
): ChannelViewGrowthData {
  const dateRange = buildRecentDateRange(7);
  const startDate = dateRange[0];
  const endDate = dateRange[dateRange.length - 1];
  const channelVideos = db.prepare(`
    SELECT video_id, view_count, published_at
    FROM videos
    WHERE channel_id = ?
  `).all(channelId) as any[];

  const dailyRows = db.prepare(`
    SELECT vd.video_id as video_id, vd.date as date, vd.view_count as view_count
    FROM video_daily vd
    INNER JOIN videos v ON v.video_id = vd.video_id
    WHERE v.channel_id = ? AND vd.date >= ? AND vd.date <= ?
    ORDER BY vd.video_id ASC, vd.date ASC
  `).all(channelId, startDate, endDate) as any[];

  const previousRows = db.prepare(`
    SELECT vd.video_id as video_id, vd.view_count as view_count
    FROM video_daily vd
    INNER JOIN (
      SELECT vd2.video_id as video_id, MAX(vd2.date) as max_date
      FROM video_daily vd2
      INNER JOIN videos v2 ON v2.video_id = vd2.video_id
      WHERE v2.channel_id = ? AND vd2.date < ?
      GROUP BY vd2.video_id
    ) prev
      ON prev.video_id = vd.video_id
     AND prev.max_date = vd.date
  `).all(channelId, startDate) as any[];

  const dailyByVideo = new Map<string, Map<string, number | null>>();
  for (const row of dailyRows) {
    const videoId = String(row?.video_id || '').trim();
    if (!videoId) continue;
    const date = String(row?.date || '').trim();
    if (!date) continue;
    const dateMap = dailyByVideo.get(videoId) || new Map<string, number | null>();
    dateMap.set(date, toNullableInt(row?.view_count));
    dailyByVideo.set(videoId, dateMap);
  }

  const previousByVideo = new Map<string, number | null>();
  for (const row of previousRows) {
    const videoId = String(row?.video_id || '').trim();
    if (!videoId) continue;
    previousByVideo.set(videoId, toNullableInt(row?.view_count));
  }

  const totalsByDate = new Map<string, number>();
  const observedDates = new Set<string>();

  for (const row of channelVideos) {
    const videoId = String(row?.video_id || '').trim();
    if (!videoId) continue;
    const dateMap = dailyByVideo.get(videoId) || new Map<string, number | null>();
    const fallbackView = toNullableInt(row?.view_count);
    let carry = previousByVideo.get(videoId) ?? null;
    let touched = false;

    for (const date of dateRange) {
      const incoming = dateMap.has(date) ? (dateMap.get(date) ?? null) : null;
      if (incoming != null) {
        carry = carry == null ? incoming : Math.max(carry, incoming);
      }
      if (carry == null) continue;
      totalsByDate.set(date, (totalsByDate.get(date) || 0) + carry);
      observedDates.add(date);
      touched = true;
    }

    if (!touched && fallbackView != null && fallbackView > 0) {
      for (const date of dateRange) {
        totalsByDate.set(date, (totalsByDate.get(date) || 0) + fallbackView);
        observedDates.add(date);
      }
    }
  }

  let points: ChannelGrowthPoint[] = dateRange.map((date) => ({
    date,
    view_count: observedDates.has(date) ? (totalsByDate.get(date) ?? 0) : null,
  }));

  if (!points.some((item) => item.view_count != null)) {
    const channelRow = db.prepare('SELECT view_count FROM channels WHERE channel_id = ?').get(channelId) as any;
    const fallback = toNullableInt(channelRow?.view_count);
    if (fallback != null) {
      points = dateRange.map((date) => ({ date, view_count: fallback }));
    }
  }

  let increaseSum = 0;
  let countedVideos = 0;
  for (const row of channelVideos) {
    const videoId = String(row?.video_id || '').trim();
    if (!videoId) continue;
    const dateMap = dailyByVideo.get(videoId) || new Map<string, number | null>();

    let latestInWindow: number | null = null;
    for (let i = dateRange.length - 1; i >= 0; i -= 1) {
      const date = dateRange[i];
      if (!dateMap.has(date)) continue;
      const candidate = dateMap.get(date) ?? null;
      if (candidate != null) {
        latestInWindow = candidate;
        break;
      }
    }

    const fallbackView = toNullableInt(row?.view_count);
    const latestView = latestInWindow
      ?? fallbackView
      ?? previousByVideo.get(videoId)
      ?? null;
    if (latestView == null) continue;

    const publishedAtText = String(row?.published_at || '').trim();
    const publishedDate = publishedAtText.includes('T')
      ? publishedAtText.slice(0, 10)
      : publishedAtText.slice(0, 10);
    const publishedInWindow = Boolean(
      /^\d{4}-\d{2}-\d{2}$/.test(publishedDate)
      && publishedDate >= startDate
      && publishedDate <= endDate,
    );

    let contribution = 0;
    if (publishedInWindow) {
      contribution = Math.max(0, latestView);
    } else {
      let firstInWindow: number | null = null;
      for (const date of dateRange) {
        if (!dateMap.has(date)) continue;
        const candidate = dateMap.get(date) ?? null;
        if (candidate != null) {
          firstInWindow = candidate;
          break;
        }
      }
      const baseline = firstInWindow
        ?? previousByVideo.get(videoId)
        ?? latestView;
      contribution = Math.max(0, latestView - Math.max(0, baseline ?? 0));
    }

    increaseSum += contribution;
    countedVideos += 1;
  }

  if (points.every((item) => item.view_count == null) && countedVideos <= 0) {
    return { channel_view_increase_7d: null, channel_view_growth_series_7d: points };
  }

  return {
    channel_view_increase_7d: Math.max(0, Math.trunc(increaseSum)),
    channel_view_growth_series_7d: points,
  };
}

export function writeChannelViewGrowthCache(
  db: Database.Database,
  channelId: string,
): ChannelViewGrowthData {
  const growth = buildChannelViewGrowthData(db, channelId);
  db.prepare(`
    UPDATE channels
    SET channel_view_increase_7d = ?,
        channel_view_growth_series_7d_json = ?,
        channel_growth_computed_at = datetime('now')
    WHERE channel_id = ?
  `).run(
    growth.channel_view_increase_7d,
    JSON.stringify(growth.channel_view_growth_series_7d),
    channelId,
  );
  return growth;
}

export function parseCachedChannelViewGrowth(row: any): ChannelViewGrowthData {
  const rawIncrease = row?.channel_view_increase_7d;
  const channel_view_increase_7d = rawIncrease == null
    ? null
    : (Number.isFinite(Number(rawIncrease)) ? Math.max(0, Math.trunc(Number(rawIncrease))) : null);

  let channel_view_growth_series_7d: ChannelGrowthPoint[] = [];
  const rawSeries = typeof row?.channel_view_growth_series_7d_json === 'string'
    ? row.channel_view_growth_series_7d_json
    : '';
  if (rawSeries.trim()) {
    try {
      const parsed = JSON.parse(rawSeries);
      if (Array.isArray(parsed)) {
        channel_view_growth_series_7d = parsed
          .map((item) => ({
            date: String(item?.date || '').trim(),
            view_count: toNullableInt(item?.view_count),
          }))
          .filter((item) => item.date);
      }
    } catch {
      channel_view_growth_series_7d = [];
    }
  }

  return {
    channel_view_increase_7d,
    channel_view_growth_series_7d,
  };
}

export function backfillChannelViewGrowthCaches(
  db: Database.Database,
  options?: {
    force?: boolean;
    logger?: (message: string) => void;
  },
): number {
  const force = options?.force === true;
  const logger = options?.logger;
  const rows = db.prepare(`
    SELECT channel_id
    FROM channels
    ${force ? '' : 'WHERE channel_growth_computed_at IS NULL'}
    ORDER BY created_at ASC, channel_id ASC
  `).all() as Array<{ channel_id: string }>;

  for (const row of rows) {
    const channelId = String(row?.channel_id || '').trim();
    if (!channelId) continue;
    writeChannelViewGrowthCache(db, channelId);
  }

  if (rows.length > 0 && logger) {
    logger(`[DB] Backfilled channel growth cache for ${rows.length} channel(s)`);
  }
  return rows.length;
}
