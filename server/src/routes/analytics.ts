import { Router, Request, Response } from 'express';
import { getDb } from '../db.js';
import { rangeToStartDate, todayDateStr } from '../utils/helpers.js';

const router = Router();

interface ChannelDailyPoint {
  date: string;
  subs_total: number | null;
  views_total: number | null;
  video_count: number | null;
}

function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
}

function getChannelFallbackSnapshot(db: ReturnType<typeof getDb>, channelId: string): {
  subs_total: number | null;
  views_total: number | null;
  video_count: number | null;
} {
  const channel = db.prepare(`
    SELECT subscriber_count, view_count, video_count
    FROM channels
    WHERE channel_id = ?
  `).get(channelId) as any;

  const videoAgg = db.prepare(`
    SELECT COUNT(*) as video_count, COALESCE(SUM(view_count), 0) as views_total
    FROM videos
    WHERE channel_id = ?
  `).get(channelId) as any;

  const subsTotal = toNullableNumber(channel?.subscriber_count);
  const channelViews = toNullableNumber(channel?.view_count);
  const channelVideoCount = toNullableNumber(channel?.video_count);
  const aggViews = toNullableNumber(videoAgg?.views_total);
  const aggVideoCount = toNullableNumber(videoAgg?.video_count);

  return {
    subs_total: subsTotal,
    views_total: channelViews ?? aggViews,
    video_count: channelVideoCount ?? aggVideoCount,
  };
}

function getNormalizedChannelDailySeries(db: ReturnType<typeof getDb>, channelId: string, startDate: string): ChannelDailyPoint[] {
  const rows = db.prepare(`
    SELECT date, subscriber_count, view_count_total, video_count
    FROM channel_daily
    WHERE channel_id = ? AND date >= ?
    ORDER BY date ASC
  `).all(channelId, startDate) as any[];

  const fallback = getChannelFallbackSnapshot(db, channelId);

  if (rows.length === 0) {
    if (fallback.subs_total == null && fallback.views_total == null && fallback.video_count == null) {
      return [];
    }

    return [{
      date: todayDateStr(),
      subs_total: fallback.subs_total,
      views_total: fallback.views_total,
      video_count: fallback.video_count,
    }];
  }

  let lastSubs: number | null = null;
  let lastViews: number | null = null;
  let lastVideoCount: number | null = null;

  return rows.map((row, index) => {
    const isLatest = index === rows.length - 1;

    let subsTotal = toNullableNumber(row.subscriber_count);
    let viewsTotal = toNullableNumber(row.view_count_total);
    let videoCount = toNullableNumber(row.video_count);

    if (subsTotal == null) subsTotal = lastSubs;
    if (viewsTotal == null) viewsTotal = lastViews;
    if (videoCount == null) videoCount = lastVideoCount;

    if (isLatest) {
      subsTotal = subsTotal ?? fallback.subs_total;
      viewsTotal = viewsTotal ?? fallback.views_total;
      videoCount = videoCount ?? fallback.video_count;
    }

    if (subsTotal != null) lastSubs = subsTotal;
    if (viewsTotal != null) lastViews = viewsTotal;
    if (videoCount != null) lastVideoCount = videoCount;

    return {
      date: row.date,
      subs_total: subsTotal,
      views_total: viewsTotal,
      video_count: videoCount,
    };
  });
}

// GET /api/channels/:id/analytics/timeseries
router.get('/:id/analytics/timeseries', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '');
  const metric = String(req.query.metric || 'views');
  const range = String(req.query.range || '28d');
  const startDate = rangeToStartDate(range);
  const rows = getNormalizedChannelDailySeries(db, channelId, startDate);

  // Calculate daily changes
  const data = rows.map((row, i) => {
    const prev = i > 0 ? rows[i - 1] : null;
    const hasPrevViews = prev && row.views_total != null && prev.views_total != null;
    const hasPrevSubs = prev && row.subs_total != null && prev.subs_total != null;

    return {
      date: row.date,
      views_total: row.views_total,
      subs_total: row.subs_total,
      video_count: row.video_count,
      views_change: hasPrevViews
        ? row.views_total! - prev.views_total!
        : (row.views_total != null ? 0 : null),
      subs_change: hasPrevSubs
        ? row.subs_total! - prev.subs_total!
        : (row.subs_total != null ? 0 : null),
    };
  });

  res.json({ data, metric, range });
});

// GET /api/channels/:id/analytics/kpi
router.get('/:id/analytics/kpi', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '');
  const range = String(req.query.range || '28d');
  const startDate = rangeToStartDate(range);
  const series = getNormalizedChannelDailySeries(db, channelId, startDate);
  const latest = series.length > 0 ? series[series.length - 1] : null;
  const earlier = series.length > 0 ? series[0] : null;

  const viewsInRange = latest && earlier &&
    latest.views_total != null && earlier.views_total != null
    ? latest.views_total - earlier.views_total
    : null;

  const subsInRange = latest && earlier &&
    latest.subs_total != null && earlier.subs_total != null
    ? latest.subs_total - earlier.subs_total
    : null;

  const today = todayDateStr();

  // Count uploads in range
  const uploads = db.prepare(`
    SELECT COUNT(*) as count FROM videos
    WHERE channel_id = ? AND published_at >= ? AND published_at <= ?
  `).get(channelId, startDate, today) as any;

  res.json({
    views: viewsInRange,
    subs: subsInRange,
    uploads: uploads?.count || 0,
    latest_snapshot_date: latest?.date || null,
    earliest_snapshot_date: earlier?.date || null,
  });
});

// GET /api/channels/:id/analytics/daily-table
router.get('/:id/analytics/daily-table', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '');
  const range = String(req.query.range || '28d');
  const sort = String(req.query.sort || 'date');
  const order = String(req.query.order || 'desc');
  const page = String(req.query.page || '1');
  const limit = String(req.query.limit || '30');
  const startDate = rangeToStartDate(range);
  const pageNum = Math.max(1, parseInt(page, 10));
  const limitNum = Math.min(365, parseInt(limit, 10) || 30);
  const offset = (pageNum - 1) * limitNum;
  const rows = getNormalizedChannelDailySeries(db, channelId, startDate);

  // Calculate changes
  const data = rows.map((row, i) => {
    const prev = i > 0 ? rows[i - 1] : null;
    return {
      date: row.date,
      subs_total: row.subs_total,
      subs_change: prev && row.subs_total != null && prev.subs_total != null
        ? row.subs_total - prev.subs_total
        : (row.subs_total != null ? 0 : null),
      views_total: row.views_total,
      views_change: prev && row.views_total != null && prev.views_total != null
        ? row.views_total - prev.views_total
        : (row.views_total != null ? 0 : null),
    };
  }).reverse();

  // Sort if needed
  let sorted = data;
  if (sort === 'views_change') {
    sorted = [...data].sort((a, b) => {
      const av = a.views_change ?? -Infinity;
      const bv = b.views_change ?? -Infinity;
      return order === 'asc' ? av - bv : bv - av;
    });
  } else if (sort === 'subs_change') {
    sorted = [...data].sort((a, b) => {
      const av = a.subs_change ?? -Infinity;
      const bv = b.subs_change ?? -Infinity;
      return order === 'asc' ? av - bv : bv - av;
    });
  }

  const paged = sorted.slice(offset, offset + limitNum);
  res.json({ data: paged, total: sorted.length, page: pageNum, limit: limitNum });
});

// GET /api/channels/:id/analytics/top-videos
router.get('/:id/analytics/top-videos', (req: Request, res: Response) => {
  const db = getDb();
  const { range = '28d', limit = '5' } = req.query;
  const days = range === '7d' ? 6 : 27;
  const today = todayDateStr();
  const startDate = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
  const limitNum = Math.min(20, parseInt(limit as string, 10) || 5);

  const rows = db.prepare(`
    SELECT v.video_id, v.title, v.published_at, v.duration_sec, v.content_type,
      v.local_thumb_path, v.download_status,
      vd_latest.view_count as latest_views,
      vd_latest.view_count as latest_views,
      CASE WHEN vd_start.view_count IS NOT NULL THEN (vd_latest.view_count - vd_start.view_count) ELSE NULL END as views_change
    FROM videos v
    LEFT JOIN (
      SELECT video_id, view_count FROM video_daily
      WHERE date = (SELECT MAX(date) FROM video_daily vd2 WHERE vd2.video_id = video_daily.video_id)
    ) vd_latest ON vd_latest.video_id = v.video_id
    LEFT JOIN (
      SELECT video_id, view_count FROM video_daily WHERE date = ?
    ) vd_start ON vd_start.video_id = v.video_id
    WHERE v.channel_id = ?
    ORDER BY views_change DESC NULLS LAST
    LIMIT ?
  `).all(startDate, req.params.id, limitNum);

  res.json({ data: rows });
});

// GET /api/channels/:id/analytics/day-top-videos
router.get('/:id/analytics/day-top-videos', (req: Request, res: Response) => {
  const db = getDb();
  const { date, limit = '20' } = req.query;
  if (!date) {
    res.status(400).json({ error: 'date parameter required (YYYY-MM-DD)' });
    return;
  }
  const limitNum = Math.min(50, parseInt(limit as string, 10) || 20);
  const prevDate = new Date(new Date(date as string).getTime() - 86400000).toISOString().slice(0, 10);

  const rows = db.prepare(`
    SELECT v.video_id, v.title, v.published_at, v.local_thumb_path,
      vd_today.view_count as views_today,
      (vd_today.view_count - COALESCE(vd_prev.view_count, 0)) as views_change
    FROM videos v
    INNER JOIN video_daily vd_today ON vd_today.video_id = v.video_id AND vd_today.date = ?
    LEFT JOIN video_daily vd_prev ON vd_prev.video_id = v.video_id AND vd_prev.date = ?
    WHERE v.channel_id = ?
    ORDER BY views_change DESC
    LIMIT ?
  `).all(date, prevDate, req.params.id, limitNum);

  res.json({ data: rows });
});

export default router;
