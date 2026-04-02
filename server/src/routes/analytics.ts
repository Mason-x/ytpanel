import { Router, Request, Response } from 'express';
import { getDb } from '../db.js';
import { rangeToStartDate, todayDateStr } from '../utils/helpers.js';
import { getJobQueue } from '../services/jobQueue.js';
import { enqueueReportingSyncForBinding } from '../services/youtubeReportingSync.js';

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

function getChannelReportingBinding(db: ReturnType<typeof getDb>, channelId: string): {
  binding_id: string;
  owner_id: string;
  owner_name: string;
  started_at: string;
  enabled: number;
  reporting_enabled: number;
} | null {
  const row = db.prepare(`
    SELECT
      b.id AS binding_id,
      b.owner_id,
      o.name AS owner_name,
      b.started_at,
      b.enabled,
      b.reporting_enabled
    FROM reporting_owner_channel_bindings b
    INNER JOIN reporting_owners o ON o.owner_id = b.owner_id
    WHERE b.channel_id = ?
    LIMIT 1
  `).get(channelId) as any;
  if (!row) return null;
  return {
    binding_id: String(row.binding_id || '').trim(),
    owner_id: String(row.owner_id || '').trim(),
    owner_name: String(row.owner_name || '').trim(),
    started_at: String(row.started_at || '').trim(),
    enabled: Number(row.enabled || 0),
    reporting_enabled: Number(row.reporting_enabled || 0),
  };
}

function aggregateTrafficShareJson(rows: Array<{ traffic_source_share_json?: string | null; impressions?: number | null }>): string {
  const totals = new Map<string, number>();
  let weightTotal = 0;
  for (const row of rows) {
    const weight = Number.isFinite(Number(row.impressions)) && Number(row.impressions) > 0 ? Number(row.impressions) : 1;
    let parsed: Record<string, number> = {};
    try {
      parsed = JSON.parse(String(row.traffic_source_share_json || '{}'));
    } catch {
      parsed = {};
    }
    for (const [source, share] of Object.entries(parsed)) {
      const numericShare = Number(share);
      if (!Number.isFinite(numericShare) || numericShare < 0) continue;
      totals.set(source, (totals.get(source) || 0) + (numericShare * weight));
    }
    weightTotal += weight;
  }
  if (weightTotal <= 0 || totals.size === 0) return '{}';
  const result: Record<string, number> = {};
  for (const [source, weightedShare] of totals.entries()) {
    result[source] = Math.round((weightedShare / weightTotal) * 1000000) / 1000000;
  }
  return JSON.stringify(result);
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

// GET /api/channels/:id/reporting/summary
router.get('/:id/reporting/summary', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  const binding = getChannelReportingBinding(db, channelId);
  if (!binding || !binding.enabled || !binding.reporting_enabled) {
    res.json({
      enabled: false,
      owner_id: null,
      owner_name: null,
      started_at: null,
      latest_imported_at: null,
      latest_date: null,
      impressions: null,
      impressions_ctr: null,
      avg_view_duration_seconds: null,
      avg_view_percentage: null,
      traffic_source_share_json: '{}',
    });
    return;
  }

  const latestImported = db.prepare(`
    SELECT MAX(imported_at) AS latest_imported_at
    FROM reporting_raw_reports
    WHERE channel_id = ?
      AND owner_id = ?
  `).get(channelId, binding.owner_id) as any;

  const latestDateRow = db.prepare(`
    SELECT MAX(date) AS latest_date
    FROM video_reporting_daily
    WHERE channel_id = ?
      AND owner_id = ?
  `).get(channelId, binding.owner_id) as any;
  const latestDate = String(latestDateRow?.latest_date || '').trim();

  const rows = latestDate
    ? db.prepare(`
      SELECT impressions, impressions_ctr, avg_view_duration_seconds, avg_view_percentage, traffic_source_share_json
      FROM video_reporting_daily
      WHERE channel_id = ?
        AND owner_id = ?
        AND date = ?
    `).all(channelId, binding.owner_id, latestDate) as any[]
    : [];

  const impressionTotal = rows.reduce((sum, row) => sum + (Number(row?.impressions || 0) || 0), 0);
  const ctrWeightedTotal = rows.reduce((sum, row) => {
    const impressions = Number(row?.impressions || 0) || 0;
    const ctr = Number(row?.impressions_ctr || 0) || 0;
    return sum + (impressions * ctr);
  }, 0);
  const durationAvg = rows.length > 0
    ? rows.reduce((sum, row) => sum + (Number(row?.avg_view_duration_seconds || 0) || 0), 0) / rows.length
    : null;
  const percentAvg = rows.length > 0
    ? rows.reduce((sum, row) => sum + (Number(row?.avg_view_percentage || 0) || 0), 0) / rows.length
    : null;

  res.json({
    enabled: true,
    owner_id: binding.owner_id,
    owner_name: binding.owner_name,
    started_at: binding.started_at,
    latest_imported_at: latestImported?.latest_imported_at || null,
    latest_date: latestDate || null,
    impressions: impressionTotal || null,
    impressions_ctr: impressionTotal > 0 ? ctrWeightedTotal / impressionTotal : null,
    avg_view_duration_seconds: durationAvg,
    avg_view_percentage: percentAvg,
    traffic_source_share_json: aggregateTrafficShareJson(rows),
  });
});

// GET /api/channels/:id/reporting/daily
router.get('/:id/reporting/daily', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  const binding = getChannelReportingBinding(db, channelId);
  if (!binding || !binding.enabled || !binding.reporting_enabled) {
    res.json({ data: [], total: 0, page: 1, limit: 30 });
    return;
  }

  const range = String(req.query.range || '28d');
  const startDate = rangeToStartDate(range);
  const rows = db.prepare(`
    SELECT date, impressions, impressions_ctr, avg_view_duration_seconds, avg_view_percentage, traffic_source_share_json
    FROM video_reporting_daily
    WHERE channel_id = ?
      AND owner_id = ?
      AND date >= ?
    ORDER BY date DESC, video_id ASC
  `).all(channelId, binding.owner_id, startDate) as any[];

  const byDate = new Map<string, any[]>();
  for (const row of rows) {
    const date = String(row?.date || '').trim();
    const current = byDate.get(date) || [];
    current.push(row);
    byDate.set(date, current);
  }

  const data = Array.from(byDate.entries()).map(([date, items]) => {
    const impressions = items.reduce((sum, item) => sum + (Number(item?.impressions || 0) || 0), 0);
    const ctrWeighted = items.reduce((sum, item) => {
      const itemImpressions = Number(item?.impressions || 0) || 0;
      const itemCtr = Number(item?.impressions_ctr || 0) || 0;
      return sum + (itemImpressions * itemCtr);
    }, 0);
    const avgDuration = items.length > 0
      ? items.reduce((sum, item) => sum + (Number(item?.avg_view_duration_seconds || 0) || 0), 0) / items.length
      : null;
    const avgPercent = items.length > 0
      ? items.reduce((sum, item) => sum + (Number(item?.avg_view_percentage || 0) || 0), 0) / items.length
      : null;
    return {
      date,
      impressions: impressions || null,
      impressions_ctr: impressions > 0 ? ctrWeighted / impressions : null,
      avg_view_duration_seconds: avgDuration,
      avg_view_percentage: avgPercent,
      traffic_source_share_json: aggregateTrafficShareJson(items),
    };
  }).sort((a, b) => b.date.localeCompare(a.date));

  res.json({ data, total: data.length, page: 1, limit: data.length || 30 });
});

// GET /api/channels/:id/reporting/videos
router.get('/:id/reporting/videos', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  const binding = getChannelReportingBinding(db, channelId);
  if (!binding || !binding.enabled || !binding.reporting_enabled) {
    res.json({ data: [], total: 0, page: 1, limit: 100 });
    return;
  }

  const range = String(req.query.range || '28d');
  const startDate = rangeToStartDate(range);
  const limit = Math.max(1, Math.min(500, Number(req.query.limit || 100) || 100));

  const rows = db.prepare(`
    SELECT
      d.date,
      d.video_id,
      d.channel_id,
      d.owner_id,
      d.impressions,
      d.impressions_ctr,
      d.avg_view_duration_seconds,
      d.avg_view_percentage,
      d.traffic_source_share_json,
      d.computed_at,
      v.title,
      v.webpage_url
    FROM video_reporting_daily d
    LEFT JOIN videos v ON v.video_id = d.video_id
    WHERE d.channel_id = ?
      AND d.owner_id = ?
      AND d.date >= ?
    ORDER BY d.date DESC, COALESCE(d.impressions, 0) DESC, d.video_id ASC
    LIMIT ?
  `).all(channelId, binding.owner_id, startDate, limit) as any[];

  res.json({ data: rows, total: rows.length, page: 1, limit });
});

// POST /api/channels/:id/reporting/sync
router.post('/:id/reporting/sync', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  const binding = getChannelReportingBinding(db, channelId);
  if (!binding || !binding.enabled || !binding.reporting_enabled) {
    res.status(404).json({ error: 'reporting binding not found for channel' });
    return;
  }
  const result = enqueueReportingSyncForBinding(binding.binding_id, 'manual');
  getJobQueue().processNext();
  res.json(result);
});

export default router;
