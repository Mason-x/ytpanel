import { Router, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { getDb } from '../db.js';

const router = Router();

interface ParsedResearchInput {
  channel_id: string;
  handle: string | null;
  input: string;
}

interface ResearchGrowthPoint {
  date: string;
  subscriber_count: number | null;
  view_count: number | null;
}

interface ResearchGrowthData {
  daily_view_increase: number | null;
  daily_subscriber_increase: number | null;
  growth_series_30d: ResearchGrowthPoint[];
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

function buildResearchGrowthMap(
  db: ReturnType<typeof getDb>,
  channelRows: any[],
): Map<string, ResearchGrowthData> {
  const growthByChannel = new Map<string, ResearchGrowthData>();
  if (!Array.isArray(channelRows) || channelRows.length === 0) {
    return growthByChannel;
  }

  const channelIds = channelRows
    .map((row) => String(row?.channel_id || '').trim())
    .filter(Boolean);
  if (channelIds.length === 0) {
    return growthByChannel;
  }

  const dateRange = buildRecentDateRange(30);
  const startDate = dateRange[0];
  const endDate = dateRange[dateRange.length - 1];
  const placeholders = channelIds.map(() => '?').join(', ');
  const dailyRows = db.prepare(`
    SELECT channel_id, date, subscriber_count, view_count
    FROM research_channel_daily
    WHERE channel_id IN (${placeholders}) AND date >= ? AND date <= ?
    ORDER BY channel_id ASC, date ASC
  `).all(...channelIds, startDate, endDate) as any[];

  const previousRows = db.prepare(`
    SELECT rd.channel_id as channel_id, rd.subscriber_count as subscriber_count, rd.view_count as view_count
    FROM research_channel_daily rd
    INNER JOIN (
      SELECT channel_id, MAX(date) as max_date
      FROM research_channel_daily
      WHERE channel_id IN (${placeholders}) AND date < ?
      GROUP BY channel_id
    ) prev
      ON prev.channel_id = rd.channel_id
     AND prev.max_date = rd.date
  `).all(...channelIds, startDate) as any[];

  const grouped = new Map<string, Map<string, { subscriber_count: number | null; view_count: number | null }>>();
  for (const item of dailyRows) {
    const channelId = String(item?.channel_id || '').trim();
    if (!channelId) continue;
    const date = String(item?.date || '').trim();
    if (!date) continue;
    const dateMap = grouped.get(channelId) || new Map<string, { subscriber_count: number | null; view_count: number | null }>();
    dateMap.set(date, {
      subscriber_count: toNullableInt(item?.subscriber_count),
      view_count: toNullableInt(item?.view_count),
    });
    grouped.set(channelId, dateMap);
  }

  const previousByChannel = new Map<string, { subscriber_count: number | null; view_count: number | null }>();
  for (const item of previousRows) {
    const channelId = String(item?.channel_id || '').trim();
    if (!channelId) continue;
    previousByChannel.set(channelId, {
      subscriber_count: toNullableInt(item?.subscriber_count),
      view_count: toNullableInt(item?.view_count),
    });
  }

  for (const row of channelRows) {
    const channelId = String(row?.channel_id || '').trim();
    if (!channelId) continue;

    const dateMap = grouped.get(channelId) || new Map<string, { subscriber_count: number | null; view_count: number | null }>();
    let carrySub = previousByChannel.get(channelId)?.subscriber_count ?? null;
    let carryView = previousByChannel.get(channelId)?.view_count ?? null;
    let points: ResearchGrowthPoint[] = dateRange.map((date) => {
      const incoming = dateMap.get(date);
      if (incoming) {
        if (incoming.subscriber_count != null) carrySub = incoming.subscriber_count;
        if (incoming.view_count != null) carryView = incoming.view_count;
      }
      return {
        date,
        subscriber_count: carrySub,
        view_count: carryView,
      };
    });

    if (!points.some((item) => item.subscriber_count != null || item.view_count != null)) {
      const fallbackSubs = toNullableInt(row?.subscriber_count);
      const fallbackViews = toNullableInt(row?.view_count);
      if (fallbackSubs != null || fallbackViews != null) {
        points = dateRange.map((date) => ({
          date,
          subscriber_count: fallbackSubs,
          view_count: fallbackViews,
        }));
      }
    }

    const validViewPoints = points.filter((item) => item.view_count != null);
    const firstView = validViewPoints.length > 0 ? validViewPoints[0] : null;
    const latestView = validViewPoints.length > 0 ? validViewPoints[validViewPoints.length - 1] : null;
    const dailyViewIncrease = (
      firstView && latestView && firstView.view_count != null && latestView.view_count != null
    )
      ? (latestView.view_count - firstView.view_count)
      : null;

    const validSubPoints = points.filter((item) => item.subscriber_count != null);
    const firstSub = validSubPoints.length > 0 ? validSubPoints[0] : null;
    const latestSub = validSubPoints.length > 0 ? validSubPoints[validSubPoints.length - 1] : null;
    const dailySubscriberIncrease = (
      firstSub && latestSub && firstSub.subscriber_count != null && latestSub.subscriber_count != null
    )
      ? (latestSub.subscriber_count - firstSub.subscriber_count)
      : null;

    growthByChannel.set(channelId, {
      daily_view_increase: dailyViewIncrease,
      daily_subscriber_increase: dailySubscriberIncrease,
      growth_series_30d: points,
    });
  }

  return growthByChannel;
}

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function normalizeHandleName(value: string): string {
  return value.trim().replace(/^@+/, '').replace(/\/+$/, '');
}

function parseResearchChannelInput(input: string): ParsedResearchInput | null {
  const raw = String(input || '').trim();
  if (!raw) return null;

  const candidate = raw.includes('://')
    ? raw
    : (raw.startsWith('www.') || raw.startsWith('youtube.com'))
      ? `https://${raw}`
      : null;

  if (candidate) {
    try {
      const url = new URL(candidate);
      const host = url.hostname.replace(/^www\./, '').toLowerCase();
      if (host.endsWith('youtube.com')) {
        const parts = url.pathname.split('/').filter(Boolean).map(safeDecode);
        if (parts.length >= 2 && parts[0] === 'channel') {
          const channelId = parts[1].trim();
          if (channelId) return { channel_id: channelId, handle: null, input: raw };
        }

        if (parts.length >= 1 && parts[0].startsWith('@')) {
          const name = normalizeHandleName(parts[0]);
          if (name) return { channel_id: name, handle: `@${name}`, input: raw };
        }

        if (parts.length >= 2 && (parts[0] === 'c' || parts[0] === 'user')) {
          const name = normalizeHandleName(parts[1]);
          if (name) return { channel_id: name, handle: `@${name}`, input: raw };
        }
      }
    } catch {
      // fallback
    }
  }

  const decoded = safeDecode(raw);
  if (decoded.startsWith('UC')) {
    return { channel_id: decoded, handle: null, input: raw };
  }

  const name = normalizeHandleName(decoded);
  if (!name) return null;
  return { channel_id: name, handle: `@${name}`, input: raw };
}

function normalizeTagList(input: unknown): string[] {
  const items = Array.isArray(input)
    ? input
    : (typeof input === 'string' ? input.split(/[,\n，]+/) : []);

  const tags: string[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    const value = String(item || '').trim().replace(/^#+/, '');
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    tags.push(value);
  }
  return tags;
}

function parseTagsJson(raw: unknown): string[] {
  if (Array.isArray(raw)) return normalizeTagList(raw);
  if (typeof raw !== 'string') return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return normalizeTagList(parsed);
  } catch {
    return [];
  }
}

function toStringArray(input: unknown): string[] {
  if (Array.isArray(input)) {
    return input.map((item) => String(item || '').trim()).filter(Boolean);
  }
  if (typeof input === 'string') {
    return input.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
  }
  return [];
}

function writeCompletedAuditJob(
  db: ReturnType<typeof getDb>,
  type: string,
  payload: Record<string, unknown>,
  messages: string[],
): string {
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (
      job_id, type, payload_json, status, progress, created_at, started_at, finished_at
    )
    VALUES (?, ?, ?, 'done', 100, datetime('now'), datetime('now'), datetime('now'))
  `).run(jobId, type, JSON.stringify(payload || {}));

  const insertEvent = db.prepare(`
    INSERT INTO job_events (job_id, ts, level, message)
    VALUES (?, datetime('now'), ?, ?)
  `);
  for (const message of messages) {
    insertEvent.run(jobId, 'info', message);
  }
  return jobId;
}

// GET /api/research/channels
router.get('/channels', (req: Request, res: Response) => {
  const db = getDb();
  const search = String(req.query.search || '').trim();
  const tag = String(req.query.tag || '').trim();

  let where = 'WHERE 1=1';
  const params: any[] = [];
  if (search) {
    where += ' AND (title LIKE ? OR handle LIKE ? OR channel_id LIKE ?)';
    params.push(`%${search}%`, `%${search}%`, `%${search}%`);
  }
  if (tag) {
    where += ' AND tags_json LIKE ?';
    params.push(`%${tag}%`);
  }

  const rows = db.prepare(`
    SELECT *
    FROM research_channels
    ${where}
    ORDER BY subscriber_count DESC, created_at DESC
  `).all(...params) as any[];

  const growthMap = buildResearchGrowthMap(db, rows);
  const data = rows.map((row) => ({
    ...row,
    tags: parseTagsJson(row.tags_json),
    ...(growthMap.get(String(row?.channel_id || '').trim()) || {
      daily_view_increase: null,
      daily_subscriber_increase: null,
      growth_series_30d: [],
    }),
  }));
  res.json({ data, total: data.length });
});

// POST /api/research/channels/bulk-add
router.post('/channels/bulk-add', async (req: Request, res: Response) => {
  const rawInputs = toStringArray(req.body?.inputs);
  const defaultTags = normalizeTagList(req.body?.tags);

  if (rawInputs.length === 0) {
    res.status(400).json({ error: '请提供至少一个频道链接/频道ID/@handle' });
    return;
  }

  const parsed: ParsedResearchInput[] = [];
  const dedupe = new Set<string>();
  let invalidCount = 0;
  for (const raw of rawInputs) {
    const item = parseResearchChannelInput(raw);
    if (!item) {
      invalidCount += 1;
      continue;
    }
    const key = String(item.channel_id || '').trim().toLowerCase();
    if (!key || dedupe.has(key)) continue;
    dedupe.add(key);
    parsed.push(item);
  }

  if (parsed.length === 0) {
    res.status(400).json({ error: '输入无法识别，请使用 YouTube 频道链接/UC频道ID/@handle' });
    return;
  }

  const db = getDb();
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'research_bulk_add', ?, 'queued')
  `).run(jobId, JSON.stringify({ channels: parsed, tags: defaultTags }));

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  res.json({
    job_id: jobId,
    status: 'queued',
    total: parsed.length,
    invalid_count: invalidCount,
  });
});

// PATCH /api/research/channels/:id
router.patch('/channels/:id', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  if (!channelId) {
    res.status(400).json({ error: 'channel_id is required' });
    return;
  }

  const existing = db.prepare('SELECT channel_id FROM research_channels WHERE channel_id = ?').get(channelId) as any;
  if (!existing) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }

  const tags = normalizeTagList(req.body?.tags);
  db.prepare(`
    UPDATE research_channels
    SET tags_json = ?, updated_at = datetime('now')
    WHERE channel_id = ?
  `).run(JSON.stringify(tags), channelId);

  const row = db.prepare('SELECT * FROM research_channels WHERE channel_id = ?').get(channelId) as any;
  res.json({ ...row, tags: parseTagsJson(row?.tags_json) });
});

// DELETE /api/research/channels/:id
router.delete('/channels/:id', (req: Request, res: Response) => {
  const db = getDb();
  const channelId = String(req.params.id || '').trim();
  if (!channelId) {
    res.status(400).json({ error: 'channel_id is required' });
    return;
  }

  const row = db.prepare('SELECT channel_id, title FROM research_channels WHERE channel_id = ?').get(channelId) as any;
  if (!row) {
    res.status(404).json({ error: 'Channel not found' });
    return;
  }

  db.prepare('DELETE FROM research_channel_daily WHERE channel_id = ?').run(channelId);
  db.prepare('DELETE FROM research_channels WHERE channel_id = ?').run(channelId);
  const auditJobId = writeCompletedAuditJob(
    db,
    'research_delete_channel',
    { channel_id: channelId, title: row.title || '' },
    [`Research channel deleted: ${channelId}`, `Title: ${row.title || channelId}`],
  );
  res.json({ deleted: true, channel_id: channelId, title: row.title || '', job_id: auditJobId });
});

export default router;
