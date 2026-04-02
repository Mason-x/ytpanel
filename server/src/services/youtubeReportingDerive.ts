import { getDb } from '../db.js';
import type {
  BasicReportRow,
  ReachReportRow,
  TrafficSourceReportRow,
} from './youtubeReportingImport.js';

type DeriveVideoReportingDailyInput = {
  ownerId: string;
  channelId: string;
  startedAt: string;
  reachRows: ReachReportRow[];
  basicRows: BasicReportRow[];
  trafficRows: TrafficSourceReportRow[];
};

export type VideoReportingDailyRow = {
  date: string;
  channel_id: string;
  video_id: string;
  owner_id: string;
  impressions: number | null;
  impressions_ctr: number | null;
  avg_view_duration_seconds: number | null;
  avg_view_percentage: number | null;
  traffic_source_share_json: string;
  source_report_ids_json: string;
  computed_at: string;
};

function nowSql(): string {
  return new Date().toISOString().replace('T', ' ').replace('Z', '');
}

function makeKey(date: string, videoId: string): string {
  return `${date}__${videoId}`;
}

function roundShare(value: number): number {
  return Math.round(value * 1000000) / 1000000;
}

export function deriveVideoReportingDaily(input: DeriveVideoReportingDailyInput): VideoReportingDailyRow[] {
  const records = new Map<string, VideoReportingDailyRow>();
  const trafficBuckets = new Map<string, Map<string, number>>();
  const startedAt = String(input.startedAt || '').trim();
  const computedAt = nowSql();

  const ensureRecord = (date: string, videoId: string): VideoReportingDailyRow | null => {
    if (!date || !videoId || (startedAt && date < startedAt)) return null;
    const key = makeKey(date, videoId);
    const existing = records.get(key);
    if (existing) return existing;
    const created: VideoReportingDailyRow = {
      date,
      channel_id: input.channelId,
      video_id: videoId,
      owner_id: input.ownerId,
      impressions: null,
      impressions_ctr: null,
      avg_view_duration_seconds: null,
      avg_view_percentage: null,
      traffic_source_share_json: '{}',
      source_report_ids_json: '[]',
      computed_at: computedAt,
    };
    records.set(key, created);
    return created;
  };

  for (const row of input.reachRows || []) {
    const record = ensureRecord(String(row.date || '').trim(), String(row.video_id || '').trim());
    if (!record) continue;
    record.impressions = row.impressions ?? null;
    record.impressions_ctr = row.impressions_ctr ?? null;
  }

  for (const row of input.basicRows || []) {
    const record = ensureRecord(String(row.date || '').trim(), String(row.video_id || '').trim());
    if (!record) continue;
    record.avg_view_duration_seconds = row.avg_view_duration_seconds ?? null;
    record.avg_view_percentage = row.avg_view_percentage ?? null;
  }

  for (const row of input.trafficRows || []) {
    const date = String(row.date || '').trim();
    const videoId = String(row.video_id || '').trim();
    const source = String(row.traffic_source_type || '').trim();
    const views = row.views ?? null;
    const record = ensureRecord(date, videoId);
    if (!record || !source || views == null) continue;
    const key = makeKey(date, videoId);
    const bucket = trafficBuckets.get(key) || new Map<string, number>();
    bucket.set(source, (bucket.get(source) || 0) + views);
    trafficBuckets.set(key, bucket);
  }

  for (const [key, bucket] of trafficBuckets.entries()) {
    const record = records.get(key);
    if (!record) continue;
    const total = Array.from(bucket.values()).reduce((sum, value) => sum + value, 0);
    if (total <= 0) continue;
    const shareMap: Record<string, number> = {};
    for (const [source, views] of bucket.entries()) {
      shareMap[source] = roundShare(views / total);
    }
    record.traffic_source_share_json = JSON.stringify(shareMap);
  }

  return Array.from(records.values()).sort((a, b) => {
    if (a.date !== b.date) return a.date.localeCompare(b.date);
    return a.video_id.localeCompare(b.video_id);
  });
}

export function upsertVideoReportingDaily(rows: VideoReportingDailyRow[]): number {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const db = getDb();
  const statement = db.prepare(`
    INSERT INTO video_reporting_daily (
      date, channel_id, video_id, owner_id, impressions, impressions_ctr,
      avg_view_duration_seconds, avg_view_percentage, traffic_source_share_json,
      source_report_ids_json, computed_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date, channel_id, video_id) DO UPDATE SET
      owner_id = excluded.owner_id,
      impressions = excluded.impressions,
      impressions_ctr = excluded.impressions_ctr,
      avg_view_duration_seconds = excluded.avg_view_duration_seconds,
      avg_view_percentage = excluded.avg_view_percentage,
      traffic_source_share_json = excluded.traffic_source_share_json,
      source_report_ids_json = excluded.source_report_ids_json,
      computed_at = excluded.computed_at
  `);

  const transaction = db.transaction((inputRows: VideoReportingDailyRow[]) => {
    for (const row of inputRows) {
      statement.run(
        row.date,
        row.channel_id,
        row.video_id,
        row.owner_id,
        row.impressions,
        row.impressions_ctr,
        row.avg_view_duration_seconds,
        row.avg_view_percentage,
        row.traffic_source_share_json,
        row.source_report_ids_json,
        row.computed_at,
      );
    }
  });

  transaction(rows);
  return rows.length;
}
