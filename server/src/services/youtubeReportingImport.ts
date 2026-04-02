export type ReachReportRow = {
  date: string;
  video_id: string;
  impressions: number | null;
  impressions_ctr: number | null;
};

export type BasicReportRow = {
  date: string;
  video_id: string;
  avg_view_duration_seconds: number | null;
  avg_view_percentage: number | null;
};

export type TrafficSourceReportRow = {
  date: string;
  video_id: string;
  traffic_source_type: string;
  views: number | null;
};

type ParseYoutubeReportingCsvInput = {
  reportTypeId: string;
  csvText: string;
};

type ParsedRow = Record<string, string>;

function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];
    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }
    if (char === ',' && !inQuotes) {
      cells.push(current);
      current = '';
      continue;
    }
    current += char;
  }

  cells.push(current);
  return cells.map((cell) => cell.trim());
}

function parseCsvRows(csvText: string): ParsedRow[] {
  const lines = String(csvText || '')
    .replace(/\r/g, '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length <= 1) return [];

  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  return lines.slice(1).map((line) => {
    const cells = parseCsvLine(line);
    const row: ParsedRow = {};
    headers.forEach((header, index) => {
      row[header] = String(cells[index] || '').trim();
    });
    return row;
  });
}

function getString(row: ParsedRow, keys: string[]): string {
  for (const key of keys) {
    const value = String(row[key] || '').trim();
    if (value) return value;
  }
  return '';
}

function normalizeDate(value: string): string {
  const text = String(value || '').trim();
  if (!text) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{8}$/.test(text)) {
    return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  }
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
  return text;
}

function getNumber(row: ParsedRow, keys: string[]): number | null {
  const raw = getString(row, keys);
  if (!raw) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseYoutubeReportingCsv(
  input: ParseYoutubeReportingCsvInput,
): Array<ReachReportRow | BasicReportRow | TrafficSourceReportRow> {
  const reportTypeId = String(input.reportTypeId || '').trim().toLowerCase();
  const rows = parseCsvRows(input.csvText);

  if (reportTypeId === 'channel_reach_basic_a1') {
    return rows
      .map((row) => ({
        date: normalizeDate(getString(row, ['day', 'date'])),
        video_id: getString(row, ['video_id', 'video']),
        impressions: getNumber(row, ['video_thumbnail_impressions', 'impressions']),
        impressions_ctr: getNumber(row, ['video_thumbnail_impressions_ctr', 'impressions_ctr', 'impression_ctr']),
      }))
      .filter((row) => row.date && row.video_id);
  }

  if (reportTypeId === 'channel_basic_a3') {
    return rows
      .map((row) => ({
        date: normalizeDate(getString(row, ['day', 'date'])),
        video_id: getString(row, ['video_id', 'video']),
        avg_view_duration_seconds: getNumber(row, ['average_view_duration_seconds', 'averageViewDuration']),
        avg_view_percentage: getNumber(row, ['average_view_duration_percentage', 'average_view_percentage', 'averageViewPercentage']),
      }))
      .filter((row) => row.date && row.video_id);
  }

  if (reportTypeId === 'channel_traffic_source_a3') {
    return rows
      .map((row) => ({
        date: normalizeDate(getString(row, ['day', 'date'])),
        video_id: getString(row, ['video_id', 'video']),
        traffic_source_type: getString(row, ['traffic_source_type', 'insightTrafficSourceType']),
        views: getNumber(row, ['views']),
      }))
      .filter((row) => row.date && row.video_id && row.traffic_source_type);
  }

  return [];
}
