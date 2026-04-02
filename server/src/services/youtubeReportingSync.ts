import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { getDb } from '../db.js';
import { insertReportingRequestLog } from './reportingProxyProbe.js';
import { updateReportingRequestLog } from './reportingProxyProbe.js';
import type { ReportingOwnerBindingRow, ReportingOwnerRow } from './reportingOwners.js';
import { deriveVideoReportingDaily, upsertVideoReportingDaily } from './youtubeReportingDerive.js';
import {
  parseYoutubeReportingCsv,
  type BasicReportRow,
  type ReachReportRow,
  type TrafficSourceReportRow,
} from './youtubeReportingImport.js';
import {
  reportingAuthorizedJsonRequest,
  reportingAuthorizedTextRequest,
} from './youtubeReportingClient.js';

export const REQUIRED_REPORT_TYPE_IDS = [
  'channel_reach_basic_a1',
  'channel_basic_a3',
  'channel_traffic_source_a3',
] as const;

type ReportingRemoteJob = {
  id: string;
  reportTypeId: string;
};

type ReportingRemoteReport = {
  id: string;
  jobId: string;
  startDate: string | null;
  endDate: string | null;
  downloadUrl: string;
};

type ReportingSyncDependencies = {
  listReportTypes?: (owner: ReportingOwnerRow) => Promise<string[]>;
  listJobs?: (owner: ReportingOwnerRow) => Promise<ReportingRemoteJob[]>;
  createJob?: (owner: ReportingOwnerRow, reportTypeId: string) => Promise<ReportingRemoteJob>;
  listReports?: (owner: ReportingOwnerRow, job: ReportingRemoteJob) => Promise<ReportingRemoteReport[]>;
  downloadReport?: (owner: ReportingOwnerRow, report: ReportingRemoteReport) => Promise<string>;
};

function resolveReportingDataRoot(): string {
  return path.resolve(process.cwd(), 'data', 'reporting');
}

function normalizeDateOnly(value: unknown): string | null {
  const text = String(value || '').trim();
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function safeSegment(value: unknown, fallback: string): string {
  const normalized = String(value || '').trim().replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_');
  return normalized || fallback;
}

function checksumText(text: string): string {
  return crypto.createHash('sha256').update(text).digest('hex');
}

function getBindingWithOwner(bindingId: string): { binding: ReportingOwnerBindingRow; owner: ReportingOwnerRow } | null {
  const row = getDb().prepare(`
    SELECT
      b.id,
      b.owner_id,
      b.channel_id,
      b.enabled,
      b.reporting_enabled,
      b.started_at,
      b.created_at,
      b.updated_at,
      o.name AS owner_name,
      o.client_id,
      o.client_secret,
      o.refresh_token,
      o.proxy_url,
      o.enabled AS owner_enabled,
      o.reporting_enabled AS owner_reporting_enabled,
      o.last_token_refresh_at,
      o.last_sync_at,
      o.last_error
    FROM reporting_owner_channel_bindings b
    INNER JOIN reporting_owners o ON o.owner_id = b.owner_id
    WHERE b.id = ?
  `).get(bindingId) as any;

  if (!row) return null;
  return {
    binding: {
      id: String(row.id || '').trim(),
      owner_id: String(row.owner_id || '').trim(),
      channel_id: String(row.channel_id || '').trim(),
      enabled: Number(row.enabled || 0),
      reporting_enabled: Number(row.reporting_enabled || 0),
      started_at: String(row.started_at || '').trim(),
      created_at: row.created_at ? String(row.created_at).trim() : null,
      updated_at: row.updated_at ? String(row.updated_at).trim() : null,
    },
    owner: {
      owner_id: String(row.owner_id || '').trim(),
      name: String(row.owner_name || '').trim(),
      client_id: String(row.client_id || '').trim(),
      client_secret: String(row.client_secret || '').trim(),
      refresh_token: String(row.refresh_token || '').trim(),
      proxy_url: String(row.proxy_url || '').trim(),
      enabled: Number(row.owner_enabled || 0),
      reporting_enabled: Number(row.owner_reporting_enabled || 0),
      started_at: null,
      last_token_refresh_at: row.last_token_refresh_at ? String(row.last_token_refresh_at).trim() : null,
      last_sync_at: row.last_sync_at ? String(row.last_sync_at).trim() : null,
      last_error: row.last_error ? String(row.last_error).trim() : null,
    },
  };
}

function listActiveBindingRows(): ReportingOwnerBindingRow[] {
  return getDb().prepare(`
    SELECT b.id, b.owner_id, b.channel_id, b.enabled, b.reporting_enabled, b.started_at, b.created_at, b.updated_at
    FROM reporting_owner_channel_bindings b
    INNER JOIN reporting_owners o ON o.owner_id = b.owner_id
    WHERE b.enabled = 1
      AND b.reporting_enabled = 1
      AND o.enabled = 1
      AND o.reporting_enabled = 1
    ORDER BY datetime(b.created_at) ASC, b.id ASC
  `).all() as ReportingOwnerBindingRow[];
}

function productionListReportTypes(owner: ReportingOwnerRow): Promise<string[]> {
  return reportingAuthorizedJsonRequest(owner, 'https://youtubereporting.googleapis.com/v1/reportTypes?pageSize=200')
    .then(({ payload }) => {
      const items = Array.isArray(payload?.reportTypes) ? payload.reportTypes : [];
      return items
        .map((item: any) => String(item?.id || '').trim())
        .filter(Boolean);
    });
}

function productionListJobs(owner: ReportingOwnerRow): Promise<ReportingRemoteJob[]> {
  return reportingAuthorizedJsonRequest(owner, 'https://youtubereporting.googleapis.com/v1/jobs?pageSize=200')
    .then(({ payload }) => {
      const items = Array.isArray(payload?.jobs) ? payload.jobs : [];
      return items
        .map((item: any) => ({
          id: String(item?.id || '').trim(),
          reportTypeId: String(item?.reportTypeId || '').trim(),
        }))
        .filter((item: ReportingRemoteJob) => item.id && item.reportTypeId);
    });
}

function productionCreateJob(owner: ReportingOwnerRow, reportTypeId: string): Promise<ReportingRemoteJob> {
  return reportingAuthorizedJsonRequest(
    owner,
    'https://youtubereporting.googleapis.com/v1/jobs',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reportTypeId }),
    },
  ).then(({ status, payload }) => {
    if (!(status >= 200 && status < 300)) {
      throw new Error(`failed to create reporting job for ${reportTypeId}`);
    }
    return {
      id: String(payload?.id || '').trim(),
      reportTypeId: String(payload?.reportTypeId || reportTypeId).trim(),
    };
  });
}

function productionListReports(owner: ReportingOwnerRow, job: ReportingRemoteJob): Promise<ReportingRemoteReport[]> {
  return reportingAuthorizedJsonRequest(owner, `https://youtubereporting.googleapis.com/v1/jobs/${encodeURIComponent(job.id)}/reports?pageSize=200`)
    .then(({ payload }) => {
      const items = Array.isArray(payload?.reports) ? payload.reports : [];
      return items
        .map((item: any) => ({
          id: String(item?.id || '').trim(),
          jobId: job.id,
          startDate: normalizeDateOnly(item?.startTime || item?.start_date || item?.startTimeMs),
          endDate: normalizeDateOnly(item?.endTime || item?.end_date || item?.endTimeMs),
          downloadUrl: String(item?.downloadUrl || item?.download_url || '').trim(),
        }))
        .filter((item: ReportingRemoteReport) => item.id && item.downloadUrl);
    });
}

function productionDownloadReport(owner: ReportingOwnerRow, report: ReportingRemoteReport): Promise<string> {
  return reportingAuthorizedTextRequest(owner, report.downloadUrl).then(({ status, body }) => {
    if (!(status >= 200 && status < 300)) {
      throw new Error(`failed to download reporting report ${report.id}`);
    }
    return body;
  });
}

function updateOwnerSyncStatus(ownerId: string, errorMessage: string | null): void {
  getDb().prepare(`
    UPDATE reporting_owners
    SET last_sync_at = datetime('now'),
        last_error = ?
    WHERE owner_id = ?
  `).run(errorMessage, ownerId);
}

function writeRawReportFile(
  ownerId: string,
  channelId: string,
  reportTypeId: string,
  report: ReportingRemoteReport,
  csvText: string,
): { filePath: string; checksum: string; fileSize: number } {
  const root = resolveReportingDataRoot();
  const directory = path.join(
    root,
    safeSegment(ownerId, 'owner'),
    safeSegment(channelId, 'channel'),
    safeSegment(reportTypeId, 'report-type'),
  );
  fs.mkdirSync(directory, { recursive: true });
  const filePath = path.join(
    directory,
    `${safeSegment(report.startDate || 'unknown-start', 'unknown-start')}_${safeSegment(report.endDate || 'unknown-end', 'unknown-end')}_${safeSegment(report.id, 'report')}.csv`,
  );
  fs.writeFileSync(filePath, csvText, 'utf8');
  const checksum = checksumText(csvText);
  const stat = fs.statSync(filePath);
  return {
    filePath,
    checksum,
    fileSize: stat.size,
  };
}

function upsertRawReportMetadata(
  ownerId: string,
  channelId: string,
  reportTypeId: string,
  jobId: string,
  report: ReportingRemoteReport,
  filePath: string,
  fileSize: number,
  checksum: string,
): void {
  const db = getDb();
  db.prepare(`
    INSERT INTO reporting_raw_reports (
      id, owner_id, channel_id, report_type_id, remote_job_id, remote_report_id,
      start_date, end_date, file_path, file_size, checksum, downloaded_at, imported_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
    ON CONFLICT(id) DO UPDATE SET
      file_path = excluded.file_path,
      file_size = excluded.file_size,
      checksum = excluded.checksum,
      imported_at = datetime('now')
  `).run(
    `${ownerId}:${report.id}`,
    ownerId,
    channelId,
    reportTypeId,
    jobId,
    report.id,
    report.startDate,
    report.endDate,
    filePath,
    fileSize,
    checksum,
  );

  db.prepare(`
    INSERT INTO reporting_job_state (
      id, owner_id, channel_id, report_type_id, remote_job_id, remote_report_id,
      report_start_date, report_end_date, discovered_at, downloaded_at, imported_at,
      status, raw_file_path, checksum, error_message, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), datetime('now'),
      'imported', ?, ?, NULL, datetime('now'))
    ON CONFLICT(owner_id, remote_report_id) DO UPDATE SET
      report_type_id = excluded.report_type_id,
      remote_job_id = excluded.remote_job_id,
      report_start_date = excluded.report_start_date,
      report_end_date = excluded.report_end_date,
      downloaded_at = datetime('now'),
      imported_at = datetime('now'),
      status = 'imported',
      raw_file_path = excluded.raw_file_path,
      checksum = excluded.checksum,
      error_message = NULL,
      updated_at = datetime('now')
  `).run(
    `${ownerId}:${report.id}`,
    ownerId,
    channelId,
    reportTypeId,
    jobId,
    report.id,
    report.startDate,
    report.endDate,
    filePath,
    checksum,
  );
}

function rebuildDerivedDailyForBinding(binding: ReportingOwnerBindingRow, owner: ReportingOwnerRow): number {
  const rows = getDb().prepare(`
    SELECT report_type_id, file_path
    FROM reporting_raw_reports
    WHERE owner_id = ?
      AND channel_id = ?
      AND report_type_id IN (?, ?, ?)
    ORDER BY start_date ASC, end_date ASC, downloaded_at ASC
  `).all(
    owner.owner_id,
    binding.channel_id,
    REQUIRED_REPORT_TYPE_IDS[0],
    REQUIRED_REPORT_TYPE_IDS[1],
    REQUIRED_REPORT_TYPE_IDS[2],
  ) as Array<{ report_type_id: string; file_path: string }>;

  const reachRows: ReachReportRow[] = [];
  const basicRows: BasicReportRow[] = [];
  const trafficRows: TrafficSourceReportRow[] = [];

  for (const row of rows) {
    const filePath = String(row.file_path || '').trim();
    if (!filePath || !fs.existsSync(filePath)) continue;
    const csvText = fs.readFileSync(filePath, 'utf8');
    const parsed = parseYoutubeReportingCsv({
      reportTypeId: row.report_type_id,
      csvText,
    });
    if (row.report_type_id === 'channel_reach_basic_a1') {
      reachRows.push(...(parsed as ReachReportRow[]));
    } else if (row.report_type_id === 'channel_basic_a3') {
      basicRows.push(...(parsed as BasicReportRow[]));
    } else if (row.report_type_id === 'channel_traffic_source_a3') {
      trafficRows.push(...(parsed as TrafficSourceReportRow[]));
    }
  }

  const derivedRows = deriveVideoReportingDaily({
    ownerId: owner.owner_id,
    channelId: binding.channel_id,
    startedAt: binding.started_at,
    reachRows,
    basicRows,
    trafficRows,
  });
  const upserted = upsertVideoReportingDaily(derivedRows);

  getDb().prepare(`
    UPDATE reporting_job_state
    SET derived_at = datetime('now'),
        status = 'derived',
        updated_at = datetime('now')
    WHERE owner_id = ?
      AND channel_id = ?
      AND status IN ('downloaded', 'imported', 'derive_failed')
  `).run(owner.owner_id, binding.channel_id);

  return upserted;
}

export function enqueueReportingSyncForBinding(bindingId: string, trigger: 'manual' | 'cron' = 'manual') {
  const binding = getDb().prepare(`
    SELECT id, owner_id, channel_id
    FROM reporting_owner_channel_bindings
    WHERE id = ?
  `).get(bindingId) as { id?: string; owner_id?: string; channel_id?: string } | undefined;
  if (!String(binding?.id || '').trim()) {
    throw new Error('reporting binding not found');
  }

  const activeJob = getDb().prepare(`
    SELECT job_id, status
    FROM jobs
    WHERE type = 'sync_reporting_channel'
      AND status IN ('queued', 'running', 'canceling')
      AND json_extract(payload_json, '$.binding_id') = ?
    ORDER BY datetime(created_at) DESC
    LIMIT 1
  `).get(bindingId) as { job_id?: string; status?: string } | undefined;
  if (String(activeJob?.job_id || '').trim()) {
    return {
      job_id: String(activeJob?.job_id || '').trim(),
      status: String(activeJob?.status || 'running').trim() as 'queued' | 'running' | 'canceling',
    };
  }

  const jobId = uuidv4();
  getDb().prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'sync_reporting_channel', ?, 'queued')
  `).run(jobId, JSON.stringify({
    binding_id: bindingId,
    owner_id: String(binding?.owner_id || '').trim(),
    channel_id: String(binding?.channel_id || '').trim(),
    trigger,
  }));
  return { job_id: jobId, status: 'queued' as const };
}

export function enqueueDailyReportingSyncs(trigger: 'manual' | 'cron' = 'cron') {
  const activeBindings = listActiveBindingRows();
  for (const binding of activeBindings) {
    enqueueReportingSyncForBinding(binding.id, trigger);
  }
  return { queued_count: activeBindings.length };
}

export async function syncReportingBinding(
  bindingId: string,
  dependencies: ReportingSyncDependencies = {},
): Promise<{
  owner_id: string;
  channel_id: string;
  downloaded_reports: number;
  derived_rows: number;
}> {
  const record = getBindingWithOwner(bindingId);
  if (!record) {
    throw new Error('reporting binding not found');
  }
  const { binding, owner } = record;

  if (!Number(binding.enabled) || !Number(binding.reporting_enabled)) {
    throw new Error('reporting binding is disabled');
  }
  if (!Number(owner.enabled) || !Number(owner.reporting_enabled)) {
    throw new Error('reporting owner is disabled');
  }

  const listReportTypes = dependencies.listReportTypes || productionListReportTypes;
  const listJobs = dependencies.listJobs || productionListJobs;
  const createJob = dependencies.createJob || productionCreateJob;
  const listReports = dependencies.listReports || productionListReports;
  const downloadReport = dependencies.downloadReport || productionDownloadReport;

  const withRequestLog = async <T,>(
    requestKind: string,
    requestUrl: string,
    run: () => Promise<T>,
    responseMeta: (value: T) => unknown = () => ({}),
  ): Promise<T> => {
    const startedAt = new Date();
    const logId = insertReportingRequestLog({
      owner_id: owner.owner_id,
      channel_id: binding.channel_id,
      request_kind: requestKind,
      request_url: requestUrl,
      proxy_url_snapshot: owner.proxy_url || null,
      success: false,
      error_code: 'pending',
      error_message: `${requestKind} started`,
      started_at: startedAt.toISOString().replace('T', ' ').replace('Z', ''),
      response_meta_json: '{}',
    });
    try {
      const value = await run();
      updateReportingRequestLog(logId, {
        status_code: 200,
        success: true,
        error_code: null,
        error_message: null,
        finished_at: new Date().toISOString().replace('T', ' ').replace('Z', ''),
        duration_ms: Date.now() - startedAt.getTime(),
        response_meta_json: JSON.stringify(responseMeta(value) || {}),
      });
      return value;
    } catch (error: any) {
      updateReportingRequestLog(logId, {
        status_code: null,
        success: false,
        error_code: `${requestKind}_failed`,
        error_message: String(error?.message || error || `${requestKind}_failed`),
        finished_at: new Date().toISOString().replace('T', ' ').replace('Z', ''),
        duration_ms: Date.now() - startedAt.getTime(),
        response_meta_json: '{}',
      });
      throw error;
    }
  };

  try {
    const availableReportTypeIds = await withRequestLog(
      'report_types_list',
      'https://youtubereporting.googleapis.com/v1/reportTypes?pageSize=200',
      () => listReportTypes(owner),
      (value) => ({ count: Array.isArray(value) ? value.length : 0 }),
    );
    const targetReportTypeIds = REQUIRED_REPORT_TYPE_IDS.filter((reportTypeId) => availableReportTypeIds.length === 0 || availableReportTypeIds.includes(reportTypeId));
    if (targetReportTypeIds.length === 0) {
      throw new Error('required reporting report types are unavailable');
    }

    const existingJobs = await withRequestLog(
      'reporting_jobs_list',
      'https://youtubereporting.googleapis.com/v1/jobs?pageSize=200',
      () => listJobs(owner),
      (value) => ({ count: Array.isArray(value) ? value.length : 0 }),
    );
    const jobsByReportType = new Map(existingJobs.map((job) => [job.reportTypeId, job] as const));

    for (const reportTypeId of targetReportTypeIds) {
      if (!jobsByReportType.has(reportTypeId)) {
        const created = await withRequestLog(
          'reporting_job_create',
          'https://youtubereporting.googleapis.com/v1/jobs',
          () => createJob(owner, reportTypeId),
          (value) => value,
        );
        jobsByReportType.set(reportTypeId, created);
      }
    }

    let downloadedReports = 0;
    for (const reportTypeId of targetReportTypeIds) {
      const job = jobsByReportType.get(reportTypeId);
      if (!job) continue;
      const reports = await withRequestLog(
        'reporting_reports_list',
        `https://youtubereporting.googleapis.com/v1/jobs/${encodeURIComponent(job.id)}/reports?pageSize=200`,
        () => listReports(owner, job),
        (value) => ({ count: Array.isArray(value) ? value.length : 0, job_id: job.id }),
      );
      for (const report of reports) {
        const existing = getDb().prepare(`
          SELECT raw_file_path
          FROM reporting_job_state
          WHERE owner_id = ? AND remote_report_id = ?
        `).get(owner.owner_id, report.id) as { raw_file_path?: string } | undefined;
        const existingFile = String(existing?.raw_file_path || '').trim();
        if (existingFile && fs.existsSync(existingFile)) {
          continue;
        }

        const csvText = await withRequestLog(
          'report_download',
          report.downloadUrl,
          () => downloadReport(owner, report),
          (value) => ({ bytes: String(value || '').length, remote_report_id: report.id }),
        );
        const { filePath, checksum, fileSize } = writeRawReportFile(
          owner.owner_id,
          binding.channel_id,
          reportTypeId,
          report,
          csvText,
        );
        upsertRawReportMetadata(
          owner.owner_id,
          binding.channel_id,
          reportTypeId,
          job.id,
          report,
          filePath,
          fileSize,
          checksum,
        );
        downloadedReports += 1;
      }
    }

    const derivedRows = rebuildDerivedDailyForBinding(binding, owner);
    updateOwnerSyncStatus(owner.owner_id, null);

    return {
      owner_id: owner.owner_id,
      channel_id: binding.channel_id,
      downloaded_reports: downloadedReports,
      derived_rows: derivedRows,
    };
  } catch (error: any) {
    updateOwnerSyncStatus(owner.owner_id, String(error?.message || error || 'unknown'));
    throw error;
  }
}
