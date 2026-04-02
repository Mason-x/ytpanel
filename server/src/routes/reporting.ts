import { Router, Request, Response } from 'express';
import { getDb } from '../db.js';
import { getJobQueue } from '../services/jobQueue.js';
import {
  createReportingBinding,
  createReportingOwner,
  deleteReportingBinding,
  deleteReportingOwner,
  listReportingBindings,
  listReportingOwners,
  updateReportingBinding,
  updateReportingOwner,
  type ReportingOwnerBindingRow,
  type ReportingOwnerRow,
} from '../services/reportingOwners.js';
import { probeReportingProxy } from '../services/reportingProxyProbe.js';
import { enqueueReportingSyncForBinding } from '../services/youtubeReportingSync.js';

const router = Router();

type ReportingOwnerUsageSummary = {
  owner_id: string;
  request_count_24h: number;
  success_count_24h: number;
  failure_count_24h: number;
  success_rate_24h: number;
  download_count_24h: number;
  last_token_refresh_at?: string | null;
  last_sync_at?: string | null;
  last_error?: string | null;
  avg_duration_ms_24h?: number | null;
};

export function buildReportingOwnerUsageSummary(row: {
  owner_id: string;
  request_count_24h?: number | null;
  success_count_24h?: number | null;
  failure_count_24h?: number | null;
  download_count_24h?: number | null;
  avg_duration_ms_24h?: number | null;
  last_token_refresh_at?: string | null;
  last_sync_at?: string | null;
  last_error?: string | null;
}): ReportingOwnerUsageSummary {
  const requestCount = Number(row.request_count_24h || 0);
  const successCount = Number(row.success_count_24h || 0);
  const failureCount = Number(row.failure_count_24h || 0);
  return {
    owner_id: String(row.owner_id || '').trim(),
    request_count_24h: requestCount,
    success_count_24h: successCount,
    failure_count_24h: failureCount,
    success_rate_24h: requestCount > 0 ? successCount / requestCount : 0,
    download_count_24h: Number(row.download_count_24h || 0),
    last_token_refresh_at: row.last_token_refresh_at ? String(row.last_token_refresh_at).trim() : null,
    last_sync_at: row.last_sync_at ? String(row.last_sync_at).trim() : null,
    last_error: row.last_error ? String(row.last_error).trim() : null,
    avg_duration_ms_24h: row.avg_duration_ms_24h == null ? null : Number(row.avg_duration_ms_24h),
  };
}

function getReportingOwnerUsage(ownerId: string): ReportingOwnerUsageSummary {
  const db = getDb();
  const row = db.prepare(`
    SELECT
      o.owner_id,
      SUM(CASE WHEN l.started_at >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS request_count_24h,
      SUM(CASE WHEN l.started_at >= datetime('now', '-1 day') AND COALESCE(l.success, 0) = 1 THEN 1 ELSE 0 END) AS success_count_24h,
      SUM(CASE WHEN l.started_at >= datetime('now', '-1 day') AND COALESCE(l.success, 0) <> 1 THEN 1 ELSE 0 END) AS failure_count_24h,
      SUM(CASE WHEN l.started_at >= datetime('now', '-1 day') AND l.request_kind = 'report_download' THEN 1 ELSE 0 END) AS download_count_24h,
      AVG(CASE WHEN l.started_at >= datetime('now', '-1 day') THEN l.duration_ms ELSE NULL END) AS avg_duration_ms_24h,
      o.last_token_refresh_at,
      o.last_sync_at,
      o.last_error
    FROM reporting_owners o
    LEFT JOIN reporting_request_logs l ON l.owner_id = o.owner_id
    WHERE o.owner_id = ?
    GROUP BY o.owner_id, o.last_token_refresh_at, o.last_sync_at, o.last_error
  `).get(ownerId) as any;
  return buildReportingOwnerUsageSummary(row || { owner_id: ownerId });
}

export function serializeReportingOwnerResponse(
  owner: ReportingOwnerRow,
  bindings: ReportingOwnerBindingRow[],
  usage: ReportingOwnerUsageSummary,
) {
  return {
    ...owner,
    bindings,
    usage,
  };
}

function listReportingRequestLogs(ownerId: string, limit = 100) {
  return getDb().prepare(`
    SELECT
      id, owner_id, channel_id, request_kind, request_url, proxy_url_snapshot,
      status_code, success, error_code, error_message, started_at, finished_at,
      duration_ms, response_meta_json
    FROM reporting_request_logs
    WHERE owner_id = ?
    ORDER BY datetime(started_at) DESC, id DESC
    LIMIT ?
  `).all(ownerId, Math.max(1, Math.min(500, limit))) as any[];
}

router.get('/owners', (_req: Request, res: Response) => {
  const owners = listReportingOwners();
  res.json({
    data: owners.map((owner) => serializeReportingOwnerResponse(
      owner,
      listReportingBindings(owner.owner_id),
      getReportingOwnerUsage(owner.owner_id),
    )),
  });
});

router.post('/owners', (req: Request, res: Response) => {
  try {
    const owner = createReportingOwner(req.body || {});
    res.status(201).json(owner);
  } catch (error: any) {
    res.status(400).json({ error: String(error?.message || error || 'failed to create reporting owner') });
  }
});

router.patch('/owners/:ownerId', (req: Request, res: Response) => {
  try {
    const owner = updateReportingOwner(String(req.params.ownerId || '').trim(), req.body || {});
    res.json(owner);
  } catch (error: any) {
    const message = String(error?.message || error || 'failed to update reporting owner');
    res.status(message.includes('not found') ? 404 : 400).json({ error: message });
  }
});

router.delete('/owners/:ownerId', (req: Request, res: Response) => {
  try {
    deleteReportingOwner(String(req.params.ownerId || '').trim());
    res.json({ success: true });
  } catch (error: any) {
    const message = String(error?.message || error || 'failed to delete reporting owner');
    res.status(message.includes('not found') ? 404 : 400).json({ error: message });
  }
});

router.post('/owners/:ownerId/proxy-test', async (req: Request, res: Response) => {
  const ownerId = String(req.params.ownerId || '').trim();
  const owner = listReportingOwners().find((item) => item.owner_id === ownerId);
  if (!owner) {
    res.status(404).json({ error: 'reporting owner not found' });
    return;
  }
  const proxyUrl = Object.prototype.hasOwnProperty.call(req.body || {}, 'proxy_url')
    ? String(req.body?.proxy_url || '').trim()
    : String(owner.proxy_url || '').trim();
  const probe = await probeReportingProxy({
    owner_id: owner.owner_id,
    proxy_url: proxyUrl,
  });
  res.json(probe);
});

router.get('/owners/:ownerId/logs', (req: Request, res: Response) => {
  const ownerId = String(req.params.ownerId || '').trim();
  const limit = Number(req.query.limit || 100);
  res.json({ data: listReportingRequestLogs(ownerId, limit) });
});

router.get('/owners/:ownerId/usage', (req: Request, res: Response) => {
  const ownerId = String(req.params.ownerId || '').trim();
  res.json(getReportingOwnerUsage(ownerId));
});

router.post('/owners/:ownerId/bindings', (req: Request, res: Response) => {
  try {
    const binding = createReportingBinding(String(req.params.ownerId || '').trim(), req.body || {});
    res.status(201).json(binding);
  } catch (error: any) {
    const message = String(error?.message || error || 'failed to create reporting binding');
    res.status(message.includes('not found') ? 404 : 400).json({ error: message });
  }
});

router.patch('/bindings/:bindingId', (req: Request, res: Response) => {
  try {
    const binding = updateReportingBinding(String(req.params.bindingId || '').trim(), req.body || {});
    res.json(binding);
  } catch (error: any) {
    const message = String(error?.message || error || 'failed to update reporting binding');
    res.status(message.includes('not found') ? 404 : 400).json({ error: message });
  }
});

router.delete('/bindings/:bindingId', (req: Request, res: Response) => {
  try {
    deleteReportingBinding(String(req.params.bindingId || '').trim());
    res.json({ success: true });
  } catch (error: any) {
    const message = String(error?.message || error || 'failed to delete reporting binding');
    res.status(message.includes('not found') ? 404 : 400).json({ error: message });
  }
});

router.post('/bindings/:bindingId/sync', (req: Request, res: Response) => {
  try {
    const result = enqueueReportingSyncForBinding(String(req.params.bindingId || '').trim(), 'manual');
    getJobQueue().processNext();
    res.json(result);
  } catch (error: any) {
    res.status(400).json({ error: String(error?.message || error || 'failed to enqueue reporting sync') });
  }
});

export default router;
