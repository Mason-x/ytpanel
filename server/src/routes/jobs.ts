import { Router, Request, Response } from 'express';
import { getDb } from '../db.js';
import { v4 as uuidv4 } from 'uuid';

const router = Router();

function firstParam(value: string | string[] | undefined): string {
  if (Array.isArray(value)) return value[0] || '';
  return value || '';
}

// GET /api/jobs
router.get('/', (req: Request, res: Response) => {
  const db = getDb();
  const { status, type, page = '1', limit = '50' } = req.query;
  const pageNum = Math.max(1, parseInt(page as string, 10) || 1);
  const limitNum = Math.min(500, parseInt(limit as string, 10) || 50);
  const offset = (pageNum - 1) * limitNum;

  let where = 'WHERE 1=1';
  const params: any[] = [];

  if (status) {
    where += ' AND status = ?';
    params.push(status);
  }
  if (type) {
    where += ' AND type = ?';
    params.push(type);
  }

  const total = (db.prepare(`SELECT COUNT(*) as count FROM jobs ${where}`).get(...params) as any).count;
  const rows = db.prepare(
    `SELECT * FROM jobs ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`
  ).all(...params, limitNum, offset);

  res.json({ data: rows, total, page: pageNum, limit: limitNum });
});

// POST /api/jobs/cancel-all — cancel all queued + running jobs
router.post('/cancel-all', async (req: Request, res: Response) => {
  const db = getDb();

  // Cancel all queued jobs
  const cancelQueued = db.prepare(`
    UPDATE jobs SET status = 'canceled', finished_at = datetime('now')
    WHERE status = 'queued'
  `).run();

  // Cancel all running jobs
  const runningJobs = db.prepare(`SELECT job_id FROM jobs WHERE status = 'running'`).all() as any[];
  const cancelingRunningIds: string[] = [];
  const detachedRunningIds: string[] = [];
  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    const queue = getJobQueue();
    for (const j of runningJobs) {
      if (queue.cancelJob(j.job_id)) {
        cancelingRunningIds.push(j.job_id);
      } else {
        detachedRunningIds.push(j.job_id);
      }
    }
  } catch {}

  if (cancelingRunningIds.length > 0) {
    const placeholders = cancelingRunningIds.map(() => '?').join(', ');
    db.prepare(`
      UPDATE jobs
      SET status = 'canceling'
      WHERE status = 'running' AND job_id IN (${placeholders})
    `).run(...cancelingRunningIds);
  }

  if (detachedRunningIds.length > 0) {
    const placeholders = detachedRunningIds.map(() => '?').join(', ');
    db.prepare(`
      UPDATE jobs
      SET status = 'canceled',
          finished_at = datetime('now'),
          error_message = COALESCE(error_message, 'Force canceled: job handle not found')
      WHERE status = 'running' AND job_id IN (${placeholders})
    `).run(...detachedRunningIds);
  }

  res.json({
    canceled_queued: cancelQueued.changes,
    canceling_running: cancelingRunningIds.length,
    canceled_detached_running: detachedRunningIds.length,
  });
});

// POST /api/jobs/clear-logs — clear terminal job logs + persisted local log records
router.post('/clear-logs', (req: Request, res: Response) => {
  const db = getDb();

  const activeCount = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM jobs
    WHERE status IN ('queued', 'running', 'canceling')
  `).get() as any)?.c || 0);

  const terminalJobCount = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM jobs
    WHERE status NOT IN ('queued', 'running', 'canceling')
  `).get() as any)?.c || 0);
  const terminalEventCount = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM job_events
    WHERE job_id IN (
      SELECT job_id FROM jobs WHERE status NOT IN ('queued', 'running', 'canceling')
    )
  `).get() as any)?.c || 0);
  const terminalResultCount = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM tool_job_results
    WHERE job_id IN (
      SELECT job_id FROM jobs WHERE status NOT IN ('queued', 'running', 'canceling')
    )
  `).get() as any)?.c || 0);
  const orphanEventCount = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM job_events
    WHERE job_id NOT IN (SELECT job_id FROM jobs)
  `).get() as any)?.c || 0);
  const orphanResultCount = Number((db.prepare(`
    SELECT COUNT(*) AS c
    FROM tool_job_results
    WHERE job_id NOT IN (SELECT job_id FROM jobs)
  `).get() as any)?.c || 0);

  const tx = db.transaction(() => {
    db.prepare(`
      DELETE FROM job_events
      WHERE job_id IN (
        SELECT job_id FROM jobs WHERE status NOT IN ('queued', 'running', 'canceling')
      )
    `).run();
    db.prepare(`
      DELETE FROM tool_job_results
      WHERE job_id IN (
        SELECT job_id FROM jobs WHERE status NOT IN ('queued', 'running', 'canceling')
      )
    `).run();
    db.prepare(`DELETE FROM jobs WHERE status NOT IN ('queued', 'running', 'canceling')`).run();

    // Cleanup possible orphan rows from previous partial data changes
    db.prepare(`DELETE FROM job_events WHERE job_id NOT IN (SELECT job_id FROM jobs)`).run();
    db.prepare(`DELETE FROM tool_job_results WHERE job_id NOT IN (SELECT job_id FROM jobs)`).run();
  });
  tx();

  res.json({
    success: true,
    preserved_active_jobs: activeCount,
    deleted_jobs: terminalJobCount,
    deleted_job_events: terminalEventCount + orphanEventCount,
    deleted_tool_results: terminalResultCount + orphanResultCount,
  });
});

// GET /api/jobs/:id
router.get('/:id', (req: Request, res: Response) => {
  const db = getDb();
  const job = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(req.params.id);
  if (!job) {
    res.status(404).json({ error: 'Job not found' });
    return;
  }
  res.json(job);
});

// POST /api/jobs
router.post('/', async (req: Request, res: Response) => {
  const db = getDb();
  const { type, payload } = req.body;

  if (!type) {
    res.status(400).json({ error: 'type is required' });
    return;
  }

  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, ?, ?, 'queued')
  `).run(jobId, type, JSON.stringify(payload || {}));

  // Trigger queue processing
  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  const job = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(jobId);
  res.status(201).json(job);
});

// POST /api/jobs/:id/cancel
router.post('/:id/cancel', async (req: Request, res: Response) => {
  const db = getDb();
  const job = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(req.params.id) as any;
  if (!job) {
    res.status(404).json({ error: 'Job not found' });
    return;
  }

  if (job.status === 'queued') {
    db.prepare(`UPDATE jobs SET status = 'canceled', finished_at = datetime('now') WHERE job_id = ?`)
      .run(req.params.id);
    res.json({ status: 'canceled' });
    return;
  }

  if (job.status === 'running') {
    const jobId = firstParam(req.params.id);
    let signaled = false;
    try {
      const { getJobQueue } = await import('../services/jobQueue.js');
      signaled = getJobQueue().cancelJob(jobId);
    } catch {}

    if (signaled) {
      db.prepare(`
        UPDATE jobs
        SET status = 'canceling'
        WHERE job_id = ? AND status = 'running'
      `).run(jobId);
      res.json({ status: 'canceling' });
      return;
    }

    // Fallback: handle lost (for example after backend restart). Force terminal state.
    db.prepare(`
      UPDATE jobs
      SET status = 'canceled',
          finished_at = datetime('now'),
          error_message = COALESCE(error_message, 'Force canceled: job handle not found')
      WHERE job_id = ? AND status = 'running'
    `).run(jobId);

    const latest = db.prepare('SELECT status FROM jobs WHERE job_id = ?').get(jobId) as any;
    res.json({ status: latest?.status || 'canceled' });
    return;
  }

  if (job.status === 'canceling') {
    res.json({ status: 'canceling' });
    return;
  }

  res.status(400).json({ error: `Cannot cancel job with status ${job.status}` });
});

// POST /api/jobs/:id/retry
router.post('/:id/retry', async (req: Request, res: Response) => {
  const db = getDb();
  const job = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(req.params.id) as any;
  if (!job) {
    res.status(404).json({ error: 'Job not found' });
    return;
  }

  if (job.status !== 'failed') {
    res.status(400).json({ error: 'Only failed jobs can be retried' });
    return;
  }

  const newJobId = uuidv4();
  const retryTx = db.transaction(() => {
    db.prepare(`
      INSERT INTO jobs (job_id, type, payload_json, status, parent_job_id)
      VALUES (?, ?, ?, 'queued', ?)
    `).run(newJobId, job.type, job.payload_json, job.job_id);

    // Retry created successfully: clear failure logs from the old failed job.
    db.prepare(`
      UPDATE jobs
      SET error_message = NULL
      WHERE job_id = ?
    `).run(job.job_id);

    db.prepare(`
      DELETE FROM job_events
      WHERE job_id = ?
    `).run(job.job_id);
  });
  retryTx();

  try {
    const { getJobQueue } = await import('../services/jobQueue.js');
    getJobQueue().processNext();
  } catch {}

  const newJob = db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(newJobId);
  res.status(201).json(newJob);
});

// GET /api/jobs/:id/events
router.get('/:id/events', (req: Request, res: Response) => {
  const db = getDb();
  const { limit = '100' } = req.query;
  const limitNum = Math.min(500, parseInt(limit as string, 10) || 100);

  const events = db.prepare(
    'SELECT * FROM job_events WHERE job_id = ? ORDER BY ts DESC LIMIT ?'
  ).all(req.params.id, limitNum);

  res.json({ data: events });
});

export default router;
