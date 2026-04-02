import cron, { type ScheduledTask } from 'node-cron';
import { v4 as uuidv4 } from 'uuid';
import { getDb, getSetting } from '../db.js';
import { getJobQueue } from './jobQueue.js';
import { enqueueDailyReportingSyncs } from './youtubeReportingSync.js';

let dailySyncTask: ScheduledTask | null = null;

function normalizeDailySyncTime(input: string | null | undefined): string {
  const text = String(input || '').trim();
  if (/^\d{2}:\d{2}$/.test(text)) {
    const [hourText, minuteText] = text.split(':');
    const hour = Number.parseInt(hourText, 10);
    const minute = Number.parseInt(minuteText, 10);
    if (Number.isInteger(hour) && Number.isInteger(minute) && hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
    }
  }
  return '03:00';
}

export function enqueueDailySyncJob(trigger: 'manual' | 'cron' = 'manual') {
  const db = getDb();
  const jobId = uuidv4();
  db.prepare(`
    INSERT INTO jobs (job_id, type, payload_json, status)
    VALUES (?, 'daily_sync', ?, 'queued')
  `).run(jobId, JSON.stringify({ trigger }));
  try {
    enqueueDailyReportingSyncs(trigger);
  } catch (error: any) {
    console.error(`[REPORTING] Failed to enqueue reporting syncs: ${String(error?.message || error || 'unknown')}`);
  }
  getJobQueue().processNext();
  return { job_id: jobId, status: 'queued' as const };
}

export function scheduleDailySyncFromSettings() {
  const syncTime = normalizeDailySyncTime(getSetting('daily_sync_time'));
  const [hour, minute] = syncTime.split(':');

  if (dailySyncTask) {
    dailySyncTask.stop();
    dailySyncTask = null;
  }

  dailySyncTask = cron.schedule(`${minute} ${hour} * * *`, () => {
    try {
      console.log(`[CRON] Starting daily sync at ${syncTime}...`);
      enqueueDailySyncJob('cron');
    } catch (err: any) {
      console.error(`[CRON] Daily sync enqueue failed: ${String(err?.message || err)}`);
    }
  });

  return syncTime;
}
