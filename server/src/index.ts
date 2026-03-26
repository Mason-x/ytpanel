import express from 'express';
import cors from 'cors';
import path from 'path';
import { initDb, getDb, getDbPath, getSetting } from './db.js';
import { getJobQueue } from './services/jobQueue.js';
import { scheduleDailySyncFromSettings } from './services/dailySyncScheduler.js';
import { checkYtDlp } from './utils/helpers.js';

import healthRoutes from './routes/health.js';
import channelRoutes from './routes/channels.js';
import analyticsRoutes from './routes/analytics.js';
import videoRoutes from './routes/videos.js';
import jobRoutes from './routes/jobs.js';
import syncRoutes from './routes/sync.js';
import fileRoutes from './routes/files.js';
import settingsRoutes from './routes/settings.js';
import toolsRoutes from './routes/tools.js';
import researchRoutes from './routes/research.js';
import hitsRoutes from './routes/hits.js';
import dashboardRoutes from './routes/dashboard.js';
import agentRoutes from './routes/agent.js';

const PORT = parseInt(process.env.PORT || '3457', 10);
const HOST = process.env.HOST || '127.0.0.1';

// Initialize database
initDb();
try {
  const db = getDb();
  const recovered = db.prepare(`
    UPDATE jobs
    SET status = 'failed',
        finished_at = datetime('now'),
        error_message = COALESCE(error_message, 'Recovered after backend restart')
    WHERE status IN ('running', 'canceling')
  `).run().changes || 0;
  if (recovered > 0) {
    console.warn(`[JOBS] Recovered ${recovered} stale running jobs after restart`);
  }
} catch {}

// Check yt-dlp
const ytdlpStatus = checkYtDlp();
if (ytdlpStatus.available) {
  console.log(`[YT-DLP] Found version: ${ytdlpStatus.version}`);
} else {
  console.warn(`[YT-DLP] WARNING: ${ytdlpStatus.error}`);
}

// Create Express app
const app = express();

app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Serve downloaded assets (thumbnails, etc.)
const downloadRoot = getSetting('download_root') || path.join(process.cwd(), 'downloads');
app.use('/assets', express.static(path.join(downloadRoot, 'assets')));

// API routes
app.use('/api/health', healthRoutes);
app.use('/api/channels', channelRoutes);
app.use('/api/channels', analyticsRoutes);
app.use('/api/videos', videoRoutes);
app.use('/api/jobs', jobRoutes);
app.use('/api/sync', syncRoutes);
app.use('/api/files', fileRoutes);
app.use('/api/settings', settingsRoutes);
app.use('/api/tools', toolsRoutes);
app.use('/api/research', researchRoutes);
app.use('/api/hits', hitsRoutes);
app.use('/api/dashboard', dashboardRoutes);
app.use('/api/agent', agentRoutes);

const frontendDist = path.join(process.cwd(), 'dist');
app.use(express.static(frontendDist));
app.get(/^\/(?!api|assets).*/, (_req, res) => {
  res.sendFile(path.join(frontendDist, 'index.html'));
});

// Error handler
app.use((err: any, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error('[ERROR]', err);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

// Start server
app.listen(PORT, HOST, () => {
  console.log(`\nYTPanel Backend running at http://${HOST}:${PORT}`);
  console.log(`Database: ${getDbPath() || path.resolve('data/ytpanel.db')}`);
  console.log(`Downloads: ${downloadRoot}`);
  console.log(`YT-DLP: ${ytdlpStatus.available ? `v${ytdlpStatus.version}` : 'Not found'}\n`);
});

// Schedule daily sync
scheduleDailySyncFromSettings();

// Process any queued jobs on startup
setTimeout(() => {
  getJobQueue().processNext();
}, 1000);

export default app;

