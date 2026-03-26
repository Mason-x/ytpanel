import { Router, Request, Response } from 'express';
import { getDb, getAllSettings } from '../db.js';
import { checkYtDlp } from '../utils/helpers.js';

const router = Router();

router.get('/', (_req: Request, res: Response) => {
  const ytdlp = checkYtDlp();
  const settings = getAllSettings();
  res.json({
    status: 'ok',
    version: '1.0.0',
    db_path: getDb().name,
    download_root: settings.download_root || '',
    ytdlp_available: ytdlp.available,
    ytdlp_version: ytdlp.version || null,
    ytdlp_error: ytdlp.error || null,
  });
});

export default router;
