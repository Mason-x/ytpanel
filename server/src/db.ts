import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { backfillChannelViewGrowthCaches } from './services/channelMetrics.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let db: Database.Database;
let dbResolvedPath = '';

const DEFAULT_DB_FILENAME = 'ytpanel.db';
const LEGACY_DB_FILENAME = 'ytmonitor.db';

function getWorkspaceRootCandidates(): string[] {
  const raw = [process.cwd(), path.resolve(process.cwd(), '..'), path.resolve(process.cwd(), '..', '..')];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of raw) {
    const resolved = path.resolve(item);
    const key = resolved.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(resolved);
  }
  return out;
}

function isInsidePath(rootPath: string, targetPath: string): boolean {
  const relative = path.relative(path.resolve(rootPath), path.resolve(targetPath));
  return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function findAppWorkspaceRoot(): string | null {
  for (const candidate of getWorkspaceRootCandidates()) {
    try {
      const hasClient = fs.existsSync(path.join(candidate, 'client'));
      const hasServer = fs.existsSync(path.join(candidate, 'server'));
      if (hasClient && hasServer) return candidate;
    } catch {
      // ignore fs errors and continue
    }
  }
  return null;
}

function relocateLegacyProjectPath(inputPath: string): string | null {
  const raw = String(inputPath || '').trim();
  if (!raw) return null;
  const resolved = path.resolve(raw);
  if (!path.isAbsolute(resolved)) return null;

  const workspaceRoot = findAppWorkspaceRoot();
  if (!workspaceRoot) return null;
  if (isInsidePath(workspaceRoot, resolved)) return resolved;

  const parts = resolved.split(/[\\/]+/).filter(Boolean);
  if (parts.length < 2) return null;

  const lowerParts = parts.map((part) => part.toLowerCase());
  const legacyIndex = lowerParts.lastIndexOf('ytmonitor');
  if (legacyIndex < 0) return null;

  const downloadsIndex = lowerParts.indexOf('downloads', legacyIndex + 1);
  if (downloadsIndex < 0) return null;

  const suffix = parts.slice(downloadsIndex);
  if (suffix.length === 0) return null;

  return path.join(workspaceRoot, ...suffix);
}

function migrateBundledProviderPathSetting(key: string, legacyRelativePath: string, bundledPath: string): void {
  const row = db.prepare('SELECT value FROM settings WHERE key = ?').get(key) as { value?: string } | undefined;
  const current = String(row?.value || '').trim();
  if (!current) return;

  const workspaceRoot = findAppWorkspaceRoot();
  const legacyFolderName = path.basename(legacyRelativePath).toLowerCase();
  const legacyCandidates = new Set<string>([
    path.resolve(path.join(__dirname, '..', '..', legacyRelativePath)).toLowerCase(),
  ]);
  if (workspaceRoot) {
    legacyCandidates.add(path.resolve(path.join(workspaceRoot, legacyRelativePath)).toLowerCase());
  }

  const resolvedCurrentPath = path.resolve(current);
  const resolvedCurrent = resolvedCurrentPath.toLowerCase();
  const isWorkspaceLocal = workspaceRoot ? isInsidePath(workspaceRoot, resolvedCurrentPath) : false;
  const looksLikeLegacyBundledFolder = path.basename(resolvedCurrentPath).toLowerCase() === legacyFolderName;
  if (!legacyCandidates.has(resolvedCurrent) && !(looksLikeLegacyBundledFolder && !isWorkspaceLocal)) return;

  const resolvedBundled = path.resolve(bundledPath);
  if (resolvedCurrent === resolvedBundled.toLowerCase()) return;

  db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run(key, resolvedBundled);
  console.log(`[DB] Migrated bundled provider path for ${key}: ${current} -> ${resolvedBundled}`);
}

function migrateLegacyDownloadRootSetting(): void {
  const row = db.prepare('SELECT value FROM settings WHERE key = ?').get('download_root') as { value?: string } | undefined;
  const current = String(row?.value || '').trim();
  if (!current) return;

  const relocated = relocateLegacyProjectPath(current);
  if (!relocated) return;

  const currentResolved = path.resolve(current);
  const relocatedResolved = path.resolve(relocated);
  if (currentResolved.toLowerCase() === relocatedResolved.toLowerCase()) return;

  db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run('download_root', relocatedResolved);
  console.log(`[DB] Migrated legacy download_root: ${currentResolved} -> ${relocatedResolved}`);
}

function hasColumn(table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((row) => row.name === column);
}

function ensureColumn(table: string, column: string, definition: string): void {
  if (hasColumn(table, column)) return;
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
}

export function getDb(): Database.Database {
  if (!db) {
    throw new Error('Database not initialized. Call initDb() first.');
  }
  return db;
}

export function getDbPath(): string {
  return dbResolvedPath;
}

function tryRenameIfExists(fromPath: string, toPath: string): void {
  if (!fs.existsSync(fromPath)) return;
  fs.renameSync(fromPath, toPath);
}

function tryCopyIfExists(fromPath: string, toPath: string): void {
  if (!fs.existsSync(fromPath)) return;
  fs.copyFileSync(fromPath, toPath);
}

function resolveDefaultDbPath(): string {
  return path.join(__dirname, '..', 'data', DEFAULT_DB_FILENAME);
}

function migrateLegacyDbFilenameIfNeeded(targetPath: string): string {
  const resolvedTarget = path.resolve(targetPath);
  const targetDir = path.dirname(resolvedTarget);
  if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
  }
  if (fs.existsSync(resolvedTarget)) return resolvedTarget;

  const legacyPath = path.join(path.dirname(resolvedTarget), LEGACY_DB_FILENAME);
  const resolvedLegacy = path.resolve(legacyPath);
  if (!fs.existsSync(resolvedLegacy)) return resolvedTarget;

  const suffixes = ['', '-wal', '-shm'];
  try {
    for (const suffix of suffixes) {
      tryRenameIfExists(`${resolvedLegacy}${suffix}`, `${resolvedTarget}${suffix}`);
    }
    console.log(`[DB] Migrated legacy database filename: ${resolvedLegacy} -> ${resolvedTarget}`);
    return resolvedTarget;
  } catch (err: any) {
    try {
      for (const suffix of suffixes) {
        tryCopyIfExists(`${resolvedLegacy}${suffix}`, `${resolvedTarget}${suffix}`);
      }
      console.log(`[DB] Copied legacy database filename: ${resolvedLegacy} -> ${resolvedTarget}`);
      return resolvedTarget;
    } catch {
      console.warn(`[DB] Legacy database filename migration failed, using existing file: ${resolvedLegacy} (${String(err?.message || 'rename_failed')})`);
      return resolvedLegacy;
    }
  }
}

export function initDb(dbPath?: string): Database.Database {
  const resolvedPath = dbPath
    ? path.resolve(dbPath)
    : migrateLegacyDbFilenameIfNeeded(resolveDefaultDbPath());
  const dir = path.dirname(resolvedPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  db = new Database(resolvedPath);
  dbResolvedPath = resolvedPath;
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  // Run schema migration
  const schemaPath = path.join(__dirname, 'schema.sql');
  const schema = fs.readFileSync(schemaPath, 'utf-8');
  const baseSchema = schema
    .replace(/^CREATE INDEX IF NOT EXISTS idx_channels_platform ON channels\(platform\);\s*$/gm, '')
    .replace(/^CREATE INDEX IF NOT EXISTS idx_videos_platform ON videos\(platform\);\s*$/gm, '');
  db.exec(baseSchema);
  ensureColumn('channels', 'monitor_status', "TEXT NOT NULL DEFAULT 'ok'");
  ensureColumn('channels', 'monitor_reason', 'TEXT');
  ensureColumn('channels', 'monitor_checked_at', 'TEXT');
  ensureColumn('channels', 'api_last_sync_at', 'TEXT');
  ensureColumn('channels', 'new_video_badge_count', "INTEGER NOT NULL DEFAULT 0");
  ensureColumn('channels', 'new_video_badge_at', 'TEXT');
  ensureColumn('channels', 'channel_view_increase_7d', 'INTEGER');
  ensureColumn('channels', 'channel_view_growth_series_7d_json', "TEXT NOT NULL DEFAULT '[]'");
  ensureColumn('channels', 'channel_growth_computed_at', 'TEXT');
  ensureColumn('channels', 'workflow_status', "TEXT NOT NULL DEFAULT 'in_progress'");
  ensureColumn('channels', 'positioning', 'TEXT');
  ensureColumn('channels', 'notes', 'TEXT');
  ensureColumn('channels', 'manual_updated_at', 'TEXT');
  ensureColumn('channels', 'platform', "TEXT NOT NULL DEFAULT 'youtube'");
  ensureColumn('channels', 'source_url', 'TEXT');
  ensureColumn('dashboard_tasks', 'task_name', 'TEXT');
  ensureColumn('dashboard_tasks', 'planned_start_time', 'TEXT');
  ensureColumn('dashboard_tasks', 'planned_end_time', 'TEXT');
  db.exec("UPDATE channels SET new_video_badge_count = 0 WHERE new_video_badge_count IS NULL OR new_video_badge_count < 0");
  db.exec("UPDATE channels SET channel_view_growth_series_7d_json = '[]' WHERE channel_view_growth_series_7d_json IS NULL OR trim(channel_view_growth_series_7d_json) = ''");
  db.exec("UPDATE channels SET platform = 'youtube' WHERE platform IS NULL OR trim(platform) = ''");
  db.exec("UPDATE channels SET workflow_status = 'in_progress' WHERE workflow_status IS NULL OR trim(workflow_status) = ''");
  db.exec('CREATE INDEX IF NOT EXISTS idx_channels_platform ON channels(platform)');
  ensureColumn('videos', 'platform', "TEXT NOT NULL DEFAULT 'youtube'");
  ensureColumn('videos', 'content_type', 'TEXT');
  ensureColumn('videos', 'content_type_source', 'TEXT');
  ensureColumn('videos', 'comment_count', 'INTEGER');
  ensureColumn('videos', 'collect_count', 'INTEGER');
  ensureColumn('videos', 'share_count', 'INTEGER');
  ensureColumn('video_daily', 'collect_count', 'INTEGER');
  ensureColumn('video_daily', 'share_count', 'INTEGER');
  db.exec(`
    UPDATE videos
    SET platform = COALESCE((
      SELECT c.platform FROM channels c WHERE c.channel_id = videos.channel_id
    ), 'youtube')
    WHERE platform IS NULL OR trim(platform) = ''
  `);
  db.exec(`
    UPDATE videos
    SET content_type = 'short',
        content_type_source = 'platform_default_short'
    WHERE lower(COALESCE(platform, 'youtube')) IN ('tiktok', 'xiaohongshu')
      AND COALESCE(content_type, '') <> 'short'
  `);
  db.exec(`
    UPDATE videos
    SET content_type = 'note',
        content_type_source = 'douyin_note_url'
    WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
      AND (
        lower(COALESCE(webpage_url, '')) LIKE '%/note/%'
        OR lower(COALESCE(webpage_url, '')) LIKE '%/slides/%'
      )
  `);
  db.exec(`
    UPDATE videos
    SET content_type = 'album',
        content_type_source = CASE
          WHEN lower(COALESCE(content_type_source, '')) LIKE 'douyin_live_photo%' THEN 'douyin_album_live_photo_meta'
          ELSE COALESCE(content_type_source, 'douyin_album_meta')
        END
    WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
      AND lower(COALESCE(content_type, '')) IN ('note', 'live_photo')
  `);
  db.exec(`
    UPDATE videos
    SET content_type = 'short',
        content_type_source = CASE
          WHEN lower(COALESCE(content_type_source, '')) = 'douyin_video_meta' THEN 'douyin_video_portrait'
          ELSE COALESCE(content_type_source, 'douyin_video_portrait')
        END
    WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
      AND lower(COALESCE(content_type, '')) = 'long'
      AND (
        lower(COALESCE(content_type_source, '')) = 'platform_default_short'
        OR lower(COALESCE(content_type_source, '')) = 'douyin_video_portrait'
        OR lower(COALESCE(content_type_source, '')) = 'douyin_video_portrait_meta'
      )
  `);
  db.exec(`
    UPDATE videos
    SET content_type = 'short',
        content_type_source = CASE
          WHEN lower(COALESCE(content_type_source, '')) LIKE 'douyin_video%' THEN content_type_source
          ELSE COALESCE(content_type_source, 'douyin_video_duration_short')
        END
    WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
      AND COALESCE(duration_sec, 0) > 0
      AND COALESCE(duration_sec, 0) < 60
  `);
  db.exec(`
    UPDATE videos
    SET content_type = 'short',
        content_type_source = CASE
          WHEN lower(COALESCE(content_type_source, '')) IN ('', 'default_long', 'videos_feed', 'main_feed', 'duration')
            THEN 'youtube_url_shorts'
          ELSE COALESCE(content_type_source, 'youtube_url_shorts')
        END
    WHERE lower(COALESCE(platform, 'youtube')) = 'youtube'
      AND lower(COALESCE(webpage_url, '')) LIKE '%/shorts/%'
      AND lower(COALESCE(content_type, '')) <> 'short'
  `);
  db.exec(`
    UPDATE video_daily
    SET view_count = NULL,
        like_count = NULL,
        comment_count = NULL,
        collect_count = NULL,
        share_count = NULL
    WHERE video_id IN (
      SELECT video_id
      FROM videos
      WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
    )
      AND COALESCE(view_count, 0) = 0
      AND COALESCE(like_count, 0) = 0
      AND COALESCE(comment_count, 0) = 0
      AND COALESCE(collect_count, 0) = 0
      AND COALESCE(share_count, 0) = 0
  `);
  db.exec(`
    UPDATE video_daily
    SET view_count = NULL
    WHERE video_id IN (
      SELECT video_id
      FROM videos
      WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
    )
      AND COALESCE(view_count, 0) = 0
      AND (
        COALESCE(like_count, 0) > 0
        OR COALESCE(comment_count, 0) > 0
        OR COALESCE(collect_count, 0) > 0
        OR COALESCE(share_count, 0) > 0
      )
  `);
  db.exec(`
    UPDATE videos
    SET view_count = NULL
    WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
      AND COALESCE(view_count, 0) = 0
      AND (
        COALESCE(like_count, 0) > 0
        OR COALESCE(comment_count, 0) > 0
        OR COALESCE(collect_count, 0) > 0
        OR COALESCE(share_count, 0) > 0
      )
  `);
  db.exec(`
    UPDATE channels
    SET view_count = NULL
    WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
      AND COALESCE(view_count, 0) = 0
      AND EXISTS (
        SELECT 1
        FROM videos v
        WHERE v.channel_id = channels.channel_id
          AND lower(COALESCE(v.platform, 'youtube')) = 'douyin'
          AND v.view_count IS NULL
      )
  `);
  db.exec(`
    UPDATE channel_daily
    SET view_count_total = NULL
    WHERE channel_id IN (
      SELECT channel_id
      FROM channels
      WHERE lower(COALESCE(platform, 'youtube')) = 'douyin'
    )
      AND COALESCE(view_count_total, 0) = 0
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_videos_platform ON videos(platform)');
  ensureColumn('hit_videos', 'platform', "TEXT NOT NULL DEFAULT 'Other'");
  db.exec('CREATE INDEX IF NOT EXISTS idx_hit_videos_platform ON hit_videos(platform)');
  db.exec(`
    CREATE TABLE IF NOT EXISTS ytdlp_exec_audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL DEFAULT (datetime('now')),
      target_url TEXT,
      cookie_platform TEXT,
      cookie_pool_enabled INTEGER NOT NULL DEFAULT 0,
      strict_mode INTEGER NOT NULL DEFAULT 0,
      used_pool_binding INTEGER NOT NULL DEFAULT 0,
      selected_cookie_id TEXT,
      selected_cookie_name TEXT,
      selected_proxy TEXT,
      status TEXT NOT NULL,
      error_code TEXT,
      message TEXT
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ytdlp_exec_audit_ts ON ytdlp_exec_audit(ts DESC)');

  // Ensure existing research channels have at least one baseline snapshot for growth views.
  db.exec(`
    INSERT OR IGNORE INTO research_channel_daily (date, channel_id, subscriber_count, view_count)
    SELECT date('now'), rc.channel_id, rc.subscriber_count, rc.view_count
    FROM research_channels rc
    WHERE NOT EXISTS (
      SELECT 1
      FROM research_channel_daily rd
      WHERE rd.channel_id = rc.channel_id
    )
  `);
  db.exec(`
    INSERT OR IGNORE INTO hit_video_daily (date, video_id, view_count, like_count, comment_count)
    SELECT date('now'), hv.video_id, hv.view_count, hv.like_count, hv.comment_count
    FROM hit_videos hv
    WHERE NOT EXISTS (
      SELECT 1
      FROM hit_video_daily hd
      WHERE hd.video_id = hv.video_id
    )
  `);

  // Seed immutable "ever invalid/unavailable" markers from existing archive rows.
  db.exec(`
    INSERT INTO channel_invalid_events (channel_id, title, handle, detected_at, reason)
    SELECT
      a.channel_id,
      a.title,
      a.handle,
      COALESCE(a.first_invalid_at, datetime('now')),
      a.first_reason
    FROM channel_invalid_archive a
    WHERE NOT EXISTS (
      SELECT 1
      FROM channel_invalid_events e
      WHERE e.channel_id = a.channel_id
    )
  `);

  db.exec(`
    INSERT INTO video_unavailable_events (video_id, channel_id, title, detected_at, reason, raw_message)
    SELECT
      va.video_id,
      va.channel_id,
      va.title,
      COALESCE(va.first_unavailable_at, datetime('now')),
      va.first_reason,
      NULL
    FROM video_unavailable_archive va
    WHERE NOT EXISTS (
      SELECT 1
      FROM video_unavailable_events ve
      WHERE ve.video_id = va.video_id
    )
  `);

  // Backfill archive snapshots only for ids that have never been recorded before.
  db.exec(`
    INSERT OR IGNORE INTO channel_invalid_archive (
      channel_id, title, handle, first_invalid_at, last_invalid_at, first_reason, last_reason, status, resolved_at
    )
    SELECT
      channel_id,
      title,
      handle,
      COALESCE(monitor_checked_at, last_sync_at, created_at, datetime('now')),
      COALESCE(monitor_checked_at, last_sync_at, created_at, datetime('now')),
      monitor_reason,
      monitor_reason,
      'active',
      NULL
    FROM channels
    WHERE lower(COALESCE(monitor_status, '')) IN ('invalid', 'not_found', 'unavailable')
      AND NOT EXISTS (
        SELECT 1
        FROM channel_invalid_events e
        WHERE e.channel_id = channels.channel_id
      )
  `);

  db.exec(`
    INSERT OR IGNORE INTO video_unavailable_archive (
      video_id, channel_id, title, webpage_url, first_unavailable_at, last_unavailable_at, first_reason, last_reason, status, resolved_at
    )
    SELECT
      video_id,
      channel_id,
      title,
      webpage_url,
      COALESCE(unavailable_at, created_at, datetime('now')),
      COALESCE(unavailable_at, created_at, datetime('now')),
      unavailable_reason,
      unavailable_reason,
      'active',
      NULL
    FROM videos
    WHERE lower(COALESCE(availability_status, '')) = 'unavailable'
      AND NOT EXISTS (
        SELECT 1
        FROM video_unavailable_events ve
        WHERE ve.video_id = videos.video_id
      )
  `);
  db.exec(`
    UPDATE videos
    SET webpage_url = (
      SELECT
        'https://www.tiktok.com/' ||
        CASE
          WHEN substr(trim(COALESCE(c.handle, '')), 1, 1) = '@' THEN trim(COALESCE(c.handle, ''))
          ELSE '@' || trim(COALESCE(c.handle, ''))
        END ||
        '/video/' || replace(COALESCE(videos.video_id, ''), 'tiktok__', '')
      FROM channels c
      WHERE c.channel_id = videos.channel_id
    )
    WHERE lower(COALESCE(videos.platform, '')) = 'tiktok'
      AND lower(COALESCE(videos.webpage_url, '')) LIKE 'https://www.tiktok.com/video/%'
      AND EXISTS (
        SELECT 1
        FROM channels c
        WHERE c.channel_id = videos.channel_id
          AND trim(COALESCE(c.handle, '')) <> ''
      )
  `);

  // Historical cleanup: "missing_from_channel_videos" means the video URL is still reachable,
  // so it should not stay in unavailable state.
  db.exec(`
    UPDATE videos
    SET availability_status = 'available',
        unavailable_reason = NULL,
        unavailable_at = NULL
    WHERE lower(COALESCE(unavailable_reason, '')) = 'missing_from_channel_videos'
  `);

  // Seed default settings
  const defaults: Record<string, string> = {
    download_root: path.join(__dirname, '..', '..', 'downloads'),
    format_selector: 'bestvideo+bestaudio/best',
    format_selector_fallback: 'best',
    container: 'mp4',
    subtitle_langs: 'en,zh,zh-Hans,zh-Hant',
    yt_dlp_use_browser_cookies: 'true',
    yt_dlp_cookies_browser: 'auto',
    yt_dlp_cookies_profile: '',
    yt_dlp_cookie_file_youtube: '',
    youtube_cookie_pool_enabled: 'false',
    youtube_cookie_pool_strict_mode: 'false',
    youtube_cookie_pool_json: '[]',
    yt_dlp_cookie_file_bilibili: '',
    yt_dlp_cookie_file_tiktok: '',
    yt_dlp_cookie_file_douyin: '',
    yt_dlp_cookie_file_xiaohongshu: '',
    yt_dlp_cookie_file: '',
    yt_dlp_use_browser_proxy: 'true',
    yt_dlp_proxy: '',
    yt_dlp_disable_plugins: 'true',
    yt_dlp_youtube_player_clients: 'default',
    yt_dlp_youtube_fallback_player_clients: 'default',
    yt_dlp_youtube_player_skip: '',
    yt_dlp_youtube_visitor_data: '',
    yt_dlp_youtube_po_token: '',
    youtube_meta_backfill_on_sync_limit: '12',
    max_concurrency: '2',
    sync_job_concurrency: '2',
    download_job_concurrency: '2',
    retry_count: '2',
    enable_resume: 'true',
    daily_sync_time: '03:00',
    recent_video_fetch_limit: '50',
    refresh_window_days: '30',
    auto_download_on_new_video: 'meta+thumb',
    yt_dlp_js_runtimes: 'node,deno',
    tiktok_downloader_enabled: 'true',
    tiktok_downloader_root: path.join(__dirname, '..', 'vendor', 'tiktokdownloader'),
    tiktok_downloader_python: 'python',
    tiktok_downloader_conda_env: '',
    tiktok_downloader_timeout_sec: '120',
    tiktok_downloader_bridge_script: path.join(__dirname, '..', 'scripts', 'tiktokdownloader_bridge.py'),
    xhs_spider_enabled: 'true',
    xhs_spider_root: path.join(__dirname, '..', 'vendor', 'spider_xhs'),
    xhs_spider_python: 'python',
    xhs_spider_conda_env: '',
    xhs_spider_timeout_sec: '120',
    xhs_spider_bridge_script: path.join(__dirname, '..', 'scripts', 'spider_xhs_bridge.py'),
    playwright_headless: 'true',
    playwright_session_enabled: 'true',
    playwright_session_login_timeout_sec: '300',
    douyin_playwright_view_sync_enabled: 'true',
    douyin_playwright_view_sync_limit: '40',
    douyin_playwright_timeout_ms: '22000',
    douyin_playwright_delay_ms: '800',
    douyin_playwright_headless: 'true',
    high_priority_list: '[]',
    channel_api_enabled: 'true',
    youtube_api_key: '',
    youtube_api_keys: '',
    youtube_api_key_proxies: '',
    youtube_api_auto_rotate_key: 'true',
    youtube_api_warning_threshold_percent: '80',
    youtube_api_daily_units_limit: '10000',
    youtube_api_channel_refresh_hours: '24',
    youtube_api_usage_json: '{}',
  };

  const insertSetting = db.prepare(
    'INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)'
  );
  for (const [key, value] of Object.entries(defaults)) {
    insertSetting.run(key, value);
  }
  migrateBundledProviderPathSetting(
    'tiktok_downloader_root',
    'TikTokDownloader',
    path.join(__dirname, '..', 'vendor', 'tiktokdownloader'),
  );
  migrateBundledProviderPathSetting(
    'xhs_spider_root',
    'Spider_XHS',
    path.join(__dirname, '..', 'vendor', 'spider_xhs'),
  );

  // Self-heal: if strict mode is enabled and pool has usable items but the pool switch
  // is off (often caused by broad settings overwrite), force-enable pool dispatch.
  try {
    const strictMode = String(
      (db.prepare('SELECT value FROM settings WHERE key = ?').get('youtube_cookie_pool_strict_mode') as any)?.value || 'false',
    ).trim().toLowerCase() === 'true';
    const poolEnabled = String(
      (db.prepare('SELECT value FROM settings WHERE key = ?').get('youtube_cookie_pool_enabled') as any)?.value || 'false',
    ).trim().toLowerCase() === 'true';
    const poolJsonRaw = String(
      (db.prepare('SELECT value FROM settings WHERE key = ?').get('youtube_cookie_pool_json') as any)?.value || '[]',
    ).trim();
    let hasUsablePoolItems = false;
    try {
      const parsed = JSON.parse(poolJsonRaw);
      if (Array.isArray(parsed)) {
        hasUsablePoolItems = parsed.some((row: any) => {
          if (!row || typeof row !== 'object') return false;
          const enabled = String(row.enabled ?? 'true').trim().toLowerCase();
          const cookieHeader = String(row.cookie_header || '').trim();
          return enabled !== 'false' && cookieHeader.length > 0;
        });
      }
    } catch {
      hasUsablePoolItems = false;
    }
    if (strictMode && !poolEnabled && hasUsablePoolItems) {
      db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run('youtube_cookie_pool_enabled', 'true');
      console.log('[DB] Repaired youtube_cookie_pool_enabled=false -> true (strict mode + usable pool items)');
    }
  } catch {
    // keep init robust
  }

  // One-time migration for project rename: legacy absolute download_root paths often
  // still point to sibling "...\\ytmonitor\\downloads" after the workspace was renamed.
  migrateLegacyDownloadRootSetting();

  // Migrate legacy YouTube client defaults to use 'default' (lets yt-dlp use its built-in logic).
  db.prepare(`
    UPDATE settings
    SET value = 'default'
    WHERE key = 'yt_dlp_youtube_player_clients'
      AND lower(trim(COALESCE(value, ''))) IN ('android,android_vr', 'android_vr,android', 'android_vr,web', 'web,android_vr')
  `).run();
  db.prepare(`
    UPDATE settings
    SET value = 'default'
    WHERE key = 'yt_dlp_youtube_fallback_player_clients'
      AND lower(trim(COALESCE(value, ''))) IN ('android', 'android_vr')
  `).run();

  backfillChannelViewGrowthCaches(db, {
    logger: (message) => console.log(message),
  });

  console.log(`[DB] Initialized at ${resolvedPath}`);
  return db;
}

export function getSetting(key: string): string | undefined {
  const row = getDb().prepare('SELECT value FROM settings WHERE key = ?').get(key) as { value: string } | undefined;
  return row?.value;
}

export function setSetting(key: string, value: string): void {
  getDb().prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run(key, value);
}

export function getAllSettings(): Record<string, string> {
  const rows = getDb().prepare('SELECT key, value FROM settings').all() as { key: string; value: string }[];
  const result: Record<string, string> = {};
  for (const row of rows) {
    result[row.key] = row.value;
  }
  return result;
}
