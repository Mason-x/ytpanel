import {
  cookieHeaderToContextCookies,
  launchPlaywrightContextWithSession,
  writePlaywrightSessionMeta,
  type PlaywrightSessionMode,
} from './playwrightSession.js';

type MaybeInt = number | null;

export interface DouyinPlaywrightVideoStats {
  videoId: string;
  webpageUrl: string;
  viewCount: MaybeInt;
  likeCount: MaybeInt;
  commentCount: MaybeInt;
  collectCount: MaybeInt;
  shareCount: MaybeInt;
  source: string;
  error?: string;
}

export interface DouyinPlaywrightChannelScanOptions {
  cookieHeader?: string;
  headless?: boolean;
  timeoutMs?: number;
  delayMs?: number;
  maxItems?: number;
  maxNoNewRounds?: number;
  maxScrollRounds?: number;
  minRoundsBeforeStop?: number;
  usePersistentSession?: boolean;
  abortSignal?: AbortSignal;
}

export interface DouyinPlaywrightBatchResult {
  ok: boolean;
  error?: string;
  scanned: number;
  rounds?: number;
  stopReason?: string;
  sessionMode?: PlaywrightSessionMode;
  results: DouyinPlaywrightVideoStats[];
}

function toNullableInt(value: unknown): MaybeInt {
  if (value == null) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  const truncated = Math.trunc(parsed);
  return truncated >= 0 ? truncated : null;
}

function normalizeVideoId(videoId: string): string {
  const value = String(videoId || '').trim();
  if (!value) return '';
  if (value.toLowerCase().startsWith('douyin__')) return value.slice('douyin__'.length);
  return value;
}

function buildDouyinVideoUrl(videoId: string, webpageUrl?: string | null): string {
  const page = String(webpageUrl || '').trim();
  if (/^https?:\/\//i.test(page)) return page;
  const rawId = normalizeVideoId(videoId);
  if (!rawId) return '';
  return `https://www.douyin.com/video/${rawId}`;
}

export async function fetchDouyinChannelCardStatsByPlaywright(
  channelUrl: string,
  options: DouyinPlaywrightChannelScanOptions = {},
): Promise<DouyinPlaywrightBatchResult> {
  const normalizedChannelUrl = String(channelUrl || '').trim();
  if (!/^https?:\/\//i.test(normalizedChannelUrl)) {
    return {
      ok: false,
      error: 'invalid_channel_url',
      scanned: 0,
      rounds: 0,
      stopReason: 'invalid_channel_url',
      results: [],
    };
  }

  let playwrightMod: any;
  try {
    playwrightMod = await import('playwright');
  } catch {
    return {
      ok: false,
      error: 'playwright_not_installed',
      scanned: 0,
      rounds: 0,
      stopReason: 'playwright_not_installed',
      results: [],
    };
  }

  const timeoutMs = Math.max(5_000, Math.min(90_000, Number(options.timeoutMs || 22_000)));
  const delayMs = Math.max(300, Math.min(12_000, Number(options.delayMs || 800)));
  const maxItems = Math.max(1, Math.min(2_000, Number(options.maxItems || 500)));
  const maxNoNewRounds = Math.max(2, Math.min(12, Number(options.maxNoNewRounds || 3)));
  const maxScrollRounds = Math.max(5, Math.min(2_000, Number(options.maxScrollRounds || 300)));
  const minRoundsBeforeStop = Math.max(
    1,
    Math.min(
      maxScrollRounds,
      Number(options.minRoundsBeforeStop || Math.max(6, Math.min(24, Math.ceil(maxItems / 40)))),
    ),
  );
  const headless = options.headless !== false;
  const cookieHeader = String(options.cookieHeader || '').trim();
  const abortSignal = options.abortSignal;
  const userAgent =
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';
  const launched = await launchPlaywrightContextWithSession(playwrightMod, {
    platform: 'douyin',
    headless,
    viewport: { width: 1360, height: 900 },
    userAgent,
    extraHTTPHeaders: {
      'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
      ...(cookieHeader ? { cookie: cookieHeader } : {}),
    },
    usePersistentSession: options.usePersistentSession,
  });
  const browser = launched.browser;
  const context = launched.context;
  const sessionMode = launched.sessionMode;

  if (cookieHeader) {
    const cookies = cookieHeaderToContextCookies(cookieHeader, 'douyin');
    if (cookies.length > 0) {
      await context.addCookies(cookies).catch(() => null);
    }
  }

  await context.addInitScript(() => {
    const g = globalThis as any;
    if (typeof g.__name !== 'function') {
      g.__name = (target: unknown) => target;
    }
  });

  const page = await context.newPage();
  const byVideo = new Map<string, DouyinPlaywrightVideoStats>();
  let fatalError: string | undefined;
  let rounds = 0;
  let stopReason = 'unknown';

  const dismissLoginOverlay = async () => {
    await page.keyboard.press('Escape').catch(() => null);
    await page.evaluate(() => {
      const clickIfPossible = (el: Element | null | undefined) => {
        const node = el as HTMLElement | null;
        if (!node) return false;
        try {
          node.click();
          return true;
        } catch {
          return false;
        }
      };

      const removeNode = (el: Element | null | undefined) => {
        const node = el as HTMLElement | null;
        if (!node || !node.parentElement) return false;
        try {
          node.parentElement.removeChild(node);
          return true;
        } catch {
          return false;
        }
      };

      // Targeted close for the known Douyin login panel structure.
      const loginPanel = document.querySelector('#login-panel-new');
      if (loginPanel) {
        const directCloseCandidates = [
          '#login-panel-new .YoNA2Hyj.qKr0RhiL',
          '#login-panel-new [class*="YoNA2Hyj"][class*="qKr0RhiL"]',
          '#login-panel-new [class*="close"]',
          '#login-panel-new button',
          '#login-panel-new svg',
          '#login-panel-new path',
        ];
        for (const selector of directCloseCandidates) {
          const node = document.querySelector(selector);
          if (!node) continue;
          if (clickIfPossible(node)) break;
          if (clickIfPossible(node.parentElement)) break;
          if (clickIfPossible(node.closest('button,div'))) break;
        }

        if (document.querySelector('#login-panel-new')) {
          const panelNode = document.querySelector('#login-panel-new');
          removeNode(panelNode);
          removeNode(panelNode?.parentElement);
        }
      }

      const selectors = [
        '#login-panel-new .YoNA2Hyj.qKr0RhiL',
        '#login-panel-new [class*="YoNA2Hyj"][class*="qKr0RhiL"]',
        '#login-panel-new [class*="close"]',
        '#login-panel-new button',
        '[data-e2e*="login"] [class*="close"]',
        '[data-e2e*="login"] button',
        '[class*="login"] [class*="close"]',
        '[id*="login"] [class*="close"]',
        '[class*="modal"] [class*="close"]',
        'button[aria-label*="close" i]',
        '[class*="close-icon"]',
      ];
      for (const selector of selectors) {
        const nodes = Array.from(document.querySelectorAll(selector)) as HTMLElement[];
        for (const node of nodes) {
          clickIfPossible(node);
          clickIfPossible(node.parentElement);
          clickIfPossible(node.closest('button,div'));
        }
      }

      // Cleanup potential masks/backdrops after closing the login panel.
      const masks = Array.from(
        document.querySelectorAll('[class*="mask"], [class*="overlay"], [class*="modal"], [class*="popup"]'),
      ) as HTMLElement[];
      for (const mask of masks) {
        const text = String(mask.innerText || '').trim();
        const id = String(mask.id || '').toLowerCase();
        const cls = String(mask.className || '').toLowerCase();
        if (id.includes('login') || cls.includes('login') || text.includes('登录')) {
          removeNode(mask);
        }
      }
    }).catch(() => null);
  };

  try {
    await page.goto(normalizedChannelUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
    await page.bringToFront().catch(() => null);
    await page.waitForTimeout(Math.max(600, delayMs));
    await dismissLoginOverlay();

    let noNewRounds = 0;
    let lastKnownCount = 0;
    let loginHintRounds = 0;

    for (let round = 0; round < maxScrollRounds; round++) {
      rounds = round + 1;

      if (abortSignal?.aborted) {
        stopReason = 'aborted';
        break;
      }

      const snapshot = await page.evaluate(() => {
        const normalizeId = (value: unknown): string => String(value || '').trim().replace(/^douyin__/i, '');

        const parseCountToken = (text: string): number | null => {
          const raw = String(text || '').trim().toLowerCase().replace(/,/g, '');
          if (!raw) return null;
          const matched = raw.match(/^([0-9]+(?:\.[0-9]+)?)\s*([\u4e07\u4ebf\u5104wk])?$/i);
          if (!matched) return null;
          const base = Number(matched[1]);
          if (!Number.isFinite(base)) return null;
          const unit = matched[2] || '';
          let multiplier = 1;
          if (unit === '\u4e07' || unit === 'w') multiplier = 10_000;
          else if (unit === 'k') multiplier = 1_000;
          else if (unit === '\u4ebf' || unit === '\u5104') multiplier = 100_000_000;
          const value = Math.trunc(base * multiplier);
          return value >= 0 ? value : null;
        };

        const pickViewCount = (text: string): number | null => {
          const compact = String(text || '').replace(/\s+/g, ' ').trim();
          if (!compact) return null;
          const tokens = compact.match(/[0-9][0-9,]*(?:\.[0-9]+)?\s*[\u4e07\u4ebf\u5104wk]?/gi) || [];
          let best: number | null = null;
          let bestScore = -1;
          for (let i = 0; i < tokens.length; i++) {
            const token = tokens[i];
            const parsed = parseCountToken(token);
            if (parsed == null) continue;
            const hasUnit = /[\u4e07\u4ebf\u5104wk]/i.test(token);
            let score = 0;
            if (hasUnit) score += 100;
            if (parsed >= 1000) score += 40;
            if (i === 0) score += 16;
            if (i < 3) score += 8;
            if (score > bestScore) {
              bestScore = score;
              best = parsed;
            }
          }
          return best;
        };

        const parseHref = (href: string): { absoluteUrl: string; videoId: string; isBaiduSpider: boolean } => {
          try {
            const parsed = new URL(href, window.location.origin);
            const matched = parsed.pathname.match(/\/(?:video|note)\/(\d{8,})/i);
            const source = String(parsed.searchParams.get('source') || '');
            return {
              absoluteUrl: parsed.toString(),
              videoId: matched?.[1] || '',
              isBaiduSpider: /baiduspider/i.test(source),
            };
          } catch {
            return { absoluteUrl: '', videoId: '', isBaiduSpider: false };
          }
        };

        const result = new Map<string, { videoId: string; webpageUrl: string; viewCount: number | null; source: string }>();
        const anchors = Array.from(document.querySelectorAll('a[href*="/video/"], a[href*="/note/"]')) as HTMLAnchorElement[];

        for (const anchor of anchors) {
          const href = String(anchor.getAttribute('href') || anchor.href || '').trim();
          if (!href) continue;
          const parsedHref = parseHref(href);
          if (parsedHref.isBaiduSpider) continue;
          if (!parsedHref.videoId) continue;

          const videoId = normalizeId(parsedHref.videoId);
          if (!videoId) continue;

          const scope = anchor.closest('li, article, section, div');
          const scopeText = String(scope?.textContent || anchor.textContent || '').replace(/\s+/g, ' ').trim();
          if (!scopeText) continue;
          // Some Douyin cards include long captions; a low cutoff drops valid cards and leaves view_count missing.
          if (scopeText.length > 2000) continue;
          const viewCount = pickViewCount(scopeText);
          const webpageUrl = parsedHref.absoluteUrl || `https://www.douyin.com/video/${videoId}`;
          const current = result.get(videoId);

          if (!current) {
            result.set(videoId, { videoId, webpageUrl, viewCount, source: 'channel_card' });
            continue;
          }

          const currentView = current.viewCount;
          const nextView = viewCount;
          if (nextView != null && (currentView == null || nextView > currentView)) {
            result.set(videoId, { videoId, webpageUrl, viewCount: nextView, source: 'channel_card' });
          }
        }

        return {
          items: Array.from(result.values()),
          anchorCount: anchors.length,
          loginHint: Boolean(
            document.querySelector('#login-panel-new, [data-e2e*="login"], [class*="login"], [id*="login"], .dy-account-login')
          ),
          scrollHeight: Math.max(
            document.body?.scrollHeight || 0,
            document.documentElement?.scrollHeight || 0,
          ),
        };
      });

      if (snapshot.loginHint && (snapshot.anchorCount || 0) < 6) {
        loginHintRounds += 1;
      } else {
        loginHintRounds = 0;
      }
      if (loginHintRounds >= 2 && byVideo.size === 0) {
        stopReason = 'login_required';
        break;
      }

      for (const item of snapshot.items || []) {
        const normalizedVideoId = normalizeVideoId(String(item.videoId || ''));
        if (!normalizedVideoId) continue;

        const incomingView = toNullableInt(item.viewCount);
        const previous = byVideo.get(normalizedVideoId);
        if (!previous) {
          byVideo.set(normalizedVideoId, {
            videoId: normalizedVideoId,
            webpageUrl: buildDouyinVideoUrl(normalizedVideoId, item.webpageUrl),
            viewCount: incomingView,
            likeCount: null,
            commentCount: null,
            collectCount: null,
            shareCount: null,
            source: String(item.source || 'channel_card'),
          });
          continue;
        }

        const previousView = toNullableInt(previous.viewCount);
        if (incomingView != null && (previousView == null || incomingView > previousView)) {
          byVideo.set(normalizedVideoId, {
            ...previous,
            viewCount: incomingView,
            source: String(item.source || previous.source || 'channel_card'),
          });
        }
      }

      if (byVideo.size >= maxItems) {
        stopReason = 'max_items';
        break;
      }

      if (byVideo.size > lastKnownCount) {
        lastKnownCount = byVideo.size;
        noNewRounds = 0;
      } else {
        noNewRounds += 1;
      }

      if (noNewRounds >= maxNoNewRounds && rounds >= minRoundsBeforeStop) {
        stopReason = 'no_new_cards';
        break;
      }

      const previousHeight = toNullableInt(snapshot.scrollHeight) || 0;
      const microScrollSteps = 4;
      const microWaitMs = Math.max(150, Math.floor(delayMs / (microScrollSteps + 1)));
      for (let step = 0; step < microScrollSteps; step++) {
        await page.evaluate(() => {
          const collectScrollable = () => {
            const nodes = Array.from(document.querySelectorAll<HTMLElement>('main, section, div'));
            return nodes.filter((node) => {
              const style = window.getComputedStyle(node);
              const canScroll = /(auto|scroll)/i.test(style.overflowY || '');
              return canScroll && node.scrollHeight > node.clientHeight + 80 && node.clientHeight > 220;
            });
          };

          const candidates = collectScrollable();
          let target: HTMLElement | null = null;
          let maxArea = 0;
          for (const node of candidates) {
            const rect = node.getBoundingClientRect();
            const area = Math.max(0, rect.width) * Math.max(0, rect.height);
            if (area > maxArea) {
              maxArea = area;
              target = node;
            }
          }

          if (target) {
            target.scrollBy({ top: 900, left: 0, behavior: 'smooth' });
          }
          window.scrollBy({ top: 900, left: 0, behavior: 'smooth' });
        });
        await page.waitForTimeout(microWaitMs);
      }
      await page.keyboard.press('PageDown').catch(() => null);
      await page.waitForTimeout(Math.max(250, Math.floor(delayMs / 2)));
      await dismissLoginOverlay();

      const nextHeight = await page.evaluate(() => Math.max(
        document.body?.scrollHeight || 0,
        document.documentElement?.scrollHeight || 0,
      ));

      if (toNullableInt(nextHeight) === previousHeight) {
        noNewRounds += 1;
        if (noNewRounds >= maxNoNewRounds && rounds >= minRoundsBeforeStop) {
          stopReason = 'no_new_cards';
          break;
        }
      }
    }

    if (stopReason === 'unknown') {
      if (rounds >= maxScrollRounds) stopReason = 'max_scroll_rounds';
      else if (abortSignal?.aborted) stopReason = 'aborted';
      else stopReason = 'completed';
    }
  } catch (err: any) {
    fatalError = String(err?.message || err || 'playwright_channel_scan_failed');
    stopReason = stopReason === 'unknown' ? 'error' : stopReason;
  } finally {
    if (!fatalError && byVideo.size > 0) {
      writePlaywrightSessionMeta('douyin', {
        scanned: byVideo.size,
        stop_reason: stopReason,
      });
    }
    try { await page.close(); } catch {}
    try { await context.close(); } catch {}
    try { if (browser) await browser.close(); } catch {}
  }

  return {
    ok: !fatalError,
    error: fatalError,
    scanned: byVideo.size,
    rounds,
    stopReason,
    sessionMode,
    results: Array.from(byVideo.values()),
  };
}

