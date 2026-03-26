import { spawn, spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import { getDb, getSetting } from "../db.js";
import {
  hasUsableYoutubeCookiePoolItems,
  isYoutubeCookiePoolEnabled,
  recordYoutubeCookiePoolExecutionResult,
  reserveYoutubeCookieBindingForYtdlp,
} from "./youtubeCookiePool.js";

export interface YtDlpResult {
  success: boolean;
  data?: any;
  error?: string;
  errorCode?: string;
  outputPath?: string;
  log?: string;
}

export interface ProgressCallback {
  (progress: number, message: string): void;
}

interface YtDlpCallOptions {
  abortSignal?: AbortSignal;
  sourceUrl?: string;
  forceOverwrite?: boolean;
}

interface RunYtDlpOptions {
  onProgress?: ProgressCallback;
  outputBaseDir?: string;
  progressParts?: number;
  abortSignal?: AbortSignal;
}

interface ExecutionOutcome {
  code: number | null;
  stdout: string;
  stderr: string;
  outputPath?: string;
  log: string;
  spawnError?: Error;
  aborted?: boolean;
}

interface ProgressState {
  totalParts: number;
  completedParts: number;
  lastPercent: number;
}

interface CommonArgsBuildResult {
  args: string[];
  targetUrl: string;
  cookiePlatform: CookiePlatform;
  strictMode: boolean;
  poolEnabled: boolean;
  usedPoolBinding: boolean;
  youtubePoolCookieName: string;
  youtubePoolProxy: string;
  youtubePoolCookieId: string;
  blockedReason: string;
}

type DownloadType = "video" | "metadata" | "cover" | "subtitles";

interface BuildDownloadArgsOptions {
  type: DownloadType;
  url: string;
  downloadPath: string;
  outputTemplate: string;
  formatSelector?: string;
  mergeOutputFormat?: string;
  enableResume?: boolean;
  subtitleLangs?: string;
  extraArgs?: string[];
  forceOverwrite?: boolean;
}

function getDownloadRoot(): string {
  return getSetting("download_root") || path.join(process.cwd(), "downloads");
}

function ensureDir(dir: string): void {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function sanitizeFilenameTemplate(template: string): string {
  const trimmed = template.trim();
  if (!trimmed) {
    return "%(uploader)s-%(upload_date)s-%(id)s-%(title)s.%(ext)s";
  }
  const normalized = trimmed.replace(/\\/g, "/");
  const safeParts = normalized
    .split("/")
    .map((part) => part.trim())
    .filter((part) => part !== "" && part !== "." && part !== "..")
    .map((part) => part.replace(/[<>:"|?*]/g, "-").replace(/[. ]+$/g, ""))
    .filter((part) => part !== "");

  if (safeParts.length === 0) {
    return "%(uploader)s-%(upload_date)s-%(id)s-%(title)s.%(ext)s";
  }

  return safeParts.join("/");
}

function clampPercent(value?: number): number {
  const normalized = typeof value === "number" ? value : 0;
  if (Number.isNaN(normalized)) return 0;
  return Math.max(0, Math.min(100, normalized));
}

function estimateProgressParts(formatSelector?: string): number {
  const selector = (formatSelector || "").trim();
  if (!selector) return 2;

  const primary = selector.split("/")[0]?.trim();
  if (!primary) return 2;

  const parts = primary
    .split("+")
    .map((part) => part.trim())
    .filter((part) => part !== "");

  if (parts.length <= 1) return 1;
  if (parts.some((part) => part === "none")) return 1;
  return parts.length;
}

function parseSizeToBytes(value?: string): number | undefined {
  if (!value) return undefined;
  const cleaned = value.trim().replace(/^~\s*/, "");
  if (!cleaned) return undefined;

  const match = cleaned.match(/^([\d.,]+)\s*([KMGTP]?i?B)$/i);
  if (!match) return undefined;

  const amount = Number(match[1].replace(/,/g, ""));
  if (Number.isNaN(amount)) return undefined;

  const unit = match[2].toUpperCase();
  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1_000,
    KIB: 1_024,
    MB: 1_000_000,
    MIB: 1_048_576,
    GB: 1_000_000_000,
    GIB: 1_073_741_824,
    TB: 1_000_000_000_000,
    TIB: 1_099_511_627_776,
  };
  const multiplier = multipliers[unit];
  if (!multiplier) return undefined;

  return Math.round(amount * multiplier);
}

function parseProgressLine(line: string): {
  percent: number;
  downloaded?: string;
  total?: string;
  currentSpeed?: string;
  eta?: string;
} | null {
  const match = line.match(
    /\[download\]\s+(\d+(?:\.\d+)?)%\s+of\s+(.+?)(?:\s+at\s+(.+?))?(?:\s+ETA\s+([0-9:]+))?(?:\s|$)/i,
  );
  if (!match) {
    return null;
  }

  const percent = clampPercent(parseFloat(match[1]));
  const total = match[2]?.trim();
  const currentSpeed = match[3]?.trim();
  const eta = match[4]?.trim();
  let downloaded: string | undefined;

  const totalBytes = parseSizeToBytes(total);
  if (totalBytes !== undefined) {
    const estimated = Math.round((totalBytes * percent) / 100);
    downloaded = `${estimated}B`;
  }

  return { percent, downloaded, total, currentSpeed, eta };
}

function mergeProgressPercent(
  rawPercent: number,
  state: ProgressState,
): number {
  const normalizedPercent = clampPercent(rawPercent);
  if (
    state.totalParts > 1 &&
    state.lastPercent >= 90 &&
    normalizedPercent <= 10 &&
    state.completedParts < state.totalParts - 1
  ) {
    state.completedParts += 1;
  }

  state.lastPercent = normalizedPercent;

  if (state.totalParts <= 1) {
    return normalizedPercent;
  }

  return (
    ((state.completedParts + normalizedPercent / 100) / state.totalParts) * 100
  );
}

function resolveOutputPath(
  rawPath: string | undefined,
  outputBaseDir?: string,
): string | null {
  if (!rawPath) return null;
  const trimmed = rawPath.trim().replace(/^"|"$/g, "");
  if (!trimmed) return null;

  if (path.isAbsolute(trimmed)) {
    return trimmed;
  }

  if (!outputBaseDir) {
    return trimmed;
  }

  return path.join(outputBaseDir, trimmed);
}

function extractOutputPathFromLog(
  message: string,
  outputBaseDir?: string,
): string | null {
  const destinationMatch = message.match(/Destination:\s*(.+)$/);
  if (destinationMatch) {
    return resolveOutputPath(destinationMatch[1], outputBaseDir);
  }

  const writingMatch = message.match(/Writing .* to:\s*(.+)$/i);
  if (writingMatch) {
    return resolveOutputPath(writingMatch[1], outputBaseDir);
  }

  const mergingMatch = message.match(/Merging formats into\s+"(.+?)"/);
  if (mergingMatch) {
    return resolveOutputPath(mergingMatch[1], outputBaseDir);
  }

  const movingMatch = message.match(/Moving file to\s+"(.+?)"/);
  if (movingMatch) {
    return resolveOutputPath(movingMatch[1], outputBaseDir);
  }

  return null;
}

function parseJsonOutput(raw: string): any | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;

  try {
    return JSON.parse(trimmed);
  } catch {
    // Fall through and try line / object extraction.
  }

  const lines = trimmed
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i--) {
    try {
      return JSON.parse(lines[i]);
    } catch {
      // Keep trying.
    }
  }

  const firstObj = trimmed.indexOf("{");
  const lastObj = trimmed.lastIndexOf("}");
  if (firstObj >= 0 && lastObj > firstObj) {
    const objectSlice = trimmed.slice(firstObj, lastObj + 1);
    try {
      return JSON.parse(objectSlice);
    } catch {
      return null;
    }
  }

  return null;
}

function buildDownloadArgs(options: BuildDownloadArgsOptions): string[] {
  const args: string[] = [
    "--no-playlist",
    "--no-mtime",
    "--newline",
    "--encoding",
    "utf-8",
  ];

  if (options.type === "metadata") {
    args.push(
      "--write-info-json",
      "--skip-download",
      "--ignore-no-formats-error",
    );
  } else if (options.type === "cover") {
    args.push(
      "--write-thumbnail",
      "--skip-download",
      "--convert-thumbnails",
      "jpg",
    );
  } else if (options.type === "subtitles") {
    args.push(
      "--write-subs",
      "--write-auto-subs",
      "--skip-download",
      "--sub-langs",
      options.subtitleLangs || "en,zh,zh-Hans,zh-Hant",
      "--sub-format",
      "vtt",
    );
  }

  if (options.formatSelector) {
    args.push("-f", options.formatSelector);
  }

  if (options.mergeOutputFormat) {
    args.push("--merge-output-format", options.mergeOutputFormat);
  }

  args.push("-P", options.downloadPath);
  args.push("-o", sanitizeFilenameTemplate(options.outputTemplate));
  if (options.forceOverwrite) {
    args.push("--force-overwrites");
  }

  if (options.extraArgs && options.extraArgs.length > 0) {
    args.push(...options.extraArgs);
  }

  if (options.enableResume) {
    args.push("-c");
  }

  args.push(options.url);
  return args;
}

function executeYtDlpOnce(
  args: string[],
  options: RunYtDlpOptions,
): Promise<ExecutionOutcome> {
  return new Promise((resolve) => {
    const proc = spawn("yt-dlp", args, { shell: false, windowsHide: true });
    (runYtDlp as any)._lastProc = proc;

    let resolved = false;
    let aborted = false;
    let stdout = "";
    let stderr = "";
    let log = "";
    let outputPath: string | undefined;
    let stdoutLineBuffer = "";
    let stderrLineBuffer = "";
    let abortKillTimer: NodeJS.Timeout | null = null;

    const progressState: ProgressState = {
      totalParts: Math.max(1, options.progressParts || 1),
      completedParts: 0,
      lastPercent: 0,
    };

    const finalize = (outcome: ExecutionOutcome) => {
      if (resolved) return;
      resolved = true;
      if (abortKillTimer) {
        clearTimeout(abortKillTimer);
        abortKillTimer = null;
      }
      if (options.abortSignal) {
        options.abortSignal.removeEventListener("abort", onAbort);
      }
      resolve(outcome);
    };

    const onAbort = () => {
      if (aborted || resolved) return;
      aborted = true;
      try {
        proc.kill("SIGTERM");
      } catch {}
      abortKillTimer = setTimeout(() => {
        if (!resolved && !proc.killed) {
          try {
            proc.kill("SIGKILL");
          } catch {}
        }
      }, 1500);
    };

    if (options.abortSignal) {
      if (options.abortSignal.aborted) {
        onAbort();
      } else {
        options.abortSignal.addEventListener("abort", onAbort, { once: true });
      }
    }

    const handleLine = (rawLine: string) => {
      const line = rawLine.trim();
      if (!line) return;

      const parsedProgress = parseProgressLine(line);
      if (parsedProgress && options.onProgress) {
        const merged = mergeProgressPercent(
          parsedProgress.percent,
          progressState,
        );
        options.onProgress(clampPercent(merged), line);
      }

      const extracted = extractOutputPathFromLog(line, options.outputBaseDir);
      if (extracted) {
        outputPath = extracted;
      }
    };

    const feedLineBuffer = (
      currentBuffer: string,
      chunk: string,
      onLine: (line: string) => void,
    ): string => {
      const normalized = `${currentBuffer}${chunk.replace(/\r/g, "\n")}`;
      const lines = normalized.split("\n");
      const tail = lines.pop() || "";
      for (const line of lines) {
        onLine(line);
      }
      return tail;
    };

    proc.stdout.on("data", (data: Buffer) => {
      const chunk = data.toString();
      stdout += chunk;
      log += chunk;
      stdoutLineBuffer = feedLineBuffer(stdoutLineBuffer, chunk, handleLine);
    });

    proc.stderr.on("data", (data: Buffer) => {
      const chunk = data.toString();
      stderr += chunk;
      log += chunk;
      stderrLineBuffer = feedLineBuffer(stderrLineBuffer, chunk, handleLine);
    });

    proc.on("close", (code) => {
      if (stdoutLineBuffer.trim()) {
        handleLine(stdoutLineBuffer);
      }
      if (stderrLineBuffer.trim()) {
        handleLine(stderrLineBuffer);
      }
      if (code === 0 && options.onProgress) {
        options.onProgress(100, "[download] completed");
      }
      finalize({
        code,
        stdout: stdout.trim(),
        stderr: aborted
          ? `${stderr.trim()}\nCancelled by user`.trim()
          : stderr.trim(),
        outputPath,
        log: log.trim(),
        aborted,
      });
    });

    proc.on("error", (err) => {
      finalize({
        code: null,
        stdout: stdout.trim(),
        stderr: aborted
          ? `${stderr.trim()}\nCancelled by user`.trim()
          : stderr.trim(),
        outputPath,
        log: log.trim(),
        spawnError: err,
        aborted,
      });
    });
  });
}

function isYoutubeCookiePoolStrictModeEnabled(): boolean {
  return String(getSetting("youtube_cookie_pool_strict_mode") || "false")
    .trim()
    .toLowerCase() === "true";
}

function truncateAuditText(value: unknown, max = 500): string {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.length <= max) return text;
  return `${text.slice(0, max)}...`;
}

function writeYtDlpExecutionAudit(payload: {
  targetUrl: string;
  cookiePlatform: string;
  poolEnabled: boolean;
  strictMode: boolean;
  usedPoolBinding: boolean;
  selectedCookieId: string;
  selectedCookieName: string;
  selectedProxy: string;
  status: "success" | "failed" | "blocked";
  errorCode?: string;
  message?: string;
}): void {
  try {
    const db = getDb();
    db.prepare(`
      INSERT INTO ytdlp_exec_audit (
        target_url, cookie_platform, cookie_pool_enabled, strict_mode, used_pool_binding,
        selected_cookie_id, selected_cookie_name, selected_proxy, status, error_code, message
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      truncateAuditText(payload.targetUrl, 300),
      truncateAuditText(payload.cookiePlatform, 32),
      payload.poolEnabled ? 1 : 0,
      payload.strictMode ? 1 : 0,
      payload.usedPoolBinding ? 1 : 0,
      truncateAuditText(payload.selectedCookieId, 64),
      truncateAuditText(payload.selectedCookieName, 120),
      truncateAuditText(payload.selectedProxy, 180),
      payload.status,
      truncateAuditText(payload.errorCode || "", 64),
      truncateAuditText(payload.message || "", 500),
    );
  } catch {
    // keep yt-dlp flow robust when audit write fails
  }
}

function shouldRecordYoutubeCookiePoolFailure(result: YtDlpResult): boolean {
  if (result.success) return true;
  const errorCode = String(result.errorCode || "").trim().toLowerCase();
  const lower = String(result.error || "").toLowerCase();

  if (!lower) return false;
  if (errorCode === "youtube_tab_missing" || isYoutubeTabMissingError(lower))
    return false;

  const nonCookieFailureCodes = new Set([
    "channel_not_found",
    "removed_by_uploader",
    "private",
    "region_restricted",
    "age_restricted",
    "format_not_available",
    "js_runtime_missing",
  ]);
  if (nonCookieFailureCodes.has(errorCode)) return false;

  if (errorCode === "login_required" || errorCode === "http_403")
    return true;
  if (errorCode === "network_error") return true;

  return (
    lower.includes("cookie") ||
    lower.includes("cookies") ||
    lower.includes("sign in") ||
    lower.includes("login") ||
    lower.includes("forbidden") ||
    lower.includes("http error 403") ||
    lower.includes("http error 429") ||
    lower.includes("http error 502") ||
    lower.includes("http error 503") ||
    lower.includes("proxy") ||
    lower.includes("timed out") ||
    lower.includes("timeout") ||
    lower.includes("unexpected_eof_while_reading") ||
    lower.includes("eof occurred in violation of protocol") ||
    lower.includes("winerror 10054") ||
    lower.includes("connection reset") ||
    lower.includes("connection aborted") ||
    lower.includes("remote end closed") ||
    lower.includes("challenge solver") ||
    lower.includes("failed to fetch player") ||
    lower.includes("failed to load player")
  );
}

function runYtDlp(
  args: string[],
  options: RunYtDlpOptions = {},
): Promise<YtDlpResult> {
  return new Promise(async (resolve) => {
    const common = withCommonArgs(args);
    const effectiveArgs = common.args;
    const poolCookieId = String(common.youtubePoolCookieId || "").trim();
    if (common.blockedReason) {
      const blockedResult: YtDlpResult = {
        success: false,
        error: common.blockedReason,
        errorCode: "youtube_cookie_pool_strict_blocked",
        log: "",
      };
      writeYtDlpExecutionAudit({
        targetUrl: common.targetUrl,
        cookiePlatform: common.cookiePlatform,
        poolEnabled: common.poolEnabled,
        strictMode: common.strictMode,
        usedPoolBinding: false,
        selectedCookieId: "",
        selectedCookieName: "",
        selectedProxy: "",
        status: "blocked",
        errorCode: blockedResult.errorCode,
        message: blockedResult.error,
      });
      resolve(blockedResult);
      return;
    }
    const finish = (result: YtDlpResult) => {
      if (
        poolCookieId &&
        result.errorCode !== "cancelled" &&
        (
          result.success ||
          (common.cookiePlatform === "youtube" && shouldRecordYoutubeCookiePoolFailure(result))
        )
      ) {
        recordYoutubeCookiePoolExecutionResult(poolCookieId, !!result.success, {
          proxy: common.youtubePoolProxy,
          errorText: result.error || "",
        });
      }
      writeYtDlpExecutionAudit({
        targetUrl: common.targetUrl,
        cookiePlatform: common.cookiePlatform,
        poolEnabled: common.poolEnabled,
        strictMode: common.strictMode,
        usedPoolBinding: common.usedPoolBinding,
        selectedCookieId: common.youtubePoolCookieId,
        selectedCookieName: common.youtubePoolCookieName,
        selectedProxy: common.youtubePoolProxy,
        status: result.success ? "success" : "failed",
        errorCode: result.errorCode || "",
        message: result.error || "",
      });
      resolve(result);
    };
    const firstTry = await executeYtDlpOnce(effectiveArgs, options);

    if (firstTry.spawnError) {
      finish({
        success: false,
        error: firstTry.spawnError.message,
        errorCode: "spawn_error",
        log: firstTry.log,
      });
      return;
    }

    if (firstTry.aborted) {
      finish({
        success: false,
        error: "Cancelled by user",
        errorCode: "cancelled",
        outputPath: firstTry.outputPath,
        log: firstTry.log,
      });
      return;
    }

    if (firstTry.code === 0) {
      finish({
        success: true,
        data: firstTry.stdout,
        outputPath: firstTry.outputPath,
        log: firstTry.log,
      });
      return;
    }

    const firstErrorText =
      firstTry.stderr || firstTry.stdout || "yt-dlp failed";
    const firstErrorCode = parseErrorCode(firstTry.stderr || firstErrorText);

    if (
      shouldRetryWithoutDenoJsRuntime(firstErrorText) &&
      hasJsRuntimeArg(effectiveArgs, "deno")
    ) {
      const retryArgs = stripJsRuntimeArg(effectiveArgs, "deno");
      const retryTry = await executeYtDlpOnce(retryArgs, options);

      if (retryTry.spawnError) {
        finish({
          success: false,
          errorCode: firstErrorCode,
          error: `${firstErrorText}\n--- retry_without_deno_spawn_error ---\n${retryTry.spawnError.message}`,
          log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
        });
        return;
      }

      if (retryTry.aborted) {
        finish({
          success: false,
          error: "Cancelled by user",
          errorCode: "cancelled",
          outputPath: retryTry.outputPath || firstTry.outputPath,
          log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
        });
        return;
      }

      if (retryTry.code === 0) {
        finish({
          success: true,
          data: retryTry.stdout,
          outputPath: retryTry.outputPath || firstTry.outputPath,
          log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
        });
        return;
      }

      const retryErrorText =
        retryTry.stderr || retryTry.stdout || "retry failed";
      finish({
        success: false,
        errorCode: parseErrorCode(retryTry.stderr || retryErrorText),
        error: [
          firstErrorText,
          "--- retry_without_deno_failed ---",
          retryErrorText,
        ].join("\n"),
        outputPath: retryTry.outputPath || firstTry.outputPath,
        log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
      });
      return;
    }

    if (
      isCookieDatabaseCopyError(firstErrorText) &&
      hasCookieArgs(effectiveArgs) &&
      !(common.strictMode && common.usedPoolBinding)
    ) {
      const retryArgs = stripCookieArgs(effectiveArgs);
      const retryTry = await executeYtDlpOnce(retryArgs, options);

      if (retryTry.spawnError) {
        finish({
          success: false,
          errorCode: firstErrorCode,
          error: `${firstErrorText}\n--- retry_without_browser_cookies_spawn_error ---\n${retryTry.spawnError.message}`,
          log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
        });
        return;
      }

      if (retryTry.aborted) {
        finish({
          success: false,
          error: "Cancelled by user",
          errorCode: "cancelled",
          outputPath: retryTry.outputPath || firstTry.outputPath,
          log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
        });
        return;
      }

      if (retryTry.code === 0) {
        finish({
          success: true,
          data: retryTry.stdout,
          outputPath: retryTry.outputPath,
          log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
        });
        return;
      }

      const retryErrorText =
        retryTry.stderr || retryTry.stdout || "retry failed";
      const retryErrorCode = parseErrorCode(retryTry.stderr || retryErrorText);
      finish({
        success: false,
        errorCode: retryErrorCode,
        error: [
          firstErrorText,
          "--- retry_without_browser_cookies_failed ---",
          retryErrorText,
        ].join("\n"),
        outputPath: retryTry.outputPath || firstTry.outputPath,
        log: [firstTry.log, retryTry.log].filter(Boolean).join("\n"),
      });
      return;
    }

    finish({
      success: false,
      error: firstErrorText,
      errorCode: firstErrorCode,
      outputPath: firstTry.outputPath,
      log: firstTry.log,
    });
  });
}

type CookiePlatform =
  | "youtube"
  | "bilibili"
  | "tiktok"
  | "douyin"
  | "xiaohongshu"
  | "generic";

function detectTargetUrlFromArgs(args: string[]): string {
  for (let i = args.length - 1; i >= 0; i--) {
    const value = String(args[i] || "").trim();
    if (/^https?:\/\//i.test(value)) return value;
  }
  return "";
}

function detectCookiePlatformFromUrl(urlText: string): CookiePlatform {
  if (!urlText) return "generic";
  try {
    const parsed = new URL(urlText);
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    if (host.includes("youtube.com") || host.includes("youtu.be"))
      return "youtube";
    if (host.includes("bilibili.com") || host.includes("b23.tv"))
      return "bilibili";
    if (host.includes("tiktok.com")) return "tiktok";
    if (host.includes("douyin.com")) return "douyin";
    if (host.includes("xiaohongshu.com") || host.includes("xhslink.com"))
      return "xiaohongshu";
  } catch {
    return "generic";
  }
  return "generic";
}

function getDefaultCookieDomain(platform: CookiePlatform): string {
  switch (platform) {
    case "youtube":
      return ".youtube.com";
    case "bilibili":
      return ".bilibili.com";
    case "tiktok":
      return ".tiktok.com";
    case "douyin":
      return ".douyin.com";
    case "xiaohongshu":
      return ".xiaohongshu.com";
    default:
      return ".youtube.com";
  }
}

function resolveCookieInputForPlatform(platform: CookiePlatform): string {
  switch (platform) {
    case "youtube":
      return (
        getSetting("yt_dlp_cookie_file_youtube") ||
        getSetting("yt_dlp_cookie_file") ||
        ""
      ).trim();
    case "bilibili":
      return (getSetting("yt_dlp_cookie_file_bilibili") || "").trim();
    case "tiktok":
      return (getSetting("yt_dlp_cookie_file_tiktok") || "").trim();
    case "douyin":
      return (getSetting("yt_dlp_cookie_file_douyin") || "").trim();
    case "xiaohongshu":
      return (getSetting("yt_dlp_cookie_file_xiaohongshu") || "").trim();
    default:
      return "";
  }
}

function withCommonArgs(args: string[]): CommonArgsBuildResult {
  const next = [...args];
  const prefix: string[] = [];
  const targetUrl = detectTargetUrlFromArgs(next);
  const cookiePlatform = detectCookiePlatformFromUrl(targetUrl);
  const storedPoolEnabled =
    cookiePlatform === "youtube" ? isYoutubeCookiePoolEnabled() : false;
  const strictMode = cookiePlatform === "youtube" ? isYoutubeCookiePoolStrictModeEnabled() : false;
  // Safety net:
  // if strict mode is enabled and there are usable pool items, force-enable pool dispatch
  // even when the persisted switch was accidentally overwritten.
  const poolEnabled =
    cookiePlatform === "youtube"
      ? (storedPoolEnabled || (strictMode && hasUsableYoutubeCookiePoolItems()))
      : false;
  let usedPoolBinding = false;
  let youtubePoolCookieId = "";
  let youtubePoolCookieName = "";
  let youtubePoolProxy = "";
  let blockedReason = "";

  const hasJsRuntimes = next.includes("--js-runtimes");
  const jsRuntimesRaw = (
    getSetting("yt_dlp_js_runtimes") || "node,deno"
  ).trim();

  if (!hasJsRuntimes && jsRuntimesRaw) {
    const runtimes = Array.from(
      new Set(
        jsRuntimesRaw
          .split(/[\s,;]+/)
          .map((x) => x.trim())
          .filter(Boolean),
      ),
    );
    for (const runtime of runtimes) {
      prefix.push("--js-runtimes", runtime);
    }
  }

  if (
    getSetting("yt_dlp_disable_plugins") !== "false" &&
    !next.includes("--no-plugin-dirs")
  ) {
    prefix.push("--no-plugin-dirs");
  }

  if (!next.includes("--extractor-args")) {
    const youtubeExtractorArgs = buildYoutubeExtractorArgs();
    if (youtubeExtractorArgs) {
      prefix.push("--extractor-args", youtubeExtractorArgs);
    }
  }

  // Keep YouTube JS challenge solver component up-to-date automatically.
  if (
    cookiePlatform === "youtube" &&
    !next.includes("--remote-components")
  ) {
    const remoteComponents = (
      getSetting("yt_dlp_remote_components") || "ejs:github"
    ).trim();
    if (remoteComponents && remoteComponents.toLowerCase() !== "off") {
      prefix.push("--remote-components", remoteComponents);
    }
  }

  if (cookiePlatform === "youtube" && poolEnabled) {
    const binding = reserveYoutubeCookieBindingForYtdlp();
    if (binding && String(binding.cookie_header || "").trim()) {
      const poolCookiePath = resolveCookiePathFromInput(
        binding.cookie_header,
        "youtube",
      );
      if (poolCookiePath) {
        prefix.push("--cookies", poolCookiePath);
        const poolProxy = normalizeProxyValue(String(binding.proxy || ""));
        if (poolProxy && !next.includes("--proxy")) {
          prefix.push("--proxy", poolProxy);
        }
        usedPoolBinding = true;
        youtubePoolCookieId = String(binding.id || "").trim();
        youtubePoolCookieName = String(binding.name || "").trim();
        youtubePoolProxy = String(poolProxy || "").trim();
      }
    }
    if (!usedPoolBinding && strictMode) {
      blockedReason = "YouTube Cookie 池严格模式已启用，但当前没有可用 Cookie（可能全部禁用/无Cookie/熔断中）";
      return {
        args: [...next],
        targetUrl,
        cookiePlatform,
        strictMode,
        poolEnabled,
        usedPoolBinding: false,
        youtubePoolCookieId: "",
        youtubePoolCookieName: "",
        youtubePoolProxy: "",
        blockedReason,
      };
    }
  }

  if (!usedPoolBinding) {
    const cookieInput = resolveCookieInputForPlatform(cookiePlatform);
    const cookiePath = resolveCookiePathFromInput(cookieInput, cookiePlatform);
    if (cookiePath) {
      prefix.push("--cookies", cookiePath);
    } else if (getSetting("yt_dlp_use_browser_cookies") !== "false") {
      const cookieBrowser = resolveCookieBrowser();
      const cookieProfile = (getSetting("yt_dlp_cookies_profile") || "").trim();
      const cookieArg = cookieProfile
        ? `${cookieBrowser}:${cookieProfile}`
        : cookieBrowser;
      if (cookieArg) {
        prefix.push("--cookies-from-browser", cookieArg);
      }
    }

    const proxy = resolveProxySetting();
    if (proxy && !next.includes("--proxy")) {
      prefix.push("--proxy", proxy);
    }
  }

  return {
    args: [...prefix, ...next],
    targetUrl,
    cookiePlatform,
    strictMode,
    poolEnabled,
    usedPoolBinding,
    youtubePoolCookieId,
    youtubePoolCookieName,
    youtubePoolProxy,
    blockedReason,
  };
}

function buildYoutubeExtractorArgs(): string {
  const parts: string[] = [];

  const poToken = (getSetting("yt_dlp_youtube_po_token") || "").trim();
  const playerClientsRaw = (
    getSetting("yt_dlp_youtube_player_clients") || "default"
  ).trim();
  const playerClients = normalizeYoutubePlayerClients(
    playerClientsRaw,
    !!poToken,
  );
  if (playerClients && playerClients !== "default") {
    parts.push(`player_client=${playerClients}`);
  }

  const playerSkip = (getSetting("yt_dlp_youtube_player_skip") || "").trim();
  if (playerSkip) {
    parts.push(`player_skip=${playerSkip}`);
  }

  const visitorData = (getSetting("yt_dlp_youtube_visitor_data") || "").trim();
  if (visitorData) {
    parts.push(`visitor_data=${visitorData}`);
  }

  if (poToken) {
    parts.push(`po_token=${poToken}`);
  }

  if (parts.length === 0) {
    return "";
  }

  return `youtube:${parts.join(";")}`;
}

function normalizeYoutubePlayerClients(
  raw: string,
  hasPoToken: boolean,
): string {
  const trimmed = raw.trim().toLowerCase();
  if (!trimmed || trimmed === "default") {
    return "default";
  }

  const replacements: Record<string, string> = {
    ios_downgraded: "ios",
    tv_downgraded: "tv",
  };

  const poSensitive = new Set([
    "ios",
    "mweb",
    "web_music",
    "web_creator",
    "tv",
    "tv_simply",
    "tv_embedded",
  ]);

  const normalized = Array.from(
    new Set(
      raw
        .split(/[\s,;]+/)
        .map((x) => x.trim())
        .filter(Boolean)
        .map((x) => replacements[x] || x)
        .filter((x) => hasPoToken || !poSensitive.has(x)),
    ),
  );

  if (normalized.length === 0) {
    return "default";
  }

  return normalized.join(",");
}

function getDefaultCookieBrowser(): string {
  if (process.platform === "win32") return "edge";
  if (process.platform === "darwin") return "safari";
  return "chrome";
}

function resolveCookieBrowser(): string {
  const configured = (getSetting("yt_dlp_cookies_browser") || "auto")
    .trim()
    .toLowerCase();
  if (configured && configured !== "auto") return configured;

  const detected = detectSystemBrowser();
  return detected || getDefaultCookieBrowser();
}

function detectSystemBrowser(): string | null {
  if (process.platform !== "win32") {
    return null;
  }

  try {
    const result = spawnSync(
      "reg",
      [
        "query",
        "HKCU\\Software\\Microsoft\\Windows\\Shell\\Associations\\UrlAssociations\\https\\UserChoice",
        "/v",
        "ProgId",
      ],
      { encoding: "utf8" },
    );

    const text = `${result.stdout || ""}${result.stderr || ""}`.toLowerCase();
    if (text.includes("chromehtml")) return "chrome";
    if (text.includes("msedgehtm")) return "edge";
    if (text.includes("firefoxurl")) return "firefox";
    if (text.includes("operastable")) return "opera";
    if (text.includes("vivaldi")) return "vivaldi";
    if (text.includes("bravehtml")) return "brave";
    if (text.includes("whale")) return "whale";
  } catch {
    return null;
  }

  return null;
}

function isCookieDatabaseCopyError(errorText: string): boolean {
  const lower = (errorText || "").toLowerCase();
  return lower.includes("could not copy") && lower.includes("cookie database");
}

function hasCookieArgs(args: string[]): boolean {
  return args.includes("--cookies-from-browser") || args.includes("--cookies");
}

function stripCookieArgs(args: string[]): string[] {
  const cleaned: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const item = args[i];
    if (item === "--cookies-from-browser" || item === "--cookies") {
      i += 1;
      continue;
    }
    cleaned.push(item);
  }
  return cleaned;
}

function hasJsRuntimeArg(args: string[], runtime: string): boolean {
  const target = String(runtime || "").trim().toLowerCase();
  if (!target) return false;
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] !== "--js-runtimes") continue;
    const value = String(args[i + 1] || "").trim().toLowerCase();
    if (value === target) return true;
  }
  return false;
}

function stripJsRuntimeArg(args: string[], runtime: string): string[] {
  const target = String(runtime || "").trim().toLowerCase();
  const cleaned: string[] = [];
  for (let i = 0; i < args.length; i += 1) {
    const item = String(args[i] || "");
    if (item === "--js-runtimes") {
      const value = String(args[i + 1] || "").trim().toLowerCase();
      if (value === target) {
        i += 1;
        continue;
      }
    }
    cleaned.push(item);
  }
  return cleaned;
}

function resolveProxySetting(): string | null {
  const manualProxy = (getSetting("yt_dlp_proxy") || "").trim();
  if (manualProxy) {
    return normalizeProxyValue(manualProxy);
  }

  if (getSetting("yt_dlp_use_browser_proxy") === "false") {
    return null;
  }

  const envProxy = (
    process.env.HTTPS_PROXY ||
    process.env.https_proxy ||
    process.env.HTTP_PROXY ||
    process.env.http_proxy ||
    ""
  ).trim();

  if (envProxy) {
    return envProxy;
  }

  if (process.platform !== "win32") {
    return null;
  }

  return resolveWindowsBrowserProxy();
}

function resolveWindowsBrowserProxy(): string | null {
  try {
    const enableRes = spawnSync(
      "reg",
      [
        "query",
        "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings",
        "/v",
        "ProxyEnable",
      ],
      { encoding: "utf8" },
    );

    const enableOut = `${enableRes.stdout || ""}${enableRes.stderr || ""}`;
    if (!/0x1\b/i.test(enableOut)) {
      return null;
    }

    const proxyRes = spawnSync(
      "reg",
      [
        "query",
        "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings",
        "/v",
        "ProxyServer",
      ],
      { encoding: "utf8" },
    );

    const out = `${proxyRes.stdout || ""}`;
    const line = out.split(/\r?\n/).find((l) => /\bProxyServer\b/i.test(l));
    if (!line) return null;

    const raw = line.replace(/^.*?REG_\w+\s+/i, "").trim();
    if (!raw) return null;

    return normalizeProxyValue(raw);
  } catch {
    return null;
  }
}

function normalizeProxyValue(value: string): string {
  const raw = value.trim();
  if (!raw) return "";

  if (raw.includes("=")) {
    const pairs = raw
      .split(";")
      .map((p) => p.trim())
      .filter(Boolean);
    const prefer = ["https=", "http=", "socks="];
    for (const key of prefer) {
      const match = pairs.find((p) => p.toLowerCase().startsWith(key));
      if (match) {
        return normalizeProxyValue(match.slice(key.length));
      }
    }
    return normalizeProxyValue(pairs[0].split("=").pop() || "");
  }

  // Accept RFC-compliant schemes like socks5://, http://, https://
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) {
    return raw;
  }

  return `http://${raw}`;
}

function resolveCookiePathFromInput(
  cookieInput: string,
  platform: CookiePlatform = "youtube",
): string | null {
  const value = cookieInput.trim();
  if (!value) return null;

  if (fs.existsSync(value)) {
    return value;
  }

  const netscape = parseCookieInputToNetscape(
    value,
    getDefaultCookieDomain(platform),
  );
  if (netscape) {
    return writeNetscapeCookieFile(netscape, platform);
  }

  return value;
}

function parseCookieInputToNetscape(
  input: string,
  defaultDomain: string,
): string | null {
  const text = input.trim();
  if (!text) return null;

  if (text.includes("\t") || text.startsWith("# Netscape HTTP Cookie File")) {
    return normalizeNetscapeText(text);
  }

  const jsonCookies = parseCookieJson(text, defaultDomain);
  if (jsonCookies.length > 0) {
    return buildNetscapeFromEntries(jsonCookies, defaultDomain);
  }

  const headerCookies = parseCookieHeader(text, defaultDomain);
  if (headerCookies.length > 0) {
    return buildNetscapeFromEntries(headerCookies, defaultDomain);
  }

  return null;
}

function parseCookieHeader(
  header: string,
  defaultDomain: string,
): CookieEntry[] {
  if (!header.includes("=")) return [];
  const parsed: CookieEntry[] = [];
  const parts = header
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean);

  for (const part of parts) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const name = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (!name) continue;

    parsed.push({
      name,
      value: value.replace(/[\r\n\t]/g, " "),
      domain: defaultDomain,
      path: "/",
      secure: true,
    });
  }

  return parsed;
}

interface CookieEntry {
  name: string;
  value: string;
  domain?: string;
  path?: string;
  secure?: boolean;
  expirationDate?: number;
}

function parseCookieJson(text: string, defaultDomain: string): CookieEntry[] {
  try {
    const parsed = JSON.parse(text);

    if (Array.isArray(parsed)) {
      const entries: CookieEntry[] = [];
      for (const item of parsed) {
        if (!item || typeof item !== "object") continue;
        const obj = item as Record<string, unknown>;
        const name = String(obj.name || "").trim();
        const value = String(obj.value || "").trim();
        if (!name || !value) continue;

        entries.push({
          name,
          value,
          domain: typeof obj.domain === "string" ? obj.domain : defaultDomain,
          path: typeof obj.path === "string" ? obj.path : "/",
          secure: !!obj.secure,
          expirationDate:
            typeof obj.expirationDate === "number"
              ? obj.expirationDate
              : undefined,
        });
      }
      return entries;
    }

    if (parsed && typeof parsed === "object") {
      const entries: CookieEntry[] = [];
      for (const [name, value] of Object.entries(
        parsed as Record<string, unknown>,
      )) {
        const cookieName = String(name || "").trim();
        const cookieValue = String(value ?? "").trim();
        if (!cookieName || !cookieValue) continue;
        entries.push({
          name: cookieName,
          value: cookieValue,
          domain: defaultDomain,
          path: "/",
          secure: true,
        });
      }
      return entries;
    }
  } catch {
    return [];
  }

  return [];
}

function buildNetscapeFromEntries(
  entries: CookieEntry[],
  defaultDomain: string,
): string {
  const lines = entries.map((c) => {
    const domain = (c.domain || defaultDomain).trim() || defaultDomain;
    const flag = domain.startsWith(".") ? "TRUE" : "FALSE";
    const pathValue = (c.path || "/").trim() || "/";
    const secure = c.secure ? "TRUE" : "FALSE";
    const expiration =
      typeof c.expirationDate === "number"
        ? Math.floor(c.expirationDate)
        : 2147483647;
    return `${domain}\t${flag}\t${pathValue}\t${secure}\t${expiration}\t${c.name}\t${c.value}`;
  });

  return normalizeNetscapeText(
    `# Netscape HTTP Cookie File\n\n${lines.join("\n")}`,
  );
}

function normalizeNetscapeText(text: string): string {
  const normalized = text.replace(/\r\n/g, "\n").trimEnd();
  if (normalized.startsWith("# Netscape HTTP Cookie File")) {
    return `${normalized}\n`;
  }
  return `# Netscape HTTP Cookie File\n\n${normalized}\n`;
}

function writeNetscapeCookieFile(
  netscapeContent: string,
  platform: CookiePlatform = "youtube",
): string | null {
  try {
    const dataDir = path.join(process.cwd(), "data");
    ensureDir(dataDir);
    const fileSuffix = platform === "generic" ? "generic" : platform;
    const cookieFilePath = path.join(
      dataDir,
      `ytmonitor.cookies.${fileSuffix}.txt`,
    );
    fs.writeFileSync(cookieFilePath, netscapeContent, "utf8");
    return cookieFilePath;
  } catch {
    return null;
  }
}

function shouldRetryVideoDownloadWithFallback(error?: string): boolean {
  const lower = (error || "").toLowerCase();
  return (
    lower.includes("http error 403") ||
    lower.includes("forbidden") ||
    lower.includes("no supported javascript runtime") ||
    lower.includes("failed to fetch player po token") ||
    lower.includes("sabr streaming")
  );
}

function shouldRetryWithoutDenoJsRuntime(error?: string): boolean {
  const lower = (error || "").toLowerCase();
  return (
    lower.includes("[jsc:deno]") ||
    (lower.includes("challenge solver") && lower.includes("deno")) ||
    lower.includes("supported version: 0.4.0")
  );
}

function shouldRunConservativeFinalRetry(error?: string): boolean {
  const lower = (error || "").toLowerCase();
  return (
    lower.includes("requested format is not available") ||
    lower.includes("only images are available") ||
    lower.includes("requires a gvs po token")
  );
}

function matchesChannelUnavailablePattern(text: string): boolean {
  return (
    text.includes("this channel does not exist") ||
    text.includes("the channel does not exist") ||
    text.includes("channel not found") ||
    text.includes("this channel is not available") ||
    text.includes("this account has been terminated") ||
    text.includes("does not have a videos tab") ||
    (text.includes("youtube:tab") && text.includes("http error 404"))
  );
}

export function isChannelUnavailableError(
  errorText?: string,
  errorCode?: string,
): boolean {
  const code = (errorCode || "").toLowerCase();
  if (code === "channel_not_found") return true;

  const lower = (errorText || "").toLowerCase();
  if (!lower) return false;
  return matchesChannelUnavailablePattern(lower);
}

export function isYoutubeTabMissingError(errorText?: string): boolean {
  const lower = (errorText || "").toLowerCase();
  if (!lower.includes("youtube:tab")) return false;
  return (
    lower.includes("does not have a videos tab") ||
    lower.includes("does not have a shorts tab") ||
    lower.includes("does not have a streams tab")
  );
}

function parseErrorCode(stderr: string): string {
  const lower = (stderr || "").toLowerCase();
  if (isYoutubeTabMissingError(lower)) return "youtube_tab_missing";
  if (matchesChannelUnavailablePattern(lower)) return "channel_not_found";
  if (
    lower.includes("requested format is not available") ||
    lower.includes("only images are available")
  )
    return "format_not_available";
  if (lower.includes("no supported javascript runtime"))
    return "js_runtime_missing";
  if (lower.includes("http error 403") || lower.includes("forbidden"))
    return "http_403";
  if (lower.includes("private video") || lower.includes("is private"))
    return "private";
  if (
    lower.includes("removed") ||
    lower.includes("been removed") ||
    lower.includes("deleted")
  )
    return "removed_by_uploader";
  if (lower.includes("not available in your country") || lower.includes("geo"))
    return "region_restricted";
  if (
    lower.includes("sign in to confirm your age") ||
    lower.includes("confirm your age") ||
    lower.includes("age-restricted") ||
    lower.includes("age restricted")
  )
    return "age_restricted";
  if (lower.includes("sign in") || lower.includes("login"))
    return "login_required";
  if (
    lower.includes("network") ||
    lower.includes("connection") ||
    lower.includes("timed out") ||
    lower.includes("timeout") ||
    lower.includes("unexpected_eof_while_reading") ||
    lower.includes("eof occurred in violation of protocol") ||
    lower.includes("winerror 10054") ||
    lower.includes("connection reset") ||
    lower.includes("connection aborted") ||
    lower.includes("proxy")
  )
    return "network_error";
  if (lower.includes("ffmpeg")) return "ffmpeg_error";
  return "unknown";
}

function videoUrl(videoId: string): string {
  return `https://www.youtube.com/watch?v=${videoId}`;
}

function extractYoutubeVideoId(input: string): string | null {
  const raw = String(input || "").trim();
  if (!raw) return null;
  if (/^[A-Za-z0-9_-]{11}$/.test(raw)) return raw;

  const normalized = /^https?:\/\//i.test(raw)
    ? raw
    : /^www\./i.test(raw)
      ? `https://${raw}`
      : "";
  if (!normalized) return null;

  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    if (host === "youtu.be") {
      const id = parsed.pathname.replace(/^\/+/, "").split("/")[0] || "";
      return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
    }
    if (!host.endsWith("youtube.com")) return null;
    if (parsed.pathname === "/watch") {
      const id = parsed.searchParams.get("v") || "";
      return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
    }
    if (parsed.pathname.startsWith("/shorts/") || parsed.pathname.startsWith("/live/")) {
      const id = parsed.pathname.split("/")[2] || "";
      return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
    }
  } catch {
    return null;
  }

  return null;
}

function resolveVideoTarget(input: string): string {
  const raw = String(input || "").trim();
  if (!raw) return raw;

  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^www\./i.test(raw)) return `https://${raw}`;
  return videoUrl(raw);
}

async function downloadYoutubeThumbnailDirect(
  videoOrUrl: string,
  outputPath: string,
  abortSignal?: AbortSignal,
): Promise<boolean> {
  const youtubeId = extractYoutubeVideoId(videoOrUrl);
  if (!youtubeId) return false;

  const candidates = [
    `https://i.ytimg.com/vi/${youtubeId}/maxresdefault.jpg`,
    `https://i.ytimg.com/vi/${youtubeId}/sddefault.jpg`,
    `https://i.ytimg.com/vi/${youtubeId}/hqdefault.jpg`,
    `https://i.ytimg.com/vi/${youtubeId}/mqdefault.jpg`,
    `https://i.ytimg.com/vi/${youtubeId}/default.jpg`,
  ];

  for (const candidate of candidates) {
    try {
      const response = await fetch(candidate, { signal: abortSignal });
      if (!response.ok) continue;
      const contentType = String(response.headers.get("content-type") || "").toLowerCase();
      if (contentType && !contentType.startsWith("image/")) continue;
      const buffer = Buffer.from(await response.arrayBuffer());
      if (buffer.length === 0) continue;
      fs.writeFileSync(outputPath, buffer);
      return true;
    } catch {
      // Try next candidate or fall back to yt-dlp.
    }
  }

  return false;
}

function resolveChannelTarget(input: string): string {
  const raw = String(input || "").trim();
  if (!raw) return raw;
  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^www\./i.test(raw)) return `https://${raw}`;
  if (/^(?:m\.)?youtube\.com\//i.test(raw)) return `https://${raw}`;
  if (/^youtu\.be\//i.test(raw)) return `https://${raw}`;
  if (/^@[\w.-]+$/i.test(raw)) return `https://www.youtube.com/${raw}`;
  if (/^UC[\w-]{10,}$/i.test(raw)) return `https://www.youtube.com/channel/${raw}`;
  if (/^(?:channel|user|c)\/[^/\s]+$/i.test(raw)) {
    return `https://www.youtube.com/${raw}`;
  }
  return raw;
}

// Get channel info as JSON
export async function getChannelInfo(
  channelUrl: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const resolvedTarget = resolveChannelTarget(channelUrl);
  const result = await runYtDlp(
    [
      "--dump-single-json",
      "--playlist-items",
      "0",
      "--flat-playlist",
      "--encoding",
      "utf-8",
      resolvedTarget,
    ],
    { abortSignal: options.abortSignal },
  );

  if (result.success && result.data) {
    const parsed = parseJsonOutput(result.data);
    if (parsed) {
      return { success: true, data: parsed };
    }
    return {
      success: false,
      error: "Failed to parse channel info JSON",
      log: result.log,
    };
  }
  return result;
}

// Get oldest channel video entry (playlist reversed, first item only)
export async function getChannelOldestVideo(
  channelUrl: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const resolvedTarget = resolveChannelTarget(channelUrl);
  const result = await runYtDlp(
    [
      "--dump-single-json",
      "--flat-playlist",
      "--playlist-reverse",
      "--playlist-end",
      "1",
      "--encoding",
      "utf-8",
      resolvedTarget,
    ],
    { abortSignal: options.abortSignal },
  );

  if (result.success && result.data) {
    const parsed = parseJsonOutput(result.data);
    if (parsed) {
      return { success: true, data: parsed };
    }
    return {
      success: false,
      error: "Failed to parse oldest channel video JSON",
      log: result.log,
    };
  }
  return result;
}

// Get channel video list
export async function getChannelVideos(
  channelUrl: string,
  limit: number = 50,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const resolvedTarget = resolveChannelTarget(channelUrl);
  const result = await runYtDlp(
    [
      "--dump-single-json",
      "--flat-playlist",
      "--playlist-end",
      String(limit),
      "--encoding",
      "utf-8",
      resolvedTarget,
    ],
    { abortSignal: options.abortSignal },
  );

  if (result.success && result.data) {
    const parsed = parseJsonOutput(result.data);
    if (parsed) {
      return { success: true, data: parsed };
    }
    return {
      success: false,
      error: "Failed to parse channel videos JSON",
      log: result.log,
    };
  }
  return result;
}

// Get single video info
export async function getVideoInfo(
  videoOrUrl: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const result = await runYtDlp(
    [
      "-j",
      "--no-playlist",
      "--no-warnings",
      "--encoding",
      "utf-8",
      resolveVideoTarget(videoOrUrl),
    ],
    { abortSignal: options.abortSignal },
  );

  if (result.success && result.data) {
    const parsed = parseJsonOutput(result.data);
    if (parsed) {
      return { success: true, data: parsed };
    }
    return {
      success: false,
      error: "Failed to parse video info JSON",
      log: result.log,
    };
  }
  return result;
}

function normalizePrintedField(value: string | undefined): string | null {
  const text = String(value || "").trim();
  if (!text) return null;
  const lower = text.toLowerCase();
  if (lower === "na" || lower === "none" || lower === "null") return null;
  return text;
}

// Probe channel identity quickly for dedupe use-cases.
export async function getVideoChannelIdentity(
  videoOrUrl: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const result = await runYtDlp(
    [
      "--no-playlist",
      "--no-warnings",
      "--skip-download",
      "--encoding",
      "utf-8",
      "--print",
      "%(channel_id)s",
      "--print",
      "%(channel)s",
      "--print",
      "%(uploader)s",
      "--print",
      "%(uploader_id)s",
      "--print",
      "%(channel_url)s",
      "--print",
      "%(uploader_url)s",
      resolveVideoTarget(videoOrUrl),
    ],
    { abortSignal: options.abortSignal },
  );

  if (!result.success) return result;

  const lines = String(result.data || "")
    .split(/\r?\n/)
    .map((line) => line.trim());
  const pick = (index: number): string | null => normalizePrintedField(lines[index]);

  const data = {
    channel_id: pick(0),
    channel: pick(1),
    uploader: pick(2),
    uploader_id: pick(3),
    channel_url: pick(4),
    uploader_url: pick(5),
  };

  if (!data.channel_id && !data.channel && !data.uploader && !data.uploader_id) {
    return {
      success: false,
      error: "Failed to probe channel identity",
      log: result.log,
    };
  }

  return {
    success: true,
    data,
    log: result.log,
  };
}

// Download metadata JSON
export async function downloadMeta(
  videoId: string,
  channelId: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const root = getDownloadRoot();
  const dir = path.join(root, "assets", "meta", channelId, videoId);
  ensureDir(dir);

  return runYtDlp(
    buildDownloadArgs({
      type: "metadata",
      url: resolveVideoTarget(options.sourceUrl || videoId),
      downloadPath: dir,
      outputTemplate: `${videoId}.%(ext)s`,
      forceOverwrite: options.forceOverwrite,
    }),
    { outputBaseDir: dir, abortSignal: options.abortSignal },
  );
}

// Download thumbnail
export async function downloadThumb(
  videoId: string,
  channelId: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const root = getDownloadRoot();
  const dir = path.join(root, "assets", "thumbs", channelId, videoId);
  ensureDir(dir);
  const outputPath = path.join(dir, `${videoId}.jpg`);

  if (
    await downloadYoutubeThumbnailDirect(
      options.sourceUrl || videoId,
      outputPath,
      options.abortSignal,
    )
  ) {
    return {
      success: true,
      outputPath,
      log: "youtube_thumb_direct",
    };
  }

  return runYtDlp(
    buildDownloadArgs({
      type: "cover",
      url: resolveVideoTarget(options.sourceUrl || videoId),
      downloadPath: dir,
      outputTemplate: `${videoId}.%(ext)s`,
    }),
    { outputBaseDir: dir, abortSignal: options.abortSignal },
  );
}

// Download subtitles
export async function downloadSubs(
  videoId: string,
  channelId: string,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const root = getDownloadRoot();
  const dir = path.join(root, "assets", "subs", channelId, videoId);
  ensureDir(dir);

  const langs = getSetting("subtitle_langs") || "en,zh,zh-Hans,zh-Hant";

  return runYtDlp(
    buildDownloadArgs({
      type: "subtitles",
      url: resolveVideoTarget(options.sourceUrl || videoId),
      downloadPath: dir,
      outputTemplate: `${videoId}.%(ext)s`,
      subtitleLangs: langs,
    }),
    { outputBaseDir: dir, abortSignal: options.abortSignal },
  );
}

// Download video
export async function downloadVideo(
  videoId: string,
  channelId: string,
  onProgress?: ProgressCallback,
  options: YtDlpCallOptions = {},
): Promise<YtDlpResult> {
  const root = getDownloadRoot();
  const dir = path.join(root, "assets", "videos", channelId, videoId);
  ensureDir(dir);

  const formatSelector =
    getSetting("format_selector") || "bestvideo+bestaudio/best";
  const container = getSetting("container") || "mp4";
  const enableResume = getSetting("enable_resume") !== "false";
  const sourceUrl = resolveVideoTarget(options.sourceUrl || videoId);

  const firstTry = await runYtDlp(
    buildDownloadArgs({
      type: "video",
      url: sourceUrl,
      downloadPath: dir,
      outputTemplate: `${videoId}.%(ext)s`,
      formatSelector,
      mergeOutputFormat: container,
      enableResume,
    }),
    {
      onProgress,
      outputBaseDir: dir,
      progressParts: estimateProgressParts(formatSelector),
      abortSignal: options.abortSignal,
    },
  );

  if (
    firstTry.success ||
    !shouldRetryVideoDownloadWithFallback(firstTry.error)
  ) {
    return firstTry;
  }

  const fallbackFormat = getSetting("format_selector_fallback") || "best";
  const fallbackPoToken = (getSetting("yt_dlp_youtube_po_token") || "").trim();
  const fallbackPlayerClients = normalizeYoutubePlayerClients(
    (
      getSetting("yt_dlp_youtube_fallback_player_clients") || "default"
    ).trim(),
    !!fallbackPoToken,
  );
  const fallbackExtraArgs = (fallbackPlayerClients && fallbackPlayerClients !== 'default')
    ? ["--extractor-args", `youtube:player_client=${fallbackPlayerClients}`]
    : [];

  const secondTry = await runYtDlp(
    buildDownloadArgs({
      type: "video",
      url: sourceUrl,
      downloadPath: dir,
      outputTemplate: `${videoId}.%(ext)s`,
      formatSelector: fallbackFormat,
      mergeOutputFormat: container,
      enableResume,
      extraArgs: fallbackExtraArgs,
    }),
    {
      onProgress,
      outputBaseDir: dir,
      progressParts: estimateProgressParts(fallbackFormat),
      abortSignal: options.abortSignal,
    },
  );
  if (secondTry.success) {
    return secondTry;
  }

  if (shouldRunConservativeFinalRetry(secondTry.error)) {
    const thirdTry = await runYtDlp(
      buildDownloadArgs({
        type: "video",
        url: sourceUrl,
        downloadPath: dir,
        outputTemplate: `${videoId}.%(ext)s`,
        formatSelector: "best",
        mergeOutputFormat: container,
        enableResume,
        extraArgs: ["--extractor-args", "youtube:player_client=android"],
      }),
      {
        onProgress,
        outputBaseDir: dir,
        progressParts: estimateProgressParts("best"),
        abortSignal: options.abortSignal,
      },
    );
    if (thirdTry.success) {
      return thirdTry;
    }

    return {
      success: false,
      errorCode:
        thirdTry.errorCode || secondTry.errorCode || firstTry.errorCode,
      outputPath:
        thirdTry.outputPath || secondTry.outputPath || firstTry.outputPath,
      log: [firstTry.log, secondTry.log, thirdTry.log]
        .filter(Boolean)
        .join("\n"),
      error: [
        firstTry.error || "first_try_failed",
        "--- retry_with_fallback_clients_failed ---",
        secondTry.error || "second_try_failed",
        "--- retry_with_android_best_failed ---",
        thirdTry.error || "third_try_failed",
      ].join("\n"),
    };
  }

  return {
    success: false,
    errorCode: secondTry.errorCode || firstTry.errorCode,
    outputPath: secondTry.outputPath || firstTry.outputPath,
    log: [firstTry.log, secondTry.log].filter(Boolean).join("\n"),
    error: [
      firstTry.error || "first_try_failed",
      "--- retry_with_fallback_clients_failed ---",
      secondTry.error || "fallback_failed",
    ].join("\n"),
  };
}

// Check video availability
export async function checkAvailability(
  videoId: string,
  options: YtDlpCallOptions = {},
): Promise<{ available: boolean; reason?: string; rawMessage?: string }> {
  const result = await runYtDlp(
    [
      "--simulate",
      "--skip-download",
      "--no-playlist",
      "--no-warnings",
      "--encoding",
      "utf-8",
      resolveVideoTarget(options.sourceUrl || videoId),
    ],
    { abortSignal: options.abortSignal },
  );

  if (result.success) {
    return { available: true };
  }

  return {
    available: false,
    reason: result.errorCode || "unknown",
    rawMessage: result.error?.substring(0, 500),
  };
}

// Parse video metadata from yt-dlp JSON output
function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
}

function toNullableInt(value: unknown): number | null {
  const num = toNullableNumber(value);
  if (num == null) return null;
  return Math.trunc(num);
}

function normalizeEpochSeconds(value: unknown): number | null {
  const raw = toNullableNumber(value);
  if (raw == null) return null;
  // ms timestamp fallback
  if (raw > 1e12) return Math.floor(raw / 1000);
  return Math.floor(raw);
}

function formatUploadDate(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const text = raw.trim();
  if (!/^\d{8}$/.test(text)) return null;
  return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
}

function epochToDateText(epochSec: number | null): string | null {
  if (epochSec == null) return null;
  const date = new Date(epochSec * 1000);
  if (Number.isNaN(date.getTime())) return null;
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function epochToIsoDateTimeText(epochSec: number | null): string | null {
  if (epochSec == null) return null;
  const date = new Date(epochSec * 1000);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function inferDurationSec(info: any): number | null {
  const raw = toNullableNumber(info?.duration ?? info?.video?.duration);
  if (raw == null) return null;
  const extractor = String(
    info?.extractor_key || info?.extractor || "",
  ).toLowerCase();
  // Douyin samples often store milliseconds in duration.
  if ((extractor.includes("douyin") || info?.aweme_id) && raw > 1000) {
    return Math.max(1, Math.round(raw / 1000));
  }
  return Math.round(raw);
}

function inferMetaPlatform(
  info: any,
  webpageUrl: string | null,
): "youtube" | "tiktok" | "douyin" | "xiaohongshu" | "other" {
  const extractor = String(
    info?.extractor_key || info?.extractor || "",
  ).toLowerCase();
  if (extractor.includes("youtube")) return "youtube";
  if (extractor.includes("tiktok")) return "tiktok";
  if (extractor.includes("douyin")) return "douyin";
  if (extractor.includes("xiaohongshu") || extractor.includes("xhs"))
    return "xiaohongshu";

  const candidates = [
    webpageUrl,
    info?.webpage_url,
    info?.original_url,
    info?.uploader_url,
    info?.channel_url,
    info?.url,
  ];
  for (const candidate of candidates) {
    const raw = String(candidate || "")
      .trim()
      .toLowerCase();
    if (!raw) continue;
    if (raw.includes("youtube.com") || raw.includes("youtu.be"))
      return "youtube";
    if (raw.includes("tiktok.com")) return "tiktok";
    if (raw.includes("douyin.com")) return "douyin";
    if (raw.includes("xiaohongshu.com") || raw.includes("xhslink.com"))
      return "xiaohongshu";
  }

  return "other";
}

type ParsedVideoContentType = "long" | "short" | "live" | "note" | "album";
type DouyinVisualKind = "video" | "album" | "live_photo";

function collectImageCandidates(value: unknown): any[] {
  if (Array.isArray(value))
    return value.filter((item) => item && typeof item === "object");
  if (!value || typeof value !== "object") return [];
  const row = value as any;
  if (Array.isArray(row?.images))
    return row.images.filter((item: any) => item && typeof item === "object");
  return [];
}

function extractMetaImages(info: any): any[] {
  const candidates = [
    info?.images,
    info?.image_infos,
    info?.image_list,
    info?.imagePost?.images,
    info?.image_post_info?.images,
    info?.raw?.images,
    info?.raw?.image_infos,
    info?.raw?.image_list,
    info?.raw?.imagePost?.images,
    info?.raw?.image_post_info?.images,
  ];
  const out: any[] = [];
  for (const candidate of candidates) {
    out.push(...collectImageCandidates(candidate));
  }
  return out;
}

function classifyDouyinVisualKindMeta(
  info: any,
  webpageUrl: string | null,
  durationSec: number | null,
): DouyinVisualKind {
  const contentType = String(info?.content_type || "")
    .trim()
    .toLowerCase();
  if (contentType === "live_photo") return "live_photo";
  if (contentType === "album" || contentType === "note") return "album";
  if (
    contentType === "short" ||
    contentType === "long" ||
    contentType === "video"
  )
    return "video";

  const images = extractMetaImages(info);
  if (images.length > 0) {
    const hasLivePhoto = images.some(
      (item) => item && typeof item === "object" && (item as any).video != null,
    );
    return hasLivePhoto ? "live_photo" : "album";
  }

  const maybeImageType = Number(info?.aweme_type ?? info?.raw?.aweme_type);
  if (Number.isFinite(maybeImageType) && [2, 68, 150].includes(maybeImageType))
    return "album";

  const urls = [
    webpageUrl,
    info?.webpage_url,
    info?.share_url,
    info?.url,
    info?.original_url,
  ];
  for (const candidate of urls) {
    const text = String(candidate || "")
      .trim()
      .toLowerCase();
    if (!text) continue;
    if (text.includes("/note/") || text.includes("/slides/")) return "album";
  }

  if ((durationSec == null || durationSec <= 0) && Boolean(info?.is_image_beat))
    return "album";
  return "video";
}

function toPositiveInt(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.trunc(parsed);
}

function readResolutionFromNode(
  value: unknown,
): { width: number; height: number } | null {
  if (!value || typeof value !== "object") return null;
  const row = value as any;
  const width = toPositiveInt(row?.width);
  const height = toPositiveInt(row?.height);
  if (width == null || height == null) return null;
  return { width, height };
}

function extractMetaResolution(
  info: any,
): { width: number; height: number } | null {
  const candidates: Array<{ width: number; height: number }> = [];
  const feed = (value: unknown) => {
    const parsed = readResolutionFromNode(value);
    if (parsed) candidates.push(parsed);
  };

  const nodeCandidates = [
    info,
    info?.video,
    info?.play_addr,
    info?.video?.play_addr,
    info?.raw,
    info?.raw?.video,
    info?.raw?.play_addr,
    info?.raw?.video?.play_addr,
  ];
  for (const node of nodeCandidates) feed(node);

  const bitRateLists = [
    info?.bit_rate,
    info?.video?.bit_rate,
    info?.raw?.bit_rate,
    info?.raw?.video?.bit_rate,
  ];
  for (const list of bitRateLists) {
    if (!Array.isArray(list)) continue;
    for (const item of list) {
      feed(item);
      feed(item?.play_addr);
    }
  }

  if (Array.isArray(info?.formats)) {
    for (const format of info.formats) {
      feed(format);
    }
  }

  if (candidates.length === 0) return null;
  candidates.sort((a, b) => b.width * b.height - a.width * a.height);
  return candidates[0];
}

function resolveMetaOrientation(
  info: any,
): "portrait" | "landscape" | "square" | null {
  const resolution = extractMetaResolution(info);
  if (!resolution) return null;
  if (resolution.width > resolution.height) return "landscape";
  if (resolution.width < resolution.height) return "portrait";
  return "square";
}

export function parseVideoMeta(info: any): {
  video_id: string;
  channel_id: string;
  title: string;
  description: string | null;
  uploader: string | null;
  webpage_url: string | null;
  published_at: string | null;
  duration_sec: number | null;
  content_type: ParsedVideoContentType;
  content_type_source: string;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  collect_count: number | null;
  share_count: number | null;
} {
  const rawWebpageUrl =
    String(info?.webpage_url || info?.share_url || info?.url || "").trim() ||
    null;
  const duration = inferDurationSec(info);
  const metaPlatform = inferMetaPlatform(info, rawWebpageUrl);
  const douyinVisualKind =
    metaPlatform === "douyin"
      ? classifyDouyinVisualKindMeta(info, rawWebpageUrl, duration)
      : "video";
  const webpageUrl =
    String(
      rawWebpageUrl ||
        (info?.aweme_id
          ? `https://www.douyin.com/${douyinVisualKind === "video" ? "video" : "note"}/${info.aweme_id}`
          : "") ||
        (info?.id &&
        String(info?.extractor_key || "")
          .toLowerCase()
          .includes("xiaohongshu")
          ? `https://www.xiaohongshu.com/explore/${info.id}`
          : ""),
    ).trim() || null;
  const isShortExplicit =
    String(webpageUrl || "").includes("/shorts/") ||
    String(info?.original_url || "").includes("/shorts/") ||
    (Array.isArray(info?.categories) && info.categories.includes("Shorts"));
  const rawLiveStatus = String(info?.live_status || info?.raw?.live_status || "")
    .trim()
    .toLowerCase();
  const isYoutubeLive = metaPlatform === "youtube" && (
    rawLiveStatus === "is_live" ||
    rawLiveStatus === "was_live" ||
    rawLiveStatus === "is_upcoming" ||
    rawLiveStatus === "post_live" ||
    info?.is_live === true ||
    info?.was_live === true
  );

  let contentType: ParsedVideoContentType = "long";
  let contentTypeSource = "default_long";

  if (isYoutubeLive) {
    contentType = "live";
    contentTypeSource = rawLiveStatus ? `youtube_live_status_${rawLiveStatus}` : "youtube_live_status";
  } else if (isShortExplicit) {
    contentType = "short";
    contentTypeSource = "url_or_category";
  } else if (metaPlatform === "douyin") {
    if (douyinVisualKind === "live_photo") {
      contentType = "album";
      contentTypeSource = "douyin_album_live_photo_meta";
    } else if (douyinVisualKind === "album") {
      contentType = "album";
      contentTypeSource = "douyin_album_meta";
    } else {
      const rawType = String(
        info?.content_type || info?.raw?.content_type || "",
      )
        .trim()
        .toLowerCase();
      if (rawType === "short") {
        contentType = "short";
        contentTypeSource = "douyin_video_raw_short_meta";
      } else {
        contentType = "long";
        const orientation = resolveMetaOrientation(info);
        if (orientation === "landscape")
          contentTypeSource = "douyin_video_landscape_meta";
        else if (orientation === "portrait") {
          contentType = "short";
          contentTypeSource = "douyin_video_portrait_meta";
        } else if (orientation === "square")
          contentTypeSource = "douyin_video_square_meta";
        else if (duration != null && duration < 60) {
          contentType = "short";
          contentTypeSource = "douyin_video_duration_short_meta";
        } else if (duration != null && duration >= 60) {
          contentType = "long";
          contentTypeSource = "douyin_video_duration_long_meta";
        } else {
          contentTypeSource = "douyin_video_meta";
        }
      }
    }
  } else if (metaPlatform === "tiktok") {
    contentType = "short";
    contentTypeSource = "platform_default_short";
  } else if (metaPlatform === "xiaohongshu") {
    const rawType = String(
      info?.content_type ||
        info?.note_type ||
        info?.raw?.content_type ||
        info?.raw?.note_type ||
        info?.note_card?.type ||
        "",
    )
      .trim()
      .toLowerCase();
    if (
      ["album", "note", "normal", "image"].includes(rawType) ||
      rawType.includes("图")
    ) {
      contentType = "album";
      contentTypeSource = "xhs_content_type_album";
    } else if (
      rawType === "short" ||
      rawType === "long" ||
      rawType.includes("video")
    ) {
      contentType = "short";
      contentTypeSource = "xhs_content_type_video";
    } else {
      contentType = "short";
      contentTypeSource = "xhs_video_default_meta";
    }
  }

  const timestamp = normalizeEpochSeconds(
    info?.timestamp ?? info?.release_timestamp ?? info?.create_time,
  );
  const publishedAt =
    epochToIsoDateTimeText(timestamp) ??
    formatUploadDate(info?.upload_date) ??
    epochToDateText(timestamp);
  const likeCount = toNullableInt(
    info?.like_count ??
      info?.liked_count ??
      info?.statistics?.digg_count ??
      info?.statistics?.diggCount ??
      info?.stats?.diggCount ??
      info?.statsV2?.diggCount ??
      info?.stat?.like ??
      info?.interact_info?.liked_count ??
      info?.note_card?.interact_info?.liked_count,
  );
  const commentCount = toNullableInt(
    info?.comment_count ??
      info?.statistics?.comment_count ??
      info?.statistics?.commentCount ??
      info?.stats?.commentCount ??
      info?.statsV2?.commentCount ??
      info?.stat?.reply ??
      info?.interact_info?.comment_count ??
      info?.note_card?.interact_info?.comment_count,
  );
  const collectCount = toNullableInt(
    info?.collect_count ??
      info?.collected_count ??
      info?.statistics?.collect_count ??
      info?.statistics?.collectCount ??
      info?.stats?.collectCount ??
      info?.statsV2?.collectCount ??
      info?.stat?.collect ??
      info?.interact_info?.collect_count ??
      info?.interact_info?.collected_count ??
      info?.note_card?.interact_info?.collect_count ??
      info?.note_card?.interact_info?.collected_count,
  );
  const shareCount = toNullableInt(
    info?.share_count ??
      info?.statistics?.share_count ??
      info?.statistics?.shareCount ??
      info?.stats?.shareCount ??
      info?.statsV2?.shareCount ??
      info?.stat?.share ??
      info?.interact_info?.share_count ??
      info?.note_card?.interact_info?.share_count,
  );
  let viewCount = toNullableInt(
    info?.view_count ??
      info?.play_count ??
      info?.statistics?.play_count ??
      info?.statistics?.playCount ??
      info?.stats?.playCount ??
      info?.statsV2?.playCount ??
      info?.stat?.view ??
      info?.note_card?.play_count ??
      info?.note_card?.interact_info?.view_count,
  );
  if (
    viewCount === 0 &&
    [likeCount, commentCount, collectCount, shareCount].some(
      (value) => value != null && value > 0,
    )
  ) {
    viewCount = null;
  }

  return {
    video_id: String(
      info?.id || info?.aweme_id || info?.bvid || info?.aid || "",
    ).trim(),
    channel_id: String(
      info?.channel_id ||
        info?.uploader_id ||
        info?.author?.sec_uid ||
        info?.author?.uid ||
        info?.author_user_id ||
        info?.user?.user_id ||
        "",
    ).trim(),
    title: String(info?.title || info?.desc || "Untitled").trim() || "Untitled",
    description:
      typeof info?.description === "string"
        ? info.description
        : typeof info?.desc === "string"
          ? info.desc
          : null,
    uploader:
      String(
        info?.uploader ||
          info?.channel ||
          info?.author?.nickname ||
          info?.user?.nickname ||
          "",
      ).trim() || null,
    webpage_url: webpageUrl,
    published_at: publishedAt,
    duration_sec: duration,
    content_type: contentType,
    content_type_source: contentTypeSource,
    view_count: viewCount,
    like_count: likeCount,
    comment_count: commentCount,
    collect_count: collectCount,
    share_count: shareCount,
  };
}

// Parse channel metadata from yt-dlp JSON output
export function parseChannelMeta(info: any): {
  channel_id: string;
  title: string;
  handle: string | null;
  avatar_url: string | null;
  subscriber_count: number | null;
  view_count_total: number | null;
  video_count: number | null;
} {
  // Find avatar from thumbnails
  let avatarUrl = null;
  if (info.thumbnails && info.thumbnails.length > 0) {
    const avatar =
      info.thumbnails.find((t: any) => t.id === "avatar_uncropped") ||
      info.thumbnails[0];
    avatarUrl = avatar?.url || null;
  }
  if (!avatarUrl) {
    avatarUrl =
      info?.author?.avatar_thumb?.url_list?.[0] ||
      info?.author?.avatar_medium?.url_list?.[0] ||
      info?.author?.avatar_larger?.url_list?.[0] ||
      null;
  }

  const firstEntry =
    Array.isArray(info?.entries) && info.entries.length > 0
      ? info.entries.find((item: any) => item && typeof item === "object")
      : null;
  const fallbackTitle = String(
    info?.channel ||
      info?.title ||
      info?.uploader ||
      info?.author?.nickname ||
      firstEntry?.channel ||
      firstEntry?.uploader ||
      "",
  ).trim();
  const fallbackChannelId = String(
    info?.channel_id ||
      info?.id ||
      info?.author?.sec_uid ||
      info?.author?.uid ||
      info?.uploader_id ||
      firstEntry?.channel_id ||
      firstEntry?.uploader_id ||
      "",
  ).trim();
  const fallbackHandle = String(
    info?.uploader_id ||
      info?.author?.unique_id ||
      info?.author?.sec_uid ||
      firstEntry?.uploader_id ||
      firstEntry?.channel_id ||
      info?.channel_url?.split("/").pop() ||
      "",
  ).trim();
  const fallbackSubscriberCount = toNullableInt(
    info?.channel_follower_count ??
      info?.follower_count ??
      info?.author?.follower_count ??
      info?.author?.fans ??
      info?.user?.followers ??
      firstEntry?.channel_follower_count ??
      firstEntry?.follower_count ??
      firstEntry?.author?.follower_count,
  );
  const fallbackViewCount = toNullableInt(
    info?.view_count ??
      info?.play_count ??
      info?.author?.total_favorited ??
      firstEntry?.view_count ??
      firstEntry?.play_count ??
      firstEntry?.statistics?.play_count ??
      firstEntry?.statistics?.playCount ??
      firstEntry?.stats?.playCount ??
      firstEntry?.statsV2?.playCount,
  );
  const extractor = String(info?.extractor_key || info?.extractor || "").toLowerCase();
  const isYoutubeExtractor = extractor.includes("youtube");
  const fallbackVideoCount = toNullableInt(
    info?.channel_video_count ??
      firstEntry?.channel_video_count ??
      info?.aweme_count ??
      info?.author?.aweme_count ??
      info?.author?.video_count ??
      // For YouTube channel root pages, `playlist_count` is "播放列表数量" instead of "视频总数".
      // Avoid using it to prevent overwriting total video count with wrong low numbers (e.g. 3).
      (isYoutubeExtractor ? null : info?.playlist_count),
  );

  return {
    channel_id: fallbackChannelId,
    title: fallbackTitle || "Unknown",
    handle: fallbackHandle || null,
    avatar_url: avatarUrl,
    subscriber_count: fallbackSubscriberCount,
    view_count_total: fallbackViewCount,
    video_count: fallbackVideoCount,
  };
}
