import { execSync } from 'child_process';

export function todayDateStr(): string {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

export function daysAgoDateStr(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

export function rangeToStartDate(range: string): string {
  switch (range) {
    case '7d': return daysAgoDateStr(6);
    case '28d': return daysAgoDateStr(27);
    case '3m': return daysAgoDateStr(89);
    case '1y': return daysAgoDateStr(364);
    case 'max': return '1970-01-01';
    default: return daysAgoDateStr(27);
  }
}

export function checkYtDlp(): { available: boolean; version?: string; error?: string } {
  try {
    const output = execSync('yt-dlp --version', { encoding: 'utf-8', timeout: 10000 }).trim();
    return { available: true, version: output };
  } catch {
    return { available: false, error: 'yt-dlp not found. Please install yt-dlp.' };
  }
}

export function parseDuration(seconds: number | null | undefined): string {
  if (seconds == null) return 'N/A';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  return `${m}:${String(s).padStart(2, '0')}`;
}

export function relativeTime(dateStr: string | null): string {
  if (!dateStr) return 'N/A';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(months / 12)}y ago`;
}

export function formatNumber(n: number | null | undefined): string {
  if (n == null) return 'N/A';
  const abs = Math.abs(n);
  const compact = (value: number) => {
    const fixed = value.toFixed(1);
    return fixed.endsWith('.0') ? fixed.slice(0, -2) : fixed;
  };

  if (abs >= 1e12) return `${compact(n / 1e12)}万亿`;
  if (abs >= 1e8) return `${compact(n / 1e8)}亿`;
  if (abs >= 1e4) return `${compact(n / 1e4)}万`;
  return `${Math.round(n)}`;
}
