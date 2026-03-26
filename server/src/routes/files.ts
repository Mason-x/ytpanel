import { Router, Request, Response } from 'express';
import { spawn, spawnSync } from 'child_process';
import path from 'path';
import fs from 'fs';

const router = Router();
const OPEN_FOLDER_LOG_PATH = path.resolve(process.cwd(), 'data', 'open-folder.log');
const OPEN_FOLDER_DEBUG = /^(1|true|yes)$/i.test(process.env.OPEN_FOLDER_DEBUG || '');

interface LaunchSpec {
  command: string;
  args: string[];
}

function launchDetached(command: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(command, args, {
        detached: false,
        stdio: 'ignore',
        windowsHide: true,
        shell: false,
      });
    } catch (err) {
      reject(err);
      return;
    }

    let settled = false;
    child.once('error', (err) => {
      if (settled) return;
      settled = true;
      reject(err);
    });
    child.once('spawn', () => {
      if (settled) return;
      settled = true;
      child.unref();
      resolve();
    });
  });
}

function escapePsSingleQuoted(value: string): string {
  return value.replace(/'/g, "''");
}

function appendOpenFolderLog(message: string): void {
  if (!OPEN_FOLDER_DEBUG) return;
  try {
    fs.mkdirSync(path.dirname(OPEN_FOLDER_LOG_PATH), { recursive: true });
    fs.appendFileSync(OPEN_FOLDER_LOG_PATH, `${new Date().toISOString()} ${message}\n`, 'utf8');
  } catch {}
}

function pickFolderOnWindows(title: string, initialPath?: string): { path: string | null; error?: string } {
  const normalizedTitle = (typeof title === 'string' && title.trim()) ? title.trim() : '请选择目标文件夹';
  const normalizedInitialPath = (typeof initialPath === 'string' && initialPath.trim())
    ? path.resolve(initialPath.trim())
    : '';

  const scriptLines = [
    'Add-Type -AssemblyName System.Windows.Forms',
    '$dialog = New-Object System.Windows.Forms.FolderBrowserDialog',
    `$dialog.Description = '${escapePsSingleQuoted(normalizedTitle)}'`,
    '$dialog.UseDescriptionForTitle = $true',
    '$dialog.ShowNewFolderButton = $true',
  ];

  if (normalizedInitialPath && fs.existsSync(normalizedInitialPath)) {
    scriptLines.push(`$dialog.SelectedPath = '${escapePsSingleQuoted(normalizedInitialPath)}'`);
  }

  scriptLines.push(
    '$result = $dialog.ShowDialog()',
    "if ($result -eq [System.Windows.Forms.DialogResult]::OK -and $dialog.SelectedPath) {",
    '  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8',
    '  Write-Output $dialog.SelectedPath',
    '}',
  );

  const pickResult = spawnSync(
    'powershell.exe',
    ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', scriptLines.join('; ')],
    {
      encoding: 'utf8',
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );

  if (pickResult.error) {
    return { path: null, error: pickResult.error.message || 'Failed to launch folder picker' };
  }

  if ((pickResult.status ?? 0) !== 0) {
    const stderrText = (pickResult.stderr || '').trim();
    if (stderrText) return { path: null, error: stderrText };
  }

  const pickedPath = (pickResult.stdout || '').trim();
  if (!pickedPath) return { path: null };

  return { path: path.resolve(pickedPath) };
}

// POST /api/files/pick-folder
router.post('/pick-folder', (req: Request, res: Response) => {
  const { title, initial_path } = req.body as { title?: string; initial_path?: string };

  if (process.platform !== 'win32') {
    res.status(400).json({ error: 'Folder picker is only supported on Windows for now' });
    return;
  }

  const result = pickFolderOnWindows(title || '请选择导出目录', initial_path);
  if (result.error) {
    appendOpenFolderLog(`[pick-folder][error] ${result.error}`);
    res.status(500).json({ error: result.error });
    return;
  }

  if (!result.path) {
    res.json({ canceled: true, path: null });
    return;
  }

  res.json({ canceled: false, path: result.path });
});

// POST /api/files/open-folder
router.post('/open-folder', async (req: Request, res: Response) => {
  const { path: inputPath, reveal } = req.body as { path?: string; reveal?: boolean };
  if (!inputPath) {
    appendOpenFolderLog('[bad_request] missing path');
    res.status(400).json({ error: 'path is required' });
    return;
  }

  const resolvedInput = path.resolve(inputPath);
  if (!fs.existsSync(resolvedInput)) {
    appendOpenFolderLog(`[not_found] path=${resolvedInput}`);
    res.status(404).json({ error: `Path does not exist: ${resolvedInput}` });
    return;
  }

  const shouldReveal = !!reveal;
  let folderToOpen = resolvedInput;
  let isFileInput = false;
  try {
    const stat = fs.statSync(resolvedInput);
    isFileInput = stat.isFile();
    if (stat.isFile()) {
      folderToOpen = path.dirname(resolvedInput);
    }
  } catch {
    res.status(500).json({ error: `Failed to resolve path type: ${resolvedInput}` });
    return;
  }

  // Open folder in system file manager. Do not rely on process exit code:
  // on Windows explorer may open successfully but still report non-zero.
  const isWindows = process.platform === 'win32';
  const isMac = process.platform === 'darwin';
  const openTargetPath = shouldReveal && isFileInput ? resolvedInput : folderToOpen;
  const launchCandidates: LaunchSpec[] = [];

  if (isWindows) {
    const winFilePath = resolvedInput.replace(/\//g, '\\');
    const winFolderPath = folderToOpen.replace(/\//g, '\\');
    const psSelectArg = `/select,${winFilePath}`;

    if (shouldReveal && isFileInput) {
      launchCandidates.push({
        command: 'powershell.exe',
        args: [
          '-NoProfile',
          '-ExecutionPolicy',
          'Bypass',
          '-Command',
          `Start-Process -FilePath 'explorer.exe' -ArgumentList '${escapePsSingleQuoted(psSelectArg)}'`,
        ],
      });
    }

    launchCandidates.push({
      command: 'powershell.exe',
      args: [
        '-NoProfile',
        '-ExecutionPolicy',
        'Bypass',
        '-Command',
        `Start-Process -FilePath 'explorer.exe' -ArgumentList '${escapePsSingleQuoted(winFolderPath)}'`,
      ],
    });

    if (shouldReveal && isFileInput) {
      launchCandidates.push({
        command: 'cmd.exe',
        args: ['/d', '/s', '/c', `start "" explorer.exe /select,"${winFilePath}"`],
      });
    }

    launchCandidates.push({
      command: 'cmd.exe',
      args: ['/d', '/s', '/c', `start "" explorer.exe "${winFolderPath}"`],
    });
    launchCandidates.push({ command: 'explorer.exe', args: [winFolderPath] });
  } else if (isMac) {
    if (shouldReveal && isFileInput) {
      launchCandidates.push({ command: 'open', args: ['-R', resolvedInput] });
    }
    launchCandidates.push({ command: 'open', args: [folderToOpen] });
  } else {
    launchCandidates.push({ command: 'xdg-open', args: [folderToOpen] });
  }

  const errors: string[] = [];
  appendOpenFolderLog(`[request] path=${resolvedInput} reveal=${shouldReveal} isFile=${isFileInput}`);
  for (const candidate of launchCandidates) {
    try {
      await launchDetached(candidate.command, candidate.args);
      appendOpenFolderLog(`[ok] command=${candidate.command} args=${JSON.stringify(candidate.args)}`);
      res.json({ success: true, path: openTargetPath, reveal: shouldReveal && isFileInput });
      return;
    } catch (err: any) {
      const msg = `${candidate.command}: ${err?.message || 'unknown error'}`;
      errors.push(msg);
      appendOpenFolderLog(`[fail] ${msg}`);
    }
  }

  appendOpenFolderLog(`[error] ${errors.join(' | ') || 'no launch strategy succeeded'}`);
  res.status(500).json({
    error: `Failed to open folder: ${errors.join(' | ') || 'no launch strategy succeeded'}`,
  });
});

export default router;
