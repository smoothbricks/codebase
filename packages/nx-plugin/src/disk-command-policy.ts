import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { basename, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Root's fixed boot script cannot execute a user-writable cowshed binary. No image verbs belong here. */
export const PHYSICAL_BOOT_LAYER = 'packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos.rs';

/** Direct disk-image executors bypass cowshed's detached lifetime and can leak kernel devices. */
export function directDiskCommands(source: string, path = ''): number[] {
  // Preserve newlines for diagnostics; prose comments are not executable call sites.
  const code = source
    .replace(/\/\*[\s\S]*?\*\//g, (comment) => comment.replace(/[^\n]/g, ' '))
    .replace(/(?:"""[\s\S]*?"""|'''[\s\S]*?''')/g, (comment) => comment.replace(/[^\n]/g, ' '))
    .replace(/^[ \t]*(?:\/\/|#(?!!)).*$/gm, '');
  const tool = String.raw`(?:\/usr\/(?:s?bin)\/)?(?:hdiutil|diskutil)`;
  const quoted = `["']${tool}["']`;
  const aliases = [
    ...code.matchAll(new RegExp(String.raw`\b([A-Za-z_]\w*)\s*(?::[^=\n]+)?=\s*(?:\(?\s*\[\s*)?${quoted}`, 'g')),
  ]
    .map((match) => match[1])
    .filter((name): name is string => name !== undefined);
  const program = `(?:${quoted}|HDIUTIL|DISKUTIL${aliases.map((name) => `|${name}`).join('')})`;
  const patterns = [
    // Rust, Node, Python and argv-list launchers, including literal aliases.
    new RegExp(
      String.raw`\b(?:Command::new|spawn(?:Sync)?|execFile(?:Sync)?|exec(?:Sync)?|execa(?:Sync)?|Popen|check_call|check_output|subprocess\.run|sh|trial)\s*\(\s*(?:\{\s*cmd:\s*)?(?:&?\[\s*)?${program}(?=\s*[,\)\]])`,
      'g',
    ),
    new RegExp(String.raw`(?:^|\n|\$\(|\$\x60|\|)[ \t]*(?:sudo\s+)?${tool}(?=\s|$)`, 'g'),
    new RegExp(String.raw`HostCommand::new\s*\(\s*${program}\s*,\s*\[\s*"(?:image|eject|attach|detach|create)"`, 'g'),
  ];
  if (['.sh', '.bash', '.zsh', '.nix', '.yaml', '.yml'].includes(extname(path))) {
    patterns.push(new RegExp(String.raw`[;\x60&][ \t]*(?:sudo\s+)?${tool}(?=\s|$)`, 'g'));
  }
  if (['package.json', 'project.json'].includes(basename(path))) {
    patterns.push(new RegExp(String.raw`"[^"\n]+"\s*:\s*"${tool}(?=\s)`, 'g'));
  }
  const lines = new Set<number>();
  for (const pattern of patterns) {
    for (const match of code.matchAll(pattern)) {
      const start = match.index + match[0].search(/\S/);
      const line = code.slice(0, start).split('\n').length;
      const text = code.split('\n')[line - 1]?.trim() ?? '';
      if (
        path === PHYSICAL_BOOT_LAYER &&
        /^(?:info=\$\()?\/usr\/sbin\/diskutil info -plist "\$uuid"\)?$|^\| \/usr\/sbin\/diskutil apfs unlockVolume "\$uuid" -nomount -stdinpassphrase$|^\/usr\/sbin\/diskutil mount -nobrowse -mountPoint "\$mountpoint" "\$uuid"$/.test(
          text,
        )
      )
        continue;
      lines.add(line);
    }
  }
  return [...lines].sort((left, right) => left - right);
}

/** Scan tracked code, tests and scripts, never build output, prose or recorded benchmark data. */
export function checkDiskCommandBoundary(root: string): string[] {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git file listing whose length is its own read of this repository: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  const files = execFileSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard'], {
    cwd: root,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
  })
    .split('\0')
    .filter(Boolean);
  const extensions = [
    '.rs',
    '.ts',
    '.tsx',
    '.js',
    '.mjs',
    '.cjs',
    '.py',
    '.sh',
    '.bash',
    '.zsh',
    '.nix',
    '.yaml',
    '.yml',
  ];
  const failures: string[] = [];
  for (const path of files) {
    if (
      extname(path) !== '' &&
      !extensions.includes(extname(path)) &&
      !['package.json', 'project.json'].includes(basename(path))
    )
      continue;
    const source = readFileSync(resolve(root, path), 'utf8');
    for (const line of directDiskCommands(source, path)) {
      failures.push(`${path}:${line}: direct disk tool; use cowshed disk-command or SystemCommandRunner`);
    }
  }
  return failures;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const roots = process.argv.slice(2);
  if (roots.length === 0) roots.push(process.cwd());
  for (const root of roots) {
    const failures = checkDiskCommandBoundary(root);
    for (const failure of failures) console.error(`${root}/${failure}`);
    if (failures.length > 0) process.exitCode = 1;
    else console.log(`${root}: no direct disk-tool executions`);
  }
}
