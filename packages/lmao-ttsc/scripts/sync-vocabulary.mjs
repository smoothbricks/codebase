#!/usr/bin/env node
import { createRequire } from 'node:module';
import { mkdtempSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const here = path.dirname(fileURLToPath(import.meta.url));
const pluginDir = path.resolve(here, '..', 'plugin');
const require_ = createRequire(import.meta.url);

let mode = 'check';
const forwarded = [];
for (const argument of process.argv.slice(2)) {
  if (argument === '--check') {
    mode = 'check';
  } else if (argument === '--write') {
    mode = 'sync-vocabulary';
  } else if (
    argument.startsWith('--cwd=') ||
    argument.startsWith('--tsconfig=') ||
    argument.startsWith('--plugins-json=') ||
    argument.startsWith('--lmao-vocabulary-manifest=')
  ) {
    forwarded.push(argument);
  } else {
    console.error(`lmao-vocabulary: unknown argument ${JSON.stringify(argument)}`);
    process.exit(2);
  }
}
if (!forwarded.some((argument) => argument.startsWith('--cwd='))) {
  forwarded.push(`--cwd=${process.cwd()}`);
}

const ttscRoot = path.dirname(require_.resolve('ttsc/package.json'));
const shimRoot = path.join(ttscRoot, 'shim');
const modules = [pluginDir, ttscRoot];
for (const entry of readdirSync(shimRoot)) {
  const candidate = path.join(shimRoot, entry);
  if (statSync(candidate).isDirectory()) modules.push(candidate);
}
for (const nested of ['cachedvfs', 'osvfs']) {
  modules.push(path.join(shimRoot, 'vfs', nested));
}
const workspaceDir = mkdtempSync(path.join(tmpdir(), 'lmao-vocabulary-'));
const goWork = [
  'go 1.26',
  '',
  'use (',
  ...modules.filter((candidate) => {
    try {
      return statSync(path.join(candidate, 'go.mod')).isFile();
    } catch {
      return false;
    }
  }).map((candidate) => `\t${candidate}`),
  ')',
  '',
  `replace github.com/samchon/ttsc/packages/ttsc v0.0.0 => ${ttscRoot}`,
  '',
].join('\n');
const goWorkPath = path.join(workspaceDir, 'go.work');
writeFileSync(goWorkPath, goWork);

let exitCode = 2;
try {
  const result = spawnSync('go', ['run', '.', mode, ...forwarded], {
    cwd: pluginDir,
    env: { ...process.env, GOWORK: goWorkPath },
    stdio: 'inherit',
  });
  if (result.error) {
    console.error(`lmao-vocabulary: ${result.error.message}`);
  } else {
    exitCode = result.status ?? 2;
  }
} finally {
  rmSync(workspaceDir, { recursive: true, force: true });
}
process.exitCode = exitCode;
