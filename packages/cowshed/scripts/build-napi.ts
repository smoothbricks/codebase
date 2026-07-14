/// <reference types="bun" />

import { copyFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';

const packageRoot = join(import.meta.dir, '..');
const artifact =
  process.platform === 'darwin'
    ? 'libcowshed_napi.dylib'
    : process.platform === 'win32'
      ? 'cowshed_napi.dll'
      : 'libcowshed_napi.so';

const build = Bun.spawn(['cargo', 'build', '--release', '-p', 'cowshed-napi'], {
  cwd: packageRoot,
  stdin: 'ignore',
  stdout: 'inherit',
  stderr: 'inherit',
});
const exitCode = await build.exited;
if (exitCode !== 0) {
  throw new Error(`cargo build -p cowshed-napi failed with exit code ${exitCode}`);
}

const dist = join(packageRoot, 'dist');
await mkdir(dist, { recursive: true });
await copyFile(join(packageRoot, 'target', 'release', artifact), join(dist, 'cowshed.node'));
