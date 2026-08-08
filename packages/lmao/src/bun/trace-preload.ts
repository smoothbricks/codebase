/**
 * Bun test tracing preload.
 *
 * Registers LMAO SQLite-backed test tracing after the Typia preload has already
 * been registered in bunfig.
 */

import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { autoSetupBunTestTracing } from '../lib/testing/bun-harness.js';

function findWorkspacePackagesDir(start: string): string | null {
  let directory = resolve(start);
  while (true) {
    const packagesDir = join(directory, 'packages');
    if (existsSync(join(packagesDir, 'lmao', 'package.json'))) {
      return packagesDir;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return null;
    }
    directory = parent;
  }
}

const packagesDir = findWorkspacePackagesDir(process.cwd());
if (packagesDir) {
  await autoSetupBunTestTracing({ packagesDir });
}
