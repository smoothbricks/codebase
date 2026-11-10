/**
 * The `ttsc-plugin-hosts` target's command (package.json): builds, through ttsc's own `prepare`, every ttsc plugin host
 * the Bun shards load into the `TTSC_CACHE_DIR` the target names, its output: this package's own (the Bun preload
 * transforms the suite through its configs) and those of the TypeScript emit executor's fixtures
 * (`PLUGIN_HOST_FIXTURES`). The shards depend on the target and name the same directory, so they only load built hosts
 * and never compile one inside their run (rule ttsc-build-own-task). ttsc names each host by a hash of what it is built
 * from and skips a host it already holds.
 */
import './ttsc-cache-dir.js';
import { spawnSync } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { PLUGIN_HOST_FIXTURES } from './typescript-emit-fixtures.js';

function prepare(host: string, cwd: string, tsconfig: string): void {
  const prepared = spawnSync('ttsc', ['prepare', '-p', tsconfig], { cwd, stdio: 'inherit' });
  if (prepared.error) throw prepared.error;
  if (prepared.status !== 0) {
    throw new Error(`ttsc prepare of ${host} exited ${String(prepared.status ?? prepared.signal)}`);
  }
}

for (const tsconfig of ['tsconfig.test.json', 'tsconfig.lib.json']) {
  prepare(`this package's ${tsconfig}`, process.cwd(), tsconfig);
}
for (const [host, create] of Object.entries(PLUGIN_HOST_FIXTURES)) {
  const root = await create();
  try {
    prepare(`the ${host} host`, root, 'tsconfig.lib.json');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
