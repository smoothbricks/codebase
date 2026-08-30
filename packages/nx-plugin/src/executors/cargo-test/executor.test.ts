import { afterEach, describe, expect, it } from 'bun:test';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { CARGO_TEST_BINS_DIR, CARGO_TEST_MANIFEST, type CargoTestManifest } from './artifacts.js';
import { runCargoTest } from './executor.js';

const workspaces: string[] = [];

describe('@smoothbricks/nx-plugin:cargo-test', () => {
  afterEach(async () => {
    await Promise.all(workspaces.splice(0).map((workspace) => rm(workspace, { recursive: true, force: true })));
  });

  it('runs staged binaries in parallel and kills a hung binary without failing the others', async () => {
    // Real clock and real processes: the run phase's contract is per-binary
    // SIGTERM/SIGKILL plus continuing the rest of the pool.
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-cargo-test-'));
    workspaces.push(root);
    const staging = join(root, CARGO_TEST_BINS_DIR);
    await mkdir(staging, { recursive: true });
    await writeExecutable(join(staging, 'ok-bin'), 'process.exit(0);\n');
    await writeExecutable(join(staging, 'hang-bin'), 'setTimeout(() => {}, 30_000);\n');
    const manifest: CargoTestManifest = {
      binaries: [
        { name: 'ok', fileName: 'ok-bin', kind: ['test'], sourcePath: join(staging, 'ok-bin') },
        { name: 'hang', fileName: 'hang-bin', kind: ['test'], sourcePath: join(staging, 'hang-bin') },
      ],
    };
    await writeFile(join(staging, CARGO_TEST_MANIFEST), `${JSON.stringify(manifest)}\n`);

    const started = Date.now();
    const result = await runCargoTest({ phase: 'run', cwd: root, timeoutMs: 80, killAfterMs: 20, jobs: 2 }, { root });
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('hang: timed out after 80ms');
  });
});

async function writeExecutable(path: string, body: string): Promise<void> {
  await writeFile(path, `#!/usr/bin/env node\n${body}`);
  await chmod(path, 0o755);
}
