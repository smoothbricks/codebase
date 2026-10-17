import { expect, it } from 'bun:test';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { promisify } from 'node:util';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

const run = promisify(execFile);
const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');

async function cpuSeconds(pid: number): Promise<number> {
  const { stdout } = await run('ps', ['-p', String(pid), '-o', 'time=']);
  const fields = stdout.trim().split(':');
  if (fields.length < 2 || fields.some((field) => !/^\d+(?:\.\d+)?$/.test(field))) {
    throw new Error(`could not read CPU time for owned daemon ${pid}: ${stdout}`);
  }
  return fields.reduce((seconds, field) => seconds * 60 + Number(field), 0);
}

// A host process-census proof, explicitly opted into through a focused Nx run.
// Keep the 30-second measurement out of ordinary bounded test shards.
it.skipIf(process.env.NX_DAEMON_CPU_PROOF !== 'true')(
  'measures the installed Nx daemon idle CPU without profiling or source writes',
  async () => {
    await withNxFixture('nx-idle-cpu-', async ({ workspace, socketDirectory }) => {
      await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, 'nx.json'), '{}\n');
      await writeFile(join(workspace, 'package.json'), '{"name":"idle-cpu-proof","private":true}\n');
      await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\n');
      // Use this checkout's real lockfile size, not a tiny synthetic stand-in.
      const lock = await readFile(join(repositoryRoot, 'bun.lock'));
      await writeFile(join(workspace, 'bun.lock'), lock);
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
      await run('node', [nxEntry, 'daemon', '--start'], { cwd: workspace, env });
      await run('node', [nxEntry, 'show', 'projects', '--json'], { cwd: workspace, env });
      const recordPath = join(workspace, '.nx/workspace-data/d/server-process.json');
      const record: unknown = JSON.parse(await readFile(recordPath, 'utf8'));
      if (
        typeof record !== 'object' ||
        record === null ||
        !('processId' in record) ||
        typeof record.processId !== 'number'
      ) {
        throw new Error(`owned fixture has no daemon process id: ${JSON.stringify(record)}`);
      }
      const pid = record.processId;
      const before = await cpuSeconds(pid);
      const started = performance.now();
      await sleep(30_000);
      const after = await cpuSeconds(pid);
      const wallSeconds = (performance.now() - started) / 1000;
      const cpu = after - before;
      const sourcePath = join(
        dirname(createRequire(import.meta.url).resolve('nx/package.json')),
        'dist/src/daemon/server/server.js',
      );
      const source = await readFile(sourcePath);
      console.log(
        JSON.stringify({
          proof: 'nx-daemon-idle-cpu',
          pid,
          wallSeconds,
          cpuSeconds: cpu,
          corePercent: (100 * cpu) / wallSeconds,
          lockBytes: lock.length,
          implementationSha256: createHash('sha256').update(source).digest('hex'),
          intervalLog: (await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8'))
            .split('\n')
            .find((line) => line.includes('interval (')),
        }),
      );
      expect(cpu).toBeGreaterThanOrEqual(0);
      expect(wallSeconds).toBeGreaterThanOrEqual(30);
    });
  },
);
