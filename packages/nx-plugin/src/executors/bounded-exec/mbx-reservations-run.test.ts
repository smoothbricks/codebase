import { afterAll, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from '../../__tests__/fixture-nx-env.js';
import { testFileInstallation } from '../../__tests__/owned-installation.js';
import { awaitExit, isRunning, processesOf } from '../../testing.js';

/**
 * An Nx run's mbx reservation endpoint lives as long as the run's client (specs/cowshed/05_gateway.md, "An Nx run
 * serves its leaves one reservation endpoint"; 14_nix.md: "a child of that run that ends with it"). A real Nx runs the
 * compiled plugin against a fixture under a daemon, the daemon dies while the run's first reserved leaf holds its
 * grant, and the run's next reserved leaf must still be granted by the endpoint the run started with. Stock Nx ran the
 * plugin's task hooks in the daemon, so the endpoint was the daemon's child and ended with it: every later leaf of a
 * land gate failed with `connect ENOENT …/mbx-r-*\/r.sock`. The test imports nothing of the plugin's source: it loads
 * only the compiled plugin, through Nx.
 */

/** The Nx CLI of a fixture workspace's own installation. */
const nxEntry = (workspace: string) => join(workspace, 'node_modules', '.bin', 'nx');
/** A leaf's whole-command bound, past any run of this test: each leaf runs only as long as the test paces it. */
const LEAF_TIMEOUT_MS = 6 * 60 * 60 * 1000;

/** The Nx and the plugin under test, one copy this file's fixtures share; none may write into it. */
const installation = testFileInstallation();
afterAll(() => installation.retire());

/**
 * `mbx reservations <socket>` at its command line: it records its pid and every reserve it hears, grants each one
 * core, and ends when its standard input ends, as mbx's does. Any other mbx command is refused.
 */
const standInMbx = (endpoints: string, heard: string) => `#!/usr/bin/env node
const { appendFileSync } = require('node:fs');
const { createServer } = require('node:net');
const [verb, socket] = process.argv.slice(2);
if (verb !== 'reservations') {
  process.stderr.write('stand-in mbx: only reservations is served, not ' + verb + '\\n');
  process.exit(97);
}
appendFileSync(${JSON.stringify(endpoints)}, process.pid + '\\n');
const server = createServer((client) => {
  let text = '';
  client.setEncoding('utf8');
  client.on('error', () => client.destroy());
  client.on('data', (chunk) => {
    text += chunk;
    for (let end = text.indexOf('\\n'); end !== -1; end = text.indexOf('\\n')) {
      const line = JSON.parse(text.slice(0, end));
      text = text.slice(end + 1);
      if (line.type !== 'reserve') continue;
      appendFileSync(${JSON.stringify(heard)}, line.cpus + '\\n');
      client.write(JSON.stringify({ type: 'granted', id: line.id, cpus: 1 }) + '\\n');
    }
  });
});
server.listen(socket, () => process.stdout.write('listening\\n'));
process.stdin.on('end', () => {
  server.close();
  process.exit(0);
});
process.stdin.resume();
`;

/** The pid the fixture's daemon record names; a start that left none fails with what it printed and logged. */
async function recordedDaemon(workspace: string, printed: string): Promise<number> {
  const data = join(workspace, '.nx/workspace-data/d');
  const text = await readFile(join(data, 'server-process.json'), 'utf8').catch(async (error: unknown) => {
    const log = await readFile(join(data, 'daemon.log'), 'utf8').catch((logError: unknown) => String(logError));
    throw new Error(`the daemon start left no record (${String(error)})\n${printed}\ndaemon.log:\n${log}`);
  });
  const record: unknown = JSON.parse(text);
  if (
    typeof record !== 'object' ||
    record === null ||
    !('processId' in record) ||
    typeof record.processId !== 'number'
  ) {
    throw new Error(`the daemon record names no process: ${JSON.stringify(record)}`);
  }
  return record.processId;
}

/** Lines of a file one stand-in or leaf appended to. */
async function lines(path: string): Promise<string[]> {
  return (await readFile(path, 'utf8')).split('\n').filter((line) => line !== '');
}

it("a run's reservation endpoint outlives the daemon that served the run, and ends with the run", async () => {
  await withNxFixture(
    'mbx-reservations-run',
    async ({ root, workspace, socketDirectory }) => {
      const bin = join(root, 'stand-in-mbx');
      const endpoints = join(root, 'endpoints');
      const heard = join(root, 'heard');
      // Two pipes pace the first leaf without a clock: it says its grant on one once it holds it, and waits on the
      // other until the daemon is gone.
      const granted = join(root, 'granted.fifo');
      const go = join(root, 'go.fifo');
      await mkdir(bin);
      await writeFile(join(bin, 'mbx'), standInMbx(endpoints, heard), { mode: 0o755 });
      for (const fifo of [granted, go]) {
        const made = spawnSync('mkfifo', [fifo], { encoding: 'utf8' });
        expect(made.status, `mkfifo ${fifo}: ${made.stderr}`).toBe(0);
      }
      const git = spawnSync('git', ['init', '--quiet', workspace], { encoding: 'utf8' });
      expect(git.status, git.stderr).toBe(0);
      await symlink(await installation.nodeModules(), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'mbx-reservations-run', private: true }));
      await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['@smoothbricks/nx-plugin'] }));
      const leaf = (command: string) => ({
        executor: '@smoothbricks/nx-plugin:bounded-exec',
        options: { command, timeoutMs: LEAF_TIMEOUT_MS },
        outputs: ['{projectRoot}/dist/'],
        cache: true,
      });
      await mkdir(join(workspace, 'p'));
      await writeFile(
        join(workspace, 'p', 'project.json'),
        JSON.stringify({
          name: 'p',
          targets: {
            build: leaf(
              `printf '%s\\n' "$MBX_RESERVE_CPUS" > ${granted} && read go < ${go} && mkdir -p p/dist && printf '%s' "$MBX_RESERVE_CPUS" > p/dist/p.txt`,
            ),
          },
        }),
      );
      // q:build reads p:build's outputs, so it is hashed, and reserved, only after p:build ended: after the daemon.
      await mkdir(join(workspace, 'q'));
      await writeFile(
        join(workspace, 'q', 'project.json'),
        JSON.stringify({
          name: 'q',
          implicitDependencies: ['p'],
          targets: {
            build: {
              ...leaf(`mkdir -p q/dist && printf '%s' "$MBX_RESERVE_CPUS" > q/dist/q.txt`),
              dependsOn: ['^build'],
              inputs: ['default', { dependentTasksOutputFiles: '**/*.txt' }],
            },
          },
        }),
      );

      // mbx's pool, with the stand-in first on PATH, in the daemon's environment and the run's alike.
      const env = {
        ...fixtureNxEnv(workspace, socketDirectory),
        NX_DAEMON: 'true',
        PATH: `${bin}:${process.env.PATH ?? ''}`,
        MBX_SCHEDULER: '1',
        MBX_CACHE_DIR: join(root, 'mbx-cache'),
      };
      const started = spawnSync(process.execPath, [nxEntry(workspace), 'show', 'projects', '--json'], {
        cwd: workspace,
        env,
        encoding: 'utf8',
      });
      expect(started.status, `starting the daemon: ${started.stdout}${started.stderr}`).toBe(0);
      const daemon = await recordedDaemon(workspace, `${started.stdout}${started.stderr}`);

      // The run is a job's: it uses the running daemon and never starts one.
      const run = spawn(process.execPath, [nxEntry(workspace), 'run-many', '-t', 'build', '--outputStyle=static'], {
        cwd: workspace,
        env: { ...env, NX_DAEMON_START: 'never' },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let printed = '';
      run.stdout.setEncoding('utf8').on('data', (text: string) => (printed += text));
      run.stderr.setEncoding('utf8').on('data', (text: string) => (printed += text));
      const exited = once(run, 'close');
      const before = <T>(step: Promise<T>, what: string) =>
        Promise.race([
          step,
          exited.then(() => {
            throw new Error(`the run ended before ${what}: ${printed}`);
          }),
        ]);

      // p:build holds its grant; the daemon that served the run's graph and hooks dies under it.
      expect(await before(readFile(granted, 'utf8'), 'p:build was granted'), printed).toBe('1\n');
      const killed = await processesOf([daemon]);
      process.kill(daemon, 'SIGKILL');
      await awaitExit(killed, 'the killed fixture daemon');
      await before(writeFile(go, 'go\n'), 'p:build read that the daemon is gone');

      const [code] = await exited;
      expect(code, `the run: ${printed}`).toBe(0);
      expect(printed, 'the daemon stopped under the run').toContain('Continuing without the daemon.');
      // Both leaves were granted by the run's one endpoint, q:build after the daemon was gone.
      expect(await readFile(join(workspace, 'p/dist/p.txt'), 'utf8'), printed).toBe('1');
      expect(await readFile(join(workspace, 'q/dist/q.txt'), 'utf8'), printed).toBe('1');
      expect(await lines(heard), 'reserves the endpoint heard').toHaveLength(2);
      // One endpoint for the run, and it ended with the run.
      const served = (await lines(endpoints)).map(Number);
      expect(served, 'endpoints started').toHaveLength(1);
      expect(isRunning(served[0] ?? 0), `endpoint ${served[0]} outlived its run`).toBe(false);
    },
    'workspace',
  );
});
