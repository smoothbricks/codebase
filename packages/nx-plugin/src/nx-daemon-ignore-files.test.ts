import { expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { isRunning, terminate } from './testing.js';

/**
 * Guards the ignore-file hunk of `patches/nx@23.2.1.patch`. Nx 23.2.1's daemon stopped itself on every watcher event
 * for a `.gitignore` or `.nxignore`, whatever the file then held. A rewrite with the same bytes (a checkout, a
 * formatter) reports such an event, and so does FSEvents for a write made shortly before the daemon's stream started,
 * whenever fseventsd numbers it after that start. Stopping destroys every client socket at once, so a file-watcher
 * client lost the notification of the batch that carried the event, and a dev watcher waiting to judge an edit in that
 * batch waited for it forever. The patched daemon stops only when an ignore file's bytes may differ from those its
 * watcher's filter read.
 *
 * A file-watcher client of the fixture's daemon sees a same-bytes `.gitignore` rewrite and a tracked edit after it.
 * Unpatched, the daemon closes the connection instead of notifying the edit; patched, the edit arrives on the same
 * connection and the same daemon still serves. A `.gitignore` with new bytes still stops it.
 *
 * The filter reads its ignore files in `watch()`; the workspace context the daemon compares against reads them later.
 * A `.gitignore` rewritten between the two reads must still stop the daemon, though the context holds its new bytes:
 * the filter keeps the old rules and would drop every event under a path they no longer ignore.
 *
 * Drop the hunk, and keep this test, once the Nx version in use compares an ignore file's bytes before it stops.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const requireNx = createRequire(join(repositoryRoot, 'package.json'));
const daemonClient = requireNx.resolve('nx/src/daemon/client/client');
const daemonStart = requireNx.resolve('nx/src/daemon/server/start.js');
const IGNORE = 'dist\n';

/**
 * One line per event the client saw, its kind then its tab-separated details: `ready` once registered, `changed` and
 * the notified files, `reconnecting` or `closed` at the connection's end, `error` and what failed.
 */
interface WatchEvent {
  readonly kind: string;
  readonly details: readonly string[];
}

// The client ends at the connection's end, before Nx's own reconnect could start another daemon.
const CLIENT = `
const { writeSync } = require('node:fs');
const { daemonClient } = require(process.env.NX_DAEMON_CLIENT);
const say = (...fields) => writeSync(1, fields.join('\\t') + '\\n');
daemonClient
  .registerFileWatcher({ watchProjects: 'all', includeGlobalWorkspaceFiles: true }, (error, data) => {
    if (error === 'reconnecting' || error === 'closed') {
      say(error);
      process.exit(0);
    }
    if (error !== null) {
      say('error', String(error));
      process.exit(1);
    }
    say('changed', ...data.changedFiles.map(({ path }) => path));
  })
  .then(() => say('ready'), (error) => {
    say('error', String(error));
    process.exit(1);
  });
`;

async function recordedDaemon(workspace: string): Promise<number> {
  const record: unknown = JSON.parse(
    await readFile(join(workspace, '.nx/workspace-data/d/server-process.json'), 'utf8'),
  );
  if (typeof record !== 'object' || record === null || !('processId' in record)) {
    throw new Error(`the fixture daemon record names no process: ${JSON.stringify(record)}`);
  }
  return Number(record.processId);
}

/** A file-watcher client of the workspace's daemon, read one event at a time. */
function watchClient(workspace: string, env: Record<string, string>) {
  const child = spawn('node', ['-e', CLIENT], {
    cwd: workspace,
    env: { ...env, NX_DAEMON_CLIENT: daemonClient },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  const seen: WatchEvent[] = [];
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  const exited = new Promise<number | null>((resolve) => child.once('exit', resolve));
  return {
    seen,
    /** The next event, or an error once the client has said all it will. */
    async next(what: string): Promise<WatchEvent> {
      const line = await guardEvent(lines.next(), what, () => `events so far ${JSON.stringify(seen)}\n${stderr}`);
      if (line.done) throw new Error(`the watch client ended before ${what}: ${JSON.stringify(seen)}\n${stderr}`);
      const [kind = '', ...details] = line.value.split('\t');
      const event = { kind, details };
      seen.push(event);
      return event;
    },
    async end(): Promise<void> {
      if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) terminate(child.pid);
      await exited;
    },
  };
}

it('keeps serving when a watched ignore file is rewritten with the bytes the daemon started with', async () => {
  await withNxFixture('nx-daemon-ignore-', async ({ workspace }) => {
    const env = { ...fixtureNxEnv(workspace), NX_DAEMON: 'true' };
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-ignore', private: true }));
    await writeFile(join(workspace, '.gitignore'), IGNORE);
    await mkdir(join(workspace, 'p'));
    await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));

    const started = spawnSync('bun', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, `${started.stdout}${started.stderr}`).toBe(0);
    const serving = await recordedDaemon(workspace);

    const client = watchClient(workspace, env);
    try {
      expect(await client.next('the file watcher to register')).toEqual({ kind: 'ready', details: [] });

      // The same bytes, then a tracked edit: the edit's notification arrives on this connection.
      await writeFile(join(workspace, '.gitignore'), IGNORE);
      await writeFile(join(workspace, 'p', 'a.txt'), 'a\n');
      for (;;) {
        const event = await client.next('the tracked edit after a same-bytes .gitignore rewrite');
        expect(
          event.kind,
          `the daemon closed its watcher on unchanged ignore bytes: ${JSON.stringify(client.seen)}`,
        ).toBe('changed');
        if (event.details.includes('p/a.txt')) break;
      }
      expect(await recordedDaemon(workspace)).toBe(serving);
      expect(isRunning(serving)).toBe(true);

      // Control: new bytes still change the daemon's ignore rules, so it stops and closes the connection.
      await writeFile(join(workspace, '.gitignore'), `${IGNORE}out\n`);
      await writeFile(join(workspace, 'p', 'b.txt'), 'b\n');
      for (;;) {
        const event = await client.next('the daemon to stop for a .gitignore with new bytes');
        if (event.kind === 'reconnecting') break;
        expect(event.kind, JSON.stringify(client.seen)).toBe('changed');
      }
    } finally {
      await client.end();
    }
  });
}, 60_000);

/** A workspace whose context walk takes long enough that a write right after `watch()` lands before it. */
async function largeWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-ignore-gap', private: true }));
  for (let directory = 0; directory < 100; directory += 1) {
    const files = join(workspace, 'files', `d${directory}`);
    await mkdir(files, { recursive: true });
    await Promise.all(Array.from({ length: 200 }, (_, file) => writeFile(join(files, `f${file}.txt`), `${file}\n`)));
  }
  await mkdir(join(workspace, 'z'));
  await writeFile(join(workspace, 'z', '.gitignore'), 'gen/\n');
}

it('stops when an ignore file changes after the watcher read it and before the daemon compared it', async () => {
  await withNxFixture('nx-daemon-ignore-gap-', async ({ workspace }) => {
    await largeWorkspace(workspace);
    const env = { ...fixtureNxEnv(workspace), NX_DAEMON: 'true', NX_NATIVE_LOGGING: 'nx::native::watch=debug' };
    // The native watcher logs `watching started` once watch() has read the ignore files and started its stream; the
    // daemon sets its workspace context up, walking every file, only after that. The rewrite lands in between.
    const daemon = spawn(process.execPath, [daemonStart], { cwd: workspace, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    let rewritten = false;
    const read = (text: string) => {
      output += text;
      if (!rewritten && output.includes('watching started')) {
        rewritten = true;
        writeFileSync(join(workspace, 'z', '.gitignore'), '');
      }
    };
    daemon.stdout.setEncoding('utf8').on('data', read);
    daemon.stderr.setEncoding('utf8').on('data', read);
    const exited = new Promise<number | null>((resolve) => daemon.once('exit', resolve));
    try {
      // Unpatched by this repair, the event for the rewrite compares equal to the context's bytes and the daemon
      // serves on with the filter's stale rules.
      const code = await guardEvent(exited, 'the daemon to stop for the rewritten .gitignore', () => output);
      expect(rewritten, output).toBe(true);
      expect(code, output).toBe(0);
      expect(output).toContain('Stopping the daemon the set of ignored files changed (native)');
    } finally {
      if (daemon.pid !== undefined && daemon.exitCode === null && daemon.signalCode === null) {
        terminate(daemon.pid);
        await exited;
      }
    }
  });
}, 60_000);
