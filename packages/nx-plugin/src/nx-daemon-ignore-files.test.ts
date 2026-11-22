import { expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { writeFileSync } from 'node:fs';
import { mkdir, readFile, symlink, utimes, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { isRunning, terminate } from './testing.js';

/**
 * Guards the daemon's ignore-file stop. Nx 23.2.1's daemon stopped itself on every watcher event for a `.gitignore` or
 * `.nxignore`, whatever the file then held. A rewrite with the same bytes (a checkout, a formatter) reports such an
 * event, and so does FSEvents for a write made shortly before the daemon's stream started, whenever fseventsd numbers it
 * after that start. Stopping destroys every client socket at once, so a file-watcher client lost the notification of
 * the batch that carried the event, and a dev watcher waiting to judge an edit in that batch waited for it forever.
 * This repository's patch stops the daemon only when an ignore file's bytes might differ from those its watch's filter
 * read (`daemon/server/restart-checks.js` on Nx 24.0.3). Nx 24's content-filtered batches leave a same-bytes rewrite
 * out, but its watch reads the ignore files once, as it starts and before its scan, so no batch shows a file rewritten
 * between the two, and the filter would keep the old rules.
 *
 * A file-watcher client of the fixture's daemon sees a same-bytes `.gitignore` rewrite and a tracked edit after it: the
 * edit arrives on the same connection and the same daemon still serves. A `.gitignore` with new bytes still stops it.
 *
 * A `.gitignore` rewritten after the daemon's walk read it must still stop the daemon: the filter keeps the old rules
 * and would drop every event under a path they no longer ignore.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const requireNx = createRequire(join(repositoryRoot, 'package.json'));
const daemonClient = requireNx.resolve('nx/src/daemon/client/client');
const daemonStart = requireNx.resolve('nx/src/daemon/server/start.js');
const { hashFile } = requireNx('nx/src/native');
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
  await withNxFixture('nx-daemon-ignore-', async ({ workspace, socketDirectory }) => {
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
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
});

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
  await withNxFixture('nx-daemon-ignore-gap-', async ({ workspace, socketDirectory }) => {
    await largeWorkspace(workspace);
    const env = {
      ...fixtureNxEnv(workspace, socketDirectory),
      NX_DAEMON: 'true',
      NX_NATIVE_LOGGING: 'nx::native::watch=debug',
    };
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
});
// Asks the workspace's daemon for its `.nxignore` hash, then reports the file as updated (UPDATE_WORKSPACE_CONTEXT, as
// smoo-nx-exec's ensureBuilt does for a file whose bytes differ from the daemon's table), then asks for the hash again.
const UPDATE_CLIENT = `
const { writeSync } = require('node:fs');
const { daemonClient } = require(process.env.NX_DAEMON_CLIENT);
const say = (...fields) => writeSync(1, fields.join('\\t') + '\\n');
const hashOf = async () =>
  (await daemonClient.getWorkspaceContextFileData()).find(({ file }) => file === '.nxignore')?.hash ?? 'absent';
(async () => {
  say('before', await hashOf());
  await daemonClient.updateWorkspaceContext([], ['.nxignore'], []);
  say('updated');
  say('after', await hashOf());
})().then(
  () => process.exit(0),
  (error) => {
    say('error', String(error?.message ?? error).split('\\n')[0]);
    process.exit(0);
  },
);
`;

/** Each line the update client printed, by its first field. */
async function explicitUpdate(workspace: string, env: Record<string, string>): Promise<Map<string, string>> {
  const child = spawn('node', ['-e', UPDATE_CLIENT], {
    cwd: workspace,
    env: { ...env, NX_DAEMON_START: 'never', NX_DAEMON_CLIENT: daemonClient },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let printed = '';
  child.stdout.setEncoding('utf8').on('data', (text: string) => {
    printed += text;
  });
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    printed += text;
  });
  await guardEvent(once(child, 'exit'), 'the explicit update client to exit', () => printed);
  const said = new Map<string, string>();
  for (const line of printed.split('\n')) {
    const [kind = '', ...rest] = line.split('\t');
    if (['before', 'updated', 'after', 'error'].includes(kind)) said.set(kind, rest.join('\t'));
  }
  return said;
}

/**
 * A workspace whose files archive proves an old hash for `.nxignore`: the archive a gather wrote over `adopted`, then
 * `current` written with that same whole-second mtime, as a build volume adopted from another checkout carries it. Nx's
 * native scan reuses an archived hash on an mtime match alone (`files_hashing.rs` `selective_files_hash`), so the
 * daemon's table holds `adopted`'s hash while its watch's filter reads `current`.
 */
async function adoptedStaleIgnore(workspace: string, env: Record<string, string>, adopted: string, current: string) {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-ignore-adopted', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
  const ignore = join(workspace, '.nxignore');
  const settled = new Date(Date.now() - 3_600_000);
  await writeFile(ignore, adopted);
  await utimes(ignore, settled, settled);
  // Daemonless: the command's own workspace context gathers the files and writes the archive.
  const gathered = spawnSync('bun', [nxEntry, 'show', 'projects'], { cwd: workspace, env, encoding: 'utf8' });
  expect(gathered.status, `${gathered.stdout}${gathered.stderr}`).toBe(0);
  await writeFile(ignore, current);
  await utimes(ignore, settled, settled);
}

it('answers an explicit update for an ignore file whose adopted table hash is stale, its filter holding the current bytes', async () => {
  await withNxFixture('nx-daemon-ignore-adopted-', async ({ workspace, socketDirectory }) => {
    const env = fixtureNxEnv(workspace, socketDirectory);
    await adoptedStaleIgnore(workspace, env, 'dist\n', 'dist\nout\n');
    const current = hashFile(join(workspace, '.nxignore'));
    const daemonEnv = { ...env, NX_DAEMON: 'true' };
    const started = spawnSync('bun', [nxEntry, 'daemon', '--start'], {
      cwd: workspace,
      env: daemonEnv,
      encoding: 'utf8',
    });
    expect(started.status, `${started.stdout}${started.stderr}`).toBe(0);
    const serving = await recordedDaemon(workspace);
    try {
      const said = await explicitUpdate(workspace, daemonEnv);
      // The fixture holds: the daemon's table proves the adopted bytes, not the ones on disk.
      expect(said.get('before'), JSON.stringify([...said])).not.toBe(current);
      // Unpatched by this repair, the update stops the daemon before it answers, its filter already holding these bytes.
      expect(said.has('updated'), JSON.stringify([...said])).toBe(true);
      expect(said.get('after')).toBe(current);
      expect(isRunning(serving)).toBe(true);
      expect(await recordedDaemon(workspace)).toBe(serving);

      // Control: rewritten with the same bytes after the daemon started, it still keeps serving.
      await writeFile(join(workspace, '.nxignore'), 'dist\nout\n');
      expect((await explicitUpdate(workspace, daemonEnv)).has('updated')).toBe(true);
      expect(isRunning(serving)).toBe(true);

      // Control: new bytes are rules the filter never read, so the daemon stops, as on a watch event.
      await writeFile(join(workspace, '.nxignore'), 'dist\nout\ngen\n');
      const stale = await explicitUpdate(workspace, daemonEnv);
      expect(stale.has('updated'), JSON.stringify([...stale])).toBe(false);
      const log = await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8');
      expect(log).toContain('Stopping the daemon the set of ignored files changed (native)');
    } finally {
      if (isRunning(serving)) terminate(serving);
    }
  });
});
