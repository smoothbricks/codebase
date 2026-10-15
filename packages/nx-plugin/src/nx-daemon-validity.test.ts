import { expect, it } from 'bun:test';
import { type ChildProcess, spawn, spawnSync } from 'node:child_process';
import { watch as fsWatch } from 'node:fs';
import { mkdir, readdir, readFile, rename, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { createConnection, createServer } from 'node:net';
import { dirname, join } from 'node:path';
import { createInterface } from 'node:readline';
import { sendMessage } from 'nx/src/daemon/socket-utils.js';
import { consumeMessagesFromSocket, parseMessage } from 'nx/src/utils/consume-messages-from-socket.js';
import typia from 'typia';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { awaitExit, isRunning, type ProcessEntry, processTable, terminate } from './testing.js';

/**
 * Guards the event-driven validity hunk of `patches/nx@23.2.1.patch` (upstream nrwl/nx#37113, fixing nrwl/nx#37112).
 * The installed daemon judged its root lockfiles and installed nx version on a 20 ms timer. These regressions preserve
 * the existing socket and process ownership guards while moving lockfile checks to applied watcher events, request
 * boundaries, and a 5 s safety poll for events the watcher ignores or drops.
 *
 * Each test runs a real daemon in a fixture workspace and asserts only what a client, a watcher, or the filesystem can
 * observe. Restarts are awaited on the record directory, and readiness on a PING answer after a change in the socket
 * directory. The one real-clock wait is the idle observation window in the last test.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const requireNx = createRequire(join(repositoryRoot, 'package.json'));
const daemonClient = requireNx.resolve('nx/src/daemon/client/client');
const daemonStart = requireNx.resolve('nx/src/daemon/server/start.js');
const daemonServer = join(dirname(daemonStart), 'server.js');

/** The root lockfiles the daemon validates. */
const LOCKFILES = ['package-lock.json', 'yarn.lock', 'pnpm-lock.yaml', 'bun.lockb', 'bun.lock'] as const;
/** The fixture's workspace directory under its root, so installed-package copies sit outside the workspace. */
const FIXTURE_WORKSPACE = 'workspace';
/** The idle observation window, longer than the 5 s safety poll so one poll would fall inside it. */
const OBSERVATION_MS = 6_000;

interface DaemonRecord {
  readonly processId: number;
  readonly socketPath: string;
}

interface Daemon {
  readonly record: DaemonRecord;
  readonly entry: ProcessEntry;
}

interface Manifest {
  readonly version: string;
  readonly [field: string]: unknown;
}

interface DaemonRequest {
  readonly type: 'PING' | 'REQUEST_PROJECT_GRAPH';
  readonly workspaceRoot: string;
}

/** A served answer, or the reason the daemon did not answer on this connection. */
type Answer =
  | { readonly answered: true; readonly message: unknown }
  | { readonly answered: false; readonly reason: string };

interface RawClient {
  request(message: DaemonRequest, what: string): Promise<Answer>;
  close(): void;
}

interface Listener {
  readonly pid: number;
  stop(): Promise<void>;
}

interface Replacement {
  readonly workspace: string;
  /** The pid the record must stop naming; absent when no daemon has published a record yet. */
  readonly previous?: number;
  /** Runs once the directory watch is open. */
  readonly action?: () => Promise<void>;
  readonly what: string;
  readonly timeoutMs?: number;
}

/** One line per event the watch client saw: its kind, then the tab-separated details. */
interface WatchEvent {
  readonly kind: string;
  readonly details: readonly string[];
}

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

/**
 * A process that stands in as a successor: it listens on the socket path it is given, and reports so on stdout. The
 * listening server keeps the process alive.
 */
const LISTENER = [
  "const { createServer } = require('node:net');",
  "createServer(() => {}).listen(process.argv[1], () => process.stdout.write('listening\\n'));",
].join('\n');

/**
 * Preloaded into the daemon. It records each call that reads a lockfile by name, the record file, or a socket, through
 * the fs functions the old 20 ms loop used. It writes with `writeSync` on a descriptor opened before it wraps anything.
 */
const PROBE = `
const fs = require('node:fs');
const path = require('node:path');
const locks = ${JSON.stringify(Object.fromEntries(LOCKFILES.map((name) => [name, true])))};
const sink = fs.openSync(process.env.NX_VALIDITY_LOG, 'a');
const write = (line) => fs.writeSync(sink, line + '\\n');
const classify = (file) => {
  if (typeof file !== 'string') return undefined;
  const name = path.basename(file);
  if (locks[name] === true) return 'lock';
  if (name === 'server-process.json') return 'record';
  if (name.endsWith('.sock')) return 'socket';
  return undefined;
};
write('attached');
for (const name of ['existsSync', 'statSync', 'lstatSync', 'readFileSync', 'openSync']) {
  const original = fs[name];
  fs[name] = function (file, ...rest) {
    const kind = classify(file);
    if (kind !== undefined) write(kind + ' ' + name + ' ' + file);
    return original.call(fs, file, ...rest);
  };
}
`;

function recordPath(workspace: string): string {
  return join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json');
}

/** A missing path (ENOENT) or a socket nothing listens on (ECONNREFUSED): the record or the daemon is absent. */
function isAbsent(error: unknown): boolean {
  if (typeof error !== 'object' || error === null || !('code' in error)) return false;
  return error.code === 'ENOENT' || error.code === 'ECONNREFUSED';
}

function gone(reason: string): Answer {
  return { answered: false, reason };
}

function ping(workspace: string): DaemonRequest {
  return { type: 'PING', workspaceRoot: workspace };
}

async function daemonRecord(workspace: string): Promise<DaemonRecord | null> {
  let text: string;
  try {
    text = await readFile(recordPath(workspace), 'utf8');
  } catch (error) {
    if (isAbsent(error)) return null;
    throw error;
  }
  return typia.json.assertParse<DaemonRecord>(text);
}

async function recordedPid(workspace: string): Promise<number> {
  const record = await daemonRecord(workspace);
  if (record === null) throw new Error(`no daemon record at ${recordPath(workspace)}`);
  return record.processId;
}

async function installedManifest(): Promise<Manifest> {
  return typia.json.assertParse<Manifest>(
    await readFile(join(repositoryRoot, 'node_modules', 'nx', 'package.json'), 'utf8'),
  );
}

function nx(workspace: string, env: Record<string, string>, ...args: string[]): string {
  const result = spawnSync('bun', [nxEntry, ...args], { cwd: workspace, env, encoding: 'utf8' });
  expect(result.status, `nx ${args.join(' ')} failed:\n${result.stdout}\n${result.stderr}`).toBe(0);
  return result.stdout;
}

/** A fixture-owned `node_modules`: every entry links to the repository's, except `nx`, which links to `nxPackage`. */
async function linkedModules(workspace: string, nxPackage: string): Promise<void> {
  const modules = join(workspace, 'node_modules');
  await mkdir(modules);
  for (const entry of await readdir(join(repositoryRoot, 'node_modules'))) {
    if (entry !== 'nx') await symlink(join(repositoryRoot, 'node_modules', entry), join(modules, entry), 'dir');
  }
  await symlink(nxPackage, join(modules, 'nx'), 'dir');
}

/** A fixture-owned copy of the installed `nx` package, with `version` in its manifest and the repository's `dist`. */
async function nxPackageCopy(directory: string, version: string): Promise<void> {
  await mkdir(directory);
  const manifest = await installedManifest();
  await writeFile(join(directory, 'package.json'), JSON.stringify({ ...manifest, version }));
  await symlink(join(repositoryRoot, 'node_modules', 'nx', 'dist'), join(directory, 'dist'), 'dir');
}

/** A project in the workspace; `node_modules` is the repository's, or a fixture-owned copy when `nxPackage` is given. */
async function fixtureWorkspace(workspace: string, nxPackage?: string): Promise<void> {
  if (nxPackage === undefined) {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  } else {
    await linkedModules(workspace, nxPackage);
  }
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), '{"name":"fixture","private":true}\n');
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), '{"name":"p"}\n');
}

async function processEntry(pid: number): Promise<ProcessEntry> {
  const entry = (await processTable()).find((candidate) => candidate.pid === pid);
  if (entry === undefined) throw new Error(`process ${pid} is not in the process table`);
  return entry;
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = Promise.withResolvers<void>();
  child.once('exit', () => exited.resolve());
  if (child.pid !== undefined) terminate(child.pid);
  await exited.promise;
}

/** A raw IPC connection that stays open, so one client can send across a lockfile change. */
function openClient(socketPath: string): Promise<RawClient> {
  const socket = createConnection(socketPath);
  const opened = Promise.withResolvers<RawClient>();
  let waiting: ((answer: Answer) => void) | undefined;
  const settle = (answer: Answer): void => {
    const resolve = waiting;
    waiting = undefined;
    resolve?.(answer);
  };
  socket.on(
    'data',
    consumeMessagesFromSocket(
      (message) => settle({ answered: true, message: parseMessage(message) }),
      (error) => settle(gone(String(error))),
    ),
  );
  socket.once('error', (error) => settle(gone(error.message)));
  socket.once('close', () => settle(gone('the daemon closed the connection')));
  socket.once('connect', () =>
    opened.resolve({
      request(message, what) {
        if (socket.destroyed) return Promise.resolve(gone('the connection was already closed'));
        const answer = Promise.withResolvers<Answer>();
        waiting = answer.resolve;
        sendMessage(socket, message, 'json');
        return guardEvent(answer.promise, what, () => `socket ${socketPath}`);
      },
      close() {
        socket.destroy();
      },
    }),
  );
  socket.once('error', opened.reject);
  return opened.promise;
}

/** One PING or request on a fresh connection. A daemon that is gone answers `gone`. */
async function requestOnce(socketPath: string, request: DaemonRequest, what: string): Promise<Answer> {
  let client: RawClient;
  try {
    client = await openClient(socketPath);
  } catch (error) {
    if (isAbsent(error)) return gone(`nothing listens on ${socketPath}: ${String(error)}`);
    throw error;
  }
  try {
    return await client.request(request, what);
  } finally {
    client.close();
  }
}

/** Whether a process accepts connections on `socketPath`. */
async function acceptsConnections(socketPath: string): Promise<boolean> {
  try {
    (await openClient(socketPath)).close();
    return true;
  } catch (error) {
    if (isAbsent(error)) return false;
    throw error;
  }
}

/** A served answer is one that is not an error. A stale daemon must answer with an error, or not at all. */
function assertNotServed(answer: Answer, what: string): void {
  if (!answer.answered) return;
  if (typeof answer.message === 'object' && answer.message !== null && 'error' in answer.message) return;
  throw new Error(`${what} was served by a daemon that should have stopped: ${JSON.stringify(answer.message)}`);
}

/**
 * Runs `action` once a watch on `directory` is open, then resolves with the first non-null `check`. Each pass runs
 * `check` before it waits, so a change at any point, including while `action` runs, is seen.
 */
async function watchUntil<T>(
  directory: string,
  what: string,
  check: () => Promise<T | null>,
  action?: () => Promise<void>,
  timeoutMs = 30_000,
): Promise<T> {
  await mkdir(directory, { recursive: true });
  let notify: (() => void) | undefined;
  const watcher = fsWatch(directory, () => notify?.());
  try {
    await action?.();
    for (;;) {
      const changed = Promise.withResolvers<void>();
      notify = () => changed.resolve();
      const found = await check();
      if (found !== null) return found;
      await guardEvent(changed.promise, what, () => `no change in ${directory}`, undefined, timeoutMs);
    }
  } finally {
    watcher.close();
  }
}

/**
 * Waits until the daemon answers PING with `true`. The daemon claims its record before it listens, so the record is not
 * readiness. A PING that finds no listener waits for the next change in the socket directory, and an answer other than
 * `true` fails at once.
 */
async function untilServing(socketPath: string, workspace: string): Promise<void> {
  await watchUntil(dirname(socketPath), `the daemon on ${socketPath} to answer PING`, async () => {
    const answer = await requestOnce(socketPath, ping(workspace), 'the readiness PING');
    if (!answer.answered) return null;
    if (answer.message === true) return true;
    throw new Error(`the daemon on ${socketPath} answered PING with ${JSON.stringify(answer.message)}, not true`);
  });
}

async function readyDaemon(workspace: string, record: DaemonRecord): Promise<Daemon> {
  await untilServing(record.socketPath, workspace);
  return { record, entry: await processEntry(record.processId) };
}

async function startDaemon(workspace: string, env: Record<string, string>): Promise<Daemon> {
  nx(workspace, env, 'daemon', '--start');
  const record = await daemonRecord(workspace);
  if (record === null) throw new Error('nx daemon --start left no daemon record');
  return readyDaemon(workspace, record);
}

/**
 * Runs `action`, then resolves with the record that names a process other than `previous`. The record directory is
 * watched before `action` runs, so a record written at any point is seen.
 */
function replacementAfter({ workspace, previous, action, what, timeoutMs }: Replacement): Promise<DaemonRecord> {
  return watchUntil(
    dirname(recordPath(workspace)),
    what,
    async () => {
      const record = await daemonRecord(workspace);
      return record !== null && record.processId !== previous ? record : null;
    },
    action,
    timeoutMs,
  );
}

/** Runs `action` under a daemon that must judge the change in place, then awaits the old daemon's exit and the new one. */
async function restarted(
  workspace: string,
  daemon: Daemon,
  what: string,
  action: () => Promise<void>,
): Promise<Daemon> {
  const record = await replacementAfter({
    workspace,
    previous: daemon.record.processId,
    action,
    what: `the daemon to restart after ${what}`,
  });
  await awaitExit([daemon.entry], `daemon ${daemon.record.processId} replaced after ${what}`);
  return readyDaemon(workspace, record);
}

/**
 * A file-watcher client of the workspace's daemon, read one event at a time. The fixture environment turns NX_DAEMON
 * off, so this client turns it back on: its watcher must reach the daemon under test, not an in-process Nx.
 */
function watchClient(workspace: string, env: Record<string, string>) {
  const child = spawn('node', ['-e', CLIENT], {
    cwd: workspace,
    env: { ...env, NX_DAEMON: 'true', NX_DAEMON_CLIENT: daemonClient },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  const seen: WatchEvent[] = [];
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  const exited = Promise.withResolvers<number | null>();
  child.once('exit', (code) => exited.resolve(code));
  return {
    seen,
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
      await exited.promise;
    },
  };
}

/**
 * Runs `change` under a file-watcher client of the daemon, then writes a marker the watcher must report. Watch events
 * arrive in order, so once the marker is reported the change was judged too. The daemon must still be the same process.
 */
async function quietAfter(
  workspace: string,
  env: Record<string, string>,
  daemon: Daemon,
  what: string,
  marker: string,
  change: () => Promise<void>,
): Promise<void> {
  const client = watchClient(workspace, env);
  try {
    expect(await client.next(`the file watcher registering before ${what}`)).toEqual({ kind: 'ready', details: [] });
    await change();
    await writeFile(join(workspace, 'p', marker), 'marker\n');
    for (;;) {
      const event = await client.next(`the file watcher reporting ${marker} after ${what}`);
      expect(
        event.kind,
        `${what} made the daemon stop instead of judging the change in place: ${JSON.stringify(client.seen)}`,
      ).toBe('changed');
      if (event.details.includes(`p/${marker}`)) break;
    }
  } finally {
    await client.end();
  }
  expect(await recordedPid(workspace), `${what} replaced the daemon`).toBe(daemon.record.processId);
  expect(isRunning(daemon.record.processId), `${what} stopped the daemon`).toBe(true);
}

/** A process that listens on `socketPath`, started after the path is free. */
async function listeningChild(socketPath: string): Promise<Listener> {
  const child = spawn('node', ['-e', LISTENER, socketPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  const pid = child.pid;
  if (pid === undefined) throw new Error(`could not start the listener for ${socketPath}`);
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  const first = await guardEvent(lines.next(), `the listener on ${socketPath} to start`, () => `stderr:\n${stderr}`);
  if (first.done || first.value !== 'listening') {
    throw new Error(`the listener on ${socketPath} did not report listening: ${JSON.stringify(first)}\n${stderr}`);
  }
  return { pid, stop: () => stopChild(child) };
}

/** Binds `socketPath` from this process, so the replacement exists before a daemon's exit could remove it. */
async function listenInProcess(socketPath: string): Promise<() => Promise<void>> {
  const server = createServer(() => {});
  const listening = Promise.withResolvers<void>();
  server.once('error', listening.reject);
  server.listen(socketPath, () => listening.resolve());
  await guardEvent(listening.promise, `this process to listen on ${socketPath}`, () => 'no listen callback');
  return () => {
    const closed = Promise.withResolvers<void>();
    server.close(() => closed.resolve());
    return closed.promise;
  };
}

it('has no 20 ms outdated-check loop or lockfile hashing left in the installed daemon', async () => {
  const source = await readFile(daemonServer, 'utf8');
  for (const removed of [
    'lockFileHashChanged',
    'daemonIsOutdated',
    'existingLockHash',
    'outdated check interval (20ms)',
  ]) {
    expect(source.includes(removed), `${daemonServer} still contains ${removed}`).toBe(false);
  }
});

/** Valid lockfiles keep the real graph usable while the watcher judges byte changes. */
async function lockContents(
  lockfile: (typeof LOCKFILES)[number],
  root: string,
): Promise<readonly [Buffer, Buffer, Buffer]> {
  let text: string;
  switch (lockfile) {
    case 'package-lock.json':
      text = '{"name":"fixture","lockfileVersion":3,"requires":true,"packages":{"":{"name":"fixture"}}}\n';
      break;
    case 'yarn.lock':
      text = '# yarn lockfile v1\n';
      break;
    case 'pnpm-lock.yaml':
      text = "lockfileVersion: '9.0'\nimporters:\n  .: {}\npackages: {}\nsnapshots: {}\n";
      break;
    case 'bun.lock':
      text = '{"lockfileVersion":1,"workspaces":{"":{"name":"fixture"}},"packages":{}}\n';
      break;
    case 'bun.lockb': {
      // Generate the legacy binary with Bun, outside the watched workspace.
      const generator = join(root, 'lock-generator');
      await mkdir(generator);
      await writeFile(join(generator, 'bunfig.toml'), '[install]\nsaveTextLockfile = false\n');
      const generated: Buffer[] = [];
      for (const version of ['1.0.0', '1.0.1', '1.0.2']) {
        const dependency = join(root, `lock-dependency-${version}`);
        await mkdir(dependency);
        await writeFile(
          join(generator, 'package.json'),
          JSON.stringify({
            name: 'fixture',
            dependencies: { 'lock-dependency': `file:../lock-dependency-${version}` },
          }),
        );
        await writeFile(join(dependency, 'package.json'), JSON.stringify({ name: 'lock-dependency', version }));
        await rm(join(generator, 'bun.lockb'), { force: true });
        const result = spawnSync('bun', ['install', '--lockfile-only', '--ignore-scripts', '--offline'], {
          cwd: generator,
          encoding: 'utf8',
        });
        expect(result.status, result.stdout + result.stderr).toBe(0);
        generated.push(await readFile(join(generator, 'bun.lockb')));
      }
      const [one, two, three] = generated;
      if (one === undefined || two === undefined || three === undefined)
        throw new Error('Bun generated fewer than three lockfiles');
      expect(one.equals(two)).toBe(false);
      expect(two.equals(three)).toBe(false);
      return [one, two, three];
    }
  }
  return [Buffer.from(text), Buffer.from(`${text}\n`), Buffer.from(`${text}\n\n`)];
}

for (const lockfile of LOCKFILES) {
  it(`restarts the daemon on ${lockfile} create, change, replace, and delete, and only then`, async () => {
    await withNxFixture(
      'nx-daemon-validity-lock-',
      async ({ root, socketDirectory, workspace }) => {
        if (lockfile === 'pnpm-lock.yaml') {
          // pnpm's parser requires its hoisting metadata, even with no packages.
          // Keep the installation directory fixture-owned, never write through the repository link.
          await fixtureWorkspace(workspace, join(repositoryRoot, 'node_modules/nx'));
          await writeFile(join(workspace, 'node_modules/.modules.yaml'), 'hoistedDependencies: {}\n');
        } else {
          await fixtureWorkspace(workspace);
        }
        const env = fixtureNxEnv(workspace, socketDirectory);
        const lock = join(workspace, lockfile);
        const [one, two, three] = await lockContents(lockfile, root);
        let daemon = await startDaemon(workspace, env);
        daemon = await restarted(workspace, daemon, `creating ${lockfile}`, () => writeFile(lock, one));
        daemon = await restarted(workspace, daemon, `changing ${lockfile}`, () => writeFile(lock, two));
        daemon = await restarted(workspace, daemon, `replacing ${lockfile} by rename`, async () => {
          await writeFile(`${lock}.next`, three);
          await rename(`${lock}.next`, lock);
        });
        await quietAfter(workspace, env, daemon, `a same-bytes rewrite of ${lockfile}`, 'same-bytes.txt', () =>
          writeFile(lock, three),
        );
        await quietAfter(workspace, env, daemon, `a nested ${lockfile} in p`, 'nested.txt', () =>
          writeFile(join(workspace, 'p', lockfile), one),
        );
        await restarted(workspace, daemon, `deleting ${lockfile}`, () => rm(lock));
      },
      FIXTURE_WORKSPACE,
    );
  }, 180_000);
}

it('does not serve a request sent on a connection opened before a lockfile change', async () => {
  await withNxFixture(
    'nx-daemon-validity-held-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const lock = join(workspace, 'yarn.lock');
      await writeFile(lock, 'lock one\n');
      const env = fixtureNxEnv(workspace, socketDirectory);
      const daemon = await startDaemon(workspace, env);
      const held = await openClient(daemon.record.socketPath);
      try {
        expect(await held.request(ping(workspace), 'the first PING on the held connection')).toEqual({
          answered: true,
          message: true,
        });
        await writeFile(lock, 'lock two\n');
        assertNotServed(
          await held.request(
            { type: 'REQUEST_PROJECT_GRAPH', workspaceRoot: workspace },
            'the graph request sent right after the lockfile change',
          ),
          'a graph request on a connection opened before the lockfile change',
        );
      } finally {
        held.close();
      }
      await awaitExit([daemon.entry], `daemon ${daemon.record.processId} after the held lockfile change`);
      await replacementAfter({
        workspace,
        previous: daemon.record.processId,
        what: 'the daemon to restart after the held lockfile change',
      });
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it('stops a daemon at the next request after an ignored lockfile changes, then its successor within its safety poll with no request', async () => {
  await withNxFixture(
    'nx-daemon-validity-ignored-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      await writeFile(join(workspace, '.gitignore'), 'yarn.lock\n');
      const lock = join(workspace, 'yarn.lock');
      await writeFile(lock, 'lock one\n');
      const env = fixtureNxEnv(workspace, socketDirectory);
      const daemon = await startDaemon(workspace, env);
      // The watcher never reports an ignored lockfile; the next request is what judges this change.
      await writeFile(lock, 'lock two\n');
      assertNotServed(
        await requestOnce(daemon.record.socketPath, ping(workspace), 'a PING sent after the ignored lockfile changed'),
        'a PING sent after the ignored lockfile changed',
      );
      await awaitExit(
        [daemon.entry],
        `daemon ${daemon.record.processId} after the ignored lockfile changed and a PING followed`,
      );
      const record = await replacementAfter({
        workspace,
        previous: daemon.record.processId,
        what: 'the daemon to restart after the ignored lockfile changed and a PING followed',
      });
      // The successor booted with 'lock two' on disk, so that is its baseline; a daemon answers a PING only when its
      // check finds the lock unchanged, and nothing requests this one afterwards.
      const successor = await readyDaemon(workspace, record);
      expect(
        await recordedPid(workspace),
        'the successor was replaced before the lockfile changed again, so no replacement could show the safety poll',
      ).toBe(successor.record.processId);
      // Nothing requests the successor after this write and its watcher ignores the file, so only the 5 s safety
      // poll can judge 'lock three'.
      await replacementAfter({
        workspace,
        previous: successor.record.processId,
        action: () => writeFile(lock, 'lock three\n'),
        what: 'a replacement of the successor within the 5 s safety poll after an ignored lockfile change',
        timeoutMs: 20_000,
      });
      await awaitExit(
        [successor.entry],
        `successor ${successor.record.processId} after the ignored lockfile changed with no request`,
      );
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it('stops a daemon, refusing its next request, once the installed nx version changes in place', async () => {
  await withNxFixture(
    'nx-daemon-validity-version-',
    async ({ root, socketDirectory, workspace }) => {
      const installed = join(root, 'installed-nx');
      const manifest = await installedManifest();
      await nxPackageCopy(installed, manifest.version);
      await fixtureWorkspace(workspace, installed);
      const env = fixtureNxEnv(workspace, socketDirectory);
      const daemon = await startDaemon(workspace, env);
      await writeFile(
        join(installed, 'package.json'),
        JSON.stringify({ ...manifest, version: `${manifest.version}-validity` }),
      );
      assertNotServed(
        await requestOnce(
          daemon.record.socketPath,
          ping(workspace),
          'a PING sent after the installed nx version changed',
        ),
        'a PING sent after the installed nx version changed',
      );
      await awaitExit([daemon.entry], `daemon ${daemon.record.processId} after the installed nx version changed`);
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it('stops a daemon once the workspace nx link points at a different version', async () => {
  await withNxFixture(
    'nx-daemon-validity-link-',
    async ({ root, socketDirectory, workspace }) => {
      const first = join(root, 'installed-nx');
      const second = join(root, 'replacement-nx');
      const manifest = await installedManifest();
      await nxPackageCopy(first, manifest.version);
      await nxPackageCopy(second, `${manifest.version}-validity`);
      await fixtureWorkspace(workspace, first);
      const env = fixtureNxEnv(workspace, socketDirectory);
      const daemon = await startDaemon(workspace, env);
      const link = join(workspace, 'node_modules', 'nx');
      await unlink(link);
      await symlink(second, link, 'dir');
      assertNotServed(
        await requestOnce(daemon.record.socketPath, ping(workspace), 'a PING sent after the nx link changed'),
        'a PING sent after the nx link changed',
      );
      await awaitExit([daemon.entry], `daemon ${daemon.record.processId} after the nx link changed`);
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it('leaves a successor record and socket in place when another process takes the record', async () => {
  await withNxFixture(
    'nx-daemon-validity-successor-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const env = fixtureNxEnv(workspace, socketDirectory);
      const old = await startDaemon(workspace, env);
      const successorSocket = join(socketDirectory, 'successor.sock');
      const successor = await listeningChild(successorSocket);
      try {
        const record = {
          processId: successor.pid,
          socketPath: successorSocket,
          nxVersion: (await installedManifest()).version,
          workspaceRoot: workspace,
        };
        await writeFile(recordPath(workspace), `${JSON.stringify(record)}\n`);
        await awaitExit([old.entry], `daemon ${old.record.processId} whose record now names ${successor.pid}`);
        expect(await daemonRecord(workspace), 'the record after the displaced daemon exited').toMatchObject({
          processId: successor.pid,
          socketPath: successorSocket,
        });
        expect(
          await acceptsConnections(successorSocket),
          'the successor socket after the displaced daemon exited',
        ).toBe(true);
      } finally {
        await successor.stop();
      }
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it("leaves a replacement socket in place when the daemon's own socket path is replaced", async () => {
  await withNxFixture(
    'nx-daemon-validity-socket-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const env = fixtureNxEnv(workspace, socketDirectory);
      const daemon = await startDaemon(workspace, env);
      await unlink(daemon.record.socketPath);
      const closeReplacement = await listenInProcess(daemon.record.socketPath);
      try {
        await awaitExit([daemon.entry], `daemon ${daemon.record.processId} whose socket path was replaced`);
        expect(
          await acceptsConnections(daemon.record.socketPath),
          'the replacement socket after the daemon exited',
        ).toBe(true);
      } finally {
        await closeReplacement();
      }
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it('stops a daemon whose own socket path is removed with nothing taking its place', async () => {
  await withNxFixture(
    'nx-daemon-validity-unlinked-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const env = fixtureNxEnv(workspace, socketDirectory);
      const daemon = await startDaemon(workspace, env);
      await unlink(daemon.record.socketPath);
      await awaitExit([daemon.entry], `daemon ${daemon.record.processId} after its socket path was removed`);
      expect(await acceptsConnections(daemon.record.socketPath), 'the socket path after the daemon exited').toBe(false);
    },
    FIXTURE_WORKSPACE,
  );
}, 180_000);

it('keeps an idle daemon off the lockfiles, record, and socket between its safety polls', async () => {
  await withNxFixture(
    'nx-daemon-validity-idle-',
    async ({ root, socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const log = join(root, 'validity-probe.log');
      const probe = join(root, 'validity-probe.cjs');
      await writeFile(probe, PROBE);
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_VALIDITY_LOG: log };
      const daemonProcess = spawn(process.execPath, ['--preload', probe, daemonStart], {
        cwd: workspace,
        env,
        stdio: ['ignore', 'ignore', 'ignore'],
      });
      try {
        const record = await replacementAfter({ workspace, what: 'the directly started daemon to publish its record' });
        const daemon = await readyDaemon(workspace, record);
        const before = (await readFile(log, 'utf8')).split('\n').filter((line) => line !== '');
        // The one real-clock wait. The daemon is a separate OS process whose idle behaviour exists only in wall-clock
        // time, so no injected or fake clock can reach it; the window has to be measured against the real clock.
        const observation = Promise.withResolvers<void>();
        setTimeout(observation.resolve, OBSERVATION_MS);
        await observation.promise;
        const observed = (await readFile(log, 'utf8'))
          .split('\n')
          .filter((line) => line !== '')
          .slice(before.length);
        expect(
          before.length,
          'the probe recorded no daemon startup reads, so its instrumentation is dead',
        ).toBeGreaterThan(1);
        expect(
          observed.length,
          `an idle daemon made ${observed.length} lockfile, record, or socket calls in ${OBSERVATION_MS} ms:\n${observed.join('\n')}`,
        ).toBeLessThan(100);
        expect(isRunning(daemon.record.processId), 'the idle daemon stopped').toBe(true);
      } finally {
        await stopChild(daemonProcess);
      }
    },
    FIXTURE_WORKSPACE,
  );
}, 120_000);
