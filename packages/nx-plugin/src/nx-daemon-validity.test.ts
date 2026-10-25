import { expect, it } from 'bun:test';
import { type ChildProcess, spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { chmodSync, existsSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, readFile, rename, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { createConnection, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as sleep } from 'node:timers/promises';
import { sendMessage } from 'nx/src/daemon/socket-utils.js';
import { consumeMessagesFromSocket, parseMessage } from 'nx/src/utils/consume-messages-from-socket.js';
import typia from 'typia';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { whenFound } from './__tests__/path-change.js';
import { awaitExit, isRunning, type ProcessEntry, processesOf, processTree, terminate } from './testing.js';

/**
 * Guards the event-driven validity hunk of `patches/nx@23.2.1.patch` (upstream nrwl/nx#37113, fixing nrwl/nx#37112).
 * The installed daemon judged its root lockfiles and installed nx version on a 20 ms timer. These regressions preserve
 * the existing socket and process ownership guards while moving lockfile checks to applied watcher events, request
 * boundaries, and a 5 s safety poll for events the watcher ignores or drops.
 *
 * Each test runs a real daemon in a fixture workspace and asserts only what a client, a watcher, or the filesystem can
 * observe. Restarts are awaited on the record directory, and readiness on an actual PING answer, not a filesystem event.
 * The one real-clock observation window is the idle walk in the last test.
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
/** How long a client waits for its daemon's successor: `WAIT_FOR_SERVER_CONFIG` in `daemon/client/client.js`. */
const RECONNECT_WINDOW_MS = 60_000;

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

/** A linked plugin's entry: it infers nothing, and `version` alone sets its bytes. */
function pluginInference(version: string): string {
  return `// inference ${version}\nmodule.exports = { name: 'linked-plugin', createNodesV2: ['**/never.json', () => []] };\n`;
}

interface LinkedPluginCheckout {
  readonly plugin: string;
  readonly entry: string;
  readonly lock: string;
}

/** A developer link's target: the plugin is a package of another checkout, which has its own lockfile. */
async function linkedPluginCheckout(root: string, name: string): Promise<LinkedPluginCheckout> {
  const plugin = join(root, name, 'plugin');
  await mkdir(join(plugin, 'dist'), { recursive: true });
  await writeFile(join(root, name, 'bun.lock'), 'checkout lock one\n');
  await writeFile(
    join(plugin, 'package.json'),
    JSON.stringify({ name: 'linked-plugin', version: '1.0.0', main: 'dist/index.js', files: ['dist'] }),
  );
  await writeFile(join(plugin, 'dist', 'index.js'), pluginInference('one'));
  return { plugin, entry: join(plugin, 'dist', 'index.js'), lock: join(root, name, 'bun.lock') };
}

async function processEntry(pid: number): Promise<ProcessEntry> {
  const [entry] = await processesOf([pid]);
  if (entry === undefined) throw new Error(`process ${pid} no longer exists`);
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
 * Runs `action`, then resolves with the first non-null `check`. `check` judges the whole state once a watch on
 * `directory` is armed and after every change there, so a change at any point, before or while `action` runs, is seen.
 */
async function watchUntil<T>(
  directory: string,
  what: string,
  check: () => Promise<T | null>,
  action?: () => Promise<void>,
): Promise<T> {
  await mkdir(directory, { recursive: true });
  const stop = new AbortController();
  const found = whenFound(directory, check, stop.signal);
  try {
    const [, value] = await Promise.all([action?.(), guardEvent(found, what, () => `no change in ${directory}`)]);
    return value;
  } finally {
    // Reaps the waker when `action` failed first; only the abort said here is not an outcome of the wait.
    stop.abort();
    await found.catch((error: unknown) => {
      if (error !== stop.signal.reason) throw error;
    });
  }
}

/** Capture the expected holder and the current record before fixture retirement removes the evidence. */
async function daemonDiagnostics(workspace: string, expectedPid: number): Promise<string> {
  const record = await daemonRecord(workspace);
  const processes = await processTree(record === null ? [expectedPid] : [expectedPid, record.processId]);
  const expected = processes.find(({ pid }) => pid === expectedPid);
  const logPath = join(dirname(recordPath(workspace)), 'daemon.log');
  let log: string;
  try {
    log = await readFile(logPath, 'utf8');
  } catch (error) {
    log = `unavailable: ${error instanceof Error ? error.message : String(error)}`;
  }
  return [
    `workspace ${workspace}; expected daemon pid ${expectedPid}: ${
      expected === undefined ? 'no longer running' : `state=${expected.stat} command=${expected.command}`
    }`,
    `current daemon record ${JSON.stringify(record)}`,
    `daemon processes ${JSON.stringify(processes)}`,
    `daemon log tail ${logPath}:\n${log.split('\n').slice(-60).join('\n')}`,
  ].join('\n');
}

/**
 * The record and socket path can exist before a daemon can answer. Binding, listening and closing a connection need not
 * emit another directory event, so readiness probes the protocol itself; only a true PING advances the fixture.
 */
async function untilServing(record: DaemonRecord, workspace: string): Promise<void> {
  const { socketPath } = record;
  const waiting = new AbortController();
  const serving = async (): Promise<void> => {
    while (!waiting.signal.aborted) {
      const answer = await requestOnce(socketPath, ping(workspace), 'the readiness PING');
      if (answer.answered) {
        if (answer.message === true) return;
        throw new Error(`the daemon on ${socketPath} answered PING with ${JSON.stringify(answer.message)}, not true`);
      }
      await sleep(50, undefined, { signal: waiting.signal });
    }
  };
  try {
    await guardEvent(serving(), `the daemon on ${socketPath} to answer PING`, () =>
      daemonDiagnostics(workspace, record.processId),
    );
  } finally {
    waiting.abort();
  }
}

async function readyDaemon(workspace: string, record: DaemonRecord): Promise<Daemon> {
  await untilServing(record, workspace);
  return { record, entry: await processEntry(record.processId) };
}

async function startDaemon(workspace: string, env: Record<string, string>): Promise<Daemon> {
  nx(workspace, env, 'daemon', '--start');
  const record = await daemonRecord(workspace);
  if (record === null) throw new Error('nx daemon --start left no daemon record');
  return readyDaemon(workspace, record);
}

interface KeptDaemon {
  readonly daemon: Daemon;
  /** Everything the daemon printed so far: it logs to its own stdout in the foreground. */
  log(): string;
  readonly exited: Promise<number | null>;
  /** Terminate the daemon unless it has exited, and await its exit. */
  stop(): Promise<void>;
}

/** A daemon as cowshed's keeper job starts it: the daemon's own entry in the foreground, kept. */
async function startKeptDaemon(workspace: string, env: Record<string, string>): Promise<KeptDaemon> {
  const kept = spawn('node', [daemonStart, '--kept'], { cwd: workspace, env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  const append = (text: string): void => {
    log += text;
  };
  kept.stdout.setEncoding('utf8').on('data', append);
  kept.stderr.setEncoding('utf8').on('data', append);
  const exited = Promise.withResolvers<number | null>();
  kept.once('exit', (code) => exited.resolve(code));
  const stop = async (): Promise<void> => {
    if (kept.exitCode === null && kept.signalCode === null && kept.pid !== undefined) terminate(kept.pid);
    await exited.promise;
  };
  try {
    const record = await watchUntil(
      dirname(recordPath(workspace)),
      'the kept daemon to write its record',
      async () => {
        const current = await daemonRecord(workspace);
        return current !== null && current.processId === kept.pid ? current : null;
      },
      async () => {},
    );
    return { daemon: await readyDaemon(workspace, record), log: () => log, exited: exited.promise, stop };
  } catch (error) {
    await stop();
    throw error;
  }
}

/**
 * Runs `action`, then resolves with the record that names a process other than `previous`. The record is judged whole
 * once its directory is watched and after every change there, so a record written at any point, before or while
 * `action` runs, is seen.
 */
function replacementAfter({ workspace, previous, action, what }: Replacement): Promise<DaemonRecord> {
  return watchUntil(
    dirname(recordPath(workspace)),
    what,
    async () => {
      const record = await daemonRecord(workspace);
      return record !== null && record.processId !== previous ? record : null;
    },
    action,
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
 * Runs `action` as a lockfile change with a connection opened before it held across the change: a request sent on
 * that connection after the change must not be served by the old daemon.
 */
async function restartedHeld(
  workspace: string,
  daemon: Daemon,
  what: string,
  action: () => Promise<void>,
): Promise<Daemon> {
  const held = await openClient(daemon.record.socketPath);
  try {
    expect(
      await held.request(ping(workspace), `the first PING on the held connection before ${what}`),
      `does not serve a request sent on a connection opened before a lockfile change: the first PING before ${what}`,
    ).toEqual({ answered: true, message: true });
    return await restarted(workspace, daemon, what, async () => {
      await action();
      assertNotServed(
        await held.request(
          { type: 'REQUEST_PROJECT_GRAPH', workspaceRoot: workspace },
          `the graph request sent right after ${what}`,
        ),
        `does not serve a request sent on a connection opened before a lockfile change: a graph request on a connection opened before ${what}`,
      );
      held.close();
    });
  } finally {
    held.close();
  }
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

/** One change that must not restart the daemon, and the marker whose report proves the watcher judged it. */
interface QuietChange {
  readonly what: string;
  readonly marker: string;
  readonly change: () => Promise<void>;
}

/**
 * Runs each change under one file-watcher client of the daemon, then writes its marker, which the watcher must report.
 * Watch events arrive in order, so once a marker is reported its change was judged too. The daemon must still be the
 * same process after all of them.
 */
async function quietAfter(
  workspace: string,
  env: Record<string, string>,
  daemon: Daemon,
  what: string,
  changes: readonly QuietChange[],
): Promise<void> {
  const client = watchClient(workspace, env);
  try {
    expect(await client.next(`the file watcher registering before ${what}`)).toEqual({ kind: 'ready', details: [] });
    for (const { what: label, marker, change } of changes) {
      await change();
      await writeFile(join(workspace, 'p', marker), 'marker\n');
      for (;;) {
        const event = await client.next(`the file watcher reporting ${marker} after ${label}`);
        expect(
          event.kind,
          `${label} made the daemon stop instead of judging the change in place: ${JSON.stringify(client.seen)}`,
        ).toBe('changed');
        if (event.details.includes(`p/${marker}`)) break;
      }
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

it('awaits an answerable daemon after a closed PING without another socket-directory event', async () => {
  // No Nx runs here: the server stands in for the daemon, so the root is a plain temp directory.
  const directory = await mkdtemp(join(tmpdir(), 'nx-validity-'));
  try {
    const socketPath = join(directory, 'readiness.sock');
    let requests = 0;
    const server = createServer((socket) => {
      socket.once('data', () => {
        requests += 1;
        if (requests === 1) socket.end();
        else sendMessage(socket, true, 'json');
      });
    });
    const listening = Promise.withResolvers<void>();
    server.once('error', listening.reject);
    server.listen(socketPath, () => listening.resolve());
    await listening.promise;
    try {
      await untilServing({ processId: process.pid, socketPath }, directory);
      expect(requests).toBe(2);
    } finally {
      const closed = Promise.withResolvers<void>();
      server.close(() => closed.resolve());
      await closed.promise;
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
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

it('restarts the daemon on each root lockfile create, change, replace, and delete, and only then', async () => {
  await withNxFixture(
    'nx-daemon-validity-lock-',
    async ({ root, socketDirectory, workspace }) => {
      // One fixture walks every root lockfile. Each one is deleted at its last step, so the next starts clean.
      // pnpm's parser requires its hoisting metadata, even with no packages.
      // Keep the installation directory fixture-owned, never write through the repository link.
      await fixtureWorkspace(workspace, join(repositoryRoot, 'node_modules/nx'));
      await writeFile(join(workspace, 'node_modules/.modules.yaml'), 'hoistedDependencies: {}\n');
      const env = fixtureNxEnv(workspace, socketDirectory);
      let daemon = await startDaemon(workspace, env);
      for (const lockfile of LOCKFILES) {
        const lock = join(workspace, lockfile);
        const [one, two, three] = await lockContents(lockfile, root);
        daemon = await restarted(workspace, daemon, `creating ${lockfile}`, () => writeFile(lock, one));
        // The yarn.lock change also holds a connection opened before it across the change.
        const change = () => writeFile(lock, two);
        daemon =
          lockfile === 'yarn.lock'
            ? await restartedHeld(workspace, daemon, `changing ${lockfile}`, change)
            : await restarted(workspace, daemon, `changing ${lockfile}`, change);
        daemon = await restarted(workspace, daemon, `replacing ${lockfile} by rename`, async () => {
          await writeFile(`${lock}.next`, three);
          await rename(`${lock}.next`, lock);
        });
        await quietAfter(workspace, env, daemon, `the quiet changes around ${lockfile}`, [
          {
            what: `a same-bytes rewrite of ${lockfile}`,
            marker: `same-bytes-${lockfile}.txt`,
            change: () => writeFile(lock, three),
          },
          {
            what: `a nested ${lockfile} in p`,
            marker: `nested-${lockfile}.txt`,
            change: () => writeFile(join(workspace, 'p', lockfile), one),
          },
        ]);
        daemon = await restarted(workspace, daemon, `deleting ${lockfile}`, () => rm(lock));
      }
    },
    FIXTURE_WORKSPACE,
  );
});

it('stops a kept daemon on a lockfile change instead of spawning a successor its keeper does not own', async () => {
  await withNxFixture(
    'nx-daemon-validity-kept-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const lock = join(workspace, 'yarn.lock');
      await writeFile(lock, 'lock one\n');
      const kept = await startKeptDaemon(workspace, fixtureNxEnv(workspace, socketDirectory));
      try {
        await writeFile(lock, 'lock two\n');
        assertNotServed(
          await requestOnce(kept.daemon.record.socketPath, ping(workspace), 'a PING sent after the lockfile changed'),
          'a PING sent to the kept daemon after the lockfile changed',
        );
        await guardEvent(kept.exited, 'the kept daemon to exit after the lockfile changed', kept.log);
        // The decision is in the daemon's own log, written before it exits: it stopped, and started nothing.
        const log = kept.log();
        expect(log, 'the kept daemon did not say it left its successor to its keeper').toContain(
          'Stopping: the process that started this daemon (--kept) starts its successor',
        );
        expect(log, 'the kept daemon spawned a successor of its own').not.toContain('Restarting daemon');
        expect(log, 'the kept daemon spawned a successor of its own').not.toContain('Started new daemon process');
      } finally {
        await kept.stop();
      }
    },
    FIXTURE_WORKSPACE,
  );
});

it('stops each daemon as its state changes: an ignored lockfile, a replaced or removed socket, a taken record, and emptied workspace files', async () => {
  // Each step's messages keep the title of the original case it came from.
  const ignored =
    'stops a daemon at the next request after an ignored lockfile changes, then its successor within its safety poll with no request';
  const replaced = "leaves a replacement socket in place when the daemon's own socket path is replaced";
  const removed = 'stops a daemon whose own socket path is removed with nothing taking its place';
  const taken = 'leaves a successor record and socket in place when another process takes the record';
  const emptied = 'stops a daemon whose workspace files are all deleted before it reads the emptied workspace';
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
        `${ignored}: a PING sent after the ignored lockfile changed`,
      );
      await awaitExit(
        [daemon.entry],
        `${ignored}: daemon ${daemon.record.processId} after the ignored lockfile changed and a PING followed`,
      );
      const record = await replacementAfter({
        workspace,
        previous: daemon.record.processId,
        what: `${ignored}: the daemon to restart after the ignored lockfile changed and a PING followed`,
      });
      // The successor booted with 'lock two' on disk, so that is its baseline; a daemon answers a PING only when its
      // check finds the lock unchanged, and nothing requests this one afterwards.
      const successor = await readyDaemon(workspace, record);
      expect(
        await recordedPid(workspace),
        `${ignored}: the successor was replaced before the lockfile changed again, so no replacement could show the safety poll`,
      ).toBe(successor.record.processId);
      // Nothing requests the successor after this write and its watcher ignores the file, so only the 5 s safety
      // poll can judge 'lock three'.
      const polled = await replacementAfter({
        workspace,
        previous: successor.record.processId,
        action: () => writeFile(lock, 'lock three\n'),
        what: `${ignored}: a replacement of the successor within the 5 s safety poll after an ignored lockfile change`,
      });
      await awaitExit(
        [successor.entry],
        `${ignored}: successor ${successor.record.processId} after the ignored lockfile changed with no request`,
      );
      // The socket path is replaced under the daemon: it stops, and the replacement stays in place.
      let current = await readyDaemon(workspace, polled);
      await unlink(current.record.socketPath);
      const closeReplacement = await listenInProcess(current.record.socketPath);
      try {
        await awaitExit(
          [current.entry],
          `${replaced}: daemon ${current.record.processId} whose socket path was replaced`,
        );
        expect(
          await acceptsConnections(current.record.socketPath),
          `${replaced}: the replacement socket after the daemon exited`,
        ).toBe(true);
      } finally {
        await closeReplacement();
      }
      // The socket path is removed with nothing taking its place: the daemon stops, and nothing listens there.
      current = await startDaemon(workspace, env);
      await unlink(current.record.socketPath);
      await awaitExit(
        [current.entry],
        `${removed}: daemon ${current.record.processId} after its socket path was removed`,
      );
      expect(
        await acceptsConnections(current.record.socketPath),
        `${removed}: the socket path after the daemon exited`,
      ).toBe(false);
      // Another process takes the record and its socket: the daemon exits and leaves both in place.
      current = await startDaemon(workspace, env);
      const successorSocket = join(socketDirectory, 'successor.sock');
      const taker = await listeningChild(successorSocket);
      try {
        const takenRecord = {
          processId: taker.pid,
          socketPath: successorSocket,
          nxVersion: (await installedManifest()).version,
          workspaceRoot: workspace,
        };
        await writeFile(recordPath(workspace), `${JSON.stringify(takenRecord)}\n`);
        await awaitExit(
          [current.entry],
          `${taken}: daemon ${current.record.processId} whose record now names ${taker.pid}`,
        );
        expect(await daemonRecord(workspace), `${taken}: the record after the displaced daemon exited`).toMatchObject({
          processId: taker.pid,
          socketPath: successorSocket,
        });
        expect(
          await acceptsConnections(successorSocket),
          `${taken}: the successor socket after the displaced daemon exited`,
        ).toBe(true);
      } finally {
        await taker.stop();
      }
      // Last, as it leaves no workspace to walk: every tracked file goes.
      current = await startDaemon(workspace, env);
      expect(
        JSON.parse(nx(workspace, { ...env, NX_DAEMON: 'true' }, 'show', 'projects', '--json')),
        `${emptied}: the projects before the workspace was emptied`,
      ).toEqual(['p']);
      // Every tracked file goes, as when a checkout is removed or unmounted under its daemon; the daemon's own record
      // under .nx stays, so only the workspace check can stop it. Reading the emptied file map instead would block
      // the daemon's only thread for good, where no SIGTERM handler runs.
      await rm(join(workspace, 'p'), { recursive: true });
      await unlink(join(workspace, 'package.json'));
      await unlink(join(workspace, 'nx.json'));
      await awaitExit(
        [current.entry],
        `${emptied}: daemon ${current.record.processId} after every workspace file was deleted`,
      );
      const log = await readFile(join(workspace, '.nx', 'workspace-data', 'd', 'daemon.log'), 'utf8');
      expect(log, `${emptied}: the daemon log`).toMatch(
        /Server stopped because: "its workspace \S+ has no nx\.json any more"/,
      );
    },
    FIXTURE_WORKSPACE,
  );
});

it('stops a daemon once the workspace nx link points at a different version, and once the installed nx version changes in place', async () => {
  const linkTitle = 'stops a daemon once the workspace nx link points at a different version';
  const versionTitle = 'stops a daemon, refusing its next request, once the installed nx version changes in place';
  await withNxFixture(
    'nx-daemon-validity-version-',
    async ({ root, socketDirectory, workspace }) => {
      const installed = join(root, 'installed-nx');
      const replacement = join(root, 'replacement-nx');
      const manifest = await installedManifest();
      await nxPackageCopy(installed, manifest.version);
      await nxPackageCopy(replacement, `${manifest.version}-replacement`);
      await fixtureWorkspace(workspace, installed);
      const env = fixtureNxEnv(workspace, socketDirectory);
      const nxLink = join(workspace, 'node_modules', 'nx');
      // Each step stops its daemon with no successor. A daemon started on a workspace whose nx is not the version the
      // repository's nx runs does not come up, so the link goes back to the installed copy before the next daemon
      // starts, as the lockfile walk deletes each lockfile so the next starts clean.
      const linked = await startDaemon(workspace, env);
      await unlink(nxLink);
      await symlink(replacement, nxLink, 'dir');
      assertNotServed(
        await requestOnce(linked.record.socketPath, ping(workspace), 'a PING sent after the nx link changed'),
        `${linkTitle}: a PING sent after the nx link changed`,
      );
      await awaitExit([linked.entry], `${linkTitle}: daemon ${linked.record.processId} after the nx link changed`);
      await unlink(nxLink);
      await symlink(installed, nxLink, 'dir');
      const versioned = await startDaemon(workspace, env);
      await writeFile(
        join(installed, 'package.json'),
        JSON.stringify({ ...manifest, version: `${manifest.version}-validity` }),
      );
      assertNotServed(
        await requestOnce(
          versioned.record.socketPath,
          ping(workspace),
          'a PING sent after the installed nx version changed',
        ),
        `${versionTitle}: a PING sent after the installed nx version changed`,
      );
      await awaitExit(
        [versioned.entry],
        `${versionTitle}: daemon ${versioned.record.processId} after the installed nx version changed`,
      );
    },
    FIXTURE_WORKSPACE,
  );
});

it('restarts a daemon once a plugin linked from outside the workspace changes its bytes, in place or by a moved link, and only then', async () => {
  await withNxFixture(
    'nx-daemon-validity-plugin-',
    async ({ root, socketDirectory, workspace }) => {
      const first = await linkedPluginCheckout(root, 'checkout-a');
      const copy = await linkedPluginCheckout(root, 'checkout-b');
      const link = join(workspace, 'node_modules', 'linked-plugin');
      const relink = async (target: string) => {
        await unlink(link);
        await symlink(target, link, 'dir');
      };
      await fixtureWorkspace(workspace, join(repositoryRoot, 'node_modules', 'nx'));
      await symlink(first.plugin, link, 'dir');
      await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['linked-plugin'] }));
      const env = fixtureNxEnv(workspace, socketDirectory);
      // Nothing in the workspace changes, so no watcher event reaches the daemon: each request judges the link.
      const serves = async (daemon: Daemon, what: string) => {
        expect(
          await requestOnce(daemon.record.socketPath, ping(workspace), `a PING after ${what}`),
          `${what} was refused`,
        ).toEqual({ answered: true, message: true });
        expect(await recordedPid(workspace), `${what} replaced the daemon`).toBe(daemon.record.processId);
      };
      const replaced = async (daemon: Daemon, what: string) => {
        assertNotServed(
          await requestOnce(daemon.record.socketPath, ping(workspace), `a PING sent after ${what}`),
          `a PING sent after ${what}`,
        );
        await awaitExit([daemon.entry], `daemon ${daemon.record.processId} after ${what}`);
        return readyDaemon(
          workspace,
          await replacementAfter({ workspace, previous: daemon.record.processId, what: `a successor after ${what}` }),
        );
      };
      let daemon = await startDaemon(workspace, env);
      await writeFile(first.entry, pluginInference('one'));
      await serves(daemon, 'a same-bytes rewrite of the linked plugin');
      await relink(copy.plugin);
      await serves(daemon, 'the link moving to a copy with the same bytes');
      await writeFile(copy.entry, pluginInference('two'));
      daemon = await replaced(daemon, 'the linked plugin changing in place');
      await relink(first.plugin);
      daemon = await replaced(daemon, 'the link moving to a plugin with other bytes');
      // The checkout's lockfile selects the plugin's dependencies, so its change restarts the daemon as well.
      await writeFile(first.lock, 'checkout lock two\n');
      await replaced(daemon, "the linked plugin checkout's lockfile changing");
    },
    FIXTURE_WORKSPACE,
  );
});

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
});

/**
 * The code that computed a run's project graph is the code its tasks were planned and keyed by. A plugin linked from
 * another checkout that changes under the run (a land there rebuilds it mid-gate) leaves the run's remaining requests
 * no daemon that computes that graph: a successor loads the new plugin, and so would the client's own process. The run
 * fails at once, naming the change, from a daemon that restarts itself and from a kept one whose keeper is not running.
 */
it('fails a run at once, naming LINKED_PLUGIN_CHANGED, when its linked plugin changes under it, on a restarting daemon and on a kept one', async () => {
  await withNxFixture(
    'nx-daemon-validity-plugin-run-',
    async ({ root, socketDirectory, workspace }) => {
      const linked = await linkedPluginCheckout(root, 'checkout');
      await fixtureWorkspace(workspace, join(repositoryRoot, 'node_modules', 'nx'));
      await symlink(linked.plugin, join(workspace, 'node_modules', 'linked-plugin'), 'dir');
      await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['linked-plugin'] }));
      await mkdir(join(workspace, 'q'));
      await writeFile(
        join(workspace, 'q', 'project.json'),
        JSON.stringify({
          name: 'q',
          implicitDependencies: ['p'],
          targets: {
            build: {
              command: 'mkdir -p q/dist && echo q > q/dist/q.txt',
              outputs: ['{projectRoot}/dist/'],
              dependsOn: ['^build'],
              cache: true,
            },
          },
        }),
      );
      const env = fixtureNxEnv(workspace, socketDirectory);
      // `p:build` rebuilds the linked plugin with other bytes. Each step's bytes differ, so its command does, and the
      // task misses the cache. The run's next daemon request, recording p's outputs or hashing q, finds the change.
      const failsNamingTheChange = async (title: string, version: string, client: Record<string, string>) => {
        const rebuilt = join(root, `inference-${version}.js`);
        await writeFile(rebuilt, pluginInference(version));
        await writeFile(
          join(workspace, 'p', 'project.json'),
          JSON.stringify({ name: 'p', targets: { build: { command: `cp ${rebuilt} ${linked.entry}`, cache: true } } }),
        );
        await rm(join(workspace, 'q', 'dist'), { recursive: true, force: true });
        // Half the client's wait for a successor: a run that waits for one at all overruns it.
        const run = spawnSync('bun', [nxEntry, 'run-many', '-t', 'build', '--outputStyle=static'], {
          cwd: workspace,
          env: { ...env, ...client, NX_DAEMON: 'true' },
          encoding: 'utf8',
          timeout: RECONNECT_WINDOW_MS / 2,
        });
        const printed = `${run.stdout}${run.stderr}`;
        expect(run.signal, `${title}: still running after ${RECONNECT_WINDOW_MS / 2} ms:\n${printed}`).toBeNull();
        // The premise: the daemon served the run until the change. A daemon that stopped before it (its watcher probe
        // not coming back, say) leaves a daemonless run, which nothing here guards.
        expect(printed, `${title}: the run went on without the daemon before the change`).not.toContain(
          'Continuing without the daemon',
        );
        expect(run.status, `${title}: the run succeeded across a plugin change:\n${printed}`).not.toBe(0);
        expect(printed, `${title}: the failure does not name the change`).toMatch(
          /The Nx daemon \(pid \d+\) stopped: LINKED_PLUGIN_CHANGED \(linked plugin file \S+index\.js changed\)/,
        );
        expect(existsSync(join(workspace, 'q', 'dist', 'q.txt')), `${title}: q:build ran:\n${printed}`).toBe(false);
      };
      // A daemon a client started restarts itself, and a host shell's client (no NX_DAEMON_START) could resume on it.
      await startDaemon(workspace, env);
      await failsNamingTheChange('a self-restarting daemon under a host client', 'two', {});
      nx(workspace, env, 'daemon', '--stop');
      // A kept daemon stops for its keeper to start the successor; the client is a cowshed job's.
      const kept = await startKeptDaemon(workspace, env);
      try {
        await failsNamingTheChange('a kept daemon under a job client', 'three', { NX_DAEMON_START: 'never' });
        await guardEvent(kept.exited, 'the kept daemon to exit after its linked plugin changed', kept.log);
        expect(kept.log(), 'the kept daemon stopped for another reason').toContain(
          'Daemon outdated: LINKED_PLUGIN_CHANGED',
        );
      } finally {
        await kept.stop();
      }
    },
    FIXTURE_WORKSPACE,
  );
});

/**
 * A client of the installed daemon client that asks three times, one request after the other, and says how each ended.
 * Which request the stop meets first (a write that fails with EPIPE, or one sent before the client saw the close) is a
 * race; the request after the failed reconnect is the one pid 23683 waited on for good.
 */
const ASK_THRICE = `
const { writeSync } = require('node:fs');
const { daemonClient } = require(process.env.NX_DAEMON_CLIENT);
const say = (...fields) => writeSync(1, fields.join('\\t') + '\\n');
const ask = (what) =>
  daemonClient.glob(['*.json'], []).then(
    () => say('asked', what, 'answered'),
    (error) => say('asked', what, 'failed', String(error?.message ?? error).split('\\n')[0]),
  );
ask(1).then(() => ask(2)).then(() => ask(3)).then(() => process.exit(0));
`;

/**
 * pid 23683, a consumer's land gate (2026-10-25): its daemon was stopped (SIGTERM) under a request, and nothing started
 * a successor. The request failed once the client's wait for one ran out, but the client stayed connecting, so every
 * later request waited for a reconnect that had already failed, and the run sat for hours with no task running.
 */
it('fails every later request at once once the daemon is lost under a request and no successor answers', async () => {
  await withNxFixture(
    'nx-daemon-validity-lost-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      // Where the fixture's own daemon would bind: the job profile admits Unix sockets by literal path.
      const socketPath = join(socketDirectory, 'd.sock');
      await mkdir(socketDirectory, { recursive: true });
      // Stands in for a daemon stopped under the client's first request: it ends that request's connection and closes
      // its socket. What binds the path next refuses the client (EACCES), which ends the client's wait for a successor
      // at once rather than after its 60 s.
      const listening = Promise.withResolvers<void>();
      const refused = Promise.withResolvers<void>();
      const refusing = createServer().once('error', refused.reject);
      const daemon = createServer((socket) => {
        socket.once('data', () => {
          daemon.close();
          socket.destroy();
          refusing.listen(socketPath, () => {
            chmodSync(socketPath, 0o000);
            refused.resolve();
          });
        });
      }).once('error', listening.reject);
      daemon.listen(socketPath, listening.resolve);
      await listening.promise;
      await mkdir(dirname(recordPath(workspace)), { recursive: true });
      const { version } = await installedManifest();
      await writeFile(
        recordPath(workspace),
        JSON.stringify({ processId: process.pid, socketPath, nxVersion: version }),
      );
      try {
        const client = spawn('node', ['-e', ASK_THRICE], {
          cwd: workspace,
          env: { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_DAEMON_CLIENT: daemonClient },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        let printed = '';
        const append = (text: string): void => {
          printed += text;
        };
        client.stdout.setEncoding('utf8').on('data', append);
        client.stderr.setEncoding('utf8').on('data', append);
        const [[code]] = await Promise.all([
          guardEvent(once(client, 'exit'), 'the client that asks three times to exit', () => printed),
          guardEvent(refused.promise, 'the refusing successor to bind', () => printed),
        ]);
        const answers = printed
          .split('\n')
          .filter((line) => line.startsWith('asked\t'))
          .map((line) => line.split('\t').slice(1));
        expect(
          answers.map(([request, outcome]) => `${request} ${outcome}`),
          printed,
        ).toEqual(['1 failed', '2 failed', '3 failed']);
        expect(answers[2]?.[2], printed).toBe('Daemon process terminated and closed the connection');
        expect(code, printed).toBe(0);
      } finally {
        // The record names this test process, and the fixture's retirement stops the daemon a record names.
        await rm(recordPath(workspace), { force: true });
        refusing.close();
        daemon.close();
      }
    },
    FIXTURE_WORKSPACE,
  );
});

/** A client of the installed daemon client that asks once and says how the request ended. */
const ASK_ONCE = `
const { writeSync } = require('node:fs');
const { daemonClient } = require(process.env.NX_DAEMON_CLIENT);
daemonClient.glob(['*.json'], []).then(
  () => writeSync(1, 'asked\\tanswered\\n'),
  (error) => writeSync(1, 'asked\\tfailed\\t' + String(error?.message ?? error).split('\\n')[0] + '\\n'),
).then(() => process.exit(0));
`;

/**
 * A client that sent its request to a socket whose daemon is gone (2026-10-25: a client waited at no CPU for a daemon
 * whose process no longer existed). Something else still holds the socket and accepts, and nothing answers. The
 * request fails once the client sees that the pid its record names is not running, naming that pid and the socket,
 * instead of waiting out the 20-minute keep-alive.
 */
it('fails a request naming the pid and socket when the recorded daemon is gone and its socket still accepts', async () => {
  await withNxFixture(
    'nx-daemon-validity-gone-',
    async ({ socketDirectory, workspace }) => {
      await fixtureWorkspace(workspace);
      const socketPath = join(socketDirectory, 'd.sock');
      await mkdir(socketDirectory, { recursive: true });
      // The listener a surviving process kept: it takes every connection and never writes.
      const listening = Promise.withResolvers<void>();
      const leftover = createServer(() => {}).once('error', listening.reject);
      leftover.listen(socketPath, listening.resolve);
      await listening.promise;
      // The daemon the record names: a process that has ended.
      const ended = spawn('node', ['-e', ''], { stdio: 'ignore' });
      const gonePid = ended.pid;
      if (gonePid === undefined) throw new Error('could not start the process the record names');
      await guardEvent(once(ended, 'exit'), `the recorded daemon ${gonePid} to end`, () => '');
      await mkdir(dirname(recordPath(workspace)), { recursive: true });
      const { version } = await installedManifest();
      await writeFile(recordPath(workspace), JSON.stringify({ processId: gonePid, socketPath, nxVersion: version }));
      try {
        const client = spawn('node', ['-e', ASK_ONCE], {
          cwd: workspace,
          env: { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_DAEMON_CLIENT: daemonClient },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        let printed = '';
        const append = (text: string): void => {
          printed += text;
        };
        client.stdout.setEncoding('utf8').on('data', append);
        client.stderr.setEncoding('utf8').on('data', append);
        const [code] = await guardEvent(
          once(client, 'exit'),
          'the client asking the gone daemon to exit',
          () => printed,
        );
        const asked = printed.split('\n').find((line) => line.startsWith('asked\t'));
        expect(asked, printed).toBe(
          `asked\tfailed\tNx daemon process ${gonePid} is not running, but its socket ${socketPath} took this client's GLOB request: nothing will answer it.`,
        );
        expect(code, printed).toBe(0);
      } finally {
        // The record names a pid nothing holds; the fixture's retirement must not signal a reuse of it.
        await rm(recordPath(workspace), { force: true });
        const closed = Promise.withResolvers<void>();
        leftover.close(() => closed.resolve());
        await closed.promise;
      }
    },
    FIXTURE_WORKSPACE,
  );
});
