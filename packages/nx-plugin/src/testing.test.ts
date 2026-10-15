import { expect, it } from 'bun:test';
import { existsSync, lstatSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, readFile, readlink, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { createConnection, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fixtureNxEnv, nxFixtureRoot, withNxFixture } from './__tests__/fixture-nx-env.js';
import {
  awaitExit,
  fixtureSocketDirectory,
  isRunning,
  NX_LONGEST_SOCKET_NAME,
  nxDaemonProcesses,
  ownedFixtureRoot,
  ownedWorkspaceSocketDirectory,
  type ProcessEntry,
  pidsWorkingIn,
  processTable,
  publishFixtureSocketDirectory,
  retireFixtureSocketDirectory,
  socketNamespace,
  stopNxDaemon,
  terminate,
  withDescendants,
} from './testing.js';

const repositoryRoot = join(import.meta.dir, '../../..');

/**
 * Make `workspace` an Nx workspace and start its daemon. The daemon and every
 * process it started, once `nx daemon --start` has returned with it up.
 */
async function startDaemon(
  workspace: string,
  socketDirectory: string,
  environment: Record<string, string> = {},
): Promise<ProcessEntry[]> {
  await mkdir(workspace, { recursive: true });
  await writeFile(join(workspace, 'package.json'), '{"name":"daemon-fixture","private":true}\n');
  await writeFile(join(workspace, '.gitignore'), 'node_modules\n.nx\n');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  expect(Bun.spawnSync(['git', 'init', '--quiet', workspace]).exitCode).toBe(0);
  const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--start'], {
    cwd: workspace,
    env: { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', ...environment },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect(code, stdout + stderr).toBe(0);
  const daemon = await nxDaemonProcesses(workspace);
  expect(daemon, stdout + stderr).not.toEqual([]);
  return daemon;
}

/** Which of `processes` still run, zombies (already exited) aside. */
async function stillRunning(processes: readonly ProcessEntry[]): Promise<string[]> {
  const live = (await processTable()).filter((entry) => !entry.stat.startsWith('Z')).map((entry) => entry.pid);
  return processes.filter((entry) => live.includes(entry.pid)).map((entry) => `${entry.pid} ${entry.command}`);
}

it('lists a process once when it descends from another of the pids it is asked about', () => {
  const table: ProcessEntry[] = [
    { pid: 1, ppid: 0, stat: 'Ss', command: 'launchd' },
    { pid: 10, ppid: 1, stat: 'Ss', command: 'sh -c sleep 600 & wait' },
    { pid: 11, ppid: 10, stat: 'S', command: 'sleep 600' },
    { pid: 12, ppid: 11, stat: 'S', command: 'sleep 1' },
    { pid: 20, ppid: 1, stat: 'S', command: 'bystander' },
  ];

  expect(
    withDescendants(table, [11, 10])
      .map((entry) => entry.pid)
      .sort((a, b) => a - b),
  ).toEqual([10, 11, 12]);
});

it('finds its Darwin working-directory holder without system sbin on PATH', async () => {
  if (process.platform !== 'darwin') return;
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cwd-restricted-path-')));
  try {
    const child = Bun.spawn(
      [
        process.execPath,
        '-e',
        `import { pidsWorkingIn } from ${JSON.stringify(join(import.meta.dir, 'testing.ts'))};
console.log(JSON.stringify(await pidsWorkingIn(process.cwd())));`,
      ],
      { cwd: root, env: { ...process.env, PATH: '/usr/bin:/bin' }, stdout: 'pipe', stderr: 'pipe' },
    );
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(code, stderr).toBe(0);
    expect(JSON.parse(stdout)).toContain(child.pid);
  } finally {
    await rm(root, { recursive: true });
  }
});

it('stops the Nx daemon of a fixture whose body throws, and deletes its socket alias and root', async () => {
  let root = '';
  let alias = '';
  let daemon: ProcessEntry[] = [];
  const thrown = withNxFixture('throwing-body-', async (fixture) => {
    root = fixture.root;
    alias = dirname(fixture.socketDirectory);
    expect(lstatSync(alias).isSymbolicLink()).toBe(true);
    daemon = await startDaemon(fixture.workspace, fixture.socketDirectory);
    throw new Error('the fixture body failed on purpose');
  });

  await expect(thrown).rejects.toThrow('the fixture body failed on purpose');
  expect(daemon).not.toEqual([]);
  expect(await stillRunning(daemon)).toEqual([]);
  expect(await pidsWorkingIn(root)).toEqual([]);
  expect(existsSync(root)).toBe(false);
  expect(lstatSync(alias, { throwIfNoEntry: false })).toBeUndefined();
}, 120_000);

it('ends a process its Nx daemon started as it exited, once the daemon is gone', async () => {
  let root = '';
  await withNxFixture('daemon-orphan-', async (fixture) => {
    root = fixture.root;
    const { workspace } = fixture;
    const pidFile = join(workspace, 'orphan.pid');
    const workerFile = join(workspace, 'worker.pid');
    // A stand-in daemon: `sleep 3600` is a worker it started, and it waits on that worker. On SIGTERM it starts
    // `sleep 60` as its last act and exits, so that child outlives the daemon, reparented to init.
    const daemon = Bun.spawn(
      [
        'sh',
        '-c',
        `trap 'sleep 60 & echo $! > "$1"; exit 0' TERM; sleep 3600 & echo $! > "$2"; echo ready; wait`,
        'sh',
        pidFile,
        workerFile,
      ],
      { cwd: workspace, stdout: 'pipe', stderr: 'ignore' },
    );
    try {
      const reader = daemon.stdout.getReader();
      const decoder = new TextDecoder();
      let output = '';
      while (!output.includes('ready\n')) {
        const next = await reader.read();
        if (next.done) {
          throw new Error(`stand-in daemon exited before it was ready: ${output}`);
        }
        output += decoder.decode(next.value);
      }
      reader.releaseLock();
      const record = join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json');
      await mkdir(dirname(record), { recursive: true });
      await writeFile(record, JSON.stringify({ processId: daemon.pid }));
      await stopNxDaemon(workspace, async () => {
        terminate(daemon.pid);
        await daemon.exited;
      });
      const orphan = Number(await readFile(pidFile, 'utf8'));
      expect(orphan).toBeGreaterThan(0);
      expect(await pidsWorkingIn(workspace)).toEqual([]);
      // A zombie has exited already; a signal probe would still count it as running.
      expect((await processTable()).filter((entry) => entry.pid === orphan && !entry.stat.startsWith('Z'))).toEqual([]);
    } finally {
      // Assert before retiring this probe, so a leaked orphan fails even though
      // every exit path still releases the real processes the test started.
      terminate(daemon.pid);
      await daemon.exited;
      const owned = [];
      for (const file of [workerFile, pidFile]) {
        const pid = existsSync(file) ? Number(await readFile(file, 'utf8')) : 0;
        if (pid > 0) {
          owned.push(pid);
          terminate(pid);
        }
      }
      await awaitExit(withDescendants(await processTable(), owned), `orphan probe of ${workspace}`);
    }
  });
  expect(await pidsWorkingIn(root)).toEqual([]);
}, 120_000);

it('binds and connects through its short alias without granting the long fixture root a socket namespace', async () => {
  let alias = '';
  await withNxFixture('short-socket-boundary-'.padEnd(100, 'x'), async ({ workspace, socketDirectory }) => {
    alias = dirname(socketDirectory);
    const socket = join(socketDirectory, NX_LONGEST_SOCKET_NAME);
    // The physical leaf is too long for Nx to bind, so only the alias can make this pass.
    expect(Buffer.byteLength(join(workspace, '.nx', 'sockets', 'nx', NX_LONGEST_SOCKET_NAME))).toBeGreaterThan(95);
    expect(Buffer.byteLength(socket)).toBeLessThanOrEqual(95);
    expect(lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(dirname(alias)).toBe(socketNamespace().directory);
    await mkdir(socketDirectory);
    expect(await realpath(socketDirectory)).toBe(join(workspace, '.nx', 'sockets', 'nx'));
    const server = createServer((client) => client.end('owned-socket-ok'));
    try {
      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(socket, resolve);
      });
      const received = await new Promise<string>((resolve, reject) => {
        const client = createConnection(socket);
        let text = '';
        client.on('data', (bytes) => {
          text += bytes.toString();
        });
        client.once('error', reject);
        client.once('end', () => resolve(text));
      });
      expect(received).toBe('owned-socket-ok');
    } finally {
      if (server.listening) {
        await new Promise<void>((resolve, reject) => {
          server.close((error) => (error === undefined ? resolve() : reject(error)));
        });
      }
    }
  });
  // The fixture's retirement unlinked its alias.
  expect(lstatSync(alias, { throwIfNoEntry: false })).toBeUndefined();
});

it('publishes no alias for a workspace outside its own fixture run, or one named through a link', async () => {
  const unowned = await realpath(await mkdtemp(join(tmpdir(), 'socket-unowned-')));
  const root = await nxFixtureRoot('socket-linked-');
  const linked = join(root, 'linked');
  try {
    await mkdir(join(root, 'real'));
    await symlink(join(root, 'real'), linked, 'dir');
    for (const workspace of [unowned, linked]) {
      await expect(publishFixtureSocketDirectory(workspace)).rejects.toThrow(
        'not a physical workspace in a fixture run this process owns',
      );
      expect(existsSync(join(workspace, '.nx'))).toBe(false);
      expect(lstatSync(dirname(fixtureSocketDirectory(workspace)), { throwIfNoEntry: false })).toBeUndefined();
    }
  } finally {
    await rm(unowned, { recursive: true });
    await rm(root, { recursive: true });
  }
});

it('refuses a symlinked socket-state ancestor before writing outside the fixture', async () => {
  const root = await nxFixtureRoot('socket-escape-boundary-');
  const foreign = await mkdtemp(join(tmpdir(), 'socket-foreign-boundary-'));
  try {
    await symlink(foreign, join(root, '.nx'), 'dir');
    await expect(publishFixtureSocketDirectory(root)).rejects.toThrow(
      'is not a real directory in its physical workspace',
    );
    expect(existsSync(join(foreign, 'sockets'))).toBe(false);
    expect(lstatSync(dirname(fixtureSocketDirectory(root)), { throwIfNoEntry: false })).toBeUndefined();
  } finally {
    await rm(root, { recursive: true });
    await rm(foreign, { recursive: true });
  }
});

it('neither uses nor removes a retargeted alias or a colliding directory at its alias name', async () => {
  const root = await nxFixtureRoot('socket-collision-');
  const foreign = await realpath(await mkdtemp(join(tmpdir(), 'socket-collision-foreign-')));
  const alias = dirname(fixtureSocketDirectory(root));
  try {
    await symlink(foreign, alias, 'dir');
    await expect(publishFixtureSocketDirectory(root)).rejects.toThrow('does not name its owned target');
    await expect(retireFixtureSocketDirectory(root)).rejects.toThrow('does not name its owned target');
    await expect(ownedWorkspaceSocketDirectory(root, 'a')).rejects.toThrow('does not name its owned target');
    expect(await readlink(alias)).toBe(foreign);
    expect(await readdir(foreign)).toEqual([]);
    await rm(alias);

    await mkdir(alias);
    await writeFile(join(alias, 'kept'), 'kept\n');
    await expect(publishFixtureSocketDirectory(root)).rejects.toThrow('does not name its owned target');
    await expect(retireFixtureSocketDirectory(root)).rejects.toThrow('does not name its owned target');
    expect(lstatSync(alias).isDirectory()).toBe(true);
    expect(await readFile(join(alias, 'kept'), 'utf8')).toBe('kept\n');
  } finally {
    await rm(alias, { recursive: true, force: true });
    await rm(root, { recursive: true });
    await rm(foreign, { recursive: true });
  }
});

it('refuses a socket namespace too long for Nx before publishing anything', async () => {
  const root = await nxFixtureRoot('socket-length-');
  const namespace = join(root, 'namespace-'.padEnd(80, 'x'));
  await mkdir(namespace);
  const outer = Object.fromEntries(
    ['NX_SOCKET_DIR', 'COWSHED_RUNTIME_LINK', 'TMPDIR'].map((name) => [name, process.env[name]]),
  );
  // Only the temp directory is left to fall back to, and it is too long.
  delete process.env.NX_SOCKET_DIR;
  delete process.env.COWSHED_RUNTIME_LINK;
  process.env.TMPDIR = namespace;
  try {
    expect(socketNamespace().directory).toBe(namespace);
    await expect(publishFixtureSocketDirectory(root)).rejects.toThrow('over the 95 a Unix socket may have');
    expect(existsSync(join(root, '.nx'))).toBe(false);
    expect(await readdir(namespace)).toEqual([]);
  } finally {
    for (const [name, value] of Object.entries(outer)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    await rm(root, { recursive: true });
  }
});

it('starts and connects to a fixture daemon through owned short aliases with a long private TMPDIR', async () => {
  await withNxFixture('long-tmp-socket-', async ({ workspace, socketDirectory }) => {
    const privateTemp = join(workspace, 'private-temp-'.padEnd(110, 'x'));
    await mkdir(privateTemp);
    expect(Buffer.byteLength(join(privateTemp, NX_LONGEST_SOCKET_NAME))).toBeGreaterThan(95);
    const daemon = await startDaemon(workspace, socketDirectory, { TMPDIR: privateTemp });
    const alias = dirname(socketDirectory);
    expect(lstatSync(alias).isSymbolicLink()).toBe(true);
    expect(lstatSync(socketDirectory).isDirectory()).toBe(true);
    expect(lstatSync(socketDirectory).isSymbolicLink()).toBe(false);
    expect(await realpath(socketDirectory)).toBe(join(workspace, '.nx', 'sockets', 'nx'));
    expect(Buffer.byteLength(join(socketDirectory, NX_LONGEST_SOCKET_NAME))).toBeLessThanOrEqual(95);
    const slot = await ownedWorkspaceSocketDirectory(workspace, 'a');
    expect(slot).toBe(join(alias, 'a'));
    expect(await realpath(slot)).toBe(join(workspace, '.nx', 'sockets', 'a'));
    expect(lstatSync(slot).isSymbolicLink()).toBe(false);
    expect(Buffer.byteLength(join(slot, NX_LONGEST_SOCKET_NAME))).toBeLessThanOrEqual(95);
    await expect(ownedWorkspaceSocketDirectory(workspace, 'a')).rejects.toThrow('EEXIST');
    await rm(slot, { recursive: true });
    const client = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'show', 'projects', '--json'], {
      cwd: workspace,
      env: { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', TMPDIR: privateTemp },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [code, stdout, stderr] = await Promise.all([
      client.exited,
      new Response(client.stdout).text(),
      new Response(client.stderr).text(),
    ]);
    expect(code, stdout + stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual([]);
    const record = JSON.parse(
      await readFile(join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json'), 'utf8'),
    );
    expect(daemon.some(({ pid }) => pid === record.processId)).toBe(true);
    expect(await stillRunning(daemon)).not.toEqual([]);
  });
}, 120_000);

/**
 * The alias a killed fixture leaves: what {@link publishFixtureSocketDirectory} makes, planted for a workspace in
 * another process's run, which this process may not publish for. `workspace` itself is not made.
 */
async function plantSocketAlias(workspace: string): Promise<string> {
  const directory = fixtureSocketDirectory(workspace);
  await symlink(join(workspace, '.nx', 'sockets'), dirname(directory), 'dir');
  return directory;
}

it("reclaims a dead run's Nx daemon, helper processes and socket aliases, and nothing of a live run", async () => {
  // A suite of its own, so this process's first fixture of it is the one that sweeps.
  const suite = `sweep-regression-${process.pid}`;
  const suiteDirectory = join(await realpath(tmpdir()), 'smoothbricks-fixtures', suite);
  // A pid that names no process: one this test started and saw exit.
  const exited = Bun.spawn(['true']);
  await exited.exited;
  const deadRun = join(suiteDirectory, `run-${exited.pid}`);
  // A run whose owner is alive: the process that started this test.
  const liveRun = join(suiteDirectory, `run-${process.ppid}`);
  const abandoned = join(deadRun, 'abandoned-fixture');
  const kept = join(liveRun, 'live-fixture');
  // A fixture of the dead run whose workspace is already gone: only its alias is left.
  const vanished = join(deadRun, 'vanished-fixture');
  await mkdir(join(kept, '.nx', 'sockets'), { recursive: true });
  await mkdir(join(abandoned, '.nx', 'sockets'), { recursive: true });
  const abandonedDirectory = await plantSocketAlias(abandoned);
  const abandonedAlias = dirname(abandonedDirectory);
  const keptAlias = dirname(await plantSocketAlias(kept));
  const vanishedAlias = dirname(await plantSocketAlias(vanished));
  // When the sweep signals the dead run's processes, it has not yet touched the run's aliases.
  const aliasAtSignal = join(suiteDirectory, 'alias-at-signal');
  const witness = Bun.spawn(
    [
      'sh',
      '-c',
      `trap 'if [ -L "$0" ]; then echo present; else echo gone; fi > "$1"; exit 0' TERM; while :; do sleep 1; done`,
      abandonedAlias,
      aliasAtSignal,
    ],
    { cwd: abandoned },
  );
  let daemon: ProcessEntry[] = [];
  const helper = Bun.spawn(['sleep', '600'], { cwd: abandoned });
  const bystander = Bun.spawn(['sleep', '600'], { cwd: kept });
  try {
    // A real daemon, as a killed test leaves it, beside a stand-in for the
    // helper processes (watchers, clients) that work in the same root.
    daemon = await startDaemon(abandoned, abandonedDirectory);

    const fresh = await ownedFixtureRoot(tmpdir(), suite, 'fresh-');

    expect(await stillRunning(daemon)).toEqual([]);
    expect(await helper.exited).not.toBe(0);
    expect(helper.signalCode).toBe('SIGTERM');
    expect(await witness.exited).toBe(0);
    expect(await readFile(aliasAtSignal, 'utf8')).toBe('present\n');
    expect(existsSync(deadRun)).toBe(false);
    expect(lstatSync(abandonedAlias, { throwIfNoEntry: false })).toBeUndefined();
    expect(lstatSync(vanishedAlias, { throwIfNoEntry: false })).toBeUndefined();
    expect(bystander.exitCode).toBeNull();
    expect(bystander.signalCode).toBeNull();
    expect(existsSync(kept)).toBe(true);
    expect(lstatSync(keptAlias).isSymbolicLink()).toBe(true);
    expect(await realpath(keptAlias)).toBe(join(await realpath(kept), '.nx', 'sockets'));
    expect(fresh.startsWith(join(suiteDirectory, `run-${process.pid}`, 'fresh-'))).toBe(true);
  } finally {
    for (const entry of [...daemon, { pid: helper.pid }, { pid: bystander.pid }, { pid: witness.pid }]) {
      terminate(entry.pid);
    }
    await Promise.all([helper.exited, bystander.exited, witness.exited]);
    for (const workspace of [abandoned, kept, vanished]) {
      await retireFixtureSocketDirectory(workspace);
    }
    await rm(suiteDirectory, { recursive: true, force: true });
  }
}, 120_000);

it('stops what works in every run it owns when the process is signalled, then dies of the signal', async () => {
  const suite = `signal-regression-${process.pid}`;
  const suiteDirectory = join(await realpath(tmpdir()), 'smoothbricks-fixtures', suite);
  // A second parent for the same suite: the owner holds a run in each, and one signal has to reclaim both.
  const otherParent = await mkdtemp(join(await realpath(tmpdir()), 'signal-regression-parent-'));
  const otherSuiteDirectory = join(otherParent, 'smoothbricks-fixtures', suite);
  const script = join(suiteDirectory, 'owner.ts');
  await mkdir(suiteDirectory, { recursive: true });
  // A detached stand-in for a fixture daemon in each run: its own session, so only the
  // owner's handler, not a process-group signal, can stop it. The interval
  // keeps the owner alive until it is signalled.
  await writeFile(
    script,
    `import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { ownedFixtureRoot } from ${JSON.stringify(join(import.meta.dir, 'testing.ts'))};
const helpers = [];
for (const parent of [tmpdir(), ${JSON.stringify(otherParent)}]) {
  const root = await ownedFixtureRoot(parent, ${JSON.stringify(suite)}, 'signalled-');
  const helper = spawn('sleep', ['600'], { cwd: root, detached: true, stdio: 'ignore' });
  helper.unref();
  helpers.push(helper.pid);
}
console.log('READY ' + helpers.join(' '));
setInterval(() => {}, 1 << 30);
`,
  );
  const owner = Bun.spawn([process.execPath, script], { stdout: 'pipe', stderr: 'pipe' });
  let helpers: number[] = [];
  try {
    const reader = owner.stdout.getReader();
    const decoder = new TextDecoder();
    let printed = '';
    while (helpers.length === 0) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`the owner exited before it was ready: ${printed}`);
      printed += decoder.decode(chunk.value, { stream: true });
      helpers = /^READY (\d+) (\d+)$/m.exec(printed)?.slice(1).map(Number) ?? [];
    }
    for (const helper of helpers) {
      expect(isRunning(helper)).toBe(true);
    }
    // One run for each parent, though both name one suite.
    const runs = [suiteDirectory, otherSuiteDirectory].map((directory) => join(directory, `run-${owner.pid}`));
    for (const run of runs) {
      expect(existsSync(run), run).toBe(true);
    }

    owner.kill('SIGTERM');
    await owner.exited;

    expect(owner.signalCode, await new Response(owner.stderr).text()).toBe('SIGTERM');
    expect(await stillRunning(helpers.map((pid) => ({ pid, ppid: 0, stat: '', command: 'sleep 600' })))).toEqual([]);
    for (const run of runs) {
      expect(existsSync(run), run).toBe(false);
    }
  } finally {
    for (const helper of helpers) terminate(helper);
    owner.kill('SIGKILL');
    await owner.exited;
    await rm(suiteDirectory, { recursive: true, force: true });
    await rm(otherParent, { recursive: true, force: true });
  }
}, 120_000);
