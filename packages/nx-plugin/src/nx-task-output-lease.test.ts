import { expect, it } from 'bun:test';
import { channel } from 'node:diagnostics_channel';
import { mkdirSync } from 'node:fs';
import { mkdir, readdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Subprocess } from 'bun';
import { TaskOutputLeases } from 'nx/src/tasks-runner/task-output-lease.js';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { runtimeInput } from './runtime-input.js';
import { bindListener } from './testing.js';

interface ProbeEvent {
  readonly promise: Promise<URL>;
  resolve(value: URL): void;
}

const repoRoot = join(import.meta.dir, '../../..');
const nx = join(repoRoot, 'node_modules/.bin/nx');
const binary = Buffer.from([0xff, 0xfe, 0x80, 0, 42]);
const fileCount = 64;

/** The producer pauses with an incomplete live tree; another client's capture must never see it. */
const producer = String.raw`
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
const label = process.env.OUTPUT_LABEL;
const event = (phase) => fetch(process.env.LEASE_PROBE + '/' + phase + '?label=' + label);
rmSync('app/dist', { recursive: true, force: true });
mkdirSync('app/dist', { recursive: true });
for (let index = 0; index < 2; index++) writeFileSync('app/dist/' + index + '.js', label);
await event('partial');
for (let index = 2; index < 64; index++) writeFileSync('app/dist/' + index + '.js', label);
writeFileSync('app/dist/binary.dat', Buffer.from([255, 254, 128, 0, 42]));
await event('built');
`;

/** Observe the real capture boundary, without replacing native put/copy or their result. */
const preload = String.raw`
const { channel } = require('node:diagnostics_channel');
const { DbCache } = require('nx/src/tasks-runner/cache');
const original = DbCache.prototype.put;
const event = (phase, hash = '') => fetch(process.env.LEASE_PROBE + '/' + phase + '?label=' + process.env.OUTPUT_LABEL + '&hash=' + hash);
channel('nx:task-output-lease').subscribe((message) => {
  if (message.phase === 'waiting' && message.targets.includes('app:build')) {
    event('waiting').catch((error) => { console.error(error); process.exitCode = 1; });
  }
});
DbCache.prototype.put = async function(task, ...args) {
  if (task.target.project === 'app' && task.target.target === 'build') {
    if (process.env.OUTPUT_LABEL === 'A') await event('capture');
    const result = await original.call(this, task, ...args);
    await event('captured', task.hash);
    return result;
  }
  return original.call(this, task, ...args);
};
`;

it('leases a target across clients through producer completion, cache capture and restore', async () => {
  await withNxFixture('nx-task-output-lease-', async ({ workspace, socketDirectory }) => {
    const observed = new Map<string, ProbeEvent>();
    const events: string[] = [];
    const signal = (name: string) => {
      let event = observed.get(name);
      if (event === undefined) {
        event = Promise.withResolvers<URL>();
        observed.set(name, event);
      }
      return event;
    };
    const capture = Promise.withResolvers<void>();
    const replacement = Promise.withResolvers<void>();
    const failed = Promise.withResolvers<never>();
    using server = await bindListener((port) =>
      Bun.serve({
        hostname: '127.0.0.1',
        port,
        async fetch(request) {
          const url = new URL(request.url);
          const key = `${url.pathname.slice(1)}:${url.searchParams.get('label')}`;
          events.push(key);
          signal(key).resolve(url);
          if (key === 'capture:A') await capture.promise;
          if (key === 'partial:B') await replacement.promise;
          return new Response('ok');
        },
      }),
    );
    await mkdir(join(workspace, 'app'));
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'produce.ts'), producer);
    await writeFile(join(workspace, 'probe.cjs'), preload);
    await writeFile(
      join(workspace, 'digest.ts'),
      String.raw`const bytes = await Bun.file('app/dist/binary.dat').bytes(); console.log(new Bun.CryptoHasher('sha256').update(bytes).digest('hex'));`,
    );
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ cacheDirectory: '.nx/cache' }));
    await writeFile(
      join(workspace, 'app/project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          build: {
            executor: 'nx:run-commands',
            cache: true,
            inputs: ['{workspaceRoot}/produce.ts', { env: 'OUTPUT_LABEL' }],
            outputs: ['{projectRoot}/dist'],
            options: { command: 'bun produce.ts' },
          },
          hash: {
            executor: 'nx:run-commands',
            cache: true,
            dependsOn: ['build'],
            inputs: [runtimeInput('bun digest.ts')],
            options: { command: "bun -e \"await Bun.write('hashed.ran', 'yes')\"" },
          },
        },
      }),
    );
    const env = fixtureNxEnv(workspace, socketDirectory);
    const children: Subprocess<'ignore', 'pipe', 'pipe'>[] = [];
    const run = (label: string, target = 'app:build') => {
      const child = Bun.spawn(
        ['bun', '--preload', join(workspace, 'probe.cjs'), nx, 'run', target, '--outputStyle=static'],
        {
          cwd: workspace,
          env: { ...env, OUTPUT_LABEL: label, LEASE_PROBE: server.url.origin },
          stdin: 'ignore',
          stdout: 'pipe',
          stderr: 'pipe',
        },
      );
      children.push(child);
      return Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]).then(
        (result) => {
          if (result[0] !== 0)
            failed.reject(new Error(`${label} ${target} exited ${result[0]}\n${result[1]}${result[2]}`));
          return result;
        },
      );
    };
    const waitFor = (name: string) =>
      guardEvent(Promise.race([signal(name).promise, failed.promise]), name, () => events.join('\n'));
    try {
      const first = run('A');
      await waitFor('capture:A');
      const second = run('B');
      // Before the repair the second producer reaches its partial live tree. With the lease it reports
      // waiting instead. Neither arm depends on a sleep or a guessed scheduler delay.
      await guardEvent(
        Promise.race([signal('waiting:B').promise, signal('partial:B').promise, failed.promise]),
        'the second client to reach the output boundary',
        () => events.join('\n'),
      );
      capture.resolve();
      const capturedA = await waitFor('captured:A');
      await waitFor('partial:B');
      replacement.resolve();
      const capturedB = await waitFor('captured:B');
      for (const result of [await first, await second]) expect(result[0], `${result[1]}${result[2]}`).toBe(0);
      for (const [label, event] of [
        ['A', capturedA],
        ['B', capturedB],
      ] as const) {
        const hash = event.searchParams.get('hash');
        if (hash === null) throw new Error(`no ${label} cache identity: ${event}`);
        const artifact = join(workspace, '.nx/cache', hash, 'app/dist');
        expect((await readdir(artifact)).length, `${label} artifact is incomplete`).toBe(fileCount + 1);
        for (let index = 0; index < fileCount; index++)
          expect(await readFile(join(artifact, `${index}.js`), 'utf8')).toBe(label);
        expect(await readFile(join(artifact, 'binary.dat'))).toEqual(binary);
      }
      // Returning to A must restore its whole artifact, and a runtime input must hash arbitrary file bytes
      // through its byte digest rather than emitting the fail-closed 0xFF stdout used for producer failures.
      const restored = await run('A', 'app:hash');
      expect(restored[0], `${restored[1]}${restored[2]}`).toBe(0);
      expect(await readFile(join(workspace, 'app/dist/0.js'), 'utf8')).toBe('A');
      expect(await readFile(join(workspace, 'app/dist/binary.dat'))).toEqual(binary);
      expect(await readFile(join(workspace, 'hashed.ran'), 'utf8')).toBe('yes');
      expect(events.filter((event) => event === 'built:A')).toHaveLength(1);
    } finally {
      capture.resolve();
      replacement.resolve();
      for (const child of children) {
        if (child.exitCode === null) child.kill('SIGKILL');
      }
      await Promise.all(children.map((child) => child.exited));
    }
  });
});

it('shares ownership across configurations without blocking independent targets, and cancels lock waiters', async () => {
  const owner = new TaskOutputLeases(2, null, 'output');
  const other = new TaskOutputLeases(2, null, 'output');
  const task = {
    id: 'lease:build:first',
    target: { project: `lease-${crypto.randomUUID()}`, target: 'build', configuration: 'first' },
    overrides: {},
    outputs: ['app/dist'],
    cache: true,
  };
  const release = await owner.acquire([task]);
  let releaseOther: (() => Promise<void>) | undefined;
  try {
    const waiting = Promise.withResolvers<void>();
    const onWaiting = () => waiting.resolve();
    const events = channel('nx:task-output-lease');
    events.subscribe(onWaiting);
    let acquired = false;
    const sameTarget = other
      .acquire([{ ...task, target: { ...task.target, configuration: 'second' } }])
      .then((releaseLease) => {
        acquired = true;
        return releaseLease;
      });
    await waiting.promise;
    events.unsubscribe(onWaiting);
    await other.withTasks([{ ...task, target: { ...task.target, target: 'independent' } }], async () => {
      expect(acquired).toBe(false);
    });
    await release();
    releaseOther = await sameTarget;
    await releaseOther();
    await expect(
      other.withTasks([task], async () => {
        throw new Error('producer refused');
      }),
    ).rejects.toThrow('producer refused');
    // The failed producer's finally released ownership, so another client can acquire it.
    const heldAgain = await owner.acquire([task]);
    try {
      const refused = other.acquire([task]).then(
        () => null,
        (error: unknown) => error,
      );
      await other.cancelPending();
      const reason = await refused;
      expect(reason).toBeInstanceOf(Error);
      if (!(reason instanceof Error)) throw new Error('the cancelled waiter unexpectedly acquired ownership');
      expect(reason.message).toContain('worker exited');
    } finally {
      await heldAgain();
    }
  } finally {
    await releaseOther?.();
    await release();
    await Promise.all([owner.close(), other.close()]);
  }
});

/** One output target of its own, so persistent lock files from other runs never contend. */
function leaseTask(target: string) {
  return {
    id: `${target}:build`,
    target: { project: `${target}-${crypto.randomUUID()}`, target: 'build' },
    overrides: {},
    outputs: ['app/dist'],
    cache: true,
  };
}

/** The helper pid a held lease's lock file names; the file is the helper's own record. */
async function leaseHolder(path: string): Promise<number> {
  const record: unknown = JSON.parse(await readFile(path, 'utf8'));
  if (typeof record !== 'object' || record === null || !('pid' in record) || typeof record.pid !== 'number') {
    throw new Error(`lease file ${path} names no helper pid`);
  }
  return record.pid;
}

/** The lock path of the next lease `acquire` asks for, read from the diagnostics channel it publishes before waiting. */
function nextLeasePath(): Promise<string> {
  const path = Promise.withResolvers<string>();
  const events = channel('nx:task-output-lease');
  const onEvent = (message: unknown) => {
    if (typeof message !== 'object' || message === null || !('phase' in message) || message.phase !== 'waiting') return;
    events.unsubscribe(onEvent);
    if ('paths' in message && Array.isArray(message.paths) && typeof message.paths[0] === 'string')
      path.resolve(message.paths[0]);
    else path.reject(new Error(`waiting event names no lock path: ${JSON.stringify(message)}`));
  };
  events.subscribe(onEvent);
  return path.promise;
}

/** Whether process `pid` is reaped: the parent's exit handler has then run in the same libuv callback. */
async function reaped(pid: number): Promise<void> {
  for (;;) {
    try {
      process.kill(pid, 0);
    } catch {
      return;
    }
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
}

it('wakes a waiter with the slot of a helper that died holding a lease, failing only the holder', async () => {
  const leases = new TaskOutputLeases(1, null, 'output');
  try {
    const heldPath = nextLeasePath();
    const release = await leases.acquire([leaseTask('held')]);
    const helper = await leaseHolder(await heldPath);
    // The one slot is taken, so this waits for it.
    const waiting = leases.acquire([leaseTask('queued')]);
    process.kill(helper, 'SIGKILL');
    await expect(release()).rejects.toThrow('worker exited (code null, signal SIGKILL)');
    // The helper's exit freed its slot: the waiter forks a successor rather than waiting for a release no one can send.
    const releaseQueued = await guardEvent(waiting, "a waiter for a dead helper's slot", () => 'no successor helper');
    await releaseQueued();
  } finally {
    await leases.close();
  }
});

it('retires a helper that replied with an error, so the next acquire takes its slot', async () => {
  const leases = new TaskOutputLeases(1, null, 'output');
  try {
    // The helper cannot open a lock path that is a directory, so it replies with an error and serves no more.
    const blockedPath = nextLeasePath().then((path) => mkdirSync(path, { recursive: true }));
    const refused = leases.acquire([leaseTask('unopenable')]);
    await blockedPath;
    await expect(refused).rejects.toThrow('failed:');
    const release = await guardEvent(
      leases.acquire([leaseTask('after-error')]),
      'an acquire after a helper replied with an error',
      () => 'the failed helper still holds the only slot',
    );
    await release();
  } finally {
    await leases.close();
  }
});

it('does not hand an idle helper that died to the next acquire', async () => {
  const leases = new TaskOutputLeases(1, null, 'output');
  try {
    const idlePath = nextLeasePath();
    const release = await leases.acquire([leaseTask('idle')]);
    const helper = await leaseHolder(await idlePath);
    await release();
    process.kill(helper, 'SIGKILL');
    await reaped(helper);
    const releaseNext = await guardEvent(
      leases.acquire([leaseTask('after-idle-death')]),
      'an acquire after the idle helper died',
      () => 'no successor helper',
    );
    await releaseNext();
  } finally {
    await leases.close();
  }
});

it('refuses an ancestor-owned target before waiting for its output lease', async () => {
  await withNxFixture('nx-task-output-loop-', async ({ workspace, socketDirectory }) => {
    await mkdir(join(workspace, 'app'));
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}');
    await writeFile(
      join(workspace, 'app/project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          loop: {
            executor: 'nx:run-commands',
            cache: false,
            outputs: ['{projectRoot}/dist'],
            options: { command: `bun ${JSON.stringify(nx)} run app:loop --outputStyle=static` },
          },
        },
      }),
    );
    const child = Bun.spawn(['bun', nx, 'run', 'app:loop', '--outputStyle=static'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace, socketDirectory),
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
    });
    try {
      const [status, stdout, stderr] = await guardEvent(
        Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]),
        'recursive target refusal before lease acquisition',
        () => `client ${child.pid}, exit ${child.exitCode}`,
      );
      expect(status, `${stdout}${stderr}`).not.toBe(0);
      expect(`${stdout}${stderr}`).toContain('Recursive task output lease');
      expect(`${stdout}${stderr}`).toContain('ancestor client');
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      await child.exited;
    }
  });
});

it('shares one continuous producer between simultaneous cold clients without waiting for its lifetime lease', async () => {
  await withNxFixture('nx-task-output-continuous-', async ({ workspace, socketDirectory }) => {
    const entered = new Set<string>();
    const bothEntered = Promise.withResolvers<void>();
    const start = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const started = Promise.withResolvers<URL>();
    const shared = Promise.withResolvers<URL>();
    const events: string[] = [];
    using server = await bindListener((port) =>
      Bun.serve({
        hostname: '127.0.0.1',
        port,
        async fetch(request) {
          const url = new URL(request.url);
          const client = url.searchParams.get('client') ?? '';
          events.push(`${url.pathname}:${client}`);
          if (url.pathname === '/enter') {
            entered.add(client);
            if (entered.size === 2) bothEntered.resolve();
            await start.promise;
          } else if (url.pathname === '/started') {
            started.resolve(url);
            await finish.promise;
          } else if (url.pathname === '/shared') {
            shared.resolve(url);
          }
          return new Response('ok');
        },
      }),
    );
    await mkdir(join(workspace, 'app'));
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}');
    await writeFile(
      join(workspace, 'app/project.json'),
      JSON.stringify({
        name: 'app',
        targets: {
          serve: {
            executor: 'nx:run-commands',
            continuous: true,
            cache: false,
            outputs: ['{projectRoot}/dist'],
            options: { command: 'bun serve.ts' },
          },
        },
      }),
    );
    await writeFile(
      join(workspace, 'serve.ts'),
      String.raw`import { mkdirSync, writeFileSync } from 'node:fs';
mkdirSync('app/dist', {recursive:true});
writeFileSync('app/dist/pid', String(process.pid));
await fetch(process.env.LEASE_PROBE + '/started?client=' + process.env.LEASE_CLIENT + '&pid=' + process.pid);`,
    );
    await writeFile(
      join(workspace, 'continuous-probe.cjs'),
      String.raw`const {TaskOrchestrator} = require('nx/src/tasks-runner/task-orchestrator');
const {SharedRunningTask} = require('nx/src/tasks-runner/running-tasks/shared-running-task');
const event = (phase) => fetch(process.env.LEASE_PROBE + '/' + phase + '?client=' + process.env.LEASE_CLIENT);
const originalStart = TaskOrchestrator.prototype.startContinuousTask;
TaskOrchestrator.prototype.startContinuousTask = async function(...args) {
  await event('enter');
  return originalStart.apply(this, args);
};
const originalWait = SharedRunningTask.prototype.waitForTaskToFinish;
SharedRunningTask.prototype.waitForTaskToFinish = async function(...args) {
  await event('shared');
  return originalWait.apply(this, args);
};`,
    );
    const children: Subprocess<'ignore', 'pipe', 'pipe'>[] = [];
    const env = fixtureNxEnv(workspace, socketDirectory);
    const run = (client: string) => {
      const child = Bun.spawn(
        ['bun', '--preload', join(workspace, 'continuous-probe.cjs'), nx, 'run', 'app:serve', '--outputStyle=static'],
        {
          cwd: workspace,
          env: { ...env, LEASE_PROBE: server.url.origin, LEASE_CLIENT: client },
          stdin: 'ignore',
          stdout: 'pipe',
          stderr: 'pipe',
        },
      );
      children.push(child);
      return Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    };
    try {
      const first = run('A');
      const second = run('B');
      await guardEvent(bothEntered.promise, 'both cold continuous clients at the startup boundary', () =>
        events.join('\n'),
      );
      start.resolve();
      const [producerEvent, sharedEvent] = await guardEvent(
        Promise.all([started.promise, shared.promise]),
        'one continuous producer and one shared waiter while the producer is alive',
        () => events.join('\n'),
      );
      expect(entered.size).toBe(2);
      expect(producerEvent.searchParams.get('client')).not.toBe(sharedEvent.searchParams.get('client'));
      expect(events.filter((event) => event.startsWith('/started:'))).toHaveLength(1);
      const producerPid = producerEvent.searchParams.get('pid');
      if (producerPid === null) throw new Error(`continuous producer omitted its pid: ${producerEvent}`);
      expect(await readFile(join(workspace, 'app/dist/pid'), 'utf8')).toBe(producerPid);
      finish.resolve();
      const results = await guardEvent(
        Promise.all([first, second]),
        'both continuous clients to finish after their one producer exits',
        () => events.join('\n'),
      );
      for (const [status, stdout, stderr] of results) expect(status, `${stdout}${stderr}`).toBe(0);
    } finally {
      start.resolve();
      finish.resolve();
      for (const child of children) {
        if (child.exitCode === null) child.kill('SIGTERM');
      }
      await Promise.all(children.map((child) => child.exited));
    }
  });
});
