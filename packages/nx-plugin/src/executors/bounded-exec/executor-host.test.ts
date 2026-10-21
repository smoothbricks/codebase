import { afterEach, describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS, BOUNDED_TEST_KILL_AFTER_MS } from '../../bounded-test-policy.js';
import { CPU_POOL_ENV, type CpuPool, createCpuPool } from './cpu-pool.js';
import { CPU_TOKENS_ENV } from './cpu-tokens.js';
import type { BoundedExecOptions } from './schema.js';
import { VERDICT_FILE } from './verdict.js';

/**
 * The production runner's built Node artifact, run in its own process, with the real RAM volume and CPU-budget owners.
 * `test-host` already depends on this package's build; these peers must not compile TypeScript inside a test.
 * These proofs need the host (`hdiutil` on darwin), so the pure shards exclude this file.
 */
const PACKAGE_ROOT = fileURLToPath(new URL('../../..', import.meta.url));
const EXECUTOR = join(PACKAGE_ROOT, 'dist/executors/bounded-exec/executor.js');

/**
 * Names a child never inherits from the process that starts it: budget markers a bounded-exec parent sets for its
 * children, and the fixture socket names `fixtureNxEnv` drops. TMPDIR stays, because each run keeps the RAM volume it
 * was admitted to (`specs/cowshed/08_testing.md` 208-212).
 */
const WITHHELD_ENV = new Set([CPU_TOKENS_ENV, CPU_POOL_ENV, 'NX_SOCKET_DIR', 'NX_DAEMON_SOCKET_DIR']);

/** One line the sibling proof's barrier heard: a sibling reporting its start or its end with its granted tokens. */
type SiblingEvent = { readonly kind: 'start' | 'end'; readonly name: string; readonly tokens: number };

const EVENT = /^(start|end) ([a-z]+) ([0-9]+)$/;

/** The marker a runner's SIGUSR2 handler prints before the Node resources it still has active, as JSON. */
const ACTIVE_RESOURCES = 'cowshed-test active-resources ';

/** Churn workers a foreground proof runs beside: two, each cycling one lease at a time. */
const CHURN_NAMES = ['churna', 'churnb'] as const;
/** Written into a case's root to stop its churn workers; each checks it between runs, never mid-run. */
const CHURN_STOP = 'churn.stop';
/**
 * One churn run: `true` on one token, so each lease is a real acquire, run and release. Its `timeoutMs` is the leg
 * backstop: a run ends on its own exit, and only a hung run reaches the bound (`specs/cowshed/08_testing.md:511-514`).
 */
const CHURN_OPTIONS: BoundedExecOptions = { command: 'true', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, parallelism: 1 };
/** A churn worker's stdout: a `begin` line before each run it starts, a `ran` line after each run that succeeded. */
const CHURN_BEGIN = /^cowshed-test churn [a-z]+ begin [0-9]+$/gm;
const CHURN_RAN = /^cowshed-test churn [a-z]+ ran [0-9]+$/gm;
/** The stderr line a run logs when its one token came from the inherited parent pool. */
const INHERITED_GRANT = /inherited=true tokens=1\/1/g;

interface ChildRun {
  /** The script's file name, so an attribution names the case its child ran. */
  readonly label: string;
  readonly exited: Promise<number | null>;
  stdout(): string;
  stderr(): string;
  /** Whether the child has exited and closed its output streams. */
  closed(): boolean;
  /**
   * Sends SIGUSR2, which a runner answers on stdout with the Node resources it still has active. The answer is in
   * `stdout()` once the child has closed, unless the runner exited before its handler ran.
   */
  askResources(): void;
  /** Resolves once `marker` has been printed to stdout; rejects if the child exits without printing it. */
  printed(marker: string): Promise<void>;
  /** Resolves once `marker` has been written to stderr; rejects if the child exits without writing it. */
  said(marker: string): Promise<void>;
  /** Stops a surviving runner with SIGTERM, which it forwards to its command's process tree, and awaits its exit. */
  stop(): Promise<void>;
}

interface Barrier {
  readonly port: number;
  events(): readonly SiblingEvent[];
  malformed(): readonly string[];
  /**
   * Resolves once `ready` holds for the events heard so far; rejects if the barrier failed or heard a malformed line.
   */
  until(ready: (events: readonly SiblingEvent[]) => boolean): Promise<void>;
  go(name: string): void;
  close(): Promise<void>;
}

/** What a foreground proof saw of its churn while its body ran. */
interface Churn<T> {
  readonly value: T;
  /** Lease attempts in flight at body entry or begun while it ran: traffic sharing the ledger with the foreground. */
  readonly begun: number;
  readonly workers: readonly ChildRun[];
}

/** Parent pools a case has opened and not yet closed; afterEach closes what a case left open. */
const pools = new Set<CpuPool>();
/** Barriers a case has opened and not yet closed; afterEach closes what a case left open. */
const barriers = new Set<Barrier>();
const workspaces: string[] = [];
const children = new Set<ChildRun>();

describe('@smoothbricks/nx-plugin:bounded-exec host proofs', () => {
  afterEach(async () => {
    // A child still open here was not awaited by its case: the case failed before awaiting it. Its cumulative output is
    // written before anything waits, so a kill of the run during the waits below cannot lose it. Each live runner is
    // also asked, by SIGUSR2, what Node still has active; the answer lands in its stdout before the child closes.
    const live = [...children].filter((child) => !child.closed());
    for (const child of live) {
      process.stderr.write(attribution(child, 'before teardown'));
      child.askResources();
    }

    // Pools close first, then every runner still open is stopped: a closed grant never stops a command on its own.
    const outcomes = await Promise.all([...pools].map((pool) => pool.close()));
    pools.clear();
    await Promise.all([...children].map((child) => child.stop()));
    for (const child of live) {
      process.stderr.write(attribution(child, 'after teardown'));
    }
    children.clear();
    await Promise.all([...barriers].map((barrier) => barrier.close()));
    await Promise.all(workspaces.splice(0).map((root) => rm(root, { recursive: true, force: true })));
    for (const outcome of outcomes) {
      if (!outcome.ok) {
        throw new Error(`cpu pool did not close: ${outcome.error}`);
      }
    }
  });

  it('runs a nested command on its parent sub-pool, granting it no more than the sub-pool holds', async () => {
    const root = await createRoot();
    await withPool(4, async (pool) => {
      const leafDirectory = join(root, 'leaf');
      const leafScript = await writeRunner(
        root,
        'leaf',
        { command: `node -e "console.log('leaf')"`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, parallelism: 1 },
        leafDirectory,
      );
      const nestedDirectory = join(root, 'nested');
      const nestedCommand = `node ${JSON.stringify(leafScript)}`;
      // Churn draws from this pool beside the nested run. It can delay the nested grant, never widen the pool.
      const churn = await withChurn(root, pool.environment, async () => {
        const run = await startRunner(
          root,
          'nested',
          { command: nestedCommand, cwd: PACKAGE_ROOT, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, parallelism: 2 },
          nestedDirectory,
          childEnv(pool.environment),
        );
        await run.exited;
        return run;
      });
      const nested = churn.value;
      expect(await nested.exited, nested.stderr()).toBe(0);

      // The nested run logs its own grant before its command starts; the leaf's grant reaches the same stderr.
      const granted = [...nested.stderr().matchAll(/inherited=true tokens=(\d+)\//g)].map((match) => Number(match[1]));
      expect(granted).toHaveLength(2);
      const [nestedTokens = 0, leafTokens = 0] = granted;
      expect(nestedTokens).toBeGreaterThanOrEqual(1);
      expect(leafTokens).toBeGreaterThanOrEqual(1);
      expect(leafTokens).toBeLessThanOrEqual(nestedTokens);

      for (const directory of [nestedDirectory, leafDirectory]) {
        expect(await readFile(join(directory, VERDICT_FILE), 'utf8')).toMatch(/"inherited":\s*true/);
      }
      await expectChurn(churn);
    });
  });

  it('admits five concurrent RAM leases without blocking the lock holder behind filesystem workers', async () => {
    const root = await createRoot();
    await withPool(1, async (pool) => {
      const script = join(root, 'contended.mjs');
      await writeFile(
        script,
        [
          `import { runDefaultBoundedExec } from ${JSON.stringify(EXECUTOR)};`,
          RESOURCES_HANDLER,
          `const options = ${JSON.stringify({
            command: `node -e "console.log('lease')"`,
            cwd: PACKAGE_ROOT,
            timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
            parallelism: 1,
          })};`,
          `const context = ${JSON.stringify({ root })};`,
          'const results = await Promise.all(Array.from({ length: 5 }, () => runDefaultBoundedExec(options, context)));',
          "console.log('leases-complete');",
          'process.exitCode = results.every((result) => result.success) ? 0 : 1;',
          '',
        ].join('\n'),
      );
      // Five requests leave four contended opens beside the holder. Blocking opens consume all
      // four libuv workers, so even the holder's marker read cannot finish. The one-token pool
      // still serializes actual commands: five lease requests are not five runnable commands.
      const run = startChild(script, childEnv({ ...pool.environment, UV_THREADPOOL_SIZE: '4' }));
      expect(await run.exited, run.stderr()).toBe(0);
      expect(run.stdout().match(/^lease$/gm)).toHaveLength(5);
      expect(run.stdout()).toContain('leases-complete');
      expect([...run.stderr().matchAll(/inherited=true tokens=1\/1/g)]).toHaveLength(5);
    });
  });

  it('holds a parent pool to its count: three siblings on a pool of two never hold more than two', async () => {
    const root = await createRoot();
    await withPool(2, async (pool) => {
      const barrier = await startBarrier();
      try {
        // Churn draws from this pool beside the siblings. It can delay a sibling's admission, never widen the pool, so
        // the two-token peak below is still the pool's own count.
        const churn = await withChurn(root, pool.environment, async () => {
          const siblings = await Promise.all(
            ['a', 'b', 'c'].map((name) =>
              startRunner(
                root,
                name,
                {
                  command: siblingCommand(barrier.port, name),
                  timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
                  parallelism: 1,
                },
                join(root, name),
                childEnv(pool.environment),
              ),
            ),
          );

          // Two siblings are admitted together and the third waits for a release. Which two is not fixed, so the proof
          // releases whichever started first, then every other started sibling in turn.
          await barrier.until((events) => namesOf(events, 'start').length >= 2);
          const [first = ''] = namesOf(barrier.events(), 'start');
          barrier.go(first);
          await barrier.until((events) => events.some((event) => event.kind === 'end' && event.name === first));
          await barrier.until((events) => namesOf(events, 'start').length >= 3);
          const ended = new Set(namesOf(barrier.events(), 'end'));
          for (const name of namesOf(barrier.events(), 'start')) {
            if (!ended.has(name)) {
              barrier.go(name);
            }
          }
          await barrier.until((events) => namesOf(events, 'end').length >= 3);
          return siblings;
        });
        for (const sibling of churn.value) {
          expect(await sibling.exited).toBe(0);
        }

        const events = barrier.events();
        expect(barrier.malformed()).toEqual([]);
        expect(namesOf(events, 'start').sort()).toEqual(['a', 'b', 'c']);
        expect(namesOf(events, 'end').sort()).toEqual(['a', 'b', 'c']);

        let held = 0;
        let peak = 0;
        for (const event of events) {
          expect(Number.isSafeInteger(event.tokens)).toBe(true);
          expect(event.tokens).toBeGreaterThanOrEqual(1);
          held += event.kind === 'start' ? event.tokens : -event.tokens;
          expect(held).toBeGreaterThanOrEqual(0);
          expect(held).toBeLessThanOrEqual(2);
          peak = Math.max(peak, held);
        }
        expect(peak).toBe(2);
        expect(held).toBe(0);
        await expectChurn(churn);
      } finally {
        await barrier.close();
      }
    });
  });

  it('uses a real backing budget after its named pool is absent and returns the allocation', async () => {
    const root = await createRoot();
    const stale = await withPool(1, async (pool) => pool.environment);
    await withPool(1, async (outer) => {
      const options: BoundedExecOptions = {
        command: `node -e "console.log('actual-tokens=' + process.env.${CPU_TOKENS_ENV})"`,
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        parallelism: 1,
      };
      const directory = join(root, 'stale');
      const context = { root, record: { task: { id: 'stale', hash: null }, directory } };
      const script = join(root, 'stale.mjs');
      await writeFile(
        script,
        [
          `import assert from 'node:assert/strict';`,
          `import { runBoundedExec, createProcessTreeKiller } from ${JSON.stringify(EXECUTOR)};`,
          `import { readInheritedCpuPool, takeParentCpuTokens } from ${JSON.stringify(join(dirname(EXECUTOR), 'cpu-pool.js'))};`,
          RESOURCES_HANDLER,
          `const backing = readInheritedCpuPool(${JSON.stringify(outer.environment)});`,
          `const stale = readInheritedCpuPool(${JSON.stringify({ ...stale, [CPU_TOKENS_ENV]: '977' })});`,
          'assert(backing !== null && stale !== null);',
          `const options = ${JSON.stringify(options)};`,
          `const context = ${JSON.stringify(context)};`,
          'const asked = [];',
          'const allocations = [];',
          'let released = 0;',
          'const budget = { async take(want, checkout, command) {',
          '  asked.push({ want, checkout, command });',
          '  const grant = await takeParentCpuTokens(backing, want);',
          '  assert(grant.granted, grant.reason);',
          '  allocations.push(grant);',
          '  return { ...grant, release() { released++; grant.release(); } };',
          '} };',
          'const result = await runBoundedExec(options, context, createProcessTreeKiller(), null, budget, stale);',
          'assert(result.success, result.terminalOutput);',
          'assert.deepEqual(asked, [{ want: 1, checkout: context.root, command: options.command }]);',
          'assert.equal(released, 1);',
          'await allocations[0].closed;',
          'const returned = await takeParentCpuTokens(backing, 1);',
          'assert(returned.granted, returned.reason);',
          'assert.equal(returned.tokens, 1);',
          'returned.release();',
          'await returned.closed;',
          `console.log('actual-allocation-returned');`,
          '',
        ].join('\n'),
      );
      const run = startChild(script, childEnv(outer.environment));
      expect(await run.exited, run.stderr()).toBe(0);
      expect(run.stderr()).toContain('cowshed: cpu-tokens parent pool absent: ');
      expect(run.stderr()).toContain('status=ok inherited=true tokens=1/1');
      expect(run.stdout()).toContain('actual-tokens=1');
      expect(run.stdout()).toContain('actual-allocation-returned');
      expect(run.stdout()).not.toContain('977');
      expect(await readFile(join(directory, VERDICT_FILE), 'utf8')).toMatch(/"tokens":\s*1/);
    });
  });

  it('keeps a command running when its parent pool closes under it, and judges it by its own exit', async () => {
    const root = await createRoot();
    const go = join(root, 'go.fifo');
    expect(spawnSync('mkfifo', [go]).status).toBe(0);
    await withPool(2, async (pool) => {
      const run = await startRunner(
        root,
        'outlives',
        {
          command: `sh -c 'echo started; read -r line < "$0"; echo finished' '${go}'`,
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
          parallelism: 1,
        },
        join(root, 'outlives'),
        childEnv(pool.environment),
      );
      await run.printed('started');
      const outcome = await pool.close();
      expect(outcome.ok).toBe(true);
      await run.said('cowshed: cpu-tokens grant ended: the parent pool closed its 1-token grant');
      expect(run.closed()).toBe(false);
      await writeFile(go, 'go\n');
      expect(await run.exited, run.stderr()).toBe(0);
      expect(run.stdout()).toContain('finished');
      expect(await readFile(join(root, 'outlives', VERDICT_FILE), 'utf8')).toMatch(/"outcome":\s*"passed"/);
    });
  });

  it('fails a run whose sub-pool does not close cleanly, even when its command exits 0', async () => {
    const root = await createRoot();
    let stuck = '';
    try {
      await withPool(1, async (pool) => {
        const unremovable = await startRunner(
          root,
          'unremovable',
          { command: UNREMOVABLE_COMMAND, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, parallelism: 1 },
          join(root, 'unremovable'),
          childEnv(pool.environment),
        );
        expect(await unremovable.exited, unremovable.stderr()).toBe(1);
        stuck = unremovable.stdout().trim();
        expect(basename(dirname(stuck)).startsWith('cpu-pool-')).toBe(true);
        expect(unremovable.stderr()).toContain('cowshed: cpu pool did not close cleanly: ');
        const record: unknown = JSON.parse(await readFile(join(root, 'unremovable', VERDICT_FILE), 'utf8'));
        expect(record).toHaveProperty('verdict', { outcome: 'failed', exitCode: 1, tests: [] });

        // The failed run gave its one token back, so a sibling on the same one-token pool is admitted.
        const next = await startRunner(
          root,
          'next',
          {
            command: `node -e "console.log(process.env.${CPU_TOKENS_ENV})"`,
            timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
            parallelism: 1,
          },
          join(root, 'next'),
          childEnv(pool.environment),
        );
        expect(await next.exited, next.stderr()).toBe(0);
        expect(next.stderr()).toContain('inherited=true tokens=1/1');
      });
    } finally {
      // The sub-pool's directory is private to it; the pool could not remove `stuck`, so the proof removes it.
      if (stuck !== '' && basename(dirname(stuck)).startsWith('cpu-pool-')) {
        await chmod(stuck, 0o700);
        await rm(dirname(stuck), { recursive: true, force: true });
      }
    }
  });
});

async function createRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-exec-host-'));
  workspaces.push(root);
  return root;
}

/** The environment a child starts with: this process's, minus the withheld names, plus `extra`. */
function childEnv(extra: Readonly<Record<string, string>>): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value !== undefined && !WITHHELD_ENV.has(name)) {
      env[name] = value;
    }
  }
  return { ...env, ...extra };
}

/** A parent's CPU pool of `tokens`, open while `body` runs. Its upstream never closes on its own, and no run owns the
 * host waits its children report. */
async function withPool<T>(tokens: number, body: (pool: CpuPool) => Promise<T>): Promise<T> {
  const created = await createCpuPool(tokens, new Promise<void>(() => {}), { begin() {}, end() {} });
  if (!created.ok) {
    throw new Error(`cpu pool: ${created.error}`);
  }
  pools.add(created.pool);
  let result: { ok: true; value: T } | { ok: false; error: unknown };
  try {
    result = { ok: true, value: await body(created.pool) };
  } catch (error) {
    result = { ok: false, error };
  }
  const outcome = await created.pool.close();
  pools.delete(created.pool);
  if (!outcome.ok) {
    const failure = new Error(`cpu pool did not close: ${outcome.error}`);
    throw result.ok ? failure : new AggregateError([result.error, failure], 'CPU pool body and cleanup failed');
  }
  if (!result.ok) throw result.error;
  return result.value;
}

/** Installed in each runner first: SIGUSR2 makes it report the Node resources it still has active. Reads only. */
const RESOURCES_HANDLER = [
  "process.on('SIGUSR2', () => {",
  '  const resources = JSON.stringify(process.getActiveResourcesInfo());',
  `  process.stdout.write(${JSON.stringify(ACTIVE_RESOURCES)} + resources + '\\n');`,
  '});',
].join('\n');

/** Writes a script that runs `options` through the production runner, recording its verdict in `directory`. */
async function writeRunner(
  root: string,
  name: string,
  options: BoundedExecOptions,
  directory: string,
): Promise<string> {
  await mkdir(directory, { recursive: true });
  const context = { root, record: { task: { id: name, hash: null }, directory } };
  const script = join(root, `${name}.mjs`);
  await writeFile(
    script,
    [
      `import { runDefaultBoundedExec } from ${JSON.stringify(EXECUTOR)};`,
      RESOURCES_HANDLER,
      `const result = await runDefaultBoundedExec(${JSON.stringify(options)}, ${JSON.stringify(context)});`,
      'process.exitCode = result.success ? 0 : 1;',
      '',
    ].join('\n'),
  );
  return script;
}

async function startRunner(
  root: string,
  name: string,
  options: BoundedExecOptions,
  directory: string,
  env: NodeJS.ProcessEnv,
): Promise<ChildRun> {
  return startChild(await writeRunner(root, name, options, directory), env);
}

/**
 * Starts `script` with Node against the exact compiled artifact Nx's executor registration names.
 * Stdout and stderr are captured; `exited` resolves once the child has closed both streams.
 */
function startChild(script: string, env: NodeJS.ProcessEnv): ChildRun {
  const child = spawn('node', [script], {
    cwd: PACKAGE_ROOT,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  let err = '';
  let closed = false;
  const waiters = new Set<() => void>();
  const wake = (): void => {
    for (const waiter of [...waiters]) {
      waiter();
    }
  };
  child.stdout.on('data', (chunk: Buffer) => {
    out += chunk.toString('utf8');
    wake();
  });
  child.stderr.on('data', (chunk: Buffer) => {
    err += chunk.toString('utf8');
    wake();
  });
  const exited = new Promise<number | null>((resolve) => {
    const done = (code: number | null): void => {
      closed = true;
      resolve(code);
      wake();
    };
    child.once('error', (error) => {
      err += `${error.message}\n`;
      done(null);
    });
    child.once('close', (code) => done(code));
  });
  const appeared = (text: () => string, marker: string, what: string): Promise<void> => {
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const check = (): void => {
      if (text().includes(marker)) {
        waiters.delete(check);
        resolve();
      } else if (closed) {
        waiters.delete(check);
        reject(new Error(`child exited before ${what} ${JSON.stringify(marker)}; stderr:\n${err}`));
      }
    };
    waiters.add(check);
    check();
    return promise;
  };
  const run: ChildRun = {
    label: basename(script),
    exited,
    stdout: () => out,
    stderr: () => err,
    closed: () => closed,
    askResources(): void {
      child.kill('SIGUSR2');
    },
    printed: (marker) => appeared(() => out, marker, 'printing'),
    said: (marker) => appeared(() => err, marker, 'writing to stderr'),
    async stop(): Promise<void> {
      if (!closed) child.kill('SIGTERM');
      await exited;
    },
  };
  children.add(run);
  return run;
}

/**
 * What a runner has printed and written so far, named by its script and the stage it is attributed at. Cumulative and
 * untrimmed: a runner still open at teardown says what it was doing, not just that it never finished.
 */
function attribution(child: ChildRun, stage: string): string {
  return [
    `cowshed host proof ${stage}: ${child.label} ${child.closed() ? 'closed' : 'still open'}`,
    '--- stdout ---',
    child.stdout(),
    '--- stderr ---',
    child.stderr(),
    `--- end ${child.label} ---`,
    '',
  ].join('\n');
}

/**
 * The sibling's command: report `start NAME TOKENS` to the barrier, wait for `go`, then report `end NAME TOKENS`.
 * It is plain `node`, so it needs no preload.
 */
function siblingCommand(port: number, name: string): string {
  const tokens = `process.env.${CPU_TOKENS_ENV}`;
  const script = [
    `const s = require('node:net').connect(${port}, '127.0.0.1', () =>`,
    `s.write('start ${name} ' + ${tokens} + '\\n'));`,
    `let got = ''; s.on('data', (d) => { got += d;`,
    `if (got.includes('go\\n')) s.end('end ${name} ' + ${tokens} + '\\n'); });`,
  ].join(' ');
  return `node -e ${JSON.stringify(script)}`;
}

/**
 * Writes one churn worker: a loop that runs `CHURN_OPTIONS` through the production runner, one lease after another,
 * until `stopFile` exists. It prints a `begin` line before each run and a `ran` line after each success, so the parent
 * reads its progress from stdout, with no socket and no clock. A failed run exits the worker with the run's output.
 */
async function writeChurnWorker(root: string, name: string, stopFile: string): Promise<string> {
  const script = join(root, `${name}.mjs`);
  await writeFile(
    script,
    [
      "import { existsSync } from 'node:fs';",
      `import { runDefaultBoundedExec } from ${JSON.stringify(EXECUTOR)};`,
      RESOURCES_HANDLER,
      `const options = ${JSON.stringify(CHURN_OPTIONS)};`,
      `const context = ${JSON.stringify({ root })};`,
      `for (let run = 1; !existsSync(${JSON.stringify(stopFile)}); run += 1) {`,
      `  process.stdout.write('cowshed-test churn ${name} begin ' + run + '\\n');`,
      '  const result = await runDefaultBoundedExec(options, context);',
      '  if (!result.success) {',
      `    process.stderr.write('cowshed host proof churn ${name} failed at run ' + run + ':\\n');`,
      '    process.stderr.write(result.terminalOutput);',
      '    process.exitCode = 1;',
      '    break;',
      '  }',
      `  process.stdout.write('cowshed-test churn ${name} ran ' + run + '\\n');`,
      '}',
      '',
    ].join('\n'),
  );
  return script;
}

/**
 * Runs `body` beside two churn workers that draw from `poolEnvironment`'s parent pool, so churn competes for the same
 * tokens and the same RAM ledger as the foreground. `body` starts only after each worker has completed a run. The
 * workers are stopped and awaited before this returns, whatever `body` did, so no assertion sees a live churn child.
 */
async function withChurn<T>(
  root: string,
  poolEnvironment: Readonly<Record<string, string>>,
  body: () => Promise<T>,
): Promise<Churn<T>> {
  const stopFile = join(root, CHURN_STOP);
  const workers: { readonly name: string; readonly run: ChildRun }[] = [];
  try {
    for (const name of CHURN_NAMES) {
      const script = await writeChurnWorker(root, name, stopFile);
      workers.push({ name, run: startChild(script, childEnv(poolEnvironment)) });
    }
    for (const { name, run } of workers) {
      await run.printed(`cowshed-test churn ${name} ran 1`);
    }
    const marks = workers.map(({ run }) => {
      const output = run.stdout();
      const inFlight = [...output.matchAll(CHURN_BEGIN)].length - [...output.matchAll(CHURN_RAN)].length;
      return { run, mark: output.length, inFlight };
    });
    const value = await body();
    const begun = marks.reduce(
      (sum, { run, mark, inFlight }) => sum + inFlight + [...run.stdout().slice(mark).matchAll(CHURN_BEGIN)].length,
      0,
    );
    return { value, begun, workers: workers.map(({ run }) => run) };
  } finally {
    await writeFile(stopFile, '');
    await Promise.all(workers.map(({ run }) => run.exited));
  }
}

/**
 * Asserts, after `withChurn` has returned, that the churn ran on the parent pool and stopped cleanly. Each worker exited
 * 0 and completed a run, and logged an inherited grant for every run it completed: no churn run took tokens the fixture
 * pool did not hold. At least one lease was in flight at body entry or began while the foreground body was running.
 */
async function expectChurn(churn: Churn<unknown>): Promise<void> {
  const report = churn.workers.map((worker) => attribution(worker, 'after the churn stopped')).join('\n');
  expect(churn.begun, report).toBeGreaterThanOrEqual(1);
  for (const worker of churn.workers) {
    expect(await worker.exited, report).toBe(0);
    const ran = [...worker.stdout().matchAll(CHURN_RAN)];
    expect(ran.length, report).toBeGreaterThanOrEqual(1);
    expect([...worker.stderr().matchAll(INHERITED_GRANT)], report).toHaveLength(ran.length);
  }
}

/**
 * A command that makes its sub-pool's directory unremovable: it creates `stuck` under the pool's private directory,
 * puts a file in it, locks it (mode 000), and exits 0. The pool's cleanup cannot enter `stuck`, so its close fails.
 */
const UNREMOVABLE_SCRIPT = [
  "const fs = require('node:fs');",
  "const path = require('node:path');",
  "const stuck = path.join(path.dirname(process.env.BOUNDED_EXEC_CPU_POOL), 'stuck');",
  "fs.mkdirSync(stuck); fs.writeFileSync(path.join(stuck, 'held'), ''); fs.chmodSync(stuck, 0o000);",
  'console.log(stuck);',
].join(' ');
const UNREMOVABLE_COMMAND = `node -e ${JSON.stringify(UNREMOVABLE_SCRIPT)}`;

/** The names of the sibling events of `kind`, in the order the barrier heard them. */
function namesOf(events: readonly SiblingEvent[], kind: SiblingEvent['kind']): string[] {
  return events.filter((event) => event.kind === kind).map((event) => event.name);
}

/**
 * A TCP barrier the sibling proof drives. It stamps each sibling event in the order it hears it, so the proof never
 * needs a clock: a sibling's `start` and `end` are the only ordering it trusts. A connection error, a server error, or
 * a malformed line fails the barrier, and every pending `until` rejects with that failure.
 */
function startBarrier(): Promise<Barrier> {
  const events: SiblingEvent[] = [];
  const malformed: string[] = [];
  const sockets = new Map<string, Socket>();
  const connections = new Set<Socket>();
  const waiters = new Set<() => void>();
  let failure: Error | null = null;

  const wake = (): void => {
    for (const waiter of [...waiters]) {
      waiter();
    }
  };
  const fail = (error: Error): void => {
    failure ??= error;
    wake();
  };
  const record = (connection: Socket, line: string): void => {
    const match = EVENT.exec(line);
    if (match === null) {
      malformed.push(line);
      fail(new Error(`barrier heard a malformed line: ${JSON.stringify(line)}`));
      return;
    }
    const [, kind = '', name = '', tokens = ''] = match;
    if (kind === 'start') {
      sockets.set(name, connection);
    }
    events.push({ kind: kind === 'start' ? 'start' : 'end', name, tokens: Number(tokens) });
    wake();
  };

  const server = createServer((connection) => {
    connections.add(connection);
    connection.on('close', () => {
      connections.delete(connection);
    });
    connection.on('error', (error) => {
      fail(new Error(`barrier connection failed: ${error.message}`));
    });
    connection.setEncoding('utf8');
    let pending = '';
    connection.on('data', (chunk: string) => {
      const lines = (pending + chunk).split('\n');
      pending = lines.pop() ?? '';
      for (const line of lines) {
        record(connection, line);
      }
    });
  });
  server.on('error', (error) => {
    fail(new Error(`barrier server failed: ${error.message}`));
  });

  return new Promise<Barrier>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (address === null || typeof address === 'string') {
        reject(new Error('the barrier is not listening on a TCP port'));
        return;
      }
      const barrier: Barrier = {
        port: address.port,
        events: () => events,
        malformed: () => malformed,
        until(ready) {
          return new Promise<void>((done, rejectUntil) => {
            const check = (): void => {
              if (failure !== null) {
                waiters.delete(check);
                rejectUntil(failure);
              } else if (ready(events)) {
                waiters.delete(check);
                done();
              }
            };
            waiters.add(check);
            check();
          });
        },
        go(name) {
          const socket = sockets.get(name);
          if (socket === undefined) {
            throw new Error(`sibling ${JSON.stringify(name)} has not reported its start`);
          }
          socket.write('go\n');
        },
        close() {
          barriers.delete(barrier);
          for (const connection of connections) {
            connection.destroy();
          }
          return new Promise<void>((done, rejectClose) => {
            server.close((error) => (error === undefined ? done() : rejectClose(error)));
          });
        },
      };
      barriers.add(barrier);
      resolve(barrier);
    });
  });
}
