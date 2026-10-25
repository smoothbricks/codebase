import { afterEach, describe, expect, it } from 'bun:test';
import { rmSync } from 'node:fs';
import type { Server, Socket } from 'node:net';
import { join } from 'node:path';
import { closeServer, fakeClock, GRANT_TWO, listen, restartedBy, scripted } from '../../__tests__/gateway-stand-in.js';
import { ownedSocketDirectory } from '../../testing.js';
import {
  askAcrossInstall,
  CPU_TOKENS_ENV,
  type CpuBudget,
  gatewayCpuBudget,
  parseInheritedTokens,
  runnerDemand,
  sizeRunner,
} from './cpu-tokens.js';
import type { InstallRestart } from './gateway-lease.js';

const servers: Server[] = [];
const directories: string[] = [];

afterEach(async () => {
  for (const server of servers.splice(0)) {
    await closeServer(server);
  }
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

/**
 * A socket path in a directory of its own, made in the socket namespace the sandbox admits for Unix
 * sockets: a literal `/tmp` path binds, but no sandboxed process may connect to it, so a client of it
 * would see a refusal in place of the absent socket or the gateway the test means.
 */
async function socketPath(name: string): Promise<string> {
  const directory = await ownedSocketDirectory('cpu-', name);
  directories.push(directory);
  return join(directory, name);
}

/** A gateway stand-in on a socket in a directory of its own that runs `serve` per client. */
async function gateway(serve: (client: Socket) => void): Promise<string> {
  const socket = await socketPath('gw.sock');
  servers.push(await listen(socket, serve));
  return socket;
}

/** The runner command the plugin infers for a nextest piece, as `index.ts` builds it. */
const NEXTEST_PIECE =
  `extracted="$(node "$(realpath node_modules/@smoothbricks/nx-plugin)/../nextest-extract/dist/ts/bin/smoo-nx-nextest-extract.js" .cache/nextest/archive.tar.zst)" && ` +
  `cargo --frozen nextest run --binaries-metadata "$extracted/m.json" --workspace-remap . -E 'package(core)' --partition hash:1/5 --no-tests=pass`;

/** The test-thread ceilings of a run whose environment names none. */
const NO_CEILINGS = { rust: null, nextest: null };

describe('cowshed CPU budget client', () => {
  it('asks for what each runner would run at once', () => {
    expect(runnerDemand(NEXTEST_PIECE, undefined, 18, {})).toEqual({ kind: 'nextest', want: 18 });
    expect(runnerDemand('cargo-nextest nextest run --archive-file a.tar.zst', undefined, 12, {})).toEqual({
      kind: 'nextest',
      want: 12,
    });
    expect(runnerDemand('cargo nextest run --test-threads=4', undefined, 18, {})).toEqual({ kind: 'nextest', want: 4 });
    // `--no-capture` runs tests serially and ignores `--test-threads`: a host smoke of one test asks for one
    // token, not one per core it would hold idle while its own scratch jobs queue for the host's tokens.
    const smoke =
      "cargo --frozen nextest run --archive-file a.tar.zst -E 'test(smoke)' --run-ignored only --no-capture";
    expect(runnerDemand(smoke, undefined, 18, {})).toEqual({ kind: 'nextest', want: 1 });
    expect(runnerDemand(`${smoke} --test-threads 4`, undefined, 18, {})).toEqual({ kind: 'nextest', want: 1 });
    expect(runnerDemand('cargo nextest run -E all() -- --nocapture', undefined, 18, {})).toEqual({
      kind: 'nextest',
      want: 1,
    });
    // A flag that only shares the prefix is not it.
    expect(runnerDemand('cargo nextest run --no-capture-x', undefined, 18, {})).toEqual({ kind: 'nextest', want: 18 });
    // The same words in a cargo build are not nextest's.
    expect(runnerDemand('cargo test --workspace -- --nocapture', undefined, 18, {})).toEqual({
      kind: 'cargo',
      want: 18,
      testThreads: NO_CEILINGS,
    });
    expect(runnerDemand('bun test --timeout=0 --parallel=3', undefined, 18, {})).toEqual({
      kind: 'bun-parallel',
      want: 3,
    });
    expect(runnerDemand('bun test --parallel ./a.test.ts', undefined, 18, {})).toEqual({
      kind: 'bun-parallel',
      want: 18,
    });
    // A runtime pinned by path is the same runner.
    expect(runnerDemand('../bun-runtime/.runtime/bun test --parallel=5', undefined, 18, {})).toEqual({
      kind: 'bun-parallel',
      want: 5,
    });
    // One bun process: `test.concurrent` inside it is not another CPU.
    expect(runnerDemand('bun test --timeout=0 native.test.ts', undefined, 18, {})).toEqual({
      kind: 'process',
      want: 1,
      testThreads: NO_CEILINGS,
    });
    expect(runnerDemand('cargo --frozen test --workspace', undefined, 18, {})).toEqual({
      kind: 'cargo',
      want: 18,
      testThreads: NO_CEILINGS,
    });
    expect(runnerDemand('napi build --platform --release', undefined, 8, {})).toEqual({
      kind: 'cargo',
      want: 8,
      testThreads: NO_CEILINGS,
    });
    expect(runnerDemand('bun scripts/test-shard.ts 3', undefined, 18, {})).toEqual({
      kind: 'process',
      want: 1,
      testThreads: NO_CEILINGS,
    });
    // A declared parallelism outranks what the command names, within inherited ceilings.
    expect(runnerDemand('bun scripts/test-shard.ts 3', 2, 18, {})).toEqual({
      kind: 'process',
      want: 2,
      testThreads: NO_CEILINGS,
    });
    expect(runnerDemand('cargo nextest run --test-threads=4', 6, 18, {})).toEqual({ kind: 'nextest', want: 6 });
  });

  it("asks for Stryker's --concurrency, else its own default of one worker per core less one past four cores", () => {
    expect(runnerDemand('bun stryker run --concurrency 4', undefined, 18, {})).toEqual({ kind: 'stryker', want: 4 });
    expect(runnerDemand('bun stryker run -c 6', undefined, 18, {})).toEqual({ kind: 'stryker', want: 6 });
    expect(runnerDemand('npx stryker run --concurrency=3', undefined, 18, {})).toEqual({ kind: 'stryker', want: 3 });
    expect(runnerDemand('../../node_modules/.bin/stryker run -c 2', undefined, 18, {})).toEqual({
      kind: 'stryker',
      want: 2,
    });
    // Stryker takes the last count it is given.
    expect(runnerDemand('bun stryker run -c 8 --concurrency 5', undefined, 18, {})).toEqual({
      kind: 'stryker',
      want: 5,
    });
    // A share is of the cores, and never less than one worker.
    expect(runnerDemand('bun stryker run --concurrency=50%', undefined, 18, {})).toEqual({ kind: 'stryker', want: 9 });
    expect(runnerDemand('bun stryker run --concurrency 0%', undefined, 18, {})).toEqual({ kind: 'stryker', want: 1 });
    // Stryker's default, which sized nightly 477's sweeps to the whole host: n - 1 past four cores, else n.
    expect(runnerDemand('bun stryker run', undefined, 32, {})).toEqual({ kind: 'stryker', want: 31 });
    expect(runnerDemand('bun stryker run', undefined, 18, {})).toEqual({ kind: 'stryker', want: 17 });
    expect(runnerDemand('bun stryker run', undefined, 4, {})).toEqual({ kind: 'stryker', want: 4 });
    // Another command's `-c` in the same line is not Stryker's.
    expect(runnerDemand('ulimit -c 0 && bun stryker run', undefined, 18, {})).toEqual({ kind: 'stryker', want: 17 });
    expect(runnerDemand('bun stryker run && nice -c 3 true', undefined, 18, {})).toEqual({ kind: 'stryker', want: 17 });
    // A declared parallelism outranks the flag.
    expect(runnerDemand('bun stryker run --concurrency 8', 2, 18, {})).toEqual({ kind: 'stryker', want: 2 });
    // A test file named for Stryker is one bun process.
    expect(runnerDemand('bun test stryker.test.ts', undefined, 18, {})).toEqual({
      kind: 'process',
      want: 1,
      testThreads: NO_CEILINGS,
    });
  });

  it("asks for Vitest's --maxWorkers and go test's -p, else each runner's own per-core default", () => {
    // Vitest outside watch mode runs one worker per core less one.
    expect(runnerDemand('bun vitest run', undefined, 18, {})).toEqual({ kind: 'vitest', want: 17 });
    expect(runnerDemand('bun vitest run --config vitest.config.workers.ts', undefined, 18, {})).toEqual({
      kind: 'vitest',
      want: 17,
    });
    expect(runnerDemand('bun vitest run --maxWorkers=4', undefined, 18, {})).toEqual({ kind: 'vitest', want: 4 });
    expect(runnerDemand('vitest run --max-workers 50%', undefined, 18, {})).toEqual({ kind: 'vitest', want: 9 });
    expect(runnerDemand('bun vitest run --no-file-parallelism', undefined, 18, {})).toEqual({
      kind: 'vitest',
      want: 1,
    });
    // `go test -p` defaults to GOMAXPROCS, normally one per core.
    expect(runnerDemand('go test -count=1 ./hts/... ./host/...', undefined, 18, {})).toEqual({
      kind: 'go-test',
      want: 18,
    });
    expect(runnerDemand('cd go/ttscwire && go test ./...', undefined, 18, { GOMAXPROCS: '4' })).toEqual({
      kind: 'go-test',
      want: 4,
    });
    expect(runnerDemand('go test -p 3 -parallel 8 ./...', undefined, 18, {})).toEqual({ kind: 'go-test', want: 3 });
    // A shell word that is no count leaves the runner's default; it is still sized by the grant.
    expect(runnerDemand('bun ../scripts/gen-go-work.mjs && go test ./... -p="$(nproc)"', undefined, 18, {})).toEqual({
      kind: 'go-test',
      want: 18,
    });
  });

  it("asks for cargo-mutants' own jobserver, one task per core unless --jobserver-tasks names fewer", () => {
    // The plugin's inferred `mutation` command, with no flag: `--jobs` defaults to one cargo process, but its
    // jobserver defaults to one task per core (`n_tasks=<cores>`), shared by every cargo that process starts.
    expect(runnerDemand('cargo --frozen mutants --workspace', undefined, 18, {})).toEqual({
      kind: 'cargo-mutants',
      want: 18,
      jobs: 1,
    });
    // `--jobs` counts cargo processes, which still share that per-core jobserver.
    expect(runnerDemand('cargo mutants --jobs 4', undefined, 18, {})).toEqual({
      kind: 'cargo-mutants',
      want: 18,
      jobs: 4,
    });
    expect(runnerDemand('cargo mutants --jobserver-tasks 4', undefined, 18, {})).toEqual({
      kind: 'cargo-mutants',
      want: 4,
      jobs: 1,
    });
    expect(runnerDemand('cargo mutants -j2 --jobserver-tasks=4', undefined, 18, {})).toEqual({
      kind: 'cargo-mutants',
      want: 4,
      jobs: 2,
    });
    // Each cargo process runs at least one thread, whatever the jobserver allows.
    expect(runnerDemand('cargo mutants --jobs=6 --jobserver-tasks 4', undefined, 18, {})).toEqual({
      kind: 'cargo-mutants',
      want: 6,
      jobs: 6,
    });
    // `CARGO_MUTANTS_JOBS` as the command will see it: the target's own environment over the inherited one.
    expect(runnerDemand('cargo mutants --jobserver-tasks 4', undefined, 18, { CARGO_MUTANTS_JOBS: '3' })).toEqual({
      kind: 'cargo-mutants',
      want: 4,
      jobs: 3,
    });
    expect(
      runnerDemand(
        'cargo mutants --jobserver-tasks 4',
        undefined,
        18,
        { CARGO_MUTANTS_JOBS: '3' },
        { CARGO_MUTANTS_JOBS: '2' },
      ),
    ).toEqual({ kind: 'cargo-mutants', want: 4, jobs: 2 });
    expect(runnerDemand('cargo-mutants mutants --in-diff pr.diff', undefined, 12, {})).toEqual({
      kind: 'cargo-mutants',
      want: 12,
      jobs: 1,
    });
    // Another command's `-j` in the same line is not cargo-mutants'.
    expect(runnerDemand('cargo mutants --jobserver-tasks 4 && make -j 9', undefined, 18, {})).toEqual({
      kind: 'cargo-mutants',
      want: 4,
      jobs: 1,
    });
    expect(runnerDemand('cargo mutants', 3, 18, {})).toEqual({ kind: 'cargo-mutants', want: 3, jobs: 1 });
    // A package named `mutants` is an ordinary cargo build.
    expect(runnerDemand('cargo test -p mutants', undefined, 18, {})).toEqual({
      kind: 'cargo',
      want: 18,
      testThreads: NO_CEILINGS,
    });
  });

  it('sizes the worker-flag runners and cargo-mutants to their grant', () => {
    expect(sizeRunner({ kind: 'stryker', want: 17 }, 'bun stryker run', 4)).toEqual({
      command: 'bun stryker run --concurrency=4',
      env: { [CPU_TOKENS_ENV]: '4' },
    });
    expect(sizeRunner({ kind: 'stryker', want: 8 }, 'bun stryker run -c 8 --fileLogLevel off', 3).command).toBe(
      'bun stryker run --concurrency=3 --fileLogLevel off',
    );
    expect(
      sizeRunner({ kind: 'stryker', want: 8 }, 'ulimit -c 0 && bun stryker run --concurrency 8 && echo done', 2)
        .command,
    ).toBe('ulimit -c 0 && bun stryker run --concurrency=2 && echo done');
    // A grant larger than the ask sizes to the ask.
    expect(sizeRunner({ kind: 'stryker', want: 4 }, 'bun stryker run --concurrency=4', 9).command).toBe(
      'bun stryker run --concurrency=4',
    );
    expect(sizeRunner({ kind: 'cargo-mutants', want: 18, jobs: 1 }, 'cargo --frozen mutants --workspace', 5)).toEqual({
      command: 'cargo --frozen mutants --jobserver-tasks=5 --workspace',
      env: { [CPU_TOKENS_ENV]: '5', CARGO_BUILD_JOBS: '5', RUST_TEST_THREADS: '5', NEXTEST_TEST_THREADS: '5' },
    });
    // Its cargo processes share the grant: each tests on its share, and there are never more of them than tokens.
    expect(
      sizeRunner({ kind: 'cargo-mutants', want: 18, jobs: 4 }, 'cargo mutants --jobs 4 --jobserver-tasks 18', 8),
    ).toEqual({
      command: 'cargo mutants --jobs=4 --jobserver-tasks=8',
      env: {
        [CPU_TOKENS_ENV]: '8',
        CARGO_MUTANTS_JOBS: '4',
        CARGO_BUILD_JOBS: '2',
        RUST_TEST_THREADS: '2',
        NEXTEST_TEST_THREADS: '2',
      },
    });
    expect(sizeRunner({ kind: 'cargo-mutants', want: 18, jobs: 4 }, 'cargo mutants -j4', 2)).toEqual({
      command: 'cargo mutants --jobserver-tasks=2 --jobs=2',
      env: {
        [CPU_TOKENS_ENV]: '2',
        CARGO_MUTANTS_JOBS: '2',
        CARGO_BUILD_JOBS: '1',
        RUST_TEST_THREADS: '1',
        NEXTEST_TEST_THREADS: '1',
      },
    });
    // `--jobs` is never added: cargo-mutants refuses it beside `--in-place`.
    expect(sizeRunner({ kind: 'cargo-mutants', want: 18, jobs: 1 }, 'cargo mutants --in-place', 3).command).toBe(
      'cargo mutants --jobserver-tasks=3 --in-place',
    );
    expect(sizeRunner({ kind: 'vitest', want: 17 }, 'bun vitest run --config vitest.config.workers.ts', 3)).toEqual({
      command: 'bun vitest run --maxWorkers=3 --config vitest.config.workers.ts',
      env: { [CPU_TOKENS_ENV]: '3' },
    });
    expect(sizeRunner({ kind: 'vitest', want: 4 }, 'bun vitest run --maxWorkers 4', 2).command).toBe(
      'bun vitest run --maxWorkers=2',
    );
    expect(
      sizeRunner({ kind: 'go-test', want: 18 }, 'bun ../scripts/gen-go-work.mjs && go test ./... -p="$(nproc)"', 4)
        .command,
    ).toBe('bun ../scripts/gen-go-work.mjs && go test ./... -p=4');
    expect(sizeRunner({ kind: 'go-test', want: 18 }, 'cd go/ttscwire && go test ./...', 2).command).toBe(
      'cd go/ttscwire && go test -p=2 ./...',
    );
  });

  it('sizes each runner to its grant', () => {
    expect(sizeRunner({ kind: 'nextest', want: 18 }, NEXTEST_PIECE, 5)).toEqual({
      command: NEXTEST_PIECE,
      env: { [CPU_TOKENS_ENV]: '5', NEXTEST_TEST_THREADS: '5' },
    });
    expect(sizeRunner({ kind: 'nextest', want: 16 }, 'cargo nextest run --test-threads 16 -E all()', 3).command).toBe(
      'cargo nextest run --test-threads=3 -E all()',
    );
    expect(sizeRunner({ kind: 'bun-parallel', want: 3 }, 'bun test --parallel=3 --timeout=1', 2)).toEqual({
      command: 'bun test --parallel=2 --timeout=1',
      env: { [CPU_TOKENS_ENV]: '2' },
    });
    expect(sizeRunner({ kind: 'bun-parallel', want: 18 }, 'bun test --parallel', 7).command).toBe(
      'bun test --parallel=7',
    );
    // Cargo's rustc count is its pool's jobserver fifo, never a `CARGO_BUILD_JOBS` from the grant; its test threads
    // take no token, so they are sized here, libtest's and nextest's alike.
    expect(sizeRunner({ kind: 'cargo', want: 18, testThreads: NO_CEILINGS }, 'cargo test', 4).env).toEqual({
      [CPU_TOKENS_ENV]: '4',
      RUST_TEST_THREADS: '4',
      NEXTEST_TEST_THREADS: '4',
    });
    expect(sizeRunner({ kind: 'process', want: 1, testThreads: NO_CEILINGS }, 'bun scripts/x.ts', 1)).toEqual({
      command: 'bun scripts/x.ts',
      env: { [CPU_TOKENS_ENV]: '1', RUST_TEST_THREADS: '1', NEXTEST_TEST_THREADS: '1' },
    });
  });

  it("sizes a process's test threads to its held grant, under inherited and configured ceilings", () => {
    // A script's own `cargo test` takes rustc tokens from the executor's jobserver fifo, but its test threads take
    // none, so the executor sets them to the whole grant the process holds.
    const unbounded = runnerDemand('bun scripts/x.ts', 5, 18, {});
    expect(unbounded).toEqual({ kind: 'process', want: 5, testThreads: NO_CEILINGS });
    expect(sizeRunner(unbounded, 'bun scripts/x.ts', 5)).toEqual({
      command: 'bun scripts/x.ts',
      env: { [CPU_TOKENS_ENV]: '5', RUST_TEST_THREADS: '5', NEXTEST_TEST_THREADS: '5' },
    });
    const capped = runnerDemand(
      'bun scripts/x.ts',
      5,
      18,
      { RUST_TEST_THREADS: '4', NEXTEST_TEST_THREADS: '3' },
      { RUST_TEST_THREADS: '2', NEXTEST_TEST_THREADS: '64' },
    );
    // A configured count lowers an inherited ceiling and never raises it.
    expect(capped).toEqual({ kind: 'process', want: 5, testThreads: { rust: 2, nextest: 3 } });
    expect(sizeRunner(capped, 'bun scripts/x.ts', 5).env).toEqual({
      [CPU_TOKENS_ENV]: '5',
      RUST_TEST_THREADS: '2',
      NEXTEST_TEST_THREADS: '3',
    });
  });

  it('treats inherited runner bounds as ceilings even under larger grants', () => {
    const inherited = { NEXTEST_TEST_THREADS: '6', CARGO_BUILD_JOBS: '4', RUST_TEST_THREADS: '2' };
    const configured = { NEXTEST_TEST_THREADS: '64', CARGO_BUILD_JOBS: '64', RUST_TEST_THREADS: '64' };
    const nextest = runnerDemand('cargo nextest run --test-threads=18', 18, 18, inherited, configured);
    const cargo = runnerDemand('cargo test', 18, 18, inherited, configured);
    expect(nextest).toEqual({ kind: 'nextest', want: 6 });
    expect(cargo).toEqual({ kind: 'cargo', want: 4, testThreads: { rust: 2, nextest: 6 } });
    for (const tokens of [15, 18]) {
      expect(sizeRunner(nextest, 'cargo nextest run --test-threads=18', tokens)).toEqual({
        command: 'cargo nextest run --test-threads=6',
        env: { [CPU_TOKENS_ENV]: String(tokens), NEXTEST_TEST_THREADS: '6' },
      });
      expect(sizeRunner(cargo, 'cargo test', tokens).env).toEqual({
        [CPU_TOKENS_ENV]: String(tokens),
        RUST_TEST_THREADS: '2',
        NEXTEST_TEST_THREADS: '4',
      });
    }
    expect(sizeRunner(nextest, NEXTEST_PIECE, 3).env.NEXTEST_TEST_THREADS).toBe('3');
    expect(sizeRunner(cargo, 'cargo test', 3).env).toEqual({
      [CPU_TOKENS_ENV]: '3',
      RUST_TEST_THREADS: '2',
      NEXTEST_TEST_THREADS: '3',
    });
    expect(runnerDemand(NEXTEST_PIECE, 18, 18, inherited, { NEXTEST_TEST_THREADS: '2' }).want).toBe(2);
    expect(runnerDemand('cargo build', 18, 18, {}, { CARGO_BUILD_JOBS: '4' }).want).toBe(4);
    expect(runnerDemand(NEXTEST_PIECE, 18, 18, { NEXTEST_TEST_THREADS: '0' }).want).toBe(18);
  });

  it('reads an inherited count only as a positive safe integer', () => {
    expect(parseInheritedTokens(undefined)).toBeNull();
    for (const invalid of ['', '0', '-1', '1.5', '2e3', '0x10', ' 3', '3 ', '03', 'three', '9007199254740993']) {
      expect(parseInheritedTokens(invalid)).toBeNull();
    }
    expect(parseInheritedTokens('1')).toBe(1);
    expect(parseInheritedTokens('16')).toBe(16);
    expect(parseInheritedTokens(String(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER);
  });

  it('reads back the count a runner was sized to', () => {
    expect(
      parseInheritedTokens(
        sizeRunner({ kind: 'process', want: 5, testThreads: NO_CEILINGS }, 'bun scripts/x.ts', 5).env[CPU_TOKENS_ENV],
      ),
    ).toBe(5);
  });

  it('asks the gateway with one line, takes the granted count, and holds until released', async () => {
    const { promise: request, resolve: requested } = Promise.withResolvers<string>();
    const { promise: closed, resolve: close } = Promise.withResolvers<void>();
    const socket = await gateway((client) => {
      client.once('data', (chunk) => {
        requested(chunk.toString('utf8'));
        client.write('{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":6}\n');
      });
      // The gateway takes a lease back when it reads EOF. With allowHalfOpen the socket stays open
      // after the peer closes until the server ends it, so 'close' is not that signal on Linux.
      client.on('end', () => close());
    });
    const grant = await gatewayCpuBudget(socket).take(18, '/w/one', 'cargo nextest run');
    expect(JSON.parse(await request)).toEqual({
      op: 'cpu-tokens',
      want: 18,
      checkout: '/w/one',
      command: 'cargo nextest run',
    });
    expect(grant.granted && grant.tokens).toBe(6);
    if (grant.granted) {
      grant.release();
      // The client's side of the connection really closed, not merely its release call.
      await grant.closed;
    }
    await closed;
  });

  it('closes a granted lease when the gateway ends the connection', async () => {
    const socket = await gateway((client) => {
      client.once('data', () => {
        client.write('{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":2}\n');
        client.end();
      });
    });
    const grant = await gatewayCpuBudget(socket).take(2, '/w', 'x');
    if (!grant.granted) {
      throw new Error(grant.reason);
    }
    await grant.closed;
  });

  it('runs unbudgeted, said as absent, when no gateway listens', async () => {
    const grant = await gatewayCpuBudget(await socketPath('none.sock')).take(4, '/w', 'x');
    expect(grant.granted === false && grant.cause).toBe('absent');
  });

  it('recognizes a gateway that does not know the operation as predating the budget', async () => {
    const socket = await gateway((client) => {
      client.once('data', () =>
        client.end(
          '{"ok":false,"code":"invalid-request","error":"gateway control encoding failed: unknown gateway control operation"}\n',
        ),
      );
    });
    const grant = await gatewayCpuBudget(socket).take(4, '/w', 'x');
    expect(grant).toEqual({
      granted: false,
      cause: 'predates',
      reason:
        'the gateway predates the CPU budget (invalid-request: gateway control encoding failed: unknown gateway control operation); restart it with `cowshed setup`',
    });
  });

  it('refuses a grant larger than it asked for', async () => {
    const socket = await gateway((client) => {
      client.once('data', () =>
        client.write('{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":9}\n'),
      );
    });
    const grant = await gatewayCpuBudget(socket).take(4, '/w', 'x');
    expect(grant).toEqual({ granted: false, cause: 'other', reason: 'the gateway granted 9 tokens for 4' });
  });

  it('returns a grant when the process holding it is killed mid-leg', async () => {
    const { promise: closed, resolve: close } = Promise.withResolvers<void>();
    const { promise: granted, resolve: grant } = Promise.withResolvers<void>();
    const socket = await gateway((client) => {
      client.once('data', () => {
        client.write('{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":2}\n');
        grant();
      });
      client.on('end', () => close());
    });
    // The holder is its own process so SIGKILL is a real death. Its script imports the module by
    // path because `bun -e` has no module of its own to import from statically; the held lease's
    // open socket is what keeps it alive.
    const holder = Bun.spawn(
      [
        process.execPath,
        '-e',
        `const { gatewayCpuBudget } = await import(${JSON.stringify(new URL('./cpu-tokens.ts', import.meta.url).pathname)});
         const grant = await gatewayCpuBudget(${JSON.stringify(socket)}).take(2, '/w', 'leg');
         if (!grant.granted) process.exit(3);
         console.log('holding');`,
      ],
      { stdout: 'pipe', stderr: 'inherit' },
    );
    await granted;
    const reader = holder.stdout.getReader();
    const { value } = await reader.read();
    expect(new TextDecoder().decode(value)).toContain('holding');
    holder.kill('SIGKILL');
    await holder.exited;
    // The gateway sees the connection close, which is how it takes the tokens back.
    await closed;
  });
});

describe('cowshed CPU budget retry across an install', () => {
  /** The install that cut `budget`'s ask off; the test fails if the ask is answered any other way. */
  async function cutOffInstall(budget: CpuBudget): Promise<InstallRestart> {
    const answer = await budget.take(2, '/w', 'x');
    if (answer.granted || answer.cause !== 'install-restart') {
      throw new Error(`expected an ask cut off by an install restart, got ${JSON.stringify(answer)}`);
    }
    return answer.install;
  }

  it('retries a cut-off ask once against the gateway the install starts, and takes its grant', async () => {
    const socket = await socketPath('gw.sock');
    const restarting = await listen(socket, scripted([restartedBy(1234)]));
    servers.push(restarting);
    const budget = gatewayCpuBudget(socket);
    const install = await cutOffInstall(budget);
    let started = false;
    const clock = fakeClock(async () => {
      if (started) return;
      started = true;
      await closeServer(restarting);
      servers.push(await listen(socket, scripted([GRANT_TWO])));
    });
    const retried = await askAcrossInstall(() => budget.take(2, '/w', 'x'), install, clock);
    if (!retried.granted) throw new Error(JSON.stringify(retried));
    expect(retried.tokens).toBe(2);
    retried.release();
  });

  it('keeps asking while no gateway listens between the restart and the one it starts', async () => {
    const socket = await socketPath('gw.sock');
    const restarting = await listen(socket, scripted([restartedBy(1234)]));
    servers.push(restarting);
    const budget = gatewayCpuBudget(socket);
    const install = await cutOffInstall(budget);
    let sleeps = 0;
    const clock = fakeClock(async () => {
      sleeps += 1;
      if (sleeps === 1) {
        await closeServer(restarting);
      } else if (sleeps === 2) {
        servers.push(await listen(socket, scripted([GRANT_TWO])));
      }
    });
    const retried = await askAcrossInstall(() => budget.take(2, '/w', 'x'), install, clock);
    if (!retried.granted) throw new Error(JSON.stringify(retried));
    expect(sleeps).toBe(2);
    retried.release();
  });

  it('keeps asking while the install that cut the ask off still answers for its old gateway', async () => {
    const socket = await gateway(scripted([restartedBy(1234), restartedBy(1234), GRANT_TWO]));
    const budget = gatewayCpuBudget(socket);
    const install = await cutOffInstall(budget);
    const retried = await askAcrossInstall(() => budget.take(2, '/w', 'x'), install, fakeClock());
    if (!retried.granted) throw new Error(JSON.stringify(retried));
    expect(retried.tokens).toBe(2);
    retried.release();
  });

  it("refuses once the install's bound has passed, naming the install, its binary and the wait", async () => {
    let asks = 0;
    const socket = await gateway((client) => {
      asks += 1;
      client.once('data', () => client.end(restartedBy(1234, 250)));
    });
    const budget = gatewayCpuBudget(socket);
    const install = await cutOffInstall(budget);
    const refused = await askAcrossInstall(() => budget.take(2, '/w', 'x'), install, fakeClock());
    expect(refused).toEqual({
      granted: false,
      cause: 'install-deadline',
      reason:
        'the install 1234 (/abs/cowshed) restarting the gateway did not bring up one that serves within 250 ms; waited 300 ms',
      install,
      waitedMs: 300,
    });
    // The cut-off ask, then the retries at 100 ms and 200 ms; none at the 300 ms deadline.
    expect(asks).toBe(3);
  });

  it('never retries a refusal other than an install restart', async () => {
    let asks = 0;
    const socket = await gateway((client) => {
      asks += 1;
      client.once('data', () => client.end('{"ok":false,"code":"rejected","error":"the gateway is shutting down"}\n'));
    });
    const budget = gatewayCpuBudget(socket);
    const install = { pid: 1234, binary: '/abs/cowshed', servesWithinMs: 180_000 };
    const answer = await askAcrossInstall(() => budget.take(2, '/w', 'x'), install, fakeClock());
    expect(answer).toMatchObject({ granted: false, cause: 'other', code: 'rejected' });
    expect(asks).toBe(1);
  });

  it('returns an install restart that names another install as it is', async () => {
    const socket = await gateway(scripted([restartedBy(1234), restartedBy(99)]));
    const budget = gatewayCpuBudget(socket);
    const install = await cutOffInstall(budget);
    const answer = await askAcrossInstall(() => budget.take(2, '/w', 'x'), install, fakeClock());
    expect(answer).toMatchObject({ granted: false, cause: 'install-restart', install: { pid: 99 } });
  });

  it('refuses an install restart that names no install as an ordinary refusal', async () => {
    const socket = await gateway(scripted(['{"ok":false,"code":"install-restart","error":"the gateway restarted"}\n']));
    const grant = await gatewayCpuBudget(socket).take(2, '/w', 'x');
    if (grant.granted) throw new Error(`expected a refusal, got ${JSON.stringify(grant)}`);
    expect(grant).toMatchObject({ granted: false, cause: 'other', code: 'install-restart' });
    expect(grant.reason).toContain('the install it names is malformed: missing');
  });
});
