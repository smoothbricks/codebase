import { afterEach, describe, expect, it } from 'bun:test';
import { rmSync } from 'node:fs';
import { createServer, type Server, type Socket } from 'node:net';
import { CPU_TOKENS_ENV, gatewayCpuBudget, runnerDemand, sizeRunner } from './cpu-tokens.js';

const servers: Server[] = [];
const sockets: string[] = [];

afterEach(() => {
  for (const server of servers.splice(0)) {
    server.close();
  }
  for (const socket of sockets.splice(0)) {
    rmSync(socket, { force: true });
  }
});

/** A gateway stand-in on a socket under /tmp (sun_path is 104 bytes; macOS links it to /private/tmp) that runs `serve` per client. */
async function gateway(serve: (client: Socket) => void): Promise<string> {
  const socket = `/tmp/smoo-cpu-${process.pid}-${sockets.length}.sock`;
  rmSync(socket, { force: true });
  const server = createServer({ allowHalfOpen: true }, serve);
  const { promise, resolve } = Promise.withResolvers<void>();
  server.listen(socket, resolve);
  await promise;
  servers.push(server);
  sockets.push(socket);
  return socket;
}

/** The runner command the plugin infers for a nextest piece, as `index.ts` builds it. */
const NEXTEST_PIECE =
  `extracted="$(node node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-nextest-extract.js .cache/nextest/archive.tar.zst)" && ` +
  `cargo --frozen nextest run --binaries-metadata "$extracted/m.json" --workspace-remap . -E 'package(core)' --partition hash:1/5 --no-tests=pass`;

describe('cowshed CPU budget client', () => {
  it('asks for what each runner would run at once', () => {
    expect(runnerDemand(NEXTEST_PIECE, undefined, 18)).toEqual({ kind: 'nextest', want: 18 });
    expect(runnerDemand('cargo-nextest nextest run --archive-file a.tar.zst', undefined, 12)).toEqual({
      kind: 'nextest',
      want: 12,
    });
    expect(runnerDemand('cargo nextest run --test-threads=4', undefined, 18)).toEqual({ kind: 'nextest', want: 4 });
    expect(runnerDemand('bun test --timeout=30000 --parallel=3', undefined, 18)).toEqual({
      kind: 'bun-parallel',
      want: 3,
    });
    expect(runnerDemand('bun test --parallel ./a.test.ts', undefined, 18)).toEqual({ kind: 'bun-parallel', want: 18 });
    // A runtime pinned by path is the same runner.
    expect(runnerDemand('../bun-runtime/.runtime/bun test --parallel=5', undefined, 18)).toEqual({
      kind: 'bun-parallel',
      want: 5,
    });
    // One bun process: `test.concurrent` inside it is not another CPU.
    expect(runnerDemand('bun test --timeout=30000 native.test.ts', undefined, 18)).toEqual({
      kind: 'process',
      want: 1,
    });
    expect(runnerDemand('cargo --frozen test --workspace', undefined, 18)).toEqual({ kind: 'cargo', want: 18 });
    expect(runnerDemand('napi build --platform --release', undefined, 8)).toEqual({ kind: 'cargo', want: 8 });
    expect(
      runnerDemand('sh ../../tooling/napi-build.sh x86_64-unknown-linux-gnu napi --bin cowshed', undefined, 18),
    ).toEqual({
      kind: 'cargo',
      want: 18,
    });
    expect(
      runnerDemand(
        'sh tooling/napi-build.sh aarch64-apple-darwin packages/cowshed/node_modules/.bin/napi',
        undefined,
        8,
      ),
    ).toEqual({
      kind: 'cargo',
      want: 8,
    });
    expect(runnerDemand('bun scripts/test-shard.ts 3', undefined, 18)).toEqual({ kind: 'process', want: 1 });
    // A declared parallelism outranks what the command names.
    expect(runnerDemand('bun scripts/test-shard.ts 3', 2, 18)).toEqual({ kind: 'process', want: 2 });
    expect(runnerDemand('cargo nextest run --test-threads=4', 6, 18)).toEqual({ kind: 'nextest', want: 6 });
  });

  it('sizes each runner to its grant', () => {
    expect(sizeRunner('nextest', NEXTEST_PIECE, 5)).toEqual({
      command: NEXTEST_PIECE,
      env: { [CPU_TOKENS_ENV]: '5', NEXTEST_TEST_THREADS: '5' },
    });
    expect(sizeRunner('nextest', 'cargo nextest run --test-threads 16 -E all()', 3).command).toBe(
      'cargo nextest run --test-threads=3 -E all()',
    );
    expect(sizeRunner('bun-parallel', 'bun test --parallel=3 --timeout=1', 2)).toEqual({
      command: 'bun test --parallel=2 --timeout=1',
      env: { [CPU_TOKENS_ENV]: '2' },
    });
    expect(sizeRunner('bun-parallel', 'bun test --parallel', 7).command).toBe('bun test --parallel=7');
    expect(sizeRunner('cargo', 'cargo test', 4).env).toEqual({
      [CPU_TOKENS_ENV]: '4',
      CARGO_BUILD_JOBS: '4',
      RUST_TEST_THREADS: '4',
    });
    expect(sizeRunner('process', 'bun scripts/x.ts', 1)).toEqual({
      command: 'bun scripts/x.ts',
      env: { [CPU_TOKENS_ENV]: '1' },
    });
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
    }
    await closed;
  });

  it('runs unbudgeted, said as absent, when no gateway listens', async () => {
    const grant = await gatewayCpuBudget(`/tmp/smoo-cpu-${process.pid}-none.sock`).take(4, '/w', 'x');
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
