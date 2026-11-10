import { describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import fc from 'fast-check';
import type { RemoteCacheV2 } from 'nx/src/tasks-runner/default-tasks-runner.js';
import {
  CostBoundRemoteCache,
  describeRemoteCacheOutage,
  isWorthStoring,
  outputBytesOf,
  parseStoreSecondsPerMiB,
  REMOTE_CACHE_ANSWER_WITHIN_MS,
  type RemoteCacheDeadline,
  RemoteCacheOutageGuard,
  settleRemoteCacheRequest,
} from 'nx/src/tasks-runner/remote-cache-outage.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { bindListener, claimRefusingPort } from './testing.js';

/**
 * Guards the remote-cache outage hunk of `patches/nx@24.0.3.patch` (`patches/README.md`, "A remote cache outage is a
 * miss"). Stock Nx 24.0.3's native self-hosted cache client rejects when its server cannot be reached
 * (`Failed to send request …`), and the rejection failed the whole run; a server that accepts and never answers held
 * the run forever, since the client sets no timeout. The patched client reads either as an outage: the lookup misses,
 * the task runs, nothing more is asked of the server, and one line names the server and the cause. A 5xx answer is an
 * outage too; credentials and the other statuses the stock client reads as misconfiguration still fail the run.
 *
 * Drop the hunk, and keep this test, once the Nx version in use treats an unreachable cache as a miss.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

/** The native client's messages for a server that never answered (`native/cache/errors.rs`, `http_remote_cache.rs`). */
const UNREACHABLE = ['Failed to send request: ', 'Failed to read remote cache response body: '] as const;
/** What else the native client rejects with: configuration, archive and disk failures, which are not outages. */
const NATIVE_FATAL = [
  'Unauthorized: token rejected',
  'Misconfigured remote cache endpoint: Unexpected response status: 400 Bad Request',
  'Misconfigured remote cache endpoint: Requests should respond with text/plain on 401s',
  'Failed to read entries from tarball',
  'Refusing to extract cache entry with unsafe path: ../escape',
  'No space left on device (os error 28)',
] as const;

const DIAGNOSTIC = /^.*Remote cache .* is unavailable.*$/gm;

/**
 * The clock the in-process guards take in place of the run's timer: a deadline already elapsed. Each case holds its
 * answer off by construction (a remote that never settles, a server that answers only after the guard gave up), so
 * the deadline passing first is the case's subject, and the outage still names the run's own bound.
 */
const ELAPSED = (): RemoteCacheDeadline => ({ elapsed: Promise.resolve(), clear: () => {} });

describe('the outage decision', () => {
  const detail = fc.string({ unit: 'grapheme', maxLength: 80 });

  it('reads a request the server never answered as an outage named on one line', () => {
    fc.assert(
      fc.property(fc.constantFrom(...UNREACHABLE), detail, fc.string({ maxLength: 4 }), (prefix, text, gap) => {
        // reqwest's error chain arrives as `\n\nCaused by: …` paragraphs.
        const message = `${prefix}${text}\n\nCaused by:${gap}${text}`;
        const outcome = settleRemoteCacheRequest({ kind: 'rejected', error: new Error(message) });
        if (outcome.kind !== 'outage') throw new Error(`${JSON.stringify(message)} was ${outcome.kind}`);
        expect(outcome.cause).not.toMatch(/[\r\n]/);
        for (const token of message.split(/\s+/).filter((part) => part.length > 0)) {
          expect(outcome.cause).toContain(token);
        }
      }),
    );
  });

  it('rethrows every other rejection unchanged', () => {
    const other = fc.oneof(
      fc.constantFrom(...NATIVE_FATAL).map((message) => new Error(message)),
      detail
        .filter(
          (message) =>
            !UNREACHABLE.some((prefix) => message.startsWith(prefix)) && !message.startsWith('Misconfigured'),
        )
        .map((message) => new Error(message)),
      // A rejection that is no Error carries no native message to read.
      fc.oneof(fc.string(), fc.integer(), fc.constant(null), fc.constant(undefined)),
    );
    fc.assert(
      fc.property(other, (error) => {
        const outcome = settleRemoteCacheRequest({ kind: 'rejected', error });
        if (outcome.kind !== 'fatal') throw new Error(`${String(error)} was ${outcome.kind}`);
        expect(outcome.error).toBe(error);
      }),
    );
  });

  it('reads a 5xx status as an outage and every other unexpected status as misconfiguration', () => {
    fc.assert(
      fc.property(fc.integer({ min: 100, max: 599 }), detail, (status, reason) => {
        const error = new Error(`Misconfigured remote cache endpoint: Unexpected response status: ${status} ${reason}`);
        expect(settleRemoteCacheRequest({ kind: 'rejected', error }).kind).toBe(status >= 500 ? 'outage' : 'fatal');
      }),
    );
  });

  it('passes an answer through and reads silence as an outage naming its bound', () => {
    fc.assert(
      fc.property(fc.oneof(fc.boolean(), fc.constant(null)), (value) => {
        const outcome = settleRemoteCacheRequest({ kind: 'answered', value });
        if (outcome.kind !== 'answered') throw new Error(`an answer was ${outcome.kind}`);
        expect(outcome.value).toBe(value);
      }),
    );
    const silence = settleRemoteCacheRequest({ kind: 'unanswered', withinMs: REMOTE_CACHE_ANSWER_WITHIN_MS });
    expect(silence).toEqual({ kind: 'outage', cause: `no result within ${REMOTE_CACHE_ANSWER_WITHIN_MS} ms` });
  });

  it('describes an outage on one line naming the server, the request and the cause', () => {
    fc.assert(
      fc.property(detail, detail, detail, (server, request, cause) => {
        const line = describeRemoteCacheOutage(server, request, cause);
        expect(line).not.toMatch(/[\r\n]/);
        for (const token of [server, request, cause].flatMap((part) => part.split(/\s+/))) {
          expect(line).toContain(token);
        }
      }),
    );
  });
});

interface Run {
  readonly exitCode: number;
  readonly log: string;
  readonly result: string | null;
}

/** A fixture workspace with one cached `app:build` that copies its input to `app/result.txt`. */
async function buildFixture(workspace: string, command = 'cp source.txt result.txt'): Promise<void> {
  await mkdir(join(workspace, 'app'), { recursive: true });
  const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
  expect(git.exitCode, git.stderr.toString()).toBe(0);
  // The repository's own node_modules: the Nx under test.
  await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ cacheDirectory: '.nx/cache' }));
  await writeFile(
    join(workspace, 'app', 'project.json'),
    JSON.stringify({
      name: 'app',
      targets: {
        build: {
          executor: 'nx:run-commands',
          cache: true,
          inputs: ['{projectRoot}/source.txt'],
          outputs: ['{projectRoot}/result.txt'],
          options: { cwd: 'app', command },
        },
      },
    }),
  );
  await writeFile(join(workspace, 'app', 'source.txt'), 'built');
}

/** Build `app` against the cache at `server`. Spawned asynchronously: the server answers from this process. */
async function build(
  workspace: string,
  socketDirectory: string,
  server: string,
  extraEnv: Record<string, string> = {},
): Promise<Run> {
  const child = Bun.spawn(['bun', nxEntry, 'run', 'app:build', '--outputStyle=static'], {
    cwd: workspace,
    env: { ...fixtureNxEnv(workspace, socketDirectory), NX_SELF_HOSTED_REMOTE_CACHE_SERVER: server, ...extraEnv },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  const output = Bun.file(join(workspace, 'app', 'result.txt'));
  const result = (await output.exists()) ? await output.text() : null;
  return { exitCode, log: `${stdout}${stderr}`, result };
}

/** The task ran and passed, and the run said once which server it went on without, and why. */
function expectRanWithoutCache(run: Run, server: string, cause: string): void {
  expect(run.exitCode, run.log).toBe(0);
  expect(run.result, run.log).toBe('built');
  expect(run.log).not.toContain('[remote cache]');
  const diagnostics = run.log.match(DIAGNOSTIC) ?? [];
  expect(diagnostics, run.log).toHaveLength(1);
  expect(diagnostics[0]).toContain(server);
  expect(diagnostics[0]).toContain(cause);
}

/** A raw TCP listener in this job's port block; `onConnection` decides what a client hears. */
async function listen(onConnection: (socket: Socket) => void): Promise<{ server: Server; origin: string }> {
  const server = await bindListener((port) => {
    const { promise, resolve, reject } = Promise.withResolvers<Server>();
    const candidate = createServer(onConnection);
    candidate.once('error', reject);
    candidate.listen(port, '127.0.0.1', () => {
      candidate.off('error', reject);
      resolve(candidate);
    });
    return promise;
  });
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error(`not a TCP listener: ${String(address)}`);
  return { server, origin: `http://127.0.0.1:${address.port}` };
}

function close(server: Server): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  server.close((error) => (error ? reject(error) : resolve()));
  return promise;
}

/** Every request head a listener's clients sent, one per connection. */
function requestHeads(sockets: Set<Socket>, heads: string[]): (socket: Socket) => void {
  return (socket) => {
    sockets.add(socket);
    let head = '';
    socket.setEncoding('latin1').on('data', (chunk: string) => {
      if (head.includes('\r\n')) return;
      head += chunk;
      if (head.includes('\r\n')) heads.push(head.slice(0, head.indexOf('\r\n')));
    });
    socket.on('close', () => sockets.delete(socket));
  };
}

describe('a run against an unreachable remote cache', () => {
  it('builds when the cache port refuses connections', async () => {
    // The session port of a service port this test holds: nothing listens on it, and no other test binds it meanwhile.
    await using refused = await claimRefusingPort();
    const origin = `http://127.0.0.1:${refused.port}`;
    await withNxFixture('nx-cache-refused-', async ({ workspace, socketDirectory }) => {
      await buildFixture(workspace);
      expectRanWithoutCache(await build(workspace, socketDirectory, origin), origin, 'Failed to send request');
    });
  });

  it('builds when the cache server hangs up without answering, and asks it nothing more', async () => {
    const sockets = new Set<Socket>();
    const heads: string[] = [];
    const record = requestHeads(sockets, heads);
    const { server, origin } = await listen((socket) => {
      record(socket);
      // Read the request, then close the connection with no response.
      socket.once('data', () => socket.destroy());
    });
    try {
      await withNxFixture('nx-cache-hangup-', async ({ workspace, socketDirectory }) => {
        await buildFixture(workspace);
        expectRanWithoutCache(await build(workspace, socketDirectory, origin), origin, 'Failed to send request');
      });
      // The lookup only: after the outage the passed task's store is not attempted.
      expect(heads).toHaveLength(1);
      expect(heads[0]).toMatch(/^GET \/v1\/cache\/\S+ HTTP\/1\.1$/);
    } finally {
      await close(server);
    }
  });

  it('keeps the result of a task whose store the server hangs up on after a miss', async () => {
    const sockets = new Set<Socket>();
    const heads: string[] = [];
    const record = requestHeads(sockets, heads);
    const { server, origin } = await listen((socket) => {
      record(socket);
      socket.once('data', (chunk) => {
        if (chunk.toString('latin1').startsWith('GET ')) {
          socket.end('HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');
        } else {
          socket.destroy();
        }
      });
    });
    try {
      await withNxFixture('nx-cache-store-hangup-', async ({ workspace, socketDirectory }) => {
        await buildFixture(workspace);
        const run = await build(workspace, socketDirectory, origin);
        expectRanWithoutCache(run, origin, 'Failed to send request');
        expect(run.log.match(DIAGNOSTIC)?.[0]).toContain('PUT /v1/cache/');
      });
      // Nx looks the task up twice before it runs; after the store's outage it asks nothing more.
      expect(heads.length, JSON.stringify(heads)).toBeGreaterThan(1);
      for (const head of heads.slice(0, -1)) expect(head).toMatch(/^GET \/v1\/cache\/\S+ HTTP\/1\.1$/);
      expect(heads.at(-1)).toMatch(/^PUT \/v1\/cache\/\S+ HTTP\/1\.1$/);
    } finally {
      await close(server);
    }
  });

  it('misses a lookup the server never answers, says so once, and asks the server nothing more', async () => {
    const server = 'http://127.0.0.1:9';
    const asked: string[] = [];
    // A native client whose server accepted and never answered: its calls never settle.
    const silent = {
      retrieve(hash: string): Promise<never> {
        asked.push(`GET ${hash}`);
        return new Promise<never>(() => {});
      },
      store(hash: string): Promise<never> {
        asked.push(`PUT ${hash}`);
        return new Promise<never>(() => {});
      },
    };
    const warn = spyOn(console, 'warn').mockImplementation(() => {});
    const cacheDirectory = await mkdtemp(join(tmpdir(), 'nx-cache-stalled-'));
    try {
      const guard = new RemoteCacheOutageGuard(silent, server, ELAPSED);
      expect(await guard.retrieve('1234', cacheDirectory)).toBeNull();
      expect(await guard.store('1234', cacheDirectory, 'built', 0)).toBe(false);
      expect(await guard.retrieve('5678', cacheDirectory)).toBeNull();
      expect(asked).toEqual(['GET 1234']);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0]?.[0])).toContain(
        describeRemoteCacheOutage(server, 'GET /v1/cache/1234', `no result within ${REMOTE_CACHE_ANSWER_WITHIN_MS} ms`),
      );
    } finally {
      warn.mockRestore();
      await rm(cacheDirectory, { recursive: true, force: true });
    }
  });

  it('builds when the cache server answers that it cannot serve', async () => {
    let asked = 0;
    using server = await bindListener((port) =>
      Bun.serve({
        hostname: '127.0.0.1',
        port,
        fetch: () => {
          asked += 1;
          return new Response('down', { status: 503 });
        },
      }),
    );
    await withNxFixture('nx-cache-unavailable-', async ({ workspace, socketDirectory }) => {
      await buildFixture(workspace);
      expectRanWithoutCache(
        await build(workspace, socketDirectory, server.url.origin),
        server.url.origin,
        'Unexpected response status: 503',
      );
    });
    expect(asked).toBe(1);
  });

  it('still fails a run whose cache answers a status that is misconfiguration', async () => {
    using server = await bindListener((port) =>
      Bun.serve({ hostname: '127.0.0.1', port, fetch: () => new Response('wrong route', { status: 400 }) }),
    );
    await withNxFixture('nx-cache-misconfigured-', async ({ workspace, socketDirectory }) => {
      await buildFixture(workspace);
      const run = await build(workspace, socketDirectory, server.url.origin);
      expect(run.exitCode, run.log).not.toBe(0);
      expect(run.log).toContain('Misconfigured remote cache endpoint: Unexpected response status: 400');
      expect(run.log.match(DIAGNOSTIC)).toBeNull();
    });
  });
});

/**
 * Guards the store-bound hunk of `patches/nx@24.0.3.patch` (`patches/README.md`, "A shed stores what is worth its
 * bytes"). Cowshed's workspaces carry `NX_SELF_HOSTED_REMOTE_CACHE_STORE_SECONDS_PER_MIB`, so Nx's own client stores
 * each result it executed whose task took at least that many seconds per MiB of outputs, counting at least one MiB.
 * Every other client of the patched Nx, with the variable unset, stores as stock Nx does.
 */
const STORE_BOUND = 'NX_SELF_HOSTED_REMOTE_CACHE_STORE_SECONDS_PER_MIB';
const MIB = 1024 * 1024;

describe('the store bound', () => {
  it('reads an unset or empty bound as no bound and a number of seconds as itself', () => {
    expect(parseStoreSecondsPerMiB(undefined)).toBeNull();
    expect(parseStoreSecondsPerMiB('')).toBeNull();
    expect(parseStoreSecondsPerMiB('0')).toBe(0);
    expect(parseStoreSecondsPerMiB('1')).toBe(1);
    expect(parseStoreSecondsPerMiB('0.25')).toBe(0.25);
  });

  it('refuses a bound that is not a number of seconds, naming the variable and the value', () => {
    for (const refused of ['abc', '-1', 'NaN', 'Infinity', '1s', ' ']) {
      expect(() => parseStoreSecondsPerMiB(refused), refused).toThrow(`${STORE_BOUND} must be`);
      expect(() => parseStoreSecondsPerMiB(refused), refused).toThrow(JSON.stringify(refused));
    }
  });

  it('stores a result that took one second per MiB of outputs, counting at least one MiB', () => {
    // Outputs under a MiB count as one: a result must have taken the bound's seconds whatever its size.
    expect(isWorthStoring(999, 0, 1)).toBe(false);
    expect(isWorthStoring(1000, 0, 1)).toBe(true);
    expect(isWorthStoring(1000, MIB, 1)).toBe(true);
    expect(isWorthStoring(1000, MIB + 1, 1)).toBe(false);
    expect(isWorthStoring(5000, 5 * MIB, 1)).toBe(true);
    expect(isWorthStoring(4999, 5 * MIB, 1)).toBe(false);
    expect(isWorthStoring(0, 0, 0)).toBe(true);
  });

  it('never stores a result whose build time is unknown', () => {
    expect(isWorthStoring(Number.NaN, 0, 0)).toBe(false);
    expect(isWorthStoring(undefined, 0, 0)).toBe(false);
  });

  it('is monotone: a longer build or smaller output stays stored', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 600_000 }),
        fc.integer({ min: 0, max: 512 * MIB }),
        fc.double({ min: 0, max: 100, noNaN: true }),
        fc.integer({ min: 0, max: 600_000 }),
        fc.integer({ min: 0, max: 512 * MIB }),
        (builtInMs, bytes, bound, moreMs, lessBytes) => {
          if (isWorthStoring(builtInMs, bytes, bound)) {
            expect(isWorthStoring(builtInMs + moreMs, bytes, bound)).toBe(true);
            expect(isWorthStoring(builtInMs, Math.max(0, bytes - lessBytes), bound)).toBe(true);
          }
        },
      ),
    );
  });
});

describe('a client with a store bound', () => {
  /** A native stand-in that records each store it is asked for. */
  function recordingRemote(): { remote: RemoteCacheV2; stores: string[]; retrievals: string[] } {
    const stores: string[] = [];
    const retrievals: string[] = [];
    return {
      stores,
      retrievals,
      remote: {
        retrieve: async (hash: string) => {
          retrievals.push(hash);
          return null;
        },
        store: async (hash: string) => {
          stores.push(hash);
          return true;
        },
      },
    };
  }

  it('stores only what the bound asks, builds no archive for the rest, and always looks up', async () => {
    const { remote, stores, retrievals } = recordingRemote();
    const sizes = new Map([
      ['slow-small', 0],
      ['fast-small', 0],
      ['slow-bulky', 8 * MIB],
    ]);
    const client = new CostBoundRemoteCache(remote, 1, async (entry) => sizes.get(entry.split('/').pop() ?? '') ?? 0);
    expect(await client.retrieve('fast-small', '/cache')).toBeNull();
    expect(await client.store('slow-small', '/cache', '', 0, 1500)).toBe(true);
    expect(await client.store('fast-small', '/cache', '', 0, 200)).toBe(false);
    expect(await client.store('slow-bulky', '/cache', '', 0, 1500)).toBe(false);
    expect(retrievals).toEqual(['fast-small']);
    expect(stores).toEqual(['slow-small']);
  });

  it('measures the outputs it would archive: the bytes of the entry directory, symlinks not followed', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-store-bound-'));
    try {
      await mkdir(join(root, 'outputs/nested'), { recursive: true });
      await writeFile(join(root, 'outputs/a.bin'), new Uint8Array(1000));
      await writeFile(join(root, 'outputs/nested/b.bin'), new Uint8Array(234));
      await writeFile(join(root, 'terminalOutput'), 'ok');
      await symlink(join(root, 'outputs/a.bin'), join(root, 'outputs/link'));
      expect(await outputBytesOf(root)).toBe(1000 + 234 + 2);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('skips a store whose entry it cannot measure, and says why once', async () => {
    const { remote, stores } = recordingRemote();
    const warn = spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const client = new CostBoundRemoteCache(remote, 1, async () => {
        throw new Error('EIO: i/o error');
      });
      expect(await client.store('a', '/cache', '', 0, 60_000)).toBe(false);
      expect(await client.store('b', '/cache', '', 0, 60_000)).toBe(false);
      expect(stores).toEqual([]);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0]?.[0])).toContain('EIO: i/o error');
    } finally {
      warn.mockRestore();
    }
  });
});

/** A cache that answers every lookup 404 and every store 200, recording each request. */
function storingCache(): { origin: string; requests: string[]; stop: () => Promise<void> } {
  const requests: string[] = [];
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      requests.push(`${request.method} ${new URL(request.url).pathname}`);
      if (request.method === 'PUT') {
        await request.arrayBuffer();
        return new Response(null, { status: 200 });
      }
      return new Response('No cache entry for that key.\n', { status: 404 });
    },
  });
  return { origin: server.url.origin, requests, stop: () => server.stop(true) };
}

/** One `app:build` against a storing cache, with `env` added to the run's environment and `command` as its task. */
async function buildAgainstStoringCache(
  env: Record<string, string>,
  command?: string,
): Promise<{ run: Run; requests: string[] }> {
  const cache = storingCache();
  try {
    return await withNxFixture('nx-cache-store-bound-', async ({ workspace, socketDirectory }) => {
      await buildFixture(workspace, command);
      return { run: await build(workspace, socketDirectory, cache.origin, env), requests: cache.requests };
    });
  } finally {
    await cache.stop();
  }
}

const SLOW_BUILD = 'sleep 1.5 && cp source.txt result.txt';

describe('a run that stores what it executed', () => {
  it('sends the cache one store for a result worth its bytes', async () => {
    const { run, requests } = await buildAgainstStoringCache({ [STORE_BOUND]: '1' }, SLOW_BUILD);
    expect(run.exitCode, run.log).toBe(0);
    expect(run.result, run.log).toBe('built');
    expect(run.log.match(DIAGNOSTIC)).toBeNull();
    expect(requests.filter((request) => request.startsWith('PUT '))).toHaveLength(1);
  });

  it('sends the cache no store for a result that cost less to build than its bound', async () => {
    const { run, requests } = await buildAgainstStoringCache({ [STORE_BOUND]: '1', NX_VERBOSE_LOGGING: 'true' });
    expect(run.exitCode, run.log).toBe(0);
    expect(run.result, run.log).toBe('built');
    expect(run.log.match(DIAGNOSTIC)).toBeNull();
    // The lookups reached the cache; the task's store never did.
    expect(requests.length, JSON.stringify(requests)).toBeGreaterThan(0);
    for (const request of requests) expect(request).toMatch(/^GET \/v1\/cache\/\S+$/);
    // Verbose logging says why: the task, how long it took and what its outputs hold.
    expect(run.log).toMatch(
      new RegExp(`Remote cache: not storing \\d+, built in \\d+ ms with \\d+ bytes of outputs \\(${STORE_BOUND}=1\\)`),
    );
  });

  it('stores as stock Nx does when the bound is unset', async () => {
    const { run, requests } = await buildAgainstStoringCache({});
    expect(run.exitCode, run.log).toBe(0);
    expect(requests.filter((request) => request.startsWith('PUT '))).toHaveLength(1);
  });

  it('fails the run that is configured with a bound it cannot read', async () => {
    const { run, requests } = await buildAgainstStoringCache({ [STORE_BOUND]: 'soon' });
    expect(run.exitCode, run.log).not.toBe(0);
    expect(run.log).toContain(`${STORE_BOUND} must be`);
    expect(requests).toEqual([]);
  });
});

describe('a lookup the deadline overtook', () => {
  it('never unpacks its late artifact into the entry the task wrote meanwhile', async () => {
    const hash = '1234567890';
    const artifacts = new Map<string, ArrayBuffer>();
    const { promise: released, resolve: release } = Promise.withResolvers<void>();
    let lookups = 0;
    let lateAnswers = 0;
    using server = await bindListener((port) =>
      Bun.serve({
        hostname: '127.0.0.1',
        port,
        // Bun closes a connection idle for 10 s by default; the held lookup must outlast the client's deadline.
        idleTimeout: 0,
        async fetch(request) {
          const key = new URL(request.url).pathname.match(/^\/v1\/cache\/([^/]+)$/)?.[1];
          if (key === undefined) return new Response(null, { status: 404 });
          if (request.method === 'PUT') {
            artifacts.set(key, await request.arrayBuffer());
            return new Response(null, { status: 200 });
          }
          lookups += 1;
          // The first lookup is the exposed one, answered at once; the second is held.
          if (lookups === 2) {
            await released;
            lateAnswers += 1;
          }
          const artifact = artifacts.get(key);
          return artifact === undefined ? new Response(null, { status: 404 }) : new Response(artifact);
        },
      }),
    );
    await withNxFixture('nx-cache-late-', async ({ root, workspace, socketDirectory }) => {
      const child = Bun.spawn(['bun', join(import.meta.dir, '__tests__', 'remote-cache-late-lookup.ts'), root, hash], {
        env: { ...fixtureNxEnv(workspace, socketDirectory), NX_SELF_HOSTED_REMOTE_CACHE_SERVER: server.url.origin },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const stderr = new Response(child.stderr).text();
      const steps = child.stdout.pipeThrough(new TextDecoderStream()).getReader();
      let printed = '';
      while (printed.split('\n').length < 4) {
        const { done, value } = await steps.read();
        if (done) throw new Error(`the lookup process ended after ${JSON.stringify(printed)}: ${await stderr}`);
        printed += value;
      }
      expect(
        printed
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line)),
      ).toEqual([{ stored: true }, { exposedHit: true }, { guarded: null }]);
      expect(await readFile(join(root, 'exposed', hash, 'app', 'result.txt'), 'utf8')).toBe('late');

      // Release the held lookup with its artifact; the child reports when its native call settled.
      release();
      while (printed.split('\n').length < 5) {
        const { done, value } = await steps.read();
        if (done) break;
        printed += value;
      }
      const log = await stderr;
      expect(await child.exited, log).toBe(0);
      expect(JSON.parse(printed.trim().split('\n')[3] ?? 'null'), log).toEqual({ late: 'hit' });
      expect(log).toContain(`no result within ${REMOTE_CACHE_ANSWER_WITHIN_MS} ms`);
      expect(lateAnswers).toBe(1);
      const cache = join(root, 'cache');
      expect(await readFile(join(cache, hash, 'app', 'result.txt'), 'utf8')).toBe('local');
      // The late artifact went to staging, which is gone once its lookup settled or the process exited.
      expect((await readdir(cache)).sort()).toEqual([hash, 'remote-retrieve']);
      expect(await readdir(join(cache, 'remote-retrieve'))).toEqual([]);
    });
  });
});
