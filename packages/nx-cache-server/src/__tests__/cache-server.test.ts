import { afterEach, describe, expect, test } from 'bun:test';
import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type CacheServerConfig, cacheServerConfigFromEnv } from '../config.js';
import { type RunningCacheServer, startCacheServer } from '../server.js';
import { type CacheStore, isCacheKey, openCacheStore, stageEntry } from '../store.js';

const WRITE_SECRET = 'write-secret-token';
const READ_SECRET = 'read-secret-token';
const KEY = '10018087211778381691';

const live: RunningCacheServer[] = [];
const scratch: string[] = [];
const stores: CacheStore[] = [];

afterEach(async () => {
  await Promise.all(live.splice(0).map((server) => server.stop()));
  await Promise.all(stores.splice(0).map((store) => store.close()));
  await Promise.all(scratch.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

async function scratchDirectory(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'nx-cache-server-'));
  scratch.push(root);
  const directory = join(root, 'entries');
  await mkdir(directory);
  return directory;
}

async function serve(
  overrides: Partial<CacheServerConfig> = {},
): Promise<{ server: RunningCacheServer; directory: string }> {
  const directory = overrides.directory ?? (await scratchDirectory());
  const started = await startCacheServer({
    directory,
    host: '127.0.0.1',
    port: 0,
    maxBytes: 8 * 1024 * 1024,
    capBytes: 64 * 1024 * 1024,
    principals: [
      { label: 'ci', bearer: `Bearer ${WRITE_SECRET}`, capability: 'write' },
      { label: 'pr', bearer: `Bearer ${READ_SECRET}`, capability: 'read' },
    ],
    ...overrides,
  });
  if (!started.ok) throw new Error(`${started.stage}: ${started.reason}`);
  const server = started.server;
  live.push(server);
  return { server, directory };
}

const put = async (
  server: RunningCacheServer,
  key: string,
  body: Uint8Array<ArrayBuffer>,
  secret = WRITE_SECRET,
): Promise<Response> =>
  fetch(`${server.origin}/v1/cache/${key}`, {
    method: 'PUT',
    body,
    headers: {
      authorization: `Bearer ${secret}`,
      'content-type': 'application/octet-stream',
    },
  });

/** `null` sends no `authorization` header at all; a default parameter would swallow `undefined`. */
const get = async (server: RunningCacheServer, key: string, secret: string | null = WRITE_SECRET) =>
  fetch(`${server.origin}/v1/cache/${key}`, {
    headers: secret === null ? {} : { authorization: `Bearer ${secret}` },
  });

function filled(size: number, byte: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(size);
  bytes.fill(byte);
  return bytes;
}

describe('artifact round trip', () => {
  test('serves back the exact bytes that were published', async () => {
    const { server } = await serve();
    const artifact = crypto.getRandomValues(new Uint8Array(64 * 1024));

    const stored = await put(server, KEY, artifact);
    expect(stored.status).toBe(200);

    const fetched = await get(server, KEY);
    expect(fetched.status).toBe(200);
    expect(fetched.headers.get('content-type')).toBe('application/octet-stream');
    expect(new Uint8Array(await fetched.arrayBuffer())).toEqual(artifact);
  });

  test('reports a miss for a key that was never published', async () => {
    const { server } = await serve();
    expect((await get(server, '404040404040404040')).status).toBe(404);
  });
});

describe('a cache entry is never replaced', () => {
  test('answers 409 and keeps the original bytes', async () => {
    const { server } = await serve();
    const original = filled(4096, 0xa1);

    expect((await put(server, KEY, original)).status).toBe(200);

    const second = await put(server, KEY, filled(4096, 0xb2));
    expect(second.status).toBe(409);

    expect(new Uint8Array(await (await get(server, KEY)).arrayBuffer())).toEqual(original);
  });

  test('admits exactly one of many concurrent writers and stores that body whole', async () => {
    const { server, directory } = await serve();
    // Large enough that each upload takes several chunks, so the writes genuinely interleave rather
    // than being serialised by luck.
    const markers = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    const size = 1024 * 1024;

    const statuses = await Promise.all(
      markers.map(async (marker) => (await put(server, KEY, filled(size, marker))).status),
    );

    expect(statuses.filter((status) => status === 200)).toHaveLength(1);
    expect(statuses.filter((status) => status === 409)).toHaveLength(markers.length - 1);

    const stored = new Uint8Array(await (await get(server, KEY)).arrayBuffer());
    expect(stored.byteLength).toBe(size);
    // One writer's bytes, start to finish: a torn or interleaved entry would hold two markers.
    const marker = stored[0] ?? -1;
    expect(markers).toContain(marker);
    expect(stored.every((byte) => byte === marker)).toBe(true);

    // Nothing staged survives a completed request, whether it won or lost.
    expect(await readdir(join(directory, '.staging'))).toEqual([]);
  });
});

describe('tokens', () => {
  test('refuses a missing or unknown token in the form nx can report', async () => {
    const { server, directory } = await serve();

    for (const attempt of [await get(server, KEY, null), await get(server, KEY, 'not-a-real-token')]) {
      expect(attempt.status).toBe(401);
      // nx only surfaces a 401's body when the content type is exactly `text/plain`; with a charset
      // parameter the operator gets "Requests should respond with text/plain on 401s" instead.
      expect(attempt.headers.get('content-type')).toBe('text/plain');
      expect(attempt.headers.get('www-authenticate')).toBe('Bearer');
      expect(await attempt.text()).toContain('NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN');
    }

    expect((await put(server, KEY, filled(16, 1), 'not-a-real-token')).status).toBe(401);
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging']);
  });

  test('a read-only token reads the cache but cannot publish into it', async () => {
    const { server, directory } = await serve();
    const artifact = filled(2048, 0xc3);
    expect((await put(server, KEY, artifact)).status).toBe(200);

    expect(new Uint8Array(await (await get(server, KEY, READ_SECRET)).arrayBuffer())).toEqual(artifact);

    const refused = await put(server, '999999999999999999', filled(32, 9), READ_SECRET);
    expect(refused.status).toBe(403);
    expect(await refused.text()).toContain('may read the cache but not publish');
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging', KEY]);
  });
});

describe('cache keys', () => {
  test('rejects every key that could name something other than one entry', () => {
    for (const hostile of ['..', '.', '../escape', 'a/b', '/etc/passwd', '.staging', '', 'a'.repeat(129)]) {
      expect(isCacheKey(hostile)).toBe(false);
    }
    for (const legitimate of [KEY, 'a', 'A-b_c', 'a'.repeat(128)]) {
      expect(isCacheKey(legitimate)).toBe(true);
    }
  });

  test('refuses a write under a key outside the charset and writes nothing', async () => {
    const { server, directory } = await serve();
    const nested = await scratchDirectory();

    const refused = await fetch(`${server.origin}/v1/cache/..%2F..%2Fescape`, {
      method: 'PUT',
      body: filled(64, 0xff),
      headers: { authorization: `Bearer ${WRITE_SECRET}` },
    });

    expect(refused.status).toBe(400);
    expect(await refused.text()).toContain('1-128 characters of [A-Za-z0-9_-]');
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging']);
    expect(await readdir(nested)).toEqual([]);
  });
});

describe('hot cache under pressure', () => {
  test('a just-published archive survives the sweep its own publication triggers', async () => {
    const { server, directory } = await serve({ maxBytes: 4096, capBytes: 12288 });
    for (const key of ['a', 'b', 'c']) {
      expect((await put(server, key, filled(4096, key.charCodeAt(0)))).status).toBe(200);
    }
    for (const key of ['a', 'b', 'c']) {
      const read = await get(server, key, READ_SECRET);
      expect(read.status).toBe(200);
      await read.arrayBuffer();
    }
    const fresh = filled(4096, 0x66);
    expect((await put(server, 'fresh', fresh)).status).toBe(200);
    const read = await get(server, 'fresh', READ_SECRET);
    expect(read.status).toBe(200);
    expect(new Uint8Array(await read.arrayBuffer())).toEqual(fresh);
    let published = 0;
    for (const name of await readdir(directory)) {
      if (!name.startsWith('.')) published += (await stat(join(directory, name))).size;
    }
    expect(published).toBeLessThanOrEqual(12288);
  });
});

describe('declared length', () => {
  test('declines an oversized artifact without failing the build that made it', async () => {
    const { server, directory } = await serve({ maxBytes: 1024 });

    const refused = await put(server, KEY, filled(4096, 0x7f));

    // 403, not 413: nx treats 403 as "not stored" and carries on, while any other refusal fails the
    // task that produced a perfectly good artifact. Bun's own `maxRequestBodySize` 413 would preempt
    // this, which is why the framing ceiling sits above the policy limit.
    expect(refused.status).toBe(403);
    expect(await refused.text()).toContain('1024-byte limit');
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging']);
  });

  test('refuses a body that declares no length, so nothing unbounded is ever streamed', async () => {
    const { server, directory } = await serve();

    const refused = await fetch(`${server.origin}/v1/cache/${KEY}`, {
      method: 'PUT',
      // A stream body makes fetch use chunked transfer encoding: no content-length.
      body: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(filled(64, 0x01));
          controller.close();
        },
      }),
      headers: { authorization: `Bearer ${WRITE_SECRET}` },
    });

    expect(refused.status).toBe(411);
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging']);
  });

  test('refuses an empty artifact rather than making it permanent', async () => {
    const { server, directory } = await serve();

    const refused = await put(server, KEY, new Uint8Array(0));

    expect(refused.status).toBe(400);
    expect(await refused.text()).toContain('positive integer');
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging']);
  });

  test('publishes nothing when a body ends short of the length it declared', async () => {
    const directory = await scratchDirectory();
    const result = await openCacheStore(directory);
    if (!result.ok) throw new Error('Store was locked.');
    const store = result.store;
    stores.push(store);
    const short = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(filled(100, 0x5a));
        controller.close();
      },
    });

    // A truncated upload published under a permanent key would poison that task hash forever: nx
    // cannot extract it, and never-replace means nobody can correct it.
    expect(await stageEntry(store, KEY, short, 4096)).toEqual({
      kind: 'incomplete',
      bytes: 100,
      stagingRetained: false,
    });
    expect((await readdir(directory)).sort()).toEqual(['.owner.lock', '.staging']);
    expect(await readdir(join(directory, '.staging'))).toEqual([]);
  });
});

describe('staging', () => {
  test('reclaims uploads a killed process left behind', async () => {
    const directory = await scratchDirectory();
    const staging = join(directory, '.staging');
    await mkdir(staging, { recursive: true });
    await writeFile(join(staging, `${KEY}.abandoned`), 'half an upload');

    const { server } = await serve({ directory });

    expect(server.reclaimedStagingFiles).toBe(1);
    expect(await readdir(staging)).toEqual([]);
  });
});

describe('startup configuration', () => {
  const withDirectory = { NX_CACHE_SERVER_DIR: '/srv/nx-cache' };

  test('accepts a complete environment and parses every principal', () => {
    const result = cacheServerConfigFromEnv({
      ...withDirectory,
      NX_CACHE_SERVER_PORT: '9123',
      NX_CACHE_SERVER_MAX_BYTES: '2048',
      NX_CACHE_SERVER_WRITE_TOKENS: 'ci:secret-one',
      NX_CACHE_SERVER_READ_TOKENS: 'pr:secret-two, shed:secret-three',
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.config.port).toBe(9123);
    expect(result.config.maxBytes).toBe(2048);
    expect(result.config.host).toBe('127.0.0.1');
    expect(result.config.principals).toEqual([
      { label: 'ci', bearer: 'Bearer secret-one', capability: 'write' },
      { label: 'pr', bearer: 'Bearer secret-two', capability: 'read' },
      { label: 'shed', bearer: 'Bearer secret-three', capability: 'read' },
    ]);
  });

  test('refuses to start with no tokens at all', () => {
    const result = cacheServerConfigFromEnv(withDirectory);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.problems.join('\n')).toContain('NX_CACHE_SERVER_WRITE_TOKENS');
  });

  test('refuses a relative cache directory', () => {
    const result = cacheServerConfigFromEnv({
      NX_CACHE_SERVER_DIR: 'relative/cache',
      NX_CACHE_SERVER_WRITE_TOKENS: 'ci:secret',
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.problems.join('\n')).toContain('absolute path');
  });

  test('refuses an unlabelled token without ever echoing the secret', () => {
    const result = cacheServerConfigFromEnv({
      ...withDirectory,
      NX_CACHE_SERVER_WRITE_TOKENS: 'bare-secret-value',
    });

    expect(result.ok).toBe(false);
    if (result.ok) return;
    const explanation = result.problems.join('\n');
    expect(explanation).toContain('NX_CACHE_SERVER_WRITE_TOKENS entry 1');
    expect(explanation).not.toContain('bare-secret-value');
  });

  test('refuses one secret configured with two capabilities', () => {
    const result = cacheServerConfigFromEnv({
      ...withDirectory,
      NX_CACHE_SERVER_WRITE_TOKENS: 'ci:shared',
      NX_CACHE_SERVER_READ_TOKENS: 'pr:shared',
    });

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.problems.join('\n')).toContain('repeats a secret');
  });

  test('refuses a port outside the range and names the bound', () => {
    const result = cacheServerConfigFromEnv({
      ...withDirectory,
      NX_CACHE_SERVER_WRITE_TOKENS: 'ci:secret',
      NX_CACHE_SERVER_PORT: '70000',
    });
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.problems.join('\n')).toContain('between 0 and 65535');
  });
});

test('concurrent different-key uploads remain independent and whole', async () => {
  const { server } = await serve();
  const keys = Array.from({ length: 8 }, (_, index) => `distinct-${index}`);
  const statuses = await Promise.all(
    keys.map(async (key, index) => (await put(server, key, filled(1024 * 1024, index + 1))).status),
  );
  expect(statuses).toEqual(keys.map(() => 200));
  for (const [index, key] of keys.entries()) {
    const response = await get(server, key, READ_SECRET);
    expect(response.status).toBe(200);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(filled(1024 * 1024, index + 1));
  }
});

test("separate project listeners cannot read each other's same-key archive", async () => {
  const first = await serve();
  const second = await serve();
  expect((await put(first.server, KEY, filled(32, 1))).status).toBe(200);
  expect((await get(second.server, KEY)).status).toBe(404);
  expect((await put(second.server, KEY, filled(32, 2))).status).toBe(200);
  expect(new Uint8Array(await (await get(first.server, KEY)).arrayBuffer())).toEqual(filled(32, 1));
  expect(new Uint8Array(await (await get(second.server, KEY)).arrayBuffer())).toEqual(filled(32, 2));
});

test('retention configuration refuses a per-upload limit larger than its cap', () => {
  const configured = cacheServerConfigFromEnv({
    NX_CACHE_SERVER_DIR: '/srv/cache/entries',
    NX_CACHE_SERVER_WRITE_TOKENS: 'publisher:secret',
    NX_CACHE_SERVER_MAX_BYTES: '4096',
    NX_CACHE_SERVER_CAP_BYTES: '2048',
  });
  expect(configured.ok).toBe(false);
  if (configured.ok) return;
  expect(configured.problems).toContain('NX_CACHE_SERVER_MAX_BYTES must not exceed NX_CACHE_SERVER_CAP_BYTES.');
});
