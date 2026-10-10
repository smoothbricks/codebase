import { expect, test } from 'bun:test';
import { mkdtemp, readdir, rm, stat, utimes, watch, writeFile } from 'node:fs/promises';
import { connect, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import typia from 'typia';
import type { CacheServerReadyRecord, CacheServerStartResult } from '../server.js';

type StartFailure = { readonly kind: 'failed' } & Extract<CacheServerStartResult, { readonly ok: false }>;

const parseReady = typia.json.createAssertParse<CacheServerReadyRecord>();
const parseFailed = typia.json.createAssertParse<StartFailure>();
const parseBackend = typia.json.createAssertParse<{ readonly backendPid: number }>();
const main = fileURLToPath(new URL('../main.ts', import.meta.url));
const supervisor = fileURLToPath(new URL('./supervisor.fixture.ts', import.meta.url));

interface StderrTap {
  readonly ended: Promise<void>;
  until(fragment: string): Promise<void>;
}

interface ProcessFixture extends CacheServerReadyRecord {
  readonly child: Bun.Subprocess<'pipe', 'pipe', 'pipe'>;
  readonly stderr: StderrTap;
}

interface Refusal {
  readonly code: number;
  readonly failure: StartFailure;
}

function startEnv(root: string, capBytes: number) {
  return {
    ...process.env,
    NX_CACHE_SERVER_DIR: join(root, 'entries'),
    NX_CACHE_SERVER_HOST: '127.0.0.1',
    NX_CACHE_SERVER_PORT: '0',
    NX_CACHE_SERVER_MAX_BYTES: String(Math.min(capBytes, 8 * 1024 * 1024)),
    NX_CACHE_SERVER_CAP_BYTES: String(capBytes),
    NX_CACHE_SERVER_WRITE_TOKENS: 'publisher:process-write',
    NX_CACHE_SERVER_READ_TOKENS: 'reader:process-read',
  };
}

function tapStderr(stream: ReadableStream<Uint8Array>): StderrTap {
  const decoder = new TextDecoder();
  let text = '';
  let done = false;
  let changed = Promise.withResolvers<void>();
  const ended = (async () => {
    for await (const chunk of stream) {
      text += decoder.decode(chunk, { stream: true });
      changed.resolve();
      changed = Promise.withResolvers<void>();
    }
    text += decoder.decode();
    done = true;
    changed.resolve();
  })();
  return {
    ended,
    async until(fragment) {
      while (!text.includes(fragment)) {
        if (done) throw new Error(`stderr ended before ${fragment}: ${text}`);
        await changed.promise;
      }
    },
  };
}

async function start(
  root: string,
  capBytes = 64 * 1024 * 1024,
  options: { readonly exitOnStdinEof?: boolean } = {},
): Promise<ProcessFixture> {
  const child = Bun.spawn([process.execPath, main, ...(options.exitOnStdinEof ? ['--exit-on-stdin-eof'] : [])], {
    env: startEnv(root, capBytes),
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const stderr = tapStderr(child.stderr);
  const reader = child.stdout.getReader();
  let text = '';
  const decoder = new TextDecoder();
  try {
    while (!text.includes('\n')) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`Server exited before readiness: ${text}`);
      text += decoder.decode(chunk.value, { stream: true });
    }
    const ready = parseReady(text.slice(0, text.indexOf('\n')));
    return { child, stderr, ...ready };
  } finally {
    reader.releaseLock();
  }
}

async function refuse(root: string): Promise<Refusal> {
  const child = Bun.spawn([process.execPath, main], {
    env: startEnv(root, 64 * 1024 * 1024),
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const stderr = tapStderr(child.stderr);
  const out = await new Response(child.stdout).text();
  const code = await child.exited;
  await stderr.ended;
  return { code, failure: parseFailed(out.trim()) };
}

async function readLines(stream: ReadableStream<Uint8Array>, count: number): Promise<string[]> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  const lines: string[] = [];
  let text = '';
  try {
    while (lines.length < count) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`stream ended after ${lines.length} lines`);
      text += decoder.decode(chunk.value, { stream: true });
      let index = text.indexOf('\n');
      while (index !== -1 && lines.length < count) {
        lines.push(text.slice(0, index));
        text = text.slice(index + 1);
        index = text.indexOf('\n');
      }
    }
    return lines;
  } finally {
    reader.releaseLock();
  }
}

/**
 * Resolve once a staged upload file holds bytes. The server emits no event after a staged write, and macOS
 * reports no directory change when data is written into an existing file, so a directory watch can sleep through
 * the write. A bounded stat-poll is the only signal that bytes landed.
 */
async function awaitStaged(staging: string, signal: AbortSignal): Promise<void> {
  const deadline = Date.now() + 15_000;
  while (!signal.aborted) {
    for (const name of await readdir(staging)) {
      if ((await stat(join(staging, name))).size > 0) return;
    }
    if (Date.now() > deadline) throw new Error(`no staged upload bytes in ${staging} within 15s`);
    await Bun.sleep(5);
  }
  throw new Error('staging wait aborted before the upload materialized');
}

function readStatus(socket: Socket): Promise<number> {
  const { promise, resolve, reject } = Promise.withResolvers<number>();
  let text = '';
  const cleanup = () => {
    socket.off('data', onData);
    socket.off('error', onFail);
    socket.off('close', onFail);
  };
  const onFail = () => {
    cleanup();
    reject(new Error(`socket closed before a status line: ${text}`));
  };
  const onData = (chunk: Buffer) => {
    text += chunk.toString('latin1');
    const match = /^HTTP\/1\.1 (\d{3})/.exec(text);
    if (match) {
      cleanup();
      resolve(Number(match[1]));
    }
  };
  socket.on('data', onData);
  socket.on('error', onFail);
  socket.on('close', onFail);
  return promise;
}

test('hard death during a real upload leaves no hit and restart reclaims staging', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-process-'));
  const first = await start(root);
  const socket = connect(first.port, '127.0.0.1');
  let restarted: ProcessFixture | undefined;
  const abort = new AbortController();
  try {
    await new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    });
    const staging = join(root, 'entries', '.staging');
    socket.write(
      'PUT /v1/cache/interrupted HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer process-write\r\nContent-Length: 8388608\r\n\r\n',
    );
    socket.write(Buffer.alloc(64 * 1024, 7));
    await awaitStaged(staging, abort.signal);
    const before = await fetch(`${first.origin}/v1/cache/interrupted`, {
      headers: { authorization: 'Bearer process-read' },
    });
    expect(before.status).toBe(404);
    first.child.kill('SIGKILL');
    await first.child.exited;
    socket.destroy();
    restarted = await start(root);
    expect(restarted.reclaimedStagingFiles).toBe(1);
    expect(await readdir(staging)).toEqual([]);
    expect(
      (
        await fetch(`${restarted.origin}/v1/cache/interrupted`, {
          headers: { authorization: 'Bearer process-read' },
        })
      ).status,
    ).toBe(404);
  } finally {
    abort.abort();
    socket.destroy();
    first.child.kill('SIGKILL');
    await first.child.exited;
    if (restarted) {
      restarted.child.kill('SIGTERM');
      expect(await restarted.child.exited).toBe(0);
    }
    await rm(root, { recursive: true, force: true });
  }
});

test('a second process on a busy root refuses store-locked and leaves in-flight staging intact', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-lock-'));
  const first = await start(root);
  const staging = join(root, 'entries', '.staging');
  let fresh: ProcessFixture | undefined;
  try {
    await writeFile(join(staging, 'planted-inflight'), 'in-flight');
    const refusal = await refuse(root);
    expect(refusal.code).toBe(1);
    expect(refusal.failure).toEqual({
      kind: 'failed',
      ok: false,
      stage: 'store-locked',
      directory: join(root, 'entries'),
      path: join(root, 'entries', '.owner.lock'),
      reason: expect.any(String),
    });
    expect(await readdir(staging)).toEqual(['planted-inflight']);
    first.child.kill('SIGKILL');
    await first.child.exited;
    fresh = await start(root);
    expect(fresh.reclaimedStagingFiles).toBe(1);
    expect(await readdir(staging)).toEqual([]);
  } finally {
    first.child.kill('SIGKILL');
    await first.child.exited;
    fresh?.child.kill('SIGTERM');
    await fresh?.child.exited;
    await rm(root, { recursive: true, force: true });
  }
});

test('--exit-on-stdin-eof exits on parent EOF and frees the lease; the no-flag mode ignores EOF', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-eof-'));
  const eof = await start(root, undefined, { exitOnStdinEof: true });
  let next: ProcessFixture | undefined;
  try {
    eof.child.stdin.end();
    expect(await eof.child.exited).toBe(0);
    next = await start(root);
    next.child.stdin.end();
    const probe = await fetch(`${next.origin}/v1/cache/missing`, {
      headers: { authorization: 'Bearer process-read' },
    });
    expect(probe.status).toBe(404);
    next.child.kill('SIGTERM');
    await next.stderr.until('ev=stopping signal=SIGTERM');
    expect(await next.child.exited).toBe(0);
  } finally {
    eof.child.kill('SIGKILL');
    await eof.child.exited;
    next?.child.kill('SIGKILL');
    await next?.child.exited;
    await rm(root, { recursive: true, force: true });
  }
});

test('stdin EOF drains an in-flight upload while the lease still refuses a second owner', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-drain-'));
  const first = await start(root, undefined, { exitOnStdinEof: true });
  const staging = join(root, 'entries', '.staging');
  const abort = new AbortController();
  const body = new Uint8Array(1048576).fill(9);
  const socket = connect(first.port, '127.0.0.1');
  let fresh: ProcessFixture | undefined;
  try {
    const connected = Promise.withResolvers<void>();
    socket.once('connect', connected.resolve);
    socket.once('error', connected.reject);
    await connected.promise;
    const staged = awaitStaged(staging, abort.signal);
    socket.write(
      `PUT /v1/cache/drained HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer process-write\r\nContent-Length: ${body.byteLength}\r\n\r\n`,
    );
    socket.write(body.subarray(0, 64 * 1024));
    await staged;
    first.child.stdin.end();
    await first.stderr.until('ev=stopping reason=supervisor-eof');
    const refusal = await refuse(root);
    expect(refusal.code).toBe(1);
    expect(refusal.failure).toEqual({
      kind: 'failed',
      ok: false,
      stage: 'store-locked',
      directory: join(root, 'entries'),
      path: join(root, 'entries', '.owner.lock'),
      reason: expect.any(String),
    });
    socket.write(body.subarray(64 * 1024));
    expect(await readStatus(socket)).toBe(200);
    socket.destroy();
    expect(await first.child.exited).toBe(0);
    fresh = await start(root);
    const got = await fetch(`${fresh.origin}/v1/cache/drained`, {
      headers: { authorization: 'Bearer process-read' },
    });
    expect(got.status).toBe(200);
    expect(new Uint8Array(await got.arrayBuffer())).toEqual(body);
    expect(await readdir(staging)).toEqual([]);
  } finally {
    abort.abort();
    socket.destroy();
    first.child.kill('SIGKILL');
    await first.child.exited;
    fresh?.child.kill('SIGTERM');
    await fresh?.child.exited;
    await rm(root, { recursive: true, force: true });
  }
});

test('SIGKILL of the supervisor releases the backend lease through stdin EOF', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-orphan-'));
  const fixture = Bun.spawn([process.execPath, supervisor, '--exit-on-stdin-eof'], {
    env: startEnv(root, 64 * 1024 * 1024),
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const stderr = tapStderr(fixture.stderr);
  let fresh: ProcessFixture | undefined;
  try {
    const [pidLine, readyLine] = await readLines(fixture.stdout, 2);
    expect(parseBackend(pidLine).backendPid).toBeGreaterThan(0);
    expect(parseReady(readyLine).kind).toBe('ready');
    fixture.kill('SIGKILL');
    await fixture.exited;
    await stderr.ended;
    fresh = await start(root);
    expect(fresh.kind).toBe('ready');
  } finally {
    fixture.kill('SIGKILL');
    await fixture.exited;
    fresh?.child.kill('SIGTERM');
    await fresh?.child.exited;
    await rm(root, { recursive: true, force: true });
  }
});

test('real server bounds published bytes and reports evicted entries as 404', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-cap-'));
  const running = await start(root, 8192);
  try {
    const body = new Uint8Array(4096).fill(4);
    for (const key of ['old', 'middle', 'new']) {
      const uploaded = await fetch(`${running.origin}/v1/cache/${key}`, {
        method: 'PUT',
        body,
        headers: { authorization: 'Bearer process-write' },
      });
      expect(uploaded.status).toBe(200);
      if (key !== 'new') {
        const seconds = key === 'old' ? 100 : 200;
        await utimes(join(root, 'entries', key), seconds, seconds);
      }
    }
    const names = (await readdir(join(root, 'entries'))).filter((name) => !name.startsWith('.'));
    let bytes = 0;
    for (const name of names) bytes += (await stat(join(root, 'entries', name))).size;
    expect(bytes).toBe(8192);
    expect(
      (
        await fetch(`${running.origin}/v1/cache/old`, {
          headers: { authorization: 'Bearer process-read' },
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await fetch(`${running.origin}/v1/cache/new`, {
          headers: { authorization: 'Bearer process-read' },
        })
      ).status,
    ).toBe(200);
  } finally {
    running.child.kill('SIGTERM');
    expect(await running.child.exited).toBe(0);
    await rm(root, { recursive: true, force: true });
  }
});

test('full staging admission refuses 403 and cancellation releases its reservation', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cache-admission-'));
  const running = await start(root, 4096);
  const socket = connect(running.port, '127.0.0.1');
  const abort = new AbortController();
  try {
    await new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    });
    const staging = join(root, 'entries', '.staging');
    const changes = watch(staging, { signal: abort.signal })[Symbol.asyncIterator]();
    let changed = changes.next();
    socket.write(
      'PUT /v1/cache/held HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer process-write\r\nContent-Length: 4096\r\n\r\npartial',
    );
    for (;;) {
      await changed;
      if ((await readdir(staging)).length > 0) break;
      changed = changes.next();
    }
    const refused = await fetch(`${running.origin}/v1/cache/refused`, {
      method: 'PUT',
      body: 'x',
      headers: { authorization: 'Bearer process-write' },
    });
    expect(refused.status).toBe(403);
    changed = changes.next();
    socket.destroy();
    for (;;) {
      await changed;
      if ((await readdir(staging)).length === 0) break;
      changed = changes.next();
    }
    const stored = await fetch(`${running.origin}/v1/cache/after-cancel`, {
      method: 'PUT',
      body: 'x',
      headers: { authorization: 'Bearer process-write' },
    });
    expect(stored.status).toBe(200);
    expect(
      (
        await fetch(`${running.origin}/v1/cache/held`, {
          headers: { authorization: 'Bearer process-read' },
        })
      ).status,
    ).toBe(404);
  } finally {
    abort.abort();
    socket.destroy();
    running.child.kill('SIGTERM');
    expect(await running.child.exited).toBe(0);
    await rm(root, { recursive: true, force: true });
  }
});
