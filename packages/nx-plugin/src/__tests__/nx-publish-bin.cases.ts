import { expect, it } from 'bun:test';
import {
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  readlink,
  realpath,
  rm,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { gunzipSync } from 'node:zlib';

const packageRoot = resolve(import.meta.dir, '../..');
const repository = resolve(packageRoot, '../..');
const executable = join(packageRoot, 'dist/bin/smoo-nx-publish.js');

it('the emitted publisher stores a missing key, counts a stored one without archiving it, and says a refusal', async () => {
  const root = await mkdtemp(join(tmpdir(), 'nx-publish-bin-'));
  try {
    const puts: { path: string; bytes: number }[] = [];
    const stored = new Set<string>();
    // The store's protocol (03_caches.md): a stored key answers GET 200 and PUT 409. 'race' is a key stored between the
    // publisher's GET and its PUT; 'refuse' is a store that declines the upload.
    let answer: 'store' | 'race' | 'refuse' = 'store';
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      async fetch(request) {
        const key = new URL(request.url).pathname;
        if (request.method === 'PUT') {
          const body = await request.arrayBuffer();
          puts.push({ path: key, bytes: body.byteLength });
          if (answer === 'refuse') return new Response('staging budget exhausted\n', { status: 403 });
          if (answer === 'race' || stored.has(key)) return new Response(null, { status: 409 });
          stored.add(key);
          return new Response(null, { status: 200 });
        }
        return stored.has(key) && answer !== 'race'
          ? new Response('archive', { headers: { 'content-type': 'application/octet-stream' } })
          : new Response('No cache entry for that key.\n', { status: 404 });
      },
    });
    const environment = {
      ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_'))),
      NX_SELF_HOSTED_REMOTE_CACHE_SERVER: new URL('nx', server.url).href,
      NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY: 'true',
    };
    async function run(args: readonly string[], stdin?: string) {
      const child = Bun.spawn(['node', ...args], {
        cwd: root,
        env: environment,
        stdin: stdin === undefined ? 'ignore' : new TextEncoder().encode(stdin),
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [status, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { status, stdout, stderr };
    }
    try {
      await writeFile(join(root, 'package.json'), '{"private":true}');
      await writeFile(join(root, 'nx.json'), '{}');
      await writeFile(join(root, 'result.txt'), 'accepted output\n');
      await symlink(join(repository, 'node_modules'), join(root, 'node_modules'));
      const adapter = await readFile(join(packageRoot, 'runtime/nx-publish/cache_adapter.js'), 'utf8');
      const prepared = await run(
        ['-', root],
        `${adapter}
const local = openNxCache(process.argv[2]);
local.requireNx("nx/src/hasher/hash-task").getTaskDetails().recordTaskDetails([{ hash: "accepted-fixture", project: "app", target: "build" }]);
local.cache.cache.put("accepted-fixture", "passed", ["result.txt"], 0);
`,
      );
      expect(prepared.status, prepared.stderr).toBe(0);

      const summary = join(root, '.nx/cache/run.json');
      await mkdir(dirname(summary), { recursive: true });
      await writeFile(
        summary,
        JSON.stringify({
          run: { command: 'nx run app:build', startTime: '2026-10-25T21:00:00.250Z' },
          tasks: [{ taskId: 'app:build', hash: 'accepted-fixture', status: 0, cacheStatus: 'local-cache-hit' }],
        }),
      );
      const captured = await run([
        executable,
        'hashes',
        summary,
        String(Date.parse('2026-10-25T21:00:00.000Z') / 1000),
      ]);
      expect(captured.status, captured.stderr).toBe(0);
      expect(captured.stdout).toBe('["accepted-fixture"]\n');
      const hashes = join(root, 'hashes.json');
      await writeFile(hashes, captured.stdout);

      const published = await run([executable, 'publish', root, hashes]);
      expect(published.status, published.stderr).toBe(0);
      expect(published.stdout).toBe('{"published":1,"existing":0,"skipped":0}\n');
      expect(puts).toHaveLength(1);
      expect(puts[0]?.path).toBe('/nx/v1/cache/accepted-fixture');
      expect(puts[0]?.bytes).toBeGreaterThan(0);
      expect(environment.NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY).toBe('true');

      // A stored key is asked for first, counted, and never archived: no second PUT reaches the store.
      const again = await run([executable, 'publish', root, hashes]);
      expect(again.status, again.stderr).toBe(0);
      expect(again.stdout).toBe('{"published":0,"existing":1,"skipped":0}\n');
      expect(puts).toHaveLength(1);

      // A key stored between the GET and the PUT answers 409: counted as existing, the store's archive kept.
      answer = 'race';
      const raced = await run([executable, 'publish', root, hashes]);
      expect(raced.status, raced.stderr).toBe(0);
      expect(raced.stdout).toBe('{"published":0,"existing":1,"skipped":0}\n');
      expect(puts).toHaveLength(2);

      stored.clear();
      answer = 'refuse';
      const refused = await run([executable, 'publish', root, hashes]);
      expect(refused.status).toBe(1);
      expect(refused.stderr).toContain(
        'task accepted-fixture was not stored: the project cache answers PUT 403: staging budget exhausted',
      );
    } finally {
      await server.stop(true);
    }
  } finally {
    await rm(root, { recursive: true });
  }
});

/** Each Nx version the cache adapter declares, as the repository installs it (packages/nx-23.2.1 holds 23.2.1). */
const DECLARED_NX = [
  ['23.2.1', join(repository, 'packages/nx-23.2.1/node_modules/nx-23.2.1')],
  ['24.0.3', join(repository, 'node_modules/nx')],
] as const;

/** A fixture of the store's protocol (03_caches.md): a key is stored once, a stored key is served whole. */
function fixtureStore() {
  const stored = new Map<string, Uint8Array>();
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      const key = new URL(request.url).pathname;
      if (request.method === 'PUT') {
        if (stored.has(key)) return new Response(null, { status: 409 });
        stored.set(key, new Uint8Array(await request.arrayBuffer()));
        return new Response(null, { status: 200 });
      }
      const entry = stored.get(key);
      return entry === undefined
        ? new Response('No cache entry for that key.\n', { status: 404 })
        : new Response(entry, { headers: { 'content-type': 'application/octet-stream' } });
    },
  });
  return { server, stored, origin: new URL('nx', server.url).href };
}

async function node(cwd: string, env: Record<string, string | undefined>, args: readonly string[], stdin?: string) {
  const child = Bun.spawn(['node', ...args], {
    cwd,
    env,
    stdin: stdin === undefined ? 'ignore' : new TextEncoder().encode(stdin),
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [status, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { status, stdout, stderr };
}

/** What Nx's restore reproduces of a cache entry: each path's kind and permission bits, a file's bytes and mtime in seconds, a link's target. */
async function entryTree(dir: string): Promise<Record<string, string>> {
  const entries: Record<string, string> = {};
  async function walk(relative: string): Promise<void> {
    for (const name of (await readdir(join(dir, relative))).sort()) {
      const path = join(relative, name);
      const info = await lstat(join(dir, path));
      const mode = (info.mode & 0o7777).toString(8);
      if (info.isSymbolicLink()) {
        entries[path] = `link ${await readlink(join(dir, path))}`;
      } else if (info.isDirectory()) {
        entries[path] = `dir ${mode}`;
        await walk(path);
      } else {
        const bytes = await readFile(join(dir, path));
        entries[path] = `file ${mode} ${Math.floor(info.mtimeMs / 1000)} ${bytes.length} ${Bun.hash(bytes)}`;
      }
    }
  }
  await walk('');
  return entries;
}

/** The first offset at which two byte strings differ, or -1 when they are equal. */
function firstDifference(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index++) {
    if (left[index] !== right[index]) return index;
  }
  return left.length === right.length ? -1 : length;
}

for (const [version, installed] of DECLARED_NX) {
  it(`the publisher's archive is Nx ${version}'s own tar at gzip level 1, and Nx's native retrieve restores it`, async () => {
    const nx = await realpath(installed);
    const ours = fixtureStore();
    const native = fixtureStore();
    const root = await mkdtemp(join(tmpdir(), 'nx-publish-archive-'));
    try {
      const landed = join(root, 'landed');
      const out = join(landed, 'out');
      await mkdir(join(out, 'nested/deeper'), { recursive: true });
      await mkdir(join(out, 'empty-dir'));
      await mkdir(join(landed, 'node_modules'));
      await writeFile(join(landed, 'package.json'), '{"private":true}');
      await writeFile(join(landed, 'nx.json'), '{}');
      await symlink(nx, join(landed, 'node_modules/nx'));
      // A name past 100 bytes takes tar's GNU long-name entry, a link target past 100 its long-link entry, and 3 MiB of
      // noise spans many gzip writes.
      const long = `${'long-name-'.repeat(14)}.txt`;
      await writeFile(join(out, 'plain.txt'), 'plain\n');
      await writeFile(join(out, 'empty'), '');
      await writeFile(join(out, 'tool.sh'), '#!/bin/sh\necho tool\n', { mode: 0o755 });
      await writeFile(join(out, 'ünïcode-名前.txt'), 'unicode\n');
      await writeFile(join(out, 'nested/deeper', long), 'long\n');
      const noise = new Uint8Array(3 << 20);
      for (let index = 0, state = 1; index < noise.length; index++) {
        state = (Math.imul(state, 1103515245) + 12345) >>> 0;
        noise[index] = state >>> 24;
      }
      await writeFile(join(out, 'nested/deeper/noise.bin'), noise);
      await utimes(join(out, 'plain.txt'), 1_000_000_000, 1_000_000_000);
      await symlink('nested/deeper/noise.bin', join(out, 'link'));
      await symlink(`nested/deeper/${long}`, join(out, 'long-link'));

      const env = {
        ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_') && key !== 'CI')),
        NX_DAEMON: 'false',
        NX_NO_CLOUD: 'true',
        NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY: 'true',
      };
      const terminal = 'passed ✓\n\u001b[32mgreen\u001b[0m\n';
      const adapter = await readFile(join(packageRoot, 'runtime/nx-publish/cache_adapter.js'), 'utf8');
      const prepared = await node(
        landed,
        env,
        ['-', landed],
        `${adapter}
const local = openNxCache(process.argv[2]);
local.requireNx("nx/src/hasher/hash-task").getTaskDetails().recordTaskDetails([{ hash: "archive-fixture", project: "app", target: "build" }]);
local.cache.cache.put("archive-fixture", ${JSON.stringify(terminal)}, ["out"], 0);
`,
      );
      expect(prepared.status, prepared.stderr).toBe(0);
      const entry = join(landed, '.nx/cache/archive-fixture');
      expect((await lstat(join(entry, 'out/long-link'))).isSymbolicLink()).toBe(true);

      const hashes = join(root, 'hashes.json');
      await writeFile(hashes, '["archive-fixture"]\n');
      const published = await node(landed, { ...env, NX_SELF_HOSTED_REMOTE_CACHE_SERVER: ours.origin }, [
        executable,
        'publish',
        landed,
        hashes,
      ]);
      expect(published.status, published.stderr).toBe(0);
      expect(published.stdout).toBe('{"published":1,"existing":0,"skipped":0}\n');
      // Nx's own writer, as the reference: its native store of the same entry.
      const reference = await node(
        landed,
        { ...env, NX_SELF_HOSTED_REMOTE_CACHE_SERVER: native.origin },
        ['-', landed],
        `${adapter}
const local = openNxCache(process.argv[2]);
const { HttpRemoteCache } = local.requireNx("nx/src/native");
new HttpRemoteCache().store("archive-fixture", local.cacheDir, local.getLocal("archive-fixture").terminalOutput, 0).then((stored) => {
  if (!stored) throw new Error("the reference store refused");
});
`,
      );
      expect(reference.status, reference.stderr).toBe(0);

      const key = '/nx/v1/cache/archive-fixture';
      const archive = ours.stored.get(key);
      const nativeArchive = native.stored.get(key);
      if (archive === undefined || nativeArchive === undefined) throw new Error('both stores hold the entry');
      // RFC 1952's XFL byte: zlib's fastest level writes 4; Nx's own default level 6 writes 0.
      expect(nativeArchive[8]).toBe(0);
      expect(archive[8]).toBe(4);
      expect(firstDifference(gunzipSync(archive), gunzipSync(nativeArchive))).toBe(-1);

      const fresh = join(root, 'fresh-cache');
      await mkdir(fresh);
      const retrieved = await node(
        root,
        { ...env, NX_SELF_HOSTED_REMOTE_CACHE_SERVER: ours.origin },
        ['-', landed, fresh],
        `const [checkout, cache] = process.argv.slice(2);
const { HttpRemoteCache } = require("node:module").createRequire(require("node:path").join(checkout, "package.json"))("nx/src/native");
new HttpRemoteCache().retrieve("archive-fixture", cache).then((result) => process.stdout.write(JSON.stringify(result)));
`,
      );
      expect(retrieved.status, retrieved.stderr).toBe(0);
      expect(JSON.parse(retrieved.stdout)).toMatchObject({
        code: 0,
        terminalOutput: terminal,
        outputsPath: join(fresh, 'archive-fixture'),
      });
      expect(await entryTree(join(fresh, 'archive-fixture'))).toEqual(await entryTree(entry));
    } finally {
      await ours.server.stop(true);
      await native.server.stop(true);
      await rm(root, { recursive: true });
    }
  });
}

/** `a:build` depends on `a:prep`; each is cached with one output and logs its execution outside its outputs. */
function project(): string {
  const task = (name: string) => ({
    executor: 'nx:run-commands',
    cache: true,
    inputs: ['{projectRoot}/src.txt'],
    outputs: [`{projectRoot}/${name}.txt`],
    options: {
      command: `node -e "const fs = require('fs'); fs.appendFileSync('a/ran.log', '${name}\\n'); fs.writeFileSync('a/${name}.txt', '${name}')"`,
    },
  });
  return JSON.stringify({
    name: 'a',
    targets: { prep: task('prep'), build: { ...task('build'), dependsOn: ['prep'] } },
  });
}

for (const [version, installed] of DECLARED_NX) {
  it(`the emitted publisher stores a real Nx ${version} run that another checkout's Nx ${version} restores without executing`, async () => {
    const nx = await realpath(installed);
    expect(JSON.parse(await readFile(join(nx, 'package.json'), 'utf8')).version).toBe(version);
    const store = new Map<string, Uint8Array>();
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      // The self-hosted cache protocol the project's store answers: a key is stored once.
      async fetch(request) {
        const key = new URL(request.url).pathname;
        if (request.method === 'PUT') {
          if (store.has(key)) return new Response(null, { status: 409 });
          store.set(key, new Uint8Array(await request.arrayBuffer()));
          return new Response(null, { status: 200 });
        }
        const entry = store.get(key);
        return entry === undefined
          ? new Response('No cache entry for that key.\n', { status: 404 })
          : new Response(entry, { headers: { 'content-type': 'application/octet-stream' } });
      },
    });
    const origin = new URL('nx', server.url).href;
    // No inherited NX_* or CI: each checkout's Nx keeps its cache and task database in its own .nx.
    const base = {
      ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_') && key !== 'CI')),
      NX_DAEMON: 'false',
      NX_NO_CLOUD: 'true',
      NX_TUI: 'false',
      NX_ISOLATE_PLUGINS: 'false',
    };
    // The landed run reads no project cache: stock 23.2.1 ignores READ_ONLY and would store its own results, so the store
    // holds only what the publisher sent. The publish and the other checkout read it, read-only, as every job does.
    const reading = {
      ...base,
      NX_SELF_HOSTED_REMOTE_CACHE_SERVER: origin,
      NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY: 'true',
    };
    async function run(cwd: string, env: Record<string, string | undefined>, args: readonly string[]) {
      const child = Bun.spawn(['node', ...args], { cwd, env, stdin: 'ignore', stdout: 'pipe', stderr: 'pipe' });
      const [status, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { status, stdout, stderr };
    }
    const root = await mkdtemp(join(tmpdir(), 'nx-publish-round-trip-'));
    try {
      const [landed, other] = [join(root, 'landed'), join(root, 'other')];
      for (const checkout of [landed, other]) {
        await mkdir(join(checkout, 'a'), { recursive: true });
        await mkdir(join(checkout, 'node_modules'));
        await writeFile(join(checkout, 'package.json'), '{"name":"fixture","private":true}');
        await writeFile(join(checkout, 'nx.json'), '{"cacheDirectory":".nx/cache","maxCacheSize":"1GB"}');
        await writeFile(join(checkout, 'a/project.json'), project());
        await writeFile(join(checkout, 'a/src.txt'), 'source\n');
        await symlink(nx, join(checkout, 'node_modules/nx'));
      }

      const since = Math.floor(Date.now() / 1000);
      const built = await run(landed, base, [join(nx, 'dist/bin/nx.js'), 'run', 'a:build']);
      expect(built.status, `${built.stdout}${built.stderr}`).toBe(0);
      expect(await readFile(join(landed, 'a/ran.log'), 'utf8')).toBe('prep\nbuild\n');
      const captured = await run(landed, base, [
        executable,
        'hashes',
        join(landed, '.nx/cache/run.json'),
        String(since),
      ]);
      expect(captured.status, captured.stderr).toBe(0);
      const hashes: string[] = JSON.parse(captured.stdout);
      expect(hashes).toHaveLength(2);
      const list = join(root, 'hashes.json');
      await writeFile(list, captured.stdout);

      const published = await run(landed, reading, [executable, 'publish', landed, list]);
      expect(published.status, published.stderr).toBe(0);
      expect(published.stdout).toBe('{"published":2,"existing":0,"skipped":0}\n');
      expect([...store.keys()].sort()).toEqual(hashes.map((hash) => `/nx/v1/cache/${hash}`).sort());

      const restored = await run(other, reading, [join(nx, 'dist/bin/nx.js'), 'run', 'a:build']);
      expect(restored.status, `${restored.stdout}${restored.stderr}`).toBe(0);
      expect(await readFile(join(other, 'a/prep.txt'), 'utf8')).toBe('prep');
      expect(await readFile(join(other, 'a/build.txt'), 'utf8')).toBe('build');
      expect(await Bun.file(join(other, 'a/ran.log')).exists()).toBe(false);
      const summary = JSON.parse(await readFile(join(other, '.nx/cache/run.json'), 'utf8'));
      expect(
        summary.tasks.map((task: { hash: string; cacheStatus: string }) => [task.hash, task.cacheStatus]).sort(),
      ).toEqual(hashes.map((hash) => [hash, 'remote-cache-hit']).sort());
    } finally {
      await server.stop(true);
      await rm(root, { recursive: true });
    }
  });
}
