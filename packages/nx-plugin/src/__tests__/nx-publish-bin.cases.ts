import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

const packageRoot = resolve(import.meta.dir, '../..');
const repository = resolve(packageRoot, '../..');
const executable = join(packageRoot, 'dist/bin/smoo-nx-publish.js');

it('the emitted publisher sends a native cached result to the accepted store while its client stays read-only', async () => {
  const root = await mkdtemp(join(tmpdir(), 'nx-publish-bin-'));
  try {
    const puts: { path: string; bytes: number }[] = [];
    let refuse = false;
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      async fetch(request) {
        if (request.method === 'PUT') {
          const body = await request.arrayBuffer();
          puts.push({ path: new URL(request.url).pathname, bytes: body.byteLength });
          return new Response(null, { status: refuse ? 403 : 200 });
        }
        return new Response('No cache entry for that key.\n', { status: 404 });
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

      refuse = true;
      const refused = await run([executable, 'publish', root, hashes]);
      expect(refused.status).toBe(1);
      expect(refused.stderr).toContain('task accepted-fixture was not stored, and the project cache answers GET 404');
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
