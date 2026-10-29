import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
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
