import { describe, expect, it, spyOn } from 'bun:test';
import { mkdir, mkdtemp, readdir, readFile, rename, rm, symlink, truncate, writeFile } from 'node:fs/promises';
import { createServer, type Server } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  collectTaskArtifacts,
  createTaskArtifactsDirectory,
  holdsRequiredArtifacts,
  MAX_ARTIFACT_BYTES,
  removeTaskArtifactsDirectory,
  SIDECAR,
  TASK_ARTIFACTS_ENV,
  type TaskArtifactsDirectory,
  withTaskArtifacts,
  writeTaskArtifacts,
} from 'nx/src/tasks-runner/task-artifacts.js';
import { output } from 'nx/src/utils/output.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { bindListener } from './testing.js';

/**
 * Guards the task-result artifact hunk of `patches/nx@24.0.3.patch` (`tasks-runner/task-artifacts.js`): each discrete
 * execution gets a private `NX_TASK_ARTIFACTS_DIR` in its own spawn environment; what it leaves there rides its result
 * to the lifecycle and into its cache entry's `.nx-task-artifacts` sidecar, digest-checked when read, so every
 * transport that moves the entry moves them; a target that names `artifacts` treats an entry without one as no hit.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

/** `app:build` in a project graph whose target declares `artifacts` (absent when undefined). */
function declaring(artifacts?: unknown): {
  task: { id: string; target: { project: string; target: string } };
  graph: unknown;
} {
  const target = artifacts === undefined ? {} : { artifacts };
  return {
    task: { id: 'app:build', target: { project: 'app', target: 'build' } },
    graph: { nodes: { app: { data: { targets: { build: target } } } } },
  };
}

/** What collecting `directory` takes, and what it says it did not take. */
function collect(directory: TaskArtifactsDirectory): { taken: Record<string, string>; said: string[] } {
  const warn = spyOn(output, 'warn').mockImplementation(() => {});
  try {
    const collected = collectTaskArtifacts(directory, 'app:build');
    const taken = Object.fromEntries(
      Object.entries(collected ?? {}).map(([name, bytes]) => [name, Buffer.from(bytes).toString()]),
    );
    const said = warn.mock.calls.flatMap(([message]) => [...(message.bodyLines ?? [])]).sort();
    expect(
      warn.mock.calls.every(([message]) => message.title === 'Task app:build: task-result artifacts not taken'),
    ).toBe(true);
    return { taken, said };
  } finally {
    warn.mockRestore();
  }
}

describe('the declaration', () => {
  it('gives no artifact directory to a target that declares none', () => {
    const { task, graph } = declaring();
    expect(createTaskArtifactsDirectory(task, graph)).toBeUndefined();
  });

  it('refuses a declaration that is not distinct artifact names', () => {
    for (const bad of ['report', ['report', 'report'], ['.hidden'], [7]]) {
      const { task, graph } = declaring(bad);
      expect(() => createTaskArtifactsDirectory(task, graph)).toThrow(
        "Task app:build: its target's `artifacts` must list",
      );
    }
  });

  it('holds a declared artifact only as its own property, never an inherited one', () => {
    const { task, graph } = declaring(['constructor', 'toString']);
    expect(holdsRequiredArtifacts(task, graph, {})).toBe(false);
    expect(holdsRequiredArtifacts(task, graph, undefined)).toBe(false);
    const own = Object.assign(Object.create(null), { constructor: new Uint8Array(1), toString: new Uint8Array(1) });
    expect(holdsRequiredArtifacts(task, graph, own)).toBe(true);
  });
});

describe('the execution side: what a run leaves in its artifact directory', () => {
  it('takes the declared regular files and says each other entry and why', async () => {
    const { task, graph } = declaring(['report', 'linked', 'fifo']);
    const directory = createTaskArtifactsDirectory(task, graph);
    if (directory === undefined) throw new Error('a declaring target gets a directory');
    const outside = `${directory.path}-outside`;
    try {
      await writeFile(join(directory.path, 'report'), 'own bytes');
      await writeFile(outside, 'outside');
      await symlink(outside, join(directory.path, 'linked'));
      expect(Bun.spawnSync(['mkfifo', join(directory.path, 'fifo')]).exitCode).toBe(0);
      await writeFile(join(directory.path, 'stray'), 'not declared');
      const { taken, said } = collect(directory);
      expect(taken).toEqual({ report: 'own bytes' });
      expect(said).toEqual([
        '"stray" is not an artifact its target declares (report, linked, fifo)',
        'fifo is not a regular file',
        'linked cannot be opened without following a link (ELOOP)',
      ]);
    } finally {
      removeTaskArtifactsDirectory(directory);
      await rm(outside, { force: true });
    }
  });

  it('refuses by size, before reading, what passes the bound on all artifacts together', async () => {
    const { task, graph } = declaring(['first', 'second']);
    const directory = createTaskArtifactsDirectory(task, graph);
    if (directory === undefined) throw new Error('a declaring target gets a directory');
    try {
      await writeFile(join(directory.path, 'first'), 'small');
      // Sparse: the size says it is over the bound; reading it would allocate all of it.
      await writeFile(join(directory.path, 'second'), '');
      await truncate(join(directory.path, 'second'), MAX_ARTIFACT_BYTES);
      const { taken, said } = collect(directory);
      expect(taken).toEqual({ first: 'small' });
      expect(said).toEqual([
        `second holds ${MAX_ARTIFACT_BYTES} bytes, over the ${MAX_ARTIFACT_BYTES - 5} bytes left of the bound`,
      ]);
    } finally {
      removeTaskArtifactsDirectory(directory);
    }
  });

  it('takes nothing from a directory that is no longer the one made for the execution', async () => {
    const { task, graph } = declaring(['report']);
    const directory = createTaskArtifactsDirectory(task, graph);
    if (directory === undefined) throw new Error('a declaring target gets a directory');
    const elsewhere = await mkdtemp(join(tmpdir(), 'task-artifacts-elsewhere-'));
    try {
      await writeFile(join(elsewhere, 'report'), 'not this execution');
      await rm(directory.path, { recursive: true });
      await symlink(elsewhere, directory.path);
      const { taken, said } = collect(directory);
      expect(taken).toEqual({});
      expect(said).toEqual([`its artifact directory ${directory.path} is no longer the directory made for it`]);
      await rm(directory.path);
      await rename(elsewhere, directory.path);
      expect(collect(directory).said).toEqual([
        `its artifact directory ${directory.path} is no longer the directory made for it`,
      ]);
    } finally {
      removeTaskArtifactsDirectory(directory);
      await rm(elsewhere, { recursive: true, force: true });
    }
  });

  it('answers none, and says nothing, for a run that left none', () => {
    const { task, graph } = declaring(['report']);
    const directory = createTaskArtifactsDirectory(task, graph);
    if (directory === undefined) throw new Error('a declaring target gets a directory');
    try {
      expect(collect(directory)).toEqual({ taken: {}, said: [] });
    } finally {
      removeTaskArtifactsDirectory(directory);
    }
  });
});

describe('the cache side: the entry sidecar', () => {
  it('reads back exactly what was written, and no hit once a byte or the manifest changes', async () => {
    const entry = await mkdtemp(join(tmpdir(), 'task-entry-'));
    try {
      writeTaskArtifacts(entry, { report: Buffer.from('produced') });
      const read = withTaskArtifacts({ outputsPath: entry });
      expect(Buffer.from(read?.artifacts?.report ?? []).toString()).toBe('produced');
      expect(Object.getPrototypeOf(read?.artifacts)).toBeNull();
      await writeFile(join(entry, SIDECAR, 'report'), 'forged!!');
      expect(withTaskArtifacts({ outputsPath: entry })).toBeNull();
      for (const shape of [
        '{',
        'null',
        '[]',
        '{"report":null}',
        '{"report":{"bytes":8}}',
        '{"report":{"bytes":-1,"sha256":""}}',
        '{"report":{"bytes":1.5,"sha256":""}}',
        `{"report":{"bytes":${MAX_ARTIFACT_BYTES + 1},"sha256":""}}`,
        '{"__proto__":{"bytes":0,"sha256":""}}',
      ]) {
        await writeFile(join(entry, SIDECAR, 'manifest.json'), shape);
        expect(withTaskArtifacts({ outputsPath: entry })).toBeNull();
      }
      // A manifest over its own bound is not read.
      await truncate(join(entry, SIDECAR, 'manifest.json'), 64 * 1024 + 1);
      expect(withTaskArtifacts({ outputsPath: entry })).toBeNull();
    } finally {
      await rm(entry, { recursive: true, force: true });
    }
  });

  it('is no hit when the sidecar is a link, wherever it points', async () => {
    const entry = await mkdtemp(join(tmpdir(), 'task-entry-'));
    const other = await mkdtemp(join(tmpdir(), 'task-entry-other-'));
    try {
      writeTaskArtifacts(other, { report: Buffer.from('elsewhere') });
      await symlink(join(other, SIDECAR), join(entry, SIDECAR));
      expect(withTaskArtifacts({ outputsPath: entry })).toBeNull();
    } finally {
      await rm(entry, { recursive: true, force: true });
      await rm(other, { recursive: true, force: true });
    }
  });

  it('leaves an entry without artifacts as it was', async () => {
    const entry = await mkdtemp(join(tmpdir(), 'task-entry-'));
    try {
      writeTaskArtifacts(entry, undefined);
      expect(withTaskArtifacts({ outputsPath: entry })).toEqual({ outputsPath: entry });
    } finally {
      await rm(entry, { recursive: true, force: true });
    }
  });
});

/** A workspace whose `app:build` writes its output and, from its input, the artifact `report`; `requires` names it. */
async function artifactWorkspace(workspace: string, requires: boolean): Promise<void> {
  await mkdir(join(workspace, 'app'), { recursive: true });
  expect(Bun.spawnSync(['git', 'init', '--quiet', workspace]).exitCode).toBe(0);
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
          ...(requires ? { artifacts: ['report'] } : {}),
          inputs: ['{projectRoot}/source.txt'],
          outputs: ['{projectRoot}/result.txt'],
          options: {
            cwd: 'app',
            // A target that declares no artifacts gets no directory: what it sees of one is its output.
            command: requires
              ? `cp source.txt result.txt && echo "$$:$(cat source.txt)" > "$${TASK_ARTIFACTS_ENV}/report"`
              : `echo "\${${TASK_ARTIFACTS_ENV}-unset}" > result.txt`,
          },
        },
      },
    }),
  );
  await writeFile(join(workspace, 'app', 'source.txt'), 'built');
}

interface Built {
  readonly exitCode: number;
  readonly log: string;
}

async function build(workspace: string, socketDirectory: string, server?: string): Promise<Built> {
  const child = Bun.spawn(['bun', nxEntry, 'run', 'app:build', '--outputStyle=static'], {
    cwd: workspace,
    env: {
      ...fixtureNxEnv(workspace, socketDirectory),
      ...(server === undefined ? {} : { NX_SELF_HOSTED_REMOTE_CACHE_SERVER: server }),
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { exitCode, log: `${stdout}${stderr}` };
}

/** The one cache entry of the workspace's local cache, and the artifact its sidecar holds. */
async function entryReport(workspace: string): Promise<{ entry: string; report: string } | undefined> {
  const cache = join(workspace, '.nx/cache');
  const entries = (await readdir(cache, { withFileTypes: true })).filter(
    (e) => e.isDirectory() && /^\d+$/.test(e.name),
  );
  const [only] = entries;
  if (only === undefined) return undefined;
  const entry = join(cache, only.name);
  return { entry, report: await readFile(join(entry, SIDECAR, 'report'), 'utf8') };
}

/** A self-hosted remote cache that keeps what it is sent, in this process. */
async function remoteCache(): Promise<{ server: Server; origin: string }> {
  const bodies = new Map<string, Buffer>();
  const server = await bindListener((port) => {
    const { promise, resolve, reject } = Promise.withResolvers<Server>();
    const candidate = createServer((request, response) => {
      const hash = (request.url ?? '').split('/').pop() ?? '';
      if (request.method === 'PUT') {
        const chunks: Buffer[] = [];
        request.on('data', (chunk: Buffer) => chunks.push(chunk));
        request.on('end', () => {
          bodies.set(hash, Buffer.concat(chunks));
          response.writeHead(200).end();
        });
        return;
      }
      const body = bodies.get(hash);
      if (body === undefined) response.writeHead(404).end();
      else response.writeHead(200, { 'content-type': 'application/octet-stream' }).end(body);
    });
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

describe('a real Nx run', () => {
  it("hands its execution's artifact into its cache entry, and a remote cache carries it to another clone", async () => {
    const { server, origin } = await remoteCache();
    try {
      let produced = '';
      await withNxFixture('nx-artifacts-producer-', async ({ workspace, socketDirectory }) => {
        await artifactWorkspace(workspace, true);
        const run = await build(workspace, socketDirectory, origin);
        expect(run.exitCode, run.log).toBe(0);
        const held = await entryReport(workspace);
        expect(held?.report, run.log).toMatch(/^\d+:built\n$/);
        produced = held?.report ?? '';
      });
      // Another clone with no local cache: its hit comes from the remote, the producer's artifact with it.
      await withNxFixture('nx-artifacts-consumer-', async ({ workspace, socketDirectory }) => {
        await artifactWorkspace(workspace, true);
        const run = await build(workspace, socketDirectory, origin);
        expect(run.exitCode, run.log).toBe(0);
        expect(run.log).toContain('remote cache');
        expect((await entryReport(workspace))?.report).toBe(produced);
      });
    } finally {
      const closed = Promise.withResolvers<void>();
      server.close(() => closed.resolve());
      await closed.promise;
    }
  });

  it('runs again over an entry whose required artifact is missing or forged', async () => {
    await withNxFixture('nx-artifacts-required-', async ({ workspace, socketDirectory }) => {
      await artifactWorkspace(workspace, true);
      const first = await build(workspace, socketDirectory);
      expect(first.exitCode, first.log).toBe(0);
      const held = await entryReport(workspace);
      expect(held).toBeDefined();
      const hit = await build(workspace, socketDirectory);
      expect(hit.log).toContain('existing outputs match the cache');
      await writeFile(join(held?.entry ?? '', SIDECAR, 'report'), 'forged');
      const forged = await build(workspace, socketDirectory);
      expect(forged.exitCode, forged.log).toBe(0);
      expect(forged.log).not.toContain('existing outputs match the cache');
      expect((await entryReport(workspace))?.report).toMatch(/^\d+:built\n$/);
      await rm(join(held?.entry ?? '', SIDECAR), { recursive: true });
      const old = await build(workspace, socketDirectory);
      expect(old.log).not.toContain('existing outputs match the cache');
    });
  });

  it('gives a target that declares no artifacts no artifact directory, and its entry no sidecar', async () => {
    await withNxFixture('nx-artifacts-undeclared-', async ({ workspace, socketDirectory }) => {
      await artifactWorkspace(workspace, false);
      const run = await build(workspace, socketDirectory);
      expect(run.exitCode, run.log).toBe(0);
      expect(await readFile(join(workspace, 'app', 'result.txt'), 'utf8')).toBe('unset\n');
      const cache = join(workspace, '.nx/cache');
      const entries = (await readdir(cache, { withFileTypes: true })).filter(
        (e) => e.isDirectory() && /^\d+$/.test(e.name),
      );
      expect(entries).toHaveLength(1);
      expect(await Bun.file(join(cache, entries[0]?.name ?? '', SIDECAR, 'manifest.json')).exists()).toBe(false);
    });
  });
});
