import { expect, it } from 'bun:test';
import { mkdir, readFile, rm, stat, symlink, utimes, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the restore-time hunk of `patches/nx@23.2.1.patch`. Nx 23.2.1
 * restores a cached output with its native `copy` (`std::fs::copy`), which on
 * macOS clones the file and keeps its timestamps: the restored output carries
 * the mtime it had when its cache entry was written. Build an output from
 * input A, rebuild it from B, put A back, and the restored A is older than
 * the B output it replaced, so every consumer that judges freshness by mtime
 * (a Cargo build script's `rerun-if-changed`, make, a file watcher) keeps
 * what it built from B. The patched restore stamps each restored file with
 * the time of the restore, as running the task would have.
 *
 * A local hit restores through `DbCache.copyFilesFromCache`; a remote hit is
 * restored inside the native `applyRemoteCacheResults`, which untars the
 * artifact with its stored mtimes and copies it the same way. Each has a case.
 *
 * Only macOS (APFS clone) reproduces the bug: on Linux `std::fs::copy` writes
 * a new file with a fresh mtime, so both cases pass there with or without the
 * hunk. Run them on macOS before dropping the hunk.
 *
 * Drop the hunk, and keep this test, once the Nx version in use restores
 * outputs newer than the files they replace.
 */

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

const OUTPUTS = ['app/result.txt', 'app/dist/nested/copy.txt'] as const;

interface Run {
  readonly log: string;
  readonly outputs: readonly { readonly output: string; readonly text: string; readonly mtimeNs: bigint }[];
}

/**
 * A fixture workspace with one cached `app:build` that copies its input into a
 * file output and a directory output. `run(text)` writes the input, builds
 * under `env`, and reports the outputs' contents and mtimes.
 */
async function restoreFixture(workspace: string, env: Record<string, string>) {
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
          // A file output and a directory output: the restore walks into the directory.
          outputs: ['{projectRoot}/result.txt', '{projectRoot}/dist'],
          options: {
            cwd: 'app',
            command: 'cp source.txt result.txt && mkdir -p dist/nested && cp source.txt dist/nested/copy.txt',
          },
        },
      },
    }),
  );
  const source = join(workspace, 'app', 'source.txt');
  // Nx 23.2.1 reuses a file's archived hash while its whole-second `st_mtime` is unchanged
  // (`selective_files_hash`), so every write of the input gets a second of its own: a rewrite
  // within the same second would hash as the previous bytes.
  let inputSecond = Math.floor(Date.now() / 1000) - 60;
  return async (text: string): Promise<Run> => {
    await writeFile(source, text);
    inputSecond += 1;
    await utimes(source, inputSecond, inputSecond);
    // Spawned asynchronously: a remote case's cache server answers from this process.
    const child = Bun.spawn(['bun', nxEntry, 'run', 'app:build', '--outputStyle=static'], {
      cwd: workspace,
      env,
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    const log = `${stdout}${stderr}`;
    expect(exitCode, log).toBe(0);
    const outputs = await Promise.all(
      OUTPUTS.map(async (output) => ({
        output,
        text: await readFile(join(workspace, output), 'utf8'),
        mtimeNs: (await stat(join(workspace, output), { bigint: true })).mtimeNs,
      })),
    );
    return { log, outputs };
  };
}

/** B must have been built, not restored: otherwise the case compares two restores. */
function expectBuilt(run: Run, text: string): void {
  expect(run.log).not.toContain('[local cache]');
  expect(run.log).not.toContain('[remote cache]');
  expect(run.outputs.map((output) => output.text)).toEqual(OUTPUTS.map(() => text));
}

function expectRestoredNewer(restored: Run, replaced: Run, text: string): void {
  restored.outputs.forEach(({ output, text: restoredText, mtimeNs }, index) => {
    expect(restoredText, output).toBe(text);
    const replacedNs = replaced.outputs[index].mtimeNs;
    expect(
      mtimeNs > replacedNs,
      `${output}: restored mtime ${mtimeNs} is not newer than the replaced output's ${replacedNs}`,
    ).toBe(true);
  });
}

it('restores a locally cached output newer than the output it replaces', async () => {
  await withNxFixture('nx-restore-mtime-', async ({ workspace }) => {
    const run = await restoreFixture(workspace, fixtureNxEnv(workspace));

    await run('A');
    const fromB = await run('B');
    expectBuilt(fromB, 'B');
    const restored = await run('A');
    expect(restored.log).toContain('[local cache]');
    expectRestoredNewer(restored, fromB, 'A');
  });
}, 60_000);

it('restores a remotely cached output newer than the output it replaces', async () => {
  // The self-hosted remote cache protocol Nx's native HTTP cache speaks: GET and PUT of an
  // artifact tarball at /v1/cache/<hash>.
  const artifacts = new Map<string, ArrayBuffer>();
  using server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      const hash = new URL(request.url).pathname.match(/^\/v1\/cache\/([^/]+)$/)?.[1];
      if (hash === undefined) return new Response(null, { status: 404 });
      if (request.method === 'PUT') {
        artifacts.set(hash, await request.arrayBuffer());
        return new Response(null, { status: 200 });
      }
      const artifact = artifacts.get(hash);
      return artifact === undefined ? new Response(null, { status: 404 }) : new Response(artifact);
    },
  });
  await withNxFixture('nx-restore-mtime-remote-', async ({ workspace }) => {
    const env = { ...fixtureNxEnv(workspace), NX_SELF_HOSTED_REMOTE_CACHE_SERVER: server.url.origin };
    const run = await restoreFixture(workspace, env);

    await run('A');
    const fromB = await run('B');
    expectBuilt(fromB, 'B');
    expect(artifacts.size).toBe(2);
    // Forget the local cache and its records, so A can only come from the remote.
    await rm(join(workspace, '.nx'), { recursive: true });
    const restored = await run('A');
    expect(restored.log).toContain('[remote cache]');
    expectRestoredNewer(restored, fromB, 'A');
  });
}, 60_000);
