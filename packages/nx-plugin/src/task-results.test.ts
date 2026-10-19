import { afterAll, expect, it } from 'bun:test';
import { mkdir, readdir, readFile, realpath, stat, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { testFileInstallation } from './__tests__/owned-installation.js';

/**
 * The plugin's `preTasksExecution`/`postTasksExecution` hooks, run by a real Nx subprocess in a
 * fixture workspace that registers the compiled `@smoothbricks/nx-plugin`. The hooks write a hint
 * only under a controller-selected receipt id: an ordinary run and an invalid id leave no namespace,
 * and a failed task is recorded as a failure, never as a success. The receipt variable is spelled
 * here rather than imported, so the test loads only the compiled plugin through Nx.
 */

const RECEIPT_ENV = 'COWSHED_NX_TASK_RECEIPT_ID';
const RECEIPT = '0123456789abcdef0123456789abcdef';
const CHANGED_RECEIPT = 'fedcba9876543210fedcba9876543210';
/** The Nx CLI of a fixture workspace's own installation. */
const nxEntry = (workspace: string) => join(workspace, 'node_modules', '.bin', 'nx');
const HINTS = join('.nx', 'cache', '.cowshed-task-results');

/** The Nx and the plugin under test, one copy this file's fixtures share; none may write into it. */
const installation = testFileInstallation('task-results-installation');
afterAll(() => installation.retire());

/** A fixture workspace: `app:build` caches its copy of `source.txt`; `app:fail` always exits 3. */
async function writeWorkspace(workspace: string): Promise<void> {
  await mkdir(join(workspace, 'app'), { recursive: true });
  const git = Bun.spawnSync(['git', 'init', '--quiet', workspace]);
  expect(git.exitCode, git.stderr.toString()).toBe(0);
  await symlink(await installation.nodeModules(), join(workspace, 'node_modules'), 'dir');
  await writeFile(
    join(workspace, 'nx.json'),
    JSON.stringify({ cacheDirectory: '.nx/cache', plugins: ['@smoothbricks/nx-plugin'] }),
  );
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
          // runs.txt is an undeclared execution marker: it counts real executions, not cache hits.
          options: { cwd: 'app', command: 'cp source.txt result.txt && echo run >> runs.txt' },
        },
        fail: { executor: 'nx:run-commands', cache: false, options: { cwd: 'app', command: 'exit 3' } },
      },
    }),
  );
  await writeFile(join(workspace, 'app', 'source.txt'), 'one');
}

/** Runs one target with `nx run` under `env`, returning its exit code, output and argv. */
async function nx(workspace: string, env: Record<string, string>, target: string) {
  const argv = [process.execPath, nxEntry(workspace), 'run', target, '--outputStyle=static'];
  const child = Bun.spawn(argv, { cwd: workspace, env, stdout: 'pipe', stderr: 'pipe' });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { exitCode, log: `${stdout}${stderr}`, argv };
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    throw error;
  }
}

/** Executions of `app:build`, one `run` line per real execution. */
async function runCount(workspace: string): Promise<number> {
  const lines = (await readFile(join(workspace, 'app', 'runs.txt'), 'utf8')).split('\n');
  return lines.filter((line) => line === 'run').length;
}

/** The single hint in a receipt's namespace, parsed. */
async function readHint(namespace: string): Promise<unknown> {
  const files = await readdir(namespace);
  expect(files).toHaveLength(1);
  const [file] = files;
  if (file === undefined) throw new Error(`no hint in ${namespace}`);
  return JSON.parse(await readFile(join(namespace, file), 'utf8'));
}

it('records a task hint only under a controller receipt, and never on an ordinary run', async () => {
  await withNxFixture(
    'task-results',
    async ({ workspace, socketDirectory }) => {
      await writeWorkspace(workspace);
      // The inherited receipt is dropped: only this test's runs choose one.
      const { [RECEIPT_ENV]: _inherited, ...ordinary } = fixtureNxEnv(workspace, socketDirectory);
      const hints = join(workspace, HINTS);
      const source = join(workspace, 'app', 'source.txt');

      const first = await nx(workspace, ordinary, 'app:build');
      expect(first.exitCode, first.log).toBe(0);
      expect(await runCount(workspace)).toBe(1);
      expect(await exists(hints)).toBe(false);

      const hit = await nx(workspace, { ...ordinary, [RECEIPT_ENV]: RECEIPT }, 'app:build');
      expect(hit.exitCode, hit.log).toBe(0);
      expect(hit.log).toContain('local cache');
      expect(await runCount(workspace)).toBe(1);
      expect(await readdir(hints)).toEqual([RECEIPT]);
      expect(await readHint(join(hints, RECEIPT))).toMatchObject({
        version: 1,
        requestId: RECEIPT,
        workspaceRoot: workspace,
        run: { argv: [process.execPath, await realpath(nxEntry(workspace)), ...hit.argv.slice(2)] },
        tasks: [{ task: { id: 'app:build', hash: expect.stringMatching(/^\d+$/) }, status: 'local-cache', code: 0 }],
      });

      const invalid = await nx(workspace, { ...ordinary, [RECEIPT_ENV]: 'NOT-A-REQUEST-ID' }, 'app:build');
      expect(invalid.exitCode, invalid.log).toBe(0);
      expect(invalid.log).toContain('is not a controller-selected 32-hex request id');
      expect(await readdir(hints)).toEqual([RECEIPT]);

      await writeFile(source, 'two');
      const changed = await nx(workspace, { ...ordinary, [RECEIPT_ENV]: CHANGED_RECEIPT }, 'app:build');
      expect(changed.exitCode, changed.log).toBe(0);
      expect(await runCount(workspace)).toBe(2);
      expect((await readdir(hints)).sort()).toEqual([RECEIPT, CHANGED_RECEIPT].sort());
      expect(await readHint(join(hints, CHANGED_RECEIPT))).toMatchObject({
        version: 1,
        requestId: CHANGED_RECEIPT,
        run: { argv: [process.execPath, await realpath(nxEntry(workspace)), ...changed.argv.slice(2)] },
        tasks: [{ task: { id: 'app:build', hash: expect.stringMatching(/^\d+$/) }, status: 'success', code: 0 }],
      });
    },
    '.',
  );
});

it('records a failed task as a failure under a receipt, never as a success', async () => {
  await withNxFixture(
    'task-results-failure',
    async ({ workspace, socketDirectory }) => {
      await writeWorkspace(workspace);
      const { [RECEIPT_ENV]: _inherited, ...ordinary } = fixtureNxEnv(workspace, socketDirectory);

      const failed = await nx(workspace, { ...ordinary, [RECEIPT_ENV]: RECEIPT }, 'app:fail');
      expect(failed.exitCode).not.toBe(0);
      expect(await readHint(join(workspace, HINTS, RECEIPT))).toMatchObject({
        requestId: RECEIPT,
        tasks: [{ task: { id: 'app:fail' }, status: 'failure', code: 1 }],
      });
    },
    '.',
  );
});

it('records hints through a daemon-backed Nx only under a checked receipt', async () => {
  await withNxFixture(
    'task-results-daemon',
    async ({ workspace, socketDirectory }) => {
      await writeWorkspace(workspace);
      // The daemon is on, as in normal use: the hooks run in the daemon, which receives the client's env.
      const daemon: Record<string, string> = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
      const { [RECEIPT_ENV]: _inherited, ...ordinary } = daemon;
      const hints = join(workspace, HINTS);
      const namespaceA = join(hints, RECEIPT);

      const first = await nx(workspace, ordinary, 'app:build');
      expect(first.exitCode, first.log).toBe(0);
      expect(await runCount(workspace)).toBe(1);
      expect(await exists(hints)).toBe(false);

      const checkedA = await nx(workspace, { ...ordinary, [RECEIPT_ENV]: RECEIPT }, 'app:build');
      expect(checkedA.exitCode, checkedA.log).toBe(0);
      expect(checkedA.log).toContain('[existing outputs match the cache, left as is]');
      expect(await runCount(workspace)).toBe(1);
      expect(await readHint(namespaceA)).toMatchObject({
        version: 1,
        requestId: RECEIPT,
        workspaceRoot: workspace,
        run: { argv: [checkedA.argv[0] ?? '', await realpath(nxEntry(workspace)), ...checkedA.argv.slice(2)] },
        tasks: [
          {
            task: { id: 'app:build', hash: expect.stringMatching(/^\d+$/) },
            status: 'local-cache-kept-existing',
            code: 0,
          },
        ],
      });

      // An ordinary run without a key must neither add a hint nor rewrite checked hint A.
      const [fileA] = await readdir(namespaceA);
      if (fileA === undefined) throw new Error(`no hint in ${namespaceA}`);
      const hintA = join(namespaceA, fileA);
      const bytesA = await readFile(hintA, 'utf8');
      const mtimeA = (await stat(hintA, { bigint: true })).mtimeNs;
      const ordinaryAgain = await nx(workspace, ordinary, 'app:build');
      expect(ordinaryAgain.exitCode, ordinaryAgain.log).toBe(0);
      expect(await readdir(hints)).toEqual([RECEIPT]);
      expect(await readdir(namespaceA)).toEqual([fileA]);
      expect(await readFile(hintA, 'utf8')).toBe(bytesA);
      expect((await stat(hintA, { bigint: true })).mtimeNs).toBe(mtimeA);

      // A new valid receipt records its own namespace, and the unchanged task is still not executed.
      const checkedB = await nx(workspace, { ...ordinary, [RECEIPT_ENV]: CHANGED_RECEIPT }, 'app:build');
      expect(checkedB.exitCode, checkedB.log).toBe(0);
      expect(await runCount(workspace)).toBe(1);
      expect((await readdir(hints)).sort()).toEqual([RECEIPT, CHANGED_RECEIPT].sort());
      expect(await readHint(join(hints, CHANGED_RECEIPT))).toMatchObject({
        version: 1,
        requestId: CHANGED_RECEIPT,
        workspaceRoot: workspace,
        run: { argv: [checkedB.argv[0] ?? '', await realpath(nxEntry(workspace)), ...checkedB.argv.slice(2)] },
        tasks: [
          {
            task: { id: 'app:build', hash: expect.stringMatching(/^\d+$/) },
            status: 'local-cache-kept-existing',
            code: 0,
          },
        ],
      });
    },
    '.',
  );
});
