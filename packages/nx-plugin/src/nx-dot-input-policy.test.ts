import { expect, it } from 'bun:test';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import {
  appendFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readlink,
  realpath,
  rm,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { Watcher, WorkspaceContext } from 'nx/src/native/index.js';
import { globWithWorkspaceContextSync, resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { awaitExit, processTable, terminate } from './testing.js';

const repositoryRoot = join(import.meta.dir, '../../..');
const lintScript = join(repositoryRoot, 'tooling/lint-dot-inputs.ts');

// The root `.nxignore` before the dot-input policy: the cowshed runtime directory and
// the devenv paths only, so every other untracked dot path was indexed. Comments omitted.
const PREVIOUS_NXIGNORE = `modules/
.cowshed/
tooling/direnv/.devenv*/
tooling/direnv/.direnv/
`;

// Untracked dot state a checkout accumulates: the daemon's and job records, editor and
// Bun staging, devenv and direnv caches. None of it is a project input.
const RUNTIME_DOT_STATE = [
  '.cowshed/cache/nx/workspace-data/d/daemon.log',
  '.cowshed/job/1/stdout.log',
  '.cowshed/run/supervisor.pid',
  '.icm-session/state.json',
  '.fseventsd/0000000000000001',
  '.proof-01234567.bun-build',
  'tooling/direnv/.devenv/state/cache',
  'tooling/direnv/.direnv/cache/env',
];

// A tracked dot path the watcher must keep reporting: its own edit wakes the graph.
const TRACKED_DOT_WRITE = '.github/workflows/ci.yml';

function trackedDotPaths(): string[] {
  return execFileSync('git', ['ls-files', '-z', '--full-name'], { cwd: repositoryRoot, encoding: 'utf8' })
    .split('\0')
    .filter((path) => path.split('/').some((part) => part.startsWith('.')));
}

/**
 * A git checkout holding the repository's tracked dot inputs (same names, contents,
 * and symlinks) plus the runtime state above, under the given root `.nxignore`.
 */
async function workspace(nxignore: string): Promise<string> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'nx-dot-input-policy-')));
  execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });
  for (const path of trackedDotPaths()) {
    const source = join(repositoryRoot, path);
    const target = join(root, path);
    await mkdir(dirname(target), { recursive: true });
    if ((await lstat(source)).isSymbolicLink()) {
      const link = await readlink(source);
      const input = resolve(dirname(source), link);
      const inputPath = relative(repositoryRoot, input);
      if (inputPath.startsWith('..')) throw new Error(`tracked dot input ${path} links outside the checkout`);
      await mkdir(dirname(join(root, inputPath)), { recursive: true });
      await writeFile(join(root, inputPath), await readFile(input));
      await symlink(link, target);
    } else {
      await writeFile(target, await readFile(source));
    }
  }
  await writeFile(join(root, '.nxignore'), nxignore);
  execFileSync('git', ['add', '--force', '--', ...trackedDotPaths()], { cwd: root, stdio: 'pipe' });
  for (const path of RUNTIME_DOT_STATE) {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), `runtime ${path}\n`);
  }
  return root;
}

async function withWorkspace<T>(nxignore: string, use: (root: string) => Promise<T>): Promise<T> {
  const root = await workspace(nxignore);
  try {
    return await use(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

/** The candidates Nx's workspace file index holds, as the project graph's file map sees them. */
function indexed(root: string, candidates: string[]): string[] {
  resetWorkspaceContext();
  const matched = new Set(globWithWorkspaceContextSync(root, candidates));
  return candidates.filter((path) => matched.has(path)).sort();
}

/**
 * Paths the native watcher reports once `TRACKED_DOT_WRITE` changes, after the runtime
 * state is written. The watcher is the one the daemon subscribes to, with no filter of
 * its own: it reports whatever its `.nxignore` lets through.
 */
async function reportedAfterWrites(root: string): Promise<string[]> {
  const reported = new Set<string>();
  const failures: string[] = [];
  let watcher: Watcher | undefined;
  let pending: { path: string; resolve(): void; reject(error: Error): void } | undefined;
  try {
    watcher = new Watcher(root);
    // Native watch() registers the filesystem subscription synchronously.
    watcher.watch((error, events) => {
      if (error !== null) {
        failures.push(error);
        pending?.reject(new Error(error));
      }
      for (const event of events) {
        reported.add(event.path);
        if (event.path === pending?.path) {
          pending.resolve();
          pending = undefined;
        }
      }
    });
    const observed = new Promise<void>((resolve, reject) => {
      pending = { path: TRACKED_DOT_WRITE, resolve, reject };
    });
    const writes = (async () => {
      for (const path of RUNTIME_DOT_STATE) await appendFile(join(root, path), 'round 1\n');
      await appendFile(join(root, TRACKED_DOT_WRITE), 'round 1\n');
    })();
    await guardEvent(Promise.all([observed, writes]), `native watcher callback for ${TRACKED_DOT_WRITE}`, () =>
      JSON.stringify({ pending: pending?.path, reported: [...reported], failures }),
    );
    for (const event of watcher.forceFlushPending()) reported.add(event.path);
    expect(failures).toEqual([]);
    return [...reported].sort();
  } finally {
    await watcher?.stop();
  }
}

function runLint(cwd: string) {
  return spawnSync(process.execPath, [lintScript], { cwd, encoding: 'utf8' });
}

it('indexes every tracked dot input and none of the untracked dot state', async () => {
  const tracked = trackedDotPaths();
  const candidates = [...tracked, ...RUNTIME_DOT_STATE];
  const nxignore = await readFile(join(repositoryRoot, '.nxignore'), 'utf8');

  const before = await withWorkspace(PREVIOUS_NXIGNORE, async (root) => indexed(root, candidates));
  // The previous policy indexed untracked dot state: the reproduction of the bug.
  expect(before).toContain('.icm-session/state.json');
  expect(before).toContain('.fseventsd/0000000000000001');

  const after = await withWorkspace(nxignore, async (root) => indexed(root, candidates));
  expect(after.filter((path) => !tracked.includes(path))).toEqual([]);
  // The policy changes the untracked state alone: every tracked input indexes as before.
  expect(after).toEqual(before.filter((path) => tracked.includes(path)));
  expect(after).toContain('.github/workflows/ci.yml');
  expect(after).toContain('.cargo/config.toml');
});

it('keeps the watcher reporting tracked dot inputs and silent about untracked dot state', async () => {
  const nxignore = await readFile(join(repositoryRoot, '.nxignore'), 'utf8');

  // The previous policy wakes the watcher on the daemon-style runtime writes.
  const previous = await withWorkspace(PREVIOUS_NXIGNORE, reportedAfterWrites);
  expect(previous).toContain('.icm-session/state.json');

  const current = await withWorkspace(nxignore, reportedAfterWrites);
  expect(current).toContain(TRACKED_DOT_WRITE);
  expect(current.filter((path) => RUNTIME_DOT_STATE.includes(path))).toEqual([]);
});

it('fails the lint when a tracked dot path lacks its re-include line, and passes with it', async () => {
  const nxignore = await readFile(join(repositoryRoot, '.nxignore'), 'utf8');
  const withoutGithub = nxignore.replace('!/.github\n', '');
  expect(withoutGithub).not.toBe(nxignore);

  await withWorkspace(withoutGithub, async (root) => {
    const result = runLint(root);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('missing "!/.github" after ".*"');
    expect(result.stderr).toContain(TRACKED_DOT_WRITE);
  });

  await withWorkspace(nxignore, async (root) => {
    const result = runLint(root);
    expect(result.stderr).toBe('');
    expect(result.status).toBe(0);
  });

  // The checkout itself must pass the same lint.
  const checkout = runLint(repositoryRoot);
  expect(checkout.stderr).toBe('');
  expect(checkout.status).toBe(0);
});

it('rejects a newly tracked dot input until its explicit negation is added', async () => {
  const nxignore = await readFile(join(repositoryRoot, '.nxignore'), 'utf8');
  await withWorkspace(nxignore, async (root) => {
    await writeFile(join(root, '.new-input'), 'tracked input\n');
    execFileSync('git', ['add', '--force', '.new-input'], { cwd: root });
    const missing = runLint(root);
    expect(missing.status).not.toBe(0);
    expect(missing.stderr).toContain('missing "!/.new-input"');
    await appendFile(join(root, '.nxignore'), '!/.new-input\n');
    const included = runLint(root);
    expect(included.status, included.stderr).toBe(0);
  });
});

it('preserves tracked input hashes and the graph while dot staging never causes graph passes', async () => {
  const nxignore = await readFile(join(repositoryRoot, '.nxignore'), 'utf8');
  await withNxFixture('nx-dot-graph-', async ({ workspace: root, socketDirectory }) => {
    const copied = await workspace(PREVIOUS_NXIGNORE);
    try {
      // Reuse the complete tracked-dot fixture, including real symlink targets.
      execFileSync('cp', ['-R', `${copied}/.`, root]);
    } finally {
      await rm(copied, { recursive: true, force: true });
    }
    await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
    await writeFile(join(root, 'nx.json'), '{}\n');
    await writeFile(join(root, 'package.json'), '{"name":"dot-graph-proof","private":true}\n');
    await mkdir(join(root, 'p'));
    await writeFile(join(root, 'p/project.json'), '{"name":"p","targets":{"check":{"executor":"nx:noop"}}}\n');
    const env = { ...fixtureNxEnv(root, socketDirectory), NX_DAEMON: 'true' };
    const nx = join(repositoryRoot, 'node_modules/.bin/nx');
    const graphPath = join(root, '.nx/graph.json');
    const graph = async () => {
      const result = spawnSync('node', [nx, 'graph', '--file', graphPath], { cwd: root, env, encoding: 'utf8' });
      expect(result.status, result.stdout + result.stderr).toBe(0);
      const parsed: unknown = JSON.parse(await readFile(graphPath, 'utf8'));
      return parsed;
    };
    const tracked = new Set(trackedDotPaths().filter((path) => path !== '.nxignore'));
    // The policy file intentionally changes; all other tracked input bytes do not.
    const beforeHashes = Object.fromEntries(
      new WorkspaceContext(root, join(root, '.nx/hash-proof'))
        .allFileData()
        .filter(({ file }) => tracked.has(file))
        .map(({ file, hash }) => [file, hash]),
    );
    const beforeGraph = await graph();
    const previous: unknown = JSON.parse(
      await readFile(join(root, '.nx/workspace-data/d/server-process.json'), 'utf8'),
    );
    if (
      typeof previous !== 'object' ||
      previous === null ||
      !('processId' in previous) ||
      typeof previous.processId !== 'number'
    ) {
      throw new Error('the graph proof did not start its owned daemon');
    }
    const beforeProcesses = (await processTable()).filter(({ pid }) => pid === previous.processId);
    await writeFile(join(root, '.nxignore'), nxignore);
    // Let the old startup ignore snapshot retire before requesting the new graph.
    await awaitExit(beforeProcesses, 'the daemon whose ignore policy changed');
    const started = spawnSync('node', [nx, 'daemon', '--start'], { cwd: root, env, encoding: 'utf8' });
    expect(started.status, started.stdout + started.stderr).toBe(0);
    const afterGraph = await graph();
    expect(afterGraph).toEqual(beforeGraph);
    const afterHashes = Object.fromEntries(
      new WorkspaceContext(root, join(root, '.nx/hash-proof'))
        .allFileData()
        .filter(({ file }) => tracked.has(file))
        .map(({ file, hash }) => [file, hash]),
    );
    expect(afterHashes).toEqual(beforeHashes);
    expect(Object.keys(beforeHashes)).toContain(TRACKED_DOT_WRITE);
    const logPath = join(root, '.nx/workspace-data/d/daemon.log');
    const passes = async () =>
      (await readFile(logPath, 'utf8'))
        .split('\n')
        .filter((line) => line.includes('Updated workspace context based on watched changes')).length;
    // IPC readiness and callbacks prove the daemon routed the native batch;
    // a log append alone is not a client readiness signal.
    const clientPath = createRequire(import.meta.url).resolve('nx/src/daemon/client/client');
    const observer = spawn(
      'node',
      [
        '-e',
        `
      const { daemonClient } = require(${JSON.stringify(clientPath)});
      daemonClient.registerFileWatcher({ watchProjects: 'all', includeGlobalWorkspaceFiles: true }, (error, data) => {
        if (error !== null) { console.log('error:' + error); process.exit(1); }
        for (const { path } of data.changedFiles) console.log(path);
      }).then(() => console.log('ready'), error => { console.log('error:' + error); process.exit(1); });
    `,
      ],
      { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] },
    );
    let stderr = '';
    observer.stderr.setEncoding('utf8').on('data', (text: string) => {
      stderr += text;
    });
    const ended = new Promise<void>((resolve) => observer.once('exit', () => resolve()));
    const lines = createInterface({ input: observer.stdout })[Symbol.asyncIterator]();
    try {
      const ready = await guardEvent(lines.next(), 'the daemon file watcher to register', () => stderr);
      expect(ready.done).toBe(false);
      expect(ready.value, stderr).toBe('ready');
      const beforePasses = await passes();
      for (const path of RUNTIME_DOT_STATE) await appendFile(join(root, path), 'staging write\n');
      // The real graph request flushes the daemon's pending native watch batch.
      expect(await graph()).toEqual(afterGraph);
      const ignoredPasses = await passes();
      expect(ignoredPasses).toBe(beforePasses);
      await appendFile(join(root, TRACKED_DOT_WRITE), '\n# tracked edit\n');
      for (;;) {
        const changed = await guardEvent(
          lines.next(),
          `the daemon watcher to report ${TRACKED_DOT_WRITE}`,
          () => stderr,
        );
        if (changed.done || changed.value.startsWith('error:')) {
          throw new Error(`daemon watcher ended before the tracked edit: ${changed.value}\n${stderr}`);
        }
        if (changed.value === TRACKED_DOT_WRITE) break;
      }
      await graph();
      const trackedPasses = await passes();
      expect(trackedPasses).toBeGreaterThan(beforePasses);
      console.log(
        JSON.stringify({
          proof: 'nx-dot-input-policy',
          trackedDotPaths: tracked.size + 1,
          unchangedFileHashes: Object.keys(beforeHashes).length,
          unchangedGraph: true,
          beforePasses,
          ignoredPasses,
          trackedPasses,
          runtimePaths: RUNTIME_DOT_STATE,
        }),
      );
    } finally {
      if (observer.pid !== undefined && observer.exitCode === null && observer.signalCode === null)
        terminate(observer.pid);
      await ended;
    }
  });
});
