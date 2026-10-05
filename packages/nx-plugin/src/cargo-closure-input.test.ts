import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, stat, symlink, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import type { CreateNodesResultV2 } from 'nx/src/devkit-exports.js';
import { Watcher } from 'nx/src/native/index.js';
import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import {
  aggregateOf,
  type CountedCargo,
  countedCargo,
  expectGraphRefusal,
  guardEvent,
  rejectionOf,
  waitForCargoJoins,
  withEnvironment,
} from './__tests__/counted-cargo.js';
import { useFixtureCargoHome } from './__tests__/fixture-cargo-home.js';
import { fixtureNxEnv } from './__tests__/fixture-nx-env.js';
import { CARGO_CLOSURE_INPUT, indexedCargoManifests } from './cargo-closure-input.js';
import { createNodesV2 } from './index.js';

const repositoryRoot = join(import.meta.dir, '../../..');

useFixtureCargoHome();

/**
 * An Nx workspace that is also the Cargo workspace. `app` reaches `leaf`
 * through `bridge`, and `external` — outside the Nx workspace — through
 * `bridge` too. `unrelated` is a member nothing depends on. `app` records
 * every real compile in `executions.log`, so a cache hit is an unchanged log.
 * `extraFiles` (paths relative to `root`) are written with the rest, last.
 */
async function closureFixture(
  root: string,
  gitignore = 'node_modules/\ntarget/\ndist/\n.nx/\n',
  extraFiles: Record<string, string> = {},
) {
  const workspace = join(root, 'workspace');
  const executions = join(root, 'executions.log');
  const output = join(workspace, 'packages/app/dist/result.txt');
  const files: Record<string, string> = {
    'workspace/.gitignore': gitignore,
    'workspace/package.json':
      '{"name":"cargo-closure-fixture","private":true,"workspaces":["packages/*","crates/*"]}\n',
    'workspace/nx.json': JSON.stringify({
      plugins: ['@smoothbricks/nx-plugin'],
      namedInputs: { externalRustCrates: [] },
    }),
    // Exercise the real linked-source path: Node's strip-only loader must
    // import the plugin when the workspace selects development exports.
    'workspace/tsconfig.base.json': JSON.stringify({ compilerOptions: { customConditions: ['development'] } }),
    'workspace/Cargo.toml':
      '[workspace]\nmembers=["packages/app","crates/bridge","crates/leaf","crates/unrelated"]\nresolver="2"\n',
    'workspace/packages/app/package.json': JSON.stringify({
      name: 'app',
      private: true,
      nx: {
        targets: {
          compile: {
            executor: 'nx:run-commands',
            cache: true,
            inputs: ['{projectRoot}/compile.ts', CARGO_CLOSURE_INPUT],
            outputs: ['{projectRoot}/dist'],
            options: { command: 'bun packages/app/compile.ts' },
          },
        },
      },
    }),
    'workspace/packages/app/compile.ts': `import { execFileSync } from 'node:child_process';
import { appendFile, mkdir, writeFile } from 'node:fs/promises';
const result = execFileSync('cargo', ['run', '--quiet', '--locked', '--offline', '--manifest-path', 'packages/app/Cargo.toml'], { encoding: 'utf8' });
await mkdir(${JSON.stringify(dirname(output))}, { recursive: true });
await writeFile(${JSON.stringify(output)}, result);
await appendFile(${JSON.stringify(executions)}, result);
`,
    'workspace/packages/app/Cargo.toml':
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../../crates/bridge"}\n',
    'workspace/packages/app/src/main.rs': 'fn main() { println!("{}", bridge::answer()); }\n',
    'workspace/crates/bridge/Cargo.toml':
      '[package]\nname="bridge"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nleaf={path="../leaf"}\nexternal={path="../../../external"}\n',
    'workspace/crates/bridge/src/lib.rs': 'pub fn answer() -> u8 { leaf::answer() + external::answer() }\n',
    'workspace/crates/leaf/package.json': '{"name":"leaf","private":true}\n',
    'workspace/crates/leaf/Cargo.toml': '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/leaf/src/lib.rs': 'pub fn answer() -> u8 { 1 }\n',
    'workspace/crates/unrelated/Cargo.toml': '[package]\nname="unrelated"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/unrelated/src/lib.rs': 'pub fn unrelated() -> u8 { 3 }\n',
    'external/Cargo.toml': '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
    'external/src/lib.rs': 'pub fn answer() -> u8 { 10 }\n',
    ...extraFiles,
  };
  for (const [path, text] of Object.entries(files)) {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), text);
  }
  execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
    stdio: 'pipe',
  });

  /** A fresh, daemonless Node CLI, loading the linked plugin through development exports. */
  async function compile(): Promise<void> {
    const child = Bun.spawn(['node', join(repositoryRoot, 'node_modules/.bin/nx'), 'run', 'app:compile'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace),
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(exitCode, stdout + stderr).toBe(0);
  }
  /**
   * Rewrite a fixture file as an edit Nx can see. Nx keeps each file's hash
   * beside its mtime in whole seconds and reuses the hash until that second
   * changes, so a rewrite inside the second the file was last hashed in is
   * invisible to every fileset input. A fast host runs this whole fixture in
   * about a second; the edit is dated at least a second past the old mtime.
   */
  async function edit(path: string, text: string): Promise<void> {
    const file = join(root, path);
    const { mtimeMs } = await stat(file);
    await writeFile(file, text);
    const edited = new Date(Math.max(Date.now(), mtimeMs + 1000));
    await utimes(file, edited, edited);
  }
  const log = () => readFile(executions, 'utf8');
  return { workspace, compile, edit, log };
}

it('keys a custom target on exactly its Cargo closure, inside the workspace and out', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-')));
  try {
    const { compile, edit, log } = await closureFixture(root);
    await compile();
    expect(await log()).toBe('11\n');
    await compile();
    expect(await log()).toBe('11\n');

    // Reached only through bridge: no path of app's own names it.
    await edit('workspace/crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n');

    // A workspace member outside the closure is not an input.
    await edit('workspace/crates/unrelated/src/lib.rs', 'pub fn unrelated() -> u8 { 4 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n');

    // Outside the Nx workspace, where no fileset reaches.
    await edit('external/src/lib.rs', 'pub fn answer() -> u8 { 20 }\n');
    await compile();
    expect(await log()).toBe('11\n12\n22\n');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

it('refuses a closure member in an ignored directory, which no fileset can see, instead of hashing the whole workspace', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-ignored-')));
  try {
    const { workspace } = await closureFixture(root, 'node_modules/\ntarget/\ndist/\n.nx/\ncrates/leaf/\n');
    resetWorkspaceContext();
    const failure = aggregateOf(await rejectionOf(graphComputation(workspace)()));
    const refusals = failure.errors.flatMap(([, error]) => (error.name === 'CargoClosureInputError' ? [error] : []));
    expect(refusals.length).toBeGreaterThan(0);
    for (const error of refusals) expect(error.message).toContain('crates/leaf');
    // `app` reaches the ignored crate through `bridge`: its closure is what no fileset can name.
    expect(refusals.map((error) => Reflect.get(error, 'projectRoot'))).toContain('packages/app');
    // No runtime hash of the whole workspace stands in for it.
    expect(JSON.stringify(failure.partialResults)).not.toContain('--include-workspace');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

it('leaves a closure inside the workspace to Nx: no process runs when its tasks are hashed', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-inferred-')));
  try {
    const { workspace } = await closureFixture(root);
    resetWorkspaceContext();
    const [, infer] = createNodesV2;
    const inferred = await infer(['package.json', 'packages/app/package.json', 'crates/leaf/package.json'], undefined, {
      workspaceRoot: workspace,
      nxJsonConfiguration: { namedInputs: { externalRustCrates: [] } },
    });
    const closureOf = (projectRoot: string) =>
      inferred.find(([file]) => dirname(file) === projectRoot)?.[1].projects?.[projectRoot]?.namedInputs?.[
        CARGO_CLOSURE_INPUT
      ];
    const runtimeCommands = (projectRoot: string) =>
      (closureOf(projectRoot) ?? []).flatMap((input) =>
        typeof input === 'object' && 'runtime' in input ? [input.runtime] : [],
      );

    expect(closureOf('crates/leaf')).toContain('{workspaceRoot}/Cargo.lock');
    expect(runtimeCommands('crates/leaf')).toEqual([]);
    // app reaches `external`, the one member only a process can hash, and
    // only it: the in-workspace members stay filesets.
    expect(runtimeCommands('packages/app')).toEqual([
      'node node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-cargo-hash.js --closure packages/app Cargo.toml',
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

/** `leaf` as the fixture writes it, and the dependency that draws `unrelated` into every closure reaching `leaf`. */
const LEAF_MANIFEST = '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n';
const LEAF_LINK = '[dependencies]\nunrelated={path="../unrelated"}\n';

/** The fileset the plugin names for the closure members `crates` (a name or a `{a,b}` set) under `crates/`. */
const crateFiles = (crates: string) =>
  `{workspaceRoot}/crates/${crates}/**/{*.rs,Cargo.toml,.cargo/config,.cargo/config.toml}`;

/** One project-graph computation over the fixture: the plugin's `createNodes`, as Nx runs it for each recomputation. */
function graphComputation(workspace: string) {
  const [, infer] = createNodesV2;
  return async () =>
    infer(['package.json', 'packages/app/package.json', 'crates/leaf/package.json'], undefined, {
      workspaceRoot: workspace,
      nxJsonConfiguration: { namedInputs: { externalRustCrates: [] } },
    });
}

/** The computation's nodes in file order: the plugin pushes them as their projects finish. */
function byFile(graph: CreateNodesResultV2): CreateNodesResultV2 {
  return [...graph].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
}

/** The `cargoClosure` definition the computation gave the project at `projectRoot`. */
function closureOf(graph: CreateNodesResultV2, projectRoot: string) {
  return (
    graph.find(([file]) => dirname(file) === projectRoot)?.[1].projects?.[projectRoot]?.namedInputs?.[
      CARGO_CLOSURE_INPUT
    ] ?? []
  );
}

/** Turn `leaf`'s dependency on `unrelated` on or off, and bring Cargo.lock in line with the manifests. */
function leafControls(workspace: string, edit: (path: string, text: string) => Promise<void>) {
  return {
    linkLeaf: (linked: boolean) => edit('workspace/crates/leaf/Cargo.toml', LEAF_MANIFEST + (linked ? LEAF_LINK : '')),
    relock: () =>
      execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
        stdio: 'pipe',
      }),
  };
}

it('shares one Cargo child among overlapping graph computations, and an unchanged workspace never asks Cargo again', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-overlap-')));
  let fixtureCargo: CountedCargo | undefined;
  try {
    const { workspace, edit } = await closureFixture(root);
    const { linkLeaf, relock } = leafControls(workspace, edit);
    const cargo = await countedCargo(root);
    fixtureCargo = cargo;
    const graph = graphComputation(workspace);
    resetWorkspaceContext();
    await withEnvironment(cargo.environment, async () => {
      // Watcher events start computations while earlier ones still run: here sixteen at once, over a
      // workspace nobody has resolved, each Cargo child holding its answer so that a child per
      // computation would certainly overlap. One child answers all of them.
      await cargo.beforeAnswer.close();
      const joins = waitForCargoJoins(join(workspace, 'Cargo.toml'), 15);
      let overlapping: CreateNodesResultV2[];
      try {
        const computations = Array.from({ length: 16 }, graph);
        await cargo.waitForRuns((runs) => runs.read === 1, 'the shared Cargo answer');
        await joins.joined;
        await cargo.beforeAnswer.open();
        overlapping = await Promise.all(computations);
      } finally {
        joins.dispose();
      }
      const overlapped = await cargo.runs();
      expect(overlapped.spawned).toBe(1);
      expect(overlapped.peak).toBe(1);

      const baseline = byFile(await graph());
      for (const computed of overlapping) expect(byFile(computed)).toEqual(baseline);
      expect(closureOf(baseline, 'packages/app')).toContain(crateFiles('{bridge,leaf}'));
      expect(closureOf(baseline, 'packages/app')).not.toContain(crateFiles('{bridge,leaf,unrelated}'));

      // Nothing Cargo reads has changed since, so no computation asks it again, however often it runs.
      expect(byFile(await graph())).toEqual(baseline);
      expect((await cargo.runs()).spawned).toBe(1);

      // An edit to what Cargo reads is seen by the next computation, and costs one child.
      await linkLeaf(true);
      relock();
      const edited = byFile(await graph());
      expect(closureOf(edited, 'packages/app')).toContain(crateFiles('{bridge,leaf,unrelated}'));
      expect(byFile(await graph())).toEqual(edited);
      const settled = await cargo.runs();
      expect(settled.spawned).toBe(2);
      expect(settled.peak).toBe(1);
    });
  } finally {
    await fixtureCargo?.release();
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

it('refuses with a typed error what Cargo refuses, and follows an edit made while a child holds its answer with a resolution after it', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-pending-')));
  let fixtureCargo: CountedCargo | undefined;
  try {
    const { workspace, edit } = await closureFixture(root);
    const { linkLeaf, relock } = leafControls(workspace, edit);
    const cargo = await countedCargo(root);
    fixtureCargo = cargo;
    const graph = graphComputation(workspace);
    const baselineLock = await readFile(join(workspace, 'Cargo.lock'), 'utf8');
    resetWorkspaceContext();
    await withEnvironment(cargo.environment, async () => {
      const baseline = byFile(await graph());

      // Cargo refuses a Cargo.lock the manifests have outgrown (`--locked`): the computation fails,
      // whole, with Cargo's reason. No runtime hash stands in for the closure, and the refusal is
      // not kept for the next computation.
      await linkLeaf(true);
      expectGraphRefusal(await rejectionOf(graph()), join(workspace, 'Cargo.toml'));
      relock();
      const repaired = byFile(await graph());
      expect(closureOf(repaired, 'packages/app')).toContain(crateFiles('{bridge,leaf,unrelated}'));
      expect(JSON.stringify(repaired)).not.toContain('--include-workspace');
      expect((await cargo.runs()).spawned).toBe(3);

      // A computation that asks while a child holds its answer waits for that child. One that asks
      // after an edit made since the child read the workspace does not take that answer for the edited
      // workspace's: a resolution follows the first, never beside it.
      await cargo.beforeAnswer.close();
      await linkLeaf(false);
      await writeFile(join(workspace, 'Cargo.lock'), baselineLock);
      const before = await cargo.runs();
      const early = graph();
      await cargo.waitForRuns((runs) => runs.read > before.read, 'the running Cargo child to read the workspace');
      // Use content the successful cache has never seen; restoring the repaired
      // content would correctly reuse that entry after discarding the held answer.
      await edit(
        'workspace/crates/leaf/Cargo.toml',
        (LEAF_MANIFEST + LEAF_LINK).replace('version="0.1.0"', 'version="0.2.0"'),
      );
      relock();
      const joins = waitForCargoJoins(join(workspace, 'Cargo.toml'), 1);
      const late = graph();
      try {
        await joins.joined;
      } finally {
        joins.dispose();
      }
      await cargo.beforeAnswer.open();
      expect(byFile(await late)).toEqual(repaired);
      // The early computation began before the edit: it saw the workspace on either side of it.
      expect([baseline, repaired]).toContainEqual(byFile(await early));
      const shared = await cargo.runs();
      expect(shared.spawned - before.spawned).toBe(2);
      expect(shared.peak).toBe(1);

      // Nothing is pending once both have settled, and nothing has changed since.
      expect(byFile(await graph())).toEqual(repaired);
      expect((await cargo.runs()).spawned).toBe(shared.spawned);
    });
  } finally {
    await fixtureCargo?.release();
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

it("indexes a source tree's Cargo manifests and none of the runtime state a cowshed keeps in it", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-runtime-state-')));
  try {
    const { workspace } = await closureFixture(root, undefined, {
      'workspace/.nxignore': await readFile(join(repositoryRoot, '.nxignore'), 'utf8'),
      'workspace/.cowshed/cache/nx/workspace-data/d/daemon.log': 'daemon log\n',
      'workspace/.cowshed/job/1/stdout.log': 'job output\n',
      'workspace/.cowshed/cache/cargo/registry/Cargo.toml':
        '[package]\nname="cached"\nversion="0.1.0"\nedition="2021"\n',
    });
    resetWorkspaceContext();
    expect([...(await indexedCargoManifests(workspace))].sort()).toEqual([
      'Cargo.toml',
      'crates/bridge/Cargo.toml',
      'crates/leaf/Cargo.toml',
      'crates/unrelated/Cargo.toml',
      'packages/app/Cargo.toml',
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 120_000);

const SOURCE_FILE = 'src/lib.rs';

/**
 * Every path the native watcher reports for a fresh workspace under `nxignore`
 * while a cowshed writes its runtime state (the daemon's own log, job output,
 * a supervisor's pid file) beside a source file that changes every round. This is
 * the watcher Nx's daemon subscribes to the workspace with: it recomputes the
 * project graph for each batch of events it reports, with no filter of its
 * own. Each round advances on its unique source file's native callback, not
 * on a guessed subscription or debounce delay.
 */
async function watchedPaths(nxignore: string): Promise<string[]> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-closure-watch-')));
  const reported = new Set<string>();
  const failures: string[] = [];
  let watcher: Watcher | undefined;
  let pending: { path: string; resolve(): void; reject(error: Error): void } | undefined;
  try {
    execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });
    await writeFile(join(root, '.nxignore'), nxignore);
    await mkdir(join(root, 'src'), { recursive: true });
    await mkdir(join(root, '.cowshed/cache/nx/workspace-data/d'), { recursive: true });
    await mkdir(join(root, '.cowshed/run'), { recursive: true });
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
    for (let round = 0; round < 5; round += 1) {
      const source = round === 0 ? SOURCE_FILE : `src/round-${round}.rs`;
      const observed = new Promise<void>((resolve, reject) => {
        pending = { path: source, resolve, reject };
      });
      const writes = (async () => {
        await appendFile(join(root, '.cowshed/cache/nx/workspace-data/d/daemon.log'), `round ${round}\n`);
        await mkdir(join(root, '.cowshed/job', String(round)), { recursive: true });
        await writeFile(join(root, '.cowshed/job', String(round), 'stdout.log'), `round ${round}\n`);
        await writeFile(join(root, '.cowshed/run/supervisor.pid'), `${round}\n`);
        await writeFile(join(root, source), `pub fn round() -> u8 { ${round} }\n`);
      })();
      await guardEvent(Promise.all([observed, writes]), `native watcher callback for ${source}`, () =>
        JSON.stringify({ pending: pending?.path, reported: [...reported], failures }),
      );
      for (const event of watcher.forceFlushPending()) reported.add(event.path);
    }
    expect(failures).toEqual([]);
    return [...reported].sort();
  } finally {
    await watcher?.stop();
    await rm(root, { recursive: true, force: true });
  }
}

it("keeps a cowshed's runtime state from waking the watcher that recomputes the project graph", async () => {
  // With nothing ignored, the watcher does report runtime state: that is what
  // makes the daemon's own log writes feed its next recomputation.
  expect((await watchedPaths('')).filter((path) => path.startsWith('.cowshed'))).not.toEqual([]);

  // With the repository's own .nxignore it reports the source file alone.
  const paths = await watchedPaths(await readFile(join(repositoryRoot, '.nxignore'), 'utf8'));
  expect(paths).toContain(SOURCE_FILE);
  expect(paths.filter((path) => path.startsWith('.cowshed'))).toEqual([]);
}, 120_000);
