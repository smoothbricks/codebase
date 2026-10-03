import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, symlink, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { delimiter, dirname, join } from 'node:path';

import type { CreateNodesResultV2 } from 'nx/src/devkit-exports.js';
import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import {
  type CannedCargo,
  type CountedCargo,
  cannedCargo,
  countedCargo,
  expectCargoMetadataError,
  expectGraphRefusal,
  rejectionOf,
  withEnvironment,
} from './__tests__/counted-cargo.js';
import { CARGO_CLOSURE_INPUT } from './cargo-closure-input.js';
import { type CargoResolve, readCargoResolve } from './cargo-source-hash.js';
import { createNodesV2 } from './index.js';

/*
 * What Cargo resolution costs, measured by the one number that matters: how many `cargo metadata`
 * children were actually started. A `cargo` ahead of the real one on PATH logs each child's life
 * (`__tests__/counted-cargo.ts`); no assertion here reads a clock.
 *
 * The file depends on nothing the resolution cache adds, so it runs against a tree without the
 * cache and fails there on the counts. `readCargoResolve`, the project graph and the hash command
 * are reached through what every version of them exports; `CargoMetadataError` is looked up by name.
 *
 * Every fixture is a temporary directory of its own, with its own Nx workspace-data directory (where
 * the cache is kept), deleted with the fixture.
 */

const sourceModule = join(import.meta.dir, 'cargo-source-hash.ts');
const cliModule = join(import.meta.dir, 'bin', 'smoo-nx-cargo-hash.ts');
const repositoryRoot = join(import.meta.dir, '../../..');

const spawned = async (cargo: CountedCargo): Promise<number> => (await cargo.runs()).spawned;

async function put(root: string, path: string, text: string): Promise<void> {
  const file = join(root, path);
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, text);
}

/** Rewrite `path` as an editor that changed nothing would: the same bytes, a later mtime. */
async function touch(root: string, path: string): Promise<void> {
  const file = join(root, path);
  await writeFile(file, await readFile(file));
  const later = new Date(Date.now() + 10_000);
  await utimes(file, later, later);
}

/**
 * Polls real processes: a child's first output and its reaping by init are
 * signals no promise in this process exposes, so no fake clock can stand in.
 */
async function eventually(condition: () => boolean | Promise<boolean>, what: string): Promise<void> {
  for (let attempt = 0; attempt < 3000; attempt++) {
    if (await condition()) return;
    await Bun.sleep(10);
  }
  throw new Error(`timed out waiting for ${what}`);
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ESRCH') return false;
    throw error;
  }
}

function summarize(resolved: CargoResolve) {
  return {
    root: resolved.root,
    members: [...resolved.members],
    local: resolved.local,
    edges: resolved.edges === null ? null : [...resolved.edges],
  };
}

/* ---- The cache's own behavior, against a Cargo that answers whatever the test says ---- */

const WORKSPACE_MANIFEST = '[workspace]\nmembers = ["crates/leaf"]\nresolver = "2"\n';
const WORKSPACE_FILES: Readonly<Record<string, string>> = {
  'Cargo.toml': WORKSPACE_MANIFEST,
  'Cargo.lock': '# fixture lock\nversion = 3\n',
  'crates/leaf/Cargo.toml': '[package]\nname = "leaf"\nversion = "0.1.0"\nedition = "2021"\n',
  'crates/leaf/src/lib.rs': 'pub fn leaf() -> u8 { 1 }\n',
  'README.md': '# fixture\n',
};

/** A package outside the Nx workspace that the member depends on by path: Cargo's answer names it, so its manifest is an input. */
const EXTERNAL_MANIFEST = '[package]\nname = "external"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n';
const EXTERNAL_SOURCE = 'pub fn external() -> u8 { 10 }\n';

/**
 * What `cargo metadata` prints for the fixture when the workspace's one member is called `name`:
 * it depends on the package outside the workspace, which Cargo reports as a local package too.
 */
function metadataNamed(root: string, name: string) {
  const workspace = join(root, 'workspace');
  return {
    packages: [
      {
        id: name,
        source: null,
        manifest_path: join(workspace, 'crates/leaf/Cargo.toml'),
        targets: [{ src_path: join(workspace, 'crates/leaf/src/lib.rs') }],
      },
      {
        id: 'external',
        source: null,
        manifest_path: join(root, 'external/Cargo.toml'),
        targets: [{ src_path: join(root, 'external/src/lib.rs') }],
      },
    ],
    resolve: {
      nodes: [
        { id: name, deps: [{ pkg: 'external' }] },
        { id: 'external', deps: [] },
      ],
    },
    workspace_members: [name],
    workspace_root: workspace,
  };
}

/** A process that has never resolved anything: it announces it is about to ask, asks, and reports the answer. */
const RESOLVE_WORKER = [
  "import { writeFileSync } from 'node:fs';",
  `import { readCargoResolve } from ${JSON.stringify(sourceModule)};`,
  'const [manifest, cwd, output] = process.argv.slice(2);',
  "writeFileSync(output + '.ready', '');",
  'const resolved = await readCargoResolve(manifest, cwd);',
  'writeFileSync(',
  '  output,',
  '  JSON.stringify({',
  '    root: resolved.root,',
  '    members: [...resolved.members],',
  '    local: resolved.local,',
  '    edges: resolved.edges === null ? null : [...resolved.edges],',
  '  }),',
  ');',
  '',
].join('\n');

interface CannedWorkspace {
  readonly root: string;
  readonly workspace: string;
  readonly manifest: string;
  readonly cargo: CannedCargo;
  /** What a process under test needs in its environment; this process already has it. */
  readonly environment: Readonly<Record<string, string>>;
  /** What Cargo says from now on: the workspace has one member, called `name`. */
  answer(name: string): Promise<void>;
  /** The members `readCargoResolve` reports for the workspace now. */
  members(): Promise<string[]>;
}

/** `body` over a temporary Cargo workspace and the canned Cargo the module under test will find first. */
async function withCannedWorkspace(body: (fixture: CannedWorkspace) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-cache-')));
  try {
    const workspace = join(root, 'workspace');
    for (const [path, text] of Object.entries(WORKSPACE_FILES)) await put(workspace, path, text);
    await put(root, 'external/Cargo.toml', EXTERNAL_MANIFEST);
    await put(root, 'external/src/lib.rs', EXTERNAL_SOURCE);
    await put(root, 'resolve-worker.ts', RESOLVE_WORKER);
    const cargo = await cannedCargo(root);
    try {
      const manifest = join(workspace, 'Cargo.toml');
      const environment = { ...cargo.environment, CARGO_HOME: join(root, 'cargo-home') };
      await withEnvironment(environment, () =>
        body({
          root,
          workspace,
          manifest,
          cargo,
          environment,
          answer: (name) => cargo.answerWith(metadataNamed(root, name)),
          members: async () => [...(await readCargoResolve(manifest, workspace)).members],
        }),
      );
    } finally {
      await cargo.release();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

/** `readCargoResolve` in a fresh process: a second Nx worker, or the next `nx` run. */
function freshResolver({ root, workspace, manifest, environment }: CannedWorkspace, name: string) {
  const output = join(root, `${name}.json`);
  const child = Bun.spawn([process.execPath, join(root, 'resolve-worker.ts'), manifest, workspace, output], {
    cwd: workspace,
    env: { ...process.env, ...environment },
    stdout: 'ignore',
    stderr: 'pipe',
  });
  const result = (async (): Promise<unknown> => {
    const [stderr, exitCode] = await Promise.all([new Response(child.stderr).text(), child.exited]);
    expect(exitCode, stderr).toBe(0);
    const resolved: unknown = JSON.parse(await readFile(output, 'utf8'));
    return resolved;
  })();
  return { ready: () => existsSync(`${output}.ready`), result };
}

it('concurrent resolutions of one Cargo workspace share one cargo child, and an unchanged workspace never runs Cargo again', async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    await answer('before');
    // The child holds its answer for half a second after it read the workspace,
    // so every call below arrives while it is pending.
    await cargo.holdFor(0.5);
    const results = await Promise.all(Array.from({ length: 16 }, () => readCargoResolve(manifest, workspace)));
    expect(await spawned(cargo)).toBe(1);
    expect(new Set(results).size).toBe(1);
    for (const result of results) expect([...result.members]).toEqual(['before']);
    await cargo.holdFor(0);

    // Nothing Cargo's answer depends on has changed, so Cargo, which would
    // answer differently now, is not asked.
    await answer('after');
    expect(await members()).toEqual(['before']);
    expect(await spawned(cargo)).toBe(1);

    // A manifest edit is what changes the answer; it costs one child.
    await appendFile(manifest, '# edited\n');
    expect(await members()).toEqual(['after']);
    expect(await members()).toEqual(['after']);
    expect(await spawned(cargo)).toBe(2);
  });
}, 20_000);

it("a failed resolution rejects every sharer with a CargoMetadataError carrying Cargo's stderr, and is not kept for the next call", async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    await cargo.failWith('fake cargo: resolution failed\n');
    await cargo.holdFor(0.3);
    const failures = await Promise.all(
      [readCargoResolve(manifest, workspace), readCargoResolve(manifest, workspace)].map(rejectionOf),
    );
    for (const failure of failures) {
      expect(expectCargoMetadataError(failure, manifest).message).toContain('fake cargo: resolution failed');
    }
    expect(await spawned(cargo)).toBe(1);

    // Nothing on disk changed, and a refusal is no answer: Cargo is asked again.
    await cargo.recover();
    await answer('recovered');
    expect(await members()).toEqual(['recovered']);
    expect(await spawned(cargo)).toBe(2);
    expect(await members()).toEqual(['recovered']);
    expect(await spawned(cargo)).toBe(2);
  });
}, 20_000);

it('a Cargo that cannot run, or whose answer is not metadata, is a CargoMetadataError', async () => {
  await withCannedWorkspace(async ({ cargo, workspace, manifest }) => {
    for (const text of ['this is not JSON', '{"packages": 3}']) {
      await cargo.answerWithText(text);
      expectCargoMetadataError(await rejectionOf(readCargoResolve(manifest, workspace)), manifest);
    }
    expect(await spawned(cargo)).toBe(2);

    await withEnvironment({ PATH: join(workspace, 'no-cargo-here') }, async () => {
      expectCargoMetadataError(await rejectionOf(readCargoResolve(manifest, workspace)), manifest);
    });
    expect(await spawned(cargo)).toBe(2);
  });
}, 20_000);

it('source edits, non-Rust edits and a rewrite of the same bytes never run Cargo', async () => {
  await withCannedWorkspace(async ({ root, cargo, answer, members, workspace }) => {
    await answer('cold');
    expect(await members()).toEqual(['cold']);
    const unkeyed: [string, () => Promise<void>][] = [
      ['a source edit', () => put(workspace, 'crates/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 2 }\n')],
      ['a new source file', () => put(workspace, 'crates/leaf/src/extra.rs', 'pub const EXTRA: u8 = 1;\n')],
      ['a non-Rust edit', () => put(workspace, 'README.md', '# edited\n')],
      ['a new non-Rust file', () => put(workspace, 'crates/leaf/notes.json', '{}\n')],
      ['the manifest rewritten with the same bytes', () => touch(workspace, 'crates/leaf/Cargo.toml')],
      ['Cargo.lock rewritten with the same bytes', () => touch(workspace, 'Cargo.lock')],
      [
        'a source edit outside the workspace',
        () => put(root, 'external/src/lib.rs', 'pub fn external() -> u8 { 11 }\n'),
      ],
    ];
    for (const [what, change] of unkeyed) {
      await change();
      // A Cargo asked after this change would say so.
      await answer(`after ${what}`);
      expect(await members(), what).toEqual(['cold']);
    }
    expect(await spawned(cargo)).toBe(1);
  });
}, 20_000);

interface KeyedFile {
  readonly name: string;
  /** Relative to the fixture root; the workspace is `workspace/` in it. */
  readonly path: string;
  /** Whether the fixture has the file before the test touches it. */
  readonly exists: boolean;
  /** Whether the test may delete it: the fixture's Cargo answers with packages whose manifests must stay. */
  readonly removable: boolean;
}

/** Every file whose bytes Cargo's answer depends on, or that the toolchain pin and configuration stand for. */
const KEYED_FILES: readonly KeyedFile[] = [
  { name: 'the workspace manifest', path: 'workspace/Cargo.toml', exists: true, removable: false },
  { name: "a member's manifest", path: 'workspace/crates/leaf/Cargo.toml', exists: true, removable: false },
  {
    name: "a path dependency's manifest outside the workspace",
    path: 'external/Cargo.toml',
    exists: true,
    removable: false,
  },
  { name: 'a new manifest in the workspace', path: 'workspace/crates/new/Cargo.toml', exists: false, removable: true },
  { name: 'Cargo.lock', path: 'workspace/Cargo.lock', exists: true, removable: true },
  {
    name: 'a .cargo/config.toml in the workspace',
    path: 'workspace/.cargo/config.toml',
    exists: false,
    removable: true,
  },
  { name: 'a .cargo/config in the workspace', path: 'workspace/.cargo/config', exists: false, removable: true },
  { name: 'a .cargo/config.toml above the workspace', path: '.cargo/config.toml', exists: false, removable: true },
  { name: 'a Cargo.toml above the workspace', path: 'Cargo.toml', exists: false, removable: true },
  { name: "CARGO_HOME's config.toml", path: 'cargo-home/config.toml', exists: false, removable: true },
  { name: 'rust-toolchain.toml', path: 'workspace/rust-toolchain.toml', exists: false, removable: true },
  { name: 'rust-toolchain', path: 'workspace/rust-toolchain', exists: false, removable: true },
  { name: 'devenv.lock', path: 'workspace/devenv.lock', exists: false, removable: true },
  { name: 'tooling/direnv/devenv.lock', path: 'workspace/tooling/direnv/devenv.lock', exists: false, removable: true },
];

for (const file of KEYED_FILES) {
  it(`resolves again exactly once when ${file.name} changes, and not again for the same bytes`, async () => {
    await withCannedWorkspace(async ({ root, cargo, answer, members }) => {
      const resolvesAs = async (name: string): Promise<string[]> => {
        await answer(name);
        return members();
      };
      expect(await resolvesAs('cold')).toEqual(['cold']);
      expect(await spawned(cargo)).toBe(1);

      const original = file.exists ? await readFile(join(root, file.path), 'utf8') : '';
      await put(root, file.path, `${original}# one\n`);
      expect(await resolvesAs('one')).toEqual(['one']);
      expect(await spawned(cargo)).toBe(2);

      // The bytes are the same, so Cargo, which would answer differently, is not asked.
      await touch(root, file.path);
      expect(await resolvesAs('same bytes')).toEqual(['one']);
      expect(await spawned(cargo)).toBe(2);

      await put(root, file.path, `${original}# two\n`);
      expect(await resolvesAs('two')).toEqual(['two']);
      expect(await spawned(cargo)).toBe(3);

      if (file.removable) {
        await rm(join(root, file.path));
        expect(await resolvesAs('removed')).toEqual(['removed']);
        expect(await spawned(cargo)).toBe(4);
      }
      expect(await resolvesAs('settled')).toEqual([file.removable ? 'removed' : 'two']);
      expect(await spawned(cargo)).toBe(file.removable ? 4 : 3);
    });
  }, 30_000);
}

it('resolves again exactly once when the cargo on PATH or the rustup selection changes', async () => {
  await withCannedWorkspace(async ({ root, cargo, answer, members, environment }) => {
    const resolvesAs = async (name: string, selection: Record<string, string> = {}): Promise<string[]> => {
      await answer(name);
      return withEnvironment(selection, members);
    };
    expect(await resolvesAs('cold')).toEqual(['cold']);
    expect(await spawned(cargo)).toBe(1);

    expect(await resolvesAs('toolchain a', { RUSTUP_TOOLCHAIN: 'fixture-a' })).toEqual(['toolchain a']);
    expect(await spawned(cargo)).toBe(2);
    expect(await resolvesAs('same selection', { RUSTUP_TOOLCHAIN: 'fixture-a' })).toEqual(['toolchain a']);
    expect(await spawned(cargo)).toBe(2);

    expect(await resolvesAs('toolchain b', { RUSTUP_TOOLCHAIN: 'fixture-b' })).toEqual(['toolchain b']);
    expect(await spawned(cargo)).toBe(3);

    const selected = { RUSTUP_TOOLCHAIN: 'fixture-b', RUSTUP_HOME: join(root, 'rustup-home') };
    expect(await resolvesAs('rustup home', selected)).toEqual(['rustup home']);
    expect(await spawned(cargo)).toBe(4);

    // The same bytes of Cargo, found elsewhere, are not the same Cargo: a toolchain proxy is a path.
    const other = join(root, 'other-bin');
    await mkdir(other, { recursive: true });
    await writeFile(join(other, 'cargo'), await readFile(join(cargo.bin, 'cargo')), { mode: 0o755 });
    const elsewhere = { ...selected, PATH: `${other}${delimiter}${environment.PATH}` };
    expect(await resolvesAs('another cargo', elsewhere)).toEqual(['another cargo']);
    expect(await spawned(cargo)).toBe(5);
    expect(await resolvesAs('same cargo', elsewhere)).toEqual(['another cargo']);
    expect(await spawned(cargo)).toBe(5);
  });
}, 30_000);

it('a discovered manifest outside the workspace, edited while the first resolution holds, is never answered from before the edit', async () => {
  await withCannedWorkspace(async ({ root, cargo, answer, members, workspace, manifest }) => {
    // Nothing has resolved yet, so nothing knows the package outside the workspace is an input:
    // Cargo's own answer is the first to name it, while the child that gave it still holds that answer.
    await answer('before');
    await cargo.beforeAnswer.close();
    const first = readCargoResolve(manifest, workspace);
    await eventually(async () => (await cargo.runs()).read === 1, 'the first cargo child to read the workspace');
    await appendFile(join(root, 'external/Cargo.toml'), '# edited\n');
    await answer('after');
    await cargo.beforeAnswer.open();
    expect([...(await first).members]).toEqual(['after']);
    const runs = await cargo.runs();
    expect(runs.spawned).toBe(2);
    expect(runs.peak).toBe(1);

    // What was kept is what the edited workspace resolved to, under the edited bytes.
    await answer('trap');
    expect(await members()).toEqual(['after']);
    expect(await spawned(cargo)).toBe(2);
  });
}, 20_000);

it('an edit made while Cargo resolves is answered by a resolution that follows the first, never beside it', async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    await answer('before');
    await cargo.beforeAnswer.close();
    const early = Array.from({ length: 4 }, () => readCargoResolve(manifest, workspace));
    await eventually(async () => (await cargo.runs()).read === 1, 'the first cargo child to read the workspace');

    // That child holds the answer for the workspace as it was.
    await appendFile(manifest, '# edited\n');
    await answer('after');
    const late = Array.from({ length: 4 }, () => readCargoResolve(manifest, workspace));
    await cargo.beforeAnswer.open();

    // A call that began after the edit is never answered from before it.
    for (const result of await Promise.all(late)) expect([...result.members]).toEqual(['after']);
    // A call that began before it may see either side of the edit, and nothing else.
    for (const result of await Promise.all(early)) expect(['before', 'after']).toContain([...result.members][0]);
    const runs = await cargo.runs();
    expect(runs.spawned).toBe(2);
    expect(runs.peak).toBe(1);

    expect(await members()).toEqual(['after']);
    expect(await spawned(cargo)).toBe(2);
  });
}, 20_000);

it('an answer Cargo read after an edit is never kept for the workspace as it was before the edit', async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    const original = await readFile(manifest, 'utf8');
    await answer('before');
    await cargo.beforeRead.close();
    const first = readCargoResolve(manifest, workspace);
    await eventually(async () => (await spawned(cargo)) === 1, 'the cargo child to start');

    // The call has taken the workspace as it is; Cargo reads it only after the edit.
    await writeFile(manifest, `${original}# edited\n`);
    await answer('after');
    await cargo.beforeRead.open();
    expect([...(await first).members]).toEqual(['after']);

    // The original bytes bring back the content the first call began with.
    // Whatever is kept for them must not be what Cargo said about the edit.
    await writeFile(manifest, original);
    await answer('before');
    expect(await members()).toEqual(['before']);
  });
}, 20_000);

it('a process that has never resolved an unchanged workspace reads the answer an earlier one left, without Cargo', async () => {
  await withCannedWorkspace(async (fixture) => {
    const { cargo, answer, workspace, manifest } = fixture;
    await answer('cold');
    const first = await freshResolver(fixture, 'first').result;
    expect(first).toMatchObject({ members: ['cold'] });
    expect(await spawned(cargo)).toBe(1);

    // A Cargo asked now would say so.
    await answer('trap');
    expect(await freshResolver(fixture, 'second').result).toEqual(first);
    expect(await spawned(cargo)).toBe(1);
    expect(first).toEqual(summarize(await readCargoResolve(manifest, workspace)));
    const together = await Promise.all(
      Array.from({ length: 4 }, (_, index) => freshResolver(fixture, `together-${index}`).result),
    );
    for (const result of together) expect(result).toEqual(first);
    expect(await spawned(cargo)).toBe(1);

    await appendFile(manifest, '# edited\n');
    await answer('edited');
    const edited = await freshResolver(fixture, 'edited').result;
    expect(edited).toMatchObject({ members: ['edited'] });
    expect(await freshResolver(fixture, 'edited again').result).toEqual(edited);
    expect(await spawned(cargo)).toBe(2);
  });
}, 60_000);

it('fresh processes that ask at once for a workspace nobody has resolved share one cargo child', async () => {
  await withCannedWorkspace(async (fixture) => {
    const { cargo, answer } = fixture;
    await answer('cold');
    // The child that starts first is held until every process is about to ask,
    // and kept for half a second after it has read, so the rest are waiting on it.
    await cargo.holdFor(0.5);
    await cargo.beforeRead.close();
    const resolvers = Array.from({ length: 4 }, (_, index) => freshResolver(fixture, `process-${index}`));
    await eventually(() => resolvers.every((resolver) => resolver.ready()), 'every process to be about to ask');
    await eventually(async () => (await spawned(cargo)) >= 1, 'a cargo child to start');
    await cargo.beforeRead.open();

    const results = await Promise.all(resolvers.map((resolver) => resolver.result));
    for (const result of results) expect(result).toEqual(results[0]);
    expect(results[0]).toMatchObject({ members: ['cold'] });
    expect(await spawned(cargo)).toBe(1);
  });
}, 60_000);

it('a worker that exits on SIGTERM while Cargo resolves leaves no cargo child behind', async () => {
  await withCannedWorkspace(async ({ root, workspace, manifest, cargo, answer, environment }) => {
    await answer('before');
    await cargo.beforeAnswer.close();
    const worker = join(root, 'worker.ts');
    await writeFile(
      worker,
      [
        `import { readCargoResolve } from ${JSON.stringify(sourceModule)};`,
        // What an Nx plugin worker does on SIGTERM, SIGINT, SIGQUIT and its host hanging up.
        "process.once('SIGTERM', () => process.exit(0));",
        `readCargoResolve(${JSON.stringify(manifest)}, ${JSON.stringify(workspace)})`,
        '  .then(() => process.exit(2), () => process.exit(3));',
        '',
      ].join('\n'),
    );
    const proc = Bun.spawn([process.execPath, worker], {
      cwd: workspace,
      env: { ...process.env, ...environment },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    await eventually(async () => (await spawned(cargo)) === 1, 'the cargo child');
    const [pid] = (await cargo.runs()).pids;
    if (pid === undefined) throw new Error('the cargo child has no pid');
    expect(alive(pid)).toBe(true);
    proc.kill('SIGTERM');
    expect(await proc.exited).toBe(0);
    // Reparented on its parent's exit, a killed child lingers until reaped.
    await eventually(() => !alive(pid), `cargo child ${pid} to die`);
  });
}, 20_000);

it("a worker that exits with callers waiting on two resolutions leaves none of Cargo's processes behind, and the next process resolves normally", async () => {
  await withCannedWorkspace(async (fixture) => {
    const { root, workspace, manifest, cargo, answer, environment } = fixture;
    const otherWorkspace = join(root, 'other');
    await put(otherWorkspace, 'Cargo.toml', WORKSPACE_MANIFEST);
    const otherManifest = join(otherWorkspace, 'Cargo.toml');

    await answer('before');
    await cargo.leaveGrandchildren(true);
    await cargo.beforeAnswer.close();
    const worker = join(root, 'shared-worker.ts');
    await writeFile(
      worker,
      [
        `import { readCargoResolve } from ${JSON.stringify(sourceModule)};`,
        "process.once('SIGTERM', () => process.exit(0));",
        'const [firstManifest, firstCwd, secondManifest, secondCwd] = process.argv.slice(2);',
        'const calls = [',
        '  ...Array.from({ length: 8 }, () => readCargoResolve(firstManifest, firstCwd)),',
        '  ...Array.from({ length: 8 }, () => readCargoResolve(secondManifest, secondCwd)),',
        '];',
        'Promise.all(calls).then(() => process.exit(2), () => process.exit(3));',
        '',
      ].join('\n'),
    );
    const proc = Bun.spawn([process.execPath, worker, manifest, workspace, otherManifest, otherWorkspace], {
      cwd: workspace,
      env: { ...process.env, ...environment },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    // Sixteen callers, two workspaces: two children, each holding an answer and a process of its own.
    await eventually(async () => {
      const runs = await cargo.runs();
      return runs.read === 2 && runs.grandchildren.length === 2;
    }, 'both cargo children to hold an answer');
    const runs = await cargo.runs();
    expect(runs.spawned).toBe(2);
    const running = [...runs.pids, ...runs.grandchildren];
    for (const pid of running) expect(alive(pid)).toBe(true);

    proc.kill('SIGTERM');
    expect(await proc.exited).toBe(0);
    await eventually(() => running.every((pid) => !alive(pid)), 'cargo and what it left behind to die');

    // Nothing the dead worker held, a lock or a half-written answer, is in the next process's way.
    await cargo.leaveGrandchildren(false);
    await cargo.beforeAnswer.open();
    await answer('after');
    const next = await freshResolver(fixture, 'next').result;
    expect(next).toMatchObject({ members: ['after'] });
    expect(await spawned(cargo)).toBe(3);
    expect(await freshResolver(fixture, 'again').result).toEqual(next);
    expect(await spawned(cargo)).toBe(3);
  });
}, 60_000);

// A standalone `smoo-nx-cargo-hash` has no handler of its own, so a signal would end it on the spot,
// before any exit hook could take the Cargo it started along.
for (const signal of ['SIGTERM', 'SIGINT', 'SIGHUP'] as const) {
  it(`a standalone smoo-nx-cargo-hash ended by ${signal} takes its cargo child and what the child left behind with it`, async () => {
    await withCannedWorkspace(async ({ workspace, cargo, answer, environment }) => {
      await answer('before');
      await cargo.leaveGrandchildren(true);
      await cargo.beforeAnswer.close();
      const proc = Bun.spawn([process.execPath, cliModule, 'Cargo.toml'], {
        cwd: workspace,
        env: { ...process.env, ...environment },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      await eventually(async () => {
        const runs = await cargo.runs();
        return runs.read === 1 && runs.grandchildren.length === 1;
      }, 'the cargo child to hold an answer');
      const runs = await cargo.runs();
      const running = [...runs.pids, ...runs.grandchildren];
      for (const pid of running) expect(alive(pid)).toBe(true);

      proc.kill(signal);
      await proc.exited;
      await eventually(() => running.every((pid) => !alive(pid)), 'cargo and what it left behind to die');
    });
  }, 30_000);
}

/* ---- The same, with the real Cargo, through the project graph and the hash command ---- */

/** `leaf` as the fixture writes it, and the dependency that draws `unrelated` into every closure reaching `leaf`. */
const LEAF_MANIFEST = '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n';
const LEAF_LINK = '[dependencies]\nunrelated={path="../unrelated"}\n';

/** The fileset the plugin names for the closure members `crates` (a name or a `{a,b}` set) under `crates/`. */
const crateFiles = (crates: string) =>
  `{workspaceRoot}/crates/${crates}/**/{*.rs,Cargo.toml,.cargo/config,.cargo/config.toml}`;

/** Bring Cargo.lock in line with the manifests. */
function relock(workspace: string): void {
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
    stdio: 'pipe',
  });
}

/**
 * An Nx workspace that is also the Cargo workspace. `app` reaches `leaf` through `bridge`, and
 * `external`, outside the Nx workspace, through `bridge` too. `unrelated` is a member nothing
 * depends on. Returns the workspace.
 */
async function graphFixture(root: string): Promise<string> {
  const workspace = join(root, 'workspace');
  const files: Record<string, string> = {
    'workspace/.gitignore': 'node_modules/\ntarget/\ndist/\n.nx/\n',
    'workspace/package.json':
      '{"name":"cargo-resolve-cache-fixture","private":true,"workspaces":["packages/*","crates/*"]}\n',
    'workspace/nx.json': JSON.stringify({
      plugins: ['@smoothbricks/nx-plugin'],
      namedInputs: { externalRustCrates: [] },
    }),
    'workspace/Cargo.toml':
      '[workspace]\nmembers=["packages/app","crates/bridge","crates/leaf","crates/unrelated"]\nresolver="2"\n',
    'workspace/packages/app/package.json': '{"name":"app","private":true}\n',
    'workspace/packages/app/Cargo.toml':
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../../crates/bridge"}\n',
    'workspace/packages/app/src/main.rs': 'fn main() { println!("{}", bridge::answer()); }\n',
    'workspace/crates/bridge/Cargo.toml':
      '[package]\nname="bridge"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nleaf={path="../leaf"}\nexternal={path="../../../external"}\n',
    'workspace/crates/bridge/src/lib.rs': 'pub fn answer() -> u8 { leaf::answer() + external::answer() }\n',
    'workspace/crates/leaf/package.json': '{"name":"leaf","private":true}\n',
    'workspace/crates/leaf/Cargo.toml': LEAF_MANIFEST,
    'workspace/crates/leaf/src/lib.rs': 'pub fn answer() -> u8 { 1 }\n',
    'workspace/crates/unrelated/Cargo.toml': '[package]\nname="unrelated"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/unrelated/src/lib.rs': 'pub fn unrelated() -> u8 { 3 }\n',
    'external/Cargo.toml': '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
    'external/src/lib.rs': 'pub fn answer() -> u8 { 10 }\n',
  };
  for (const [path, text] of Object.entries(files)) await put(root, path, text);
  execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  relock(workspace);
  return workspace;
}

interface GraphWorkspace {
  readonly root: string;
  readonly workspace: string;
  readonly cargo: CountedCargo;
  /** One project-graph computation over the fixture: the plugin's `createNodes`, as Nx runs it for each recomputation. */
  graph(): Promise<CreateNodesResultV2>;
}

/** `body` over a temporary Nx and Cargo workspace, with the real Cargo counted. */
async function withGraphWorkspace(body: (fixture: GraphWorkspace) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-graph-')));
  try {
    const workspace = await graphFixture(root);
    const cargo = await countedCargo(root);
    const [, infer] = createNodesV2;
    try {
      await withEnvironment(cargo.environment, () => {
        resetWorkspaceContext();
        return body({
          root,
          workspace,
          cargo,
          graph: async () =>
            infer(['package.json', 'packages/app/package.json', 'crates/leaf/package.json'], undefined, {
              workspaceRoot: workspace,
              nxJsonConfiguration: { namedInputs: { externalRustCrates: [] } },
            }),
        });
      });
    } finally {
      await cargo.release();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
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

it('a project graph over an unchanged Cargo workspace runs Cargo once, however often it is recomputed', async () => {
  await withGraphWorkspace(async ({ root, workspace, cargo, graph }) => {
    const baseline = byFile(await graph());
    expect(closureOf(baseline, 'packages/app')).toContain(crateFiles('{bridge,leaf}'));
    expect(closureOf(baseline, 'packages/app')).not.toContain(crateFiles('{bridge,leaf,unrelated}'));
    expect(await spawned(cargo)).toBe(1);

    // The daemon recomputes the graph for every batch of file events.
    for (let round = 0; round < 3; round += 1) expect(byFile(await graph())).toEqual(baseline);
    expect(await spawned(cargo)).toBe(1);

    // A source, a new source file, a file that is not Rust, a manifest or lockfile rewritten byte for byte.
    await put(workspace, 'crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
    await put(workspace, 'crates/leaf/src/extra.rs', 'pub const EXTRA: u8 = 1;\n');
    await put(workspace, 'crates/leaf/README.md', '# leaf\n');
    await touch(workspace, 'crates/leaf/Cargo.toml');
    await touch(workspace, 'Cargo.lock');
    expect(byFile(await graph())).toEqual(baseline);
    expect(await spawned(cargo)).toBe(1);

    // What Cargo reads brings it back once for the bytes that changed, not for every recomputation after.
    const inputs: [string, () => Promise<void>][] = [
      ['a member manifest', () => appendFile(join(workspace, 'crates/leaf/Cargo.toml'), '# note\n')],
      ['Cargo.lock', () => appendFile(join(workspace, 'Cargo.lock'), '# note\n')],
      ['Cargo configuration', () => put(workspace, '.cargo/config.toml', '[env]\nFIXTURE = "1"\n')],
      ['a manifest outside the Nx workspace', () => appendFile(join(root, 'external/Cargo.toml'), '# note\n')],
      ['the toolchain pin', () => put(workspace, 'devenv.lock', '{"nodes":{}}\n')],
    ];
    let expected = 1;
    for (const [what, change] of inputs) {
      await change();
      const changed = byFile(await graph());
      // A manifest comment also changes the inferred versionless-manifest task
      // input. Its dependency closure stays the same, and the new graph is stable.
      expect(closureOf(changed, 'packages/app'), what).toEqual(closureOf(baseline, 'packages/app'));
      expect(byFile(await graph()), what).toEqual(changed);
      expected += 1;
      expect(await spawned(cargo), what).toBe(expected);
    }

    // And what changes the resolution is seen: the new graph is not the cached one.
    await put(workspace, 'crates/leaf/Cargo.toml', LEAF_MANIFEST + LEAF_LINK);
    relock(workspace);
    const edited = byFile(await graph());
    expect(closureOf(edited, 'packages/app')).toContain(crateFiles('{bridge,leaf,unrelated}'));
    expect(byFile(await graph())).toEqual(edited);
    expect(await spawned(cargo)).toBe(expected + 1);
  });
}, 120_000);

it('a Cargo workspace Cargo refuses fails the project graph with a CargoMetadataError, and no runtime hash stands in', async () => {
  await withGraphWorkspace(async ({ workspace, cargo, graph }) => {
    expect(closureOf(byFile(await graph()), 'packages/app')).toContain(crateFiles('{bridge,leaf}'));

    // Cargo refuses a Cargo.lock the manifests have outgrown (`--locked`).
    await put(workspace, 'crates/leaf/Cargo.toml', LEAF_MANIFEST + LEAF_LINK);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const refusal = expectGraphRefusal(await rejectionOf(graph()), join(workspace, 'Cargo.toml'));
      for (const [, error] of refusal.errors) {
        expect(error.message).toContain('lock file');
        expect(error.message).toContain('--locked');
      }
    }
    // A refusal is no answer: the second computation asked Cargo again.
    expect(await spawned(cargo)).toBe(3);

    relock(workspace);
    const repaired = byFile(await graph());
    expect(closureOf(repaired, 'packages/app')).toContain(crateFiles('{bridge,leaf,unrelated}'));
    expect(JSON.stringify(repaired)).not.toContain('--include-workspace');
    expect(byFile(await graph())).toEqual(repaired);
    expect(await spawned(cargo)).toBe(4);
  });
}, 120_000);

it('fresh smoo-nx-cargo-hash processes over an unchanged Cargo workspace run Cargo once between them', async () => {
  await withGraphWorkspace(async ({ root, workspace, cargo }) => {
    // As nx.json's runtime input runs it: a process per hash, many at once at the start of a run.
    const hash = async (...flags: string[]): Promise<string> => {
      const child = Bun.spawn([process.execPath, cliModule, ...flags, '--closure', 'packages/app', 'Cargo.toml'], {
        cwd: workspace,
        env: { ...process.env, ...cargo.environment },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
      ]);
      expect(exitCode, stderr).toBe(0);
      expect(stderr).toBe('');
      return stdout;
    };

    const cold = await Promise.all(Array.from({ length: 4 }, () => hash()));
    const [initial] = cold;
    if (initial === undefined) throw new Error('no hash command ran');
    for (const output of cold) expect(output).toBe(initial);
    expect(await spawned(cargo)).toBe(1);
    for (let round = 0; round < 3; round += 1) expect(await hash()).toBe(initial);
    expect(await spawned(cargo)).toBe(1);

    // The digest still follows what it hashes: a source outside the Nx workspace, not one inside it.
    await put(root, 'external/src/lib.rs', 'pub fn answer() -> u8 { 20 }\n');
    const sourceEdited = await hash();
    expect(sourceEdited).not.toBe(initial);
    await put(workspace, 'crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
    expect(await hash()).toBe(sourceEdited);
    expect(await spawned(cargo)).toBe(1);

    // A manifest outside the Nx workspace is one of Cargo's inputs: one more child, for it alone.
    await appendFile(join(root, 'external/Cargo.toml'), '# note\n');
    const manifestEdited = await hash();
    expect(manifestEdited).not.toBe(sourceEdited);
    expect(await hash()).toBe(manifestEdited);
    expect(await spawned(cargo)).toBe(2);
  });
}, 120_000);
