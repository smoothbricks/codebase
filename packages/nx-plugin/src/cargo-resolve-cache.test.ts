import { expect, it, jest } from 'bun:test';
import { execFileSync } from 'node:child_process';
import {
  appendFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { delimiter, dirname, join } from 'node:path';

import { parseCargoMetadata } from '@smoothbricks/build-inputs/cargo-closure';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import type { CreateNodesResultV2 } from 'nx/src/devkit-exports.js';
import { FileLock } from 'nx/src/native/index.js';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { assertRealKeyMatchesCacheKeyLaws } from './__tests__/cargo-key-law-binding.js';
import {
  type CannedCargo,
  type CountedCargo,
  cannedCargo,
  countedCargo,
  expectCargoMetadataError,
  expectGraphRefusal,
  guardEvent,
  rejectionOf,
  waitForCargoJoins,
  withEnvironment,
} from './__tests__/counted-cargo.js';
import { loadRecordedCargoResolve, writeWorkspaceCargoFixtureFiles } from './__tests__/recorded-cargo-metadata.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from './bounded-test-policy.js';
import { CARGO_CLOSURE_INPUT } from './cargo-closure-input.js';
import {
  acquireCargoResolveLock,
  type CargoResolve,
  CargoResolveLockError,
  cargoFromMetadata,
  readCargoResolve,
  resolutionFiles,
  resolutionKey,
} from './cargo-source-hash.js';
import { createNodesV2 } from './index.js';

useFixtureCargoHome();

/*
 * What Cargo resolution costs, measured by the one number that matters: how many `cargo metadata`
 * children were actually started. A `cargo` ahead of the real one on PATH reports each child's life
 * to the fixture as it happens (`__tests__/counted-cargo.ts`); no assertion here reads a clock, and
 * nothing here waits for one: every wait advances on a child's report, a process's output or exit,
 * or a sharer joining an in-flight resolution. Only the outer hang guards keep time, and they report
 * what was still open instead of advancing anything.
 *
 * Pure key membership (`resolutionFiles`) and differential key-law proofs (`resolutionKey`) run
 * directly without spawning Cargo; process-boundary tests cover shared flights, concurrent edit
 * races, kill/signal teardown, cache publication failures, and real-Cargo graph/CLI wiring.
 *
 * Every fixture is a temporary directory of its own, with its own Nx workspace-data directory (where
 * the cache is kept), deleted with the fixture.
 */

const sourceModule = join(import.meta.dir, 'cargo-source-hash.ts');
const cliModule = join(import.meta.dir, 'bin', 'smoo-nx-cargo-hash.ts');

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

/** Whether `pid` names a process right now: one syscall, never a wait. */
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ESRCH') return false;
    throw error;
  }
}

/** How a subprocess ended, with everything it printed. */
interface Ended {
  readonly exitCode: number | null;
  readonly signalCode: Bun.Subprocess['signalCode'];
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * A subprocess whose stdout and stderr are read from the moment it starts, so a full pipe never
 * stalls it and every failure message can say what it printed.
 */
interface Drained {
  readonly pid: number;
  kill(signal: NodeJS.Signals): void;
  /** Settles when stdout has printed `line` as a whole line; rejects if stdout ends first. */
  printed(line: string): Promise<void>;
  /** Settles once the process has exited and both of its streams have ended. */
  readonly ended: Promise<Ended>;
  /** The command, whether it is still running, and what it has printed so far. */
  describe(): string;
}

async function drain(stream: ReadableStream<Uint8Array>, append: (text: string) => void): Promise<void> {
  const decoder = new TextDecoder();
  const reader = stream.getReader();
  for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) {
    append(decoder.decode(chunk.value, { stream: true }));
  }
  append(decoder.decode());
}

function spawnDrained(command: readonly string[], cwd: string, environment: Readonly<Record<string, string>>): Drained {
  const proc = Bun.spawn([...command], {
    cwd,
    env: { ...process.env, ...environment },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  let stdout = '';
  let stderr = '';
  let stdoutEnded = false;
  const waiting = new Set<{ readonly line: string; resolve(): void; reject(error: Error): void }>();
  const describe = (): string => {
    const state =
      proc.signalCode !== null
        ? `killed by ${proc.signalCode}`
        : proc.exitCode !== null
          ? `exited ${proc.exitCode}`
          : 'running';
    return `${command.join(' ')} (pid ${proc.pid}, ${state})\nstdout: ${JSON.stringify(stdout)}\nstderr: ${JSON.stringify(stderr)}`;
  };
  const notify = (): void => {
    const lines = new Set(stdout.split('\n').slice(0, -1));
    for (const waiter of waiting) {
      if (lines.has(waiter.line)) waiter.resolve();
      else if (stdoutEnded) waiter.reject(new Error(`stdout ended before printing ${waiter.line}: ${describe()}`));
      else continue;
      waiting.delete(waiter);
    }
  };
  const out = drain(proc.stdout, (text) => {
    stdout += text;
    notify();
  }).finally(() => {
    stdoutEnded = true;
    notify();
  });
  const err = drain(proc.stderr, (text) => {
    stderr += text;
  });
  return {
    pid: proc.pid,
    kill: (signal) => proc.kill(signal),
    printed: (line) =>
      new Promise<void>((resolve, reject) => {
        waiting.add({ line, resolve, reject });
        notify();
      }),
    ended: Promise.all([out, err, proc.exited]).then(
      (): Ended => ({
        exitCode: proc.exitCode,
        signalCode: proc.signalCode,
        stdout,
        stderr,
      }),
    ),
    describe,
  };
}

function summarize(resolved: CargoResolve) {
  return {
    root: resolved.root,
    members: [...resolved.members],
    local: resolved.local,
    edges: resolved.edges === null ? null : [...resolved.edges],
  };
}

/* ---- Pure key membership and cache_key.bend differential laws (no Cargo process) ---- */

it('resolutionKey satisfies key_is_pure (cache_key.bend:790) and key_moves_with_input (cache_key.bend:828) differentially against cache-key-model', async () => {
  // Model items listed in `xs` map to indexed `Cargo.toml` manifests under `drawn/in-<slug>/`; items absent from `xs`
  // map to unindexed sibling manifests under `drawn/out-<slug>/`, which are never ancestors of any indexed path.
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-laws-')));
  try {
    const { root: canonical, workspace, manifest } = await writeWorkspaceCargoFixtureFiles(root);
    const cargo = loadRecordedCargoResolve(canonical);
    const baseIndexed = [
      'Cargo.toml',
      'packages/app/Cargo.toml',
      'crates/bridge/Cargo.toml',
      'crates/leaf/Cargo.toml',
      'crates/unrelated/Cargo.toml',
    ];
    const drawnDir = join(workspace, 'drawn');

    await withEnvironment({ CARGO_HOME: join(canonical, 'cargo-home') }, async () => {
      await assertRealKeyMatchesCacheKeyLaws({
        name: 'resolutionKey',
        drawsPerSeed: 100,
        evaluate: async ({ entries }) => {
          await rm(drawnDir, { recursive: true, force: true });
          const indexed = new Set(baseIndexed);
          const files: { readonly file: string; readonly bytes1: string; readonly bytes2: string }[] = [];
          for (const { slug, inKey, bytes1, bytes2 } of entries) {
            const rel = inKey ? `drawn/in-${slug}/Cargo.toml` : `drawn/out-${slug}/Cargo.toml`;
            if (inKey) indexed.add(rel);
            files.push({ file: join(workspace, rel), bytes1, bytes2 });
          }
          await Promise.all(
            files.map(async ({ file, bytes1 }) => {
              await mkdir(dirname(file), { recursive: true });
              await writeFile(file, bytes1);
            }),
          );
          const key1 = resolutionKey(workspace, manifest, cargo, indexed);
          await Promise.all(files.map(({ file, bytes2 }) => writeFile(file, bytes2)));
          const key2 = resolutionKey(workspace, manifest, cargo, indexed);
          return [key1, key2];
        },
      });
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it("resolutionFiles includes every input Cargo's answer keys on and excludes unkeyed files", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-files-')));
  try {
    const workspace = join(root, 'workspace');
    const manifest = join(workspace, 'Cargo.toml');
    const cargoHome = join(root, 'cargo-home');
    const cargo = cargoFromMetadata(
      parseCargoMetadata(
        JSON.stringify({
          packages: [
            {
              id: 'leaf',
              name: 'leaf',
              version: '0.1.0',
              source: null,
              manifest_path: join(workspace, 'crates/leaf/Cargo.toml'),
              targets: [{ src_path: join(workspace, 'crates/leaf/src/lib.rs') }],
              governing_manifest_path: manifest,
            },
            {
              id: 'external-sub',
              name: 'external-sub',
              version: '0.1.0',
              source: null,
              manifest_path: join(root, 'external/sub/Cargo.toml'),
              targets: [{ src_path: join(root, 'external/sub/src/lib.rs') }],
              governing_manifest_path: join(root, 'external/Cargo.toml'),
            },
          ],
          resolve: {
            nodes: [
              { id: 'leaf', deps: [{ pkg: 'external-sub', dep_kinds: [{ kind: null }] }] },
              { id: 'external-sub', deps: [] },
            ],
          },
          workspace_members: ['leaf'],
          workspace_root: workspace,
        }),
      ),
    );
    const indexed = new Set(['Cargo.toml', 'crates/leaf/Cargo.toml', 'crates/new/Cargo.toml']);

    await withEnvironment({ CARGO_HOME: cargoHome }, async () => {
      const files = resolutionFiles(workspace, manifest, cargo, indexed);
      const coldFiles = resolutionFiles(workspace, manifest, null, indexed);

      // Every input kind Cargo's answer depends on must be present in the membership set.
      const included: readonly [string, string, boolean][] = [
        ['workspace manifest', join(workspace, 'Cargo.toml'), true],
        ["member's manifest", join(workspace, 'crates/leaf/Cargo.toml'), true],
        ['newly indexed manifest in workspace', join(workspace, 'crates/new/Cargo.toml'), true],
        ["path dependency's manifest outside workspace", join(root, 'external/sub/Cargo.toml'), false],
        ['governing manifest of external path dependency', join(root, 'external/Cargo.toml'), false],
        ['Cargo.lock at workspace root', join(workspace, 'Cargo.lock'), true],
        ['Cargo.lock at member directory', join(workspace, 'crates/leaf/Cargo.lock'), true],
        ['Cargo.lock at new indexed manifest directory', join(workspace, 'crates/new/Cargo.lock'), true],
        ['Cargo.lock at external package directory', join(root, 'external/sub/Cargo.lock'), false],
        ['Cargo.lock at external governing directory', join(root, 'external/Cargo.lock'), false],
        ['.cargo/config in workspace', join(workspace, '.cargo/config'), true],
        ['.cargo/config.toml in workspace', join(workspace, '.cargo/config.toml'), true],
        ['.cargo/config.toml in member directory', join(workspace, 'crates/leaf/.cargo/config.toml'), true],
        ['.cargo/config.toml in intermediate directory', join(workspace, 'crates/.cargo/config.toml'), true],
        ['.cargo/config above workspace', join(root, '.cargo/config'), true],
        ['.cargo/config.toml above workspace', join(root, '.cargo/config.toml'), true],
        ['.cargo/config.toml in external ancestor', join(root, 'external/.cargo/config.toml'), false],
        ['rust-toolchain in workspace', join(workspace, 'rust-toolchain'), true],
        ['rust-toolchain.toml in workspace', join(workspace, 'rust-toolchain.toml'), true],
        ['rust-toolchain above workspace', join(root, 'rust-toolchain'), true],
        ['rust-toolchain.toml above workspace', join(root, 'rust-toolchain.toml'), true],
        ['Cargo.toml above workspace', join(root, 'Cargo.toml'), true],
        ['Cargo.toml in intermediate ancestor', join(workspace, 'crates/Cargo.toml'), true],
        ['root devenv.lock toolchain pin', join(workspace, 'devenv.lock'), true],
        ['tooling/direnv/devenv.lock toolchain pin', join(workspace, 'tooling/direnv/devenv.lock'), true],
        ["CARGO_HOME's config", join(cargoHome, 'config'), true],
        ["CARGO_HOME's config.toml", join(cargoHome, 'config.toml'), true],
      ];
      for (const [label, path, alsoWhenCold] of included) {
        expect(files.has(path), `expected in resolutionFiles: ${label} (${path})`).toBe(true);
        if (alsoWhenCold) {
          expect(coldFiles.has(path), `expected in cold resolutionFiles: ${label} (${path})`).toBe(true);
        }
      }

      // Files Cargo's resolution never keys on must stay out of the set.
      const excluded: readonly [string, string][] = [
        ['member Rust source', join(workspace, 'crates/leaf/src/lib.rs')],
        ['new Rust source file', join(workspace, 'crates/leaf/src/extra.rs')],
        ['non-Rust workspace file', join(workspace, 'README.md')],
        ['non-Rust file in member crate', join(workspace, 'crates/leaf/notes.json')],
        ['source edit outside workspace', join(root, 'external/sub/src/lib.rs')],
        ['unindexed non-ancestor manifest', join(workspace, 'crates/unindexed/Cargo.toml')],
      ];
      for (const [label, path] of excluded) {
        expect(files.has(path), `expected out of resolutionFiles: ${label} (${path})`).toBe(false);
        expect(coldFiles.has(path), `expected out of cold resolutionFiles: ${label} (${path})`).toBe(false);
      }
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('resolutionKey tracks content bytes, canonical symlink targets, and toolchain/PATH selection while ignoring mtime and unkeyed edits', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-key-')));
  try {
    const { root: canonical, workspace, manifest } = await writeWorkspaceCargoFixtureFiles(root);
    await put(workspace, 'Cargo.lock', '# fixture lock\nversion = 3\n');
    await put(workspace, 'README.md', '# fixture\n');
    const cargo = loadRecordedCargoResolve(canonical);
    const indexed = new Set([
      'Cargo.toml',
      'packages/app/Cargo.toml',
      'crates/bridge/Cargo.toml',
      'crates/leaf/Cargo.toml',
      'crates/unrelated/Cargo.toml',
    ]);
    const cargoHome = join(canonical, 'cargo-home');
    const binA = join(canonical, 'bin-a');
    const binB = join(canonical, 'bin-b');
    await mkdir(binA, { recursive: true });
    await mkdir(binB, { recursive: true });
    const dummyCargo = '#!/bin/sh\nexit 0\n';
    await writeFile(join(binA, 'cargo'), dummyCargo, { mode: 0o755 });
    await writeFile(join(binB, 'cargo'), dummyCargo, { mode: 0o755 });

    const baseEnv = {
      CARGO_HOME: cargoHome,
      PATH: `${binA}${delimiter}${process.env.PATH ?? ''}`,
      RUSTUP_TOOLCHAIN: 'fixture-a',
      RUSTUP_HOME: '',
    };

    await withEnvironment(baseEnv, async () => {
      const key = () => resolutionKey(workspace, manifest, cargo, indexed);
      const baseline = key();

      // Unkeyed edits and same-bytes rewrites (later mtime) never move the key.
      await put(workspace, 'crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
      await put(workspace, 'crates/leaf/src/extra.rs', 'pub const EXTRA: u8 = 1;\n');
      await put(workspace, 'README.md', '# edited\n');
      await put(workspace, 'crates/leaf/notes.json', '{}\n');
      await put(canonical, 'external/src/lib.rs', 'pub fn answer() -> u8 { 11 }\n');
      await touch(workspace, 'crates/leaf/Cargo.toml');
      await touch(workspace, 'Cargo.lock');
      expect(key()).toBe(baseline);

      // Identical size and mtime cannot stand in for changed bytes.
      const leafManifest = join(workspace, 'crates/leaf/Cargo.toml');
      const originalLeaf = await readFile(leafManifest, 'utf8');
      await writeFile(leafManifest, `${originalLeaf}# one\n`);
      const afterOne = key();
      expect(afterOne).not.toBe(baseline);
      const stamped = await stat(leafManifest);
      await writeFile(leafManifest, `${originalLeaf}# two\n`);
      await utimes(leafManifest, stamped.atime, stamped.mtime);
      expect(key()).not.toBe(afterOne);

      // Adding and removing a removable keyed file moves the key and restores it on removal.
      const beforePin = key();
      const pin = join(workspace, 'devenv.lock');
      await writeFile(pin, '{}\n');
      expect(key()).not.toBe(beforePin);
      await rm(pin);
      expect(key()).toBe(beforePin);

      // Retargeting a configuration symlink with identical bytes still keys the new canonical path.
      const config = join(workspace, '.cargo/config.toml');
      await put(canonical, 'config-a.toml', '[net]\noffline=true\n');
      await put(canonical, 'config-b.toml', '[net]\noffline=true\n');
      await mkdir(dirname(config), { recursive: true });
      await symlink(join(canonical, 'config-a.toml'), config);
      const symlinkA = key();
      expect(symlinkA).not.toBe(beforePin);
      await rm(config);
      await symlink(join(canonical, 'config-b.toml'), config);
      const symlinkB = key();
      expect(symlinkB).not.toBe(symlinkA);
      expect(key()).toBe(symlinkB);

      // Toolchain selection, rustup home, default-home equivalence, and cargo PATH identity.
      const toolchainA = key();
      const toolchainB = await withEnvironment({ RUSTUP_TOOLCHAIN: 'fixture-b' }, async () => key());
      expect(toolchainB).not.toBe(toolchainA);
      expect(key()).toBe(toolchainA);

      const customHome = await withEnvironment({ RUSTUP_HOME: join(canonical, 'rustup-home') }, async () => key());
      expect(customHome).not.toBe(toolchainA);

      // Default rustup home is one selection whether RUSTUP_HOME is unset/empty or names `~/.rustup`.
      const defaultUnset = await withEnvironment({ RUSTUP_HOME: '' }, async () => key());
      const defaultNamed = await withEnvironment({ RUSTUP_HOME: join(homedir(), '.rustup') }, async () => key());
      expect(defaultNamed).toBe(defaultUnset);

      // Identical cargo binary bytes at a different PATH location yield a distinct key.
      const elsewhere = await withEnvironment({ PATH: `${binB}${delimiter}${process.env.PATH ?? ''}` }, async () =>
        key(),
      );
      expect(elsewhere).not.toBe(toolchainA);
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

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
        name,
        version: '0.1.0',
        source: null,
        manifest_path: join(workspace, 'crates/leaf/Cargo.toml'),
        targets: [{ src_path: join(workspace, 'crates/leaf/src/lib.rs') }],
      },
      {
        id: 'external',
        name: 'external',
        version: '0.1.0',
        source: null,
        manifest_path: join(root, 'external/Cargo.toml'),
        targets: [{ src_path: join(root, 'external/src/lib.rs') }],
      },
    ],
    resolve: {
      nodes: [
        { id: name, deps: [{ pkg: 'external', dep_kinds: [{ kind: null, target: null }] }] },
        { id: 'external', deps: [] },
      ],
    },
    workspace_members: [name],
    workspace_root: workspace,
  };
}

/** A process that has never resolved anything: it says on stdout that it is about to ask, asks, and prints the answer. */
const RESOLVE_WORKER = [
  `import { readCargoResolve } from ${JSON.stringify(sourceModule)};`,
  'const [manifest, cwd] = process.argv.slice(2);',
  "process.stdout.write('ready\\n');",
  'const resolved = await readCargoResolve(manifest, cwd);',
  'process.stdout.write(',
  '  JSON.stringify({',
  '    root: resolved.root,',
  '    members: [...resolved.members],',
  '    local: resolved.local,',
  '    edges: resolved.edges === null ? null : [...resolved.edges],',
  "  }) + '\\n',",
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

/**
 * `readCargoResolve` in a fresh process: a second Nx worker, or the next `nx` run. `ready` settles when
 * the process has said it is about to ask; `result` is the answer it printed once it exited cleanly.
 * Both are held to the fixture's hang guard, which reports what the process printed.
 */
function freshResolver({ root, workspace, manifest, environment }: CannedWorkspace, name: string) {
  const worker = spawnDrained(
    [process.execPath, join(root, 'resolve-worker.ts'), manifest, workspace],
    workspace,
    environment,
  );
  const result = guardEvent(worker.ended, `fresh resolver ${name} to exit`, () => worker.describe()).then(
    (ended): unknown => {
      expect(ended.exitCode, worker.describe()).toBe(0);
      const [announced, ...rest] = ended.stdout.trimEnd().split('\n');
      expect(announced, worker.describe()).toBe('ready');
      const answer = rest.at(-1);
      if (answer === undefined) throw new Error(`fresh resolver ${name} printed no answer: ${worker.describe()}`);
      const resolved: unknown = JSON.parse(answer);
      return resolved;
    },
  );
  return {
    ready: () =>
      guardEvent(worker.printed('ready'), `fresh resolver ${name} to say it is about to ask`, () => worker.describe()),
    result,
  };
}

it('concurrent resolutions of one Cargo workspace share one cargo child, and an unchanged workspace never runs Cargo again', async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    await answer('before');
    // The child holds its answer, after it read the workspace, until each of the other fifteen calls
    // has joined its flight, so every call below arrives while it is pending.
    await cargo.beforeAnswer.close();
    const joins = waitForCargoJoins(manifest, 15);
    let results: CargoResolve[];
    try {
      const calls = Promise.all(Array.from({ length: 16 }, () => readCargoResolve(manifest, workspace)));
      await joins.joined;
      await cargo.beforeAnswer.open();
      results = await calls;
    } finally {
      joins.dispose();
    }
    expect(await spawned(cargo)).toBe(1);
    expect(new Set(results).size).toBe(1);
    for (const result of results) expect([...result.members]).toEqual(['before']);

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
});

it("a failed resolution rejects every sharer with a CargoMetadataError carrying Cargo's stderr, and is not kept for the next call", async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    await cargo.failWith('fake cargo: resolution failed\n');
    // The child that fails holds its refusal until the second call has joined its flight.
    await cargo.beforeAnswer.close();
    const joins = waitForCargoJoins(manifest, 1);
    let failures: unknown[];
    try {
      const refused = Promise.all(
        [readCargoResolve(manifest, workspace), readCargoResolve(manifest, workspace)].map(rejectionOf),
      );
      await joins.joined;
      await cargo.beforeAnswer.open();
      failures = await refused;
    } finally {
      joins.dispose();
    }
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
});

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
});

it('resolves again exactly once when an input changes, and zero times for unchanged bytes or an unkeyed edit', async () => {
  // Full per-input membership and byte/mtime/env keying live in the pure tests above; this single canned-Cargo run
  // proves the cache shell wires `resolutionKey` to spawn once on a key miss and zero times on a hit.
  await withCannedWorkspace(async ({ workspace, cargo, answer, members }) => {
    await answer('cold');
    expect(await members()).toEqual(['cold']);
    expect(await spawned(cargo)).toBe(1);

    // Unkeyed edit and same-bytes rewrite never re-spawn Cargo.
    await put(workspace, 'crates/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 2 }\n');
    await touch(workspace, 'crates/leaf/Cargo.toml');
    await answer('unkeyed');
    expect(await members()).toEqual(['cold']);
    expect(await spawned(cargo)).toBe(1);

    // One keyed input change resolves again once, and a repeat read with unchanged bytes resolves zero times.
    await appendFile(join(workspace, 'crates/leaf/Cargo.toml'), '# edited\n');
    await answer('edited');
    expect(await members()).toEqual(['edited']);
    expect(await spawned(cargo)).toBe(2);
    await answer('trap');
    expect(await members()).toEqual(['edited']);
    expect(await spawned(cargo)).toBe(2);
  });
});

it('a resolution input read failure is not a missing file or a reusable answer', async () => {
  await withCannedWorkspace(async ({ workspace, manifest, cargo, answer }) => {
    // Use the graph caller's inventory so this tests the resolution reader,
    // independently of how the native workspace census treats a symlink loop.
    const indexed = new Set(['Cargo.toml', 'crates/leaf/Cargo.toml']);
    const members = async () => [...(await readCargoResolve(manifest, workspace, indexed)).members];
    await answer('cold');
    expect(await members()).toEqual(['cold']);
    const config = join(workspace, '.cargo/config.toml');
    await mkdir(dirname(config), { recursive: true });
    await symlink('config.toml', config);
    const failure = expectCargoMetadataError(await rejectionOf(members()), manifest);
    expect(failure.message).toContain('ELOOP');
    expect(failure.message).toContain(config);
    expect(await spawned(cargo)).toBe(1);

    await rm(config);
    await answer('trap');
    expect(await members()).toEqual(['cold']);
    expect(await spawned(cargo)).toBe(1);
  });
});

it('a discovered manifest outside the workspace, edited while the first resolution holds, is never answered from before the edit', async () => {
  await withCannedWorkspace(async ({ root, cargo, answer, members, workspace, manifest }) => {
    // Nothing has resolved yet, so nothing knows the package outside the workspace is an input:
    // Cargo's own answer is the first to name it, while the child that gave it still holds that answer.
    await answer('before');
    await cargo.beforeAnswer.close();
    const first = readCargoResolve(manifest, workspace);
    await cargo.waitForRuns((runs) => runs.read === 1, 'the first cargo child to read the workspace');
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
});

it('an edit made while Cargo resolves is answered by a resolution that follows the first, never beside it', async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    await answer('before');
    await cargo.beforeAnswer.close();
    const early = Array.from({ length: 4 }, () => readCargoResolve(manifest, workspace));
    await cargo.waitForRuns((runs) => runs.read === 1, 'the first cargo child to read the workspace');

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
});

it('an answer Cargo read after an edit is never kept for the workspace as it was before the edit', async () => {
  await withCannedWorkspace(async ({ cargo, answer, members, workspace, manifest }) => {
    const original = await readFile(manifest, 'utf8');
    await answer('before');
    await cargo.beforeRead.close();
    const first = readCargoResolve(manifest, workspace);
    await cargo.waitForRuns((runs) => runs.spawned === 1, 'the cargo child to start');

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
});

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
});

it('fresh processes that ask at once for a workspace nobody has resolved share one cargo child', async () => {
  await withCannedWorkspace(async (fixture) => {
    const { cargo, answer } = fixture;
    await answer('cold');
    // The child that starts first is held before it reads the workspace until every process has
    // said, on its stdout, that it is about to ask: they ask while that child resolves.
    await cargo.beforeRead.close();
    const resolvers = Array.from({ length: 4 }, (_, index) => freshResolver(fixture, `process-${index}`));
    await Promise.all(resolvers.map((resolver) => resolver.ready()));
    await cargo.waitForRuns((runs) => runs.spawned >= 1, 'a cargo child to start');
    await cargo.beforeRead.open();

    const results = await Promise.all(resolvers.map((resolver) => resolver.result));
    for (const result of results) expect(result).toEqual(results[0]);
    expect(results[0]).toMatchObject({ members: ['cold'] });
    expect(await spawned(cargo)).toBe(1);
  });
});

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
    const proc = spawnDrained(
      [process.execPath, worker, manifest, workspace, otherManifest, otherWorkspace],
      workspace,
      environment,
    );
    // Sixteen callers, two workspaces: two children, each holding an answer and a process of its own.
    const runs = await cargo.waitForRuns(
      (held) => held.read === 2 && held.grandchildren.length === 2,
      'both cargo children to hold an answer',
    );
    expect(runs.spawned).toBe(2);
    const running = [...runs.pids, ...runs.grandchildren];
    for (const pid of running) expect(alive(pid)).toBe(true);

    proc.kill('SIGTERM');
    const ended = await guardEvent(proc.ended, 'the worker to exit on SIGTERM', () => proc.describe());
    expect(ended.exitCode, proc.describe()).toBe(0);
    // Every child and what it left behind is killed: each one's connection ends without a normal end.
    await cargo.waitForDisconnects(running);

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
});

// A standalone `smoo-nx-cargo-hash` has no handler of its own, so a signal would end it on the spot,
// before any exit hook could take the Cargo it started along. The module forwards the signal to
// Cargo's process group and then raises it again, so the command still ends by that signal.
const signals = ['SIGTERM', 'SIGINT', 'SIGHUP'] as const;
it('a standalone smoo-nx-cargo-hash ended by SIGTERM, SIGINT or SIGHUP takes its cargo child and what the child left behind with it', async () => {
  await withCannedWorkspace(async ({ workspace, cargo, answer, environment }) => {
    await answer('before');
    await cargo.leaveGrandchildren(true);
    // One workspace serves all three: each process ends while its own child holds before the answer, so each starts a
    // child of its own and the children before it are already dead.
    await cargo.beforeAnswer.close();
    for (const [index, signal] of signals.entries()) {
      const proc = spawnDrained([process.execPath, cliModule, 'Cargo.toml'], workspace, environment);
      const runs = await cargo.waitForRuns(
        (held) => held.read === index + 1 && held.grandchildren.length === index + 1,
        `the cargo child of the ${signal} process to hold an answer`,
      );
      const running = [...runs.pids.slice(index), ...runs.grandchildren.slice(index)];
      for (const pid of running) expect(alive(pid), signal).toBe(true);

      proc.kill(signal);
      const ended = await guardEvent(proc.ended, `smoo-nx-cargo-hash to end by ${signal}`, () => proc.describe());
      expect(ended.signalCode, proc.describe()).toBe(signal);
      await cargo.waitForDisconnects(running);
    }
  });
});

/* ---- Real Cargo through the project graph, hash command, and cache publication ---- */

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
 * depends on. Calls `createNodesV2` in process, so it needs no copied `node_modules` installation.
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

it('a project graph and fresh smoo-nx-cargo-hash processes over one Cargo workspace share cached Cargo runs, and a refused resolution fails the graph', async () => {
  // Per-input key membership is proven purely above; this real-Cargo smoke proves graph inference and fresh CLI
  // processes share the cache, refuse stale locks without caching refusals, and re-resolve once on manifest changes.
  await withGraphWorkspace(async ({ root, workspace, cargo, graph }) => {
    const baseline = byFile(await graph());
    expect(closureOf(baseline, 'packages/app')).toContain(crateFiles('{bridge,leaf}'));
    expect(closureOf(baseline, 'packages/app')).not.toContain(crateFiles('{bridge,leaf,unrelated}'));
    expect(await spawned(cargo)).toBe(1);

    // Repeated recomputations and unkeyed edits reuse the cached resolution without re-spawning Cargo.
    await put(workspace, 'crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n');
    await touch(workspace, 'crates/leaf/Cargo.toml');
    for (let round = 0; round < 2; round += 1) expect(byFile(await graph())).toEqual(baseline);
    expect(await spawned(cargo)).toBe(1);

    // Cargo refuses a Cargo.lock the manifests have outgrown (`--locked`). A refusal is no answer: each computation
    // asks Cargo again.
    const refused =
      'a Cargo workspace Cargo refuses fails the project graph with a CargoMetadataError, and no runtime hash stands in';
    await put(workspace, 'crates/leaf/Cargo.toml', LEAF_MANIFEST + LEAF_LINK);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const refusal = expectGraphRefusal(await rejectionOf(graph()), join(workspace, 'Cargo.toml'));
      for (const [, error] of refusal.errors) {
        expect(error.message, refused).toContain('lock file');
        expect(error.message, refused).toContain('--locked');
      }
    }
    let expected = 3;
    expect(await spawned(cargo), refused).toBe(expected);

    // Relocked to the manifests, the graph names the new dependency, and is stable under it.
    relock(workspace);
    const edited = byFile(await graph());
    expect(closureOf(edited, 'packages/app')).toContain(crateFiles('{bridge,leaf,unrelated}'));
    expect(JSON.stringify(edited)).not.toContain('--include-workspace');
    expect(byFile(await graph())).toEqual(edited);
    expected += 1;
    expect(await spawned(cargo), refused).toBe(expected);

    // Fresh smoo-nx-cargo-hash processes, as nx.json's runtime input runs them: a process per hash, many at once at the
    // start of a run.
    const fresh = 'fresh smoo-nx-cargo-hash processes over an unchanged Cargo workspace run Cargo once between them';
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
    // A manifest comment the cache has not keyed yet: the cold processes resolve it, once between them.
    await appendFile(join(workspace, 'crates/leaf/Cargo.toml'), '# cli\n');
    const cold = await Promise.all(Array.from({ length: 4 }, () => hash()));
    const [initial] = cold;
    if (initial === undefined) throw new Error('no hash command ran');
    for (const output of cold) expect(output, fresh).toBe(initial);
    expected += 1;
    expect(await spawned(cargo), fresh).toBe(expected);
    expect(await hash(), fresh).toBe(initial);
    expect(await spawned(cargo), fresh).toBe(expected);

    // The digest still follows what it hashes: a source outside the Nx workspace, not one inside it.
    await put(root, 'external/src/lib.rs', 'pub fn answer() -> u8 { 20 }\n');
    const sourceEdited = await hash();
    expect(sourceEdited, fresh).not.toBe(initial);
    await put(workspace, 'crates/leaf/src/lib.rs', 'pub fn answer() -> u8 { 3 }\n');
    expect(await hash(), fresh).toBe(sourceEdited);
    expect(await spawned(cargo), fresh).toBe(expected);

    // A manifest outside the Nx workspace is one of Cargo's inputs: one more child, for it alone.
    await appendFile(join(root, 'external/Cargo.toml'), '# note\n');
    const manifestEdited = await hash();
    expect(manifestEdited, fresh).not.toBe(sourceEdited);
    expect(await hash(), fresh).toBe(manifestEdited);
    expected += 1;
    expect(await spawned(cargo), fresh).toBe(expected);
  });
});

for (const replaced of [false, true]) {
  it(`cache publication records a ${replaced ? 'rebound' : 'missing'} parent with the original filesystem cause`, async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-pub-')));
    try {
      const workspace = join(root, 'workspace');
      const manifest = join(workspace, 'Cargo.toml');
      await put(workspace, 'Cargo.toml', '[package]\nname="pub"\nversion="0.1.0"\nedition="2021"\n[workspace]\n');
      await put(workspace, 'src/lib.rs', 'pub fn pub_test() {}\n');
      relock(workspace);
      const cargo = await countedCargo(root);
      try {
        await withEnvironment(cargo.environment, async () => {
          await cargo.beforeAnswer.close();
          const refused = rejectionOf(readCargoResolve(manifest, workspace));
          await cargo.waitForRuns(
            (runs) => runs.read === 1,
            'real locked/offline Cargo to read before cache publication',
          );
          const namespace = join(workspaceDataDirectoryForWorkspace(workspace), 'cargo-resolve');
          const entries = await readdir(namespace);
          expect(entries).toHaveLength(1);
          const [entry] = entries;
          if (entry === undefined) throw new Error('the real resolver created no owned cache namespace');
          const directory = join(namespace, entry);
          const before = await stat(directory, { bigint: true });
          let atOpen = before;
          if (replaced) {
            await rename(directory, `${directory}.retired`);
            await mkdir(directory);
            atOpen = await stat(directory, { bigint: true });
            expect(atOpen.ino).not.toBe(before.ino);
            // Force publication to report the replacement rather than silently sign under a new namespace.
            await mkdir(join(directory, 'entry.cache'));
          } else {
            await rm(directory, { recursive: true });
          }
          await cargo.beforeAnswer.open();
          const failure = expectCargoMetadataError(await refused, manifest);
          expect(failure.message).toContain(directory);
          expect(failure.message).toContain('entry.cache.');
          expect(failure.message).toContain(
            `"parentBeforeLock":${JSON.stringify({ state: 'present', dev: before.dev.toString(), ino: before.ino.toString() })}`,
          );
          if (replaced) {
            const expected = JSON.stringify({
              state: 'present',
              dev: atOpen.dev.toString(),
              ino: atOpen.ino.toString(),
            });
            expect(failure.message).toContain(`"parentAtTmpOpen":${expected}`);
            expect(failure.message).toContain(`"parentAfterFailure":${expected}`);
          } else {
            expect(failure.message).toContain('"parentAtTmpOpen":{"state":"unavailable","code":"ENOENT"');
            expect(failure.message).toContain('"parentAfterFailure":{"state":"unavailable","code":"ENOENT"');
          }
          const publication = failure.cause;
          expect(publication).toBeInstanceOf(Error);
          if (!(publication instanceof Error)) throw new Error('cache publication lost its structured cause');
          expect(publication.cause).toHaveProperty('code', replaced ? 'EISDIR' : 'ENOENT');
          expect(await spawned(cargo)).toBe(1);
        });
      } finally {
        await cargo.release();
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
}

it('a Cargo child that never answers is killed and named at the resolve bound', async () => {
  await withCannedWorkspace(async ({ cargo, answer, workspace, manifest }) => {
    await answer('before');
    await cargo.beforeAnswer.close();
    try {
      // The resolve bound is a timer in this process, started only once the lock is held and Cargo is spawned. The
      // clock stays stopped until the canned Cargo reports that it read the workspace and is held at the closed
      // answer latch; only then is the bound advanced, so it expires on a child that can never answer.
      jest.useFakeTimers();
      try {
        const refusal = rejectionOf(readCargoResolve(manifest, workspace, undefined, 100));
        await cargo.waitForRuns((runs) => runs.read >= 1, 'the canned Cargo to read the workspace and be held');
        jest.advanceTimersByTime(100);
        expect(String(await refusal)).toContain('did not finish within 100ms');
      } finally {
        jest.useRealTimers();
      }
    } finally {
      await cargo.beforeAnswer.open();
    }
  });
});

it('a held resolve lock times out with its name and elapsed wait and leaves no lock child', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-lock-')));
  try {
    const lockFile = join(root, 'resolve.lock');
    const holder = new FileLock(lockFile);
    holder.lock();
    try {
      // The lock's bound is a timer in this process, and the waiter cannot take the lock while the holder keeps it.
      // The clock stays stopped while the waiter queues; advancing it by the bound is what ends the wait, and the
      // refusal must then name the lock, the wait and the reaped waiter while the holder still holds the lock.
      jest.useFakeTimers();
      let refused: unknown;
      try {
        const attempt = rejectionOf(acquireCargoResolveLock(lockFile, 100));
        jest.advanceTimersByTime(100);
        refused = await attempt;
      } finally {
        jest.useRealTimers();
      }
      if (!(refused instanceof CargoResolveLockError)) throw new Error(`unexpected lock refusal: ${refused}`);
      expect(refused.name).toBe('CargoResolveLockError');
      expect(refused.message).toContain(lockFile);
      expect(refused.message).toContain(`waited ${refused.waitedMs}ms`);
      expect(refused.message).toContain('bound 100ms');
      expect(refused.waitedMs).toBeGreaterThanOrEqual(100);
      if (refused.waiterPid === undefined) throw new Error('the lock refusal named no spawned waiter');
      expect(refused.message).toContain(`reaped lock waiter pid ${refused.waiterPid}`);
      expect(alive(refused.waiterPid)).toBe(false);
      expect(new FileLock(lockFile).check()).toBe(true);
    } finally {
      holder.unlock();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('a successful resolve lock lease holds the kernel lock until its child is released and reaped', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-resolve-lease-')));
  try {
    const lockFile = join(root, 'resolve.lock');
    // The lock is granted by the child's `locked` line and released by the child's exit, so I/O decides this case; the
    // bound is the leg backstop, which an uncontended lock never reaches.
    const lease = await acquireCargoResolveLock(lockFile, BOUNDED_TEST_IDLE_TIMEOUT_MS);
    try {
      expect(alive(lease.pid)).toBe(true);
      expect(new FileLock(lockFile).check()).toBe(true);
    } finally {
      await lease.release();
    }
    expect(alive(lease.pid)).toBe(false);
    expect(new FileLock(lockFile).check()).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
