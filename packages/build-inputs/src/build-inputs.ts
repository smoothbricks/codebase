/**
 * The input record a build writes beside its outputs: every source file it read, by content.
 *
 * A live developer link runs what its checkout last built. Whether that build is current is a question about bytes,
 * not times: a branch switch or a stash rewrites a file with its own bytes and leaves the outputs current, while a
 * pull changes bytes behind the outputs. So each producer writes `.build-inputs.json` into its output directory as
 * part of its Nx outputs. Nx restores it with those outputs on a cache hit, so it always describes the bytes that
 * built the outputs beside it, and a cached build is enough to make it current again.
 *
 * The record is data, verified by the consumer's link check (managed `tooling/direnv/developer-links.ts`):
 *
 * - `files`: workspace-relative path to the SHA-256 of the bytes the build read;
 * - `complete`: Git pathspecs whose every match is an input, so a source added since the build is noticed. Absent when
 *   the producer only knows the files it read (a Cargo dep-info file lists what rustc opened, and a new module reaches
 *   it only through an edited file that is already listed).
 * - `producer`: the Nx task that wrote the record (`buildInputProducer`), when one did: the target whose cached run
 *   rewrites the outputs beside it. A record a standalone writer made has none.
 *
 * The consumer re-evaluates `complete` with the same `git ls-files` invocation, so both sides agree by construction.
 *
 * Every Cargo producer takes its record from `cargoBuildInputs`: `smoo-nx-build-inputs` after a macOS `napi build` and
 * the cargo-zigbuild executor for a Linux artifact, so the consumer's one check judges both alike.
 */

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, realpathSync, renameSync, writeFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import { dirname as dirnamePosix, join as joinPosix, normalize as normalizePosix } from 'node:path/posix';

/** The record's file name inside each output directory. */
export const BUILD_INPUTS_FILE = '.build-inputs.json';

/** The Nx task that wrote a record: its project, its target and the configuration the run named, if any. */
export interface BuildInputProducer {
  readonly project: string;
  readonly target: string;
  readonly configuration?: string;
}

export interface BuildInputRecord {
  readonly version: 1;
  readonly files: Readonly<Record<string, string>>;
  readonly complete?: readonly string[];
  readonly producer?: BuildInputProducer;
}

/**
 * Files matching `pathspecs` in the working tree, workspace-relative: tracked and untracked-but-not-ignored, less a
 * tracked file deleted in the working tree (no build reads it; `git ls-files --cached` still lists it). The consumer
 * takes the same list the same way.
 */
export function pathspecFiles(workspaceRoot: string, pathspecs: readonly string[]): string[] {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git file listing whose length is its own read of this repository: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  const run = spawnSync(
    'git',
    ['-C', workspaceRoot, 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', ...pathspecs],
    { encoding: 'utf8' },
  );
  if (run.error !== undefined || run.status !== 0) {
    throw new Error(`git ls-files ${pathspecs.join(' ')} in ${workspaceRoot}: ${run.error?.message ?? run.stderr}`);
  }
  return [...new Set(run.stdout.split('\0').filter(Boolean))]
    .filter((file) => existsSync(join(workspaceRoot, file)))
    .sort();
}

/**
 * The workspace files a Cargo dep-info file names, workspace-relative. Dep-info paths are absolute in whichever
 * checkout compiled the artifact, which for a target directory cloned with its checkout is a sibling clone (measured:
 * a shed's `target/` naming another shed's paths), and may climb out of a crate (`src/../../../package.json`).
 * So each path is normalized and split into a clone root and the longest tracked path it ends with, and only paths
 * under the clone root most paths share are taken: a registry file whose tail happens to match a tracked path is not.
 * Paths outside the clone (the registry, the toolchain) are not inputs this checkout can change and are left to
 * `Cargo.lock`.
 */
export function depInfoFiles(tracked: ReadonlySet<string>, depInfo: string): string[] {
  const byRoot = new Map<string, Set<string>>();
  // `target: dep dep …` lines; spaces inside paths are escaped as `\ `.
  for (const line of depInfo.split('\n')) {
    const colon = line.indexOf(': ');
    if (colon < 0) continue;
    for (const raw of line.slice(colon + 2).split(/(?<!\\) /)) {
      const path = raw.replaceAll('\\ ', ' ').trim();
      if (path.length === 0) continue;
      const normalized = normalizePosix(path.split(sep).join('/'));
      for (let at = normalized.indexOf('/'); at >= 0; at = normalized.indexOf('/', at + 1)) {
        const file = normalized.slice(at + 1);
        if (!tracked.has(file)) continue;
        const root = normalized.slice(0, at);
        const files = byRoot.get(root) ?? new Set<string>();
        files.add(file);
        byRoot.set(root, files);
        break;
      }
    }
  }
  let chosen = new Set<string>();
  for (const files of byRoot.values()) if (files.size > chosen.size) chosen = files;
  return [...chosen].sort();
}

/**
 * The record of `files` (and `complete` pathspecs) as their bytes are now. A producer that knows its inputs before it
 * builds takes this first, so a source edited during the build is recorded with the bytes the build could have read
 * before the edit, and the consumer sees the edit. `producer` is the Nx task that writes it (`buildInputProducer`).
 */
export function buildInputRecord(
  workspaceRoot: string,
  files: readonly string[],
  complete?: readonly string[],
  producer?: BuildInputProducer,
): BuildInputRecord {
  const hashed: Record<string, string> = {};
  for (const file of [...new Set(files)].sort()) {
    hashed[file] = createHash('sha256')
      .update(readFileSync(join(workspaceRoot, file)))
      .digest('hex');
  }
  return {
    version: 1,
    files: hashed,
    ...(complete ? { complete: [...complete] } : {}),
    ...(producer ? { producer } : {}),
  };
}

/** Writes `record` into `outputDirectory`, replacing any earlier one whole. */
export function writeBuildInputs(outputDirectory: string, record: BuildInputRecord): void {
  const target = join(outputDirectory, BUILD_INPUTS_FILE);
  mkdirSync(outputDirectory, { recursive: true });
  const staged = `${target}.${process.pid}`;
  writeFileSync(staged, `${JSON.stringify(record, null, 2)}\n`);
  renameSync(staged, target);
}

/**
 * The Nx task a producer runs as, from the environment Nx gives every task (`NX_TASK_TARGET_PROJECT`,
 * `NX_TASK_TARGET_TARGET` and, for a run that names one, `NX_TASK_TARGET_CONFIGURATION`), or undefined outside one. The
 * writer's shell passes its own `process.env`: nothing here reads it, so the same environment gives the same record.
 */
export function buildInputProducer(env: NodeJS.ProcessEnv): BuildInputProducer | undefined {
  const project = env.NX_TASK_TARGET_PROJECT;
  const target = env.NX_TASK_TARGET_TARGET;
  const configuration = env.NX_TASK_TARGET_CONFIGURATION;
  if (!project || !target) return undefined;
  return configuration ? { project, target, configuration } : { project, target };
}

/** A `:(glob)` pathspec for a workspace-relative glob, so `**` means what tsconfig `include` means by it. */
export function globPathspec(workspaceRelativeGlob: string, exclude = false): string {
  return `:(${exclude ? 'exclude,' : ''}glob)${workspaceRelativeGlob}`;
}

/** `path` relative to `workspaceRoot` in the record's spelling (forward slashes). */
export function workspacePath(workspaceRoot: string, path: string): string {
  return normalizePosix(relative(workspaceRoot, path).split(sep).join('/'));
}

/** The Git work tree `directory` is in, physically resolved, or null outside one: a build there records nothing. */
export function gitWorkspaceRoot(directory: string): string | null {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git work-tree lookup: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
  const run = spawnSync('git', ['-C', directory, 'rev-parse', '--show-toplevel'], { encoding: 'utf8' });
  return run.status === 0 && run.error === undefined ? realpathSync(run.stdout.trim()) : null;
}

/**
 * The record of a Cargo build, from the text of the dep-info file rustc wrote for its artifact: the workspace files it
 * names (`depInfoFiles`), the `Cargo.toml` of each crate they belong to, and the Cargo workspace's own `Cargo.toml`
 * and `Cargo.lock`, which decide every dependency's bytes. The caller resolves the facts: `workspaceRoot` is the Git
 * work tree the record's paths are relative to (`gitWorkspaceRoot`) and `cargoWorkspaceRoot` the `workspace_root`
 * `cargo metadata` names. Null when the dep-info names no file of `workspaceRoot`. There is no `complete`: a new
 * module reaches the dep-info only through an edited file that is already listed. `producer` is the Nx task that
 * writes the record (`buildInputProducer`).
 *
 * Hashed when called, after the build, the only time rustc's list exists: a source edited during the build is recorded
 * with its new bytes. Nx's own cache key is computed before the build and has the same window.
 */
export function cargoBuildInputs(
  workspaceRoot: string,
  depInfo: string,
  cargoWorkspaceRoot: string,
  producer?: BuildInputProducer,
): BuildInputRecord | null {
  const tracked = new Set(pathspecFiles(workspaceRoot, ['.']));
  const sources = depInfoFiles(tracked, depInfo);
  if (sources.length === 0) return null;
  const manifests = new Set<string>();
  for (const source of sources) {
    for (let crate = dirnamePosix(source); crate !== '.'; crate = dirnamePosix(crate)) {
      const manifest = joinPosix(crate, 'Cargo.toml');
      if (tracked.has(manifest)) {
        manifests.add(manifest);
        break;
      }
    }
  }
  const cargoRoot = realpathSync(cargoWorkspaceRoot);
  for (const name of ['Cargo.toml', 'Cargo.lock']) {
    const file = workspacePath(workspaceRoot, join(cargoRoot, name));
    if (tracked.has(file)) manifests.add(file);
  }
  return buildInputRecord(workspaceRoot, [...sources, ...manifests], undefined, producer);
}
