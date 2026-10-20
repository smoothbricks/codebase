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
 *
 * The consumer re-evaluates `complete` with the same `git ls-files` invocation, so both sides agree by construction.
 */

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, realpathSync, renameSync, writeFileSync } from 'node:fs';
import { join, posix, relative, sep } from 'node:path';

/** The record's file name inside each output directory. */
export const BUILD_INPUTS_FILE = '.build-inputs.json';

/** The producer command for native builds, from the workspace root: node_modules/.bin links it only on a fresh install. */
export const BUILD_INPUTS_BIN = 'node_modules/@smoothbricks/nx-plugin/dist/bin/smoo-nx-build-inputs.js';

export interface BuildInputRecord {
  readonly version: 1;
  readonly files: Readonly<Record<string, string>>;
  readonly complete?: readonly string[];
}

/**
 * Files matching `pathspecs` in the working tree, workspace-relative: tracked and untracked-but-not-ignored, less a
 * tracked file deleted in the working tree (no build reads it; `git ls-files --cached` still lists it). The consumer
 * takes the same list the same way.
 */
export function pathspecFiles(workspaceRoot: string, pathspecs: readonly string[]): string[] {
  const run = spawnSync(
    'git',
    ['-C', workspaceRoot, 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', ...pathspecs],
    { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 },
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
 * a shed's `target/` naming `sb-train-20261020f` paths), and may climb out of a crate (`src/../../../package.json`).
 * So each path is normalized and split into a clone root and the longest tracked path it ends with, and only paths
 * under the clone root most paths share are taken: a registry file whose tail happens to match a tracked path is not.
 * Paths outside the clone (the registry, the toolchain) are not inputs this checkout can change and are left to
 * `Cargo.lock`.
 */
export function depInfoFiles(workspaceRoot: string, depInfo: string): string[] {
  const tracked = new Set(pathspecFiles(workspaceRoot, ['.']));
  const byRoot = new Map<string, Set<string>>();
  // `target: dep dep …` lines; spaces inside paths are escaped as `\ `.
  for (const line of depInfo.split('\n')) {
    const colon = line.indexOf(': ');
    if (colon < 0) continue;
    for (const raw of line.slice(colon + 2).split(/(?<!\\) /)) {
      const path = raw.replaceAll('\\ ', ' ').trim();
      if (path.length === 0) continue;
      const normalized = posix.normalize(path.split(sep).join('/'));
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
 * before the edit, and the consumer sees the edit.
 */
export function buildInputRecord(
  workspaceRoot: string,
  files: readonly string[],
  complete?: readonly string[],
): BuildInputRecord {
  const hashed: Record<string, string> = {};
  for (const file of [...new Set(files)].sort()) {
    hashed[file] = createHash('sha256')
      .update(readFileSync(join(workspaceRoot, file)))
      .digest('hex');
  }
  return { version: 1, files: hashed, ...(complete ? { complete: [...complete] } : {}) };
}

/** Writes `record` into `outputDirectory`, replacing any earlier one whole. */
export function writeBuildInputs(outputDirectory: string, record: BuildInputRecord): void {
  const target = join(outputDirectory, BUILD_INPUTS_FILE);
  mkdirSync(outputDirectory, { recursive: true });
  const staged = `${target}.${process.pid}`;
  writeFileSync(staged, `${JSON.stringify(record, null, 2)}\n`);
  renameSync(staged, target);
}

/** A `:(glob)` pathspec for a workspace-relative glob, so `**` means what tsconfig `include` means by it. */
export function globPathspec(workspaceRelativeGlob: string, exclude = false): string {
  return `:(${exclude ? 'exclude,' : ''}glob)${workspaceRelativeGlob}`;
}

/** `path` relative to `workspaceRoot` in the record's spelling (forward slashes). */
export function workspacePath(workspaceRoot: string, path: string): string {
  return posix.normalize(relative(workspaceRoot, path).split(sep).join('/'));
}

/** The Git work tree `directory` is in, physically resolved, or null outside one: a build there records nothing. */
export function gitWorkspaceRoot(directory: string): string | null {
  const run = spawnSync('git', ['-C', directory, 'rev-parse', '--show-toplevel'], { encoding: 'utf8' });
  return run.status === 0 && run.error === undefined ? realpathSync(run.stdout.trim()) : null;
}
