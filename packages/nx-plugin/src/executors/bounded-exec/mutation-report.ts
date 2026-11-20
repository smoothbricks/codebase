/**
 * A mutation run's report, handed to Nx as this execution's task-result artifact.
 *
 * The tool is named by a closed selector, never a path. Each spawn gets a fresh run-private capture directory inside the
 * executor's own scratch ({@link MUTATION_REPORT_ENV}, in that spawn's environment only, so it keys nothing), and the
 * tool writes its report there at a fixed leaf: Stryker's `jsonReporter.fileName` (`mutation.json`, set by the shared
 * Stryker config from the variable) or cargo-mutants' `--output` (its `mutants.out/outcomes.json`).
 *
 * Once the command's process tree is retired, so nothing of the run writes any more, the leaf is read and written as
 * the artifact `mutationReport` into the directory Nx made for this one execution (`NX_TASK_ARTIFACTS_DIR`, in the
 * executor's own environment only: the command never sees it). The read opens every directory between the capture and
 * the leaf as itself, never a link, and they must be the same directories after the read; the leaf is a regular file
 * with one link, opened without following a link or waiting on a FIFO, and refused by its size, before a byte is read,
 * when it holds more than Nx's artifact bound. Nx carries it with the task's result to its lifecycle and into its cache
 * entry, so a hit returns the producing run's report. A run that wrote none hands off nothing. No declared output is
 * written, promoted or cleared, and the capture goes with the executor's scratch.
 */

import { constants, type Stats } from 'node:fs';
import { type FileHandle, lstat, open, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export type MutationReportTool = 'stryker' | 'cargo-mutants';

/** The spawn's run-private directory the tool writes its report into. */
export const MUTATION_REPORT_ENV = 'BOUNDED_EXEC_MUTATION_REPORT_DIR';
/** The directory Nx makes for one execution's task-result artifacts (the SmoothBricks Nx patch, `task-artifacts.js`). */
export const TASK_ARTIFACTS_ENV = 'NX_TASK_ARTIFACTS_DIR';
/** The artifact a mutation run hands off. */
export const MUTATION_REPORT_ARTIFACT = 'mutationReport';

/** Each tool's report leaf under the capture directory, by path component: the directories the tool makes, then the file. */
const LEAVES: Readonly<Record<MutationReportTool, readonly [...string[], string]>> = {
  stryker: ['mutation.json'],
  'cargo-mutants': ['mutants.out', 'outcomes.json'],
};

export type HandOff =
  | { readonly kind: 'handed'; readonly bytes: number }
  | { readonly kind: 'absent' }
  | { readonly kind: 'refused'; readonly reason: string };

function code(error: unknown): unknown {
  return error instanceof Error && 'code' in error ? error.code : undefined;
}

/** A directory as lstat sees it at its path: itself, never a link to one. */
type Pinned = Pick<Stats, 'dev' | 'ino'>;

/** The directory at `path`; null when there is none; a link or another kind refuses. */
async function pin(path: string): Promise<Pinned | null> {
  let info: Stats;
  try {
    info = await lstat(path);
  } catch (error) {
    if (code(error) === 'ENOENT') return null;
    throw error;
  }
  if (!info.isDirectory()) throw new Error(`${path} is not a directory but a link or another kind of file`);
  return { dev: info.dev, ino: info.ino };
}

/** The directories from `capture` down to the leaf's parent, each pinned; null when one does not exist. */
async function pinChain(directories: readonly string[]): Promise<Pinned[] | null> {
  const pinned: Pinned[] = [];
  for (const directory of directories) {
    const one = await pin(directory);
    if (one === null) return null;
    pinned.push(one);
  }
  return pinned;
}

/**
 * The captured report, or null when the run wrote none. Every directory on its path is pinned before it is opened and
 * must be the same after the read; a link, a non-regular or multiply-linked file, or one over `limit` bytes refuses,
 * the last before a byte is read.
 */
async function captured(capture: string, tool: MutationReportTool, limit: number): Promise<Buffer | null> {
  const components = LEAVES[tool];
  const directories = components
    .slice(0, -1)
    .reduce<string[]>((chain, part) => [...chain, join(chain.at(-1) ?? capture, part)], [capture]);
  const before = await pinChain(directories);
  if (before === null) return null;
  let file: FileHandle;
  try {
    file = await open(join(capture, ...components), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  } catch (error) {
    if (code(error) === 'ENOENT') return null;
    throw error;
  }
  let bytes: Buffer;
  try {
    const info = await file.stat();
    if (!info.isFile() || info.nlink !== 1) throw new Error('the captured report is not a regular file with one link');
    if (info.size > limit)
      throw new Error(`the captured report holds ${info.size} bytes, over Nx's ${limit}-byte artifact bound`);
    bytes = Buffer.alloc(info.size);
    let read = 0;
    while (read < bytes.length) {
      const { bytesRead } = await file.read(bytes, read, bytes.length - read, read);
      if (bytesRead === 0) throw new Error(`the captured report shrank while read (${read} of ${bytes.length} bytes)`);
      read += bytesRead;
    }
    if ((await file.read(Buffer.alloc(1), 0, 1, read)).bytesRead !== 0) {
      throw new Error(`the captured report grew while read (past ${read} bytes)`);
    }
  } finally {
    await file.close();
  }
  const after = await pinChain(directories);
  if (after === null || after.some((one, at) => one.dev !== before[at]?.dev || one.ino !== before[at]?.ino)) {
    throw new Error("a directory on the captured report's path was replaced while it was read");
  }
  return bytes;
}

/** Hand the report `tool` wrote into `capture` to Nx as this execution's `mutationReport` artifact. */
export async function handOffMutationReport(
  tool: MutationReportTool,
  capture: string,
  environment: Readonly<NodeJS.ProcessEnv>,
): Promise<HandOff> {
  const artifacts = environment[TASK_ARTIFACTS_ENV] ?? '';
  if (artifacts === '') {
    return {
      kind: 'refused',
      reason: `the Nx running this task makes no task-artifact directory (${TASK_ARTIFACTS_ENV} is unset), so its report would be no result's`,
    };
  }
  try {
    // Nx made the directory, so it is the Nx whose artifact bound the report must fit.
    const { MAX_ARTIFACT_BYTES } = await import('nx/src/tasks-runner/task-artifacts.js');
    const bytes = await captured(capture, tool, MAX_ARTIFACT_BYTES);
    if (bytes === null) return { kind: 'absent' };
    await writeFile(join(artifacts, MUTATION_REPORT_ARTIFACT), bytes, { flag: 'wx' });
    return { kind: 'handed', bytes: bytes.byteLength };
  } catch (error) {
    return { kind: 'refused', reason: error instanceof Error ? error.message : String(error) };
  }
}
