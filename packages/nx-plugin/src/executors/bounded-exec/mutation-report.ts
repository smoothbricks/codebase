/**
 * A mutation run's report, handed to Nx as this execution's task-result artifact.
 *
 * The tool is named by a closed selector, never a path (`capture.ts`). The executor makes a fresh run-private capture
 * directory inside its own scratch and opens it at once ({@link openCapture}); the command learns its path
 * ({@link MUTATION_REPORT_ENV}, in that spawn's environment only, so it keys nothing) and the tool writes its report
 * there at a fixed leaf: Stryker's `jsonReporter.fileName` (`mutation.json`, set by the shared Stryker config from the
 * variable) or cargo-mutants' `--output` (its `mutants.out/outcomes.json`).
 *
 * Once the command's process tree is retired, the capture reader (`capture-reader.ts`, on the executor's Bun) reads
 * the leaf beneath the descriptor the executor has held since it made the capture, never by its path, bounded by Nx's
 * artifact bound before a byte is read. The bytes are written into the `mutationReport` file Nx made and holds for this
 * one execution in its artifact directory (`NX_TASK_ARTIFACTS_DIR`, in the executor's own environment only: the
 * command never sees it), which Nx reads back through its own descriptor and carries with the task's result to its
 * lifecycle and into its cache entry. A run that wrote none hands off nothing. No declared output is written, promoted
 * or cleared, and the capture goes with the executor's scratch.
 */

import { spawn } from 'node:child_process';
import { closeSync, constants, openSync } from 'node:fs';
import { type FileHandle, mkdtemp, open } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { MAX_ARTIFACT_BYTES } from 'nx/src/tasks-runner/task-artifacts.js';
import { CAPTURE_FD, CAPTURE_READ_ABSENT, type MutationReportTool } from './capture.js';
import { executorBun } from './leaf-peak.js';

/** The spawn's run-private directory the tool writes its report into. */
export const MUTATION_REPORT_ENV = 'BOUNDED_EXEC_MUTATION_REPORT_DIR';
/** The directory Nx makes for one execution's task-result artifacts (the SmoothBricks Nx patch, `task-artifacts.js`). */
export const TASK_ARTIFACTS_ENV = 'NX_TASK_ARTIFACTS_DIR';
/** The artifact a mutation run hands off. */
export const MUTATION_REPORT_ARTIFACT = 'mutationReport';

/** The reader's module, in the form this one was loaded in: compiled JavaScript under Nx, TypeScript under Bun. */
const READER = fileURLToPath(
  new URL(import.meta.url.endsWith('.ts') ? './capture-reader.ts' : './capture-reader.js', import.meta.url),
);
/** What the reader may say on stderr: one reason line. */
const MAX_REASON_BYTES = 4096;

/** A run's capture directory: its path, which the command is told, and the descriptor the executor reads it through. */
export interface Capture {
  readonly path: string;
  readonly fd: number;
}

/** A fresh capture under `scratch`, held open from the moment it is made; a link or another kind refuses. */
export async function openCapture(scratch: string): Promise<Capture> {
  const path = await mkdtemp(join(scratch, 'smoo-mutation-'));
  // Node opens descriptors close-on-exec, so the command never inherits it.
  const fd = openSync(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  return { path, fd };
}

export function closeCapture(capture: Capture): void {
  closeSync(capture.fd);
}

export type HandOff =
  | { readonly kind: 'handed'; readonly bytes: number }
  | { readonly kind: 'absent' }
  | { readonly kind: 'refused'; readonly reason: string };

/** The report beneath `capture`'s descriptor, read by the capture reader on `bun`. */
async function readCapture(bun: string, tool: MutationReportTool, capture: Capture): Promise<Buffer | HandOff> {
  // The capture is the reader's descriptor CAPTURE_FD: stdin, stdout and stderr, then it.
  const padding = Array.from({ length: CAPTURE_FD - 3 }, () => 'ignore' as const);
  const reader = spawn(bun, ['--config=/dev/null', READER, tool, String(MAX_ARTIFACT_BYTES)], {
    stdio: ['ignore', 'pipe', 'pipe', ...padding, capture.fd],
  });
  // The package's lib (es2022) has no Promise.withResolvers; the exit is awaited from the spawn's own events.
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the reader is this executor's own child and waits on no peer: it opens a fixed leaf beneath a local directory without blocking (O_NONBLOCK), reads at most Nx's artifact bound and exits; its pipes are its alone, so they close when it does
  const ended = new Promise<[number | null, NodeJS.Signals | null]>((resolve, reject) => {
    reader.once('error', reject);
    reader.once('close', (exit, by) => resolve([exit, by]));
  });
  const { stdout, stderr } = reader;
  if (stdout === null || stderr === null) throw new Error('the capture reader was spawned without its output pipes');
  const out: Buffer[] = [];
  let outBytes = 0;
  let overran = false;
  stdout.on('data', (chunk: Buffer) => {
    outBytes += chunk.length;
    if (outBytes > MAX_ARTIFACT_BYTES) {
      overran = true;
      reader.kill('SIGKILL');
    } else out.push(chunk);
  });
  const err: Buffer[] = [];
  let errBytes = 0;
  stderr.on('data', (chunk: Buffer) => {
    if (errBytes < MAX_REASON_BYTES) err.push(chunk.subarray(0, MAX_REASON_BYTES - errBytes));
    errBytes += chunk.length;
  });
  const [code, signal] = await ended;
  if (overran)
    return { kind: 'refused', reason: `the capture reader wrote more than Nx's ${MAX_ARTIFACT_BYTES}-byte bound` };
  if (code === 0) return Buffer.concat(out, outBytes);
  if (code === CAPTURE_READ_ABSENT) return { kind: 'absent' };
  const said = Buffer.concat(err).toString('utf8').trim();
  return {
    kind: 'refused',
    reason: said === '' ? `the capture reader ended ${signal === null ? `with ${code}` : `by ${signal}`}` : said,
  };
}

/** Write `bytes` into the `mutationReport` file Nx made in `artifacts`; one it did not make refuses. */
async function intoArtifact(artifacts: string, bytes: Buffer): Promise<HandOff> {
  let file: FileHandle;
  try {
    file = await open(
      join(artifacts, MUTATION_REPORT_ARTIFACT),
      constants.O_WRONLY | constants.O_NOFOLLOW | constants.O_TRUNC,
    );
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return {
        kind: 'refused',
        reason: `Nx made no ${MUTATION_REPORT_ARTIFACT} artifact for this task: its target must declare \`artifacts: ["${MUTATION_REPORT_ARTIFACT}"]\``,
      };
    }
    throw error;
  }
  try {
    await file.writeFile(bytes);
  } finally {
    await file.close();
  }
  return { kind: 'handed', bytes: bytes.byteLength };
}

/** Hand the report `tool` wrote into `capture` to Nx as this execution's `mutationReport` artifact. */
export async function handOffMutationReport(
  tool: MutationReportTool,
  capture: Capture,
  environment: Readonly<NodeJS.ProcessEnv>,
): Promise<HandOff> {
  const artifacts = environment[TASK_ARTIFACTS_ENV] ?? '';
  if (artifacts === '') {
    return {
      kind: 'refused',
      reason: `the Nx running this task makes no task-artifact directory (${TASK_ARTIFACTS_ENV} is unset), so its report would be no result's`,
    };
  }
  const bun = executorBun();
  if (bun === null) return { kind: 'refused', reason: 'no bun is on PATH to read the capture beneath its descriptor' };
  try {
    const read = await readCapture(bun, tool, capture);
    return Buffer.isBuffer(read) ? await intoArtifact(artifacts, read) : read;
  } catch (error) {
    return { kind: 'refused', reason: error instanceof Error ? error.message : String(error) };
  }
}
