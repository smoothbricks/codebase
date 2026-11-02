import type { IOType } from 'node:child_process';
import type { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import typia from 'typia';
import { HOST_MEMORY } from './host-memory.js';
import { resolveOnPath } from './runtime-pin.js';

/**
 * A leaf's memory peak (specs/cowshed/05_gateway.md, "Without mbx, every Nx run budgets itself"; 13_telemetry.md, "Job
 * resource peaks"): the largest sum of what the exited command's process tree held side by side, as its waiter
 * (`peak-waiter.ts`) sampled it, never less than the command's rusage `ru_maxrss`, which the waiter also reads. The
 * leaf runs no cgroup of its own, even under a cowshed supervisor, whose subtree of the command is an observed fold
 * inside the job's cgroup (07_api.md, `JobSubtreeHandle`), so the peak is read from the leaf's own tree on Linux and
 * macOS alike. The executor writes it into the task's record, and the task's next run is charged it in its run's pool.
 */

/** The descriptor the waiter reports on: past the lifecycle channel (3), the subtree admission (4) and mbx's grant (5). */
export const PEAK_FD = 6;

/** The waiter's module, in the form this one was loaded in: compiled JavaScript under Nx, TypeScript under Bun. */
const WAITER = fileURLToPath(
  new URL(import.meta.url.endsWith('.ts') ? './peak-waiter.ts' : './peak-waiter.js', import.meta.url),
);

/**
 * One JSON line the waiter writes: the command's pid once it started it, then, once the command ended, its peak in
 * bytes or why it has none.
 */
type WaiterLine =
  | { readonly started: number & typia.tags.Type<'uint32'> }
  | { readonly peakBytes: number & typia.tags.Type<'uint64'> }
  | { readonly unmeasured: string };
const isWaiterLine = typia.createEquals<WaiterLine>();

/** A leaf's peak in bytes, or why it went unmeasured. */
export type LeafPeak =
  | { readonly kind: 'measured'; readonly bytes: number }
  | { readonly kind: 'unmeasured'; readonly reason: string };

/** The Bun a leaf's waiter runs on, or why its peak goes unmeasured. */
export type PeakWaiter =
  | { readonly kind: 'waiter'; readonly runtime: string }
  | { readonly kind: 'unmeasured'; readonly reason: string };

/**
 * The Bun that runs this executor, else the first `bun` on the executor's own PATH, never one a target's environment
 * names: the waiter is the executor's, not the command's.
 */
export function peakWaiter(): PeakWaiter {
  if (HOST_MEMORY === null) {
    return { kind: 'unmeasured', reason: `${process.platform} has no reader of a leaf's peak` };
  }
  const runtime =
    process.versions.bun === undefined ? resolveOnPath('bun', process.env.PATH, process.cwd()) : process.execPath;
  return runtime === null
    ? { kind: 'unmeasured', reason: 'no bun is on PATH to wait on the command and read its rusage' }
    : { kind: 'waiter', runtime };
}

/**
 * The spawn of `file args` under the waiter on `runtime`. `descriptors` are the command's own past stderr, from fd 3:
 * the waiter passes each one that is not ignored at its own number, and takes `PEAK_FD` for its report. The runtime's
 * config is empty, so no preload a workspace's `bunfig.toml` names runs in the waiter.
 */
export function underWaiter(
  runtime: string,
  file: string,
  args: readonly string[],
  descriptors: readonly IOType[],
): { file: string; args: string[]; stdio: IOType[] } {
  if (descriptors.length > PEAK_FD - 3) {
    throw new RangeError(
      `a leaf's own descriptors end before ${PEAK_FD}, the waiter's report; it names ${descriptors.length + 3}`,
    );
  }
  const passed = descriptors.flatMap((io, at) => (io === 'ignore' ? [] : [at + 3]));
  const padded = Array.from({ length: PEAK_FD - 3 }, (_, at): IOType => descriptors[at] ?? 'ignore');
  return {
    file: runtime,
    args: ['--config=/dev/null', WAITER, String(PEAK_FD), passed.join(','), file, ...args],
    stdio: ['pipe', 'pipe', 'pipe', ...padded, 'pipe'],
  };
}

/** What a leaf's waiter reports on its descriptor. */
export interface WaiterReport {
  /**
   * The command's pid once the waiter started it: the leaf's runner, a member of the group the waiter leads. Null when
   * the waiter ended without naming one.
   */
  readonly started: Promise<number | null>;
  /** Once the waiter ends: the exited command's peak, or why it reported none. */
  readonly peak: Promise<LeafPeak>;
}

/** What the waiter reports on `stream`: the command's pid as it starts it, and the command's peak once it ends. */
export function readWaiterReport(stream: Readable | null | undefined): WaiterReport {
  if (stream === null || stream === undefined) {
    return {
      started: Promise.resolve(null),
      peak: Promise.resolve({ kind: 'unmeasured', reason: 'the waiter was started without its report descriptor' }),
    };
  }
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib es2022 from
  // tsconfig.base.json. The report's close names null at the latest.
  let name!: (pid: number | null) => void;
  const started = new Promise<number | null>((named) => {
    name = named;
  });
  // The waiter's last word: the line after its start, or what was wrong with a line.
  let end: LeafPeak | null = null;
  const line = (text: string): void => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
    }
    if (!isWaiterLine(parsed)) {
      end = { kind: 'unmeasured', reason: `the waiter reported ${JSON.stringify(text)}, which is no peak` };
    } else if ('started' in parsed) {
      name(parsed.started);
    } else {
      end =
        'peakBytes' in parsed
          ? { kind: 'measured', bytes: parsed.peakBytes }
          : { kind: 'unmeasured', reason: parsed.unmeasured };
    }
  };
  let pending = '';
  stream.setEncoding('utf8');
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the report pipe is the waiter's alone (Bun closes every descriptor it does not pass), so it ends when the waiter does, and the executor's bounds end the waiter
  const peak = new Promise<LeafPeak>((settle) => {
    stream.on('data', (chunk: string) => {
      pending += chunk;
      for (let at = pending.indexOf('\n'); at !== -1; at = pending.indexOf('\n')) {
        line(pending.slice(0, at));
        pending = pending.slice(at + 1);
      }
    });
    stream.once('error', (error: Error) => {
      name(null);
      settle({ kind: 'unmeasured', reason: `its report descriptor: ${error.message}` });
    });
    stream.once('close', () => {
      if (pending.trim() !== '') line(pending.trim());
      name(null);
      settle(end ?? { kind: 'unmeasured', reason: 'the waiter ended before it could report the command' });
    });
  });
  return { started, peak };
}
