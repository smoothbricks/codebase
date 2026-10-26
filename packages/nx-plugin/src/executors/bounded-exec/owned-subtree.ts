import { spawn } from 'node:child_process';
import type { Readable } from 'node:stream';
import { isRecord } from '../../type-guards.js';

/**
 * What a `bounded-exec` command leaves running, owned the way cowshed's supervisor owns a job's processes
 * (specs/cowshed/11_shell.md, "Process identity and signal ownership"): by the tree the supervisor observes from
 * fork, never by who works in a directory. The command runs as `cowshed subtree-root`, which asks its job's
 * supervisor for a handle on itself while it is alive, writes that answer to an inherited descriptor, and execs the
 * command in the same life. At the end, `cowshed retire-descendants` hands the handle back: the supervisor signals
 * the live members its tree records under that root, and nothing else, so a process that merely works in the
 * command's directory is never touched.
 */

/** Set by a cowshed runtime for a job only when a supervisor serves that job's socket. */
export const SUPERVISOR_SOCKET_ENV = 'COWSHED_SUPERVISOR_SOCKET';

/**
 * The variable naming the cowshed executable that runs `cowshed disk-command` and `cowshed subtree-root`. Unset, it is
 * the installed `cowshed` on PATH. A repository that tests its own cowshed sets it, on the targets that test it, to that
 * checkout's build: an installed cowshed that predates a command its own checkout added would fail the very code under
 * test. The executor passes the one it resolved to the command, so it is part of what the command runs.
 */
export const COWSHED_EXECUTABLE_ENV = 'COWSHED_EXECUTABLE';

/**
 * The descriptor `cowshed subtree-root` writes its admission to. fd 3 is the runner lifecycle's duplex socket
 * (`runner-hook.ts`), which must reach the command, so the admission takes the next one.
 */
export const HANDLE_FD = 4;

/** The supervisor's opaque capability for one subtree; only `retire-descendants` reads it. */
export type SubtreeHandle = Readonly<Record<string, unknown>>;

/** Why the supervisor answered no, by its own typed reason. */
export type SubtreeRefusal = 'foldUnavailable' | 'notMember' | 'foreignHandle' | 'jobEnded' | 'spentHandle';

const REFUSALS: readonly string[] = ['foldUnavailable', 'notMember', 'foreignHandle', 'jobEnded', 'spentHandle'];

/** What `cowshed subtree-root` wrote, or why nothing readable came. */
export type SubtreeAdmission =
  | { kind: 'admitted'; handle: SubtreeHandle }
  | { kind: 'refused'; reason: SubtreeRefusal }
  | { kind: 'unread'; detail: string };

/** A process the retirement signalled: its pid, and its image when the supervisor read one. */
export interface SignalledProcess {
  readonly pid: number;
  readonly image: string | null;
  readonly signal: number;
}

/** Where the supervisor's observation of the subtree may have missed something. */
export interface CoverageGap {
  readonly kind: string;
  readonly pid: number | null;
}

export type SubtreeRetirement =
  | {
      kind: 'retired';
      signalled: readonly SignalledProcess[];
      /** True when the supervisor could not list every process it signalled. */
      signalledIncomplete: boolean;
      gaps: readonly CoverageGap[];
    }
  | { kind: 'refused'; reason: SubtreeRefusal }
  | { kind: 'failed'; detail: string };

/** The client half: how a command becomes an owned subtree root, and how its subtree ends. */
export interface SubtreeOwner {
  /** The argv that runs the shell `command` as an owned subtree root, its admission on {@link HANDLE_FD}. */
  rootArgv(command: string): { file: string; args: string[] };
  /** Retire every live member recorded under `handle`'s root: TERM, then KILL after `graceMs`. */
  retire(handle: SubtreeHandle, graceMs: number): Promise<SubtreeRetirement>;
}

function isRefusal(value: unknown): value is SubtreeRefusal {
  return typeof value === 'string' && REFUSALS.includes(value);
}

/** Parse the admission JSON line `cowshed subtree-root` writes. */
export function parseAdmission(text: string): SubtreeAdmission {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return {
      kind: 'unread',
      detail: `cowshed subtree-root wrote no admission JSON: ${JSON.stringify(text.slice(0, 200))}`,
    };
  }
  if (isRecord(value) && value.kind === 'admitted' && isRecord(value.handle)) {
    return { kind: 'admitted', handle: value.handle };
  }
  if (isRecord(value) && value.kind === 'refused' && isRefusal(value.reason)) {
    return { kind: 'refused', reason: value.reason };
  }
  return {
    kind: 'unread',
    detail: `cowshed subtree-root wrote an admission this client does not know: ${text.trim()}`,
  };
}

/** A handle recorded as JSON, or null when the record holds none: a writer that died mid-write left no handle. */
export function parseHandle(text: string): SubtreeHandle | null {
  try {
    const value: unknown = JSON.parse(text);
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

/** Read the admission from the command's handle descriptor; it closes before the command execs. */
export function readAdmission(stream: Readable | null | undefined): Promise<SubtreeAdmission> {
  if (stream === null || stream === undefined) {
    return Promise.resolve({ kind: 'unread', detail: 'the command was started without its handle descriptor' });
  }
  const chunks: Buffer[] = [];
  let settle!: (admission: SubtreeAdmission) => void;
  const answer = new Promise<SubtreeAdmission>((resolve) => {
    settle = resolve;
  });
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));
  stream.once('error', (error: Error) => settle({ kind: 'unread', detail: `handle descriptor: ${error.message}` }));
  stream.once('end', () => {
    const text = Buffer.concat(chunks).toString('utf8');
    settle(
      text.trim() === ''
        ? { kind: 'unread', detail: 'cowshed subtree-root closed its handle descriptor without an admission' }
        : parseAdmission(text),
    );
  });
  return answer;
}

/** Parse the retirement JSON line `cowshed retire-descendants` prints. */
export function parseRetirement(text: string): SubtreeRetirement {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return {
      kind: 'failed',
      detail: `cowshed retire-descendants printed no JSON: ${JSON.stringify(text.slice(0, 200))}`,
    };
  }
  if (isRecord(value) && value.kind === 'refused' && isRefusal(value.reason)) {
    return { kind: 'refused', reason: value.reason };
  }
  if (isRecord(value) && value.kind === 'retired' && isRecord(value.retirement)) {
    const { containment, outlived, outlivedIncomplete } = value.retirement;
    // `kernel` is a kernel-proven membership (a Linux cgroup), so it carries no gaps; `observed` lists what its fold
    // may have missed (specs/cowshed/07_api.md, `containment`).
    const lost = !isRecord(containment)
      ? null
      : containment.kind === 'kernel'
        ? []
        : containment.kind === 'observed' && Array.isArray(containment.lost)
          ? containment.lost
          : null;
    if (lost !== null && Array.isArray(outlived)) {
      const signalled: SignalledProcess[] = [];
      for (const entry of outlived) {
        if (!isRecord(entry) || typeof entry.pid !== 'number' || typeof entry.signal !== 'number') {
          return {
            kind: 'failed',
            detail: `cowshed retire-descendants named a process this client cannot read: ${JSON.stringify(entry)}`,
          };
        }
        signalled.push({
          pid: entry.pid,
          image: typeof entry.image === 'string' ? entry.image : null,
          signal: entry.signal,
        });
      }
      const gaps: CoverageGap[] = [];
      for (const entry of lost) {
        if (!isRecord(entry) || typeof entry.kind !== 'string') {
          return {
            kind: 'failed',
            detail: `cowshed retire-descendants named a coverage gap this client cannot read: ${JSON.stringify(entry)}`,
          };
        }
        gaps.push({ kind: entry.kind, pid: typeof entry.pid === 'number' ? entry.pid : null });
      }
      return { kind: 'retired', signalled, signalledIncomplete: outlivedIncomplete === true, gaps };
    }
  }
  return {
    kind: 'failed',
    detail: `cowshed retire-descendants printed a retirement this client does not know: ${text.trim()}`,
  };
}

/** The owner a cowshed runtime serves, through the installed `cowshed`'s own native socket client. */
export function cowshedSubtreeOwner(cowshed: string): SubtreeOwner {
  return {
    rootArgv(command) {
      return {
        file: cowshed,
        args: ['subtree-root', '--handle-fd', String(HANDLE_FD), '--', '/bin/sh', '-c', command],
      };
    },
    retire(handle, graceMs) {
      const child = spawn(cowshed, ['retire-descendants', '--grace-ms', String(graceMs)], {
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      const out: Buffer[] = [];
      const err: Buffer[] = [];
      child.stdout.on('data', (chunk: Buffer) => out.push(chunk));
      child.stderr.on('data', (chunk: Buffer) => err.push(chunk));
      child.stdin.end(`${JSON.stringify(handle)}\n`);
      // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the cowshed CLI bounds its own supervisor call (control bound, then the 15 s heartbeat silence; 05_gateway.md, "Deadlines") and exits with the answer or its TIMEOUT/SILENT failure
      return new Promise<SubtreeRetirement>((resolve) => {
        child.once('error', (error) =>
          resolve({ kind: 'failed', detail: `${cowshed} retire-descendants: ${error.message}` }),
        );
        child.once('close', (code, signal) => {
          const stdout = Buffer.concat(out).toString('utf8');
          if (code === 0) {
            resolve(parseRetirement(stdout));
            return;
          }
          const stderr = Buffer.concat(err).toString('utf8').trim();
          resolve({
            kind: 'failed',
            detail: `${cowshed} retire-descendants exited ${code ?? `on ${signal}`}${stderr === '' ? '' : `: ${stderr}`}`,
          });
        });
      });
    },
  };
}
