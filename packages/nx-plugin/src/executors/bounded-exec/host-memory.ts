import { darwinMemory } from './host-memory.darwin.js';
import { linuxMemory } from './host-memory.linux.js';

/**
 * The memory facts a run's CPU pool charges by (specs/cowshed/05_gateway.md, "Without mbx, every Nx run budgets
 * itself"; 13_telemetry.md, "Job resource peaks"), read by each platform's own mechanism: `host-memory.linux.ts`,
 * `host-memory.darwin.ts`. Callers read them here and never ask which platform they run on.
 */
export interface HostMemory {
  /** The memory the run's pool charges its leaves' peaks against. */
  readRunMemory(): Promise<RunMemoryReading>;
  /**
   * A leaf's peak in bytes, from the `maxRSS` that Bun's `Subprocess.resourceUsage()` on `bunVersion` reports for
   * it: the exited leaf's `ru_maxrss`.
   */
  peakBytes(maxRss: number, bunVersion: string): number;
  /**
   * What the process tree rooted at `root` holds resident at this instant: the sum over `root` and every descendant the
   * kernel links to it, each process's footprint where the platform keeps one (macOS `ri_phys_footprint`, which counts
   * its compressed pages) and its resident set where it does not (Linux `VmRSS`). A process that exits between the
   * listing and its read adds nothing, and a root that has exited holds nothing.
   */
  treeResidentBytes(root: number): Promise<number>;
  /**
   * What the run's bound holds that the kernel cannot take back, read now: its anonymous and shared memory. `bound` is
   * the `source` of the run's memory (`RunMemory`): the cgroup's `memory.max`, whose `memory.stat` is read, or the
   * host's `MemTotal`, whose `AnonPages` and `Shmem` are. The pool says it on every grant, so what the leaves are not
   * charged for (the baseline, a tmpfs) and what a suite holds beyond its record are measured, not guessed.
   */
  liveUsage(bound: string): LiveUsageReading;
}

/** What a bound holds live, in bytes, and the file it was read from. */
export interface LiveUsage {
  readonly anon: number;
  readonly shmem: number;
  readonly source: string;
}

export type LiveUsageReading =
  | { readonly ok: true; readonly usage: LiveUsage }
  | { readonly ok: false; readonly reason: string };

/** The run's memory in bytes, and where it was read: the file or sysctl that holds it. */
export interface RunMemory {
  readonly bytes: number;
  readonly source: string;
}

/** Why the run's memory could not be read: the file or sysctl, and what reading it gave. */
export interface RunMemoryError {
  readonly operation: 'read-run-memory';
  /**
   * `unreadable`: reading it failed; `malformed`: it held no byte count; `unmounted`: the run's cgroup v2 hierarchy is
   * mounted nowhere this process can see; `unsupported-platform`: the platform has no reader.
   */
  readonly code: 'unreadable' | 'malformed' | 'unmounted' | 'unsupported-platform';
  readonly subject: string;
  readonly cause: string;
}

export type RunMemoryReading =
  | { readonly ok: true; readonly memory: RunMemory }
  | { readonly ok: false; readonly error: RunMemoryError };

/** A run whose memory cannot be read does not start: it is not shown to have room. */
export class RunMemoryUnreadable extends Error {
  constructor(readonly error: RunMemoryError) {
    super(
      `bounded-exec: this run's memory cannot be read (${error.operation} ${error.code} ${error.subject}: ${error.cause}); a run whose memory cannot be read is not shown to have room, so it does not start`,
    );
    this.name = 'RunMemoryUnreadable';
  }
}

const PLATFORMS: Partial<Record<NodeJS.Platform, HostMemory>> = { darwin: darwinMemory, linux: linuxMemory };

/** This platform's mechanisms, or null where it has none: there a leaf's peak goes unmeasured. */
export const HOST_MEMORY: HostMemory | null = PLATFORMS[process.platform] ?? null;

/** The run's memory on this platform, or why it cannot be read. */
export function readRunMemory(): Promise<RunMemoryReading> {
  if (HOST_MEMORY === null) {
    return Promise.resolve({
      ok: false,
      error: {
        operation: 'read-run-memory',
        code: 'unsupported-platform',
        subject: process.platform,
        cause: 'only Linux and macOS have a reader of the run memory',
      },
    });
  }
  return HOST_MEMORY.readRunMemory();
}

/** The run's bound live on this platform, or why it cannot be read: a pool says it, never leaves it out. */
export function readLiveUsage(bound: string): LiveUsageReading {
  return HOST_MEMORY === null
    ? { ok: false, reason: `${process.platform} has no reader of live memory` }
    : HOST_MEMORY.liveUsage(bound);
}
