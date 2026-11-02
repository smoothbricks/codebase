import { writeSync } from 'node:fs';
import { HOST_MEMORY } from './host-memory.js';

/**
 * The waiter a leaf's command runs under (`leaf-peak.ts`; specs/cowshed/13_telemetry.md, "Job resource peaks"). Started
 * on Bun as `bun --config=/dev/null peak-waiter.js <report fd> <fds> <file> [args…]`, it spawns `file args` with its
 * own stdin, stdout and stderr and each descriptor `fds` names (comma-separated) at its own number, writes the
 * command's pid to the report descriptor, waits for the command, writes its peak there, each as one JSON line, and
 * ends as the command did: with its exit code, or with its signal raised on itself.
 *
 * The peak is what the command's tree held side by side. Every {@link SAMPLE_MS} while the command runs, the waiter
 * sums what the command and each descendant the kernel links to it hold (`HostMemory.treeResidentBytes`) and keeps the
 * largest sum. At the exit it takes the larger of that and the command's `ru_maxrss`, the largest resident set any one
 * process of the tree it waited on reached, which a spike between two samples still reaches: Node, which runs the
 * executor, reaps a child without its rusage; Bun's `Subprocess.resourceUsage()` reports it. A tree it could not read
 * is said on the command's stderr, and its peak is then the larger of what was read and `ru_maxrss`.
 *
 * The waiter leads the leaf's process group, so the executor's group signals reach the command directly: it ignores
 * SIGINT, SIGTERM and SIGHUP and outlives the command to report it. A SIGKILL of the group ends it unreported. The
 * command whose pid it names is the leaf's runner, which a hang snapshot samples (specs/cowshed/08_testing.md).
 */

interface BunSubprocess {
  readonly exited: Promise<number>;
  readonly exitCode: number | null;
  readonly signalCode: NodeJS.Signals | null;
  readonly pid: number;
  resourceUsage(): { readonly maxRSS: number } | undefined;
}

/** The two Bun APIs the waiter uses. */
interface BunSpawner {
  readonly version: string;
  spawn(argv: string[], options: { stdio: (number | 'inherit' | 'ignore')[] }): BunSubprocess;
}

function isBunSpawner(runtime: unknown): runtime is BunSpawner {
  return (
    typeof runtime === 'object' &&
    runtime !== null &&
    'spawn' in runtime &&
    typeof runtime.spawn === 'function' &&
    'version' in runtime &&
    typeof runtime.version === 'string'
  );
}

/** How often, in ms, the waiter sums what its command's tree holds while the command runs. */
const SAMPLE_MS = 250;
const IGNORED = ['SIGINT', 'SIGTERM', 'SIGHUP'] as const;
const bun: unknown = Reflect.get(globalThis, 'Bun');
const [reportFd, list, file, ...args] = process.argv.slice(2);
if (!isBunSpawner(bun) || HOST_MEMORY === null || reportFd === undefined || list === undefined || file === undefined) {
  throw new Error('peak-waiter runs on Bun, on Linux or macOS, as `peak-waiter.js <report fd> <fds> <file> [args…]`');
}
const memory = HOST_MEMORY;
const report = (line: { started: number } | { peakBytes: number } | { unmeasured: string }): void => {
  try {
    writeSync(Number(reportFd), `${JSON.stringify(line)}\n`);
  } catch {
    // The executor that would read the report is gone.
  }
};
const passed = list === '' ? [] : list.split(',').map(Number);
const stdio: (number | 'inherit' | 'ignore')[] = ['inherit', 'inherit', 'inherit'];
for (let fd = 3; fd <= Math.max(2, ...passed); fd += 1) stdio.push(passed.includes(fd) ? fd : 'ignore');
const ignore = (): void => {};
for (const signal of IGNORED) process.on(signal, ignore);
let child: BunSubprocess;
try {
  child = bun.spawn([file, ...args], { stdio });
} catch (error) {
  const reason = error instanceof Error ? error.message : String(error);
  report({ unmeasured: `the command did not start: ${reason}` });
  writeSync(2, `bounded-exec: could not start ${file}: ${reason}\n`);
  process.exit(1);
}
report({ started: child.pid });
const root = child.pid;
let treePeak = 0;
let unreadTree: string | null = null;
let reading: Promise<void> | null = null;
const sample = async (): Promise<void> => {
  try {
    treePeak = Math.max(treePeak, await memory.treeResidentBytes(root));
  } catch (error) {
    unreadTree ??= error instanceof Error ? error.message : String(error);
  }
};
// A read still running when the next period starts takes that period too: reads of one tree never overlap.
const sampler = setInterval(() => {
  reading ??= sample().finally(() => {
    reading = null;
  });
}, SAMPLE_MS);
await child.exited;
clearInterval(sampler);
await reading;
if (unreadTree !== null) {
  writeSync(
    2,
    `bounded-exec: the command tree's resident sum could not be read (${unreadTree}); its peak is the larger of what was read and its largest process's ru_maxrss\n`,
  );
}
const usage = child.resourceUsage();
report(
  usage === undefined
    ? { unmeasured: 'Bun read no resource usage for the exited command' }
    : { peakBytes: Math.max(treePeak, memory.peakBytes(usage.maxRSS, bun.version)) },
);
if (child.signalCode !== null) {
  for (const signal of IGNORED) process.removeListener(signal, ignore);
  process.kill(process.pid, child.signalCode);
}
process.exit(child.exitCode ?? 1);
