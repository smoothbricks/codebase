import { writeSync } from 'node:fs';
import { HOST_MEMORY } from './host-memory.js';

/**
 * The waiter a leaf's command runs under (`leaf-peak.ts`; specs/cowshed/13_telemetry.md, "Job resource peaks"). Started
 * on Bun as `bun --config=/dev/null peak-waiter.js <report fd> <fds> <file> [args…]`, it spawns `file args` with its
 * own stdin, stdout and stderr and each descriptor `fds` names (comma-separated) at its own number, writes the
 * command's pid to the report descriptor, waits for the command, writes its peak there, each as one JSON line, and
 * ends as the command did: with its exit code, or with its signal raised on itself. Node, which runs the executor,
 * reaps a child without its rusage; Bun's `Subprocess.resourceUsage()` reports it: `ru_maxrss`, the largest resident
 * set any one process of the tree it waited on reached.
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

const IGNORED = ['SIGINT', 'SIGTERM', 'SIGHUP'] as const;
const bun: unknown = Reflect.get(globalThis, 'Bun');
const [reportFd, list, file, ...args] = process.argv.slice(2);
if (!isBunSpawner(bun) || HOST_MEMORY === null || reportFd === undefined || list === undefined || file === undefined) {
  throw new Error('peak-waiter runs on Bun, on Linux or macOS, as `peak-waiter.js <report fd> <fds> <file> [args…]`');
}
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
await child.exited;
const usage = child.resourceUsage();
report(
  usage === undefined
    ? { unmeasured: 'Bun read no resource usage for the exited command' }
    : { peakBytes: HOST_MEMORY.peakBytes(usage.maxRSS, bun.version) },
);
if (child.signalCode !== null) {
  for (const signal of IGNORED) process.removeListener(signal, ignore);
  process.kill(process.pid, child.signalCode);
}
process.exit(child.exitCode ?? 1);
