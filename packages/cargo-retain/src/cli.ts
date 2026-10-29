import { describeRetention, describeRetentionError, refusedAnywhere, retainCargoWorkspace } from './cargo-retention.js';
import type { LockProvider } from './locks.js';

// The report goes to stdout as JSON; the one-line summary and every failure go to stderr. `--automatic` keeps the Nx
// hook's schedule (a successful pass within the interval answers not-due): the hook's due pass is this CLI, started
// detached by launch-node.ts.
// Exit 0: planned, reclaimed or busy. 3: a pass was refused in any root (a profile it cannot read, a lock it cannot
// attempt). 1: no pass ran. 2: usage.
const USAGE = 'usage: smoo-nx-cargo-retain [--apply [--automatic]] [cargo-workspace-dir]';

/**
 * The CLI with its locks taken by `locks` and its pass dated `now`, in milliseconds since the epoch. The entries differ
 * only in the provider they pass; each reads the clock once. Returns the exit code.
 */
export async function runCli(args: readonly string[], locks: LockProvider, now: number): Promise<number> {
  const apply = args[0] === '--apply';
  const automatic = apply && args[1] === '--automatic';
  const operands = args.slice((apply ? 1 : 0) + (automatic ? 1 : 0));
  if (operands.length > 1 || operands.some((operand) => operand.startsWith('-'))) {
    process.stderr.write(`${USAGE}\n`);
    return 2;
  }
  const result = await retainCargoWorkspace(operands[0] ?? process.cwd(), { apply, automatic, now }, locks);
  if (!result.ok) {
    process.stderr.write(`smoo-nx-cargo-retain: ${describeRetentionError(result.error)}\n`);
    return 1;
  }
  process.stdout.write(`${JSON.stringify(result.value, null, 2)}\n`);
  process.stderr.write(`smoo-nx-cargo-retain: ${describeRetention(result.value)}\n`);
  return refusedAnywhere(result.value) ? 3 : 0;
}
