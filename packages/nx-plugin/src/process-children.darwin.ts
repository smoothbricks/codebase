import { listing } from './process-listing.js';

/**
 * macOS: `pgrep -P`, which takes each process's parent from the kernel's process list, as `proc_listchildpids` does,
 * and reads none of their files, arguments or memory. pgrep leaves its own ancestors out unless given `-a`, which would
 * hide this process from a walk that starts above it.
 */
export async function childPids(parents: readonly number[]): Promise<number[]> {
  const stdout = await listing('pgrep', ['-a', '-P', parents.join(',')]);
  return stdout
    .split('\n')
    .filter((line) => line !== '')
    .map(Number);
}
