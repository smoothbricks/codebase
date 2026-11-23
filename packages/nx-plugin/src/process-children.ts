/**
 * The children of named processes, as procinfo reads them from the kernel's own parent links (`childPids`,
 * specs/procinfo/01_procinfo.md): one read per parent, never a spawned listing or a census of the process table.
 */
import { childPids as childrenOf } from '@smoothbricks/procinfo';

/**
 * The pids whose parent is one of `parents`, as the kernel links them now: a child of a parent that has exited is
 * listed by none, and an empty list is no failure. Only the named parents' children are answered, whatever else runs.
 */
export async function childPids(parents: readonly number[]): Promise<number[]> {
  return parents.flatMap((parent) => childrenOf(parent));
}
