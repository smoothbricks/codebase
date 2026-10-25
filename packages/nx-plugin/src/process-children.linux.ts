import { readdir, readFile } from 'node:fs/promises';

/**
 * Linux: each parent's own `/proc/<pid>/task/<tid>/children`, which the kernel keeps for every thread that forked. Only
 * the named parents are read. A parent that has exited, or a thread that has, lists no children. The kernel answers
 * from its task list under RCU, never from either process's memory or files, so no process's kernel wait holds the read:
 * it needs no deadline, unlike a `ps` that reads arguments.
 */
export async function childPids(parents: readonly number[]): Promise<number[]> {
  const listed = await Promise.all(
    parents.map(async (parent) => {
      const tasks = await readdir(`/proc/${parent}/task`).catch(gone);
      const children = await Promise.all(
        (tasks ?? []).map((task) => readFile(`/proc/${parent}/task/${task}/children`, 'utf8').catch(gone)),
      );
      return children.flatMap((text) =>
        (text ?? '')
          .split(' ')
          .filter((word) => word.trim() !== '')
          .map(Number),
      );
    }),
  );
  return listed.flat();
}

/** An entry of a process or thread that exited since it was named: no children, not a failure. */
function gone(error: unknown): null {
  if (error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ESRCH')) {
    return null;
  }
  throw error;
}
