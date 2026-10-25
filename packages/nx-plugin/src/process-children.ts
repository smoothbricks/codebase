/**
 * The children of named processes, each platform by its own mechanism ({@link ChildPids}). Callers walk a tree with
 * `childPids` and never ask which platform answers.
 */
import { childPids as darwinChildPids } from './process-children.darwin.js';
import { childPids as linuxChildPids } from './process-children.linux.js';

/**
 * The pids whose parent is one of `parents`, as the kernel links them now: a child of a parent that has exited is
 * listed by none, and an empty list is no failure. Only the named parents' children are answered, whatever else runs.
 */
export type ChildPids = (parents: readonly number[]) => Promise<number[]>;

const BY_PLATFORM: Partial<Record<NodeJS.Platform, ChildPids>> = { darwin: darwinChildPids, linux: linuxChildPids };

export const childPids: ChildPids = (parents) => {
  const list = BY_PLATFORM[process.platform];
  if (list === undefined) {
    throw new Error(`No child-process listing for ${process.platform}`);
  }
  return list(parents);
};
