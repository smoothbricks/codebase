import type { FileChange, Tree } from 'nx/src/devkit-exports.js';
import {
  extractInlineLocalBlocks,
  ManagedContentConflict,
  reinsertInlineLocalBlocks,
  splitLocalSection,
} from './managed-content.js';

/** Rendered template input. A disabled descriptor never authorizes deletion. */
export interface ManagedFile {
  target: string;
  content: string | null;
  executable?: boolean;
}

/** Only facts Tree cannot expose. File contents always come from Tree. */
export interface ManagedPathInfo {
  symlink?: boolean;
  executable?: boolean;
  error?: string;
}

/** User-facing diagnostics, not a second representation of Tree's file changes. */
export interface FileResult {
  target: string;
  action: 'created' | 'updated' | 'unchanged' | 'skipped' | 'skipped-symlink' | 'drifted' | 'ok-symlink';
  reason?: string;
}

export function isManagedTarget(target: string): boolean {
  return (
    target.length > 0 &&
    !/[\0\\:]/.test(target) &&
    target.split('/').every((part) => part !== '' && part !== '.' && part !== '..')
  );
}

/** Pure ownership rule; retain exact existing bytes when only permissions need repair. */
export function mergeManagedContent(current: string, desired: string): string {
  const { managed, localTail } = splitLocalSection(current);
  const { withoutInline, blocks } = extractInlineLocalBlocks(managed);
  const rendered = reinsertInlineLocalBlocks(desired, blocks);
  if (withoutInline === desired || (localTail !== '' && withoutInline === `${desired}\n`)) return current;
  return localTail === '' ? rendered : `${rendered}\n${localTail}`;
}

/** Pending Nx edits override disk facts; updates without a mode retain the disk mode. */
function stagedExecutable(change: FileChange | undefined, onDisk: boolean | undefined): boolean | undefined {
  const mode = change?.options?.mode;
  if (mode === undefined) return change?.type === 'CREATE' ? false : onDisk;
  return ((typeof mode === 'string' ? Number.parseInt(mode, 8) : mode) & 0o100) !== 0;
}

/**
 * Stage updates in Nx's virtual filesystem. No disk, process, graph, or network
 * access. The caller decides whether to inspect or flush Tree.listChanges().
 * A conflict may leave earlier edits staged; neither caller may flush that Tree.
 */
export function stageManagedFiles(
  tree: Tree,
  files: readonly ManagedFile[],
  paths: ReadonlyMap<string, ManagedPathInfo> = new Map(),
): FileResult[] {
  const seen = new Set<string>();
  const pending = new Map(tree.listChanges().map((change) => [change.path, change]));
  const targets = new Set(files.map((file) => file.target));
  return files.map(({ target, content, executable = false }): FileResult => {
    const conflict = (reason: string): FileResult => ({ target, action: 'drifted', reason });
    if (!isManagedTarget(target)) return conflict('expected a canonical workspace-relative path');
    if (seen.has(target)) return conflict('duplicate managed target');
    seen.add(target);
    const parts = target.split('/');
    if (parts.some((_, i) => i > 0 && targets.has(parts.slice(0, i).join('/')))) {
      return conflict('a managed file is also declared as a parent directory');
    }
    if (content === null) return { target, action: 'skipped' };
    const path = paths.get(target);
    if (path?.error) return conflict(path.error);
    for (let i = 1; i < parts.length; i++) {
      if (tree.isFile(parts.slice(0, i).join('/'))) return conflict('parent path is a file');
    }
    if (tree.exists(target) && !tree.isFile(target)) return conflict('target is not a regular file');
    const current = tree.read(target, 'utf8');
    if (current === null && tree.exists(target)) return conflict('target cannot be read');
    try {
      const rendered = current === null ? content : mergeManagedContent(current, content);
      const currentExecutable = stagedExecutable(pending.get(target), path?.executable);
      const modeMatches = currentExecutable === undefined || currentExecutable === executable;
      if (path?.symlink) {
        return current === rendered && modeMatches
          ? { target, action: 'ok-symlink' }
          : conflict('symlink content or executable mode differs; update its source explicitly');
      }
      const mode = executable ? 0o755 : 0o644;
      if (current !== rendered) tree.write(target, rendered);
      // A write reverting a prior generator's edit to the on-disk content can
      // erase its pending mode too. Stage permissions after every content write,
      // as well as for permission-only repairs, using Nx's dedicated API.
      if (current !== rendered || !modeMatches) tree.changePermissions(target, mode);
      return {
        target,
        action: current === null ? 'created' : current !== rendered || !modeMatches ? 'updated' : 'unchanged',
      };
    } catch (error) {
      if (!(error instanceof ManagedContentConflict)) throw error;
      return conflict(error.message);
    }
  });
}

export function assertNoManagedConflicts(results: readonly FileResult[]): void {
  const conflicts = results.filter((result) => result.reason !== undefined);
  if (conflicts.length > 0) {
    throw new ManagedContentConflict(
      `Managed-file update refused before writing:\n${conflicts.map(({ target, reason }) => `${target}: ${reason}`).join('\n')}`,
    );
  }
}
