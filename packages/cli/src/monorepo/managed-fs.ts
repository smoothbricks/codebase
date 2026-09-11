import { inspectManagedPaths } from '@smoothbricks/nx-plugin/managed-files/paths';
import {
  assertNoManagedConflicts,
  type FileResult,
  type ManagedFile,
  stageManagedFiles,
} from '@smoothbricks/nx-plugin/managed-files/tree';
// Nx's programmatic disk adapter is kept at this boundary, not in policy or renderers.
import type { Tree } from 'nx/src/devkit-exports.js';
import { FsTree, flushChanges } from 'nx/src/generators/tree.js';

export function syncManagedFiles(
  root: string,
  files: readonly ManagedFile[],
  mode: 'update' | 'check' | 'diff',
): FileResult[] {
  const tree = new FsTree(root, false);
  const paths = inspectManagedPaths(
    root,
    files.filter((file) => file.content !== null).map((file) => file.target),
  );
  const results = stageManagedFiles(tree, files, paths);
  return finishManagedFiles(root, tree, results, mode);
}

export function finishManagedFiles(
  root: string,
  tree: Tree,
  results: FileResult[],
  mode: 'update' | 'check' | 'diff',
): FileResult[] {
  if (mode !== 'update')
    return results.map((result) =>
      result.action === 'created' || result.action === 'updated' ? { ...result, action: 'drifted' } : result,
    );
  assertNoManagedConflicts(results);
  flushChanges(root, tree.listChanges());
  return results.map((result) => (result.action === 'ok-symlink' ? { ...result, action: 'skipped-symlink' } : result));
}
