import native from 'nx/src/native/index.js';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import { globWithWorkspaceContext, globWithWorkspaceContextSync } from 'nx/src/utils/workspace-context.js';
import { workspaceRoot as daemonWorkspaceRoot } from 'nx/src/utils/workspace-root.js';

/**
 * The files of Nx's workspace file index that match `globs`, workspace-relative:
 * .gitignore and .nxignore applied, `node_modules` never entered. These are
 * exactly the files Nx's hasher can read, so a value inference derives from
 * them is the same in every checkout of one tree, whatever ignored build
 * output happens to be on disk.
 *
 * The daemon serves only its own workspace's index. Any other root, such as a
 * test fixture or a foreign checkout, is indexed by this process's context.
 */
export async function globIndexedFiles(workspaceRoot: string, globs: string[]): Promise<string[]> {
  return workspaceRoot === daemonWorkspaceRoot
    ? globWithWorkspaceContext(workspaceRoot, globs)
    : globWithWorkspaceContextSync(workspaceRoot, globs);
}

/**
 * Like {@link globIndexedFiles}, read from the filesystem now: a new native context, the same walk and the same ignore
 * rules, never the table of this process or of the daemon. A table only learns of a write when a watcher event reaches
 * it, so one the watcher missed leaves a deleted file listed and an added one unlisted until that process ends. What
 * runs on the files as they are asks here; what describes a graph asks {@link globIndexedFiles}.
 */
export function freshIndexedFiles(workspaceRoot: string, globs: string[]): string[] {
  return new native.WorkspaceContext(workspaceRoot, workspaceDataDirectoryForWorkspace(workspaceRoot)).glob(globs);
}
