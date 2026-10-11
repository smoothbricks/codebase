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
