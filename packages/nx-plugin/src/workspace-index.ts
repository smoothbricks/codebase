import { join } from 'node:path';
import native, { type FileData } from 'nx/src/native/index.js';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';
import { getAllFileDataInContext, globWithWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { workspaceRoot as daemonWorkspaceRoot } from 'nx/src/utils/workspace-root.js';

/**
 * The files of Nx's workspace file index that match `globs`, workspace-relative:
 * .gitignore and .nxignore applied, `node_modules` never entered. These are
 * exactly the files Nx's hasher can read, so a value inference derives from
 * them is the same in every checkout of one tree, whatever ignored build
 * output happens to be on disk.
 *
 * The daemon serves only its own workspace's index. Any other root, such as a
 * test fixture or a foreign checkout, is read from the filesystem now
 * ({@link freshIndexedFiles}), under that root's own data directory: Nx's
 * process context for it would resolve the inherited NX_WORKSPACE_DATA_DIRECTORY,
 * this workspace's, and take its files lock and archive.
 */
export async function globIndexedFiles(workspaceRoot: string, globs: string[]): Promise<string[]> {
  return workspaceRoot === daemonWorkspaceRoot
    ? globWithWorkspaceContext(workspaceRoot, globs)
    : freshIndexedFiles(workspaceRoot, globs);
}

/**
 * Like {@link globIndexedFiles}, read from the filesystem now: a new native context, the same walk and the same ignore
 * rules, never the table of this process or of the daemon. A table only learns of a write when a watcher event reaches
 * it, so one the watcher missed leaves a deleted file listed and an added one unlisted until that process ends. What
 * runs on the files as they are asks here; what describes a graph asks {@link globIndexedFiles}.
 */
export function freshIndexedFiles(workspaceRoot: string, globs: string[]): string[] {
  return new native.WorkspaceContext(workspaceRoot, indexDataDirectory(workspaceRoot)).glob(globs);
}

/**
 * Every file of Nx's workspace file index with Nx's content hash of it, workspace-relative: the hashes Nx's task
 * hasher keys every fileset on, kept current by its watcher, so a caller can tell an unchanged file without reading
 * it. The daemon serves its own workspace; any other root, such as a test fixture, is read from the filesystem now
 * ({@link freshIndexedFileData}), since the daemon's answer would be about another tree.
 */
export async function indexedFileData(workspaceRoot: string): Promise<FileData[]> {
  return workspaceRoot === daemonWorkspaceRoot
    ? getAllFileDataInContext(workspaceRoot)
    : freshIndexedFileData(workspaceRoot);
}

/** Like {@link indexedFileData}, read from the filesystem now, as {@link freshIndexedFiles} lists it. */
export function freshIndexedFileData(workspaceRoot: string): FileData[] {
  return new native.WorkspaceContext(workspaceRoot, indexDataDirectory(workspaceRoot)).allFileData();
}

/**
 * Where a fresh native context of `workspaceRoot` keeps its files lock and archive. Nx's
 * `workspaceDataDirectoryForWorkspace` answers `NX_WORKSPACE_DATA_DIRECTORY` for every root, and that variable names
 * this process's workspace's data (CI points it at a shared directory outside the checkout). A walk of another root
 * there takes this workspace's files lock, and when another process holds it, loads the archive that process writes,
 * this workspace's files, as the other root's (CI's Lint, run 42738719423: the compiled-in fixtures found no crate).
 * Any other root walks under Nx's default for it, inside itself.
 */
function indexDataDirectory(workspaceRoot: string): string {
  return workspaceRoot === daemonWorkspaceRoot
    ? workspaceDataDirectoryForWorkspace(workspaceRoot)
    : join(workspaceRoot, '.nx', 'workspace-data');
}
