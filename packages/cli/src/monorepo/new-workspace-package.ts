import { join } from 'node:path';
import { printCommandOutput, runResult } from '../lib/run.js';
import { escapeRegex, getWorkspacePatterns, readWorkspacePackageManifest } from '../lib/workspace.js';

/**
 * The Nx projects of the package manifests the index adds directly under a workspace glob (`packages/*`): the
 * pre-commit hook runs the monorepo validator only for such a commit, and builds and pack-validates only these projects
 * (Nx builds what they depend on). It lives apart from the validator so the hook's every other commit answers it
 * without loading the validator's Nx graph.
 */
export async function newWorkspacePackageProjects(root: string): Promise<string[]> {
  const result = await runResult(
    'git',
    ['diff', '--cached', '--name-only', '--diff-filter=A', '--', '*/package.json'],
    root,
  );
  if (result.exitCode !== 0) {
    printCommandOutput(result.stdout, result.stderr);
    throw new Error(
      `git diff --cached --name-only --diff-filter=A -- */package.json failed with exit code ${result.exitCode}`,
    );
  }
  const manifests = result.stdout
    .split('\n')
    .map((path) => path.trim())
    .filter(Boolean);
  if (manifests.length === 0) {
    return [];
  }
  const patterns = getWorkspacePatterns(root)
    .filter((pattern) => pattern.endsWith('/*'))
    .map((pattern) => new RegExp(`^${escapeRegex(pattern.slice(0, -2))}/[^/]+/package\\.json$`));
  return manifests
    .filter((manifest) => patterns.some((pattern) => pattern.test(manifest)))
    .map((manifest) => {
      const pkg = readWorkspacePackageManifest(join(root, manifest));
      if (pkg === null || pkg.projectName === '') {
        throw new Error(`${manifest} is staged as a new workspace package but names no package`);
      }
      return pkg.projectName;
    });
}
