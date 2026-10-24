import { printCommandOutput, runResult } from '../lib/run.js';
import { escapeRegex, getWorkspacePatterns } from '../lib/workspace.js';

/**
 * Whether the index adds a package manifest directly under a workspace glob (`packages/*`): the pre-commit hook runs
 * the monorepo validator only for such a commit. It lives apart from the validator so the hook's every other commit
 * answers it without loading the validator's Nx graph.
 */
export async function hasNewWorkspacePackage(root: string): Promise<boolean> {
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
    return false;
  }
  const patterns = getWorkspacePatterns(root)
    .filter((pattern) => pattern.endsWith('/*'))
    .map((pattern) => new RegExp(`^${escapeRegex(pattern.slice(0, -2))}/[^/]+/package\\.json$`));
  return manifests.some((manifest) => patterns.some((pattern) => pattern.test(manifest)));
}
