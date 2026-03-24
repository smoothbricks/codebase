/**
 * Lock file maintenance command
 *
 * Regenerates bun.lock without modifying package.json files,
 * refreshing transitive dependency resolutions to their latest
 * compatible versions.
 */

import * as p from '@clack/prompts';
import type { PatchnoteConfig } from '../config.js';
import { commit, createBranch, deleteRemoteBranch, getRepoRoot, pushWithUpstream, stageFiles } from '../git.js';
import { createPR } from '../pr/stacking.js';
import { resolveSemanticPrefix } from '../semantic.js';
import type { UpdateOptions } from '../types.js';
import { refreshLockFile } from '../updaters/bun.js';

/**
 * Generate a timestamped branch name for lock file maintenance
 */
function generateLockMaintenanceBranchName(prefix: string): string {
  const now = new Date();
  const dateStr = now.toISOString().split('T')[0];
  const timeStr = now.toISOString().split('T')[1]?.substring(0, 5).replace(':', '');
  return `${prefix}-${dateStr}-${timeStr}`;
}

/**
 * Lock file maintenance command
 *
 * Refreshes bun.lock by running `bun install --force` and creates
 * a standalone PR targeting the base branch directly (no stacking).
 */
export async function lockFileMaintenance(config: PatchnoteConfig, options: UpdateOptions): Promise<void> {
  p.intro('Lock file maintenance');

  const repoRoot = config.repoRoot || (await getRepoRoot());

  // Refresh lock file
  const refreshSpinner = p.spinner();
  refreshSpinner.start('Refreshing lock file');
  const result = await refreshLockFile(repoRoot, {
    dryRun: options.dryRun,
    logger: config.logger,
  });
  refreshSpinner.stop(result.changed ? 'Lock file has changes' : 'Lock file is up to date');

  // Handle error from refreshLockFile
  if (result.error) {
    p.log.error(`Lock file refresh failed: ${result.error}`);
    p.outro('Lock file maintenance failed');
    return;
  }

  // Handle no changes
  if (!result.changed) {
    if (options.dryRun) {
      p.note('Lock file would be refreshed by running bun install --force', 'Dry Run');
    }
    p.outro('Lock file is already up to date');
    return;
  }

  // Handle skipGit
  if (options.skipGit) {
    p.log.info('Lock file refreshed (git operations skipped)');
    p.outro('Lock file maintenance complete (skipGit)');
    return;
  }

  // Create branch, commit, push, and PR
  const branchPrefix = config.lockFileMaintenance?.branchPrefix || 'chore/lock-file-maintenance';
  const branchName = generateLockMaintenanceBranchName(branchPrefix);
  const baseBranch = config.git?.baseBranch || 'main';
  const remote = config.git?.remote || 'origin';

  // Resolve semantic prefix
  const semanticPrefix = await resolveSemanticPrefix(config, repoRoot, []);
  const commitTitle = semanticPrefix ? `${semanticPrefix}: lock file maintenance` : 'chore: lock file maintenance';
  const commitBody =
    'Refreshed lock file to update transitive dependency resolutions.\n\nRan `bun install --force` to re-resolve all dependencies from the registry.';

  // Create branch
  const branchSpinner = p.spinner();
  branchSpinner.start('Creating maintenance branch');
  await createBranch(repoRoot, branchName);
  branchSpinner.stop(`Created branch: ${branchName}`);

  // Stage only lock files (not all changes — avoids sweeping unrelated local changes)
  const commitSpinner = p.spinner();
  commitSpinner.start('Committing lock file changes');
  await stageFiles(repoRoot, ['bun.lock', 'bun.lockb']);
  await commit(repoRoot, commitTitle, commitBody);
  commitSpinner.stop('Commit created');

  // Push
  const pushSpinner = p.spinner();
  pushSpinner.start('Pushing to remote');
  await pushWithUpstream(repoRoot, remote, branchName);
  pushSpinner.stop(`Pushed to ${remote}/${branchName}`);

  // Create PR (standalone, targeting base branch directly -- no stacking)
  const prSpinner = p.spinner();
  prSpinner.start('Creating pull request');

  try {
    const pr = await createPR(config, repoRoot, {
      title: commitTitle,
      body: commitBody,
      baseBranch,
      headBranch: branchName,
    });

    prSpinner.stop(`Created PR #${pr.number}`);
    p.note(`${pr.url}\nBase: ${baseBranch}`, 'Pull Request');

    // Enable auto-merge if configured (lock file maintenance is always patch-level risk)
    if (config.autoMerge.enabled && config.autoMerge.mode !== 'none') {
      try {
        const { GitHubCLIClient } = await import('../auth/github-client.js');
        const client = new GitHubCLIClient();
        await client.enableAutoMerge(repoRoot, pr.number, config.autoMerge.strategy);
        config.logger?.info(`Auto-merge enabled for PR #${pr.number} (${config.autoMerge.strategy})`);
      } catch (error) {
        config.logger?.warn(
          `Could not enable auto-merge for PR #${pr.number}: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }

    p.outro('Lock file maintenance complete!');
  } catch (error) {
    prSpinner.stop('PR creation failed');
    config.logger?.error(error instanceof Error ? error.message : String(error));

    // Clean up orphan branch on remote
    config.logger?.error('Cleaning up remote branch...');
    try {
      await deleteRemoteBranch(repoRoot, remote, branchName);
      config.logger?.info(`Deleted orphan branch: ${remote}/${branchName}`);
    } catch (cleanupError) {
      config.logger?.warn(
        `Failed to clean up remote branch ${branchName}:`,
        cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
      );
    }

    p.outro('Lock file maintenance failed');
  }
}
