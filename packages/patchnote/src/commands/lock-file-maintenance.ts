/**
 * Lock file maintenance command
 *
 * Regenerates the lock file without modifying package.json files,
 * refreshing transitive dependency resolutions to their latest
 * compatible versions.
 */

import * as p from '@clack/prompts';
import type { PatchnoteConfig } from '../config.js';
import * as gitModule from '../git.js';
import * as stackingModule from '../pr/stacking.js';
import * as semanticModule from '../semantic.js';
import type { UpdateOptions } from '../types.js';
import * as bunModule from '../updaters/bun.js';
import { getPackageManagerCommands } from '../updaters/package-manager.js';
import { detectProjectSetup } from '../utils/project-detection.js';

/** Injectable dependencies for testing */
export interface LockFileMaintenanceDeps {
  getRepoRoot: typeof gitModule.getRepoRoot;
  createBranch: typeof gitModule.createBranch;
  stageFiles: typeof gitModule.stageFiles;
  commit: typeof gitModule.commit;
  pushWithUpstream: typeof gitModule.pushWithUpstream;
  deleteRemoteBranch: typeof gitModule.deleteRemoteBranch;
  createPR: typeof stackingModule.createPR;
  resolveSemanticPrefix: typeof semanticModule.resolveSemanticPrefix;
  refreshLockFile: typeof bunModule.refreshLockFile;
}

const defaultDeps: LockFileMaintenanceDeps = {
  getRepoRoot: gitModule.getRepoRoot,
  createBranch: gitModule.createBranch,
  stageFiles: gitModule.stageFiles,
  commit: gitModule.commit,
  pushWithUpstream: gitModule.pushWithUpstream,
  deleteRemoteBranch: gitModule.deleteRemoteBranch,
  createPR: stackingModule.createPR,
  resolveSemanticPrefix: semanticModule.resolveSemanticPrefix,
  refreshLockFile: bunModule.refreshLockFile,
};

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
 * Refreshes the lock file by running a force install and creates
 * a standalone PR targeting the base branch directly (no stacking).
 */
export async function lockFileMaintenance(
  config: PatchnoteConfig,
  options: UpdateOptions,
  deps: LockFileMaintenanceDeps = defaultDeps,
): Promise<void> {
  p.intro('Lock file maintenance');

  const repoRoot = config.repoRoot || (await deps.getRepoRoot());

  // Detect package manager and get PM-specific commands
  const setup = await detectProjectSetup(repoRoot);
  const pmCommands = getPackageManagerCommands(setup.packageManager);

  // Refresh lock file
  const refreshSpinner = p.spinner();
  refreshSpinner.start('Refreshing lock file');
  const result = await deps.refreshLockFile(repoRoot, {
    dryRun: options.dryRun,
    logger: config.logger,
    packageManager: setup.packageManager,
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
      p.note(
        `Lock file would be refreshed by running ${pmCommands.cmd} ${pmCommands.forceRefreshArgs.join(' ')}`,
        'Dry Run',
      );
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
  const semanticPrefix = await deps.resolveSemanticPrefix(config, repoRoot, []);
  const commitTitle = semanticPrefix ? `${semanticPrefix}: lock file maintenance` : 'chore: lock file maintenance';
  const commitBody = `Refreshed lock file to update transitive dependency resolutions.\n\nRan \`${pmCommands.cmd} ${pmCommands.forceRefreshArgs.join(' ')}\` to re-resolve all dependencies from the registry.`;

  // Create branch
  const branchSpinner = p.spinner();
  branchSpinner.start('Creating maintenance branch');
  await deps.createBranch(repoRoot, branchName);
  branchSpinner.stop(`Created branch: ${branchName}`);

  // Stage only lock files (not all changes -- avoids sweeping unrelated local changes)
  const commitSpinner = p.spinner();
  commitSpinner.start('Committing lock file changes');
  await deps.stageFiles(repoRoot, pmCommands.lockFileNames);
  await deps.commit(repoRoot, commitTitle, commitBody);
  commitSpinner.stop('Commit created');

  // Push
  const pushSpinner = p.spinner();
  pushSpinner.start('Pushing to remote');
  await deps.pushWithUpstream(repoRoot, remote, branchName);
  pushSpinner.stop(`Pushed to ${remote}/${branchName}`);

  // Create PR (standalone, targeting base branch directly -- no stacking)
  const prSpinner = p.spinner();
  prSpinner.start('Creating pull request');

  try {
    const pr = await deps.createPR(config, repoRoot, {
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
      await deps.deleteRemoteBranch(repoRoot, remote, branchName);
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
