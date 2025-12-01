/**
 * Update all dependencies command
 */

import * as p from '@clack/prompts';
import { shutdownOpenCodeClient } from '../ai/opencode-client.js';
import { analyzeChangelogs, generateCommitMessage } from '../changelog/analyzer.js';
import { fetchChangelogs } from '../changelog/fetcher.js';
import type { DepUpdaterConfig } from '../config.js';
import { executeConfigScript, findConfigFile, isConfigScript } from '../config.js';
import { createUpdateCommit, fetch, getRepoRoot, switchBranch } from '../git.js';
import { determineBaseBranch, generateBranchName, generatePRTitle } from '../pr/stacking.js';
import type { PackageUpdate, UpdateOptions, UpdateResult } from '../types.js';
import { updateBunDependencies } from '../updaters/bun.js';
import { updateDevenv } from '../updaters/devenv.js';
import { updateNixpkgsOverlay } from '../updaters/nixpkgs.js';
import { safeResolve } from '../utils/path-validation.js';

/**
 * Setup branch for stacked PR workflow
 * Returns the base branch to use for the PR
 */
async function setupBranchForStacking(
  config: DepUpdaterConfig,
  repoRoot: string,
): Promise<{ stackBase: string; mainBranch: string }> {
  // Determine base branch for stacking FIRST (before running updates)
  const { baseBranch: stackBase, reason } = await determineBaseBranch(config, repoRoot);
  p.log.info(`Base branch: ${stackBase} (${reason})`);

  const mainBranch = config.git?.baseBranch || 'main';

  // If stacking is enabled and we're basing on a PR branch, checkout that branch first
  // This ensures updates only find NEW changes beyond what's in the base PR
  if (stackBase !== mainBranch && config.prStrategy.stackingEnabled) {
    const checkoutSpinner = p.spinner();
    checkoutSpinner.start(`Checking out base branch ${stackBase}`);
    const remote = config.git?.remote || 'origin';
    await fetch(repoRoot, remote);
    await switchBranch(repoRoot, stackBase);
    checkoutSpinner.stop(`Switched to ${stackBase}`);
  }

  return { stackBase, mainBranch };
}

/**
 * Run all dependency updaters (npm, devenv, nixpkgs)
 * Returns collected updates and errors
 */
async function runAllUpdaters(
  config: DepUpdaterConfig,
  repoRoot: string,
  options: UpdateOptions,
): Promise<{
  allUpdates: PackageUpdate[];
  allDowngrades: PackageUpdate[];
  errors: string[];
  results: {
    bun: UpdateResult;
    devenv: UpdateResult;
    nixpkgs: UpdateResult;
  };
}> {
  const allUpdates: PackageUpdate[] = [];
  const allDowngrades: PackageUpdate[] = [];
  const errors: string[] = [];

  // Update Bun dependencies (will only find updates beyond what's in current branch)
  const bunSpinner = p.spinner();
  bunSpinner.start('Updating npm dependencies');
  const bunResult = await updateBunDependencies(repoRoot, {
    dryRun: options.dryRun,
    recursive: true,
    syncpackFixCommand: config.syncpack?.fixScriptName,
    logger: config.logger,
  });

  if (bunResult.success) {
    allUpdates.push(...bunResult.updates);
    bunSpinner.stop(`npm: ${bunResult.updates.length} updates`);
  } else {
    errors.push(`Bun update failed: ${bunResult.error}`);
    bunSpinner.stop('npm: failed');
  }

  // Nix updates (optional)
  let devenvResult: UpdateResult = {
    success: true,
    updates: [],
    ecosystem: 'nix',
  };
  let nixpkgsResult: UpdateResult = {
    success: true,
    updates: [],
    ecosystem: 'nix',
  };

  if (config.nix?.enabled) {
    // Update devenv
    const devenvSpinner = p.spinner();
    devenvSpinner.start('Updating devenv');
    const devenvPath = safeResolve(repoRoot, config.nix.devenvPath);
    devenvResult = await updateDevenv(devenvPath, {
      dryRun: options.dryRun,
      logger: config.logger,
    });

    if (devenvResult.success) {
      allUpdates.push(...devenvResult.updates);
      if (devenvResult.downgrades) {
        allDowngrades.push(...devenvResult.downgrades);
      }
      devenvSpinner.stop(`devenv: ${devenvResult.updates.length} updates`);
    } else {
      errors.push(`Devenv update failed: ${devenvResult.error}`);
      devenvSpinner.stop('devenv: failed');
    }

    // Update nixpkgs overlay
    const nixpkgsSpinner = p.spinner();
    nixpkgsSpinner.start('Updating nixpkgs overlay');
    const overlayPath = safeResolve(repoRoot, config.nix.nixpkgsOverlayPath);
    nixpkgsResult = await updateNixpkgsOverlay(overlayPath, {
      dryRun: options.dryRun,
      logger: config.logger,
    });

    if (nixpkgsResult.success) {
      allUpdates.push(...nixpkgsResult.updates);
      nixpkgsSpinner.stop(`nixpkgs: ${nixpkgsResult.updates.length} updates`);
    } else {
      errors.push(`Nixpkgs update failed: ${nixpkgsResult.error}`);
      nixpkgsSpinner.stop('nixpkgs: failed');
    }
  }

  // Report summary using p.note()
  const summaryLines = [`Total: ${allUpdates.length} updates`, `  npm: ${bunResult.updates.length}`];
  if (config.nix?.enabled) {
    summaryLines.push(`  devenv: ${devenvResult.updates.length}`);
    summaryLines.push(`  nixpkgs: ${nixpkgsResult.updates.length}`);
  }
  if (errors.length > 0) {
    summaryLines.push('');
    summaryLines.push('Errors:');
    for (const error of errors) {
      summaryLines.push(`  ${error}`);
    }
  }
  p.note(summaryLines.join('\n'), 'Summary');

  return {
    allUpdates,
    allDowngrades,
    errors,
    results: {
      bun: bunResult,
      devenv: devenvResult,
      nixpkgs: nixpkgsResult,
    },
  };
}

/**
 * Generate commit title and body from updates
 * Fetches changelogs and uses AI analysis if enabled
 */
async function generateCommitData(
  allUpdates: PackageUpdate[],
  config: DepUpdaterConfig,
  options: UpdateOptions,
  allDowngrades: PackageUpdate[] = [],
): Promise<{ commitTitle: string; prBody: string }> {
  if (allUpdates.length === 0) {
    // Lock file only update
    return {
      commitTitle: 'chore: update lock file',
      prBody: 'Updated lock file to resolve dependencies within existing semver ranges.',
    };
  }

  // Fetch changelogs
  let changelogs: Map<string, string>;
  if (options.skipAI) {
    changelogs = new Map<string, string>();
  } else {
    const changelogSpinner = p.spinner();
    changelogSpinner.start('Fetching changelogs');
    changelogs = await fetchChangelogs(allUpdates, 5, config.logger);
    changelogSpinner.stop(`Fetched ${changelogs.size} changelogs`);
  }

  // Generate commit message
  let prBody: string;

  if (options.skipAI || changelogs.size === 0) {
    const { body } = await generateCommitMessage(allUpdates, config, allDowngrades);
    prBody = body;
  } else {
    const aiSpinner = p.spinner();
    aiSpinner.start('Analyzing changelogs with AI');
    prBody = await analyzeChangelogs(allUpdates, changelogs, config, allDowngrades);
    aiSpinner.stop('AI analysis complete');
  }

  const { title } = await generateCommitMessage(allUpdates, config, allDowngrades);

  return {
    commitTitle: title,
    prBody,
  };
}

/**
 * Create branch, commit, push, and PR for updates
 */
async function createPRWorkflow(
  config: DepUpdaterConfig,
  repoRoot: string,
  commitTitle: string,
  prBody: string,
  stackBase: string,
  allUpdates: PackageUpdate[],
): Promise<void> {
  const branchName = generateBranchName(config);
  const remote = config.git?.remote || 'origin';

  // Create new branch from current position (already checked out correct base)
  const branchSpinner = p.spinner();
  branchSpinner.start('Creating update branch');
  const { createBranch, pushWithUpstream, deleteRemoteBranch } = await import('../git.js');
  await createBranch(repoRoot, branchName);
  branchSpinner.stop(`Created branch: ${branchName}`);

  // Create commit on the new branch
  const commitSpinner = p.spinner();
  commitSpinner.start('Creating commit');
  await createUpdateCommit(config, commitTitle, prBody);
  commitSpinner.stop('Commit created');

  // Push the branch
  const pushSpinner = p.spinner();
  pushSpinner.start('Pushing to remote');
  await pushWithUpstream(repoRoot, remote, branchName);
  pushSpinner.stop(`Pushed to ${remote}/${branchName}`);

  // Create PR (uses stackBase determined at the beginning)
  // If PR creation fails, clean up the orphan branch on remote
  const prSpinner = p.spinner();
  prSpinner.start('Creating pull request');
  const hasBreaking = allUpdates.some((u) => u.updateType === 'major');
  const prTitle = generatePRTitle(config, hasBreaking);

  try {
    const { createPR } = await import('../pr/stacking.js');
    const pr = await createPR(config, repoRoot, {
      title: prTitle,
      body: prBody,
      baseBranch: stackBase,
      headBranch: branchName,
    });

    prSpinner.stop(`Created PR #${pr.number}`);
    p.note(`${pr.url}\nBase: ${stackBase}`, 'Pull Request');
  } catch (error) {
    prSpinner.stop('PR creation failed');
    // Clean up orphan branch on remote if PR creation fails
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
    throw error;
  }
}

/**
 * Main command: Update all dependencies
 */
export async function updateDeps(config: DepUpdaterConfig, options: UpdateOptions): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot());

  // Check if config file is an executable script (script mode)
  const configPath = await findConfigFile(repoRoot);
  if (configPath && (await isConfigScript(configPath))) {
    p.intro('🔧 Running config script');
    await executeConfigScript(configPath);
    p.outro('✓ Script execution complete!');
    return;
  }

  p.intro('📦 Updating dependencies');

  try {
    // Setup branch for stacking
    const { stackBase, mainBranch } = await setupBranchForStacking(config, repoRoot);

    // Run all updaters
    const { allUpdates, allDowngrades } = await runAllUpdaters(config, repoRoot, options);

    // Check if there are any uncommitted changes (including lock files)
    const { isWorkingDirectoryClean } = await import('../git.js');
    const isClean = await isWorkingDirectoryClean(repoRoot);

    // Handle no updates case
    if (allUpdates.length === 0 && isClean) {
      p.outro('No updates available');
      return;
    }

    // Handle lock file only updates on PR branches
    if (allUpdates.length === 0 && !isClean) {
      p.log.warn('No package.json updates, but lock files were updated');
      p.log.info('Dependencies updated within existing semver ranges');

      // If we're on a PR branch (not main) and stacking is enabled, commit the lock file changes to it
      // The stackingEnabled check ensures we actually switched to stackBase in setupBranchForStacking
      if (stackBase !== mainBranch && config.prStrategy.stackingEnabled) {
        if (!options.skipGit) {
          const lockSpinner = p.spinner();
          lockSpinner.start('Committing lock file updates to existing PR branch');
          // Commit lock file changes to existing PR branch
          await createUpdateCommit(
            config,
            'chore: update lock file',
            'Updated lock file to resolve dependencies within existing semver ranges.',
          );

          // Push to remote
          const remote = config.git?.remote || 'origin';
          const { push } = await import('../git.js');
          await push(repoRoot, remote, stackBase);
          lockSpinner.stop(`Lock file changes pushed to ${stackBase}`);
        }
        p.outro('Lock file update complete');
        return;
      }
    }

    // Dry run exit - show what would be created
    if (options.dryRun) {
      const branchName = generateBranchName(config);
      const { commitTitle, prBody } = await generateCommitData(allUpdates, config, options, allDowngrades);

      const dryRunInfo = [`Branch: ${branchName}`, `Commit: ${commitTitle}`, `PR base: ${stackBase}`].join('\n');
      p.note(dryRunInfo, 'Dry Run - Would Create');
      p.note(prBody, 'PR Description');
      p.outro('Dry run complete');
      return;
    }

    // Generate commit data
    const { commitTitle, prBody } = await generateCommitData(allUpdates, config, options, allDowngrades);

    // Create PR workflow
    if (!options.skipGit) {
      await createPRWorkflow(config, repoRoot, commitTitle, prBody, stackBase, allUpdates);
    }

    p.outro('Dependency update complete!');
  } finally {
    // Shutdown OpenCode server if it was started (allows process to exit cleanly)
    await shutdownOpenCodeClient(config.logger);
  }
}
