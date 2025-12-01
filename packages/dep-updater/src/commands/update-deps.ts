/**
 * Update all dependencies command
 */

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
  config.logger?.info(`Base branch: ${stackBase} (${reason})\n`);

  const mainBranch = config.git?.baseBranch || 'main';

  // If stacking is enabled and we're basing on a PR branch, checkout that branch first
  // This ensures updates only find NEW changes beyond what's in the base PR
  if (stackBase !== mainBranch && config.prStrategy.stackingEnabled) {
    config.logger?.info(`=== Checking out base branch ${stackBase} ===`);
    const remote = config.git?.remote || 'origin';
    await fetch(repoRoot, remote);
    await switchBranch(repoRoot, stackBase);
    config.logger?.info(`✓ Switched to ${stackBase}\n`);
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
  config.logger?.info('=== Updating npm dependencies (Bun) ===');
  const bunResult = await updateBunDependencies(repoRoot, {
    dryRun: options.dryRun,
    recursive: true,
    syncpackFixCommand: config.syncpack?.fixScriptName,
    logger: config.logger,
  });

  if (bunResult.success) {
    allUpdates.push(...bunResult.updates);
  } else {
    errors.push(`Bun update failed: ${bunResult.error}`);
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
    config.logger?.info('\n=== Updating devenv (Nix) ===');
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
    } else {
      errors.push(`Devenv update failed: ${devenvResult.error}`);
    }

    // Update nixpkgs overlay
    config.logger?.info('\n=== Updating nixpkgs overlay ===');
    const overlayPath = safeResolve(repoRoot, config.nix.nixpkgsOverlayPath);
    nixpkgsResult = await updateNixpkgsOverlay(overlayPath, {
      dryRun: options.dryRun,
      logger: config.logger,
    });

    if (nixpkgsResult.success) {
      allUpdates.push(...nixpkgsResult.updates);
    } else {
      errors.push(`Nixpkgs update failed: ${nixpkgsResult.error}`);
    }
  } else {
    config.logger?.info('\n=== Skipping Nix updates (disabled in config) ===');
  }

  // Report results
  config.logger?.info('\n=== Update Summary ===');
  config.logger?.info(`Total updates: ${allUpdates.length}`);
  config.logger?.info(`- npm: ${bunResult.updates.length}`);
  if (config.nix?.enabled) {
    config.logger?.info(`- nix: ${devenvResult.updates.length}`);
    config.logger?.info(`- nixpkgs: ${nixpkgsResult.updates.length}`);
  }

  if (errors.length > 0) {
    config.logger?.error('\nErrors:');
    for (const error of errors) {
      config.logger?.error(`  - ${error}`);
    }
  }

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
  config.logger?.info('\n=== Fetching changelogs ===');
  const changelogs = options.skipAI ? new Map<string, string>() : await fetchChangelogs(allUpdates, 5, config.logger);

  // Generate commit message
  config.logger?.info('\n=== Generating commit message ===');
  let prBody: string;

  if (options.skipAI || changelogs.size === 0) {
    const { body } = await generateCommitMessage(allUpdates, config, allDowngrades);
    prBody = body;
  } else {
    config.logger?.info('Analyzing changelogs with AI...');
    prBody = await analyzeChangelogs(allUpdates, changelogs, config, allDowngrades);
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
  config.logger?.info('\n=== Creating update branch ===');
  const branchName = generateBranchName(config);
  const remote = config.git?.remote || 'origin';

  // Create new branch from current position (already checked out correct base)
  const { createBranch, pushWithUpstream, deleteRemoteBranch } = await import('../git.js');
  await createBranch(repoRoot, branchName);
  config.logger?.info('✓ Created branch:', branchName);

  // Create commit on the new branch
  config.logger?.info('\n=== Creating commit ===');
  await createUpdateCommit(config, commitTitle, prBody);

  // Push the branch
  config.logger?.info('\n=== Pushing branch ===');
  await pushWithUpstream(repoRoot, remote, branchName);
  config.logger?.info('✓ Pushed to remote:', `${remote}/${branchName}`);

  // Create PR (uses stackBase determined at the beginning)
  // If PR creation fails, clean up the orphan branch on remote
  config.logger?.info('\n=== Creating PR ===');
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

    config.logger?.info(`\n✓ Created PR #${pr.number}: ${pr.url}`);
    config.logger?.info(`  Base: ${stackBase}`);
  } catch (error) {
    // Clean up orphan branch on remote if PR creation fails
    config.logger?.error('PR creation failed, cleaning up remote branch...');
    try {
      await deleteRemoteBranch(repoRoot, remote, branchName);
      config.logger?.info(`✓ Deleted orphan branch: ${remote}/${branchName}`);
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
    config.logger?.info('🔧 Running config script...\n');
    await executeConfigScript(configPath);
    config.logger?.info('\n✓ Script execution complete!');
    return;
  }

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
      config.logger?.info('\n✓ No updates available');
      return;
    }

    // Handle lock file only updates on PR branches
    if (allUpdates.length === 0 && !isClean) {
      config.logger?.info('\n!  No package.json updates, but lock files were updated');
      config.logger?.info('    (dependencies updated within existing semver ranges)');

      // If we're on a PR branch (not main) and stacking is enabled, commit the lock file changes to it
      // The stackingEnabled check ensures we actually switched to stackBase in setupBranchForStacking
      if (stackBase !== mainBranch && config.prStrategy.stackingEnabled) {
        config.logger?.info('    Committing lock file updates to existing PR branch');

        if (!options.skipGit) {
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
          config.logger?.info(`✓ Lock file changes pushed to ${stackBase}`);
        }
        return;
      }
    }

    // Dry run exit - show what would be created
    if (options.dryRun) {
      const branchName = generateBranchName(config);
      const { commitTitle, prBody } = await generateCommitData(allUpdates, config, options, allDowngrades);
      config.logger?.info('\n[DRY RUN] Would create:');
      config.logger?.info(`  Branch: ${branchName}`);
      config.logger?.info(`  Commit: ${commitTitle}`);
      config.logger?.info(`  PR base: ${stackBase}`);
      config.logger?.info('\n  PR Description:');
      config.logger?.info(`  ${'─'.repeat(50)}`);
      for (const line of prBody.split('\n')) {
        config.logger?.info(`  ${line}`);
      }
      config.logger?.info(`  ${'─'.repeat(50)}`);
      return;
    }

    // Generate commit data
    const { commitTitle, prBody } = await generateCommitData(allUpdates, config, options, allDowngrades);

    // Create PR workflow
    if (!options.skipGit) {
      await createPRWorkflow(config, repoRoot, commitTitle, prBody, stackBase, allUpdates);
    }

    config.logger?.info('\n✓ Dependency update complete!');
  } finally {
    // Shutdown OpenCode server if it was started (allows process to exit cleanly)
    await shutdownOpenCodeClient(config.logger);
  }
}
