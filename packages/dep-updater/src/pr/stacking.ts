/**
 * Stacked PR management
 */

import { execa as execaOriginal } from 'execa';
import { GitHubCLIClient } from '../auth/github-client.js';
import type { DepUpdaterConfig } from '../config.js';
import type { Logger } from '../logger.js';
import type { CommandExecutor, IGitHubClient, OpenPR } from '../types.js';

/** Default executor - execa cast to CommandExecutor type */
const defaultExecutor = execaOriginal as unknown as CommandExecutor;

/**
 * Get list of open update PRs
 * @throws Error if GitHub API call fails (callers should handle this)
 */
export async function getOpenUpdatePRs(
  repoRoot: string,
  branchPrefix: string,
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
  logger?: Logger,
): Promise<OpenPR[]> {
  // Use GitHub client if provided, otherwise fall back to CLI
  const githubClient = client || new GitHubCLIClient(executor);

  // Get all open PRs - let errors propagate so callers can distinguish
  // "no PRs" from "GitHub unreachable"
  const allPRs = await githubClient.listUpdatePRs(repoRoot);

  // Filter by branch prefix
  const filteredPRs = allPRs.filter((pr) => pr.headRefName.startsWith(branchPrefix));

  // Check each PR for conflicts
  const prsWithConflicts = await Promise.all(
    filteredPRs.map(async (pr) => {
      const hasConflicts = await checkPRConflicts(repoRoot, pr.number, executor, client, logger);
      return {
        number: pr.number,
        title: pr.title,
        branch: pr.headRefName,
        createdAt: new Date(pr.createdAt),
        hasConflicts,
        url: pr.url,
      };
    }),
  );

  // Sort by creation date (oldest first)
  prsWithConflicts.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());

  return prsWithConflicts;
}

/**
 * Check if a PR has merge conflicts
 * Returns true (assume conflicts) if check fails - safer than assuming no conflicts
 */
export async function checkPRConflicts(
  repoRoot: string,
  prNumber: number,
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
  logger?: Logger,
): Promise<boolean> {
  try {
    // Use GitHub client if provided, otherwise fall back to CLI
    const githubClient = client || new GitHubCLIClient(executor);
    return await githubClient.checkPRConflicts(repoRoot, prNumber);
  } catch (error) {
    // Return true (assume conflicts) when check fails - safer than assuming no conflicts
    // This prevents stacking on a potentially conflicted branch when GitHub is unreachable
    logger?.warn(
      `Failed to check PR #${prNumber} conflicts (assuming conflicts):`,
      error instanceof Error ? error.message : String(error),
    );
    return true;
  }
}

/**
 * Determine base branch for new PR based on stacking strategy
 */
export async function determineBaseBranch(
  config: DepUpdaterConfig,
  repoRoot: string,
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
): Promise<{ baseBranch: string; reason: string }> {
  const { prStrategy, git, logger } = config;
  const mainBranch = git?.baseBranch || 'main';

  if (!prStrategy.stackingEnabled) {
    return {
      baseBranch: mainBranch,
      reason: 'Stacking disabled',
    };
  }

  // Get open update PRs - fall back to mainBranch if GitHub is unreachable
  let openPRs: OpenPR[];
  try {
    openPRs = await getOpenUpdatePRs(repoRoot, prStrategy.branchPrefix, executor, client, logger);
  } catch (error) {
    logger?.warn(
      'Failed to fetch open PRs, falling back to main branch:',
      error instanceof Error ? error.message : String(error),
    );
    return {
      baseBranch: mainBranch,
      reason: 'GitHub unreachable, using main branch',
    };
  }

  if (openPRs.length === 0) {
    return {
      baseBranch: mainBranch,
      reason: 'No existing update PRs',
    };
  }

  // Get most recent PR
  const latestPR = openPRs[openPRs.length - 1];

  // Check if latest PR has conflicts
  if (prStrategy.stopOnConflicts && latestPR.hasConflicts) {
    return {
      baseBranch: mainBranch,
      reason: `Latest PR #${latestPR.number} has conflicts`,
    };
  }

  // Stack on latest PR
  return {
    baseBranch: latestPR.branch,
    reason: `Stacking on PR #${latestPR.number}`,
  };
}

/**
 * Auto-close old PRs to maintain max stack depth
 */
export async function autoCloseOldPRs(
  config: DepUpdaterConfig,
  repoRoot: string,
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
): Promise<void> {
  const { prStrategy, logger } = config;

  if (!prStrategy.autoCloseOldPRs) {
    return;
  }

  // Get open PRs - if GitHub is unreachable, skip auto-close (safe default)
  let openPRs: OpenPR[];
  try {
    openPRs = await getOpenUpdatePRs(repoRoot, prStrategy.branchPrefix, executor, client, logger);
  } catch (error) {
    logger?.warn('Failed to fetch open PRs for auto-close:', error instanceof Error ? error.message : String(error));
    return;
  }

  if (openPRs.length < prStrategy.maxStackDepth) {
    return;
  }

  // Use GitHub client if provided, otherwise fall back to CLI
  const githubClient = client || new GitHubCLIClient(executor);

  // Close oldest PRs beyond maxStackDepth
  const prsToClose = openPRs.slice(0, openPRs.length - prStrategy.maxStackDepth + 1);

  for (const pr of prsToClose) {
    try {
      config.logger?.info(`Closing old PR #${pr.number}: ${pr.title}`);

      await githubClient.closePR(repoRoot, pr.number, 'Auto-closed: superseded by newer dependency updates');

      config.logger?.info(`✓ Closed PR #${pr.number}`);
    } catch (error) {
      config.logger?.warn(`Failed to close PR #${pr.number}:`, error);
    }
  }
}

/**
 * Create a new PR using GitHub client
 */
export async function createPR(
  config: DepUpdaterConfig,
  repoRoot: string,
  options: {
    title: string;
    body: string;
    baseBranch: string;
    headBranch: string;
  },
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
): Promise<{ number: number; url: string }> {
  const { title, body, baseBranch, headBranch } = options;

  try {
    config.logger?.info(`Creating PR: ${title}`);
    config.logger?.info(`Base: ${baseBranch} ← Head: ${headBranch}`);

    // Use GitHub client if provided, otherwise fall back to CLI
    const githubClient = client || new GitHubCLIClient(executor);

    const result = await githubClient.createPR(repoRoot, {
      title,
      body,
      head: headBranch,
      base: baseBranch,
    });

    config.logger?.info(`✓ Created PR #${result.number}: ${result.url}`);

    return result;
  } catch (error) {
    throw new Error(`Failed to create PR: ${error}`);
  }
}

/**
 * Generate branch name for update PR
 * Includes timestamp to ensure uniqueness when multiple PRs created same day
 */
export function generateBranchName(config: DepUpdaterConfig, date?: Date): string {
  const { prStrategy } = config;
  const now = date || new Date();
  const dateStr = now.toISOString().split('T')[0];
  // Add hour-minute for uniqueness (multiple PRs same day)
  const timeStr = now.toISOString().split('T')[1]?.substring(0, 5).replace(':', '');

  return `${prStrategy.branchPrefix}-${dateStr}-${timeStr}`;
}

/**
 * Generate PR title
 */
export function generatePRTitle(config: DepUpdaterConfig, hasBreaking = false): string {
  const { prStrategy } = config;

  if (hasBreaking) {
    return `${prStrategy.prTitlePrefix} (includes breaking changes)`;
  }

  return prStrategy.prTitlePrefix;
}

/**
 * Complete stacked PR workflow
 *
 * This handles:
 * 1. Determining base branch
 * 2. Auto-closing old PRs
 * 3. Creating new PR
 */
export async function createStackedPR(
  config: DepUpdaterConfig,
  repoRoot: string,
  options: {
    title: string;
    body: string;
    headBranch: string;
  },
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
): Promise<{ number: number; url: string; baseBranch: string; reason: string }> {
  // Determine base branch
  const { baseBranch, reason } = await determineBaseBranch(config, repoRoot, executor, client);
  config.logger?.info(`Base branch: ${baseBranch} (${reason})`);

  // Auto-close old PRs
  await autoCloseOldPRs(config, repoRoot, executor, client);

  // Create PR
  const pr = await createPR(
    config,
    repoRoot,
    {
      ...options,
      baseBranch,
    },
    executor,
    client,
  );

  return { ...pr, baseBranch, reason };
}
