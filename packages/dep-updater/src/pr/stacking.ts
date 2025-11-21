/**
 * Stacked PR management
 */

import { execa as execaOriginal } from 'execa';
import type { DepUpdaterConfig } from '../config.js';
import type { CommandExecutor, OpenPR } from '../types.js';

/** Default executor - execa cast to CommandExecutor type */
const defaultExecutor = execaOriginal as unknown as CommandExecutor;

/**
 * Get list of open update PRs
 */
export async function getOpenUpdatePRs(
  repoRoot: string,
  branchPrefix: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<OpenPR[]> {
  try {
    // Use gh CLI to list PRs
    const { stdout } = await executor(
      'gh',
      ['pr', 'list', '--json', 'number,title,headRefName,createdAt,url', '--state', 'open'],
      { cwd: repoRoot },
    );

    const rawData = JSON.parse(stdout);
    if (!Array.isArray(rawData)) {
      console.error('Invalid GitHub CLI response: expected array');
      return [];
    }

    const allPRs = rawData as Array<{
      number: number;
      title: string;
      headRefName: string;
      createdAt: string;
      url: string;
    }>;

    // Filter to only update PRs matching our branch prefix
    const updatePRs = allPRs.filter((pr) => pr.headRefName.startsWith(branchPrefix));

    // Check each PR for conflicts
    const prsWithConflicts = await Promise.all(
      updatePRs.map(async (pr) => {
        const hasConflicts = await checkPRConflicts(repoRoot, pr.number, executor);
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
  } catch (error) {
    console.warn('Failed to fetch open PRs:', error);
    return [];
  }
}

/**
 * Check if a PR has merge conflicts
 */
export async function checkPRConflicts(
  repoRoot: string,
  prNumber: number,
  executor: CommandExecutor = defaultExecutor,
): Promise<boolean> {
  try {
    const { stdout } = await executor('gh', ['pr', 'view', prNumber.toString(), '--json', 'mergeable'], {
      cwd: repoRoot,
    });

    const data = JSON.parse(stdout) as { mergeable: string };
    return data.mergeable === 'CONFLICTING';
  } catch (error) {
    console.warn(`Failed to check PR #${prNumber} conflicts:`, error instanceof Error ? error.message : String(error));
    return false;
  }
}

/**
 * Determine base branch for new PR based on stacking strategy
 */
export async function determineBaseBranch(
  config: DepUpdaterConfig,
  repoRoot: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<{ baseBranch: string; reason: string }> {
  const { prStrategy, git } = config;
  const mainBranch = git?.baseBranch || 'main';

  if (!prStrategy.stackingEnabled) {
    return {
      baseBranch: mainBranch,
      reason: 'Stacking disabled',
    };
  }

  // Get open update PRs
  const openPRs = await getOpenUpdatePRs(repoRoot, prStrategy.branchPrefix, executor);

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
): Promise<void> {
  const { prStrategy } = config;

  if (!prStrategy.autoCloseOldPRs) {
    return;
  }

  const openPRs = await getOpenUpdatePRs(repoRoot, prStrategy.branchPrefix, executor);

  if (openPRs.length < prStrategy.maxStackDepth) {
    return;
  }

  // Close oldest PRs beyond maxStackDepth
  const prsToClose = openPRs.slice(0, openPRs.length - prStrategy.maxStackDepth + 1);

  for (const pr of prsToClose) {
    try {
      config.logger?.info(`Closing old PR #${pr.number}: ${pr.title}`);

      await executor(
        'gh',
        ['pr', 'close', pr.number.toString(), '--comment', 'Auto-closed: superseded by newer dependency updates'],
        { cwd: repoRoot },
      );

      config.logger?.info(`✓ Closed PR #${pr.number}`);
    } catch (error) {
      config.logger?.warn(`Failed to close PR #${pr.number}:`, error);
    }
  }
}

/**
 * Create a new PR using gh CLI
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
): Promise<{ number: number; url: string }> {
  const { title, body, baseBranch, headBranch } = options;

  try {
    config.logger?.info(`Creating PR: ${title}`);
    config.logger?.info(`Base: ${baseBranch} ← Head: ${headBranch}`);

    const { stdout } = await executor(
      'gh',
      ['pr', 'create', '--title', title, '--body', body, '--base', baseBranch, '--head', headBranch],
      { cwd: repoRoot },
    );

    // Extract PR URL from output
    const url = stdout.trim();
    const prNumberMatch = url.match(/\/pull\/(\d+)$/);
    const number = prNumberMatch ? Number.parseInt(prNumberMatch[1], 10) : 0;

    config.logger?.info(`✓ Created PR #${number}: ${url}`);

    return { number, url };
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
): Promise<{ number: number; url: string; baseBranch: string; reason: string }> {
  // Determine base branch
  const { baseBranch, reason } = await determineBaseBranch(config, repoRoot, executor);
  config.logger?.info(`Base branch: ${baseBranch} (${reason})`);

  // Auto-close old PRs
  await autoCloseOldPRs(config, repoRoot, executor);

  // Create PR
  const pr = await createPR(
    config,
    repoRoot,
    {
      ...options,
      baseBranch,
    },
    executor,
  );

  return { ...pr, baseBranch, reason };
}
