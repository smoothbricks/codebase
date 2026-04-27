/**
 * Rebase open patchnote PRs onto updated base branch
 *
 * When new commits land on main/master, this command rebases all
 * open patchnote-created PRs onto the updated base. Stacked PRs
 * are rebased in topological order (base-of-stack first).
 */

import { execa } from 'execa';
import { GitHubCLIClient } from '../auth/github-client.js';
import type { PatchnoteConfig } from '../config.js';
import { fetch, getCurrentBranch, push, rebase, switchBranch } from '../git.js';
import type { CommandExecutor, GitHubPR, IGitHubClient, RebaseOpenPRsOptions, RebaseResult } from '../types.js';

const defaultExecutor = execa as unknown as CommandExecutor;

/**
 * Build topological rebase order from a list of PRs.
 * Returns an array of "levels": level 0 = roots (base is the main branch),
 * level 1 = children of roots, etc.
 *
 * PRs whose baseRefName does not match any other PR's headRefName are roots.
 */
export function buildRebaseOrder(prs: GitHubPR[], baseBranch: string): GitHubPR[][] {
  if (prs.length === 0) return [];

  // Map headRefName -> PR for quick lookup
  const headMap = new Map<string, GitHubPR>();
  for (const pr of prs) {
    headMap.set(pr.headRefName, pr);
  }

  // Build parent -> children adjacency
  const children = new Map<string, GitHubPR[]>();
  const roots: GitHubPR[] = [];

  for (const pr of prs) {
    const base = pr.baseRefName || baseBranch;
    if (headMap.has(base)) {
      // This PR is stacked on another PR
      const existing = children.get(base) || [];
      existing.push(pr);
      children.set(base, existing);
    } else {
      // Root PR (base is the main branch or something not in our set)
      roots.push(pr);
    }
  }

  // BFS from roots
  const levels: GitHubPR[][] = [];
  let currentLevel = roots;

  while (currentLevel.length > 0) {
    levels.push(currentLevel);
    const nextLevel: GitHubPR[] = [];
    for (const pr of currentLevel) {
      const kids = children.get(pr.headRefName);
      if (kids) {
        nextLevel.push(...kids);
      }
    }
    currentLevel = nextLevel;
  }

  return levels;
}

/**
 * Rebase all open patchnote PRs onto their updated base branches.
 *
 * Algorithm:
 * 1. Save current branch
 * 2. Fetch remote
 * 3. List open patchnote PRs (via GitHub API)
 * 4. Filter stale PRs
 * 5. Build topological order for stacked PRs
 * 6. Rebase each PR in order; on conflict, post comment + skip children
 * 7. Restore original branch
 */
export async function rebaseOpenPRs(
  config: PatchnoteConfig,
  options: RebaseOpenPRsOptions,
  executor: CommandExecutor = defaultExecutor,
  client?: IGitHubClient,
): Promise<RebaseResult[]> {
  const repoRoot = config.repoRoot || process.cwd();
  const remote = config.git?.remote || 'origin';
  const baseBranch = config.git?.baseBranch || 'main';
  const branchPrefix = config.prStrategy.branchPrefix;
  const githubClient = client || new GitHubCLIClient(executor);
  const logger = config.logger;

  // 1. Get all open PRs
  const allPRs = await githubClient.listUpdatePRs(repoRoot);

  // Filter by branch prefix (only patchnote PRs)
  const patchnotePRs = allPRs.filter((pr) => pr.headRefName.startsWith(branchPrefix));

  if (patchnotePRs.length === 0) {
    logger?.info('No open patchnote PRs found');
    return [];
  }

  // 2. Filter stale PRs
  const now = Date.now();
  const maxAgeMs = options.maxAgeDays * 24 * 60 * 60 * 1000;
  const results: RebaseResult[] = [];

  const activePRs: GitHubPR[] = [];
  for (const pr of patchnotePRs) {
    const age = now - new Date(pr.createdAt).getTime();
    if (age > maxAgeMs) {
      results.push({
        prNumber: pr.number,
        branch: pr.headRefName,
        success: false,
        skipped: true,
        skipReason: 'stale',
      });
      logger?.info(`Skipping stale PR #${pr.number}: ${pr.title}`);
    } else {
      activePRs.push(pr);
    }
  }

  if (activePRs.length === 0) {
    logger?.info('All patchnote PRs are stale, nothing to rebase');
    return results;
  }

  // 3. Build topological order
  const levels = buildRebaseOrder(activePRs, baseBranch);

  // 4. Dry-run mode: list planned actions without mutating
  if (options.dryRun) {
    logger?.info('Dry-run mode: listing planned rebase actions\n');
    for (let i = 0; i < levels.length; i++) {
      for (const pr of levels[i]!) {
        const rebaseTarget =
          pr.baseRefName && pr.baseRefName !== baseBranch ? `origin/${pr.baseRefName}` : `origin/${baseBranch}`;
        logger?.info(`  PR #${pr.number} (${pr.headRefName}) -> rebase onto ${rebaseTarget}`);
        results.push({
          prNumber: pr.number,
          branch: pr.headRefName,
          success: false,
          skipped: true,
        });
      }
    }
    return results;
  }

  // 5. Save original branch and fetch
  const originalBranch = await getCurrentBranch(repoRoot, executor);
  await fetch(repoRoot, remote, executor);

  // Track failed PRs to cascade to children
  const failedBranches = new Set<string>();

  try {
    for (const level of levels) {
      for (const pr of level) {
        // Check if parent failed
        const parentBase = pr.baseRefName || baseBranch;
        if (failedBranches.has(parentBase)) {
          results.push({
            prNumber: pr.number,
            branch: pr.headRefName,
            success: false,
            skipped: true,
            skipReason: 'parent-conflict',
          });
          // Mark this branch as failed too so its children are skipped
          failedBranches.add(pr.headRefName);
          logger?.info(`Skipping PR #${pr.number}: parent branch had conflict`);
          continue;
        }

        // Try to switch to PR branch
        try {
          await switchBranch(repoRoot, pr.headRefName, executor);
        } catch {
          results.push({
            prNumber: pr.number,
            branch: pr.headRefName,
            success: false,
            skipped: true,
            skipReason: 'branch-missing',
          });
          failedBranches.add(pr.headRefName);
          logger?.warn(`Branch ${pr.headRefName} not found, skipping PR #${pr.number}`);
          continue;
        }

        // Determine rebase target
        const rebaseTarget =
          parentBase !== baseBranch && activePRs.some((p) => p.headRefName === parentBase)
            ? `${remote}/${parentBase}`
            : `${remote}/${baseBranch}`;

        // Attempt rebase
        const rebaseSuccess = await rebase(repoRoot, rebaseTarget, executor);

        if (rebaseSuccess) {
          await push(repoRoot, remote, pr.headRefName, true, executor);
          results.push({
            prNumber: pr.number,
            branch: pr.headRefName,
            success: true,
            skipped: false,
          });
          logger?.info(`Rebased PR #${pr.number} onto ${rebaseTarget}`);
        } else {
          // Conflict detected -- post comment and mark as failed
          failedBranches.add(pr.headRefName);
          try {
            await githubClient.commentOnPR(
              repoRoot,
              pr.number,
              `Automatic rebase onto \`${rebaseTarget}\` failed due to conflicts. Please rebase manually.`,
            );
          } catch {
            logger?.warn(`Failed to post conflict comment on PR #${pr.number}`);
          }
          results.push({
            prNumber: pr.number,
            branch: pr.headRefName,
            success: false,
            skipped: false,
            skipReason: 'conflict',
          });
          logger?.warn(`Rebase of PR #${pr.number} failed due to conflicts`);
        }
      }
    }
  } finally {
    // 6. Restore original branch
    try {
      await switchBranch(repoRoot, originalBranch, executor);
    } catch {
      logger?.warn(`Failed to restore original branch: ${originalBranch}`);
    }
  }

  // 7. Log summary
  const rebased = results.filter((r) => r.success).length;
  const skipped = results.filter((r) => r.skipped).length;
  const conflicted = results.filter((r) => r.skipReason === 'conflict').length;
  logger?.info(`\nRebase complete: ${rebased} rebased, ${skipped} skipped, ${conflicted} conflicts`);

  return results;
}
