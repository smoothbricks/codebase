/**
 * Semantic commit detection and prefix resolution
 *
 * Detects whether a repository uses conventional commit format
 * and resolves the appropriate semantic prefix for dependency update
 * commit messages and PR titles.
 */

import type { PatchnoteConfig } from './config.js';
import { defaultConfig } from './config.js';
import { getRecentCommitMessages } from './git.js';
import type { CommandExecutor, PackageUpdate } from './types.js';

/**
 * Regex matching conventional commit format
 * Matches: type[(scope)][!]: description
 */
export const CONVENTIONAL_COMMIT_REGEX =
  /^(feat|fix|chore|docs|style|refactor|perf|test|build|ci|revert)(\(.+\))?!?:\s/;

/**
 * Detect whether commit messages follow conventional commit format
 *
 * @param commitMessages - Array of commit subject lines
 * @returns true if strictly more than 50% match conventional format
 */
export function detectSemanticCommits(commitMessages: string[]): boolean {
  if (commitMessages.length === 0) return false;

  const conventionalCount = commitMessages.filter((msg) => CONVENTIONAL_COMMIT_REGEX.test(msg)).length;
  return conventionalCount / commitMessages.length > 0.5;
}

/**
 * Resolve the semantic prefix to use for commit messages and PR titles
 *
 * @param config - Patchnote configuration
 * @param repoRoot - Repository root directory
 * @param updates - Package updates (used to determine dev vs prod prefix)
 * @param executor - Command executor for testing
 * @returns The resolved prefix string, or null if semantic commits are disabled
 */
export async function resolveSemanticPrefix(
  config: PatchnoteConfig,
  repoRoot: string,
  updates: PackageUpdate[],
  executor?: CommandExecutor,
): Promise<string | null> {
  const semanticConfig = config.semanticCommits ??
    defaultConfig.semanticCommits ?? {
      enabled: 'auto',
      prefix: 'chore(deps)',
      devPrefix: 'chore(dev-deps)',
    };
  const { enabled, prefix, devPrefix } = semanticConfig;

  // Explicitly disabled
  if (enabled === false) {
    return null;
  }

  // Auto-detect from repository history
  if (enabled === 'auto') {
    const baseBranch = config.git?.baseBranch ?? 'main';
    const args: [string, number, string | undefined] = [repoRoot, 10, baseBranch];
    const messages = executor
      ? await getRecentCommitMessages(...args, executor)
      : await getRecentCommitMessages(...args);

    if (!detectSemanticCommits(messages)) {
      return null;
    }
  }

  // Determine dev vs prod prefix
  // Use devPrefix only when ALL updates are dev dependencies
  if (updates.length > 0 && updates.every((u) => u.isDev === true)) {
    return devPrefix;
  }

  return prefix;
}
