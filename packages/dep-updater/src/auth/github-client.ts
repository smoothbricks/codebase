/**
 * GitHub CLI client for PR operations
 * Uses gh CLI for all GitHub operations
 */

import { execa } from 'execa';
import type { CommandExecutor, GitHubPR, IGitHubClient } from '../types.js';

/**
 * GitHub CLI client implementation
 * Uses gh CLI for all GitHub operations
 */
export class GitHubCLIClient implements IGitHubClient {
  private executor: CommandExecutor;

  constructor(executor?: CommandExecutor) {
    this.executor = executor || (execa as unknown as CommandExecutor);
  }

  async listUpdatePRs(repoRoot: string): Promise<GitHubPR[]> {
    try {
      const { stdout } = await this.executor(
        'gh',
        ['pr', 'list', '--json', 'number,title,headRefName,createdAt,url', '--state', 'open'],
        { cwd: repoRoot },
      );

      const parsed = JSON.parse(stdout);

      // Validate that gh CLI returned an array
      if (!Array.isArray(parsed)) {
        throw new Error(`Expected array from gh pr list, got ${typeof parsed}`);
      }

      // Return all PRs - let the caller filter by branch prefix
      return parsed as GitHubPR[];
    } catch (error: unknown) {
      throw this.enhanceError(error, 'list PRs');
    }
  }

  async checkPRConflicts(repoRoot: string, prNumber: number): Promise<boolean> {
    try {
      const { stdout } = await this.executor('gh', ['pr', 'view', prNumber.toString(), '--json', 'mergeable'], {
        cwd: repoRoot,
      });

      const parsed = JSON.parse(stdout);

      // Validate that gh CLI returned an object with mergeable field
      if (typeof parsed !== 'object' || parsed === null || !('mergeable' in parsed)) {
        throw new Error('Expected object with mergeable field from gh pr view');
      }

      const data = parsed as { mergeable: 'MERGEABLE' | 'CONFLICTING' | 'UNKNOWN' };
      return data.mergeable === 'CONFLICTING';
    } catch (error: unknown) {
      throw this.enhanceError(error, `check PR #${prNumber} conflicts`);
    }
  }

  async createPR(
    repoRoot: string,
    options: { title: string; body: string; head: string; base: string },
  ): Promise<{ number: number; url: string }> {
    try {
      const { stdout } = await this.executor(
        'gh',
        [
          'pr',
          'create',
          '--title',
          options.title,
          '--body',
          options.body,
          '--base',
          options.base,
          '--head',
          options.head,
        ],
        { cwd: repoRoot },
      );

      // Extract PR URL from output (format: https://github.com/owner/repo/pull/123)
      const url = stdout.trim();

      // Validate full GitHub PR URL format
      const prNumberMatch = url.match(/^https:\/\/github\.com\/[\w-]+\/[\w-]+\/pull\/(\d+)$/);

      if (!prNumberMatch) {
        throw new Error(`Expected GitHub PR URL from gh pr create, got: ${url}`);
      }

      const number = Number.parseInt(prNumberMatch[1], 10);
      return { number, url };
    } catch (error: unknown) {
      throw this.enhanceError(error, 'create PR');
    }
  }

  async closePR(repoRoot: string, prNumber: number, comment: string): Promise<void> {
    try {
      await this.executor('gh', ['pr', 'close', prNumber.toString(), '--comment', comment], { cwd: repoRoot });
    } catch (error: unknown) {
      throw this.enhanceError(error, `close PR #${prNumber}`);
    }
  }

  /**
   * Enhance GitHub CLI errors with helpful troubleshooting information
   */
  private enhanceError(error: unknown, operation: string): Error {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const stderr = (error as { stderr?: string }).stderr || '';

    // Check for common error patterns
    const is401 = errorMessage.includes('401') || stderr.includes('401') || errorMessage.includes('Unauthorized');
    const is404 = errorMessage.includes('404') || stderr.includes('404') || errorMessage.includes('Not Found');
    const is403 = errorMessage.includes('403') || stderr.includes('403') || errorMessage.includes('Forbidden');

    let enhancedMessage = `Failed to ${operation}: ${errorMessage}`;
    let troubleshooting: string[] = [];

    if (is401) {
      troubleshooting = [
        'Check that DEP_UPDATER_APP_ID is set correctly',
        'Check that DEP_UPDATER_APP_PRIVATE_KEY contains valid PEM content',
        'Verify GitHub App is installed on this repository',
        'Ensure App has required permissions (contents:write, pull-requests:write)',
      ];
    } else if (is404) {
      troubleshooting = [
        'Check that GitHub App is installed on this repository',
        'Verify repository exists and you have access',
        'For organization secrets, check repository access is granted',
      ];
    } else if (is403) {
      troubleshooting = [
        'Check GitHub App permissions in app settings',
        'Ensure Contents permission is set to "Read and write"',
        'Ensure Pull requests permission is set to "Read and write"',
        'Re-install the app if permissions were changed',
      ];
    }

    if (troubleshooting.length > 0) {
      enhancedMessage += `\n\nTroubleshooting:\n${troubleshooting.map((tip) => `  • ${tip}`).join('\n')}`;
      enhancedMessage += '\n\n📖 See docs/SETUP.md in the dep-updater package for detailed instructions';
      enhancedMessage += '\n🔍 Run: dep-updater validate-setup';
    }

    const enhancedError = new Error(enhancedMessage);
    if (error instanceof Error) {
      enhancedError.stack = error.stack; // Preserve original stack trace
    }
    return enhancedError;
  }
}
