/**
 * GitHub CLI client for PR operations
 * Uses gh CLI for all GitHub operations
 */

import typia from 'typia';
import { executeCommand } from '../executor.js';
import type { CommandExecutor, GitHubPR, IGitHubClient, MergeStrategy } from '../types.js';

/**
 * GitHub CLI client implementation
 * Uses gh CLI for all GitHub operations
 */
export class GitHubCLIClient implements IGitHubClient {
  private executor: CommandExecutor;

  constructor(executor?: CommandExecutor) {
    this.executor = executor || executeCommand;
  }

  async listUpdatePRs(repoRoot: string): Promise<GitHubPR[]> {
    try {
      const { stdout } = await this.executor(
        'gh',
        ['pr', 'list', '--json', 'number,title,headRefName,baseRefName,createdAt,url', '--state', 'open'],
        { cwd: repoRoot },
      );

      const parsed = typia.json.validateParse<GitHubPR[]>(stdout);
      if (!parsed.success) throw new Error('Expected array of valid PRs from gh pr list');
      // Return all PRs - let the caller filter by branch prefix.
      return parsed.data;
    } catch (error: unknown) {
      throw this.enhanceError(error, 'list PRs');
    }
  }

  async checkPRConflicts(repoRoot: string, prNumber: number): Promise<boolean> {
    try {
      const { stdout } = await this.executor('gh', ['pr', 'view', prNumber.toString(), '--json', 'mergeable'], {
        cwd: repoRoot,
      });

      const parsed = typia.json.validateParse<{ mergeable: 'MERGEABLE' | 'CONFLICTING' | 'UNKNOWN' }>(stdout);
      if (!parsed.success) throw new Error('Expected object with mergeable field from gh pr view');
      return parsed.data.mergeable === 'CONFLICTING';
    } catch (error: unknown) {
      throw this.enhanceError(error, `check PR #${prNumber} conflicts`);
    }
  }

  async createPR(
    repoRoot: string,
    options: {
      title: string;
      body: string;
      head: string;
      base: string;
      labels?: string[];
      assignees?: string[];
      reviewers?: string[];
      draft?: boolean;
    },
  ): Promise<{ number: number; url: string }> {
    try {
      const args: string[] = [
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
      ];

      if (options.labels) {
        for (const label of options.labels) {
          args.push('--label', label);
        }
      }
      if (options.assignees) {
        for (const assignee of options.assignees) {
          args.push('--assignee', assignee);
        }
      }
      if (options.reviewers) {
        for (const reviewer of options.reviewers) {
          args.push('--reviewer', reviewer);
        }
      }
      if (options.draft) {
        args.push('--draft');
      }

      const { stdout } = await this.executor('gh', args, { cwd: repoRoot });

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

  async enableAutoMerge(repoRoot: string, prNumber: number, strategy: MergeStrategy): Promise<void> {
    try {
      await this.executor('gh', ['pr', 'merge', prNumber.toString(), '--auto', `--${strategy}`], { cwd: repoRoot });
    } catch (error: unknown) {
      throw this.enhanceError(error, `enable auto-merge on PR #${prNumber}`);
    }
  }

  async findPRByHead(repoRoot: string, headBranch: string): Promise<GitHubPR | null> {
    try {
      const { stdout } = await this.executor(
        'gh',
        [
          'pr',
          'list',
          '--head',
          headBranch,
          '--state',
          'open',
          '--json',
          'number,title,headRefName,baseRefName,createdAt,url',
          '--limit',
          '1',
        ],
        { cwd: repoRoot },
      );

      const parsed = typia.json.validateParse<GitHubPR[]>(stdout);
      if (!parsed.success) throw new Error('Expected array of valid PRs from gh pr list');
      return parsed.data[0] ?? null;
    } catch (error: unknown) {
      throw this.enhanceError(error, 'find PR by head branch');
    }
  }

  async editPR(repoRoot: string, prNumber: number, options: { body: string }): Promise<void> {
    try {
      await this.executor('gh', ['pr', 'edit', prNumber.toString(), '--body', options.body], { cwd: repoRoot });
    } catch (error: unknown) {
      throw this.enhanceError(error, `edit PR #${prNumber}`);
    }
  }

  async commentOnPR(repoRoot: string, prNumber: number, body: string): Promise<void> {
    try {
      await this.executor('gh', ['pr', 'comment', prNumber.toString(), '--body', body], { cwd: repoRoot });
    } catch (error: unknown) {
      throw this.enhanceError(error, `comment on PR #${prNumber}`);
    }
  }

  /**
   * Enhance GitHub CLI errors with helpful troubleshooting information
   */
  private enhanceError(error: unknown, operation: string): Error {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const stderr = typia.is<Pick<Awaited<ReturnType<CommandExecutor>>, 'stderr'>>(error) ? error.stderr : '';

    // Check for common error patterns
    const is401 = errorMessage.includes('401') || stderr.includes('401') || errorMessage.includes('Unauthorized');
    const is404 = errorMessage.includes('404') || stderr.includes('404') || errorMessage.includes('Not Found');
    const is403 = errorMessage.includes('403') || stderr.includes('403') || errorMessage.includes('Forbidden');

    let enhancedMessage = `Failed to ${operation}: ${errorMessage}`;
    let troubleshooting: string[] = [];

    if (is401) {
      troubleshooting = [
        'Check that PATCHNOTE_APP_ID is set correctly',
        'Check that PATCHNOTE_APP_PRIVATE_KEY contains valid PEM content',
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
      enhancedMessage += '\n\n📖 See docs/SETUP.md in the patchnote package for detailed instructions';
      enhancedMessage += '\n🔍 Run: patchnote validate-setup';
    }

    const enhancedError = new Error(enhancedMessage);
    if (error instanceof Error) {
      enhancedError.stack = error.stack; // Preserve original stack trace
    }
    return enhancedError;
  }
}
