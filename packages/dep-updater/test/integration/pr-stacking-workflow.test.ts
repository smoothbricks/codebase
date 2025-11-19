/**
 * Integration tests for PR stacking workflows
 * Tests end-to-end scenarios with multiple PRs and stacking logic
 */

import { describe, expect, test } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import { autoCloseOldPRs, createStackedPR, getOpenUpdatePRs } from '../../src/pr/stacking.js';
import type { CommandExecutor } from '../../src/types.js';

/**
 * Helper to create a mock executor that simulates a full PR workflow
 */
function createPRWorkflowMock(scenario: {
  existingPRs: Array<{ number: number; branch: string; createdAt: string; mergeable: string }>;
  newPRNumber: number;
  closedPRs?: number[];
}): CommandExecutor {
  const { existingPRs, newPRNumber, closedPRs = [] } = scenario;
  const closedSet = new Set(closedPRs);

  return async (cmd: string | URL, args?: readonly string[]) => {
    const command = typeof cmd === 'string' ? cmd : cmd.toString();
    const key = [command, ...(args || [])].join(' ');

    // List all PRs
    if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
      const openPRs = existingPRs
        .filter((pr) => !closedSet.has(pr.number))
        .map((pr) => ({
          number: pr.number,
          title: `Update dependencies #${pr.number}`,
          headRefName: pr.branch,
          createdAt: pr.createdAt,
          url: `https://github.com/owner/repo/pull/${pr.number}`,
        }));
      return { stdout: JSON.stringify(openPRs), stderr: '', exitCode: 0 };
    }

    // Check mergeable status for any PR
    if (key.startsWith('gh pr view') && key.includes('--json mergeable')) {
      const prNumberMatch = key.match(/gh pr view (\d+)/);
      if (prNumberMatch) {
        const prNumber = Number.parseInt(prNumberMatch[1]!, 10);
        const pr = existingPRs.find((p) => p.number === prNumber);
        if (pr) {
          return {
            stdout: JSON.stringify({ mergeable: pr.mergeable }),
            stderr: '',
            exitCode: 0,
          };
        }
      }
      return { stdout: JSON.stringify({ mergeable: 'UNKNOWN' }), stderr: '', exitCode: 0 };
    }

    // Close PR
    if (key.startsWith('gh pr close')) {
      const prNumberMatch = key.match(/gh pr close (\d+)/);
      if (prNumberMatch) {
        const prNumber = Number.parseInt(prNumberMatch[1]!, 10);
        closedSet.add(prNumber);
      }
      return { stdout: '', stderr: '', exitCode: 0 };
    }

    // Create PR
    if (key.startsWith('gh pr create')) {
      return {
        stdout: `https://github.com/owner/repo/pull/${newPRNumber}`,
        stderr: '',
        exitCode: 0,
      };
    }

    throw new Error(`Unexpected command: ${key}`);
  };
}

describe('PR Stacking Workflow - Sequential PR Creation', () => {
  const baseConfig: DepUpdaterConfig = {
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 3,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      requireTests: true,
    },
    ai: {
      provider: 'anthropic',
    },
  };

  test('should create first PR on main when no existing PRs', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [],
      newPRNumber: 1,
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'Updated packages',
        headBranch: 'chore/update-deps-2025-01-01',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('No existing update PRs');
    expect(result.number).toBe(1);
  });

  test('should stack second PR on first PR branch', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'MERGEABLE',
        },
      ],
      newPRNumber: 2,
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'Updated more packages',
        headBranch: 'chore/update-deps-2025-01-02',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('chore/update-deps-2025-01-01');
    expect(result.reason).toBe('Stacking on PR #1');
    expect(result.number).toBe(2);
  });

  test('should stack third PR on second PR branch', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'MERGEABLE',
        },
        {
          number: 2,
          branch: 'chore/update-deps-2025-01-02',
          createdAt: '2025-01-02T10:00:00Z',
          mergeable: 'MERGEABLE',
        },
      ],
      newPRNumber: 3,
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'Updated even more packages',
        headBranch: 'chore/update-deps-2025-01-03',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('chore/update-deps-2025-01-02');
    expect(result.reason).toBe('Stacking on PR #2');
    expect(result.number).toBe(3);
  });

  test('should auto-close oldest PR when creating fourth PR at maxStackDepth=3', async () => {
    const closedPRs: number[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        const allPRs = [
          {
            number: 1,
            title: 'Update dependencies #1',
            headRefName: 'chore/update-deps-2025-01-01',
            createdAt: '2025-01-01T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/1',
          },
          {
            number: 2,
            title: 'Update dependencies #2',
            headRefName: 'chore/update-deps-2025-01-02',
            createdAt: '2025-01-02T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/2',
          },
          {
            number: 3,
            title: 'Update dependencies #3',
            headRefName: 'chore/update-deps-2025-01-03',
            createdAt: '2025-01-03T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/3',
          },
        ].filter((pr) => !closedPRs.includes(pr.number));
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr view') && key.includes('--json mergeable')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr close')) {
        const prNumberMatch = key.match(/gh pr close (\d+)/);
        if (prNumberMatch) {
          closedPRs.push(Number.parseInt(prNumberMatch[1]!, 10));
        }
        return { stdout: '', stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr create')) {
        return { stdout: 'https://github.com/owner/repo/pull/4', stderr: '', exitCode: 0 };
      }

      throw new Error(`Unexpected command: ${key}`);
    };

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'Fourth update',
        headBranch: 'chore/update-deps-2025-01-04',
      },
      mockExeca,
    );

    // Should close PR #1 (oldest)
    expect(closedPRs).toEqual([1]);
    // Should stack on PR #3 (latest)
    expect(result.baseBranch).toBe('chore/update-deps-2025-01-03');
    expect(result.number).toBe(4);
  });
});

describe('PR Stacking Workflow - Conflict Handling', () => {
  const baseConfig: DepUpdaterConfig = {
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 5,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      requireTests: true,
    },
    ai: {
      provider: 'anthropic',
    },
  };

  test('should create PR on main when latest PR has conflicts', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
      ],
      newPRNumber: 2,
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'New updates',
        headBranch: 'chore/update-deps-2025-01-02',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('Latest PR #1 has conflicts');
    expect(result.number).toBe(2);
  });

  test('should stack on conflicting PR when stopOnConflicts=false', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      prStrategy: {
        ...baseConfig.prStrategy,
        stopOnConflicts: false,
      },
    };

    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
      ],
      newPRNumber: 2,
    });

    const result = await createStackedPR(
      config,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'New updates',
        headBranch: 'chore/update-deps-2025-01-02',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('chore/update-deps-2025-01-01');
    expect(result.reason).toBe('Stacking on PR #1');
    expect(result.number).toBe(2);
  });

  test('should fall back to main when latest PR has conflicts even with older non-conflicting PRs', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'MERGEABLE',
        },
        {
          number: 2,
          branch: 'chore/update-deps-2025-01-02',
          createdAt: '2025-01-02T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
        {
          number: 3,
          branch: 'chore/update-deps-2025-01-03',
          createdAt: '2025-01-03T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
      ],
      newPRNumber: 4,
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'New updates',
        headBranch: 'chore/update-deps-2025-01-04',
      },
      mockExeca,
    );

    // Should fall back to main because latest PR has conflicts
    // Note: determineBaseBranch only checks the latest PR, not all PRs
    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('Latest PR #3 has conflicts');
    expect(result.number).toBe(4);
  });

  test('should create on main when all existing PRs have conflicts', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
        {
          number: 2,
          branch: 'chore/update-deps-2025-01-02',
          createdAt: '2025-01-02T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
      ],
      newPRNumber: 3,
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'New updates',
        headBranch: 'chore/update-deps-2025-01-03',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('Latest PR #2 has conflicts');
    expect(result.number).toBe(3);
  });
});

describe('PR Stacking Workflow - Stacking Disabled', () => {
  const config: DepUpdaterConfig = {
    prStrategy: {
      stackingEnabled: false,
      maxStackDepth: 3,
      autoCloseOldPRs: false,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      requireTests: true,
    },
    ai: {
      provider: 'anthropic',
    },
  };

  test('should always create PRs on main when stacking is disabled', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'MERGEABLE',
        },
        {
          number: 2,
          branch: 'chore/update-deps-2025-01-02',
          createdAt: '2025-01-02T10:00:00Z',
          mergeable: 'MERGEABLE',
        },
      ],
      newPRNumber: 3,
    });

    const result = await createStackedPR(
      config,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'New updates',
        headBranch: 'chore/update-deps-2025-01-03',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('Stacking disabled');
    expect(result.number).toBe(3);
  });

  test('should not auto-close PRs when stacking is disabled', async () => {
    const closedPRs: number[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify([]), stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr close')) {
        const prNumberMatch = key.match(/gh pr close (\d+)/);
        if (prNumberMatch) {
          closedPRs.push(Number.parseInt(prNumberMatch[1]!, 10));
        }
        return { stdout: '', stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr create')) {
        return { stdout: 'https://github.com/owner/repo/pull/1', stderr: '', exitCode: 0 };
      }

      throw new Error(`Unexpected command: ${key}`);
    };

    await createStackedPR(
      config,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'New updates',
        headBranch: 'chore/update-deps-2025-01-01',
      },
      mockExeca,
    );

    expect(closedPRs).toEqual([]);
  });
});

describe('PR Stacking Workflow - Custom Base Branch', () => {
  const config: DepUpdaterConfig = {
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 3,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      requireTests: true,
    },
    ai: {
      provider: 'anthropic',
    },
    git: {
      remote: 'origin',
      baseBranch: 'develop',
    },
  };

  test('should use custom base branch when no existing PRs', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [],
      newPRNumber: 1,
    });

    const result = await createStackedPR(
      config,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'Updates',
        headBranch: 'chore/update-deps-2025-01-01',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('develop');
    expect(result.reason).toBe('No existing update PRs');
  });

  test('should fall back to custom base branch on conflicts', async () => {
    const mockExeca = createPRWorkflowMock({
      existingPRs: [
        {
          number: 1,
          branch: 'chore/update-deps-2025-01-01',
          createdAt: '2025-01-01T10:00:00Z',
          mergeable: 'CONFLICTING',
        },
      ],
      newPRNumber: 2,
    });

    const result = await createStackedPR(
      config,
      '/repo',
      {
        title: 'chore: update dependencies',
        body: 'Updates',
        headBranch: 'chore/update-deps-2025-01-02',
      },
      mockExeca,
    );

    expect(result.baseBranch).toBe('develop');
    expect(result.reason).toBe('Latest PR #1 has conflicts');
  });
});

describe('PR Stacking Workflow - getOpenUpdatePRs Integration', () => {
  test('should correctly filter and sort PRs in workflow', async () => {
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return {
          stdout: JSON.stringify([
            {
              number: 5,
              title: 'Other PR',
              headRefName: 'feat/new-feature',
              createdAt: '2025-01-05T10:00:00Z',
              url: 'https://github.com/owner/repo/pull/5',
            },
            {
              number: 3,
              title: 'Update deps 3',
              headRefName: 'chore/update-deps-2025-01-03',
              createdAt: '2025-01-03T10:00:00Z',
              url: 'https://github.com/owner/repo/pull/3',
            },
            {
              number: 1,
              title: 'Update deps 1',
              headRefName: 'chore/update-deps-2025-01-01',
              createdAt: '2025-01-01T10:00:00Z',
              url: 'https://github.com/owner/repo/pull/1',
            },
            {
              number: 2,
              title: 'Update deps 2',
              headRefName: 'chore/update-deps-2025-01-02',
              createdAt: '2025-01-02T10:00:00Z',
              url: 'https://github.com/owner/repo/pull/2',
            },
          ]),
          stderr: '',
          exitCode: 0,
        };
      }

      if (key.startsWith('gh pr view') && key.includes('--json mergeable')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }

      throw new Error(`Unexpected command: ${key}`);
    };

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    // Should filter out feat/new-feature and sort by date (oldest first)
    expect(prs).toHaveLength(3);
    expect(prs[0]?.number).toBe(1);
    expect(prs[1]?.number).toBe(2);
    expect(prs[2]?.number).toBe(3);
  });
});

describe('PR Stacking Workflow - autoCloseOldPRs Integration', () => {
  const config: DepUpdaterConfig = {
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 2,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: {
      enabled: false,
      mode: 'none',
      requireTests: true,
    },
    ai: {
      provider: 'anthropic',
    },
  };

  test('should close multiple PRs when significantly over maxStackDepth', async () => {
    const closedPRs: number[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        const allPRs = [
          {
            number: 1,
            title: 'Update 1',
            headRefName: 'chore/update-deps-1',
            createdAt: '2025-01-01T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/1',
          },
          {
            number: 2,
            title: 'Update 2',
            headRefName: 'chore/update-deps-2',
            createdAt: '2025-01-02T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/2',
          },
          {
            number: 3,
            title: 'Update 3',
            headRefName: 'chore/update-deps-3',
            createdAt: '2025-01-03T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/3',
          },
          {
            number: 4,
            title: 'Update 4',
            headRefName: 'chore/update-deps-4',
            createdAt: '2025-01-04T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/4',
          },
          {
            number: 5,
            title: 'Update 5',
            headRefName: 'chore/update-deps-5',
            createdAt: '2025-01-05T10:00:00Z',
            url: 'https://github.com/owner/repo/pull/5',
          },
        ].filter((pr) => !closedPRs.includes(pr.number));
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr view') && key.includes('--json mergeable')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }

      if (key.startsWith('gh pr close')) {
        const prNumberMatch = key.match(/gh pr close (\d+)/);
        if (prNumberMatch) {
          closedPRs.push(Number.parseInt(prNumberMatch[1]!, 10));
        }
        return { stdout: '', stderr: '', exitCode: 0 };
      }

      throw new Error(`Unexpected command: ${key}`);
    };

    await autoCloseOldPRs(config, '/repo', mockExeca);

    // maxStackDepth is 2, we have 5 PRs
    // Logic: prsToClose = slice(0, 5 - 2 + 1) = slice(0, 4)
    // So we close 4 PRs, leaving 1, then new PR makes it 2 (at maxStackDepth)
    expect(closedPRs).toEqual([1, 2, 3, 4]);
  });
});
