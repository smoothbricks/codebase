/**
 * Tests for PR modification functions (state-changing operations)
 */

import { describe, expect, test } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import { autoCloseOldPRs, createPR } from '../../src/pr/stacking.js';
import { createErrorExeca, createExecaSpy } from '../helpers/mock-execa.js';

describe('createPR', () => {
  const config: DepUpdaterConfig = {
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

  test('should create PR and extract number from URL', async () => {
    const spy = createExecaSpy({
      'gh pr create --title Test PR --body PR body --base main --head feature':
        'https://github.com/owner/repo/pull/123',
    });

    const result = await createPR(
      config,
      '/repo',
      {
        title: 'Test PR',
        body: 'PR body',
        baseBranch: 'main',
        headBranch: 'feature',
      },
      spy.mock,
    );

    expect(result.number).toBe(123);
    expect(result.url).toBe('https://github.com/owner/repo/pull/123');
  });

  test('should call gh with correct arguments', async () => {
    const spy = createExecaSpy({
      'gh pr create --title Update deps --body Changelog here --base main --head chore/update-deps-2025-01-01':
        'https://github.com/owner/repo/pull/456',
    });

    await createPR(
      config,
      '/repo',
      {
        title: 'Update deps',
        body: 'Changelog here',
        baseBranch: 'main',
        headBranch: 'chore/update-deps-2025-01-01',
      },
      spy.mock,
    );

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]?.[0]).toBe('gh');
    expect(spy.calls[0]?.[1]).toEqual([
      'pr',
      'create',
      '--title',
      'Update deps',
      '--body',
      'Changelog here',
      '--base',
      'main',
      '--head',
      'chore/update-deps-2025-01-01',
    ]);
    expect(spy.calls[0]?.[2]?.cwd).toBe('/repo');
  });

  test('should handle PR with multi-line body', async () => {
    const spy = createExecaSpy({
      'gh pr create --title PR --body Line 1\nLine 2\nLine 3 --base main --head feature':
        'https://github.com/owner/repo/pull/789',
    });

    const result = await createPR(
      config,
      '/repo',
      {
        title: 'PR',
        body: 'Line 1\nLine 2\nLine 3',
        baseBranch: 'main',
        headBranch: 'feature',
      },
      spy.mock,
    );

    expect(result.number).toBe(789);
  });

  test('should return 0 for number if URL does not match pattern', async () => {
    const spy = createExecaSpy({
      'gh pr create --title PR --body body --base main --head feature': 'https://github.com/owner/repo',
    });

    const result = await createPR(
      config,
      '/repo',
      {
        title: 'PR',
        body: 'body',
        baseBranch: 'main',
        headBranch: 'feature',
      },
      spy.mock,
    );

    expect(result.number).toBe(0);
    expect(result.url).toBe('https://github.com/owner/repo');
  });

  test('should throw error on failure', async () => {
    const mockExeca = createErrorExeca('gh: failed to create PR');

    await expect(
      createPR(
        config,
        '/repo',
        {
          title: 'PR',
          body: 'body',
          baseBranch: 'main',
          headBranch: 'feature',
        },
        mockExeca,
      ),
    ).rejects.toThrow('Failed to create PR');
  });
});

describe('autoCloseOldPRs', () => {
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

  test('should not close PRs if autoCloseOldPRs is disabled', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      prStrategy: {
        ...baseConfig.prStrategy,
        autoCloseOldPRs: false,
      },
    };

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify([]),
    });

    await autoCloseOldPRs(config, '/repo', spy.mock);

    // Should not make any calls
    expect(spy.calls).toHaveLength(0);
  });

  test('should not close PRs if under maxStackDepth', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(prs), stderr: '', exitCode: 0 };
      }
      if (key.startsWith('gh pr view')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    await autoCloseOldPRs(baseConfig, '/repo', mockExeca);

    // Only calls to get PRs, no close commands (maxStackDepth is 3, we have 2 PRs)
  });

  test('should close oldest PRs when at maxStackDepth', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-3',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 3 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr close 1 --comment Auto-closed: superseded by newer dependency updates': '',
    });

    await autoCloseOldPRs(baseConfig, '/repo', spy.mock);

    // Should close PR #1 (oldest)
    const closeCalls = spy.calls.filter((c) => c[1]?.[0] === 'pr' && c[1]?.[1] === 'close');
    expect(closeCalls).toHaveLength(1);
    expect(closeCalls[0]?.[1]?.[2]).toBe('1');
  });

  test('should close multiple PRs when over maxStackDepth', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-3',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
      {
        number: 4,
        title: 'PR 4',
        headRefName: 'chore/update-deps-4',
        createdAt: '2025-01-04T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/4',
      },
      {
        number: 5,
        title: 'PR 5',
        headRefName: 'chore/update-deps-5',
        createdAt: '2025-01-05T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/5',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 3 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 4 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 5 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr close 1 --comment Auto-closed: superseded by newer dependency updates': '',
      'gh pr close 2 --comment Auto-closed: superseded by newer dependency updates': '',
      'gh pr close 3 --comment Auto-closed: superseded by newer dependency updates': '',
    });

    await autoCloseOldPRs(baseConfig, '/repo', spy.mock);

    // Should close PRs #1, #2, #3 (5 PRs total, maxStackDepth is 3, so close oldest 3)
    const closeCalls = spy.calls.filter((c) => c[1]?.[0] === 'pr' && c[1]?.[1] === 'close');
    expect(closeCalls).toHaveLength(3);
    expect(closeCalls[0]?.[1]?.[2]).toBe('1');
    expect(closeCalls[1]?.[1]?.[2]).toBe('2');
    expect(closeCalls[2]?.[1]?.[2]).toBe('3');
  });

  test('should continue on close error', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-3',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(prs), stderr: '', exitCode: 0 };
      }
      if (key.startsWith('gh pr view')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      if (key.includes('pr close 1')) {
        throw new Error('PR already closed');
      }
      if (key.includes('pr close')) {
        return { stdout: '', stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    // Should not throw
    await autoCloseOldPRs(baseConfig, '/repo', mockExeca);
  });
});
