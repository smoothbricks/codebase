/**
 * Tests for PR stacking algorithm (core logic)
 */

import { describe, expect, test } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import { createStackedPR, determineBaseBranch } from '../../src/pr/stacking.js';
import { createExecaSpy } from '../helpers/mock-execa.js';

describe('determineBaseBranch', () => {
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
    git: {
      remote: 'origin',
      baseBranch: 'main',
    },
  };

  test('should return main when stacking is disabled', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      prStrategy: {
        ...baseConfig.prStrategy,
        stackingEnabled: false,
      },
    };

    const spy = createExecaSpy({});

    const result = await determineBaseBranch(config, '/repo', spy.mock);

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('Stacking disabled');
    expect(spy.calls).toHaveLength(0); // Should not call gh at all
  });

  test('should return main when no open PRs exist', async () => {
    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify([]),
    });

    const result = await determineBaseBranch(baseConfig, '/repo', spy.mock);

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('No existing update PRs');
  });

  test('should return latest PR branch when one PR exists without conflicts', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
    });

    const result = await determineBaseBranch(baseConfig, '/repo', spy.mock);

    expect(result.baseBranch).toBe('chore/update-deps-2025-01-01');
    expect(result.reason).toBe('Stacking on PR #1');
  });

  test('should return latest PR branch when multiple PRs exist', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2025-01-02',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-2025-01-03',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 3 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
    });

    const result = await determineBaseBranch(baseConfig, '/repo', spy.mock);

    expect(result.baseBranch).toBe('chore/update-deps-2025-01-03');
    expect(result.reason).toBe('Stacking on PR #3');
  });

  test('should return main when latest PR has conflicts and stopOnConflicts is true', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2025-01-02',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
    });

    const result = await determineBaseBranch(baseConfig, '/repo', spy.mock);

    expect(result.baseBranch).toBe('main');
    expect(result.reason).toBe('Latest PR #2 has conflicts');
  });

  test('should return latest PR branch when it has conflicts but stopOnConflicts is false', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      prStrategy: {
        ...baseConfig.prStrategy,
        stopOnConflicts: false,
      },
    };

    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
    });

    const result = await determineBaseBranch(config, '/repo', spy.mock);

    expect(result.baseBranch).toBe('chore/update-deps-2025-01-01');
    expect(result.reason).toBe('Stacking on PR #1');
  });

  test('should use custom base branch from config', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      git: {
        remote: 'origin',
        baseBranch: 'develop',
      },
      prStrategy: {
        ...baseConfig.prStrategy,
        stackingEnabled: false,
      },
    };

    const spy = createExecaSpy({});

    const result = await determineBaseBranch(config, '/repo', spy.mock);

    expect(result.baseBranch).toBe('develop');
  });

  test('should default to main when git config is not set', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      git: undefined,
      prStrategy: {
        ...baseConfig.prStrategy,
        stackingEnabled: false,
      },
    };

    const spy = createExecaSpy({});

    const result = await determineBaseBranch(config, '/repo', spy.mock);

    expect(result.baseBranch).toBe('main');
  });

  test('should filter PRs by branch prefix', async () => {
    const allPRs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'Other PR',
        headRefName: 'feat/new-feature',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-2025-01-03',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(allPRs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 3 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
    });

    const result = await determineBaseBranch(baseConfig, '/repo', spy.mock);

    // Should stack on PR #3 (latest update PR), ignoring PR #2 (different prefix)
    expect(result.baseBranch).toBe('chore/update-deps-2025-01-03');
    expect(result.reason).toBe('Stacking on PR #3');
  });

  test('should handle older PRs with conflicts correctly', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2025-01-02',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-2025-01-03',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
      'gh pr view 3 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
    });

    const result = await determineBaseBranch(baseConfig, '/repo', spy.mock);

    // Should stack on PR #3 (latest, and no conflicts)
    expect(result.baseBranch).toBe('chore/update-deps-2025-01-03');
    expect(result.reason).toBe('Stacking on PR #3');
  });
});

describe('createStackedPR', () => {
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
    git: {
      remote: 'origin',
      baseBranch: 'main',
    },
  };

  test('should determine base, auto-close, and create PR', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2025-01-02',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
    ];

    const spy = createExecaSpy({
      // Called twice: once in determineBaseBranch, once in autoCloseOldPRs
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr create --title Update deps --body Changelog --base chore/update-deps-2025-01-02 --head chore/update-deps-2025-01-03':
        'https://github.com/owner/repo/pull/3',
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'Update deps',
        body: 'Changelog',
        headBranch: 'chore/update-deps-2025-01-03',
      },
      spy.mock,
    );

    expect(result.number).toBe(3);
    expect(result.baseBranch).toBe('chore/update-deps-2025-01-02');
    expect(result.url).toBe('https://github.com/owner/repo/pull/3');
  });

  test('should auto-close old PRs when at maxStackDepth', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2025-01-02',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-2025-01-03',
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
      'gh pr create --title Update deps --body Changelog --base chore/update-deps-2025-01-03 --head chore/update-deps-2025-01-04':
        'https://github.com/owner/repo/pull/4',
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'Update deps',
        body: 'Changelog',
        headBranch: 'chore/update-deps-2025-01-04',
      },
      spy.mock,
    );

    // Should close PR #1
    const closeCalls = spy.calls.filter((c) => c[1]?.[0] === 'pr' && c[1]?.[1] === 'close');
    expect(closeCalls).toHaveLength(1);
    expect(closeCalls[0]?.[1]?.[2]).toBe('1');

    // Should create new PR
    expect(result.number).toBe(4);
    expect(result.baseBranch).toBe('chore/update-deps-2025-01-03');
  });

  test('should base on main when no existing PRs', async () => {
    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify([]),
      'gh pr create --title Update deps --body Changelog --base main --head chore/update-deps-2025-01-01':
        'https://github.com/owner/repo/pull/1',
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'Update deps',
        body: 'Changelog',
        headBranch: 'chore/update-deps-2025-01-01',
      },
      spy.mock,
    );

    expect(result.baseBranch).toBe('main');
    expect(result.number).toBe(1);
  });

  test('should base on main when latest PR has conflicts', async () => {
    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
      'gh pr create --title Update deps --body Changelog --base main --head chore/update-deps-2025-01-02':
        'https://github.com/owner/repo/pull/2',
    });

    const result = await createStackedPR(
      baseConfig,
      '/repo',
      {
        title: 'Update deps',
        body: 'Changelog',
        headBranch: 'chore/update-deps-2025-01-02',
      },
      spy.mock,
    );

    expect(result.baseBranch).toBe('main');
    expect(result.number).toBe(2);
  });

  test('should not auto-close when autoCloseOldPRs is disabled', async () => {
    const config: DepUpdaterConfig = {
      ...baseConfig,
      prStrategy: {
        ...baseConfig.prStrategy,
        autoCloseOldPRs: false,
      },
    };

    const prs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2025-01-02',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'PR 3',
        headRefName: 'chore/update-deps-2025-01-03',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const spy = createExecaSpy({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(prs),
      'gh pr view 1 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 2 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr view 3 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      'gh pr create --title Update deps --body Changelog --base chore/update-deps-2025-01-03 --head chore/update-deps-2025-01-04':
        'https://github.com/owner/repo/pull/4',
    });

    await createStackedPR(
      config,
      '/repo',
      {
        title: 'Update deps',
        body: 'Changelog',
        headBranch: 'chore/update-deps-2025-01-04',
      },
      spy.mock,
    );

    // Should not close any PRs
    const closeCalls = spy.calls.filter((c) => c[1]?.[0] === 'pr' && c[1]?.[1] === 'close');
    expect(closeCalls).toHaveLength(0);
  });
});
