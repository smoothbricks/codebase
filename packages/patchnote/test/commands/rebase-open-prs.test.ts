/**
 * Tests for rebase-open-prs command
 */

import { describe, expect, test } from 'bun:test';
import { buildRebaseOrder, rebaseOpenPRs } from '../../src/commands/rebase-open-prs.js';
import type { PatchnoteConfig } from '../../src/config.js';
import type { CommandExecutor, GitHubPR, IGitHubClient, RebaseResult } from '../../src/types.js';

// --- Helpers ---

function makePR(overrides: Partial<GitHubPR> & { number: number; headRefName: string }): GitHubPR {
  return {
    title: `PR #${overrides.number}`,
    createdAt: new Date().toISOString(),
    url: `https://github.com/owner/repo/pull/${overrides.number}`,
    baseRefName: 'main',
    ...overrides,
  };
}

function makeConfig(overrides?: Partial<PatchnoteConfig>): PatchnoteConfig {
  return {
    repoRoot: '/repo',
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
      strategy: 'squash',
      requireTests: true,
    },
    ai: { provider: 'zai' },
    ...overrides,
  } as PatchnoteConfig;
}

function createMockClient(
  prs: GitHubPR[],
): IGitHubClient & { commentCalls: Array<{ prNumber: number; body: string }> } {
  const commentCalls: Array<{ prNumber: number; body: string }> = [];
  return {
    commentCalls,
    listUpdatePRs: async () => prs,
    checkPRConflicts: async () => false,
    createPR: async () => ({ number: 1, url: '' }),
    closePR: async () => {},
    enableAutoMerge: async () => {},
    findPRByHead: async () => null,
    editPR: async () => {},
    commentOnPR: async (_repoRoot: string, prNumber: number, body: string) => {
      commentCalls.push({ prNumber, body });
    },
  };
}

interface CallTracker {
  mock: CommandExecutor;
  calls: Array<[string | URL, readonly string[] | undefined]>;
}

function createCallTracker(options?: {
  rebaseFailBranches?: Set<string>;
  switchFailBranches?: Set<string>;
}): CallTracker {
  const calls: Array<[string | URL, readonly string[] | undefined]> = [];
  const rebaseFailBranches = options?.rebaseFailBranches || new Set();
  const switchFailBranches = options?.switchFailBranches || new Set();

  const mock: CommandExecutor = async (cmd, args, _opts) => {
    calls.push([cmd, args]);
    const command = typeof cmd === 'string' ? cmd : cmd.toString();
    const key = [command, ...(args || [])].join(' ');

    // Handle specific commands
    if (key === 'git rev-parse --abbrev-ref HEAD') {
      return { stdout: 'main', stderr: '', exitCode: 0 };
    }
    if (key.startsWith('git fetch')) {
      return { stdout: '', stderr: '', exitCode: 0 };
    }
    if (key.startsWith('git checkout')) {
      const branch = args?.[1];
      if (branch && switchFailBranches.has(branch)) {
        throw new Error(`error: pathspec '${branch}' did not match`);
      }
      return { stdout: '', stderr: '', exitCode: 0 };
    }
    if (key.startsWith('git rebase --abort')) {
      return { stdout: '', stderr: '', exitCode: 0 };
    }
    if (key.startsWith('git rebase')) {
      // Check if this branch should fail
      // The rebase is for the currently checked-out branch
      const lastCheckout = [...calls].reverse().find(([c, a]) => {
        const cmdStr = typeof c === 'string' ? c : c.toString();
        return cmdStr === 'git' && a?.[0] === 'checkout';
      });
      const currentBranch = lastCheckout?.[1]?.[1];
      if (currentBranch && rebaseFailBranches.has(currentBranch as string)) {
        throw new Error('CONFLICT');
      }
      return { stdout: '', stderr: '', exitCode: 0 };
    }
    if (key.startsWith('git push')) {
      return { stdout: '', stderr: '', exitCode: 0 };
    }

    return { stdout: '', stderr: '', exitCode: 0 };
  };

  return { mock, calls };
}

// --- buildRebaseOrder tests ---

describe('buildRebaseOrder', () => {
  test('should return empty array for no PRs', () => {
    const result = buildRebaseOrder([], 'main');
    expect(result).toEqual([]);
  });

  test('should put independent PRs all in level 0', () => {
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'main' }),
      makePR({ number: 3, headRefName: 'chore/update-deps-03', baseRefName: 'main' }),
    ];

    const levels = buildRebaseOrder(prs, 'main');

    expect(levels).toHaveLength(1);
    expect(levels[0]).toHaveLength(3);
  });

  test('should order stacked PRs correctly (base first, then children)', () => {
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'chore/update-deps-01' }),
      makePR({ number: 3, headRefName: 'chore/update-deps-03', baseRefName: 'chore/update-deps-02' }),
    ];

    const levels = buildRebaseOrder(prs, 'main');

    expect(levels).toHaveLength(3);
    expect(levels[0]![0]!.number).toBe(1); // root
    expect(levels[1]![0]!.number).toBe(2); // child of 1
    expect(levels[2]![0]!.number).toBe(3); // child of 2
  });

  test('should handle diamond dependencies', () => {
    // PR 1 (root)
    // PR 2 based on PR 1
    // PR 3 based on PR 1
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'chore/update-deps-01' }),
      makePR({ number: 3, headRefName: 'chore/update-deps-03', baseRefName: 'chore/update-deps-01' }),
    ];

    const levels = buildRebaseOrder(prs, 'main');

    expect(levels).toHaveLength(2);
    expect(levels[0]![0]!.number).toBe(1);
    expect(levels[1]).toHaveLength(2); // Both 2 and 3 are children of 1
    const childNumbers = levels[1]!.map((pr) => pr.number).sort();
    expect(childNumbers).toEqual([2, 3]);
  });

  test('should treat PRs with missing baseRefName as roots', () => {
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: undefined }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'main' }),
    ];

    const levels = buildRebaseOrder(prs, 'main');

    expect(levels).toHaveLength(1);
    expect(levels[0]).toHaveLength(2); // Both are roots
  });
});

// --- rebaseOpenPRs tests ---

describe('rebaseOpenPRs', () => {
  const baseOptions = {
    dryRun: false,
    skipGit: false,
    skipAI: false,
    maxAgeDays: 30,
  };

  test('should return empty results when no patchnote PRs exist', async () => {
    const mockClient = createMockClient([]);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toEqual([]);
  });

  test('should return empty results when no PRs match branch prefix', async () => {
    const prs = [makePR({ number: 1, headRefName: 'feat/unrelated' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toEqual([]);
  });

  test('should successfully rebase a single PR', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toHaveLength(1);
    expect(results[0]!.success).toBe(true);
    expect(results[0]!.skipped).toBe(false);
    expect(results[0]!.prNumber).toBe(1);

    // Verify call order: getCurrentBranch, fetch, checkout, rebase, push, restore
    const gitCalls = tracker.calls.filter(([cmd]) => cmd === 'git').map(([, args]) => args?.[0]);
    expect(gitCalls).toEqual(['rev-parse', 'fetch', 'checkout', 'rebase', 'push', 'checkout']);
  });

  test('should force-push after successful rebase', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    // Find the push call
    const pushCall = tracker.calls.find(([cmd, args]) => cmd === 'git' && args?.[0] === 'push');
    expect(pushCall).toBeDefined();
    expect(pushCall![1]).toContain('--force');
  });

  test('should skip stale PRs (older than maxAgeDays)', async () => {
    const oldDate = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString(); // 60 days ago
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', createdAt: oldDate })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, { ...baseOptions, maxAgeDays: 30 }, tracker.mock, mockClient);

    expect(results).toHaveLength(1);
    expect(results[0]!.skipped).toBe(true);
    expect(results[0]!.skipReason).toBe('stale');

    // Should NOT fetch or rebase
    const gitCalls = tracker.calls.filter(([cmd]) => cmd === 'git');
    expect(gitCalls).toHaveLength(0);
  });

  test('should handle conflict on rebase (abort + comment)', async () => {
    const prs = [makePR({ number: 42, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker({ rebaseFailBranches: new Set(['chore/update-deps-01']) });
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toHaveLength(1);
    expect(results[0]!.success).toBe(false);
    expect(results[0]!.skipReason).toBe('conflict');

    // Should have posted a comment
    expect(mockClient.commentCalls).toHaveLength(1);
    expect(mockClient.commentCalls[0]!.prNumber).toBe(42);
    expect(mockClient.commentCalls[0]!.body).toContain('conflicts');

    // Should NOT have pushed
    const pushCalls = tracker.calls.filter(([cmd, args]) => cmd === 'git' && args?.[0] === 'push');
    expect(pushCalls).toHaveLength(0);
  });

  test('should cascade parent conflict to child PRs (parent-conflict)', async () => {
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'chore/update-deps-01' }),
    ];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker({ rebaseFailBranches: new Set(['chore/update-deps-01']) });
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toHaveLength(2);
    expect(results[0]!.prNumber).toBe(1);
    expect(results[0]!.skipReason).toBe('conflict');
    expect(results[1]!.prNumber).toBe(2);
    expect(results[1]!.skipped).toBe(true);
    expect(results[1]!.skipReason).toBe('parent-conflict');
  });

  test('should rebase stacked PRs in topological order', async () => {
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'chore/update-deps-01' }),
    ];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toHaveLength(2);
    expect(results[0]!.success).toBe(true);
    expect(results[1]!.success).toBe(true);

    // Verify order: checkout PR1, rebase, push, checkout PR2, rebase, push
    const checkoutCalls = tracker.calls.filter(
      ([cmd, args]) => cmd === 'git' && args?.[0] === 'checkout' && args?.[1] !== 'main',
    );
    expect(checkoutCalls[0]![1]![1]).toBe('chore/update-deps-01'); // base first
    expect(checkoutCalls[1]![1]![1]).toBe('chore/update-deps-02'); // then child
  });

  test('should skip branch-missing when checkout fails', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker({ switchFailBranches: new Set(['chore/update-deps-01']) });
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    expect(results).toHaveLength(1);
    expect(results[0]!.skipped).toBe(true);
    expect(results[0]!.skipReason).toBe('branch-missing');
  });

  test('should restore original branch after all operations', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    // Last checkout should be restoring original branch
    const checkoutCalls = tracker.calls.filter(([cmd, args]) => cmd === 'git' && args?.[0] === 'checkout');
    const lastCheckout = checkoutCalls[checkoutCalls.length - 1];
    expect(lastCheckout![1]![1]).toBe('main'); // original branch
  });

  test('should restore original branch even on error', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker({ rebaseFailBranches: new Set(['chore/update-deps-01']) });
    const config = makeConfig();

    await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    // Should still restore original branch
    const checkoutCalls = tracker.calls.filter(([cmd, args]) => cmd === 'git' && args?.[0] === 'checkout');
    const lastCheckout = checkoutCalls[checkoutCalls.length - 1];
    expect(lastCheckout![1]![1]).toBe('main');
  });

  test('should not fetch/rebase/push in dry-run mode', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, { ...baseOptions, dryRun: true }, tracker.mock, mockClient);

    expect(results).toHaveLength(1);
    expect(results[0]!.skipped).toBe(true);

    // No git commands should have been issued
    const gitCalls = tracker.calls.filter(([cmd]) => cmd === 'git');
    expect(gitCalls).toHaveLength(0);
  });

  test('should use stacked PR branch as rebase target for child PRs', async () => {
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', baseRefName: 'chore/update-deps-01' }),
    ];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    // Find the rebase calls
    const rebaseCalls = tracker.calls.filter(([cmd, args]) => cmd === 'git' && args?.[0] === 'rebase');
    expect(rebaseCalls).toHaveLength(2);
    expect(rebaseCalls[0]![1]![1]).toBe('origin/main'); // PR 1 rebases onto main
    expect(rebaseCalls[1]![1]![1]).toBe('origin/chore/update-deps-01'); // PR 2 rebases onto parent
  });

  test('should handle mixed stale and active PRs', async () => {
    const oldDate = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
    const newDate = new Date().toISOString();
    const prs = [
      makePR({ number: 1, headRefName: 'chore/update-deps-01', createdAt: oldDate, baseRefName: 'main' }),
      makePR({ number: 2, headRefName: 'chore/update-deps-02', createdAt: newDate, baseRefName: 'main' }),
    ];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig();

    const results = await rebaseOpenPRs(config, { ...baseOptions, maxAgeDays: 30 }, tracker.mock, mockClient);

    expect(results).toHaveLength(2);
    const staleResult = results.find((r) => r.prNumber === 1);
    const activeResult = results.find((r) => r.prNumber === 2);
    expect(staleResult!.skipReason).toBe('stale');
    expect(activeResult!.success).toBe(true);
  });

  test('should use configured remote name', async () => {
    const prs = [makePR({ number: 1, headRefName: 'chore/update-deps-01', baseRefName: 'main' })];
    const mockClient = createMockClient(prs);
    const tracker = createCallTracker();
    const config = makeConfig({ git: { remote: 'upstream', baseBranch: 'main' } });

    await rebaseOpenPRs(config, baseOptions, tracker.mock, mockClient);

    // Fetch should use 'upstream'
    const fetchCall = tracker.calls.find(([cmd, args]) => cmd === 'git' && args?.[0] === 'fetch');
    expect(fetchCall![1]![1]).toBe('upstream');

    // Push should use 'upstream'
    const pushCall = tracker.calls.find(([cmd, args]) => cmd === 'git' && args?.[0] === 'push');
    expect(pushCall![1]![1]).toBe('upstream');
  });
});
