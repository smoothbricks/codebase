/**
 * Integration tests: conventional commit detection against real git repos.
 * Fast -- no network access needed.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { execa } from 'execa';
import type { PatchnoteConfig } from '../../src/config.js';
import { getRecentCommitMessages } from '../../src/git.js';
import { resolveSemanticPrefix } from '../../src/semantic.js';
import { createTestRepo, type TestRepo } from './helpers/test-repo.js';

describe('Semantic commit detection - real repo', () => {
  let repo: TestRepo;

  beforeEach(async () => {
    repo = await createTestRepo('patchnote-test-minimal');
  });

  afterEach(async () => {
    await repo.cleanup();
  });

  const baseConfig: PatchnoteConfig = {
    semanticCommits: {
      enabled: 'auto',
      prefix: 'chore(deps)',
      devPrefix: 'chore(dev-deps)',
    },
    git: { remote: 'origin', baseBranch: 'main' },
    prStrategy: {
      stackingEnabled: false,
      maxStackDepth: 3,
      autoCloseOldPRs: false,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'patchnote/',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: { enabled: false, mode: 'none', requireTests: true, strategy: 'squash' },
    ai: { provider: 'zai' },
  };

  test('resolveSemanticPrefix returns non-null after conventional commits', async () => {
    // Create several conventional commits
    const messages = ['feat: add feature', 'fix: bug fix', 'chore: cleanup', 'test: add tests', 'refactor: simplify'];
    for (const msg of messages) {
      await execa('git', ['commit', '--allow-empty', '-m', msg], { cwd: repo.path });
    }

    const prefix = await resolveSemanticPrefix(baseConfig, repo.path, []);
    expect(prefix).not.toBeNull();
    expect(prefix).toBe('chore(deps)');
  });

  test('resolveSemanticPrefix returns null after non-conventional commits', async () => {
    // Create non-conventional commits
    const messages = ['added feature', 'fixed bug', 'cleanup', 'added tests', 'simplified code'];
    for (const msg of messages) {
      await execa('git', ['commit', '--allow-empty', '-m', msg], { cwd: repo.path });
    }

    const prefix = await resolveSemanticPrefix(baseConfig, repo.path, []);
    expect(prefix).toBeNull();
  });

  test('getRecentCommitMessages returns real commit messages', async () => {
    await execa('git', ['commit', '--allow-empty', '-m', 'feat: first'], { cwd: repo.path });
    await execa('git', ['commit', '--allow-empty', '-m', 'fix: second'], { cwd: repo.path });

    const messages = await getRecentCommitMessages(repo.path, 3);
    expect(messages.length).toBeGreaterThanOrEqual(2);
    expect(messages[0]).toBe('fix: second');
    expect(messages[1]).toBe('feat: first');
  });
});
