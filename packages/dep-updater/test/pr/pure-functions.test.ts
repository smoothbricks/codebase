/**
 * Tests for pure PR functions (no external dependencies)
 */

import { describe, expect, test } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import { generateBranchName, generatePRTitle } from '../../src/pr/stacking.js';

describe('generateBranchName', () => {
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

  test('should generate branch name with current date', () => {
    const date = new Date('2025-01-15T10:00:00Z');
    const branchName = generateBranchName(config, date);

    expect(branchName).toBe('chore/update-deps-2025-01-15-1000');
  });

  test('should use current date if not provided', () => {
    const branchName = generateBranchName(config);

    expect(branchName).toMatch(/^chore\/update-deps-\d{4}-\d{2}-\d{2}-\d{4}$/);
  });

  test('should handle different branch prefixes', () => {
    const customConfig: DepUpdaterConfig = {
      ...config,
      prStrategy: {
        ...config.prStrategy,
        branchPrefix: 'feat/deps-update',
      },
    };

    const date = new Date('2025-02-20T10:00:00Z');
    const branchName = generateBranchName(customConfig, date);

    expect(branchName).toBe('feat/deps-update-2025-02-20-1000');
  });

  test('should handle branch prefix without slash', () => {
    const customConfig: DepUpdaterConfig = {
      ...config,
      prStrategy: {
        ...config.prStrategy,
        branchPrefix: 'update-deps',
      },
    };

    const date = new Date('2025-03-01T10:00:00Z');
    const branchName = generateBranchName(customConfig, date);

    expect(branchName).toBe('update-deps-2025-03-01-1000');
  });
});

describe('generatePRTitle', () => {
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

  test('should generate PR title without breaking changes flag', () => {
    const title = generatePRTitle(config);

    expect(title).toBe('chore: update dependencies');
  });

  test('should generate PR title without breaking changes when explicitly false', () => {
    const title = generatePRTitle(config, false);

    expect(title).toBe('chore: update dependencies');
  });

  test('should add breaking changes note when flag is true', () => {
    const title = generatePRTitle(config, true);

    expect(title).toBe('chore: update dependencies (includes breaking changes)');
  });

  test('should handle different title prefixes', () => {
    const customConfig: DepUpdaterConfig = {
      ...config,
      prStrategy: {
        ...config.prStrategy,
        prTitlePrefix: 'feat: upgrade dependencies',
      },
    };

    const title = generatePRTitle(customConfig);

    expect(title).toBe('feat: upgrade dependencies');
  });

  test('should handle different title prefixes with breaking changes', () => {
    const customConfig: DepUpdaterConfig = {
      ...config,
      prStrategy: {
        ...config.prStrategy,
        prTitlePrefix: 'feat: upgrade packages',
      },
    };

    const title = generatePRTitle(customConfig, true);

    expect(title).toBe('feat: upgrade packages (includes breaking changes)');
  });
});
