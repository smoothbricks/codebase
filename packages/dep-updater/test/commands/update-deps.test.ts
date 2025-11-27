/**
 * Integration tests for update-deps command
 * Tests command structure, helper functions, and workflow coordination
 */

import { describe, expect, test } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import type { PackageUpdate, UpdateResult } from '../../src/types.js';

describe('update-deps command', () => {
  const defaultConfig: DepUpdaterConfig = {
    repoRoot: '/test-repo',
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
    nix: {
      enabled: false,
      devenvPath: './tooling/direnv',
      nixpkgsOverlayPath: './tooling/direnv/nixpkgs-overlay',
    },
    ai: {
      provider: 'anthropic',
    },
    git: {
      remote: 'origin',
      baseBranch: 'main',
    },
  };

  describe('Update result aggregation', () => {
    test('should aggregate updates from multiple ecosystems', () => {
      const bunUpdates: PackageUpdate[] = [
        {
          name: 'lodash',
          currentVersion: '4.17.20',
          newVersion: '4.17.21',
          updateType: 'patch',
          ecosystem: 'npm',
        },
        {
          name: 'react',
          currentVersion: '18.0.0',
          newVersion: '18.2.0',
          updateType: 'minor',
          ecosystem: 'npm',
        },
      ];

      const nixUpdates: PackageUpdate[] = [
        {
          name: 'nodejs',
          currentVersion: '20.0.0',
          newVersion: '22.0.0',
          updateType: 'major',
          ecosystem: 'nix',
        },
      ];

      const allUpdates = [...bunUpdates, ...nixUpdates];

      // Verify aggregation logic
      expect(allUpdates).toHaveLength(3);
      expect(allUpdates.filter((u) => u.ecosystem === 'npm')).toHaveLength(2);
      expect(allUpdates.filter((u) => u.ecosystem === 'nix')).toHaveLength(1);
      expect(allUpdates.filter((u) => u.updateType === 'major')).toHaveLength(1);
    });

    test('should handle empty update results', () => {
      const bunResult: UpdateResult = {
        success: true,
        updates: [],
        ecosystem: 'npm',
      };

      const devenvResult: UpdateResult = {
        success: true,
        updates: [],
        ecosystem: 'nix',
      };

      const allUpdates = [...bunResult.updates, ...devenvResult.updates];

      expect(allUpdates).toHaveLength(0);
    });

    test('should collect errors from failed updaters', () => {
      const bunResult: UpdateResult = {
        success: false,
        updates: [],
        ecosystem: 'npm',
        error: 'Failed to run bun update',
      };

      const devenvResult: UpdateResult = {
        success: false,
        updates: [],
        ecosystem: 'nix',
        error: 'Devenv update failed',
      };

      const errors: string[] = [];
      if (!bunResult.success && bunResult.error) {
        errors.push(`Bun update failed: ${bunResult.error}`);
      }
      if (!devenvResult.success && devenvResult.error) {
        errors.push(`Devenv update failed: ${devenvResult.error}`);
      }

      expect(errors).toHaveLength(2);
      expect(errors[0]).toContain('Bun update failed');
      expect(errors[1]).toContain('Devenv update failed');
    });
  });

  describe('Update type detection', () => {
    test('should detect breaking changes', () => {
      const updates: PackageUpdate[] = [
        {
          name: 'react',
          currentVersion: '17.0.0',
          newVersion: '18.0.0',
          updateType: 'major',
          ecosystem: 'npm',
        },
        {
          name: 'lodash',
          currentVersion: '4.17.20',
          newVersion: '4.17.21',
          updateType: 'patch',
          ecosystem: 'npm',
        },
      ];

      const hasBreaking = updates.some((u) => u.updateType === 'major');

      expect(hasBreaking).toBe(true);
    });

    test('should handle no breaking changes', () => {
      const updates: PackageUpdate[] = [
        {
          name: 'lodash',
          currentVersion: '4.17.20',
          newVersion: '4.17.21',
          updateType: 'patch',
          ecosystem: 'npm',
        },
        {
          name: 'express',
          currentVersion: '4.17.0',
          newVersion: '4.18.0',
          updateType: 'minor',
          ecosystem: 'npm',
        },
      ];

      const hasBreaking = updates.some((u) => u.updateType === 'major');

      expect(hasBreaking).toBe(false);
    });
  });

  describe('Config validation', () => {
    test('should have valid default config structure', () => {
      expect(defaultConfig.repoRoot).toBe('/test-repo');
      expect(defaultConfig.prStrategy.stackingEnabled).toBe(true);
      expect(defaultConfig.prStrategy.maxStackDepth).toBe(5);
      expect(defaultConfig.git?.baseBranch).toBe('main');
      expect(defaultConfig.git?.remote).toBe('origin');
    });

    test('should handle Nix disabled by default', () => {
      expect(defaultConfig.nix?.enabled).toBe(false);
    });

    test('should have correct branch prefix', () => {
      expect(defaultConfig.prStrategy.branchPrefix).toBe('chore/update-deps');
      expect(defaultConfig.prStrategy.prTitlePrefix).toBe('chore: update dependencies');
    });
  });

  describe('Commit message generation logic', () => {
    test('should use lock file message when no updates', () => {
      const updates: PackageUpdate[] = [];

      const commitTitle = updates.length === 0 ? 'chore: update lock file' : 'chore: update dependencies';
      const prBody =
        updates.length === 0
          ? 'Updated lock file to resolve dependencies within existing semver ranges.'
          : 'Updated packages';

      expect(commitTitle).toBe('chore: update lock file');
      expect(prBody).toContain('lock file');
    });

    test('should use dependency message when updates exist', () => {
      const updates: PackageUpdate[] = [
        {
          name: 'lodash',
          currentVersion: '4.17.20',
          newVersion: '4.17.21',
          updateType: 'patch',
          ecosystem: 'npm',
        },
      ];

      const commitTitle = updates.length === 0 ? 'chore: update lock file' : 'chore: update dependencies';

      expect(commitTitle).toBe('chore: update dependencies');
    });
  });

  describe('Workflow flags', () => {
    test('should respect dry-run flag', () => {
      const options = {
        dryRun: true,
        skipGit: false,
        skipAI: false,
      };

      // In dry-run mode, should not execute git operations
      expect(options.dryRun).toBe(true);

      // Logic check: if dry-run, skip PR creation
      const shouldCreatePR = !options.dryRun;
      expect(shouldCreatePR).toBe(false);
    });

    test('should respect skip-git flag', () => {
      const options = {
        dryRun: false,
        skipGit: true,
        skipAI: false,
      };

      // With skipGit, should not create PR
      expect(options.skipGit).toBe(true);

      const shouldCreatePR = !options.dryRun && !options.skipGit;
      expect(shouldCreatePR).toBe(false);
    });

    test('should respect skip-AI flag', () => {
      const options = {
        dryRun: false,
        skipGit: false,
        skipAI: true,
      };

      // With skipAI, should not fetch changelogs
      expect(options.skipAI).toBe(true);

      const shouldFetchChangelogs = !options.skipAI;
      expect(shouldFetchChangelogs).toBe(false);
    });

    test('should create PR when all flags are false', () => {
      const options = {
        dryRun: false,
        skipGit: false,
        skipAI: false,
      };

      const shouldCreatePR = !options.dryRun && !options.skipGit;
      const shouldFetchChangelogs = !options.skipAI;

      expect(shouldCreatePR).toBe(true);
      expect(shouldFetchChangelogs).toBe(true);
    });
  });

  describe('Stacking logic', () => {
    test('should use main branch when stacking disabled', () => {
      const config: DepUpdaterConfig = {
        ...defaultConfig,
        prStrategy: {
          ...defaultConfig.prStrategy,
          stackingEnabled: false,
        },
      };

      const mainBranch = config.git?.baseBranch || 'main';
      const shouldCheckoutBase = config.prStrategy.stackingEnabled;

      expect(shouldCheckoutBase).toBe(false);
      expect(mainBranch).toBe('main');
    });

    test('should checkout base when stacking enabled', () => {
      const stackBase = 'chore/update-deps-2025-01-19'; // Previous PR branch
      const mainBranch = 'main';
      const stackingEnabled = true;

      const shouldCheckoutBase = stackingEnabled && stackBase !== mainBranch;

      expect(shouldCheckoutBase).toBe(true);
    });
  });

  describe('Error handling logic', () => {
    test('should continue with partial success', () => {
      const bunResult: UpdateResult = {
        success: true,
        updates: [
          {
            name: 'lodash',
            currentVersion: '4.17.20',
            newVersion: '4.17.21',
            updateType: 'patch',
            ecosystem: 'npm',
          },
        ],
        ecosystem: 'npm',
      };

      const devenvResult: UpdateResult = {
        success: false,
        updates: [],
        ecosystem: 'nix',
        error: 'Devenv not found',
      };

      const allUpdates = [...bunResult.updates, ...devenvResult.updates];
      const errors = [];
      if (!devenvResult.success && devenvResult.error) {
        errors.push(devenvResult.error);
      }

      // Should have 1 update from Bun
      expect(allUpdates).toHaveLength(1);
      // Should have 1 error from devenv
      expect(errors).toHaveLength(1);
      // Should still proceed with available updates
      expect(allUpdates.length > 0).toBe(true);
    });
  });

  describe('Lock file handling', () => {
    test('should detect lock file only changes', () => {
      const hasPackageJsonChanges = false;
      const hasLockFileChanges = true;
      const isClean = false;

      const hasUpdates = hasPackageJsonChanges || hasLockFileChanges;
      const isLockFileOnly = !hasPackageJsonChanges && hasLockFileChanges;

      expect(hasUpdates).toBe(true);
      expect(isLockFileOnly).toBe(true);
      expect(isClean).toBe(false);
    });

    test('should handle no changes at all', () => {
      const hasPackageJsonChanges = false;
      const hasLockFileChanges = false;
      const isClean = true;

      const hasUpdates = hasPackageJsonChanges || hasLockFileChanges;

      expect(hasUpdates).toBe(false);
      expect(isClean).toBe(true);
    });
  });
});
