import { describe, expect, test } from 'bun:test';
import { generateCommitMessage } from '../../src/changelog/analyzer.js';
import type { DepUpdaterConfig } from '../../src/config.js';
import type { PackageUpdate } from '../../src/types.js';

describe('Changelog Analyzer', () => {
  const mockConfig: DepUpdaterConfig = {
    expo: { enabled: false, packageJsonPath: './package.json' },
    syncpack: { configPath: './.syncpackrc.json', preserveCustomRules: true, fixScriptName: 'syncpack:fix' },
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 5,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
      branchPrefix: 'chore/update-deps',
      prTitlePrefix: 'chore: update dependencies',
    },
    autoMerge: { enabled: false, mode: 'none', requireTests: true },
    ai: { provider: 'anthropic' }, // No API key = fallback mode
    git: { remote: 'origin', baseBranch: 'main' },
  };

  describe('generateCommitMessage', () => {
    test('generates title with breaking changes warning for major updates', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'react',
          fromVersion: '19.1.0',
          toVersion: '20.0.0',
          updateType: 'major',
          ecosystem: 'npm',
        },
      ];

      const { title } = await generateCommitMessage(updates, mockConfig);

      expect(title).toContain('breaking changes');
    });

    test('generates normal title for non-breaking updates', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'vite',
          fromVersion: '7.2.0',
          toVersion: '7.2.1',
          updateType: 'patch',
          ecosystem: 'npm',
        },
      ];

      const { title } = await generateCommitMessage(updates, mockConfig);

      expect(title).not.toContain('breaking changes');
      expect(title).toContain('update dependencies');
    });

    test('generates fallback summary with grouped updates', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'react',
          fromVersion: '19.1.0',
          toVersion: '20.0.0',
          updateType: 'major',
          ecosystem: 'npm',
        },
        {
          name: 'vite',
          fromVersion: '7.2.0',
          toVersion: '7.3.0',
          updateType: 'minor',
          ecosystem: 'npm',
        },
        {
          name: 'typescript',
          fromVersion: '5.9.0',
          toVersion: '5.9.1',
          updateType: 'patch',
          ecosystem: 'npm',
        },
      ];

      const { body } = await generateCommitMessage(updates, mockConfig);

      expect(body).toContain('## Dependency Updates');
      expect(body).toContain('### ! Major Updates');
      expect(body).toContain('**react**: 19.1.0 → 20.0.0');
      expect(body).toContain('### Minor Updates');
      expect(body).toContain('vite: 7.2.0 → 7.3.0');
      expect(body).toContain('### Patch Updates');
      expect(body).toContain('typescript: 5.9.0 → 5.9.1');
    });

    test('includes total update count', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'react',
          fromVersion: '19.1.0',
          toVersion: '19.2.0',
          updateType: 'minor',
          ecosystem: 'npm',
        },
        {
          name: 'vite',
          fromVersion: '7.2.0',
          toVersion: '7.2.1',
          updateType: 'patch',
          ecosystem: 'npm',
        },
      ];

      const { body } = await generateCommitMessage(updates, mockConfig);

      expect(body).toContain('Total updates: 2');
    });

    test('includes ecosystem information', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'react',
          fromVersion: '19.1.0',
          toVersion: '19.2.0',
          updateType: 'minor',
          ecosystem: 'npm',
        },
        {
          name: 'devenv',
          fromVersion: '1.0.0',
          toVersion: '1.1.0',
          updateType: 'minor',
          ecosystem: 'nix',
        },
      ];

      const { body } = await generateCommitMessage(updates, mockConfig);

      expect(body).toContain('npm, nix');
    });

    test('includes Claude Code attribution', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'vite',
          fromVersion: '7.2.0',
          toVersion: '7.2.1',
          updateType: 'patch',
          ecosystem: 'npm',
        },
      ];

      const { body } = await generateCommitMessage(updates, mockConfig);

      expect(body).toContain('Generated with [Claude Code]');
      expect(body).toContain('Co-Authored-By: Claude');
    });

    test('handles empty updates array', async () => {
      const updates: PackageUpdate[] = [];

      const { title, body } = await generateCommitMessage(updates, mockConfig);

      expect(title).toBeTruthy();
      expect(body).toContain('Total updates: 0');
    });

    test('handles unknown update types', async () => {
      const updates: PackageUpdate[] = [
        {
          name: 'some-package',
          fromVersion: '1.0.0',
          toVersion: '2.0.0',
          updateType: 'unknown',
          ecosystem: 'npm',
        },
      ];

      const { body } = await generateCommitMessage(updates, mockConfig);

      // Should still generate valid output
      expect(body).toContain('Total updates: 1');
    });
  });
});
