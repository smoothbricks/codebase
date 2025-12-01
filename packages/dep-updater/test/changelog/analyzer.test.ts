import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import { countTokens } from '../../src/ai/token-counter.js';
// Import directly without mocking - we'll test fallback paths
// This avoids mock persistence issues with Bun's module mocking
import { analyzeChangelogs, generateCommitMessage } from '../../src/changelog/analyzer.js';
import type { DepUpdaterConfig } from '../../src/config.js';
import type { Logger } from '../../src/logger.js';
import type { PackageUpdate } from '../../src/types.js';

function createMockLogger(): Logger {
  return {
    info: mock(() => {}),
    warn: mock(() => {}),
    error: mock(() => {}),
    debug: mock(() => {}),
  };
}

describe('Changelog Analyzer', () => {
  const mockConfig: DepUpdaterConfig = {
    expo: { enabled: false, autoDetect: true, projects: [] },
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

  beforeEach(() => {
    // Clear environment variables to ensure fallback behavior
    delete process.env.ANTHROPIC_API_KEY;
    delete process.env.OPENAI_API_KEY;
    delete process.env.GOOGLE_API_KEY;
  });

  afterEach(() => {
    delete process.env.ANTHROPIC_API_KEY;
    delete process.env.OPENAI_API_KEY;
    delete process.env.GOOGLE_API_KEY;
  });

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
      expect(body).toContain('**react: 19.1.0 → 20.0.0**');
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

  describe('analyzeChangelogs', () => {
    const updates: PackageUpdate[] = [
      {
        name: 'react',
        fromVersion: '18.0.0',
        toVersion: '19.0.0',
        updateType: 'major',
        ecosystem: 'npm',
        changelogUrl: 'https://github.com/facebook/react/releases',
      },
    ];
    const changelogs = new Map([['react', '## 19.0.0\n- Breaking: New rendering model']]);

    // Note: Tests that require mocking sendPrompt are in a separate file
    // to avoid Bun's module mock persistence issues. These tests only cover
    // fallback scenarios that don't make network calls.

    describe('fallback behavior (no API key)', () => {
      test('falls back to fallback summary when paid provider has no API key', async () => {
        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'anthropic' }, // No API key
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(updates, changelogs, config);

        // Should return fallback summary (not make AI call)
        expect(result).toContain('## Dependency Updates');
        expect(result).toContain('react: 18.0.0 → 19.0.0');
        // Should warn about missing key
        expect(config.logger?.warn).toHaveBeenCalledWith(
          expect.stringContaining("No API key found for provider 'anthropic'"),
        );
      });

      test('falls back when openai provider has no API key', async () => {
        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'openai' },
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(updates, changelogs, config);

        expect(result).toContain('## Dependency Updates');
        expect(config.logger?.warn).toHaveBeenCalledWith(
          expect.stringContaining("No API key found for provider 'openai'"),
        );
      });

      test('falls back when google provider has no API key', async () => {
        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'google' },
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(updates, changelogs, config);

        expect(result).toContain('## Dependency Updates');
        expect(config.logger?.warn).toHaveBeenCalledWith(
          expect.stringContaining("No API key found for provider 'google'"),
        );
      });
    });

    describe('fallback summary formatting', () => {
      test('includes changelog URLs in fallback summary', async () => {
        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'anthropic' }, // No API key = fallback
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(updates, changelogs, config);

        expect(result).toContain('[changelog](https://github.com/facebook/react/releases)');
      });

      test('groups updates by type in fallback summary', async () => {
        const mixedUpdates: PackageUpdate[] = [
          { name: 'react', fromVersion: '18.0.0', toVersion: '19.0.0', updateType: 'major', ecosystem: 'npm' },
          { name: 'vite', fromVersion: '5.0.0', toVersion: '5.1.0', updateType: 'minor', ecosystem: 'npm' },
          { name: 'lodash', fromVersion: '4.17.20', toVersion: '4.17.21', updateType: 'patch', ecosystem: 'npm' },
        ];

        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'anthropic' }, // No API key = fallback
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(mixedUpdates, new Map(), config);

        expect(result).toContain('### ! Major Updates');
        expect(result).toContain('### Minor Updates');
        expect(result).toContain('### Patch Updates');
        expect(result).toContain('Total updates: 3');
      });

      test('includes downgrades section in fallback summary', async () => {
        const downgrades: PackageUpdate[] = [
          { name: 'python3', fromVersion: '3.13.0', toVersion: '3.12.0', updateType: 'unknown', ecosystem: 'nix' },
        ];

        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'anthropic' }, // No API key = fallback
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(updates, changelogs, config, downgrades);

        expect(result).toContain('### i Downgrades & Removals');
        expect(result).toContain('python3: 3.13.0 → 3.12.0');
      });

      test('includes ecosystem labels for non-npm packages', async () => {
        const nixUpdates: PackageUpdate[] = [
          { name: 'nodejs', fromVersion: '20.0.0', toVersion: '22.0.0', updateType: 'unknown', ecosystem: 'nix' },
        ];

        const config: DepUpdaterConfig = {
          ...mockConfig,
          ai: { provider: 'anthropic' }, // No API key = fallback
          logger: createMockLogger(),
        };

        const result = await analyzeChangelogs(nixUpdates, new Map(), config);

        expect(result).toContain('(nix)');
      });
    });
  });
});
