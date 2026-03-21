import { describe, expect, test } from 'bun:test';
import type { PatchnoteConfig } from '../src/config.js';
import { defaultConfig } from '../src/config.js';
import { getRecentCommitMessages } from '../src/git.js';
import { CONVENTIONAL_COMMIT_REGEX, detectSemanticCommits, resolveSemanticPrefix } from '../src/semantic.js';
import type { PackageUpdate } from '../src/types.js';
import { createMockExeca } from './helpers/mock-execa.js';

describe('Semantic Commits', () => {
  describe('CONVENTIONAL_COMMIT_REGEX', () => {
    test('matches feat commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('feat: add new feature')).toBe(true);
    });

    test('matches fix commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('fix: resolve issue')).toBe(true);
    });

    test('matches chore commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('chore: update deps')).toBe(true);
    });

    test('matches docs commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('docs: update readme')).toBe(true);
    });

    test('matches style commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('style: fix formatting')).toBe(true);
    });

    test('matches refactor commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('refactor: simplify logic')).toBe(true);
    });

    test('matches perf commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('perf: optimize query')).toBe(true);
    });

    test('matches test commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('test: add unit tests')).toBe(true);
    });

    test('matches build commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('build: update config')).toBe(true);
    });

    test('matches ci commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('ci: update pipeline')).toBe(true);
    });

    test('matches revert commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('revert: undo change')).toBe(true);
    });

    test('matches scoped commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('feat(auth): add login')).toBe(true);
    });

    test('matches breaking change indicator', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('feat!: breaking change')).toBe(true);
    });

    test('does not match non-conventional commits', () => {
      expect(CONVENTIONAL_COMMIT_REGEX.test('update dependencies')).toBe(false);
      expect(CONVENTIONAL_COMMIT_REGEX.test('Fix something')).toBe(false);
      expect(CONVENTIONAL_COMMIT_REGEX.test('WIP: work in progress')).toBe(false);
    });
  });

  describe('detectSemanticCommits', () => {
    test('returns false for empty array', () => {
      expect(detectSemanticCommits([])).toBe(false);
    });

    test('returns true when >50% of commits are conventional', () => {
      const commits = [
        'feat: add feature',
        'fix: resolve bug',
        'chore: update deps',
        'some random commit',
        'another random commit',
        'docs: add docs',
      ];
      // 4 out of 6 = 66% > 50%
      expect(detectSemanticCommits(commits)).toBe(true);
    });

    test('returns false when <=50% of commits are conventional', () => {
      const commits = ['feat: add feature', 'random commit 1', 'random commit 2', 'random commit 3'];
      // 1 out of 4 = 25% <= 50%
      expect(detectSemanticCommits(commits)).toBe(false);
    });

    test('returns true when exactly >50% (threshold boundary)', () => {
      const commits = [
        'feat: add feature',
        'fix: resolve bug',
        'chore: update deps',
        'random commit 1',
        'random commit 2',
      ];
      // 3 out of 5 = 60% > 50%
      expect(detectSemanticCommits(commits)).toBe(true);
    });

    test('returns false when exactly 50%', () => {
      const commits = ['feat: add feature', 'fix: resolve bug', 'random commit 1', 'random commit 2'];
      // 2 out of 4 = 50% -- not strictly >50%
      expect(detectSemanticCommits(commits)).toBe(false);
    });

    test('handles scoped commits', () => {
      const commits = ['feat(auth): add login', 'fix(api): handle error', 'chore(deps): update packages'];
      expect(detectSemanticCommits(commits)).toBe(true);
    });
  });

  describe('getRecentCommitMessages', () => {
    test('returns last N commit messages from HEAD', async () => {
      const mockExeca = createMockExeca({
        'git log --format=%s -n 10': 'feat: add feature\nfix: resolve bug\nchore: update deps\n',
      });

      const messages = await getRecentCommitMessages('/repo', 10, undefined, mockExeca);

      expect(messages).toEqual(['feat: add feature', 'fix: resolve bug', 'chore: update deps']);
    });

    test('returns last N commit messages from specified branch', async () => {
      const mockExeca = createMockExeca({
        'git log --format=%s -n 10 main': 'feat: first\nfix: second\n',
      });

      const messages = await getRecentCommitMessages('/repo', 10, 'main', mockExeca);

      expect(messages).toEqual(['feat: first', 'fix: second']);
    });

    test('filters empty lines', async () => {
      const mockExeca = createMockExeca({
        'git log --format=%s -n 5': 'feat: one\n\nfix: two\n\n',
      });

      const messages = await getRecentCommitMessages('/repo', 5, undefined, mockExeca);

      expect(messages).toEqual(['feat: one', 'fix: two']);
    });
  });

  describe('resolveSemanticPrefix', () => {
    const baseConfig: PatchnoteConfig = {
      ...defaultConfig,
      semanticCommits: {
        enabled: true,
        prefix: 'chore(deps)',
        devPrefix: 'chore(dev-deps)',
      },
    };

    test('returns configured prefix when enabled=true', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: true,
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
      };

      const result = await resolveSemanticPrefix(config, '/repo', []);
      expect(result).toBe('chore(deps)');
    });

    test('returns null when enabled=false', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: false,
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
      };

      const result = await resolveSemanticPrefix(config, '/repo', []);
      expect(result).toBeNull();
    });

    test('returns prefix when auto mode detects conventional repo', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: 'auto',
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
        git: { remote: 'origin', baseBranch: 'main' },
      };

      // 8 out of 10 = 80% conventional
      const mockExeca = createMockExeca({
        'git log --format=%s -n 10 main':
          'feat: one\nfix: two\nchore: three\ndocs: four\nstyle: five\nrefactor: six\nperf: seven\ntest: eight\nrandom one\nrandom two\n',
      });

      const result = await resolveSemanticPrefix(config, '/repo', [], mockExeca);
      expect(result).toBe('chore(deps)');
    });

    test('returns null when auto mode detects non-conventional repo', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: 'auto',
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
        git: { remote: 'origin', baseBranch: 'main' },
      };

      // 2 out of 10 = 20% conventional
      const mockExeca = createMockExeca({
        'git log --format=%s -n 10 main':
          'feat: one\nfix: two\nrandom 1\nrandom 2\nrandom 3\nrandom 4\nrandom 5\nrandom 6\nrandom 7\nrandom 8\n',
      });

      const result = await resolveSemanticPrefix(config, '/repo', [], mockExeca);
      expect(result).toBeNull();
    });

    test('returns devPrefix when all updates have isDev=true', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: true,
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
      };

      const updates: PackageUpdate[] = [
        {
          name: 'vitest',
          fromVersion: '1.0.0',
          toVersion: '2.0.0',
          updateType: 'major',
          ecosystem: 'npm',
          isDev: true,
        },
        {
          name: '@types/node',
          fromVersion: '20.0.0',
          toVersion: '22.0.0',
          updateType: 'major',
          ecosystem: 'npm',
          isDev: true,
        },
      ];

      const result = await resolveSemanticPrefix(config, '/repo', updates);
      expect(result).toBe('chore(dev-deps)');
    });

    test('returns prefix when updates are mixed dev and prod', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: true,
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
      };

      const updates: PackageUpdate[] = [
        {
          name: 'react',
          fromVersion: '18.0.0',
          toVersion: '19.0.0',
          updateType: 'major',
          ecosystem: 'npm',
          isDev: false,
        },
        {
          name: 'vitest',
          fromVersion: '1.0.0',
          toVersion: '2.0.0',
          updateType: 'major',
          ecosystem: 'npm',
          isDev: true,
        },
      ];

      const result = await resolveSemanticPrefix(config, '/repo', updates);
      expect(result).toBe('chore(deps)');
    });

    test('returns prefix when updates are all prod', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: true,
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
      };

      const updates: PackageUpdate[] = [
        {
          name: 'react',
          fromVersion: '18.0.0',
          toVersion: '19.0.0',
          updateType: 'major',
          ecosystem: 'npm',
          isDev: false,
        },
      ];

      const result = await resolveSemanticPrefix(config, '/repo', updates);
      expect(result).toBe('chore(deps)');
    });

    test('returns prefix when updates have no isDev set (undefined)', async () => {
      const config: PatchnoteConfig = {
        ...baseConfig,
        semanticCommits: {
          enabled: true,
          prefix: 'chore(deps)',
          devPrefix: 'chore(dev-deps)',
        },
      };

      const updates: PackageUpdate[] = [
        { name: 'react', fromVersion: '18.0.0', toVersion: '19.0.0', updateType: 'major', ecosystem: 'npm' },
      ];

      const result = await resolveSemanticPrefix(config, '/repo', updates);
      expect(result).toBe('chore(deps)');
    });

    test('uses defaultConfig values when semanticCommits is undefined', async () => {
      const config: PatchnoteConfig = {
        ...defaultConfig,
      };

      // auto mode (default) with conventional repo
      const mockExeca = createMockExeca({
        'git log --format=%s -n 10 main':
          'feat: one\nfix: two\nchore: three\ndocs: four\nstyle: five\nrefactor: six\nperf: seven\ntest: eight\nrandom one\nrandom two\n',
      });

      const result = await resolveSemanticPrefix(config, '/repo', [], mockExeca);
      expect(result).toBe('chore(deps)');
    });
  });
});
