/**
 * Tests for git query functions (read-only operations)
 */

import { describe, expect, test } from 'bun:test';
import {
  branchExists,
  getChangedFiles,
  getCurrentBranch,
  getDiff,
  getLastCommitMessage,
  getRepoRoot,
  hasConflicts,
  isWorkingDirectoryClean,
} from '../../src/git.js';
import { createErrorExeca, createMockExeca } from '../helpers/mock-execa.js';

describe('getCurrentBranch', () => {
  test('should return branch name', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --abbrev-ref HEAD': 'main\n',
    });

    const branch = await getCurrentBranch('/repo', mockExeca);

    expect(branch).toBe('main');
  });

  test('should trim whitespace', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --abbrev-ref HEAD': '  feature/test  \n',
    });

    const branch = await getCurrentBranch('/repo', mockExeca);

    expect(branch).toBe('feature/test');
  });

  test('should handle branch names with slashes', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --abbrev-ref HEAD': 'feature/add-tests\n',
    });

    const branch = await getCurrentBranch('/repo', mockExeca);

    expect(branch).toBe('feature/add-tests');
  });

  test('should throw when not in git repo', async () => {
    const mockExeca = createErrorExeca('not a git repository');

    await expect(getCurrentBranch('/repo', mockExeca)).rejects.toThrow();
  });
});

describe('isWorkingDirectoryClean', () => {
  test('should return true for clean repo', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': '',
    });

    const isClean = await isWorkingDirectoryClean('/repo', mockExeca);

    expect(isClean).toBe(true);
  });

  test('should return false for dirty repo', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': ' M src/git.ts\n',
    });

    const isClean = await isWorkingDirectoryClean('/repo', mockExeca);

    expect(isClean).toBe(false);
  });

  test('should return false for repo with untracked files', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': '?? new-file.ts\n',
    });

    const isClean = await isWorkingDirectoryClean('/repo', mockExeca);

    expect(isClean).toBe(false);
  });

  test('should handle whitespace-only output as clean', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': '  \n',
    });

    const isClean = await isWorkingDirectoryClean('/repo', mockExeca);

    // Whitespace gets trimmed to empty string, so it's considered clean
    expect(isClean).toBe(true);
  });
});

describe('getChangedFiles', () => {
  test('should return empty array for clean repo', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': '',
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files).toEqual([]);
  });

  test('should parse modified files', async () => {
    // Git status porcelain format: XY filename
    // X = index status, Y = worktree status (both single chars), then space, then filename
    const line1 = ' M src/git.ts'; // space, M, space, filename
    const line2 = ' M src/cli.ts';
    const mockExeca = createMockExeca({
      'git status --porcelain': `${line1}\n${line2}\n`,
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files).toEqual(['src/git.ts', 'src/cli.ts']);
  });

  test('should parse new files', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': '?? new-file.ts\n',
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files).toEqual(['new-file.ts']);
  });

  test('should handle mixed status codes', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': ' M modified.ts\n?? new.ts\n D deleted.ts\n',
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files).toContain('modified.ts');
    expect(files).toContain('new.ts');
    expect(files).toContain('deleted.ts');
    expect(files).toHaveLength(3);
  });

  test('should filter empty lines', async () => {
    const line1 = ' M file.ts';
    const line2 = ' M other.ts';
    const mockExeca = createMockExeca({
      'git status --porcelain': `${line1}\n\n\n${line2}\n`,
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files).toEqual(['file.ts', 'other.ts']);
  });

  test('should strip status codes from filenames', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': 'MM src/complex.ts\n',
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files[0]).toBe('src/complex.ts');
    expect(files[0]).not.toContain('MM');
  });

  test('should handle single file', async () => {
    const mockExeca = createMockExeca({
      'git status --porcelain': ' M single.ts\n',
    });

    const files = await getChangedFiles('/repo', mockExeca);

    expect(files).toEqual(['single.ts']);
  });
});

describe('branchExists', () => {
  test('should return true for existing local branch', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --verify refs/heads/main': '',
    });

    const exists = await branchExists('/repo', 'main', false, mockExeca);

    expect(exists).toBe(true);
  });

  test('should return true for existing remote branch', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --verify refs/remotes/origin/main': '',
    });

    const exists = await branchExists('/repo', 'main', true, mockExeca);

    expect(exists).toBe(true);
  });

  test('should return false for non-existent local branch', async () => {
    const mockExeca = createErrorExeca('fatal: Needed a single revision');

    const exists = await branchExists('/repo', 'nonexistent', false, mockExeca);

    expect(exists).toBe(false);
  });

  test('should return false for non-existent remote branch', async () => {
    const mockExeca = createErrorExeca('fatal: Needed a single revision');

    const exists = await branchExists('/repo', 'nonexistent', true, mockExeca);

    expect(exists).toBe(false);
  });

  test('should handle branch names with slashes', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --verify refs/heads/feature/test': '',
    });

    const exists = await branchExists('/repo', 'feature/test', false, mockExeca);

    expect(exists).toBe(true);
  });
});

describe('getLastCommitMessage', () => {
  test('should return last commit message', async () => {
    const mockExeca = createMockExeca({
      'git log -1 --pretty=%B': 'feat: add new feature\n',
    });

    const message = await getLastCommitMessage('/repo', mockExeca);

    expect(message).toBe('feat: add new feature');
  });

  test('should trim whitespace', async () => {
    const mockExeca = createMockExeca({
      'git log -1 --pretty=%B': '  fix: bug fix  \n\n',
    });

    const message = await getLastCommitMessage('/repo', mockExeca);

    expect(message).toBe('fix: bug fix');
  });

  test('should handle multi-line commit messages', async () => {
    const mockExeca = createMockExeca({
      'git log -1 --pretty=%B': 'feat: add feature\n\nDetailed description\nMore details\n',
    });

    const message = await getLastCommitMessage('/repo', mockExeca);

    expect(message).toContain('feat: add feature');
    expect(message).toContain('Detailed description');
  });

  test('should handle empty commit message', async () => {
    const mockExeca = createMockExeca({
      'git log -1 --pretty=%B': '\n',
    });

    const message = await getLastCommitMessage('/repo', mockExeca);

    expect(message).toBe('');
  });
});

describe('hasConflicts', () => {
  test('should return false when no conflicts', async () => {
    const mockExeca = createMockExeca({
      'git merge-tree main feature': '',
    });

    const conflicts = await hasConflicts('/repo', 'feature', 'main', mockExeca);

    expect(conflicts).toBe(false);
  });

  test('should return true when conflicts exist', async () => {
    const mockExeca = createErrorExeca('Merge conflict');

    const conflicts = await hasConflicts('/repo', 'feature', 'main', mockExeca);

    expect(conflicts).toBe(true);
  });

  test('should return true on merge-tree error', async () => {
    const mockExeca = createErrorExeca('fatal: not a valid object name');

    const conflicts = await hasConflicts('/repo', 'invalid', 'main', mockExeca);

    expect(conflicts).toBe(true);
  });
});

describe('getRepoRoot', () => {
  test('should return absolute path', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': '/home/user/repo\n',
    });

    const root = await getRepoRoot('/home/user/repo', mockExeca);

    expect(root).toBe('/home/user/repo');
  });

  test('should trim whitespace', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': '  /home/user/repo  \n',
    });

    const root = await getRepoRoot('/home/user/repo', mockExeca);

    expect(root).toBe('/home/user/repo');
  });

  test('should accept Unix absolute paths', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': '/usr/local/repo\n',
    });

    const root = await getRepoRoot('/usr/local/repo', mockExeca);

    expect(root).toBe('/usr/local/repo');
  });

  test('should accept Windows absolute paths', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': 'C:\\Users\\user\\repo\n',
    });

    const root = await getRepoRoot('C:\\Users\\user\\repo', mockExeca);

    expect(root).toBe('C:\\Users\\user\\repo');
  });

  test('should accept Windows paths with forward slashes', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': 'C:/Users/user/repo\n',
    });

    const root = await getRepoRoot('C:/Users/user/repo', mockExeca);

    expect(root).toBe('C:/Users/user/repo');
  });

  test('should throw on empty output', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': '',
    });

    await expect(getRepoRoot(undefined, mockExeca)).rejects.toThrow('git returned empty string');
  });

  test('should throw on relative path', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': 'relative/path\n',
    });

    await expect(getRepoRoot(undefined, mockExeca)).rejects.toThrow('Expected absolute path');
  });

  test('should throw on path starting with dot', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': './repo\n',
    });

    await expect(getRepoRoot(undefined, mockExeca)).rejects.toThrow('Expected absolute path');
  });

  test('should throw on path with only whitespace', async () => {
    const mockExeca = createMockExeca({
      'git rev-parse --show-toplevel': '   \n',
    });

    await expect(getRepoRoot(undefined, mockExeca)).rejects.toThrow('git returned empty string');
  });
});

describe('getDiff', () => {
  test('should return diff between branches', async () => {
    const mockExeca = createMockExeca({
      'git diff main feature': 'diff --git a/file.ts b/file.ts\n+added line\n',
    });

    const diff = await getDiff('/repo', 'main', 'feature', undefined, mockExeca);

    expect(diff).toContain('diff --git');
    expect(diff).toContain('+added line');
  });

  test('should handle file filtering', async () => {
    const mockExeca = createMockExeca({
      'git diff main feature -- src/git.ts src/cli.ts': 'diff --git a/src/git.ts\n',
    });

    const diff = await getDiff('/repo', 'main', 'feature', ['src/git.ts', 'src/cli.ts'], mockExeca);

    expect(diff).toContain('diff --git');
  });

  test('should handle empty diff', async () => {
    const mockExeca = createMockExeca({
      'git diff main feature': '',
    });

    const diff = await getDiff('/repo', 'main', 'feature', undefined, mockExeca);

    expect(diff).toBe('');
  });

  test('should work without file filtering', async () => {
    const mockExeca = createMockExeca({
      'git diff v1.0.0 v2.0.0': 'diff content\n',
    });

    const diff = await getDiff('/repo', 'v1.0.0', 'v2.0.0', undefined, mockExeca);

    // getDiff returns raw output including newline
    expect(diff).toBe('diff content\n');
  });

  test('should handle commit SHAs', async () => {
    const mockExeca = createMockExeca({
      'git diff abc123 def456': 'diff content\n',
    });

    const diff = await getDiff('/repo', 'abc123', 'def456', undefined, mockExeca);

    // getDiff returns raw output including newline
    expect(diff).toBe('diff content\n');
  });
});
