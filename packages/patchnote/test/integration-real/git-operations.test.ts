/**
 * Integration tests: git operations that have non-trivial parsing logic.
 * Tests getChangedFiles porcelain parsing and getRecentCommitMessages filtering
 * against real git repos with various file states.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { execa } from 'execa';
import { commit, getChangedFiles, getRecentCommitMessages, stageAll } from '../../src/git.js';
import { createTestRepo, type TestRepo } from './helpers/test-repo.js';

describe('getChangedFiles - porcelain parsing', () => {
  let repo: TestRepo;

  beforeEach(async () => {
    repo = await createTestRepo('patchnote-test-minimal');
  });

  afterEach(async () => {
    await repo.cleanup();
  });

  test('detects new, modified, and deleted files simultaneously', async () => {
    // Create mixed git state: new file + modify existing + delete existing
    await writeFile(join(repo.path, 'brand-new.txt'), 'new content');
    await writeFile(join(repo.path, 'package.json'), '{ "name": "modified" }');
    // Stage a file then delete it to create a staged+deleted state
    await writeFile(join(repo.path, 'to-delete.txt'), 'temporary');
    await execa('git', ['add', 'to-delete.txt'], { cwd: repo.path });
    await execa('git', ['commit', '-m', 'add to-delete'], { cwd: repo.path });
    await rm(join(repo.path, 'to-delete.txt'));

    const changed = await getChangedFiles(repo.path);

    expect(changed).toContain('brand-new.txt');
    expect(changed).toContain('package.json');
    expect(changed).toContain('to-delete.txt');
    // Filenames should be clean (no status prefixes like "M " or "?? ")
    for (const file of changed) {
      expect(file).not.toMatch(/^[A-Z?]{1,2}\s/);
    }
  });

  test('untracked subdirectories appear as directory entries in porcelain output', async () => {
    // Git porcelain reports untracked directories as just the dir name, not individual files
    await mkdir(join(repo.path, 'src', 'utils'), { recursive: true });
    await writeFile(join(repo.path, 'src', 'utils', 'deep-file.ts'), 'export const x = 1');
    await writeFile(join(repo.path, 'src', 'index.ts'), 'export {}');

    const changed = await getChangedFiles(repo.path);

    // Untracked: git reports the top-level untracked dir, not individual files
    expect(changed).toContain('src/');
  });

  test('staged subdirectory files appear as full paths', async () => {
    // Once staged, git reports individual files with full relative paths
    await mkdir(join(repo.path, 'src', 'utils'), { recursive: true });
    await writeFile(join(repo.path, 'src', 'utils', 'deep-file.ts'), 'export const x = 1');
    await writeFile(join(repo.path, 'src', 'index.ts'), 'export {}');
    await execa('git', ['add', 'src/'], { cwd: repo.path });

    const changed = await getChangedFiles(repo.path);

    expect(changed).toContain('src/utils/deep-file.ts');
    expect(changed).toContain('src/index.ts');
  });

  test('filenames with spaces are quoted by git porcelain', async () => {
    // Git porcelain wraps filenames with special chars in double quotes
    await writeFile(join(repo.path, 'file with spaces.txt'), 'content');

    const changed = await getChangedFiles(repo.path);

    // The parser preserves git's quoting -- callers must handle this
    expect(changed).toContain('"file with spaces.txt"');
  });

  test('returns empty array on clean repo', async () => {
    const changed = await getChangedFiles(repo.path);
    expect(changed).toEqual([]);
  });

  test('detects staged but uncommitted files', async () => {
    await writeFile(join(repo.path, 'staged.txt'), 'staged content');
    await execa('git', ['add', 'staged.txt'], { cwd: repo.path });

    const changed = await getChangedFiles(repo.path);

    expect(changed).toContain('staged.txt');
  });
});

describe('getRecentCommitMessages - filtering', () => {
  let repo: TestRepo;

  beforeEach(async () => {
    repo = await createTestRepo('patchnote-test-minimal');
  });

  afterEach(async () => {
    await repo.cleanup();
  });

  test('returns messages in reverse chronological order', async () => {
    await execa('git', ['commit', '--allow-empty', '-m', 'first'], { cwd: repo.path });
    await execa('git', ['commit', '--allow-empty', '-m', 'second'], { cwd: repo.path });
    await execa('git', ['commit', '--allow-empty', '-m', 'third'], { cwd: repo.path });

    const messages = await getRecentCommitMessages(repo.path, 3);

    expect(messages[0]).toBe('third');
    expect(messages[1]).toBe('second');
    expect(messages[2]).toBe('first');
  });

  test('respects the count limit', async () => {
    for (let i = 0; i < 10; i++) {
      await execa('git', ['commit', '--allow-empty', '-m', `commit-${i}`], { cwd: repo.path });
    }

    const messages = await getRecentCommitMessages(repo.path, 3);

    expect(messages).toHaveLength(3);
    expect(messages[0]).toBe('commit-9');
  });

  test('filters out empty lines from git log output', async () => {
    // Multi-line commit message -- git log can produce blank lines
    await execa('git', ['commit', '--allow-empty', '-m', 'feat: real message'], { cwd: repo.path });

    const messages = await getRecentCommitMessages(repo.path, 5);

    for (const msg of messages) {
      expect(msg.length).toBeGreaterThan(0);
    }
  });
});

describe('stageAll + commit pipeline', () => {
  let repo: TestRepo;

  beforeEach(async () => {
    repo = await createTestRepo('patchnote-test-minimal');
  });

  afterEach(async () => {
    await repo.cleanup();
  });

  test('commit with body produces correct git log output', async () => {
    await writeFile(join(repo.path, 'update.txt'), 'change');
    await stageAll(repo.path);
    await commit(repo.path, 'chore: update deps', 'Updated lodash 4.17.0 → 4.17.21');

    // Verify the commit message has both title and body
    const { stdout: title } = await execa('git', ['log', '-1', '--format=%s'], { cwd: repo.path });
    const { stdout: body } = await execa('git', ['log', '-1', '--format=%b'], { cwd: repo.path });

    expect(title).toBe('chore: update deps');
    expect(body.trim()).toBe('Updated lodash 4.17.0 → 4.17.21');
  });
});
