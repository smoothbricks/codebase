/**
 * Tests for git modification functions (state-changing operations)
 */

import { describe, expect, test } from 'bun:test';
import {
  commit,
  createBranch,
  fetch,
  push,
  pushWithUpstream,
  stageAll,
  stageFiles,
  switchBranch,
} from '../../src/git.js';
import { createErrorExeca, createExecaSpy } from '../helpers/mock-execa.js';

describe('createBranch', () => {
  test('should create branch without base', async () => {
    const spy = createExecaSpy({
      'git checkout -b feature': '',
    });

    await createBranch('/repo', 'feature', undefined, spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['checkout', '-b', 'feature'], { cwd: '/repo' }]);
  });

  test('should create branch from base branch', async () => {
    const spy = createExecaSpy({
      'git checkout -b feature main': '',
    });

    await createBranch('/repo', 'feature', 'main', spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['checkout', '-b', 'feature', 'main'], { cwd: '/repo' }]);
  });

  test('should handle branch names with slashes', async () => {
    const spy = createExecaSpy({
      'git checkout -b feature/add-tests': '',
    });

    await createBranch('/repo', 'feature/add-tests', undefined, spy.mock);

    expect(spy.calls[0][1]).toEqual(['checkout', '-b', 'feature/add-tests']);
  });

  test('should throw when branch already exists', async () => {
    const mockExeca = createErrorExeca("fatal: A branch named 'feature' already exists");

    await expect(createBranch('/repo', 'feature', undefined, mockExeca)).rejects.toThrow();
  });

  test('should throw when base branch does not exist', async () => {
    const mockExeca = createErrorExeca("fatal: 'nonexistent' is not a commit");

    await expect(createBranch('/repo', 'feature', 'nonexistent', mockExeca)).rejects.toThrow();
  });
});

describe('switchBranch', () => {
  test('should switch to branch', async () => {
    const spy = createExecaSpy({
      'git checkout main': '',
    });

    await switchBranch('/repo', 'main', spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['checkout', 'main'], { cwd: '/repo' }]);
  });

  test('should handle branch names with slashes', async () => {
    const spy = createExecaSpy({
      'git checkout feature/test': '',
    });

    await switchBranch('/repo', 'feature/test', spy.mock);

    expect(spy.calls[0][1]).toEqual(['checkout', 'feature/test']);
  });

  test('should throw when branch does not exist', async () => {
    const mockExeca = createErrorExeca("error: pathspec 'nonexistent' did not match");

    await expect(switchBranch('/repo', 'nonexistent', mockExeca)).rejects.toThrow();
  });
});

describe('stageFiles', () => {
  test('should stage single file', async () => {
    const spy = createExecaSpy({
      'git add src/git.ts': '',
    });

    await stageFiles('/repo', ['src/git.ts'], spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['add', 'src/git.ts'], { cwd: '/repo' }]);
  });

  test('should stage multiple files', async () => {
    const spy = createExecaSpy({
      'git add src/git.ts src/cli.ts README.md': '',
    });

    await stageFiles('/repo', ['src/git.ts', 'src/cli.ts', 'README.md'], spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0][1]).toEqual(['add', 'src/git.ts', 'src/cli.ts', 'README.md']);
  });

  test('should handle files with spaces', async () => {
    const spy = createExecaSpy({
      'git add file with spaces.ts': '',
    });

    await stageFiles('/repo', ['file with spaces.ts'], spy.mock);

    expect(spy.calls[0][1]).toContain('file with spaces.ts');
  });

  test('should throw when file does not exist', async () => {
    const mockExeca = createErrorExeca("fatal: pathspec 'nonexistent.ts' did not match");

    await expect(stageFiles('/repo', ['nonexistent.ts'], mockExeca)).rejects.toThrow();
  });
});

describe('stageAll', () => {
  test('should stage all changes', async () => {
    const spy = createExecaSpy({
      'git add -A': '',
    });

    await stageAll('/repo', spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['add', '-A'], { cwd: '/repo' }]);
  });
});

describe('commit', () => {
  test('should create commit with message only', async () => {
    const spy = createExecaSpy({
      'git commit -m test commit': '',
    });

    await commit('/repo', 'test commit', undefined, spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0][1]).toEqual(['commit', '-m', 'test commit']);
  });

  test('should create commit with message and body', async () => {
    const spy = createExecaSpy({
      'git commit -m feat: add feature\n\nDetailed description here': '',
    });

    await commit('/repo', 'feat: add feature', 'Detailed description here', spy.mock);

    expect(spy.calls).toHaveLength(1);
    const commitMessage = spy.calls[0]?.[1]?.[2];
    expect(commitMessage).toContain('feat: add feature');
    expect(commitMessage).toContain('Detailed description here');
    expect(commitMessage).toContain('\n\n'); // Double newline separator
  });

  test('should handle multi-line commit bodies', async () => {
    const body = 'Line 1\nLine 2\nLine 3';
    const spy = createExecaSpy({
      [`git commit -m fix: bug\n\n${body}`]: '',
    });

    await commit('/repo', 'fix: bug', body, spy.mock);

    const commitMessage = spy.calls[0]?.[1]?.[2];
    expect(commitMessage).toBe(`fix: bug\n\n${body}`);
  });

  test('should handle commit messages with quotes', async () => {
    const spy = createExecaSpy({
      'git commit -m fix: handle "quoted" strings': '',
    });

    await commit('/repo', 'fix: handle "quoted" strings', undefined, spy.mock);

    expect(spy.calls[0]?.[1]?.[2]).toContain('"quoted"');
  });

  test('should throw when nothing staged', async () => {
    const mockExeca = createErrorExeca('nothing to commit');

    await expect(commit('/repo', 'test', undefined, mockExeca)).rejects.toThrow();
  });
});

describe('push', () => {
  test('should push without force', async () => {
    const spy = createExecaSpy({
      'git push origin main': '',
    });

    await push('/repo', 'origin', 'main', false, spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['push', 'origin', 'main'], { cwd: '/repo' }]);
  });

  test('should push with force flag', async () => {
    const spy = createExecaSpy({
      'git push origin main --force': '',
    });

    await push('/repo', 'origin', 'main', true, spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0][1]).toEqual(['push', 'origin', 'main', '--force']);
  });

  test('should handle different remote names', async () => {
    const spy = createExecaSpy({
      'git push upstream feature': '',
    });

    await push('/repo', 'upstream', 'feature', false, spy.mock);

    expect(spy.calls[0][1]).toEqual(['push', 'upstream', 'feature']);
  });

  test('should throw on network error', async () => {
    const mockExeca = createErrorExeca('fatal: Could not read from remote repository');

    await expect(push('/repo', 'origin', 'main', false, mockExeca)).rejects.toThrow();
  });

  test('should throw when push rejected', async () => {
    const mockExeca = createErrorExeca('error: failed to push some refs');

    await expect(push('/repo', 'origin', 'main', false, mockExeca)).rejects.toThrow();
  });

  test('should throw when remote does not exist', async () => {
    const mockExeca = createErrorExeca("fatal: 'nonexistent' does not appear to be a git repository");

    await expect(push('/repo', 'nonexistent', 'main', false, mockExeca)).rejects.toThrow();
  });
});

describe('pushWithUpstream', () => {
  test('should push with upstream tracking', async () => {
    const spy = createExecaSpy({
      'git push -u origin feature': '',
    });

    await pushWithUpstream('/repo', 'origin', 'feature', spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['push', '-u', 'origin', 'feature'], { cwd: '/repo' }]);
  });

  test('should handle different remote names', async () => {
    const spy = createExecaSpy({
      'git push -u upstream main': '',
    });

    await pushWithUpstream('/repo', 'upstream', 'main', spy.mock);

    expect(spy.calls[0][1]).toEqual(['push', '-u', 'upstream', 'main']);
  });

  test('should throw on network error', async () => {
    const mockExeca = createErrorExeca('fatal: Could not read from remote repository');

    await expect(pushWithUpstream('/repo', 'origin', 'main', mockExeca)).rejects.toThrow();
  });
});

describe('fetch', () => {
  test('should fetch from remote', async () => {
    const spy = createExecaSpy({
      'git fetch origin': '',
    });

    await fetch('/repo', 'origin', spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]).toEqual(['git', ['fetch', 'origin'], { cwd: '/repo' }]);
  });

  test('should handle different remote names', async () => {
    const spy = createExecaSpy({
      'git fetch upstream': '',
    });

    await fetch('/repo', 'upstream', spy.mock);

    expect(spy.calls[0][1]).toEqual(['fetch', 'upstream']);
  });

  test('should throw on network error', async () => {
    const mockExeca = createErrorExeca('fatal: Could not read from remote repository');

    await expect(fetch('/repo', 'origin', mockExeca)).rejects.toThrow();
  });

  test('should throw when remote does not exist', async () => {
    const mockExeca = createErrorExeca("fatal: 'nonexistent' does not appear to be a git repository");

    await expect(fetch('/repo', 'nonexistent', mockExeca)).rejects.toThrow();
  });
});
