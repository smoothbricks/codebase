/**
 * Tests for git workflow functions (high-level orchestration)
 */

import { describe, expect, test } from 'bun:test';
import { createUpdateBranch, createUpdateCommit } from '../../src/git.js';
import { createErrorExeca, createExecaSpy } from '../helpers/mock-execa.js';

describe('createUpdateCommit', () => {
  test('should stage all and commit with message', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/repo\n',
      'git add -A': '',
      'git commit -m chore: update dependencies': '',
    });

    await createUpdateCommit({}, 'chore: update dependencies', undefined, spy.mock);

    expect(spy.calls).toHaveLength(3);
    expect(spy.getCallsFor('git')).toHaveLength(3);
    // First: getRepoRoot
    expect(spy.calls[0]?.[1]).toEqual(['rev-parse', '--show-toplevel']);
    // Second: stageAll
    expect(spy.calls[1]?.[1]).toEqual(['add', '-A']);
    // Third: commit
    expect(spy.calls[2]?.[1]?.[0]).toBe('commit');
    expect(spy.calls[2]?.[1]?.[2]).toBe('chore: update dependencies');
  });

  test('should stage all and commit with message and body', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/repo\n',
      'git add -A': '',
      'git commit -m feat: add feature\n\nDetailed description': '',
    });

    await createUpdateCommit({}, 'feat: add feature', 'Detailed description', spy.mock);

    const commitMessage = spy.calls[2]?.[1]?.[2];
    expect(commitMessage).toContain('feat: add feature');
    expect(commitMessage).toContain('\n\n');
    expect(commitMessage).toContain('Detailed description');
  });

  test('should use repoRoot from config if present', async () => {
    const spy = createExecaSpy({
      'git add -A': '',
      'git commit -m test': '',
    });

    await createUpdateCommit({ repoRoot: '/custom/repo' }, 'test', undefined, spy.mock);

    // Should NOT call getRepoRoot
    expect(spy.getCallsFor('git').filter((c) => c[1]?.[0] === 'rev-parse')).toHaveLength(0);
    // Should use /custom/repo as cwd
    expect(spy.calls[0]?.[2]?.cwd).toBe('/custom/repo');
    expect(spy.calls[1]?.[2]?.cwd).toBe('/custom/repo');
  });

  test('should call getRepoRoot if repoRoot not in config', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/detected/repo\n',
      'git add -A': '',
      'git commit -m test': '',
    });

    await createUpdateCommit({}, 'test', undefined, spy.mock);

    // Should call getRepoRoot first
    expect(spy.calls[0]?.[1]).toEqual(['rev-parse', '--show-toplevel']);
    // Should use detected repo as cwd
    expect(spy.calls[1]?.[2]?.cwd).toBe('/detected/repo');
    expect(spy.calls[2]?.[2]?.cwd).toBe('/detected/repo');
  });

  test('should propagate stageAll errors', async () => {
    const mockExeca = createErrorExeca('fatal: unable to stage');

    await expect(createUpdateCommit({ repoRoot: '/repo' }, 'test', undefined, mockExeca)).rejects.toThrow();
  });

  test('should propagate commit errors', async () => {
    const calls: string[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');
      calls.push(key);
      if (key === 'git add -A') {
        return { stdout: '', stderr: '', exitCode: 0 };
      }
      if (key.startsWith('git commit')) {
        throw new Error('nothing to commit');
      }
      throw new Error(`Unexpected: ${key}`);
    };

    await expect(createUpdateCommit({ repoRoot: '/repo' }, 'test', undefined, mockExeca)).rejects.toThrow(
      'nothing to commit',
    );
  });
});

describe('createUpdateBranch', () => {
  test('should create branch and push', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/repo\n',
      'git checkout -b feature/update': '',
      'git push -u origin feature/update': '',
    });

    await createUpdateBranch({}, 'feature/update', undefined, spy.mock);

    expect(spy.calls).toHaveLength(3);
    // First: getRepoRoot
    expect(spy.calls[0][1]).toEqual(['rev-parse', '--show-toplevel']);
    // Second: createBranch
    expect(spy.calls[1][1]).toEqual(['checkout', '-b', 'feature/update']);
    // Third: pushWithUpstream
    expect(spy.calls[2][1]).toEqual(['push', '-u', 'origin', 'feature/update']);
  });

  test('should use config remote (default: origin)', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/repo\n',
      'git checkout -b feature': '',
      'git push -u origin feature': '',
    });

    await createUpdateBranch({}, 'feature', undefined, spy.mock);

    expect(spy.calls[2][1]).toEqual(['push', '-u', 'origin', 'feature']);
  });

  test('should use custom remote from config', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/repo\n',
      'git checkout -b feature': '',
      'git push -u upstream feature': '',
    });

    await createUpdateBranch({ git: { remote: 'upstream', baseBranch: 'main' } }, 'feature', undefined, spy.mock);

    expect(spy.calls[2][1]).toEqual(['push', '-u', 'upstream', 'feature']);
  });

  test('should support custom baseBranch', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/repo\n',
      'git checkout -b feature develop': '',
      'git push -u origin feature': '',
    });

    await createUpdateBranch({}, 'feature', 'develop', spy.mock);

    expect(spy.calls[1][1]).toEqual(['checkout', '-b', 'feature', 'develop']);
  });

  test('should use repoRoot from config if present', async () => {
    const spy = createExecaSpy({
      'git checkout -b feature': '',
      'git push -u origin feature': '',
    });

    await createUpdateBranch({ repoRoot: '/custom/repo' }, 'feature', undefined, spy.mock);

    // Should NOT call getRepoRoot
    expect(spy.getCallsFor('git').filter((c) => c[1]?.[0] === 'rev-parse')).toHaveLength(0);
    // Should use /custom/repo as cwd
    expect(spy.calls[0]?.[2]?.cwd).toBe('/custom/repo');
    expect(spy.calls[1]?.[2]?.cwd).toBe('/custom/repo');
  });

  test('should call getRepoRoot if repoRoot not in config', async () => {
    const spy = createExecaSpy({
      'git rev-parse --show-toplevel': '/detected/repo\n',
      'git checkout -b feature': '',
      'git push -u origin feature': '',
    });

    await createUpdateBranch({}, 'feature', undefined, spy.mock);

    // Should call getRepoRoot first
    expect(spy.calls[0]?.[1]).toEqual(['rev-parse', '--show-toplevel']);
    // Should use detected repo as cwd
    expect(spy.calls[1]?.[2]?.cwd).toBe('/detected/repo');
    expect(spy.calls[2]?.[2]?.cwd).toBe('/detected/repo');
  });

  test('should propagate createBranch errors', async () => {
    const calls: string[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');
      calls.push(key);
      if (key === 'git rev-parse --show-toplevel') {
        return { stdout: '/repo\n', stderr: '', exitCode: 0 };
      }
      if (key.startsWith('git checkout')) {
        throw new Error("fatal: A branch named 'feature' already exists");
      }
      throw new Error(`Unexpected: ${key}`);
    };

    await expect(createUpdateBranch({}, 'feature', undefined, mockExeca)).rejects.toThrow('already exists');
  });

  test('should propagate push errors', async () => {
    const calls: string[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');
      calls.push(key);
      if (key === 'git rev-parse --show-toplevel') {
        return { stdout: '/repo\n', stderr: '', exitCode: 0 };
      }
      if (key.startsWith('git checkout')) {
        return { stdout: '', stderr: '', exitCode: 0 };
      }
      if (key.startsWith('git push')) {
        throw new Error('fatal: Could not read from remote repository');
      }
      throw new Error(`Unexpected: ${key}`);
    };

    await expect(createUpdateBranch({}, 'feature', undefined, mockExeca)).rejects.toThrow('Could not read from remote');
  });
});
