/**
 * Tests for PR query functions (read-only operations)
 */

import { describe, expect, test } from 'bun:test';
import { checkPRConflicts, getOpenUpdatePRs } from '../../src/pr/stacking.js';
import { createErrorExeca, createExecaSpy, createMockExeca } from '../helpers/mock-execa.js';

describe('checkPRConflicts', () => {
  test('should return true for conflicting PR', async () => {
    const mockExeca = createMockExeca({
      'gh pr view 123 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
    });

    const hasConflicts = await checkPRConflicts('/repo', 123, mockExeca);

    expect(hasConflicts).toBe(true);
  });

  test('should return false for mergeable PR', async () => {
    const mockExeca = createMockExeca({
      'gh pr view 456 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
    });

    const hasConflicts = await checkPRConflicts('/repo', 456, mockExeca);

    expect(hasConflicts).toBe(false);
  });

  test('should return false for unknown mergeable state', async () => {
    const mockExeca = createMockExeca({
      'gh pr view 789 --json mergeable': JSON.stringify({ mergeable: 'UNKNOWN' }),
    });

    const hasConflicts = await checkPRConflicts('/repo', 789, mockExeca);

    expect(hasConflicts).toBe(false);
  });

  test('should call gh with correct arguments', async () => {
    const spy = createExecaSpy({
      'gh pr view 100 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
    });

    await checkPRConflicts('/repo', 100, spy.mock);

    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0]?.[0]).toBe('gh');
    expect(spy.calls[0]?.[1]).toEqual(['pr', 'view', '100', '--json', 'mergeable']);
    expect(spy.calls[0]?.[2]?.cwd).toBe('/repo');
  });

  test('should return false on error', async () => {
    const mockExeca = createErrorExeca('gh: command not found');

    const hasConflicts = await checkPRConflicts('/repo', 999, mockExeca);

    expect(hasConflicts).toBe(false);
  });
});

describe('getOpenUpdatePRs', () => {
  test('should return empty array when no PRs exist', async () => {
    const mockExeca = createMockExeca({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify([]),
    });

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toEqual([]);
  });

  test('should filter PRs by branch prefix', async () => {
    const allPRs = [
      {
        number: 1,
        title: 'Update deps',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'Other PR',
        headRefName: 'feat/new-feature',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
      {
        number: 3,
        title: 'Another update',
        headRefName: 'chore/update-deps-2025-01-03',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }
      if (key === 'gh pr view 1 --json mergeable') {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      if (key === 'gh pr view 3 --json mergeable') {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toHaveLength(2);
    expect(prs[0]?.number).toBe(1);
    expect(prs[1]?.number).toBe(3);
  });

  test('should sort PRs by creation date (oldest first)', async () => {
    const allPRs = [
      {
        number: 3,
        title: 'Third',
        headRefName: 'chore/update-deps-3',
        createdAt: '2025-01-03T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/3',
      },
      {
        number: 1,
        title: 'First',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'Second',
        headRefName: 'chore/update-deps-2',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }
      if (key.startsWith('gh pr view')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toHaveLength(3);
    expect(prs[0]?.number).toBe(1); // Oldest first
    expect(prs[1]?.number).toBe(2);
    expect(prs[2]?.number).toBe(3);
  });

  test('should check conflicts for each PR', async () => {
    const allPRs = [
      {
        number: 1,
        title: 'PR 1',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
      {
        number: 2,
        title: 'PR 2',
        headRefName: 'chore/update-deps-2',
        createdAt: '2025-01-02T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/2',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }
      if (key === 'gh pr view 1 --json mergeable') {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      if (key === 'gh pr view 2 --json mergeable') {
        return { stdout: JSON.stringify({ mergeable: 'CONFLICTING' }), stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toHaveLength(2);
    expect(prs[0]?.hasConflicts).toBe(false);
    expect(prs[1]?.hasConflicts).toBe(true);
  });

  test('should handle date parsing', async () => {
    const allPRs = [
      {
        number: 1,
        title: 'PR',
        headRefName: 'chore/update-deps-1',
        createdAt: '2025-01-15T14:30:00Z',
        url: 'https://github.com/owner/repo/pull/1',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }
      if (key.startsWith('gh pr view')) {
        return { stdout: JSON.stringify({ mergeable: 'MERGEABLE' }), stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toHaveLength(1);
    expect(prs[0]?.createdAt).toBeInstanceOf(Date);
    expect(prs[0]?.createdAt.toISOString()).toBe('2025-01-15T14:30:00.000Z');
  });

  test('should return empty array on error', async () => {
    const mockExeca = createErrorExeca('gh: authentication required');

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toEqual([]);
  });

  test('should return empty array for non-array response', async () => {
    const mockExeca = createMockExeca({
      'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify({
        error: 'invalid',
      }),
    });

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toEqual([]);
  });

  test('should include all PR fields', async () => {
    const allPRs = [
      {
        number: 42,
        title: 'Update dependencies',
        headRefName: 'chore/update-deps-2025-01-01',
        createdAt: '2025-01-01T10:00:00Z',
        url: 'https://github.com/owner/repo/pull/42',
      },
    ];

    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh pr list --json number,title,headRefName,createdAt,url --state open') {
        return { stdout: JSON.stringify(allPRs), stderr: '', exitCode: 0 };
      }
      if (key.startsWith('gh pr view')) {
        return { stdout: JSON.stringify({ mergeable: 'CONFLICTING' }), stderr: '', exitCode: 0 };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const prs = await getOpenUpdatePRs('/repo', 'chore/update-deps', mockExeca);

    expect(prs).toHaveLength(1);
    expect(prs[0]).toEqual({
      number: 42,
      title: 'Update dependencies',
      branch: 'chore/update-deps-2025-01-01',
      createdAt: new Date('2025-01-01T10:00:00Z'),
      hasConflicts: true,
      url: 'https://github.com/owner/repo/pull/42',
    });
  });
});
