import { describe, expect, test } from 'bun:test';
import { GitHubCLIClient } from '../../src/auth/github-client.js';
import { createExecaSpy, createMockExeca } from '../helpers/mock-execa.js';

describe('GitHubCLIClient', () => {
  describe('listUpdatePRs', () => {
    test('should parse gh CLI JSON output correctly', async () => {
      const mockPRs = [
        {
          number: 123,
          title: 'chore: update dependencies',
          headRefName: 'chore/update-deps-2024-01-15',
          createdAt: '2024-01-15T10:00:00Z',
          url: 'https://github.com/owner/repo/pull/123',
        },
        {
          number: 124,
          title: 'feat: add new feature',
          headRefName: 'feat/new-feature',
          createdAt: '2024-01-16T10:00:00Z',
          url: 'https://github.com/owner/repo/pull/124',
        },
      ];

      const mockExeca = createMockExeca({
        'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify(mockPRs),
      });

      const client = new GitHubCLIClient(mockExeca);
      const prs = await client.listUpdatePRs('/repo');

      expect(prs).toEqual(mockPRs);
      expect(prs).toHaveLength(2);
      expect(prs[0]?.number).toBe(123);
      expect(prs[0]?.headRefName).toBe('chore/update-deps-2024-01-15');
    });

    test('should return empty array when no PRs exist', async () => {
      const mockExeca = createMockExeca({
        'gh pr list --json number,title,headRefName,createdAt,url --state open': '[]',
      });

      const client = new GitHubCLIClient(mockExeca);
      const prs = await client.listUpdatePRs('/repo');

      expect(prs).toEqual([]);
    });

    test('should use correct gh CLI command', async () => {
      const spy = createExecaSpy({
        'gh pr list --json number,title,headRefName,createdAt,url --state open': '[]',
      });

      const client = new GitHubCLIClient(spy.mock);
      await client.listUpdatePRs('/test-repo');

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0]?.[0]).toBe('gh');
      expect(spy.calls[0]?.[1]).toEqual([
        'pr',
        'list',
        '--json',
        'number,title,headRefName,createdAt,url',
        '--state',
        'open',
      ]);
      expect(spy.calls[0]?.[2]).toEqual({ cwd: '/test-repo' });
    });

    test('should throw on malformed JSON', async () => {
      const mockExeca = createMockExeca({
        'gh pr list --json number,title,headRefName,createdAt,url --state open': 'not valid json{',
      });

      const client = new GitHubCLIClient(mockExeca);
      await expect(client.listUpdatePRs('/repo')).rejects.toThrow();
    });

    test('should throw on non-array JSON response', async () => {
      const mockExeca = createMockExeca({
        'gh pr list --json number,title,headRefName,createdAt,url --state open': JSON.stringify({
          error: 'authentication required',
        }),
      });

      const client = new GitHubCLIClient(mockExeca);
      await expect(client.listUpdatePRs('/repo')).rejects.toThrow('Expected array');
    });
  });

  describe('checkPRConflicts', () => {
    test('should return true for CONFLICTING status', async () => {
      const mockExeca = createMockExeca({
        'gh pr view 123 --json mergeable': JSON.stringify({ mergeable: 'CONFLICTING' }),
      });

      const client = new GitHubCLIClient(mockExeca);
      const hasConflicts = await client.checkPRConflicts('/repo', 123);

      expect(hasConflicts).toBe(true);
    });

    test('should return false for MERGEABLE status', async () => {
      const mockExeca = createMockExeca({
        'gh pr view 456 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      });

      const client = new GitHubCLIClient(mockExeca);
      const hasConflicts = await client.checkPRConflicts('/repo', 456);

      expect(hasConflicts).toBe(false);
    });

    test('should return false for UNKNOWN status', async () => {
      const mockExeca = createMockExeca({
        'gh pr view 789 --json mergeable': JSON.stringify({ mergeable: 'UNKNOWN' }),
      });

      const client = new GitHubCLIClient(mockExeca);
      const hasConflicts = await client.checkPRConflicts('/repo', 789);

      expect(hasConflicts).toBe(false);
    });

    test('should use correct gh CLI command', async () => {
      const spy = createExecaSpy({
        'gh pr view 999 --json mergeable': JSON.stringify({ mergeable: 'MERGEABLE' }),
      });

      const client = new GitHubCLIClient(spy.mock);
      await client.checkPRConflicts('/test-repo', 999);

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0]?.[0]).toBe('gh');
      expect(spy.calls[0]?.[1]).toEqual(['pr', 'view', '999', '--json', 'mergeable']);
      expect(spy.calls[0]?.[2]).toEqual({ cwd: '/test-repo' });
    });

    test('should throw on malformed JSON', async () => {
      const mockExeca = createMockExeca({
        'gh pr view 123 --json mergeable': 'not valid json{',
      });

      const client = new GitHubCLIClient(mockExeca);
      await expect(client.checkPRConflicts('/repo', 123)).rejects.toThrow();
    });

    test('should throw on JSON without mergeable field', async () => {
      const mockExeca = createMockExeca({
        'gh pr view 123 --json mergeable': JSON.stringify({ error: 'not found' }),
      });

      const client = new GitHubCLIClient(mockExeca);
      await expect(client.checkPRConflicts('/repo', 123)).rejects.toThrow('Expected object with mergeable field');
    });
  });

  describe('createPR', () => {
    test('should extract PR number from URL', async () => {
      const mockExeca = createMockExeca({
        'gh pr create --title Update dependencies --body PR description --base main --head update-deps':
          'https://github.com/owner/repo/pull/123\n',
      });

      const client = new GitHubCLIClient(mockExeca);
      const result = await client.createPR('/repo', {
        title: 'Update dependencies',
        body: 'PR description',
        base: 'main',
        head: 'update-deps',
      });

      expect(result.number).toBe(123);
      expect(result.url).toBe('https://github.com/owner/repo/pull/123');
    });

    test('should handle URL without trailing newline', async () => {
      const mockExeca = createMockExeca({
        'gh pr create --title Test PR --body Body text --base main --head feature':
          'https://github.com/owner/repo/pull/456',
      });

      const client = new GitHubCLIClient(mockExeca);
      const result = await client.createPR('/repo', {
        title: 'Test PR',
        body: 'Body text',
        base: 'main',
        head: 'feature',
      });

      expect(result.number).toBe(456);
      expect(result.url).toBe('https://github.com/owner/repo/pull/456');
    });

    test('should throw if URL format is invalid', async () => {
      const mockExeca = createMockExeca({
        'gh pr create --title Test --body Body --base main --head branch': 'invalid-url-format',
      });

      const client = new GitHubCLIClient(mockExeca);

      await expect(
        client.createPR('/repo', {
          title: 'Test',
          body: 'Body',
          base: 'main',
          head: 'branch',
        }),
      ).rejects.toThrow('Expected GitHub PR URL');
    });

    test('should use correct gh CLI command with all options', async () => {
      const spy = createExecaSpy({
        'gh pr create --title My PR Title --body My PR Body --base develop --head my-feature-branch':
          'https://github.com/owner/repo/pull/789\n',
      });

      const client = new GitHubCLIClient(spy.mock);
      await client.createPR('/test-repo', {
        title: 'My PR Title',
        body: 'My PR Body',
        base: 'develop',
        head: 'my-feature-branch',
      });

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0]?.[0]).toBe('gh');
      expect(spy.calls[0]?.[1]).toEqual([
        'pr',
        'create',
        '--title',
        'My PR Title',
        '--body',
        'My PR Body',
        '--base',
        'develop',
        '--head',
        'my-feature-branch',
      ]);
      expect(spy.calls[0]?.[2]).toEqual({ cwd: '/test-repo' });
    });
  });

  describe('closePR', () => {
    test('should call gh pr close with correct arguments', async () => {
      const spy = createExecaSpy({
        'gh pr close 123 --comment Closing this PR': '',
      });

      const client = new GitHubCLIClient(spy.mock);
      await client.closePR('/repo', 123, 'Closing this PR');

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0]?.[0]).toBe('gh');
      expect(spy.calls[0]?.[1]).toEqual(['pr', 'close', '123', '--comment', 'Closing this PR']);
      expect(spy.calls[0]?.[2]).toEqual({ cwd: '/repo' });
    });

    test('should handle PR number as string correctly', async () => {
      const spy = createExecaSpy({
        'gh pr close 999 --comment Superseded by newer PR': '',
      });

      const client = new GitHubCLIClient(spy.mock);
      await client.closePR('/test-repo', 999, 'Superseded by newer PR');

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0]?.[1]?.[1]).toBe('close');
      expect(spy.calls[0]?.[1]?.[2]).toBe('999'); // Should be string
    });

    test('should not return anything', async () => {
      const mockExeca = createMockExeca({
        'gh pr close 456 --comment Test comment': '',
      });

      const client = new GitHubCLIClient(mockExeca);
      const result = await client.closePR('/repo', 456, 'Test comment');

      expect(result).toBeUndefined();
    });
  });

  describe('constructor', () => {
    test('should use provided executor', async () => {
      const spy = createExecaSpy({
        'gh pr list --json number,title,headRefName,createdAt,url --state open': '[]',
      });

      const client = new GitHubCLIClient(spy.mock);
      await client.listUpdatePRs('/repo');

      expect(spy.calls).toHaveLength(1);
    });

    test('should use default executor (execa) when not provided', () => {
      const client = new GitHubCLIClient();

      // Should not throw when constructed without executor
      expect(client).toBeDefined();
    });
  });
});
