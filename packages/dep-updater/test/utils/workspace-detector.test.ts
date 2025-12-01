import { describe, expect, test } from 'bun:test';
import { resolve } from 'node:path';
import { detectWorkspaceScopes, generateWorkspacePrefixes } from '../../src/utils/workspace-detector.js';

describe('Workspace Detector', () => {
  describe('detectWorkspaceScopes', () => {
    test('returns empty array for non-monorepo', async () => {
      // Test with dep-updater itself (not a workspace root)
      const repoRoot = resolve(__dirname, '..');
      const scopes = await detectWorkspaceScopes(repoRoot);

      // dep-updater package.json has no workspaces
      expect(scopes).toEqual([]);
    });
  });

  describe('generateWorkspacePrefixes', () => {
    test('converts scopes to glob patterns', () => {
      const scopes = ['@company', '@example'];
      const prefixes = generateWorkspacePrefixes(scopes);

      expect(prefixes).toEqual(['@company/*', '@example/*']);
    });

    test('handles single scope', () => {
      const scopes = ['@test'];
      const prefixes = generateWorkspacePrefixes(scopes);

      expect(prefixes).toEqual(['@test/*']);
    });

    test('handles empty array', () => {
      const scopes: string[] = [];
      const prefixes = generateWorkspacePrefixes(scopes);

      expect(prefixes).toEqual([]);
    });

    test('preserves scope order', () => {
      const scopes = ['@zebra', '@alpha', '@beta'];
      const prefixes = generateWorkspacePrefixes(scopes);

      expect(prefixes).toEqual(['@zebra/*', '@alpha/*', '@beta/*']);
    });
  });
});
