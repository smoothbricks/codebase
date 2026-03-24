import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import {
  detectWorkspaceScopes,
  generateWorkspacePrefixes,
  getWorkspacePatterns,
  parsePnpmWorkspaceYaml,
} from '../../src/utils/workspace-detector.js';

describe('Workspace Detector', () => {
  describe('detectWorkspaceScopes', () => {
    test('returns empty array for non-monorepo', async () => {
      // Test with patchnote itself (not a workspace root)
      const repoRoot = resolve(__dirname, '..');
      const scopes = await detectWorkspaceScopes(repoRoot);

      // patchnote package.json has no workspaces
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

  describe('getWorkspacePatterns', () => {
    let tmpDir: string;

    afterEach(async () => {
      if (tmpDir) {
        await rm(tmpDir, { recursive: true, force: true });
      }
    });

    test('reads pnpm-workspace.yaml when it exists', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'pnpm-workspace.yaml'),
        `packages:
  - 'packages/*'
  - 'apps/*'
`,
      );
      await writeFile(join(tmpDir, 'package.json'), '{}');

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'apps/*']);
    });

    test('filters out exclusion patterns from pnpm-workspace.yaml', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'pnpm-workspace.yaml'),
        `packages:
  - 'packages/*'
  - '!**/test/**'
  - 'apps/*'
`,
      );
      await writeFile(join(tmpDir, 'package.json'), '{}');

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'apps/*']);
    });

    test('reads package.json workspaces (array format) when no pnpm-workspace.yaml', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'package.json'),
        JSON.stringify({
          workspaces: ['packages/*', 'libs/*'],
        }),
      );

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'libs/*']);
    });

    test('reads package.json workspaces (object format with .packages) when no pnpm-workspace.yaml', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'package.json'),
        JSON.stringify({
          workspaces: {
            packages: ['packages/*', 'tools/*'],
          },
        }),
      );

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'tools/*']);
    });

    test('returns empty array when neither source has workspace config', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(join(tmpDir, 'package.json'), JSON.stringify({ name: 'no-workspaces' }));

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual([]);
    });

    test('returns empty array for empty directory (no package.json)', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual([]);
    });

    test('reads pnpm-workspace.yaml with inline array format', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(join(tmpDir, 'pnpm-workspace.yaml'), "packages: ['packages/*', 'apps/*']\n");
      await writeFile(join(tmpDir, 'package.json'), '{}');

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'apps/*']);
    });

    test('reads pnpm-workspace.yaml with comments on lines', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'pnpm-workspace.yaml'),
        `packages:
  - 'packages/*' # main packages
  - 'apps/*' # application packages
`,
      );
      await writeFile(join(tmpDir, 'package.json'), '{}');

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'apps/*']);
    });

    test('reads pnpm-workspace.yaml with unquoted patterns', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'pnpm-workspace.yaml'),
        `packages:
  - packages/*
  - apps/*
`,
      );
      await writeFile(join(tmpDir, 'package.json'), '{}');

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'apps/*']);
    });

    test('reads pnpm-workspace.yaml with double-quoted patterns', async () => {
      tmpDir = await mkdtemp(join(tmpdir(), 'ws-test-'));
      await writeFile(
        join(tmpDir, 'pnpm-workspace.yaml'),
        `packages:
  - "packages/*"
  - "apps/*"
`,
      );
      await writeFile(join(tmpDir, 'package.json'), '{}');

      const patterns = await getWorkspacePatterns(tmpDir);

      expect(patterns).toEqual(['packages/*', 'apps/*']);
    });
  });

  describe('parsePnpmWorkspaceYaml', () => {
    test('parses block list format', () => {
      const result = parsePnpmWorkspaceYaml(`packages:
  - 'packages/*'
  - 'apps/*'
`);
      expect(result).toEqual(['packages/*', 'apps/*']);
    });

    test('parses inline array format', () => {
      const result = parsePnpmWorkspaceYaml("packages: ['packages/*', 'apps/*']");
      expect(result).toEqual(['packages/*', 'apps/*']);
    });

    test('parses inline array with double quotes', () => {
      const result = parsePnpmWorkspaceYaml('packages: ["packages/*", "apps/*"]');
      expect(result).toEqual(['packages/*', 'apps/*']);
    });

    test('strips inline comments', () => {
      const result = parsePnpmWorkspaceYaml(`packages:
  - 'packages/*' # main packages
  - 'apps/*' # app packages
`);
      expect(result).toEqual(['packages/*', 'apps/*']);
    });

    test('filters exclusion patterns', () => {
      const result = parsePnpmWorkspaceYaml(`packages:
  - 'packages/*'
  - '!**/test/**'
  - 'apps/*'
`);
      expect(result).toEqual(['packages/*', 'apps/*']);
    });

    test('handles empty packages key', () => {
      const result = parsePnpmWorkspaceYaml('packages:\n');
      expect(result).toEqual([]);
    });

    test('handles no packages key', () => {
      const result = parsePnpmWorkspaceYaml('catalog:\n  foo: 1.0.0\n');
      expect(result).toEqual([]);
    });

    test('stops block list at next top-level key', () => {
      const result = parsePnpmWorkspaceYaml(`packages:
  - 'packages/*'
catalog:
  foo: 1.0.0
`);
      expect(result).toEqual(['packages/*']);
    });

    test('handles inline array exclusion patterns', () => {
      const result = parsePnpmWorkspaceYaml("packages: ['packages/*', '!**/test/**', 'apps/*']");
      expect(result).toEqual(['packages/*', 'apps/*']);
    });
  });
});
