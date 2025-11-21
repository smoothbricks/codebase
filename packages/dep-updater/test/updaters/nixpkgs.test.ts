import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Logger } from '../../src/logger.js';
import { updateNixpkgsOverlay } from '../../src/updaters/nixpkgs.js';

describe('Nixpkgs Overlay Updater', () => {
  const testDir = '/tmp/dep-updater-test-nixpkgs';
  const sourcesDir = join(testDir, '_sources');
  let mockLogger: Logger;

  beforeEach(async () => {
    // Clean up and create test directory
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
    await mkdir(sourcesDir, { recursive: true });

    mockLogger = {
      info: mock(() => {}),
      warn: mock(() => {}),
      error: mock(() => {}),
      debug: mock(() => {}),
    };
  });

  afterEach(async () => {
    // Clean up
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
  });

  describe('updateNixpkgsOverlay - dry-run mode', () => {
    test('supports dry-run mode with valid sources file', async () => {
      const sources = JSON.stringify({
        bun: {
          version: '1.1.30',
          src: {
            type: 'url',
            url: 'https://github.com/oven-sh/bun/releases/download/bun-v1.1.30/bun-linux-x64.zip',
            sha256: 'sha256-abc123',
          },
        },
      });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.ecosystem).toBe('nixpkgs');
      expect(result.updates).toBeInstanceOf(Array);
    });

    test('handles missing sources file in dry-run', async () => {
      // No sources file created

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('handles malformed JSON in dry-run', async () => {
      const sources = 'invalid json{';
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      // Should handle parse error gracefully
      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('logs info messages in dry-run', async () => {
      const sources = JSON.stringify({ bun: { version: '1.1.30' } });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(mockLogger.info).toHaveBeenCalledWith('Updating nixpkgs overlay...');
      expect(mockLogger.info).toHaveBeenCalledWith(expect.stringContaining('Found'));
    });
  });

  describe('updateNixpkgsOverlay - error handling', () => {
    test('handles non-existent directory', async () => {
      const nonExistentDir = '/tmp/dep-updater-test-nonexistent-dir-9999';

      const result = await updateNixpkgsOverlay(nonExistentDir, { dryRun: false, logger: mockLogger });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.ecosystem).toBe('nixpkgs');
    });
  });

  describe('updateNixpkgsOverlay - version parsing', () => {
    test('extracts version from JSON', async () => {
      const sources = JSON.stringify({
        bun: {
          version: '1.1.30',
          src: {
            type: 'url',
            url: 'https://example.com/bun-1.1.30.zip',
            sha256: 'sha256-abc123',
          },
        },
      });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      // In dry-run, before & after are the same, so no updates
      expect(result.updates).toHaveLength(0);
    });

    test('handles sources file without version field', async () => {
      const sources = JSON.stringify({
        bun: {
          src: {
            type: 'url',
            url: 'https://example.com/bun.zip',
            sha256: 'sha256-abc123',
          },
        },
      });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('handles multiple packages', async () => {
      const sources = JSON.stringify({
        bun: { version: '1.1.30' },
        another: { version: '2.0.0' },
      });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      // Parser extracts all packages, but in dry-run before & after are the same
      expect(result.updates).toHaveLength(0);
    });

    test('handles empty sources file', async () => {
      await writeFile(join(sourcesDir, 'generated.json'), '');

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('handles empty JSON object', async () => {
      const sources = JSON.stringify({});
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('extracts version from complex source structure', async () => {
      const sources = JSON.stringify({
        bun: {
          version: '1.1.30',
          src: {
            type: 'url',
            url: 'https://github.com/oven-sh/bun/releases/download/bun-v1.1.30/bun-linux-x64.zip',
            sha256: 'sha256-very-long-hash-here',
          },
          meta: {
            description: 'Fast all-in-one JavaScript runtime',
          },
        },
      });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      const result = await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });
  });
});
