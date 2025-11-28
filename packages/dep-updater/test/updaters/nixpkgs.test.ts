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

  describe('updateNixpkgsOverlay - without nvfetcher', () => {
    // Note: These tests run without nvfetcher available, so nvfetcher update fails.
    // They test that the function handles this gracefully by returning success: false.

    test('fails without nvfetcher available', async () => {
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

      // nvfetcher is not available in test environment
      expect(result.success).toBe(false);
      expect(result.ecosystem).toBe('nixpkgs');
      expect(result.error).toBeDefined();
    });

    test('logs info message before attempting update', async () => {
      const sources = JSON.stringify({ bun: { version: '1.1.30' } });
      await writeFile(join(sourcesDir, 'generated.json'), sources);

      await updateNixpkgsOverlay(testDir, { dryRun: true, logger: mockLogger });

      expect(mockLogger.info).toHaveBeenCalledWith('Updating nixpkgs overlay...');
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

    test('returns error when nvfetcher not available', async () => {
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

      // nvfetcher is required and not available in test environment
      expect(result.success).toBe(false);
      expect(result.error).toContain('nvfetcher not available');
    });
  });
});
