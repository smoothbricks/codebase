import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Logger } from '../../src/logger.js';
import { updateDevenv } from '../../src/updaters/devenv.js';

describe('Devenv Updater', () => {
  const testDir = '/tmp/dep-updater-test-devenv';
  let mockLogger: Logger;

  beforeEach(async () => {
    // Clean up and create test directory
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
    await mkdir(testDir, { recursive: true });

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

  describe('updateDevenv - dry-run mode', () => {
    test('supports dry-run mode with valid lock file', async () => {
      const lock = {
        nodes: {
          devenv: {
            locked: {
              lastModified: 1234567890,
              narHash: 'sha256-abc123',
              rev: 'abc1234567890def',
            },
            original: {
              owner: 'cachix',
              repo: 'devenv',
              type: 'github',
            },
          },
        },
      };

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      const result = await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.ecosystem).toBe('nix');
      expect(result.updates).toBeInstanceOf(Array);
    });

    test('handles missing lock file in dry-run', async () => {
      // No lock file created

      const result = await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('handles invalid JSON in dry-run', async () => {
      await writeFile(join(testDir, 'devenv.lock'), 'invalid json{');

      const result = await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      // Should handle parse error gracefully
      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('logs info messages in dry-run', async () => {
      const lock = {
        nodes: {
          devenv: {
            locked: { rev: 'abc1234567890def' },
          },
        },
      };

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      expect(mockLogger.info).toHaveBeenCalledWith('Updating devenv dependencies...');
      expect(mockLogger.info).toHaveBeenCalledWith(expect.stringContaining('Found'));
    });
  });

  describe('updateDevenv - error handling', () => {
    test('handles non-existent directory', async () => {
      const nonExistentDir = '/tmp/dep-updater-test-nonexistent-dir-9999';

      const result = await updateDevenv(nonExistentDir, { dryRun: false, logger: mockLogger });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.ecosystem).toBe('nix');
    });
  });

  describe('updateDevenv - lock file parsing', () => {
    test('extracts short commit hashes from lock file', async () => {
      const lock = {
        nodes: {
          devenv: {
            locked: { rev: 'abcdef1234567890' },
          },
          nixpkgs: {
            locked: { rev: '1234567890abcdef' },
          },
        },
      };

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      const result = await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      // In dry-run, it compares before & after which are the same, so no updates
      expect(result.updates).toHaveLength(0);
    });

    test('handles lock file without nodes', async () => {
      const lock = {};

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      const result = await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });

    test('handles nodes without rev field', async () => {
      const lock = {
        nodes: {
          devenv: {
            locked: {
              lastModified: 1234567890,
              // No rev field
            },
          },
        },
      };

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      const result = await updateDevenv(testDir, { dryRun: true, logger: mockLogger });

      expect(result.success).toBe(true);
      expect(result.updates).toHaveLength(0);
    });
  });
});
