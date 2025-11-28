import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import type { Logger } from '../../src/logger.js';
import { isVersionDowngrade, parseDixOutput, updateDevenv } from '../../src/updaters/devenv.js';

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

  describe('updateDevenv - without valid devenv setup', () => {
    // Note: These tests run in a directory without devenv.nix, so devenv update fails.
    // They test that the function handles this gracefully by returning success: false

    test('returns failure when devenv.nix is missing', async () => {
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

      const result = await updateDevenv(testDir, {
        dryRun: true,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      // devenv update fails without devenv.nix
      expect(result.success).toBe(false);
      expect(result.ecosystem).toBe('nix');
      expect(result.error).toBeDefined();
    });

    test('handles missing lock file gracefully', async () => {
      // No lock file created - devenv update still fails without devenv.nix

      const result = await updateDevenv(testDir, {
        dryRun: true,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    test('logs info message before attempting update', async () => {
      const lock = {
        nodes: {
          devenv: {
            locked: { rev: 'abc1234567890def' },
          },
        },
      };

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      await updateDevenv(testDir, {
        dryRun: true,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      expect(mockLogger.info).toHaveBeenCalledWith('Updating devenv dependencies...');
    });
  });

  describe('updateDevenv - error handling', () => {
    test('handles non-existent directory', async () => {
      const nonExistentDir = '/tmp/dep-updater-test-nonexistent-dir-9999';

      // Disable derivation diff for this test to avoid slow devenv info calls
      const result = await updateDevenv(nonExistentDir, {
        dryRun: false,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.ecosystem).toBe('nix');
    });
  });

  describe('updateDevenv - lock file parsing (requires devenv)', () => {
    // Note: These tests require a valid devenv setup to pass.
    // Without devenv.nix, devenv update fails, so we test error handling instead.

    test('fails with lock file when devenv.nix is missing', async () => {
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

      const result = await updateDevenv(testDir, {
        dryRun: true,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      // devenv update fails without devenv.nix
      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    test('fails with empty lock file when devenv.nix is missing', async () => {
      const lock = {};

      await writeFile(join(testDir, 'devenv.lock'), JSON.stringify(lock, null, 2));

      const result = await updateDevenv(testDir, {
        dryRun: true,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    test('fails with partial lock file when devenv.nix is missing', async () => {
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

      const result = await updateDevenv(testDir, {
        dryRun: true,
        logger: mockLogger,
        useDerivationDiff: false,
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });
  });
});

describe('isVersionDowngrade', () => {
  test('detects simple version downgrade', () => {
    expect(isVersionDowngrade('3.13.9', '3.13.8')).toBe(true);
    expect(isVersionDowngrade('22.11.0', '22.10.0')).toBe(true);
    expect(isVersionDowngrade('2.0.0', '1.9.9')).toBe(true);
  });

  test('detects upgrade is not downgrade', () => {
    expect(isVersionDowngrade('3.13.8', '3.13.9')).toBe(false);
    expect(isVersionDowngrade('22.10.0', '22.11.0')).toBe(false);
    expect(isVersionDowngrade('1.0.0', '2.0.0')).toBe(false);
  });

  test('handles same version', () => {
    expect(isVersionDowngrade('3.13.9', '3.13.9')).toBe(false);
    expect(isVersionDowngrade('1.0.0', '1.0.0')).toBe(false);
  });

  test('handles special markers', () => {
    expect(isVersionDowngrade('(new)', '1.0.0')).toBe(false);
    expect(isVersionDowngrade('1.0.0', '(removed)')).toBe(false);
  });

  test('handles patch versions like p3', () => {
    expect(isVersionDowngrade('5.3p3', '5.3p2')).toBe(true);
    expect(isVersionDowngrade('5.3p2', '5.3p3')).toBe(false);
  });

  test('handles single number versions', () => {
    expect(isVersionDowngrade('40', '39')).toBe(true);
    expect(isVersionDowngrade('39', '40')).toBe(false);
  });
});

describe('parseDixOutput', () => {
  test('parses upgrades and separates from downgrades', () => {
    const output = `<<< /nix/store/old-profile
>>> /nix/store/new-profile

CHANGED
[D.] python3  3.13.9 -> 3.13.8
[D.] nodejs   22.10.0 -> 22.11.0

SIZE: 1.57 GiB -> 1.56 GiB
DIFF: -17.7 MiB`;

    const result = parseDixOutput(output);

    // nodejs is an upgrade, python3 is a downgrade
    expect(result.updates).toHaveLength(1);
    expect(result.downgrades).toHaveLength(1);
    expect(result.updates[0]).toEqual({
      name: 'nodejs',
      fromVersion: '22.10.0',
      toVersion: '22.11.0',
      updateType: 'unknown',
      ecosystem: 'nix',
    });
    expect(result.downgrades[0]).toEqual({
      name: 'python3',
      fromVersion: '3.13.9',
      toVersion: '3.13.8',
      updateType: 'unknown',
      ecosystem: 'nix',
    });
  });

  test('skips packages with same version (rebuild only)', () => {
    const output = `CHANGED
[D.] bash     5.3p3 ×2 -> 5.3p3
[D.] openssl  3.6.0 ×2 -> 3.6.0`;

    const result = parseDixOutput(output);

    // Same version means rebuild without version change - skip
    expect(result.updates).toHaveLength(0);
    expect(result.downgrades).toHaveLength(0);
  });

  test('parses changed packages with multiple versions', () => {
    const output = `CHANGED
[D.] python3  3.13.8, 3.13.8-env -> 3.13.9, 3.13.9-env`;

    const result = parseDixOutput(output);

    expect(result.updates).toHaveLength(1);
    expect(result.updates[0]).toEqual({
      name: 'python3',
      fromVersion: '3.13.8',
      toVersion: '3.13.9',
      updateType: 'unknown',
      ecosystem: 'nix',
    });
  });

  test('parses added packages as updates', () => {
    const output = `ADDED
[A.] nss-cacert 3.101
[A.] nodejs 22.0.0`;

    const result = parseDixOutput(output);

    expect(result.updates).toHaveLength(2);
    expect(result.downgrades).toHaveLength(0);
    expect(result.updates[0]).toEqual({
      name: 'nss-cacert',
      fromVersion: '(new)',
      toVersion: '3.101',
      updateType: 'unknown',
      ecosystem: 'nix',
    });
  });

  test('parses removed packages as downgrades (informational)', () => {
    const output = `REMOVED
[R.] libffi   40
[R.] oldpkg   1.0.0`;

    const result = parseDixOutput(output);

    expect(result.updates).toHaveLength(0);
    expect(result.downgrades).toHaveLength(2);
    expect(result.downgrades[0]).toEqual({
      name: 'libffi',
      fromVersion: '40',
      toVersion: '(removed)',
      updateType: 'unknown',
      ecosystem: 'nix',
    });
  });

  test('parses mixed output with upgrades, downgrades, added, and removed', () => {
    const output = `<<< /nix/store/old-profile
>>> /nix/store/new-profile

CHANGED
[D.] python3  3.13.9 -> 3.13.8
[D.] nodejs   22.10.0 -> 22.11.0

ADDED
[A.] newpkg  1.0.0

REMOVED
[R.] oldpkg  2.0.0

SIZE: 1.57 GiB -> 1.56 GiB`;

    const result = parseDixOutput(output);

    // Updates: nodejs upgrade + newpkg added
    expect(result.updates).toHaveLength(2);
    expect(result.updates.find((u) => u.name === 'nodejs')).toBeDefined();
    expect(result.updates.find((u) => u.name === 'newpkg')).toBeDefined();

    // Downgrades: python3 downgrade + oldpkg removed
    expect(result.downgrades).toHaveLength(2);
    expect(result.downgrades.find((u) => u.name === 'python3')).toBeDefined();
    expect(result.downgrades.find((u) => u.name === 'oldpkg')).toBeDefined();
  });

  test('returns empty arrays for empty output', () => {
    const result = parseDixOutput('');
    expect(result.updates).toHaveLength(0);
    expect(result.downgrades).toHaveLength(0);
  });

  test('returns empty arrays for output with no packages', () => {
    const output = `<<< /nix/store/old-profile
>>> /nix/store/new-profile

SIZE: 1.57 GiB -> 1.57 GiB
DIFF: 0 B`;

    const result = parseDixOutput(output);
    expect(result.updates).toHaveLength(0);
    expect(result.downgrades).toHaveLength(0);
  });
});
