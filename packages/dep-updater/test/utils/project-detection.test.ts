import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { detectProjectSetup } from '../../src/utils/project-detection.js';

describe('project-detection', () => {
  const testDir = '/tmp/dep-updater-test-project-detection';

  beforeEach(async () => {
    // Clean up and create test directory
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
    await mkdir(testDir, { recursive: true });
  });

  afterEach(async () => {
    // Clean up
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
  });

  describe('detectProjectSetup', () => {
    test('should detect Expo project', async () => {
      const packageJson = {
        dependencies: {
          expo: '^52.0.0',
          react: '^18.0.0',
        },
      };

      await writeFile(join(testDir, 'package.json'), JSON.stringify(packageJson, null, 2));
      await writeFile(join(testDir, 'bun.lock'), ''); // Bun lock file

      const result = await detectProjectSetup(testDir);

      expect(result.hasExpo).toBe(true);
      expect(result.packageManager).toBe('bun');
    });

    test('should detect Expo in devDependencies', async () => {
      const packageJson = {
        devDependencies: {
          expo: '^52.0.0',
        },
      };

      await writeFile(join(testDir, 'package.json'), JSON.stringify(packageJson, null, 2));
      await writeFile(join(testDir, 'bun.lockb'), ''); // Bun lock file

      const result = await detectProjectSetup(testDir);

      expect(result.hasExpo).toBe(true);
    });

    test('should detect package manager: bun (bun.lock)', async () => {
      await writeFile(join(testDir, 'package.json'), JSON.stringify({}, null, 2));
      await writeFile(join(testDir, 'bun.lock'), '');

      const result = await detectProjectSetup(testDir);

      expect(result.packageManager).toBe('bun');
    });

    test('should detect package manager: bun (bun.lockb)', async () => {
      await writeFile(join(testDir, 'package.json'), JSON.stringify({}, null, 2));
      await writeFile(join(testDir, 'bun.lockb'), '');

      const result = await detectProjectSetup(testDir);

      expect(result.packageManager).toBe('bun');
    });

    test('should detect package manager: pnpm', async () => {
      await writeFile(join(testDir, 'package.json'), JSON.stringify({}, null, 2));
      await writeFile(join(testDir, 'pnpm-lock.yaml'), '');

      const result = await detectProjectSetup(testDir);

      expect(result.packageManager).toBe('pnpm');
    });

    test('should detect package manager: yarn', async () => {
      await writeFile(join(testDir, 'package.json'), JSON.stringify({}, null, 2));
      await writeFile(join(testDir, 'yarn.lock'), '');

      const result = await detectProjectSetup(testDir);

      expect(result.packageManager).toBe('yarn');
    });

    test('should default to npm when no lock file found', async () => {
      await writeFile(join(testDir, 'package.json'), JSON.stringify({}, null, 2));

      const result = await detectProjectSetup(testDir);

      expect(result.packageManager).toBe('npm');
    });

    test('should detect Nix from flake.nix', async () => {
      await writeFile(join(testDir, 'flake.nix'), '# Nix flake');

      const result = await detectProjectSetup(testDir);

      expect(result.hasNix).toBe(true);
    });

    test('should detect Nix from .envrc', async () => {
      await writeFile(join(testDir, '.envrc'), 'use flake');

      const result = await detectProjectSetup(testDir);

      expect(result.hasNix).toBe(true);
    });

    test('should detect syncpack from .syncpackrc.json', async () => {
      await writeFile(join(testDir, '.syncpackrc.json'), '{}');

      const result = await detectProjectSetup(testDir);

      expect(result.hasSyncpack).toBe(true);
    });

    test('should detect syncpack from .syncpackrc.yml', async () => {
      await writeFile(join(testDir, '.syncpackrc.yml'), '---');

      const result = await detectProjectSetup(testDir);

      expect(result.hasSyncpack).toBe(true);
    });

    test('should handle missing package.json', async () => {
      // No files created

      const result = await detectProjectSetup(testDir);

      expect(result.hasExpo).toBe(false);
      expect(result.hasNix).toBe(false);
      expect(result.hasSyncpack).toBe(false);
      expect(result.packageManager).toBe('bun'); // Default
    });

    test('should handle invalid package.json', async () => {
      await writeFile(join(testDir, 'package.json'), 'invalid json{');

      const result = await detectProjectSetup(testDir);

      expect(result.hasExpo).toBe(false);
      expect(result.packageManager).toBe('bun'); // Default on error
    });

    test('should detect all features in a full setup', async () => {
      const packageJson = {
        dependencies: {
          expo: '^52.0.0',
        },
      };

      await writeFile(join(testDir, 'package.json'), JSON.stringify(packageJson, null, 2));
      await writeFile(join(testDir, 'bun.lock'), '');
      await writeFile(join(testDir, 'flake.nix'), '# Nix flake');
      await writeFile(join(testDir, '.syncpackrc.json'), '{}');

      const result = await detectProjectSetup(testDir);

      expect(result.hasExpo).toBe(true);
      expect(result.hasNix).toBe(true);
      expect(result.hasSyncpack).toBe(true);
      expect(result.packageManager).toBe('bun');
    });

    test('should detect Nix from nested devenv directory', async () => {
      await mkdir(join(testDir, 'tooling', 'devenv'), { recursive: true });
      await writeFile(join(testDir, 'tooling', 'devenv', 'flake.nix'), '# Nix flake');

      const result = await detectProjectSetup(testDir);

      expect(result.hasNix).toBe(true);
    });

    test('should detect Nix from devenv.yaml anywhere', async () => {
      await mkdir(join(testDir, 'tooling', 'direnv'), { recursive: true });
      await writeFile(join(testDir, 'tooling', 'direnv', 'devenv.yaml'), 'inputs: {}');

      const result = await detectProjectSetup(testDir);

      expect(result.hasNix).toBe(true);
    });

    test('should detect Nix from .envrc in subdirectory', async () => {
      await mkdir(join(testDir, 'nix'), { recursive: true });
      await writeFile(join(testDir, 'nix', '.envrc'), 'use flake');

      const result = await detectProjectSetup(testDir);

      expect(result.hasNix).toBe(true);
    });

    test('should detect untracked Nix files (not yet committed)', async () => {
      // This tests the key advantage of fast-glob over git ls-files
      // Create files but don't commit them to git
      await mkdir(join(testDir, 'tooling', 'devenv'), { recursive: true });
      await writeFile(join(testDir, 'tooling', 'devenv', 'devenv.yaml'), 'inputs: {}');

      const result = await detectProjectSetup(testDir);

      // Should detect even though not committed (unlike git ls-files)
      expect(result.hasNix).toBe(true);
    });
  });
});
