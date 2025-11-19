/**
 * Project setup detection utilities
 */

import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

export interface ProjectSetup {
  hasExpo: boolean;
  hasNix: boolean;
  hasSyncpack: boolean;
  packageManager: 'bun' | 'npm' | 'pnpm' | 'yarn';
}

/**
 * Detect project setup by checking for specific files/config
 */
export async function detectProjectSetup(repoRoot: string): Promise<ProjectSetup> {
  const packageJsonPath = join(repoRoot, 'package.json');
  let hasExpo = false;
  let packageManager: 'bun' | 'npm' | 'pnpm' | 'yarn' = 'bun';

  // Check package.json for Expo
  if (existsSync(packageJsonPath)) {
    try {
      const packageJson = JSON.parse(await readFile(packageJsonPath, 'utf-8'));
      hasExpo = Boolean(packageJson.dependencies?.expo || packageJson.devDependencies?.expo);

      // Detect package manager from lock files
      if (existsSync(join(repoRoot, 'bun.lockb')) || existsSync(join(repoRoot, 'bun.lock'))) {
        packageManager = 'bun';
      } else if (existsSync(join(repoRoot, 'pnpm-lock.yaml'))) {
        packageManager = 'pnpm';
      } else if (existsSync(join(repoRoot, 'yarn.lock'))) {
        packageManager = 'yarn';
      } else {
        packageManager = 'npm';
      }
    } catch {
      // Ignore parse errors
    }
  }

  // Check for Nix
  const hasNix = existsSync(join(repoRoot, 'flake.nix')) || existsSync(join(repoRoot, '.envrc'));

  // Check for syncpack
  const hasSyncpack = existsSync(join(repoRoot, '.syncpackrc.json')) || existsSync(join(repoRoot, '.syncpackrc.yml'));

  return { hasExpo, hasNix, hasSyncpack, packageManager };
}
