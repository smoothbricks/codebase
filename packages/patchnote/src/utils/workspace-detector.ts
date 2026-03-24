/**
 * Auto-detect workspace scopes from package.json, pnpm-workspace.yaml, and find Expo projects
 */

import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { relative, resolve } from 'node:path';
import glob from 'fast-glob';
import type { Logger } from '../logger.js';
import type { ExpoProject } from '../types.js';

interface PackageJson {
  name?: string;
  workspaces?: string[] | { packages?: string[] };
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}

/**
 * Get workspace glob patterns from pnpm-workspace.yaml or package.json workspaces field.
 *
 * Priority:
 * 1. pnpm-workspace.yaml (pnpm ignores package.json workspaces)
 * 2. package.json workspaces (array or object format)
 *
 * @param repoRoot - Repository root directory
 * @returns Array of workspace glob patterns (e.g. ['packages/*', 'apps/*'])
 */
export async function getWorkspacePatterns(repoRoot: string): Promise<string[]> {
  // 1. Check for pnpm-workspace.yaml first
  const pnpmWorkspacePath = resolve(repoRoot, 'pnpm-workspace.yaml');
  if (existsSync(pnpmWorkspacePath)) {
    try {
      const content = await readFile(pnpmWorkspacePath, 'utf-8');
      const patterns: string[] = [];
      let inPackages = false;

      for (const line of content.split('\n')) {
        if (line.match(/^packages:/)) {
          inPackages = true;
          continue;
        }
        if (inPackages && line.match(/^\s+-\s+/)) {
          const pattern = line.replace(/^\s+-\s+['"]?/, '').replace(/['"]?\s*$/, '');
          if (!pattern.startsWith('!')) {
            patterns.push(pattern);
          }
        } else if (inPackages && line.trim().length > 0 && !line.match(/^\s/)) {
          break; // End of packages section
        }
      }

      return patterns;
    } catch {
      // Fall through to package.json
    }
  }

  // 2. Fall back to package.json workspaces field
  try {
    const packageJsonPath = resolve(repoRoot, 'package.json');
    const content = await readFile(packageJsonPath, 'utf-8');
    const packageJson: PackageJson = JSON.parse(content);

    if (Array.isArray(packageJson.workspaces)) {
      return packageJson.workspaces;
    }
    if (packageJson.workspaces?.packages) {
      return packageJson.workspaces.packages;
    }
  } catch {
    // No package.json or parse error
  }

  return [];
}

/**
 * Read package.json and extract workspace package names
 */
async function getWorkspacePackageNames(repoRoot: string, logger?: Logger): Promise<string[]> {
  try {
    const workspacePatterns = await getWorkspacePatterns(repoRoot);

    if (workspacePatterns.length === 0) {
      return [];
    }

    // Find all package.json files in workspace directories
    const packageJsonFiles = await glob(
      workspacePatterns.map((pattern) => `${pattern}/package.json`),
      {
        cwd: repoRoot,
        absolute: true,
        ignore: ['**/node_modules/**'],
      },
    );

    // Read package names from each workspace package
    const packageNames: string[] = [];
    for (const pkgPath of packageJsonFiles) {
      try {
        const pkgContent = await readFile(pkgPath, 'utf-8');
        const pkg: PackageJson = JSON.parse(pkgContent);
        if (pkg.name) {
          packageNames.push(pkg.name);
        }
      } catch (error) {
        logger?.warn(
          `Failed to read workspace package at ${pkgPath}:`,
          error instanceof Error ? error.message : String(error),
        );
      }
    }

    return packageNames;
  } catch (error) {
    logger?.warn(
      `Failed to detect workspace packages in ${repoRoot}:`,
      error instanceof Error ? error.message : String(error),
    );
    return [];
  }
}

/**
 * Extract unique scopes from scoped package names
 * Example: ['@company/cms', '@company/api', '@example/app'] → ['@company', '@example']
 */
function extractScopes(packageNames: string[]): string[] {
  const scopes = new Set<string>();

  for (const name of packageNames) {
    // Check if it's a scoped package (@scope/name)
    const match = name.match(/^(@[^/]+)\//);
    if (match) {
      scopes.add(match[1]);
    }
  }

  return Array.from(scopes).sort();
}

/**
 * Auto-detect workspace scopes from package.json
 * Returns unique scope prefixes like ['@company', '@example']
 */
export async function detectWorkspaceScopes(repoRoot: string, logger?: Logger): Promise<string[]> {
  const packageNames = await getWorkspacePackageNames(repoRoot, logger);
  return extractScopes(packageNames);
}

/**
 * Generate workspace prefixes for syncpack
 * Converts scopes to glob patterns: ['@company', '@example'] → ['@company/*', '@example/*']
 */
export function generateWorkspacePrefixes(scopes: string[]): string[] {
  return scopes.map((scope) => `${scope}/*`);
}

/**
 * Check if a package.json has Expo dependency
 */
function hasExpoDependency(pkg: PackageJson): boolean {
  return !!(pkg.dependencies?.expo || pkg.devDependencies?.expo);
}

/**
 * Auto-detect all Expo projects in the monorepo
 * @param repoRoot - Root directory of the repository
 * @param logger - Optional logger instance
 * @returns Array of Expo project configurations
 */
export async function detectExpoProjects(repoRoot: string, logger?: Logger): Promise<ExpoProject[]> {
  try {
    const packageJsonPath = resolve(repoRoot, 'package.json');
    const content = await readFile(packageJsonPath, 'utf-8');
    const packageJson: PackageJson = JSON.parse(content);

    // Get workspace patterns using the shared utility
    const workspacePatterns = await getWorkspacePatterns(repoRoot);

    // If no workspaces, check root package.json
    if (workspacePatterns.length === 0) {
      if (hasExpoDependency(packageJson)) {
        return [
          {
            name: packageJson.name || 'root',
            packageJsonPath: './package.json',
          },
        ];
      }
      return [];
    }

    // Find all package.json files in workspace directories
    const packageJsonFiles = await glob(
      workspacePatterns.map((pattern) => `${pattern}/package.json`),
      {
        cwd: repoRoot,
        absolute: true,
        ignore: ['**/node_modules/**'],
      },
    );

    // Check each package for Expo dependency
    const expoProjects: ExpoProject[] = [];
    for (const pkgPath of packageJsonFiles) {
      try {
        const pkgContent = await readFile(pkgPath, 'utf-8');
        const pkg: PackageJson = JSON.parse(pkgContent);

        if (hasExpoDependency(pkg)) {
          expoProjects.push({
            name: pkg.name || relative(repoRoot, pkgPath).replace('/package.json', ''),
            packageJsonPath: `./${relative(repoRoot, pkgPath)}`,
          });
        }
      } catch (error) {
        logger?.warn(`Failed to read package at ${pkgPath}:`, error instanceof Error ? error.message : String(error));
      }
    }

    return expoProjects;
  } catch (error) {
    logger?.warn(
      `Failed to detect Expo projects in ${repoRoot}:`,
      error instanceof Error ? error.message : String(error),
    );
    return [];
  }
}
