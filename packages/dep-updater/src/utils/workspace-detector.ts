/**
 * Auto-detect workspace scopes from package.json and find Expo projects
 */

import { readFile } from 'node:fs/promises';
import { relative, resolve } from 'node:path';
import glob from 'fast-glob';
import type { ExpoProject } from '../types.js';

interface PackageJson {
  name?: string;
  workspaces?: string[] | { packages?: string[] };
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}

/**
 * Read package.json and extract workspace package names
 */
async function getWorkspacePackageNames(repoRoot: string): Promise<string[]> {
  try {
    const packageJsonPath = resolve(repoRoot, 'package.json');
    const content = await readFile(packageJsonPath, 'utf-8');
    const packageJson: PackageJson = JSON.parse(content);

    // Get workspace patterns
    let workspacePatterns: string[] = [];
    if (Array.isArray(packageJson.workspaces)) {
      workspacePatterns = packageJson.workspaces;
    } else if (packageJson.workspaces?.packages) {
      workspacePatterns = packageJson.workspaces.packages;
    }

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
        console.warn(
          `Failed to read workspace package at ${pkgPath}:`,
          error instanceof Error ? error.message : String(error),
        );
      }
    }

    return packageNames;
  } catch (error) {
    console.warn(
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
export async function detectWorkspaceScopes(repoRoot: string): Promise<string[]> {
  const packageNames = await getWorkspacePackageNames(repoRoot);
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
 * @returns Array of Expo project configurations
 */
export async function detectExpoProjects(repoRoot: string): Promise<ExpoProject[]> {
  try {
    const packageJsonPath = resolve(repoRoot, 'package.json');
    const content = await readFile(packageJsonPath, 'utf-8');
    const packageJson: PackageJson = JSON.parse(content);

    // Get workspace patterns
    let workspacePatterns: string[] = [];
    if (Array.isArray(packageJson.workspaces)) {
      workspacePatterns = packageJson.workspaces;
    } else if (packageJson.workspaces?.packages) {
      workspacePatterns = packageJson.workspaces.packages;
    }

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
        console.warn(`Failed to read package at ${pkgPath}:`, error instanceof Error ? error.message : String(error));
      }
    }

    return expoProjects;
  } catch (error) {
    console.warn(
      `Failed to detect Expo projects in ${repoRoot}:`,
      error instanceof Error ? error.message : String(error),
    );
    return [];
  }
}
