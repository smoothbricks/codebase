/**
 * Bun dependency updater
 */

import { execa } from 'execa';
import type { Logger } from '../logger.js';
import type { PackageUpdate, UpdateResult } from '../types.js';

/**
 * Parse package updates from git diff of package.json files
 *
 * Parses diff lines like:
 * -    "package": "^1.0.0"
 * +    "package": "^1.0.1"
 */
export function parsePackageJsonDiff(diff: string): PackageUpdate[] {
  const updates: PackageUpdate[] = [];
  const lines = diff.split('\n');

  // Pattern: "package-name": "version"
  const versionPattern = /^\s*[+-]\s*"(@?[^"]+)":\s*"([~^]?[\d.]+(?:-[\w.]+)?)"/;

  // Collect all removals and additions
  const removals = new Map<string, string>();
  const additions = new Map<string, string>();

  for (const line of lines) {
    const match = line.match(versionPattern);
    if (!match) continue;

    const [, packageName, version] = match;
    const cleanVersion = version.replace(/^[~^]/, ''); // Strip semver prefix

    if (line.trim()[0] === '-') {
      removals.set(packageName, cleanVersion);
    } else if (line.trim()[0] === '+') {
      additions.set(packageName, cleanVersion);
    }
  }

  // Match removals with additions
  for (const [packageName, fromVersion] of removals) {
    const toVersion = additions.get(packageName);

    if (toVersion && fromVersion !== toVersion) {
      updates.push({
        name: packageName,
        fromVersion,
        toVersion,
        updateType: classifyUpdateType(fromVersion, toVersion),
        ecosystem: 'npm',
      });
    }
  }

  return updates;
}

/**
 * Parse package updates from bun update output (fallback)
 *
 * Bun update outputs lines like:
 * ↑ @biomejs/biome 2.3.3 → 2.3.5
 * ↑ vite 7.2.0 → 7.2.2
 */
export function parseBunUpdateOutput(output: string): PackageUpdate[] {
  const updates: PackageUpdate[] = [];
  const lines = output.split('\n');

  // Pattern: ↑ package-name version1 → version2
  const updatePattern = /^[↑↓+]\s+(@?[\w/-]+)\s+([\d.]+(?:-[\w.]+)?)\s+→\s+([\d.]+(?:-[\w.]+)?)/;

  for (const line of lines) {
    const match = line.match(updatePattern);
    if (!match) continue;

    const [, packageName, fromVersion, toVersion] = match;

    updates.push({
      name: packageName,
      fromVersion,
      toVersion,
      updateType: classifyUpdateType(fromVersion, toVersion),
      ecosystem: 'npm',
    });
  }

  return updates;
}

/**
 * Classify update type (major/minor/patch)
 */
function classifyUpdateType(from: string, to: string): 'major' | 'minor' | 'patch' | 'unknown' {
  const fromParts = from.split('.').map((p) => Number.parseInt(p, 10));
  const toParts = to.split('.').map((p) => Number.parseInt(p, 10));

  if (
    fromParts.length < 3 ||
    toParts.length < 3 ||
    fromParts.some((n) => Number.isNaN(n)) ||
    toParts.some((n) => Number.isNaN(n))
  ) {
    return 'unknown';
  }

  if (fromParts[0] !== toParts[0]) return 'major';
  if (fromParts[1] !== toParts[1]) return 'minor';
  if (fromParts[2] !== toParts[2]) return 'patch';

  return 'unknown';
}

/**
 * Update Bun dependencies
 */
export async function updateBunDependencies(
  repoRoot: string,
  options: {
    dryRun?: boolean;
    recursive?: boolean;
    syncpackFixCommand?: string;
    logger?: Logger;
  } = {},
): Promise<UpdateResult> {
  const { dryRun = false, recursive = true, syncpackFixCommand = 'syncpack:fix', logger } = options;

  try {
    logger?.info('Updating Bun dependencies...');

    let updates: PackageUpdate[] = [];

    if (!dryRun) {
      // Run bun update and capture output
      const args = ['update'];
      if (recursive) {
        args.push('--recursive');
      }

      const result = await execa('bun', args, {
        cwd: repoRoot,
      });

      // Show the output to user
      logger?.info(result.stdout);
      logger?.info('✓ Bun update completed');

      // Run bun install to sync lock file with package.json changes
      // This ensures bun.lock workspaces section matches updated package.json
      try {
        await execa('bun', ['install'], {
          cwd: repoRoot,
        });
        logger?.info('✓ Lock file synced');
      } catch (error) {
        logger?.warn('Warning: bun install failed, lock file may be out of sync');
      }

      // Run syncpack to fix any version mismatches
      try {
        const syncpackResult = await execa('bun', ['run', syncpackFixCommand], {
          cwd: repoRoot,
        });
        logger?.info(syncpackResult.stdout);
        logger?.info(`✓ Syncpack ${syncpackFixCommand} completed`);
      } catch (error) {
        logger?.warn(`Warning: ${syncpackFixCommand} failed, continuing...`);
      }

      // Parse updates from git diff (more reliable than stdout)
      try {
        // Get all package.json changes (root and subdirectories)
        const diffResult = await execa('git', ['diff', '--', 'package.json', '*/package.json', '*/*/package.json'], {
          cwd: repoRoot,
        });

        if (diffResult.stdout) {
          updates = parsePackageJsonDiff(diffResult.stdout);
        }
      } catch (error) {
        logger?.warn('Warning: Could not get git diff, falling back to stdout parsing');
        // Fallback to stdout parsing
        updates = parseBunUpdateOutput(result.stdout);
      }
    }

    logger?.info(`✓ Found ${updates.length} package updates`);

    return {
      success: true,
      updates,
      ecosystem: 'npm',
    };
  } catch (error) {
    return {
      success: false,
      updates: [],
      error: String(error),
      ecosystem: 'npm',
    };
  }
}
