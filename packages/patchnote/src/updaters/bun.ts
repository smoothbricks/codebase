/**
 * Bun dependency updater
 */

import { execa } from 'execa';
import type { Logger } from '../logger.js';
import type { CommandExecutor, PackageUpdate, UpdateResult } from '../types.js';

/**
 * Parse bun outdated output to detect available updates
 *
 * Output format:
 * | Package | Current | Update | Latest |
 * |---------|---------|--------|--------|
 * | axios   | 1.6.0   | 1.13.2 | 1.13.2 |
 */
export function parseBunOutdated(output: string): PackageUpdate[] {
  const updates: PackageUpdate[] = [];
  const lines = output.split('\n');

  for (const line of lines) {
    // Skip header, separator, and empty lines
    if (!line.includes('|') || line.includes('Package') || line.includes('---')) {
      continue;
    }

    // Parse table row: | name | current | update | latest |
    const parts = line
      .split('|')
      .map((p) => p.trim())
      .filter((p) => p.length > 0);

    if (parts.length < 4) continue;

    const [nameWithDev, current, update] = parts;
    // Detect and remove "(dev)" suffix
    const isDev = /\s*\(dev\)\s*$/.test(nameWithDev);
    const name = nameWithDev.replace(/\s*\(dev\)\s*$/, '');

    // Only include if there's an actual update (current !== update)
    if (current && update && current !== update) {
      updates.push({
        name,
        fromVersion: current,
        toVersion: update,
        updateType: classifyUpdateType(current, update),
        ecosystem: 'npm',
        isDev,
      });
    }
  }

  return updates;
}

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

  // Section header pattern to detect dependencies vs devDependencies
  const sectionPattern = /^\s*[+-]?\s*"(dependencies|devDependencies)":/;

  // Track current section and isDev status per package
  let currentSection: 'dependencies' | 'devDependencies' | null = null;

  // Collect all removals and additions with their section info
  const removals = new Map<string, { version: string; isDev: boolean }>();
  const additions = new Map<string, { version: string; isDev: boolean }>();

  for (const line of lines) {
    // Check for section headers
    const sectionMatch = line.match(sectionPattern);
    if (sectionMatch) {
      currentSection = sectionMatch[1] as 'dependencies' | 'devDependencies';
      continue;
    }

    const match = line.match(versionPattern);
    if (!match) continue;

    const [, packageName, version] = match;
    const cleanVersion = version.replace(/^[~^]/, ''); // Strip semver prefix
    const isDev = currentSection === 'devDependencies';

    if (line.trim()[0] === '-') {
      removals.set(packageName, { version: cleanVersion, isDev });
    } else if (line.trim()[0] === '+') {
      additions.set(packageName, { version: cleanVersion, isDev });
    }
  }

  // Match removals with additions
  for (const [packageName, removal] of removals) {
    const addition = additions.get(packageName);

    if (addition && removal.version !== addition.version) {
      updates.push({
        name: packageName,
        fromVersion: removal.version,
        toVersion: addition.version,
        updateType: classifyUpdateType(removal.version, addition.version),
        ecosystem: 'npm',
        isDev: addition.isDev,
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
      isDev: false,
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
    /** When set, only update these specific packages (for per-group updates) */
    packages?: string[];
  } = {},
): Promise<UpdateResult> {
  const { dryRun = false, recursive = true, syncpackFixCommand = 'syncpack:fix', logger, packages } = options;

  try {
    logger?.info('Updating Bun dependencies...');

    let updates: PackageUpdate[] = [];

    if (!dryRun) {
      // Run bun update and capture output
      const args = ['update'];
      if (recursive) {
        args.push('--recursive');
      }
      // When packages are specified, only update those (for per-group runs)
      if (packages && packages.length > 0) {
        args.push(...packages);
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
      } catch (_error) {
        logger?.warn('Warning: bun install failed, lock file may be out of sync');
      }

      // Run syncpack to fix any version mismatches
      try {
        const syncpackResult = await execa('bun', ['run', syncpackFixCommand], {
          cwd: repoRoot,
        });
        logger?.info(syncpackResult.stdout);
        logger?.info(`✓ Syncpack ${syncpackFixCommand} completed`);
      } catch (_error) {
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
      } catch (_error) {
        logger?.warn('Warning: Could not get git diff, falling back to stdout parsing');
        // Fallback to stdout parsing
        updates = parseBunUpdateOutput(result.stdout);
      }
    } else {
      // Dry run: use bun outdated to detect available updates
      logger?.info('Checking for available updates...');
      try {
        const outdatedResult = await execa('bun', ['outdated'], {
          cwd: repoRoot,
        });
        updates = parseBunOutdated(outdatedResult.stdout);
      } catch (_error) {
        // bun outdated may fail if no updates available
        logger?.info('No updates detected');
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

/**
 * Refresh the lock file by running `bun install --force`
 * This re-resolves all transitive dependencies from the registry
 * without modifying package.json files.
 *
 * @param repoRoot - Repository root directory
 * @param options - Options including dryRun, logger, and executor for testing
 * @returns Whether bun.lock changed and any error that occurred
 */
export async function refreshLockFile(
  repoRoot: string,
  options: {
    dryRun?: boolean;
    logger?: Logger;
    executor?: CommandExecutor;
  } = {},
): Promise<{ changed: boolean; error?: string }> {
  const { dryRun = false, logger, executor = execa as unknown as CommandExecutor } = options;

  if (dryRun) {
    logger?.info('Dry run: would run bun install --force to refresh lock file');
    return { changed: false };
  }

  try {
    logger?.info('Running bun install --force to refresh transitive dependencies...');
    await executor('bun', ['install', '--force'], { cwd: repoRoot });
    logger?.info('Lock file refresh complete');

    // Check if bun.lock actually changed (scoped to lock files only to avoid false positives from unrelated changes)
    const { stdout } = await executor('git', ['diff', '--name-only', '--', 'bun.lock', 'bun.lockb'], { cwd: repoRoot });
    const changed = stdout.trim().length > 0;

    return { changed };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger?.error('Lock file refresh failed:', message);
    return { changed: false, error: message };
  }
}
