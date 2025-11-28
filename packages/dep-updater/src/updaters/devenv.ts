/**
 * Devenv (Nix) dependency updater
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import { execa } from 'execa';

import type { Logger } from '../logger.js';
import type { PackageUpdate, UpdateResult } from '../types.js';

/**
 * Compare two version strings to determine if newVersion is a downgrade from oldVersion
 * Returns true if newVersion < oldVersion (downgrade)
 */
export function isVersionDowngrade(oldVersion: string, newVersion: string): boolean {
  // Handle special cases
  if (oldVersion === newVersion) return false;
  if (oldVersion === '(new)' || newVersion === '(removed)') return false;

  // Split versions into parts for comparison
  // Handle versions like "3.13.9", "5.3p3", "1.0.8", "40"
  const oldParts = oldVersion.split(/[.-]/);
  const newParts = newVersion.split(/[.-]/);

  const maxLen = Math.max(oldParts.length, newParts.length);

  for (let i = 0; i < maxLen; i++) {
    const oldPart = oldParts[i] || '0';
    const newPart = newParts[i] || '0';

    // Check if both parts are purely numeric
    const oldIsNumeric = /^\d+$/.test(oldPart);
    const newIsNumeric = /^\d+$/.test(newPart);

    if (oldIsNumeric && newIsNumeric) {
      const oldNum = Number.parseInt(oldPart, 10);
      const newNum = Number.parseInt(newPart, 10);
      if (newNum < oldNum) return true;
      if (newNum > oldNum) return false;
    } else {
      // Handle mixed alphanumeric like "3p3" vs "3p2"
      // Extract leading number and suffix separately
      const oldMatch = oldPart.match(/^(\d+)(.*)$/);
      const newMatch = newPart.match(/^(\d+)(.*)$/);

      if (oldMatch && newMatch) {
        const oldNum = Number.parseInt(oldMatch[1], 10);
        const newNum = Number.parseInt(newMatch[1], 10);
        if (newNum < oldNum) return true;
        if (newNum > oldNum) return false;

        // If numbers are equal, compare suffixes
        const oldSuffix = oldMatch[2];
        const newSuffix = newMatch[2];
        if (newSuffix < oldSuffix) return true;
        if (newSuffix > oldSuffix) return false;
      } else {
        // Fall back to pure string comparison
        if (newPart < oldPart) return true;
        if (newPart > oldPart) return false;
      }
    }
  }

  return false;
}

export interface DixParseResult {
  updates: PackageUpdate[];
  downgrades: PackageUpdate[];
}

/**
 * Parse dix output to extract package version changes
 *
 * dix output format:
 * ```
 * CHANGED
 * [D.] python3  3.13.8 -> 3.13.9
 * ADDED
 * [A.] nodejs  22.0.0
 * REMOVED
 * [R.] libffi  40
 * ```
 */
export function parseDixOutput(output: string): DixParseResult {
  const updates: PackageUpdate[] = [];
  const downgrades: PackageUpdate[] = [];

  // Match changed packages: [D.] name  oldVersions -> newVersions
  // Example: [D.] python3  3.13.9, 3.13.9-env -> 3.13.8, 3.13.8-env
  // Example: [D.] bash     5.3p3 ×2 -> 5.3p3
  const changedRegex = /\[D\.\]\s+(\S+)\s+(.+?)\s*(?:→|->)\s*(.+)$/gm;
  for (const match of output.matchAll(changedRegex)) {
    // Extract the first version from potentially comma-separated list
    // Also strip any ×N multiplier notation
    const fromVersions = match[2].replace(/\s*×\d+$/, '').trim();
    const toVersions = match[3].trim();
    const fromVersion = fromVersions.split(',')[0].trim();
    const toVersion = toVersions.split(',')[0].trim();

    const pkg: PackageUpdate = {
      name: match[1],
      fromVersion,
      toVersion,
      updateType: 'unknown',
      ecosystem: 'nix',
    };

    // Separate downgrades from updates
    if (isVersionDowngrade(fromVersion, toVersion)) {
      downgrades.push(pkg);
    } else if (fromVersion !== toVersion) {
      updates.push(pkg);
    }
    // Skip if versions are identical (rebuild without version change)
  }

  // Match added packages: [A.] name  version (always an update)
  const addedRegex = /\[A\.\]\s+(\S+)\s+(\S+)/g;
  for (const match of output.matchAll(addedRegex)) {
    updates.push({
      name: match[1],
      fromVersion: '(new)',
      toVersion: match[2],
      updateType: 'unknown',
      ecosystem: 'nix',
    });
  }

  // Match removed packages: [R.] name  version (treat as informational, like downgrade)
  const removedRegex = /\[R\.\]\s+(\S+)\s+(\S+)/g;
  for (const match of output.matchAll(removedRegex)) {
    downgrades.push({
      name: match[1],
      fromVersion: match[2],
      toVersion: '(removed)',
      updateType: 'unknown',
      ecosystem: 'nix',
    });
  }

  return { updates, downgrades };
}

/**
 * Get the current devenv profile path from devenv info output
 */
async function getDevenvProfilePath(devenvPath: string): Promise<string | undefined> {
  try {
    const result = await execa('devenv', ['info'], { cwd: devenvPath });
    // Parse: "- DEVENV_PROFILE: /nix/store/..."
    const match = result.stdout.match(/DEVENV_PROFILE:\s*(\S+)/);
    return match?.[1];
  } catch {
    return undefined;
  }
}

/**
 * Compare two devenv profiles using dix to get actual package version changes
 */
async function diffDevenvProfiles(beforePath: string, afterPath: string, logger?: Logger): Promise<DixParseResult> {
  try {
    const result = await execa('nix', ['shell', 'github:faukah/dix', '-c', 'dix', beforePath, afterPath]);
    const parsed = parseDixOutput(result.stdout);

    // Warn if dix produced output but we couldn't parse any packages
    // This might indicate a dix output format change
    if (result.stdout.trim() && parsed.updates.length === 0 && parsed.downgrades.length === 0) {
      logger?.debug?.('dix output present but no packages parsed - format may have changed');
      logger?.debug?.(`dix output: ${result.stdout.substring(0, 500)}`);
    }

    return parsed;
  } catch (error) {
    logger?.warn(`dix diff failed: ${error instanceof Error ? error.message : String(error)}`);
    return { updates: [], downgrades: [] };
  }
}

interface DevenvLock {
  nodes?: Record<
    string,
    {
      locked?: {
        lastModified?: number;
        narHash?: string;
        rev?: string;
      };
      original?: {
        owner?: string;
        repo?: string;
        type?: string;
      };
    }
  >;
}

/**
 * Parse devenv.lock to extract input versions
 */
async function parseDevenvLock(lockPath: string): Promise<Map<string, string>> {
  try {
    const content = await readFile(lockPath, 'utf-8');
    const lock: DevenvLock = JSON.parse(content);

    const versions = new Map<string, string>();

    if (lock.nodes) {
      for (const [name, node] of Object.entries(lock.nodes)) {
        if (node.locked?.rev) {
          // Use short commit hash as version
          versions.set(name, node.locked.rev.substring(0, 7));
        }
      }
    }

    return versions;
  } catch (error) {
    // ENOENT is expected if lock file doesn't exist
    if (error && typeof error === 'object' && 'code' in error && error.code !== 'ENOENT') {
      console.warn(
        `Failed to parse devenv.lock from ${lockPath}:`,
        error instanceof Error ? error.message : String(error),
      );
    }
    return new Map();
  }
}

/**
 * Compare devenv lock versions to detect updates
 */
function compareDevenvLocks(before: Map<string, string>, after: Map<string, string>): PackageUpdate[] {
  const updates: PackageUpdate[] = [];

  for (const [name, afterVersion] of after) {
    const beforeVersion = before.get(name);

    if (beforeVersion && beforeVersion !== afterVersion) {
      updates.push({
        name,
        fromVersion: beforeVersion,
        toVersion: afterVersion,
        updateType: 'unknown',
        ecosystem: 'nix',
      });
    }
  }

  return updates;
}

/**
 * Update devenv dependencies
 */
export async function updateDevenv(
  devenvPath: string,
  options: {
    dryRun?: boolean;
    logger?: Logger;
    /** Use dix to diff actual derivation changes instead of lock file revisions */
    useDerivationDiff?: boolean;
  } = {},
): Promise<UpdateResult> {
  const { dryRun = false, logger, useDerivationDiff = true } = options;

  try {
    logger?.info('Updating devenv dependencies...');

    const lockPath = join(devenvPath, 'devenv.lock');

    // IMPORTANT: Parse lock BEFORE any devenv commands, as devenv info auto-updates the lock
    const lockBefore = await parseDevenvLock(lockPath);

    // Get profile path before update (for derivation diffing)
    // Note: This may trigger lock auto-update, but we already have lockBefore saved
    let profileBefore: string | undefined;
    if (useDerivationDiff) {
      profileBefore = await getDevenvProfilePath(devenvPath);
      if (profileBefore) {
        logger?.debug?.(`Profile before: ${profileBefore}`);
      }
    }

    // Always run devenv update to detect available updates
    // In dry-run mode, we'll restore the lock file afterwards
    await execa('devenv', ['update'], {
      cwd: devenvPath,
      stdio: 'inherit',
    });

    logger?.info('✓ Devenv update completed');

    // Parse lock after update
    const lockAfter = await parseDevenvLock(lockPath);

    // Try derivation diffing first if enabled
    let updates: PackageUpdate[] = [];
    let downgrades: PackageUpdate[] = [];
    if (useDerivationDiff && profileBefore) {
      // Force environment rebuild to get the new profile path
      // devenv info only returns cached profile, we need to evaluate the updated config
      try {
        await execa('devenv', ['shell', '--', 'true'], {
          cwd: devenvPath,
          timeout: 300000, // 5 min timeout for rebuild
        });
      } catch {
        logger?.warn('Failed to rebuild devenv environment for diff');
      }
      const profileAfter = await getDevenvProfilePath(devenvPath);
      logger?.debug?.(`Profile before: ${profileBefore}`);
      logger?.debug?.(`Profile after: ${profileAfter}`);
      if (profileAfter && profileBefore !== profileAfter) {
        const diffResult = await diffDevenvProfiles(profileBefore, profileAfter, logger);
        updates = diffResult.updates;
        downgrades = diffResult.downgrades;
        if (updates.length > 0) {
          logger?.info(`✓ Found ${updates.length} package version changes (via dix)`);
        }
        if (downgrades.length > 0) {
          logger?.info(`i Found ${downgrades.length} package downgrades/removals (informational only)`);
        }
      } else if (profileAfter === profileBefore) {
        logger?.debug?.('Profiles are identical - no derivation changes detected');
      } else {
        logger?.debug?.('Could not get profile after update');
      }
    } else if (!useDerivationDiff) {
      logger?.debug?.('Derivation diff disabled');
    } else {
      logger?.debug?.('No profile before update - skipping derivation diff');
    }

    // Fallback to lock file comparison only if we couldn't compare derivations
    // If profiles were identical, there are no actual package changes - don't show lock file hash changes
    const profilesWereIdentical = profileBefore && profileBefore === (await getDevenvProfilePath(devenvPath));

    if (updates.length === 0 && downgrades.length === 0 && !profilesWereIdentical) {
      logger?.debug?.(`Lock before: ${lockBefore.size} entries, after: ${lockAfter.size} entries`);
      updates = compareDevenvLocks(lockBefore, lockAfter);
      if (updates.length > 0) {
        logger?.info(`✓ Found ${updates.length} Nix input updates (via lock file)`);
      } else {
        logger?.info('✓ No updates found');
        // Debug: show what's in the locks to help diagnose
        if (lockBefore.size > 0) {
          logger?.debug?.(`Lock entries: ${[...lockBefore.keys()].join(', ')}`);
        }
      }
    } else if (profilesWereIdentical && updates.length === 0) {
      logger?.info('✓ Nix inputs updated but no package version changes detected');
    }

    // In dry-run mode, restore the lock file to its original state
    if (dryRun) {
      try {
        await execa('git', ['restore', lockPath], { cwd: devenvPath });
        logger?.debug?.('Restored devenv.lock (dry-run mode)');
      } catch {
        // If git restore fails (e.g., not a git repo), changes will persist
        logger?.warn('Could not restore devenv.lock via git, changes will persist');
      }
    }

    return {
      success: true,
      updates,
      downgrades: downgrades.length > 0 ? downgrades : undefined,
      ecosystem: 'nix',
    };
  } catch (error) {
    return {
      success: false,
      updates: [],
      error: String(error),
      ecosystem: 'nix',
    };
  }
}
