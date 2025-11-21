/**
 * Devenv (Nix) dependency updater
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { execa } from 'execa';
import type { PackageUpdate, UpdateResult } from '../types.js';

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
    logger?: import('../logger.js').Logger;
  } = {},
): Promise<UpdateResult> {
  const { dryRun = false, logger } = options;

  try {
    logger?.info('Updating devenv dependencies...');

    const lockPath = join(devenvPath, 'devenv.lock');

    // Parse lock before update
    const lockBefore = await parseDevenvLock(lockPath);

    if (!dryRun) {
      // Run devenv update
      await execa('devenv', ['update'], {
        cwd: devenvPath,
        stdio: 'inherit',
      });

      logger?.info('✓ Devenv update completed');
    }

    // Parse lock after update
    const lockAfter = await parseDevenvLock(lockPath);

    // Compare to find updates
    const updates = compareDevenvLocks(lockBefore, lockAfter);

    logger?.info(`✓ Found ${updates.length} Nix input updates`);

    return {
      success: true,
      updates,
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
