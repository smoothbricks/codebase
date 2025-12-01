/**
 * Nixpkgs overlay updater via nvfetcher
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { execa } from 'execa';
import type { NvfetcherSources, PackageUpdate, UpdateResult } from '../types.js';

/**
 * Parse nvfetcher generated sources from JSON
 */
async function parseNvfetcherSources(sourcesPath: string): Promise<Map<string, string>> {
  try {
    const content = await readFile(sourcesPath, 'utf-8');
    const sources: NvfetcherSources = JSON.parse(content);

    const versions = new Map<string, string>();
    for (const [packageName, data] of Object.entries(sources)) {
      if (data.version) {
        versions.set(packageName, data.version);
      }
    }

    return versions;
  } catch (error) {
    // ENOENT is expected if sources file doesn't exist yet
    if (error && typeof error === 'object' && 'code' in error && error.code !== 'ENOENT') {
      console.warn(
        `Failed to parse nvfetcher sources from ${sourcesPath}:`,
        error instanceof Error ? error.message : String(error),
      );
    }
    return new Map();
  }
}

/**
 * Check if a command is available in PATH
 */
async function isCommandAvailable(command: string): Promise<boolean> {
  try {
    await execa('which', [command]);
    return true;
  } catch {
    return false;
  }
}

/**
 * Update nixpkgs overlay via nvfetcher
 */
export async function updateNixpkgsOverlay(
  overlayPath: string,
  options: {
    dryRun?: boolean;
    logger?: import('../logger.js').Logger;
  } = {},
): Promise<UpdateResult> {
  const { dryRun = false, logger } = options;

  try {
    logger?.info('Updating nixpkgs overlay...');

    // Quick check if nvfetcher is available before proceeding
    const hasNvfetcher = await isCommandAvailable('nvfetcher');

    if (!hasNvfetcher) {
      return {
        success: false,
        updates: [],
        error: 'nvfetcher not available (not found in PATH)',
        ecosystem: 'nixpkgs',
      };
    }

    const sourcesPath = join(overlayPath, '_sources', 'generated.json');

    // Parse sources before update
    const versionsBefore = await parseNvfetcherSources(sourcesPath);

    // Pass GitHub token to nvfetcher for API rate limits
    const githubToken = process.env.GITHUB_TOKEN || process.env.GH_TOKEN;
    const nvfetcherEnv = githubToken ? { ...process.env, GITHUB_TOKEN: githubToken } : process.env;

    // Run nvfetcher to detect and apply updates
    try {
      await execa('nvfetcher', [], {
        cwd: overlayPath,
        stdio: 'inherit',
        env: nvfetcherEnv,
      });
      logger?.info('✓ nvfetcher update completed');
    } catch (error) {
      return {
        success: false,
        updates: [],
        error: `nvfetcher failed: ${error instanceof Error ? error.message : String(error)}`,
        ecosystem: 'nixpkgs',
      };
    }

    // Parse sources after update
    const versionsAfter = await parseNvfetcherSources(sourcesPath);

    // Compare versions
    const updates: PackageUpdate[] = [];
    for (const [name, afterVersion] of versionsAfter) {
      const beforeVersion = versionsBefore.get(name);

      if (beforeVersion && beforeVersion !== afterVersion) {
        updates.push({
          name: `nixpkgs-${name}`,
          fromVersion: beforeVersion,
          toVersion: afterVersion,
          updateType: 'unknown',
          ecosystem: 'nixpkgs',
        });
      }
    }

    logger?.info(`✓ Found ${updates.length} nixpkgs overlay updates`);

    // In dry-run mode, restore the _sources directory to its original state
    if (dryRun) {
      try {
        const sourcesDir = join(overlayPath, '_sources');
        await execa('git', ['restore', sourcesDir], { cwd: overlayPath });
        logger?.debug?.('Restored _sources directory (dry-run mode)');
      } catch {
        // If git restore fails (e.g., not a git repo), changes will persist
        logger?.warn('Could not restore _sources via git, changes will persist');
      }
    }

    // Verify updated packages can build (non-fatal, skip in dry-run)
    if (!dryRun && updates.length > 0) {
      for (const update of updates) {
        const packageName = update.name.replace('nixpkgs-', '');
        try {
          await execa('nix', ['build', `.#${packageName}`, '--no-link'], {
            cwd: overlayPath,
            stdio: 'inherit',
          });
          logger?.info(`✓ Nix build verification passed for ${packageName}`);
        } catch (_error) {
          logger?.warn(`Warning: Nix build verification failed for ${packageName}`);
        }
      }
    }

    return {
      success: true,
      updates,
      ecosystem: 'nixpkgs',
    };
  } catch (error) {
    return {
      success: false,
      updates: [],
      error: String(error),
      ecosystem: 'nixpkgs',
    };
  }
}
