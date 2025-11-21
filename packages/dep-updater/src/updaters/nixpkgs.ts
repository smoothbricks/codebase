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

    const sourcesPath = join(overlayPath, '_sources', 'generated.json');

    // Parse sources before update
    const versionsBefore = await parseNvfetcherSources(sourcesPath);

    if (!dryRun) {
      // Try running nvfetcher with fallback chain
      let nvfetcherSuccess = false;

      // Method 1: Try direct nvfetcher (fast, works in devenv)
      try {
        await execa('nvfetcher', [], {
          cwd: overlayPath,
          stdio: 'inherit',
        });
        nvfetcherSuccess = true;
        logger?.info('✓ nvfetcher update completed');
      } catch (_error) {
        logger?.info('Direct nvfetcher not found, trying via nix shell...');

        // Method 2: Try via nix shell (slower, but self-contained)
        try {
          await execa('nix', ['shell', 'nixpkgs#nvfetcher', '-c', 'nvfetcher'], {
            cwd: overlayPath,
            stdio: 'inherit',
          });
          nvfetcherSuccess = true;
          logger?.info('✓ nvfetcher update completed (via nix shell)');
        } catch (_nixError) {
          logger?.warn('Warning: nvfetcher update failed');
          logger?.warn('  - nvfetcher not in PATH');
          logger?.warn('  - nix shell also failed');
          logger?.warn('Skipping nixpkgs overlay update...');
        }
      }

      if (!nvfetcherSuccess) {
        return {
          success: false,
          updates: [],
          error: 'nvfetcher not available (tried direct command and nix shell)',
          ecosystem: 'nixpkgs',
        };
      }
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

    // Verify updated packages can build (non-fatal)
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
