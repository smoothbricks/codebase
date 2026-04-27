/**
 * npm deprecated package detection
 *
 * Detects when a package version is marked as deprecated in the npm registry
 * and looks up known replacement packages from Renovate's curated mapping.
 *
 * Deprecation is informational only -- it never blocks auto-merge.
 */

import type { Logger } from '../logger.js';
import type { PackageUpdate } from '../types.js';
import { findReplacement } from './replacements.js';

/**
 * Check if a specific package version is deprecated in the npm registry.
 *
 * Fetches the per-version packument from the npm registry and checks
 * for the presence of the `deprecated` field.
 *
 * @param packageName - npm package name (supports scoped packages)
 * @param version - Semver version string
 * @returns Deprecation message string if deprecated, null otherwise
 */
export async function getDeprecationStatus(packageName: string, version: string): Promise<string | null> {
  try {
    const url = `https://registry.npmjs.org/${encodeURIComponent(packageName)}/${version}`;
    const response = await fetch(url, { signal: AbortSignal.timeout(30_000) });
    if (!response.ok) return null;

    const data = (await response.json()) as { deprecated?: string };
    return data.deprecated ?? null;
  } catch {
    return null; // Fail open: treat fetch errors as "unknown" (not deprecated)
  }
}

/**
 * Check npm deprecation status for a list of package updates.
 *
 * Filters to npm ecosystem packages only, then checks each package's
 * toVersion for deprecation status. If deprecated, sets `update.deprecatedMessage`.
 * Also looks up Renovate replacement mappings and sets `update.replacementName`
 * and `update.replacementVersion` when a known replacement exists.
 *
 * Uses batched concurrent fetching to avoid npm registry rate limiting.
 *
 * @param updates - Package updates to check (mutated in place)
 * @param maxConcurrent - Maximum concurrent registry requests (default: 5)
 * @param logger - Optional logger for warnings
 */
export async function checkDeprecations(updates: PackageUpdate[], maxConcurrent = 5, logger?: Logger): Promise<void> {
  // Only check npm packages
  const npmUpdates = updates.filter((u) => u.ecosystem === 'npm');
  if (npmUpdates.length === 0) return;

  // Process in batches
  for (let i = 0; i < npmUpdates.length; i += maxConcurrent) {
    const batch = npmUpdates.slice(i, i + maxConcurrent);
    await Promise.all(
      batch.map(async (update) => {
        const deprecationMessage = await getDeprecationStatus(update.name, update.toVersion);
        if (deprecationMessage) {
          update.deprecatedMessage = deprecationMessage;
          logger?.warn(`Deprecated: ${update.name} ${update.fromVersion} -> ${update.toVersion}`);

          // Look up known replacement
          const replacement = findReplacement(update.name);
          if (replacement) {
            update.replacementName = replacement.replacementName;
            update.replacementVersion = replacement.replacementVersion;
          }
        }
      }),
    );
  }
}
