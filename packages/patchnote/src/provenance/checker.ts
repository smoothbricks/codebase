/**
 * npm provenance downgrade detection
 *
 * Detects when a package update would move from a version published with
 * npm provenance attestation to a version without it. This is a strong
 * signal of a potential supply chain compromise.
 *
 * Only flags downgrades (had provenance -> lost it), never flags absence
 * (packages that never had provenance are not flagged).
 */

import type { Logger } from '../logger.js';
import type { PackageUpdate } from '../types.js';

/**
 * Check if a specific package version has npm provenance attestation.
 *
 * Fetches the per-version packument from the npm registry and checks
 * for the presence of `dist.attestations.provenance`.
 *
 * @param packageName - npm package name (supports scoped packages)
 * @param version - Semver version string
 * @returns true if the version has provenance attestation
 */
export async function getProvenanceStatus(packageName: string, version: string): Promise<boolean> {
  try {
    const url = `https://registry.npmjs.org/${encodeURIComponent(packageName)}/${version}`;
    const response = await fetch(url);
    if (!response.ok) return false;

    const data = (await response.json()) as {
      dist?: {
        attestations?: {
          provenance?: { predicateType: string };
        };
      };
    };

    return !!data.dist?.attestations?.provenance;
  } catch {
    return false; // Fail open: treat fetch errors as "unknown" (not a downgrade)
  }
}

/**
 * Check npm provenance downgrades for a list of package updates.
 *
 * Filters to npm ecosystem packages only, then checks each package's
 * fromVersion and toVersion for provenance status. If fromVersion has
 * provenance but toVersion does not, sets `update.provenanceDowngraded = true`.
 *
 * Uses batched concurrent fetching to avoid npm registry rate limiting.
 * Short-circuits: only fetches toVersion if fromVersion has provenance.
 *
 * @param updates - Package updates to check (mutated in place)
 * @param maxConcurrent - Maximum concurrent registry requests (default: 5)
 * @param logger - Optional logger for warnings
 */
export async function checkProvenanceDowngrades(
  updates: PackageUpdate[],
  maxConcurrent = 5,
  logger?: Logger,
): Promise<void> {
  // Only check npm packages
  const npmUpdates = updates.filter((u) => u.ecosystem === 'npm');
  if (npmUpdates.length === 0) return;

  // Process in batches
  for (let i = 0; i < npmUpdates.length; i += maxConcurrent) {
    const batch = npmUpdates.slice(i, i + maxConcurrent);
    await Promise.all(
      batch.map(async (update) => {
        // Check current version first (short-circuit optimization)
        const currentHasProvenance = await getProvenanceStatus(update.name, update.fromVersion);
        if (!currentHasProvenance) return; // No downgrade possible

        // Current has provenance -- check if target also has it
        const targetHasProvenance = await getProvenanceStatus(update.name, update.toVersion);
        if (!targetHasProvenance) {
          update.provenanceDowngraded = true;
          logger?.warn(`Provenance downgrade detected: ${update.name} ${update.fromVersion} -> ${update.toVersion}`);
        }
      }),
    );
  }
}
