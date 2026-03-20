/**
 * Format provenance downgrade warnings for PR descriptions
 */

import type { PackageUpdate } from '../types.js'

/**
 * Format provenance downgrade warnings as markdown for PR body.
 *
 * Filters updates to those with `provenanceDowngraded === true` and
 * generates a prominent warning block for the PR description.
 *
 * @param updates - Package updates (checks provenanceDowngraded flag)
 * @returns Markdown warning string, or empty string if no downgrades
 */
export function formatProvenanceWarnings(updates: PackageUpdate[]): string {
  const downgraded = updates.filter((u) => u.provenanceDowngraded)
  if (downgraded.length === 0) return ''

  const lines = [
    '### !! Supply Chain Warning: Provenance Downgrade\n',
    '> The following packages previously had npm provenance attestation but the new version does not.',
    '> This may indicate a compromised publish pipeline. **Review these updates carefully.**\n',
  ]

  for (const pkg of downgraded) {
    lines.push(`- **${pkg.name}**: ${pkg.fromVersion} (provenance) -> ${pkg.toVersion} (no provenance)`)
  }

  return lines.join('\n')
}
