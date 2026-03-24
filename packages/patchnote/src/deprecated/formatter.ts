/**
 * Format deprecated package warnings for PR descriptions
 */

import type { PackageUpdate } from '../types.js';

/**
 * Format deprecation warnings as markdown for PR body.
 *
 * Filters updates to those with `deprecatedMessage` set and generates
 * an informational warning block for the PR description.
 *
 * @param updates - Package updates (checks deprecatedMessage field)
 * @returns Markdown warning string, or empty string if no deprecated packages
 */
export function formatDeprecationWarnings(updates: PackageUpdate[]): string {
  const deprecated = updates.filter((u) => u.deprecatedMessage);
  if (deprecated.length === 0) return '';

  const lines = [
    '### Deprecated Packages\n',
    '> The following packages are marked as deprecated in the npm registry.\n',
  ];

  for (const pkg of deprecated) {
    const replacement = pkg.replacementName ? ` -> **${pkg.replacementName}**@${pkg.replacementVersion}` : '';
    lines.push(`- **${pkg.name}** ${pkg.fromVersion} -> ${pkg.toVersion}: ${pkg.deprecatedMessage}${replacement}`);
  }

  return lines.join('\n');
}
