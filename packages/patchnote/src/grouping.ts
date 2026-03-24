/**
 * Update grouping/batching for patchnote.
 *
 * Partitions dependency updates into named groups by semver level
 * and/or package name pattern, enabling separate PRs per group.
 *
 * Priority order: (1) separateMajor, (2) separateMinorPatch,
 * (3) name-pattern groups, (4) default.
 */

import micromatch from 'micromatch'
import type { GroupingConfig, PackageUpdate } from './types.js'


/**
 * Partition a list of updates into named groups based on the grouping config.
 *
 * Each step removes matched updates from the remaining pool so no update
 * appears in multiple groups. Empty groups are omitted from the result.
 *
 * @param updates - All dependency updates to partition
 * @param config - Grouping configuration (undefined = all in "default")
 * @returns Map of group name to its updates (only non-empty groups)
 */
export function partitionUpdates(
  updates: PackageUpdate[],
  config: GroupingConfig | undefined,
): Map<string, PackageUpdate[]> {
  const result = new Map<string, PackageUpdate[]>()

  if (!config || Object.keys(config).length === 0) {
    if (updates.length > 0) {
      result.set('default', [...updates])
    }
    return result
  }

  let remaining = [...updates]

  // Step 1: separateMajor - extract major updates
  if (config.separateMajor) {
    const major: PackageUpdate[] = []
    const rest: PackageUpdate[] = []
    for (const u of remaining) {
      if (u.updateType === 'major') {
        major.push(u)
      } else {
        rest.push(u)
      }
    }
    if (major.length > 0) {
      result.set('major', major)
    }
    remaining = rest
  }

  // Step 2: separateMinorPatch - extract minor and patch updates
  if (config.separateMinorPatch) {
    const minor: PackageUpdate[] = []
    const patch: PackageUpdate[] = []
    const rest: PackageUpdate[] = []
    for (const u of remaining) {
      if (u.updateType === 'minor') {
        minor.push(u)
      } else if (u.updateType === 'patch') {
        patch.push(u)
      } else {
        rest.push(u)
      }
    }
    if (minor.length > 0) {
      result.set('minor', minor)
    }
    if (patch.length > 0) {
      result.set('patch', patch)
    }
    remaining = rest
  }

  // Step 3: name-pattern groups
  if (config.groups && config.groups.length > 0) {
    for (const group of config.groups) {
      const patterns = Array.isArray(group.match) ? group.match : [group.match]
      const matched: PackageUpdate[] = []
      const rest: PackageUpdate[] = []
      for (const u of remaining) {
        if (micromatch.isMatch(u.name, patterns)) {
          matched.push(u)
        } else {
          rest.push(u)
        }
      }
      if (matched.length > 0) {
        result.set(group.name, matched)
      }
      remaining = rest
    }
  }

  // Step 4: default group for anything left
  if (remaining.length > 0) {
    result.set('default', remaining)
  }

  return result
}
