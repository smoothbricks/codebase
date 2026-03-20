/**
 * Package rules / policies system for patchnote.
 *
 * Provides per-package overrides for update behavior (automerge, pin, ignore,
 * group, allowedVersions) with pattern matching (glob, exact, regex) and
 * filtering by update type and dependency type.
 *
 * Rules are evaluated in order with last-match-wins override semantics.
 */

import micromatch from 'micromatch'
import type { Logger } from './logger.js'
import type { DepType, PackageRule, PackageUpdate, ResolvedPackagePolicy } from './types.js'
import { shouldAutoMerge } from './commands/update-deps.js'

/**
 * Test whether a package name matches a single pattern.
 * Supports exact names, globs (via micromatch), brace expansion, and `/regex/` patterns.
 *
 * Invalid regex patterns return false without throwing.
 */
export function matchesPattern(packageName: string, pattern: string): boolean {
  // Detect regex: starts and ends with `/` and has length > 2
  if (pattern.length > 2 && pattern.startsWith('/') && pattern.endsWith('/')) {
    try {
      const re = new RegExp(pattern.slice(1, -1))
      return re.test(packageName)
    } catch {
      return false
    }
  }

  return micromatch.isMatch(packageName, pattern)
}

/**
 * Check whether a rule matches a given update, considering:
 * - match patterns (string or string[])
 * - updateTypes constraint
 * - depTypes constraint
 */
function matchesRule(update: PackageUpdate, rule: PackageRule): boolean {
  // Check match patterns
  const patterns = Array.isArray(rule.match) ? rule.match : [rule.match]
  const nameMatch = patterns.some((p) => matchesPattern(update.name, p))
  if (!nameMatch) return false

  // Check updateTypes constraint
  if (rule.updateTypes && rule.updateTypes.length > 0) {
    if (!rule.updateTypes.includes(update.updateType)) return false
  }

  // Check depTypes constraint
  if (rule.depTypes && rule.depTypes.length > 0) {
    const depType: DepType = update.isDev ? 'devDependencies' : 'dependencies'
    if (!rule.depTypes.includes(depType)) return false
  }

  return true
}

/**
 * Resolve the effective policy for a single package update by evaluating all rules
 * in order. Later matching rules override fields set by earlier rules (last-match-wins).
 * Only explicitly defined fields are merged; undefined fields are skipped.
 */
export function resolvePolicy(update: PackageUpdate, rules: PackageRule[]): ResolvedPackagePolicy {
  const policy: ResolvedPackagePolicy = {}

  for (const rule of rules) {
    if (!matchesRule(update, rule)) continue

    if (rule.automerge !== undefined) policy.automerge = rule.automerge
    if (rule.pin !== undefined) policy.pin = rule.pin
    if (rule.ignore !== undefined) policy.ignore = rule.ignore
    if (rule.group !== undefined) policy.group = rule.group
    if (rule.allowedVersions !== undefined) policy.allowedVersions = rule.allowedVersions
  }

  return policy
}

/**
 * Apply package rules to a list of updates.
 *
 * For each update, resolves its policy and then:
 * - **ignore**: removes the package from the update list
 * - **pin**: computes `allowedVersions` as `~major.minor` from `fromVersion`
 * - **allowedVersions**: filters out updates where `toVersion` does not satisfy the range
 *
 * @returns Filtered updates and a policies map keyed by package name.
 */
export function applyPackageRules(
  updates: PackageUpdate[],
  rules: PackageRule[] | undefined,
  logger?: Logger,
): { updates: PackageUpdate[]; policies: Map<string, ResolvedPackagePolicy> } {
  if (!rules || rules.length === 0) {
    return { updates, policies: new Map() }
  }

  const policies = new Map<string, ResolvedPackagePolicy>()
  const filtered: PackageUpdate[] = []

  for (const update of updates) {
    const policy = resolvePolicy(update, rules)
    policies.set(update.name, policy)

    // Handle ignore action
    if (policy.ignore) {
      logger?.info(`Package rule: ignoring ${update.name}`)
      continue
    }

    // Handle pin action: compute allowedVersions from fromVersion
    if (policy.pin) {
      const parts = update.fromVersion.split('.')
      const major = parts[0] ?? '0'
      const minor = parts[1] ?? '0'
      policy.allowedVersions = `~${major}.${minor}`
    }

    // Handle allowedVersions action
    if (policy.allowedVersions) {
      if (!Bun.semver.satisfies(update.toVersion, policy.allowedVersions)) {
        logger?.info(
          `Package rule: version ${update.toVersion} of ${update.name} does not satisfy ${policy.allowedVersions}`,
        )
        continue
      }
    }

    filtered.push(update)
  }

  return { updates: filtered, policies }
}

/**
 * Determine whether auto-merge should be enabled, considering per-package policies.
 *
 * - If no policies explicitly set `automerge`, falls back to global `shouldAutoMerge`.
 * - If any policy has `automerge: false`, returns `false`.
 * - If all policies with explicit `automerge` have it set to `true`, returns `true`.
 * - Mixed (some explicit true, some without automerge): falls back to global.
 */
export function resolveAutoMerge(
  policies: Map<string, ResolvedPackagePolicy>,
  globalMode: 'none' | 'patch' | 'minor',
  updates: PackageUpdate[],
): boolean {
  // Collect all policies that explicitly set automerge
  const explicitPolicies: boolean[] = []
  for (const [, policy] of policies) {
    if (policy.automerge !== undefined) {
      explicitPolicies.push(policy.automerge)
    }
  }

  // No policies explicitly set automerge => fall back to global
  if (explicitPolicies.length === 0) {
    return shouldAutoMerge(globalMode, updates)
  }

  // Any explicit false => false
  if (explicitPolicies.some((v) => v === false)) {
    return false
  }

  // All explicit values are true
  if (explicitPolicies.every((v) => v === true)) {
    // If all policies have automerge explicitly set (same count as total policies), override global
    if (explicitPolicies.length === policies.size) {
      return true
    }
    // Mixed: some explicit true, some without => fall back to global
    return shouldAutoMerge(globalMode, updates)
  }

  return shouldAutoMerge(globalMode, updates)
}
