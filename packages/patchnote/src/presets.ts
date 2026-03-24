/**
 * Shareable config presets for patchnote
 *
 * Provides built-in presets (recommended, aggressive, conservative) and
 * resolution logic for npm-published preset packages.
 */

import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import type { PatchnoteConfig } from './config.js'
import type { DeepPartial } from './types.js'

type PartialConfig = DeepPartial<PatchnoteConfig>

/** Sections that use shallow-merge (base + override spread) */
const SHALLOW_MERGE_SECTIONS = [
  'expo',
  'syncpack',
  'nix',
  'prStrategy',
  'autoMerge',
  'provenanceCheck',
  'ai',
  'git',
  'semanticCommits',
  'filters',
] as const

/** Sections where override completely replaces base (last wins) */
const REPLACE_SECTIONS = ['packageRules', 'grouping'] as const

/**
 * Built-in preset definitions.
 *
 * Each preset is a DeepPartial<PatchnoteConfig> that can be referenced
 * in the config's `extends` array as `patchnote:<name>`.
 */
export const BUILT_IN_PRESETS: Record<string, PartialConfig> = {
  recommended: {
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 5,
      autoCloseOldPRs: true,
      resetOnMerge: true,
      stopOnConflicts: true,
    },
    autoMerge: { enabled: false },
    grouping: { separateMajor: true },
  },

  aggressive: {
    autoMerge: { enabled: true, mode: 'minor' },
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 10,
      autoCloseOldPRs: true,
    },
  },

  conservative: {
    autoMerge: { enabled: false, mode: 'none' },
    prStrategy: {
      stackingEnabled: true,
      maxStackDepth: 3,
    },
    grouping: { separateMajor: true, separateMinorPatch: true },
    packageRules: [{ match: '*', updateTypes: ['patch'] }],
  },
}

/**
 * Merge two DeepPartial<PatchnoteConfig> objects with section-aware shallow merge.
 *
 * Mirrors the mergeConfig() pattern from config.ts but for two partials:
 * - Named config sections (expo, prStrategy, etc.) are shallow-merged
 * - packageRules and grouping use last-wins replacement
 * - Top-level scalars are spread normally
 */
export function mergePartials(base: PartialConfig, override: PartialConfig): PartialConfig {
  const result: PartialConfig = { ...base }

  // Shallow-merge each config section
  for (const section of SHALLOW_MERGE_SECTIONS) {
    const baseSection = base[section]
    const overrideSection = override[section]
    if (overrideSection !== undefined) {
      result[section] = baseSection
        ? { ...baseSection, ...overrideSection }
        : { ...overrideSection }
    }
    // If override doesn't have this section, base's value is already in result via spread
  }

  // Replace sections: override completely replaces base
  for (const section of REPLACE_SECTIONS) {
    if (override[section] !== undefined) {
      ;(result as Record<string, unknown>)[section] = override[section]
    }
  }

  // Top-level scalars (repoRoot, etc.)
  if (override.repoRoot !== undefined) {
    result.repoRoot = override.repoRoot
  }

  return result
}

/**
 * Resolve a single preset reference.
 *
 * - `patchnote:<name>` resolves to a built-in preset
 * - Other strings resolve as npm package names via createRequire
 * - Cycle detection via visited Set
 *
 * @param ref - Preset reference string
 * @param visited - Set of already-visited refs for cycle detection
 * @returns Resolved partial config
 */
export async function resolvePreset(ref: string, visited?: Set<string>): Promise<PartialConfig> {
  // Cycle detection
  const seen = visited ?? new Set<string>()
  if (seen.has(ref)) {
    throw new Error(`Circular preset reference detected: ${ref}`)
  }
  seen.add(ref)

  // Built-in preset
  if (ref.startsWith('patchnote:')) {
    const name = ref.slice('patchnote:'.length)
    const preset = BUILT_IN_PRESETS[name]
    if (!preset) {
      throw new Error(`Unknown built-in preset: ${ref}. Available presets: ${Object.keys(BUILT_IN_PRESETS).join(', ')}`)
    }
    return preset
  }

  // npm preset resolution
  const require = createRequire(import.meta.url)
  const candidates = [ref]

  // If ref doesn't look like a scoped package or already prefixed, try with prefix
  if (!ref.startsWith('@') && !ref.startsWith('patchnote-config-')) {
    candidates.push(`patchnote-config-${ref}`)
  }

  for (const candidate of candidates) {
    try {
      const resolved = require.resolve(candidate)
      const module = await import(pathToFileURL(resolved).href)
      return module.default || module
    } catch {
      // Try next candidate
    }
  }

  throw new Error(`Cannot resolve preset '${ref}'. Install it with: bun add -d ${ref}`)
}

/**
 * Resolve and merge multiple preset references left-to-right.
 *
 * @param refs - Array of preset reference strings
 * @param visited - Set of already-visited refs for cycle detection
 * @returns Merged partial config from all presets
 */
export async function resolvePresets(refs: string[], visited?: Set<string>): Promise<PartialConfig> {
  if (refs.length === 0) {
    return {}
  }

  const seen = visited ?? new Set<string>()
  let merged: PartialConfig = {}

  for (const ref of refs) {
    const preset = await resolvePreset(ref, seen)
    merged = mergePartials(merged, preset)
  }

  return merged
}
