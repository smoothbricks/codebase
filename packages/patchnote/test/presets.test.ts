/**
 * Unit tests for preset resolution, merging, and error handling
 */

import { describe, expect, test } from 'bun:test'
import { BUILT_IN_PRESETS, mergePartials, resolvePreset, resolvePresets } from '../src/presets.js'

describe('mergePartials', () => {
  test('mergePartials({}, {}) returns {}', () => {
    const result = mergePartials({}, {})
    expect(result).toEqual({})
  })

  test('mergePartials shallow-merges each config section', () => {
    const base = {
      prStrategy: { stackingEnabled: true, maxStackDepth: 5 },
      autoMerge: { enabled: false },
    }
    const override = {
      prStrategy: { maxStackDepth: 10 },
      ai: { provider: 'zai' as const },
    }
    const result = mergePartials(base, override)

    // prStrategy should be shallow-merged (override's maxStackDepth wins, base's stackingEnabled preserved)
    expect(result.prStrategy?.stackingEnabled).toBe(true)
    expect(result.prStrategy?.maxStackDepth).toBe(10)
    // ai from override only
    expect(result.ai?.provider).toBe('zai')
    // autoMerge from base only
    expect(result.autoMerge?.enabled).toBe(false)
  })

  test('mergePartials preserves array-replace semantics for packageRules', () => {
    const base = {
      packageRules: [{ match: '*', updateTypes: ['patch' as const] }],
    }
    const override = {
      packageRules: [{ match: '@types/*', automerge: true }],
    }
    const result = mergePartials(base, override)
    // Override completely replaces base packageRules
    expect(result.packageRules).toEqual([{ match: '@types/*', automerge: true }])
  })

  test('mergePartials preserves array-replace semantics for grouping', () => {
    const base = {
      grouping: { separateMajor: true },
    }
    const override = {
      grouping: { separateMinorPatch: true },
    }
    const result = mergePartials(base, override)
    // Override replaces base grouping entirely
    expect(result.grouping).toEqual({ separateMinorPatch: true })
  })

  test('mergePartials handles top-level scalars', () => {
    const base = { repoRoot: '/old/path' }
    const override = { repoRoot: '/new/path' }
    const result = mergePartials(base, override)
    expect(result.repoRoot).toBe('/new/path')
  })

  test('mergePartials keeps base sections when override lacks them', () => {
    const base = {
      expo: { enabled: true },
      nix: { enabled: false },
    }
    const override = {
      expo: { autoDetect: false },
    }
    const result = mergePartials(base, override)
    // expo should be shallow-merged
    expect(result.expo?.enabled).toBe(true)
    expect(result.expo?.autoDetect).toBe(false)
    // nix from base preserved
    expect(result.nix?.enabled).toBe(false)
  })
})

describe('BUILT_IN_PRESETS', () => {
  test('recommended preset has correct values', () => {
    const preset = BUILT_IN_PRESETS.recommended
    expect(preset.prStrategy?.stackingEnabled).toBe(true)
    expect(preset.prStrategy?.maxStackDepth).toBe(5)
    expect(preset.prStrategy?.autoCloseOldPRs).toBe(true)
    expect(preset.prStrategy?.resetOnMerge).toBe(true)
    expect(preset.prStrategy?.stopOnConflicts).toBe(true)
    expect(preset.autoMerge?.enabled).toBe(false)
    expect(preset.grouping?.separateMajor).toBe(true)
  })

  test('aggressive preset has correct values', () => {
    const preset = BUILT_IN_PRESETS.aggressive
    expect(preset.autoMerge?.enabled).toBe(true)
    expect(preset.autoMerge?.mode).toBe('minor')
    expect(preset.prStrategy?.stackingEnabled).toBe(true)
    expect(preset.prStrategy?.maxStackDepth).toBe(10)
    expect(preset.prStrategy?.autoCloseOldPRs).toBe(true)
  })

  test('conservative preset has correct values', () => {
    const preset = BUILT_IN_PRESETS.conservative
    expect(preset.autoMerge?.enabled).toBe(false)
    expect(preset.autoMerge?.mode).toBe('none')
    expect(preset.prStrategy?.stackingEnabled).toBe(true)
    expect(preset.prStrategy?.maxStackDepth).toBe(3)
    expect(preset.grouping?.separateMajor).toBe(true)
    expect(preset.grouping?.separateMinorPatch).toBe(true)
    expect(preset.packageRules).toEqual([{ match: '*', updateTypes: ['patch'] }])
  })
})

describe('resolvePreset', () => {
  test('resolvePreset("patchnote:recommended") returns the recommended preset', async () => {
    const result = await resolvePreset('patchnote:recommended')
    expect(result).toEqual(BUILT_IN_PRESETS.recommended)
  })

  test('resolvePreset("patchnote:aggressive") returns the aggressive preset', async () => {
    const result = await resolvePreset('patchnote:aggressive')
    expect(result).toEqual(BUILT_IN_PRESETS.aggressive)
  })

  test('resolvePreset("patchnote:conservative") returns the conservative preset', async () => {
    const result = await resolvePreset('patchnote:conservative')
    expect(result).toEqual(BUILT_IN_PRESETS.conservative)
  })

  test('resolvePreset("patchnote:nonexistent") throws Error matching "Unknown built-in preset"', async () => {
    await expect(resolvePreset('patchnote:nonexistent')).rejects.toThrow('Unknown built-in preset')
  })

  test('resolvePreset("not-installed-package") throws Error matching "Cannot resolve preset"', async () => {
    await expect(resolvePreset('not-installed-package')).rejects.toThrow('Cannot resolve preset')
  })

  test('resolvePreset error for npm package includes install hint', async () => {
    try {
      await resolvePreset('not-installed-package')
      throw new Error('Should have thrown')
    } catch (err) {
      expect((err as Error).message).toContain('bun add -d')
    }
  })

  test('cycle detection throws Error matching "Circular"', async () => {
    const visited = new Set<string>()
    visited.add('patchnote:recommended')
    await expect(resolvePreset('patchnote:recommended', visited)).rejects.toThrow('Circular')
  })
})

describe('resolvePresets', () => {
  test('resolvePresets([]) returns {}', async () => {
    const result = await resolvePresets([])
    expect(result).toEqual({})
  })

  test('resolvePresets with single preset returns that preset', async () => {
    const result = await resolvePresets(['patchnote:recommended'])
    expect(result).toEqual(BUILT_IN_PRESETS.recommended)
  })

  test('resolvePresets merges left-to-right (aggressive wins where overlapping)', async () => {
    const result = await resolvePresets(['patchnote:recommended', 'patchnote:aggressive'])

    // aggressive overrides recommended's autoMerge.enabled
    expect(result.autoMerge?.enabled).toBe(true)
    expect(result.autoMerge?.mode).toBe('minor')
    // aggressive overrides recommended's maxStackDepth (10 vs 5)
    expect(result.prStrategy?.maxStackDepth).toBe(10)
    // recommended's grouping.separateMajor persists (aggressive has no grouping)
    expect(result.grouping?.separateMajor).toBe(true)
  })

  test('resolvePresets with conservative then aggressive merges correctly', async () => {
    const result = await resolvePresets(['patchnote:conservative', 'patchnote:aggressive'])

    // aggressive wins on autoMerge
    expect(result.autoMerge?.enabled).toBe(true)
    expect(result.autoMerge?.mode).toBe('minor')
    // aggressive wins on maxStackDepth
    expect(result.prStrategy?.maxStackDepth).toBe(10)
    // conservative's grouping remains (aggressive has no grouping)
    expect(result.grouping?.separateMajor).toBe(true)
    expect(result.grouping?.separateMinorPatch).toBe(true)
  })
})
