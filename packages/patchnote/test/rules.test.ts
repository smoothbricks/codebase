/**
 * Unit tests for package rules / policies system
 * Tests matchesPattern, resolvePolicy, applyPackageRules, resolveAutoMerge
 */

import { describe, expect, test } from 'bun:test'
import { applyPackageRules, matchesPattern, resolveAutoMerge, resolvePolicy } from '../src/rules.js'
import type { Logger } from '../src/logger.js'
import type { PackageRule, PackageUpdate, ResolvedPackagePolicy } from '../src/types.js'

/** Helper: create a minimal PackageUpdate fixture */
function makeUpdate(name: string, overrides?: Partial<PackageUpdate>): PackageUpdate {
  return {
    name,
    fromVersion: '1.0.0',
    toVersion: '1.1.0',
    updateType: 'minor',
    ecosystem: 'npm',
    ...overrides,
  }
}

/** Helper: create a mock logger that captures messages */
function createMockLogger(): { logger: Logger; messages: string[] } {
  const messages: string[] = []
  const logger: Logger = {
    debug: (msg: string) => messages.push(`debug: ${msg}`),
    info: (msg: string) => messages.push(`info: ${msg}`),
    warn: (msg: string) => messages.push(`warn: ${msg}`),
    error: (msg: string) => messages.push(`error: ${msg}`),
  }
  return { logger, messages }
}

// ─── matchesPattern ────────────────────────────────────────────────────

describe('matchesPattern', () => {
  test('exact name match', () => {
    expect(matchesPattern('react', 'react')).toBe(true)
  })

  test('exact name miss', () => {
    expect(matchesPattern('react-dom', 'react')).toBe(false)
  })

  test('glob pattern match', () => {
    expect(matchesPattern('@types/react', '@types/*')).toBe(true)
  })

  test('glob pattern miss', () => {
    expect(matchesPattern('react', '@types/*')).toBe(false)
  })

  test('brace expansion match', () => {
    expect(matchesPattern('react', '{react,react-dom}')).toBe(true)
    expect(matchesPattern('react-dom', '{react,react-dom}')).toBe(true)
  })

  test('regex match', () => {
    expect(matchesPattern('react-native', '/^react-.*/')).toBe(true)
  })

  test('regex miss', () => {
    expect(matchesPattern('preact', '/^react-.*/')).toBe(false)
  })

  test('invalid regex returns false without throwing', () => {
    expect(matchesPattern('test', '/[invalid/')).toBe(false)
  })
})

// ─── resolvePolicy ────────────────────────────────────────────────────

describe('resolvePolicy', () => {
  test('no matching rules returns empty policy', () => {
    const update = makeUpdate('lodash')
    const rules: PackageRule[] = [{ match: 'react', automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy).toEqual({})
  })

  test('single matching rule with automerge', () => {
    const update = makeUpdate('react')
    const rules: PackageRule[] = [{ match: 'react', automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy).toEqual({ automerge: true })
  })

  test('later rule overrides earlier (last-match-wins)', () => {
    const update = makeUpdate('react')
    const rules: PackageRule[] = [
      { match: '*', automerge: true },
      { match: 'react', automerge: false },
    ]
    const policy = resolvePolicy(update, rules)
    expect(policy.automerge).toBe(false)
  })

  test('partial override preserves earlier fields', () => {
    const update = makeUpdate('react')
    const rules: PackageRule[] = [
      { match: '*', automerge: true, group: 'all' },
      { match: 'react', ignore: true },
    ]
    const policy = resolvePolicy(update, rules)
    expect(policy.automerge).toBe(true)
    expect(policy.group).toBe('all')
    expect(policy.ignore).toBe(true)
  })

  test('updateTypes constraint: rule with updateTypes patch does not match minor update', () => {
    const update = makeUpdate('react', { updateType: 'minor' })
    const rules: PackageRule[] = [{ match: 'react', updateTypes: ['patch'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy).toEqual({})
  })

  test('updateTypes constraint: rule with updateTypes patch matches patch update', () => {
    const update = makeUpdate('react', { updateType: 'patch' })
    const rules: PackageRule[] = [{ match: 'react', updateTypes: ['patch'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy.automerge).toBe(true)
  })

  test('depTypes constraint: devDependencies matches isDev=true', () => {
    const update = makeUpdate('vitest', { isDev: true })
    const rules: PackageRule[] = [{ match: '*', depTypes: ['devDependencies'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy.automerge).toBe(true)
  })

  test('depTypes constraint: dependencies does not match isDev=true', () => {
    const update = makeUpdate('vitest', { isDev: true })
    const rules: PackageRule[] = [{ match: '*', depTypes: ['dependencies'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy).toEqual({})
  })

  test('depTypes constraint: dependencies matches isDev=undefined', () => {
    const update = makeUpdate('react')
    const rules: PackageRule[] = [{ match: '*', depTypes: ['dependencies'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy.automerge).toBe(true)
  })

  test('array match patterns: matches if any pattern hits', () => {
    const update = makeUpdate('react')
    const rules: PackageRule[] = [{ match: ['lodash', 'react'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy.automerge).toBe(true)
  })

  test('array match patterns: no match if none hit', () => {
    const update = makeUpdate('vite')
    const rules: PackageRule[] = [{ match: ['lodash', 'react'], automerge: true }]
    const policy = resolvePolicy(update, rules)
    expect(policy).toEqual({})
  })
})

// ─── applyPackageRules ─────────────────────────────────────────────────

describe('applyPackageRules', () => {
  test('undefined rules returns all updates unchanged with empty policies map', () => {
    const updates = [makeUpdate('react'), makeUpdate('vite')]
    const result = applyPackageRules(updates, undefined)
    expect(result.updates).toEqual(updates)
    expect(result.policies.size).toBe(0)
  })

  test('empty rules array returns all updates unchanged with empty policies map', () => {
    const updates = [makeUpdate('react'), makeUpdate('vite')]
    const result = applyPackageRules(updates, [])
    expect(result.updates).toEqual(updates)
    expect(result.policies.size).toBe(0)
  })

  test('ignore action removes package from updates list', () => {
    const updates = [makeUpdate('react'), makeUpdate('vite')]
    const rules: PackageRule[] = [{ match: 'react', ignore: true }]
    const result = applyPackageRules(updates, rules)
    expect(result.updates).toHaveLength(1)
    expect(result.updates[0]!.name).toBe('vite')
  })

  test('allowedVersions filters out update outside range', () => {
    const updates = [makeUpdate('react', { toVersion: '19.0.0' })]
    const rules: PackageRule[] = [{ match: 'react', allowedVersions: '^18' }]
    const result = applyPackageRules(updates, rules)
    expect(result.updates).toHaveLength(0)
  })

  test('allowedVersions keeps update within range', () => {
    const updates = [makeUpdate('react', { toVersion: '18.3.0' })]
    const rules: PackageRule[] = [{ match: 'react', allowedVersions: '^18' }]
    const result = applyPackageRules(updates, rules)
    expect(result.updates).toHaveLength(1)
  })

  test('pin action sets allowedVersions to ~major.minor from fromVersion', () => {
    const updates = [makeUpdate('react', { fromVersion: '18.2.0', toVersion: '19.0.0' })]
    const rules: PackageRule[] = [{ match: 'react', pin: true }]
    const result = applyPackageRules(updates, rules)
    const policy = result.policies.get('react')
    expect(policy?.allowedVersions).toBe('~18.2')
    // Pin should filter out version 19.0.0 since it does not satisfy ~18.2
    expect(result.updates).toHaveLength(0)
  })

  test('pin action keeps update within pinned range', () => {
    const updates = [makeUpdate('react', { fromVersion: '18.2.0', toVersion: '18.2.5' })]
    const rules: PackageRule[] = [{ match: 'react', pin: true }]
    const result = applyPackageRules(updates, rules)
    expect(result.updates).toHaveLength(1)
    expect(result.policies.get('react')?.allowedVersions).toBe('~18.2')
  })

  test('group action stored in policy but no filtering effect', () => {
    const updates = [makeUpdate('react'), makeUpdate('react-dom')]
    const rules: PackageRule[] = [{ match: '{react,react-dom}', group: 'react-core' }]
    const result = applyPackageRules(updates, rules)
    expect(result.updates).toHaveLength(2)
    expect(result.policies.get('react')?.group).toBe('react-core')
    expect(result.policies.get('react-dom')?.group).toBe('react-core')
  })

  test('automerge action stored in policy but no filtering effect', () => {
    const updates = [makeUpdate('react')]
    const rules: PackageRule[] = [{ match: '*', automerge: true }]
    const result = applyPackageRules(updates, rules)
    expect(result.updates).toHaveLength(1)
    expect(result.policies.get('react')?.automerge).toBe(true)
  })

  test('logs ignored packages when logger provided', () => {
    const { logger, messages } = createMockLogger()
    const updates = [makeUpdate('react')]
    const rules: PackageRule[] = [{ match: 'react', ignore: true }]
    applyPackageRules(updates, rules, logger)
    expect(messages.some((m) => m.includes('react') && m.includes('ignore'))).toBe(true)
  })

  test('logs version-rejected packages when logger provided', () => {
    const { logger, messages } = createMockLogger()
    const updates = [makeUpdate('react', { toVersion: '19.0.0' })]
    const rules: PackageRule[] = [{ match: 'react', allowedVersions: '^18' }]
    applyPackageRules(updates, rules, logger)
    expect(messages.some((m) => m.includes('react') && m.includes('version'))).toBe(true)
  })

  test('policies map contains resolved policy for non-filtered packages', () => {
    const updates = [makeUpdate('react'), makeUpdate('vite')]
    const rules: PackageRule[] = [{ match: 'react', automerge: true }]
    const result = applyPackageRules(updates, rules)
    expect(result.policies.has('react')).toBe(true)
    expect(result.policies.get('react')?.automerge).toBe(true)
    // vite has no matching rule so policy is empty
    expect(result.policies.get('vite')).toEqual({})
  })
})

// ─── resolveAutoMerge ──────────────────────────────────────────────────

describe('resolveAutoMerge', () => {
  test('empty policies map falls back to global shouldAutoMerge', () => {
    const policies = new Map<string, ResolvedPackagePolicy>()
    const updates = [makeUpdate('react', { updateType: 'patch' })]
    // Global mode 'patch' + all patch updates => true
    expect(resolveAutoMerge(policies, 'patch', updates)).toBe(true)
  })

  test('empty policies with global mode none returns false', () => {
    const policies = new Map<string, ResolvedPackagePolicy>()
    const updates = [makeUpdate('react', { updateType: 'patch' })]
    expect(resolveAutoMerge(policies, 'none', updates)).toBe(false)
  })

  test('all packages have automerge: true returns true regardless of global mode', () => {
    const policies = new Map<string, ResolvedPackagePolicy>([
      ['react', { automerge: true }],
      ['vite', { automerge: true }],
    ])
    const updates = [
      makeUpdate('react', { updateType: 'major' }),
      makeUpdate('vite', { updateType: 'major' }),
    ]
    // Global mode 'none' but all policies say automerge: true => true
    expect(resolveAutoMerge(policies, 'none', updates)).toBe(true)
  })

  test('any package has automerge: false returns false regardless of global mode', () => {
    const policies = new Map<string, ResolvedPackagePolicy>([
      ['react', { automerge: true }],
      ['vite', { automerge: false }],
    ])
    const updates = [
      makeUpdate('react', { updateType: 'patch' }),
      makeUpdate('vite', { updateType: 'patch' }),
    ]
    expect(resolveAutoMerge(policies, 'patch', updates)).toBe(false)
  })

  test('mixed: some with automerge true, some without automerge set falls back to global', () => {
    const policies = new Map<string, ResolvedPackagePolicy>([
      ['react', { automerge: true }],
      ['vite', { group: 'tools' }], // no automerge field
    ])
    const updates = [
      makeUpdate('react', { updateType: 'patch' }),
      makeUpdate('vite', { updateType: 'patch' }),
    ]
    // Only react explicitly sets automerge; vite does not => mixed => fall back to global
    // Global mode 'patch' + all patch updates => true
    expect(resolveAutoMerge(policies, 'patch', updates)).toBe(true)
  })

  test('no policies set automerge falls back to global shouldAutoMerge', () => {
    const policies = new Map<string, ResolvedPackagePolicy>([
      ['react', { group: 'core' }],
      ['vite', { group: 'tools' }],
    ])
    const updates = [
      makeUpdate('react', { updateType: 'minor' }),
      makeUpdate('vite', { updateType: 'minor' }),
    ]
    // Global mode 'patch' + minor updates => false (minor > patch threshold)
    expect(resolveAutoMerge(policies, 'patch', updates)).toBe(false)
  })
})
