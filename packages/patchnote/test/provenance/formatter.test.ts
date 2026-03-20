/**
 * Unit tests for provenance formatter
 * Tests PR description warning formatting
 */

import { describe, expect, test } from 'bun:test'
import { formatProvenanceWarnings } from '../../src/provenance/formatter.js'
import type { PackageUpdate } from '../../src/types.js'

/** Helper: create a minimal PackageUpdate fixture */
function makeUpdate(name: string, overrides?: Partial<PackageUpdate>): PackageUpdate {
  return {
    name,
    fromVersion: '1.0.0',
    toVersion: '2.0.0',
    updateType: 'major',
    ecosystem: 'npm',
    ...overrides,
  }
}

describe('formatProvenanceWarnings', () => {
  test('returns empty string when no packages have provenanceDowngraded', () => {
    const updates = [
      makeUpdate('react'),
      makeUpdate('lodash'),
    ]
    expect(formatProvenanceWarnings(updates)).toBe('')
  })

  test('formats single downgrade warning', () => {
    const updates = [
      makeUpdate('compromised-pkg', {
        fromVersion: '1.0.0',
        toVersion: '1.0.1',
        provenanceDowngraded: true,
      }),
      makeUpdate('safe-pkg'),
    ]

    const result = formatProvenanceWarnings(updates)

    expect(result).toContain('### !! Supply Chain Warning: Provenance Downgrade')
    expect(result).toContain('compromised publish pipeline')
    expect(result).toContain('**compromised-pkg**: 1.0.0 (provenance) -> 1.0.1 (no provenance)')
    expect(result).not.toContain('safe-pkg')
  })

  test('formats multiple downgrade warnings', () => {
    const updates = [
      makeUpdate('pkg-a', { fromVersion: '1.0.0', toVersion: '1.0.1', provenanceDowngraded: true }),
      makeUpdate('pkg-b', { fromVersion: '2.0.0', toVersion: '3.0.0', provenanceDowngraded: true }),
      makeUpdate('safe-pkg'),
    ]

    const result = formatProvenanceWarnings(updates)

    expect(result).toContain('**pkg-a**: 1.0.0 (provenance) -> 1.0.1 (no provenance)')
    expect(result).toContain('**pkg-b**: 2.0.0 (provenance) -> 3.0.0 (no provenance)')
    expect(result).not.toContain('safe-pkg')
  })

  test('returns empty string for empty updates array', () => {
    expect(formatProvenanceWarnings([])).toBe('')
  })
})
