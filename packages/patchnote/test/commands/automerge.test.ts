import { describe, expect, test } from 'bun:test'
import { getMaxUpdateSeverity, shouldAutoMerge } from '../../src/commands/update-deps.js'
import type { PackageUpdate } from '../../src/types.js'

function makeUpdate(updateType: 'major' | 'minor' | 'patch' | 'unknown'): PackageUpdate {
  return {
    name: `test-pkg-${updateType}`,
    fromVersion: '1.0.0',
    toVersion: '2.0.0',
    updateType,
    ecosystem: 'npm',
  }
}

describe('shouldAutoMerge', () => {
  test('should return false when mode is none', () => {
    expect(shouldAutoMerge('none', [makeUpdate('patch')])).toBe(false)
  })

  test('should return true for patch-only updates when mode is patch', () => {
    expect(shouldAutoMerge('patch', [makeUpdate('patch')])).toBe(true)
  })

  test('should return true for multiple patch updates when mode is patch', () => {
    expect(shouldAutoMerge('patch', [makeUpdate('patch'), makeUpdate('patch')])).toBe(true)
  })

  test('should return false for minor updates when mode is patch', () => {
    expect(shouldAutoMerge('patch', [makeUpdate('minor')])).toBe(false)
  })

  test('should return false for mixed patch+minor when mode is patch', () => {
    expect(shouldAutoMerge('patch', [makeUpdate('patch'), makeUpdate('minor')])).toBe(false)
  })

  test('should return true for patch updates when mode is minor', () => {
    expect(shouldAutoMerge('minor', [makeUpdate('patch')])).toBe(true)
  })

  test('should return true for minor updates when mode is minor', () => {
    expect(shouldAutoMerge('minor', [makeUpdate('minor')])).toBe(true)
  })

  test('should return true for mixed patch+minor when mode is minor', () => {
    expect(shouldAutoMerge('minor', [makeUpdate('patch'), makeUpdate('minor')])).toBe(true)
  })

  test('should return false for major updates regardless of mode', () => {
    expect(shouldAutoMerge('minor', [makeUpdate('major')])).toBe(false)
    expect(shouldAutoMerge('patch', [makeUpdate('major')])).toBe(false)
  })

  test('should return false for mixed minor+major when mode is minor', () => {
    expect(shouldAutoMerge('minor', [makeUpdate('minor'), makeUpdate('major')])).toBe(false)
  })

  test('should return false when any update has unknown type', () => {
    expect(shouldAutoMerge('minor', [makeUpdate('unknown')])).toBe(false)
    expect(shouldAutoMerge('patch', [makeUpdate('unknown')])).toBe(false)
    expect(shouldAutoMerge('minor', [makeUpdate('patch'), makeUpdate('unknown')])).toBe(false)
  })
})

describe('getMaxUpdateSeverity', () => {
  test('should return patch for empty array', () => {
    expect(getMaxUpdateSeverity([])).toBe('patch')
  })

  test('should return patch for single patch update', () => {
    expect(getMaxUpdateSeverity([makeUpdate('patch')])).toBe('patch')
  })

  test('should return minor for single minor update', () => {
    expect(getMaxUpdateSeverity([makeUpdate('minor')])).toBe('minor')
  })

  test('should return major for single major update', () => {
    expect(getMaxUpdateSeverity([makeUpdate('major')])).toBe('major')
  })

  test('should return unknown for single unknown update', () => {
    expect(getMaxUpdateSeverity([makeUpdate('unknown')])).toBe('unknown')
  })

  test('should return highest severity for mixed types', () => {
    expect(getMaxUpdateSeverity([makeUpdate('patch'), makeUpdate('minor')])).toBe('minor')
    expect(getMaxUpdateSeverity([makeUpdate('patch'), makeUpdate('major')])).toBe('major')
    expect(getMaxUpdateSeverity([makeUpdate('minor'), makeUpdate('major')])).toBe('major')
  })

  test('should return unknown when any update is unknown', () => {
    expect(getMaxUpdateSeverity([makeUpdate('patch'), makeUpdate('unknown')])).toBe('unknown')
  })
})
