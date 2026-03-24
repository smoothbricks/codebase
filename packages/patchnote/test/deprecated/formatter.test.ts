/**
 * Unit tests for deprecated package formatter
 * Tests PR description deprecation warning formatting
 */

import { describe, expect, test } from 'bun:test';
import { formatDeprecationWarnings } from '../../src/deprecated/formatter.js';
import type { PackageUpdate } from '../../src/types.js';

/** Helper: create a minimal PackageUpdate fixture */
function makeUpdate(name: string, overrides?: Partial<PackageUpdate>): PackageUpdate {
  return {
    name,
    fromVersion: '1.0.0',
    toVersion: '2.0.0',
    updateType: 'major',
    ecosystem: 'npm',
    ...overrides,
  };
}

describe('formatDeprecationWarnings', () => {
  test('returns empty string when no deprecated packages', () => {
    const updates = [makeUpdate('react'), makeUpdate('lodash')];
    expect(formatDeprecationWarnings(updates)).toBe('');
  });

  test('returns markdown section with heading for deprecated packages', () => {
    const updates = [
      makeUpdate('old-pkg', {
        fromVersion: '1.0.0',
        toVersion: '1.0.1',
        deprecatedMessage: 'This package has been deprecated',
      }),
    ];

    const result = formatDeprecationWarnings(updates);

    expect(result).toContain('### Deprecated Packages');
    expect(result).toContain('deprecated in the npm registry');
    expect(result).toContain('**old-pkg** 1.0.0 -> 1.0.1: This package has been deprecated');
  });

  test('includes deprecation message for each deprecated package', () => {
    const updates = [
      makeUpdate('pkg-a', {
        fromVersion: '1.0.0',
        toVersion: '2.0.0',
        deprecatedMessage: 'Use pkg-b instead',
      }),
      makeUpdate('pkg-c', {
        fromVersion: '3.0.0',
        toVersion: '4.0.0',
        deprecatedMessage: 'Deprecated in favor of pkg-d',
      }),
      makeUpdate('safe-pkg'),
    ];

    const result = formatDeprecationWarnings(updates);

    expect(result).toContain('**pkg-a** 1.0.0 -> 2.0.0: Use pkg-b instead');
    expect(result).toContain('**pkg-c** 3.0.0 -> 4.0.0: Deprecated in favor of pkg-d');
    expect(result).not.toContain('safe-pkg');
  });

  test('includes replacement suggestion arrow when replacementName is set', () => {
    const updates = [
      makeUpdate('babel-eslint', {
        fromVersion: '10.1.0',
        toVersion: '10.2.0',
        deprecatedMessage: 'Use @babel/eslint-parser instead',
        replacementName: '@babel/eslint-parser',
        replacementVersion: '7.11.0',
      }),
    ];

    const result = formatDeprecationWarnings(updates);

    expect(result).toContain(
      '**babel-eslint** 10.1.0 -> 10.2.0: Use @babel/eslint-parser instead -> **@babel/eslint-parser**@7.11.0',
    );
  });

  test('returns empty string for empty array', () => {
    expect(formatDeprecationWarnings([])).toBe('');
  });
});
