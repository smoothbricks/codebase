/**
 * Tests for update grouping/batching logic
 */

import { describe, expect, test } from 'bun:test';
import { partitionUpdates } from '../src/grouping.js';
import type { GroupingConfig, PackageUpdate } from '../src/types.js';

/** Helper factory for creating PackageUpdate test data */
function makeUpdate(overrides: Partial<PackageUpdate> = {}): PackageUpdate {
  return {
    name: overrides.name ?? 'some-package',
    fromVersion: overrides.fromVersion ?? '1.0.0',
    toVersion: overrides.toVersion ?? '1.1.0',
    updateType: overrides.updateType ?? 'minor',
    ecosystem: overrides.ecosystem ?? 'npm',
    ...overrides,
  };
}

describe('partitionUpdates', () => {
  test('with no config returns all updates in a single "default" group', () => {
    const updates = [makeUpdate({ name: 'foo' }), makeUpdate({ name: 'bar' })];

    const result = partitionUpdates(updates, undefined);

    expect(result.size).toBe(1);
    expect(result.has('default')).toBe(true);
    expect(result.get('default')).toHaveLength(2);
  });

  test('with empty config returns all updates in "default" group', () => {
    const updates = [makeUpdate({ name: 'foo' })];
    const config: GroupingConfig = {};

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(1);
    expect(result.get('default')).toHaveLength(1);
  });

  test('with separateMajor=true moves major updates to "major" group', () => {
    const updates = [
      makeUpdate({ name: 'major-pkg', updateType: 'major' }),
      makeUpdate({ name: 'minor-pkg', updateType: 'minor' }),
      makeUpdate({ name: 'patch-pkg', updateType: 'patch' }),
    ];
    const config: GroupingConfig = { separateMajor: true };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(2);
    expect(result.get('major')).toHaveLength(1);
    expect(result.get('major')![0]!.name).toBe('major-pkg');
    expect(result.get('default')).toHaveLength(2);
  });

  test('with separateMinorPatch=true splits into minor, patch, and default groups', () => {
    const updates = [
      makeUpdate({ name: 'minor-pkg', updateType: 'minor' }),
      makeUpdate({ name: 'patch-pkg', updateType: 'patch' }),
      makeUpdate({ name: 'unknown-pkg', updateType: 'unknown' }),
    ];
    const config: GroupingConfig = { separateMinorPatch: true };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(3);
    expect(result.get('minor')).toHaveLength(1);
    expect(result.get('minor')![0]!.name).toBe('minor-pkg');
    expect(result.get('patch')).toHaveLength(1);
    expect(result.get('patch')![0]!.name).toBe('patch-pkg');
    expect(result.get('default')).toHaveLength(1);
    expect(result.get('default')![0]!.name).toBe('unknown-pkg');
  });

  test('with both separateMajor and separateMinorPatch creates up to 4 groups', () => {
    const updates = [
      makeUpdate({ name: 'major-pkg', updateType: 'major' }),
      makeUpdate({ name: 'minor-pkg', updateType: 'minor' }),
      makeUpdate({ name: 'patch-pkg', updateType: 'patch' }),
      makeUpdate({ name: 'unknown-pkg', updateType: 'unknown' }),
    ];
    const config: GroupingConfig = { separateMajor: true, separateMinorPatch: true };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(4);
    expect(result.get('major')).toHaveLength(1);
    expect(result.get('minor')).toHaveLength(1);
    expect(result.get('patch')).toHaveLength(1);
    expect(result.get('default')).toHaveLength(1);
  });

  test('with name-pattern groups moves matching packages to named group', () => {
    const updates = [
      makeUpdate({ name: '@types/react', updateType: 'minor' }),
      makeUpdate({ name: '@types/node', updateType: 'patch' }),
      makeUpdate({ name: 'lodash', updateType: 'minor' }),
    ];
    const config: GroupingConfig = {
      groups: [{ name: 'types', match: '@types/*' }],
    };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(2);
    expect(result.get('types')).toHaveLength(2);
    expect(result.get('default')).toHaveLength(1);
    expect(result.get('default')![0]!.name).toBe('lodash');
  });

  test('with groups accepting string[] match patterns', () => {
    const updates = [
      makeUpdate({ name: 'eslint', updateType: 'minor' }),
      makeUpdate({ name: 'eslint-plugin-react', updateType: 'patch' }),
      makeUpdate({ name: 'prettier', updateType: 'minor' }),
      makeUpdate({ name: 'lodash', updateType: 'patch' }),
    ];
    const config: GroupingConfig = {
      groups: [{ name: 'lint', match: ['eslint*', 'prettier'] }],
    };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(2);
    expect(result.get('lint')).toHaveLength(3);
    expect(result.get('default')).toHaveLength(1);
    expect(result.get('default')![0]!.name).toBe('lodash');
  });

  test('semver grouping takes precedence over name-pattern grouping', () => {
    const updates = [
      makeUpdate({ name: '@types/react', updateType: 'major' }),
      makeUpdate({ name: '@types/node', updateType: 'minor' }),
      makeUpdate({ name: 'lodash', updateType: 'minor' }),
    ];
    const config: GroupingConfig = {
      separateMajor: true,
      groups: [{ name: 'types', match: '@types/*' }],
    };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(3);
    // Major @types/react goes to "major", not "types"
    expect(result.get('major')).toHaveLength(1);
    expect(result.get('major')![0]!.name).toBe('@types/react');
    // Minor @types/node goes to "types" (not caught by separateMajor)
    expect(result.get('types')).toHaveLength(1);
    expect(result.get('types')![0]!.name).toBe('@types/node');
    // lodash goes to default
    expect(result.get('default')).toHaveLength(1);
  });

  test('empty result groups are not included in returned Map', () => {
    const updates = [makeUpdate({ name: 'foo', updateType: 'minor' })];
    const config: GroupingConfig = {
      separateMajor: true,
      separateMinorPatch: true,
      groups: [{ name: 'types', match: '@types/*' }],
    };

    const result = partitionUpdates(updates, config);

    // Only "minor" should exist, no "major", "patch", "types", or "default"
    expect(result.size).toBe(1);
    expect(result.has('minor')).toBe(true);
    expect(result.has('major')).toBe(false);
    expect(result.has('patch')).toBe(false);
    expect(result.has('types')).toBe(false);
    expect(result.has('default')).toBe(false);
  });

  test('preserves all update data in returned groups', () => {
    const original = makeUpdate({
      name: 'foo',
      fromVersion: '1.0.0',
      toVersion: '2.0.0',
      updateType: 'major',
      ecosystem: 'npm',
      changelog: 'some changes',
      changelogUrl: 'https://example.com',
      breakingChanges: ['removed API'],
      isDev: true,
    });
    const config: GroupingConfig = { separateMajor: true };

    const result = partitionUpdates([original], config);

    const grouped = result.get('major')![0]!;
    expect(grouped).toEqual(original);
  });

  test('returns Map<string, PackageUpdate[]>', () => {
    const updates = [makeUpdate({ name: 'foo' })];
    const result = partitionUpdates(updates, undefined);

    expect(result).toBeInstanceOf(Map);
    expect(Array.isArray(result.get('default'))).toBe(true);
  });

  test('multiple name-pattern groups partition correctly', () => {
    const updates = [
      makeUpdate({ name: '@types/react', updateType: 'patch' }),
      makeUpdate({ name: 'eslint', updateType: 'minor' }),
      makeUpdate({ name: 'prettier', updateType: 'minor' }),
      makeUpdate({ name: 'lodash', updateType: 'patch' }),
    ];
    const config: GroupingConfig = {
      groups: [
        { name: 'types', match: '@types/*' },
        { name: 'lint', match: ['eslint*', 'prettier'] },
      ],
    };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(3);
    expect(result.get('types')).toHaveLength(1);
    expect(result.get('lint')).toHaveLength(2);
    expect(result.get('default')).toHaveLength(1);
  });

  test('full combination: semver + name patterns + default', () => {
    const updates = [
      makeUpdate({ name: '@types/react', updateType: 'major' }),
      makeUpdate({ name: '@types/node', updateType: 'minor' }),
      makeUpdate({ name: 'eslint', updateType: 'minor' }),
      makeUpdate({ name: 'lodash', updateType: 'patch' }),
      makeUpdate({ name: 'react', updateType: 'minor' }),
    ];
    const config: GroupingConfig = {
      separateMajor: true,
      groups: [
        { name: 'types', match: '@types/*' },
        { name: 'lint', match: 'eslint*' },
      ],
    };

    const result = partitionUpdates(updates, config);

    expect(result.size).toBe(4);
    expect(result.get('major')!.map((u) => u.name)).toEqual(['@types/react']);
    expect(result.get('types')!.map((u) => u.name)).toEqual(['@types/node']);
    expect(result.get('lint')!.map((u) => u.name)).toEqual(['eslint']);
    expect(result.get('default')!.map((u) => u.name)).toEqual(['lodash', 'react']);
  });
});
