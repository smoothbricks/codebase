import { describe, expect, test } from 'bun:test';
import { runtimeTypesRangeForPublishedVersions } from './runtime.js';

describe('runtimeTypesRangeForPublishedVersions', () => {
  test('uses the installed Node major when @types/node has published it', () => {
    expect(
      runtimeTypesRangeForPublishedVersions('@types/node', '24.12.0', 'major', ['24.0.0', '24.12.4', '25.9.1']),
    ).toBe('~24.0.0');
  });

  test('falls back to latest published @types/node when the Node major is unpublished', () => {
    expect(
      runtimeTypesRangeForPublishedVersions('@types/node', '26.0.0', 'major', ['24.12.4', '25.9.0', '25.9.1']),
    ).toBe('~25.9.1');
  });

  test('uses the installed Bun version when @types/bun has published it', () => {
    expect(runtimeTypesRangeForPublishedVersions('@types/bun', '1.3.14', 'exact', ['1.3.13', '1.3.14'])).toBe('1.3.14');
  });

  test('falls back to latest published @types/bun when the Bun version is unpublished', () => {
    expect(runtimeTypesRangeForPublishedVersions('@types/bun', '1.3.15', 'exact', ['1.3.13', '1.3.14'])).toBe('1.3.14');
  });

  test('ignores non-stable version strings when choosing the fallback', () => {
    expect(runtimeTypesRangeForPublishedVersions('@types/node', '26.0.0', 'major', ['25.9.1', '26.0.0-beta.1'])).toBe(
      '~25.9.1',
    );
  });
});
