import { describe, expect, test } from 'bun:test';
import type { PackageJson } from '../lib/json.js';
import { runtimeTypesRangeForPublishedVersions, validateRuntimePins } from './runtime.js';

describe('runtimeTypesRangeForPublishedVersions', () => {
  test('uses the newest published minor within the installed Node major, never the ~major.0.0 floor', () => {
    expect(
      runtimeTypesRangeForPublishedVersions('@types/node', '24.12.0', 'major', ['24.0.0', '24.12.4', '25.9.1']),
    ).toBe('~24.12.4');
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

describe('validateRuntimePins', () => {
  const runtime = { node: '24.16.0', bun: '1.3.14' };
  const aligned = () => ({
    engines: { node: '>=24.0.0' },
    packageManager: 'bun@1.3.14',
    devDependencies: { '@types/node': '~24.13.0' },
  });

  test('passes when every pin agrees with the PATH runtimes', () => {
    expect(validateRuntimePins(aligned(), runtime)).toBe(0);
  });

  test('fails when @types/node tracks a different major than the PATH node', () => {
    const pkg = aligned();
    pkg.devDependencies['@types/node'] = '~26.1.1';
    expect(validateRuntimePins(pkg, runtime)).toBe(1);
  });

  test('fails the ~major.0.0 floor pin even when the major matches', () => {
    const pkg = aligned();
    pkg.devDependencies['@types/node'] = '~24.0.0';
    expect(validateRuntimePins(pkg, runtime)).toBe(1);
  });

  test('fails engines.node and packageManager drift against the PATH runtimes', () => {
    const pkg = aligned();
    pkg.engines.node = '>=22.0.0';
    pkg.packageManager = 'bun@1.2.0';
    expect(validateRuntimePins(pkg, runtime)).toBe(2);
  });

  test('fails a missing @types/node pin', () => {
    const pkg: PackageJson = aligned();
    pkg.devDependencies = {};
    expect(validateRuntimePins(pkg, runtime)).toBe(1);
  });
});
