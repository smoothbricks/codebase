import { describe, expect, test } from 'bun:test';
import type { PackageJson } from '../lib/json.js';
import { runtimeTypesRangeForPublishedVersions, validateRuntimePins } from './runtime.js';

describe('runtimeTypesRangeForPublishedVersions', () => {
  test('anchors a caret range at the newest published version in the installed Node major', () => {
    expect(
      runtimeTypesRangeForPublishedVersions('@types/node', '24.12.0', 'major', ['24.0.0', '24.12.4', '25.9.1']),
    ).toBe('^24.12.4');
  });

  test('rejects fallback types from a different Node major', () => {
    expect(() =>
      runtimeTypesRangeForPublishedVersions('@types/node', '26.0.0', 'major', ['24.12.4', '25.9.0', '25.9.1']),
    ).toThrow('runtime major 26');
  });

  test('uses the installed Bun version when @types/bun has published it', () => {
    expect(runtimeTypesRangeForPublishedVersions('@types/bun', '1.3.14', 'exact', ['1.3.13', '1.3.14'])).toBe('1.3.14');
  });

  test('falls back to latest published @types/bun when the Bun version is unpublished', () => {
    expect(runtimeTypesRangeForPublishedVersions('@types/bun', '1.3.15', 'exact', ['1.3.13', '1.3.14'])).toBe('1.3.14');
  });

  test('ignores non-stable version strings when choosing the same-major anchor', () => {
    expect(
      runtimeTypesRangeForPublishedVersions('@types/node', '26.0.0', 'major', ['25.9.1', '26.0.2', '26.1.0-beta.1']),
    ).toBe('^26.0.2');
  });
});

describe('validateRuntimePins', () => {
  const runtime = { node: '24.16.0', bun: '1.3.14' };
  const aligned = () => ({
    engines: { node: '>=24.0.0' },
    packageManager: 'bun@1.3.14',
    devDependencies: { '@types/node': '^24.13.0' },
  });

  test('accepts caret ranges anchored at any patch or minor in the PATH Node major', () => {
    for (const version of ['^24.0.3', '^24.13.0']) {
      const pkg = aligned();
      pkg.devDependencies['@types/node'] = version;
      expect(validateRuntimePins(pkg, runtime)).toBe(0);
    }
  });

  test('fails when @types/node tracks a different major than the PATH node', () => {
    const pkg = aligned();
    pkg.devDependencies['@types/node'] = '^26.1.1';
    expect(validateRuntimePins(pkg, runtime)).toBe(1);
  });

  test('rejects exact and tilde @types/node pins even when the major matches', () => {
    for (const version of ['24.13.0', '~24.13.0']) {
      const pkg = aligned();
      pkg.devDependencies['@types/node'] = version;
      expect(validateRuntimePins(pkg, runtime)).toBe(1);
    }
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
