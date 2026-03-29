import { describe, expect, it } from 'bun:test';
import { isRecord } from '@smoothbricks/validation';

type ExportConditions = {
  development?: string;
  types?: string;
  import?: string;
  default?: string;
};

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`Expected ${label} to be an object`);
  }
  return value;
}

function requireStringProperty(entry: Record<string, unknown>, key: keyof ExportConditions): string {
  const value = entry[key];
  if (typeof value !== 'string') {
    throw new Error(`Expected export condition '${key}' to be a string`);
  }
  return value;
}

function expectCoreConditions(entry: unknown): asserts entry is ExportConditions {
  const record = requireRecord(entry, 'export conditions');
  expect(requireStringProperty(record, 'development')).toBeString();
  expect(requireStringProperty(record, 'types')).toBeString();
  expect(requireStringProperty(record, 'import')).toBeString();
  expect(requireStringProperty(record, 'default')).toBeString();
}

async function readPackageExports(packageUrl: URL): Promise<Record<string, unknown>> {
  const pkg = await Bun.file(packageUrl).json();
  const pkgRecord = requireRecord(pkg, 'package.json');
  return requireRecord(pkgRecord.exports, 'package.json exports');
}

describe('package export conditions', () => {
  it('lmao package exposes root + platform entry points with expected conditions', async () => {
    const exports = await readPackageExports(new URL('../../../package.json', import.meta.url));

    expectCoreConditions(exports['.']);
    expectCoreConditions(exports['./node']);
    expectCoreConditions(exports['./es']);

    expect(exports['./package.json']).toBe('./package.json');
  });

  it('arrow-builder package root export has expected conditions', async () => {
    const exports = await readPackageExports(new URL('../../../../arrow-builder/package.json', import.meta.url));

    expectCoreConditions(exports['.']);
    expect(exports['./package.json']).toBe('./package.json');
  });
});
