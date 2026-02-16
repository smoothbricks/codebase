import { describe, expect, it } from 'bun:test';

type ExportConditions = {
  development?: string;
  types?: string;
  import?: string;
  default?: string;
};

function expectCoreConditions(entry: unknown): asserts entry is ExportConditions {
  expect(typeof entry).toBe('object');
  expect(entry).not.toBeNull();
  expect(typeof (entry as ExportConditions).development).toBe('string');
  expect(typeof (entry as ExportConditions).types).toBe('string');
  expect(typeof (entry as ExportConditions).import).toBe('string');
  expect(typeof (entry as ExportConditions).default).toBe('string');
}

describe('package export conditions', () => {
  it('lmao package exposes root + platform entry points with expected conditions', async () => {
    const pkg = (await Bun.file(new URL('../../../package.json', import.meta.url)).json()) as {
      exports: Record<string, unknown>;
    };

    expectCoreConditions(pkg.exports['.']);
    expectCoreConditions(pkg.exports['./node']);
    expectCoreConditions(pkg.exports['./es']);

    expect(pkg.exports['./package.json']).toBe('./package.json');
  });

  it('arrow-builder package root export has expected conditions', async () => {
    const pkg = (await Bun.file(new URL('../../../../arrow-builder/package.json', import.meta.url)).json()) as {
      exports: Record<string, unknown>;
    };

    expectCoreConditions(pkg.exports['.']);
    expect(pkg.exports['./package.json']).toBe('./package.json');
  });
});
