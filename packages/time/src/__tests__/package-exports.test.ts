import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  getImportTarget,
  getTypesTarget,
  importDeclaredModule,
  readPackageManifest,
} from '../../../validation/src/testing/index.js';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares the published root export', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const conditions = getExportConditions(manifest, '.');

    expect(manifest.name).toBe('@smoothbricks/time');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(conditions.development).toBe('./src/index.ts');
    expect(conditions.types).toBe('./dist/index.d.ts');
    expect(conditions.import).toBe('./dist/index.js');
    expect(conditions.default).toBe('./dist/index.js');
    expect(await Bun.file(new URL(getTypesTarget(conditions, '.'), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(getImportTarget(conditions, '.'), packageUrl)).exists()).toBe(true);
  });

  it('loads the published root target truthfully', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.');

    expect(Object.keys(mod).sort()).toEqual([
      'EpochMicros',
      'EpochMillis',
      'TIME_BOUNDARIES',
      'dateToMicros',
      'dateToMillis',
      'epochMicrosToMillis',
      'epochMillisToMicros',
      'isTimeBoundary',
      'microsToDate',
      'microsToISODate',
      'millisToDate',
      'nowMicros',
      'nowMillis',
    ]);
  });
});
