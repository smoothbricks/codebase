/// <reference types="bun" />

import { isRecord } from '../guards.js';

export type ExportConditions = {
  [key: string]: string | undefined;
  development?: string;
  types?: string;
  import?: string;
  default?: string;
  bun?: string;
  node?: string;
  require?: string;
};

export type PackageManifest = {
  name: string;
  exports: Record<string, string | ExportConditions>;
};

export function assertPackageManifest(value: unknown): asserts value is PackageManifest {
  if (!isRecord(value)) {
    throw new TypeError('package manifest must be an object');
  }

  if (typeof value.name !== 'string') {
    throw new TypeError('package manifest must include a string name');
  }

  if (!isRecord(value.exports)) {
    throw new TypeError('package manifest must include an exports map');
  }
}

export function getExportConditions(manifest: PackageManifest, subpath: string): ExportConditions {
  const entry = manifest.exports[subpath];

  if (!isRecord(entry)) {
    throw new TypeError(`export ${subpath} must use conditional exports`);
  }

  const conditions: ExportConditions = {};

  for (const [key, value] of Object.entries(entry)) {
    if (typeof value !== 'string') {
      throw new TypeError(`export ${subpath} condition ${key} must be a string`);
    }

    conditions[key] = value;
  }

  return conditions;
}

export function pickTarget(conditions: ExportConditions, subpath: string, keys: readonly string[]): string {
  for (const key of keys) {
    const value = conditions[key];
    if (value !== undefined) {
      return value;
    }
  }

  throw new TypeError(`export ${subpath} must declare one of: ${keys.join(', ')}`);
}

export function getImportTarget(conditions: ExportConditions, subpath: string): string {
  return pickTarget(conditions, subpath, ['import', 'default', 'development']);
}

export function getTypesTarget(conditions: ExportConditions, subpath: string): string {
  return pickTarget(conditions, subpath, ['types']);
}

export async function readPackageManifest(packageUrl: URL): Promise<PackageManifest> {
  const manifest = await Bun.file(packageUrl).json();
  assertPackageManifest(manifest);
  return manifest;
}

export async function importDeclaredModule(
  manifest: PackageManifest,
  packageUrl: URL,
  subpath: string,
  keys: readonly string[] = ['import', 'default', 'development'],
): Promise<Record<string, unknown>> {
  const conditions = getExportConditions(manifest, subpath);
  const target = pickTarget(conditions, subpath, keys);
  return import(new URL(target, packageUrl).href);
}
