import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { type PackageJson, type PackagePublishConfig, parsePackageJsonText } from '../lib/json.js';

/**
 * Apply pnpm-style publishConfig field overrides to a manifest: each override
 * replaces its top-level field and is removed from publishConfig, so the
 * packed artifact carries the published shape and no dangling override.
 * `access` and other publisher directives are not manifest fields and stay put.
 * Returns the input untouched when there is nothing to apply.
 */
export function applyPublishConfigOverrides(pkg: PackageJson): { manifest: PackageJson; applied: string[] } {
  const overrides = pkg.publishConfig;
  const applied: string[] = [];
  if (!overrides) {
    return { manifest: pkg, applied };
  }
  const publishConfig: PackagePublishConfig = { ...overrides };
  const manifest: PackageJson = { ...pkg, publishConfig };
  // One branch per overridable field keeps every assignment fully typed —
  // PackageJson and PackagePublishConfig deliberately share these signatures.
  if (overrides.main !== undefined) {
    manifest.main = overrides.main;
    delete publishConfig.main;
    applied.push('main');
  }
  if (overrides.module !== undefined) {
    manifest.module = overrides.module;
    delete publishConfig.module;
    applied.push('module');
  }
  if (overrides.types !== undefined) {
    manifest.types = overrides.types;
    delete publishConfig.types;
    applied.push('types');
  }
  if (overrides.browser !== undefined) {
    manifest.browser = overrides.browser;
    delete publishConfig.browser;
    applied.push('browser');
  }
  if (overrides.bin !== undefined) {
    manifest.bin = overrides.bin;
    delete publishConfig.bin;
    applied.push('bin');
  }
  if (overrides.exports !== undefined) {
    manifest.exports = overrides.exports;
    delete publishConfig.exports;
    applied.push('exports');
  }
  if (overrides.imports !== undefined) {
    manifest.imports = overrides.imports;
    delete publishConfig.imports;
    applied.push('imports');
  }
  return { manifest, applied };
}

/**
 * Run `fn` (a `bun pm pack`) with the package's on-disk manifest rewritten to
 * its published shape, restoring the original bytes afterwards — the same
 * rewrite-around-pack pattern syncBunLockfileVersions uses for bun.lock.
 * Needed because `bun pm pack` (unlike pnpm) never applies publishConfig
 * field overrides itself.
 */
export async function withPublishManifest<T>(
  packageDir: string,
  fn: () => Promise<T>,
  options: { log?: boolean } = {},
): Promise<T> {
  const manifestPath = join(packageDir, 'package.json');
  const originalText = readFileSync(manifestPath, 'utf8');
  const parsed = parsePackageJsonText(originalText);
  if (!parsed) {
    // A manifest smoo cannot parse: pack anyway and let bun report it.
    return fn();
  }
  const { manifest, applied } = applyPublishConfigOverrides(parsed);
  if (applied.length === 0) {
    return fn();
  }
  if (options.log) {
    console.log(`${manifest.name}: applying publishConfig overrides for pack: ${applied.join(', ')}`);
  }
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  try {
    return await fn();
  } finally {
    writeFileSync(manifestPath, originalText);
  }
}
