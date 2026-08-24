import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { type PackageJson, parsePackageJsonText } from '../lib/json.js';

/**
 * publishConfig fields that override their top-level counterparts in the
 * published manifest (pnpm's publish_config contract). `access` and other
 * publisher directives are not manifest fields and stay where they are.
 */
export const PUBLISH_OVERRIDE_FIELDS = ['main', 'module', 'types', 'browser', 'bin', 'exports', 'imports'] as const;

export type PublishOverrideField = (typeof PUBLISH_OVERRIDE_FIELDS)[number];

/**
 * Apply pnpm-style publishConfig field overrides to a manifest: each override
 * replaces its top-level field and is removed from publishConfig, so the
 * packed artifact carries the published shape and no dangling override.
 * Returns the input untouched when there is nothing to apply.
 */
export function applyPublishConfigOverrides(pkg: PackageJson): { manifest: PackageJson; applied: string[] } {
  const overrides = pkg.publishConfig;
  const applied: PublishOverrideField[] = [];
  if (!overrides) {
    return { manifest: pkg, applied };
  }
  const manifest: PackageJson = { ...pkg, publishConfig: { ...overrides } };
  for (const field of PUBLISH_OVERRIDE_FIELDS) {
    if (overrides[field] === undefined) {
      continue;
    }
    (manifest as Record<string, unknown>)[field] = overrides[field];
    delete (manifest.publishConfig as Record<string, unknown>)[field];
    applied.push(field);
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
