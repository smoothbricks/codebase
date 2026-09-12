import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { parsePackageJsonText } from '../lib/json.js';
import { prunePublishedExports } from './published-exports.js';

export { prunePublishedExports } from './published-exports.js';

/**
 * Run `fn` (a `bun pm pack`) with the package's on-disk manifest rewritten to
 * its published shape, restoring the original bytes afterwards — the same
 * rewrite-around-pack pattern syncBunLockfileVersions uses for bun.lock.
 * A manifest transform (rather than a bun feature) because `bun pm pack`
 * offers no publish-time manifest hook.
 *
 * When `publishConfigRegistry` is set (private user-run release path), the
 * resolved literal registry is written into the packed publishConfig and the
 * workspace bytes are restored afterwards. Only a resolved literal is ever
 * written here, never an environment placeholder.
 */
export async function withPublishManifest<T>(
  packageDir: string,
  fn: () => Promise<T>,
  options: { log?: boolean; publishConfigRegistry?: string } = {},
): Promise<T> {
  const manifestPath = join(packageDir, 'package.json');
  const originalText = readFileSync(manifestPath, 'utf8');
  const parsed = parsePackageJsonText(originalText);
  if (!parsed) {
    // A manifest smoo cannot parse: pack anyway and let bun report it.
    return fn();
  }
  const { manifest, adjustments } = prunePublishedExports(parsed);
  if (options.publishConfigRegistry) {
    manifest.publishConfig = { ...(manifest.publishConfig ?? {}), registry: options.publishConfigRegistry };
  }
  const registryChanged = options.publishConfigRegistry !== undefined;
  if (adjustments.length === 0 && !registryChanged) {
    return fn();
  }
  if (options.log) {
    if (adjustments.length > 0) {
      console.log(`${manifest.name}: adjusting published exports for pack: ${adjustments.join(', ')}`);
    }
    if (registryChanged) {
      console.log(`${manifest.name}: setting publishConfig.registry for pack`);
    }
  }
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  try {
    return await fn();
  } finally {
    writeFileSync(manifestPath, originalText);
  }
}
