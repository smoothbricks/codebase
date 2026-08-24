import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { type PackageExports, type PackageJson, parsePackageJsonText } from '../lib/json.js';

/**
 * TypeScript source (not declarations) — what a published exports map should
 * not offer runtimes: a consumer whose runtime activates the matching
 * condition (Nx forces `development` onto its plugin worker; bun enables it
 * outside production) resolves raw TS out of node_modules and either crashes
 * (Node refuses to strip types there) or silently runs unbuilt source.
 */
function isTypeScriptSourceTarget(target: string): boolean {
  return /\.(?:[mc]?ts|tsx)$/.test(target) && !/\.d\.[mc]?ts$/.test(target);
}

function pruneEntry(value: PackageExports, path: string, pruned: string[]): PackageExports {
  if (typeof value === 'string') {
    if (isTypeScriptSourceTarget(value)) {
      pruned.push(path);
      return undefined;
    }
    return value;
  }
  if (value === null || value === undefined) {
    return value;
  }
  const result: Record<string, PackageExports> = {};
  for (const [condition, target] of Object.entries(value)) {
    // The `types` condition is resolved by TypeScript only — a .ts target
    // there is valid ("source as types") and poses no runtime hazard.
    if (condition === 'types') {
      result[condition] = target;
      continue;
    }
    const kept = pruneEntry(target, `${path}[${condition}]`, pruned);
    if (kept !== undefined) {
      result[condition] = kept;
    }
  }
  return result;
}

/** Whether an entry still offers runtimes a target to resolve (`types` alone is not one). */
function hasRuntimeTarget(value: PackageExports): boolean {
  if (typeof value === 'string') {
    return true;
  }
  if (value === null || value === undefined) {
    return false;
  }
  return Object.entries(value).some(([condition, target]) => condition !== 'types' && hasRuntimeTarget(target));
}

function pruneSubpath(value: PackageExports, path: string, pruned: string[]): PackageExports {
  if (!hasRuntimeTarget(value)) {
    return value;
  }
  const candidates: string[] = [];
  const result = pruneEntry(value, path, candidates);
  // A subpath whose every runtime target is TypeScript source is published
  // that way on purpose (a source-only package has no built alternative) —
  // leave it exactly as authored rather than breaking resolution.
  if (!hasRuntimeTarget(result)) {
    return value;
  }
  pruned.push(...candidates);
  return result;
}

/**
 * The workspace convention maps conditions like `development` and `bun` at
 * `./src/*.ts` so the repo itself loads live source; published manifests
 * should resolve to built artifacts instead. Per subpath, conditions whose
 * target is TypeScript source are dropped as long as a built runtime target
 * remains. `types` conditions and subpaths with no built alternative
 * (deliberately source-only packages) stay untouched. Returns the input when
 * nothing changes.
 */
export function prunePublishedExports(pkg: PackageJson): { manifest: PackageJson; pruned: string[] } {
  const pruned: string[] = [];
  const exports = pkg.exports;
  if (exports === undefined || exports === null || typeof exports === 'string') {
    return { manifest: pkg, pruned };
  }
  const isSubpathMap = Object.keys(exports).some((key) => key.startsWith('.'));
  const next = isSubpathMap
    ? Object.fromEntries(
        Object.entries(exports).map(([subpath, value]) => [
          subpath,
          pruneSubpath(value, `exports[${subpath}]`, pruned),
        ]),
      )
    : // A bare condition object is the sugar form of a single "." subpath.
      pruneSubpath(exports, 'exports', pruned);
  if (pruned.length === 0) {
    return { manifest: pkg, pruned };
  }
  return { manifest: { ...pkg, exports: next }, pruned };
}

/**
 * Run `fn` (a `bun pm pack`) with the package's on-disk manifest rewritten to
 * its published shape, restoring the original bytes afterwards — the same
 * rewrite-around-pack pattern syncBunLockfileVersions uses for bun.lock.
 * A manifest transform (rather than a bun feature) because `bun pm pack`
 * offers no publish-time manifest hook.
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
  const { manifest, pruned } = prunePublishedExports(parsed);
  if (pruned.length === 0) {
    return fn();
  }
  if (options.log) {
    console.log(`${manifest.name}: pruning TypeScript-source export entries for pack: ${pruned.join(', ')}`);
  }
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  try {
    return await fn();
  } finally {
    writeFileSync(manifestPath, originalText);
  }
}
