import type { PackageExports, PackageJson } from '../lib/json.js';

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

function pruneEntry(value: PackageExports, path: string, adjustments: string[]): PackageExports {
  if (typeof value === 'string') {
    if (isTypeScriptSourceTarget(value)) {
      adjustments.push(path);
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
    const kept = pruneEntry(target, `${path}[${condition}]`, adjustments);
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

/**
 * publint EXPORT_TYPES_SHOULD_BE_FIRST: export conditions are order-sensitive,
 * and TypeScript must be able to resolve `types` before a runtime condition in
 * earlier key positions wins. The workspace convention orders `bun` and
 * `development` first so the repo itself loads live source; a published
 * manifest must offer `types` first instead. Moves the `types` entry to the
 * front of every condition map, preserving every other condition's relative
 * order, and recurses through subpath maps.
 */
function typesFirstInConditionMaps(value: PackageExports, path: string, adjustments: string[]): PackageExports {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  const entries = Object.entries(value);
  if (entries.some(([key]) => key.startsWith('.'))) {
    return Object.fromEntries(
      entries.map(([subpath, target]) => [
        subpath,
        typesFirstInConditionMaps(target, `${path}[${subpath}]`, adjustments),
      ]),
    );
  }
  const typesIndex = entries.findIndex(([condition]) => condition === 'types');
  if (typesIndex <= 0) {
    return value;
  }
  adjustments.push(`${path}: types condition moved first`);
  return Object.fromEntries([entries[typesIndex], ...entries.filter((_, index) => index !== typesIndex)]);
}

function pruneSubpath(value: PackageExports, path: string, adjustments: string[]): PackageExports {
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
  adjustments.push(...candidates);
  return result;
}

/**
 * The workspace convention maps conditions like `development` and `bun` at
 * `./src/*.ts` so the repo itself loads live source; published manifests
 * should resolve to built artifacts instead. Per subpath, conditions whose
 * target is TypeScript source are dropped as long as a built runtime target
 * remains, and the `types` condition moves to the front of every condition
 * map (publint EXPORT_TYPES_SHOULD_BE_FIRST). `types` targets and subpaths
 * with no built alternative (deliberately source-only packages) stay
 * untouched. Returns the input when nothing changes.
 */
export function prunePublishedExports(pkg: PackageJson): { manifest: PackageJson; adjustments: string[] } {
  const adjustments: string[] = [];
  const exports = pkg.exports;
  if (exports === undefined || exports === null || typeof exports === 'string') {
    return { manifest: pkg, adjustments };
  }
  const isSubpathMap = Object.keys(exports).some((key) => key.startsWith('.'));
  const prunedExports = isSubpathMap
    ? Object.fromEntries(
        Object.entries(exports).map(([subpath, value]) => [
          subpath,
          pruneSubpath(value, `exports[${subpath}]`, adjustments),
        ]),
      )
    : // A bare condition object is the sugar form of a single "." subpath.
      pruneSubpath(exports, 'exports', adjustments);
  const next = typesFirstInConditionMaps(prunedExports, 'exports', adjustments);
  if (adjustments.length === 0) {
    return { manifest: pkg, adjustments };
  }
  return { manifest: { ...pkg, exports: next }, adjustments };
}
