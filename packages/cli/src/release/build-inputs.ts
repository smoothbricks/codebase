import type { NxJson, NxProjectJson, NxTargetConfig } from '../lib/json.js';
import { globToRegExp } from './core.js';

/**
 * The file patterns whose change makes a package worth releasing.
 *
 * Derived from the project's own `build` target, because that is the only declaration of what
 * ends up in the published artifact. A package with no `build` target falls back to `production`.
 */
export function resolveBuildInputPatterns(project: NxProjectJson, nxJson: NxJson): string[] {
  const targets = project.targets;
  if (!targets) {
    return [];
  }
  return normalizeInputPatterns(collectBuildInputs(targets), nxJson);
}

export function collectBuildInputs(targets: Record<string, NxTargetConfig>): string[] {
  const build = targets.build;
  if (!build) {
    return ['production'];
  }
  const directInputs = stringInputs(build.inputs);
  if (directInputs.length > 0) {
    return directInputs;
  }
  const inputs: string[] = [];
  for (const dependency of build.dependsOn ?? []) {
    if (typeof dependency !== 'string' || dependency.startsWith('^')) {
      continue;
    }
    const targetName = dependency.includes(':') ? dependency.split(':')[1] : dependency;
    if (!targetName) {
      continue;
    }
    for (const matched of matchingTargetNames(targets, targetName)) {
      inputs.push(...stringInputs(targets[matched]?.inputs));
    }
  }
  return inputs.length > 0 ? inputs : ['production'];
}

/**
 * Nx resolves a glob in `dependsOn` against the project's own target names, so an aggregating
 * `build` fans out to `*-napi`, `*-macos`, `*-js` and the real inputs are declared on those
 * targets. Reading the glob as a literal target name finds nothing and falls back to
 * `production` — `{projectRoot}/src/**` plus the manifest — which for a native package covers
 * none of its Rust sources, so a cargo-only change reads as "nothing to release" and the fix
 * never reaches npm.
 */
function matchingTargetNames(targets: Record<string, NxTargetConfig>, pattern: string): string[] {
  if (!pattern.includes('*')) {
    return pattern in targets ? [pattern] : [];
  }
  const matcher = globToRegExp(pattern);
  return Object.keys(targets).filter((name) => matcher.test(name));
}

function stringInputs(inputs: NxTargetConfig['inputs'] | undefined): string[] {
  return Array.isArray(inputs) ? inputs.filter((entry): entry is string => typeof entry === 'string') : [];
}

function normalizeInputPatterns(inputs: string[], nxJson: NxJson): string[] {
  const patterns: string[] = [];
  const seen = new Set<string>();
  for (const input of inputs) {
    for (const pattern of expandInputPattern(input, nxJson, seen)) {
      patterns.push(pattern);
    }
  }
  return patterns;
}

function expandInputPattern(input: string, nxJson: NxJson, seen: Set<string>): string[] {
  if (seen.has(input)) {
    return [];
  }
  seen.add(input);
  if (!input.includes('{')) {
    const namedInput = nxJson.namedInputs?.[input];
    if (Array.isArray(namedInput)) {
      return namedInput.flatMap((entry) => (typeof entry === 'string' ? expandInputPattern(entry, nxJson, seen) : []));
    }
    return [];
  }
  const excluded = input.startsWith('!');
  const rawInput = excluded ? input.slice(1) : input;
  if (!rawInput.startsWith('{projectRoot}/')) {
    return [];
  }
  const packageRelative = rawInput.slice('{projectRoot}/'.length);
  return [`${excluded ? '!' : ''}${packageRelative}`];
}
