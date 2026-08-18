/**
 * Resolve the consumer package whose tests are running.
 *
 * WHY nearest-package.json, anchored on cwd: standard resolution semantics
 * identify "the package whose tests are running" identically in every layout —
 * a monorepo checkout, a consumer of the Bun isolated store
 * (`node_modules/.bun/…`), or plain node_modules. This module ships inside an
 * installed dependency, so nothing may ever be derived from its own file
 * location; the test process cwd is the only truthful anchor.
 */

import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

export type ConsumerTarget =
  | { readonly kind: 'package'; readonly packageRoot: string }
  | { readonly kind: 'workspace-root'; readonly workspaceRoot: string; readonly packageRoots: readonly string[] }
  | { readonly kind: 'not-found'; readonly searched: readonly string[] };

/**
 * WHY a hand-rolled structural probe instead of Typia: this runs on the
 * preload bootstrap path, before the validation toolchain is guaranteed to be
 * registered, and a malformed package.json must degrade to "plain package
 * root", never to a crash that disables tracing.
 */
function readWorkspacePatterns(packageJsonPath: string): readonly string[] | null {
  try {
    const parsed: unknown = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
    if (typeof parsed !== 'object' || parsed === null) return null;
    const workspaces: unknown = Reflect.get(parsed, 'workspaces');
    const patterns = Array.isArray(workspaces) ? workspaces : null;
    if (!patterns) return null;
    return patterns.filter((entry): entry is string => typeof entry === 'string');
  } catch {
    return null;
  }
}

/**
 * Expand workspace member patterns to existing package roots. Supports the
 * forms Bun workspaces support in practice: a literal directory and a single
 * trailing `/*` glob.
 */
function expandWorkspaceMembers(workspaceRoot: string, patterns: readonly string[]): readonly string[] {
  const roots: string[] = [];
  for (const pattern of patterns) {
    if (pattern.endsWith('/*')) {
      const parent = join(workspaceRoot, pattern.slice(0, -2));
      if (!existsSync(parent)) continue;
      for (const entry of readdirSync(parent, { withFileTypes: true })) {
        if (!entry.isDirectory()) continue;
        const candidate = join(parent, entry.name);
        if (existsSync(join(candidate, 'package.json'))) roots.push(candidate);
      }
    } else {
      const candidate = join(workspaceRoot, pattern);
      if (existsSync(join(candidate, 'package.json'))) roots.push(candidate);
    }
  }
  return roots;
}

/**
 * Walk up from `startDir` to the nearest directory holding a `package.json`.
 * A nearest package.json that declares `workspaces` is a workspace-root run
 * (cwd sits at or under the root but inside no member package — a member's own
 * package.json would have been nearer); its members are expanded for the
 * cross-package tracer scan.
 */
export function resolveConsumerTarget(startDir: string): ConsumerTarget {
  const searched: string[] = [];
  let directory = resolve(startDir);
  while (true) {
    const packageJsonPath = join(directory, 'package.json');
    searched.push(packageJsonPath);
    if (existsSync(packageJsonPath)) {
      const workspacePatterns = readWorkspacePatterns(packageJsonPath);
      if (workspacePatterns !== null) {
        return {
          kind: 'workspace-root',
          workspaceRoot: directory,
          packageRoots: expandWorkspaceMembers(directory, workspacePatterns),
        };
      }
      return { kind: 'package', packageRoot: directory };
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return { kind: 'not-found', searched };
    }
    directory = parent;
  }
}
