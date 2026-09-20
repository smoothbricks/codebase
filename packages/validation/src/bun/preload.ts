/**
 * Bun runtime preload — registers the native ttsc compiler integration.
 *
 * This MUST run before importing source that contains Typia or LMAO transform
 * callsites. ttsc discovers active transformers from direct package dependency
 * descriptors.
 *
 * Usage in bunfig.toml:
 *   preload = ["@smoothbricks/validation/bun/preload"]
 *
 * A runtime entry point may be excluded from the library's emit program. Use
 * TypeScript's config parser to prefer the nearest project that explicitly
 * includes the requested source, rather than treating an unchanged transform
 * result as proof of membership. Files reached only through imports need not
 * be configured roots, so they retain the nearest project's native transform.
 * No second TypeScript Program or TypeChecker is constructed.
 *
 * This drives `@ttsc/unplugin`'s public transform API rather than its
 * `bun-register`/`bun` adapter for one reason: that adapter calls
 * `createTtscTransformCache()` with no operations, and the cache is the only
 * place a host can supply ttsc's directory-watch seam. Under Bun that seam is
 * not optional — see {@link pollingDirectoryWatch} for the measurements —
 * because ttsc opens one watcher per project and host-input directory to prove
 * a generation reusable, and Bun on macOS charges seconds for every `fs.watch`
 * registration after the first. `Bun.build` callers still pass the adapter in
 * `plugins` and get its own cache, so they keep ttsc's `fs.watch` fallback.
 */

import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import {
  beginTtscTransformBuild,
  createTtscTransformCache,
  isTransformTarget,
  readTsconfigSourceSnapshot,
  resolveOptions,
  type TtscTransformHooks,
  transformTtsc,
} from '@ttsc/unplugin/api';
import type { BunLoader } from '@ttsc/unplugin/bun';
import { plugin } from 'bun';
import ts from 'typescript';
import { pollingDirectoryWatch } from './directory-watch.js';

/** Transformed source, in the shape Bun's runtime loader requires. */
interface LoadedSource {
  readonly contents: string;
  readonly loader: BunLoader;
}

/**
 * How often the watch seam re-reads an observed directory.
 *
 * The window to cover is one project compile, and every observed directory is
 * re-read each cycle: a few hundred `bigint` stats is single-digit
 * milliseconds, so 250 ms spends well under 1% of one core for the life of the
 * process.
 */
const WATCH_POLL_INTERVAL_MS = 250;
/**
 * Paths Bun's loader hands this plugin. `@ttsc/unplugin`'s exported
 * `sourceFilePattern` is unanchored and matches the virtual module ids other
 * plugins create, which carry a NUL byte and no file behind them; an `onLoad`
 * filter claims every path it matches, and this one now reads the file itself.
 */
const SOURCE_FILE_PATTERN = /^[^\0]*\.[cm]?tsx?$/;
const TEST_SOURCE_PATTERN = /(?:^|[/\\])(?:__tests__|tests)(?:[/\\])|\.(?:test|spec)\.[cm]?tsx?$/;
/** Candidate preference within each directory; configured source owners win. */
const TEST_FIRST = ['tsconfig.test.json', 'tsconfig.json', 'tsconfig.lib.json'] as const;
const LIB_FIRST = ['tsconfig.lib.json', 'tsconfig.json', 'tsconfig.test.json'] as const;
/**
 * The shared transform calls `addWatchFile` once per plugin-reported
 * dependency so type-only inputs can enter a bundler's watch graph. Bun's
 * runtime loader has no such channel, so there is nothing to forward.
 */
const TRANSFORM_HOOKS: TtscTransformHooks = { addWatchFile: () => undefined };
const cache = createTtscTransformCache({ watch: pollingDirectoryWatch(WATCH_POLL_INTERVAL_MS) });
/**
 * Resolved options per candidate project. `ResolvedTtscUnpluginOptions` is
 * deliberately not exported by `@ttsc/unplugin` — callers are not meant to
 * construct one — so the map is seeded with the auto-discovery entry every
 * candidate list ends with, and takes its value type from that.
 */
const optionsByProject = new Map([['', resolveOptions({ project: undefined })]]);
interface ProjectRoots {
  readonly snapshot: ReturnType<typeof readTsconfigSourceSnapshot>;
  readonly files: ReadonlySet<string>;
  readonly queriedFiles: Set<string>;
}
const rootsByProject = new Map<string, ProjectRoots>();
const configHost: ts.ParseConfigFileHost = {
  ...ts.sys,
  onUnRecoverableConfigFileDiagnostic(diagnostic) {
    throw new Error(ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'));
  },
};
const canonicalPath = ts.sys.useCaseSensitiveFileNames ? (path: string) => path : (path: string) => path.toLowerCase();

plugin({
  name: 'ttsc-project-router',
  setup(build) {
    // One setup invocation is one runtime process and module-loading session,
    // so first delivery of every emitted project module is constant-time
    // instead of re-reading the whole project.
    beginTtscTransformBuild(cache);
    build.onLoad({ filter: SOURCE_FILE_PATTERN }, (args) => loadThroughOwningProject(args.path));
  },
});

/** Select a configured source owner without rejecting transitive imports. */
async function loadThroughOwningProject(path: string): Promise<LoadedSource> {
  const loader: BunLoader = /x$/i.test(path) ? 'tsx' : 'ts';
  const source = await readFile(path, 'utf8');
  // Bun's runtime onLoad hook requires source even for a no-op transform.
  if (!isTransformTarget(path)) return { contents: source, loader };

  const candidates = candidateProjects(path);
  const project =
    candidates.find((candidate) => candidate !== undefined && isConfiguredRoot(candidate, path)) ?? candidates[0];
  const result = await transformTtsc(path, source, optionsFor(project), undefined, cache, TRANSFORM_HOOKS);
  return { contents: result?.code ?? source, loader };
}

/** Config-chain edits invalidate root selection independently of native output. */
function isConfiguredRoot(project: string, file: string): boolean {
  const snapshot = readTsconfigSourceSnapshot(project);
  const key = canonicalPath(file);
  const existing = rootsByProject.get(project);
  if (
    existing !== undefined &&
    existing.snapshot.length === snapshot.length &&
    existing.snapshot.every(
      (entry, index) => entry.path === snapshot[index]?.path && entry.contents === snapshot[index]?.contents,
    ) &&
    (existing.files.has(key) || existing.queriedFiles.has(key))
  ) {
    return existing.files.has(key);
  }
  // A newly requested path can have appeared beneath an unchanged include glob.
  // Reparse its directory membership rather than retaining a negative answer.
  const parsed = ts.getParsedCommandLineOfConfigFile(project, undefined, configHost);
  const roots: ProjectRoots = {
    snapshot,
    files: new Set(parsed?.fileNames.map(canonicalPath)),
    queriedFiles: new Set([key]),
  };
  rootsByProject.set(project, roots);
  return roots.files.has(key);
}

/**
 * Every project that could hold this file, nearest directory first.
 *
 * Ends with `undefined` — ttsc's own nearest-`tsconfig.json` discovery — so a
 * tree with none of these names keeps working as it did.
 */
function candidateProjects(file: string): (string | undefined)[] {
  const names = TEST_SOURCE_PATTERN.test(file) ? TEST_FIRST : LIB_FIRST;
  const projects: (string | undefined)[] = [];
  let directory = dirname(file);
  while (true) {
    for (const name of names) {
      const candidate = join(directory, name);
      if (existsSync(candidate)) projects.push(candidate);
    }
    const parent = dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }
  projects.push(undefined);
  return projects;
}

/** Each candidate project's options, normalised once per process. */
function optionsFor(project: string | undefined) {
  const key = project ?? '';
  const existing = optionsByProject.get(key);
  if (existing !== undefined) return existing;
  const resolved = resolveOptions({ project });
  optionsByProject.set(key, resolved);
  return resolved;
}
