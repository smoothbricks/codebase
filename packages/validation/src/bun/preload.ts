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
 *
 * Every process used to compile every project it touched from scratch: ttsc
 * keeps a generation for one process only. A proven generation is now stored
 * beside the managed shell's ttsc cache and served to later processes while
 * every input it was derived from is unchanged; `transform-store.ts` says what
 * that proof covers.
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
import {
  createRecording,
  loadGeneration,
  projectWalk,
  recordingHooks,
  recordModule,
  type StoredGeneration,
  sha256,
  storeGeneration,
} from './transform-store.js';

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
  /** The config's root files, spelled as TypeScript lists them. */
  readonly fileNames: readonly string[];
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
/**
 * Where proven project generations persist across processes. Only beside the
 * managed shell's ttsc cache, the build state a checkout declares for ttsc
 * (specs/cowshed/03_caches.md, "Retiring the caches volume"): without it there
 * is no agreed place, and each process compiles as it always did.
 */
const STORE = process.env.TTSC_CACHE_DIR === undefined ? undefined : join(process.env.TTSC_CACHE_DIR, 'bun-preload');
/** One project's store state in this process, reopened when its config chain changes. */
interface ProjectSession {
  readonly snapshot: ProjectRoots['snapshot'];
  /** Whether the stored generation was read, and proven or refused, yet. */
  loaded: boolean;
  /** The stored generation proven current in this process. */
  generation: StoredGeneration | undefined;
  /** The project walk taken before this process first compiled the project. */
  walk: Readonly<Record<string, string>> | undefined;
  queue: Promise<void> | undefined;
}
const sessions = new Map<string, ProjectSession>();

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
  const roots = project === undefined ? undefined : rootsByProject.get(project);
  if (STORE === undefined || project === undefined || roots === undefined) {
    const result = await transformTtsc(path, source, optionsFor(project), undefined, cache, TRANSFORM_HOOKS);
    return { contents: result?.code ?? source, loader };
  }
  return { contents: await deliverThroughStore(STORE, project, roots, path, source), loader };
}

/**
 * Serialize one project's deliveries, so concurrent first imports wait for the
 * single compile that stores them all instead of each starting its own.
 */
function deliverThroughStore(
  store: string,
  project: string,
  roots: ProjectRoots,
  path: string,
  source: string,
): Promise<string> {
  let session = sessions.get(project);
  if (session === undefined || !sameConfigSnapshot(session.snapshot, roots.snapshot)) {
    session = { snapshot: roots.snapshot, loaded: false, generation: undefined, walk: undefined, queue: undefined };
    sessions.set(project, session);
  }
  const current = session;
  const delivery = (current.queue ?? Promise.resolve()).then(() =>
    deliver(store, project, current, roots, path, source),
  );
  // The chain only orders deliveries; each caller still receives its own rejection.
  current.queue = delivery.then(
    () => undefined,
    () => undefined,
  );
  return delivery;
}

async function deliver(
  store: string,
  project: string,
  session: ProjectSession,
  roots: ProjectRoots,
  path: string,
  source: string,
): Promise<string> {
  const options = optionsFor(project);
  if (!session.loaded) {
    session.loaded = true;
    try {
      session.generation = loadGeneration(store, project, options.compilerOptions);
    } catch (error) {
      reportStoreFailure(project, 'reading', error);
    }
  }
  const stored = session.generation?.modules[path];
  if (stored !== undefined && stored.sourceSha256 === sha256(source)) return stored.code ?? source;

  session.walk ??= projectWalk(project, options.compilerOptions);
  if (session.walk === undefined) {
    const result = await transformTtsc(path, source, options, undefined, cache, TRANSFORM_HOOKS);
    return result?.code ?? source;
  }
  const recording = createRecording(project, session.walk);
  const hooks = recordingHooks(recording);
  const result = await transformTtsc(path, source, options, undefined, cache, hooks);
  recordModule(recording, path, source, result?.code);
  try {
    // The compile already produced every root's output; delivering them now
    // costs milliseconds each and lets every later process hit, whichever
    // modules it happens to import.
    for (const root of roots.fileNames) {
      if (recording.modules.has(root) || session.generation?.modules[root] !== undefined) continue;
      if (!SOURCE_FILE_PATTERN.test(root) || !isTransformTarget(root)) continue;
      const rootSource = await readFile(root, 'utf8');
      const rootResult = await transformTtsc(root, rootSource, options, undefined, cache, hooks);
      recordModule(recording, root, rootSource, rootResult?.code);
    }
    const outcome = storeGeneration(store, recording, session.generation, options.compilerOptions);
    if (outcome.kind === 'stored') {
      session.generation = outcome.generation;
    } else {
      session.generation = undefined;
      session.walk = undefined;
      process.stderr.write(`@smoothbricks/validation: not storing ttsc output for ${project}: ${outcome.reason}\n`);
    }
  } catch (error) {
    session.generation = undefined;
    session.walk = undefined;
    reportStoreFailure(project, 'storing', error);
  }
  return result?.code ?? source;
}

/** The store is an accelerator: failing it falls back to compiling, out loud. */
function reportStoreFailure(project: string, action: string, error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`@smoothbricks/validation: ${action} stored ttsc output for ${project} failed: ${message}\n`);
}

function sameConfigSnapshot(left: ProjectRoots['snapshot'], right: ProjectRoots['snapshot']): boolean {
  return (
    left.length === right.length &&
    left.every((entry, index) => entry.path === right[index]?.path && entry.contents === right[index]?.contents)
  );
}

/** Config-chain edits invalidate root selection independently of native output. */
function isConfiguredRoot(project: string, file: string): boolean {
  const snapshot = readTsconfigSourceSnapshot(project);
  const key = canonicalPath(file);
  const existing = rootsByProject.get(project);
  if (
    existing !== undefined &&
    sameConfigSnapshot(existing.snapshot, snapshot) &&
    (existing.files.has(key) || existing.queriedFiles.has(key))
  ) {
    return existing.files.has(key);
  }
  // A newly requested path can have appeared beneath an unchanged include glob.
  // Reparse its directory membership rather than retaining a negative answer.
  const parsed = ts.getParsedCommandLineOfConfigFile(project, undefined, configHost);
  const fileNames = parsed?.fileNames ?? [];
  const roots: ProjectRoots = {
    snapshot,
    fileNames,
    files: new Set(fileNames.map(canonicalPath)),
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
