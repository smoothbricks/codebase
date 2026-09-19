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
 * A file can only be transformed by a project whose program contains it: ttsc
 * emits the files its program holds, and asking it for any other file yields
 * "ttsc transform did not return output for <file>". Which project that is
 * cannot be read off a path. `tsconfig.lib.json` is the emit program and lists
 * `src/` alone, so an entry point deliberately kept out of the bundle — a
 * `scripts/deploy.ts` that only the repo-wide program lists — is not in it, and
 * routing by "nearest lib config" claimed exactly those files and lost their
 * validators. So the path decides only which project to ASK FIRST; the compiler
 * decides. Each candidate is tried in turn and the first program that holds the
 * file transforms it.
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
  resolveOptions,
  type TtscTransformHooks,
  transformTtsc,
} from '@ttsc/unplugin/api';
import type { BunLoader } from '@ttsc/unplugin/bun';
import { plugin } from 'bun';
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
/** The compiler's verdict that the project's program does not hold the file. */
const MISSING_PROGRAM_OUTPUT = /^ttsc transform did not return output for /;
/**
 * Config names in the order a source is offered to them. The intended home
 * first — tests belong to the test program, everything else to the emit
 * program — then the repo-wide program that lists what the narrow ones leave
 * out, then the remaining one.
 */
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

/**
 * Transform one source with the first candidate project whose program holds it.
 *
 * Only the "no output for this file" verdict moves on to the next candidate:
 * that one means the project was the wrong question. A compiler failure or
 * exception is the answer to the right question and is rethrown untouched, so
 * diagnostics still reach the operator from the project that produced them.
 */
async function loadThroughOwningProject(path: string): Promise<LoadedSource> {
  const loader: BunLoader = /x$/i.test(path) ? 'tsx' : 'ts';
  const source = await readFile(path, 'utf8');
  // Not a transform target: hand the source back for Bun to transpile, because
  // `Bun.plugin()` rejects an undefined `onLoad` result.
  if (!isTransformTarget(path)) return { contents: source, loader };

  const candidates = candidateProjects(path);
  let firstVerdict: Error | undefined;
  for (const project of candidates) {
    try {
      const result = await transformTtsc(path, source, optionsFor(project), undefined, cache, TRANSFORM_HOOKS);
      // A no-op transform returns nothing; the file is still this program's.
      return { contents: result?.code ?? source, loader };
    } catch (error) {
      if (!(error instanceof Error) || !MISSING_PROGRAM_OUTPUT.test(error.message)) throw error;
      firstVerdict ??= error;
    }
  }

  throw new Error(
    `No TypeScript project holds ${path}, so its transforms (Typia, LMAO) cannot be applied. ` +
      `Tried ${candidates.map((candidate) => candidate ?? 'the nearest tsconfig.json').join(', ')}. ` +
      'Add the file to a project\'s "include" — the repo-wide tsconfig.json is where entry points kept out of the emit program belong.',
    { cause: firstVerdict },
  );
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
