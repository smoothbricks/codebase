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
 * validators. So the path decides only which project to ASK FIRST; the adapter
 * decides. Each candidate is tried in turn and the first program that holds the
 * file transforms it. For `Bun.build`, pass the adapter directly in `plugins`.
 */

import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { sourceFilePattern, type TtscUnpluginOptions } from '@ttsc/unplugin/api';
import type { BunLikePlugin, BunLoader } from '@ttsc/unplugin/bun';
import * as bunAdapterModule from '@ttsc/unplugin/bun';
import { plugin } from 'bun';

/** The module Bun is about to evaluate. */
interface SourceToLoad {
  readonly path: string;
}

/** Transformed source, in the shape Bun's runtime loader requires. */
interface LoadedSource {
  readonly contents: string;
  readonly loader: BunLoader;
}

type BunLoaderCallback = (args: SourceToLoad) => Promise<LoadedSource | undefined>;

const TEST_SOURCE_PATTERN = /(?:^|[/\\])(?:__tests__|tests)(?:[/\\])|\.(?:test|spec)\.[cm]?tsx?$/;
/** The adapter's verdict that its program does not hold the file. */
const MISSING_PROGRAM_OUTPUT = /^ttsc transform did not return output for /;
/**
 * Config names in the order a source is offered to them. The intended home
 * first — tests belong to the test program, everything else to the emit
 * program — then the repo-wide program that lists what the narrow ones leave
 * out, then the remaining one.
 */
const TEST_FIRST = ['tsconfig.test.json', 'tsconfig.json', 'tsconfig.lib.json'] as const;
const LIB_FIRST = ['tsconfig.lib.json', 'tsconfig.json', 'tsconfig.test.json'] as const;
const bunAdapter: unknown = bunAdapterModule.default;
const loaders = new Map<string, Promise<BunLoaderCallback>>();

plugin({
  name: 'ttsc-project-router',
  setup(build) {
    build.onLoad({ filter: sourceFilePattern }, (args) => loadThroughOwningProject(args));
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
async function loadThroughOwningProject(args: SourceToLoad): Promise<LoadedSource | undefined> {
  const candidates = candidateProjects(args.path);
  let firstVerdict: Error | undefined;
  for (const project of candidates) {
    const load = await getBunLoader(project);
    try {
      return await load(args);
    } catch (error) {
      if (!(error instanceof Error) || !MISSING_PROGRAM_OUTPUT.test(error.message)) throw error;
      firstVerdict ??= error;
    }
  }

  throw new Error(
    `No TypeScript project holds ${args.path}, so its transforms (Typia, LMAO) cannot be applied. ` +
      `Tried ${candidates.map((candidate) => candidate ?? 'the nearest tsconfig.json').join(', ')}. ` +
      'Add the file to a project\'s "include" — the repo-wide tsconfig.json is where entry points kept out of the emit program belong.',
    { cause: firstVerdict },
  );
}

/**
 * Every project that could hold this file, nearest directory first.
 *
 * Ends with `undefined` — the adapter's own nearest-`tsconfig.json` discovery —
 * so a tree with none of these names keeps working as it did.
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

function getBunLoader(project: string | undefined): Promise<BunLoaderCallback> {
  const key = project ?? '';
  const existing = loaders.get(key);
  if (existing !== undefined) return existing;
  const pending = captureBunLoader(project);
  loaders.set(key, pending);
  return pending;
}

async function captureBunLoader(project: string | undefined): Promise<BunLoaderCallback> {
  let loader: BunLoaderCallback | undefined;
  await createBunTtscPlugin({ project }).setup({
    onLoad(_options, registered) {
      loader = registered;
    },
  });
  if (loader === undefined) throw new TypeError('@ttsc/unplugin/bun did not register its TypeScript loader');
  return loader;
}

function createBunTtscPlugin(options: TtscUnpluginOptions): BunLikePlugin {
  if (typeof bunAdapter === 'function') return bunAdapter(options);
  if (
    typeof bunAdapter === 'object' &&
    bunAdapter !== null &&
    'default' in bunAdapter &&
    typeof bunAdapter.default === 'function'
  ) {
    return bunAdapter.default(options);
  }
  throw new TypeError('@ttsc/unplugin/bun did not export a Bun adapter factory');
}
