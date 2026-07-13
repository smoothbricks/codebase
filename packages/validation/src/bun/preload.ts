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
 * Bun test files belong to `tsconfig.test.json`; imported production and preload
 * files belong to their nearest library project. Route each source to the
 * matching native ttsc adapter while registering one Bun loader. For
 * `Bun.build`, pass the adapter directly in `plugins`.
 */

import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { sourceFilePattern, type TtscUnpluginOptions } from '@ttsc/unplugin/api';
import type { BunLikeBuild, BunLikePlugin } from '@ttsc/unplugin/bun';
import * as bunAdapterModule from '@ttsc/unplugin/bun';
import { plugin } from 'bun';

type BunLoaderCallback = Parameters<BunLikeBuild['onLoad']>[1];

const TEST_SOURCE_PATTERN = /(?:^|[/\\])(?:__tests__|tests)(?:[/\\])|\.(?:test|spec)\.[cm]?tsx?$/;
const bunAdapter: unknown = bunAdapterModule.default;
const loaders = new Map<string, Promise<BunLoaderCallback>>();

plugin({
  name: 'ttsc-project-router',
  setup(build) {
    build.onLoad({ filter: sourceFilePattern }, async (args) => {
      const configName = TEST_SOURCE_PATTERN.test(args.path) ? 'tsconfig.test.json' : 'tsconfig.lib.json';
      const project = findNearestProject(args.path, configName);
      const load = await getBunLoader(project);
      return load(args);
    });
  },
});

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

function findNearestProject(file: string, configName: string): string | undefined {
  let directory = dirname(file);
  while (true) {
    const candidate = join(directory, configName);
    if (existsSync(candidate)) return candidate;
    const parent = dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
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
