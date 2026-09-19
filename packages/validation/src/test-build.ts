import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { encodedMap, TraceMap } from '@jridgewell/trace-mapping';
import typia from 'typia';

export const TYPESCRIPT_TEST_OUTPUT_DIRECTORY = '.cache/test-build';
export const TYPESCRIPT_LIBRARY_MANIFEST = '.cache/ttsc-library.json';
export const TYPESCRIPT_TEST_MANIFEST = `${TYPESCRIPT_TEST_OUTPUT_DIRECTORY}/manifest.json`;
export const PRECOMPILED_TEST_ENV = 'SMOOTHBRICKS_PRECOMPILED_TESTS';

export type TypeScriptEmissionManifest = Record<string, string>;
const parseManifest = typia.json.createAssertParse<TypeScriptEmissionManifest>();
const projectRoots = new Map<string, string>();
const manifests = new Map<string, Promise<TypeScriptEmissionManifest>>();

/** Derive source ownership from actual compiler outputs, never tsconfig path guesses. */
export function typescriptEmissionManifest(
  root: string,
  output: Readonly<Record<string, string>>,
): TypeScriptEmissionManifest {
  const manifest: TypeScriptEmissionManifest = {};
  for (const [name, sourceMap] of Object.entries(output)) {
    if (!/\.(?:[cm]?js|jsx)\.map$/.test(name)) continue;
    const artifact = name.slice(0, -4);
    if (output[artifact] === undefined) throw new Error(`Compiler emitted ${name} without its JavaScript artifact.`);
    const mapPath = resolve(root, name);
    const traceMap = new TraceMap(sourceMap, pathToFileURL(mapPath).href);
    // A bundled file has no individual source-module identity. Its published
    // bundle remains loadable normally; it cannot replace each source module.
    if (traceMap.resolvedSources.length !== 1) continue;
    const source = traceMap.resolvedSources[0];
    if (!source.startsWith('file:')) continue;
    const sourceKey = relative(root, fileURLToPath(source));
    const artifactKey = relative(root, resolve(root, artifact));
    if (isAbsolute(sourceKey) || sourceKey === '..' || sourceKey.startsWith(`..${sep}`)) continue;
    if (isAbsolute(artifactKey) || artifactKey === '..' || artifactKey.startsWith(`..${sep}`)) {
      throw new Error(`Compiler artifact must remain inside its owning project: ${artifact}`);
    }
    manifest[sourceKey] = artifactKey;
  }
  return manifest;
}

async function readManifest(path: string): Promise<TypeScriptEmissionManifest> {
  let pending = manifests.get(path);
  if (pending === undefined) {
    pending = readFile(path, 'utf8').then(parseManifest);
    manifests.set(path, pending);
  }
  return pending;
}

/** Load only emitted artifacts when Nx has completed the test-build prerequisite. */
export async function loadPrecompiledTestSource(filePath: string) {
  if (process.env[PRECOMPILED_TEST_ENV] !== '1') return null;
  if (filePath.includes('\0') || !/\.[cm]?tsx?$/.test(filePath) || /\.d\.[cm]?ts$/.test(filePath)) return null;
  if (filePath.includes(`${sep}node_modules${sep}`)) return null;

  const directory = dirname(filePath);
  let root = projectRoots.get(directory);
  if (root === undefined) {
    root = directory;
    while (!existsSync(join(root, 'package.json')) || !existsSync(join(root, 'tsconfig.test.json'))) {
      const parent = dirname(root);
      if (parent === root) throw new Error(`No Nx test-build project owns ${filePath}.`);
      root = parent;
    }
    projectRoots.set(directory, root);
  }
  const key = relative(root, filePath);
  const testManifest = await readManifest(join(root, TYPESCRIPT_TEST_MANIFEST));
  let artifact = testManifest[key];
  if (artifact === undefined) {
    const libraryManifest = await readManifest(join(root, TYPESCRIPT_LIBRARY_MANIFEST));
    artifact = libraryManifest[key];
  }
  if (artifact === undefined) {
    throw new Error(
      `No compiler output owns ${filePath}. Include it in the owning project's library or test program and run its Nx test target.`,
    );
  }
  const emittedPath = resolve(root, artifact);
  const contained = relative(root, emittedPath);
  if (isAbsolute(contained) || contained === '..' || contained.startsWith(`..${sep}`)) {
    throw new Error(`Compiled test manifest points outside its project: ${artifact}`);
  }
  const [code, sourceMap] = await Promise.all([readFile(emittedPath, 'utf8'), readFile(`${emittedPath}.map`, 'utf8')]);
  const traceMap = new TraceMap(sourceMap, pathToFileURL(`${emittedPath}.map`).href);
  const map = { ...encodedMap(traceMap), sourceRoot: '', sources: traceMap.resolvedSources };
  return { code, map, loader: emittedPath.endsWith('.jsx') ? ('jsx' as const) : ('js' as const) };
}

/** Vite retains source module identities and assets; ttsc work has already finished. */
export function precompiledTestPlugin() {
  if (process.env[PRECOMPILED_TEST_ENV] !== '1') {
    throw new Error('Run the Nx test target: its ttsc-test-compile prerequisite must finish before Vitest starts.');
  }
  return {
    name: 'smoothbricks-precompiled-tests',
    enforce: 'pre' as const,
    async load(id: string) {
      return loadPrecompiledTestSource(id);
    },
  };
}
