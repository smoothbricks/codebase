/**
 * The workspace files an ESLint flat config loads when ESLint loads it, found
 * without running it: the files a lint verdict depends on beyond its sources.
 *
 * ESLint 10 under Node loads a TypeScript config through jiti and any other
 * config with a native `import()` (eslint/lib/config/config-loader.js). Every
 * module a config reaches loads the same way, so each module is read and each
 * specifier resolved by that loader's own machinery, never a model of it:
 *
 *   - jiti: its transform decides which imports survive type erasure, and its
 *     resolver (`esmResolve`, under the call's `require` or `import`
 *     conditions) picks the file.
 *   - Node: `import type` and `export type` are erased and every other import,
 *     `import()` and `require()` loads. One `node` process per walk layer, the
 *     `node` that runs ESLint, resolves each `require` with
 *     `createRequire(module).resolve` and each `import` with
 *     `import.meta.resolve(specifier, module)`.
 *
 * A builtin, or a package installed outside the workspace's own files, adds
 * nothing: `bun.lock` and the root manifest key what is installed. A package
 * that resolves into the workspace's own files (a workspace member linked into
 * `node_modules`) is keyed and walked like a relative module. Every loaded
 * file keys the package.json files that decide how it loads: the nearest one
 * (its module format), a member's own (its entry), and that of a directory a
 * relative specifier enters (its `main`). A load whose target the source does not spell
 * (`require(name)`, `import(path)`, `createRequire`), a specifier that is
 * neither relative nor a package name, a specifier the loader cannot resolve,
 * and a workspace file Nx does not index all refuse: lint cannot cache a
 * verdict whose key misses a file it read.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync, realpathSync, statSync } from 'node:fs';
import { createRequire, isBuiltin } from 'node:module';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import type * as Jiti from 'jiti';
import ts from 'typescript';
import typia from 'typia';

type LoadKind = 'require' | 'import';
/** One load a module performs: CommonJS `require` and ESM `import` resolve under different conditions. */
type LoadSite =
  | { readonly kind: LoadKind; readonly specifier: string }
  | { readonly kind: 'computed'; readonly text: string };
/** A load still to resolve, with the module that performs it. */
interface PendingLoad {
  readonly importer: string;
  readonly from: string;
  readonly kind: LoadKind;
  readonly specifier: string;
  readonly bare: boolean;
  readonly jiti: Jiti.Jiti | null;
}

/** The options ESLint creates its jiti with; `fsCache: false` only keeps graph construction from writing a cache. */
const JITI_OPTIONS: Jiti.JitiOptions = { moduleCache: false, fsCache: false };
const TYPESCRIPT_CONFIG = /\.[mc]?ts$/u;
/** The files a module that loads nothing is: data and native addons. */
const LEAF_MODULE = /\.(?:json|node)$/u;
const RELATIVE_SPECIFIER = /^\.\.?(?:\/|$)/u;
/** Node's own resolution of each `{kind, specifier, from}` on stdin: a path or file URL, `null` where Node throws. */
const NODE_RESOLVER = `import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
const loads = JSON.parse(readFileSync(0, 'utf8'));
console.log(JSON.stringify(loads.map(({ kind, specifier, from }) => {
  try {
    return kind === 'require' ? createRequire(from).resolve(specifier) : import.meta.resolve(specifier, from);
  } catch {
    return null;
  }
})));`;

/**
 * The workspace-relative files `configs` load, transitively, the configs
 * excluded, sorted. `indexed` is Nx's workspace file index (at least its
 * JavaScript, TypeScript, JSON and package.json files); a file outside it refuses.
 */
export function eslintConfigClosure(
  workspaceRoot: string,
  configs: readonly string[],
  indexed: ReadonlySet<string>,
): string[] {
  const root = realpathSync(workspaceRoot);
  const keyed = new Set<string>();
  const jiti = configs.some((config) => TYPESCRIPT_CONFIG.test(config)) ? workspaceJiti(workspaceRoot) : null;
  for (const config of configs) {
    const loader = TYPESCRIPT_CONFIG.test(config) ? jiti : null;
    for (let layer = [config]; layer.length > 0; ) {
      const pending = layer.flatMap((module) => moduleLoads(root, config, module, loader));
      const targets =
        loader === null
          ? nodeResolve(pending)
          : pending.map(({ jiti: instance, kind, specifier, from }) => jitiResolve(instance, kind, specifier, from));
      layer = [];
      pending.forEach((load, index) => {
        const target = targets[index] ?? null;
        if (target === null) {
          throw new Error(
            `${load.importer} imports '${load.specifier}', which names no file ${loader === null ? 'Node' : 'jiti'} resolves, so no lint input can key it`,
          );
        }
        const real = realpathSync(target);
        const workspacePath = relative(root, real).split(sep).join('/');
        const installed = workspacePath.split('/').includes('node_modules');
        if (workspacePath.startsWith('../') || isAbsolute(workspacePath) || installed) {
          if (load.bare || installed) return;
          throw new Error(
            `${load.importer} imports '${load.specifier}', which resolves outside the workspace to ${real}`,
          );
        }
        for (const file of [workspacePath, ...packageMetadata(root, load, real)]) {
          if (!indexed.has(file)) {
            throw new Error(
              `${load.importer} imports '${load.specifier}', which reads ${file}, a file Nx's workspace file index does not hold, so no lint input can key it`,
            );
          }
          if (keyed.has(file) || configs.includes(file)) continue;
          keyed.add(file);
          if (file === workspacePath && !LEAF_MODULE.test(file)) layer.push(file);
        }
      });
    }
  }
  return [...keyed].sort();
}

/** The loads `module` performs that resolve to a file; refuses a computed load or an unresolvable specifier kind. */
function moduleLoads(root: string, config: string, module: string, jiti: typeof Jiti | null): PendingLoad[] {
  const from = join(root, module);
  const source = readFileSync(from, 'utf8');
  const instance = jiti === null ? null : jiti.createJiti(from, JITI_OPTIONS);
  const sites =
    instance === null ? loadSites(source, from) : loadSites(jitiTransform(instance, source, from), `${from}.cjs`);
  const importer = module === config ? config : `${config} loads ${module}, which`;
  return sites.flatMap((site): PendingLoad[] => {
    if (site.kind === 'computed') {
      throw new Error(
        `${importer} loads a module it computes at runtime (${site.text}), so no lint input can key what it loads`,
      );
    }
    if (isBuiltin(site.specifier)) return [];
    const bare = /^[@\w]/u.test(site.specifier) && !site.specifier.includes(':');
    if (!bare && !RELATIVE_SPECIFIER.test(site.specifier)) {
      throw new Error(
        `${importer} imports '${site.specifier}', which is neither relative nor a package name, so no lint input can key it`,
      );
    }
    return [{ importer, from, kind: site.kind, specifier: site.specifier, bare, jiti: instance }];
  });
}

/**
 * `source` as jiti compiles it, leaving the process's stack-trace globals as
 * they were.
 *
 * jiti compiles with Babel, whose first transform replaces
 * `Error.prepareStackTrace` with a frame-hiding wrapper around the one it found
 * and raises `Error.stackTraceLimit` to 50, for the life of the process. This
 * walk runs inside whichever process builds the Nx graph (the Nx CLI, its
 * daemon, smoo), so the wrapper would outlive it there. Under Bun the wrapped
 * default is Bun's native `Error.prepareStackTrace`, which throws "First
 * argument must be an Error object" for the non-Error targets
 * `Error.captureStackTrace` accepts (oven-sh/bun#46393): every later stack
 * capture onto such an object then fails in that process, as follow-redirects'
 * error types do when axios loads. The walk only reads the transform's output.
 */
function jitiTransform(instance: Jiti.Jiti, source: string, filename: string): string {
  const { prepareStackTrace, stackTraceLimit } = Error;
  try {
    return instance.transform({ source, filename, ts: /\.[mc]?tsx?$/u.test(filename) });
  } finally {
    Error.prepareStackTrace = prepareStackTrace;
    Error.stackTraceLimit = stackTraceLimit;
  }
}

/**
 * The jiti ESLint loads a TypeScript config with: the one ESLint's own
 * `import('jiti')` reaches from the workspace's ESLint, which a linker that
 * isolates packages need not hoist to the workspace root.
 */
function workspaceJiti(workspaceRoot: string): typeof Jiti {
  try {
    const eslint = createRequire(join(workspaceRoot, 'package.json')).resolve('eslint');
    const jiti: typeof Jiti = createRequire(eslint)('jiti');
    return jiti;
  } catch (error) {
    throw new Error(
      `ESLint loads a TypeScript config through jiti, which the ESLint ${workspaceRoot} resolves cannot import: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Every load in `code`: a static import or re-export that survives type
 * erasure, and every `import()`, `require()` or jiti `jitiImport()` call. A
 * call whose specifier is not a literal, a loader function used as a value,
 * and `createRequire` are computed loads.
 */
function loadSites(code: string, fileName: string): LoadSite[] {
  const file = ts.createSourceFile(fileName, code, ts.ScriptTarget.Latest, true);
  const sites: LoadSite[] = [];
  const visit = (node: ts.Node): void => {
    if (ts.isImportDeclaration(node) && !node.importClause?.isTypeOnly && ts.isStringLiteral(node.moduleSpecifier)) {
      sites.push({ kind: 'import', specifier: node.moduleSpecifier.text });
    } else if (
      ts.isExportDeclaration(node) &&
      !node.isTypeOnly &&
      node.moduleSpecifier !== undefined &&
      ts.isStringLiteral(node.moduleSpecifier)
    ) {
      sites.push({ kind: 'import', specifier: node.moduleSpecifier.text });
    } else if (ts.isCallExpression(node)) {
      const callee = node.expression;
      const kind: LoadKind | null =
        callee.kind === ts.SyntaxKind.ImportKeyword || (ts.isIdentifier(callee) && callee.text === 'jitiImport')
          ? 'import'
          : ts.isIdentifier(callee) && callee.text === 'require'
            ? 'require'
            : null;
      if (kind !== null) {
        const [argument] = node.arguments;
        sites.push(
          argument !== undefined && ts.isStringLiteralLike(argument)
            ? { kind, specifier: argument.text }
            : { kind: 'computed', text: node.getText(file) },
        );
        node.arguments.forEach(visit);
        return;
      }
    } else if (ts.isIdentifier(node) && loaderValue(node)) {
      sites.push({ kind: 'computed', text: node.parent.getText(file) });
    } else if (ts.isElementAccessExpression(node) && loaderElement(node)) {
      sites.push({ kind: 'computed', text: node.getText(file) });
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return sites;
}

/**
 * A loader the walk cannot read a specifier from: `createRequire` anywhere, a
 * `.require` member (`module.require`), and `require` or `jitiImport`
 * anywhere but a direct call, `typeof`, or `require.resolve`, which loads
 * nothing (`require.call`, `require.apply`, aliases, arguments).
 */
function loaderValue(node: ts.Identifier): boolean {
  const parent = node.parent;
  if (node.text === 'createRequire') return true;
  if (ts.isPropertyAccessExpression(parent) && parent.name === node) return node.text === 'require';
  if (node.text !== 'require' && node.text !== 'jitiImport') return false;
  if (ts.isPropertyAssignment(parent) && parent.name === node) return false;
  return !(
    (ts.isTypeOfExpression(parent) && parent.expression === node) ||
    (node.text === 'require' &&
      ts.isPropertyAccessExpression(parent) &&
      parent.expression === node &&
      parent.name.text === 'resolve')
  );
}

/** Loader functions a module can reach by name. */
const LOADER_NAMES = new Set(['require', 'createRequire', 'jitiImport']);
/** Values a loader function hangs off, whose computed members can be one. */
const LOADER_HOLDERS = new Set(['module', 'require', 'jitiImport', 'globalThis', 'global']);

/**
 * A loader reached through brackets: a literal loader name (`module['require']`),
 * or any computed member of a value that holds one (`module[key]`).
 */
function loaderElement(node: ts.ElementAccessExpression): boolean {
  const key = node.argumentExpression;
  if (ts.isStringLiteralLike(key)) return LOADER_NAMES.has(key.text);
  return ts.isIdentifier(node.expression) && LOADER_HOLDERS.has(node.expression.text);
}

/** The file jiti loads for `specifier` from `from`: its runtime resolver, under the call's conditions. */
function jitiResolve(jiti: Jiti.Jiti | null, kind: LoadKind, specifier: string, from: string): string | null {
  if (jiti === null) throw new Error(`${from}: a jiti-loaded module has no jiti instance`);
  const url = jiti.esmResolve(specifier, {
    parentURL: pathToFileURL(from).href,
    conditions: ['node', kind],
    try: true,
  });
  return url === undefined ? null : fileURLToPath(url);
}

/** Node's own resolution of every load in one layer, from the `node` that runs ESLint; `null` where Node throws. */
function nodeResolve(loads: readonly PendingLoad[]): (string | null)[] {
  if (loads.length === 0) return [];
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: node resolves the specifiers on its stdin from the filesystem and exits; it waits on no other process
  const output = execFileSync(
    'node',
    ['--experimental-import-meta-resolve', '--no-warnings', '--input-type=module', '--eval', NODE_RESOLVER],
    {
      input: JSON.stringify(
        loads.map(({ kind, specifier, from }) => ({
          kind,
          specifier,
          from: kind === 'require' ? from : pathToFileURL(from).href,
        })),
      ),
      encoding: 'utf8',
    },
  );
  const resolved = typia.json.assertParse<(string | null)[]>(output);
  if (resolved.length !== loads.length) {
    throw new Error(`node resolved ${resolved.length} of ${loads.length} ESLint config loads`);
  }
  return resolved.map((target) => {
    if (target === null) return null;
    const path = target.startsWith('file:') ? fileURLToPath(target) : target;
    return statSync(path, { throwIfNoEntry: false })?.isFile() === true ? path : null;
  });
}

/**
 * The workspace package.json files that decide how `real` loads: the nearest
 * one above it, whose `type` sets its module format; the one in the directory
 * a relative specifier entered, whose `main` chose it; and a bare specifier's
 * package root, whose `main` or `exports` chose it. The root manifest is a
 * check input already.
 */
function packageMetadata(root: string, load: PendingLoad, real: string): string[] {
  const manifests = new Set<string>();
  const nearest = manifestAbove(root, dirname(real), () => true);
  if (nearest !== null) manifests.add(nearest);
  const base = resolve(dirname(load.from), load.specifier);
  if (load.bare) {
    const [scope, name] = load.specifier.split('/');
    const packageName = load.specifier.startsWith('@') ? `${scope}/${name}` : scope;
    const owner = manifestAbove(
      root,
      dirname(real),
      (manifest) => typia.json.isParse<{ name?: string }>(readFileSync(manifest, 'utf8'))?.name === packageName,
    );
    if (owner !== null) manifests.add(owner);
  } else if (statSync(base, { throwIfNoEntry: false })?.isDirectory() === true) {
    const entered = join(realpathSync(base), 'package.json');
    if (dirname(entered) !== root && isFile(entered)) manifests.add(entered);
  }
  return [...manifests].map((manifest) => relative(root, manifest).split(sep).join('/'));
}

/** The first package.json `accept` takes at or above `start`, below the workspace root. */
function manifestAbove(root: string, start: string, accept: (manifest: string) => boolean): string | null {
  for (let directory = start; directory.startsWith(root + sep); directory = dirname(directory)) {
    const manifest = join(directory, 'package.json');
    if (isFile(manifest) && accept(manifest)) return manifest;
  }
  return null;
}

function isFile(path: string): boolean {
  return statSync(path, { throwIfNoEntry: false })?.isFile() === true;
}
