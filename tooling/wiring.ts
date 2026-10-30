/// <reference types="bun" />
// WIRING: what a change adds is reached by production code.
//
// A component that is built, tested and accepted but that no production path calls is dead weight that reads as a
// feature (AGENTS.md rule 7). This check reads only what a diff adds and asks, of each
// added item, whether non-test code in the post-change tree reaches it:
//
//   export  a TS/JS exported value: a non-test module imports it through an import that resolves to its module
//           (relative paths and workspace package `exports` maps, through re-exporting barrels), or its own module
//           uses it. A same-name identifier elsewhere is not a caller. A published package's entry modules are roots.
//   pub     a Rust `pub` item: a non-test, non-`#[cfg(test)]` occurrence in its own crate or in a crate that depends on
//           it; a `#[napi]` item by its JavaScript name in non-test TS/JS; a `#[no_mangle]` item by any other file; or an
//           external consumer its crate's `Cargo.toml` declares (`[[package.metadata.wiring.external]]`, below).
//
// An external consumer is a crate outside this repository that calls a `pub` item, such as a consumer repository's
// layer over a library this one publishes. Its declaration names the item, the consumer crate and the path in the
// consumer's repository that calls it:
//
//   [[package.metadata.wiring.external]]
//   item = "feed_bind"
//   consumer = "rete"
//   path = "crates/rete/src/pass.rs"
//
// This repository cannot see that call, so the declaration is checked from both sides: here, the consumer must be no
// crate of this tree (a caller here is wired here); in the consumer's repository, its own lint joins every declaration
// naming one of its crates against its tree (through `cargo metadata`, which carries each dependency's
// `package.metadata`) and fails when the named path no longer calls the item. Such an item lands in the same round as
// its consumer's call.
//   op      a `*_OP` wire-op constant: a send site, i.e. a non-test use that is not a dispatch (`case X:`, `X =>`,
//           `[X]:`, a comparison), by its constant or its string value.
//   verb    a CLI verb (a clap `Subcommand` variant, a typer `@….command`): a non-test file outside the CLI's own
//           package that invokes it.
//   script  a new file under bin/, scripts/ or tooling/: a non-test file names its path or imports it, or its published
//           package's manifest exposes it (`bin`, `exports`, also under `publishConfig`) through the package's emit.
//   target  an Nx target: another target's dependsOn or command, or a non-test file, runs it; or its own metadata says
//           who runs its cases (`ownedBy`) or why no gate does (`ungated`, a closed enum; `ci` names a workflow that
//           runs it). A continuous watcher has no verdict.
//
// Test code is never a caller: a component only a test reaches is the defect, so there is no exemption marker.
//
//   bun tooling/wiring.ts --base <commit oid>
//
// The base is a commit oid, never a ref: the Nx target caches on the tree plus its arguments, and an oid names
// immutable content, so a cached verdict cannot outlive a moved branch.
import { readFileSync, statSync } from 'node:fs';
import { posix, resolve } from 'node:path';
import { isRecord } from '@smoothbricks/validation';
import ts from 'typescript';

const REPO_ROOT = resolve(import.meta.dir, '..');

// =============================================================================
// Paths
// =============================================================================

/** Trees no source of this checkout lives in: installs, build outputs, vendored code. */
const SKIPPED = /(^|\/)(node_modules|dist|target|\.nx|vendor|extern)\//;
/** Test code: test trees, fixtures, benches and examples, and test-named files. */
const TEST_PATH =
  /(^|\/)(tests?|__tests__|__mocks__|fixtures?|testdata|test-support|testing|benches|examples)\/|[._-](test|spec|fixtures?|bench|property)\.[^/]+$|_test\.(go|py|rs)$|(^|\/)(test_[^/]*|conftest)\.py$/;
const TS_SOURCE = /\.(ts|tsx|mts|cts|js|jsx|mjs|cjs)$/;
const TS_EXTENSIONS = ['.ts', '.tsx', '.mts', '.cts', '.js', '.jsx', '.mjs', '.cjs'] as const;
/** The directories whose new files are run, not imported: their reach is a caller naming them. */
const SCRIPT_DIR = /(^|\/)(bin|scripts)\/|^tooling\//;
const SCRIPT_FILE = /(\.(ts|js|mjs|cjs|sh|bash|py)|\/[^./]+)$/;

function isTestPath(path: string): boolean {
  return TEST_PATH.test(path);
}

function isSourcePath(path: string): boolean {
  return !SKIPPED.test(path) && !isTestPath(path);
}

function isTsSource(path: string): boolean {
  return TS_SOURCE.test(path) && !path.endsWith('.d.ts') && isSourcePath(path);
}

// =============================================================================
// The tree and the change, injected
// =============================================================================

/** The post-change tree the verdict reads; the shell backs it with git, the fixtures with a map. */
export interface WiringTree {
  /** Every tracked path. */
  readonly files: readonly string[];
  /** A tracked file's text, undefined when it cannot be read. */
  textOf(path: string): string | undefined;
  /** Non-test tracked paths whose text holds `needle`; with `word`, only as a whole word. A superset is harmless. */
  filesHolding(needle: string, word: boolean): readonly string[];
}

/** One file the diff added or modified: its text at the base (undefined when new) and after. */
export interface WiringChange {
  readonly path: string;
  readonly before: string | undefined;
  readonly after: string;
}

export type WiringKind = 'export' | 'pub' | 'op' | 'verb' | 'script' | 'target';

/** One item the change added. */
export interface Added {
  readonly kind: WiringKind;
  readonly name: string;
  readonly file: string;
  readonly line: number;
}

/** An added item no production code reaches, with what would wire it. */
export interface WiringFinding {
  readonly added: Added;
  readonly claim: string;
}

function lineAt(text: string, offset: number): number {
  let line = 1;
  for (let i = 0; i < offset && i < text.length; i++) if (text.charCodeAt(i) === 10) line++;
  return line;
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function wordIn(text: string, word: string): boolean {
  return new RegExp(`(?<![A-Za-z0-9_$])${escapeRegExp(word)}(?![A-Za-z0-9_$])`).test(text);
}

// =============================================================================
// TypeScript / JavaScript modules
// =============================================================================

/** One exported value of a module: the name importers use, the local binding, its line. */
interface TsExport {
  readonly name: string;
  readonly local: string;
  readonly line: number;
}

/** How a module uses another one, by specifier. */
type ModuleUse =
  | { readonly spec: string; readonly kind: 'named'; readonly name: string }
  | { readonly spec: string; readonly kind: 'all' }
  | { readonly spec: string; readonly kind: 'reexport'; readonly name: string; readonly as: string }
  | { readonly spec: string; readonly kind: 'reexport-all' };

interface TsModule {
  readonly exports: readonly TsExport[];
  readonly uses: readonly ModuleUse[];
  /** Identifier references by name, declarations' own names and property names excluded. */
  readonly references: ReadonlyMap<string, number>;
}

function scriptKind(path: string): ts.ScriptKind {
  if (path.endsWith('.tsx')) return ts.ScriptKind.TSX;
  if (path.endsWith('.jsx')) return ts.ScriptKind.JSX;
  if (/\.(js|mjs|cjs)$/.test(path)) return ts.ScriptKind.JS;
  return ts.ScriptKind.TS;
}

function hasModifier(node: ts.Node, kind: ts.SyntaxKind): boolean {
  return ts.canHaveModifiers(node) && (ts.getModifiers(node)?.some((m) => m.kind === kind) ?? false);
}

function bindingNames(name: ts.BindingName, out: ts.Identifier[]): void {
  if (ts.isIdentifier(name)) {
    out.push(name);
    return;
  }
  for (const element of name.elements) if (!ts.isOmittedExpression(element)) bindingNames(element.name, out);
}

/** An identifier that names a binding's use, not a declaration, a property or an import/export specifier. */
function isReference(id: ts.Identifier): boolean {
  const parent = id.parent;
  if (
    (ts.isPropertyAccessExpression(parent) && parent.name === id) ||
    (ts.isQualifiedName(parent) && parent.right === id) ||
    (ts.isPropertyAssignment(parent) && parent.name === id) ||
    ts.isImportSpecifier(parent) ||
    ts.isExportSpecifier(parent) ||
    ts.isImportClause(parent) ||
    ts.isNamespaceImport(parent) ||
    ts.isLabeledStatement(parent) ||
    ts.isBreakOrContinueStatement(parent)
  )
    return false;
  if (
    (ts.isFunctionDeclaration(parent) ||
      ts.isClassDeclaration(parent) ||
      ts.isEnumDeclaration(parent) ||
      ts.isInterfaceDeclaration(parent) ||
      ts.isTypeAliasDeclaration(parent) ||
      ts.isModuleDeclaration(parent) ||
      ts.isVariableDeclaration(parent) ||
      ts.isParameter(parent) ||
      ts.isBindingElement(parent) ||
      ts.isMethodDeclaration(parent) ||
      ts.isPropertyDeclaration(parent) ||
      ts.isPropertySignature(parent) ||
      ts.isMethodSignature(parent) ||
      ts.isGetAccessorDeclaration(parent) ||
      ts.isSetAccessorDeclaration(parent) ||
      ts.isEnumMember(parent) ||
      ts.isTypeParameterDeclaration(parent) ||
      ts.isFunctionExpression(parent) ||
      ts.isClassExpression(parent)) &&
    parent.name === id
  )
    return false;
  // `{ a }` in a destructuring pattern declares `a`; `{ a }` in an object literal uses it.
  if (ts.isBindingElement(parent) && parent.propertyName === id) return false;
  return true;
}

function stringSpec(node: ts.Node | undefined): string | undefined {
  return node !== undefined && ts.isStringLiteralLike(node) ? node.text : undefined;
}

function parseTsModule(path: string, text: string): TsModule {
  const sf = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, scriptKind(path));
  const exports: TsExport[] = [];
  const uses: ModuleUse[] = [];
  const references = new Map<string, number>();
  const localTypes = new Set<string>();
  /** Each value import binding: the specifier and the name it imports (`*` a namespace, `default` a default). */
  const imports = new Map<string, { readonly spec: string; readonly name: string }>();
  const line = (node: ts.Node): number => sf.getLineAndCharacterOfPosition(node.getStart(sf)).line + 1;
  const exportValue = (name: string, local: string, node: ts.Node): void => {
    exports.push({ name, local, line: line(node) });
  };
  for (const st of sf.statements) {
    if (ts.isInterfaceDeclaration(st) || ts.isTypeAliasDeclaration(st)) localTypes.add(st.name.text);
    const spec = ts.isImportDeclaration(st) ? stringSpec(st.moduleSpecifier) : undefined;
    const clause = ts.isImportDeclaration(st) ? st.importClause : undefined;
    if (spec === undefined || clause === undefined || clause.isTypeOnly) continue;
    if (clause.name !== undefined) imports.set(clause.name.text, { spec, name: 'default' });
    const bindings = clause.namedBindings;
    if (bindings !== undefined && ts.isNamespaceImport(bindings)) imports.set(bindings.name.text, { spec, name: '*' });
    else if (bindings !== undefined)
      for (const el of bindings.elements)
        if (!el.isTypeOnly) imports.set(el.name.text, { spec, name: (el.propertyName ?? el.name).text });
  }
  for (const st of sf.statements) {
    const exported = hasModifier(st, ts.SyntaxKind.ExportKeyword);
    const isDefault = hasModifier(st, ts.SyntaxKind.DefaultKeyword);
    const ambient = hasModifier(st, ts.SyntaxKind.DeclareKeyword);
    if (exported && !ambient) {
      if ((ts.isFunctionDeclaration(st) || ts.isClassDeclaration(st)) && (st.name !== undefined || isDefault)) {
        const local = st.name?.text ?? 'default';
        exportValue(isDefault ? 'default' : local, local, st);
      } else if (ts.isEnumDeclaration(st)) {
        exportValue(st.name.text, st.name.text, st);
      } else if (ts.isVariableStatement(st)) {
        for (const decl of st.declarationList.declarations) {
          const ids: ts.Identifier[] = [];
          bindingNames(decl.name, ids);
          for (const id of ids) exportValue(id.text, id.text, decl);
        }
      }
    }
    if (ts.isExportAssignment(st) && !st.isExportEquals) {
      const local = ts.isIdentifier(st.expression) ? st.expression.text : 'default';
      exportValue('default', local, st);
    }
    if (ts.isExportDeclaration(st) && !st.isTypeOnly) {
      const spec = stringSpec(st.moduleSpecifier);
      const clause = st.exportClause;
      if (spec !== undefined) {
        if (clause === undefined) uses.push({ spec, kind: 'reexport-all' });
        else if (ts.isNamespaceExport(clause)) uses.push({ spec, kind: 'reexport', name: '*', as: clause.name.text });
        else
          for (const el of clause.elements)
            if (!el.isTypeOnly)
              uses.push({ spec, kind: 'reexport', name: (el.propertyName ?? el.name).text, as: el.name.text });
      } else if (clause !== undefined && ts.isNamedExports(clause)) {
        for (const el of clause.elements) {
          const local = (el.propertyName ?? el.name).text;
          if (el.isTypeOnly) continue;
          // `import { a } from './m'; export { a }` re-exports m's `a`; it declares nothing here.
          const imported = imports.get(local);
          if (imported !== undefined)
            uses.push({ spec: imported.spec, kind: 'reexport', name: imported.name, as: el.name.text });
          else if (!localTypes.has(local)) exportValue(el.name.text, local, el);
        }
      }
    }
    if (ts.isImportEqualsDeclaration(st) && ts.isExternalModuleReference(st.moduleReference)) {
      const spec = stringSpec(st.moduleReference.expression);
      if (spec !== undefined) uses.push({ spec, kind: 'all' });
    }
    // CommonJS: `module.exports = { a, b }`, `exports.a = …`, `module.exports.a = …`.
    if (ts.isExpressionStatement(st) && ts.isBinaryExpression(st.expression)) {
      const left = st.expression.left.getText(sf);
      const right = st.expression.right;
      if (left === 'module.exports' && ts.isObjectLiteralExpression(right)) {
        for (const p of right.properties)
          if (p.name !== undefined && ts.isIdentifier(p.name)) exportValue(p.name.text, p.name.text, p);
      }
      const cjs = /^(?:module\.)?exports\.([A-Za-z_$][\w$]*)$/.exec(left);
      if (cjs?.[1] !== undefined) exportValue(cjs[1], cjs[1], st);
    }
  }
  // An import is used where its binding is referenced: `ns.a` uses a namespace's `a`, any other use of `ns` (a
  // spread, an argument) uses everything. An import only re-exported is judged through its re-export.
  const namespaceUses = new Map<string, Set<string> | 'all'>();
  const visit = (node: ts.Node): void => {
    if (ts.isIdentifier(node) && isReference(node)) {
      references.set(node.text, (references.get(node.text) ?? 0) + 1);
      if (imports.get(node.text)?.name === '*') {
        const parent = node.parent;
        const seen = namespaceUses.get(node.text);
        if (ts.isPropertyAccessExpression(parent) && parent.expression === node) {
          if (seen !== 'all') namespaceUses.set(node.text, (seen ?? new Set()).add(parent.name.text));
        } else namespaceUses.set(node.text, 'all');
      }
    }
    if (ts.isCallExpression(node)) {
      const spec = stringSpec(node.arguments[0]);
      const dynamic = node.expression.kind === ts.SyntaxKind.ImportKeyword;
      const required = ts.isIdentifier(node.expression) && node.expression.text === 'require';
      if (spec !== undefined && (dynamic || required)) uses.push({ spec, kind: 'all' });
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  for (const [local, { spec, name }] of imports) {
    if (name !== '*') {
      if ((references.get(local) ?? 0) > 0) uses.push({ spec, kind: 'named', name });
      continue;
    }
    const seen = namespaceUses.get(local);
    if (seen === 'all') uses.push({ spec, kind: 'all' });
    else for (const member of seen ?? []) uses.push({ spec, kind: 'named', name: member });
  }
  return { exports, uses, references };
}

/** A line that receives an op rather than sending it: a match arm, a `case`, a handler key, a comparison. */
function lineDispatches(line: string, needle: string): boolean {
  const n = escapeRegExp(needle);
  return (
    new RegExp(`\\bcase\\s+["'\`]?${n}["'\`]?\\s*:`).test(line) ||
    new RegExp(`^(?:\\|\\s*)?["']?${n}["']?\\s*(?:=>|\\|)`).test(line.trim()) ||
    new RegExp(`\\[\\s*${n}\\s*\\]\\s*:`).test(line) ||
    new RegExp(`(?:[=!]==?)\\s*["'\`]?${n}(?![\\w-])|(?<![\\w-])${n}["'\`]?\\s*[=!]==?`).test(line) ||
    new RegExp(`^\\s*["']${n}["']\\s*:`).test(line) ||
    // A match arm names it before `=>` (`Some(X_OP) =>`), `matches!` compares it.
    new RegExp(`(?<![\\w-])${n}(?![\\w-])["']?[^\\n]*=>`).test(line) ||
    new RegExp(`\\bmatches!\\([^\\n]*(?<![\\w-])${n}(?![\\w-])`).test(line)
  );
}

const COMPARISONS: readonly ts.SyntaxKind[] = [
  ts.SyntaxKind.EqualsEqualsEqualsToken,
  ts.SyntaxKind.EqualsEqualsToken,
  ts.SyntaxKind.ExclamationEqualsEqualsToken,
  ts.SyntaxKind.ExclamationEqualsToken,
];

/**
 * Whether a TS/JS module sends op `name` (value `value`): a reference to the constant, or the string value, outside
 * a dispatch position (a `case`, a computed handler key, a comparison, a property key, a type) and outside the
 * constant's own declaration and import/export specifiers.
 */
function tsOpSends(path: string, text: string, name: string, value: string | undefined): boolean {
  const sf = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, scriptKind(path));
  const dispatching = (node: ts.Node): boolean => {
    const parent = node.parent;
    return (
      (ts.isCaseClause(parent) && parent.expression === node) ||
      ts.isComputedPropertyName(parent) ||
      (ts.isBinaryExpression(parent) && COMPARISONS.includes(parent.operatorToken.kind)) ||
      (ts.isPropertyAssignment(parent) && parent.name === node) ||
      ts.isLiteralTypeNode(parent) ||
      (ts.isVariableDeclaration(parent) && ts.isIdentifier(parent.name) && parent.name.text === name)
    );
  };
  let sends = false;
  const visit = (node: ts.Node): void => {
    if (sends) return;
    if (ts.isIdentifier(node) && node.text === name && isReference(node) && !dispatching(node)) sends = true;
    if (ts.isStringLiteralLike(node) && node.text === value && !dispatching(node)) sends = true;
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return sends;
}

// =============================================================================
// Module resolution over the tree: relative paths and workspace package exports
// =============================================================================

interface Package {
  readonly dir: string;
  readonly name: string;
  readonly private: boolean;
  readonly manifest: Record<string, unknown>;
  /** Where its TypeScript build puts each source directory, declared layouts first. */
  readonly emits: readonly Emit[];
}

/** One TypeScript emit: the build of the sources under `root` lands under `out`. */
interface Emit {
  readonly root: string;
  readonly out: string;
}

/**
 * The emits of the package in `dir`, from every tsconfig it owns: each effective `rootDir` → `outDir` (see
 * {@link compilerDirs}). A package that declares none builds the conventional layouts, `dist/` from `src/` or from
 * itself. An emit that leaves its package (a proof build into the workspace cache) builds nothing its manifest names,
 * so a package whose every emit leaves it maps no build path back to a source.
 */
function packageEmits(dir: string, tsconfigs: readonly string[], tree: WiringTree): Emit[] {
  const inside = (path: string): boolean =>
    path !== dir && under(path, dir) && path !== '..' && !path.startsWith('../');
  const emits: Emit[] = [];
  let declares = false;
  for (const path of tsconfigs) {
    const { rootDir, outDir } = compilerDirs(path, tree, []);
    if (typeof outDir !== 'string') continue;
    if (typeof rootDir !== 'string')
      throw new WiringInputError(
        `${path} sets outDir without rootDir, so its build cannot be mapped back to the sources it compiles`,
      );
    declares = true;
    if ((rootDir === dir || inside(rootDir)) && inside(outDir)) emits.push({ root: rootDir, out: outDir });
  }
  if (declares) return emits;
  return [
    { root: posix.join(dir, 'src'), out: posix.join(dir, 'dist') },
    { root: dir, out: posix.join(dir, 'dist') },
  ];
}

/** A tsconfig's effective `rootDir`/`outDir`, workspace-relative; null where a file unset it, undefined where none set it. */
interface CompilerDirs {
  readonly rootDir: string | null | undefined;
  readonly outDir: string | null | undefined;
}

/**
 * The `rootDir`/`outDir` of the tsconfig at `path`: its own over those of every file it `extends`, later over
 * earlier, each resolved against the file that sets it. A package specifier is a published base, which sets no emit of
 * this workspace.
 */
function compilerDirs(path: string, tree: WiringTree, chain: readonly string[]): CompilerDirs {
  if (chain.includes(path)) throw new WiringInputError(`${[...chain, path].join(' extends ')} is a cycle`);
  const text = tree.textOf(path);
  if (text === undefined)
    throw new WiringInputError(`${chain.at(-1) ?? path} extends ${path}, which is not in the tree`);
  const parsed = ts.parseConfigFileTextToJson(path, text);
  if (parsed.error !== undefined)
    throw new WiringInputError(
      `${path} is not a readable tsconfig: ${ts.flattenDiagnosticMessageText(parsed.error.messageText, ' ')}`,
    );
  const config: unknown = parsed.config;
  if (!isRecord(config)) throw new WiringInputError(`${path} is not a JSON object`);
  const dir = posix.dirname(path);
  const parents =
    typeof config.extends === 'string' ? [config.extends] : Array.isArray(config.extends) ? config.extends : [];
  let dirs: CompilerDirs = { rootDir: undefined, outDir: undefined };
  for (const parent of parents) {
    if (typeof parent !== 'string' || !parent.startsWith('.')) continue;
    const file = posix.join(dir, parent.endsWith('.json') ? parent : `${parent}.json`);
    dirs = over(dirs, compilerDirs(file, tree, [...chain, path]));
  }
  const options = isRecord(config.compilerOptions) ? config.compilerOptions : {};
  const own = (value: unknown): string | null | undefined =>
    typeof value === 'string' ? posix.join(dir, value) : value === null ? null : undefined;
  return over(dirs, { rootDir: own(options.rootDir), outDir: own(options.outDir) });
}

/** `top`'s keys over `base`'s: a set or unset key wins, an absent one inherits. */
function over(base: CompilerDirs, top: CompilerDirs): CompilerDirs {
  return {
    rootDir: top.rootDir === undefined ? base.rootDir : top.rootDir,
    outDir: top.outDir === undefined ? base.outDir : top.outDir,
  };
}

/** Whether `path` is `dir` or lies beneath it; `.` holds every workspace path. */
function under(path: string, dir: string): boolean {
  return dir === '.' || path === dir || path.startsWith(`${dir}/`);
}

/** The emits whose `side` holds `path` most specifically: the longest such directory, ties kept. */
function nearest(emits: readonly Emit[], path: string, side: 'root' | 'out'): Emit[] {
  let best: Emit[] = [];
  let depth = -1;
  for (const emit of emits) {
    const at = emit[side];
    if (!under(path, at)) continue;
    const length = at === '.' ? 0 : at.length;
    if (length > depth) {
      best = [emit];
      depth = length;
    } else if (length === depth) best.push(emit);
  }
  return best;
}

/** `path` moved from under `from` to under `to`, or undefined when it is not under `from`. */
function rebased(path: string, from: string, to: string): string | undefined {
  if (!under(path, from)) return undefined;
  if (from === '.') return posix.join(to, path);
  return path === from ? to : posix.join(to, path.slice(from.length + 1));
}

/** A manifest the verdict cannot read: the run fails naming it instead of judging a tree with a hole in it. */
class WiringInputError extends Error {}

/** A crate outside this tree that calls one of this crate's `pub` items, from `[[package.metadata.wiring.external]]`. */
export interface ExternalConsumer {
  readonly item: string;
  readonly consumer: string;
  readonly path: string;
}

/** The external consumers `manifest` (a `Cargo.toml`'s text) declares; a malformed declaration is refused, never skipped. */
export function externalConsumers(manifestPath: string, manifest: string): ExternalConsumer[] {
  if (!manifest.includes('wiring')) return [];
  let parsed: unknown;
  try {
    parsed = Bun.TOML.parse(manifest);
  } catch (cause) {
    throw new WiringInputError(
      `${manifestPath} cannot be parsed: ${cause instanceof Error ? cause.message : String(cause)}`,
    );
  }
  const at = (value: unknown, key: string): unknown =>
    typeof value === 'object' && value !== null ? Reflect.get(value, key) : undefined;
  const declared = at(at(at(at(parsed, 'package'), 'metadata'), 'wiring'), 'external');
  if (declared === undefined) return [];
  if (!Array.isArray(declared))
    throw new WiringInputError(`${manifestPath}: package.metadata.wiring.external is not an array of tables`);
  return declared.map((entry: unknown, index) => {
    const field = (key: string): string => {
      const value = at(entry, key);
      if (typeof value !== 'string' || value === '')
        throw new WiringInputError(
          `${manifestPath}: package.metadata.wiring.external[${index}] names no ${key} (item, consumer and path are required)`,
        );
      return value;
    };
    return { item: field('item'), consumer: field('consumer'), path: field('path') };
  });
}

/** A production manifest's JSON object (package.json, project.json, executors.json). */
function parseManifest(path: string, text: string): Record<string, unknown> {
  let json: unknown;
  try {
    json = JSON.parse(text);
  } catch (cause) {
    throw new WiringInputError(`${path} is not valid JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
  if (!isRecord(json)) throw new WiringInputError(`${path} is not a JSON object`);
  return json;
}

/**
 * Manifest fields that resolve imports, run commands or list dependencies. Any other field naming a module of its
 * package (a host's extension list) is a host's loading contract, so that module is an entry.
 */
const RESOLUTION_FIELDS: Readonly<Record<string, true>> = {
  exports: true,
  main: true,
  module: true,
  types: true,
  typings: true,
  bin: true,
  scripts: true,
  nx: true,
  files: true,
  workspaces: true,
  dependencies: true,
  devDependencies: true,
  peerDependencies: true,
  optionalDependencies: true,
  publishConfig: true,
};

/** Every string leaf of an `exports`/`bin` value: condition objects and arrays flatten. */
function leaves(value: unknown): string[] {
  if (typeof value === 'string') return [value];
  if (Array.isArray(value)) return value.flatMap(leaves);
  if (isRecord(value)) return Object.values(value).flatMap(leaves);
  return [];
}

/** The `exports` targets a package subpath (`.`, `./x`) names, `*` patterns substituted. */
function exportTargets(manifest: Record<string, unknown>, subpath: string): string[] {
  const field = manifest.exports;
  if (field === undefined) {
    if (subpath !== '.') return [subpath];
    const entry = [manifest.module, manifest.main, manifest.types].filter((v) => typeof v === 'string');
    return entry.length > 0 ? entry.flatMap(leaves) : ['./index'];
  }
  if (!isRecord(field) || !Object.keys(field).some((k) => k.startsWith('.')))
    return subpath === '.' ? leaves(field) : [];
  const exact = field[subpath];
  if (exact !== undefined) return leaves(exact);
  const out: string[] = [];
  for (const [key, value] of Object.entries(field)) {
    const star = key.indexOf('*');
    if (star < 0) continue;
    const prefix = key.slice(0, star);
    const suffix = key.slice(star + 1);
    if (subpath.startsWith(prefix) && subpath.endsWith(suffix) && subpath.length >= prefix.length + suffix.length) {
      const hole = subpath.slice(prefix.length, subpath.length - suffix.length);
      out.push(...leaves(value).map((t) => t.replaceAll('*', hole)));
    }
  }
  return out;
}

/** The tracked source files a module path may name: JS extensions mapped to their sources, an emit's build to its root. */
function sourceCandidates(path: string, tracked: ReadonlySet<string>, emits: readonly Emit[]): string[] {
  const norm = posix.normalize(path);
  const bases = new Set<string>([norm]);
  for (const emit of nearest(emits, norm, 'out')) {
    const source = rebased(norm, emit.out, emit.root);
    if (source !== undefined) bases.add(source);
  }
  const out = new Set<string>();
  for (const base of bases) {
    if (tracked.has(base) && TS_SOURCE.test(base)) out.add(base);
    const stem = base.replace(/\.d\.(ts|mts|cts)$/, '').replace(/\.(ts|tsx|mts|cts|js|jsx|mjs|cjs)$/, '');
    // A module file wins over a same-stem directory's index, as Node and TypeScript resolve it.
    const files = TS_EXTENSIONS.map((ext) => stem + ext).filter((p) => tracked.has(p));
    const found =
      files.length > 0 ? files : TS_EXTENSIONS.map((ext) => `${stem}/index${ext}`).filter((p) => tracked.has(p));
    for (const p of found) out.add(p);
  }
  return [...out];
}

class Resolver {
  readonly #tracked: ReadonlySet<string>;
  readonly #packages: readonly Package[];
  readonly #cache = new Map<string, readonly string[]>();

  constructor(tree: WiringTree) {
    this.#tracked = new Set(tree.files);
    const manifests: Omit<Package, 'emits'>[] = [];
    for (const path of tree.files) {
      if (!path.endsWith('package.json') || SKIPPED.test(path) || isTestPath(path)) continue;
      if (path !== 'package.json' && !path.endsWith('/package.json')) continue;
      const manifest = parseManifest(path, tree.textOf(path) ?? '');
      if (typeof manifest.name !== 'string') continue;
      manifests.push({ dir: posix.dirname(path), name: manifest.name, private: manifest.private === true, manifest });
    }
    // Deepest dir first for ownership: a tsconfig belongs to the deepest package holding it.
    manifests.sort((a, b) => b.dir.length - a.dir.length);
    const tsconfigs = new Map<string, string[]>();
    for (const path of tree.files) {
      if (!/(^|\/)tsconfig[^/]*\.json$/.test(path) || SKIPPED.test(path) || isTestPath(path)) continue;
      const owner = manifests.find((p) => under(path, p.dir));
      if (owner !== undefined) tsconfigs.set(owner.dir, [...(tsconfigs.get(owner.dir) ?? []), path]);
    }
    this.#packages = manifests.map((p) => ({ ...p, emits: packageEmits(p.dir, tsconfigs.get(p.dir) ?? [], tree) }));
  }

  /** The source files `spec`, imported from `from`, may name. */
  resolve(from: string, spec: string): readonly string[] {
    const key = `${posix.dirname(from)}\0${spec}`;
    const hit = this.#cache.get(key);
    if (hit !== undefined) return hit;
    const result = this.#resolve(from, spec);
    this.#cache.set(key, result);
    return result;
  }

  #resolve(from: string, spec: string): readonly string[] {
    if (spec.startsWith('./') || spec.startsWith('../') || spec === '.' || spec === '..')
      return this.sources(posix.join(posix.dirname(from), spec));
    const bare = spec.startsWith('node:') ? undefined : spec;
    if (bare === undefined) return [];
    const owner = [...this.#packages]
      .filter((p) => bare === p.name || bare.startsWith(`${p.name}/`))
      .sort((a, b) => b.name.length - a.name.length)[0];
    if (owner === undefined) return [];
    const subpath = `.${bare.slice(owner.name.length)}`;
    return exportTargets(owner.manifest, subpath).flatMap((target) =>
      sourceCandidates(posix.join(owner.dir, target), this.#tracked, owner.emits),
    );
  }

  /** The tracked source files module path `path` may name, through its owning package's emits. */
  sources(path: string): readonly string[] {
    return sourceCandidates(path, this.#tracked, this.ownerOf(path)?.emits ?? []);
  }

  /** The package that owns `path`: the deepest directory with a named package.json. */
  ownerOf(path: string): Package | undefined {
    return this.#packages.find((p) => p.dir === '.' || path.startsWith(`${p.dir}/`));
  }

  /**
   * A module its own package's manifest makes an entry: a host field (an extension or plugin list) names it,
   * or a published (non-private) package exposes it through `exports`, `main`, `module` or `bin`, at the top level or
   * under `publishConfig` (what a publish rewrites them to). A private package's `publishConfig` exposes nothing.
   */
  isManifestEntry(path: string): boolean {
    const owner = this.ownerOf(path);
    if (owner === undefined || owner.dir === '.') return false;
    const m = owner.manifest;
    const host = Object.entries(m)
      .filter(([field]) => !Object.hasOwn(RESOLUTION_FIELDS, field))
      .flatMap(([, value]) => leaves(value))
      .filter((leaf) => leaf.startsWith('./'));
    const exposed = (fields: Record<string, unknown>): string[] => [
      ...leaves(fields.exports),
      ...leaves(fields.main),
      ...leaves(fields.module),
      ...leaves(fields.bin),
    ];
    const published = owner.private
      ? []
      : [...exposed(m), ...(isRecord(m.publishConfig) ? exposed(m.publishConfig) : [])];
    return [...host, ...published].some((template) => {
      const pattern = new RegExp(`^${escapeRegExp(posix.join(owner.dir, template)).replaceAll('\\*', '.+')}$`);
      return this.#addressesOf(path).some((address) => pattern.test(address));
    });
  }

  /** The paths a manifest entry may spell for a source file: itself, and its build under each emit of its package. */
  #addressesOf(path: string): string[] {
    const builds = new Set<string>([path]);
    for (const emit of nearest(this.ownerOf(path)?.emits ?? [], path, 'root')) {
      const built = rebased(path, emit.root, emit.out);
      if (built !== undefined) builds.add(built);
    }
    const out: string[] = [];
    for (const built of builds) {
      const stem = built.replace(/\.(ts|tsx|mts|cts)$/, '');
      out.push(built, `${stem}.js`, `${stem}.d.ts`, `${stem}.mjs`, `${stem}.cjs`);
    }
    return out;
  }
}

// =============================================================================
// Rust
// =============================================================================

/**
 * Whether the string literal opening at `at` is an argument of a macro invocation (`format!("…")`,
 * `write!(f, "…")`): only there does `{name}` capture a binding.
 */
function macroArgument(text: string, at: number): boolean {
  let depth = 0;
  for (let k = at - 1; k >= 0 && k > at - 4000; k--) {
    const c = text[k];
    if (c === ')' || c === ']' || c === '}') depth++;
    else if (c === '(' || c === '[' || c === '{') {
      if (depth === 0) return /![ \t]*$/.test(text.slice(Math.max(0, k - 8), k));
      depth--;
    } else if (depth === 0 && c === ';') return false;
  }
  return false;
}

/**
 * Rust text with comments, string and char literal contents, lifetimes and labels blanked to spaces; offsets and
 * newlines kept. A macro's format string keeps its inline captures (`{name}`, `{name:?}`), which are uses.
 */
function blankRust(text: string): string {
  const out = text.split('');
  const blank = (from: number, to: number): void => {
    for (let k = from; k < to; k++) if (out[k] !== '\n') out[k] = ' ';
  };
  let i = 0;
  while (i < text.length) {
    const c = text[i];
    const next = text[i + 1];
    if (c === '/' && next === '/') {
      const end = text.indexOf('\n', i);
      const stop = end < 0 ? text.length : end;
      blank(i, stop);
      i = stop;
    } else if (c === '/' && next === '*') {
      let depth = 1;
      let j = i + 2;
      while (j < text.length && depth > 0) {
        if (text[j] === '/' && text[j + 1] === '*') {
          depth++;
          j += 2;
        } else if (text[j] === '*' && text[j + 1] === '/') {
          depth--;
          j += 2;
        } else j++;
      }
      blank(i, j);
      i = j;
    } else if ((c === 'r' || (c === 'b' && next === 'r')) && /^b?r#*"/.test(text.slice(i, i + 260))) {
      const head = /^b?r(#*)"/.exec(text.slice(i, i + 260));
      const hashes = head?.[1] ?? '';
      const open = i + (head?.[0].length ?? 0);
      const close = text.indexOf(`"${hashes}`, open);
      const stop = close < 0 ? text.length : close;
      blank(open, stop);
      i = stop + 1 + hashes.length;
    } else if (c === '"') {
      let j = i + 1;
      while (j < text.length && text[j] !== '"') j += text[j] === '\\' ? 2 : 1;
      blank(i + 1, j);
      if (macroArgument(text, i)) {
        // `{{` is an escaped brace, not a capture.
        for (const capture of text.slice(i + 1, j).matchAll(/(?<!\{)\{([A-Za-z_][A-Za-z0-9_]*)[:}]/g)) {
          const at = i + 1 + capture.index + 1;
          for (let k = 0; k < (capture[1]?.length ?? 0); k++) out[at + k] = text[at + k] ?? ' ';
        }
      }
      i = j + 1;
    } else if (c === "'") {
      // A char literal ('a', '\n', '\u{..}'), else a lifetime or label ('a, 'outer), which names no item.
      const char = /^'(?:\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.)|[^\\'])'/u.exec(text.slice(i, i + 12));
      const lifetime = char === null ? /^'[A-Za-z_][A-Za-z0-9_]*/.exec(text.slice(i, i + 256)) : null;
      if (char !== null) {
        blank(i + 1, i + char[0].length - 1);
        i += char[0].length;
      } else if (lifetime !== null) {
        blank(i, i + lifetime[0].length);
        i += lifetime[0].length;
      } else i++;
    } else i++;
  }
  return out.join('');
}

function matchingBrace(text: string, open: number): number {
  let depth = 0;
  for (let i = open; i < text.length; i++) {
    if (text[i] === '{') depth++;
    else if (text[i] === '}') {
      depth--;
      if (depth === 0) return i;
    }
  }
  return text.length;
}

/** `#[cfg(test)]`, and `#[cfg(all(…))]`/`#[cfg(any(…))]` naming `test` (not `not(test)`); `#!` makes it the module's. */
const CFG_TEST = /#(!?)\[cfg\((?:test|(?:all|any)\([^\]]*(?<!not\()\btest\b[^\]]*\))\)\]/g;

/** Blanked Rust ({@link blankRust}) with every `#[cfg(test)]` item blanked too; `#![cfg(test)]` blanks it all. */
function withoutTests(blanked: string): string {
  const out = blanked.split('');
  for (const m of blanked.matchAll(CFG_TEST)) {
    if (m[1] === '!') return blanked.replace(/[^\n]/g, ' ');
    const start = m.index;
    const brace = blanked.indexOf('{', start);
    const semi = blanked.indexOf(';', start);
    const end = semi >= 0 && (brace < 0 || semi < brace) ? semi : matchingBrace(blanked, brace);
    for (let k = start; k <= end && k < out.length; k++) if (out[k] !== '\n') out[k] = ' ';
  }
  return out.join('');
}

/** The files of the out-of-line modules a Rust file declares under `#[cfg(test)]` (`#[cfg(test)] mod tests;`). */
function testModuleFiles(path: string, text: string): string[] {
  const blanked = blankRust(text);
  const base = posix.basename(path);
  const dir = /^(lib|main|mod)\.rs$/.test(base) ? posix.dirname(path) : path.replace(/\.rs$/, '');
  const out: string[] = [];
  for (const m of blanked.matchAll(CFG_TEST)) {
    if (m[1] === '!') continue;
    const decl = /^(?:\s*#\[[^\]]*\])*\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;/.exec(
      blanked.slice(m.index + m[0].length),
    );
    const name = decl?.[1];
    if (decl === null || name === undefined) continue;
    const declared = text.slice(m.index, m.index + m[0].length + decl[0].length);
    const pathAttr = /#\[path\s*=\s*"([^"]+)"\]/.exec(declared)?.[1];
    if (pathAttr !== undefined) out.push(posix.join(posix.dirname(path), pathAttr));
    else out.push(`${dir}/${name}.rs`, `${dir}/${name}/mod.rs`);
  }
  return out;
}

/** The original text of the outer attributes directly above `at`, doc comments between them included. */
function attributesBefore(text: string, blanked: string, at: number): string {
  let start = at;
  let k = at - 1;
  while (k >= 0) {
    while (k >= 0 && /\s/.test(blanked[k] ?? '')) k--;
    if (blanked[k] !== ']') break;
    let depth = 0;
    let j = k;
    for (; j >= 0; j--) {
      if (blanked[j] === ']') depth++;
      else if (blanked[j] === '[' && --depth === 0) break;
    }
    if (j < 1 || blanked[j - 1] !== '#') break;
    start = j - 1;
    k = j - 2;
  }
  return text.slice(start, at);
}

interface RustItem {
  readonly name: string;
  /** The restriction, whitespace dropped: empty for plain `pub`, `(crate)` for `pub(crate)`. */
  readonly visibility: string;
  readonly line: number;
  /** The JavaScript name a `#[napi]` item is called by. */
  readonly napi: string | undefined;
  /** The symbol a `#[no_mangle]`/`#[export_name]` item exports: its callers are outside Rust. */
  readonly abi: string | undefined;
}

/** A `pub` item, its restricted visibility (`pub(crate)`, `pub(super)`, `pub(in …)`) included and captured. */
const RUST_PUB =
  /\bpub(\s*\([^)]*\))?\s+(?:(?:const|async|unsafe|extern(?:\s+"\s*")?)\s+)*(fn|struct|enum|trait|type|const|static(?:\s+mut)?|union)\s+([A-Za-z_][A-Za-z0-9_]*)/g;

function camelCase(snake: string): string {
  return snake.replace(/_([a-z0-9])/g, (_, c: string) => c.toUpperCase());
}

function rustPubItems(text: string): RustItem[] {
  const blanked = blankRust(text);
  const prod = withoutTests(blanked);
  const items: RustItem[] = [];
  for (const m of prod.matchAll(RUST_PUB)) {
    const name = m[3];
    if (name === undefined) continue;
    const attrs = attributesBefore(text, blanked, m.index);
    const napiAttr = /#\[napi\b([^\]]*)\]/.exec(attrs);
    const jsName = napiAttr === null ? undefined : (/js_name\s*=\s*"([^"]+)"/.exec(napiAttr[1] ?? '')?.[1] ?? null);
    const napi = jsName === undefined ? undefined : (jsName ?? (m[2] === 'fn' ? camelCase(name) : name));
    const exported = /#\[(?:unsafe\()?export_name\s*=\s*"([^"]+)"/.exec(attrs)?.[1];
    const abi = exported ?? (/#\[(?:unsafe\()?no_mangle\b/.test(attrs) ? name : undefined);
    items.push({ name, visibility: (m[1] ?? '').replace(/\s+/g, ''), line: lineAt(text, m.index), napi, abi });
  }
  return items;
}

/** The crate directory owning a Rust file: the nearest ancestor with a Cargo.toml holding a [package]. */
function crateOf(path: string, tree: WiringTree): { dir: string; name: string } | undefined {
  let dir = posix.dirname(path);
  while (true) {
    const manifest = tree.textOf(dir === '.' ? 'Cargo.toml' : `${dir}/Cargo.toml`);
    const name = manifest === undefined ? undefined : /\[package\][^[]*?\bname\s*=\s*"([^"]+)"/s.exec(manifest)?.[1];
    if (name !== undefined) return { dir, name };
    if (dir === '.' || dir === '') return undefined;
    dir = posix.dirname(dir);
  }
}

function dependsOnCrate(dependent: { dir: string }, crate: string, tree: WiringTree): boolean {
  const manifest = tree.textOf(dependent.dir === '.' ? 'Cargo.toml' : `${dependent.dir}/Cargo.toml`) ?? '';
  // Cargo keys spell a crate with `-` or `_`, or rename it and name the crate as `package`.
  const spelled = escapeRegExp(crate).replace(/[-_]/g, '[-_]');
  return new RegExp(`^\\s*(?:${spelled}|[\\w-]+\\s*=\\s*\\{[^}]*package\\s*=\\s*"${spelled}")\\s*[=.]`, 'm').test(
    manifest,
  );
}

// =============================================================================
// CLI verbs
// =============================================================================

/** A clap variant's verb under the enum's `rename_all` rule (kebab-case when it names none). */
function clapCase(variant: string, rule: string | undefined): string {
  const words = variant.split(/(?<=[a-z0-9])(?=[A-Z])|_/).filter((w) => w !== '');
  const lower = words.map((w) => w.toLowerCase());
  switch (rule) {
    case undefined:
    case 'kebab-case':
      return lower.join('-');
    case 'snake_case':
      return lower.join('_');
    case 'SCREAMING_SNAKE_CASE':
      return lower.join('_').toUpperCase();
    case 'lower':
    case 'lowercase':
      return lower.join('');
    case 'UPPER':
    case 'UPPERCASE':
      return lower.join('').toUpperCase();
    case 'camelCase':
      return lower.map((w, k) => (k === 0 ? w : w.charAt(0).toUpperCase() + w.slice(1))).join('');
    case 'PascalCase':
    case 'verbatim':
      return variant;
    default:
      return lower.join('-');
  }
}

/** The verbs of every clap `Subcommand` enum in a Rust file. */
function clapVerbs(text: string): { name: string; line: number }[] {
  const blanked = blankRust(text);
  const prod = withoutTests(blanked);
  const out: { name: string; line: number }[] = [];
  for (const m of prod.matchAll(/#\[derive\([^)]*\bSubcommand\b[^)]*\)\][\s\S]*?\benum\s+\w+[^{]*\{/g)) {
    const item = m.index + m[0].search(/(?:\bpub(?:\s*\([^)]*\))?\s+)?\benum\b/);
    const rename = /#\[(?:command|clap)\([^\]]*\brename_all\s*=\s*"([^"]+)"/.exec(
      attributesBefore(text, blanked, item),
    );
    const open = m.index + m[0].length - 1;
    const close = matchingBrace(prod, open);
    let depth = 0;
    let i = open + 1;
    let attrs = '';
    while (i < close) {
      const ch = prod[i];
      if (ch === '{' || ch === '(') depth++;
      else if (ch === '}' || ch === ')') depth--;
      else if (depth === 0 && ch === '#') {
        const end = text.indexOf(']', i);
        attrs += text.slice(i, end + 1);
        i = end + 1;
        continue;
      } else if (depth === 0 && /[A-Z]/.test(ch ?? '')) {
        const variant = /^[A-Za-z0-9_]+/.exec(prod.slice(i))?.[0] ?? '';
        const named = /#\[(?:command|clap)\([^\]]*\bname\s*=\s*"([^"]+)"/.exec(attrs)?.[1];
        out.push({ name: named ?? clapCase(variant, rename?.[1]), line: lineAt(text, i) });
        attrs = '';
        i += variant.length;
        // Skip to the variant's end at this depth.
        while (i < close && !(depth === 0 && prod[i] === ',')) {
          if (prod[i] === '{' || prod[i] === '(') depth++;
          else if (prod[i] === '}' || prod[i] === ')') depth--;
          i++;
        }
        continue;
      }
      i++;
    }
  }
  return out;
}

/** The verbs of every typer `@app.command(...)` in a Python file. */
function typerVerbs(text: string): { name: string; line: number }[] {
  const out: { name: string; line: number }[] = [];
  for (const m of text.matchAll(/@\w+\.command\(([^)]*)\)\s*\n(?:\s*@[^\n]*\n)*\s*(?:async\s+)?def\s+(\w+)/g)) {
    const named = /^\s*(?:name\s*=\s*)?["']([^"']+)["']/.exec(m[1] ?? '')?.[1];
    out.push({ name: named ?? (m[2] ?? '').replaceAll('_', '-'), line: lineAt(text, m.index) });
  }
  return out;
}

// =============================================================================
// Nx targets
// =============================================================================

/** Why a target no gate runs is still accounted for: the closed set of reasons. */
const UNGATED_KINDS = [
  'operator',
  'bench',
  'release',
  'platform-run',
  'live-daemon',
  'sibling-checkout',
  'generate',
  'ci',
] as const;
type UngatedKind = (typeof UNGATED_KINDS)[number];

interface NxTarget {
  readonly project: string;
  readonly name: string;
  readonly line: number;
  readonly config: Record<string, unknown>;
  /** The package name of the project's package.json, which another project's dependencies name it by. */
  readonly packageName: string | undefined;
  /** The packages and projects this project depends on: what a `^target` dependency reaches. */
  readonly dependencies: readonly string[];
}

function isUngatedKind(value: unknown): value is UngatedKind {
  return UNGATED_KINDS.some((kind) => kind === value);
}

/** The targets a package.json (`nx.targets`) or project.json (`targets`) declares, with the project's name. */
function nxTargets(path: string, text: string): NxTarget[] {
  const isPackage = path === 'package.json' || path.endsWith('/package.json');
  const isProject = path === 'project.json' || path.endsWith('/project.json');
  if (!isPackage && !isProject) return [];
  const json = parseManifest(path, text);
  const nx = isPackage ? json.nx : json;
  const targets = isRecord(nx) ? nx.targets : undefined;
  // Nx names a package.json project by its `nx.name`, else the package name; a project.json by its `name`.
  const nxName = isRecord(nx) && typeof nx.name === 'string' ? nx.name : undefined;
  const project = nxName ?? (typeof json.name === 'string' ? json.name : '');
  if (!isRecord(targets)) return [];
  const packageName = isPackage && typeof json.name === 'string' ? json.name : undefined;
  const implicit = isRecord(nx) ? nx.implicitDependencies : undefined;
  const dependencies = [
    ...['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'].flatMap((field) => {
      const deps = json[field];
      return isRecord(deps) ? Object.keys(deps) : [];
    }),
    ...(Array.isArray(implicit) ? implicit.filter((d) => typeof d === 'string') : []),
  ];
  const block = text.indexOf('"targets"');
  return Object.entries(targets).flatMap(([name, config]) => {
    if (!isRecord(config)) return [];
    const key = new RegExp(`"${escapeRegExp(name)}"\\s*:\\s*\\{`, 'g');
    key.lastIndex = Math.max(0, block);
    const at = key.exec(text);
    return [{ project, name, line: at === null ? 1 : lineAt(text, at.index), config, packageName, dependencies }];
  });
}

/**
 * Whether `depender`'s `dependsOn` runs `target`: a bare name in its own project, `^name` in a project it depends on,
 * `project:name`, or the object forms of each.
 */
function dependsOnReaches(depender: NxTarget, target: NxTarget): boolean {
  const deps = depender.config.dependsOn;
  if (!Array.isArray(deps)) return false;
  const upstream =
    depender.dependencies.includes(target.project) ||
    (target.packageName !== undefined && depender.dependencies.includes(target.packageName));
  return deps.some((d) => {
    if (typeof d === 'string') {
      if (d === `${target.project}:${target.name}`) return true;
      if (d === `^${target.name}`) return upstream;
      return d === target.name && depender.project === target.project;
    }
    if (!isRecord(d) || d.target !== target.name) return false;
    if (d.dependencies === true) return upstream;
    const projects = dependsOnProjects(d);
    return projects === undefined ? depender.project === target.project : projectsName(projects, target.project);
  });
}

/** The `projects` a `dependsOn` object scopes itself to; undefined when it names none. */
function dependsOnProjects(d: Record<string, unknown>): readonly unknown[] | undefined {
  return typeof d.projects === 'string' ? [d.projects] : Array.isArray(d.projects) ? d.projects : undefined;
}

/** Whether a `dependsOn` `projects` scope holds `project`. */
function projectsName(projects: readonly unknown[], project: string): boolean {
  return projects.some((p) => p === project || p === '*');
}

/** What a target's own metadata says about its reach; undefined when it says nothing. */
function targetDeclaration(
  target: NxTarget,
  tree: WiringTree,
): { ok: true } | { ok: false; claim: string } | undefined {
  const metadata = target.config.metadata;
  // A watcher never ends, so it has no verdict for a gate to run.
  if (target.config.continuous === true) return { ok: true };
  if (!isRecord(metadata)) return undefined;
  const owned = metadata.ownedBy;
  if (owned !== undefined) {
    const owners = typeof owned === 'string' ? [owned] : Array.isArray(owned) ? owned : [];
    return owners.length > 0 && owners.every((o) => typeof o === 'string' && /^[^:\s]+:[^:\s]+$/.test(o))
      ? { ok: true }
      : { ok: false, claim: 'metadata.ownedBy must name each owner as "<project>:<target>"' };
  }
  const ungated = metadata.ungated;
  if (ungated === undefined) return undefined;
  if (!isRecord(ungated) || !isUngatedKind(ungated.kind))
    return {
      ok: false,
      claim: `metadata.ungated must be { "kind": ${UNGATED_KINDS.map((k) => `"${k}"`).join(' | ')} }, not prose`,
    };
  if (ungated.kind !== 'ci') return { ok: true };
  const workflow = ungated.workflow;
  const text = typeof workflow === 'string' ? tree.textOf(workflow) : undefined;
  if (text === undefined)
    return { ok: false, claim: 'metadata.ungated kind "ci" must name its workflow file as "workflow"' };
  return targetRunIn(text, target.project, target.name)
    ? { ok: true }
    : { ok: false, claim: `metadata.ungated names workflow ${String(workflow)}, which does not run this target` };
}

/**
 * Whether `text` runs `project:name`: `project:name`, `nx name project`, a `-t`/`--targets` list naming it on a line
 * whose `-p`/`--projects` (if any) names the project, or a `{ project, target }` object naming both.
 */
function targetRunIn(text: string, project: string, name: string): boolean {
  const n = escapeRegExp(name);
  const p = escapeRegExp(project);
  if (project !== '' && new RegExp(`(?<![\\w@/.-])${p}:${n}(?![\\w:-])`).test(text)) return true;
  if (project !== '' && new RegExp(`\\bnx\\s+${n}\\s+["']?${p}(?![\\w@/.-])`).test(text)) return true;
  for (const line of text.split('\n')) {
    // Only an Nx invocation's `-t`: `tar -t` or `sort -t` names no target.
    if (!/\bnx\b|\brun-many\b|\baffected\b/.test(line)) continue;
    for (const m of line.matchAll(/(?:^|[\s"'])(?:-t|--targets?)(?:=|\s+)([\w:-]+(?:(?:,|\s+)(?!-)[\w:-]+)*)/g)) {
      if (!(m[1] ?? '').split(/[,\s]+/).includes(name)) continue;
      const scoped = /(?:^|[\s"'])(?:-p|--projects?)(?:=|\s+)(\S+)/.exec(line)?.[1];
      if (
        scoped === undefined ||
        scoped.split(',').some((s) => s === project || s.includes('*') || s.startsWith('tag:'))
      )
        return true;
    }
  }
  if (project === '') return false;
  return [...text.matchAll(/\{[^{}]*\}/g)].some(
    ([object]) =>
      new RegExp(`\\btarget["']?\\s*:\\s*["']${n}["']`).test(object) &&
      new RegExp(`\\bproject["']?\\s*:\\s*["']${p}["']`).test(object),
  );
}

// =============================================================================
// What a change adds
// =============================================================================

function tsExportNames(path: string, text: string | undefined): Map<string, TsExport> {
  const out = new Map<string, TsExport>();
  if (text === undefined) return out;
  for (const e of parseTsModule(path, text).exports) if (!out.has(e.name)) out.set(e.name, e);
  return out;
}

/** Every item the change adds, in file order. */
export function addedItems(changes: readonly WiringChange[]): Added[] {
  const out: Added[] = [];
  for (const { path, before, after } of changes) {
    if (!isSourcePath(path)) continue;
    if (before === undefined && SCRIPT_DIR.test(path) && SCRIPT_FILE.test(path) && !path.endsWith('.json'))
      out.push({ kind: 'script', name: posix.basename(path), file: path, line: 1 });
    if (isTsSource(path)) {
      const old = tsExportNames(path, before);
      for (const [name, e] of tsExportNames(path, after)) {
        if (old.has(name)) continue;
        out.push({ kind: /_OP$/.test(name) ? 'op' : 'export', name, file: path, line: e.line });
      }
    }
    if (path.endsWith('.rs')) {
      // Keyed by visibility too: widening `pub(crate)` to `pub` adds a public item.
      const key = (i: RustItem): string => `${i.visibility} ${i.name}`;
      const old = new Set(before === undefined ? [] : rustPubItems(before).map(key));
      for (const item of rustPubItems(after))
        if (!old.has(key(item)))
          out.push({ kind: /_OP$/.test(item.name) ? 'op' : 'pub', name: item.name, file: path, line: item.line });
      const oldVerbs = new Set(before === undefined ? [] : clapVerbs(before).map((v) => v.name));
      for (const v of clapVerbs(after))
        if (!oldVerbs.has(v.name)) out.push({ kind: 'verb', name: v.name, file: path, line: v.line });
    }
    if (path.endsWith('.py')) {
      const oldVerbs = new Set(before === undefined ? [] : typerVerbs(before).map((v) => v.name));
      for (const v of typerVerbs(after))
        if (!oldVerbs.has(v.name)) out.push({ kind: 'verb', name: v.name, file: path, line: v.line });
    }
    if (path.endsWith('package.json') || path.endsWith('project.json')) {
      const old = new Set(before === undefined ? [] : nxTargets(path, before).map((t) => t.name));
      for (const t of nxTargets(path, after)) {
        // A block with no executor and no command overrides a plugin-inferred target; it adds none.
        const options = t.config.options;
        const runs =
          t.config.executor !== undefined ||
          t.config.command !== undefined ||
          (isRecord(options) && options.command !== undefined);
        if (runs && !old.has(t.name)) out.push({ kind: 'target', name: t.name, file: path, line: t.line });
      }
    }
  }
  return out;
}

// =============================================================================
// The root lint's scope: projects that declare themselves wired
// =============================================================================

/**
 * The Nx project tag that puts a whole project under the root lint: every item it holds is judged as if the change
 * had added it. A project carries it once it is clean, and the wire/delete sweep adds it as it clears each one, so the
 * enforced scope only grows. There is no exemption list: an untagged project is judged only by the diff check.
 */
export const WIRING_TAG = 'wiring:enforced';

interface NxProject {
  readonly name: string;
  readonly root: string;
  readonly tags: readonly string[];
}

/** The Nx projects the manifests declare: a project.json, else a package.json (Nx's own two sources). */
function nxProjects(tree: WiringTree): NxProject[] {
  const byRoot = new Map<string, NxProject>();
  for (const path of tree.files) {
    const base = posix.basename(path);
    if ((base !== 'package.json' && base !== 'project.json') || SKIPPED.test(path) || isTestPath(path)) continue;
    const json = parseManifest(path, tree.textOf(path) ?? '');
    const root = posix.dirname(path);
    const nx = base === 'package.json' ? json.nx : json;
    const nxName = isRecord(nx) && typeof nx.name === 'string' ? nx.name : undefined;
    const name = nxName ?? (typeof json.name === 'string' ? json.name : undefined);
    if (name === undefined || (base === 'package.json' && byRoot.get(root) !== undefined)) continue;
    const rawTags = isRecord(nx) ? nx.tags : undefined;
    const tags = Array.isArray(rawTags) ? rawTags.filter((t) => typeof t === 'string') : [];
    byRoot.set(root, { name, root, tags });
  }
  return [...byRoot.values()];
}

/** The enforced projects and every file they own (the deepest project root holding it), each as newly added. */
export function enforcedTree(tree: WiringTree): { projects: string[]; changes: WiringChange[] } {
  const projects = nxProjects(tree).sort((a, b) => b.root.length - a.root.length);
  const ownerOf = (path: string): NxProject | undefined =>
    projects.find((p) => p.root === '.' || path.startsWith(`${p.root}/`));
  const changes: WiringChange[] = [];
  for (const path of tree.files) {
    if (!ownerOf(path)?.tags.includes(WIRING_TAG)) continue;
    const after = tree.textOf(path);
    if (after !== undefined) changes.push({ path, before: undefined, after });
  }
  return {
    projects: projects.filter((p) => p.tags.includes(WIRING_TAG)).map((p) => p.name),
    changes,
  };
}

// =============================================================================
// The judge
// =============================================================================

class Judge {
  readonly #tree: WiringTree;
  readonly #resolver: Resolver;
  readonly #modules = new Map<string, TsModule | undefined>();

  constructor(tree: WiringTree) {
    this.#tree = tree;
    this.#resolver = new Resolver(tree);
  }

  #module(path: string): TsModule | undefined {
    if (this.#modules.has(path)) return this.#modules.get(path);
    const text = this.#tree.textOf(path);
    const parsed = text === undefined || !TS_SOURCE.test(path) ? undefined : parseTsModule(path, text);
    this.#modules.set(path, parsed);
    return parsed;
  }

  /** Rust files only `#[cfg(test)] mod x;` declarations compile: test code under a non-test path. */
  #testRust: ReadonlySet<string> | undefined;

  /** Whether `path` is test code: by its path, or as an out-of-line `#[cfg(test)]` module. */
  isTest(path: string): boolean {
    if (!isSourcePath(path)) return true;
    if (!path.endsWith('.rs')) return false;
    this.#testRust ??= new Set(
      this.#tree
        .filesHolding('cfg', true)
        .filter((file) => file.endsWith('.rs') && isSourcePath(file))
        .flatMap((file) => testModuleFiles(file, this.#tree.textOf(file) ?? '')),
    );
    return this.#testRust.has(path);
  }

  #production(paths: readonly string[]): string[] {
    return paths.filter((path) => !this.isTest(path));
  }

  /** Whether module `at`'s export `name` is (or re-exports) `origin`'s export `originName`. */
  #exportsOrigin(at: string, name: string, origin: string, originName: string, seen: Set<string>): boolean {
    const key = `${at}\0${name}`;
    if (seen.has(key)) return false;
    seen.add(key);
    if (at === origin && name === originName) return true;
    const mod = this.#module(at);
    if (mod === undefined) return false;
    // A local export (`export { foo as bar }`) is the origin's export when both name the same binding.
    const local = mod.exports.find((e) => e.name === name);
    if (local !== undefined)
      return at === origin && mod.exports.some((e) => e.name === originName && e.local === local.local);
    for (const use of mod.uses) {
      const targets = this.#resolver.resolve(at, use.spec);
      if (use.kind === 'reexport' && use.as === name) {
        if (use.name === '*') {
          if (targets.includes(origin)) return true;
        } else if (targets.some((t) => this.#exportsOrigin(t, use.name, origin, originName, seen))) return true;
      }
      if (use.kind === 'reexport-all' && name !== 'default')
        if (targets.some((t) => this.#exportsOrigin(t, name, origin, originName, seen))) return true;
    }
    return false;
  }

  /** Whether `use` in module `at` reaches `origin`'s export `name`; `as` is the name it re-exports, if any. */
  #useReaches(at: string, use: ModuleUse, origin: string, name: string): 'no' | 'yes' | { reexportedAs: string } {
    const targets = this.#resolver.resolve(at, use.spec);
    if (targets.length === 0) return 'no';
    const via = (n: string): boolean => targets.some((t) => this.#exportsOrigin(t, n, origin, name, new Set()));
    switch (use.kind) {
      case 'named':
        return via(use.name) ? 'yes' : 'no';
      case 'all':
        return targets.includes(origin) || via(name) ? 'yes' : 'no';
      case 'reexport':
        if (use.name === '*') return targets.includes(origin) || via(name) ? { reexportedAs: use.as } : 'no';
        return via(use.name) ? { reexportedAs: use.as } : 'no';
      case 'reexport-all':
        return name !== 'default' && via(name) ? { reexportedAs: name } : 'no';
    }
  }

  /** The names a module may be addressed by in an import specifier, beyond the export's own name. */
  #moduleKeys(path: string): string[] {
    const keys: string[] = [];
    const stem = posix.basename(path).replace(/\.[^.]+$/, '');
    keys.push(stem === 'index' ? posix.basename(posix.dirname(path)) : stem);
    const owner = this.#resolver.ownerOf(path);
    if (owner !== undefined && owner.dir !== '.') keys.push(owner.name);
    return keys.filter((k) => k.length > 0);
  }

  /** Whether non-test code reaches export `name` of module `path`, through importers and re-exporting barrels. */
  exportReached(path: string, name: string, seen = new Set<string>()): boolean {
    const key = `${path}\0${name}`;
    if (seen.has(key)) return false;
    seen.add(key);
    if (this.#resolver.isManifestEntry(path)) return true;
    if (name === 'default' && /\.config\.[cm]?[jt]s$/.test(path)) return true;
    const mod = this.#module(path);
    const local = mod?.exports.find((e) => e.name === name)?.local ?? name;
    // Its own module uses it: declarations, property names and specifiers are not references.
    if ((mod?.references.get(local) ?? 0) > 0) return true;
    const candidates = new Set<string>(this.#production(this.#tree.filesHolding(name, true)));
    for (const k of this.#moduleKeys(path))
      for (const f of this.#production(this.#tree.filesHolding(k, false))) candidates.add(f);
    for (const file of candidates) {
      if (file === path) continue;
      if (this.#nxConfigNames(file, path, name)) return true;
      if (!TS_SOURCE.test(file)) continue;
      const m = this.#module(file);
      if (m === undefined) continue;
      for (const use of m.uses) {
        const reach = this.#useReaches(file, use, path, name);
        if (reach === 'yes') return true;
        if (reach !== 'no' && this.exportReached(file, reach.reexportedAs, seen)) return true;
      }
    }
    return false;
  }

  /** An Nx `executors.json`/`generators.json` entry naming the module (and `#name`, else its default export). */
  #nxConfigNames(file: string, path: string, name: string): boolean {
    if (!/(^|\/)(executors|generators|migrations)\.json$/.test(file)) return false;
    const json = parseManifest(file, this.#tree.textOf(file) ?? '');
    const dir = posix.dirname(file);
    for (const entry of leaves(json)) {
      const [modulePath, member] = entry.split('#');
      if (modulePath === undefined || !modulePath.startsWith('.')) continue;
      if (!this.#resolver.sources(posix.join(dir, modulePath)).includes(path)) continue;
      if ((member ?? 'default') === name) return true;
    }
    return false;
  }

  /** Whether non-test Rust outside the item's own declaration names it, in its crate or a crate depending on it. */
  #rustUsed(path: string, item: RustItem): boolean {
    const crate = crateOf(path, this.#tree);
    const word = new RegExp(`(?<![A-Za-z0-9_])${escapeRegExp(item.name)}(?![A-Za-z0-9_])`, 'g');
    for (const file of this.#production(this.#tree.filesHolding(item.name, true))) {
      if (!file.endsWith('.rs')) continue;
      const other = crateOf(file, this.#tree);
      const foreign = crate !== undefined && other !== undefined && other.dir !== crate.dir;
      if (foreign && !dependsOnCrate(other, crate.name, this.#tree)) continue;
      const text = withoutTests(blankRust(this.#tree.textOf(file) ?? ''));
      for (const m of text.matchAll(word)) if (file !== path || lineAt(text, m.index) !== item.line) return true;
    }
    return false;
  }

  #rustItem(path: string, name: string): RustItem | undefined {
    return rustPubItems(this.#tree.textOf(path) ?? '').find((i) => i.name === name);
  }

  pubReached(path: string, name: string): boolean {
    const item = this.#rustItem(path, name);
    if (item === undefined) return true;
    const { napi, abi } = item;
    if (napi !== undefined) {
      for (const file of this.#production(this.#tree.filesHolding(napi, true)))
        if (TS_SOURCE.test(file) && wordIn(this.#tree.textOf(file) ?? '', napi)) return true;
    }
    // An exported symbol's callers name its symbol, in any language, outside its own file.
    if (abi !== undefined && this.#production(this.#tree.filesHolding(abi, true)).some((file) => file !== path))
      return true;
    return this.#rustUsed(path, item) || this.#externallyConsumed(path, name);
  }

  /** Whether the item's crate declares a consumer outside this tree that calls it (`[[package.metadata.wiring.external]]`). */
  #externallyConsumed(path: string, name: string): boolean {
    const crate = crateOf(path, this.#tree);
    if (crate === undefined) return false;
    const manifest = crate.dir === '.' ? 'Cargo.toml' : `${crate.dir}/Cargo.toml`;
    const declared = externalConsumers(manifest, this.#tree.textOf(manifest) ?? '').filter((d) => d.item === name);
    if (declared.length === 0) return false;
    const local = new Set(
      this.#tree.files.flatMap((file) => {
        if (posix.basename(file) !== 'Cargo.toml') return [];
        const package_ = /\[package\][^[]*?\bname\s*=\s*"([^"]+)"/s.exec(this.#tree.textOf(file) ?? '')?.[1];
        return package_ === undefined ? [] : [package_];
      }),
    );
    // A consumer of this tree is wired here or not at all: its declaration vouches for nothing.
    return declared.some((d) => !local.has(d.consumer));
  }

  /** A send site of a wire op: a non-test use of its constant or its string value that is not a dispatch. */
  opSent(path: string, name: string): boolean {
    const text = this.#tree.textOf(path) ?? '';
    const declared = new RegExp(`\\b${escapeRegExp(name)}\\b[^=\\n]*=\\s*["'\`]([^"'\`]+)["'\`]`).exec(text);
    const value = declared?.[1];
    const declLine = declared === null ? 0 : lineAt(text, declared.index);
    const files = new Set(this.#production(this.#tree.filesHolding(name, true)));
    if (value !== undefined) for (const f of this.#production(this.#tree.filesHolding(value, false))) files.add(f);
    for (const file of files) {
      const body = this.#tree.textOf(file) ?? '';
      if (TS_SOURCE.test(file)) {
        if (tsOpSends(file, body, name, value)) return true;
        continue;
      }
      // Rust reads its production code, `use` imports blanked; other files are read as written.
      const code = file.endsWith('.rs')
        ? withoutTests(blankRust(body)).replace(/\b(?:pub(?:\([^)]*\))?\s+)?use\s+[^;]*;/g, (m) =>
            m.replace(/[^\n]/g, ' '),
          )
        : body;
      const lines = body.split('\n');
      const codeLines = code.split('\n');
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i] ?? '';
        const codeLine = codeLines[i] ?? '';
        if ((file === path && i + 1 === declLine) || codeLine.trim() === '') continue;
        const named = wordIn(codeLine, name);
        const valued = value !== undefined && new RegExp(`["'\`]${escapeRegExp(value)}["'\`]`).test(line);
        if ((named && !lineDispatches(line, name)) || (valued && !lineDispatches(line, value))) return true;
      }
    }
    return false;
  }

  verbInvoked(path: string, name: string): boolean {
    const owner = path.endsWith('.rs') ? crateOf(path, this.#tree)?.dir : this.#resolver.ownerOf(path)?.dir;
    const quoted = new RegExp(`["'\`]${escapeRegExp(name)}["'\`]|\\s${escapeRegExp(name)}(?:\\s|$)`, 'm');
    for (const file of this.#production(this.#tree.filesHolding(name, false))) {
      if (owner !== undefined && owner !== '.' && file.startsWith(`${owner}/`)) continue;
      if (/\.md$/.test(file)) continue;
      if (quoted.test(this.#tree.textOf(file) ?? '')) return true;
    }
    return false;
  }

  scriptNamed(path: string): boolean {
    // A published package's `bin` (or another entry its manifest exposes) is the script's caller.
    if (this.#resolver.isManifestEntry(path)) return true;
    const base = posix.basename(path);
    const named = new RegExp(`(?:^|[\\s/'"\`=(])${escapeRegExp(base)}(?![\\w.-])`, 'm');
    for (const file of this.#production(this.#tree.filesHolding(base, false))) {
      if (file === path || /\.md$/.test(file)) continue;
      if (named.test(this.#tree.textOf(file) ?? '')) return true;
    }
    if (!TS_SOURCE.test(path)) return false;
    const stem = base.replace(/\.[^.]+$/, '');
    for (const file of this.#production(this.#tree.filesHolding(stem, false))) {
      if (file === path || !TS_SOURCE.test(file)) continue;
      const m = this.#module(file);
      if (m?.uses.some((u) => this.#resolver.resolve(file, u.spec).includes(path))) return true;
    }
    return false;
  }

  targetVerdict(path: string, name: string): { ok: true } | { ok: false; claim: string } {
    const target = nxTargets(path, this.#tree.textOf(path) ?? '').find((t) => t.name === name);
    if (target === undefined) return { ok: true };
    const declared = targetDeclaration(target, this.#tree);
    if (declared !== undefined) return declared;
    // nx.json targetDefaults give every project's target of that name its dependsOn, `^name` included; an object
    // entry scoped by `projects` runs the target only in the projects it names.
    const nxJson = this.#tree.textOf('nx.json');
    const defaults = nxJson === undefined ? undefined : parseManifest('nx.json', nxJson).targetDefaults;
    if (isRecord(defaults)) {
      for (const config of Object.values(defaults)) {
        const deps = isRecord(config) && Array.isArray(config.dependsOn) ? config.dependsOn : [];
        if (
          deps.some(
            (d) =>
              d === name ||
              d === `^${name}` ||
              (isRecord(d) && d.target === name && projectsName(dependsOnProjects(d) ?? ['*'], target.project)),
          )
        )
          return { ok: true };
      }
    }
    for (const file of this.#production(this.#tree.filesHolding(name, false))) {
      if (/\.md$/.test(file)) continue;
      const body = this.#tree.textOf(file) ?? '';
      for (const other of nxTargets(file, body)) {
        if (other.project === target.project && other.name === name) continue;
        if (dependsOnReaches(other, target)) return { ok: true };
        if (targetRunIn(JSON.stringify(other.config), target.project, name)) return { ok: true };
      }
      if (file !== path && targetRunIn(body, target.project, name)) return { ok: true };
    }
    return {
      ok: false,
      claim:
        "no gate, target or script runs it: add it to a gated target's dependsOn, or declare metadata.ownedBy " +
        `("<project>:<target>") or metadata.ungated ({ "kind": ${UNGATED_KINDS.map((k) => `"${k}"`).join(' | ')} })`,
    };
  }
}

const CLAIMS: Readonly<Record<Exclude<WiringKind, 'target'>, string>> = {
  export: 'no non-test module imports it: wire it into the production path that needs it, or delete it',
  pub: 'no non-test code outside #[cfg(test)] uses it: wire it into the production path that needs it, or delete it',
  op: 'no non-test code sends it (only dispatch sites name it): add the client call that sends it, or delete the op',
  verb: 'nothing outside its CLI invokes it: call it from the script, target or code that needs it, or delete it',
  script: 'no target, script or module names it: run it from the target that needs it, or delete it',
};

/** Every added item no production code reaches. */
export function judgeAdded(added: readonly Added[], tree: WiringTree): WiringFinding[] {
  const judge = new Judge(tree);
  const findings: WiringFinding[] = [];
  for (const item of added) {
    if (judge.isTest(item.file)) continue;
    switch (item.kind) {
      case 'export':
        if (!judge.exportReached(item.file, item.name)) findings.push({ added: item, claim: CLAIMS.export });
        break;
      case 'pub':
        if (!judge.pubReached(item.file, item.name)) findings.push({ added: item, claim: CLAIMS.pub });
        break;
      case 'op':
        if (!judge.opSent(item.file, item.name)) findings.push({ added: item, claim: CLAIMS.op });
        break;
      case 'verb':
        if (!judge.verbInvoked(item.file, item.name)) findings.push({ added: item, claim: CLAIMS.verb });
        break;
      case 'script':
        if (!judge.scriptNamed(item.file)) findings.push({ added: item, claim: CLAIMS.script });
        break;
      case 'target': {
        const verdict = judge.targetVerdict(item.file, item.name);
        if (!verdict.ok) findings.push({ added: item, claim: verdict.claim });
        break;
      }
      default:
        throw new Error(`wiring: unexpected kind ${item.kind satisfies never}`);
    }
  }
  return findings;
}

function formatFinding(f: WiringFinding): string {
  return `error[WIRING] ${f.added.file}:${f.added.line} ${f.added.kind} ${f.added.name}: ${f.claim}`;
}

// =============================================================================
// Shell: git supplies the tree and the change
// =============================================================================

function git(args: readonly string[]): { ok: true; out: string } | { ok: false; err: string } {
  const run = Bun.spawnSync(['git', ...args], { cwd: REPO_ROOT, stdout: 'pipe', stderr: 'pipe' });
  return run.exitCode === 0
    ? { ok: true, out: run.stdout.toString() }
    : { ok: false, err: `git ${args.join(' ')} exited ${run.exitCode}: ${run.stderr.toString().trim()}` };
}

/** Text files the verdict searches: production sources and configs, read once; binaries and blobs skipped. */
const SEARCH_MAX_BYTES = 4_000_000;

/** The files Nx hashes for a whole-workspace input: tracked, and untracked but not ignored, on disk. */
function gitTree(): WiringTree {
  const listed = git(['ls-files', '--cached', '--others', '--exclude-standard', '-z']);
  if (!listed.ok) throw new WiringInputError(listed.err);
  const files = [...new Set(listed.out.split('\0'))].filter((path) => {
    if (path === '') return false;
    try {
      return statSync(resolve(REPO_ROOT, path)).isFile();
    } catch {
      // Listed by the index but deleted from the worktree: Nx hashes no such file, so neither does the verdict.
      return false;
    }
  });
  const known = new Set(files);
  const texts = new Map<string, string>();
  const textOf = (path: string): string | undefined => {
    if (!known.has(path)) return undefined;
    const cached = texts.get(path);
    if (cached !== undefined) return cached;
    let text: string;
    try {
      text = readFileSync(resolve(REPO_ROOT, path), 'utf8');
    } catch (cause) {
      throw new WiringInputError(`${path} cannot be read: ${cause instanceof Error ? cause.message : String(cause)}`);
    }
    texts.set(path, text);
    return text;
  };
  let searched: readonly (readonly [string, string])[] | undefined;
  const searchable = (): readonly (readonly [string, string])[] =>
    (searched ??= files.filter(isSourcePath).flatMap((path) => {
      const text = textOf(path);
      return text === undefined || text.length > SEARCH_MAX_BYTES || text.includes('\0') ? [] : [[path, text] as const];
    }));
  // Whole-word lookups of an identifier read one index built on first use; anything else scans the texts.
  let words: Map<string, string[]> | undefined;
  const wordIndex = (): Map<string, string[]> => {
    if (words !== undefined) return words;
    const index = new Map<string, string[]>();
    for (const [path, text] of searchable()) {
      for (const token of new Set(text.match(/[A-Za-z_$][A-Za-z0-9_$]*/g))) {
        const paths = index.get(token);
        if (paths === undefined) index.set(token, [path]);
        else paths.push(path);
      }
    }
    words = index;
    return index;
  };
  const holding = new Map<string, readonly string[]>();
  return {
    files,
    textOf,
    filesHolding(needle, word) {
      const key = `${word ? 'w' : 's'}\0${needle}`;
      const hit = holding.get(key);
      if (hit !== undefined) return hit;
      const out =
        word && /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(needle)
          ? (wordIndex().get(needle) ?? [])
          : searchable()
              .filter(([, text]) => text.includes(needle) && (!word || wordIn(text, needle)))
              .map(([path]) => path);
      holding.set(key, out);
      return out;
    },
  };
}

/** What the worktree adds or modifies since `base`, untracked files included as added. */
function gitChanges(base: string, tree: WiringTree): WiringChange[] {
  const diff = git(['diff', '--name-status', '-z', '--no-renames', base]);
  if (!diff.ok) throw new WiringInputError(diff.err);
  const untracked = git(['ls-files', '--others', '--exclude-standard', '-z']);
  if (!untracked.ok) throw new WiringInputError(untracked.err);
  const parts = diff.out.split('\0').filter((p) => p !== '');
  const statuses: [string, string][] = [];
  for (let i = 0; i + 1 < parts.length; i += 2) statuses.push([parts[i] ?? '', parts[i + 1] ?? '']);
  for (const path of untracked.out.split('\0')) if (path !== '') statuses.push(['A', path]);
  const out: WiringChange[] = [];
  for (const [status, path] of statuses) {
    const after = status === 'D' ? undefined : tree.textOf(path);
    if (after === undefined) continue;
    if (status === 'A') {
      out.push({ path, before: undefined, after });
      continue;
    }
    const shown = git(['show', `${base}:${path}`]);
    if (!shown.ok) throw new WiringInputError(shown.err);
    out.push({ path, before: shown.out, after });
  }
  return out;
}

/** What the run judges: the change since `base`, or with no base every file of the enforced projects. */
function changeScope(base: string | undefined, tree: WiringTree): { changes: WiringChange[]; since: string } {
  if (base !== undefined) return { changes: gitChanges(base, tree), since: `since ${base.slice(0, 12)}` };
  const { projects, changes } = enforcedTree(tree);
  return { changes, since: `in ${projects.length} project(s) tagged ${WIRING_TAG}` };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const at = args.indexOf('--base');
  const base = at >= 0 ? args[at + 1] : undefined;
  const whole = args.includes('--tree');
  if (whole === (base !== undefined) || (base !== undefined && !/^[0-9a-f]{40}$/.test(base))) {
    console.error(
      'wiring: pass --tree (every project tagged ' +
        WIRING_TAG +
        ', the root lint) or --base <commit oid> (what a change adds since that commit; the land passes the ' +
        'main head it rebased onto, by hand --base "$(git merge-base HEAD main)"). A ref name would let a cached verdict ' +
        'outlive its move.',
    );
    process.exit(2);
  }
  const tree = gitTree();
  const { changes, since } = changeScope(base, tree);
  const added = addedItems(changes);
  const findings = judgeAdded(added, tree);
  if (findings.length === 0) {
    console.log(`Wiring check passed: ${added.length} item(s) ${since}, each reached by production code.`);
    process.exit(0);
  }
  console.error(findings.map(formatFinding).join('\n'));
  console.error(
    `\n${findings.length} of ${added.length} item(s) ${since} have no production caller. ` +
      'Done means the production path calls it: wire each, or delete it.',
  );
  process.exit(1);
}
