import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { readdirSync, readFileSync } from 'node:fs';
import { copyFile, mkdir, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, relative } from 'node:path';

import ts from 'typescript';
import typia from 'typia';

import { isRecord } from '../../type-guards.js';
import { ffiPathProblem } from './ffi-paths.js';
import type { ScriptcNativeFfi, ScriptcNativeOptions } from './schema.js';

interface ScriptcNativeContext {
  root: string;
}

interface ScriptcNativeResult {
  success: boolean;
}

/** One ttsc emit of the package's `tsconfig` (an overlay of its `tsconfig.lib.json`) into `outDir`. */
export interface EmitInvocation {
  readonly tsconfig: string;
  readonly outDir: string;
  readonly cwd: string;
}

/** One `scriptc build`: the emitted entry module and the executable it links for `triple`, run from the project root. */
export interface ScriptcInvocation {
  readonly entry: string;
  readonly output: string;
  readonly cwd: string;
  readonly triple: string;
  /** The staged FFI manifest, when the package calls C. */
  readonly ffi?: string;
}

/** One C compile for `triple`: `sources` into the static archive `archive`, run from the project root. */
export interface ArchiveInvocation {
  readonly sources: readonly string[];
  readonly archive: string;
  readonly cwd: string;
  readonly triple: string;
}
export interface ScriptcRunner {
  emit(invocation: EmitInvocation): Promise<boolean>;
  archive(invocation: ArchiveInvocation): Promise<boolean>;
  build(invocation: ScriptcInvocation): Promise<boolean>;
}

/** A typia call the transform left in emitted JavaScript: it throws at run time instead of validating. */
export interface UntransformedTypiaCall {
  readonly file: string;
  readonly line: number;
  readonly column: number;
  readonly text: string;
}

/**
 * The compiler options scriptc type-checks emitted JavaScript under. scriptc reads the nearest `tsconfig.json`; the
 * workspace's (`noImplicitReturns`, `noUnusedLocals`) refuses JavaScript that carries no types, and an empty one is
 * refused for lacking `strictNullChecks`.
 */
export const EMITTED_JAVASCRIPT_TSCONFIG = {
  compilerOptions: {
    strict: true,
    target: 'es2022',
    module: 'nodenext',
    moduleResolution: 'nodenext',
    types: ['bun', 'node'],
    skipLibCheck: true,
  },
} as const;

/** Where a run stages its emit: inside the project, so the emitted JavaScript resolves the project's dependencies. */
const STAGING_DIRECTORY = '.cache/scriptc';

export default function scriptcNativeExecutor(
  options: ScriptcNativeOptions,
  context: ScriptcNativeContext,
): Promise<ScriptcNativeResult> {
  return runScriptcNative(options, context, { emit: runTtsc, archive: runZigArchive, build: runScriptc });
}

/**
 * Every platform's run emits on its own: the four targets of a package run side by side on one host and each family is
 * collected from its own publish leg, so none of them reads or writes another's JavaScript, and the emit is no output of
 * any of them. The overlay config and the emit live under the gitignored staging root, so a killed run leaves nothing a
 * project input hashes.
 */
export async function runScriptcNative(
  options: ScriptcNativeOptions,
  context: ScriptcNativeContext,
  runner: ScriptcRunner,
): Promise<ScriptcNativeResult> {
  const problem = options.ffi === undefined ? undefined : ffiPathProblem(options.ffi);
  if (problem !== undefined) throw new Error(`scriptc-native: ffi ${problem}`);
  const cwd = isAbsolute(options.cwd) ? options.cwd : join(context.root, options.cwd);
  const run = `${options.triple}-${process.pid}-${randomUUID()}`;
  const staging = join(cwd, STAGING_DIRECTORY, run);
  const overlay = join(cwd, STAGING_DIRECTORY, `${run}.json`);
  const tsconfig = {
    extends: relative(dirname(overlay), join(cwd, options.tsConfig)),
    compilerOptions: {
      outDir: staging,
      composite: false,
      declaration: false,
      declarationMap: false,
      emitDeclarationOnly: false,
      incremental: false,
      sourceMap: false,
    },
  };
  await mkdir(staging, { recursive: true });
  await writeFile(overlay, `${JSON.stringify(tsconfig, null, 2)}\n`, { flag: 'wx' });
  try {
    if (!(await runner.emit({ tsconfig: overlay, outDir: staging, cwd }))) return { success: false };
    const untransformed = untransformedTypiaCalls(staging);
    for (const call of untransformed) {
      console.error(
        'scriptc-native: ' +
          relative(context.root, call.file) +
          ':' +
          call.line +
          ':' +
          call.column +
          ': untransformed ' +
          call.text +
          ': ttsc emitted this typia call as written, which it does when the emitting package does not list typia itself',
      );
    }
    if (untransformed.length > 0) return { success: false };
    // The nearest configs of the emitted JavaScript: scriptc's type-check, and the module format the sources were written in.
    await writeFile(join(staging, 'tsconfig.json'), `${JSON.stringify(EMITTED_JAVASCRIPT_TSCONFIG, null, 2)}\n`);
    await writeFile(join(staging, 'package.json'), `${JSON.stringify({ type: 'module' })}\n`);
    let ffi: string | undefined;
    if (options.ffi !== undefined) {
      ffi = await stageFfi(options.ffi, { cwd, staging, triple: options.triple }, runner);
      if (ffi === undefined) return { success: false };
    }
    const binaries = join(cwd, options.outputDirectory);
    await mkdir(binaries, { recursive: true });
    const outputs = Object.keys(options.entries).map((name) => join(binaries, name));
    for (const [name, entry] of Object.entries(options.entries)) {
      const invocation = {
        entry: join(staging, entry),
        output: join(binaries, name),
        cwd,
        triple: options.triple,
        ...(ffi === undefined ? {} : { ffi }),
      };
      if (!(await runner.build(invocation))) {
        // A failed run defines none of its outputs: no executable an earlier bin of this run wrote stays to be packed.
        await Promise.all(outputs.map((output) => rm(output, { force: true })));
        return { success: false };
      }
    }
    return { success: true };
  } finally {
    await rm(overlay, { force: true });
    await rm(staging, { recursive: true, force: true });
  }
}

/**
 * Every call through a binding of a `typia` import in the emitted JavaScript under `root`. The transform
 * replaces each such call with the code it generates; one that survives means ttsc never loaded the typia plugin.
 */
export function untransformedTypiaCalls(root: string): UntransformedTypiaCall[] {
  const calls: UntransformedTypiaCall[] = [];
  for (const file of javascriptFiles(root)) {
    const source = ts.createSourceFile(
      file,
      readFileSync(file, 'utf8'),
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.JS,
    );
    const bindings = typiaBindings(source);
    if (bindings.size === 0) continue;
    const visit = (node: ts.Node): void => {
      if (ts.isCallExpression(node)) {
        const root = chainRoot(node.expression);
        if (root !== undefined && bindings.has(root.text)) {
          const { line, character } = source.getLineAndCharacterOfPosition(node.expression.getStart(source));
          calls.push({ file, line: line + 1, column: character + 1, text: node.expression.getText(source) });
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }
  return calls;
}

/** The identifier a member chain starts at: `typia` for `typia.json.assertParse`, `t` for `t['is']`. */
function chainRoot(expression: ts.Expression): ts.Identifier | undefined {
  let node = expression;
  while (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) node = node.expression;
  return ts.isIdentifier(node) ? node : undefined;
}

/**
 * Every name a typia call can be made through: the default, namespace and named bindings of the module's `typia`
 * imports, and, in source order, each name destructured from one of those (`const { createIs } = typia`). The helpers
 * under `typia/lib/internal/` are what the transform emits calls to, so they are never such an import.
 */
function typiaBindings(source: ts.SourceFile): Set<string> {
  const bindings = new Set<string>();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier)) continue;
    const specifier = statement.moduleSpecifier.text;
    if (specifier !== 'typia' && !specifier.startsWith('typia/')) continue;
    if (specifier.startsWith('typia/lib/internal/')) continue;
    const clause = statement.importClause;
    if (clause === undefined || clause.isTypeOnly) continue;
    if (clause.name !== undefined) bindings.add(clause.name.text);
    const named = clause.namedBindings;
    if (named !== undefined && ts.isNamespaceImport(named)) bindings.add(named.name.text);
    if (named !== undefined && ts.isNamedImports(named)) {
      for (const element of named.elements) if (!element.isTypeOnly) bindings.add(element.name.text);
    }
  }
  if (bindings.size === 0) return bindings;
  const destructure = (node: ts.Node): void => {
    if (ts.isVariableDeclaration(node) && ts.isObjectBindingPattern(node.name) && node.initializer !== undefined) {
      const root = chainRoot(node.initializer);
      if (root !== undefined && bindings.has(root.text)) {
        for (const element of node.name.elements) if (ts.isIdentifier(element.name)) bindings.add(element.name.text);
      }
    }
    ts.forEachChild(node, destructure);
  };
  destructure(source);
  return bindings;
}

function javascriptFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true, recursive: true })
    .filter((entry) => entry.isFile() && /\.[cm]?js$/.test(entry.name))
    .map((entry) => join(entry.parentPath, entry.name))
    .sort();
}

/** The executable of the project's own `scriptc` dependency, the declaration that opts a package into this target. */
function scriptcCompiler(cwd: string): string {
  const manifestPath = createRequire(join(cwd, 'package.json')).resolve('scriptc/package.json');
  const manifest: unknown = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const bin = isRecord(manifest) && isRecord(manifest.bin) ? manifest.bin.scriptc : undefined;
  if (typeof bin !== 'string') throw new Error(`${manifestPath}: bin.scriptc must name the scriptc executable`);
  return join(dirname(manifestPath), bin);
}

/**
 * What the run needs of a package's FFI manifest: its libraries, which must be none. Every other field passes
 * through.
 */
interface FfiManifest {
  readonly libraries: readonly string[];
}

const validateFfiManifest = typia.createValidate<FfiManifest>();

function readFfiManifest(path: string): FfiManifest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    throw new Error(`${path}: ${error instanceof Error ? error.message : String(error)}`);
  }
  const checked = validateFfiManifest(parsed);
  if (!checked.success) {
    const found = checked.errors.map((error) => `${error.path} expected ${error.expected}`).join('; ');
    throw new Error(`${path}: an FFI manifest lists its libraries (${found})`);
  }
  if (checked.data.libraries.length > 0) {
    throw new Error(
      `${path}: lists libraries (${checked.data.libraries.join(', ')}); a prebuilt archive holds one triple's code and ` +
        'every platform builds this one manifest, so list the C in "sources" and each run links the archive it compiles',
    );
  }
  return checked.data;
}

/**
 * The package's FFI made ready beside the staged JavaScript, and the manifest scriptc is handed: each declaration
 * copied to where its emitted module looks for it, the C sources compiled for the triple into one archive, and a copy
 * of the manifest that links that archive. Every platform builds one manifest, so it names no prebuilt library (an
 * archive holds one triple's code). Undefined when the C does not compile.
 */
async function stageFfi(
  ffi: ScriptcNativeFfi,
  where: { readonly cwd: string; readonly staging: string; readonly triple: string },
  runner: ScriptcRunner,
): Promise<string | undefined> {
  const manifestPath = join(where.cwd, ffi.manifest);
  const manifest = readFfiManifest(manifestPath);
  for (const [declaration, destination] of Object.entries(ffi.declarations)) {
    await mkdir(dirname(join(where.staging, destination)), { recursive: true });
    await copyFile(join(where.cwd, declaration), join(where.staging, destination));
  }
  const libraries: string[] = [];
  if (ffi.sources.length > 0) {
    const archive = join(where.staging, 'native', 'ffi.a');
    await mkdir(dirname(archive), { recursive: true });
    const sources = ffi.sources.map((source) => join(where.cwd, source));
    if (!(await runner.archive({ sources, archive, cwd: where.cwd, triple: where.triple }))) return undefined;
    libraries.push(archive);
  }
  const staged = join(where.staging, 'ffi.json');
  await writeFile(staged, `${JSON.stringify({ ...manifest, libraries }, null, 2)}\n`);
  return staged;
}

async function runTtsc(invocation: EmitInvocation): Promise<boolean> {
  return exited(
    'ttsc',
    spawn('ttsc', ['-p', invocation.tsconfig, '--emit'], { cwd: invocation.cwd, stdio: 'inherit' }),
  );
}

/** The arguments of one `scriptc build`: the staged FFI manifest, when the package has one, rides as `--ffi`. */
export function scriptcBuildArguments(invocation: ScriptcInvocation): string[] {
  return [
    'build',
    invocation.entry,
    '-o',
    invocation.output,
    '--strip',
    '--no-keep-llvm',
    ...(invocation.ffi === undefined ? [] : ['--ffi', invocation.ffi]),
  ];
}

/**
 * The zig commands that archive a package's C: one compile per source for the triple, then one archive of the objects.
 * zig cc without an optimization flag compiles in Debug mode with UBSan, whose handlers only a zig link carries;
 * scriptc links the archive itself, so the C is compiled `-O2` (scriptc's own release posture) with UBSan off.
 */
export function zigArchiveCommands(invocation: ArchiveInvocation): string[][] {
  const compiled = invocation.sources.map((source, index) => ({ source, object: `${invocation.archive}.${index}.o` }));
  return [
    ...compiled.map(({ source, object }) => [
      'cc',
      '-target',
      invocation.triple,
      '-O2',
      '-fno-sanitize=undefined',
      '-c',
      source,
      '-o',
      object,
    ]),
    ['ar', 'rcs', invocation.archive, ...compiled.map(({ object }) => object)],
  ];
}

async function runScriptc(invocation: ScriptcInvocation): Promise<boolean> {
  // Every triple, the host's own included, builds the same way: scriptc refuses a SCRIPTC_TARGET build unless zig is
  // its C compiler, since no other driver carries the other platforms' sysroots. zig is found on PATH (devenv.smoo.nix).
  // The LLVM IR scriptc keeps beside an executable by default is no declared output.
  const child = spawn(scriptcCompiler(invocation.cwd), scriptcBuildArguments(invocation), {
    cwd: invocation.cwd,
    env: { ...process.env, SCRIPTC_TARGET: invocation.triple, SCRIPTC_CC: 'zigcc' },
    stdio: 'inherit',
    windowsHide: true,
  });
  return exited('scriptc', child);
}

/**
 * The C sources compiled and archived by the zig that is scriptc's own C compiler (devenv.smoo.nix), so an archive and
 * the executable that links it agree on the target's libc and ABI.
 */
async function runZigArchive(invocation: ArchiveInvocation): Promise<boolean> {
  for (const args of zigArchiveCommands(invocation)) {
    const child = spawn('zig', args, { cwd: invocation.cwd, stdio: 'inherit', windowsHide: true });
    if (!(await exited(`zig ${args[0]}`, child))) return false;
  }
  return true;
}
async function exited(command: string, child: ReturnType<typeof spawn>): Promise<boolean> {
  try {
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a scriptc build or a zig compile lasts as long as its work; the operator's interrupt or the enclosing runner's bound ends a hung one
    await once(child, 'exit');
    return child.exitCode === 0 && child.signalCode === null;
  } catch (error) {
    console.error(`${command}: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}
