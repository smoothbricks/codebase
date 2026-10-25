import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { readdirSync, readFileSync } from 'node:fs';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, relative } from 'node:path';

import ts from 'typescript';

import { isRecord } from '../../type-guards.js';
import type { ScriptcNativeOptions } from './schema.js';

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
}

export interface ScriptcRunner {
  emit(invocation: EmitInvocation): Promise<boolean>;
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
  return runScriptcNative(options, context, { emit: runTtsc, build: runScriptc });
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
    const binaries = join(cwd, options.outputDirectory);
    await mkdir(binaries, { recursive: true });
    const outputs = Object.keys(options.entries).map((name) => join(binaries, name));
    for (const [name, entry] of Object.entries(options.entries)) {
      const invocation = { entry: join(staging, entry), output: join(binaries, name), cwd, triple: options.triple };
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
 * imports, and, in source order, each name destructured from one of those (`const { createIs } = typia`).
 */
function typiaBindings(source: ts.SourceFile): Set<string> {
  const bindings = new Set<string>();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier)) continue;
    const specifier = statement.moduleSpecifier.text;
    if (specifier !== 'typia' && !specifier.startsWith('typia/')) continue;
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

async function runTtsc(invocation: EmitInvocation): Promise<boolean> {
  return exited(
    'ttsc',
    spawn('ttsc', ['-p', invocation.tsconfig, '--emit'], { cwd: invocation.cwd, stdio: 'inherit' }),
  );
}

async function runScriptc(invocation: ScriptcInvocation): Promise<boolean> {
  // Every triple, the host's own included, builds the same way: scriptc refuses a SCRIPTC_TARGET build unless zig is
  // its C compiler, since no other driver carries the other platforms' sysroots. zig is found on PATH (devenv.smoo.nix).
  // The LLVM IR scriptc keeps beside an executable by default is no declared output.
  const child = spawn(
    scriptcCompiler(invocation.cwd),
    ['build', invocation.entry, '-o', invocation.output, '--strip', '--no-keep-llvm'],
    {
      cwd: invocation.cwd,
      env: { ...process.env, SCRIPTC_TARGET: invocation.triple, SCRIPTC_CC: 'zigcc' },
      stdio: 'inherit',
      windowsHide: true,
    },
  );
  return exited('scriptc', child);
}

async function exited(command: string, child: ReturnType<typeof spawn>): Promise<boolean> {
  try {
    await once(child, 'exit');
    return child.exitCode === 0 && child.signalCode === null;
  } catch (error) {
    console.error(`${command}: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}
