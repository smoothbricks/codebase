import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readdirSync, readFileSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
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

/** One `scriptc build`: the emitted entry module and the executable it links, run from the project root. */
export interface ScriptcInvocation {
  readonly entry: string;
  readonly output: string;
  readonly cwd: string;
}

export type ScriptcRunner = (invocation: ScriptcInvocation) => Promise<boolean>;

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

export default function scriptcNativeExecutor(
  options: ScriptcNativeOptions,
  context: ScriptcNativeContext,
): Promise<ScriptcNativeResult> {
  return runScriptcNative(options, context, runScriptc);
}

export async function runScriptcNative(
  options: ScriptcNativeOptions,
  context: ScriptcNativeContext,
  runner: ScriptcRunner,
): Promise<ScriptcNativeResult> {
  const cwd = isAbsolute(options.cwd) ? options.cwd : join(context.root, options.cwd);
  const untransformed = untransformedTypiaCalls(join(cwd, options.javascriptRoot));
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
  const outputRoot = join(cwd, options.outputRoot);
  const binaries = join(outputRoot, 'bin', options.platform);
  await mkdir(binaries, { recursive: true });
  await writeFile(join(outputRoot, 'tsconfig.json'), `${JSON.stringify(EMITTED_JAVASCRIPT_TSCONFIG, null, 2)}\n`);
  for (const [name, entry] of Object.entries(options.entries)) {
    if (!(await runner({ entry: join(cwd, entry), output: join(binaries, name), cwd }))) return { success: false };
  }
  return { success: true };
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

async function runScriptc(invocation: ScriptcInvocation): Promise<boolean> {
  // The LLVM IR scriptc keeps beside an executable by default is no declared output.
  const child = spawn(
    scriptcCompiler(invocation.cwd),
    ['build', invocation.entry, '-o', invocation.output, '--strip', '--no-keep-llvm'],
    { cwd: invocation.cwd, stdio: 'inherit', windowsHide: true },
  );
  try {
    await once(child, 'exit');
    return child.exitCode === 0 && child.signalCode === null;
  } catch (error) {
    console.error(`scriptc: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}
