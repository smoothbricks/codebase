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
 * Every member access on a binding imported from `typia` in the emitted JavaScript under `root`. The transform
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
      if (
        (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) &&
        ts.isIdentifier(node.expression) &&
        bindings.has(node.expression.text)
      ) {
        // `typia.json.assertParse` is one call: name the whole chain at the binding.
        let chain: ts.Node = node;
        while (
          (ts.isPropertyAccessExpression(chain.parent) || ts.isElementAccessExpression(chain.parent)) &&
          chain.parent.expression === chain
        ) {
          chain = chain.parent;
        }
        const { line, character } = source.getLineAndCharacterOfPosition(node.getStart(source));
        calls.push({ file, line: line + 1, column: character + 1, text: chain.getText(source) });
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }
  return calls;
}

/** The default and namespace bindings of the module's `typia` imports: the names a typia call is made through. */
function typiaBindings(source: ts.SourceFile): Set<string> {
  const bindings = new Set<string>();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier)) continue;
    const specifier = statement.moduleSpecifier.text;
    if (specifier !== 'typia' && !specifier.startsWith('typia/')) continue;
    const clause = statement.importClause;
    if (clause?.name !== undefined) bindings.add(clause.name.text);
    if (clause?.namedBindings !== undefined && ts.isNamespaceImport(clause.namedBindings)) {
      bindings.add(clause.namedBindings.name.text);
    }
  }
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
