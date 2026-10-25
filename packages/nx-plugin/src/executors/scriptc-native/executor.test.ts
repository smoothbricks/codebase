import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  type EmitInvocation,
  runScriptcNative,
  type ScriptcInvocation,
  type ScriptcRunner,
  untransformedTypiaCalls,
} from './executor.js';

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function workspace(): { root: string; project: string } {
  const root = mkdtempSync(join(tmpdir(), 'scriptc-native-'));
  roots.push(root);
  const project = join(root, 'packages/tool');
  mkdirSync(project, { recursive: true });
  writeFileSync(join(project, 'tsconfig.lib.json'), '{"compilerOptions":{"rootDir":"src","outDir":"dist/ts"}}\n');
  return { root, project };
}

function write(directory: string, files: Record<string, string>): void {
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(directory, path)), { recursive: true });
    writeFileSync(join(directory, path), text);
  }
}

const options = {
  cwd: 'packages/tool',
  tsConfig: 'tsconfig.lib.json',
  triple: 'aarch64-linux-musl',
  outputDirectory: 'dist/bin/linux-arm64',
  entries: { 'smoo-nx-tool': 'bin/smoo-nx-tool.js' },
};

/** A runner whose emit writes `emitted` into the staging directory, recording what each step saw. */
function fake(emitted: Record<string, string>, builds: (count: number) => boolean) {
  const emits: Array<EmitInvocation & { overlay: unknown }> = [];
  const runs: Array<ScriptcInvocation & { staged: string[] }> = [];
  const runner: ScriptcRunner = {
    emit: async (invocation) => {
      emits.push({ ...invocation, overlay: JSON.parse(readFileSync(invocation.tsconfig, 'utf8')) });
      write(invocation.outDir, emitted);
      return true;
    },
    build: async (invocation) => {
      runs.push({ ...invocation, staged: readdirSync(dirname(dirname(invocation.entry))).sort() });
      const built = builds(runs.length);
      if (built) writeFileSync(invocation.output, '');
      return built;
    },
  };
  return { runner, emits, runs };
}

describe('scriptc-native', () => {
  it('names every call through a typia binding that the transform left behind, and no runtime member read', () => {
    const { project } = workspace();
    write(join(project, 'staged'), {
      'bin/smoo-nx-tool.js': "import { run } from '../tool.js';\nrun();\n",
      'tool.js': [
        "import typia, { TypeGuardError } from 'typia';",
        'export const isEntry = typia.createIs();',
        'export function run(raw) {',
        '  return typia.json.assertParse(raw) instanceof TypeGuardError;',
        '  if (raw instanceof typia.TypeGuardError) throw new typia.TypeGuardError(raw);',
        '}',
        '',
      ].join('\n'),
      'namespace.js': "import * as t from 'typia/lib/index.js';\nexport const check = t['assert'](input);\n",
      'named.js': [
        "import { createIs, TypeGuardError } from 'typia';",
        'export const isEntry = createIs();',
        'export const isError = (error) => error instanceof TypeGuardError;',
        '',
      ].join('\n'),
      'destructured.js': [
        "import typia from 'typia';",
        'const { assertParse } = typia.json;',
        'export const parse = (raw) => assertParse(raw);',
        '',
      ].join('\n'),
      // ttsc keeps the import after replacing every call; a kept import is not a call.
      'transformed.js': "import typia from 'typia';\nexport const is = (input) => typeof input === 'string';\n",
      // A name the module never imported from typia, and the text of a call, are not calls.
      'unrelated.js':
        "const typia = { createIs: () => true };\nexport const note = 'typia.createIs()' + typia.createIs();\n",
    });
    expect(
      untransformedTypiaCalls(join(project, 'staged')).map(
        (call) => `${call.file.slice(project.length + 1)}:${call.line}:${call.column} ${call.text}`,
      ),
    ).toEqual([
      'staged/destructured.js:3:31 assertParse',
      'staged/named.js:2:24 createIs',
      "staged/namespace.js:2:22 t['assert']",
      'staged/tool.js:2:24 typia.createIs',
      'staged/tool.js:4:10 typia.json.assertParse',
    ]);
  });

  it('compiles nothing while an untransformed typia call remains, and leaves no staging behind', async () => {
    const { root, project } = workspace();
    const { runner, runs } = fake(
      { 'bin/smoo-nx-tool.js': "import typia from 'typia';\nconsole.log(typia.is(process.argv[2]));\n" },
      () => true,
    );
    expect(await runScriptcNative(options, { root }, runner)).toEqual({ success: false });
    expect(runs).toEqual([]);
    expect(existsSync(join(project, 'dist/bin'))).toBe(false);
    expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    expect(readdirSync(project).sort()).toEqual(['.cache', 'tsconfig.lib.json']);
  });

  it("emits into its own staging, then builds each bin there for the target's triple, stopping at a failed build", async () => {
    const { root, project } = workspace();
    const three = { ...options, entries: { ...options.entries, other: 'bin/other.js', third: 'bin/third.js' } };
    const { runner, emits, runs } = fake(
      { 'bin/smoo-nx-tool.js': 'console.log(1);\n', 'bin/other.js': '' },
      (count) => count === 1,
    );
    expect(await runScriptcNative(three, { root }, runner)).toEqual({ success: false });
    const staging = emits[0]?.outDir ?? '';
    expect(dirname(staging)).toBe(join(project, '.cache/scriptc'));
    expect(emits).toEqual([
      {
        tsconfig: expect.stringMatching(/\/packages\/tool\/\.cache\/scriptc\/aarch64-linux-musl-[^/]+\.json$/),
        outDir: staging,
        cwd: project,
        // An overlay of the package's own emit config, writing only JavaScript, into staging.
        overlay: {
          extends: '../../tsconfig.lib.json',
          compilerOptions: {
            outDir: staging,
            composite: false,
            declaration: false,
            declarationMap: false,
            emitDeclarationOnly: false,
            incremental: false,
            sourceMap: false,
          },
        },
      },
    ]);
    // scriptc type-checks the staged JavaScript under the strict config and module format written beside it.
    const staged = ['bin', 'package.json', 'tsconfig.json'];
    expect(runs).toEqual([
      {
        entry: join(staging, 'bin/smoo-nx-tool.js'),
        output: join(project, 'dist/bin/linux-arm64/smoo-nx-tool'),
        cwd: project,
        triple: 'aarch64-linux-musl',
        staged,
      },
      {
        entry: join(staging, 'bin/other.js'),
        output: join(project, 'dist/bin/linux-arm64/other'),
        cwd: project,
        triple: 'aarch64-linux-musl',
        staged,
      },
    ]);
    // The overlay and the staging are gone, and so is the executable the first bin wrote: the run failed.
    expect(readdirSync(join(project, 'dist/bin/linux-arm64'))).toEqual([]);
    expect(readdirSync(join(project, '.cache/scriptc'))).toEqual([]);
    expect(readdirSync(project).sort()).toEqual(['.cache', 'dist', 'tsconfig.lib.json']);
  });
});
