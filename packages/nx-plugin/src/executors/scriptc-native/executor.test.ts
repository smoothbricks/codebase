import { afterEach, describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  EMITTED_JAVASCRIPT_TSCONFIG,
  runScriptcNative,
  type ScriptcInvocation,
  untransformedTypiaCalls,
} from './executor.js';

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

/** A workspace with one package whose emitted JavaScript is `files` under `dist/ts`. */
function workspace(files: Record<string, string>): { root: string; project: string } {
  const root = mkdtempSync(join(tmpdir(), 'scriptc-native-'));
  roots.push(root);
  const project = join(root, 'packages/tool');
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(dirname(join(project, 'dist/ts', path)), { recursive: true });
    writeFileSync(join(project, 'dist/ts', path), text);
  }
  return { root, project };
}

const options = {
  cwd: 'packages/tool',
  platform: 'darwin-arm64',
  javascriptRoot: 'dist/ts',
  outputRoot: 'dist',
  entries: { 'smoo-nx-tool': 'dist/ts/bin/smoo-nx-tool.js' },
};

describe('scriptc-native', () => {
  it('names every member access on a typia import binding that the transform left behind', () => {
    const { project } = workspace({
      'bin/smoo-nx-tool.js': "import { run } from '../tool.js';\nrun();\n",
      'tool.js': [
        "import typia, { TypeGuardError } from 'typia';",
        'export const isEntry = typia.createIs();',
        'export function run(raw) {',
        '  return typia.json.assertParse(raw) instanceof TypeGuardError;',
        '}',
        '',
      ].join('\n'),
      'namespace.js': "import * as t from 'typia/lib/index.js';\nexport const check = t['assert'];\n",
      // ttsc keeps the import after replacing every call; a kept import is not a call.
      'transformed.js': "import typia from 'typia';\nexport const is = (input) => typeof input === 'string';\n",
      // A name the module never imported from typia, and the text of a call, are not calls.
      'unrelated.js':
        "const typia = { createIs: () => true };\nexport const note = 'typia.createIs()' + typia.createIs();\n",
    });
    expect(
      untransformedTypiaCalls(join(project, 'dist/ts')).map(
        (call) => `${call.file.slice(project.length + 1)}:${call.line}:${call.column} ${call.text}`,
      ),
    ).toEqual([
      "dist/ts/namespace.js:2:22 t['assert']",
      'dist/ts/tool.js:2:24 typia.createIs',
      'dist/ts/tool.js:4:10 typia.json.assertParse',
    ]);
  });

  it('compiles nothing while an untransformed typia call remains', async () => {
    const { root, project } = workspace({
      'bin/smoo-nx-tool.js': "import typia from 'typia';\nconsole.log(typia.is(process.argv[2]));\n",
    });
    const runs: ScriptcInvocation[] = [];
    const result = await runScriptcNative(options, { root }, async (invocation) => {
      runs.push(invocation);
      return true;
    });
    expect(result).toEqual({ success: false });
    expect(runs).toEqual([]);
    expect(() => readFileSync(join(project, 'dist/tsconfig.json'))).toThrow();
  });

  it('builds each bin into dist/bin/<platform> under the emitted JavaScript config, stopping at a failed build', async () => {
    const { root, project } = workspace({ 'bin/smoo-nx-tool.js': 'console.log(1);\n', 'bin/other.js': '' });
    const runs: ScriptcInvocation[] = [];
    const two = { ...options, entries: { ...options.entries, other: 'dist/ts/bin/other.js' } };
    const result = await runScriptcNative(two, { root }, async (invocation) => {
      runs.push(invocation);
      return runs.length === 1;
    });
    expect(result).toEqual({ success: false });
    expect(runs).toEqual([
      {
        entry: join(project, 'dist/ts/bin/smoo-nx-tool.js'),
        output: join(project, 'dist/bin/darwin-arm64/smoo-nx-tool'),
        cwd: project,
      },
      {
        entry: join(project, 'dist/ts/bin/other.js'),
        output: join(project, 'dist/bin/darwin-arm64/other'),
        cwd: project,
      },
    ]);
    expect(JSON.parse(readFileSync(join(project, 'dist/tsconfig.json'), 'utf8'))).toEqual(EMITTED_JAVASCRIPT_TSCONFIG);
  });
});
