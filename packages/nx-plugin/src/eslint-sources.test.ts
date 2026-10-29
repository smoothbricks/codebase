import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { eslintSources, expandEslintSources, freshEslintSources } from './eslint-sources.js';

const PROJECT = 'packages/example';
const DECLARATION = '{projectRoot}/tsconfig.test.json';

let root = '';
let external = '';
let inheritedData: string | undefined;

// One Nx state per checkout: the fixture is its own workspace, so the runner's NX_WORKSPACE_DATA_DIRECTORY must not
// name the archive its file index writes.
beforeEach(async () => {
  root = await realpath(await mkdtemp(join(tmpdir(), 'eslint-sources-')));
  external = await realpath(await mkdtemp(join(tmpdir(), 'eslint-sources-outside-')));
  inheritedData = process.env.NX_WORKSPACE_DATA_DIRECTORY;
  process.env.NX_WORKSPACE_DATA_DIRECTORY = join(root, '.nx', 'workspace-data');
  await mkdir(process.env.NX_WORKSPACE_DATA_DIRECTORY, { recursive: true });
});

afterEach(async () => {
  if (inheritedData === undefined) delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
  else process.env.NX_WORKSPACE_DATA_DIRECTORY = inheritedData;
  await rm(root, { recursive: true, force: true });
  await rm(external, { recursive: true, force: true });
});

async function write(path: string, text = 'export const value = 1;\n'): Promise<void> {
  await mkdir(dirname(join(root, path)), { recursive: true });
  await writeFile(join(root, path), text);
}

describe('expandEslintSources', () => {
  it.each<[string, string, readonly string[], string]>([
    ['bare words for paths the shell leaves alone', 'eslint {eslintSources}', ['a/b.ts', 'c.ts'], 'eslint a/b.ts c.ts'],
    [
      'quotes a path the shell would split or expand',
      'eslint {eslintSources}',
      ["dir/it's here.ts"],
      `eslint 'dir/it'"'"'s here.ts'`,
    ],
    ['every occurrence', 'a {eslintSources} b {eslintSources}', ['x.ts'], 'a x.ts b x.ts'],
    ['a replacement pattern inside a path stays literal', '{eslintSources}', ['a$&b.ts'], "'a$&b.ts'"],
    ['a command that names no placeholder', 'biome check .', ['x.ts'], 'biome check .'],
  ])('%s', (_name, command, files, expanded) => {
    expect(expandEslintSources(command, files)).toBe(expanded);
  });
});

interface Scenario {
  /** The project whose sources are read; `packages/example` unless named. */
  readonly project?: string;
  /** Files on disk, workspace-relative; `tsconfig.test.json` among them is the declaration under test. */
  readonly disk: Readonly<Record<string, string>>;
  /** The JavaScript and TypeScript modules the index holds. */
  readonly indexed: readonly string[];
  readonly files: readonly string[];
  readonly inputs: readonly string[];
}

const KINDS = '*.{js,jsx,cjs,mjs,ts,tsx,cts,mts}';

describe('eslintSources', () => {
  it.each<[string, Scenario]>([
    [
      'the src files of the project, never a directory a build writes into',
      {
        disk: {},
        indexed: [
          `${PROJECT}/src/a.ts`,
          `${PROJECT}/src/lib/b.ts`,
          `${PROJECT}/src/dist/c.ts`,
          `${PROJECT}/src/.cache/d.ts`,
          `${PROJECT}/src/node_modules/e.ts`,
          `${PROJECT}/lib/f.ts`,
          'packages/other/src/g.ts',
        ],
        files: [`${PROJECT}/src/a.ts`, `${PROJECT}/src/lib/b.ts`],
        inputs: [],
      },
    ],
    [
      'a directory the declaration walks, keyed as one glob; the modules Nx does not index are not linted',
      {
        disk: {
          [`${PROJECT}/tsconfig.test.json`]: '{"include":["tests/**/*.ts"],"exclude":["tests/excluded.ts"]}',
          [`${PROJECT}/tests/check.test.ts`]: '',
          [`${PROJECT}/tests/deep/helper.ts`]: '',
          [`${PROJECT}/tests/excluded.ts`]: '',
          [`${PROJECT}/tests/generated.ts`]: '',
        },
        indexed: [`${PROJECT}/tests/check.test.ts`, `${PROJECT}/tests/deep/helper.ts`, `${PROJECT}/tests/excluded.ts`],
        files: [`${PROJECT}/tests/check.test.ts`, `${PROJECT}/tests/deep/helper.ts`],
        inputs: [DECLARATION, `{workspaceRoot}/${PROJECT}/tests/**/${KINDS}`],
      },
    ],
    [
      'a directory walked one level is keyed without descending',
      {
        disk: {
          [`${PROJECT}/tsconfig.test.json`]: '{"include":["tests/*.ts"]}',
          [`${PROJECT}/tests/a.test.ts`]: '',
          [`${PROJECT}/tests/deep/b.ts`]: '',
        },
        indexed: [`${PROJECT}/tests/a.test.ts`, `${PROJECT}/tests/deep/b.ts`],
        files: [`${PROJECT}/tests/a.test.ts`],
        inputs: [DECLARATION, `{workspaceRoot}/${PROJECT}/tests/${KINDS}`],
      },
    ],
    [
      'a file the declaration names outside the project is keyed by name',
      {
        disk: {
          [`${PROJECT}/tsconfig.test.json`]: '{"files":["../contract/schema.d.ts"]}',
          'packages/contract/schema.d.ts': '',
        },
        indexed: ['packages/contract/schema.d.ts'],
        files: ['packages/contract/schema.d.ts'],
        inputs: [DECLARATION, '{workspaceRoot}/packages/contract/schema.d.ts'],
      },
    ],
    [
      'a declaration that selects no file still keys the directory a file will be added to',
      {
        disk: { [`${PROJECT}/tsconfig.test.json`]: '{"include":["later/**/*.ts"]}' },
        indexed: [],
        files: [],
        inputs: [DECLARATION, `{workspaceRoot}/${PROJECT}/later/**/${KINDS}`],
      },
    ],
    [
      'a directory outside the workspace, and one a build writes into, are never keyed',
      {
        disk: {
          [`${PROJECT}/tsconfig.test.json`]: '{"include":["dist/**/*.ts","../../../no-such-sibling/**/*.ts"]}',
          [`${PROJECT}/dist/built.ts`]: '',
        },
        indexed: [`${PROJECT}/dist/built.ts`],
        files: [],
        inputs: [DECLARATION],
      },
    ],
    [
      'the root project reads src beside the workspace root, and keys a directory from the root',
      {
        project: '.',
        disk: { 'tsconfig.test.json': '{"include":["tests/**/*.ts"]}', 'tests/b.test.ts': '' },
        indexed: ['src/a.ts', 'tests/b.test.ts', 'packages/other/src/c.ts'],
        files: ['src/a.ts', 'tests/b.test.ts'],
        inputs: [DECLARATION, `{workspaceRoot}/tests/**/${KINDS}`],
      },
    ],
  ])('%s', async (_name, scenario) => {
    for (const [path, text] of Object.entries(scenario.disk)) await write(path, text);
    expect(eslintSources(root, scenario.project ?? PROJECT, new Set(scenario.indexed))).toEqual({
      files: scenario.files,
      inputs: scenario.inputs,
    });
  });

  it('refuses a declaration TypeScript refuses, naming its code', async () => {
    await write(`${PROJECT}/tsconfig.test.json`, '{"compilerOptions":{"target":"not-a-target"}}');
    expect(() => eslintSources(root, PROJECT, new Set())).toThrow(
      /tsconfig\.test\.json: TS6046: "Argument for '--target' option must be/,
    );
  });

  it('refuses a program file outside the workspace, which no hash can name', async () => {
    await write(`${PROJECT}/tsconfig.test.json`, JSON.stringify({ files: [join(external, 'outside.ts')] }));
    await writeFile(join(external, 'outside.ts'), 'export const outside = 1;\n');
    expect(() => eslintSources(root, PROJECT, new Set())).toThrow(/outside\.ts is outside the workspace/);
  });
});

describe('freshEslintSources', () => {
  it('lists the files the filesystem holds now, not the table an earlier read left behind', async () => {
    await write('.gitignore', `${PROJECT}/src/generated/\n`);
    await write(`${PROJECT}/src/a.ts`);
    await write(`${PROJECT}/src/b.ts`);
    await write(`${PROJECT}/src/generated/schemas.gen.ts`);
    expect(freshEslintSources(root, PROJECT).files).toEqual([`${PROJECT}/src/a.ts`, `${PROJECT}/src/b.ts`]);

    // What a daemon's graph never learns when its watcher misses the events: a file deleted and a file added.
    await rm(join(root, PROJECT, 'src/b.ts'));
    await write(`${PROJECT}/src/c.ts`);
    expect(freshEslintSources(root, PROJECT).files).toEqual([`${PROJECT}/src/a.ts`, `${PROJECT}/src/c.ts`]);
  });
});
