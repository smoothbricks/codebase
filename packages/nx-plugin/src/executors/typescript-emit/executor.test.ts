import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { BuildInputRecord } from '@smoothbricks/build-inputs';
import typescriptEmitExecutor, { type CompilerInvocation, runTypeScriptEmit } from './executor.js';

async function fixture(source: string) {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-'));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'node_modules'));
  await symlink(
    dirname(createRequire(import.meta.url).resolve('typia/package.json')),
    join(root, 'node_modules/typia'),
    'dir',
  );
  await writeFile(join(root, 'package.json'), JSON.stringify({ type: 'module', dependencies: { typia: '*' } }));
  await writeFile(
    join(root, 'tsconfig.lib.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        rootDir: 'src',
        outDir: 'dist',
        declaration: true,
        declarationMap: true,
        emitDeclarationOnly: true,
        types: [],
        skipLibCheck: true,
      },
      include: ['src/**/*.ts'],
    }),
  );
  await writeFile(join(root, 'src/index.ts'), source);
  return root;
}

describe('@smoothbricks/nx-plugin TypeScript emit executor', () => {
  it('emits executable transformed validation and declarations from the same program, and nothing an earlier build left', async () => {
    const root = await fixture(
      "#!/usr/bin/env node\nimport typia from 'typia';\nexport const validate = typia.createValidate<{ value: number }>();\n",
    );
    try {
      // What an earlier build left: a bin since moved to another package, and the output of a since-deleted module.
      await mkdir(join(root, 'dist/bin'), { recursive: true });
      await writeFile(join(root, 'dist/bin/moved.js'), 'export {};\n');
      await writeFile(join(root, 'dist/deleted.js'), 'export {};\n');
      expect(
        await typescriptEmitExecutor(
          { cwd: root, tsConfig: 'tsconfig.lib.json', executableOutputs: ['dist/index.js'] },
          { root },
        ),
      ).toEqual({ success: true });
      expect((await stat(join(root, 'dist/index.js'))).mode & 0o111).toBe(0o111);
      expect((await readdir(join(root, 'dist'))).sort()).toEqual(['index.d.ts', 'index.d.ts.map', 'index.js']);
      expect((await readdir(root)).sort()).toEqual([
        'dist',
        'node_modules',
        'package.json',
        'src',
        'tsconfig.lib.json',
      ]);
      const child = Bun.spawn(
        [
          'bun',
          '-e',
          "import { validate } from './dist/index.js'; process.stdout.write(JSON.stringify([validate({value: 7}).success, validate({value: 'bad'}).success]))",
        ],
        { cwd: root, stdout: 'pipe', stderr: 'pipe' },
      );
      const [text, errors, exit] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
      ]);
      expect(exit, errors).toBe(0);
      expect(text).toBe('[true,false]');
      // A second emit over the same outputs: the compiler rewrites each one, so the removal keeps them all.
      expect(
        await typescriptEmitExecutor(
          { cwd: root, tsConfig: 'tsconfig.lib.json', executableOutputs: ['dist/index.js'] },
          { root },
        ),
      ).toEqual({ success: true });
      expect((await readdir(join(root, 'dist'))).sort()).toEqual(['index.d.ts', 'index.d.ts.map', 'index.js']);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports real type errors and removes nothing from the previous output instead of claiming a successful build', async () => {
    const root = await fixture("export const amount: number = 'not a number';\n");
    try {
      await mkdir(join(root, 'dist'));
      await writeFile(join(root, 'dist/previous.js'), 'export const amount = 1;\n');
      expect(await typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.lib.json' }, { root })).toEqual({
        success: false,
      });
      expect(await readdir(join(root, 'dist'))).toContain('previous.js');
      expect((await readdir(root)).sort()).toEqual([
        'dist',
        'node_modules',
        'package.json',
        'src',
        'tsconfig.lib.json',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

const GIT_IDENTITY = ['-c', 'user.name=fixture', '-c', 'user.email=fixture@example.test'];

/** A workspace's `packages/lib`: a config that emits its `src` less the tests, a source and a test. */
const LIB = {
  'packages/lib/tsconfig.lib.json': JSON.stringify({
    compilerOptions: { outDir: 'dist' },
    include: ['src'],
    exclude: ['src/**/*.test.ts'],
  }),
  'packages/lib/src/index.ts': 'export const index = 1;\n',
  'packages/lib/src/index.test.ts': "import { index } from './index.js';\n\nexport const checked = index;\n",
};

function git(root: string, ...args: string[]): void {
  execFileSync('git', ['-C', root, ...GIT_IDENTITY, ...args], { stdio: 'pipe' });
}

function sha256(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}

/** Runs `body` in a Git work tree of `LIB`, committed, in a new physical temp directory removed however it ends. */
async function inLibWorkspace(body: (root: string) => unknown): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-inputs-')));
  try {
    for (const [path, text] of Object.entries(LIB)) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), text);
    }
    git(root, 'init', '-q');
    git(root, 'add', '.');
    git(root, 'commit', '-qm', 'fixture');
    await body(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

describe('@smoothbricks/nx-plugin TypeScript emit build input record', () => {
  it('records the sources and config the emit reads, and the pathspecs that complete them', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      expect(await runTypeScriptEmit(options, { root }, async () => true)).toEqual({ success: true });
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(record.version).toBe(1);
      expect('producer' in record).toBe(false);
      expect(record.files).toEqual({
        'packages/lib/src/index.ts': sha256(LIB['packages/lib/src/index.ts']),
        'packages/lib/tsconfig.lib.json': sha256(LIB['packages/lib/tsconfig.lib.json']),
      });
      expect(record.complete).toContain(':(glob)packages/lib/src/**/*.ts');
      expect(record.complete).toContain(':(exclude,glob)packages/lib/src/**/*.test.ts');
    });
  });

  it('records the bytes the emit began with when a source is edited while it runs', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      const edited = 'export const index = 2;\n';
      const editDuringEmit = async () => {
        await writeFile(join(lib, 'src/index.ts'), edited);
        return true;
      };
      const producer = { project: 'lib', target: 'tsc-js', configuration: 'development' };
      expect(await runTypeScriptEmit(options, { root }, editDuringEmit, producer)).toEqual({ success: true });
      expect(await readFile(join(lib, 'src/index.ts'), 'utf8')).toBe(edited);
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(record.files['packages/lib/src/index.ts']).toBe(sha256(LIB['packages/lib/src/index.ts']));
      expect(record.producer).toEqual(producer);
    });
  });

  it('records the files that exist when a tracked source was deleted without staging it', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      await writeFile(join(lib, 'src/old.ts'), 'export const old = 1;\n');
      git(root, 'add', '.');
      git(root, 'commit', '-qm', 'old');
      // What a plain `rm` leaves: gone from the work tree, still in the index.
      await rm(join(lib, 'src/old.ts'));
      expect(await runTypeScriptEmit(options, { root }, async () => true)).toEqual({ success: true });
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(Object.keys(record.files)).toEqual(['packages/lib/src/index.ts', 'packages/lib/tsconfig.lib.json']);
    });
  });

  it('writes nothing beside the tsconfig while the compiler runs, and hands ttsc that tsconfig itself', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const before = (await readdir(lib)).sort();
      let during: string[] = [];
      let invocation: CompilerInvocation | undefined;
      const observe = async (call: CompilerInvocation) => {
        invocation = call;
        during = (await readdir(lib)).sort();
        return true;
      };
      expect(await runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, observe)).toEqual({
        success: true,
      });
      // Another project's ttsc transform proves this directory's listing while it compiles.
      expect(during).toEqual(before);
      expect(invocation?.args.slice(0, 3)).toEqual(['-p', join(lib, 'tsconfig.lib.json'), '--emit']);
    });
  });
  it('removes only the outputs the emit left untouched, and the directories that leaves empty', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      await mkdir(join(lib, 'dist/bin'), { recursive: true });
      await writeFile(join(lib, 'dist/bin/moved.js'), 'export {};\n');
      await writeFile(join(lib, 'dist/index.js'), 'export const index = 1;\n');
      await writeFile(join(lib, 'dist/deleted.js'), 'export {};\n');
      // The compiler rewrites what it still produces, unchanged bytes included, and writes what it newly produces.
      const emit = async () => {
        await writeFile(join(lib, 'dist/index.js'), 'export const index = 1;\n');
        await writeFile(join(lib, 'dist/added.js'), 'export {};\n');
        return true;
      };
      expect(await runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, emit)).toEqual({
        success: true,
      });
      expect((await readdir(join(lib, 'dist'))).sort()).toEqual(['.build-inputs.json', 'added.js', 'index.js']);
    });
  });

  it('refuses an outDir that holds a TypeScript source before the compiler runs', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      await mkdir(join(lib, 'dist'));
      await writeFile(join(lib, 'dist/kept.ts'), 'export const kept = 1;\n');
      let compiled = false;
      const emit = async () => {
        compiled = true;
        return true;
      };
      await expect(runTypeScriptEmit({ cwd: lib, tsConfig: 'tsconfig.lib.json' }, { root }, emit)).rejects.toThrow(
        `holds the TypeScript source ${join(lib, 'dist/kept.ts')}`,
      );
      expect(compiled).toBe(false);
      expect(await readdir(join(lib, 'dist'))).toEqual(['kept.ts']);
    });
  });

  it('writes no record when the compiler fails', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      expect(await runTypeScriptEmit(options, { root }, async () => false)).toEqual({ success: false });
      await expect(stat(join(lib, 'dist/.build-inputs.json'))).rejects.toMatchObject({ code: 'ENOENT' });
    });
  });
});
