import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { BuildInputRecord } from '../../build-inputs.js';
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
  it('emits executable transformed validation and declarations from the same program', async () => {
    const root = await fixture(
      "#!/usr/bin/env node\nimport typia from 'typia';\nexport const validate = typia.createValidate<{ value: number }>();\n",
    );
    try {
      expect(
        await typescriptEmitExecutor(
          { cwd: root, tsConfig: 'tsconfig.lib.json', executableOutputs: ['dist/index.js'] },
          { root },
        ),
      ).toEqual({ success: true });
      expect((await stat(join(root, 'dist/index.js'))).mode & 0o111).toBe(0o111);
      expect((await stat(join(root, 'dist/index.d.ts'))).isFile()).toBe(true);
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
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports real type errors instead of claiming a successful build', async () => {
    const root = await fixture("export const amount: number = 'not a number';\n");
    try {
      expect(await typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.lib.json' }, { root })).toEqual({
        success: false,
      });
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
      expect(await runTypeScriptEmit(options, { root }, editDuringEmit)).toEqual({ success: true });
      expect(await readFile(join(lib, 'src/index.ts'), 'utf8')).toBe(edited);
      const record: BuildInputRecord = JSON.parse(await readFile(join(lib, 'dist/.build-inputs.json'), 'utf8'));
      expect(record.files['packages/lib/src/index.ts']).toBe(sha256(LIB['packages/lib/src/index.ts']));
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
  it('writes no record when the compiler fails', async () => {
    await inLibWorkspace(async (root) => {
      const lib = join(root, 'packages/lib');
      const options = { cwd: lib, tsConfig: 'tsconfig.lib.json' };
      expect(await runTypeScriptEmit(options, { root }, async () => false)).toEqual({ success: false });
      await expect(stat(join(lib, 'dist/.build-inputs.json'))).rejects.toMatchObject({ code: 'ENOENT' });
    });
  });
});
