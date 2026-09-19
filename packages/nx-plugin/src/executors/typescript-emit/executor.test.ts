import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import typescriptEmitExecutor from './executor.js';

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
