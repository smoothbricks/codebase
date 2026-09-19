import { describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { loadPrecompiledTestSource, PRECOMPILED_TEST_ENV } from '@smoothbricks/validation/test-build';

import typescriptEmitExecutor from './executor.js';

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'smoo-typescript-emit-'));
  await mkdir(join(root, 'src'));
  await writeFile(join(root, 'package.json'), JSON.stringify({ type: 'module' }));
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
  await writeFile(
    join(root, 'src/index.ts'),
    '#!/usr/bin/env node\nexport function identity<T>(value: T): T { return value; }\n',
  );
  return root;
}

describe('@smoothbricks/nx-plugin TypeScript emit executor', () => {
  it('emits executable JavaScript and usable generic declarations', async () => {
    const root = await fixture();
    try {
      expect(
        await typescriptEmitExecutor(
          { cwd: root, tsConfig: 'tsconfig.lib.json', executableOutputs: ['dist/index.js'] },
          { root },
        ),
      ).toEqual({ success: true });
      expect((await stat(join(root, 'dist/index.js'))).mode & 0o111).toBe(0o111);
      expect(await readFile(join(root, 'dist/index.d.ts'), 'utf8')).toContain('identity<T>(value: T): T');
      const child = Bun.spawn(
        ['bun', '-e', "import { identity } from './dist/index.js'; process.stdout.write(String(identity(42)))"],
        { cwd: root, stdout: 'pipe', stderr: 'pipe' },
      );
      const text = await new Response(child.stdout).text();
      expect(await child.exited).toBe(0);
      expect(Number(text)).toBe(42);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('builds the no-emit test program outside published outputs without modifying its config', async () => {
    const root = await fixture();
    const testConfig = JSON.stringify({
      extends: './tsconfig.lib.json',
      compilerOptions: {
        noEmit: true,
        composite: false,
        declaration: false,
        declarationMap: false,
        emitDeclarationOnly: false,
      },
    });
    await writeFile(join(root, 'tsconfig.test.json'), testConfig);
    const previousMode = process.env[PRECOMPILED_TEST_ENV];
    process.env[PRECOMPILED_TEST_ENV] = '1';
    try {
      expect(
        await typescriptEmitExecutor({ cwd: root, tsConfig: 'tsconfig.test.json', kind: 'tests' }, { root }),
      ).toEqual({ success: true });
      expect(existsSync(join(root, '.cache/test-build/src/index.js'))).toBe(true);
      expect(existsSync(join(root, '.cache/test-build/src/index.d.ts'))).toBe(false);
      expect(existsSync(join(root, 'dist'))).toBe(false);
      expect(await readFile(join(root, 'tsconfig.test.json'), 'utf8')).toBe(testConfig);
      const loaded = await loadPrecompiledTestSource(join(root, 'src/index.ts'));
      expect(loaded?.map.sources).toEqual([pathToFileURL(join(root, 'src/index.ts')).href]);
      await rm(join(root, '.cache/test-build'), { recursive: true });
      await expect(loadPrecompiledTestSource(join(root, 'src/index.ts'))).rejects.toThrow();
    } finally {
      if (previousMode === undefined) delete process.env[PRECOMPILED_TEST_ENV];
      else process.env[PRECOMPILED_TEST_ENV] = previousMode;
      await rm(root, { recursive: true, force: true });
    }
  });
});
