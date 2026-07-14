import { describe, expect, it } from 'bun:test';
import { readJson, type Tree } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import generator from './generator.js';

function setupTree(rootName = '@smoothbricks/codebase'): Tree {
  const tree = createTreeWithEmptyWorkspace();
  tree.write(
    'package.json',
    JSON.stringify(
      {
        name: rootName,
        version: '0.0.0',
        private: true,
        repository: { type: 'git', url: 'git+https://github.com/smoothbricks/codebase.git' },
        workspaces: ['packages/*'],
      },
      null,
      2,
    ),
  );
  return tree;
}

describe('create-package generator', () => {
  describe('ts-lib variant', () => {
    it('creates basic package structure', async () => {
      const tree = setupTree();
      await generator(tree, { name: 'duration', variant: 'ts-lib' });

      // package.json
      const pkg = readJson(tree, 'packages/duration/package.json');
      expect(pkg.name).toBe('@smoothbricks/duration');
      expect(pkg.version).toBe('0.0.0');
      expect(pkg.private).toBe(true);
      expect(pkg.type).toBe('module');
      expect(pkg.sideEffects).toBe(false);
      expect(pkg.main).toBe('./dist/index.js');
      expect(pkg.module).toBe('./dist/index.js');
      expect(pkg.types).toBe('./dist/index.d.ts');
      expect(pkg.exports).toEqual({
        './package.json': './package.json',
        '.': {
          types: './dist/index.d.ts',
          development: './src/index.ts',
          import: './dist/index.js',
          default: './dist/index.js',
        },
      });
      expect(pkg.dependencies).toEqual({ tslib: '^2.8.1' });
      expect(pkg.devDependencies).toEqual({ '@smoothbricks/validation': 'workspace:*' });
      expect(pkg.scripts?.test).toBe('nx run duration:test --outputStyle=stream');
      expect(pkg.nx?.name).toBe('duration');
      expect(pkg.nx?.targets?.lint).toEqual({});
      expect(pkg.nx?.targets?.test?.executor).toBe('@smoothbricks/nx-plugin:bounded-exec');
      expect(pkg.nx?.targets?.test?.options?.command).toBe('bun test');

      // tsconfig.json (wrapper)
      const tsconfig = readJson(tree, 'packages/duration/tsconfig.json');
      expect(tsconfig.extends).toBe('../../tsconfig.base.json');
      expect(tsconfig.files).toEqual([]);
      expect(tsconfig.include).toEqual([]);
      expect(tsconfig.references).toEqual([{ path: './tsconfig.lib.json' }]);

      // tsconfig.lib.json
      const tsconfigLib = readJson(tree, 'packages/duration/tsconfig.lib.json');
      expect(tsconfigLib.extends).toBe('../../tsconfig.base.json');
      expect(tsconfigLib.compilerOptions.rootDir).toBe('src');
      expect(tsconfigLib.compilerOptions.outDir).toBe('dist');
      expect(tsconfigLib.compilerOptions.types).toEqual([]);
      expect(tsconfigLib.include).toEqual(['src/**/*.ts']);
      expect(tsconfigLib.exclude).toEqual(['src/**/*.test.ts', 'src/**/__tests__/**']);

      // tsconfig.test.json
      const tsconfigTest = readJson(tree, 'packages/duration/tsconfig.test.json');
      expect(tsconfigTest.extends).toBe('../../tsconfig.base.json');
      expect(tsconfigTest.compilerOptions.types).toEqual(['bun']);
      expect(tsconfigTest.compilerOptions.composite).toBe(false);
      expect(tsconfigTest.compilerOptions.noEmit).toBe(true);
      expect(tsconfigTest.include).toContain('src/**/*.test.ts');
      expect(tsconfigTest.references).toEqual([{ path: './tsconfig.lib.json' }]);

      // bunfig.toml
      const bunfig = tree.read('packages/duration/bunfig.toml', 'utf-8');
      expect(bunfig).toContain('[test]');
      expect(bunfig).toContain('timeout = 30000');
      expect(bunfig).toContain('preload = ["@smoothbricks/validation/bun/preload"]');

      // src/index.ts
      expect(tree.exists('packages/duration/src/index.ts')).toBe(true);
    });

    it('adds publication metadata when public', async () => {
      const tree = setupTree();
      await generator(tree, { name: 'mylib', variant: 'ts-lib', public: true });

      const pkg = readJson(tree, 'packages/mylib/package.json');
      expect(pkg.private).toBeUndefined();
      expect(pkg.license).toBe('MIT');
      expect(pkg.publishConfig).toEqual({ access: 'public' });
      expect(pkg.repository).toEqual({
        type: 'git',
        url: 'git+https://github.com/smoothbricks/codebase.git',
        directory: 'packages/mylib',
      });
      expect(pkg.nx?.tags).toEqual(['npm:public']);
    });

    it('derives scope from root package.json', async () => {
      const tree = setupTree('@other/monorepo');
      await generator(tree, { name: 'mylib', variant: 'ts-lib' });

      const pkg = readJson(tree, 'packages/mylib/package.json');
      expect(pkg.name).toBe('@other/mylib');
    });

    it('works without scope', async () => {
      const tree = setupTree('my-monorepo');
      await generator(tree, { name: 'mylib', variant: 'ts-lib' });

      const pkg = readJson(tree, 'packages/mylib/package.json');
      expect(pkg.name).toBe('mylib');
    });
  });

  it('rejects existing package', async () => {
    const tree = setupTree();
    tree.write('packages/existing/package.json', '{}');

    await expect(generator(tree, { name: 'existing', variant: 'ts-lib' })).rejects.toThrow(
      'Package already exists at packages/existing',
    );
  });
});
