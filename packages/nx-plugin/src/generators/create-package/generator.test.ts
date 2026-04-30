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

  describe('ts-zig variant', () => {
    it('creates zig scaffold on top of ts-lib', async () => {
      const tree = setupTree();
      await generator(tree, { name: 'columine', variant: 'ts-zig' });

      // Zig files exist
      expect(tree.exists('packages/columine/build.zig')).toBe(true);
      expect(tree.exists('packages/columine/build.zig.zon')).toBe(true);
      expect(tree.exists('packages/columine/src/columine.zig')).toBe(true);

      // build.zig contains wasm step
      const buildZig = tree.read('packages/columine/build.zig', 'utf-8') ?? '';
      expect(buildZig).toContain('b.step("wasm", "Build WASM artifact")');
      expect(buildZig).toContain('.name = "columine"');
      expect(buildZig).toContain('b.path("src/columine.zig")');

      // build.zig.zon
      const buildZigZon = tree.read('packages/columine/build.zig.zon', 'utf-8') ?? '';
      expect(buildZigZon).toContain('.name = .columine');

      // src/<name>.zig
      const zigSrc = tree.read('packages/columine/src/columine.zig', 'utf-8') ?? '';
      expect(zigSrc).toContain('export fn add');

      // package.json has wasm export and build scripts
      const pkg = readJson(tree, 'packages/columine/package.json');
      expect(pkg.exports['./wasm']).toBe('./dist/columine.wasm');
      expect(pkg.exports['.'].development).toBeUndefined();
      expect(pkg.scripts['build:zig']).toBe('nx run columine:zig-wasm');
      expect(pkg.scripts['build:ts']).toBe('nx run columine:tsc-js');
      expect(pkg.scripts.build).toBe('bun run build:ts && bun run build:zig');
      expect(pkg.files).toEqual(['dist']);

      // tsconfig.lib.json has webworker lib
      const tsconfigLib = readJson(tree, 'packages/columine/tsconfig.lib.json');
      expect(tsconfigLib.compilerOptions.lib).toEqual(['es2022', 'webworker']);
      expect(tsconfigLib.compilerOptions.declaration).toBe(true);
      expect(tsconfigLib.compilerOptions.sourceMap).toBe(true);

      // tsconfig.test.json has webworker lib
      const tsconfigTest = readJson(tree, 'packages/columine/tsconfig.test.json');
      expect(tsconfigTest.compilerOptions.lib).toEqual(['es2022', 'webworker']);
    });

    it('combines zig and publication metadata when public', async () => {
      const tree = setupTree();
      await generator(tree, { name: 'zigpub', variant: 'ts-zig', public: true });

      const pkg = readJson(tree, 'packages/zigpub/package.json');

      // Has zig files
      expect(tree.exists('packages/zigpub/build.zig')).toBe(true);
      expect(tree.exists('packages/zigpub/build.zig.zon')).toBe(true);
      expect(tree.exists('packages/zigpub/src/zigpub.zig')).toBe(true);

      // Has publication metadata
      expect(pkg.private).toBeUndefined();
      expect(pkg.license).toBe('MIT');
      expect(pkg.publishConfig).toEqual({ access: 'public' });
      expect(pkg.repository).toEqual({
        type: 'git',
        url: 'git+https://github.com/smoothbricks/codebase.git',
        directory: 'packages/zigpub',
      });
      expect(pkg.nx?.tags).toEqual(['npm:public']);

      // Has wasm export
      expect(pkg.exports['./wasm']).toBe('./dist/zigpub.wasm');
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
