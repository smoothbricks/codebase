import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { generateManagedFiles } from '@smoothbricks/nx-plugin/managed-files/generator';
import { readJson, writeJson } from 'nx/src/devkit-exports.js';
import { FsTree, flushChanges } from 'nx/src/generators/tree.js';
import { finishManagedFiles } from './managed-fs.js';

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function workspace(): FsTree {
  const root = mkdtempSync(join(tmpdir(), 'smoo-managed-tsconfig-'));
  roots.push(root);
  const tree = new FsTree(root, false);
  writeJson(tree, 'package.json', { name: '@fixture/workspace', workspaces: ['packages/*'] });
  writeJson(tree, 'nx.json', {});
  // Library programs are composite by inheritance, as in a real workspace.
  writeJson(tree, 'tsconfig.base.json', { compilerOptions: { composite: true } });
  writeJson(tree, 'packages/app/package.json', {
    name: '@fixture/app',
    nx: { name: 'app', targets: { test: { command: 'bun test' } } },
  });
  writeJson(tree, 'packages/app/tsconfig.lib.json', {
    extends: ['../../tsconfig.base.json', './tsconfig.runtime.json'],
    compilerOptions: { lib: ['es2020'], rootDir: 'src', outDir: 'dist' },
  });
  writeJson(tree, 'packages/app/tsconfig.json', {
    references: [{ path: './tsconfig.lib.json' }, { path: './tsconfig.test.json' }],
  });
  flushChanges(root, tree.listChanges());
  return tree;
}

const projects = { app: { root: 'packages/app', targets: { test: { command: 'bun test' } } } };
const testConfigPath = 'packages/app/tsconfig.test.json';

async function reconcile(tree: FsTree, mode: 'update' | 'check' | 'diff') {
  return finishManagedFiles(tree.root, tree, await generateManagedFiles(tree, projects), mode);
}

describe('managed test tsconfig reconciliation', () => {
  it('reports missing test typechecking without writes, then repairs it to a fixed point', async () => {
    const initial = workspace();
    const root = initial.root;
    const originalRootConfig = readFileSync(join(root, 'packages/app/tsconfig.json'), 'utf8');

    for (const mode of ['check', 'diff'] satisfies ('check' | 'diff')[]) {
      const results = await reconcile(new FsTree(root, false), mode);
      expect(results).toContainEqual({ target: testConfigPath, action: 'drifted' });
      expect(results).toContainEqual({ target: 'packages/app/tsconfig.json', action: 'drifted' });
      expect(existsSync(join(root, testConfigPath))).toBe(false);
      expect(readFileSync(join(root, 'packages/app/tsconfig.json'), 'utf8')).toBe(originalRootConfig);
    }

    await reconcile(new FsTree(root, false), 'update');
    const updated = new FsTree(root, false);
    expect(readJson(updated, testConfigPath)).toMatchObject({
      extends: ['../../tsconfig.base.json', './tsconfig.runtime.json'],
      compilerOptions: { noEmit: true, composite: false, types: ['bun'] },
      references: [{ path: './tsconfig.lib.json' }],
    });
    expect(readJson(updated, 'packages/app/tsconfig.json')).toEqual({
      references: [{ path: './tsconfig.lib.json' }],
    });
    expect((await reconcile(updated, 'check')).filter((result) => result.action === 'drifted')).toEqual([]);
    expect(updated.listChanges()).toEqual([]);
  });

  it('uses staged package dependencies and test runtime options instead of disk snapshots', async () => {
    const tree = workspace();
    writeJson(tree, 'packages/dependency/package.json', { name: '@fixture/dependency', nx: { name: 'dependency' } });
    writeJson(tree, 'packages/dependency/tsconfig.lib.json', { extends: '../../tsconfig.base.json' });
    writeJson(tree, 'packages/app/package.json', {
      name: '@fixture/app',
      devDependencies: { '@fixture/dependency': 'workspace:*' },
      nx: { name: 'app', targets: { test: { command: 'bun test' } } },
    });
    writeJson(tree, testConfigPath, {
      extends: ['../../tsconfig.base.json', './tsconfig.runtime.json'],
      compilerOptions: { lib: ['es2023'], noEmit: false, outDir: 'dist-test' },
      include: ['fixtures/**/*.ts'],
      exclude: ['src/browser-only.ts'],
    });

    await generateManagedFiles(tree, projects);
    const config = readJson(tree, testConfigPath);
    expect(config).toMatchObject({
      extends: ['../../tsconfig.base.json', './tsconfig.runtime.json'],
      compilerOptions: { lib: ['es2023'], noEmit: true, composite: false },
      exclude: ['src/browser-only.ts'],
      include: expect.arrayContaining(['fixtures/**/*.ts', 'src/**/*.test.ts']),
      references: [{ path: './tsconfig.lib.json' }, { path: '../dependency/tsconfig.lib.json' }],
    });
    expect(config).not.toHaveProperty('compilerOptions.outDir');
    expect(existsSync(join(tree.root, testConfigPath))).toBe(false);
  });

  it('refuses a documented drifted config rather than deleting comments or flushing other managed files', async () => {
    const tree = workspace();
    const documented =
      '{\n  // Bun owns this test runtime.\n  "compilerOptions": { "lib": ["es2023"], "noEmit": false }\n}\n';
    tree.write(testConfigPath, documented);
    flushChanges(tree.root, tree.listChanges());

    await expect(reconcile(new FsTree(tree.root, false), 'update')).rejects.toThrow(testConfigPath);
    expect(readFileSync(join(tree.root, testConfigPath), 'utf8')).toBe(documented);
    expect(existsSync(join(tree.root, '.github/workflows/ci.yml'))).toBe(false);
  });
});
