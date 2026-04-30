import { describe, expect, it } from 'bun:test';
import { addProjectConfiguration, readJson, type Tree } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import generator from './generator.js';

function setupTree(): Tree {
  const tree = createTreeWithEmptyWorkspace();
  tree.write(
    'package.json',
    JSON.stringify(
      {
        name: '@smoothbricks/codebase',
        license: 'MIT',
        repository: { type: 'git', url: 'git+https://github.com/smoothbricks/codebase.git' },
        workspaces: ['packages/*'],
      },
      null,
      2,
    ),
  );
  return tree;
}

function addPackage(tree: Tree, name: string, root: string, extra: Record<string, unknown> = {}): void {
  addProjectConfiguration(tree, name, { root, targets: {} });
  if (tree.exists(`${root}/project.json`)) tree.delete(`${root}/project.json`);
  tree.write(
    `${root}/package.json`,
    JSON.stringify(
      {
        name: `@smoothbricks/${name}`,
        version: '0.0.0',
        private: true,
        nx: { name },
        ...extra,
      },
      null,
      2,
    ),
  );
}

describe('make-public generator', () => {
  it('promotes private package to public', async () => {
    const tree = setupTree();
    addPackage(tree, 'my-lib', 'packages/my-lib');

    await generator(tree, { project: 'my-lib' });

    const pkg = readJson(tree, 'packages/my-lib/package.json');
    expect(pkg.private).toBeUndefined();
    expect(pkg.license).toBe('MIT');
    expect(pkg.publishConfig).toEqual({ access: 'public' });
    expect(pkg.repository).toEqual({
      type: 'git',
      url: 'git+https://github.com/smoothbricks/codebase.git',
      directory: 'packages/my-lib',
    });
    expect(pkg.nx.tags).toContain('npm:public');
    expect(pkg.version).toBe('0.1.0');
  });

  it('preserves existing license', async () => {
    const tree = setupTree();
    addPackage(tree, 'my-lib', 'packages/my-lib', { license: 'Apache-2.0' });

    await generator(tree, { project: 'my-lib' });

    const pkg = readJson(tree, 'packages/my-lib/package.json');
    expect(pkg.license).toBe('Apache-2.0');
  });

  it('does not duplicate npm:public tag', async () => {
    const tree = setupTree();
    addPackage(tree, 'my-lib', 'packages/my-lib', { nx: { name: 'my-lib', tags: ['npm:public'] } });

    await generator(tree, { project: 'my-lib' });

    const pkg = readJson(tree, 'packages/my-lib/package.json');
    const publicTags = pkg.nx.tags.filter((t: string) => t === 'npm:public');
    expect(publicTags).toHaveLength(1);
  });

  it('preserves version above 0.0.0', async () => {
    const tree = setupTree();
    addPackage(tree, 'my-lib', 'packages/my-lib', { version: '1.2.3' });

    await generator(tree, { project: 'my-lib' });

    const pkg = readJson(tree, 'packages/my-lib/package.json');
    expect(pkg.version).toBe('1.2.3');
  });

  it('resolves by project name, package name, and root', async () => {
    const tree = setupTree();
    addPackage(tree, 'my-lib', 'packages/my-lib');

    // By project name
    await generator(tree, { project: 'my-lib' });
    expect(readJson(tree, 'packages/my-lib/package.json').private).toBeUndefined();

    // Reset private for next lookup test
    tree.write(
      'packages/my-lib/package.json',
      JSON.stringify(
        { name: '@smoothbricks/my-lib', version: '0.0.0', private: true, nx: { name: 'my-lib' } },
        null,
        2,
      ),
    );

    // By package name
    await generator(tree, { project: '@smoothbricks/my-lib' });
    expect(readJson(tree, 'packages/my-lib/package.json').private).toBeUndefined();

    // Reset private for next lookup test
    tree.write(
      'packages/my-lib/package.json',
      JSON.stringify(
        { name: '@smoothbricks/my-lib', version: '0.0.0', private: true, nx: { name: 'my-lib' } },
        null,
        2,
      ),
    );

    // By root path
    await generator(tree, { project: 'packages/my-lib' });
    expect(readJson(tree, 'packages/my-lib/package.json').private).toBeUndefined();
  });

  it('throws for unknown project', async () => {
    const tree = setupTree();

    expect(generator(tree, { project: 'nonexistent' })).rejects.toThrow('Could not resolve project nonexistent');
  });
});
