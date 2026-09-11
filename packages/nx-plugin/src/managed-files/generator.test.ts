/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Tests assert literal credential and GitHub expressions. */
import { afterEach, describe, expect, it } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readJson, type Tree, updateJson } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';
import { FsTree, flushChanges } from 'nx/src/generators/tree.js';
import type { PackageJson } from '../workspace-manifest.js';
import type { WorkspaceProjects } from './context.js';
import { generateManagedFiles } from './generator.js';
import { INLINE_LOCAL_BEGIN, INLINE_LOCAL_END, LOCAL_SECTION_MARKER } from './managed-content.js';
import { assertNoManagedConflicts } from './tree.js';

const repository = 'https://github.com/example/workspace.git';
const projects: WorkspaceProjects = { library: { root: 'packages/library', targets: { build: {}, test: {} } } };
const temporaryRoots: string[] = [];
afterEach(() => {
  for (const root of temporaryRoots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function workspace(): Tree {
  const tree = createTreeWithEmptyWorkspace({ formatter: 'none' });
  tree.write(
    'package.json',
    JSON.stringify({ name: '@example/workspace', version: '1.0.0', repository, workspaces: ['packages/*'] }),
  );
  tree.write(
    'packages/library/package.json',
    JSON.stringify({
      name: '@example/library',
      version: '1.0.0',
      repository,
      nx: { name: 'library', tags: ['npm:public'] },
    }),
  );
  tree.write('bun.lock', '{}');
  return tree;
}

function generate(tree: Tree, graph: WorkspaceProjects = projects): void {
  assertNoManagedConflicts(generateManagedFiles(tree, graph, new Map()));
}

describe('managed-files generator', () => {
  it('generates workflows and packaged bootstrap assets in a virtual workspace', () => {
    const tree = workspace();
    generate(tree);
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).toContain('name: CI');
    expect(tree.read('.github/workflows/publish.yml', 'utf8')).toContain('name: Publish');
    expect(tree.read('tooling/direnv/devenv.smoo.nix', 'utf8')).toContain('languages.rust');
    expect(tree.listChanges().find((change) => change.path === 'tooling/devenv')?.options?.mode).toBe(0o755);
  });

  it('uses manifest edits from Tree, not the checkout that runs the tests', () => {
    const tree = workspace();
    updateJson<PackageJson>(tree, 'package.json', (pkg) => ({
      ...pkg,
      smoo: { github: { pushBranches: ['trunk'], runsOn: 'ubuntu-24.04' } },
    }));
    generate(tree);
    const ci = tree.read('.github/workflows/ci.yml', 'utf8');
    expect(ci).toContain('      - trunk');
    expect(ci).toContain('    runs-on: ubuntu-24.04');
    expect(ci).not.toContain('nixos-latest');
  });

  it('discovers publication from workspace packages and does not publish another repository', () => {
    const tree = workspace();
    updateJson<PackageJson>(tree, 'packages/library/package.json', (pkg) => ({
      ...pkg,
      repository: 'https://github.com/example/other.git',
    }));
    generate(tree);
    expect(tree.exists('.github/workflows/publish.yml')).toBe(false);
  });

  it('uses inferred graph targets rather than requiring explicit package target configuration', () => {
    const tree = workspace();
    generate(tree, {
      library: {
        root: 'packages/library',
        tags: ['stage-deploy-target'],
        targets: { deploy: { options: { command: 'wrangler deploy' } }, 'test-browser': {}, 'e2e-deployment': {} },
      },
    });
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).toContain('E2E Tests');
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).toContain('Browser Tests');
    expect(tree.exists('.github/workflows/pr-preview-cleanup.yml')).toBe(true);
    expect(readJson<PackageJson>(tree, 'packages/library/package.json').nx?.targets).toBeUndefined();
  });

  it('preserves repository-owned Nix configuration and managed local sections', () => {
    const tree = workspace();
    tree.write('tooling/direnv/devenv.nix', '{ ... }: { imports = [ ./devenv.smoo.nix ]; }');
    tree.write('.gitattributes', `old\n${LOCAL_SECTION_MARKER}\n*.custom merge=custom\n`);
    generate(tree);
    expect(tree.read('tooling/direnv/devenv.nix', 'utf8')).toBe('{ ... }: { imports = [ ./devenv.smoo.nix ]; }');
    expect(tree.read('.gitattributes', 'utf8')).toEndWith(`${LOCAL_SECTION_MARKER}\n*.custom merge=custom\n`);
  });

  it('rejects an ownership conflict before callers may flush the Tree', () => {
    const tree = workspace();
    tree.write('.gitattributes', ['removed anchor', INLINE_LOCAL_BEGIN, 'local', INLINE_LOCAL_END].join('\n'));
    expect(() => generate(tree)).toThrow('before writing');
  });

  it('does not resolve configured secrets or interpolate their values into generated files', () => {
    const tree = workspace();
    updateJson<PackageJson>(tree, 'package.json', (pkg) => ({
      ...pkg,
      smoo: {
        secrets: { TEST_KEY: { command: ['sh', '-c', 'exit 99'] } },
        privateNpm: { scope: '@private', readTokenEnv: 'TEST_KEY' },
      },
    }));
    generate(tree);
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).not.toContain('TEST_KEY');
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).not.toContain('exit 99');
  });

  it('uses committed registry configuration from Tree when a private package is actually consumed', () => {
    const tree = workspace();
    updateJson<PackageJson>(tree, 'package.json', (pkg) => ({
      ...pkg,
      dependencies: { '@private/library': '1.0.0' },
      smoo: { privateNpm: { scope: '@private' } },
    }));
    tree.write(
      '.npmrc',
      '@private:registry=https://registry.example.net/npm/\n//registry.example.net/npm/:_authToken=${PRIVATE_READ}\n',
    );
    generate(tree);
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).toContain('PRIVATE_READ: ${{ secrets.PRIVATE_READ }}');
    expect(tree.read('.github/workflows/ci.yml', 'utf8')).not.toContain('PRIVATE_WRITE');
  });

  it('stages nothing on a fresh Tree after applying the first generation', () => {
    const root = mkdtempSync(join(tmpdir(), 'smoo-generator-'));
    temporaryRoots.push(root);
    const first = workspace();
    generate(first);
    flushChanges(root, first.listChanges());
    const second = new FsTree(root, false);
    const results = generateManagedFiles(second, projects);
    assertNoManagedConflicts(results);
    expect(second.listChanges()).toEqual([]);
    expect(results.every((result) => result.action === 'unchanged' || result.action === 'skipped')).toBe(true);
    expect(readFileSync(join(root, 'tooling/devenv'), 'utf8')).toBe(first.read('tooling/devenv', 'utf8') ?? '');
  });
});
