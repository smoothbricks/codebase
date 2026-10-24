import { describe, expect, it } from 'bun:test';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { PackageJson } from '../../lib/json.js';
import { parsePackageJsonText } from '../../lib/json.js';
import { packageInfo } from '../../lib/workspace.js';
import { releaseCandidateShell } from '../candidate-shell.js';
import {
  type AutoReleaseCandidateShell,
  autoReleaseCandidatePackages,
  parseReleaseProjectSelection,
  releaseCandidatePackages,
} from '../candidates.js';
import type { ReleasePackageInfo } from '../core.js';
import { git, tag, withFixtureRepo, writePackage, writeWorkspace } from './helpers/fixture-repo.js';

const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };
const b: ReleasePackageInfo = { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '1.0.0' };
const c: ReleasePackageInfo = { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '0.1.0' };
const d: ReleasePackageInfo = { name: '@scope/d', projectName: 'd', path: 'packages/d', version: '0.1.0' };
const cli: ReleasePackageInfo = { name: '@scope/cli', projectName: 'cli', path: 'packages/cli', version: '0.2.0' };
const next: ReleasePackageInfo = {
  name: '@scope/next',
  projectName: 'next',
  path: 'packages/next',
  version: '1.1.0-next.0',
};

describe('auto release candidate filtering', () => {
  it('selects tagged packages only when their package path changed', async () => {
    const repo = new MemoryRepository();
    repo.commit({ ...packageManifest(a), ...packageManifest(b) });
    repo.tag('a@1.0.0');
    repo.tag('b@1.0.0');

    repo.commit({ 'README.md': 'root-only change\n' });

    await expect(autoReleaseCandidatePackages(repo, [a, b])).resolves.toEqual([]);

    repo.commit({ [join(a.path, 'src/index.ts')]: 'export const changed = true;\n' });

    await expect(autoReleaseCandidatePackages(repo, [a, b])).resolves.toEqual([a]);
  });

  it('ignores package-local config changes that do not affect published users', async () => {
    const repo = new MemoryRepository();
    repo.commit(packageManifest(a));
    repo.tag('a@1.0.0');

    repo.commit({
      [join(a.path, 'package.json')]: jsonText({
        name: a.name,
        version: a.version,
        scripts: { test: 'nx run a:test' },
        devDependencies: { typescript: '^5.9.3' },
        nx: { name: 'a' },
      }),
      [join(a.path, 'tsconfig.test.json')]: jsonText({ compilerOptions: {} }),
    });

    await expect(autoReleaseCandidatePackages(repo, [a])).resolves.toEqual([]);
  });

  it('selects package manifest changes that affect published users', async () => {
    const repo = new MemoryRepository();
    repo.commit(packageManifest(a));
    repo.tag('a@1.0.0');

    repo.commit({
      [join(a.path, 'package.json')]: jsonText({
        name: a.name,
        version: a.version,
        exports: { '.': './dist/index.js' },
        dependencies: { '@scope/shared': '^1.0.0' },
      }),
    });

    await expect(autoReleaseCandidatePackages(repo, [a])).resolves.toEqual([a]);
  });

  it('selects files included by package manifest files entries', async () => {
    const repo = new MemoryRepository();
    repo.commit({
      [join(a.path, 'package.json')]: jsonText({ name: a.name, version: a.version, files: ['schema'] }),
      [join(a.path, 'schema/index.json')]: '{}\n',
    });
    repo.tag('a@1.0.0');

    repo.commit({ [join(a.path, 'schema/index.json')]: '{"changed":true}\n' });

    await expect(autoReleaseCandidatePackages(repo, [a])).resolves.toEqual([a]);
  });

  it('selects package changes from Nx build input patterns', async () => {
    const repo = new MemoryRepository({ buildInputPatterns: ['generated/**/*'] });
    repo.commit(packageManifest(a));
    repo.tag('a@1.0.0');

    repo.commit({ [join(a.path, 'generated/schema.ts')]: 'export const schema = 1;\n' });

    await expect(autoReleaseCandidatePackages(repo, [a])).resolves.toEqual([a]);
  });

  it('uses precise Nx production inputs to ignore config and test-only package paths', async () => {
    const repo = new MemoryRepository({ buildInputPatterns: ['src/**/*.ts', '!src/**/*.test.ts'] });
    repo.commit(packageManifest(a));
    repo.tag('a@1.0.0');

    repo.commit({
      [join(a.path, 'vite.config.ts')]: 'export default {};\n',
      [join(a.path, 'src/index.test.ts')]: 'export const testOnly = true;\n',
    });

    await expect(autoReleaseCandidatePackages(repo, [a])).resolves.toEqual([]);

    repo.commit({ [join(a.path, 'src/index.ts')]: 'export const shipped = true;\n' });

    await expect(autoReleaseCandidatePackages(repo, [a])).resolves.toEqual([a]);
  });

  it('includes untagged packages only when their package path has history', async () => {
    const repo = new MemoryRepository();
    repo.commit(packageManifest(c));

    await expect(autoReleaseCandidatePackages(repo, [c, d])).resolves.toEqual([c]);
  });

  it('does not auto-release an untagged next prerelease package version with no prior stable release', async () => {
    const repo = new MemoryRepository();
    repo.commit(packageManifest(next));

    await expect(autoReleaseCandidatePackages(repo, [next])).resolves.toEqual([]);
  });

  // The one real-git case: the production shell reads a real repository and a real Nx workspace.
  it('selects a prerelease package when releasable changes exist since the last stable tag', async () => {
    await withFixtureRepo(async (root) => {
      await writeWorkspace(root);
      await writeNxPackage(root, next, '1.0.0');
      await writePackage(root, c.name, c.path, c.version);
      await git(root, ['add', 'package.json', 'nx.json', 'packages']);
      await git(root, ['commit', '-m', 'chore(release): publish']);
      await tag(root, 'next@1.0.0', '2025-01-01T00:00:00Z');

      // Simulate the post-release prerelease bump
      await writeNxPackage(root, next, next.version);
      await git(root, ['add', 'packages']);
      await git(root, ['commit', '-m', 'chore(release): prepare next prerelease']);

      // A real fix lands on the prerelease version
      await mkdir(join(root, next.path, 'src'), { recursive: true });
      await writeFile(join(root, next.path, 'src/index.ts'), 'export const fix = true;\n');
      await git(root, ['add', join(next.path, 'src/index.ts')]);
      await git(root, ['commit', '-m', 'fix(next): repair something']);

      const shell = await releaseCandidateShell(root);
      await expect(autoReleaseCandidatePackages(shell, [next])).resolves.toEqual([next]);
      // c is untagged but has history, so it is a candidate; d has neither a package nor history.
      await expect(autoReleaseCandidatePackages(shell, [c, d])).resolves.toEqual([c]);
    });
  });

  it('treats a manifest that cannot be read at the tag or at HEAD as a release change', async () => {
    const unreadableAtTag = new MemoryRepository();
    unreadableAtTag.commit({ [join(a.path, 'package.json')]: '{ "name": ' });
    unreadableAtTag.tag('a@1.0.0');
    unreadableAtTag.commit(packageManifest(a));

    const unreadableAtHead = new MemoryRepository();
    unreadableAtHead.commit(packageManifest(a));
    unreadableAtHead.tag('a@1.0.0');
    unreadableAtHead.commit({ [join(a.path, 'package.json')]: '[]' });

    await expect(autoReleaseCandidatePackages(unreadableAtTag, [a])).resolves.toEqual([a]);
    await expect(autoReleaseCandidatePackages(unreadableAtHead, [a])).resolves.toEqual([a]);
  });

  it('does not select a prerelease package when no releasable changes exist since the last stable tag', async () => {
    const repo = new MemoryRepository();
    repo.commit({
      ...packageManifest(next, '1.0.0'),
      [join(next.path, 'src/index.ts')]: 'export const v1 = true;\n',
    });
    repo.tag('next@1.0.0');

    // Only the prerelease bump, no releasable source changes
    repo.commit(packageManifest(next));

    await expect(autoReleaseCandidatePackages(repo, [next])).resolves.toEqual([]);
  });

  it('uses a repaired project-name tag as the follow-up auto release baseline for a scoped package', async () => {
    const repo = new MemoryRepository({ buildInputPatterns: ['src/**/*.ts'] });
    repo.commit(packageManifest(cli));
    repo.tag('cli@0.2.0');

    repo.commit({ [join(cli.path, 'src/index.ts')]: 'export const fixed = true;\n' });

    const candidates = await autoReleaseCandidatePackages(repo, [cli]);

    expect(candidates).toEqual([cli]);
    expect(repo.refLookups).toEqual(['refs/tags/cli@0.2.0']);
    expect(repo.refLookups).not.toContain('refs/tags/@scope/cli@0.2.0');
  });

  it('selects a private-scoped package by the same path-change rule as public packages', async () => {
    const widget: ReleasePackageInfo = {
      name: '@priv.test/widget',
      projectName: 'widget',
      path: 'packages/widget',
      version: '0.1.0',
    };
    const repo = new MemoryRepository();
    repo.commit({ ...packageManifest(widget), ...packageManifest(a) });
    repo.tag('widget@0.1.0');
    repo.tag('a@1.0.0');

    repo.commit({ [join(widget.path, 'src/index.ts')]: 'export const changed = true;\n' });

    // Release candidacy is registry-class agnostic: both npm:public and
    // npm:private packages version through the same selection. Only the
    // npmjs bootstrap/trust-publisher flows stay public-only.
    await expect(autoReleaseCandidatePackages(repo, [widget, a])).resolves.toEqual([widget]);
  });
});

describe('release project selection', () => {
  it('parses blank, all, and explicit project selections', () => {
    expect(parseReleaseProjectSelection(undefined, [a.projectName, b.projectName])).toEqual({ mode: 'changed' });
    expect(parseReleaseProjectSelection('', [a.projectName, b.projectName])).toEqual({ mode: 'changed' });
    expect(parseReleaseProjectSelection('  ', [a.projectName, b.projectName])).toEqual({ mode: 'changed' });
    expect(parseReleaseProjectSelection('all', [a.projectName, b.projectName])).toEqual({ mode: 'all' });
    expect(parseReleaseProjectSelection('a, b', [a.projectName, b.projectName])).toEqual({
      mode: 'named',
      projects: ['a', 'b'],
    });
    expect(() => parseReleaseProjectSelection('a, ghost', [a.projectName, b.projectName])).toThrow(
      'Unknown release project(s): ghost. Owned release projects: a, b.',
    );
  });

  it('scopes a selection to the changed set like the auto filter, whatever the bump mode is', async () => {
    const repo = new MemoryRepository();
    repo.commit({ ...packageManifest(a), ...packageManifest(b) });
    repo.tag('a@1.0.0');
    repo.tag('b@1.0.0');

    repo.commit({ [join(a.path, 'src/index.ts')]: 'export const changed = true;\n' });

    // An explicit bump keeps its force meaning for the specifier only: the
    // unchanged package has no changed dependents, so it stays skipped.
    await expect(releaseCandidatePackages(repo, [a, b], { mode: 'changed' })).resolves.toEqual([a]);
  });

  it('releases the named projects without change detection', async () => {
    const repo = new MemoryRepository();
    repo.commit({ ...packageManifest(a), ...packageManifest(b) });
    repo.tag('a@1.0.0');
    repo.tag('b@1.0.0');

    await expect(releaseCandidatePackages(repo, [a, b], { mode: 'named', projects: ['b'] })).resolves.toEqual([b]);
  });

  it('a named release closes over the workspace dependencies that carry unreleased changes', async () => {
    // b depends on a and on c; a changed since its release, c did not.
    const c: ReleasePackageInfo = { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '1.0.0' };
    const repo = new MemoryRepository();
    repo.commit({
      ...packageManifest(a),
      ...packageManifest(c),
      [join(b.path, 'package.json')]: jsonText({
        name: b.name,
        version: b.version,
        dependencies: { [a.name]: 'workspace:*', [c.name]: 'workspace:*' },
      }),
    });
    repo.tag('a@1.0.0');
    repo.tag('b@1.0.0');
    repo.tag('c@1.0.0');

    repo.commit({ [join(a.path, 'src/index.ts')]: 'export const changed = true;\n' });

    // b is built against a's HEAD; releasing b alone would pin a@1.0.0, a build nobody tested.
    await expect(releaseCandidatePackages(repo, [a, b, c], { mode: 'named', projects: ['b'] })).resolves.toEqual([
      a,
      b,
    ]);
  });

  it('releases the whole fleet only when the selection is all', async () => {
    const repo = new MemoryRepository();
    repo.commit({ ...packageManifest(a), ...packageManifest(b) });
    repo.tag('a@1.0.0');
    repo.tag('b@1.0.0');

    await expect(releaseCandidatePackages(repo, [a, b], { mode: 'all' })).resolves.toEqual([a, b]);
  });
});

/** `package.json` as writePackage writes it, keyed by its path in the repository. */
function packageManifest(pkg: ReleasePackageInfo, version = pkg.version): Record<string, string> {
  return { [join(pkg.path, 'package.json')]: jsonText({ name: pkg.name, version }) };
}

function jsonText(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

/** Paths mapped to file contents: the shape of a git tree. */
type Tree = ReadonlyMap<string, string>;

const EMPTY_TREE: Tree = new Map<string, string>();

/**
 * An in-memory git history for the candidate rules: linear commits, each a full file tree, and refs that name a tree.
 * The answers match what rev-parse --verify, tag --list, diff <ref>..HEAD, show <ref>:<path> and log -- <path> give on
 * such a history. The working tree is the HEAD tree: every fixture commits its changes before the rules read them.
 */
class MemoryRepository implements AutoReleaseCandidateShell {
  /** Every ref the rules asked about, in order. */
  readonly refLookups: string[] = [];
  readonly #commits: Tree[] = [];
  readonly #refs = new Map<string, Tree>();
  readonly #buildInputPatterns: string[] | undefined;

  constructor(options: { buildInputPatterns?: string[] } = {}) {
    this.#buildInputPatterns = options.buildInputPatterns;
  }

  /** Commits file contents on top of HEAD; paths not named keep their HEAD content. */
  commit(files: Record<string, string>): void {
    this.#commits.push(new Map([...this.#head(), ...Object.entries(files)]));
  }

  /** Names HEAD as refs/tags/<name>. */
  tag(name: string): void {
    this.#refs.set(`refs/tags/${name}`, this.#head());
  }

  async gitRefExists(ref: string): Promise<boolean> {
    this.refLookups.push(ref);
    return this.#refs.has(ref);
  }

  async latestStableReleaseRef(projectName: string): Promise<string | null> {
    const prefix = `refs/tags/${projectName}@`;
    let latest: string | null = null;
    for (const ref of this.#refs.keys()) {
      if (!ref.startsWith(prefix) || ref.slice(prefix.length).includes('-')) continue;
      if (latest === null || compareStableVersions(ref.slice(prefix.length), latest.slice(prefix.length)) > 0) {
        latest = ref;
      }
    }
    return latest;
  }

  async packageChangedFilesSince(ref: string, packagePath: string): Promise<string[]> {
    const prefix = `${packagePath}/`;
    return changedPaths(this.#refTree(ref), this.#head())
      .filter((path) => path.startsWith(prefix))
      .map((path) => path.slice(prefix.length));
  }

  async packageJsonAtRef(ref: string, packagePath: string): Promise<PackageJson | null> {
    return manifestIn(this.#refTree(ref), packagePath);
  }

  async currentPackageJson(packagePath: string): Promise<PackageJson | null> {
    // readPackageJson: a manifest without a name and a version is no workspace package.
    const manifest = manifestIn(this.#head(), packagePath);
    return manifest !== null && packageInfo(join(packagePath, 'package.json'), manifest) !== null ? manifest : null;
  }

  async packageBuildInputPatterns(): Promise<string[]> {
    return this.#buildInputPatterns ?? ['**/*'];
  }

  async packageHasHistory(packagePath: string): Promise<boolean> {
    const prefix = `${packagePath}/`;
    let parent: Tree = EMPTY_TREE;
    for (const tree of this.#commits) {
      if (changedPaths(parent, tree).some((path) => path.startsWith(prefix))) return true;
      parent = tree;
    }
    return false;
  }

  #head(): Tree {
    return this.#commits.at(-1) ?? EMPTY_TREE;
  }

  #refTree(ref: string): Tree {
    const tree = this.#refs.get(ref);
    if (tree === undefined) throw new Error(`${ref} names no commit in the in-memory repository`);
    return tree;
  }
}

/** Paths whose content differs between two trees: added, removed or modified. */
function changedPaths(before: Tree, after: Tree): string[] {
  const paths = new Set([...before.keys(), ...after.keys()]);
  return [...paths].filter((path) => before.get(path) !== after.get(path)).sort();
}

/** A manifest as candidate-shell.ts reads it: null when absent, not JSON, or not a manifest shape. */
function manifestIn(tree: Tree, packagePath: string): PackageJson | null {
  const text = tree.get(join(packagePath, 'package.json'));
  if (text === undefined) return null;
  try {
    return parsePackageJsonText(text);
  } catch {
    return null;
  }
}

/** Orders dotted stable versions numerically, the order git tag --sort=-v:refname gives them. */
function compareStableVersions(left: string, right: string): number {
  const leftParts = left.split('.').map(Number);
  const rightParts = right.split('.').map(Number);
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const difference = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

/**
 * A workspace package that Nx names by its project name, with a build target whose inputs are the package's own
 * files: the target `nx show project` reports and the production shell derives its build inputs from.
 */
async function writeNxPackage(root: string, pkg: ReleasePackageInfo, version: string): Promise<void> {
  await mkdir(join(root, pkg.path), { recursive: true });
  const manifest = {
    name: pkg.name,
    version,
    nx: {
      name: pkg.projectName,
      targets: {
        build: {
          executor: 'nx:run-commands',
          inputs: ['{projectRoot}/**/*'],
          options: { command: "mkdir -p dist && echo '{}' > dist/index.js", cwd: pkg.path },
        },
      },
    },
  };
  await writeFile(join(root, pkg.path, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`);
}
