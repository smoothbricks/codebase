import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { git, withFixtureRepo } from './fixture-repo.js';
import { bunBinary } from './private-registry.js';

/** Every fixture package claims this repository, so root ownership checks pass. */
export const PACK_FIXTURE_REPOSITORY = 'https://github.com/example/private-closure.git';

export interface PackWorkspaceOptions {
  /** Extra runtime edges on the packed entry package. */
  alphaDependencies?: Record<string, string>;
  alphaScripts?: Record<string, string>;
  /** Extra files written under packages/alpha, relative to the package root. */
  alphaFiles?: Record<string, string>;
  /** Overrides alpha's `types` / `exports` manifest fields. */
  alphaTypes?: string;
  alphaExports?: Record<string, unknown>;
  /** Overrides beta's version (e.g. an unpublished prerelease the publish-mode lock sync rewrites). */
  betaVersion?: string;
  /** Lightweight git tags created after the fixture commit (e.g. `beta@0.2.0` stable tags). */
  stableTags?: string[];
  /** Adds packages whose tags/private flags are ambiguous under the publish policy. */
  ambiguousTagPackages?: boolean;
  /** Add a public package with a runtime edge onto the private closure. */
  publicFace?: boolean;
  /** Add a publishable-tagged package owned by a different repository. */
  foreignPackage?: boolean;
}

/**
 * A committed private closure with the shapes classification must distinguish:
 * two publishable private packages, one internal `private: true` package that
 * may never be published, and optionally a public package reaching into the
 * private closure. The repo starts clean and committed so any working-tree or
 * ref change a command makes is directly observable.
 */
export async function withPackWorkspace(
  fn: (root: string) => Promise<void>,
  options: PackWorkspaceOptions = {},
): Promise<void> {
  await withFixtureRepo(async (root) => {
    // Build output stays out of the index so a command's Nx build does not
    // register as a working-tree mutation.
    await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\nartifacts\nbun-home\nbun-cache\ndist\n*.tgz\n');
    await writeFile(
      join(root, 'package.json'),
      manifestText({
        name: '@priv.test/source',
        private: true,
        version: '0.0.0',
        workspaces: ['packages/*'],
        repository: { type: 'git', url: PACK_FIXTURE_REPOSITORY },
      }),
    );
    await writeFile(
      join(root, 'nx.json'),
      manifestText({ targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } } }),
    );
    await writePackWorkspacePackage(root, {
      name: '@priv.test/alpha',
      projectName: 'alpha',
      path: 'packages/alpha',
      version: '0.1.0',
      dependencies: { '@priv.test/beta': 'workspace:*', ...(options.alphaDependencies ?? {}) },
      tags: ['npm:private'],
      ...(options.alphaTypes ? { types: options.alphaTypes } : {}),
      ...(options.alphaExports ? { exports: options.alphaExports } : {}),
    });
    for (const [relativePath, content] of Object.entries(options.alphaFiles ?? {})) {
      const filePath = join(root, 'packages/alpha', relativePath);
      await mkdir(dirname(filePath), { recursive: true });
      await writeFile(filePath, content);
    }
    await writePackWorkspacePackage(root, {
      name: '@priv.test/beta',
      projectName: 'beta',
      path: 'packages/beta',
      version: options.betaVersion ?? '0.2.0',
      tags: ['npm:private'],
    });
    await writePackWorkspacePackage(root, {
      name: '@priv.test/internal',
      projectName: 'internal',
      path: 'packages/internal',
      version: '0.0.0',
      tags: [],
      private: true,
    });
    if (options.publicFace === true) {
      await writePackWorkspacePackage(root, {
        name: '@priv.test/public-face',
        projectName: 'public-face',
        path: 'packages/public-face',
        version: '0.3.0',
        tags: ['npm:public'],
        dependencies: { '@priv.test/alpha': 'workspace:*' },
      });
    }
    if (options.foreignPackage === true) {
      await writePackWorkspacePackage(root, {
        name: '@priv.test/vendored',
        projectName: 'vendored',
        path: 'packages/vendored',
        version: '0.4.0',
        tags: ['npm:private'],
        repository: 'https://github.com/example/other-repo.git',
      });
    }
    if (options.ambiguousTagPackages === true) {
      await writePackWorkspacePackage(root, {
        name: '@priv.test/tagged-both',
        projectName: 'tagged-both',
        path: 'packages/tagged-both',
        version: '0.5.0',
        tags: ['npm:public', 'npm:private'],
      });
      await writePackWorkspacePackage(root, {
        name: '@priv.test/private-true',
        projectName: 'private-true',
        path: 'packages/private-true',
        version: '0.6.0',
        tags: ['npm:private'],
        private: true,
      });
    }
    // without one ("Failed to resolve workspace version"), so the fixture needs
    // a real lockfile. `--lockfile-only` writes it from the local workspace
    // alone: no network, no node_modules. It runs before the node_modules
    // symlink exists so nothing can reach the real workspace tree.
    await writeLockfile(root);
    // Nx needs a resolvable node_modules; the workspace's own is reused.
    await symlink(join(import.meta.dir, '../../../../../../node_modules'), join(root, 'node_modules'), 'dir');
    await git(root, ['add', '-A']);
    await git(root, ['commit', '-m', 'fixture closure']);
    for (const tag of options.stableTags ?? []) {
      await git(root, ['tag', tag]);
    }
    await withFixtureNxEnv(() => fn(root));
  });
}

export interface PackWorkspacePackage {
  name: string;
  projectName: string;
  path: string;
  version: string;
  tags: string[];
  dependencies?: Record<string, string>;
  scripts?: Record<string, string>;
  private?: boolean;
  /** Defaults to the fixture root's repository; a different URL is not owned by it. */
  repository?: string;
  /** Written verbatim into the package manifest. */
  types?: string;
  exports?: Record<string, unknown>;
}

export async function writePackWorkspacePackage(root: string, pkg: PackWorkspacePackage): Promise<void> {
  await mkdir(join(root, pkg.path, 'dist'), { recursive: true });
  await writeFile(join(root, pkg.path, 'dist', 'index.js'), `export const name = ${JSON.stringify(pkg.name)};\n`);
  await writeFile(
    join(root, pkg.path, 'package.json'),
    manifestText({
      name: pkg.name,
      version: pkg.version,
      ...(pkg.private === true ? { private: true } : {}),
      main: 'dist/index.js',
      files: ['dist'],
      repository: { type: 'git', url: pkg.repository ?? PACK_FIXTURE_REPOSITORY, directory: pkg.path },
      ...(pkg.tags.includes('npm:private') ? { publishConfig: { access: 'restricted' } } : {}),
      ...(pkg.dependencies ? { dependencies: pkg.dependencies } : {}),
      ...(pkg.scripts ? { scripts: pkg.scripts } : {}),
      ...(pkg.types ? { types: pkg.types } : {}),
      ...(pkg.exports ? { exports: pkg.exports } : {}),
      nx: {
        name: pkg.projectName,
        tags: pkg.tags,
        targets: {
          build: {
            executor: 'nx:run-commands',
            options: { command: "mkdir -p dist && echo 'export const built = true;' > dist/index.js", cwd: pkg.path },
          },
        },
      },
    }),
  );
}

function manifestText(value: object): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

async function writeLockfile(root: string): Promise<void> {
  const result = await Bun.$`${bunBinary()} install --lockfile-only`
    .cwd(root)
    .env({
      PATH: process.env.PATH ?? '',
      HOME: join(root, 'bun-home'),
      BUN_INSTALL_CACHE_DIR: join(root, 'bun-cache'),
    })
    .nothrow()
    .quiet();
  if (result.exitCode !== 0) {
    throw new Error(`fixture lockfile generation failed: ${result.stderr.toString()}`);
  }
}

/** `package/package.json` inside a packed tarball, as text so version pins are directly assertable. */
export async function packedManifestJson(tarball: string): Promise<string> {
  const result = await Bun.$`tar -xzOf ${tarball} package/package.json`.nothrow().quiet();
  if (result.exitCode !== 0) {
    throw new Error(`unable to read package.json from ${tarball}: ${result.stderr.toString()}`);
  }
  return result.stdout.toString();
}

/**
 * The bun-test preload (`src/bun/isolate-nx-env.ts`) already deletes
 * NX_CACHE_DIRECTORY/NX_WORKSPACE_DATA_DIRECTORY, so a fixture Nx keeps its
 * state under its own root. What remains is the daemon: a background daemon
 * started for a workspace that is about to be deleted outlives the test.
 */
export async function withFixtureNxEnv(fn: () => Promise<void>): Promise<void> {
  const previous = process.env.NX_DAEMON;
  process.env.NX_DAEMON = 'false';
  try {
    await fn();
  } finally {
    if (previous === undefined) {
      delete process.env.NX_DAEMON;
    } else {
      process.env.NX_DAEMON = previous;
    }
  }
}
