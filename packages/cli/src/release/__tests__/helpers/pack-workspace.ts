import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { git, withFixtureRepo } from './fixture-repo.js';
import { bunBinary } from './private-registry.js';

/** Every fixture package claims this repository, so root ownership checks pass. */
export const PACK_FIXTURE_REPOSITORY = 'https://github.com/example/private-closure.git';

export interface PackWorkspaceOptions {
  /**
   * Further packages written beside alpha, beta and internal before the one lockfile install, so a single workspace
   * can hold every variant a set of cases needs.
   */
  packages?: readonly PackWorkspacePackage[];
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
 * ref change a command makes is directly observable. Any further shape joins
 * through `packages`, so one workspace and one lockfile install can serve every
 * case that needs it.
 */
export async function withPackWorkspace(
  fn: (root: string) => Promise<void>,
  options: PackWorkspaceOptions = {},
): Promise<void> {
  await withFixtureRepo(async (root) => {
    // Pack and install output stays out of the index so neither registers as
    // a working-tree mutation.
    await writeFile(join(root, '.gitignore'), 'node_modules\nartifacts\nbun-home\nbun-cache\ndist\n*.tgz\n');
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
    await writePackWorkspacePackage(root, {
      name: '@priv.test/alpha',
      projectName: 'alpha',
      path: 'packages/alpha',
      version: '0.1.0',
      dependencies: { '@priv.test/beta': 'workspace:*' },
      tags: ['npm:private'],
    });
    await writePackWorkspacePackage(root, {
      name: '@priv.test/beta',
      projectName: 'beta',
      path: 'packages/beta',
      version: '0.2.0',
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
    for (const pkg of options.packages ?? []) {
      await writePackWorkspacePackage(root, pkg);
    }
    // `bun pm pack` resolves `workspace:*` from bun.lock and refuses to pack
    // without one ("Failed to resolve workspace version"), so the fixture needs
    // a real lockfile. `--lockfile-only` writes it from the local workspace
    // alone: no network, no node_modules.
    await writeLockfile(root);
    await git(root, ['add', '-A']);
    await git(root, ['commit', '-m', 'fixture closure']);
    await fn(root);
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
  /** Extra files under the package root, by path relative to it. */
  files?: Record<string, string>;
}

/**
 * A package as a release finds it after its build: `dist/index.js` is already
 * there. The tests pack; they never build.
 */
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
      nx: { name: pkg.projectName, tags: pkg.tags },
    }),
  );
  for (const [relativePath, content] of Object.entries(pkg.files ?? {})) {
    const filePath = join(root, pkg.path, relativePath);
    await mkdir(dirname(filePath), { recursive: true });
    await writeFile(filePath, content);
  }
}

function manifestText(value: object): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

async function writeLockfile(root: string): Promise<void> {
  const home = join(root, 'bun-home');
  await mkdir(home, { recursive: true });
  const result = await Bun.$`${bunBinary()} install --lockfile-only --ignore-scripts`
    .cwd(root)
    .env({
      PATH: process.env.PATH ?? '',
      HOME: home,
      // Bun leaves a file in its temporary directory on every install: the fixture's own, not /tmp.
      TMPDIR: home,
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
