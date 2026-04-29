import { describe, expect, it } from 'bun:test';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import { isRecord } from '../../lib/json.js';
import { type AutoReleaseCandidateShell, autoReleaseCandidatePackages } from '../candidates.js';
import type { ReleasePackageInfo } from '../core.js';
import { git, tag, withFixtureRepo, writePackage } from './helpers/fixture-repo.js';

const a: ReleasePackageInfo = { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '1.0.0' };
const b: ReleasePackageInfo = { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '1.0.0' };
const c: ReleasePackageInfo = { name: '@scope/c', projectName: 'c', path: 'packages/c', version: '0.1.0' };
const d: ReleasePackageInfo = { name: '@scope/d', projectName: 'd', path: 'packages/d', version: '0.1.0' };

describe('auto release candidate filtering', () => {
  it('selects tagged packages only when their package path changed', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, a.version);
      await writePackage(root, b.name, b.path, b.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial packages']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(root, '@scope/b@1.0.0', '2025-01-01T00:00:01Z');

      await writeFile(join(root, 'README.md'), 'root-only change\n');
      await git(root, ['add', 'README.md']);
      await git(root, ['commit', '-m', 'docs: root only']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a, b])).resolves.toEqual([]);

      await mkdir(join(root, a.path, 'src'), { recursive: true });
      await writeFile(join(root, a.path, 'src/index.ts'), 'export const changed = true;\n');
      await git(root, ['add', join(a.path, 'src/index.ts')]);
      await git(root, ['commit', '-m', 'feat(a): package local']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a, b])).resolves.toEqual([a]);
    });
  });

  it('ignores package-local config changes that do not affect published users', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, a.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial package']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');

      await mkdir(join(root, a.path), { recursive: true });
      await writeFile(
        join(root, a.path, 'package.json'),
        `${JSON.stringify(
          {
            name: a.name,
            version: a.version,
            scripts: { test: 'nx run a:test' },
            devDependencies: { typescript: '^5.9.3' },
            nx: { name: 'a' },
          },
          null,
          2,
        )}\n`,
      );
      await writeFile(join(root, a.path, 'tsconfig.test.json'), `${JSON.stringify({ compilerOptions: {} })}\n`);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'chore(a): normalize package config']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a])).resolves.toEqual([]);
    });
  });

  it('selects package manifest changes that affect published users', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, a.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial package']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');

      await writeFile(
        join(root, a.path, 'package.json'),
        `${JSON.stringify(
          {
            name: a.name,
            version: a.version,
            exports: { '.': './dist/index.js' },
            dependencies: { '@scope/shared': '^1.0.0' },
          },
          null,
          2,
        )}\n`,
      );
      await git(root, ['add', join(a.path, 'package.json')]);
      await git(root, ['commit', '-m', 'fix(a): update public entrypoint']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a])).resolves.toEqual([a]);
    });
  });

  it('selects files included by package manifest files entries', async () => {
    await withFixtureRepo(async (root) => {
      await mkdir(join(root, a.path), { recursive: true });
      await writeFile(
        join(root, a.path, 'package.json'),
        `${JSON.stringify({ name: a.name, version: a.version, files: ['schema'] }, null, 2)}\n`,
      );
      await mkdir(join(root, a.path, 'schema'), { recursive: true });
      await writeFile(join(root, a.path, 'schema/index.json'), '{}\n');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial package']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');

      await writeFile(join(root, a.path, 'schema/index.json'), '{"changed":true}\n');
      await git(root, ['add', join(a.path, 'schema/index.json')]);
      await git(root, ['commit', '-m', 'fix(a): update shipped schema']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [a])).resolves.toEqual([a]);
    });
  });

  it('selects package changes from Nx build input patterns', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, a.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial package']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');

      await mkdir(join(root, a.path, 'generated'), { recursive: true });
      await writeFile(join(root, a.path, 'generated/schema.ts'), 'export const schema = 1;\n');
      await git(root, ['add', join(a.path, 'generated/schema.ts')]);
      await git(root, ['commit', '-m', 'fix(a): update generated build input']);

      await expect(
        autoReleaseCandidatePackages(gitCandidateShell(root, { buildInputPatterns: ['generated/**/*'] }), [a]),
      ).resolves.toEqual([a]);
    });
  });

  it('uses precise Nx production inputs to ignore config and test-only package paths', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, a.name, a.path, a.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial package']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');

      const shell = gitCandidateShell(root, { buildInputPatterns: ['src/**/*.ts', '!src/**/*.test.ts'] });

      await mkdir(join(root, a.path, 'src'), { recursive: true });
      await writeFile(join(root, a.path, 'vite.config.ts'), 'export default {};\n');
      await writeFile(join(root, a.path, 'src/index.test.ts'), 'export const testOnly = true;\n');
      await git(root, ['add', join(a.path, 'vite.config.ts'), join(a.path, 'src/index.test.ts')]);
      await git(root, ['commit', '-m', 'test(a): update local-only inputs']);

      await expect(autoReleaseCandidatePackages(shell, [a])).resolves.toEqual([]);

      await writeFile(join(root, a.path, 'src/index.ts'), 'export const shipped = true;\n');
      await git(root, ['add', join(a.path, 'src/index.ts')]);
      await git(root, ['commit', '-m', 'fix(a): update production input']);

      await expect(autoReleaseCandidatePackages(shell, [a])).resolves.toEqual([a]);
    });
  });

  it('includes untagged packages only when their package path has history', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, c.name, c.path, c.version);
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'feat(c): add package']);

      await expect(autoReleaseCandidatePackages(gitCandidateShell(root), [c, d])).resolves.toEqual([c]);
    });
  });
});

function gitCandidateShell(root: string, options: { buildInputPatterns?: string[] } = {}): AutoReleaseCandidateShell {
  return {
    gitRefExists: async (ref) => {
      const result = await $`git rev-parse --verify ${ref}`.cwd(root).quiet().nothrow();
      return result.exitCode === 0;
    },
    packageChangedFilesSince: async (ref, packagePath) => {
      const result = await $`git diff --name-only ${`${ref}..HEAD`} -- ${packagePath}`.cwd(root).quiet().nothrow();
      if (result.exitCode === 0) {
        const packagePrefix = `${packagePath}/`;
        return new TextDecoder()
          .decode(result.stdout)
          .split('\n')
          .map((path) => path.trim())
          .filter(Boolean)
          .map((path) => (path.startsWith(packagePrefix) ? path.slice(packagePrefix.length) : path));
      }
      throw new Error(`Unable to inspect package changes under ${packagePath}.`);
    },
    packageJsonAtRef: async (ref, packagePath) => {
      const result = await $`git show ${`${ref}:${packagePath}/package.json`}`.cwd(root).quiet().nothrow();
      if (result.exitCode !== 0) {
        return null;
      }
      return parseJsonObject(new TextDecoder().decode(result.stdout));
    },
    currentPackageJson: async (packagePath) =>
      parseJsonObject(await readFile(join(root, packagePath, 'package.json'), 'utf8')),
    packageBuildInputPatterns: async () => options.buildInputPatterns ?? ['**/*'],
    packageHasHistory: async (packagePath) => {
      const result = await $`git log --format=%H -- ${packagePath}`.cwd(root).quiet().nothrow();
      if (result.exitCode !== 0) {
        throw new Error(`Unable to inspect package history under ${packagePath}.`);
      }
      return new TextDecoder().decode(result.stdout).trim().length > 0;
    },
  };
}

function parseJsonObject(text: string): Record<string, unknown> {
  const parsed = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error('expected JSON object');
  }
  return parsed;
}
