import { describe, expect, it } from 'bun:test';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import { collectOwnedReleaseTagRecords, pendingReleaseTargets, type ReleasePackageInfo, releaseTag } from '../core.js';
import { completeReleaseAtHead, type ReleaseRepairShell, repairPendingTargets } from '../orchestration.js';
import {
  git,
  gitIsAncestor,
  gitOutput,
  gitReleaseTagsByCreatorDate,
  packageVersionAtRef,
  tag,
  withFixtureRepo,
  writeBuildablePackage,
  writePackage,
  writeWorkspace,
} from './helpers/fixture-repo.js';

describe('release planning with fixture git repositories', () => {
  it('plans repairs from real annotated release tags and fake durable npm/GitHub state', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, '@scope/a', 'packages/a', '1.0.0');
      await writePackage(root, '@scope/b', 'packages/b', '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'initial release']);
      const first = await gitOutput(root, ['rev-parse', 'HEAD']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(root, '@scope/b@1.0.0', '2025-01-01T00:00:01Z');

      await writePackage(root, '@scope/a', 'packages/a', '1.1.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'second release']);
      const second = await gitOutput(root, ['rev-parse', 'HEAD']);
      await tag(root, '@scope/a@1.1.0', '2025-01-02T00:00:00Z');

      await writeFile(join(root, 'readme.md'), 'not a release\n');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'work after release']);
      const head = await gitOutput(root, ['rev-parse', 'HEAD']);
      await tag(root, 'not-owned@9.9.9', '2025-01-03T00:00:00Z');

      const npmPublished = new Set(['@scope/a@1.0.0', '@scope/b@1.0.0']);
      const githubReleases = new Set(['@scope/a@1.0.0']);
      const records = await collectOwnedReleaseTagRecords(
        [
          { name: '@scope/a', projectName: 'a', path: 'packages/a' },
          { name: '@scope/b', projectName: 'b', path: 'packages/b' },
        ],
        head,
        {
          listReleaseTagsByCreatorDate: () => gitReleaseTagsByCreatorDate(root),
          isAncestor: (ancestor, descendant) => gitIsAncestor(root, ancestor, descendant),
          packageVersionAtRef: (packagePath, ref) => packageVersionAtRef(root, packagePath, ref),
          durableTagState: async (pkg, tagName) => ({
            npmPublished: npmPublished.has(`${pkg.name}@${pkg.version}`),
            githubReleaseExists: githubReleases.has(tagName),
          }),
        },
      );

      expect(records.map((record) => record.tag)).toEqual(['@scope/a@1.1.0', '@scope/b@1.0.0', '@scope/a@1.0.0']);
      const pending = pendingReleaseTargets(records, head);
      expect(pending.map((target) => target.sha)).toEqual([first, second]);
      expect(pending[0]?.packages.map((pkg) => pkg.name)).toEqual(['@scope/b']);
      expect(pending[0]?.npmPackages).toEqual([]);
      expect(pending[0]?.githubPackages.map((pkg) => pkg.name)).toEqual(['@scope/b']);
      expect(pending[1]?.npmPackages.map((pkg) => pkg.name)).toEqual(['@scope/a']);
    });
  });

  it('collects release tags from a fetched remote checkout instead of walking local commit history', async () => {
    await withFixtureRepo(async (root) => {
      await writePackage(root, '@scope/a', 'packages/a', '1.0.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'release a']);
      await tag(root, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');
      await git(root, ['init', '--bare', 'remote.git']);
      await git(root, ['remote', 'add', 'origin', join(root, 'remote.git')]);
      await git(root, ['push', 'origin', 'main', '--tags']);
      await $`git clone --branch main ${join(root, 'remote.git')} checkout`.cwd(root).quiet();
      const checkout = join(root, 'checkout');
      await git(checkout, ['fetch', '--tags', 'origin', 'main']);
      const head = await gitOutput(checkout, ['rev-parse', 'HEAD']);

      const records = await collectOwnedReleaseTagRecords(
        [{ name: '@scope/a', projectName: 'a', path: 'packages/a' }],
        head,
        {
          listReleaseTagsByCreatorDate: () => gitReleaseTagsByCreatorDate(checkout),
          isAncestor: (ancestor, descendant) => gitIsAncestor(checkout, ancestor, descendant),
          packageVersionAtRef: (packagePath, ref) => packageVersionAtRef(checkout, packagePath, ref),
          durableTagState: async () => ({ npmPublished: false, githubReleaseExists: false }),
        },
      );

      expect(records.map((record) => record.tag)).toEqual(['@scope/a@1.0.0']);
      expect(pendingReleaseTargets(records, 'not-head').map((target) => target.sha)).toEqual([head]);
    });
  });

  it('runs real Nx builds for comma-separated npm-missing package projects', async () => {
    await withFixtureRepo(async (root) => {
      await writeWorkspace(root);
      await writeBuildablePackage(root, '@scope/a', 'packages/a');
      await writeBuildablePackage(root, '@scope/b', 'packages/b');

      await $`nx run-many -t build --projects=${'a,b'}`.cwd(root).quiet();

      await expect(readFile(join(root, 'packages/a/dist/index.js'), 'utf8')).resolves.toBe('{}\n');
      await expect(readFile(join(root, 'packages/b/dist/index.js'), 'utf8')).resolves.toBe('{}\n');
    });
  });

  it('repairs multiple fetched remote targets from a runner clone with real git checkout and Nx build', async () => {
    await withFixtureRepo(async (author) => {
      await writeWorkspace(author);
      await writeBuildablePackage(author, '@scope/a', 'packages/a', '1.0.0');
      await writeBuildablePackage(author, '@scope/b', 'packages/b', '1.0.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'initial release']);
      await tag(author, '@scope/a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(author, '@scope/b@1.0.0', '2025-01-01T00:00:01Z');

      await writeBuildablePackage(author, '@scope/a', 'packages/a', '1.1.0');
      await git(author, ['add', 'packages/a/package.json']);
      await git(author, ['commit', '-m', 'release a 1.1.0']);
      const githubOnlySha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, '@scope/a@1.1.0', '2025-01-02T00:00:00Z');

      await writeBuildablePackage(author, '@scope/b', 'packages/b', '2.0.0-beta.1');
      await git(author, ['add', 'packages/b/package.json']);
      await git(author, ['commit', '-m', 'release b prerelease']);
      const npmAndGithubSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, '@scope/b@2.0.0-beta.1', '2025-01-03T00:00:00Z');

      await writeBuildablePackage(author, '@scope/a', 'packages/a', '1.2.0');
      await git(author, ['add', 'packages/a/package.json']);
      await git(author, ['commit', '-m', 'head release a 1.2.0']);
      const headSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, '@scope/a@1.2.0', '2025-01-04T00:00:00Z');

      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main', '--tags']);
      await $`git clone --branch main ${join(author, 'remote.git')} runner`.cwd(author).quiet();
      const runner = join(author, 'runner');
      await git(runner, ['config', 'user.name', 'Test User']);
      await git(runner, ['config', 'user.email', 'test@example.com']);
      await git(runner, ['fetch', '--tags', 'origin', 'main']);
      const restoreRef = 'origin/main';
      const packages = releaseFixturePackages();
      const npmPublished = new Set(['@scope/a@1.0.0', '@scope/b@1.0.0', '@scope/a@1.1.0']);
      const githubReleases = new Set(['@scope/a@1.0.0', '@scope/b@1.0.0']);

      const records = await collectOwnedReleaseTagRecords(packages, restoreRef, {
        listReleaseTagsByCreatorDate: () => gitReleaseTagsByCreatorDate(runner),
        isAncestor: (ancestor, descendant) => gitIsAncestor(runner, ancestor, descendant),
        packageVersionAtRef: (packagePath, ref) => packageVersionAtRef(runner, packagePath, ref),
        durableTagState: async (pkg, tagName) => ({
          npmPublished: npmPublished.has(`${pkg.name}@${pkg.version}`),
          githubReleaseExists: githubReleases.has(tagName),
        }),
      });
      const pending = pendingReleaseTargets(records, headSha);

      expect(pending.map((target) => target.sha)).toEqual([githubOnlySha, npmAndGithubSha]);
      expect(pending[0]?.npmPackages).toEqual([]);
      expect(pending[0]?.githubPackages.map((pkg) => `${pkg.name}@${pkg.version}`)).toEqual(['@scope/a@1.1.0']);
      expect(pending[1]?.npmPackages.map((pkg) => `${pkg.name}@${pkg.version}`)).toEqual(['@scope/b@2.0.0-beta.1']);
      expect(pending[1]?.githubPackages.map((pkg) => `${pkg.name}@${pkg.version}`)).toEqual(['@scope/b@2.0.0-beta.1']);

      const shell = new LocalGitRepairShell(runner);
      const summaries = await repairPendingTargets(shell, pending, restoreRef, false);

      expect(shell.checkouts).toEqual([githubOnlySha, npmAndGithubSha, restoreRef]);
      expect(shell.devenvLoads).toBe(2);
      expect(shell.builds).toEqual([['@scope/b']]);
      expect(shell.publishes).toEqual([{ name: '@scope/b', version: '2.0.0-beta.1', distTag: 'next', dryRun: false }]);
      expect(shell.githubCreates).toEqual([
        { name: '@scope/a', version: '1.1.0', dryRun: false },
        { name: '@scope/b', version: '2.0.0-beta.1', dryRun: false },
      ]);
      expect(shell.pushes).toEqual([['@scope/a@1.1.0'], ['@scope/b@2.0.0-beta.1']]);
      expect(summaries.map((summary) => summary.sha)).toEqual([githubOnlySha, npmAndGithubSha]);
      await expect(readFile(join(runner, 'packages/b/dist/index.js'), 'utf8')).resolves.toBe('{}\n');
      await expect(readFile(join(runner, 'packages/a/dist/index.js'), 'utf8')).rejects.toThrow();
    });
  });

  it('pushes current release refs to a local bare remote and another clone can fetch them', async () => {
    await withFixtureRepo(async (author) => {
      await writeWorkspace(author);
      await writeBuildablePackage(author, '@scope/pushed', 'packages/pushed', '1.0.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'release pushed package']);
      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main']);
      await $`git clone --branch main ${join(author, 'remote.git')} runner`.cwd(author).quiet();
      const runner = join(author, 'runner');
      await git(runner, ['config', 'user.name', 'Test User']);
      await git(runner, ['config', 'user.email', 'test@example.com']);
      const pkg: ReleasePackageInfo = {
        name: '@scope/pushed',
        projectName: 'pushed',
        path: 'packages/pushed',
        version: '1.0.0',
      };
      const shell = new LocalGitRepairShell(runner);

      const summary = await completeReleaseAtHead(shell, [pkg], false, false);

      expect(summary.pushed).toBe(true);
      expect(shell.pushes).toEqual([['@scope/pushed@1.0.0']]);
      await $`git clone --branch main ${join(author, 'remote.git')} auditor`.cwd(author).quiet();
      const auditor = join(author, 'auditor');
      await git(auditor, ['fetch', '--tags', 'origin', 'main']);
      await expect(gitOutput(auditor, ['rev-parse', 'refs/tags/@scope/pushed@1.0.0^{}'])).resolves.toBe(
        await gitOutput(runner, ['rev-parse', 'HEAD']),
      );
    });
  });
});

function releaseFixturePackages(): ReleasePackageInfo[] {
  return [
    { name: '@scope/a', projectName: 'a', path: 'packages/a', version: '0.0.0' },
    { name: '@scope/b', projectName: 'b', path: 'packages/b', version: '0.0.0' },
  ];
}

class LocalGitRepairShell implements ReleaseRepairShell<ReleasePackageInfo> {
  readonly checkouts: string[] = [];
  readonly pushes: string[][] = [];
  readonly builds: string[][] = [];
  readonly publishes: Array<{ name: string; version: string; distTag: string; dryRun: boolean }> = [];
  readonly githubCreates: Array<{ name: string; version: string; dryRun: boolean }> = [];
  devenvLoads = 0;

  constructor(private readonly root: string) {}

  async gitHead(): Promise<string> {
    return gitOutput(this.root, ['rev-parse', 'HEAD']);
  }

  async pushReleaseRefs(packages: ReleasePackageInfo[]): Promise<boolean> {
    this.pushes.push(packages.map((pkg) => `${pkg.name}@${pkg.version}`));
    await git(this.root, ['fetch', '--tags', 'origin', 'main']);
    for (const pkg of packages) {
      await this.ensureLocalReleaseTag(pkg);
    }
    const refspecs: string[] = [];
    const remoteRefExists = await gitSucceeds(this.root, ['rev-parse', '--verify', 'origin/main']);
    if (!remoteRefExists || !(await gitIsAncestor(this.root, await this.gitHead(), 'origin/main'))) {
      refspecs.push('HEAD:refs/heads/main');
    }
    for (const pkg of packages) {
      const tagRef = `refs/tags/${releaseTag(pkg)}`;
      if (!(await gitSucceeds(this.root, ['ls-remote', '--exit-code', '--tags', 'origin', tagRef]))) {
        refspecs.push(`${tagRef}:${tagRef}`);
      }
    }
    if (refspecs.length === 0) {
      return false;
    }
    await git(this.root, ['push', '--atomic', 'origin', ...refspecs]);
    return true;
  }

  async listNpmMissingPackages(): Promise<ReleasePackageInfo[]> {
    return [];
  }

  async buildReleaseCandidate(packages: ReleasePackageInfo[]): Promise<void> {
    this.builds.push(packages.map((pkg) => pkg.name));
    await $`nx run-many -t build --projects=${packages.map((pkg) => pkg.projectName).join(',')}`.cwd(this.root).quiet();
  }

  async publishPackage(pkg: ReleasePackageInfo, distTag: string, dryRun: boolean): Promise<void> {
    this.publishes.push({ name: pkg.name, version: pkg.version, distTag, dryRun });
  }

  async listGithubMissingPackages(): Promise<ReleasePackageInfo[]> {
    return [];
  }

  async createGithubRelease(pkg: ReleasePackageInfo, dryRun: boolean): Promise<void> {
    this.githubCreates.push({ name: pkg.name, version: pkg.version, dryRun });
  }

  async checkout(ref: string): Promise<void> {
    await git(this.root, ['switch', '--detach', ref]);
    this.checkouts.push(ref);
  }

  async withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T> {
    this.devenvLoads += 1;
    return runWithEnv();
  }

  private async ensureLocalReleaseTag(pkg: ReleasePackageInfo): Promise<void> {
    const tagName = releaseTag(pkg);
    if (await gitSucceeds(this.root, ['rev-parse', '--verify', `refs/tags/${tagName}`])) {
      return;
    }
    await git(this.root, ['tag', '-a', tagName, '-m', tagName, 'HEAD']);
  }
}

async function gitSucceeds(root: string, args: string[]): Promise<boolean> {
  const result = await $`git ${args}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}
