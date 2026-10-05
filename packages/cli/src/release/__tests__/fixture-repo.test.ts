import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  nxDaemonProcesses,
  pidsWorkingIn,
  processTable,
  reclaimDeadFixtureRuns,
} from '@smoothbricks/nx-plugin/testing';
import {
  classifyReleaseBranchPush,
  collectOwnedReleaseTagRecords,
  divergedReleaseBranchMessage,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  releaseTag,
} from '../core.js';
import { completeReleaseAtHead, type ReleaseRepairShell, repairPendingTargets } from '../orchestration.js';
import {
  git,
  gitIsAncestor,
  gitOutput,
  gitReleaseTagsByCreatorDate,
  gitSucceeds,
  packageVersionAtRef,
  runFixtureNx,
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
      await tag(root, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(root, 'b@1.0.0', '2025-01-01T00:00:01Z');

      await writePackage(root, '@scope/a', 'packages/a', '1.1.0');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'second release']);
      const second = await gitOutput(root, ['rev-parse', 'HEAD']);
      await tag(root, 'a@1.1.0', '2025-01-02T00:00:00Z');

      await writeFile(join(root, 'readme.md'), 'not a release\n');
      await git(root, ['add', '.']);
      await git(root, ['commit', '-m', 'work after release']);
      const head = await gitOutput(root, ['rev-parse', 'HEAD']);
      await tag(root, 'not-owned@9.9.9', '2025-01-03T00:00:00Z');

      const npmPublished = new Set(['@scope/a@1.0.0', '@scope/b@1.0.0']);
      const githubReleases = new Set(['a@1.0.0']);
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

      expect(records.map((record) => record.tag)).toEqual(['a@1.1.0', 'b@1.0.0', 'a@1.0.0']);
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
      await tag(root, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await git(root, ['init', '--bare', 'remote.git']);
      await git(root, ['remote', 'add', 'origin', join(root, 'remote.git')]);
      await git(root, ['push', 'origin', 'main', '--tags']);
      await git(root, ['clone', '--branch', 'main', join(root, 'remote.git'), 'checkout']);
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

      expect(records.map((record) => record.tag)).toEqual(['a@1.0.0']);
      expect(pendingReleaseTargets(records, 'not-head').map((target) => target.sha)).toEqual([head]);
    });
  });

  it('runs real Nx builds for comma-separated npm-missing package projects', async () => {
    await withFixtureRepo(async (root) => {
      await writeWorkspace(root);
      await writeBuildablePackage(root, '@scope/a', 'packages/a');
      await writeBuildablePackage(root, '@scope/b', 'packages/b');

      await runFixtureNx(root, ['run-many', '-t', 'build', '--projects=a,b']);

      await expect(readFile(join(root, 'packages/a/dist/index.js'), 'utf8')).resolves.toBe('{}\n');
      await expect(readFile(join(root, 'packages/b/dist/index.js'), 'utf8')).resolves.toBe('{}\n');
    });
  });

  it('keeps fixture Nx state out of a shared workspace-data directory', async () => {
    // Host CI runners export these to a per-lane tree shared by every task in the
    // run. A fixture inheriting them overwrote the real workspace's project graph
    // with its own one-project graph, and the concurrent `nx run-many -t test`
    // died with "Could not find project lmao-ttsc". Assert the outer directories
    // stay untouched, which is the only thing that made that failure possible.
    await withFixtureRepo(async (root) => {
      const shared = await mkdtemp(join(tmpdir(), 'nx-shared-'));
      const sharedData = join(shared, 'workspace-data');
      const sharedCache = join(shared, 'cache');
      const inherited = {
        NX_WORKSPACE_ROOT_PATH: process.env.NX_WORKSPACE_ROOT_PATH,
        NX_WORKSPACE_DATA_DIRECTORY: process.env.NX_WORKSPACE_DATA_DIRECTORY,
        NX_CACHE_DIRECTORY: process.env.NX_CACHE_DIRECTORY,
      };
      process.env.NX_WORKSPACE_ROOT_PATH = shared;
      await mkdir(sharedData, { recursive: true });
      await mkdir(sharedCache, { recursive: true });
      process.env.NX_WORKSPACE_DATA_DIRECTORY = sharedData;
      process.env.NX_CACHE_DIRECTORY = sharedCache;
      try {
        await writeWorkspace(root);
        await writeBuildablePackage(root, '@scope/a', 'packages/a');

        await runFixtureNx(root, ['run-many', '-t', 'build', '--projects=a']);

        expect(await readdir(sharedData)).toEqual([]);
        expect(await readdir(sharedCache)).toEqual([]);
        expect(await readdir(join(root, '.nx'))).toContain('workspace-data');
      } finally {
        for (const [key, value] of Object.entries(inherited)) {
          if (value === undefined) delete process.env[key];
          else process.env[key] = value;
        }
        await rm(shared, { recursive: true, force: true });
      }
    });
  });

  it('leaves no process of its Nx daemon running once the fixture retires', async () => {
    let fixtureRoot = '';
    let daemonPids: number[] = [];
    await withFixtureRepo(async (root) => {
      fixtureRoot = root;
      await writeWorkspace(root);
      await writeBuildablePackage(root, '@scope/a', 'packages/a');
      await runFixtureNx(root, ['show', 'projects'], { daemon: true });
      daemonPids = (await nxDaemonProcesses(root)).map((entry) => entry.pid);
    });

    // The daemon and its plugin workers; fewer would prove nothing.
    expect(daemonPids.length).toBeGreaterThan(1);
    expect(await survivors(fixtureRoot, daemonPids)).toEqual([]);
  });

  it('leaves no process of its Nx daemon running once a fixture body throws', async () => {
    let fixtureRoot = '';
    let daemonPids: number[] = [];
    const thrown = withFixtureRepo(async (root) => {
      fixtureRoot = root;
      await writeWorkspace(root);
      await writeBuildablePackage(root, '@scope/a', 'packages/a');
      await runFixtureNx(root, ['show', 'projects'], { daemon: true });
      daemonPids = (await nxDaemonProcesses(root)).map((entry) => entry.pid);
      throw new Error('the fixture body failed on purpose');
    });

    await expect(thrown).rejects.toThrow('the fixture body failed on purpose');
    expect(daemonPids.length).toBeGreaterThan(1);
    expect(await survivors(fixtureRoot, daemonPids)).toEqual([]);
  });

  // The two abandoned-fixture tests start a nested `bun test` process and a real
  // daemon: the default 30 s is the budget of one test, not of a second runner in it.
  it('stops the Nx daemon of a fixture whose test finished without its body', async () => {
    const { root, pids } = await abandonFixture(`
      await runFixtureNx(root, ['show', 'projects'], { daemon: true });
      const daemon = await nxDaemonProcesses(root);
      console.log('ABANDONED_FIXTURE_PIDS ' + daemon.map((entry) => entry.pid).join(' '));
    `);

    expect(pids.length).toBeGreaterThan(1);
    expect(await survivors(root, pids)).toEqual([]);
  }, 120_000);

  it('stops an abandoned fixture nx client still starting its daemon, and that daemon', async () => {
    // The client logs this line just before it spawns the daemon, which records
    // itself only once it is up, so retiring now finds no record to stop by and
    // the client lives on in the deleted root. The client is another process
    // and its log the only signal it gives, so the body polls for it. The
    // retirement kills the client, hence the catch.
    const { root } = await abandonFixture(`
      void runFixtureNx(root, ['daemon', '--start'], { daemon: true }).catch(() => {});
      const log = join(root, '.nx', 'workspace-data', 'd', 'daemon.log');
      while (!(existsSync(log) && readFileSync(log, 'utf8').includes('Starting new daemon server in background'))) {
        await Bun.sleep(5);
      }
    `);

    expect(await survivors(root, [])).toEqual([]);
  }, 120_000);

  it('repairs multiple fetched remote targets from a runner clone with real git checkout and Nx build', async () => {
    await withFixtureRepo(async (author) => {
      await writeWorkspace(author);
      await writeBuildablePackage(author, '@scope/a', 'packages/a', '1.0.0');
      await writeBuildablePackage(author, '@scope/b', 'packages/b', '1.0.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'initial release']);
      await tag(author, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(author, 'b@1.0.0', '2025-01-01T00:00:01Z');

      await writeBuildablePackage(author, '@scope/a', 'packages/a', '1.1.0');
      await git(author, ['add', 'packages/a/package.json']);
      await git(author, ['commit', '-m', 'release a 1.1.0']);
      const githubOnlySha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, 'a@1.1.0', '2025-01-02T00:00:00Z');

      await writeBuildablePackage(author, '@scope/b', 'packages/b', '2.0.0-beta.1');
      await git(author, ['add', 'packages/b/package.json']);
      await git(author, ['commit', '-m', 'release b prerelease']);
      const npmAndGithubSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, 'b@2.0.0-beta.1', '2025-01-03T00:00:00Z');

      await writeBuildablePackage(author, '@scope/a', 'packages/a', '1.2.0');
      await git(author, ['add', 'packages/a/package.json']);
      await git(author, ['commit', '-m', 'head release a 1.2.0']);
      const headSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, 'a@1.2.0', '2025-01-04T00:00:00Z');

      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main', '--tags']);
      await git(author, ['clone', '--branch', 'main', join(author, 'remote.git'), 'runner']);
      const runner = join(author, 'runner');
      await git(runner, ['config', 'user.name', 'Test User']);
      await git(runner, ['config', 'user.email', 'test@example.com']);
      await git(runner, ['fetch', '--tags', 'origin', 'main']);
      const restoreRef = 'origin/main';
      const packages = releaseFixturePackages();
      const npmPublished = new Set(['@scope/a@1.0.0', '@scope/b@1.0.0', '@scope/a@1.1.0']);
      const githubReleases = new Set(['a@1.0.0', 'b@1.0.0']);

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
      expect(shell.devenvLoads).toBe(3);
      expect(shell.devenvRefs).toEqual([githubOnlySha, npmAndGithubSha, headSha]);
      await expect(readFile(join(runner, '.generated-tool-ref'), 'utf8')).resolves.toBe(`${headSha}\n`);
      expect(shell.builds).toEqual([['@scope/b']]);
      expect(shell.publishes).toEqual([{ name: '@scope/b', version: '2.0.0-beta.1', distTag: 'next', dryRun: false }]);
      expect(shell.githubCreates).toEqual([
        { name: '@scope/a', version: '1.1.0', dryRun: false },
        { name: '@scope/b', version: '2.0.0-beta.1', dryRun: false },
      ]);
      expect(shell.pushes).toEqual([['a@1.1.0'], ['b@2.0.0-beta.1']]);
      expect(summaries.map((summary) => summary.sha)).toEqual([githubOnlySha, npmAndGithubSha]);
      await expect(readFile(join(runner, 'packages/b/dist/index.js'), 'utf8')).resolves.toBe('{}\n');
      await expect(readFile(join(runner, 'packages/a/dist/index.js'), 'utf8')).rejects.toThrow();
    });
  });

  it('pushes current release refs to a local bare remote', async () => {
    await withFixtureRepo(async (author) => {
      await writeWorkspace(author);
      await writeBuildablePackage(author, '@scope/pushed', 'packages/pushed', '1.0.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'release pushed package']);
      const releaseSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main']);
      const pkg: ReleasePackageInfo = {
        name: '@scope/pushed',
        projectName: 'pushed',
        path: 'packages/pushed',
        version: '1.0.0',
      };
      const shell = new LocalGitRepairShell(author);

      const summary = await completeReleaseAtHead(shell, [pkg], false, false);

      expect(summary.pushed).toBe(true);
      expect(shell.pushes).toEqual([['pushed@1.0.0']]);
      const remoteTags = await gitOutput(author, [
        'ls-remote',
        '--tags',
        'origin',
        'refs/tags/pushed@1.0.0^{}',
        'refs/tags/@scope/pushed@1.0.0^{}',
      ]);
      expect(remoteTags).toBe(`${releaseSha}\trefs/tags/pushed@1.0.0^{}`);
    });
  });

  it('kills timed-out fixture Git process groups so descendants cannot hold output pipes open', async () => {
    await withFixtureRepo(async (root) => {
      await expect(
        git(root, ['-c', "alias.hold-pipes=!sh -c 'sleep 30 & exit 0'", 'hold-pipes'], undefined, 100),
      ).rejects.toThrow('timed out after 100ms');
    });
  }, 5_000);

  it('repairs a scoped package from its project-name release tag without creating a package-name tag', async () => {
    await withFixtureRepo(async (author) => {
      await writeWorkspace(author);
      await writeBuildablePackage(author, '@scope/cli', 'packages/cli', '0.2.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'release cli 0.2.0']);
      const releaseSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, 'cli@0.2.0', '2025-01-01T00:00:00Z');

      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main', '--tags']);
      await git(author, ['clone', '--branch', 'main', join(author, 'remote.git'), 'runner']);
      const runner = join(author, 'runner');
      await git(runner, ['config', 'user.name', 'Test User']);
      await git(runner, ['config', 'user.email', 'test@example.com']);
      await git(runner, ['fetch', '--tags', 'origin', 'main']);

      const pkg: ReleasePackageInfo = {
        name: '@scope/cli',
        projectName: 'cli',
        path: 'packages/cli',
        version: '0.0.0',
      };
      const records = await collectOwnedReleaseTagRecords([pkg], 'origin/main', {
        listReleaseTagsByCreatorDate: () => gitReleaseTagsByCreatorDate(runner),
        isAncestor: (ancestor, descendant) => gitIsAncestor(runner, ancestor, descendant),
        packageVersionAtRef: (packagePath, ref) => packageVersionAtRef(runner, packagePath, ref),
        durableTagState: async () => ({ npmPublished: false, githubReleaseExists: false }),
      });
      const pending = pendingReleaseTargets(records, 'not-head');

      const shell = new LocalGitRepairShell(runner);
      const summaries = await repairPendingTargets(shell, pending, 'origin/main', false);

      expect(records.map((record) => record.tag)).toEqual(['cli@0.2.0']);
      expect(pending.map((target) => target.sha)).toEqual([releaseSha]);
      expect(shell.pushes).toEqual([['cli@0.2.0']]);
      expect(shell.publishes).toEqual([{ name: '@scope/cli', version: '0.2.0', distTag: 'latest', dryRun: false }]);
      expect(shell.githubCreates).toEqual([{ name: '@scope/cli', version: '0.2.0', dryRun: false }]);
      expect(summaries[0]?.packages.map((releasePackage) => releasePackage.name)).toEqual(['@scope/cli']);
      await expect(gitSucceeds(runner, ['rev-parse', '--verify', 'refs/tags/cli@0.2.0'])).resolves.toBe(true);
      await expect(gitSucceeds(runner, ['rev-parse', '--verify', 'refs/tags/@scope/cli@0.2.0'])).resolves.toBe(false);
    });
  });

  it('refuses a release whose branch diverged mid-run instead of dying on raw git output', async () => {
    await withFixtureRepo(async (author) => {
      await writePackage(author, '@scope/cli', 'packages/cli', '0.1.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'initial']);
      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main']);
      await git(author, ['clone', '--branch', 'main', join(author, 'remote.git'), 'runner']);
      const runner = join(author, 'runner');
      await git(runner, ['config', 'user.name', 'Test User']);
      await git(runner, ['config', 'user.email', 'test@example.com']);

      // The release job's own version bump: a commit that exists only in this job.
      await writePackage(runner, '@scope/cli', 'packages/cli', '0.2.0');
      await git(runner, ['add', '.']);
      await git(runner, ['commit', '-m', 'chore(release): publish']);
      const releaseSha = await gitOutput(runner, ['rev-parse', 'HEAD']);

      // An unrelated push lands on the release branch while the candidate builds.
      await writeFile(join(author, 'readme.md'), 'concurrent work\n');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'unrelated work']);
      await git(author, ['push', 'origin', 'main']);
      const remoteSha = await gitOutput(author, ['rev-parse', 'HEAD']);

      const pkg: ReleasePackageInfo = {
        name: '@scope/cli',
        projectName: 'cli',
        path: 'packages/cli',
        version: '0.2.0',
      };
      const shell = new LocalGitRepairShell(runner);

      expect(releaseSha).not.toBe(remoteSha);
      await expect(shell.pushReleaseRefs([pkg])).rejects.toThrow(
        new RegExp(
          `diverged: origin/main is at ${remoteSha},.+release commit history at ${releaseSha}.+Nothing was published.+Re-run the release workflow`,
          's',
        ),
      );
      // The atomic ref push is the release transaction boundary: a refused release
      // leaves no tag behind for npm publishing to be reconciled against.
      await expect(
        gitSucceeds(runner, ['ls-remote', '--exit-code', '--tags', 'origin', 'refs/tags/cli@0.2.0']),
      ).resolves.toBe(false);
    });
  });
});

/**
 * Run a test file whose single test returns while its fixture body is still
 * running, once `body` has run in that fixture. Bun ends a timed-out test
 * without cancelling its body, and the run can exit before that body returns;
 * a test that stops awaiting its fixture reaches the same state without
 * waiting out a deadline. `body` sees `root` and may print the pids to check
 * as `ABANDONED_FIXTURE_PIDS`.
 */
async function abandonFixture(body: string): Promise<{ root: string; pids: number[] }> {
  const helper = JSON.stringify(join(import.meta.dir, 'helpers', 'fixture-repo.ts'));
  // The test file lives outside this package, where the package name does not resolve.
  const testing = JSON.stringify(Bun.resolveSync('@smoothbricks/nx-plugin/testing', import.meta.dir));
  const scratch = await realpath(await mkdtemp(join(tmpdir(), 'smoo-abandoned-fixture-')));
  try {
    const testFile = join(scratch, 'abandoned.test.ts');
    await writeFile(
      testFile,
      `import { test } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { nxDaemonProcesses } from ${testing};
import { runFixtureNx, withFixtureRepo, writeBuildablePackage, writeWorkspace } from ${helper};

test('returns without its fixture body', async () => {
  const started = Promise.withResolvers();
  void withFixtureRepo(async (root) => {
    await writeWorkspace(root);
    await writeBuildablePackage(root, '@scope/a', 'packages/a');
    ${body}
    console.log('ABANDONED_FIXTURE_ROOT ' + root);
    started.resolve();
    await Promise.withResolvers().promise;
  });
  await started.promise;
});
`,
    );
    const run = Bun.spawn([process.execPath, 'test', testFile], {
      cwd: join(import.meta.dir, '..', '..', '..'),
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      run.exited,
      new Response(run.stdout).text(),
      new Response(run.stderr).text(),
    ]);
    const root = /^ABANDONED_FIXTURE_ROOT (.+)$/m.exec(stdout)?.[1];
    if (exitCode !== 0 || root === undefined) {
      // A nested run killed before its fixture retired (Bun SIGTERMs it when
      // this test times out) leaves its daemon behind: reclaim it now, not at
      // the next run. Only then: a run that exited cleanly must have retired
      // its own fixture, which is what the caller asserts.
      await reclaimDeadFixtureRuns('cli');
      throw new Error(`abandoned fixture test exited ${exitCode}\n${stdout}\n${stderr}`);
    }
    const pids = /^ABANDONED_FIXTURE_PIDS (.+)$/m.exec(stdout)?.[1]?.split(' ').map(Number) ?? [];
    return { root, pids };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

/**
 * What still runs of `pids`, and every process of this user whose working
 * directory lies in `root`.
 */
async function survivors(root: string, pids: readonly number[]): Promise<string[]> {
  const running = (await processTable())
    .filter((entry) => !entry.stat.startsWith('Z') && pids.includes(entry.pid))
    .map((entry) => `${entry.pid} ${entry.command}`);
  const working = (await pidsWorkingIn(root)).map((pid) => `${pid} works in ${root}`);
  return [...running, ...working];
}

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
  readonly devenvRefs: string[] = [];

  constructor(private readonly root: string) {}

  async gitHead(): Promise<string> {
    return gitOutput(this.root, ['rev-parse', 'HEAD']);
  }

  async pushReleaseRefs(packages: ReleasePackageInfo[]): Promise<boolean> {
    this.pushes.push(packages.map((pkg) => releaseTag(pkg)));
    await git(this.root, ['fetch', '--tags', 'origin', 'main']);
    for (const pkg of packages) {
      await this.ensureLocalReleaseTag(pkg);
    }
    const refspecs: string[] = [];
    const remoteExists = await gitSucceeds(this.root, ['rev-parse', '--verify', 'origin/main']);
    const head = await this.gitHead();
    const branchPush = classifyReleaseBranchPush({
      remoteExists,
      headIsAncestorOfRemote: remoteExists && (await gitIsAncestor(this.root, head, 'origin/main')),
      remoteIsAncestorOfHead: remoteExists && (await gitIsAncestor(this.root, 'origin/main', head)),
    });
    if (branchPush === 'diverged') {
      throw new Error(
        divergedReleaseBranchMessage({
          branch: 'main',
          remoteRef: 'origin/main',
          head,
          remoteSha: await gitOutput(this.root, ['rev-parse', 'origin/main']),
        }),
      );
    }
    if (branchPush === 'push') {
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
    await runFixtureNx(this.root, [
      'run-many',
      '-t',
      'build',
      `--projects=${packages.map((pkg) => pkg.projectName).join(',')}`,
    ]);
  }

  async publishPackage(pkg: ReleasePackageInfo, distTag: string, dryRun: boolean): Promise<void> {
    this.publishes.push({ name: pkg.name, version: pkg.version, distTag, dryRun });
  }

  async listGithubMissingPackages(): Promise<ReleasePackageInfo[]> {
    return [];
  }

  async createGithubRelease(pkg: ReleasePackageInfo, dryRun: boolean): Promise<string | null> {
    this.githubCreates.push({ name: pkg.name, version: pkg.version, dryRun });
    return dryRun ? null : `https://github.test/${releaseTag(pkg)}`;
  }

  async checkout(ref: string): Promise<void> {
    await git(this.root, ['switch', '--detach', ref]);
    this.checkouts.push(ref);
  }

  async withDevenvEnv<T>(runWithEnv: () => Promise<T>): Promise<T> {
    this.devenvLoads += 1;
    const currentRef = await this.gitHead();
    this.devenvRefs.push(currentRef);
    await writeFile(join(this.root, '.generated-tool-ref'), `${currentRef}\n`);
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
