import { describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  fixtureSocketDirectory,
  nxDaemonProcesses,
  ownedFixtureRoot,
  processesOf,
  publishFixtureSocketDirectory,
  reclaimDeadFixtureRuns,
  retireFixtureSocketDirectory,
} from '@smoothbricks/nx-plugin/testing';
import {
  classifyReleaseBranchPush,
  collectOwnedReleaseTagRecords,
  divergedReleaseBranchMessage,
  pendingReleaseTargets,
  type ReleasePackageInfo,
  releaseTag,
} from '../core.js';
import {
  dispatchRelease,
  type PublishRun,
  type ReleaseDispatchOptions,
  type ReleaseDispatchShell,
} from '../dispatch.js';
import { completeReleaseAtHead, type ReleaseRepairShell, repairPendingTargets } from '../orchestration.js';
import {
  FIXTURE_DAEMON_MARKER,
  FIXTURE_STARTS_ITS_OWN_DAEMON,
  type FixtureNxOptions,
  fixtureDaemonOwnedBy,
  fixtureNxEnv,
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

  it('keeps fixture Nx state out of a shared workspace-data directory', async () => {
    // Host CI runners export these to a per-lane tree shared by every task in the run. A fixture
    // Nx inheriting any of them works in, or overwrites, another workspace's state.
    const shared = '/runner/lane/nx';
    const root = await mkdtemp(join(tmpdir(), 'smoo-release-env-'));
    const inherited = {
      NX_WORKSPACE_ROOT_PATH: shared,
      NX_WORKSPACE_DATA_DIRECTORY: join(shared, 'workspace-data'),
      NX_CACHE_DIRECTORY: join(shared, 'cache'),
      NX_SOCKET_DIR: join(shared, 'socket'),
      NX_DAEMON_SOCKET_DIR: join(shared, 'socket'),
    };
    // Named before the inherited variables are planted: the fixture's directory is its own, not derived from them.
    const socketDirectory = fixtureSocketDirectory(root);
    const outer = Object.fromEntries(
      [...Object.keys(inherited), 'NX_DAEMON_START', FIXTURE_DAEMON_MARKER].map((key) => [key, process.env[key]]),
    );
    // As every cowshed job runs, plus a marker a fixture must never inherit.
    Object.assign(process.env, inherited, { NX_DAEMON_START: 'never', [FIXTURE_DAEMON_MARKER]: shared });
    try {
      for (const [options, daemon] of [
        [{ daemon: 'off' }, { NX_DAEMON: 'false' }],
        [{ daemon: 'job-rule' }, { NX_DAEMON: 'true', NX_DAEMON_START: 'never' }],
        [FIXTURE_STARTS_ITS_OWN_DAEMON, { NX_DAEMON: 'true', [FIXTURE_DAEMON_MARKER]: root }],
      ] satisfies [FixtureNxOptions, Record<string, string>][]) {
        const env = fixtureNxEnv(root, options, socketDirectory);
        expect(Object.entries(env).filter(([, value]) => value.includes(shared))).toEqual([]);
        expect(env).toMatchObject({
          ...daemon,
          NX_WORKSPACE_ROOT_PATH: root,
          NX_SOCKET_DIR: socketDirectory,
          NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx', 'workspace-data'),
          NX_CACHE_DIRECTORY: join(root, '.nx', 'cache'),
        });
        // The escape lifts the job's rule for its one child alone, never for anything else. A fixture `nx` without
        // it runs under `NX_DAEMON_START=never`, which the patched Nx honours by starting no daemon
        // (`packages/nx-plugin/src/nx-daemon-start-never.test.ts` owns that half).
        expect('NX_DAEMON_START' in env).toBe(options.daemon !== 'fixture-owned');
      }
      expect(process.env.NX_DAEMON_START).toBe('never');
    } finally {
      for (const [key, value] of Object.entries(outer)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      await rm(root, { recursive: true, force: true });
    }
  });

  it("retires only the fixture workspace's own socket alias, leaving another workspace's intact", async () => {
    // A bystander: an owned workspace in the same socket namespace, with its own alias and a real socket directory.
    // None of it is this fixture's to remove, and retiring the fixture must leave all of it in place.
    const bystander = await ownedFixtureRoot(tmpdir(), 'cli', 'smoo-release-bystander-');
    try {
      const bystanderSocketDirectory = await publishFixtureSocketDirectory(bystander);
      await mkdir(bystanderSocketDirectory);
      await writeFile(join(bystanderSocketDirectory, 'kept'), 'kept\n');
      const bystanderAlias = dirname(bystanderSocketDirectory);

      let fixtureRoot = '';
      let socketDirectory = '';
      let alias = '';
      let physicalDirectory = '';
      await withFixtureRepo(async (root) => {
        fixtureRoot = await realpath(root);
        socketDirectory = await publishFixtureSocketDirectory(root);
        alias = dirname(socketDirectory);
        await mkdir(socketDirectory);
        physicalDirectory = await realpath(socketDirectory);
      });

      // The fixture's leaf was a real directory inside its own root, reached through the alias it retired.
      expect(physicalDirectory).toBe(join(fixtureRoot, '.nx', 'sockets', 'nx'));
      expect(existsSync(socketDirectory)).toBe(false);
      expect(await entryExists(alias)).toBe(false);
      expect(existsSync(physicalDirectory)).toBe(false);

      // The bystander's alias still names its own sockets directory, and its leaf still holds what it held.
      const bystanderRoot = await realpath(bystander);
      expect(await entryExists(bystanderAlias)).toBe(true);
      expect(await realpath(bystanderAlias)).toBe(join(bystanderRoot, '.nx', 'sockets'));
      expect(existsSync(bystanderSocketDirectory)).toBe(true);
      expect(await realpath(bystanderSocketDirectory)).toBe(join(bystanderRoot, '.nx', 'sockets', 'nx'));
      expect(await readFile(join(bystanderSocketDirectory, 'kept'), 'utf8')).toBe('kept\n');
    } finally {
      await retireFixtureSocketDirectory(bystander);
      await rm(bystander, { recursive: true, force: true });
    }
  });

  it('leaves no process of its Nx daemon running once the fixture retires, though its body threw', async () => {
    let daemonPids: number[] = [];
    let daemonOwned = false;
    // The body fails with its daemon up; a body that returns retires through the same finally.
    const thrown = withFixtureRepo(async (root) => {
      await writeWorkspace(root);
      await writeBuildablePackage(root, '@scope/a', 'packages/a');
      await runFixtureNx(root, ['show', 'projects'], FIXTURE_STARTS_ITS_OWN_DAEMON);
      daemonPids = (await nxDaemonProcesses(root)).map((entry) => entry.pid);
      // The fixture's own daemon, by the marker its escaped client started it with.
      daemonOwned = daemonPids[0] !== undefined && (await fixtureDaemonOwnedBy(daemonPids[0], root));
      throw new Error('the fixture body failed on purpose');
    });

    await expect(thrown).rejects.toThrow('the fixture body failed on purpose');
    // Ownership proves a live fixture daemon existed; Nx may run its plugins in-process.
    expect(daemonOwned).toBe(true);
    expect(await survivors(daemonPids)).toEqual([]);
  });

  it('stops the Nx daemon of a fixture whose test finished without its body', async () => {
    const { pids } = await abandonFixture(`
      await runFixtureNx(root, ['show', 'projects'], FIXTURE_STARTS_ITS_OWN_DAEMON);
      const daemon = await nxDaemonProcesses(root);
      if (!(await fixtureDaemonOwnedBy(daemon[0]?.pid ?? 0, root))) throw new Error('not the fixture daemon');
      console.log('ABANDONED_FIXTURE_PIDS ' + daemon.map((entry) => entry.pid).join(' '));
    `);

    expect(await survivors(pids)).toEqual([]);
  });

  it('stops an abandoned fixture nx client still starting its daemon, and that daemon', async () => {
    // The client logs this line just before it spawns the daemon, which records
    // itself only once it is up, so retiring now finds no record to stop by and
    // the client lives on in the deleted root. The client is another process
    // and its log the only signal it gives, so the body polls for it. The
    // retirement kills the client, hence the catch.
    const { pids } = await abandonFixture(`
      void runFixtureNx(root, ['daemon', '--start'], FIXTURE_STARTS_ITS_OWN_DAEMON).catch(() => {});
      const log = join(root, '.nx', 'workspace-data', 'd', 'daemon.log');
      while (!(existsSync(log) && readFileSync(log, 'utf8').includes('Starting new daemon server in background'))) {
        await Bun.sleep(5);
      }
      // What this run started, the client and anything it has spawned: retiring must end each of them.
      const tree = await processTree([process.pid]);
      console.log('ABANDONED_FIXTURE_PIDS ' + tree.filter((entry) => entry.pid !== process.pid).map((entry) => entry.pid).join(' '));
    `);

    expect(pids).not.toEqual([]);
    expect(await survivors(pids)).toEqual([]);
  });

  it('repairs multiple fetched remote targets from a runner clone, building each at its own checkout', async () => {
    await withFixtureRepo(async (author) => {
      await writePackage(author, '@scope/a', 'packages/a', '1.0.0');
      await writePackage(author, '@scope/b', 'packages/b', '1.0.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'initial release']);
      await tag(author, 'a@1.0.0', '2025-01-01T00:00:00Z');
      await tag(author, 'b@1.0.0', '2025-01-01T00:00:01Z');

      await writePackage(author, '@scope/a', 'packages/a', '1.1.0');
      await git(author, ['add', 'packages/a/package.json']);
      await git(author, ['commit', '-m', 'release a 1.1.0']);
      const githubOnlySha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, 'a@1.1.0', '2025-01-02T00:00:00Z');

      await writePackage(author, '@scope/b', 'packages/b', '2.0.0-beta.1');
      await git(author, ['add', 'packages/b/package.json']);
      await git(author, ['commit', '-m', 'release b prerelease']);
      const npmAndGithubSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await tag(author, 'b@2.0.0-beta.1', '2025-01-03T00:00:00Z');

      await writePackage(author, '@scope/a', 'packages/a', '1.2.0');
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
      expect(shell.builds).toEqual([{ head: npmAndGithubSha, packages: ['@scope/b'] }]);
      expect(shell.publishes).toEqual([{ name: '@scope/b', version: '2.0.0-beta.1', distTag: 'next', dryRun: false }]);
      expect(shell.githubCreates).toEqual([
        { name: '@scope/a', version: '1.1.0', dryRun: false },
        { name: '@scope/b', version: '2.0.0-beta.1', dryRun: false },
      ]);
      expect(shell.pushes).toEqual([['a@1.1.0'], ['b@2.0.0-beta.1']]);
      expect(summaries.map((summary) => summary.sha)).toEqual([githubOnlySha, npmAndGithubSha]);
    });
  });

  it('ends a timed-out fixture Git process group, descendants and members forked mid-kill included', async () => {
    await withFixtureRepo(async (root) => {
      // The descendant outlives any run: a broken group kill leaves its pipes open, so the test never returns and the
      // leg's no-progress bound fails the run, rather than passing once a shorter sleep ended on its own.
      await expect(
        git(root, ['-c', "alias.hold-pipes=!sh -c 'sleep 86400 & exit 0'", 'hold-pipes'], undefined, 100),
      ).rejects.toThrow('timed out after 100ms');

      // Eight shell chains, each forking its successor and exiting, keep some member inside fork at
      // almost any instant. One group SIGKILL misses the child a fork in progress makes, and that
      // child keeps the chain, and git's pipes, alive: then the test never returns and the leg's
      // no-progress bound fails the run.
      const chains = "sh -c 'step() { (step) & exit 0; }; for i in 1 2 3 4 5 6 7 8; do (step) & done; wait'";
      await expect(git(root, ['-c', `alias.forks=!${chains}`, 'forks'], undefined, 100)).rejects.toThrow(
        'timed out after 100ms',
      );
    });
  });

  it('repairs a fetched remote checkout from its project-name release tag, creating no package-name tag', async () => {
    await withFixtureRepo(async (author) => {
      await writePackage(author, '@scope/cli', 'packages/cli', '0.2.0');
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
      // The tags come from the fetched remote, not from walking local commit history. The release tip is named by its
      // remote ref or, when the remote has no such ref, by the HEAD commit (fetchReleaseRepairRef); both name it.
      const collect = (tip: string) =>
        collectOwnedReleaseTagRecords([pkg], tip, {
          listReleaseTagsByCreatorDate: () => gitReleaseTagsByCreatorDate(runner),
          isAncestor: (ancestor, descendant) => gitIsAncestor(runner, ancestor, descendant),
          packageVersionAtRef: (packagePath, ref) => packageVersionAtRef(runner, packagePath, ref),
          durableTagState: async () => ({ npmPublished: false, githubReleaseExists: false }),
        });
      const records = await collect('origin/main');
      const recordsAtHead = await collect(releaseSha);
      const pending = pendingReleaseTargets(records, 'not-head');

      const shell = new LocalGitRepairShell(runner);
      const summaries = await repairPendingTargets(shell, pending, 'origin/main', false);

      expect(records.map((record) => record.tag)).toEqual(['cli@0.2.0']);
      expect(recordsAtHead.map((record) => record.tag)).toEqual(['cli@0.2.0']);
      expect(pendingReleaseTargets(recordsAtHead, 'not-head').map((target) => target.sha)).toEqual([releaseSha]);
      expect(pending.map((target) => target.sha)).toEqual([releaseSha]);
      expect(shell.pushes).toEqual([['cli@0.2.0']]);
      expect(shell.builds).toEqual([{ head: releaseSha, packages: ['@scope/cli'] }]);
      expect(shell.publishes).toEqual([{ name: '@scope/cli', version: '0.2.0', distTag: 'latest', dryRun: false }]);
      expect(shell.githubCreates).toEqual([{ name: '@scope/cli', version: '0.2.0', dryRun: false }]);
      expect(summaries[0]?.packages.map((releasePackage) => releasePackage.name)).toEqual(['@scope/cli']);
      await expect(gitSucceeds(runner, ['rev-parse', '--verify', 'refs/tags/cli@0.2.0'])).resolves.toBe(true);
      await expect(gitSucceeds(runner, ['rev-parse', '--verify', 'refs/tags/@scope/cli@0.2.0'])).resolves.toBe(false);
    });
  });

  it('pushes release refs to a local bare remote, then refuses a release whose branch diverged mid-run', async () => {
    await withFixtureRepo(async (author) => {
      await writePackage(author, '@scope/cli', 'packages/cli', '0.1.0');
      await git(author, ['add', '.']);
      await git(author, ['commit', '-m', 'release cli 0.1.0']);
      const firstSha = await gitOutput(author, ['rev-parse', 'HEAD']);
      await git(author, ['init', '--bare', 'remote.git']);
      await git(author, ['remote', 'add', 'origin', join(author, 'remote.git')]);
      await git(author, ['push', 'origin', 'main']);
      const first: ReleasePackageInfo = {
        name: '@scope/cli',
        projectName: 'cli',
        path: 'packages/cli',
        version: '0.1.0',
      };
      const pushing = new LocalGitRepairShell(author);

      const summary = await completeReleaseAtHead(pushing, [first], false, false);

      expect(summary.pushed).toBe(true);
      expect(pushing.pushes).toEqual([['cli@0.1.0']]);
      const remoteTags = await gitOutput(author, [
        'ls-remote',
        '--tags',
        'origin',
        'refs/tags/cli@0.1.0^{}',
        'refs/tags/@scope/cli@0.1.0^{}',
      ]);
      expect(remoteTags).toBe(`${firstSha}\trefs/tags/cli@0.1.0^{}`);

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

      const next: ReleasePackageInfo = { ...first, version: '0.2.0' };
      const shell = new LocalGitRepairShell(runner);

      expect(releaseSha).not.toBe(remoteSha);
      await expect(shell.pushReleaseRefs([next])).rejects.toThrow(
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

describe('a publish dispatched as the land turn command', () => {
  it('holds the turn until its run ends, so a land queued behind it never moves main under the release push', async () => {
    await withFixtureRepo(async (main) => {
      const remote = await publishFixture(main);
      const turn = new LandTurn();
      const shell = new LocalPublishShell(main, remote);
      // Boxed: a deferred resolved with a promise adopts it, and would wait for the land itself.
      const landAsked = Promise.withResolvers<{ landing: Promise<void> }>();
      // The candidate builds until a land has either moved main or is queued behind the publish's turn.
      shell.buildWindow = async () => {
        await Promise.race([turn.contended(), (await landAsked.promise).landing]);
      };

      const publishing = turn.hold(() => dispatchRelease(shell, publishOptions()));
      // The land asks once the publish is past its dispatch: returned, or holding the turn on its run.
      await Promise.race([publishing, shell.awaiting.promise]);
      const landing = turn.hold(() => landFastForward(main, 'land.md')).then(() => pushMain(main));
      landAsked.resolve({ landing });
      await Promise.all([publishing, landing]);

      expect(await shell.outcome(0)).toEqual({ conclusion: 'success' });
      expect(await gitOutput(remote, ['log', '--format=%s', 'main'])).toBe(
        ['land.md', 'chore(release): publish', 'release cli 0.1.0'].join('\n'),
      );
      expect(await gitOutput(remote, ['rev-parse', 'cli@0.2.0^{commit}'])).toBe(
        await gitOutput(remote, ['rev-parse', 'main~1']),
      );
    });
  });

  it('refuses to dispatch while main holds a landed commit its remote lacks, and releases on top once it is pushed', async () => {
    await withFixtureRepo(async (main) => {
      const remote = await publishFixture(main);
      const turn = new LandTurn();
      const shell = new LocalPublishShell(main, remote);
      // A land fast-forwarded main and ended its turn; its push runs after the turn, detached.
      await landFastForward(main, 'landed.md');
      const landed = await gitOutput(main, ['rev-parse', 'HEAD']);
      const pushed = await gitOutput(remote, ['rev-parse', 'main']);
      shell.buildWindow = () => pushMain(main);

      await expect(turn.hold(() => dispatchRelease(shell, publishOptions()))).rejects.toThrow(
        `Cannot dispatch publish.yml: HEAD is at ${landed.slice(0, 12)}, which origin/main at ${pushed.slice(0, 12)} does not hold.`,
      );
      expect(shell.dispatches).toBe(0);

      await pushMain(main);
      shell.buildWindow = async () => {};
      await turn.hold(() => dispatchRelease(shell, publishOptions()));

      expect(await shell.outcome(0)).toEqual({ conclusion: 'success' });
      expect(await gitOutput(remote, ['log', '--format=%s', 'main'])).toBe(
        ['chore(release): publish', 'landed.md', 'release cli 0.1.0'].join('\n'),
      );
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
 *
 * The nested runner is part of the harness, so it is bounded and configured explicitly. The calling test's bound
 * cannot reach the nested test or its `onTestFinished` retirement, which would otherwise run under Bun's 5 s
 * default; both get the per-test bound every bounded Bun leg declares. It runs from this package, because the
 * fixture's socket namespace is the checkout's (`socketNamespace` reads the working directory), but under an
 * empty config: the nested file imports plain TypeScript and the built nx-plugin, nothing that needs the package
 * preload (the ttsc transform), so it is not loaded. That preload also isolated this process's Nx environment, so
 * the nested run gets the environment as this process holds it now.
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
import { nxDaemonProcesses, processTree } from ${testing};
import {
  FIXTURE_STARTS_ITS_OWN_DAEMON,
  fixtureDaemonOwnedBy,
  runFixtureNx,
  withFixtureRepo,
  writeBuildablePackage,
  writeWorkspace,
} from ${helper};

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
    const config = join(scratch, 'bunfig.toml');
    await writeFile(config, '');
    const run = Bun.spawn([process.execPath, 'test', `--config=${config}`, '--timeout=0', testFile], {
      cwd: join(import.meta.dir, '..', '..', '..'),
      env: process.env,
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
      await reclaimDeadFixtureRuns(tmpdir(), 'cli');
      throw new Error(`abandoned fixture test exited ${exitCode}\n${stdout}\n${stderr}`);
    }
    const pids = /^ABANDONED_FIXTURE_PIDS (.+)$/m.exec(stdout)?.[1]?.split(' ').map(Number) ?? [];
    return { root, pids };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

/** What still runs of `pids`, a zombie counting as exited. */
async function survivors(pids: readonly number[]): Promise<string[]> {
  return (await processesOf(pids))
    .filter((entry) => !entry.stat.startsWith('Z'))
    .map((entry) => `${entry.pid} ${entry.command}`);
}

/**
 * Whether anything exists at `path`, a symlink whose target is gone included.
 * Only ENOENT answers no: any other failure is the test's to report.
 */
async function entryExists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return false;
    }
    throw error;
  }
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
  /** Each build the release asked for, with the commit checked out when it asked. */
  readonly builds: Array<{ head: string; packages: string[] }> = [];
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
    this.builds.push({ head: await this.gitHead(), packages: packages.map((pkg) => pkg.name) });
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

/** The fixture's host main: one package at 0.1.0, pushed to a bare remote the publish runs clone. */
async function publishFixture(main: string): Promise<string> {
  await writePackage(main, '@scope/cli', 'packages/cli', '0.1.0');
  await git(main, ['add', 'packages']);
  await git(main, ['commit', '-m', 'release cli 0.1.0']);
  const remote = join(main, 'remote.git');
  await git(main, ['init', '--bare', '-b', 'main', remote]);
  await git(main, ['remote', 'add', 'origin', remote]);
  await git(main, ['push', 'origin', 'main']);
  return remote;
}

function publishOptions(): ReleaseDispatchOptions {
  return {
    workflow: 'publish.yml',
    remote: 'origin',
    branch: 'main',
    inputs: { bump: 'auto', projects: '', dryRun: false },
  };
}

/**
 * A declared land's turn span (02_workspaces.md "A declared land"): main is brought to the remote's branch (step 3),
 * then the gated commit is fast-forwarded onto it (step 5). Its push comes after the turn ends (step 6).
 */
async function landFastForward(main: string, file: string): Promise<void> {
  await git(main, ['fetch', 'origin', 'main']);
  await git(main, ['merge', '--ff-only', 'origin/main']);
  await writeFile(join(main, file), `${file}\n`);
  await git(main, ['add', file]);
  await git(main, ['commit', '-m', file]);
}

/** A land's detached push of main (step 6). */
async function pushMain(main: string): Promise<void> {
  await git(main, ['push', 'origin', 'main']);
}

/**
 * The repository's land turn as the gateway keeps it (05_gateway.md "Land turns", laws `one_holder` and `fifo`): the
 * first ticket holds the turn until its command ends, and each later ticket waits in the order it asked.
 */
class LandTurn {
  private tail: Promise<void> = Promise.resolve();
  private tickets = 0;
  private readonly contention = Promise.withResolvers<void>();

  hold<T>(command: () => Promise<T>): Promise<T> {
    this.tickets += 1;
    if (this.tickets > 1) {
      this.contention.resolve();
    }
    const turn = this.tail.then(command);
    const release = () => {
      this.tickets -= 1;
    };
    this.tail = turn.then(release, release);
    return turn;
  }

  /** Settles once a ticket waits behind the holder. */
  contended(): Promise<void> {
    return this.contention.promise;
  }
}

type PublishRunOutcome = { conclusion: 'success' } | { conclusion: 'failure'; reason: string };

/**
 * The host's main checkout and the forge. A dispatch creates a run that clones the branch as it is at that moment,
 * commits the version bump, builds through `buildWindow`, and pushes the release refs as `smoo release publish` does.
 */
class LocalPublishShell implements ReleaseDispatchShell {
  readonly logs: string[] = [];
  readonly awaiting = Promise.withResolvers<void>();
  buildWindow: () => Promise<void> = async () => {};
  private readonly runs: Array<{ run: PublishRun; outcome: Promise<PublishRunOutcome> }> = [];

  constructor(
    private readonly main: string,
    private readonly remote: string,
  ) {}

  get dispatches(): number {
    return this.runs.length;
  }

  async outcome(index: number): Promise<PublishRunOutcome> {
    const run = this.runs[index];
    if (!run) {
      throw new Error(`no run ${index} was dispatched`);
    }
    return run.outcome;
  }

  head(): Promise<string> {
    return gitOutput(this.main, ['rev-parse', 'HEAD']);
  }

  async fetchRemoteHead(branch: string): Promise<string | null> {
    await git(this.main, ['fetch', 'origin', branch]);
    return gitOutput(this.main, ['rev-parse', `origin/${branch}`]);
  }

  isAncestor(ancestor: string, descendant: string): Promise<boolean> {
    return gitIsAncestor(this.main, ancestor, descendant);
  }

  async dispatchPublishWorkflow(_workflow: string, branch: string): Promise<PublishRun> {
    const number = this.runs.length + 1;
    const runner = join(this.main, `runner-${number}`);
    await git(this.main, ['clone', '--branch', branch, this.remote, runner]);
    await git(runner, ['config', 'user.name', 'Test User']);
    await git(runner, ['config', 'user.email', 'test@example.com']);
    const run = { id: String(number), url: `https://github.test/scope/repo/actions/runs/${number}` };
    const outcome = this.release(runner).then(
      (): PublishRunOutcome => ({ conclusion: 'success' }),
      (error: unknown): PublishRunOutcome => ({
        conclusion: 'failure',
        reason: error instanceof Error ? error.message : String(error),
      }),
    );
    this.runs.push({ run, outcome });
    return run;
  }

  async awaitPublishRun(run: PublishRun): Promise<string> {
    this.awaiting.resolve();
    const found = this.runs.find((candidate) => candidate.run.id === run.id);
    if (!found) {
      throw new Error(`run ${run.id} was never dispatched`);
    }
    return (await found.outcome).conclusion;
  }

  log(message: string): void {
    this.logs.push(message);
  }

  private async release(runner: string): Promise<void> {
    await writePackage(runner, '@scope/cli', 'packages/cli', '0.2.0');
    await git(runner, ['add', 'packages/cli/package.json']);
    await git(runner, ['commit', '-m', 'chore(release): publish']);
    await this.buildWindow();
    const next: ReleasePackageInfo = { name: '@scope/cli', projectName: 'cli', path: 'packages/cli', version: '0.2.0' };
    await new LocalGitRepairShell(runner).pushReleaseRefs([next]);
  }
}
