import { Command, CommanderError } from 'commander';
import { cliPackageVersion } from './lib/cli-package.js';
import { findRepoRoot } from './lib/run.js';

export async function runCli(argv = process.argv.slice(2)): Promise<void> {
  const program = buildProgram();
  try {
    await program.parseAsync(argv, { from: 'user' });
  } catch (error) {
    if (error instanceof CommanderError) {
      if (error.code !== 'commander.helpDisplayed') {
        process.exitCode = error.exitCode;
      }
      return;
    }
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
  }
}

function buildProgram(): Command {
  const program = new Command();
  program
    .name('smoo')
    .description('SmoothBricks monorepo tooling')
    .version(cliPackageVersion, '-v, --version', 'print smoo version')
    .exitOverride()
    .showHelpAfterError();

  const monorepo = program.command('monorepo').description('Manage SmoothBricks-style monorepos');
  monorepo
    .command('init')
    .option('--runtime-only', 'only sync root Bun/Node runtime metadata')
    .option('--sync-runtime', 'sync root Bun/Node runtime metadata outside devenv')
    .action(async (options: { runtimeOnly?: boolean; syncRuntime?: boolean }) => {
      const { initMonorepo } = await import('./monorepo/index.js');
      await initMonorepo(await findRepoRoot(), options);
    });
  monorepo
    .command('validate')
    .option('--fix', 'apply safe monorepo policy fixes before validation')
    .option('--fail-fast', 'stop after the first failing validation pack')
    .option('--only-if-new-workspace-package', 'skip validation unless a new workspace package manifest is staged')
    .option('--verbose', 'print validation progress and successful checks')
    .action(
      async (options: {
        fix?: boolean;
        failFast?: boolean;
        onlyIfNewWorkspacePackage?: boolean;
        verbose?: boolean;
      }) => {
        const { validateMonorepo } = await import('./monorepo/index.js');
        await validateMonorepo(await findRepoRoot(), options);
      },
    );
  monorepo.command('update').action(async () => {
    const { updateManagedFiles } = await import('./monorepo/index.js');
    updateManagedFiles(await findRepoRoot());
  });
  monorepo.command('check').action(async () => {
    const { checkManagedFiles } = await import('./monorepo/index.js');
    checkManagedFiles(await findRepoRoot());
  });
  monorepo.command('diff').action(async () => {
    const { diffManagedFiles } = await import('./monorepo/index.js');
    diffManagedFiles(await findRepoRoot());
  });
  monorepo
    .command('validate-commit-msg <commitMsgFile>')
    .option('--fix', 'format the commit message before validation')
    .action(async (commitMsgFile: string, options: { fix?: boolean }) => {
      const { validateCommitMessageFile } = await import('./monorepo/index.js');
      validateCommitMessageFile(commitMsgFile, options, await findRepoRoot());
    });
  monorepo.command('sync-bun-lockfile-versions').action(async () => {
    const { syncBunLockfileVersions } = await import('./monorepo/index.js');
    syncBunLockfileVersions(await findRepoRoot());
  });
  monorepo
    .command('list-release-packages')
    .option('--fail-empty', 'fail when no owned release packages are found')
    .option('--github-output <path>', 'append projects=<nx-projects> to a GitHub Actions output file')
    .action(async (options: { failEmpty?: boolean; githubOutput?: string }) => {
      const { listReleaseProjectNamesForNx } = await import('./monorepo/index.js');
      const packages = listReleaseProjectNamesForNx(await findRepoRoot(), options);
      if (!options.githubOutput) {
        console.log(packages);
      }
    });
  monorepo.command('validate-public-tags').action(async () => {
    const { validatePublicPackageTags } = await import('./monorepo/index.js');
    validatePublicPackageTags(await findRepoRoot());
  });
  const release = program.command('release').description('Version, publish, and create GitHub Releases');
  release.command('npm-status').action(async () => {
    const { printReleaseState } = await import('./release/index.js');
    await printReleaseState(await findRepoRoot());
  });
  release
    .command('repair-pending')
    .description('Repair incomplete older release commits before releasing the current HEAD')
    .option('--dry-run [dryRun]', 'run without pushing, publishing, or writing GitHub Releases')
    .action(async (options: { dryRun?: string | boolean }) => {
      const { releaseRepairPending } = await import('./release/index.js');
      await releaseRepairPending(await findRepoRoot(), { ...options, dryRun: booleanOption(options.dryRun) });
    });
  release
    .command('version')
    .option('--bump <bump>', 'auto, patch, minor, major, or prerelease', 'auto')
    .option('--dry-run [dryRun]', 'run without writing versions or tags')
    .option('--github-output <path>', 'append mode=<mode> and projects=<nx-projects> to a GitHub Actions output file')
    .action(async (options: { bump: string; dryRun?: string | boolean; githubOutput?: string }) => {
      const { releaseVersion } = await import('./release/index.js');
      await releaseVersion(await findRepoRoot(), {
        bump: options.bump,
        dryRun: booleanOption(options.dryRun),
        githubOutput: options.githubOutput,
      });
    });
  release
    .command('publish')
    .option('--bump <bump>', 'auto, patch, minor, major, or prerelease', 'auto')
    .option('--dry-run [dryRun]', 'run without pushing, publishing, or writing GitHub Releases')
    .action(async (options: { bump: string; dryRun?: string | boolean }) => {
      const { releasePublish } = await import('./release/index.js');
      await releasePublish(await findRepoRoot(), { ...options, dryRun: booleanOption(options.dryRun) });
    });
  release
    .command('retag-unpublished')
    .description('Move unpublished owned release tags to a later commit without bumping versions')
    .argument('<tag...>', 'owned release tags to move, for example @scope/pkg@1.2.3')
    .option('--to <ref>', 'commit or ref to move tags to', 'HEAD')
    .option('--push', 'push moved tags with force-with-lease')
    .option('--dispatch', 'push moved tags and start publish.yml with bump=auto')
    .option('--remote <remote>', 'git remote used for pushed tags')
    .option('--branch <branch>', 'branch used for publish workflow dispatch')
    .option('--dry-run [dryRun]', 'validate and print the retag operation without mutating refs')
    .action(
      async (
        tags: string[],
        options: {
          to?: string;
          push?: boolean;
          dispatch?: boolean;
          remote?: string;
          branch?: string;
          dryRun?: string | boolean;
        },
      ) => {
        const { releaseRetagUnpublished } = await import('./release/index.js');
        await releaseRetagUnpublished(await findRepoRoot(), {
          tags,
          to: options.to,
          push: options.push === true,
          dispatch: options.dispatch === true,
          remote: options.remote,
          branch: options.branch,
          dryRun: booleanOption(options.dryRun),
        });
      },
    );
  release
    .command('bootstrap-npm-packages')
    .alias('bootstrap')
    .description('Publish minimal npm placeholder packages so trusted publishing can be configured')
    .option('--dry-run [dryRun]', 'show placeholder publishes without logging in or publishing')
    .option('--skip-login', 'skip npm browser login before publishing placeholders')
    .option('--otp <otp>', 'npm one-time password for placeholder publish operations')
    .option('--package <name...>', 'only bootstrap the selected owned release package names')
    .action(async (options: { dryRun?: string | boolean; skipLogin?: boolean; otp?: string; package?: string[] }) => {
      const { releaseBootstrapNpmPackages } = await import('./release/index.js');
      await releaseBootstrapNpmPackages(await findRepoRoot(), {
        dryRun: booleanOption(options.dryRun),
        skipLogin: options.skipLogin === true,
        otp: options.otp,
        packages: options.package ?? [],
      });
    });
  release
    .command('trust-publisher')
    .description('Configure npm trusted publishing for owned release packages')
    .option('--dry-run [dryRun]', 'show npm trust changes without saving them')
    .option('--bootstrap', 'publish missing npm placeholder packages before configuring trust')
    .option('--otp <otp>', 'npm one-time password for trust operations')
    .option('--bootstrap-otp <otp>', 'npm one-time password for placeholder publishes during --bootstrap')
    .option('--skip-login', 'skip npm browser login before configuring trust')
    .action(
      async (options: {
        dryRun?: string | boolean;
        bootstrap?: boolean;
        otp?: string;
        bootstrapOtp?: string;
        skipLogin?: boolean;
      }) => {
        const { releaseTrustPublisher } = await import('./release/index.js');
        await releaseTrustPublisher(await findRepoRoot(), {
          dryRun: booleanOption(options.dryRun),
          bootstrap: options.bootstrap === true,
          otp: options.otp,
          bootstrapOtp: options.bootstrapOtp,
          skipLogin: options.skipLogin === true,
        });
      },
    );

  const devenv = program.command('devenv').description('Manage the repository devenv shell');
  devenv.command('update').action(async () => {
    const { updateDevenv } = await import('./devenv/index.js');
    await updateDevenv(await findRepoRoot());
  });
  devenv.command('reload').action(async () => {
    const { reloadDevenv } = await import('./devenv/index.js');
    await reloadDevenv(await findRepoRoot());
  });

  const nixpkgsOverlay = program.command('nixpkgs-overlay').description('Manage the repository nixpkgs overlay');
  nixpkgsOverlay.command('update').action(async () => {
    const { updateNixpkgsOverlay } = await import('./devenv/index.js');
    await updateNixpkgsOverlay(await findRepoRoot());
  });

  const nx = program.command('nx').description('Nx workspace helpers');
  nx.command('list-targets')
    .description('List project:target entries for every Nx project')
    .action(async () => {
      const { listTargets } = await import('./nx/index.js');
      await listTargets(await findRepoRoot());
    });
  nx.command('list-projects')
    .description('List Nx projects matching filters')
    .requiredOption('--with-target <target>', 'only include projects defining this target')
    .action(async (options: { withTarget?: string }) => {
      const { listProjects } = await import('./nx/index.js');
      await listProjects(await findRepoRoot(), options);
    });
  nx.command('reset-cache')
    .description('Run nx reset to clear Nx daemon and cache state')
    .action(async () => {
      const { resetCache } = await import('./nx/index.js');
      await resetCache(await findRepoRoot());
    });
  nx.command('clean-cache')
    .description('Remove local Nx cache directories when present')
    .action(async () => {
      const { cleanCache } = await import('./nx/index.js');
      await cleanCache(await findRepoRoot());
    });

  const githubCi = program.command('github-ci').description('GitHub Actions helpers');
  githubCi.command('cleanup-cache').action(async () => {
    const { cleanupGithubCiCache } = await import('./github-ci/index.js');
    await cleanupGithubCiCache(await findRepoRoot());
  });
  githubCi
    .command('nx-smart')
    .requiredOption('--target <target>')
    .option('--name <name>')
    .option('--step <step>')
    .action(async (options: { target: string; name?: string; step?: string }) => {
      const { githubCiNxSmart } = await import('./github-ci/index.js');
      await githubCiNxSmart(await findRepoRoot(), options);
    });
  githubCi
    .command('nx-run-many')
    .requiredOption('--targets <targets>')
    .option('--projects <projects>')
    .action(async (options: { targets: string; projects?: string }) => {
      const { githubCiNxRunMany } = await import('./github-ci/index.js');
      await githubCiNxRunMany(await findRepoRoot(), options);
    });

  return program;
}

function booleanOption(value: string | boolean | undefined): boolean {
  return value === true || value === 'true';
}
