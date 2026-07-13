import { Command, CommanderError } from 'commander';
import { variants } from './generate/index.js';
import { cliPackageVersion } from './lib/cli-package.js';
import { findRepoRoot } from './lib/run.js';
import { resolvePrConflicts } from './pr/index.js';
import { scaffold } from './wrangler/scaffold.js';

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
  monorepo
    .command('sync-bun-lockfile-versions')
    .option('--stage', 'stage bun.lock when versions were resynced (pre-commit self-heal); quiet when clean')
    .action(async (options: { stage?: boolean }) => {
      const { syncBunLockfileVersions } = await import('./monorepo/index.js');
      syncBunLockfileVersions(await findRepoRoot(), options.stage ? { log: false, stage: true } : {});
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
  monorepo
    .command('setup-test-tracing')
    .description('Configure LMAO Bun test tracing for workspace packages')
    .option('--all', 'configure every workspace package')
    .option('--projects <projects>', 'comma-separated Nx project names, package names, or package roots')
    .option('--op-context-export <exportName>', 'named op context export imported by test-suite-tracer', 'opContext')
    .option(
      '--tracer-module <module>',
      'module specifier that exports defineTestTracer',
      '@smoothbricks/lmao/testing/bun',
    )
    .option('--dry-run', 'print generator invocations without writing files')
    .action(
      async (options: {
        all?: boolean;
        projects?: string;
        opContextExport?: string;
        tracerModule?: string;
        dryRun?: boolean;
      }) => {
        const { setupTestTracing } = await import('./monorepo/index.js');
        await setupTestTracing(await findRepoRoot(), options);
      },
    );
  // `smoo g` / `smoo generate` — subcommands are driven by the variant
  // registry in src/generate/index.ts. To add a new variant, add an entry
  // there; the CLI wiring below picks it up automatically.
  const g = program.command('g').alias('generate').description('Scaffold workspace packages and components');
  for (const [variantName, variant] of Object.entries(variants)) {
    const sub = g.command(`${variantName} <name>`).description(variant.description);
    for (const opt of variant.options ?? []) {
      sub.option(opt.flag, opt.description);
    }
    sub.option('--dry-run', 'preview changes without writing');
    sub.action(async (name: string, options: Record<string, unknown>) => {
      const { generate } = await import('./generate/index.js');
      await generate(await findRepoRoot(), variantName, name, options);
    });
  }

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
    .option('--bootstrap-otp <otp>', 'npm one-time password for placeholder publishes during --bootstrap')
    .option('--skip-login', 'skip npm browser login before publishing placeholders during --bootstrap')
    .option('--package <name...>', 'only configure the selected owned release package names')
    .action(
      async (options: {
        dryRun?: string | boolean;
        bootstrap?: boolean;
        bootstrapOtp?: string;
        skipLogin?: boolean;
        package?: string[];
      }) => {
        const { releaseTrustPublisher } = await import('./release/index.js');
        await releaseTrustPublisher(await findRepoRoot(), {
          dryRun: booleanOption(options.dryRun),
          bootstrap: options.bootstrap === true,
          bootstrapOtp: options.bootstrapOtp,
          skipLogin: options.skipLogin === true,
          packages: options.package ?? [],
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
    .option('--mode <mode>', 'auto, affected, or run-many', 'auto')
    .option('--configuration <configuration>')
    .action(
      async (options: {
        target: string;
        name?: string;
        step?: string;
        mode?: 'auto' | 'affected' | 'run-many';
        configuration?: string;
      }) => {
        const { githubCiNxSmart } = await import('./github-ci/index.js');
        await githubCiNxSmart(await findRepoRoot(), options);
      },
    );
  githubCi
    .command('nx-run-many')
    .requiredOption('--targets <targets>')
    .option('--projects <projects>')
    .option('--configuration <configuration>')
    .action(async (options: { targets: string; projects?: string; configuration?: string }) => {
      const { githubCiNxRunMany } = await import('./github-ci/index.js');
      await githubCiNxRunMany(await findRepoRoot(), options);
    });
  githubCi
    .command('nx-deploy')
    .requiredOption('--configuration <configuration>')
    .option('--mode <mode>', 'auto, affected, or run-many', 'run-many')
    .option('--name <name>')
    .option('--step <step>')
    .option('--verify', 'run build, lint, and test before deploy')
    .action(
      async (options: {
        configuration: string;
        mode?: 'auto' | 'affected' | 'run-many';
        name?: string;
        step?: string;
        verify?: boolean;
      }) => {
        const { githubCiNxDeploy } = await import('./github-ci/index.js');
        await githubCiNxDeploy(await findRepoRoot(), options);
      },
    );

  const pr = program.command('pr').description('Work with GitHub pull requests');
  pr.command('resolve [pr]')
    .description('Resolve conflict markers in a PR (agent-first, two-phase)')
    .option('--remote <name>', 'git remote hosting the PR branch (auto-inferred when omitted)')
    .option('--abort', 'discard an in-progress resolution and return to the original branch')
    .action(async (prArg: string | undefined, options: { remote?: string; abort?: boolean }) => {
      const exitCode = await resolvePrConflicts(await findRepoRoot(), prArg, options);
      if (exitCode !== 0) {
        process.exitCode = exitCode;
      }
    });

  const wrangler = program.command('wrangler').description('Cloudflare wrangler project helpers');
  wrangler
    .command('scaffold <project>')
    .description('Write a starter scripts/prepare-env.ts (manifest-driven) and wire its nx target')
    .option('--force', 'overwrite an existing scripts/prepare-env.ts')
    .action(async (project: string, options: { force?: boolean }) => {
      scaffold(await findRepoRoot(), project, { force: options.force });
    });

  return program;
}

function booleanOption(value: string | boolean | undefined): boolean {
  return value === true || value === 'true';
}
