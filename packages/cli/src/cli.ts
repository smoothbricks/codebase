import { Command, CommanderError } from 'commander';
import { variants } from './generate/index.js';
import { cliPackageVersion } from './lib/cli-package.js';
import { decode, findRepoRoot, printCommandOutput } from './lib/run.js';
import { ensureChromium } from './playwright/index.js';
import { resolvePrConflicts } from './pr/index.js';
import { cleanupPullRequest, deployStage } from './wrangler/deploy-stage.js';
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
    reportFatal(error);
    process.exitCode = 1;
  }
}

// A failure's diagnostics must never die here. This printed only `error.message`,
// so a Bun ShellError -- whose message is the useless literal "Failed with exit
// code 1" and whose captured stdout/stderr hang off the error as properties --
// reduced a real CI failure to one unactionable line. Print everything the error
// carries: captured output, the cause chain, and the stack that names the call
// site that ran the command.
function reportFatal(error: unknown): void {
  if (!(error instanceof Error)) {
    console.error(String(error));
    return;
  }
  console.error(error.stack ?? error.message);
  printCapturedStreams(error);
  let cause: unknown = error.cause;
  while (cause !== undefined) {
    if (!(cause instanceof Error)) {
      console.error(`Caused by: ${String(cause)}`);
      return;
    }
    console.error(`Caused by: ${cause.stack ?? cause.message}`);
    printCapturedStreams(cause);
    cause = cause.cause;
  }
}

function printCapturedStreams(error: Error): void {
  printCommandOutput(capturedStream(error, 'stdout'), capturedStream(error, 'stderr'));
}

function capturedStream(error: Error, key: 'stdout' | 'stderr'): string {
  if (!(key in error)) {
    return '';
  }
  const value: unknown = Reflect.get(error, key);
  if (typeof value === 'string') {
    return value;
  }
  return value instanceof Uint8Array ? decode(value) : '';
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
    await updateManagedFiles(await findRepoRoot());
  });
  monorepo
    .command('check')
    .option('--warn', 'report drift as warnings (GitHub annotations) instead of failing')
    .action(async (options: { warn?: boolean }) => {
      const { checkManagedFiles } = await import('./monorepo/index.js');
      await checkManagedFiles(await findRepoRoot(), { warn: options.warn });
    });
  monorepo.command('diff').action(async () => {
    const { diffManagedFiles } = await import('./monorepo/index.js');
    await diffManagedFiles(await findRepoRoot());
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
    .option('--stage', 'stage bun.lock when versions were resynced; quiet when clean')
    .option(
      '--mode <mode>',
      'install: match package.json (default, CI); publish: map unpublished -next to last stable tag (pre-pack only)',
      'install',
    )
    .action(async (options: { stage?: boolean; mode?: 'install' | 'publish' }) => {
      const { syncBunLockfileVersions } = await import('./monorepo/index.js');
      const mode = options.mode === 'publish' ? 'publish' : 'install';
      syncBunLockfileVersions(await findRepoRoot(), {
        mode,
        ...(options.stage ? { log: false, stage: true } : {}),
      });
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
    .option('--ref <ref>', 'fixed release graph ref to inspect')
    .option('--platform-outputs <paths>', 'comma-separated cross-platform repair output roots grouped by release SHA')
    .action(async (options: { dryRun?: string | boolean; platformOutputs?: string; ref?: string }) => {
      // The source self-hosting shim has no Typia transform; release commands import transformed output validators.
      const { releaseRepairPending } = await import('./release/index.js');
      await releaseRepairPending(await findRepoRoot(), { ...options, dryRun: booleanOption(options.dryRun) });
    });
  release
    .command('build-platform-outputs')
    .description('Build selected current and pending-release platform outputs')
    .requiredOption('--bump <bump>', 'auto, patch, minor, major, or prerelease')
    .requiredOption('--targets <targets>', 'comma-separated Nx platform target names or globs')
    .requiredOption('--output <path>', 'output directory for current and repair artifacts')
    .option('--ref <ref>', 'fixed release graph ref to inspect')
    .option('--github-output <path>', 'append selected current platform projects to a GitHub Actions output file')
    .action(async (options: { bump: string; githubOutput?: string; output: string; ref?: string; targets: string }) => {
      // The source self-hosting shim has no Typia transform; release commands import transformed output validators.
      const { releaseCollectPlatformOutputs } = await import('./release/index.js');
      await releaseCollectPlatformOutputs(await findRepoRoot(), options);
    });
  release
    .command('version')
    .description('Bump release package versions and create the release commit; writes no tags')
    .option('--bump <bump>', 'auto, patch, minor, major, or prerelease', 'auto')
    .option('--dry-run [dryRun]', 'preview the bump without writing versions or a release commit')
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
    .command('tag')
    .description('Create the release tags for the release commit at HEAD')
    .option('--dry-run [dryRun]', 'report the tags without creating them')
    .action(async (options: { dryRun?: string | boolean }) => {
      const { releaseCreateTags } = await import('./release/index.js');
      await releaseCreateTags(await findRepoRoot(), { dryRun: booleanOption(options.dryRun) });
    });
  release
    .command('publish')
    .option('--bump <bump>', 'auto, patch, minor, major, or prerelease', 'auto')
    .option('--dry-run [dryRun]', 'run without pushing, publishing, or writing GitHub Releases')
    .option(
      '--prebuilt <directories...>',
      'publish only outputs matching the collected artifact manifests in these directories',
    )
    .action(async (options: { bump: string; dryRun?: string | boolean; prebuilt?: string[] }) => {
      const { releasePublish } = await import('./release/index.js');
      await releasePublish(await findRepoRoot(), {
        ...options,
        dryRun: booleanOption(options.dryRun),
        prebuilt: options.prebuilt,
      });
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
    .option('--stage <stage>')
    .option('--stream-output', 'stream Nx task output without prefixes')
    .action(
      async (options: {
        target: string;
        name?: string;
        step?: string;
        mode?: 'auto' | 'affected' | 'run-many';
        configuration?: string;
        stage?: string;
        streamOutput?: boolean;
      }) => {
        const { githubCiNxSmart } = await import('./github-ci/index.js');
        await githubCiNxSmart(await findRepoRoot(), options);
      },
    );
  githubCi
    .command('nx-run-many')
    .requiredOption('--targets <targets>')
    .option('--projects <projects>')
    .option('--projects-with-targets <targets>', 'select projects owning any comma-separated target or target glob')
    .option('--configuration <configuration>')
    .option('--collect-outputs <directory>')
    .action(
      async (options: {
        targets: string;
        projects?: string;
        projectsWithTargets?: string;
        configuration?: string;
        collectOutputs?: string;
      }) => {
        const { githubCiNxRunMany } = await import('./github-ci/index.js');
        await githubCiNxRunMany(await findRepoRoot(), options);
      },
    );
  githubCi
    .command('apply-outputs <directories...>')
    .requiredOption('--source-sha <sha>', 'expected source commit SHA')
    .action(async (directories: string[], options: { sourceSha: string }) => {
      // GitHub CI commands stay lazy so source self-hosting can initialize Typia only at manifest boundaries.
      const { githubCiApplyOutputs } = await import('./github-ci/index.js');
      await githubCiApplyOutputs(await findRepoRoot(), directories, options.sourceSha);
    });
  githubCi
    .command('nx-deploy')
    .option('--stage <stage>', 'explicit staging, production, or prN override')
    .option('--mode <mode>', 'auto, affected, or run-many', 'run-many')
    .option('--name <name>')
    .option('--step <step>')
    .option('--verify', 'run build, lint, and test before deploy')
    .action(
      async (options: {
        stage?: string;
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

  const playwright = program.command('playwright').description('Manage Playwright browsers');
  const playwrightEnsure = playwright.command('ensure').description('Ensure a Playwright browser is available');
  playwrightEnsure
    .command('chromium')
    .description('Ensure Chromium is available for browser tests')
    .action(async () => {
      await ensureChromium();
    });

  const wrangler = program.command('wrangler').description('Cloudflare wrangler project helpers');
  wrangler
    .command('scaffold <project>')
    .description('Write a starter scripts/prepare-env.ts (manifest-driven) and wire its nx target')
    .option('--force', 'overwrite an existing scripts/prepare-env.ts')
    .action(async (project: string, options: { force?: boolean }) => {
      scaffold(await findRepoRoot(), project, { force: options.force });
    });
  wrangler
    .command('deploy-stage')
    .requiredOption('--stage <stage>', 'staging, production, or prN')
    .action(async (options: { stage: string }) => {
      await deployStage(process.cwd(), options.stage);
    });
  wrangler
    .command('cleanup-pr')
    .requiredOption('--pr <number>', 'pull-request number')
    .action(async (options: { pr: string }) => {
      await cleanupPullRequest(process.cwd(), Number(options.pr));
    });

  return program;
}

function booleanOption(value: string | boolean | undefined): boolean {
  return value === true || value === 'true';
}
