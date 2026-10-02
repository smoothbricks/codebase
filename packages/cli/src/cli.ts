import { resolve } from 'node:path';
import { Command, CommanderError, InvalidArgumentError, Option } from 'commander';
import { variants } from './generate/index.js';
import { dispatchCiWorkflow, ensureCiPullRequest } from './github-ci/api.js';
import { cliPackageVersion } from './lib/cli-package.js';
import { decode, findRepoRoot, printCommandOutput } from './lib/run.js';
import { checkPublicDenylist } from './monorepo/public-denylist.js';
import { ensureChromium, runWithChromium } from './playwright/index.js';
import { resolvePrConflicts } from './pr/index.js';
import { RELEASE_BUMPS } from './release/core.js';
import { secretsSet, secretsStatus, secretsSync } from './secrets/commands.js';
import { secretsRun } from './secrets/run.js';
import {
  cleanupPullRequest,
  deployStage,
  describeCleanup,
  describeInventory,
  inventoryPullRequest,
} from './wrangler/deploy-stage.js';
import { deployedVersion } from './wrangler/deployed-version.js';
import { scaffold } from './wrangler/scaffold.js';
import { type DeploymentStage, parseDeploymentStage, parsePullRequestNumber } from './wrangler/stage.js';

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
export function reportFatal(error: unknown): void {
  if (!(error instanceof Error)) {
    console.error(error);
    return;
  }
  console.error(error.stack ?? error.message);
  printCapturedStreams(error);
  let cause: unknown = error.cause;
  while (cause !== undefined) {
    if (!(cause instanceof Error)) {
      console.error('Caused by:', cause);
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
    // `smoo secrets run <group> <command...>` hands the rest of argv to a
    // child verbatim, flags included. Commander only stops parsing after the
    // first operand when positional options are enabled here, on the parent
    // of the command that declares `passThroughOptions`.
    .enablePositionalOptions()
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
    .option(
      '--projects <names>',
      'comma-separated Nx project names to build and pack-validate (a release selection); default: every project',
    )
    .action(
      async (options: {
        fix?: boolean;
        failFast?: boolean;
        onlyIfNewWorkspacePackage?: boolean;
        verbose?: boolean;
        projects?: string;
      }) => {
        const { validateMonorepo } = await import('./monorepo/index.js');
        const projects = options.projects
          ?.split(',')
          .map((name) => name.trim())
          .filter((name) => name.length > 0);
        await validateMonorepo(await findRepoRoot(), { ...options, projects });
      },
    );
  monorepo.command('update').action(async () => {
    const { updateManagedFiles } = await import('./monorepo/index.js');
    await updateManagedFiles(await findRepoRoot());
  });
  monorepo
    .command('check')
    .option(
      '--warn',
      'report managed-file drift as warnings (GitHub annotations) instead of failing; package policies still fail',
    )
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
    .command('check-public-denylist [revisions...]')
    .description(
      'refuse revisions (default HEAD) whose tree matches the local smoothbricks.publicDenylist git config or SMOOTHBRICKS_PUBLIC_DENYLIST; silent when neither is set',
    )
    .action(async (revisions: string[]) => {
      await checkPublicDenylist(await findRepoRoot(), revisions.length > 0 ? revisions : ['HEAD']);
    });
  monorepo
    .command('sync-bun-lockfile-versions')
    .option('--stage', 'stage bun.lock when versions were resynced; quiet when clean')
    .addOption(
      new Option(
        '--mode <mode>',
        'install: match package.json (default, CI); publish: map unpublished -next to last stable tag (pre-pack only)',
      )
        .choices(['install', 'publish'])
        .default('install'),
    )
    .action(async (options: { stage?: boolean; mode: 'install' | 'publish' }) => {
      const { syncBunLockfileVersions } = await import('./monorepo/index.js');
      syncBunLockfileVersions(await findRepoRoot(), {
        mode: options.mode,
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
    .option('--dry-run [dryRun]', 'run without pushing, publishing, or writing GitHub Releases', dryRunOption)
    .option('--ref <ref>', 'fixed release graph ref to inspect')
    .option('--platform-outputs <paths>', 'comma-separated cross-platform repair output roots grouped by release SHA')
    .action(async (options: { dryRun?: boolean; platformOutputs?: string; ref?: string }) => {
      // The source self-hosting shim has no Typia transform; release commands import transformed output validators.
      const { releaseRepairPending } = await import('./release/index.js');
      await releaseRepairPending(await findRepoRoot(), { ...options, dryRun: options.dryRun === true });
    });
  release
    .command('build-platform-outputs')
    .description('Build selected current and pending-release platform outputs')
    .addOption(bumpOption().makeOptionMandatory())
    .option(
      '--projects <projects>',
      'comma-separated Nx projects to release, or all for every owned release package; blank releases package-local changes',
    )
    .requiredOption('--targets <targets>', 'comma-separated Nx platform target names or globs')
    .requiredOption('--output <path>', 'output directory for current and repair artifacts')
    .option('--ref <ref>', 'fixed release graph ref to inspect')
    .option('--github-output <path>', 'append selected current platform projects to a GitHub Actions output file')
    .action(
      async (options: {
        bump: string;
        projects?: string;
        githubOutput?: string;
        output: string;
        ref?: string;
        targets: string;
      }) => {
        // The source self-hosting shim has no Typia transform; release commands import transformed output validators.
        const { releaseCollectPlatformOutputs } = await import('./release/index.js');
        await releaseCollectPlatformOutputs(await findRepoRoot(), options);
      },
    );
  release
    .command('version')
    .description('Bump release package versions and create the release commit; writes no tags')
    .addOption(bumpOption().default('auto'))
    .option(
      '--projects <projects>',
      'comma-separated Nx projects to release, or all for every owned release package; blank releases package-local changes',
    )
    .option('--dry-run [dryRun]', 'preview the bump without writing versions or a release commit', dryRunOption)
    .option('--github-output <path>', 'append mode=<mode> and projects=<nx-projects> to a GitHub Actions output file')
    .action(async (options: { bump: string; projects?: string; dryRun?: boolean; githubOutput?: string }) => {
      const { releaseVersion } = await import('./release/index.js');
      await releaseVersion(await findRepoRoot(), {
        bump: options.bump,
        projects: options.projects,
        dryRun: options.dryRun === true,
        githubOutput: options.githubOutput,
      });
    });
  release
    .command('tag')
    .description('Create the release tags for the release commit at HEAD')
    .option('--dry-run [dryRun]', 'report the tags without creating them', dryRunOption)
    .action(async (options: { dryRun?: boolean }) => {
      const { releaseCreateTags } = await import('./release/index.js');
      await releaseCreateTags(await findRepoRoot(), { dryRun: options.dryRun === true });
    });
  release
    .command('publish')
    .addOption(bumpOption().default('auto'))
    .option('--dry-run [dryRun]', 'run without pushing, publishing, or writing GitHub Releases', dryRunOption)
    .option(
      '--prebuilt <directories...>',
      'publish only outputs matching the collected artifact manifests in these directories',
    )
    .action(async (options: { bump: string; dryRun?: boolean; prebuilt?: string[] }) => {
      const { releasePublish } = await import('./release/index.js');
      await releasePublish(await findRepoRoot(), {
        ...options,
        dryRun: options.dryRun === true,
        prebuilt: options.prebuilt,
      });
    });
  release
    .command('pack')
    .description('Pack publishable artifacts and write a manifest without publishing or touching Git')
    .requiredOption('--projects <projects>', 'comma-separated Nx project names')
    .requiredOption('--output <path>', 'empty output directory for tarballs and the manifest')
    .action(async (options: { projects: string; output: string }) => {
      const { releasePack } = await import('./release/index.js');
      await releasePack(await findRepoRoot(), options);
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
    .option('--dry-run [dryRun]', 'validate and print the retag operation without mutating refs', dryRunOption)
    .action(
      async (
        tags: string[],
        options: {
          to?: string;
          push?: boolean;
          dispatch?: boolean;
          remote?: string;
          branch?: string;
          dryRun?: boolean;
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
          dryRun: options.dryRun === true,
        });
      },
    );
  release
    .command('bootstrap-npm-packages')
    .alias('bootstrap')
    .description('Publish minimal npm placeholder packages so trusted publishing can be configured')
    .option('--dry-run [dryRun]', 'show placeholder publishes without logging in or publishing', dryRunOption)
    .option('--skip-login', 'skip npm browser login before publishing placeholders')
    .option('--otp <otp>', 'npm one-time password for placeholder publish operations')
    .option('--package <name...>', 'only bootstrap the selected owned release package names')
    .action(async (options: { dryRun?: boolean; skipLogin?: boolean; otp?: string; package?: string[] }) => {
      const { releaseBootstrapNpmPackages } = await import('./release/index.js');
      await releaseBootstrapNpmPackages(await findRepoRoot(), {
        dryRun: options.dryRun === true,
        skipLogin: options.skipLogin === true,
        otp: options.otp,
        packages: options.package ?? [],
      });
    });
  release
    .command('trust-publisher')
    .description('Configure npm trusted publishing for owned release packages')
    .option('--dry-run [dryRun]', 'show npm trust changes without saving them', dryRunOption)
    .option('--bootstrap', 'publish missing npm placeholder packages before configuring trust')
    .option('--bootstrap-otp <otp>', 'npm one-time password for placeholder publishes during --bootstrap')
    .option('--skip-login', 'skip npm browser login before publishing placeholders during --bootstrap')
    .option('--package <name...>', 'only configure the selected owned release package names')
    .action(
      async (options: {
        dryRun?: boolean;
        bootstrap?: boolean;
        bootstrapOtp?: string;
        skipLogin?: boolean;
        package?: string[];
      }) => {
        const { releaseTrustPublisher } = await import('./release/index.js');
        await releaseTrustPublisher(await findRepoRoot(), {
          dryRun: options.dryRun === true,
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
  githubCi
    .command('dispatch-workflow')
    .requiredOption('--workflow <workflow>')
    .requiredOption('--ref <ref>')
    .action(async (options: { workflow: string; ref: string }) => {
      await dispatchCiWorkflow(options.workflow, options.ref);
    });
  githubCi
    .command('ensure-pull-request')
    .requiredOption('--head <branch>')
    .requiredOption('--base <branch>')
    .requiredOption('--title <title>')
    .requiredOption('--body <body>')
    .action(ensureCiPullRequest);
  githubCi
    .command('nx-smart')
    .requiredOption('--target <target>')
    .option('--name <name>')
    .option('--step <step>')
    .addOption(new Option('--mode <mode>', 'how Nx selects projects').choices(NX_MODES).default('auto'))
    .option('--configuration <configuration>')
    .option('--stage <stage>')
    .option('--stream-output', 'stream Nx task output without prefixes')
    .action(
      async (options: {
        target: string;
        name?: string;
        step?: string;
        mode: (typeof NX_MODES)[number];
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
    .option('--stage <stage>', 'explicit staging, production, or prN override', usageArgument(parseDeploymentStage))
    .addOption(new Option('--mode <mode>', 'how Nx selects projects').choices(NX_MODES).default('run-many'))
    .option('--name <name>')
    .option('--step <step>')
    .option('--verify', 'run build, lint, and test before deploy')
    .option('--select-tag <tag>', 'deploy only projects carrying this nx tag')
    .action(
      async (options: {
        stage?: DeploymentStage;
        mode: (typeof NX_MODES)[number];
        name?: string;
        step?: string;
        verify?: boolean;
        selectTag?: string;
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

  const playwright = program.command('playwright').description('Manage Playwright browsers').enablePositionalOptions();
  const playwrightEnsure = playwright.command('ensure').description('Ensure a Playwright browser is available');
  playwrightEnsure
    .command('chromium')
    .description('Ensure Chromium is available for browser tests')
    .action(async () => {
      await ensureChromium();
    });
  playwright
    .command('run <command> [args...]')
    .description('Run a command with Chromium prepared and its browser path in the child environment')
    .passThroughOptions()
    .action(async (command: string, args: string[]) => {
      await runWithChromium(command, args);
    });

  const secrets = program
    .command('secrets')
    .description('Reconcile declared secrets: what Workers need, what workflows pass, what the repository holds')
    .enablePositionalOptions();
  secrets
    .command('run [group] [command...]')
    .description(
      'Run one command with one group of declared secrets in its environment. Shell entry resolves the ' +
        '`shell` group only, so a registry credential or the Nx cache token is resolved here, by the command ' +
        'that needs it, instead of by every direnv reload. The group is required and positional: with none, ' +
        'this lists the groups this repository declares and the secrets in each',
    )
    // Everything after the group reaches the child verbatim, flags included:
    // `smoo secrets run registry bun add -d @acme/x` must run `bun add -d
    // @acme/x`, not lose `-d` to this command's own parser.
    .passThroughOptions()
    .action(async (group: string | undefined, command: string[]) => {
      process.exitCode = await secretsRun(await findRepoRoot(), group, command);
    });
  secrets
    .command('status')
    .description(
      'Show every declared secret with the scopes holding it. Exits 0 when every secret a managed workflow ' +
        'passes has a value in a scope the job reads, and 1 when one does not or when the repository, its ' +
        'secrets or a stage declaration could not be read',
    )
    .option(
      '-R, --repo <owner/name|remote>',
      "repository or remote name; defaults to the current branch's upstream remote",
    )
    .option(
      '--env <environment>',
      "also read this GitHub Environment; its value takes precedence over the repository's for a job bound to it",
    )
    .option(
      '--json',
      'write one JSON document to stdout instead of the table, for another tool to read; the exit code is ' +
        'unchanged, so a status that refuses still refuses',
    )
    .action(async (options: { repo?: string; env?: string; json?: boolean }) => {
      process.exitCode = await secretsStatus(await findRepoRoot(), options);
    });
  secrets
    .command('set [name]')
    .description('Set secrets from pasted values; with no name, prompts for every secret the target scope still lacks')
    .option(
      '-R, --repo <owner/name|remote>',
      "repository or remote name; defaults to the current branch's upstream remote",
    )
    .option(
      '--env <environment>',
      "write into this GitHub Environment; its value takes precedence over the repository's for a job bound to it",
    )
    .action(async (name: string | undefined, options: { repo?: string; env?: string }) => {
      process.exitCode = await secretsSet(await findRepoRoot(), name, options);
    });
  secrets
    .command('sync')
    .description('Push every secret smoo.secrets can fetch locally to the repository')
    .option(
      '-R, --repo <owner/name|remote>',
      "repository or remote name; defaults to the current branch's upstream remote",
    )
    .option(
      '--env <environment>',
      "write into this GitHub Environment; its value takes precedence over the repository's for a job bound to it",
    )
    .action(async (options: { repo?: string; env?: string }) => {
      process.exitCode = await secretsSync(await findRepoRoot(), options);
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
    .requiredOption('--stage <stage>', 'staging, production, or prN', usageArgument(parseDeploymentStage))
    .option('--config <path>', "deploy a build-generated flat wrangler.json instead of the project's own config")
    .option('--version-endpoint <url>', 'URL served by this worker whose trimmed body is the running version tag')
    .action(async (options: { stage: DeploymentStage; config?: string; versionEndpoint?: string }) => {
      await deployStage(process.cwd(), {
        stage: options.stage,
        repositoryRoot: await findRepoRoot(),
        ...(options.config ? { config: resolve(options.config) } : {}),
        ...(options.versionEndpoint ? { versionEndpoint: options.versionEndpoint } : {}),
      });
    });
  wrangler
    .command('deployed-version')
    .description('Print the version tag serving all traffic for this project\u2019s worker on a stage')
    .requiredOption('--stage <stage>', 'staging, production, or prN', usageArgument(parseDeploymentStage))
    .option('--config <path>', 'resolve the worker name from a build-generated flat wrangler.json')
    .option('--refresh', 'ask Cloudflare even when a fresh cached answer exists')
    .action(async (options: { stage: DeploymentStage; config?: string; refresh?: boolean }) => {
      const report = await deployedVersion(process.cwd(), {
        stage: options.stage,
        ...(options.config ? { config: resolve(options.config) } : {}),
        ...(options.refresh ? { refresh: true } : {}),
      });
      // The tag alone on stdout: this is read by humans and by `$(...)`, and a version deployed
      // outside Nx genuinely has no tag, which `untagged` says without pretending to be one.
      console.log(report.versionTag ?? 'untagged');
    });
  wrangler
    .command('cleanup-pr')
    .description('Delete what the deploys of this repository\u2019s prN stage recorded, then those records')
    .requiredOption('--pr <number>', 'pull-request number', usageArgument(parsePullRequestNumber))
    .option('--dry-run', 'list what cleanup would delete and delete nothing')
    .option('--json', 'write the result as JSON instead of a sentence')
    .action(async (options: { pr: number; dryRun?: boolean; json?: boolean }) => {
      const root = await findRepoRoot();
      const { pr } = options;
      const json = options.json === true;
      if (options.dryRun === true) {
        const inventory = await inventoryPullRequest(root, pr);
        console.log(json ? JSON.stringify(inventory, null, 2) : describeInventory(inventory));
        return;
      }
      const result = await cleanupPullRequest(root, pr);
      console.log(json ? JSON.stringify(result, null, 2) : describeCleanup(result));
    });

  return program;
}

const NX_MODES = ['auto', 'affected', 'run-many'] as const;

function bumpOption(): Option {
  return new Option('--bump <bump>', 'how far to bump each released package').choices(RELEASE_BUMPS);
}

/**
 * Commander's parser for a value only a domain parser can judge. A value it refuses becomes a usage
 * error, which commander prints as one line before the command runs (`error: option '--pr <number>'
 * argument '0' is invalid.` and the parser's reason) and exits 1, where a throw from inside the
 * command would print a stack that names smoo's own call sites instead of what was wrong.
 */
function usageArgument<T>(parse: (value: string) => T): (value: string) => T {
  return (value) => {
    try {
      return parse(value);
    } catch (error) {
      if (error instanceof Error) throw new InvalidArgumentError(error.message);
      throw error;
    }
  };
}

/**
 * `--dry-run` alone, or `--dry-run true|false` from a workflow input. Anything else is refused: read
 * as "not a dry run", a typo would publish.
 */
function dryRunOption(value: string): boolean {
  if (value === 'true') return true;
  if (value === 'false') return false;
  throw new InvalidArgumentError('Expected true or false, or the flag alone.');
}
