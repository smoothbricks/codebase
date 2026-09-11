/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

import {
  cargoCrossTestArchiveFile,
  cargoCrossTestArchiveTargetName,
  cargoCrossTestTargetName,
} from '@smoothbricks/nx-plugin/cross-check-policy';
import { PRODUCTION_PUSH_DEPLOY_TAG } from '../lib/deploy-tags.js';
import type {
  NonEmptyArray,
  PackageCargoCredentialsConfig,
  PackageCargoGitOrigin,
  PackagePrivateNpmConfig,
  PackageRemoteCacheConfig,
  PackageSmooGithub,
  PackageSmooGithubEnvironments,
  PackageSourceCheckoutConfig,
} from '../lib/json.js';
import { renderRunsOnLine } from './github-runs-on.js';

export enum CiWorkflowStepKind {
  Checkout = 'checkout',
  SourceCheckouts = 'source-checkouts',
  SetupDevenv = 'setup-devenv',
  CargoCredentials = 'cargo-credentials',
  SetNxShas = 'set-nx-shas',
  RestoreNxCache = 'restore-nx-cache',
  Build = 'build',
  BrowserTests = 'browser-tests',
  Lint = 'lint',
  UnitTests = 'unit-tests',
  CrossToolchainPreflight = 'cross-toolchain-preflight',
  CrossTestArchives = 'cross-test-archives',
  UploadCrossTestArchives = 'upload-cross-test-archives',
  DownloadCrossTestArchives = 'download-cross-test-archives',
  CrossTests = 'cross-tests',
  ManagedFilesCheck = 'managed-files-check',
  ManagedFilesDispatch = 'managed-files-dispatch',
  Deploy = 'deploy',
  SaveNxCache = 'save-nx-cache',
  UploadTraceDbs = 'upload-trace-dbs',
  SaveNixDevenv = 'save-nix-devenv',
}

export interface CiWorkflowStep {
  kind: CiWorkflowStepKind;
  name: string;
  number: number;
}

/**
 * What a deploy step needs to authenticate, as declared configuration. Both
 * workflow generators deploy production with the same credentials, so the
 * declaration and its rendering live here once: a repo that declares
 * `smoo.github.deploySecrets` gets them wherever a managed workflow deploys,
 * not only on CI's stage deploy.
 */
export interface DeployStepSecretConfig {
  deployProvider?: 'cloudflare';
  /** Extra deploy-step secrets, env var name → repository secret name. */
  deploySecrets?: Record<string, string>;
}

/**
 * One foreign target triple's test archive: what the Linux job cross-builds and
 * a native runner executes.
 *
 * Both fields are DERIVED, never hand-written: the triple comes from the cargo
 * workspace's `[workspace.metadata.smoothbricks.test] cross-targets`, and the
 * path is where the inferred `cargo-cross-test-archive-<triple>` target
 * declares its output. That is why this is not a `smoo.github` key — a triple
 * repeated in CI config could name an archive the graph cannot build, and the
 * workflow would only find out on the runner.
 */
export interface CiCrossTestArchive {
  triple: string;
  /** Repository-relative path of the archive file, from the target's output. */
  path: string;
}

export interface CiWorkflowDefinitionOptions extends DeployStepSecretConfig {
  actionsProvider?: PackageSmooGithub['actionsProvider'];
  deploy: boolean;
  browserTests: boolean;
  e2eDeployment: boolean;
  /** The first entry is the branch whose pushes deploy the staging stage. */
  pushBranches: NonEmptyArray<string>;
  /** Default ubuntu-latest when omitted. */
  runsOn?: string | string[];
  /**
   * Declared private-npm opt-in. Exposes the read token to the installing job
   * before SetupDevenv so `.npmrc` `${TOKEN}` expansion can run, and skips
   * fork PRs, which never receive secrets. The registry URL lives in `.npmrc`,
   * not job env. Publish tokens never belong here.
   */
  privateNpm?: PackagePrivateNpmConfig;
  /**
   * Declared sibling source checkouts (package.json `smoo.github.sourceCheckouts`).
   * Cloned beside the main checkout before SetupDevenv so path dependencies
   * resolve exactly as in the developer workspace. Absent means none.
   */
  sourceCheckouts?: PackageSourceCheckoutConfig[];
  /**
   * Declared Cargo private-dependency credentials
   * (package.json `smoo.github.cargoCredentials`). Registry tokens ride the
   * job env; private git origins get a host-gated credential helper installed
   * before SetupDevenv. Absent means no private Cargo fetch.
   */
  cargoCredentials?: PackageCargoCredentialsConfig;
  /**
   * Declared self-hosted Nx remote cache (package.json `smoo.remoteCache`).
   * Every job that runs Nx carries the server and the access token, so one
   * cache serves the whole fleet. Absent means each runner caches locally.
   */
  remoteCache?: PackageRemoteCacheConfig;
  /** GitHub Environments: staging for the validate/e2e jobs, production for the production-on-push job. */
  environments?: PackageSmooGithubEnvironments;
  /** Secrets for the e2e-deployment step, env var name → repository secret name. */
  e2eSecrets?: Record<string, string>;
  /** Emit the production-on-push job (some project carries PRODUCTION_PUSH_DEPLOY_TAG). */
  productionOnPush?: boolean;
  /**
   * Cross-built test archives, one per foreign target triple the cargo
   * workspace declares. The Linux job builds and uploads each one; a second job
   * downloads the `*-apple-darwin` ones and EXECUTES them on `macosRunsOn`,
   * which is how a cross compile gets proved by running rather than by linking.
   *
   * Empty or absent renders exactly what a repository without cross archives
   * renders — no step, no job, no numbering shift.
   */
  crossTestArchives?: readonly CiCrossTestArchive[];
  /**
   * macOS runner labels for the darwin execution job
   * (`smoo.github.macosRunsOn`, the same labels the publish workflow's macOS
   * legs use). Default macos-latest.
   *
   * A self-hosted label here is one specific machine. That machine being asleep
   * does not skip the job: it queues, and the run FAILS on the job's
   * `timeout-minutes`. That is the trade this opt-in buys — a red run when the
   * Mac is unreachable, in exchange for the darwin binaries being tested by
   * execution at all. `continue-on-error` would buy the green back by making
   * the job unable to report anything, which is worse than not having it.
   */
  macosRunsOn?: string | string[];
  /**
   * Declared Linux cross-platform producer (`smoo.github.platformProducer`):
   * the toolchain preflight and the environment a foreign-target build needs on
   * a Linux runner. The publish workflow already renders this declaration for
   * its cross artifact job, so the cross test archive reuses it rather than
   * carrying a second copy of one repository's SDK and linker setup.
   *
   * Rendered STEP-scoped here, never as job env: this environment turns the
   * repository's native producers into cross producers, and Validate's other
   * steps are host builds that must stay host builds.
   */
  platformProducer?: PackageSmooGithub['platformProducer'];
}

type CiWorkflowStepInput = Omit<CiWorkflowStep, 'number'>;

export function defineCiWorkflow(options: CiWorkflowDefinitionOptions): CiWorkflowStep[] {
  const steps: CiWorkflowStepInput[] = [{ kind: CiWorkflowStepKind.Checkout, name: '📥 Checkout' }];
  if (options.cargoCredentials !== undefined) {
    steps.push({ kind: CiWorkflowStepKind.CargoCredentials, name: 'Prepare Cargo credentials' });
  }
  if (options.sourceCheckouts?.length) {
    steps.push({ kind: CiWorkflowStepKind.SourceCheckouts, name: '📦 Check out sibling sources' });
  }
  steps.push(
    { kind: CiWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv' },
    { kind: CiWorkflowStepKind.SetNxShas, name: '🧭 Set Nx SHAs' },
    { kind: CiWorkflowStepKind.RestoreNxCache, name: '🧠 Restore Nx cache' },
    { kind: CiWorkflowStepKind.Build, name: '🔨 Build' },
  );
  if (options.deploy) {
    steps.push({ kind: CiWorkflowStepKind.Deploy, name: '🚀 Deploy Stage' });
  }
  steps.push(
    { kind: CiWorkflowStepKind.Lint, name: '🔍 Lint' },
    { kind: CiWorkflowStepKind.UnitTests, name: '🧪 Unit Tests' },
  );
  if (options.crossTestArchives?.length) {
    if (options.platformProducer !== undefined) {
      steps.push({
        kind: CiWorkflowStepKind.CrossToolchainPreflight,
        name: 'Check cross-platform toolchain prerequisites',
      });
    }
    steps.push({ kind: CiWorkflowStepKind.CrossTestArchives, name: '🎯 Build cross-target test archives' });
    // The artifact has exactly one consumer, the execution job below. A triple
    // with no native runner here is already proved by the build above, so
    // uploading its archive would ship a file nothing in the run downloads —
    // and would move the artifact's root off the directory the download
    // restores to.
    if (darwinCrossTestArchives(options).length > 0) {
      steps.push({ kind: CiWorkflowStepKind.UploadCrossTestArchives, name: '📤 Upload cross-target test archives' });
    }
  }
  if (options.browserTests) {
    steps.push({ kind: CiWorkflowStepKind.BrowserTests, name: '🌐 Browser Tests' });
  }
  steps.push(
    { kind: CiWorkflowStepKind.ManagedFilesCheck, name: '🩺 Check managed-file drift' },
    { kind: CiWorkflowStepKind.ManagedFilesDispatch, name: '🔁 Dispatch managed-file drift healing' },
    { kind: CiWorkflowStepKind.SaveNxCache, name: '💾 Save Nx cache' },
    { kind: CiWorkflowStepKind.UploadTraceDbs, name: '📎 Upload trace DBs' },
    { kind: CiWorkflowStepKind.SaveNixDevenv, name: '🧹 Cleanup and cache Nix/devenv' },
  );
  return steps.map((step, index) => ({ ...step, number: index + 2 }));
}

export function renderCiWorkflowYaml(options: CiWorkflowDefinitionOptions): string {
  const steps = defineCiWorkflow(options);
  return [
    renderCiWorkflowHeader(options),
    renderCiWorkflowSteps(steps, options),
    renderCrossTestExecutionJob(options),
    renderE2eDeploymentJob(options),
    renderProductionDeployJob(options),
  ].join('');
}

function renderCiWorkflowHeader(options: CiWorkflowDefinitionOptions): string {
  return `name: CI

on:
  push:
    branches:
${renderYamlList(options.pushBranches.map(yamlScalar), 6)}
  pull_request:

permissions:
  # actions:write lets the drift step dispatch the managed-files workflow.
  actions: write
  contents: read
${options.deploy ? '  deployments: write\n' : ''}  statuses: write

concurrency:
${
  options.deploy
    ? `  # One in-flight run per ref. Pushes to the staging push branch queue behind a
  # running workflow instead of canceling it, so a newer push never cancels the
  # deploy job mid-flight. Pull requests and other branches keep canceling
  # superseded runs.
  group: \${{ github.workflow }}-\${{ github.ref }}
  cancel-in-progress: \${{ github.ref != ${stagingRefLiteral(options)} }}`
    : `  # This workflow validates and never deploys, so there is no in-flight
  # deployment for a newer push to protect: every ref cancels its superseded
  # runs. Queuing them instead serializes the staging branch, and a burst of
  # pushes then reports the newest commit one full run per queued push late.
  group: \${{ github.workflow }}-\${{ github.ref }}
  cancel-in-progress: true`
}

defaults:
  run:
    working-directory: tooling/direnv

jobs:
  main:
    name: Validate
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 45
${
  options.privateNpm?.readTokenEnv || options.cargoCredentials !== undefined || options.remoteCache !== undefined
    ? `    # Fork PRs receive no secrets, so neither a private dependency install nor an
    # authenticated remote cache read can run there.
    if: \${{ github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository }}
`
    : ''
}${environmentLine(options.deploy ? options.environments?.staging : undefined)}${
  options.e2eDeployment
    ? `    outputs:
      deployment-stage: ${githubExpression('steps.deploy.outputs.stage')}
`
    : ''
}    env:
      GH_TOKEN: ${githubExpression('github.token')}
${remoteCacheJobEnvLines(options.remoteCache)}${cargoCredentialJobEnvLines(options.cargoCredentials)}${privateNpmReadTokenJobEnv(options)}    steps:
`;
}

function githubExpression(expression: string): string {
  return `$${`{{ ${expression} }}`}`;
}

/** Job env for `.npmrc` token expansion. Registry URL is not a GitHub variable. */
export function privateNpmReadTokenJobEnv(options: Pick<CiWorkflowDefinitionOptions, 'privateNpm'>): string {
  const tokenEnv = options.privateNpm?.readTokenEnv;
  if (!tokenEnv) {
    return '';
  }
  return `      ${tokenEnv}: ${githubExpression(`secrets.${tokenEnv}`)}\n`;
}

function renderCiWorkflowSteps(steps: CiWorkflowStep[], options: CiWorkflowDefinitionOptions): string {
  const lines: string[] = [];
  for (const step of steps) {
    lines.push(...sectionLinesBefore(step));
    lines.push(...commentLinesForStep(step));
    lines.push(...yamlLinesForStep(step, options));
    lines.push('');
  }
  return `${lines.join('\n').trimEnd()}\n`;
}

function sectionLinesBefore(step: CiWorkflowStep): string[] {
  if (step.kind === CiWorkflowStepKind.SetNxShas) {
    return ['      # --- Nx -----------------------------------------------------------------', ''];
  }
  if (step.kind === CiWorkflowStepKind.SaveNixDevenv) {
    return ['      # --- Cleanup ------------------------------------------------------------', ''];
  }
  return [];
}

function commentLinesForStep(step: CiWorkflowStep): string[] {
  if (step.kind === CiWorkflowStepKind.Checkout) {
    return ['      # Step 1: GitHub adds "Set up job" automatically', '      # Step 2'];
  }
  if (step.kind === CiWorkflowStepKind.CargoCredentials) {
    return [
      `      # Step ${step.number}`,
      '      # Installs a host-gated git credential helper for private Cargo git',
      '      # dependencies through GITHUB_ENV: CARGO_NET_GIT_FETCH_WITH_CLI makes',
      '      # cargo shell out to git, whose per-process GIT_CONFIG_* environment',
      '      # names the helper. The helper reads the token from the environment',
      '      # when git calls it, answers only for the declared origins, and an',
      '      # empty credential.helper reset keeps an ambient credential store',
      '      # (osxkeychain, store) from persisting the token after a fetch. The',
      '      # token never reaches a URL, argv, or stored git/cargo configuration.',
    ];
  }
  if (step.kind === CiWorkflowStepKind.SetupDevenv) {
    return [
      `      # Step ${step.number}. Composite action internals do not affect top-level job step`,
      '      # anchors; update the nx-smart --step values below if top-level steps move.',
    ];
  }
  if (step.kind === CiWorkflowStepKind.SetNxShas) {
    return [`      # Step ${step.number}`, '      # Sets the base and head SHAs required for the nx affected commands'];
  }
  if (step.kind === CiWorkflowStepKind.SaveNxCache) {
    return [
      `      # Step ${step.number}`,
      "      # Nx's database cache needs artifact files and .nx/workspace-data DB",
      '      # metadata restored together; GitHub Actions cache is only the archive',
      '      # transport. Save runs only after prior required steps succeed on the default',
      '      # branch, so PRs may restore shared cache but cannot publish it.',
      '      #',
      '      # Host-nix runners skip the transport entirely: setup-devenv puts their',
      '      # Nx cache on the shared /var/cache/ci bind, which already survives the',
      '      # job, so shipping it through the Actions cache would upload a copy of a',
      '      # cache the next job reads directly.',
    ];
  }
  return [`      # Step ${step.number}`];
}

function yamlLinesForStep(step: CiWorkflowStep, options: CiWorkflowDefinitionOptions): string[] {
  switch (step.kind) {
    case CiWorkflowStepKind.Checkout:
      return [
        `      - name: ${step.name}`,
        '        uses: actions/checkout@v6.0.2',
        '        with:',
        '          filter: blob:none',
        '          fetch-depth: 0',
      ];
    case CiWorkflowStepKind.SourceCheckouts:
      return sourceCheckoutsStepLines(step, options.sourceCheckouts ?? []);
    case CiWorkflowStepKind.CargoCredentials:
      return cargoCredentialStepLines(step, normalizeCargoCredentials(options.cargoCredentials ?? {}));
    case CiWorkflowStepKind.SetupDevenv:
      // One step everywhere. setup-devenv detects host-nix (GARM) vs ephemeral
      // and skips GH install/cache steps internally — same for ci/publish/managed.
      return [`      - name: ${step.name}`, '        id: setup', '        uses: ./.github/actions/setup-devenv'];

    case CiWorkflowStepKind.SetNxShas:
      return [
        `      - name: ${step.name}`,
        "        if: github.server_url == 'https://github.com' || endsWith(github.api_url, '/api/v3')",
        '        uses: nrwl/nx-set-shas@v5.0.1',
        '        with:',
        '          workflow-id: ci.yml',
      ];
    case CiWorkflowStepKind.RestoreNxCache:
      return [
        `      - name: ${step.name}`,
        '        id: nx-cache',
        // The shared bind is the cache on host runners; a restore there would
        // overwrite it with an older archive.
        "        if: steps.setup.outputs.host-runner != 'true'",
        '        uses: ./.github/actions/cache-nx',
      ];
    case CiWorkflowStepKind.Build:
      return nxSmartStep(step, 'build', 'Build');
    case CiWorkflowStepKind.BrowserTests:
      return nxSmartStep(step, 'test-browser', 'Browser Tests');
    case CiWorkflowStepKind.Lint:
      return nxSmartStep(step, 'lint', 'Lint');
    case CiWorkflowStepKind.UnitTests:
      return nxSmartStep(step, 'test', 'Unit Tests');
    case CiWorkflowStepKind.CrossToolchainPreflight:
      // The repository's own preflight, the same one the publish workflow's
      // cross artifact job runs — an SDK to provision, a linker to check.
      // `working-directory: .` because this workflow's steps default to the
      // direnv shell directory while a preflight is repository-relative.
      return [
        `      - name: ${step.name}`,
        '        working-directory: .',
        ...platformProducerStepEnvLines(options),
        '        run: |',
        '          set -euo pipefail',
        ...crossToolchainPreflight(options).map((line) => `          ${line}`),
      ];
    case CiWorkflowStepKind.CrossTestArchives:
      // Cross-COMPILES the workspace's test binaries for each declared foreign
      // triple and packs them; runs nothing. `nx-run-many` rather than
      // `nx-smart`, because an affected-graph selection would skip the archive
      // on a commit that changed nothing Rust — and the sibling job that
      // executes it has no way to build one.
      return [
        `      - name: ${step.name}`,
        ...platformProducerStepEnvLines(options),
        `        run: smoo github-ci nx-run-many --targets "${crossArchiveTargetNames(options).join(',')}"`,
      ];
    case CiWorkflowStepKind.UploadCrossTestArchives:
      // Exactly the archives the sibling job EXECUTES, which is what keeps the
      // artifact's root where the download expects it: upload-artifact roots an
      // artifact at the least common ancestor of the files it uploaded, so a
      // foreign triple's archive under another cargo workspace would raise that
      // root to the repository and restore every path one directory too deep.
      // retention-days: 1 — the only consumer is that job in this same run, and
      // these archives are the whole workspace's test binaries.
      return artifactStepLines(options.actionsProvider, step.name, 'upload', [
        `name: ${CROSS_TEST_ARCHIVE_ARTIFACT}`,
        'path: |',
        ...darwinCrossTestArchives(options).map((archive) => `  ${archive.path}`),
        // error, not ignore: a missing archive means the cross build silently
        // produced nothing, and the execute job would then have nothing to run
        // and no reason to say so.
        'if-no-files-found: error',
        'retention-days: 1',
      ]);
    case CiWorkflowStepKind.DownloadCrossTestArchives:
      // Restores each archive at the path its own Nx target declares as output,
      // which is where the runner target reads it from. upload-artifact roots
      // the artifact at the least common ancestor of what it uploaded, so the
      // download path is that shared directory.
      return artifactStepLines(options.actionsProvider, step.name, 'download', [
        `name: ${CROSS_TEST_ARCHIVE_ARTIFACT}`,
        `path: ${crossTestArchiveDirectory(options)}`,
      ]);
    case CiWorkflowStepKind.CrossTests:
      // EXECUTES, and that is all it can do: every one of these targets is
      // `nextest run --archive-file`, which extracts prebuilt binaries into its
      // own temporary directory. nextest never invokes cargo on this path
      // (measured), so nothing here can compile even if a toolchain is present.
      return [
        `      - name: ${step.name}`,
        `        run: smoo github-ci nx-run-many --targets "${darwinCrossTestTargetNames(options).join(',')}"`,
      ];
    case CiWorkflowStepKind.ManagedFilesCheck:
      // The authoritative drift check is nearly free here (devenv is already
      // up). --warn never fails the job and publishes the step output
      // `drifted=<count>` for the dispatch step below.
      return [
        `      - name: ${step.name}`,
        '        id: managed-drift',
        '        if:',
        "          ${{ github.event_name == 'push' && github.ref == format('refs/heads/{0}',",
        '          github.event.repository.default_branch) }}',
        '        run: smoo monorepo check --warn',
      ];
    case CiWorkflowStepKind.ManagedFilesDispatch:
      // Dispatches the managed-files workflow, which maintains the persistent
      // review PR. workflow_dispatch is exempt from GitHub's GITHUB_TOKEN
      // recursion guard, so the default token suffices.
      return [
        `      - name: ${step.name}`,
        "        if: steps.managed-drift.outputs.drifted != '' && steps.managed-drift.outputs.drifted != '0'",
        '        run: smoo github-ci dispatch-workflow --workflow managed-files.yml --ref "$GITHUB_REF_NAME"',
      ];
    case CiWorkflowStepKind.Deploy:
      return [
        `      - name: ${step.name}`,
        '        id: deploy',
        '        if: >-',
        '          ${{',
        "            (github.event_name == 'pull_request' &&",
        '              contains(fromJSON(\'["opened","reopened","synchronize"]\'), github.event.action) &&',
        '              github.event.pull_request.head.repo.full_name == github.repository) ||',
        `            (github.event_name == 'push' && github.ref == ${stagingRefLiteral(options)})`,
        '          }}',
        ...deployStepSecretEnvLines(options),
        `        run: smoo github-ci nx-deploy --mode run-many --name "Deploy Stage" --step ${step.number}`,
      ];
    case CiWorkflowStepKind.SaveNxCache:
      return [
        `      - name: ${step.name}`,
        '        if:',
        "          ${{ steps.setup.outputs.host-runner != 'true' && github.event_name == 'push' && github.ref ==",
        "          format('refs/heads/{0}', github.event.repository.default_branch) && steps.nx-cache.outputs.cache-hit != 'true'",
        '          }}',
        '        uses: actions/cache/save@v5.0.5',
        '        with:',
        '          path: |',
        '            .nx/cache',
        '            .nx/workspace-data/*.db*',
        '          key: ${{ runner.os }}-${{ runner.arch }}-nx-db-v1-${{ github.sha }}',
      ];
    case CiWorkflowStepKind.UploadTraceDbs:
      return artifactStepLines(
        options.actionsProvider,
        step.name,
        'upload',
        [
          'name: trace-results-${{ github.run_id }}',
          'path: packages/*/.cache/trace-results.db*',
          'if-no-files-found: ignore',
          'retention-days: 14',
          'include-hidden-files: true',
        ],
        'always()',
      );
    case CiWorkflowStepKind.SaveNixDevenv:
      return [
        `      - name: ${step.name}`,
        '        # always() still saves after a red job. Nix NAR is ephemeral-only;',
        '        # devenv eval-cache also saves on host-nix when setup missed.',
        '        if: always()',
        '        uses: ./.github/actions/save-nix-devenv',
        '        with:',
        '          devenv-cache-hit: ${{ steps.setup.outputs.devenv-cache-hit }}',
      ];
  }
}

/**
 * One clone command per declared sibling source, pinned to its configured
 * ref. The step's `if` keeps fork PRs — which receive no secrets — from
 * touching source-read credentials, and their builds would lack the private
 * siblings anyway. The clone URL stays clean: authorization rides per git
 * command through `GIT_CONFIG_*` environment config whose
 * `http.<origin>.extraheader` key names the exact origin, so the token never
 * reaches argv, a file, the sibling's stored `.git/config`, or any other
 * host. The token itself is only ever a shell expansion of the step env.
 */
export function sourceCheckoutsStepLines(
  step: CiWorkflowStep,
  checkouts: readonly PackageSourceCheckoutConfig[],
): string[] {
  const lines = [`      - name: ${step.name}`, ...SAME_REPO_GATE_FOLDED];
  const tokenEnvs = [
    ...new Set(checkouts.map((checkout) => checkout.tokenEnv).filter((env): env is string => env !== undefined)),
  ];
  if (tokenEnvs.length > 0) {
    lines.push('        env:');
    for (const env of tokenEnvs) {
      lines.push(`          ${env}: ${githubExpression(`secrets.${env}`)}`);
    }
  }
  lines.push('        run: |');
  lines.push('          root="$GITHUB_WORKSPACE"');
  for (const checkout of checkouts.map(normalizeSourceCheckout)) {
    const destination = `"$root/${checkout.path}"`;
    const auth =
      checkout.tokenEnv === undefined
        ? []
        : [
            `          key='http.${new URL(checkout.repository).origin}/.extraheader'`,
            `          val="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$${checkout.tokenEnv}" | base64 | tr -d '\\n')"`,
            '          GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="$key" GIT_CONFIG_VALUE_0="$val" \\',
          ];
    lines.push(...auth);
    lines.push(`          git clone --filter=blob:none ${checkout.repository} ${destination}`);
    if (checkout.ref !== undefined) {
      lines.push(...auth);
      lines.push(`          git -C ${destination} checkout --detach ${checkout.ref}`);
    }
  }
  return lines;
}

/** Mechanism errors surface at managed-file render time, never inside CI. */
export function normalizeSourceCheckout(config: PackageSourceCheckoutConfig): PackageSourceCheckoutConfig {
  if (config.path === undefined || config.path === '' || config.path.startsWith('/')) {
    throw new Error(
      `smoo.github.sourceCheckouts entry needs a path relative to the workspace root, got ${JSON.stringify(config.path)}`,
    );
  }
  let url: URL | null = null;
  try {
    url = config.repository === undefined ? null : new URL(config.repository);
  } catch {
    url = null;
  }
  if (
    url === null ||
    url.protocol !== 'https:' ||
    url.username !== '' ||
    url.password !== '' ||
    url.search !== '' ||
    url.hash !== ''
  ) {
    throw new Error(
      `smoo.github.sourceCheckouts entry needs a credential-free https repository URL, got ${JSON.stringify(config.repository)}`,
    );
  }
  if (config.tokenEnv !== undefined && !/^[A-Z_][A-Z0-9_]*$/.test(config.tokenEnv)) {
    throw new Error(
      `smoo.github.sourceCheckouts entry needs an upper-case secret env name, got ${JSON.stringify(config.tokenEnv)}`,
    );
  }
  return config;
}

/**
 * Job env lines carrying the declared Nx remote cache. The pair rides the job
 * env rather than a step because every Nx invocation in the job — build, lint,
 * tests, deploy — reads it, and Nx enables the cache on a nonempty server
 * alone: an empty or absent token would make it authenticate with nothing and
 * fail every task on 401, which is why the job carries the same fork-PR gate
 * as a private dependency install (see `renderCiWorkflowHeader`).
 *
 * A declared `internalServer` is the address managed runners use, exactly as a
 * git origin's `internalMirror` is: both say this repository's runners sit
 * inside that network, and shells outside it keep the public origin.
 * Mechanism errors surface at managed-file render time, never inside CI.
 */
export function remoteCacheJobEnvLines(config: PackageRemoteCacheConfig | undefined): string {
  if (config === undefined) {
    return '';
  }
  const normalized = normalizeRemoteCache(config);
  return [
    `      NX_SELF_HOSTED_REMOTE_CACHE_SERVER: ${normalized.internalServer ?? normalized.server}\n`,
    `      NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN: ${githubExpression(`secrets.${normalized.tokenSecret}`)}\n`,
  ].join('');
}

/** Mechanism errors surface at managed-file render time, never inside CI. */
export function normalizeRemoteCache(config: PackageRemoteCacheConfig): PackageRemoteCacheConfig {
  assertCacheOrigin('server', config.server);
  if (config.internalServer !== undefined) {
    assertCacheOrigin('internalServer', config.internalServer);
    if (config.internalServer === config.server) {
      throw new Error(
        `smoo.remoteCache internalServer repeats server ${config.server}; drop internalServer or point it at the address internal runners reach`,
      );
    }
  }
  if (!/^[A-Z_][A-Z0-9_]*$/.test(config.tokenSecret)) {
    throw new Error(
      `smoo.remoteCache needs an upper-case secret name for tokenSecret, got ${JSON.stringify(config.tokenSecret)}`,
    );
  }
  return config;
}

/**
 * A cache origin is exactly `scheme://host[:port]`. Nx appends
 * `/v1/cache/<hash>` to it, so a trailing slash silently requests a doubled
 * slash the server routes nowhere, and a path or credential would be dropped
 * or leaked rather than honored.
 */
function assertCacheOrigin(field: 'server' | 'internalServer', value: string): void {
  let url: URL | null = null;
  try {
    url = new URL(value);
  } catch {
    url = null;
  }
  if (
    url === null ||
    (url.protocol !== 'https:' && url.protocol !== 'http:') ||
    url.pathname !== '/' ||
    value.endsWith('/') ||
    url.username !== '' ||
    url.password !== '' ||
    url.search !== '' ||
    url.hash !== ''
  ) {
    throw new Error(
      `smoo.remoteCache ${field} needs a credential-free http(s) origin with no path and no trailing slash, got ${JSON.stringify(value)}`,
    );
  }
}

/**
 * Job env lines carrying the declared Cargo credential secrets. Both registry
 * tokens and git-origin tokens ride the job env so the credential helper is
 * still in scope when cargo spawns git much later in the build. Fork PRs
 * receive empty values and skip the helper step, so nothing answers for a
 * private origin there.
 */
export function cargoCredentialJobEnvLines(config: PackageCargoCredentialsConfig | undefined): string {
  if (config === undefined) {
    return '';
  }
  const tokenEnvs = distinctCargoTokenEnvs(normalizeCargoCredentials(config));
  if (tokenEnvs.length === 0) {
    return '';
  }
  return tokenEnvs.map((env) => `      ${env}: ${githubExpression(`secrets.${env}`)}\n`).join('');
}

/**
 * The credential-transport step preflights every declared token, then writes a
 * host-gated helper script into `$RUNNER_TEMP` for git origins and points the
 * per-process `GIT_CONFIG_*` environment at it through GITHUB_ENV, plus
 * `CARGO_NET_GIT_FETCH_WITH_CLI` so Cargo shells out to git. Runners whose
 * egress requires a proxy export it as `HTTP(S)_PROXY`, which cargo honors
 * natively but git never reads, so the step mirrors it into git config or
 * Cargo git fetches bypass the proxy and fail to connect. Origins with an
 * `internalMirror` are rewritten to the mirror with `url.<mirror>.insteadOf`
 * and the helper answers the same credential for the mirror's host, since git
 * passes helpers the rewritten URL. The helper reads the environment at call
 * time and answers only for declared origins. Declared `sshOrigins` rewrite
 * onto the same mirror and stay credential-free: the rewrite happens before
 * transport, so git only ever asks for the mirror, and an SSH pin the runner
 * has no key for fails loudly instead of collecting a token.
 * Mechanism errors surface at managed-file render time, never inside CI.
 */
export function cargoCredentialStepLines(step: CiWorkflowStep, config: PackageCargoCredentialsConfig): string[] {
  const normalized = normalizeCargoCredentials(config);
  const origins = normalized.gitOrigins ?? [];
  const tokenEnvs = distinctCargoTokenEnvs(normalized);
  const lines = [`      - name: ${step.name}`, ...SAME_REPO_GATE_FOLDED];
  lines.push('        env:');
  for (const env of tokenEnvs) {
    lines.push(`          ${env}: ${githubExpression(`secrets.${env}`)}`);
  }
  lines.push('        run: |');
  for (const env of tokenEnvs) {
    lines.push(
      `          : "\${${env}:?Missing ${env}; configure the repository secret before fetching Cargo dependencies}"`,
    );
  }
  if (origins.length === 0) {
    return lines;
  }
  lines.push('          helper="$RUNNER_TEMP/cargo-git-credential.sh"');
  lines.push('          cat > "$helper" <<\'SMOO_CARGO_HELPER_EOF\'');
  lines.push('          #!/bin/sh');
  lines.push('          [ "$1" = get ] || exit 0');
  lines.push('          # Host-gated credential answers for private Cargo git dependencies. The');
  lines.push('          # token is read from the environment when git calls this helper; it is');
  lines.push('          # never stored, echoed, or attached to any other origin.');
  lines.push('          protocol=');
  lines.push('          host=');
  lines.push('          while IFS= read -r line && [ -n "$line" ]; do');
  lines.push('            case "$line" in');
  lines.push('              protocol=*) protocol=${line#protocol=} ;;');
  lines.push('              host=*) host=${line#host=} ;;');
  lines.push('            esac');
  lines.push('          done');
  lines.push('          key="${protocol}|${host}"');
  lines.push('          case "$key" in');
  for (const origin of origins) {
    const url = new URL(origin.origin);
    const host = url.host.replaceAll("'", "'\\''");
    lines.push(
      `            'https|${host}'${url.port === '' ? `|'https|${host}:443'` : ''})`,
      `              printf 'username=x-access-token\\npassword=%s\\n' "$${origin.tokenEnv}" ;;`,
    );
    if (origin.internalMirror !== undefined) {
      // Git passes helpers the rewritten URL, so the mirror answers with the
      // same credential under its own scheme.
      const mirror = new URL(origin.internalMirror);
      const scheme = mirror.protocol === 'http:' ? 'http' : 'https';
      const mirrorHost = mirror.host.replaceAll("'", "'\\''");
      lines.push(
        `            '${scheme}|${mirrorHost}'${mirror.port === '' ? `|'${scheme}|${mirror.hostname}:${scheme === 'http' ? '80' : '443'}'` : ''})`,
        `              printf 'username=x-access-token\\npassword=%s\\n' "$${origin.tokenEnv}" ;;`,
      );
    }
  }
  lines.push('            *) exit 0 ;;');
  lines.push('          esac');
  lines.push('          SMOO_CARGO_HELPER_EOF');
  lines.push('          chmod 700 "$helper"');
  lines.push('          {');
  lines.push('            echo "CARGO_NET_GIT_FETCH_WITH_CLI=true"');
  lines.push('            # An empty value clears any ambient credential helper (osxkeychain,');
  lines.push('            # store) so a successful fetch cannot persist the token anywhere.');
  lines.push('            echo "GIT_CONFIG_KEY_0=credential.helper"');
  lines.push('            echo "GIT_CONFIG_VALUE_0="');
  lines.push('            echo "GIT_CONFIG_KEY_1=credential.helper"');
  lines.push('            echo "GIT_CONFIG_VALUE_1=$helper"');
  lines.push('            # Cargo honors proxy env natively; git does not read it. Mirror the');
  lines.push('            # runner proxy into git config so Cargo git fetches take the same');
  lines.push('            # route instead of failing to connect. Absent on direct networks,');
  lines.push('            # so this block is a no-op there.');
  lines.push('            idx=2');
  lines.push('            https_proxy_value="${HTTPS_PROXY:-${https_proxy:-}}"');
  lines.push('            http_proxy_value="${HTTP_PROXY:-${http_proxy:-${https_proxy_value:-}}}"');
  lines.push('            if [ -n "$https_proxy_value" ]; then');
  lines.push('              echo "GIT_CONFIG_KEY_${idx}=https.proxy"');
  lines.push('              echo "GIT_CONFIG_VALUE_${idx}=${https_proxy_value}"');
  lines.push('              idx=$((idx + 1))');
  lines.push('            fi');
  lines.push('            if [ -n "$http_proxy_value" ]; then');
  lines.push('              echo "GIT_CONFIG_KEY_${idx}=http.proxy"');
  lines.push('              echo "GIT_CONFIG_VALUE_${idx}=${http_proxy_value}"');
  lines.push('              idx=$((idx + 1))');
  lines.push('            fi');
  lines.push('            # Internal mirrors rewrite the declared origin prefix so runners');
  lines.push('            # that cannot reach the public URL fetch over guest networking.');
  if (origins.some((origin) => origin.sshOrigins !== undefined)) {
    lines.push('            # The same forge over SSH, as Cargo and uv pin it: git matches');
    lines.push('            # insteadOf values as literal URL prefixes, so every declared');
    lines.push('            # spelling rewrites onto the mirror on its own line.');
  }
  for (const origin of origins) {
    if (origin.internalMirror === undefined) {
      continue;
    }
    const mirrorUrl = new URL(origin.internalMirror);
    const mirrorBase = `${mirrorUrl.protocol}//${mirrorUrl.host}/`.replaceAll("'", "'\\''");
    for (const prefix of mirrorRewritePrefixes(origin)) {
      lines.push('            echo "GIT_CONFIG_KEY_${idx}=url.' + mirrorBase + '.insteadOf"');
      lines.push('            echo "GIT_CONFIG_VALUE_${idx}=' + prefix.replaceAll("'", "'\\''") + '"');
      lines.push('            idx=$((idx + 1))');
    }
  }
  lines.push('            echo "GIT_CONFIG_COUNT=${idx}"');
  lines.push('          } >> "$GITHUB_ENV"');
  return lines;
}

/**
 * Every URL prefix a mirrored origin rewrites from: the declared https origin
 * first, then each declared SSH spelling of the same forge. One `insteadOf`
 * value per prefix, because git matches them as literal prefixes and derives
 * no spelling from another. Rendering runs after `normalizeCargoCredentials`,
 * which parsed and refused every malformed entry.
 */
function mirrorRewritePrefixes(origin: PackageCargoGitOrigin): string[] {
  const originUrl = new URL(origin.origin);
  return [
    `${originUrl.protocol}//${originUrl.host}/`,
    ...(origin.sshOrigins ?? []).map((spelling) => sshRewritePrefix(new URL(spelling))),
  ];
}

/**
 * The SSH spelling as a rewritable prefix: userinfo is part of the spelling
 * git matches, and the trailing slash keeps `ssh://host:2223` from also
 * rewriting `ssh://host:22230/`.
 */
function sshRewritePrefix(spelling: URL): string {
  return `ssh://${spelling.username === '' ? '' : `${spelling.username}@`}${spelling.host}/`;
}

function distinctCargoTokenEnvs(config: PackageCargoCredentialsConfig): string[] {
  const seen = new Set<string>();
  for (const env of config.registryTokenEnvs ?? []) {
    seen.add(env);
  }
  for (const origin of config.gitOrigins ?? []) {
    seen.add(origin.tokenEnv);
  }
  return [...seen];
}

/** Mechanism errors surface at managed-file render time, never inside CI. */
export function normalizeCargoCredentials(config: PackageCargoCredentialsConfig): PackageCargoCredentialsConfig {
  for (const env of config.registryTokenEnvs ?? []) {
    if (!/^CARGO_REGISTRIES_[A-Z0-9_]+_TOKEN$/.test(env)) {
      throw new Error(
        `smoo.github.cargoCredentials needs upper-case secret env names matching CARGO_REGISTRIES_<NAME>_TOKEN for registryTokenEnvs, got ${JSON.stringify(env)}`,
      );
    }
  }
  const hosts = new Set<string>();
  const sshPrefixes = new Set<string>();
  for (const origin of config.gitOrigins ?? []) {
    const host = new URL(normalizeCargoGitOrigin(origin).origin).host;
    if (hosts.has(host)) {
      throw new Error(`smoo.github.cargoCredentials repeats git origin host ${host}; declare one token per origin`);
    }
    hosts.add(host);
    for (const spelling of origin.sshOrigins ?? []) {
      // One spelling, one rewrite, whole config: a repeat renders two
      // identical insteadOf keys and git silently keeps the last one read.
      const prefix = sshRewritePrefix(assertSshOrigin(spelling));
      if (sshPrefixes.has(prefix)) {
        throw new Error(
          `smoo.github.cargoCredentials repeats the sshOrigins spelling ${prefix}; declare each spelling once, on the origin whose mirror rewrites it`,
        );
      }
      sshPrefixes.add(prefix);
    }
  }
  if ((config.registryTokenEnvs?.length ?? 0) === 0 && (config.gitOrigins?.length ?? 0) === 0) {
    throw new Error(
      'smoo.github.cargoCredentials needs at least one gitOrigins or registryTokenEnvs entry, got an empty configuration',
    );
  }
  return config;
}

function normalizeCargoGitOrigin(origin: PackageCargoGitOrigin): PackageCargoGitOrigin {
  let url: URL | null = null;
  try {
    url = origin.origin === undefined ? null : new URL(origin.origin);
  } catch {
    url = null;
  }
  if (
    url === null ||
    url.protocol !== 'https:' ||
    url.pathname !== '/' ||
    url.username !== '' ||
    url.password !== '' ||
    url.search !== '' ||
    url.hash !== ''
  ) {
    throw new Error(
      `smoo.github.cargoCredentials gitOrigins entry needs a credential-free https origin without a path, got ${JSON.stringify(origin.origin)}`,
    );
  }
  if (!/^[A-Z_][A-Z0-9_]*$/.test(origin.tokenEnv)) {
    throw new Error(
      `smoo.github.cargoCredentials gitOrigins entry needs an upper-case secret env name, got ${JSON.stringify(origin.tokenEnv)}`,
    );
  }
  if (origin.internalMirror !== undefined) {
    let mirror: URL | null = null;
    try {
      mirror = new URL(origin.internalMirror);
    } catch {
      mirror = null;
    }
    if (
      mirror === null ||
      (mirror.protocol !== 'https:' && mirror.protocol !== 'http:') ||
      mirror.pathname !== '/' ||
      mirror.username !== '' ||
      mirror.password !== '' ||
      mirror.search !== '' ||
      mirror.hash !== ''
    ) {
      throw new Error(
        `smoo.github.cargoCredentials gitOrigins entry needs a credential-free http(s) internalMirror origin without a path, got ${JSON.stringify(origin.internalMirror)}`,
      );
    }
    if (mirror.host === url.host && mirror.protocol === url.protocol) {
      throw new Error(
        `smoo.github.cargoCredentials gitOrigins entry rewrites ${origin.origin} to itself; drop internalMirror or point it at the internal mirror`,
      );
    }
  }
  if (origin.sshOrigins !== undefined) {
    if (origin.internalMirror === undefined) {
      throw new Error(
        `smoo.github.cargoCredentials gitOrigins entry declares sshOrigins for ${origin.origin} without an internalMirror to rewrite them onto; an SSH spelling is a rewrite source and nothing else`,
      );
    }
    if (origin.sshOrigins.length === 0) {
      throw new Error(
        `smoo.github.cargoCredentials gitOrigins entry for ${origin.origin} declares an empty sshOrigins list; name every SSH spelling a lockfile can carry, or omit the field`,
      );
    }
    for (const spelling of origin.sshOrigins) {
      assertSshOrigin(spelling);
    }
  }
  return origin;
}

/**
 * One declared SSH spelling, parsed. Credential-free and path-free: a
 * password here would be a secret committed in package.json, and a path
 * would rewrite a single repository instead of the forge. scp syntax
 * (`git@host:org/repo.git`) is no URL, so git cannot rewrite it from a Cargo
 * or uv pin at all; the `ssh://` spelling is the one to declare.
 */
function assertSshOrigin(spelling: string): URL {
  let ssh: URL | null = null;
  try {
    ssh = new URL(spelling);
  } catch {
    ssh = null;
  }
  if (
    ssh === null ||
    ssh.protocol !== 'ssh:' ||
    ssh.host === '' ||
    (ssh.pathname !== '' && ssh.pathname !== '/') ||
    ssh.password !== '' ||
    ssh.search !== '' ||
    ssh.hash !== ''
  ) {
    throw new Error(
      `smoo.github.cargoCredentials gitOrigins entry needs credential-free ssh:// sshOrigins without a path, got ${JSON.stringify(spelling)}`,
    );
  }
  return ssh;
}

/**
 * The same-repo gate as prettier folds it: the raw single line exceeds the
 * print width, so the generator emits the folded form itself to keep the
 * checked-in managed output byte-identical across regeneration and hooks.
 */
export const SAME_REPO_GATE_FOLDED = [
  '        if:',
  "          ${{ github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository",
  '          }}',
];

/** The first push branch is the one whose pushes deploy the staging stage. */
function stagingPushBranch(options: CiWorkflowDefinitionOptions): string {
  return options.pushBranches[0];
}

/**
 * A YAML scalar for operator-configured names. Plain-safe names stay bare so
 * generated output stays readable; anything YAML-significant is double-quoted
 * (JSON string syntax is valid YAML for these values) rather than rejecting
 * names the platform itself accepts.
 */
function yamlScalar(value: string): string {
  if (/^[A-Za-z0-9_][A-Za-z0-9_./-]*$/.test(value) && Bun.YAML.parse(value) === value) {
    return value;
  }
  return JSON.stringify(value);
}

/**
 * A `refs/heads/<branch>` GitHub expression literal. Single quotes double
 * inside expression literals; the branch itself is never restricted here
 * because git permits quotes in ref names.
 */
function stagingRefLiteral(options: CiWorkflowDefinitionOptions): string {
  return `'refs/heads/${stagingPushBranch(options).replaceAll("'", "''")}'`;
}

function environmentLine(name: string | undefined): string {
  return name ? `    environment: ${yamlScalar(name)}\n` : '';
}

/**
 * The step-scoped `env:` block for a deploy step. Step scope is the property
 * being rendered, not an accident of indentation: job env would hand the
 * deployment credentials to every other step in that job — checkout, setup,
 * build, and in the publish workflow the pending-release repair and the npm
 * publish itself.
 *
 * An explicit declaration wins over the provider default, so a repo whose
 * Cloudflare credentials live under different repository secret names says so
 * once in `deploySecrets` instead of being overridden here.
 */
export function deployStepSecretEnvLines(config: DeployStepSecretConfig): string[] {
  const cloudflare: Record<string, string> =
    config.deployProvider === 'cloudflare'
      ? { CLOUDFLARE_API_TOKEN: 'CLOUDFLARE_API_TOKEN', CLOUDFLARE_ACCOUNT_ID: 'CLOUDFLARE_ACCOUNT_ID' }
      : {};
  return secretEnvLines({ ...cloudflare, ...config.deploySecrets });
}

/** A step-level `env:` block mapping env var names to repository secrets; nothing when there are none. */
function secretEnvLines(secrets: Record<string, string>): string[] {
  const entries = Object.entries(secrets);
  if (entries.length === 0) return [];
  return [
    '        env:',
    ...entries.map(([name, secret]) => `          ${name}: ${githubExpression(`secrets.${secret}`)}`),
  ];
}

/** Optional lines as a newline-terminated block, or nothing, so templates never gain a blank line. */
function renderOptionalLines(lines: string[]): string {
  return lines.length > 0 ? `${lines.join('\n')}\n` : '';
}

function nxSmartStep(step: CiWorkflowStep, target: string, name: string): string[] {
  return [
    `      - name: ${step.name}`,
    `        run: smoo github-ci nx-smart --target ${target} --name "${name}" --step ${step.number}`,
  ];
}

function renderYamlList(values: string[], spaces: number): string {
  const indent = ' '.repeat(spaces);
  return values.map((value) => `${indent}- ${value}`).join('\n');
}
/**
 * Top-level step numbers for the jobs after Validate. Checkout is always 2 (1
 * is GitHub's automatic "Set up job"); every preflight the main job runs also
 * runs here before SetupDevenv, so the middle and cleanup anchors shift with
 * the configuration instead of baking a fixed step number.
 *
 * `middle` is the FIRST of the job's own steps. A job with more than one — the
 * cross-test job downloads before it runs — passes how many it has, so the
 * cleanup anchor stays the number of the step GitHub will actually report.
 */
interface FollowUpStepNumbers {
  cargo: number | undefined;
  sources: number | undefined;
  setup: number;
  middle: number;
  cleanup: number;
}

function followUpStepNumbers(options: CiWorkflowDefinitionOptions, middleSteps = 1): FollowUpStepNumbers {
  let number = 3;
  const cargo = options.cargoCredentials !== undefined ? number++ : undefined;
  const sources = options.sourceCheckouts?.length ? number++ : undefined;
  const setup = number++;
  const middle = number;
  number += middleSteps;
  const cleanup = number++;
  return { cargo, sources, setup, middle, cleanup };
}

/** Checkout plus every preflight the main job runs, numbered so later anchors track the configuration. */
function followUpSetupSteps(options: CiWorkflowDefinitionOptions, numbers: FollowUpStepNumbers): CiWorkflowStep[] {
  const steps: CiWorkflowStep[] = [{ kind: CiWorkflowStepKind.Checkout, name: '📥 Checkout', number: 2 }];
  if (numbers.cargo !== undefined && options.cargoCredentials !== undefined) {
    steps.push({ kind: CiWorkflowStepKind.CargoCredentials, name: 'Prepare Cargo credentials', number: numbers.cargo });
  }
  if (numbers.sources !== undefined) {
    steps.push({
      kind: CiWorkflowStepKind.SourceCheckouts,
      name: '📦 Check out sibling sources',
      number: numbers.sources,
    });
  }
  steps.push({ kind: CiWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv', number: numbers.setup });
  return steps;
}

/** The final cache-save step, at its configured anchor. */
function followUpCleanupStep(numbers: FollowUpStepNumbers): CiWorkflowStep[] {
  return [{ kind: CiWorkflowStepKind.SaveNixDevenv, name: '🧹 Cleanup and cache Nix/devenv', number: numbers.cleanup }];
}

/**
 * How long the darwin execution job may take, download included. Generous
 * because the archive is the whole workspace's test binaries, and bounded
 * because the runner behind `macosRunsOn` may be one person's laptop: an
 * unreachable runner queues, and this number is when the run says so.
 */
const CROSS_TEST_JOB_TIMEOUT_MINUTES = 30;

/** Rust names every Apple-desktop triple this way; iOS and simulator do not. */
const DARWIN_TRIPLE_SUFFIX = '-apple-darwin';

/** One artifact carrying every triple's archive, scoped to the run that built it. */
const CROSS_TEST_ARCHIVE_ARTIFACT = `cross-test-archives-${githubExpression('github.run_id')}`;

/**
 * Executes the cross-built test binaries on a machine where they are native.
 *
 * This job COMPILES NOTHING, and cannot: its only Rust input is the archive the
 * Validate job uploaded, and `nextest run --archive-file` extracts prebuilt
 * binaries rather than invoking cargo (measured: zero cargo invocations). It
 * needs the checkout for the workspace `--workspace-remap .` points at and for
 * Nx to resolve the target, and setup-devenv for `nx` and `cargo-nextest`
 * themselves — the repository's ordinary shell, not a cross toolchain, no SDK,
 * no `rustup target add`.
 *
 * `needs: main` and a success gate rather than `!cancelled()`: an archive that
 * was never uploaded cannot be executed, and a job that starts anyway would
 * fail on a missing artifact and report it as a test failure.
 *
 * Only `*-apple-darwin` archives run here, because macOS runner labels are the
 * only native runner this declaration names. A repository declaring some other
 * foreign triple still gets its archive BUILT — which proves that triple
 * compiles — and no execution job, which is the honest rendering of having
 * nowhere to run it.
 */
function renderCrossTestExecutionJob(options: CiWorkflowDefinitionOptions): string {
  const archives = darwinCrossTestArchives(options);
  if (archives.length === 0) return '';
  const numbers = followUpStepNumbers(options, 2);
  const middleSteps: CiWorkflowStep[] = [
    {
      kind: CiWorkflowStepKind.DownloadCrossTestArchives,
      name: '📥 Download cross-target test archives',
      number: numbers.middle,
    },
    { kind: CiWorkflowStepKind.CrossTests, name: '🧪 Cross-Target Unit Tests', number: numbers.middle + 1 },
  ];
  return `
  macos-cross-tests:
    name: Unit Tests (${archives.map((archive) => archive.triple).join(', ')})
    needs: main
${options.macosRunsOn === undefined ? '    runs-on: macos-latest' : renderRunsOnLine(options.macosRunsOn)}
    timeout-minutes: ${CROSS_TEST_JOB_TIMEOUT_MINUTES}
    if: \${{ needs.main.result == 'success' }}
    env:
      GH_TOKEN: \${{ github.token }}
${remoteCacheJobEnvLines(options.remoteCache)}${cargoCredentialJobEnvLines(options.cargoCredentials)}${privateNpmReadTokenJobEnv(options)}    steps:
${renderCiWorkflowSteps(followUpSetupSteps(options, numbers), options)}
${renderCiWorkflowSteps(middleSteps, options)}
${renderCiWorkflowSteps(followUpCleanupStep(numbers), options)}`;
}

/**
 * The declared archives, checked against the naming the Nx graph would have
 * produced. A path that is not `cargoCrossTestArchiveFile(triple)` under some
 * project root cannot have come from that target, so it is refused here rather
 * than uploaded as an artifact nothing on the other side can find.
 */
function crossTestArchives(options: CiWorkflowDefinitionOptions): CiCrossTestArchive[] {
  return (options.crossTestArchives ?? []).map((archive) => {
    const suffix = cargoCrossTestArchiveFile(archive.triple);
    const path = archive.path.split('\\').join('/');
    if (!/^[a-z0-9][a-z0-9._-]*$/.test(archive.triple)) {
      throw new Error(`smoo cross test archive triple must be a target triple, got ${JSON.stringify(archive.triple)}`);
    }
    if (path !== suffix && !path.endsWith(`/${suffix}`)) {
      throw new Error(
        `smoo cross test archive for ${archive.triple} must be a repository-relative ${suffix}, got ${archive.path}`,
      );
    }
    if (path.startsWith('/') || path.split('/').includes('..')) {
      throw new Error(`smoo cross test archive path must stay inside the repository, got ${archive.path}`);
    }
    return { triple: archive.triple, path };
  });
}

function crossArchiveTargetNames(options: CiWorkflowDefinitionOptions): string[] {
  return crossTestArchives(options).map((archive) => cargoCrossTestArchiveTargetName(archive.triple));
}

/**
 * The declared preflight, as lines. A declaration with nothing in it is a
 * mechanism error, and it surfaces here at render time rather than as a step
 * whose `run:` block is empty.
 */
function crossToolchainPreflight(options: CiWorkflowDefinitionOptions): string[] {
  const preflight = options.platformProducer?.preflight ?? '';
  if (preflight.trim().length === 0) {
    throw new Error('smoo.github.platformProducer requires a nonempty toolchain preflight command.');
  }
  return preflight.split('\n');
}

/** The producer environment, step-scoped, quoted exactly as the publish workflow quotes it. */
function platformProducerStepEnvLines(options: CiWorkflowDefinitionOptions): string[] {
  const entries = Object.entries(options.platformProducer?.env ?? {});
  if (entries.length === 0) return [];
  return ['        env:', ...entries.map(([name, value]) => `          ${name}: ${JSON.stringify(value)}`)];
}

function darwinCrossTestArchives(options: CiWorkflowDefinitionOptions): CiCrossTestArchive[] {
  return crossTestArchives(options).filter((archive) => archive.triple.endsWith(DARWIN_TRIPLE_SUFFIX));
}

function darwinCrossTestTargetNames(options: CiWorkflowDefinitionOptions): string[] {
  return darwinCrossTestArchives(options).map((archive) => cargoCrossTestTargetName(archive.triple));
}

/**
 * Where the download lands, and the artifact's root by construction: the upload
 * step carries exactly these archives, and upload-artifact roots an artifact at
 * the least common ancestor of the files it uploaded.
 *
 * One cargo workspace's triples all share `target/nextest` and travel together.
 * Two workspaces that each declare a darwin triple do not: their common
 * ancestor is above the directory the runner targets read from, so the pair is
 * refused here instead of restoring both archives one directory too high.
 */
function crossTestArchiveDirectory(options: CiWorkflowDefinitionOptions): string {
  const directories = new Set(
    darwinCrossTestArchives(options).map((archive) => archive.path.slice(0, archive.path.lastIndexOf('/') + 1) || './'),
  );
  if (directories.size > 1) {
    throw new Error(
      `smoo cross test archives must share one directory to travel as one artifact, got ${[...directories].join(', ')}`,
    );
  }
  const [directory] = [...directories];
  return (directory ?? './').replace(/\/$/, '');
}

function renderE2eDeploymentJob(options: CiWorkflowDefinitionOptions): string {
  if (!options.e2eDeployment) return '';
  const numbers = followUpStepNumbers(options);
  return `
  e2e-deployment:
    name: E2E Tests (Deployed Stage)
    needs: main
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 15
${environmentLine(options.environments?.staging)}    if: \${{ needs.main.result == 'success' && needs.main.outputs.deployment-stage != '' }}
    env:
      GH_TOKEN: \${{ github.token }}
${remoteCacheJobEnvLines(options.remoteCache)}${cargoCredentialJobEnvLines(options.cargoCredentials)}${privateNpmReadTokenJobEnv(options)}    steps:
${renderCiWorkflowSteps(followUpSetupSteps(options, numbers), options)}
      # Step ${numbers.middle}
      - name: E2E Tests (Deployed Stage)
${renderOptionalLines(secretEnvLines(options.e2eSecrets ?? {}))}        # prettier-ignore
        run: smoo github-ci nx-smart --target e2e-deployment --mode run-many --stage "\${{ needs.main.outputs.deployment-stage }}" --stream-output --name "E2E Tests (Deployed Stage)" --step ${numbers.middle}

${renderCiWorkflowSteps(followUpCleanupStep(numbers), options)}`;
}

/**
 * Deploys the tagged projects to production on a push to the staging push branch, once Validate and the e2e job
 * succeed. `!cancelled()` lets the job evaluate its own gate when the e2e job was skipped rather than inheriting a skip.
 * A `'skipped'` e2e result is allowed so a repo without e2e-deployment projects still deploys production.
 */
function renderProductionDeployJob(options: CiWorkflowDefinitionOptions): string {
  if (!options.deploy || !options.productionOnPush) return '';
  const needs = options.e2eDeployment ? '[main, e2e-deployment]' : '[main]';
  const e2eGate = options.e2eDeployment
    ? " && (needs.e2e-deployment.result == 'success' || needs.e2e-deployment.result == 'skipped')"
    : '';
  const numbers = followUpStepNumbers(options);
  return `
  deploy-production:
    name: Deploy Production
    needs: ${needs}
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 30
    # prettier-ignore
    if: \${{ !cancelled() && github.event_name == 'push' && github.ref == ${stagingRefLiteral(options)} && needs.main.result == 'success'${e2eGate} }}
${environmentLine(options.environments?.production)}    env:
      GH_TOKEN: \${{ github.token }}
${remoteCacheJobEnvLines(options.remoteCache)}${cargoCredentialJobEnvLines(options.cargoCredentials)}${privateNpmReadTokenJobEnv(options)}    steps:
${renderCiWorkflowSteps(followUpSetupSteps(options, numbers), options)}
      # Step ${numbers.middle}
      - name: 🚀 Deploy Production
${renderOptionalLines(deployStepSecretEnvLines(options))}        # prettier-ignore
        run: smoo github-ci nx-deploy --stage production --mode run-many --select-tag ${PRODUCTION_PUSH_DEPLOY_TAG} --name "Deploy Production" --step ${numbers.middle}

${renderCiWorkflowSteps(followUpCleanupStep(numbers), options)}`;
}

export function artifactStepLines(
  provider: PackageSmooGithub['actionsProvider'],
  name: string,
  kind: 'upload' | 'download',
  inputs: readonly string[],
  condition = 'success()',
): string[] {
  // GitHub resolves every action before evaluating step conditions, so a
  // Forgejo absolute action URL must never appear in a GitHub workflow.
  // Forgejo 15 supports v4 artifacts; its pinned clients remove the GHES gate.
  const action =
    provider === 'forgejo'
      ? kind === 'upload'
        ? 'https://code.forgejo.org/forgejo/upload-artifact@cb8afe72b42edc798abfb8fcb556cf660d894245'
        : 'https://code.forgejo.org/forgejo/download-artifact@769f970437aa3291b13f35dc23fc87967d7fb19f'
      : kind === 'upload'
        ? 'actions/upload-artifact@v7.0.1'
        : 'actions/download-artifact@v8.0.1';
  return [
    `      - name: ${name}`,
    `        if: ${condition}`,
    `        uses: ${action}`,
    '        with:',
    ...inputs.map((input) => `          ${input}`),
  ];
}
