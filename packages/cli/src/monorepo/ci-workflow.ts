/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

import type {
  PackageCargoCredentialsConfig,
  PackageCargoGitOrigin,
  PackagePrivateNpmConfig,
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

export interface CiWorkflowDefinitionOptions {
  deploy: boolean;
  browserTests: boolean;
  e2eDeployment: boolean;
  deployProvider?: 'cloudflare';
  pushBranches: string[];
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
  return `${renderCiWorkflowHeader(options)}${renderCiWorkflowSteps(steps, options)}${renderE2eDeploymentJob(options)}`;
}

function renderCiWorkflowHeader(options: CiWorkflowDefinitionOptions): string {
  return `name: CI

on:
  push:
    branches:
${renderYamlList(options.pushBranches, 6)}
  pull_request:

permissions:
  # actions:write lets the drift step dispatch the managed-files workflow.
  actions: write
  contents: read
${options.deploy ? '  deployments: write\n' : ''}  statuses: write

concurrency:
  group: \${{ github.workflow }}-\${{ github.ref }}
  cancel-in-progress: true

defaults:
  run:
    working-directory: tooling/direnv

jobs:
  main:
    name: Validate
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 45
${
  options.privateNpm?.readTokenEnv || options.cargoCredentials !== undefined
    ? `    # Fork PRs receive no secrets; private dependency installs cannot run there.
    if: \${{ github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository }}
`
    : ''
}${
  options.e2eDeployment
    ? `    outputs:
      deployment-stage: ${githubExpression('steps.deploy.outputs.stage')}
`
    : ''
}    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}
${cargoCredentialJobEnvLines(options.cargoCredentials)}${privateNpmReadTokenJobEnv(options)}    steps:
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
        "            (github.event_name == 'push' && github.ref == 'refs/heads/private')",
        '          }}',
        ...deployEnvLines(options),
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
      return artifactStepLines(step.name, 'upload', [
        'name: trace-results-${{ github.run_id }}',
        'path: packages/*/.cache/trace-results.db*',
        'if-no-files-found: ignore',
        'retention-days: 14',
        'include-hidden-files: true',
      ], 'always()');
    case CiWorkflowStepKind.SaveNixDevenv:
      return [
        `      - name: ${step.name}`,
        '        # always() still saves after a red job. Nix NAR is ephemeral-only;',
        '        # devenv eval-cache also saves on host-nix when setup missed.',
        '        if: always()',
        '        uses: ./.github/actions/save-nix-devenv',
        '        with:',
        '          nix-cache-hit: ${{ steps.setup.outputs.nix-cache-hit }}',
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
 * `CARGO_NET_GIT_FETCH_WITH_CLI` so Cargo shells out to git.
 * The helper reads the environment at call time and answers only for declared origins.
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
  lines.push('          [ "$protocol" = https ] || exit 0');
  lines.push('          case "$host" in');
  for (const origin of origins) {
    const url = new URL(origin.origin);
    const host = url.host.replaceAll("'", "'\\''");
    lines.push(
      `            '${host}'${url.port === '' ? `|'${host}:443'` : ''})`,
      `              printf 'username=x-access-token\\npassword=%s\\n' "$${origin.tokenEnv}" ;;`,
    );
  }
  lines.push('            *) exit 0 ;;');
  lines.push('          esac');
  lines.push('          SMOO_CARGO_HELPER_EOF');
  lines.push('          chmod 700 "$helper"');
  lines.push('          {');
  lines.push('            echo "CARGO_NET_GIT_FETCH_WITH_CLI=true"');
  lines.push('            echo "GIT_CONFIG_COUNT=2"');
  lines.push('            # An empty value clears any ambient credential helper (osxkeychain,');
  lines.push('            # store) so a successful fetch cannot persist the token anywhere.');
  lines.push('            echo "GIT_CONFIG_KEY_0=credential.helper"');
  lines.push('            echo "GIT_CONFIG_VALUE_0="');
  lines.push('            echo "GIT_CONFIG_KEY_1=credential.helper"');
  lines.push('            echo "GIT_CONFIG_VALUE_1=$helper"');
  lines.push('          } >> "$GITHUB_ENV"');
  return lines;
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
  for (const origin of config.gitOrigins ?? []) {
    const host = new URL(normalizeCargoGitOrigin(origin).origin).host;
    if (hosts.has(host)) {
      throw new Error(`smoo.github.cargoCredentials repeats git origin host ${host}; declare one token per origin`);
    }
    hosts.add(host);
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
  return origin;
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

function deployEnvLines(options: CiWorkflowDefinitionOptions): string[] {
  if (options.deployProvider !== 'cloudflare') {
    return [];
  }
  return [
    '        env:',
    '          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}',
    '          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}',
  ];
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

function renderE2eDeploymentJob(options: CiWorkflowDefinitionOptions): string {
  if (!options.e2eDeployment) return '';
  return `
  e2e-deployment:
    name: E2E Tests (Deployed Stage)
    needs: main
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 15
    if: \${{ needs.main.result == 'success' && needs.main.outputs.deployment-stage != '' }}
    env:
      GH_TOKEN: \${{ github.token }}
${privateNpmReadTokenJobEnv(options)}    steps:
      # Step 1: GitHub adds "Set up job" automatically
      # Step 2
      - name: 📥 Checkout
        uses: actions/checkout@v6.0.2
        with:
          filter: blob:none
          fetch-depth: 0

      # Step 3. Composite action internals do not affect top-level job step anchors.
      - name: 🧱 Setup Nix/devenv
        id: setup
        uses: ./.github/actions/setup-devenv

      # Step 4
      - name: E2E Tests (Deployed Stage)
        # prettier-ignore
        run: smoo github-ci nx-smart --target e2e-deployment --mode run-many --stage "\${{ needs.main.outputs.deployment-stage }}" --stream-output --name "E2E Tests (Deployed Stage)" --step 4

      # Step 5
      - name: 🧹 Cleanup and cache Nix/devenv
        if: always()
        uses: ./.github/actions/save-nix-devenv
        with:
          nix-cache-hit: \${{ steps.setup.outputs.nix-cache-hit }}
          devenv-cache-hit: \${{ steps.setup.outputs.devenv-cache-hit }}
`;
}

export function artifactStepLines(name: string, kind: 'upload' | 'download', inputs: readonly string[], condition = 'success()'): string[] {
  const github = "github.server_url == 'https://github.com' || endsWith(github.api_url, '/api/v3')";
  // Forgejo 15 implements the v4 artifact protocol. Upstream clients reject
  // non-GitHub hosts as GHES; these pinned Forgejo forks remove that host gate.
  const providers = [
    { condition: `(${github})`, action: kind === 'upload' ? 'actions/upload-artifact@v7.0.1' : 'actions/download-artifact@v8.0.1', suffix: '' },
    { condition: `!(${github})`, action: kind === 'upload'
      ? 'https://code.forgejo.org/forgejo/upload-artifact@cb8afe72b42edc798abfb8fcb556cf660d894245'
      : 'https://code.forgejo.org/forgejo/download-artifact@769f970437aa3291b13f35dc23fc87967d7fb19f', suffix: ' (Forgejo)' },
  ];
  return providers.flatMap((provider) => [
    `      - name: ${name}${provider.suffix}`,
    `        if: (${condition}) && ${provider.condition}`,
    `        uses: ${provider.action}`,
    '        with:',
    ...inputs.map((input) => `          ${input}`),
  ]);
}
