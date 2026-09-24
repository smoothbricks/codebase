/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

import type {
  PackageCargoCredentialsConfig,
  PackagePrivateNpmConfig,
  PackageSmooGithubEnvironments,
  PackageSourceCheckoutConfig,
} from '../workspace-manifest.js';
import {
  CiWorkflowStepKind,
  cargoCredentialJobEnvLines,
  cargoCredentialStepLines,
  cloudflareCredentialEnvLines,
  environmentLine,
  privateNpmReadTokenJobEnv,
  sourceCheckoutsStepLines,
} from './ci-workflow.js';
import { renderRunsOnLine } from './github-runs-on.js';

export interface PrPreviewCleanupWorkflowOptions {
  runsOn?: string | string[];
  /**
   * Declared private-npm opt-in. The job runs setup-devenv, whose dependency
   * install refuses in CI when a declared registry credential is absent from
   * the job environment, so the read token rides the job env exactly as it
   * does on every CI job that runs setup-devenv. The remote-cache token stays
   * out: shell entry defers it, and nothing in this job runs Nx.
   */
  privateNpm?: PackagePrivateNpmConfig;
  /**
   * Declared sibling source checkouts, cloned before setup-devenv as on every
   * CI job that runs it: the devenv shell and the install resolve path
   * dependencies on them.
   */
  sourceCheckouts?: PackageSourceCheckoutConfig[];
  /**
   * Declared Cargo credentials, rendered as on every CI job that runs
   * setup-devenv: the tokens at job level, where setup resolves the declared
   * secrets, and the credential step before the sibling clones. That step is
   * not only for Cargo: later git commands in the job read the git
   * configuration it writes to GITHUB_ENV (the credential helper, the runner
   * proxy, the rewrite from each origin to its internal mirror), so a sibling
   * clone declared without `tokenEnv` takes the mirror as it does in CI. One
   * declared with `tokenEnv` sets GIT_CONFIG_COUNT=1 for its own auth header,
   * which replaces that configuration: it gets no helper, proxy or rewrite.
   */
  cargoCredentials?: PackageCargoCredentialsConfig;
  /**
   * The cleanup deletes what the stage deploy created, in the account the
   * deploy's CLOUDFLARE_ACCOUNT_ID names, so it must read the credentials
   * that deploy read. The job therefore runs in the staging Environment the
   * stage deploy runs in, whose secrets shadow the repository's, and reads
   * the Cloudflare credentials through the same `deploySecrets` mapping.
   */
  environments?: PackageSmooGithubEnvironments;
  /** Deploy-step secrets; only the two Cloudflare credentials reach the cleanup. */
  deploySecrets?: Record<string, string>;
}

export function renderPrPreviewCleanupWorkflowYaml(options: PrPreviewCleanupWorkflowOptions = {}): string {
  return `name: PR Preview Cleanup

on:
  pull_request:
    types: [closed]

permissions:
  contents: read

jobs:
  cleanup:
    name: Cleanup PR environment
    if: github.event.pull_request.head.repo.full_name == github.repository
${renderRunsOnLine(options.runsOn)}
    timeout-minutes: 15
${environmentLine(options.environments?.staging)}    defaults:
      run:
        working-directory: tooling/direnv
${setupJobEnv(options)}    steps:
      - name: Checkout
        uses: actions/checkout@v6.0.2
        with:
          filter: blob:none
          fetch-depth: 1
${preflightSteps(options)}
      - name: Setup Nix/devenv
        uses: ./.github/actions/setup-devenv

      - name: Cleanup PR environment
${cloudflareCredentialEnvLines(options.deploySecrets).join('\n')}
        run: smoo wrangler cleanup-pr --pr \${{ github.event.pull_request.number }}
`;
}

/** The job env setup-devenv needs, as the CI jobs spell it; nothing for a repository that declares no credential. */
function setupJobEnv(options: PrPreviewCleanupWorkflowOptions): string {
  const tokenLines = `${cargoCredentialJobEnvLines(options.cargoCredentials)}${privateNpmReadTokenJobEnv(options)}`;
  return tokenLines === '' ? '' : `    env:\n${tokenLines}`;
}

/**
 * The CI jobs' steps between checkout and setup-devenv, in their order and each
 * set off by blank lines: the Cargo credential step, then the sibling-source
 * clones. Nothing when neither is declared.
 */
function preflightSteps(options: PrPreviewCleanupWorkflowOptions): string {
  const steps: string[][] = [];
  if (options.cargoCredentials !== undefined) {
    const step = { kind: CiWorkflowStepKind.CargoCredentials, name: 'Prepare Cargo credentials', number: 0 };
    steps.push(cargoCredentialStepLines(step, options.cargoCredentials));
  }
  if (options.sourceCheckouts?.length) {
    const step = { kind: CiWorkflowStepKind.SourceCheckouts, name: 'Check out sibling sources', number: 0 };
    steps.push(sourceCheckoutsStepLines(step, options.sourceCheckouts));
  }
  return steps.map((lines) => `\n${lines.join('\n')}\n`).join('');
}
