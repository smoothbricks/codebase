/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

import {
  LINUX_PLATFORM_TARGET_GLOBS,
  MACOS_PLATFORM_TARGET_GLOBS,
} from '@smoothbricks/nx-plugin/workspace-config-policy';
import { makeModuleSynchronized } from 'make-synchronized';
import type * as PrettierModule from 'prettier';
import type { Options as PrettierOptions } from 'prettier';
import { isSmoothBricksCodebasePackageName } from '../lib/cli-package.js';
import type {
  PackageCargoCredentialsConfig,
  PackagePrivateNpmConfig,
  PackageSmooGithub,
  PackageSourceCheckoutConfig,
} from '../lib/json.js';
import {
  artifactStepLines,
  CiWorkflowStepKind,
  cargoCredentialJobEnvLines,
  cargoCredentialStepLines,
  type DeployStepSecretConfig,
  deployStepSecretEnvLines,
  sourceCheckoutsStepLines,
} from './ci-workflow.js';
import { GITHUB_HOSTED_LINUX_RUNNER, renderRunsOnLine, type WorkflowRunsOn } from './github-runs-on.js';

const PUBLISH_WORKFLOW_FORMAT_OPTIONS = Object.freeze({
  parser: 'yaml',
  printWidth: 120,
  proseWrap: 'always',
  // Mirrors .prettierrc: git-format-staged reformats the committed workflow
  // with the repo config on every commit, so the render must agree or the
  // first empty-string scalar (default: '') drifts on quote style alone.
  singleQuote: true,
} satisfies PrettierOptions);

// @prettier/sync is unusable here: its module body eagerly instantiates a
// synchronized module for the bare specifier "prettier", which resolves from
// make-synchronized's own package directory inside its worker. Under Bun's
// isolated install store that directory only sees make-synchronized's declared
// dependencies, so the import throws a ResolveMessage that Bun then fails to
// structured-clone across postMessage ("Cannot serialize worker response").
// Resolving the entry HERE keys resolution to this package, which declares
// prettier, and hands the worker an absolute URL that loads in any store layout.
const synchronizedPrettier = makeModuleSynchronized<typeof PrettierModule>(import.meta.resolve('prettier'));

export type PublishWorkflowBump = 'auto' | 'patch' | 'minor' | 'major' | 'prerelease';
export type PublishWorkflowCondition =
  | 'version-mode-not-none'
  | 'deploy-production'
  | 'deploy-production-standalone'
  | 'failure'
  | 'always';
export type PublishWorkflowNxTarget = 'build' | 'lint' | 'test';
export type PublishWorkflowDeployStage = 'none' | 'production';

export enum PublishWorkflowStepKind {
  Checkout = 'checkout',
  SourceCheckouts = 'source-checkouts',
  CargoCredentials = 'cargo-credentials',
  SetupDevenv = 'setup-devenv',
  ConfigureReleaseAuthor = 'configure-release-author',
  BuildNxVersionActions = 'build-nx-version-actions',
  RepairPendingReleases = 'repair-pending-releases',
  VersionRelease = 'version-release',
  CheckManagedMonorepoFiles = 'check-managed-monorepo-files',
  Build = 'build',
  Lint = 'lint',
  UnitTests = 'unit-tests',
  UploadTraceDbs = 'upload-trace-dbs',
  ValidateMonorepoConfig = 'validate-monorepo-config',
  TagRelease = 'tag-release',
  PublishRelease = 'publish-release',
  DeployProduction = 'deploy-production',
  SaveNixDevenv = 'save-nix-devenv',
}

export interface PublishWorkflowStep {
  kind: PublishWorkflowStepKind;
  name: string;
  number: number;
  id?: string;
  condition?: PublishWorkflowCondition;
  nxTarget?: PublishWorkflowNxTarget;
}

export interface PublishWorkflowDefinition {
  steps: PublishWorkflowStep[];
}

export interface PublishWorkflowDefinitionOptions extends DeployStepSecretConfig {
  deploy?: boolean;
  /**
   * Whether the repo owns packages this workflow can release. False drops the
   * release half entirely: a repo publishing nothing has no version to gate a
   * production deploy on, and every `smoo release` command throws without one.
   */
  release?: boolean;
  repoName?: string;
  platformTargetGlobs?: readonly string[];
  /**
   * Architectures the macOS platform job fans out over, one job per entry.
   * Empty falls back to a single job selecting every macOS platform family.
   */
  macosPlatformArchitectures?: readonly string[];
  /** Linux jobs only. Default ubuntu-latest. Same smoo.github.runsOn as CI. */
  runsOn?: WorkflowRunsOn;
  /** macOS platform job runs-on labels. Default macos-latest. Same smoo.github.macosRunsOn as CI. */
  macosRunsOn?: WorkflowRunsOn;
  platformProducer?: PackageSmooGithub['platformProducer'];
  actionsProvider?: PackageSmooGithub['actionsProvider'];
  /**
   * Declared private-npm opt-in. The publish step is the only place receiving
   * the publish token; the read token rides along for `.npmrc` `${TOKEN}`
   * expansion. The registry URL is not a job env. No env is emitted without
   * this configuration.
   */
  privateNpm?: PackagePrivateNpmConfig;
  /** Declared sibling source checkouts cloned before SetupDevenv. */
  sourceCheckouts?: PackageSourceCheckoutConfig[];
  /**
   * Declared Cargo private-dependency credentials. Private git origins add a
   * host-gated credential helper step before SetupDevenv; the named secrets
   * become job env because every later cargo fetch resolves them.
   */
  cargoCredentials?: PackageCargoCredentialsConfig;
}

export interface PublishWorkflowInputs {
  bump: PublishWorkflowBump;
  deployStage: PublishWorkflowDeployStage;
  dryRun: boolean;
}

export interface PublishWorkflowVersionOutputs {
  mode: 'new' | 'none';
  projects: string[];
}

export interface PublishWorkflowSetupOutputs {
  nixCacheHit: string;
  devenvCacheHit: string;
}

export interface PublishWorkflowCallbacks {
  checkout(): Promise<void>;
  setupDevenv(): Promise<PublishWorkflowSetupOutputs>;
  configureReleaseAuthor(): Promise<void>;
  buildNxVersionActions(): Promise<void>;
  repairPendingReleases(input: { dryRun: boolean }): Promise<void>;
  versionRelease(input: { bump: PublishWorkflowBump; dryRun: boolean }): Promise<PublishWorkflowVersionOutputs>;
  checkManagedMonorepoFiles(): Promise<void>;
  nxRunMany(input: { target: PublishWorkflowNxTarget; projects: string[] }): Promise<void>;
  uploadTraceDbs(): Promise<void>;
  validateMonorepoConfig(): Promise<void>;
  tagRelease(input: { dryRun: boolean }): Promise<void>;
  publishRelease(input: { bump: PublishWorkflowBump; dryRun: boolean }): Promise<void>;
  deployProduction(): Promise<void>;
  saveNixDevenv(input: PublishWorkflowSetupOutputs): Promise<void>;
}

export interface PublishWorkflowRunContext {
  inputs: PublishWorkflowInputs;
  callbacks: PublishWorkflowCallbacks;
}

export interface PublishWorkflowRunResult {
  version: PublishWorkflowVersionOutputs;
  failed: boolean;
}

type PublishWorkflowStepInput = Omit<PublishWorkflowStep, 'number'>;

export function definePublishWorkflow(options: PublishWorkflowDefinitionOptions = {}): PublishWorkflowDefinition {
  const versionMode = githubExpression('steps.version.outputs.mode');
  if (options.release === false) {
    return { steps: defineDeployOnlyWorkflowSteps(options) };
  }
  const setupSteps: PublishWorkflowStepInput[] = [
    { kind: PublishWorkflowStepKind.Checkout, name: '📥 Checkout' },
    ...(options.cargoCredentials !== undefined
      ? [{ kind: PublishWorkflowStepKind.CargoCredentials, name: CARGO_CREDENTIALS_STEP_NAME }]
      : []),
    ...(options.sourceCheckouts?.length
      ? [{ kind: PublishWorkflowStepKind.SourceCheckouts, name: '📦 Check out sibling sources' }]
      : []),
    { kind: PublishWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv', id: 'setup' },
    { kind: PublishWorkflowStepKind.ConfigureReleaseAuthor, name: '🤖 Configure release author' },
  ];
  if (isSmoothBricksCodebasePackageName(options.repoName)) {
    setupSteps.push({
      kind: PublishWorkflowStepKind.BuildNxVersionActions,
      name: '🏗️ Build smoo Nx version actions',
    });
  }
  const releaseSteps: PublishWorkflowStepInput[] = [
    { kind: PublishWorkflowStepKind.TagRelease, name: '🏷️ Tag release' },
    {
      kind: PublishWorkflowStepKind.PublishRelease,
      name: `📦 Publish release (${versionMode})`,
    },
  ];
  if (options.deploy === true) {
    releaseSteps.push({
      kind: PublishWorkflowStepKind.DeployProduction,
      name: '🚀 Deploy production',
      condition: 'deploy-production',
    });
  }
  return {
    steps: numberWorkflowSteps([
      ...setupSteps,
      {
        kind: PublishWorkflowStepKind.RepairPendingReleases,
        name: '🧯 Repair pending releases',
      },
      { kind: PublishWorkflowStepKind.VersionRelease, name: '🔢 Version release', id: 'version' },
      {
        kind: PublishWorkflowStepKind.CheckManagedMonorepoFiles,
        name: `✅ Check managed monorepo files (${versionMode})`,
        condition: 'version-mode-not-none',
      },
      {
        kind: PublishWorkflowStepKind.Build,
        name: `🔨 Build (${versionMode})`,
        condition: 'version-mode-not-none',
        nxTarget: 'build',
      },
      {
        kind: PublishWorkflowStepKind.Lint,
        name: `🔍 Lint (${versionMode})`,
        condition: 'version-mode-not-none',
        nxTarget: 'lint',
      },
      {
        kind: PublishWorkflowStepKind.UnitTests,
        name: `🧪 Unit Tests (${versionMode})`,
        condition: 'version-mode-not-none',
        nxTarget: 'test',
      },
      { kind: PublishWorkflowStepKind.UploadTraceDbs, name: '📎 Upload trace DBs', condition: 'failure' },
      {
        kind: PublishWorkflowStepKind.ValidateMonorepoConfig,
        name: `✅ Validate monorepo config (${versionMode})`,
        condition: 'version-mode-not-none',
      },
      ...releaseSteps,
      {
        kind: PublishWorkflowStepKind.SaveNixDevenv,
        name: '🧹 Cleanup and cache Nix/devenv',
        condition: 'always',
      },
    ]),
  };
}

/**
 * Repos that deploy but own no release packages. Build, lint and test are not
 * dropped safety: `nx-deploy --verify` runs all three before it deploys.
 */
function defineDeployOnlyWorkflowSteps(options: PublishWorkflowDefinitionOptions): PublishWorkflowStep[] {
  return numberWorkflowSteps([
    { kind: PublishWorkflowStepKind.Checkout, name: '📥 Checkout' },
    ...(options.cargoCredentials !== undefined
      ? [{ kind: PublishWorkflowStepKind.CargoCredentials, name: CARGO_CREDENTIALS_STEP_NAME }]
      : []),
    ...(options.sourceCheckouts?.length
      ? [{ kind: PublishWorkflowStepKind.SourceCheckouts, name: '📦 Check out sibling sources' }]
      : []),
    { kind: PublishWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv', id: 'setup' },
    {
      kind: PublishWorkflowStepKind.DeployProduction,
      name: '🚀 Deploy production',
      condition: 'deploy-production-standalone',
    },
    { kind: PublishWorkflowStepKind.UploadTraceDbs, name: '📎 Upload trace DBs', condition: 'failure' },
    {
      kind: PublishWorkflowStepKind.SaveNixDevenv,
      name: '🧹 Cleanup and cache Nix/devenv',
      condition: 'always',
    },
  ]);
}

function numberWorkflowSteps(steps: PublishWorkflowStepInput[]): PublishWorkflowStep[] {
  return steps.map((step, index) => ({ ...step, number: index + 2 }));
}

export async function runPublishWorkflow(
  workflow: PublishWorkflowDefinition,
  context: PublishWorkflowRunContext,
): Promise<PublishWorkflowRunResult> {
  let setupOutputs: PublishWorkflowSetupOutputs = { nixCacheHit: '', devenvCacheHit: '' };
  let version: PublishWorkflowVersionOutputs = { mode: 'none', projects: [] };
  let failed = false;
  let failure: unknown;
  for (const step of workflow.steps) {
    if (!shouldRunStep(step, version, failed, context.inputs)) {
      continue;
    }
    try {
      switch (step.kind) {
        case PublishWorkflowStepKind.Checkout:
          await context.callbacks.checkout();
          break;
        case PublishWorkflowStepKind.SourceCheckouts:
        case PublishWorkflowStepKind.CargoCredentials:
          // The generated job runs these setup shells; runtime simulation has no callback.
          break;
        case PublishWorkflowStepKind.SetupDevenv:
          setupOutputs = await context.callbacks.setupDevenv();
          break;
        case PublishWorkflowStepKind.ConfigureReleaseAuthor:
          await context.callbacks.configureReleaseAuthor();
          break;
        case PublishWorkflowStepKind.BuildNxVersionActions:
          await context.callbacks.buildNxVersionActions();
          break;
        case PublishWorkflowStepKind.RepairPendingReleases:
          await context.callbacks.repairPendingReleases({
            dryRun: context.inputs.dryRun,
          });
          break;
        case PublishWorkflowStepKind.VersionRelease:
          version = await context.callbacks.versionRelease(context.inputs);
          break;
        case PublishWorkflowStepKind.CheckManagedMonorepoFiles:
          await context.callbacks.checkManagedMonorepoFiles();
          break;
        case PublishWorkflowStepKind.Build:
        case PublishWorkflowStepKind.Lint:
        case PublishWorkflowStepKind.UnitTests:
          if (!step.nxTarget) {
            throw new Error(`Workflow step ${step.kind} is missing an Nx target.`);
          }
          await context.callbacks.nxRunMany({ target: step.nxTarget, projects: version.projects });
          break;
        case PublishWorkflowStepKind.UploadTraceDbs:
          await context.callbacks.uploadTraceDbs();
          break;
        case PublishWorkflowStepKind.ValidateMonorepoConfig:
          await context.callbacks.validateMonorepoConfig();
          break;
        case PublishWorkflowStepKind.TagRelease:
          await context.callbacks.tagRelease({ dryRun: context.inputs.dryRun });
          break;
        case PublishWorkflowStepKind.PublishRelease:
          await context.callbacks.publishRelease({
            bump: context.inputs.bump,
            dryRun: context.inputs.dryRun,
          });
          break;
        case PublishWorkflowStepKind.DeployProduction:
          await context.callbacks.deployProduction();
          break;
        case PublishWorkflowStepKind.SaveNixDevenv:
          await context.callbacks.saveNixDevenv(setupOutputs);
          break;
      }
    } catch (error) {
      failed = true;
      failure = error;
    }
  }
  if (failure) {
    throw failure;
  }
  return { version, failed };
}

function shouldRunStep(
  step: PublishWorkflowStep,
  version: PublishWorkflowVersionOutputs,
  failed: boolean,
  inputs: PublishWorkflowInputs,
): boolean {
  if (step.condition === 'version-mode-not-none') {
    return version.mode !== 'none';
  }
  if (step.condition === 'deploy-production') {
    return version.mode !== 'none' && inputs.deployStage === 'production' && !inputs.dryRun;
  }
  if (step.condition === 'deploy-production-standalone') {
    return inputs.deployStage === 'production' && !inputs.dryRun;
  }
  if (step.condition === 'failure') {
    return failed;
  }
  return true;
}

export function renderPublishWorkflowYaml(options: PublishWorkflowDefinitionOptions = {}): string {
  let workflow: string;
  if (options.release === false) {
    const steps = definePublishWorkflow(options).steps;
    workflow = `${renderPublishWorkflowHeader(options)}${renderPublishWorkflowSteps(steps, options)}`;
  } else if (hasMacosPlatformTargets(options)) {
    workflow = renderPlatformPublishWorkflowYaml(options);
  } else if (hasLinuxPlatformTargets(options)) {
    workflow = `${renderPublishWorkflowHeader(options)}${renderSingleJobPublishWorkflowSteps(
      definePublishWorkflow(options).steps,
      options,
    )}`;
  } else {
    const steps = definePublishWorkflow(options).steps;
    workflow = `${renderPublishWorkflowHeader(options)}${renderPublishWorkflowSteps(steps, options)}`;
  }
  return synchronizedPrettier.format(workflow, PUBLISH_WORKFLOW_FORMAT_OPTIONS);
}

function renderPublishWorkflowHeader(options: PublishWorkflowDefinitionOptions): string {
  const dryRunDescription =
    options.release === false
      ? 'Skip the deploy and report what would run.'
      : 'Run release commands without writing versions, tags, publishes, or GitHub Releases.';
  const bumpInput =
    options.release === false
      ? ''
      : `
      bump:
        type: choice
        description:
          Use auto for conventional commits, or force a semver bump. Prerelease publishes to next; all others publish to
          latest.
        options: [auto, patch, minor, major, prerelease]
        default: auto
      projects:
        type: string
        description:
          Comma-separated Nx projects to release, or all for every owned release package. Blank releases package-local
          changes since the last release, whatever the bump mode is.
        default: ''`;
  const deployInput =
    options.deploy === true
      ? `
      deploy_stage:
        type: choice
        description: Deploy live systems after a successful publish.
        options: [none, production]
        default: none`
      : '';
  return `name: Publish

on:
  workflow_dispatch:
    inputs:${bumpInput}
      dry_run:
        type: boolean
        description: ${dryRunDescription}
        default: false${deployInput}

permissions:
  contents: write
  id-token: write

concurrency:
  group: release-${githubExpression('github.ref')}
  cancel-in-progress: false

defaults:
  run:
    working-directory: tooling/direnv

jobs:
  publish:
${options.release === false ? renderRunsOnLine(options.runsOn) : publishJobRunsOnLine(options)}
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}${cargoCredentialsJobEnv(options)}${privateNpmInstallJobEnv(options)}
    steps:
`;
}

function renderPublishWorkflowSteps(steps: PublishWorkflowStep[], options: PublishWorkflowDefinitionOptions): string {
  const lines: string[] = [];
  for (const step of steps) {
    lines.push(...sectionLinesBefore(step));
    lines.push(...commentLinesForStep(step));
    lines.push(...yamlLinesForStep(step, options));
    lines.push('');
  }
  return `${lines.join('\n').trimEnd()}\n`;
}

function sectionLinesBefore(step: PublishWorkflowStep): string[] {
  if (step.kind === PublishWorkflowStepKind.Checkout) {
    return ['      # --- Setup --------------------------------------------------------------', ''];
  }
  if (step.kind === PublishWorkflowStepKind.CheckManagedMonorepoFiles) {
    return [
      '      # --- Validation ---------------------------------------------------------',
      '',
      '      # Release validation intentionally does not restore persisted Nx task',
      '      # cache. Nx may reuse tasks produced earlier in this same job, but publish',
      '      # never relies on task outputs restored from CI cache. version runs before',
      '      # validation so the commit completed below is the commit that was checked.',
      '',
    ];
  }
  if (step.kind === PublishWorkflowStepKind.TagRelease) {
    return ['      # --- Release ------------------------------------------------------------', ''];
  }
  if (step.kind === PublishWorkflowStepKind.SaveNixDevenv) {
    return ['      # --- Cleanup ------------------------------------------------------------', ''];
  }
  return [];
}

function commentLinesForStep(step: PublishWorkflowStep): string[] {
  if (step.kind === PublishWorkflowStepKind.Checkout) {
    return ['      # Step 1: GitHub adds "Set up job" automatically', '      # Step 2'];
  }
  if (step.kind === PublishWorkflowStepKind.SetupDevenv) {
    return [
      `      # Step ${step.number}. Composite action internals do not affect top-level job step`,
      '      # anchors; update these comments if top-level steps move.',
    ];
  }
  return [`      # Step ${step.number}`];
}

function yamlLinesForStep(step: PublishWorkflowStep, options: PublishWorkflowDefinitionOptions): string[] {
  switch (step.kind) {
    case PublishWorkflowStepKind.Checkout:
      return [
        `      - name: ${step.name}`,
        '        uses: actions/checkout@v6.0.2',
        '        with:',
        '          filter: blob:none',
        '          fetch-depth: 0',
      ];
    case PublishWorkflowStepKind.SourceCheckouts:
      return siblingSourceCheckoutStepLines(options);
    case PublishWorkflowStepKind.CargoCredentials:
      return cargoCredentialsStepLines(options);
    case PublishWorkflowStepKind.SetupDevenv:
      return [`      - name: ${step.name}`, '        id: setup', '        uses: ./.github/actions/setup-devenv'];
    case PublishWorkflowStepKind.ConfigureReleaseAuthor:
      return [
        `      - name: ${step.name}`,
        '        run:',
        '          git config user.name "github-actions[bot]" && git config user.email',
        '          "41898282+github-actions[bot]@users.noreply.github.com"',
      ];
    case PublishWorkflowStepKind.BuildNxVersionActions:
      return [
        `      - name: ${step.name}`,
        '        # Nx Release loads @smoothbricks/nx-plugin/version-actions through the',
        '        # export map in its own node process, and versioning runs before the',
        '        # build phase, so the hook must exist in dist first. Downstream',
        '        # consumers install the published package and skip this bootstrap.',
        '        #',
        '        # `nx` cannot do it: the project graph loads this very plugin, so',
        '        # `nx build nx-plugin` deadlocks on the executor it is building.',
        '        # ttsc needs only the package tsconfig -- same bootstrap the devenv',
        '        # shell runs (tooling/direnv/enter-shell.ts).',
        '        working-directory: packages/nx-plugin',
        '        run: ttsc -p tsconfig.lib.json --emit',
      ];
    case PublishWorkflowStepKind.RepairPendingReleases:
      return [
        `      - name: ${step.name}`,
        ...privateNpmPublisherStepEnv(options),
        `        run: smoo release repair-pending --dry-run "${githubExpression('inputs.dry_run')}"`,
      ];
    case PublishWorkflowStepKind.VersionRelease:
      return [
        `      - name: ${step.name}`,
        '        id: version',
        '        run:',
        `          smoo release version --bump "${githubExpression('inputs.bump')}" --projects "${githubExpression('inputs.projects')}" --dry-run "${githubExpression('inputs.dry_run')}" --github-output`,
        '          "$GITHUB_OUTPUT"',
      ];
    case PublishWorkflowStepKind.CheckManagedMonorepoFiles:
      // Drift is derived state with its own remediation PR; publishing only
      // blocks on actual package issues (smoo monorepo validate).
      return conditionalRunStep(step, 'smoo monorepo check --warn');
    case PublishWorkflowStepKind.Build:
      return conditionalRunStep(
        step,
        `smoo github-ci nx-run-many --targets build --projects "${githubExpression('steps.version.outputs.projects')}"`,
      );
    case PublishWorkflowStepKind.Lint:
      return conditionalRunStep(
        step,
        `smoo github-ci nx-run-many --targets lint --projects "${githubExpression('steps.version.outputs.projects')}"`,
      );
    case PublishWorkflowStepKind.UnitTests:
      return conditionalRunStep(
        step,
        `smoo github-ci nx-run-many --targets test --projects "${githubExpression('steps.version.outputs.projects')}"`,
      );
    case PublishWorkflowStepKind.UploadTraceDbs:
      return artifactStepLines(
        options.actionsProvider,
        step.name,
        'upload',
        [
          `name: trace-results-${githubExpression('github.run_id')}`,
          'path: packages/*/.cache/trace-results.db*',
          'if-no-files-found: ignore',
          'retention-days: 14',
          'include-hidden-files: true',
        ],
        'failure()',
      );
    case PublishWorkflowStepKind.ValidateMonorepoConfig:
      return conditionalRunStep(step, 'smoo monorepo validate');
    case PublishWorkflowStepKind.TagRelease:
      return tagReleaseStepLines(step.name);
    case PublishWorkflowStepKind.PublishRelease:
      return [
        `      - name: ${step.name}`,
        ...privateNpmPublisherStepEnv(options),
        ...(options.privateNpm
          ? ['        # Private publication uses the declared registry and step-scoped publisher credential.']
          : [
              '        # smoo packs with Bun, then publishes tarballs with npm. Existing',
              '        # packages must already exist on npm and use trusted publishing/OIDC.',
              '        # Missing package names are bootstrapped locally before trust setup.',
            ]),
        `        run: smoo release publish --bump "${githubExpression('inputs.bump')}" --dry-run "${githubExpression('inputs.dry_run')}"`,
      ];
    case PublishWorkflowStepKind.DeployProduction:
      return deployProductionStep(step, options);
    case PublishWorkflowStepKind.SaveNixDevenv:
      return [
        `      - name: ${step.name}`,
        '        # success() is default; always() still saves GH Nix cache after a red job.',
        '        if: always()',
        '        uses: ./.github/actions/save-nix-devenv',
        '        with:',
        `          nix-cache-hit: ${githubExpression('steps.setup.outputs.nix-cache-hit')}`,
        `          devenv-cache-hit: ${githubExpression('steps.setup.outputs.devenv-cache-hit')}`,
      ];
  }
}

/**
 * The one place release tags come into existence. Release candidates version
 * without tagging, so a candidate that fails Build, Lint, or Unit Tests leaves
 * no ref behind and the next run recomputes the same version cleanly. Tagging
 * here — after validation, immediately before the npm publish — preserves the
 * "tagged but not published" state that `smoo release repair-pending`
 * reconciles, because git and npm cannot be written atomically.
 */
function tagReleaseStepLines(name: string): string[] {
  return [
    `      - name: ${name}`,
    '        # Ordered before the publish below so an interrupted release stays',
    '        # detectable by the repair step at the top of this job on a later run.',
    `        run: smoo release tag --dry-run "${githubExpression('inputs.dry_run')}"`,
  ];
}

function deployProductionStep(step: PublishWorkflowStep, options: PublishWorkflowDefinitionOptions): string[] {
  const conditionLines =
    step.condition === 'deploy-production-standalone'
      ? ["        if: ${{ inputs.deploy_stage == 'production' && inputs.dry_run != 'true' }}"]
      : [
          '        if:',
          "          ${{ steps.version.outputs.mode != 'none' && inputs.deploy_stage == 'production' && inputs.dry_run !=",
          "          'true' }}",
        ];
  return [
    `      - name: ${step.name}`,
    ...conditionLines,
    ...deployStepSecretEnvLines(options),
    '        run: smoo github-ci nx-deploy --stage production --mode run-many --verify --name "Deploy Production"',
  ];
}

function conditionalRunStep(step: PublishWorkflowStep, run: string): string[] {
  const condition = "steps.version.outputs.mode != 'none'";
  return [`      - name: ${step.name}`, `        if: ${condition}`, `        run: ${run}`];
}

function siblingSourceCheckoutStepLines(options: PublishWorkflowDefinitionOptions): string[] {
  const checkouts = options.sourceCheckouts;
  if (!checkouts?.length) {
    return [];
  }
  return sourceCheckoutsStepLines(
    { kind: CiWorkflowStepKind.SourceCheckouts, name: '📦 Check out sibling sources', number: 0 },
    checkouts,
  );
}

const CARGO_CREDENTIALS_STEP_NAME = 'Prepare Cargo credentials';

/**
 * One credential-helper install per job, because the helper is process
 * configuration and every later cargo fetch in that job resolves through it:
 * the release candidate, both native platform legs, and the final publish job
 * whose pending-release repair still builds historical versions from source.
 */
function cargoCredentialsStepLines(options: PublishWorkflowDefinitionOptions): string[] {
  const config = options.cargoCredentials;
  if (config === undefined) {
    return [];
  }
  return cargoCredentialStepLines(
    { kind: CiWorkflowStepKind.CargoCredentials, name: CARGO_CREDENTIALS_STEP_NAME, number: 0 },
    config,
  );
}

/**
 * Cargo credentials are job env, not step env: the helper reads its token at
 * call time from whichever later step runs the fetch, and build, lint, test
 * and platform-output builds all do. Narrowing it to the install step would
 * leave those fetches unauthenticated. Only `${{ secrets.NAME }}` is ever
 * rendered — no token value, URL, or argv. Malformed declarations throw here,
 * on every render path, rather than in CI.
 */
function cargoCredentialsJobEnv(options: PublishWorkflowDefinitionOptions): string {
  const rendered = cargoCredentialJobEnvLines(options.cargoCredentials).trimEnd();
  return rendered === '' ? '' : `\n${rendered}`;
}

function renderSingleJobPublishWorkflowSteps(
  steps: PublishWorkflowStep[],
  options: PublishWorkflowDefinitionOptions,
): string {
  const lines: string[] = [];
  for (const step of steps) {
    lines.push(...sectionLinesBefore(step));
    lines.push(...commentLinesForStep(step));
    lines.push(...yamlLinesForStep(step, options));
    lines.push('');
    if (step.kind === PublishWorkflowStepKind.Build) {
      lines.push(
        '      - name: 🐧 Build supplemental Linux targets',
        "        if: steps.version.outputs.mode != 'none'",
        `        run: smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(',')}" --projects "${githubExpression(
          'steps.version.outputs.projects',
        )}"`,
        '',
      );
    }
  }
  return `${lines.join('\n').trimEnd()}\n`;
}

function renderPlatformPublishWorkflowYaml(options: PublishWorkflowDefinitionOptions): string {
  const deployInput =
    options.deploy === true
      ? `
      deploy_stage:
        type: choice
        description: Deploy live systems after a successful publish.
        options: [none, production]
        default: none`
      : '';
  const steps = definePublishWorkflow(options).steps;
  return `name: Publish

on:
  workflow_dispatch:
    inputs:
      bump:
        type: choice
        description:
          Use auto for conventional commits, or force a semver bump. Prerelease publishes to next; all others publish to
          latest.
        options: [auto, patch, minor, major, prerelease]
        default: auto
      projects:
        type: string
        description:
          Comma-separated Nx projects to release, or all for every owned release package. Blank releases package-local
          changes since the last release, whatever the bump mode is.
        default: ''
      dry_run:
        type: boolean
        description: Run release commands without writing versions, tags, publishes, or GitHub Releases.
        default: false${deployInput}

permissions:
  contents: read

concurrency:
  group: release-${githubExpression('github.ref')}
  cancel-in-progress: false

defaults:
  run:
    working-directory: tooling/direnv

jobs:
${renderPlatformPlanJob(options)}

  linux-release-candidate:
    needs: [platform-plan]
${renderRunsOnLine(options.runsOn)}
    permissions:
      contents: write
      id-token: none
    outputs:
      mode: ${githubExpression('steps.version.outputs.mode')}
      release-sha: ${githubExpression('steps.release-state.outputs.sha')}
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}${cargoCredentialsJobEnv(options)}${privateNpmInstallJobEnv(options)}
    steps:
${renderLinuxReleaseCandidateSteps(steps, options)}

  ${platformJobName(options)}:
    # Ten minutes of toolchain setup only when the plan found a run for this
    # platform; the plan job decided that without a toolchain.
    needs: [platform-plan]
    if: ${githubExpression("needs.platform-plan.outputs.platform-work == 'true'")}
${renderMacosJobHeaderLines(options)}
    permissions:
      contents: read
      id-token: none
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}${cargoCredentialsJobEnv(options)}${privateNpmInstallJobEnv(options)}
${Object.entries(options.platformProducer?.env ?? {})
  .map(([name, value]) => `      ${name}: ${JSON.stringify(value)}`)
  .join('\n')}
    steps:
${renderMacosPlatformSteps(options)}

  publish-on-linux:
    needs: [platform-plan, linux-release-candidate, ${platformJobName(options)}]
    # A platform leg the plan skipped is not a failure; a leg that ran must have passed.
    if:
      \${{ !cancelled() && needs.linux-release-candidate.result == 'success' && (needs.${platformJobName(options)}.result == 'success'
      || needs.${platformJobName(options)}.result == 'skipped') }}
${publishJobRunsOnLine(options)}
    permissions:
      contents: write
      id-token: write
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      TTSC_TSGO_BINARY: ${githubExpression('github.workspace')}/node_modules/@typescript/native/bin/tsc
      GH_TOKEN: ${githubExpression('github.token')}${cargoCredentialsJobEnv(options)}${privateNpmInstallJobEnv(options)}
    steps:
${renderFinalLinuxPublishSteps(options)}
`;
}

function platformJobName(options: PublishWorkflowDefinitionOptions): string {
  return options.platformProducer?.kind === 'linux-cross' ? 'cross-platform' : 'macos-platform';
}

/**
 * Decide, without a toolchain, whether the platform runners have anything to
 * build: the release selection, which selected projects carry platform
 * targets, and the pending releases only they can repair are git, the Nx
 * graph, npm and GitHub metadata. Bun runs smoo and Nx directly; the ten-minute
 * Nix setup is then spent only on a platform leg with a run.
 */
function renderPlatformPlanJob(options: PublishWorkflowDefinitionOptions): string {
  const selector = macosPlatformFamilySelector(options);
  const lines = [
    '  platform-plan:',
    publishJobRunsOnLine(options),
    '    permissions:',
    '      contents: read',
    '      id-token: none',
    '    outputs:',
    `      platform-work: ${githubExpression('steps.plan.outputs.platform-work')}`,
    `      projects: ${githubExpression('steps.plan.outputs.projects')}`,
    `      repairs: ${githubExpression('steps.plan.outputs.repairs')}`,
    '    env:',
    `      GH_TOKEN: ${githubExpression('github.token')}${privateNpmInstallJobEnv(options)}`,
    '    steps:',
    '      - name: 📥 Checkout dispatch commit',
    '        uses: actions/checkout@v6.0.2',
    '        with:',
    `          ref: ${githubExpression('github.sha')}`,
    '          filter: blob:none',
    '          fetch-depth: 0',
    '      - name: 🥟 Setup Bun',
    '        uses: oven-sh/setup-bun@v2',
    '        with:',
    '          bun-version-file: package.json',
    '      - name: 📦 Install workspace dependencies',
    '        working-directory: .',
    '        # repo-path exposes smoo: the source shim here, node_modules/.bin elsewhere.',
    '        run: |',
    '          bun install --frozen-lockfile',
    '          tooling/direnv/repo-path --github-path',
  ];
  if (isSmoothBricksCodebasePackageName(options.repoName)) {
    lines.push(
      '      - name: 🏗️ Build smoo Nx version actions',
      '        # The plan previews Nx Release versioning, which loads',
      '        # @smoothbricks/nx-plugin/version-actions from dist; ttsc runs on Bun.',
      '        working-directory: packages/nx-plugin',
      '        run: bunx ttsc -p tsconfig.lib.json --emit',
    );
  }
  lines.push(
    '      - name: 🗺️ Plan platform outputs',
    '        id: plan',
    '        run:',
    `          smoo release build-platform-outputs --plan --bump "${githubExpression('inputs.bump')}" --projects "${githubExpression('inputs.projects')}"`,
    `          --ref "${githubExpression('github.sha')}" --targets "${selector}" --github-output "$GITHUB_OUTPUT"`,
  );
  return lines.join('\n');
}

function renderLinuxReleaseCandidateSteps(
  steps: PublishWorkflowStep[],
  options: PublishWorkflowDefinitionOptions,
): string {
  const lines: string[] = [];
  let stepNumber = 2;
  for (const step of steps) {
    if (
      step.kind === PublishWorkflowStepKind.RepairPendingReleases ||
      step.kind === PublishWorkflowStepKind.TagRelease ||
      step.kind === PublishWorkflowStepKind.PublishRelease ||
      step.kind === PublishWorkflowStepKind.DeployProduction ||
      step.kind === PublishWorkflowStepKind.SaveNixDevenv
    ) {
      continue;
    }
    lines.push(...sectionLinesBefore(step));
    if (step.kind === PublishWorkflowStepKind.Checkout) {
      lines.push('      # Step 1: GitHub adds "Set up job" automatically', `      # Step ${stepNumber}`);
    } else if (step.kind === PublishWorkflowStepKind.SetupDevenv) {
      lines.push(
        `      # Step ${stepNumber}. Composite action internals do not affect top-level job step`,
        '      # anchors; update these comments if top-level steps move.',
      );
    } else {
      lines.push(`      # Step ${stepNumber}`);
    }
    if (step.kind === PublishWorkflowStepKind.Checkout) {
      lines.push(
        `      - name: ${step.name}`,
        '        uses: actions/checkout@v6.0.2',
        '        with:',
        `          ref: ${githubExpression('github.sha')}`,
        '          filter: blob:none',
        '          fetch-depth: 0',
      );
    } else if (step.kind === PublishWorkflowStepKind.Build) {
      lines.push(
        `      - name: ${step.name}`,
        "        if: steps.version.outputs.mode != 'none'",
        '        run:',
        `          smoo github-ci nx-run-many --targets build --projects "${githubExpression(
          'steps.version.outputs.projects',
        )}" --collect-outputs "${githubExpression('runner.temp')}/release-build-outputs"`,
      );
    } else {
      lines.push(...yamlLinesForStep(step, options));
    }
    stepNumber += 1;
    if (step.kind === PublishWorkflowStepKind.VersionRelease) {
      lines.push(
        '',
        `      # Step ${stepNumber}`,
        '      - name: 🔒 Capture candidate release SHA',
        '        id: release-state',
        '        run: echo "sha=$(git rev-parse HEAD)" >> "$GITHUB_OUTPUT"',
      );
      stepNumber += 1;
    }
    if (step.kind === PublishWorkflowStepKind.Build && hasLinuxPlatformTargets(options)) {
      lines.push(
        '',
        `      # Step ${stepNumber}`,
        '      - name: 🐧 Build supplemental Linux targets',
        "        if: steps.version.outputs.mode != 'none'",
        '        run:',
        `          smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(
          ',',
        )}" --projects "${githubExpression(
          'steps.version.outputs.projects',
        )}" --collect-outputs "${githubExpression('runner.temp')}/linux-platform-outputs"`,
      );
      stepNumber += 1;
    }
    lines.push('');
  }
  lines.push(
    '      # --- Candidate transfer --------------------------------------------------',
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 📦 Bundle validated release state',
    '        run:',
    `          mkdir -p "${githubExpression('runner.temp')}/publish-release-state" && git bundle create`,
    `          "${githubExpression('runner.temp')}/publish-release-state/release-state.bundle" HEAD --tags && git rev-parse HEAD >`,
    `          "${githubExpression('runner.temp')}/publish-release-state/release-head"`,
    '',
    `      # Step ${stepNumber++}`,
    ...artifactStepLines(options.actionsProvider, '📤 Upload validated release state', 'upload', [
      `name: publish-release-state-${githubExpression('github.run_id')}`,
      `path: ${githubExpression('runner.temp')}/publish-release-state`,
      'if-no-files-found: error',
      'retention-days: 1',
    ]),
    '',
    `      # Step ${stepNumber++}`,
    ...artifactStepLines(
      options.actionsProvider,
      '📤 Upload validated build outputs',
      'upload',
      [
        `name: publish-release-outputs-${githubExpression('github.run_id')}`,
        `path: ${githubExpression('runner.temp')}/release-build-outputs`,
        'if-no-files-found: error',
        'retention-days: 1',
        'include-hidden-files: true',
      ],
      "steps.version.outputs.mode != 'none'",
    ),
  );
  if (hasLinuxPlatformTargets(options)) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      ...artifactStepLines(
        options.actionsProvider,
        '📤 Upload supplemental Linux outputs',
        'upload',
        [
          `name: publish-linux-outputs-${githubExpression('github.run_id')}`,
          `path: ${githubExpression('runner.temp')}/linux-platform-outputs`,
          'if-no-files-found: error',
          'retention-days: 1',
          'include-hidden-files: true',
        ],
        "steps.version.outputs.mode != 'none'",
      ),
    );
  }
  lines.push(
    '',
    '      # --- Cleanup ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber}`,
    '      - name: 🧹 Cleanup and cache Nix/devenv',
    '        # success() is default; always() still saves GH Nix cache after a red job.',
    '        if: always()',
    '        uses: ./.github/actions/save-nix-devenv',
    '        with:',
    `          nix-cache-hit: ${githubExpression('steps.setup.outputs.nix-cache-hit')}`,
    `          devenv-cache-hit: ${githubExpression('steps.setup.outputs.devenv-cache-hit')}`,
  );
  return lines.join('\n').trimEnd();
}

function renderMacosPlatformSteps(options: PublishWorkflowDefinitionOptions): string {
  let stepNumber = 3;
  const lines = [
    '      # --- Setup --------------------------------------------------------------',
    '',
    '      # Step 1: GitHub adds "Set up job" automatically',
    '      # Step 2',
    '      - name: 📥 Checkout dispatch commit',
    '        uses: actions/checkout@v6.0.2',
    '        with:',
    `          ref: ${githubExpression('github.sha')}`,
    '          filter: blob:none',
    '          fetch-depth: 0',
  ];
  if (options.platformProducer) {
    if (!options.platformProducer.preflight.trim()) {
      throw new Error('Linux cross-platform production requires a nonempty toolchain preflight command.');
    }
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: Check cross-platform toolchain prerequisites',
      '        working-directory: .',
      '        run: |',
      '          set -euo pipefail',
      ...options.platformProducer.preflight.split('\n').map((line) => `          ${line}`),
    );
  }
  const macosCargoCredentials = cargoCredentialsStepLines(options);
  if (macosCargoCredentials.length > 0) {
    lines.push('', `      # Step ${stepNumber++}`, ...macosCargoCredentials);
  }
  const siblingSourceCheckouts = siblingSourceCheckoutStepLines(options);
  if (siblingSourceCheckouts.length > 0) {
    lines.push('', `      # Step ${stepNumber++}`, ...siblingSourceCheckouts);
  }
  lines.push(
    '',
    `      # Step ${stepNumber++}. Composite action internals do not affect top-level job step`,
    '      # anchors; update these comments if top-level steps move.',
    '      - name: 🧱 Setup Nix/devenv',
    '        id: setup',
    '        uses: ./.github/actions/setup-devenv',
  );
  if (isSmoothBricksCodebasePackageName(options.repoName)) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: 🏗️ Build smoo Nx version actions',
      '        # Nx Release loads @smoothbricks/nx-plugin/version-actions through the',
      '        # export map in its own node process, and versioning runs before the',
      '        # build phase, so the hook must exist in dist first. Downstream',
      '        # consumers install the published package and skip this bootstrap.',
      '        #',
      '        # `nx` cannot do it: the project graph loads this very plugin, so',
      '        # `nx build nx-plugin` deadlocks on the executor it is building.',
      '        # ttsc needs only the package tsconfig -- same bootstrap the devenv',
      '        # shell runs (tooling/direnv/enter-shell.ts).',
      '        working-directory: packages/nx-plugin',
      '        run: ttsc -p tsconfig.lib.json --emit',
    );
  }
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🤖 Configure release author',
    '        run:',
    '          git config user.name "github-actions[bot]" && git config user.email',
    '          "41898282+github-actions[bot]@users.noreply.github.com"',
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🔢 Version release',
    '        id: version',
    '        run:',
    `          smoo release version --bump "${githubExpression('inputs.bump')}" --projects "${githubExpression('inputs.projects')}" --dry-run "${githubExpression('inputs.dry_run')}" --github-output`,
    '          "$GITHUB_OUTPUT"',
  );
  const architectures = macosPlatformArchitectures(options);
  const isMatrix = architectures.length >= 2;
  const testArchitecture = macosTestArchitecture(architectures);
  const legLabel = isMatrix ? ` (${githubExpression('matrix.arch')})` : '';
  const testCondition =
    isMatrix && testArchitecture !== undefined
      ? `matrix.arch == '${testArchitecture}' && steps.platform-outputs.outputs.projects != ''`
      : `steps.platform-outputs.outputs.projects != ''`;
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    `      - name: 🍎 Build selected macOS and iOS release outputs${legLabel}`,
    '        id: platform-outputs',
    '        run:',
    `          smoo release build-platform-outputs --bump "${githubExpression(
      'inputs.bump',
    )}" --projects "${githubExpression('inputs.projects')}" --ref "${githubExpression('github.sha')}" --targets "${macosPlatformTargetSelector(options)}" --output`,
    `          "${githubExpression('runner.temp')}/macos-platform-outputs" --github-output "$GITHUB_OUTPUT"`,
  );
  if (options.platformProducer?.kind !== 'linux-cross') {
    lines.push('', `      # Step ${stepNumber++}`, '      - name: 🧪 Unit test selected macOS and iOS packages');
    if (isMatrix) {
      lines.push(
        '        # Only the runner-native leg can execute what it built; the foreign',
        '        # architecture ships as an artifact without running here.',
      );
    }
    lines.push(
      `        if: ${testCondition}`,
      '        run:',
      '          smoo github-ci nx-run-many --targets test --projects',
      `          "${githubExpression('steps.platform-outputs.outputs.projects')}"`,
    );
  }
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    ...artifactStepLines(options.actionsProvider, `📤 Upload macOS platform outputs${legLabel}`, 'upload', [
      `name: ${
        isMatrix
          ? `publish-macos-${githubExpression('matrix.arch')}-outputs-${githubExpression('github.run_id')}`
          : `publish-macos-outputs-${githubExpression('github.run_id')}`
      }`,
      `path: ${githubExpression('runner.temp')}/macos-platform-outputs`,
      'if-no-files-found: error',
      'retention-days: 1',
      'include-hidden-files: true',
    ]),
    '',
    '      # --- Cleanup ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber}`,
    '      - name: 🧹 Cleanup and cache Nix/devenv',
    '        # success() is default; always() still saves GH Nix cache after a red job.',
    '        if: always()',
    '        uses: ./.github/actions/save-nix-devenv',
    '        with:',
    `          nix-cache-hit: ${githubExpression('steps.setup.outputs.nix-cache-hit')}`,
    `          devenv-cache-hit: ${githubExpression('steps.setup.outputs.devenv-cache-hit')}`,
  );
  return lines.join('\n').trimEnd();
}

function renderFinalLinuxPublishSteps(options: PublishWorkflowDefinitionOptions): string {
  const mode = 'needs.linux-release-candidate.outputs.mode';
  let stepNumber = 2;
  const lines = [
    '      # --- Setup --------------------------------------------------------------',
    '',
    '      # Step 1: GitHub adds "Set up job" automatically',
    `      # Step ${stepNumber++}`,
    '      - name: 📥 Checkout dispatch commit',
    '        uses: actions/checkout@v6.0.2',
    '        with:',
    `          ref: ${githubExpression('github.sha')}`,
    '          filter: blob:none',
    '          fetch-depth: 0',
  ];
  const publishCargoCredentials = cargoCredentialsStepLines(options);
  if (publishCargoCredentials.length > 0) {
    lines.push('', `      # Step ${stepNumber++}`, ...publishCargoCredentials);
  }
  const siblingSourceCheckouts = siblingSourceCheckoutStepLines(options);
  if (siblingSourceCheckouts.length > 0) {
    lines.push('', `      # Step ${stepNumber++}`, ...siblingSourceCheckouts);
  }
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🧱 Setup Nix/devenv',
    '        # Pending release repair builds historical npm gaps before the current',
    '        # verified artifacts are restored, so this job still needs the full',
    '        # toolchain even though current release publishing is prebuilt-only.',
    '        id: setup',
    '        uses: ./.github/actions/setup-devenv',
    '',
    `      # Step ${stepNumber++}`,
    ...artifactStepLines(options.actionsProvider, '📥 Download candidate artifacts', 'download', [
      `pattern: publish-*-${githubExpression('github.run_id')}`,
      `path: ${githubExpression('runner.temp')}/publish-artifacts`,
      'merge-multiple: false',
    ]),
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🤖 Configure release author',
    '        run:',
    '          git config user.name "github-actions[bot]" && git config user.email',
    '          "41898282+github-actions[bot]@users.noreply.github.com"',
  );
  if (isSmoothBricksCodebasePackageName(options.repoName)) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: 🏗️ Build smoo Nx version actions',
      '        # Nx Release loads @smoothbricks/nx-plugin/version-actions through the',
      '        # export map in its own node process, and that export has no source',
      '        # condition, so the next-prerelease bump inside smoo release publish',
      '        # needs dist/version-actions.cjs on disk. Downstream consumers install',
      '        # the published package and skip this bootstrap.',
      '        #',
      '        # `nx` cannot do it: the project graph loads this very plugin, so',
      '        # `nx build nx-plugin` deadlocks on the executor it is building.',
      '        # ttsc needs only the package tsconfig -- same bootstrap the devenv',
      '        # shell runs (tooling/direnv/enter-shell.ts).',
      '        working-directory: packages/nx-plugin',
      '        run: ttsc -p tsconfig.lib.json --emit',
    );
  }
  lines.push(
    '',
    '      # --- Repair -------------------------------------------------------------',
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🧯 Repair pending releases',
    '        run:',
    `          smoo release repair-pending --ref "${githubExpression('github.sha')}" --platform-outputs`,
    `          "${macosPlatformArtifactNames(options)
      .map((name) => `${githubExpression('runner.temp')}/publish-artifacts/${name}/repairs`)
      .join(',')}" --dry-run "${githubExpression('inputs.dry_run')}"`,
    '',
    `      # Step ${stepNumber++}`,
    '      - name: ♻️ Restore validated release state',
    '        env:',
    `          EXPECTED_RELEASE_SHA: ${githubExpression('needs.linux-release-candidate.outputs.release-sha')}`,
    '        run: |',
    '          set -euo pipefail',
    `          state="${githubExpression('runner.temp')}/publish-artifacts/publish-release-state-${githubExpression('github.run_id')}"`,
    '          actual="$(cat "$state/release-head")"',
    '          if [ -z "$EXPECTED_RELEASE_SHA" ] || [ "$actual" != "$EXPECTED_RELEASE_SHA" ]; then',
    '            echo "Release state does not match the validated candidate SHA." >&2',
    '            exit 1',
    '          fi',
    '          git bundle verify "$state/release-state.bundle"',
    '          git fetch "$state/release-state.bundle" HEAD --tags',
    '          if [ "$(git rev-parse FETCH_HEAD)" != "$EXPECTED_RELEASE_SHA" ]; then',
    '            echo "Release bundle HEAD does not match the validated candidate SHA." >&2',
    '            exit 1',
    '          fi',
    '          git reset --hard "$EXPECTED_RELEASE_SHA"',
  );
  lines.push(
    '',
    '      # --- Validation ---------------------------------------------------------',
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 📦 Apply verified Linux outputs',
    `        if: ${mode} != 'none'`,
    '        run:',
    `          smoo github-ci apply-outputs --source-sha "${githubExpression(
      'needs.linux-release-candidate.outputs.release-sha',
    )}"`,
    `          "${githubExpression('runner.temp')}/publish-artifacts/publish-release-outputs-${githubExpression(
      'github.run_id',
    )}"`,
  );
  if (hasLinuxPlatformTargets(options)) {
    lines.push(
      `          "${githubExpression('runner.temp')}/publish-artifacts/publish-linux-outputs-${githubExpression(
        'github.run_id',
      )}"`,
    );
  }
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🧾 Select prebuilt platform outputs',
    '        # A platform leg the plan skipped left no artifact, and that is the',
    '        # expected shape; a leg the plan asked for must have delivered one.',
    '        id: platform-outputs',
    '        run: |',
    '          set -euo pipefail',
    '          dirs=""',
    `          for dir in ${macosPlatformArtifactNames(options)
      .map((name) => `"${githubExpression('runner.temp')}/publish-artifacts/${name}/current"`)
      .join(' ')}; do`,
    '            if [ -d "$dir" ]; then',
    '              # runner.temp carries no spaces; the publish step splits this on them.',
    '              dirs="$dirs $dir"',
    `            elif [ "${githubExpression('needs.platform-plan.outputs.platform-work')}" = "true" ]; then`,
    '              echo "Platform outputs the plan asked for are missing: $dir" >&2',
    '              exit 1',
    '            fi',
    '          done',
    '          echo "dirs=$dirs" >> "$GITHUB_OUTPUT"',
  );
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 🍎 Apply verified macOS outputs',
    `        if: ${mode} != 'none' && steps.platform-outputs.outputs.dirs != ''`,
    `        run: smoo github-ci apply-outputs --source-sha "${githubExpression('github.sha')}" ${githubExpression(
      'steps.platform-outputs.outputs.dirs',
    )}`,
    '',
    '      # --- Release ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber++}`,
    ...tagReleaseStepLines('🏷️ Tag release'),
    '',
    `      # Step ${stepNumber++}`,
    `      - name: 📦 Publish release (${githubExpression(mode)})`,
    ...privateNpmPublisherStepEnv(options),
    '        # Pack verified outputs; --prebuilt refuses missing artifacts rather than rebuilding.',
    ...(options.privateNpm
      ? ['        # Private publication uses the declared registry and step-scoped publisher credential.']
      : ['        # Public npm packages use trusted publishing/OIDC after local bootstrap.']),
    '        run:',
    '          smoo release publish --prebuilt',
    `          "${githubExpression('runner.temp')}/publish-artifacts/publish-release-outputs-${githubExpression(
      'github.run_id',
    )}"`,
    ...(hasLinuxPlatformTargets(options)
      ? [
          `          "${githubExpression('runner.temp')}/publish-artifacts/publish-linux-outputs-${githubExpression(
            'github.run_id',
          )}"`,
        ]
      : []),
    `          ${githubExpression('steps.platform-outputs.outputs.dirs')}`,
    `          --bump "${githubExpression('inputs.bump')}" --dry-run "${githubExpression('inputs.dry_run')}"`,
  );
  if (options.deploy === true) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: 🚀 Deploy production',
      '        if:',
      "          ${{ needs.linux-release-candidate.outputs.mode != 'none' && inputs.deploy_stage == 'production' &&",
      "          inputs.dry_run != 'true' }}",
      ...deployStepSecretEnvLines(options),
      '        run: smoo github-ci nx-deploy --stage production --mode run-many --verify --name "Deploy Production"',
    );
  }
  lines.push(
    '',
    '      # --- Cleanup ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber}`,
    '      - name: 🧹 Cleanup and cache Nix/devenv',
    '        if: always()',
    '        uses: ./.github/actions/save-nix-devenv',
    '        with:',
    `          nix-cache-hit: ${githubExpression('steps.setup.outputs.nix-cache-hit')}`,
    `          devenv-cache-hit: ${githubExpression('steps.setup.outputs.devenv-cache-hit')}`,
  );
  return lines.join('\n').trimEnd();
}

function hasMacosPlatformTargets(options: PublishWorkflowDefinitionOptions): boolean {
  return MACOS_PLATFORM_TARGET_GLOBS.some((glob) => options.platformTargetGlobs?.includes(glob) === true);
}

/**
 * The architecture `macos-latest` runs natively. Only that leg can execute the
 * binaries it just built, so it owns the unit-test step; keep this beside the
 * `runs-on` label it describes.
 */
const MACOS_RUNNER_ARCHITECTURE = 'arm64';

function macosPlatformFamilies(options: PublishWorkflowDefinitionOptions): string[] {
  return MACOS_PLATFORM_TARGET_GLOBS.filter((glob) => options.platformTargetGlobs?.includes(glob) === true);
}

/**
 * One matrix leg per architecture. A single runner cannot overlap two platform
 * builds of one package: cargo holds an exclusive lock on the package's whole
 * `target/` directory, so extra Nx workers only queue behind it. Separate
 * runners have separate target directories, so the architectures compile in
 * parallel for real.
 */
function macosPlatformArchitectures(options: PublishWorkflowDefinitionOptions): string[] {
  if (options.platformProducer?.kind === 'linux-cross') return [];
  return hasMacosPlatformTargets(options) ? [...(options.macosPlatformArchitectures ?? [])] : [];
}

function macosTestArchitecture(architectures: readonly string[]): string | undefined {
  return architectures.includes(MACOS_RUNNER_ARCHITECTURE) ? MACOS_RUNNER_ARCHITECTURE : architectures[0];
}

function renderMacosJobHeaderLines(options: PublishWorkflowDefinitionOptions): string {
  if (options.platformProducer?.kind === 'linux-cross') return renderRunsOnLine(options.runsOn);
  const architectures = macosPlatformArchitectures(options);
  const lines = [
    options.macosRunsOn === undefined ? '    runs-on: macos-latest' : renderRunsOnLine(options.macosRunsOn),
  ];
  if (architectures.length >= 2) {
    lines.push(
      '    strategy:',
      '      # One architecture per runner: a leg that fails still leaves the other',
      '      # architecture uploaded for inspection.',
      '      fail-fast: false',
      '      matrix:',
      `        arch: [${architectures.join(', ')}]`,
    );
  }
  return lines.join('\n');
}

/** Every macOS family this workflow produces, architecture-agnostic: what the plan job asks about. */
function macosPlatformFamilySelector(options: PublishWorkflowDefinitionOptions): string {
  const families = macosPlatformFamilies(options);
  return (families.length > 0 ? families : [...MACOS_PLATFORM_TARGET_GLOBS]).join(',');
}

/** Per-leg target selector, or every macOS family when there is no matrix. */
function macosPlatformTargetSelector(options: PublishWorkflowDefinitionOptions): string {
  const families = macosPlatformFamilies(options);
  const selectors = families.length > 0 ? families : [...MACOS_PLATFORM_TARGET_GLOBS];
  if (macosPlatformArchitectures(options).length < 2) {
    return selectors.join(',');
  }
  return selectors.map((glob) => `*-${githubExpression('matrix.arch')}-${glob.slice(2)}`).join(',');
}

/** Artifact names the final job downloads, one per rendered macOS leg. */
function macosPlatformArtifactNames(options: PublishWorkflowDefinitionOptions): string[] {
  const architectures = macosPlatformArchitectures(options);
  const runId = githubExpression('github.run_id');
  if (architectures.length < 2) {
    return [`publish-macos-outputs-${runId}`];
  }
  return architectures.map((architecture) => `publish-macos-${architecture}-outputs-${runId}`);
}

function hasLinuxPlatformTargets(options: PublishWorkflowDefinitionOptions): boolean {
  return LINUX_PLATFORM_TARGET_GLOBS.some((glob) => options.platformTargetGlobs?.includes(glob) === true);
}

/**
 * Runner for the job that tags and publishes, on GitHub Actions always the
 * GitHub-hosted one.
 *
 * npmjs mints provenance from the job's OIDC token and then rejects the upload
 * with `422 Unsupported GitHub Actions runner environment "self-hosted", only
 * github-hosted supported for provenance`: the tarball is signed, the registry
 * refuses it, and the release is left tagged but unpublished. Publishing from
 * a GitHub-hosted runner is the fix; dropping `--provenance` is not.
 *
 * A `forgejo` actions provider keeps the configured labels: that workflow does
 * not run on GitHub Actions, so `ubuntu-latest` names no runner there and
 * there is no GitHub OIDC to mint provenance from in the first place.
 * Private-registry publishing is unaffected either way — its static publisher
 * token works from any runner, and its packages ride the same job.
 *
 * Build lanes keep `smoo.github.runsOn`: only the publishing job moves. Where
 * build and publish share one job (no macOS platform fan-out) that whole job
 * follows the publisher, because that job is the one npmjs sees.
 */
function publishJobRunsOnLine(options: PublishWorkflowDefinitionOptions): string {
  return renderRunsOnLine(options.actionsProvider === 'forgejo' ? options.runsOn : GITHUB_HOSTED_LINUX_RUNNER);
}

function githubExpression(expression: string): string {
  return ['$', '{{ ', expression, ' }}'].join('');
}

function privateNpmInstallJobEnv(options: PublishWorkflowDefinitionOptions): string {
  const tokenEnv = options.privateNpm?.readTokenEnv;
  if (!tokenEnv) {
    return '';
  }
  return ['', `      ${tokenEnv}: ${githubExpression(`secrets.${tokenEnv}`)}`].join('\n');
}

/** Both repair and publish can write packages; neither exposes the credential to setup/build. */
function privateNpmPublisherStepEnv(options: PublishWorkflowDefinitionOptions): string[] {
  const name = options.privateNpm?.publishTokenEnv;
  return name ? ['        env:', `          ${name}: ${githubExpression(`secrets.${name}`)}`] : [];
}
