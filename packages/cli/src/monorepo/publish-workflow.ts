/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

import {
  LINUX_PLATFORM_TARGET_GLOBS,
  MACOS_PLATFORM_TARGET_GLOBS,
} from '@smoothbricks/nx-plugin/workspace-config-policy';
import { isSmoothBricksCodebasePackageName } from '../lib/cli-package.js';

export type PublishWorkflowBump = 'auto' | 'patch' | 'minor' | 'major' | 'prerelease';
export type PublishWorkflowCondition = 'version-mode-not-none' | 'deploy-production' | 'failure' | 'always';
export type PublishWorkflowNxTarget = 'build' | 'lint' | 'test';
export type PublishWorkflowDeployEnvironment = 'none' | 'production';

export enum PublishWorkflowStepKind {
  Checkout = 'checkout',
  SetupDevenv = 'setup-devenv',
  ConfigureReleaseAuthor = 'configure-release-author',
  BuildSelfHostedCli = 'build-self-hosted-cli',
  RepairPendingReleases = 'repair-pending-releases',
  VersionRelease = 'version-release',
  CheckManagedMonorepoFiles = 'check-managed-monorepo-files',
  Build = 'build',
  Lint = 'lint',
  UnitTests = 'unit-tests',
  UploadTraceDbs = 'upload-trace-dbs',
  ValidateMonorepoConfig = 'validate-monorepo-config',
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

export interface PublishWorkflowDefinitionOptions {
  deploy?: boolean;
  deployProvider?: 'cloudflare';
  repoName?: string;
  platformTargetGlobs?: readonly string[];
}

export interface PublishWorkflowInputs {
  bump: PublishWorkflowBump;
  deployEnvironment: PublishWorkflowDeployEnvironment;
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
  buildSelfHostedCli(): Promise<void>;
  repairPendingReleases(input: { dryRun: boolean }): Promise<void>;
  versionRelease(input: { bump: PublishWorkflowBump; dryRun: boolean }): Promise<PublishWorkflowVersionOutputs>;
  checkManagedMonorepoFiles(): Promise<void>;
  nxRunMany(input: { target: PublishWorkflowNxTarget; projects: string[] }): Promise<void>;
  uploadTraceDbs(): Promise<void>;
  validateMonorepoConfig(): Promise<void>;
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
const MACOS_CANDIDATE_STEP_KINDS: readonly PublishWorkflowStepKind[] = [
  PublishWorkflowStepKind.Checkout,
  PublishWorkflowStepKind.SetupDevenv,
  PublishWorkflowStepKind.ConfigureReleaseAuthor,
  PublishWorkflowStepKind.BuildSelfHostedCli,
  PublishWorkflowStepKind.RepairPendingReleases,
  PublishWorkflowStepKind.VersionRelease,
  PublishWorkflowStepKind.CheckManagedMonorepoFiles,
];

export function definePublishWorkflow(options: PublishWorkflowDefinitionOptions = {}): PublishWorkflowDefinition {
  const versionMode = githubExpression('steps.version.outputs.mode');
  const setupSteps: PublishWorkflowStepInput[] = [
    { kind: PublishWorkflowStepKind.Checkout, name: '📥 Checkout' },
    { kind: PublishWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv', id: 'setup' },
    { kind: PublishWorkflowStepKind.ConfigureReleaseAuthor, name: '🤖 Configure release author' },
  ];
  if (isSmoothBricksCodebasePackageName(options.repoName)) {
    setupSteps.push({ kind: PublishWorkflowStepKind.BuildSelfHostedCli, name: '🏗️ Build self-hosted smoo' });
  }
  const releaseSteps: PublishWorkflowStepInput[] = [
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
      { kind: PublishWorkflowStepKind.VersionRelease, name: '🏷️ Version release', id: 'version' },
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
        case PublishWorkflowStepKind.SetupDevenv:
          setupOutputs = await context.callbacks.setupDevenv();
          break;
        case PublishWorkflowStepKind.ConfigureReleaseAuthor:
          await context.callbacks.configureReleaseAuthor();
          break;
        case PublishWorkflowStepKind.BuildSelfHostedCli:
          await context.callbacks.buildSelfHostedCli();
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
    return version.mode !== 'none' && inputs.deployEnvironment === 'production' && !inputs.dryRun;
  }
  if (step.condition === 'failure') {
    return failed;
  }
  return true;
}

export function renderPublishWorkflowYaml(options: PublishWorkflowDefinitionOptions = {}): string {
  if (hasMacosPlatformTargets(options)) {
    return renderPlatformPublishWorkflowYaml(options);
  }
  if (hasLinuxPlatformTargets(options)) {
    return `${renderPublishWorkflowHeader(options)}${renderSingleJobPublishWorkflowSteps(
      definePublishWorkflow(options).steps,
      options,
    )}`;
  }
  const steps = definePublishWorkflow(options).steps;
  return `${renderPublishWorkflowHeader(options)}${renderPublishWorkflowSteps(steps, options)}`;
}

function renderPublishWorkflowHeader(options: PublishWorkflowDefinitionOptions): string {
  const deployInput =
    options.deploy === true
      ? `
      deploy_environment:
        type: choice
        description: Deploy live systems after a successful publish.
        options: [none, production]
        default: none`
      : '';
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
      dry_run:
        type: boolean
        description: Run release commands without writing versions, tags, publishes, or GitHub Releases.
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
    runs-on: ubuntu-latest
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}
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
  if (step.kind === PublishWorkflowStepKind.PublishRelease) {
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
      '      # Step 3. Composite action internals do not affect top-level job step',
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
    case PublishWorkflowStepKind.SetupDevenv:
      return [`      - name: ${step.name}`, '        id: setup', '        uses: ./.github/actions/setup-devenv'];
    case PublishWorkflowStepKind.ConfigureReleaseAuthor:
      return [
        `      - name: ${step.name}`,
        '        run:',
        '          git config user.name "github-actions[bot]" && git config user.email',
        '          "41898282+github-actions[bot]@users.noreply.github.com"',
      ];
    case PublishWorkflowStepKind.BuildSelfHostedCli:
      return [
        `      - name: ${step.name}`,
        '        # SmoothBricks self-hosts smoo from source for release commands.',
        '        run: nx build cli',
      ];
    case PublishWorkflowStepKind.RepairPendingReleases:
      return [
        `      - name: ${step.name}`,
        `        run: smoo release repair-pending --dry-run "${githubExpression('inputs.dry_run')}"`,
      ];
    case PublishWorkflowStepKind.VersionRelease:
      return [
        `      - name: ${step.name}`,
        '        id: version',
        '        run:',
        `          smoo release version --bump "${githubExpression('inputs.bump')}" --dry-run "${githubExpression('inputs.dry_run')}" --github-output`,
        '          "$GITHUB_OUTPUT"',
      ];
    case PublishWorkflowStepKind.CheckManagedMonorepoFiles:
      return conditionalRunStep(step, 'smoo monorepo check');
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
      return [
        `      - name: ${step.name}`,
        `        if: ${githubExpression('failure()')}`,
        '        uses: actions/upload-artifact@v7.0.1',
        '        with:',
        `          name: trace-results-${githubExpression('github.run_id')}`,
        '          path: packages/*/.trace-results.db',
        '          if-no-files-found: ignore',
        '          retention-days: 14',
        '          include-hidden-files: true',
      ];
    case PublishWorkflowStepKind.ValidateMonorepoConfig:
      return conditionalRunStep(step, 'smoo monorepo validate');
    case PublishWorkflowStepKind.PublishRelease:
      return [
        `      - name: ${step.name}`,
        '        # smoo packs with Bun, then publishes tarballs with npm. Existing',
        '        # packages must already exist on npm and use trusted publishing/OIDC.',
        '        # Missing package names are bootstrapped locally before trust setup.',
        `        run: smoo release publish --bump "${githubExpression('inputs.bump')}" --dry-run "${githubExpression('inputs.dry_run')}"`,
      ];
    case PublishWorkflowStepKind.DeployProduction:
      return deployProductionStep(step, options);
    case PublishWorkflowStepKind.SaveNixDevenv:
      return [
        `      - name: ${step.name}`,
        `        if: ${githubExpression('always()')}`,
        '        uses: ./.github/actions/save-nix-devenv',
        '        with:',
        `          nix-cache-hit: ${githubExpression('steps.setup.outputs.nix-cache-hit')}`,
        `          devenv-cache-hit: ${githubExpression('steps.setup.outputs.devenv-cache-hit')}`,
      ];
  }
}

function deployProductionStep(step: PublishWorkflowStep, options: PublishWorkflowDefinitionOptions): string[] {
  return [
    `      - name: ${step.name}`,
    '        if:',
    "          ${{ steps.version.outputs.mode != 'none' && inputs.deploy_environment == 'production' && inputs.dry_run !=",
    "          'true' }}",
    ...deployEnvLines(options),
    '        run: smoo github-ci nx-deploy --configuration production --mode run-many --verify --name "Deploy Production"',
  ];
}

function deployEnvLines(options: PublishWorkflowDefinitionOptions): string[] {
  if (options.deployProvider !== 'cloudflare') {
    return [];
  }
  return [
    '        env:',
    '          CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}',
    '          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}',
  ];
}

function conditionalRunStep(step: PublishWorkflowStep, run: string): string[] {
  const condition = "steps.version.outputs.mode != 'none'";
  return [`      - name: ${step.name}`, `        if: ${githubExpression(condition)}`, `        run: ${run}`];
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
        `        if: ${githubExpression("steps.version.outputs.mode != 'none'")}`,
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
      deploy_environment:
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
      dry_run:
        type: boolean
        description: Run release commands without writing versions, tags, publishes, or GitHub Releases.
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
  macos-release-candidate:
    runs-on: macos-latest
    outputs:
      mode: ${githubExpression('steps.version.outputs.mode')}
      projects: ${githubExpression('steps.version.outputs.projects')}
      release-sha: ${githubExpression('steps.release-state.outputs.sha')}
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}
    steps:
${renderMacosReleaseCandidateSteps(steps, options)}

  publish-on-linux:
    needs: macos-release-candidate
    runs-on: ubuntu-latest
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}
    steps:
${renderFinalLinuxPublishSteps(options)}
`;
}

function renderMacosReleaseCandidateSteps(
  steps: PublishWorkflowStep[],
  options: PublishWorkflowDefinitionOptions,
): string {
  const lines: string[] = [];
  let stepNumber = 2;
  for (const step of steps) {
    if (!MACOS_CANDIDATE_STEP_KINDS.includes(step.kind)) {
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
    lines.push('');
  }
  lines.push(
    `      # Step ${stepNumber}`,
    '      - name: 🍎 Build macOS and iOS targets',
    `        if: ${githubExpression("steps.version.outputs.mode != 'none'")}`,
    '        env:',
    `          GITHUB_SHA: ${githubExpression('steps.release-state.outputs.sha')}`,
    '        run:',
    `          smoo github-ci nx-run-many --targets "${MACOS_PLATFORM_TARGET_GLOBS.join(',')}" --projects`,
    `          "${githubExpression('steps.version.outputs.projects')}" --collect-outputs`,
    `          "${githubExpression('runner.temp')}/macos-platform-outputs"`,
    '',
    '      # --- Candidate transfer --------------------------------------------------',
    '',
    `      # Step ${stepNumber + 1}`,
    '      - name: 📦 Bundle validated release state',
    '        run:',
    `          mkdir -p "${githubExpression('runner.temp')}/publish-release-state" && git bundle create`,
    `          "${githubExpression('runner.temp')}/publish-release-state/release-state.bundle" HEAD --tags && git rev-parse HEAD >`,
    `          "${githubExpression('runner.temp')}/publish-release-state/release-head"`,
    '',
    `      # Step ${stepNumber + 2}`,
    '      - name: 📤 Upload validated release state',
    '        uses: actions/upload-artifact@v7.0.1',
    '        with:',
    `          name: publish-release-state-${githubExpression('github.run_id')}`,
    `          path: ${githubExpression('runner.temp')}/publish-release-state`,
    '          if-no-files-found: error',
    '          retention-days: 1',
    '',
    `      # Step ${stepNumber + 3}`,
    '      - name: 📤 Upload macOS platform outputs',
    `        if: ${githubExpression("steps.version.outputs.mode != 'none'")}`,
    '        uses: actions/upload-artifact@v7.0.1',
    '        with:',
    `          name: publish-macos-outputs-${githubExpression('github.run_id')}`,
    `          path: ${githubExpression('runner.temp')}/macos-platform-outputs`,
    '          if-no-files-found: error',
    '          retention-days: 1',
    '',
    '      # --- Cleanup ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber + 4}`,
    '      - name: 🧹 Cleanup and cache Nix/devenv',
    `        if: ${githubExpression('always()')}`,
    '        uses: ./.github/actions/save-nix-devenv',
    '        with:',
    `          nix-cache-hit: ${githubExpression('steps.setup.outputs.nix-cache-hit')}`,
    `          devenv-cache-hit: ${githubExpression('steps.setup.outputs.devenv-cache-hit')}`,
  );
  return lines.join('\n').trimEnd();
}

function renderFinalLinuxPublishSteps(options: PublishWorkflowDefinitionOptions): string {
  const mode = 'needs.macos-release-candidate.outputs.mode';
  const projects = githubExpression('needs.macos-release-candidate.outputs.projects');
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
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 📥 Download candidate artifacts',
    '        uses: actions/download-artifact@v8.0.1',
    '        with:',
    `          pattern: publish-*-${githubExpression('github.run_id')}`,
    `          path: ${githubExpression('runner.temp')}/publish-artifacts`,
    '          merge-multiple: false',
    '',
    `      # Step ${stepNumber++}`,
    '      - name: ♻️ Restore validated release state',
    '        run:',
    `          git fetch "${githubExpression(
      'runner.temp',
    )}/publish-artifacts/publish-release-state-${githubExpression('github.run_id')}/release-state.bundle" HEAD --tags && git reset`,
    `          --hard "$(cat "${githubExpression(
      'runner.temp',
    )}/publish-artifacts/publish-release-state-${githubExpression('github.run_id')}/release-head")"`,
    '',
    `      # Step ${stepNumber}. Composite action internals do not affect top-level job step`,
    '      # anchors; update these comments if top-level steps move.',
    '      - name: 🧱 Setup Nix/devenv',
    '        id: setup',
    '        uses: ./.github/actions/setup-devenv',
    '',
    `      # Step ${++stepNumber}`,
    '      - name: 🤖 Configure release author',
    '        run:',
    '          git config user.name "github-actions[bot]" && git config user.email',
    '          "41898282+github-actions[bot]@users.noreply.github.com"',
  ];
  stepNumber += 1;
  if (isSmoothBricksCodebasePackageName(options.repoName)) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: 🏗️ Build self-hosted smoo',
      '        # SmoothBricks self-hosts smoo from source for release commands.',
      '        run: nx build cli',
    );
  }
  lines.push(
    '',
    '      # --- Validation ---------------------------------------------------------',
    '',
    '      # Release validation intentionally does not restore persisted Nx task',
    '      # cache. Nx may reuse tasks produced earlier in this same job, but publish',
    '      # never relies on task outputs restored from CI cache. version runs before',
    '      # validation so the commit completed below is the commit that was checked.',
    '',
    `      # Step ${stepNumber++}`,
    `      - name: 🔨 Build (${githubExpression(mode)})`,
    `        if: ${githubExpression(`${mode} != 'none'`)}`,
    `        run: smoo github-ci nx-run-many --targets build --projects "${projects}"`,
  );
  if (hasLinuxPlatformTargets(options)) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: 🐧 Build supplemental Linux targets',
      `        if: ${githubExpression(`${mode} != 'none'`)}`,
      `        run: smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(
        ',',
      )}" --projects "${projects}"`,
    );
  }
  lines.push(
    '',
    `      # Step ${stepNumber++}`,
    `      - name: 🔍 Lint (${githubExpression(mode)})`,
    `        if: ${githubExpression(`${mode} != 'none'`)}`,
    `        run: smoo github-ci nx-run-many --targets lint --projects "${projects}"`,
    '',
    `      # Step ${stepNumber++}`,
    `      - name: 🧪 Unit Tests (${githubExpression(mode)})`,
    `        if: ${githubExpression(`${mode} != 'none'`)}`,
    `        run: smoo github-ci nx-run-many --targets test --projects "${projects}"`,
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 📎 Upload trace DBs',
    `        if: ${githubExpression('failure()')}`,
    '        uses: actions/upload-artifact@v7.0.1',
    '        with:',
    `          name: trace-results-${githubExpression('github.run_id')}`,
    '          path: packages/*/.trace-results.db',
    '          if-no-files-found: ignore',
    '          retention-days: 14',
    '          include-hidden-files: true',
    '',
    `      # Step ${stepNumber++}`,
    '      - name: 📦 Apply verified native outputs',
    `        if: ${githubExpression(`${mode} != 'none'`)}`,
    '        run:',
    `          smoo github-ci apply-outputs --source-sha "${githubExpression(
      'needs.macos-release-candidate.outputs.release-sha',
    )}"`,
    `          "${githubExpression('runner.temp')}/publish-artifacts/publish-macos-outputs-${githubExpression(
      'github.run_id',
    )}"`,
    '',
    `      # Step ${stepNumber++}`,
    `      - name: ✅ Validate restored release (${githubExpression(mode)})`,
    `        if: ${githubExpression(`${mode} != 'none'`)}`,
    '        run: smoo monorepo validate',
    '',
    '      # --- Release ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber++}`,
    `      - name: 📦 Publish release (${githubExpression(mode)})`,
    '        # smoo packs with Bun, then publishes tarballs with npm. Existing',
    '        # packages must already exist on npm and use trusted publishing/OIDC.',
    '        # Missing package names are bootstrapped locally before trust setup.',
    `        run: smoo release publish --bump "${githubExpression('inputs.bump')}" --dry-run "${githubExpression(
      'inputs.dry_run',
    )}"`,
  );
  if (options.deploy === true) {
    lines.push(
      '',
      `      # Step ${stepNumber++}`,
      '      - name: 🚀 Deploy production',
      '        if:',
      "          ${{ needs.macos-release-candidate.outputs.mode != 'none' && inputs.deploy_environment == 'production' &&",
      "          inputs.dry_run != 'true' }}",
      ...deployEnvLines(options),
      '        run: smoo github-ci nx-deploy --configuration production --mode run-many --verify --name "Deploy Production"',
    );
  }
  lines.push(
    '',
    '      # --- Cleanup ------------------------------------------------------------',
    '',
    `      # Step ${stepNumber}`,
    '      - name: 🧹 Cleanup and cache Nix/devenv',
    `        if: ${githubExpression('always()')}`,
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

function hasLinuxPlatformTargets(options: PublishWorkflowDefinitionOptions): boolean {
  return LINUX_PLATFORM_TARGET_GLOBS.some((glob) => options.platformTargetGlobs?.includes(glob) === true);
}

function githubExpression(expression: string): string {
  return ['$', '{{ ', expression, ' }}'].join('');
}
