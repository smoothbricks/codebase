export type PublishWorkflowBump = 'auto' | 'patch' | 'minor' | 'major' | 'prerelease';
export type PublishWorkflowCondition = 'version-mode-not-none' | 'failure' | 'always';
export type PublishWorkflowNxTarget = 'build' | 'lint' | 'test';

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
  SaveNixDevenv = 'save-nix-devenv',
}

export interface PublishWorkflowStep {
  kind: PublishWorkflowStepKind;
  name: string;
  number: number;
  id?: string;
  condition?: PublishWorkflowCondition;
  nxTarget?: PublishWorkflowNxTarget;
  needsNodeAuthToken?: boolean;
}

export interface PublishWorkflowDefinition {
  steps: PublishWorkflowStep[];
}

export interface PublishWorkflowDefinitionOptions {
  repoName?: string;
}

export interface PublishWorkflowInputs {
  bump: PublishWorkflowBump;
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
  repairPendingReleases(input: { dryRun: boolean; nodeAuthToken: boolean }): Promise<void>;
  versionRelease(input: { bump: PublishWorkflowBump; dryRun: boolean }): Promise<PublishWorkflowVersionOutputs>;
  checkManagedMonorepoFiles(): Promise<void>;
  nxRunMany(input: { target: PublishWorkflowNxTarget; projects: string[] }): Promise<void>;
  uploadTraceDbs(): Promise<void>;
  validateMonorepoConfig(): Promise<void>;
  publishRelease(input: { bump: PublishWorkflowBump; dryRun: boolean; nodeAuthToken: boolean }): Promise<void>;
  saveNixDevenv(input: PublishWorkflowSetupOutputs): Promise<void>;
}

export interface PublishWorkflowRunContext {
  inputs: PublishWorkflowInputs;
  nodeAuthToken: boolean;
  callbacks: PublishWorkflowCallbacks;
}

export interface PublishWorkflowRunResult {
  version: PublishWorkflowVersionOutputs;
  failed: boolean;
}

type PublishWorkflowStepInput = Omit<PublishWorkflowStep, 'number'>;

export function definePublishWorkflow(options: PublishWorkflowDefinitionOptions = {}): PublishWorkflowDefinition {
  const versionMode = githubExpression('steps.version.outputs.mode');
  const setupSteps: PublishWorkflowStepInput[] = [
    { kind: PublishWorkflowStepKind.Checkout, name: '📥 Checkout' },
    { kind: PublishWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv', id: 'setup' },
    { kind: PublishWorkflowStepKind.ConfigureReleaseAuthor, name: '🤖 Configure release author' },
  ];
  if (options.repoName === '@smoothbricks/codebase') {
    setupSteps.push({ kind: PublishWorkflowStepKind.BuildSelfHostedCli, name: '🏗️ Build self-hosted smoo' });
  }
  return {
    steps: numberWorkflowSteps([
      ...setupSteps,
      {
        kind: PublishWorkflowStepKind.RepairPendingReleases,
        name: '🧯 Repair pending releases',
        needsNodeAuthToken: true,
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
      {
        kind: PublishWorkflowStepKind.PublishRelease,
        name: `📦 Publish release (${versionMode})`,
        needsNodeAuthToken: true,
      },
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
    if (!shouldRunStep(step, version, failed)) {
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
            nodeAuthToken: context.nodeAuthToken,
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
            nodeAuthToken: context.nodeAuthToken,
          });
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

function shouldRunStep(step: PublishWorkflowStep, version: PublishWorkflowVersionOutputs, failed: boolean): boolean {
  if (step.condition === 'version-mode-not-none') {
    return version.mode !== 'none';
  }
  if (step.condition === 'failure') {
    return failed;
  }
  return true;
}

export function renderPublishWorkflowYaml(options: PublishWorkflowDefinitionOptions = {}): string {
  const steps = definePublishWorkflow(options).steps;
  return `${renderPublishWorkflowHeader()}${renderPublishWorkflowSteps(steps)}`;
}

function renderPublishWorkflowHeader(): string {
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
        default: false

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

function renderPublishWorkflowSteps(steps: PublishWorkflowStep[]): string {
  const lines: string[] = [];
  for (const step of steps) {
    lines.push(...sectionLinesBefore(step));
    lines.push(...commentLinesForStep(step));
    lines.push(...yamlLinesForStep(step));
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

function yamlLinesForStep(step: PublishWorkflowStep): string[] {
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
        '        # SmoothBricks self-hosts smoo from source, but Nx release loads',
        '        # @smoothbricks/cli/nx-version-actions through the built package export.',
        '        run: nx build @smoothbricks/cli',
      ];
    case PublishWorkflowStepKind.RepairPendingReleases:
      return [
        `      - name: ${step.name}`,
        '        env:',
        `          NODE_AUTH_TOKEN: ${githubExpression('secrets.NPM_TOKEN')}`,
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
        '        uses: actions/upload-artifact@v4',
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
        '        # packages use trusted publishing/OIDC; first publishes use NODE_AUTH_TOKEN',
        '        # because npm trust can only be configured after a package exists.',
        '        env:',
        `          NODE_AUTH_TOKEN: ${githubExpression('secrets.NPM_TOKEN')}`,
        `        run: smoo release publish --bump "${githubExpression('inputs.bump')}" --dry-run "${githubExpression('inputs.dry_run')}"`,
      ];
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

function conditionalRunStep(step: PublishWorkflowStep, run: string): string[] {
  return [
    `      - name: ${step.name}`,
    `        if: ${githubExpression("steps.version.outputs.mode != 'none'")}`,
    `        run: ${run}`,
  ];
}

function githubExpression(expression: string): string {
  return ['$', '{{ ', expression, ' }}'].join('');
}
