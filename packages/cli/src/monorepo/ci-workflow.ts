/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are emitted literally. */

export enum CiWorkflowStepKind {
  Checkout = 'checkout',
  SetupDevenv = 'setup-devenv',
  SetNxShas = 'set-nx-shas',
  RestoreNxCache = 'restore-nx-cache',
  Build = 'build',
  Lint = 'lint',
  UnitTests = 'unit-tests',
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
  deployProvider?: 'cloudflare';
  pushBranches: string[];
}

type CiWorkflowStepInput = Omit<CiWorkflowStep, 'number'>;

export function defineCiWorkflow(options: CiWorkflowDefinitionOptions): CiWorkflowStep[] {
  const steps: CiWorkflowStepInput[] = [
    { kind: CiWorkflowStepKind.Checkout, name: '📥 Checkout' },
    { kind: CiWorkflowStepKind.SetupDevenv, name: '🧱 Setup Nix/devenv' },
    { kind: CiWorkflowStepKind.SetNxShas, name: '🧭 Set Nx SHAs' },
    { kind: CiWorkflowStepKind.RestoreNxCache, name: '🧠 Restore Nx cache' },
    { kind: CiWorkflowStepKind.Build, name: '🔨 Build' },
    { kind: CiWorkflowStepKind.Lint, name: '🔍 Lint' },
    { kind: CiWorkflowStepKind.UnitTests, name: '🧪 Unit Tests' },
  ];
  if (options.deploy) {
    steps.push({ kind: CiWorkflowStepKind.Deploy, name: '🚀 Deploy Staging' });
  }
  steps.push(
    { kind: CiWorkflowStepKind.SaveNxCache, name: '💾 Save Nx cache' },
    { kind: CiWorkflowStepKind.UploadTraceDbs, name: '📎 Upload trace DBs' },
    { kind: CiWorkflowStepKind.SaveNixDevenv, name: '🧹 Cleanup and cache Nix/devenv' },
  );
  return steps.map((step, index) => ({ ...step, number: index + 2 }));
}

export function renderCiWorkflowYaml(options: CiWorkflowDefinitionOptions): string {
  const steps = defineCiWorkflow(options);
  return `${renderCiWorkflowHeader(options)}${renderCiWorkflowSteps(steps, options)}`;
}

function renderCiWorkflowHeader(options: CiWorkflowDefinitionOptions): string {
  return `name: CI

on:
  push:
    branches:
${renderYamlList(options.pushBranches, 6)}
  pull_request:

permissions:
  actions: read
  contents: read
  statuses: write

defaults:
  run:
    working-directory: tooling/direnv

jobs:
  main:
    name: Validate
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      NIX_STORE_NAR: ${githubExpression('github.workspace')}/nix-store.nar
      GH_TOKEN: ${githubExpression('github.token')}
    steps:
`;
}

function githubExpression(expression: string): string {
  return `$${`{{ ${expression} }}`}`;
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
  if (step.kind === CiWorkflowStepKind.SetupDevenv) {
    return [
      '      # Step 3. Composite action internals do not affect top-level job step',
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
    case CiWorkflowStepKind.SetupDevenv:
      return [`      - name: ${step.name}`, '        id: setup', '        uses: ./.github/actions/setup-devenv'];
    case CiWorkflowStepKind.SetNxShas:
      return [
        `      - name: ${step.name}`,
        '        uses: nrwl/nx-set-shas@v5.0.1',
        '        with:',
        '          workflow-id: ci.yml',
      ];
    case CiWorkflowStepKind.RestoreNxCache:
      return [`      - name: ${step.name}`, '        id: nx-cache', '        uses: ./.github/actions/cache-nx'];
    case CiWorkflowStepKind.Build:
      return nxSmartStep(step, 'build', 'Build');
    case CiWorkflowStepKind.Lint:
      return nxSmartStep(step, 'lint', 'Lint');
    case CiWorkflowStepKind.UnitTests:
      return nxSmartStep(step, 'test', 'Unit Tests');
    case CiWorkflowStepKind.Deploy:
      return [
        `      - name: ${step.name}`,
        '        if:',
        "          ${{ github.event_name == 'push' && github.ref == format('refs/heads/{0}',",
        '          github.event.repository.default_branch) }}',
        ...deployEnvLines(options),
        `        run: smoo github-ci nx-deploy --configuration staging --mode affected --name "Deploy Staging" --step ${step.number}`,
      ];
    case CiWorkflowStepKind.SaveNxCache:
      return [
        `      - name: ${step.name}`,
        '        if:',
        "          ${{ github.event_name == 'push' && github.ref == format('refs/heads/{0}',",
        "          github.event.repository.default_branch) && steps.nx-cache.outputs.cache-hit != 'true' }}",
        '        uses: actions/cache/save@v5.0.5',
        '        with:',
        '          path: |',
        '            .nx/cache',
        '            .nx/workspace-data/*.db*',
        '          key: ${{ runner.os }}-nx-db-v1-${{ github.sha }}',
      ];
    case CiWorkflowStepKind.UploadTraceDbs:
      return [
        `      - name: ${step.name}`,
        '        if: ${{ always() }}',
        '        uses: actions/upload-artifact@v7.0.1',
        '        with:',
        '          name: trace-results-${{ github.run_id }}',
        '          path: packages/*/.trace-results.db',
        '          if-no-files-found: ignore',
        '          retention-days: 14',
        '          include-hidden-files: true',
      ];
    case CiWorkflowStepKind.SaveNixDevenv:
      return [
        `      - name: ${step.name}`,
        '        if: ${{ always() }}',
        '        uses: ./.github/actions/save-nix-devenv',
        '        with:',
        '          nix-cache-hit: ${{ steps.setup.outputs.nix-cache-hit }}',
        '          devenv-cache-hit: ${{ steps.setup.outputs.devenv-cache-hit }}',
      ];
  }
}

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
