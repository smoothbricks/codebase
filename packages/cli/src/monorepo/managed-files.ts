import { execFileSync } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { listReleasePackages, readPackageJson } from '../lib/workspace.js';
import { renderCiWorkflowYaml } from './ci-workflow.js';
import { renderPublishWorkflowYaml } from './publish-workflow.js';

type ManagedKind = 'raw' | 'template' | 'generated';

interface ManagedFile {
  kind: ManagedKind;
  source: string;
  target: string;
  executable?: boolean;
  releasePackagesOnly?: boolean;
}

export interface FileResult {
  target: string;
  action: 'created' | 'updated' | 'unchanged' | 'skipped' | 'skipped-symlink' | 'drifted' | 'ok-symlink';
}

interface ManagedFileContext {
  hasReleasePackages: boolean;
  hasStagingDeployTargets: boolean;
  hasProductionDeployTargets: boolean;
  stagingDeployProvider?: 'cloudflare';
  productionDeployProvider?: 'cloudflare';
  ciPushBranches: string[];
  nodeModulesCacheKey: string;
  repoName: string;
}

interface DeployTargetInfo {
  exists: boolean;
  provider?: 'cloudflare';
}

const managedFiles: ManagedFile[] = [
  {
    kind: 'raw',
    source: 'tooling/direnv/repo-path',
    target: 'tooling/direnv/repo-path',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/github-actions-bootstrap.sh',
    target: 'tooling/direnv/github-actions-bootstrap.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/pre-commit.sh',
    target: 'tooling/git-hooks/pre-commit.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/commit-msg.sh',
    target: 'tooling/git-hooks/commit-msg.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'git-format-staged.yml',
    target: '.git-format-staged.yml',
  },
  {
    kind: 'generated',
    source: 'ci-workflow',
    target: '.github/workflows/ci.yml',
  },
  {
    kind: 'generated',
    source: 'publish-workflow',
    target: '.github/workflows/publish.yml',
    releasePackagesOnly: true,
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nix-devenv/action.yml',
    target: '.github/actions/cache-nix-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/setup-devenv/action.yml',
    target: '.github/actions/setup-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/save-nix-devenv/action.yml',
    target: '.github/actions/save-nix-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-node-modules/action.yml',
    target: '.github/actions/cache-node-modules/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nx/action.yml',
    target: '.github/actions/cache-nx/action.yml',
  },
];

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

export function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): FileResult[] {
  const context = getManagedFileContext(root);
  return managedFiles.map((file) => applyManagedFile(root, file, mode, context));
}

function applyManagedFile(
  root: string,
  file: ManagedFile,
  mode: 'update' | 'check' | 'diff',
  context: ManagedFileContext,
): FileResult {
  if (file.releasePackagesOnly === true && !context.hasReleasePackages && !context.hasProductionDeployTargets) {
    return { target: file.target, action: 'skipped' };
  }
  const target = resolve(root, file.target);
  const content = getManagedContent(file, context);
  if (existsSync(target)) {
    const info = lstatSync(target);
    if (info.isSymbolicLink()) {
      return { target: file.target, action: mode === 'check' ? 'ok-symlink' : 'skipped-symlink' };
    }
    if (!info.isFile()) {
      throw new Error(`${file.target} exists but is not a regular file or symlink`);
    }
    const current = readFileSync(target, 'utf8');
    if (current === content) {
      return { target: file.target, action: 'unchanged' };
    }
    if (mode === 'check' || mode === 'diff') {
      return { target: file.target, action: 'drifted' };
    }
    writeManagedFile(target, content, file.executable === true);
    return { target: file.target, action: 'updated' };
  }
  if (mode === 'check' || mode === 'diff') {
    return { target: file.target, action: 'drifted' };
  }
  writeManagedFile(target, content, file.executable === true);
  return { target: file.target, action: 'created' };
}

function getManagedContent(file: ManagedFile, context: ManagedFileContext): string {
  if (file.kind === 'generated') {
    if (file.source === 'ci-workflow') {
      return renderCiWorkflowYaml({
        deploy: context.hasStagingDeployTargets,
        deployProvider: context.stagingDeployProvider,
        pushBranches: context.ciPushBranches,
      });
    }
    if (file.source === 'publish-workflow') {
      return renderPublishWorkflowYaml({
        deploy: context.hasProductionDeployTargets,
        deployProvider: context.productionDeployProvider,
        repoName: context.repoName,
      });
    }
    throw new Error(`Unknown generated managed file source ${file.source}`);
  }
  const sourceRoot = file.kind === 'raw' ? 'managed/raw' : 'managed/templates';
  const sourcePath = join(packageRoot, sourceRoot, file.source);
  const content = readFileSync(sourcePath, 'utf8');
  if (file.kind === 'raw') {
    return content;
  }
  return renderTemplate(context, content);
}

function getManagedFileContext(root: string): ManagedFileContext {
  const packageJson = readPackageJson(join(root, 'package.json'));
  const repoName = packageJson?.name ?? 'monorepo';
  const ciPushBranches = getCiPushBranches(packageJson?.json);
  const stagingDeploy = getDeployTargetInfo(root, 'staging');
  const productionDeploy = getDeployTargetInfo(root, 'production');
  const nodeModulesCacheKey = existsSync(join(root, 'bun.lock'))
    ? `$${"{{ hashFiles('bun.lock', 'package.json', 'packages/*/package.json') }}"}`
    : `$${"{{ hashFiles('bun.lockb', 'package.json', 'packages/*/package.json') }}"}`;
  return {
    hasReleasePackages: listReleasePackages(root, packageJson).length > 0,
    hasStagingDeployTargets: stagingDeploy.exists,
    hasProductionDeployTargets: productionDeploy.exists,
    stagingDeployProvider: stagingDeploy.provider,
    productionDeployProvider: productionDeploy.provider,
    ciPushBranches,
    nodeModulesCacheKey,
    repoName,
  };
}

function renderTemplate(context: ManagedFileContext, template: string): string {
  return template
    .replaceAll('{{REPO_NAME}}', context.repoName)
    .replaceAll('__SMOO_CI_PUSH_BRANCHES__', renderYamlFlowList(context.ciPushBranches))
    .replaceAll('{{NODE_MODULES_CACHE_KEY}}', context.nodeModulesCacheKey);
}

function getDeployTargetInfo(root: string, configuration: string): DeployTargetInfo {
  const output = execFileSync('nx', ['show', 'projects', '--withTarget', 'deploy', '--json'], {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const projects: unknown = JSON.parse(output);
  if (!Array.isArray(projects) || !projects.every((project) => typeof project === 'string')) {
    return { exists: false };
  }
  let exists = false;
  let provider: DeployTargetInfo['provider'];
  for (const project of projects) {
    const target = nxDeployTarget(root, project, configuration);
    if (!target.exists) {
      continue;
    }
    exists = true;
    provider ??= target.provider;
  }
  return { exists, provider };
}

function nxDeployTarget(root: string, project: string, configuration: string): DeployTargetInfo {
  const output = execFileSync('nx', ['show', 'project', project, '--json'], {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const parsed: unknown = JSON.parse(output);
  const projectJson = recordValue(parsed);
  const targets = recordValue(projectJson?.targets);
  const deploy = recordValue(targets?.deploy);
  const configurations = recordValue(deploy?.configurations);
  const config = recordValue(configurations?.[configuration]);
  if (!config) {
    return { exists: false };
  }
  const command = deployCommand(deploy, config);
  return { exists: true, provider: command.includes('wrangler ') ? 'cloudflare' : undefined };
}

function deployCommand(deploy: Record<string, unknown> | undefined, config: Record<string, unknown>): string {
  const command = stringValue(config.command) ?? stringValue(recordValue(config.options)?.command);
  return command ?? stringValue(recordValue(deploy?.options)?.command) ?? '';
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function getCiPushBranches(packageJson: unknown): string[] {
  const configured = readCiPushBranches(packageJson);
  return configured.length > 0 ? configured : ['main'];
}

function readCiPushBranches(packageJson: unknown): string[] {
  const rootPackage = recordValue(packageJson);
  const smoo = recordValue(rootPackage?.smoo);
  const github = recordValue(smoo?.github);
  const branches = Array.isArray(github?.pushBranches) ? github.pushBranches : [];
  return branches.filter((branch): branch is string => typeof branch === 'string' && branch.length > 0);
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function renderYamlFlowList(values: string[]): string {
  return JSON.stringify(values);
}

function writeManagedFile(path: string, content: string, executable: boolean): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, { mode: executable ? 0o755 : 0o644 });
}

export function printResults(results: FileResult[]): void {
  for (const result of results) {
    console.log(`${result.action.padEnd(15)} ${result.target}`);
  }
}

export function validateManagedFiles(root: string): number {
  const results = applyManagedFiles(root, 'check');
  printResults(results);
  const failures = results.filter((result) => result.action === 'drifted').length;
  if (failures > 0) {
    console.error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
  return failures;
}
