import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export const BOUNDED_TEST_EXECUTOR = '@smoothbricks/nx-plugin:bounded-exec';
export const BOUNDED_TEST_TIMEOUT_MS = 600_000;
export const BOUNDED_TEST_KILL_AFTER_MS = 10_000;

export interface BoundedTestPolicyPackageJson {
  name?: string;
  workspaces?: unknown;
  scripts?: Record<string, unknown>;
  nx?: {
    name?: string;
    targets?: Record<string, Record<string, unknown>>;
  };
}

export interface BoundedTestPolicyIssue {
  path: string;
  message: string;
}

export interface ApplyBoundedTestTargetPolicyOptions {
  projectName: string;
  defaultCommand?: string;
}

export function applyBoundedTestTargetPolicy(
  packageJson: BoundedTestPolicyPackageJson,
  options: ApplyBoundedTestTargetPolicyOptions,
): void {
  const command = resolveTestCommand(packageJson, options.defaultCommand ?? 'bun test');

  packageJson.nx ??= {};
  packageJson.nx.targets ??= {};

  const existingTestTarget = packageJson.nx.targets.test;
  const nextTestTarget: Record<string, unknown> = isRecord(existingTestTarget) ? { ...existingTestTarget } : {};
  nextTestTarget.executor = BOUNDED_TEST_EXECUTOR;
  nextTestTarget.options = {
    command,
    cwd: '{projectRoot}',
    timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
    killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
  };

  packageJson.nx.targets.test = nextTestTarget;
  packageJson.scripts ??= {};
  packageJson.scripts.test = boundedTestScriptAlias(options.projectName);
}

export function applyWorkspaceBoundedTestTargetPolicy(root: string): boolean {
  let changed = false;
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const packageJson = readPackageJson(packageJsonPath);
    if (!hasTestEntrypoint(packageJson)) {
      continue;
    }
    const projectName = packageProjectName(packageJson);
    if (!projectName) {
      continue;
    }
    const before = JSON.stringify(packageJson);
    applyBoundedTestTargetPolicy(packageJson, { projectName });
    if (JSON.stringify(packageJson) === before) {
      continue;
    }
    writePackageJson(packageJsonPath, packageJson);
    changed = true;
  }
  return changed;
}

export function checkWorkspaceBoundedTestTargetPolicy(root: string): BoundedTestPolicyIssue[] {
  const issues: BoundedTestPolicyIssue[] = [];
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const packageJson = readPackageJson(packageJsonPath);
    if (!hasTestEntrypoint(packageJson)) {
      continue;
    }
    const projectName = packageProjectName(packageJson);
    if (!projectName) {
      issues.push({ path: packageJsonPath, message: 'test entrypoint requires package.json name or nx.name' });
      continue;
    }
    if (!checkBoundedTestTargetPolicy(packageJson, { projectName })) {
      issues.push({
        path: packageJsonPath,
        message: `nx.targets.test must use ${BOUNDED_TEST_EXECUTOR} with bounded test policy`,
      });
    }
  }
  return issues;
}

export function checkBoundedTestTargetPolicy(
  packageJson: BoundedTestPolicyPackageJson,
  options: ApplyBoundedTestTargetPolicyOptions,
): boolean {
  const testTarget = packageJson.nx?.targets?.test;
  if (!isRecord(testTarget) || testTarget.executor !== BOUNDED_TEST_EXECUTOR) {
    return false;
  }
  const targetOptions = testTarget.options;
  if (
    !isRecord(targetOptions) ||
    typeof targetOptions.command !== 'string' ||
    targetOptions.command.length === 0 ||
    isPackageTestScriptRunnerCommand(targetOptions.command) ||
    targetOptions.cwd !== '{projectRoot}' ||
    targetOptions.timeoutMs !== BOUNDED_TEST_TIMEOUT_MS ||
    targetOptions.killAfterMs !== BOUNDED_TEST_KILL_AFTER_MS
  ) {
    return false;
  }
  return packageJson.scripts?.test === boundedTestScriptAlias(options.projectName);
}

export function boundedTestScriptAlias(projectName: string): string {
  return `nx run ${projectName}:test --tui=false --outputStyle=stream`;
}

export function resolveTestCommand(packageJson: BoundedTestPolicyPackageJson, defaultCommand = 'bun test'): string {
  const existingTarget = packageJson.nx?.targets?.test;
  if (isRecord(existingTarget)) {
    const targetOptions = existingTarget.options;
    if (
      isRecord(targetOptions) &&
      typeof targetOptions.command === 'string' &&
      targetOptions.command.length > 0 &&
      !isPackageTestScriptRunnerCommand(targetOptions.command)
    ) {
      return targetOptions.command;
    }
  }

  const scriptCommand = packageJson.scripts?.test;
  if (typeof scriptCommand === 'string' && !isNxRunTestAlias(scriptCommand)) {
    return scriptCommand;
  }

  return defaultCommand;
}

function isPackageTestScriptRunnerCommand(command: string): boolean {
  return /^(?:bun\s+run|npm(?:\s+run)?|pnpm(?:\s+run)?|yarn(?:\s+run)?)\s+test(?:\s|$)/.test(command.trim());
}

function isNxRunTestAlias(command: string): boolean {
  return /^nx\s+run\s+[^\s:]+:test(?:\s|$)/.test(command.trim());
}

function listWorkspacePackageJsonPaths(root: string): string[] {
  const rootPackagePath = join(root, 'package.json');
  if (!existsSync(rootPackagePath)) {
    return [];
  }
  const rootPackage = readPackageJson(rootPackagePath);
  const workspacePatterns = Array.isArray(rootPackage.workspaces)
    ? rootPackage.workspaces.filter((entry): entry is string => typeof entry === 'string')
    : [];
  const paths: string[] = [];
  for (const pattern of workspacePatterns) {
    if (!pattern.endsWith('/*')) {
      continue;
    }
    const parent = join(root, pattern.slice(0, -2));
    if (!existsSync(parent)) {
      continue;
    }
    for (const entry of readdirSync(parent, { withFileTypes: true })) {
      if (!entry.isDirectory()) {
        continue;
      }
      const packageJsonPath = join(parent, entry.name, 'package.json');
      if (existsSync(packageJsonPath)) {
        paths.push(packageJsonPath);
      }
    }
  }
  return paths.sort((a, b) => a.localeCompare(b));
}

function readPackageJson(path: string): BoundedTestPolicyPackageJson {
  return JSON.parse(readFileSync(path, 'utf8')) as BoundedTestPolicyPackageJson;
}

function writePackageJson(path: string, packageJson: BoundedTestPolicyPackageJson): void {
  writeFileSync(path, `${JSON.stringify(packageJson, null, 2)}\n`);
}

function hasTestEntrypoint(packageJson: BoundedTestPolicyPackageJson): boolean {
  return typeof packageJson.scripts?.test === 'string' || isRecord(packageJson.nx?.targets?.test);
}

function packageProjectName(packageJson: BoundedTestPolicyPackageJson): string | null {
  return packageJson.nx?.name ?? packageJson.name ?? null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}
