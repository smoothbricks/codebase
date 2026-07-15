import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

export const BOUNDED_TEST_EXECUTOR = '@smoothbricks/nx-plugin:bounded-exec';
export const BOUNDED_TEST_TIMEOUT_MS = 120_000;
export const BOUNDED_TEST_KILL_AFTER_MS = 10_000;
// Per-test timeout passed to `bun test --timeout=<ms>`. Bun's default is
// 5000ms which is too tight for git-fixture tests on slower CI runners
// (Actions). The bunfig.toml `[test] timeout` key is silently ignored by
// Bun — the only working surfaces are this CLI flag and `setDefaultTimeout()`.
export const BOUNDED_TEST_PER_TEST_TIMEOUT_MS = 30_000;

export interface BoundedTestPolicyPackageJson {
  name?: string;
  workspaces?: unknown;
  scripts?: Record<string, unknown>;
  nx?: {
    name?: string;
    targets?: Record<string, Record<string, unknown>>;
  };
}

export interface BoundedTestPolicyProjectJson {
  name?: string;
  targets?: Record<string, Record<string, unknown>>;
}

export interface BoundedTestPolicyIssue {
  path: string;
  message: string;
}

export interface ApplyBoundedTestTargetPolicyOptions {
  projectName: string;
  defaultCommand?: string;
  projectJson?: BoundedTestPolicyProjectJson;
}

export function applyBoundedTestTargetPolicy(
  packageJson: BoundedTestPolicyPackageJson,
  options: ApplyBoundedTestTargetPolicyOptions,
): void {
  const command = resolveTestCommand(packageJson, options.defaultCommand ?? 'bun test', options.projectJson);
  let targetOwner = options.projectJson;
  if (!targetOwner) {
    packageJson.nx ??= {};
    targetOwner = packageJson.nx;
  }

  targetOwner.targets ??= {};

  const existingTestTarget = targetOwner.targets.test;
  const nextTestTarget: Record<string, unknown> = isRecord(existingTestTarget) ? { ...existingTestTarget } : {};
  nextTestTarget.executor = BOUNDED_TEST_EXECUTOR;
  nextTestTarget.options = {
    command,
    cwd: '{projectRoot}',
    timeoutMs: BOUNDED_TEST_TIMEOUT_MS,
    killAfterMs: BOUNDED_TEST_KILL_AFTER_MS,
  };

  targetOwner.targets.test = nextTestTarget;
  packageJson.scripts ??= {};
  packageJson.scripts.test = boundedTestScriptAlias(options.projectName);
}

export function applyWorkspaceBoundedTestTargetPolicy(root: string): boolean {
  let changed = false;
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const packageJson = readPackageJson(packageJsonPath);
    const projectJsonPath = projectJsonPathForPackageJson(packageJsonPath);
    const projectJson = existsSync(projectJsonPath) ? readProjectJson(projectJsonPath) : undefined;
    if (!hasTestEntrypoint(packageJson, projectJson)) {
      continue;
    }
    const projectName = packageProjectName(packageJson, projectJson);
    if (!projectName) {
      continue;
    }
    const beforePackageJson = JSON.stringify(packageJson);
    const beforeProjectJson = JSON.stringify(projectJson);
    applyBoundedTestTargetPolicy(packageJson, { projectName, projectJson });
    if (JSON.stringify(packageJson) === beforePackageJson && JSON.stringify(projectJson) === beforeProjectJson) {
      continue;
    }
    writePackageJson(packageJsonPath, packageJson);
    if (projectJson) {
      writeProjectJson(projectJsonPath, projectJson);
    }
    changed = true;
  }
  return changed;
}

export function checkWorkspaceBoundedTestTargetPolicy(root: string): BoundedTestPolicyIssue[] {
  const issues: BoundedTestPolicyIssue[] = [];
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const packageJson = readPackageJson(packageJsonPath);
    const projectJsonPath = projectJsonPathForPackageJson(packageJsonPath);
    const projectJson = existsSync(projectJsonPath) ? readProjectJson(projectJsonPath) : undefined;
    if (!hasTestEntrypoint(packageJson, projectJson)) {
      continue;
    }
    const projectName = packageProjectName(packageJson, projectJson);
    if (!projectName) {
      issues.push({
        path: packageJsonPath,
        message: 'test entrypoint requires package.json name, nx.name, or project.json name',
      });
      continue;
    }
    if (!checkBoundedTestTargetPolicy(packageJson, { projectName, projectJson })) {
      issues.push({
        path: projectJson ? projectJsonPath : packageJsonPath,
        message: `${projectJson ? 'targets' : 'nx.targets'}.test must use ${BOUNDED_TEST_EXECUTOR} with bounded test policy`,
      });
    }
  }
  return issues;
}

export function checkBoundedTestTargetPolicy(
  packageJson: BoundedTestPolicyPackageJson,
  options: ApplyBoundedTestTargetPolicyOptions,
): boolean {
  const testTarget = options.projectJson ? options.projectJson.targets?.test : packageJson.nx?.targets?.test;
  if (!isRecord(testTarget) || testTarget.executor !== BOUNDED_TEST_EXECUTOR) {
    return false;
  }
  const targetOptions = testTarget.options;
  if (
    !isRecord(targetOptions) ||
    typeof targetOptions.command !== 'string' ||
    targetOptions.command.length === 0 ||
    isPackageTestScriptRunnerCommand(targetOptions.command) ||
    targetOptions.command !== ensureBunTestTimeoutFlag(targetOptions.command) ||
    targetOptions.cwd !== '{projectRoot}' ||
    targetOptions.timeoutMs !== BOUNDED_TEST_TIMEOUT_MS ||
    targetOptions.killAfterMs !== BOUNDED_TEST_KILL_AFTER_MS
  ) {
    return false;
  }
  return packageJson.scripts?.test === boundedTestScriptAlias(options.projectName);
}

export function boundedTestScriptAlias(projectName: string): string {
  return `nx run ${projectName}:test --outputStyle=stream`;
}

export function resolveTestCommand(
  packageJson: BoundedTestPolicyPackageJson,
  defaultCommand = 'bun test',
  projectJson?: BoundedTestPolicyProjectJson,
): string {
  return ensureBunTestTimeoutFlag(resolveRawTestCommand(packageJson, defaultCommand, projectJson));
}

function resolveRawTestCommand(
  packageJson: BoundedTestPolicyPackageJson,
  defaultCommand: string,
  projectJson: BoundedTestPolicyProjectJson | undefined,
): string {
  const commandFromProjectTarget = resolveTargetCommand(projectJson?.targets?.test);
  if (commandFromProjectTarget) {
    return commandFromProjectTarget;
  }

  const commandFromPackageTarget = resolveTargetCommand(packageJson.nx?.targets?.test);
  if (commandFromPackageTarget) {
    return commandFromPackageTarget;
  }

  const scriptCommand = packageJson.scripts?.test;
  if (typeof scriptCommand === 'string' && !isNxRunTestAlias(scriptCommand)) {
    return scriptCommand;
  }

  return defaultCommand;
}

function resolveTargetCommand(existingTarget: unknown): string | null {
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
  return null;
}

const BUN_TEST_PREFIX = /^bun\s+test(?=\s|$)/;
const EXISTING_BUN_TIMEOUT = /(^|\s)--timeout(?:=|\s+)(\S+)/;

// Bun's `[test] timeout` bunfig key is silently ignored — only the CLI flag
// and `setDefaultTimeout()` actually take effect. Normalize every `bun test`
// command so the policy enforces the workspace per-test timeout in the one
// place where Bun will honor it.
export function ensureBunTestTimeoutFlag(command: string, timeoutMs = BOUNDED_TEST_PER_TEST_TIMEOUT_MS): string {
  if (!BUN_TEST_PREFIX.test(command)) {
    return command;
  }
  const flag = `--timeout=${timeoutMs}`;
  const existing = EXISTING_BUN_TIMEOUT.exec(command);
  if (!existing) {
    return command.replace(BUN_TEST_PREFIX, `bun test ${flag}`);
  }
  if (existing[2] === String(timeoutMs)) {
    return command;
  }
  return command.replace(EXISTING_BUN_TIMEOUT, `${existing[1]}${flag}`);
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
  const parsed = readJsonObject(path);
  const nx = isRecord(parsed.nx) ? parsed.nx : undefined;
  const normalizedNx = nx
    ? {
        ...nx,
        name: typeof nx.name === 'string' ? nx.name : undefined,
        targets: readTargets(nx.targets),
      }
    : undefined;
  return {
    ...parsed,
    name: typeof parsed.name === 'string' ? parsed.name : undefined,
    workspaces: parsed.workspaces,
    scripts: isRecord(parsed.scripts) ? parsed.scripts : undefined,
    nx: normalizedNx,
  };
}

function writePackageJson(path: string, packageJson: BoundedTestPolicyPackageJson): void {
  writeFileSync(path, `${JSON.stringify(packageJson, null, 2)}\n`);
}

function readProjectJson(path: string): BoundedTestPolicyProjectJson {
  const parsed = readJsonObject(path);
  return {
    ...parsed,
    name: typeof parsed.name === 'string' ? parsed.name : undefined,
    targets: readTargets(parsed.targets),
  };
}

function readJsonObject(path: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(readFileSync(path, 'utf8'));
  if (!isRecord(parsed)) {
    throw new Error(`${path} must contain a JSON object`);
  }
  return parsed;
}

function readTargets(value: unknown): Record<string, Record<string, unknown>> | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const targets: Record<string, Record<string, unknown>> = {};
  for (const [name, target] of Object.entries(value)) {
    targets[name] = isRecord(target) ? target : {};
  }
  return targets;
}

function writeProjectJson(path: string, projectJson: BoundedTestPolicyProjectJson): void {
  writeFileSync(path, `${JSON.stringify(projectJson, null, 2)}\n`);
}

function hasTestEntrypoint(
  packageJson: BoundedTestPolicyPackageJson,
  projectJson: BoundedTestPolicyProjectJson | undefined,
): boolean {
  return (
    typeof packageJson.scripts?.test === 'string' ||
    isRecord(projectJson?.targets?.test) ||
    isRecord(packageJson.nx?.targets?.test)
  );
}

function packageProjectName(
  packageJson: BoundedTestPolicyPackageJson,
  projectJson: BoundedTestPolicyProjectJson | undefined,
): string | null {
  return projectJson?.name ?? packageJson.nx?.name ?? packageJson.name ?? null;
}

function projectJsonPathForPackageJson(packageJsonPath: string): string {
  return join(dirname(packageJsonPath), 'project.json');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}
