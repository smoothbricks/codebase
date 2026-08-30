import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { CARGO_TEST_TARGET } from './cargo-workspace.js';
import type { PackageTargetPolicyOptions, ResolvedProjectTargets } from './package-target-policy.js';

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
  resolvedProject?: ResolvedProjectTargets;
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

export function applyWorkspaceBoundedTestTargetPolicy(root: string, options: PackageTargetPolicyOptions = {}): boolean {
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
    const resolvedProject = resolvedProjectFor(options, projectName);
    if (checkBoundedTestTargetPolicy(packageJson, { projectName, projectJson, resolvedProject })) {
      continue;
    }
    if (!boundableTestTarget(packageJson, projectJson)) {
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

export function checkWorkspaceBoundedTestTargetPolicy(
  root: string,
  options: PackageTargetPolicyOptions = {},
): BoundedTestPolicyIssue[] {
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
    const resolvedProject = resolvedProjectFor(options, projectName);
    if (!checkBoundedTestTargetPolicy(packageJson, { projectName, projectJson, resolvedProject })) {
      issues.push({
        path: projectJson ? projectJsonPath : packageJsonPath,
        message:
          `${projectJson ? 'targets' : 'nx.targets'}.test must use ${BOUNDED_TEST_EXECUTOR} ` +
          'or delegate through no-op targets to bounded test execution',
      });
    }
  }
  return issues;
}

/**
 * A cargo workspace's `test` aggregate MUST reach `cargo-test`, and `cargo-test`
 * must actually RUN tests.
 *
 * Inference wires `test` to `cargo-test` (see `index.ts`), but two things unwire
 * it silently and both leave the target green:
 *
 * 1. `targetDefaults.test.dependsOn` in `nx.json` REPLACES an inferred
 *    `dependsOn` rather than merging, and a package-local `nx.targets.test`
 *    replaces it outright. `nx test <project>` then executes nothing.
 * 2. `dependsOn` REPLACES rather than unions at every merge layer, so a
 *    package-local `nx.targets['cargo-test'].dependsOn` that omits the inferred
 *    runner leg leaves a target that still "reaches" its dependencies while no
 *    test binary is ever run. Reachability alone cannot see this, because
 *    `cargo-test-compile` is `--no-run`. (Inference itself no longer skips a
 *    declared cargo key, so the executor and options survive the declaration —
 *    see `index.ts`.)
 *
 * So this checks both: `test` reaches `cargo-test`, AND some target in
 * `cargo-test`'s closure actually executes tests rather than only compiling them.
 */
export function checkWorkspaceCargoTestReachabilityPolicy(
  root: string,
  options: PackageTargetPolicyOptions = {},
): BoundedTestPolicyIssue[] {
  const issues: BoundedTestPolicyIssue[] = [];
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const packageJson = readPackageJson(packageJsonPath);
    const projectJsonPath = projectJsonPathForPackageJson(packageJsonPath);
    const projectJson = existsSync(projectJsonPath) ? readProjectJson(projectJsonPath) : undefined;
    const projectName = packageProjectName(packageJson, projectJson);
    if (!projectName) {
      continue;
    }
    const resolvedProject = resolvedProjectFor(options, projectName);
    // Without a resolved graph there is nothing to judge, and without a
    // cargo-test target this is not a cargo workspace.
    if (!resolvedProject?.targets?.has(CARGO_TEST_TARGET) || !resolvedProject.targets.has('test')) {
      continue;
    }
    const scope = projectJson ? 'targets' : 'nx.targets';
    const path = projectJson ? projectJsonPath : packageJsonPath;
    if (!resolvedTargetReaches(resolvedProject, 'test', CARGO_TEST_TARGET)) {
      issues.push({
        path,
        message:
          `${scope}.test must depend on ${CARGO_TEST_TARGET} ` +
          'so the Rust tests run in the test aggregate; nx.json targetDefaults replaces the inferred dependsOn',
      });
    } else if (!resolvedTargetRunsTests(resolvedProject, CARGO_TEST_TARGET)) {
      issues.push({
        path,
        message:
          `${scope}.${CARGO_TEST_TARGET} reaches no target that RUNS tests — only ones that compile them. ` +
          'A declared dependsOn replaces the inferred one; spread it with "..." instead of omitting the runner leg',
      });
    }
  }
  return issues;
}

/**
 * True when `target`, or something in its dependency closure, invokes cargo in a
 * mode that executes tests. `cargo-test-compile` is deliberately excluded: it is
 * `cargo test --no-run`, so it proves the binaries build and nothing about them
 * running.
 */
function resolvedTargetRunsTests(project: ResolvedProjectTargets, target: string): boolean {
  const visiting = new Set<string>();
  const visit = (targetName: string): boolean => {
    if (visiting.has(targetName) || !project.targets.has(targetName)) {
      return false;
    }
    visiting.add(targetName);
    const command = commandOf(project, targetName);
    const runs = command !== undefined && /\btest\b|\bnextest\b/.test(command) && !command.includes('--no-run');
    const reached =
      runs ||
      (project.targetDependencies?.get(targetName) ?? []).some((dependency) =>
        matchingResolvedTargets(dependency, project.targets).some(visit),
      );
    visiting.delete(targetName);
    return reached;
  };
  return visit(target);
}

function commandOf(project: ResolvedProjectTargets, targetName: string): string | undefined {
  const options = project.targetOptions?.get(targetName);
  if (options === undefined || options === null) {
    return undefined;
  }
  const command = (options as Record<string, unknown>).command;
  return typeof command === 'string' ? command : undefined;
}

function resolvedTargetReaches(project: ResolvedProjectTargets, from: string, goal: string): boolean {
  const visiting = new Set<string>();
  const visit = (targetName: string): boolean => {
    if (targetName === goal) {
      return true;
    }
    if (visiting.has(targetName) || !project.targets.has(targetName)) {
      return false;
    }
    visiting.add(targetName);
    const reached = (project.targetDependencies?.get(targetName) ?? []).some((dependency) =>
      matchingResolvedTargets(dependency, project.targets).some(visit),
    );
    visiting.delete(targetName);
    return reached;
  };
  return visit(from);
}

export function checkBoundedTestTargetPolicy(
  packageJson: BoundedTestPolicyPackageJson,
  options: ApplyBoundedTestTargetPolicyOptions,
): boolean {
  const testTarget = options.projectJson ? options.projectJson.targets?.test : packageJson.nx?.targets?.test;
  if (!isRecord(testTarget)) {
    return false;
  }
  // `<root>/src` is equally bounded: bun-test targets run from src/ so bun's
  // test-discovery scan never walks generated trees (a Rust package's cargo
  // target/ directory alone costs tens of seconds per run).
  if (
    isBoundedExecutionTarget(testTarget.executor, testTarget.options, new Set(['{projectRoot}', '{projectRoot}/src']))
  ) {
    return packageJson.scripts?.test === boundedTestScriptAlias(options.projectName);
  }
  if (!isNoopAggregateTarget(testTarget)) {
    return false;
  }
  const testScript = packageJson.scripts?.test;
  if (testScript !== undefined && testScript !== boundedTestScriptAlias(options.projectName)) {
    return false;
  }
  return resolvedAggregateTestIsBounded(options.resolvedProject);
}

/**
 * Apply may only rewrite a test target it can express faithfully as one
 * bounded `bun test` command. A `commands` array (a multi-runner gate) would
 * be flattened to the default command — deleting the very checks the target
 * exists to run — and a non-bun runner (cargo, go) would inherit wall-clock
 * bounds scaled for bun suites. Those targets are left in place for the check
 * to report, so restructuring them stays a human decision.
 */
function boundableTestTarget(
  packageJson: BoundedTestPolicyPackageJson,
  projectJson: BoundedTestPolicyProjectJson | undefined,
): boolean {
  const target = projectJson?.targets?.test ?? packageJson.nx?.targets?.test;
  if (!isRecord(target)) {
    return true;
  }
  if (isNoopAggregateTarget(target)) {
    return true;
  }
  const targetOptions = isRecord(target.options) ? target.options : undefined;
  if (targetOptions && Array.isArray(targetOptions.commands)) {
    return false;
  }
  const command = resolveTargetCommand(target);
  return command === null || BUN_TEST_PREFIX.test(command);
}

function isNoopAggregateTarget(target: Record<string, unknown>): boolean {
  if (target.executor !== undefined && target.executor !== 'nx:noop') {
    return false;
  }
  if (typeof target.command === 'string' || (isRecord(target.options) && typeof target.options.command === 'string')) {
    return false;
  }
  return Array.isArray(target.dependsOn) && target.dependsOn.length > 0;
}

function isBoundedExecutionTarget(executor: unknown, rawOptions: unknown, allowedCwds: ReadonlySet<string>): boolean {
  if (executor !== BOUNDED_TEST_EXECUTOR || !isRecord(rawOptions)) {
    return false;
  }
  const command = rawOptions.command;
  return (
    typeof command === 'string' &&
    command.length > 0 &&
    isPackageTestScriptRunnerCommand(command) === false &&
    command === ensureBunTestTimeoutFlag(command) &&
    typeof rawOptions.cwd === 'string' &&
    allowedCwds.has(rawOptions.cwd) &&
    rawOptions.timeoutMs === BOUNDED_TEST_TIMEOUT_MS &&
    rawOptions.killAfterMs === BOUNDED_TEST_KILL_AFTER_MS
  );
}

function resolvedAggregateTestIsBounded(project: ResolvedProjectTargets | undefined): boolean {
  if (!project?.targetDependencies || !project.targetExecutors || !project.targetOptions) {
    return false;
  }
  const visiting = new Set<string>();
  const verified = new Map<string, boolean>();
  const allowedCwds = new Set(
    ['{projectRoot}', '{projectRoot}/src'].concat(project.root ? [project.root, `${project.root}/src`] : []),
  );

  const visit = (targetName: string): boolean => {
    const cached = verified.get(targetName);
    if (cached !== undefined) {
      return cached;
    }
    if (visiting.has(targetName) || !project.targets.has(targetName)) {
      return false;
    }
    visiting.add(targetName);
    const executor = project.targetExecutors?.get(targetName);
    let valid: boolean;
    if (executor !== undefined && executor !== 'nx:noop') {
      valid = isBoundedExecutionTarget(executor, project.targetOptions?.get(targetName), allowedCwds);
    } else {
      const dependencies = project.targetDependencies?.get(targetName) ?? [];
      valid =
        dependencies.length > 0 &&
        dependencies.every((dependency) => {
          const matches = matchingResolvedTargets(dependency, project.targets);
          return matches.length > 0 && matches.every(visit);
        });
    }
    visiting.delete(targetName);
    verified.set(targetName, valid);
    return valid;
  };

  return visit('test');
}

function matchingResolvedTargets(dependency: string, targets: ReadonlySet<string>): string[] {
  if (dependency.startsWith('^')) {
    return [];
  }
  if (!dependency.startsWith('*')) {
    return targets.has(dependency) ? [dependency] : [];
  }
  const suffix = dependency.slice(1);
  return [...targets].filter((targetName) => targetName.endsWith(suffix));
}

function resolvedProjectFor(
  options: PackageTargetPolicyOptions,
  projectName: string,
): ResolvedProjectTargets | undefined {
  const project = options.resolvedTargetsByProject?.get(projectName);
  return project && 'targets' in project ? project : undefined;
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
