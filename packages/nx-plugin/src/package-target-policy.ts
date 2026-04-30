import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import { getProjects, readJson, type Tree, updateJson } from 'nx/src/devkit-exports.js';

import { boundedTestScriptAlias } from './bounded-test-policy.js';
import type { NxPolicyIssue } from './workspace-config-policy.js';

export type { NxPolicyIssue };

export interface ResolvedProjectTargets {
  targets: ReadonlySet<string>;
  buildDependsOn?: readonly string[];
}

export interface PackageTargetPolicyOptions {
  resolvedTargetsByProject?: ReadonlyMap<string, ReadonlySet<string> | ResolvedProjectTargets>;
}

export const BUILD_OUTPUT_DEPENDENCIES = [
  '*-js',
  '*-web',
  '*-html',
  '*-css',
  '*-ios',
  '*-android',
  '*-native',
  '*-napi',
  '*-bun',
  '*-wasm',
];

const workspaceDependencyFields = ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stringProperty(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function recordProperty(record: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = record[key];
  return isRecord(value) ? value : null;
}

function getOrCreateRecord(record: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = record[key];
  if (isRecord(value)) {
    return value;
  }
  const next: Record<string, unknown> = {};
  record[key] = next;
  return next;
}

function setStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

function setStringArrayProperty(record: Record<string, unknown>, key: string, value: string[]): boolean {
  const current = record[key];
  if (stringArrayEquals(current, value)) {
    return false;
  }
  record[key] = value;
  return true;
}

function stringArrayEquals(value: unknown, expected: readonly string[]): boolean {
  return (
    Array.isArray(value) && value.length === expected.length && value.every((entry, index) => entry === expected[index])
  );
}

function readJsonObject(path: string): Record<string, unknown> | null {
  if (!existsSync(path)) {
    return null;
  }
  const parsed: unknown = JSON.parse(readFileSync(path, 'utf8'));
  return isRecord(parsed) ? parsed : null;
}

function writeJsonObject(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function listWorkspacePackageJsonPaths(root: string): string[] {
  const rootPackagePath = join(root, 'package.json');
  if (!existsSync(rootPackagePath)) {
    return [];
  }
  const rootPackage = readJsonObject(rootPackagePath);
  if (!rootPackage) {
    return [];
  }
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

function getWorkspacePackageNames(root: string): Set<string> {
  const names = new Set<string>();
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const pkg = readJsonObject(packageJsonPath);
    if (!pkg) {
      continue;
    }
    const name = stringProperty(pkg, 'name');
    if (name) {
      names.add(name);
    }
  }
  return names;
}

function packagePathFromJsonPath(root: string, packageJsonPath: string): string {
  const absolute = packageJsonPath.replace(/[/\\]package\.json$/, '');
  const rootNormalized = root.replace(/[/\\]$/, '');
  if (absolute.startsWith(rootNormalized)) {
    const relative = absolute.slice(rootNormalized.length + 1);
    return relative || '.';
  }
  return absolute;
}

export function packageNxProjectName(pkg: Record<string, unknown>): string | null {
  const nx = recordProperty(pkg, 'nx');
  return (nx ? stringProperty(nx, 'name') : null) ?? stringProperty(pkg, 'name');
}

interface PackageNxConfig {
  nx: Record<string, unknown>;
  targets: Record<string, unknown> | null;
  changed: boolean;
}

interface PackageNxTargetsConfig {
  nx: Record<string, unknown>;
  targets: Record<string, unknown>;
  changed: boolean;
}

function applyPackageNxConfig(
  pkg: Record<string, unknown>,
  options: { projectName: string; targets: true },
): PackageNxTargetsConfig;
function applyPackageNxConfig(pkg: Record<string, unknown>, options: { projectName: string }): PackageNxConfig;
function applyPackageNxConfig(
  pkg: Record<string, unknown>,
  options: { projectName: string; targets?: boolean },
): PackageNxConfig {
  const nx = getOrCreateRecord(pkg, 'nx');
  const changed = setStringProperty(nx, 'name', options.projectName);
  const targets = options.targets === true ? getOrCreateRecord(nx, 'targets') : null;
  return { nx, targets, changed };
}

function resolvedProjectTargetNames(
  resolvedProject?: ReadonlySet<string> | ResolvedProjectTargets,
): ReadonlySet<string> | undefined {
  if (!resolvedProject) {
    return undefined;
  }
  return isResolvedProjectTargets(resolvedProject) ? resolvedProject.targets : resolvedProject;
}

function resolvedProjectBuildDependsOn(
  resolvedProject?: ReadonlySet<string> | ResolvedProjectTargets,
): readonly string[] | undefined {
  if (!isResolvedProjectTargets(resolvedProject)) {
    return undefined;
  }
  return resolvedProject.buildDependsOn;
}

function isResolvedProjectTargets(
  value: ReadonlySet<string> | ResolvedProjectTargets | undefined,
): value is ResolvedProjectTargets {
  return isRecord(value) && value.targets instanceof Set;
}

function targetExistsInResolvedProject(targetName: string, resolvedTargets?: ReadonlySet<string>): boolean {
  return resolvedTargets?.has(targetName) === true;
}

function isBuildOutputDependencyPattern(dependency: string): boolean {
  return BUILD_OUTPUT_DEPENDENCIES.includes(dependency);
}

function expectedTargetDependencies(targetName: string): string[] {
  return targetName === 'preview' ? ['build'] : ['^build'];
}

function applyTargetDependencyPolicy(target: Record<string, unknown>, targetName: string): boolean {
  return setStringArrayProperty(target, 'dependsOn', expectedTargetDependencies(targetName));
}

function targetDependsOn(target: Record<string, unknown>, expected: string[]): boolean {
  const dependsOn = target.dependsOn;
  return Array.isArray(dependsOn) && expected.every((entry) => dependsOn.includes(entry));
}

function isNoopTarget(target: Record<string, unknown>): boolean {
  const executor = stringProperty(target, 'executor');
  if (executor !== null && executor !== 'nx:noop') {
    return false;
  }
  const options = recordProperty(target, 'options');
  return !options || stringProperty(options, 'command') === null;
}

function targetDependenciesMatchResolvedBuild(
  target: Record<string, unknown>,
  resolvedBuildDependsOn: readonly string[],
): boolean {
  if (!Array.isArray(target.dependsOn)) {
    return false;
  }
  const expected = new Set(resolvedBuildDependsOn);
  if (target.dependsOn.length !== expected.size) {
    return false;
  }
  return target.dependsOn.every((dependency) => {
    if (typeof dependency !== 'string') {
      return false;
    }
    return expected.has(dependency);
  });
}

function removeRedundantNoopBuildTarget(
  pkg: Record<string, unknown>,
  resolvedProject?: ReadonlySet<string> | ResolvedProjectTargets,
): boolean {
  const resolvedTargets = resolvedProjectTargetNames(resolvedProject);
  const resolvedBuildDependsOn = resolvedProjectBuildDependsOn(resolvedProject);
  if (!resolvedTargets?.has('build') || !resolvedBuildDependsOn) {
    return false;
  }
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  const build = targets ? recordProperty(targets, 'build') : null;
  if (
    !targets ||
    !build ||
    !isNoopTarget(build) ||
    !targetDependenciesMatchResolvedBuild(build, resolvedBuildDependsOn)
  ) {
    return false;
  }
  delete targets.build;
  return true;
}

function isNxRunAlias(command: string): boolean {
  return /^nx\s+run\s+\S+:\S+/.test(command.trim());
}

function parseNxRunAlias(command: string): { projectName: string; targetName: string } | null {
  const match = /^nx\s+run\s+([^:\s]+):([^\s]+)(?:\s|$)/.exec(command.trim());
  if (!match?.[1] || !match[2]) {
    return null;
  }
  return { projectName: match[1], targetName: match[2] };
}

export function nxRunAlias(projectName: string, targetName: string, continuous: boolean): string {
  if (targetName === 'test') {
    return boundedTestScriptAlias(projectName);
  }
  const flags = continuous || targetName === 'test' ? ' --tui=false --outputStyle=stream' : '';
  return `nx run ${projectName}:${targetName}${flags}`;
}

function isContinuousTarget(targetName: string, command: string): boolean {
  return (
    /(?:^|:|-)(?:dev|serve|preview|watch)(?:$|:|-)/.test(targetName) ||
    /(?:^|\s)(?:dev|serve|preview|--watch|-w)(?:\s|$)/.test(command)
  );
}

function isSafeNxScriptCommand(command: string): boolean {
  const trimmed = command.trim();
  return (
    /^bun\s+test(?:\s|$)/.test(trimmed) ||
    /^tsc-bun-test(?:\s|$)/.test(trimmed) ||
    /^vitest(?:\s|$)/.test(trimmed) ||
    /^tsc\s+(?:--build|--noEmit)(?:\s|$)/.test(trimmed) ||
    /^tsdown(?:\s|$)/.test(trimmed) ||
    /^vite\s+(?:build|dev|preview)(?:\s|$)/.test(trimmed) ||
    /^astro\s+(?:build|dev|preview|check)(?:\s|$)/.test(trimmed) ||
    /^zig\s+build(?:\s|$)/.test(trimmed) ||
    /^bun\s+[./\w-]*build[\w.-]*\.ts(?:\s|$)/.test(trimmed) ||
    /(?:^|\s)(?:bench|benchmark)(?:\s|$)/.test(trimmed) ||
    /^wrangler\s+build(?:\s|$)/.test(trimmed)
  );
}

function isBlockedScriptCommand(scriptName: string, command: string): boolean {
  if (/^(?:deploy|db|release|sync|subtree|publish|pack)(?::|$)/.test(scriptName)) {
    return true;
  }
  const trimmed = parseEnvPrefixedCommand(command).command;
  return /^(?:deploy|db|release|sync|subtree|publish|pack)(?:\s|$)/.test(trimmed) || trimmed === 'astro';
}

function isScriptRunnerCommand(command: string | null, scriptName: string): boolean {
  if (!command) {
    return false;
  }
  const escaped = scriptName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`^(?:bun|npm)\\s+run\\s+${escaped}(?:\\s|$)`).test(command.trim());
}

function parseEnvPrefixedCommand(command: string): { command: string; env: Record<string, string> } {
  const env: Record<string, string> = {};
  let rest = command.trimStart();
  while (true) {
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=/.exec(rest);
    if (!match?.[1]) {
      return { command: rest.trim(), env };
    }
    let index = match[0].length;
    let value = '';
    const quote = rest[index];
    if (quote === '"' || quote === "'") {
      index += 1;
      const end = rest.indexOf(quote, index);
      if (end === -1) {
        return { command: command.trim(), env: {} };
      }
      value = rest.slice(index, end);
      index = end + 1;
    } else {
      const end = rest.slice(index).search(/\s/);
      const valueEnd = end === -1 ? rest.length : index + end;
      value = rest.slice(index, valueEnd);
      index = valueEnd;
    }
    if (index < rest.length && !/\s/.test(rest[index] ?? '')) {
      return { command: command.trim(), env: {} };
    }
    env[match[1]] = value;
    rest = rest.slice(index).trimStart();
  }
}

function targetNameForCommand(command: string): string | null {
  const trimmed = command.trim();
  if (/^tsc\s+--build\s+tsconfig\.lib\.json(?:\s|$)/.test(trimmed)) {
    return 'tsc-js';
  }
  if (/^tsdown(?:\s|$)/.test(trimmed)) {
    return 'tsdown-js';
  }
  const zigStep = /^zig\s+build\s+([A-Za-z0-9_-]+)(?:\s|$)/.exec(trimmed)?.[1];
  if (zigStep) {
    return `zig-${zigStep}`;
  }
  if (/^wrangler\s+build(?:\s|$)/.test(trimmed)) {
    return 'build';
  }
  return null;
}

function rewriteTargetName(scriptName: string, command: string): string | null {
  return targetNameForCommand(command) ?? (scriptName.includes(':') ? null : scriptName);
}

function replacementTargetName(
  targetName: string,
  command: string | null,
  resolvedTargets?: ReadonlySet<string>,
): string | null {
  if (command) {
    const commandTargetName = targetNameForCommand(command);
    if (commandTargetName) {
      return commandTargetName;
    }
  }
  const suffix = targetName.slice(targetName.lastIndexOf(':') + 1);
  if (suffix && targetExistsInResolvedProject(suffix, resolvedTargets)) {
    return suffix;
  }
  const dashed = targetName.replaceAll(':', '-');
  if (targetExistsInResolvedProject(dashed, resolvedTargets)) {
    return dashed;
  }
  return null;
}

interface ScriptRewrite {
  targetName: string;
  continuous: boolean;
  command: string;
  env: Record<string, string>;
}

function classifyScriptRewrite(scriptName: string, command: string): ScriptRewrite | null {
  if (isNxRunAlias(command) || isBlockedScriptCommand(scriptName, command)) {
    return null;
  }
  const parsed = parseEnvPrefixedCommand(command);
  const targetName = rewriteTargetName(scriptName, parsed.command);
  if (!targetName || !isSafeNxScriptCommand(parsed.command)) {
    return null;
  }
  return {
    targetName,
    continuous: isContinuousTarget(targetName, parsed.command),
    command: parsed.command,
    env: parsed.env,
  };
}

function scriptTargetAliasesForProject(
  pkg: Record<string, unknown>,
  projectName: string | null,
): ReadonlyMap<string, string> {
  if (!projectName) {
    return new Map();
  }
  const scripts = recordProperty(pkg, 'scripts');
  if (!scripts) {
    return new Map();
  }
  const aliases = new Map<string, string>();
  for (const [scriptName, rawCommand] of Object.entries(scripts)) {
    if (typeof rawCommand !== 'string' || !scriptName.includes(':')) {
      continue;
    }
    const alias = parseNxRunAlias(rawCommand);
    if (alias?.projectName === projectName && !alias.targetName.includes(':')) {
      aliases.set(scriptName, alias.targetName);
    }
  }
  return aliases;
}

function hasWorkspaceDependency(pkg: Record<string, unknown>, workspaceNames: ReadonlySet<string>): boolean {
  for (const field of workspaceDependencyFields) {
    const dependencies = recordProperty(pkg, field);
    if (!dependencies) {
      continue;
    }
    for (const name of Object.keys(dependencies)) {
      if (workspaceNames.has(name)) {
        return true;
      }
    }
  }
  return false;
}

function targetCommand(target: Record<string, unknown>): string | null {
  const options = recordProperty(target, 'options');
  return options ? stringProperty(options, 'command') : null;
}

function rewriteTargetDependencies(
  target: Record<string, unknown>,
  renamedTargets: ReadonlyMap<string, string>,
): boolean {
  if (!Array.isArray(target.dependsOn)) {
    return false;
  }
  let changed = false;
  target.dependsOn = target.dependsOn.map((dependency) => {
    if (typeof dependency !== 'string') {
      return dependency;
    }
    const next = renamedTargets.get(dependency);
    if (!next) {
      return dependency;
    }
    changed = true;
    return next;
  });
  return changed;
}

function hasTestEntrypoint(pkg: Record<string, unknown>): boolean {
  const scripts = recordProperty(pkg, 'scripts');
  if (typeof scripts?.test === 'string') {
    return true;
  }
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  return Boolean(targets && isRecord(targets.test));
}

function packageHasTestFiles(root: string, packagePath: string): boolean {
  return directoryContainsTestFiles(join(root, packagePath));
}

function directoryContainsTestFiles(path: string): boolean {
  if (!existsSync(path)) {
    return false;
  }
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name === 'coverage' || entry.name === '.git') {
      continue;
    }
    const entryPath = join(path, entry.name);
    if (entry.isDirectory()) {
      if (directoryContainsTestFiles(entryPath)) {
        return true;
      }
      continue;
    }
    const normalizedPath = entryPath.replaceAll('\\', '/');
    if (normalizedPath.includes('/__tests__/')) {
      return true;
    }
    if (/\.(?:test|spec)\.[cm]?[jt]sx?$/.test(entry.name)) {
      return true;
    }
  }
  return false;
}

function validateBuildZigPolicy(root: string, packagePath: string): NxPolicyIssue[] {
  const path = join(root, packagePath, 'build.zig');
  if (!existsSync(path)) {
    return [];
  }
  if (/\bb\.step\s*\(/.test(readFileSync(path, 'utf8'))) {
    return [];
  }
  return [
    { path: join(root, packagePath), message: `${packagePath}/build.zig must define at least one b.step(...) target` },
  ];
}

function validateExplicitNxTargets(
  pkg: Record<string, unknown>,
  packagePath: string,
  resolvedTargets?: ReadonlySet<string>,
): NxPolicyIssue[] {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return [];
  }
  const issues: NxPolicyIssue[] = [];
  for (const [targetName, rawTarget] of Object.entries(targets)) {
    if (targetName.includes(':')) {
      issues.push({
        path: packagePath,
        message:
          `package.json nx.targets.${targetName} must not use colon target names. ` +
          'Nx CLI syntax already uses project:target:configuration; use a concrete tool-output target name and keep colon names only as package-script aliases.',
      });
    }
    if (!isRecord(rawTarget)) {
      continue;
    }
    issues.push(...validateTargetDependencies(rawTarget, `${packagePath}: nx.targets.${targetName}`, resolvedTargets));
  }
  return issues;
}

function validateTargetDependencies(
  target: Record<string, unknown>,
  label: string,
  resolvedTargets?: ReadonlySet<string>,
): NxPolicyIssue[] {
  if (!Array.isArray(target.dependsOn)) {
    return [];
  }
  const issues: NxPolicyIssue[] = [];
  for (const dependency of target.dependsOn) {
    if (typeof dependency !== 'string') {
      continue;
    }
    if (dependency.includes(':')) {
      issues.push({
        path: label.split(':')[0]?.trim() ?? label,
        message: `${label}.dependsOn must not include colon target dependency ${dependency}`,
      });
      continue;
    }
    if (
      label.endsWith('nx.targets.build') &&
      !dependency.startsWith('^') &&
      !isBuildOutputDependencyPattern(dependency) &&
      !targetExistsInResolvedProject(dependency, resolvedTargets)
    ) {
      issues.push({
        path: label.split(':')[0]?.trim() ?? label,
        message: `${label}.dependsOn references missing target ${dependency}`,
      });
    }
  }
  return issues;
}

function validateTestEntrypointPresence(
  root: string,
  packagePath: string,
  pkg: Record<string, unknown>,
): NxPolicyIssue[] {
  if (packagePath === '.') {
    return [];
  }
  if (!packageHasTestFiles(root, packagePath) || hasTestEntrypoint(pkg)) {
    return [];
  }
  return [{ path: packagePath, message: `${packagePath}: test files require scripts.test or nx.targets.test` }];
}

function validatePackageScriptPolicy(
  pkg: Record<string, unknown>,
  packagePath: string,
  workspaceNames: ReadonlySet<string>,
  options: { resolvedTargets?: ReadonlySet<string> } = {},
): NxPolicyIssue[] {
  if (!hasWorkspaceDependency(pkg, workspaceNames)) {
    return [];
  }
  const scripts = recordProperty(pkg, 'scripts');
  if (!scripts) {
    return [];
  }
  const nx = recordProperty(pkg, 'nx');
  const projectName = nx ? stringProperty(nx, 'name') : stringProperty(pkg, 'name');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  const issues: NxPolicyIssue[] = [];
  for (const [scriptName, rawCommand] of Object.entries(scripts)) {
    if (typeof rawCommand !== 'string') {
      continue;
    }
    const alias = parseNxRunAlias(rawCommand);
    const rewrite = alias
      ? { targetName: alias.targetName, continuous: isContinuousTarget(alias.targetName, '') }
      : classifyScriptRewrite(scriptName, rawCommand);
    if (!rewrite || (alias && projectName && alias.projectName !== projectName)) {
      if (alias && projectName && alias.projectName !== projectName) {
        issues.push({
          path: packagePath,
          message: `${packagePath}: scripts.${scriptName} must delegate to project ${projectName}`,
        });
      }
      continue;
    }
    if (!projectName) {
      issues.push({
        path: packagePath,
        message: `${packagePath}: package scripts that use workspace dependencies require package.json nx.name`,
      });
      continue;
    }
    const expectedAlias = nxRunAlias(projectName, rewrite.targetName, rewrite.continuous);
    if (rawCommand !== expectedAlias) {
      issues.push({
        path: packagePath,
        message: `${packagePath}: scripts.${scriptName} must delegate to ${expectedAlias}`,
      });
      continue;
    }
    const target = targets ? recordProperty(targets, rewrite.targetName) : null;
    if (
      !target &&
      rewrite.targetName !== scriptName &&
      targetExistsInResolvedProject(rewrite.targetName, options.resolvedTargets)
    ) {
      continue;
    }
    if (rewrite.targetName.includes(':')) {
      continue;
    }
    if (rewrite.targetName === 'test') {
      continue;
    }
    const targetOptions = target ? recordProperty(target, 'options') : null;
    const command = targetOptions ? stringProperty(targetOptions, 'command') : null;
    if (!target || stringProperty(target, 'executor') !== 'nx:run-commands' || !targetOptions || !command) {
      issues.push({
        path: packagePath,
        message: `${packagePath}: nx.targets.${rewrite.targetName} must use nx:run-commands with options.command`,
      });
      continue;
    }
    if (stringProperty(targetOptions, 'cwd') !== '{projectRoot}') {
      issues.push({
        path: packagePath,
        message: `${packagePath}: nx.targets.${rewrite.targetName}.options.cwd must be {projectRoot}`,
      });
    }
    const deps = expectedTargetDependencies(rewrite.targetName);
    if (!targetDependsOn(target, deps)) {
      issues.push({
        path: packagePath,
        message: `${packagePath}: nx.targets.${rewrite.targetName}.dependsOn must include ${deps.join(', ')}`,
      });
    }
    if (rewrite.continuous && target.continuous !== true) {
      issues.push({
        path: packagePath,
        message: `${packagePath}: nx.targets.${rewrite.targetName}.continuous must be true`,
      });
    }
    if (isScriptRunnerCommand(command, scriptName)) {
      issues.push({
        path: packagePath,
        message: `${packagePath}: nx.targets.${rewrite.targetName}.options.command must not call scripts.${scriptName}`,
      });
    }
  }
  return issues;
}

function migratePackageColonTargets(pkg: Record<string, unknown>, resolvedTargets?: ReadonlySet<string>): boolean {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  const projectName = packageNxProjectName(pkg);
  const scripts = recordProperty(pkg, 'scripts');
  let changed = false;
  const renamedTargets = new Map<string, string>();

  for (const [targetName, rawTarget] of Object.entries(targets)) {
    if (!targetName.includes(':') || !isRecord(rawTarget)) {
      continue;
    }
    const nextTargetName = replacementTargetName(targetName, targetCommand(rawTarget), resolvedTargets);
    if (!nextTargetName || nextTargetName.includes(':')) {
      continue;
    }
    if (!targetExistsInResolvedProject(nextTargetName, resolvedTargets)) {
      targets[nextTargetName] = rawTarget;
    }
    delete targets[targetName];
    renamedTargets.set(targetName, nextTargetName);
    changed = true;
  }

  if (renamedTargets.size === 0) {
    return changed;
  }

  for (const target of Object.values(targets)) {
    if (isRecord(target)) {
      changed = rewriteTargetDependencies(target, renamedTargets) || changed;
    }
  }

  if (scripts && projectName) {
    for (const [scriptName, rawCommand] of Object.entries(scripts)) {
      if (typeof rawCommand !== 'string') {
        continue;
      }
      const alias = parseNxRunAlias(rawCommand);
      if (!alias || alias.projectName !== projectName) {
        continue;
      }
      const nextTargetName = renamedTargets.get(alias.targetName);
      if (!nextTargetName) {
        continue;
      }
      scripts[scriptName] = nxRunAlias(projectName, nextTargetName, isContinuousTarget(nextTargetName, ''));
      changed = true;
    }
  }

  return changed;
}

function collectWorkspaceNamesTree(tree: Tree): Set<string> {
  const names = new Set<string>();
  for (const [, config] of getProjects(tree)) {
    const pkgPath = `${config.root}/package.json`;
    if (!tree.exists(pkgPath)) {
      continue;
    }
    const pkg = readJson<Record<string, unknown>>(tree, pkgPath);
    const name = stringProperty(pkg, 'name');
    if (name) {
      names.add(name);
    }
  }
  return names;
}

function directoryContainsTestFilesTree(tree: Tree, path: string): boolean {
  if (!tree.exists(path)) {
    return false;
  }
  for (const entry of tree.children(path)) {
    if (entry === 'node_modules' || entry === 'dist' || entry === 'coverage' || entry === '.git') {
      continue;
    }
    const entryPath = `${path}/${entry}`;
    if (tree.isFile(entryPath)) {
      const normalizedPath = entryPath.replaceAll('\\', '/');
      if (normalizedPath.includes('/__tests__/')) {
        return true;
      }
      if (/\.(?:test|spec)\.[cm]?[jt]sx?$/.test(entry)) {
        return true;
      }
    } else {
      if (directoryContainsTestFilesTree(tree, entryPath)) {
        return true;
      }
    }
  }
  return false;
}

function validateBuildZigPolicyTree(tree: Tree, packagePath: string): NxPolicyIssue[] {
  const path = `${packagePath}/build.zig`;
  if (!tree.exists(path)) {
    return [];
  }
  const content = tree.read(path, 'utf-8');
  if (content && /\bb\.step\s*\(/.test(content)) {
    return [];
  }
  return [{ path: packagePath, message: `${packagePath}/build.zig must define at least one b.step(...) target` }];
}

function validateTestEntrypointPresenceTree(
  tree: Tree,
  packagePath: string,
  pkg: Record<string, unknown>,
): NxPolicyIssue[] {
  if (packagePath === '.') {
    return [];
  }
  if (!directoryContainsTestFilesTree(tree, packagePath) || hasTestEntrypoint(pkg)) {
    return [];
  }
  return [{ path: packagePath, message: `${packagePath}: test files require scripts.test or nx.targets.test` }];
}

/**
 * Check package target policy using an Nx Tree.
 * Uses `getProjects()` for project discovery instead of manual filesystem walking.
 */
export function checkPackageTargetPolicyTree(tree: Tree, options: PackageTargetPolicyOptions = {}): NxPolicyIssue[] {
  const issues: NxPolicyIssue[] = [];
  const workspaceNames = collectWorkspaceNamesTree(tree);

  for (const [projectName, config] of getProjects(tree)) {
    const pkgPath = `${config.root}/package.json`;
    if (!tree.exists(pkgPath)) {
      continue;
    }
    const pkg = readJson<Record<string, unknown>>(tree, pkgPath);
    const packagePath = config.root;
    const resolvedProject = options.resolvedTargetsByProject?.get(projectName);
    const resolvedTargets = resolvedProjectTargetNames(resolvedProject);

    issues.push(...validateExplicitNxTargets(pkg, packagePath, resolvedTargets));
    issues.push(...validateTestEntrypointPresenceTree(tree, packagePath, pkg));
    issues.push(...validateBuildZigPolicyTree(tree, packagePath));
    issues.push(...validatePackageScriptPolicy(pkg, packagePath, workspaceNames, { resolvedTargets }));
  }

  return issues;
}

/**
 * Apply package target policy using an Nx Tree.
 * Uses `getProjects()` for project discovery and `updateJson()` for writes.
 * Returns whether anything changed.
 */
export function applyPackageTargetPolicyTree(tree: Tree, options: PackageTargetPolicyOptions = {}): boolean {
  let changed = false;
  const workspaceNames = collectWorkspaceNamesTree(tree);

  for (const [projectName, config] of getProjects(tree)) {
    const pkgPath = `${config.root}/package.json`;
    if (!tree.exists(pkgPath)) {
      continue;
    }
    const packagePath = config.root;
    const resolvedProject = options.resolvedTargetsByProject?.get(projectName);
    const resolvedTargets = resolvedProjectTargetNames(resolvedProject);

    updateJson(tree, pkgPath, (pkg: Record<string, unknown>) => {
      let packageChanged = migratePackageColonTargets(pkg, resolvedTargets);
      packageChanged = rewriteColonTargetDependenciesInPackage(pkg, resolvedTargets) || packageChanged;
      packageChanged = removePackageColonTargets(pkg) || packageChanged;
      packageChanged = removeRedundantNoopBuildTarget(pkg, resolvedProject) || packageChanged;
      packageChanged =
        applyPackageScriptPolicyForPkg(pkg, packagePath, workspaceNames, { resolvedTargets }) || packageChanged;
      if (packageChanged) {
        changed = true;
      }
      return pkg;
    });
  }

  return changed;
}

function removePackageColonTargets(pkg: Record<string, unknown>): boolean {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  let changed = false;
  for (const targetName of Object.keys(targets)) {
    if (targetName.includes(':')) {
      delete targets[targetName];
      changed = true;
    }
  }
  return changed;
}

function rewriteColonTargetDependenciesInPackage(
  pkg: Record<string, unknown>,
  resolvedTargets?: ReadonlySet<string>,
): boolean {
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return false;
  }
  const projectName = packageNxProjectName(pkg);
  const scriptAliases = scriptTargetAliasesForProject(pkg, projectName);
  let changed = false;
  for (const target of Object.values(targets)) {
    if (!isRecord(target) || !Array.isArray(target.dependsOn)) {
      continue;
    }
    target.dependsOn = target.dependsOn.map((dependency) => {
      if (typeof dependency !== 'string' || !dependency.includes(':')) {
        return dependency;
      }
      const next = scriptAliases.get(dependency) ?? replacementTargetName(dependency, null, resolvedTargets);
      if (!next) {
        return dependency;
      }
      changed = true;
      return next;
    });
  }
  return changed;
}

function applyPackageScriptPolicyForPkg(
  pkg: Record<string, unknown>,
  _packagePath: string,
  workspaceNames: ReadonlySet<string>,
  options: { resolvedTargets?: ReadonlySet<string> } = {},
): boolean {
  if (!hasWorkspaceDependency(pkg, workspaceNames)) {
    return false;
  }
  const scripts = recordProperty(pkg, 'scripts');
  if (!scripts) {
    return false;
  }
  const projectName = packageNxProjectName(pkg);
  if (!projectName) {
    return false;
  }
  const nxConfig = applyPackageNxConfig(pkg, { projectName, targets: true });
  const targets = nxConfig.targets;
  let changed = nxConfig.changed;
  for (const [scriptName, rawCommand] of Object.entries(scripts)) {
    if (typeof rawCommand !== 'string') {
      continue;
    }
    const parsedAlias = parseNxRunAlias(rawCommand);
    if (parsedAlias?.projectName === projectName && parsedAlias.targetName === 'test') {
      continue;
    }
    const rewrite = classifyScriptRewrite(scriptName, rawCommand);
    if (!rewrite) {
      continue;
    }
    const targetName = rewrite.targetName;
    const alias = nxRunAlias(projectName, targetName, rewrite.continuous);
    if (targetName === 'test') {
      continue;
    }
    const existingTarget = recordProperty(targets, targetName);
    if (
      !existingTarget &&
      targetName !== scriptName &&
      targetExistsInResolvedProject(targetName, options.resolvedTargets)
    ) {
      if (scripts[scriptName] !== alias) {
        scripts[scriptName] = alias;
        changed = true;
      }
      continue;
    }
    const existingOptions = existingTarget ? recordProperty(existingTarget, 'options') : null;
    const existingCommand = existingOptions ? stringProperty(existingOptions, 'command') : null;
    const command = isScriptRunnerCommand(existingCommand, scriptName)
      ? rewrite.command
      : (existingCommand ?? rewrite.command);
    const target = existingTarget ?? {};
    changed = setStringProperty(target, 'executor', 'nx:run-commands') || changed;
    changed = applyTargetDependencyPolicy(target, targetName) || changed;
    if (rewrite.continuous && target.continuous !== true) {
      target.continuous = true;
      changed = true;
    }
    const targetOptions = getOrCreateRecord(target, 'options');
    changed = setStringProperty(targetOptions, 'command', command) || changed;
    changed = setStringProperty(targetOptions, 'cwd', '{projectRoot}') || changed;
    for (const [name, value] of Object.entries(rewrite.env)) {
      const env = getOrCreateRecord(targetOptions, 'env');
      changed = setStringProperty(env, name, value) || changed;
    }
    if (targets[targetName] !== target) {
      targets[targetName] = target;
      changed = true;
    }
    if (scripts[scriptName] !== alias) {
      scripts[scriptName] = alias;
      changed = true;
    }
  }
  return changed;
}

export function checkPackageTargets(
  pkg: Record<string, unknown>,
  packagePath: string,
  resolvedTargets?: ReadonlySet<string>,
): NxPolicyIssue[] {
  return [...validateExplicitNxTargets(pkg, packagePath, resolvedTargets)];
}

export function applyPackageTargets(
  pkg: Record<string, unknown>,
  packagePath: string,
  workspaceNames: ReadonlySet<string>,
  options: { resolvedTargets?: ReadonlySet<string> } = {},
): boolean {
  let changed = migratePackageColonTargets(pkg, options.resolvedTargets);
  changed = rewriteColonTargetDependenciesInPackage(pkg, options.resolvedTargets) || changed;
  changed = removePackageColonTargets(pkg) || changed;
  changed = applyPackageScriptPolicyForPkg(pkg, packagePath, workspaceNames, options) || changed;
  return changed;
}

export function checkPackageTargetPolicy(root: string, options: PackageTargetPolicyOptions = {}): NxPolicyIssue[] {
  const issues: NxPolicyIssue[] = [];
  const workspaceNames = getWorkspacePackageNames(root);

  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const pkg = readJsonObject(packageJsonPath);
    if (!pkg) {
      continue;
    }
    const packagePath = packagePathFromJsonPath(root, packageJsonPath);
    const projectName = packageNxProjectName(pkg);
    const resolvedProject = projectName ? options.resolvedTargetsByProject?.get(projectName) : undefined;
    const resolvedTargets = resolvedProjectTargetNames(resolvedProject);

    issues.push(...validateExplicitNxTargets(pkg, packagePath, resolvedTargets));
    issues.push(...validateTestEntrypointPresence(root, packagePath, pkg));
    issues.push(...validateBuildZigPolicy(root, packagePath));
    issues.push(...validatePackageScriptPolicy(pkg, packagePath, workspaceNames, { resolvedTargets }));
  }

  return issues;
}

export function applyPackageTargetPolicy(root: string, options: PackageTargetPolicyOptions = {}): boolean {
  let changed = false;
  const workspaceNames = getWorkspacePackageNames(root);

  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const pkg = readJsonObject(packageJsonPath);
    if (!pkg) {
      continue;
    }
    const packagePath = packagePathFromJsonPath(root, packageJsonPath);
    const projectName = packageNxProjectName(pkg);
    const resolvedProject = projectName ? options.resolvedTargetsByProject?.get(projectName) : undefined;
    const resolvedTargets = resolvedProjectTargetNames(resolvedProject);

    let packageChanged = migratePackageColonTargets(pkg, resolvedTargets);
    packageChanged = rewriteColonTargetDependenciesInPackage(pkg, resolvedTargets) || packageChanged;
    packageChanged = removePackageColonTargets(pkg) || packageChanged;
    packageChanged = removeRedundantNoopBuildTarget(pkg, resolvedProject) || packageChanged;
    packageChanged =
      applyPackageScriptPolicyForPkg(pkg, packagePath, workspaceNames, { resolvedTargets }) || packageChanged;

    if (packageChanged) {
      writeJsonObject(packageJsonPath, pkg);
      changed = true;
    }
  }

  return changed;
}
