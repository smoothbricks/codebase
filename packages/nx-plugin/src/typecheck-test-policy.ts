import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';

import type { NxPolicyIssue } from './workspace-config-policy.js';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

type TestRunner = 'bun' | 'vitest';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const workspaceDependencyFields = ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'];

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Check all workspace packages for typecheck test policy compliance.
 *
 * For each package that uses bun test or vitest:
 * - tsconfig.test.json must exist
 * - tsconfig.test.json must be noEmit, not composite, no declaration, no dist-test output
 * - tsconfig.json must NOT reference ./tsconfig.test.json
 */
export function checkTypecheckTestPolicy(root: string): NxPolicyIssue[] {
  const issues: NxPolicyIssue[] = [];
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const pkg = readJsonObject(packageJsonPath);
    if (!pkg) {
      continue;
    }
    const packageDir = packageJsonPath.slice(0, -'/package.json'.length);
    const packagePath = relative(root, packageDir);
    if (packagePath === '' || packagePath === '.') {
      continue;
    }

    const testRunners = collectTestRunners(pkg);
    if (testRunners.size === 0) {
      continue;
    }

    // tsconfig.test.json must exist
    const tsconfigTestPath = join(root, packagePath, 'tsconfig.test.json');
    if (!existsSync(tsconfigTestPath)) {
      issues.push({
        path: tsconfigTestPath,
        message: `${formatTestRunnerList(testRunners)} requires tsconfig.test.json because those runners do not typecheck test files by default`,
      });
      continue;
    }

    // tsconfig.test.json must have correct settings
    const tsconfig = readJsonObject(tsconfigTestPath);
    if (tsconfig) {
      const compilerOptions = recordProperty(tsconfig, 'compilerOptions');
      if (!compilerOptions || compilerOptions.noEmit !== true) {
        issues.push({
          path: tsconfigTestPath,
          message: 'compilerOptions.noEmit must be true',
        });
      }
      if (compilerOptions?.composite === true) {
        issues.push({
          path: tsconfigTestPath,
          message:
            'must not set compilerOptions.composite = true. ' +
            'Bun test typechecking is a no-emit validation pass, not a TypeScript build-mode project.',
        });
      }
      if (compilerOptions?.declaration === true) {
        issues.push({
          path: tsconfigTestPath,
          message: 'must not set compilerOptions.declaration = true',
        });
      }
      if (compilerOptions?.declarationMap === true) {
        issues.push({
          path: tsconfigTestPath,
          message: 'must not set compilerOptions.declarationMap = true',
        });
      }
      if (compilerOptions?.outDir === 'dist-test') {
        issues.push({
          path: tsconfigTestPath,
          message: 'must not emit to dist-test',
        });
      }
      if (
        typeof compilerOptions?.tsBuildInfoFile === 'string' &&
        compilerOptions.tsBuildInfoFile.includes('dist-test')
      ) {
        issues.push({
          path: tsconfigTestPath,
          message: 'must not write tsbuildinfo under dist-test',
        });
      }
    }

    // tsconfig.json must NOT reference ./tsconfig.test.json
    const projectTsconfigPath = join(root, packagePath, 'tsconfig.json');
    const projectTsconfig = readJsonObject(projectTsconfigPath);
    if (projectTsconfigHasTestReference(projectTsconfig)) {
      issues.push({
        path: projectTsconfigPath,
        message:
          'must not reference ./tsconfig.test.json. ' +
          'Test typechecking is run by the inferred typecheck-tests target with tsc --noEmit, not TypeScript build mode.',
      });
    }
  }
  return issues;
}

/**
 * Fix all workspace packages' tsconfig.test.json configuration.
 * Returns whether any files changed.
 */
export function applyTypecheckTestPolicy(root: string): boolean {
  const workspaceNames = getWorkspacePackageNames(root);
  let changed = false;

  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const pkg = readJsonObject(packageJsonPath);
    if (!pkg) {
      continue;
    }
    const packageDir = packageJsonPath.slice(0, -'/package.json'.length);
    const packagePath = relative(root, packageDir);
    if (packagePath === '' || packagePath === '.') {
      continue;
    }

    const testRunners = collectTestRunners(pkg);
    if (testRunners.size === 0) {
      continue;
    }

    // Create/update tsconfig.test.json
    const tsconfigTestPath = join(root, packagePath, 'tsconfig.test.json');
    const existing = readJsonObject(tsconfigTestPath);
    const tsconfigTest = existing ?? {};
    let fileChanged = existing === null;

    fileChanged =
      applyTsconfigTestDefaults(root, packagePath, pkg, tsconfigTest, workspaceNames, testRunners) || fileChanged;
    if (fileChanged) {
      writeJsonObject(tsconfigTestPath, tsconfigTest);
      changed = true;
    }

    // Remove ./tsconfig.test.json from tsconfig.json references if present
    const projectTsconfigPath = join(root, packagePath, 'tsconfig.json');
    const projectTsconfig = readJsonObject(projectTsconfigPath);
    if (projectTsconfig && projectTsconfigHasTestReference(projectTsconfig)) {
      const references = Array.isArray(projectTsconfig.references) ? projectTsconfig.references : [];
      projectTsconfig.references = references.filter(
        (entry) => !isRecord(entry) || entry.path !== './tsconfig.test.json',
      );
      writeJsonObject(projectTsconfigPath, projectTsconfig);
      changed = true;
    }
  }

  return changed;
}

// ---------------------------------------------------------------------------
// Test runner detection
// ---------------------------------------------------------------------------

function collectTestRunners(pkg: Record<string, unknown>): ReadonlySet<TestRunner> {
  const runners = new Set<TestRunner>();
  const scripts = recordProperty(pkg, 'scripts');
  if (scripts) {
    for (const command of Object.values(scripts)) {
      if (typeof command !== 'string') {
        continue;
      }
      const runner = detectTestRunnerFromCommand(command);
      if (runner) {
        runners.add(runner);
      }
    }
  }
  const nx = recordProperty(pkg, 'nx');
  const targets = nx ? recordProperty(nx, 'targets') : null;
  if (!targets) {
    return runners;
  }
  for (const target of Object.values(targets)) {
    if (!isRecord(target)) {
      continue;
    }
    const options = recordProperty(target, 'options');
    const command = options ? stringProperty(options, 'command') : null;
    const runner = command ? detectTestRunnerFromCommand(command) : null;
    if (runner) {
      runners.add(runner);
    }
  }
  return runners;
}

function detectTestRunnerFromCommand(command: string): TestRunner | null {
  const trimmed = parseEnvPrefixedCommand(command).command.trim();
  if (/^bun\s+test(?:\s|$)/.test(trimmed)) {
    return 'bun';
  }
  if (/^vitest(?:\s|$)/.test(trimmed)) {
    return 'vitest';
  }
  return null;
}

function formatTestRunnerList(testRunners: ReadonlySet<TestRunner>): string {
  const labels: string[] = [];
  if (testRunners.has('bun')) {
    labels.push('bun test');
  }
  if (testRunners.has('vitest')) {
    labels.push('vitest');
  }
  return labels.join(' or ');
}

// ---------------------------------------------------------------------------
// Env prefix command parsing
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// tsconfig.test.json defaults
// ---------------------------------------------------------------------------

function applyTsconfigTestDefaults(
  root: string,
  packagePath: string,
  pkg: Record<string, unknown>,
  tsconfigTest: Record<string, unknown>,
  workspaceNames: ReadonlySet<string>,
  testRunners: ReadonlySet<TestRunner>,
): boolean {
  let changed = setMissingStringProperty(tsconfigTest, 'extends', defaultTsconfigTestExtends(root, packagePath));
  const compilerOptions = getOrCreateRecord(tsconfigTest, 'compilerOptions');
  changed = copyLibCompilerOptions(root, packagePath, compilerOptions) || changed;
  changed = setBooleanProperty(compilerOptions, 'composite', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'declaration', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'declarationMap', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'emitDeclarationOnly', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'noEmit', true) || changed;
  if (testRunners.has('bun')) {
    changed = mergeStringListProperty(compilerOptions, 'types', ['bun']) || changed;
  }
  if ('outDir' in compilerOptions) {
    delete compilerOptions.outDir;
    changed = true;
  }
  if ('tsBuildInfoFile' in compilerOptions) {
    delete compilerOptions.tsBuildInfoFile;
    changed = true;
  }
  changed =
    mergeStringListProperty(tsconfigTest, 'include', [
      'src/**/*.test.ts',
      'src/**/*.spec.ts',
      'src/**/__tests__/**/*.ts',
      'src/**/__tests__/**/*.tsx',
      'src/test-suite-tracer.ts',
    ]) || changed;
  for (const referencePath of collectTsconfigTestReferencePaths(root, packagePath, pkg, workspaceNames)) {
    changed = addTsconfigReference(tsconfigTest, referencePath) || changed;
  }
  return changed;
}

function defaultTsconfigTestExtends(root: string, packagePath: string): string {
  const libTsconfig = readJsonObject(join(root, packagePath, 'tsconfig.lib.json'));
  return stringProperty(libTsconfig ?? {}, 'extends') ?? '../../tsconfig.base.json';
}

function copyLibCompilerOptions(root: string, packagePath: string, target: Record<string, unknown>): boolean {
  const libTsconfig = readJsonObject(join(root, packagePath, 'tsconfig.lib.json'));
  const libCompilerOptions = libTsconfig ? recordProperty(libTsconfig, 'compilerOptions') : null;
  if (!libCompilerOptions) {
    return false;
  }
  let changed = false;
  for (const key of ['baseUrl', 'module', 'moduleResolution', 'jsx', 'lib']) {
    if (Object.hasOwn(libCompilerOptions, key) && target[key] !== libCompilerOptions[key]) {
      target[key] = libCompilerOptions[key];
      changed = true;
    }
  }
  return changed;
}

function collectTsconfigTestReferencePaths(
  root: string,
  packagePath: string,
  pkg: Record<string, unknown>,
  workspaceNames: ReadonlySet<string>,
): string[] {
  const paths = existsSync(join(root, packagePath, 'tsconfig.lib.json')) ? ['./tsconfig.lib.json'] : [];
  const packagesByName = new Map(getWorkspacePackages(root).map((workspacePkg) => [workspacePkg.name, workspacePkg]));
  for (const field of workspaceDependencyFields) {
    const dependencies = recordProperty(pkg, field);
    if (!dependencies) {
      continue;
    }
    for (const dependencyName of Object.keys(dependencies)) {
      if (!workspaceNames.has(dependencyName)) {
        continue;
      }
      const dependencyPackage = packagesByName.get(dependencyName);
      if (!dependencyPackage) {
        continue;
      }
      const dependencyTsconfig = join(root, dependencyPackage.path, 'tsconfig.lib.json');
      if (!existsSync(dependencyTsconfig)) {
        continue;
      }
      const refPath = relative(join(root, packagePath), dependencyTsconfig).replaceAll('\\', '/');
      if (!paths.includes(refPath)) {
        paths.push(refPath);
      }
    }
  }
  return paths;
}

// ---------------------------------------------------------------------------
// tsconfig.json reference check
// ---------------------------------------------------------------------------

function projectTsconfigHasTestReference(projectTsconfig: Record<string, unknown> | null): boolean {
  return Boolean(
    projectTsconfig &&
      Array.isArray(projectTsconfig.references) &&
      projectTsconfig.references.some((entry) => isRecord(entry) && entry.path === './tsconfig.test.json'),
  );
}

// ---------------------------------------------------------------------------
// Workspace walking
// ---------------------------------------------------------------------------

interface WorkspacePackageInfo {
  name: string;
  path: string;
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
    if (pkg) {
      const name = stringProperty(pkg, 'name');
      if (name) {
        names.add(name);
      }
    }
  }
  return names;
}

function getWorkspacePackages(root: string): WorkspacePackageInfo[] {
  const packages: WorkspacePackageInfo[] = [];
  for (const packageJsonPath of listWorkspacePackageJsonPaths(root)) {
    const pkg = readJsonObject(packageJsonPath);
    if (!pkg) {
      continue;
    }
    const name = stringProperty(pkg, 'name');
    if (!name) {
      continue;
    }
    const packageDir = packageJsonPath.slice(0, -'/package.json'.length);
    packages.push({ name, path: relative(root, packageDir) });
  }
  return packages;
}

// ---------------------------------------------------------------------------
// JSON helpers (self-contained, following bounded-test-policy.ts pattern)
// ---------------------------------------------------------------------------

function readJsonObject(path: string): Record<string, unknown> | null {
  if (!existsSync(path)) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(readFileSync(path, 'utf8'));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function writeJsonObject(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

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

function setMissingStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (typeof record[key] === 'string') {
    return false;
  }
  record[key] = value;
  return true;
}

function setBooleanProperty(record: Record<string, unknown>, key: string, value: boolean): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

function mergeStringListProperty(record: Record<string, unknown>, key: string, values: string[]): boolean {
  const rawCurrent = record[key];
  const current = Array.isArray(rawCurrent)
    ? rawCurrent.filter((entry): entry is string => typeof entry === 'string')
    : [];
  const next = [...current];
  for (const value of values) {
    if (!next.includes(value)) {
      next.push(value);
    }
  }
  if (
    Array.isArray(rawCurrent) &&
    next.length === rawCurrent.length &&
    next.every((entry, index) => entry === rawCurrent[index])
  ) {
    return false;
  }
  record[key] = next;
  return true;
}

function addTsconfigReference(tsconfig: Record<string, unknown>, path: string): boolean {
  const current = Array.isArray(tsconfig.references)
    ? tsconfig.references.filter((entry): entry is Record<string, unknown> => isRecord(entry))
    : [];
  if (current.some((entry) => entry.path === path)) {
    return false;
  }
  tsconfig.references = [...current, { path }];
  return true;
}
