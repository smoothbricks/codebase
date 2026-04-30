import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';
import type { Tree } from 'nx/src/devkit-exports.js';
import { getProjects, readJson, readProjectConfiguration, updateJson, writeJson } from 'nx/src/devkit-exports.js';

import type { NxPolicyIssue } from './workspace-config-policy.js';

export type TestRunner = 'bun' | 'vitest';

const workspaceDependencyFields = ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'];

/**
 * Detect test runners from a package.json object and optional project targets
 * (from project.json or nx config).
 */
export function detectPackageTestRunners(
  pkg: Record<string, unknown>,
  projectTargets?: Record<string, unknown>,
): ReadonlySet<TestRunner> {
  const runners = new Set<TestRunner>();

  // Check package.json scripts
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

  // Check package.json nx.targets
  const nxTargets = recordProperty(recordProperty(pkg, 'nx'), 'targets');
  if (nxTargets) {
    for (const target of Object.values(nxTargets)) {
      if (!isRecord(target)) {
        continue;
      }
      const options = recordProperty(target, 'options');
      const command = options ? stringProperty(options, 'command') : null;
      if (command) {
        const runner = detectTestRunnerFromCommand(command);
        if (runner) {
          runners.add(runner);
        }
      }
    }
  }

  // Check project.json / project config targets
  if (projectTargets) {
    for (const target of Object.values(projectTargets)) {
      if (!isRecord(target)) {
        continue;
      }
      const options = recordProperty(target, 'options');
      const command = options ? stringProperty(options, 'command') : null;
      if (command) {
        const runner = detectTestRunnerFromCommand(command);
        if (runner) {
          runners.add(runner);
        }
      }
    }
  }

  return runners;
}

/**
 * Validate tsconfig.test.json contents (in-memory object).
 * Returns policy issues found.
 */
export function checkTypecheckTestConfig(
  tsconfigTest: Record<string, unknown> | null,
  packagePath: string,
): NxPolicyIssue[] {
  if (!tsconfigTest) {
    return [];
  }
  const issues: NxPolicyIssue[] = [];
  const tsconfigTestPath = join(packagePath, 'tsconfig.test.json');
  const compilerOptions = recordProperty(tsconfigTest, 'compilerOptions');

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
  if (typeof compilerOptions?.tsBuildInfoFile === 'string' && compilerOptions.tsBuildInfoFile.includes('dist-test')) {
    issues.push({
      path: tsconfigTestPath,
      message: 'must not write tsbuildinfo under dist-test',
    });
  }

  return issues;
}

/**
 * Check if tsconfig.json has a bad reference to ./tsconfig.test.json.
 */
export function checkTsconfigTestReference(
  projectTsconfig: Record<string, unknown> | null,
  packagePath: string,
): NxPolicyIssue[] {
  if (!projectTsconfigHasTestReference(projectTsconfig)) {
    return [];
  }
  return [
    {
      path: join(packagePath, 'tsconfig.json'),
      message:
        'must not reference ./tsconfig.test.json. ' +
        'Test typechecking is run by the inferred typecheck-tests target with tsc --noEmit, not TypeScript build mode.',
    },
  ];
}

/**
 * Apply defaults to a tsconfig.test.json object (in-memory mutation).
 * Returns whether anything changed.
 */
export function applyTypecheckTestDefaults(
  tsconfigTest: Record<string, unknown>,
  options: {
    testRunners: ReadonlySet<TestRunner>;
    tsconfigLibExtends?: string;
    libCompilerOptions?: Record<string, unknown>;
    referencePaths: string[];
  },
): boolean {
  const extendsValue = options.tsconfigLibExtends ?? '../../tsconfig.base.json';
  let changed = setMissingStringProperty(tsconfigTest, 'extends', extendsValue);

  const compilerOptions = getOrCreateRecord(tsconfigTest, 'compilerOptions');

  // Copy relevant compiler options from lib tsconfig
  if (options.libCompilerOptions) {
    for (const key of ['baseUrl', 'module', 'moduleResolution', 'jsx', 'lib']) {
      if (Object.hasOwn(options.libCompilerOptions, key) && compilerOptions[key] !== options.libCompilerOptions[key]) {
        compilerOptions[key] = options.libCompilerOptions[key];
        changed = true;
      }
    }
  }

  changed = setBooleanProperty(compilerOptions, 'composite', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'declaration', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'declarationMap', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'emitDeclarationOnly', false) || changed;
  changed = setBooleanProperty(compilerOptions, 'noEmit', true) || changed;

  if (options.testRunners.has('bun')) {
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

  for (const referencePath of options.referencePaths) {
    changed = addTsconfigReference(tsconfigTest, referencePath) || changed;
  }

  return changed;
}

/**
 * Remove the ./tsconfig.test.json reference from a tsconfig.json object.
 * Returns whether anything changed.
 */
export function removeTsconfigTestReference(projectTsconfig: Record<string, unknown>): boolean {
  if (!projectTsconfigHasTestReference(projectTsconfig)) {
    return false;
  }
  const references = Array.isArray(projectTsconfig.references) ? projectTsconfig.references : [];
  projectTsconfig.references = references.filter((entry) => !isRecord(entry) || entry.path !== './tsconfig.test.json');
  return true;
}

/**
 * Check all workspace packages for typecheck test policy compliance using an Nx Tree.
 */
export function checkTypecheckTestPolicyTree(tree: Tree): NxPolicyIssue[] {
  const issues: NxPolicyIssue[] = [];

  for (const [projectName, config] of getProjects(tree)) {
    const pkgPath = `${config.root}/package.json`;
    if (!tree.exists(pkgPath)) continue;
    if (config.root === '.' || config.root === '') continue;

    const pkg = readJson<Record<string, unknown>>(tree, pkgPath);

    // Get targets from project config (handles both package.json and project.json)
    const projectConfig = readProjectConfiguration(tree, projectName);
    const projectTargets = projectConfig.targets ?? {};

    const testRunners = detectPackageTestRunners(pkg, projectTargets);
    if (testRunners.size === 0) continue;

    // Check tsconfig.test.json exists
    const tsconfigTestPath = `${config.root}/tsconfig.test.json`;
    if (!tree.exists(tsconfigTestPath)) {
      issues.push({
        path: tsconfigTestPath,
        message: `${formatTestRunnerList(testRunners)} requires tsconfig.test.json because those runners do not typecheck test files by default`,
      });
      continue;
    }

    // Check tsconfig.test.json contents
    const tsconfigTest = readJson<Record<string, unknown>>(tree, tsconfigTestPath);
    issues.push(...checkTypecheckTestConfig(tsconfigTest, config.root));

    // Check tsconfig.json reference
    const tsconfigPath = `${config.root}/tsconfig.json`;
    if (tree.exists(tsconfigPath)) {
      const tsconfig = readJson<Record<string, unknown>>(tree, tsconfigPath);
      issues.push(...checkTsconfigTestReference(tsconfig, config.root));
    }
  }

  return issues;
}

/**
 * Fix all workspace packages' tsconfig.test.json configuration using an Nx Tree.
 * Returns whether any files changed.
 */
export function applyTypecheckTestPolicyTree(tree: Tree): boolean {
  let changed = false;
  const workspacePackages = collectWorkspacePackagesTree(tree);

  for (const [projectName, config] of getProjects(tree)) {
    const pkgPath = `${config.root}/package.json`;
    if (!tree.exists(pkgPath)) continue;
    if (config.root === '.' || config.root === '') continue;

    const pkg = readJson<Record<string, unknown>>(tree, pkgPath);
    const projectConfig = readProjectConfiguration(tree, projectName);
    const projectTargets = projectConfig.targets ?? {};

    const testRunners = detectPackageTestRunners(pkg, projectTargets);
    if (testRunners.size === 0) continue;

    // Apply tsconfig.test.json defaults
    const tsconfigTestPath = `${config.root}/tsconfig.test.json`;
    const isNew = !tree.exists(tsconfigTestPath);
    const tsconfigTest = isNew ? {} : readJson<Record<string, unknown>>(tree, tsconfigTestPath);

    // Read lib tsconfig for extends and compiler options
    const libTsconfigPath = `${config.root}/tsconfig.lib.json`;
    const libTsconfig = tree.exists(libTsconfigPath) ? readJson<Record<string, unknown>>(tree, libTsconfigPath) : null;
    const tsconfigLibExtends =
      (libTsconfig ? stringProperty(libTsconfig, 'extends') : null) ?? '../../tsconfig.base.json';
    const libCompilerOptions = libTsconfig ? recordProperty(libTsconfig, 'compilerOptions') : null;

    // Collect reference paths
    const referencePaths = collectReferencePathsTree(tree, config.root, pkg, workspacePackages);

    if (
      applyTypecheckTestDefaults(tsconfigTest, {
        testRunners,
        tsconfigLibExtends,
        libCompilerOptions: libCompilerOptions ?? undefined,
        referencePaths,
      }) ||
      isNew
    ) {
      writeJson(tree, tsconfigTestPath, tsconfigTest);
      changed = true;
    }

    // Remove test reference from tsconfig.json
    const tsconfigPath = `${config.root}/tsconfig.json`;
    if (tree.exists(tsconfigPath)) {
      updateJson(tree, tsconfigPath, (tsconfig: Record<string, unknown>) => {
        if (removeTsconfigTestReference(tsconfig)) {
          changed = true;
        }
        return tsconfig;
      });
    }
  }

  return changed;
}

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

    const testRunners = detectPackageTestRunners(pkg);
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
      // Use absolute path for filesystem layer
      const absoluteIssues = checkTypecheckTestConfig(tsconfig, join(root, packagePath));
      issues.push(...absoluteIssues);
    }

    // tsconfig.json must NOT reference ./tsconfig.test.json
    const projectTsconfigPath = join(root, packagePath, 'tsconfig.json');
    const projectTsconfig = readJsonObject(projectTsconfigPath);
    const refIssues = checkTsconfigTestReference(projectTsconfig, join(root, packagePath));
    issues.push(...refIssues);
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

    const testRunners = detectPackageTestRunners(pkg);
    if (testRunners.size === 0) {
      continue;
    }

    // Create/update tsconfig.test.json
    const tsconfigTestPath = join(root, packagePath, 'tsconfig.test.json');
    const existing = readJsonObject(tsconfigTestPath);
    const tsconfigTest = existing ?? {};
    let fileChanged = existing === null;

    // Read lib tsconfig for extends and compiler options
    const libTsconfig = readJsonObject(join(root, packagePath, 'tsconfig.lib.json'));
    const tsconfigLibExtends = stringProperty(libTsconfig ?? {}, 'extends') ?? '../../tsconfig.base.json';
    const libCompilerOptions = libTsconfig ? recordProperty(libTsconfig, 'compilerOptions') : null;

    // Collect reference paths
    const referencePaths = collectTsconfigTestReferencePaths(root, packagePath, pkg, workspaceNames);

    fileChanged =
      applyTypecheckTestDefaults(tsconfigTest, {
        testRunners,
        tsconfigLibExtends,
        libCompilerOptions: libCompilerOptions ?? undefined,
        referencePaths,
      }) || fileChanged;

    if (fileChanged) {
      writeJsonObjectFs(tsconfigTestPath, tsconfigTest);
      changed = true;
    }

    // Remove ./tsconfig.test.json from tsconfig.json references if present
    const projectTsconfigPath = join(root, packagePath, 'tsconfig.json');
    const projectTsconfig = readJsonObject(projectTsconfigPath);
    if (projectTsconfig && removeTsconfigTestReference(projectTsconfig)) {
      writeJsonObjectFs(projectTsconfigPath, projectTsconfig);
      changed = true;
    }
  }

  return changed;
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

function projectTsconfigHasTestReference(projectTsconfig: Record<string, unknown> | null): boolean {
  return Boolean(
    projectTsconfig &&
      Array.isArray(projectTsconfig.references) &&
      projectTsconfig.references.some((entry) => isRecord(entry) && entry.path === './tsconfig.test.json'),
  );
}

interface TreeWorkspacePackage {
  name: string;
  path: string;
}

function collectWorkspacePackagesTree(tree: Tree): TreeWorkspacePackage[] {
  const packages: TreeWorkspacePackage[] = [];
  for (const [, config] of getProjects(tree)) {
    const pkgPath = `${config.root}/package.json`;
    if (!tree.exists(pkgPath)) continue;
    const pkg = readJson<{ name?: string }>(tree, pkgPath);
    if (pkg.name) {
      packages.push({ name: pkg.name, path: config.root });
    }
  }
  return packages;
}

function collectReferencePathsTree(
  tree: Tree,
  packageRoot: string,
  pkg: Record<string, unknown>,
  workspacePackages: TreeWorkspacePackage[],
): string[] {
  const paths: string[] = [];
  const libTsconfigPath = `${packageRoot}/tsconfig.lib.json`;
  if (tree.exists(libTsconfigPath)) {
    paths.push('./tsconfig.lib.json');
  }

  const packagesByName = new Map(workspacePackages.map((p) => [p.name, p]));
  for (const field of workspaceDependencyFields) {
    const deps = recordProperty(pkg, field);
    if (!deps) continue;
    for (const depName of Object.keys(deps)) {
      const depPkg = packagesByName.get(depName);
      if (!depPkg) continue;
      const depTsconfig = `${depPkg.path}/tsconfig.lib.json`;
      if (!tree.exists(depTsconfig)) continue;
      const refPath = relative(packageRoot, depTsconfig).replaceAll('\\', '/');
      if (!paths.includes(refPath)) paths.push(refPath);
    }
  }
  return paths;
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

interface WorkspacePackageInfo {
  name: string;
  path: string;
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

function writeJsonObjectFs(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stringProperty(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function recordProperty(record: Record<string, unknown> | null, key: string): Record<string, unknown> | null {
  if (!record) return null;
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
