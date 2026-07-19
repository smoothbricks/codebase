import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { publint } from 'publint';
import { formatMessage } from 'publint/utils';
import typia from 'typia';
import type { PackageExports, PackageJson } from '../lib/json.js';
import { parsePackageJsonText } from '../lib/json.js';
import { printCommandOutput, runResult } from '../lib/run.js';
import type { PackageInfo } from '../lib/workspace.js';
import { listPublicPackages } from '../lib/workspace.js';
import { readPackedPackageJson, validatePackedWorkspaceDependencies } from './packed-manifest.js';

export async function validatePackedPublicPackages(root: string): Promise<number> {
  return (
    (await validatePackedPublicPackagePublint(root)) +
    (await validatePackedPublicPackageManifest(root)) +
    (await validatePackedPublicPackageTypes(root))
  );
}

export async function validatePackedPublicPackagePublint(root: string): Promise<number> {
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    failures += await validatePackedPublicPackageTool(root, pkg, validatePublint);
  }
  return failures;
}

export async function validatePackedPublicPackageManifest(root: string): Promise<number> {
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    failures += await validatePackedPublicPackageTool(root, pkg, validatePackedManifest);
  }
  return failures;
}

export async function validatePackedPublicPackageTypes(root: string): Promise<number> {
  let failures = 0;
  for (const pkg of listPublicPackages(root)) {
    failures += await validatePackedPublicPackageTool(root, pkg, validateAttw);
  }
  return failures;
}

async function validatePackedPublicPackageTool(
  root: string,
  pkg: PackageInfo,
  validate: (root: string, pkg: PackageInfo, packed: { path: string; arrayBuffer: ArrayBuffer }) => Promise<number>,
): Promise<number> {
  const packed = await packPackage(root, pkg);
  try {
    return await validate(root, pkg, packed);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`${pkg.path}: packed package validation failed: ${message}`);
    return 1;
  } finally {
    unlinkSync(packed.path);
  }
}

async function validatePublint(
  _root: string,
  pkg: PackageInfo,
  packed: { path: string; arrayBuffer: ArrayBuffer },
): Promise<number> {
  let failures = 0;
  const lint = await publint({ pack: { tarball: packed.arrayBuffer }, level: 'warning', strict: true });
  for (const message of lint.messages) {
    console.error(`${pkg.path}: publint ${message.type} ${message.code}: ${formatMessage(message, lint.pkg)}`);
    failures++;
  }
  return failures;
}

async function validatePackedManifest(
  root: string,
  pkg: PackageInfo,
  packed: { path: string; arrayBuffer: ArrayBuffer },
): Promise<number> {
  let failures = 0;
  const packedPackage = await readPackedPackageJson(root, packed.path, pkg.name);
  for (const message of validatePackedWorkspaceDependencies(root, pkg, packedPackage)) {
    console.error(message);
    failures++;
  }
  return failures;
}

async function validateAttw(root: string, pkg: PackageInfo, packed: { path: string }): Promise<number> {
  const attw = await loadAttwCore();
  const analysis = await attw.checkPackage(await createAttwPackageFromTarball(attw, root, packed.path), {
    excludeEntrypoints: nonJsExportEntrypoints(pkg.json.exports),
  });
  if (!isAttwAnalysis(analysis) || analysis.types === false) {
    return 0;
  }
  const problems = (analysis.problems ?? []).filter(isReportedAttwProblem);
  if (problems.length === 0) {
    return 0;
  }
  for (const problem of problems) {
    console.error(`${pkg.path}: are-the-types-wrong ${problem.kind}${formatProblemLocation(problem)}`);
  }
  console.error(`${pkg.path}: are-the-types-wrong validation failed`);
  return 1;
}

interface AttwCore {
  createPackage: (files: Record<string, Uint8Array>, packageName: string, packageVersion: string) => unknown;
  checkPackage: (pkg: unknown, options: { excludeEntrypoints: string[] }) => Promise<unknown>;
}

interface AttwAnalysis {
  types?: unknown;
  problems?: unknown[];
}

interface AttwProblem {
  kind: string;
  resolutionKind?: string;
  entrypoint?: string;
}

interface AttwCoreExport {
  Package?: unknown;
  checkPackage?: unknown;
}

const isAttwAnalysis = typia.createIs<AttwAnalysis>();
const isAttwProblem = typia.createIs<AttwProblem>();
const isAttwCoreExport = typia.createIs<AttwCoreExport>();

async function loadAttwCore(): Promise<AttwCore> {
  const packageJson = fileURLToPath(import.meta.resolve('@arethetypeswrong/core/package.json'));
  const core: unknown = await import(pathToFileURL(join(dirname(packageJson), 'dist', 'index.js')).href);
  if (!isAttwCoreExport(core)) {
    throw new Error('@arethetypeswrong/core does not expose the expected API');
  }
  const PackageConstructor = core.Package;
  const checkPackage = core.checkPackage;
  if (typeof PackageConstructor !== 'function' || typeof checkPackage !== 'function') {
    throw new Error('@arethetypeswrong/core does not expose the expected API');
  }
  return {
    createPackage: (files, packageName, packageVersion) =>
      Reflect.construct(PackageConstructor, [files, packageName, packageVersion]),
    checkPackage: (pkg, options) => Promise.resolve(checkPackage(pkg, options)),
  };
}

async function createAttwPackageFromTarball(attw: AttwCore, root: string, tarballPath: string): Promise<unknown> {
  const temp = mkdtempSync(join(tmpdir(), 'smoo-attw-'));
  try {
    const extract = await runResult('tar', ['-xzf', tarballPath, '-C', temp], root);
    if (extract.exitCode !== 0) {
      printCommandOutput(extract.stdout, extract.stderr);
      throw new Error('unable to extract packed package for are-the-types-wrong');
    }
    return createAttwPackageFromDirectory(attw, join(temp, 'package'));
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}

function createAttwPackageFromDirectory(attw: AttwCore, packageDir: string): unknown {
  const packageJson = readJsonFile(join(packageDir, 'package.json'));
  const packageName = packageJson.name;
  const packageVersion = packageJson.version;
  if (!packageName || !packageVersion) {
    throw new Error('packed package.json must contain name and version');
  }
  const files: Record<string, Uint8Array> = {};
  collectAttwFiles(packageDir, packageDir, packageName, files);
  return attw.createPackage(files, packageName, packageVersion);
}

function collectAttwFiles(root: string, current: string, packageName: string, files: Record<string, Uint8Array>): void {
  for (const entry of readdirSync(current)) {
    const path = join(current, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      collectAttwFiles(root, path, packageName, files);
      continue;
    }
    if (!stat.isFile()) {
      continue;
    }
    const packageRelativePath = relative(root, path).split('\\').join('/');
    files[`/node_modules/${packageName}/${packageRelativePath}`] = new Uint8Array(readFileSync(path));
  }
}

function readJsonFile(path: string): PackageJson {
  const parsed = parsePackageJsonText(readFileSync(path, 'utf8'));
  if (!parsed) {
    throw new Error(`${path} is not a JSON object`);
  }
  return parsed;
}

function isReportedAttwProblem(problem: unknown): problem is AttwProblem {
  if (!isAttwProblem(problem)) {
    return false;
  }
  const flag = attwProblemFlag(problem.kind);
  if (flag === 'cjs-resolves-to-esm') {
    return false;
  }
  return problem.resolutionKind !== 'node10';
}

function attwProblemFlag(kind: string): string | null {
  switch (kind) {
    case 'NoResolution':
      return 'no-resolution';
    case 'UntypedResolution':
      return 'untyped-resolution';
    case 'FalseCJS':
      return 'false-cjs';
    case 'FalseESM':
      return 'false-esm';
    case 'CJSResolvesToESM':
      return 'cjs-resolves-to-esm';
    case 'FallbackCondition':
      return 'fallback-condition';
    case 'CJSOnlyExportsDefault':
      return 'cjs-only-exports-default';
    case 'NamedExports':
      return 'named-exports';
    case 'FalseExportDefault':
      return 'false-export-default';
    case 'MissingExportEquals':
      return 'missing-export-equals';
    case 'UnexpectedModuleSyntax':
      return 'unexpected-module-syntax';
    case 'InternalResolutionError':
      return 'internal-resolution-error';
    default:
      return null;
  }
}

function formatProblemLocation(problem: AttwProblem): string {
  const entrypoint = typeof problem.entrypoint === 'string' ? ` ${problem.entrypoint}` : '';
  const resolution =
    typeof problem.resolutionKind === 'string' ? ` ${formatResolutionKind(problem.resolutionKind)}` : '';
  return `${entrypoint}${resolution}`;
}

function formatResolutionKind(kind: string): string {
  if (kind === 'node16-cjs') {
    return 'node16 (from CJS)';
  }
  if (kind === 'node16-esm') {
    return 'node16 (from ESM)';
  }
  return kind;
}

function nonJsExportEntrypoints(exports: PackageExports): string[] {
  if (exports === null || exports === undefined || typeof exports === 'string') {
    return [];
  }
  return Object.entries(exports)
    .filter(([key, value]) => key.startsWith('.') && exportPointsToNonJs(value))
    .map(([key]) => key);
}

function exportPointsToNonJs(value: PackageExports): boolean {
  if (typeof value === 'string') {
    // attw resolves every entrypoint as a module; assets can never have types.
    // Existence of the target files is still validated by publint.
    return value.endsWith('.wasm') || value.endsWith('.css');
  }
  if (value === null || value === undefined) {
    return false;
  }
  return Object.values(value).some(exportPointsToNonJs);
}

async function packPackage(root: string, pkg: PackageInfo): Promise<{ path: string; arrayBuffer: ArrayBuffer }> {
  const packageDir = join(root, pkg.path);
  const tarballName = `.smoo-${process.pid}-${Date.now()}.tgz`;
  const tarballPath = join(root, tarballName);
  try {
    const result = await runResult(
      'bun',
      ['pm', 'pack', '--filename', tarballName, '--ignore-scripts', '--quiet'],
      packageDir,
    );
    if (result.exitCode !== 0) {
      printCommandOutput(result.stdout, result.stderr);
      throw new Error(`bun pm pack failed with exit code ${result.exitCode}`);
    }
    const bytes = new Uint8Array(readFileSync(tarballPath));
    return { path: tarballPath, arrayBuffer: bytes.slice().buffer };
  } catch (error) {
    rmSync(tarballPath, { force: true });
    throw error;
  }
}
