import { readFileSync, rmSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { publint } from 'publint';
import { formatMessage } from 'publint/utils';
import { isRecord } from '../lib/json.js';
import { runResult, runStatus } from '../lib/run.js';
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
  const attwArgs = [
    packed.path,
    '--format',
    'ascii',
    '--no-color',
    '--profile',
    'node16',
    '--ignore-rules',
    'cjs-resolves-to-esm',
    ...attwExcludedEntrypointArgs(pkg),
  ];
  const attw = await runResult('attw', attwArgs, root);
  if (attw.exitCode === 0) {
    return 0;
  }
  printAttwOutput(attw.stdout);
  printAttwOutput(attw.stderr);
  console.error(`${pkg.path}: are-the-types-wrong validation failed`);
  return 1;
}

function printAttwOutput(output: string): void {
  for (const line of output.split('\n')) {
    if (!line || line.includes('(ignored)')) {
      continue;
    }
    console.error(line);
  }
}

function attwExcludedEntrypointArgs(pkg: PackageInfo): string[] {
  const excluded = wasmExportEntrypoints(pkg.json.exports);
  return excluded.length === 0 ? [] : ['--exclude-entrypoints', ...excluded];
}

function wasmExportEntrypoints(exports: unknown): string[] {
  if (!isRecord(exports)) {
    return [];
  }
  return Object.entries(exports)
    .filter(([key, value]) => key.startsWith('.') && exportPointsToWasm(value))
    .map(([key]) => key);
}

function exportPointsToWasm(value: unknown): boolean {
  if (typeof value === 'string') {
    return value.endsWith('.wasm');
  }
  return isRecord(value) && Object.values(value).some(exportPointsToWasm);
}

async function packPackage(root: string, pkg: PackageInfo): Promise<{ path: string; arrayBuffer: ArrayBuffer }> {
  const packageDir = join(root, pkg.path);
  const tarballName = `.smoo-${process.pid}-${Date.now()}.tgz`;
  const tarballPath = join(root, tarballName);
  try {
    const status = await runStatus(
      'bun',
      ['pm', 'pack', '--filename', tarballName, '--ignore-scripts', '--quiet'],
      packageDir,
      true,
    );
    if (status !== 0) {
      throw new Error(`bun pm pack failed with exit code ${status}`);
    }
    const bytes = new Uint8Array(readFileSync(tarballPath));
    return { path: tarballPath, arrayBuffer: bytes.slice().buffer };
  } catch (error) {
    rmSync(tarballPath, { force: true });
    throw error;
  }
}
