#!/usr/bin/env node
/**
 * Writes a native build's input record (build-inputs.ts) into its output directory, after the build:
 *
 *   smoo-nx-build-inputs --manifest-path <Cargo.toml> --dep-info <triple-or-profile-relative .d file> --output <directory>
 *
 * `--manifest-path` is the one the build was given, so `cargo metadata` names the same target directory.
 *
 * `--dep-info` is relative to Cargo's target directory (`aarch64-apple-darwin/debug/libcowshed_napi.d`), the file
 * rustc wrote listing every source it read. The record lists those workspace files, the `Cargo.toml` of each crate
 * they belong to, and the workspace `Cargo.lock`, which decides every dependency's bytes. It is part of the target's
 * Nx outputs, so a cache hit restores the record that describes the restored binary.
 */
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { isAbsolute, join, resolve } from 'node:path';
import { dirname as dirnamePosix, join as joinPosix } from 'node:path/posix';
import {
  buildInputRecord,
  depInfoFiles,
  gitWorkspaceRoot,
  pathspecFiles,
  workspacePath,
  writeBuildInputs,
} from '../build-inputs.js';

const USAGE =
  'usage: smoo-nx-build-inputs --manifest-path <Cargo.toml> --dep-info <path under the Cargo target directory> --output <directory>';

function fail(message: string): never {
  process.stderr.write(`smoo-nx-build-inputs: ${message}\n${USAGE}\n`);
  process.exit(2);
}

function flag(name: string): string {
  const at = process.argv.indexOf(name);
  const value = at < 0 ? undefined : process.argv[at + 1];
  if (value === undefined || value.startsWith('-')) fail(`missing ${name}`);
  return value;
}

function run(command: string, args: readonly string[]): string {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: cargo metadata --offline --no-deps reads the workspace's manifests and resolves nothing, so it takes no package-cache lock and waits on no other process; the enclosing build's runner bound or the operator's interrupt ends a hung one
  const result = spawnSync(command, args, { encoding: 'utf8' });
  if (result.error !== undefined || result.status !== 0) {
    fail(`${command} ${args.join(' ')} in ${process.cwd()}: ${result.error?.message ?? result.stderr.trim()}`);
  }
  return result.stdout.trim();
}

const cwd = process.cwd();
const manifestPath = flag('--manifest-path');
const depInfoPath = flag('--dep-info');
const output = resolve(cwd, flag('--output'));
const workspaceRoot = gitWorkspaceRoot(cwd) ?? fail(`${cwd} is not in a Git work tree, so no input can be named`);
const metadata: unknown = JSON.parse(
  run('cargo', ['metadata', '--no-deps', '--format-version', '1', '--offline', '--manifest-path', manifestPath]),
);
const targetDirectory =
  typeof metadata === 'object' && metadata !== null ? Reflect.get(metadata, 'target_directory') : undefined;
if (typeof targetDirectory !== 'string') fail('cargo metadata names no target_directory');
const cargoRoot =
  typeof metadata === 'object' && metadata !== null ? Reflect.get(metadata, 'workspace_root') : undefined;
if (typeof cargoRoot !== 'string') fail('cargo metadata names no workspace_root');
const depInfo = isAbsolute(depInfoPath) ? depInfoPath : join(targetDirectory, depInfoPath);
if (!existsSync(depInfo)) fail(`${depInfo} does not exist: the build did not write the dep-info this target names`);

const sources = depInfoFiles(workspaceRoot, readFileSync(depInfo, 'utf8'));
if (sources.length === 0) fail(`${depInfo} names no file of ${workspaceRoot}`);
const tracked = new Set(pathspecFiles(workspaceRoot, ['.']));
const manifests = new Set<string>();
for (const source of sources) {
  for (let directory = dirnamePosix(source); directory !== '.'; directory = dirnamePosix(directory)) {
    const manifest = joinPosix(directory, 'Cargo.toml');
    if (tracked.has(manifest)) {
      manifests.add(manifest);
      break;
    }
  }
}
for (const name of ['Cargo.toml', 'Cargo.lock']) {
  const file = workspacePath(workspaceRoot, join(realpathSync(cargoRoot), name));
  if (tracked.has(file)) manifests.add(file);
}
// Hashed after the build, the only time rustc's list exists: a source edited during the build is recorded with its new
// bytes. Nx's own cache key is computed before the build and has the same window.
writeBuildInputs(output, buildInputRecord(workspaceRoot, [...sources, ...manifests]));
