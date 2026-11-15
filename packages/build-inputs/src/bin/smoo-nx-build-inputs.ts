#!/usr/bin/env node
/**
 * Writes a build's input record (build-inputs.ts) into its output directory, after the build, in one of two forms:
 *
 *   smoo-nx-build-inputs --manifest-path <Cargo.toml> --dep-info <triple-or-profile-relative .d file> --output <directory>
 *   smoo-nx-build-inputs --input <file> [--input <file>...] [--manifest-path <Cargo.toml> --lock-closure <directory>] --output <directory>
 *
 * A native build names its dep-info. `--manifest-path` is the one the build was given, so `cargo metadata` names the
 * same target directory, and `--dep-info` is relative to Cargo's target directory
 * (`aarch64-apple-darwin/debug/libcowshed_napi.d`), the file rustc wrote listing every source it read. The record lists
 * those workspace files, the `Cargo.toml` of each crate they belong to, the workspace `Cargo.toml`, and the lockfile
 * entries Cargo resolved for those crates (`cargoBuildInputs`, the same record the cargo-zigbuild executor writes for a
 * Linux build). The lockfile itself is never listed: an entry of another crate's closure is no input of this build.
 *
 * A producer that knows its inputs as files, such as one that copies them, names each with `--input`, relative to the
 * working directory, and the record lists exactly those. Each must be a file of the work tree. One that also reads the
 * lockfile for crates (a copy that ships them with the entries they resolve) names their directory with
 * `--lock-closure` and the workspace with `--manifest-path`: the record then states the lockfile closure of every
 * crate under that directory, tests included, as the task's `cargo-lock:<directory>` key does.
 *
 * The record is part of the target's Nx outputs, so a cache hit restores the record that describes the restored
 * outputs. Run by an Nx task, it also names that task (`buildInputProducer`).
 */
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { isAbsolute, join, resolve } from 'node:path';
import {
  type BuildInputRecord,
  buildInputProducer,
  buildInputRecord,
  cargoBuildInputs,
  cargoLockInputs,
  gitWorkspaceRoot,
  workspacePath,
  writeBuildInputs,
} from '../build-inputs.js';
import {
  buildClosure,
  type CargoLockResolve,
  cargoLockResolve,
  localPackagesUnder,
  parseCargoMetadata,
} from '../cargo-closure.js';

const USAGE = [
  'usage: smoo-nx-build-inputs --manifest-path <Cargo.toml> --dep-info <path under the Cargo target directory> --output <directory>',
  '       smoo-nx-build-inputs --input <file> [--input <file>...] [--manifest-path <Cargo.toml> --lock-closure <directory>] --output <directory>',
].join('\n');

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

/**
 * What locked, offline `cargo metadata` prints for `manifestPath`. A workspace's document is megabytes, past any pipe
 * buffer a synchronous spawn holds, so Cargo writes it to a file of a directory this call makes and removes.
 */
function cargoMetadata(manifestPath: string): string {
  const directory = mkdtempSync(join(tmpdir(), 'smoo-build-inputs-'));
  const file = join(directory, 'metadata.json');
  const command = 'exec cargo metadata --format-version 1 --locked --offline --manifest-path "$1" >"$2"';
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: cargo metadata --locked --offline resolves from the lockfile and the fetched registry alone, asks no remote, and its length is Cargo's own; the enclosing build's runner bound or the operator's interrupt ends a hung one
  const result = spawnSync('sh', ['-c', command, 'sh', manifestPath, file], { encoding: 'utf8' });
  const refused = result.error !== undefined || result.status !== 0;
  const stdout = refused ? '' : readFileSync(file, 'utf8');
  rmSync(directory, { recursive: true, force: true });
  if (refused) {
    fail(
      `cargo metadata --locked --offline --manifest-path ${manifestPath} in ${process.cwd()}: ${result.error?.message ?? result.stderr.trim()}`,
    );
  }
  return stdout;
}

/** Every value given for the repeatable `name`. */
function values(name: string): string[] {
  const found: string[] = [];
  process.argv.forEach((argument, at) => {
    if (argument !== name) return;
    const value = process.argv[at + 1];
    if (value === undefined || value.startsWith('-')) fail(`missing the value of ${name}`);
    found.push(value);
  });
  return found;
}

const cwd = process.cwd();
const output = resolve(cwd, flag('--output'));
const workspaceRoot = gitWorkspaceRoot(cwd) ?? fail(`${cwd} is not in a Git work tree, so no input can be named`);
const producer = buildInputProducer(process.env);

/**
 * The resolve Cargo reports for `manifestPath` under its lockfile, by the invocation the nx-plugin's task key reads
 * (`cargo metadata --locked --offline`), and the target directory it names.
 */
function cargoResolve(manifestPath: string): { readonly cargo: CargoLockResolve; readonly targetDirectory: string } {
  const stdout = cargoMetadata(manifestPath);
  const document: unknown = JSON.parse(stdout);
  const targetDirectory =
    typeof document === 'object' && document !== null ? Reflect.get(document, 'target_directory') : undefined;
  if (typeof targetDirectory !== 'string') fail('cargo metadata names no target_directory');
  const metadata = parseCargoMetadata(stdout);
  metadata.workspace_root = realpathSync(metadata.workspace_root);
  for (const pkg of metadata.packages) if (pkg.source === null) pkg.manifest_path = realpathSync(pkg.manifest_path);
  return { cargo: cargoLockResolve(metadata), targetDirectory };
}

/** What `body` returns, or the refusal its thrown reason states: a resolve that cannot answer writes no record. */
function orFail<T>(body: () => T): T {
  try {
    return body();
  } catch (error) {
    return fail(error instanceof Error ? error.message : String(error));
  }
}

/** The record of the files `named`, each a file of the work tree, and of the lockfile closure `--lock-closure` names. */
function namedInputs(named: readonly string[]): BuildInputRecord {
  if (process.argv.includes('--dep-info')) fail('--input names the inputs itself, so it takes no --dep-info');
  const files = named.map((file) => {
    const absolute = resolve(cwd, file);
    if (!existsSync(absolute)) fail(`${file} does not exist in ${cwd}`);
    const relative = workspacePath(workspaceRoot, absolute);
    if (relative.startsWith('../')) fail(`${file} is outside the work tree ${workspaceRoot}`);
    return relative;
  });
  if (!process.argv.includes('--lock-closure')) {
    if (process.argv.includes('--manifest-path'))
      fail('--manifest-path with --input names the workspace of a --lock-closure, which is missing');
    return buildInputRecord(workspaceRoot, files, undefined, producer);
  }
  const directory = resolve(cwd, flag('--lock-closure'));
  if (!existsSync(directory)) fail(`${directory} does not exist`);
  const { cargo } = cargoResolve(flag('--manifest-path'));
  const selected = localPackagesUnder(cargo.local, realpathSync(directory));
  if (selected.length === 0) fail(`no local Cargo package lies under ${directory}`);
  const closure = orFail(() =>
    buildClosure(
      cargo,
      selected.map((pkg) => pkg.id),
    ),
  );
  const cargoLock = orFail(() => cargoLockInputs(workspaceRoot, cargo, closure));
  return buildInputRecord(workspaceRoot, files, undefined, producer, cargoLock);
}

/** The record of the workspace files a Cargo build's dep-info names. */
function cargoInputs(): BuildInputRecord {
  const manifestPath = flag('--manifest-path');
  const depInfoPath = flag('--dep-info');
  const { cargo, targetDirectory } = cargoResolve(manifestPath);
  const depInfo = isAbsolute(depInfoPath) ? depInfoPath : join(targetDirectory, depInfoPath);
  if (!existsSync(depInfo)) fail(`${depInfo} does not exist: the build did not write the dep-info this target names`);
  return (
    orFail(() => cargoBuildInputs(workspaceRoot, readFileSync(depInfo, 'utf8'), cargo, producer)) ??
    fail(`${depInfo} names no file of ${workspaceRoot}`)
  );
}

const named = values('--input');
writeBuildInputs(output, named.length > 0 ? namedInputs(named) : cargoInputs());
