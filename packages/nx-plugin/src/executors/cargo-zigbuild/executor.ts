import { spawn } from 'node:child_process';
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { chmod, copyFile, mkdir, rename } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline';
import {
  type BuildInputProducer,
  type BuildInputRecord,
  buildInputProducer,
  cargoBuildInputs,
  gitWorkspaceRoot,
  writeBuildInputs,
} from '@smoothbricks/build-inputs';
import { cargoWorkspace, describeRetentionError, type Result } from '@smoothbricks/cargo-retain';
import typia from 'typia';

import type { CargoZigbuildOptions } from './schema.js';

interface CargoZigbuildContext {
  root: string;
}

/**
 * The fields of cargo's `compiler-artifact` JSON message this executor reads.
 * https://doc.rust-lang.org/cargo/reference/external-tools.html#artifact-messages
 */
export interface CargoArtifactMessage {
  reason: 'compiler-artifact';
  manifest_path: string;
  target: { kind: string[]; name: string };
  filenames: string[];
  executable: string | null;
}

const validateReason = typia.createValidate<{ reason: string }>();
const validateArtifact = typia.createValidate<CargoArtifactMessage>();

/** What one build produces: a named binary, or the package's cdylib. */
export type ZigbuildArtifact = { kind: 'bin'; name: string } | { kind: 'cdylib' };

export function zigbuildArtifact(options: Pick<CargoZigbuildOptions, 'bin'>): ZigbuildArtifact {
  return options.bin === undefined ? { kind: 'cdylib' } : { kind: 'bin', name: options.bin };
}

/**
 * `cargo-zigbuild zigbuild`, not `cargo zigbuild`. Run through cargo, the
 * subcommand inherits `CARGO` naming the real cargo binary and calls it
 * directly, bypassing the `cargo` on PATH; run directly, its inner `cargo` is
 * the PATH one every other cargo target runs. `--locked` keeps Cargo.lock
 * exactly as committed. The glibc suffix stays on `--target`: it is how
 * cargo-zigbuild pins the symbol versions, and cargo itself sees the bare
 * triple.
 */
export function zigbuildArgs(options: CargoZigbuildOptions, manifestPath: string): string[] {
  const artifact = zigbuildArtifact(options);
  return [
    'zigbuild',
    '--locked',
    '--message-format=json-render-diagnostics',
    '--target',
    options.target,
    '--manifest-path',
    manifestPath,
    '--package',
    options.package,
    ...(options.release === true ? ['--release'] : []),
    ...(artifact.kind === 'bin' ? ['--bin', artifact.name] : ['--lib']),
  ];
}

/** `x86_64-unknown-linux-gnu.2.17` → `x86_64-unknown-linux-gnu`. */
export function cargoTriple(target: string): string {
  const glibcSuffix = /\.\d+\.\d+$/.exec(target);
  return glibcSuffix === null ? target : target.slice(0, glibcSuffix.index);
}

/**
 * The environment without any compiler or linker named for this triple.
 *
 * cargo-zigbuild supplies the triple's `CC_`/`CXX_`/`AR_`/`RANLIB_` and
 * linker only where none is set, and cc-rs reads the hyphenated spelling
 * before cargo-zigbuild's underscored one. A shell that pins its host
 * compiler for the same triple — a Linux devenv shell pins
 * `CC_x86_64_unknown_linux_gnu` to Nix's cc wrapper for host builds — would
 * otherwise compile the artifact's C against the build host's glibc headers,
 * which is exactly the build-host leak the zig toolchain exists to remove.
 */
export function zigbuildEnv(env: NodeJS.ProcessEnv, target: string): NodeJS.ProcessEnv {
  const triple = cargoTriple(target);
  const underscored = triple.replaceAll('-', '_');
  const owned = new Set([
    ...['CC', 'CXX', 'AR', 'RANLIB'].flatMap((tool) => [`${tool}_${triple}`, `${tool}_${underscored}`]),
    `CARGO_TARGET_${underscored.toUpperCase()}_LINKER`,
  ]);
  return Object.fromEntries(Object.entries(env).filter(([name]) => !owned.has(name)));
}

/**
 * The artifact file one cargo JSON message names, or null when the message is
 * about anything else. `manifestPath` must be canonical, as cargo reports it.
 */
export function artifactFile(line: string, manifestPath: string, artifact: ZigbuildArtifact): string | null {
  if (!line.startsWith('{')) return null;
  const parsed: unknown = JSON.parse(line);
  const header = validateReason(parsed);
  if (!header.success) {
    throw new Error(`cargo emitted a JSON message without a reason: ${line}`);
  }
  if (header.data.reason !== 'compiler-artifact') return null;
  const message = validateArtifact(parsed);
  if (!message.success) {
    throw new Error(
      `cargo compiler-artifact message does not match the artifact contract: ${JSON.stringify(message.errors)}`,
    );
  }
  const { data } = message;
  if (data.manifest_path !== manifestPath) return null;
  if (artifact.kind === 'bin') {
    return data.target.kind.includes('bin') && data.target.name === artifact.name ? data.executable : null;
  }
  if (!data.target.kind.includes('cdylib')) return null;
  return data.filenames.find((file) => file.endsWith('.so')) ?? null;
}

/**
 * The dep-info file cargo uplifts beside an artifact, which lists every source rustc read for it: the artifact's file
 * name with any extension replaced by `.d`. The static CLI `cowshed` has none (`cowshed.d`); the addon
 * `libcowshed_napi.so` loses its `.so`.
 */
export function depInfoPath(built: string): string {
  const { dir, name } = path.parse(built);
  return path.join(dir, `${name}.d`);
}

/**
 * The input record (build-inputs.ts) of the build that wrote `built`, or why none can be made: the record
 * `smoo-nx-build-inputs` writes after a macOS build, from the dep-info beside the artifact, so one link check judges
 * both. `cargoRoot` is the Cargo workspace root, whose manifest and lock the record names as well. `producer` is the
 * Nx task running the build, which the executor reads from its environment.
 */
export function buildInputsOf(
  built: string,
  manifestPath: string,
  cargoRoot: string,
  producer?: BuildInputProducer,
): Result<BuildInputRecord, string> {
  const directory = path.dirname(manifestPath);
  const workspaceRoot = gitWorkspaceRoot(directory);
  if (workspaceRoot === null) {
    return { ok: false, error: `${directory} is not in a Git work tree, so no input can be named` };
  }
  const depInfo = depInfoPath(built);
  if (!existsSync(depInfo)) {
    return { ok: false, error: `${depInfo} does not exist: cargo wrote no dep-info beside ${built}` };
  }
  const record = cargoBuildInputs(workspaceRoot, readFileSync(depInfo, 'utf8'), cargoRoot, producer);
  if (record === null) return { ok: false, error: `${depInfo} names no file of ${workspaceRoot}` };
  return { ok: true, value: record };
}

/**
 * Writes the artifact and its input record into the directory `output` is in, which the target declares as its Nx
 * output (`{options.output}/../`), so a cache hit restores both. The artifact is copied then renamed, so the output
 * path never holds a partial file, and the record follows it.
 */
export async function publishArtifact(
  built: string,
  output: string,
  artifact: ZigbuildArtifact,
  inputs: BuildInputRecord,
): Promise<void> {
  const directory = path.dirname(output);
  await mkdir(directory, { recursive: true });
  const staging = `${output}.partial`;
  await copyFile(built, staging);
  if (artifact.kind === 'bin') {
    await chmod(staging, 0o755);
  }
  await rename(staging, output);
  writeBuildInputs(directory, inputs);
}

export default async function cargoZigbuildExecutor(
  options: CargoZigbuildOptions,
  context: CargoZigbuildContext,
): Promise<{ success: boolean }> {
  const producer = buildInputProducer(process.env);
  const manifestPath = realpathSync(path.resolve(context.root, options.manifestPath));
  const output = path.resolve(context.root, options.output);
  const artifact = zigbuildArtifact(options);
  const args = zigbuildArgs(options, manifestPath);
  const command = `cargo-zigbuild ${args.join(' ')}`;

  // Cargo discovers `.cargo/config.toml` from its working directory upward,
  // so the crate's own directory sees every config a build of it would.
  const child = spawn('cargo-zigbuild', args, {
    cwd: path.dirname(manifestPath),
    env: zigbuildEnv(process.env, options.target),
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a cargo-zigbuild build lasts as long as its work; the operator's interrupt or the enclosing runner's bound ends a hung one
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });

  let built: string | null = null;
  for await (const line of createInterface({ input: child.stdout })) {
    built = artifactFile(line, manifestPath, artifact) ?? built;
  }

  let status: { code: number | null; signal: NodeJS.Signals | null };
  try {
    status = await exited;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      console.error(
        `${command}: cargo-zigbuild is not on PATH. Linux artifacts build with the linux-cross toolchain; ` +
          'on macOS run the target inside it: tooling/devenv -P linux-cross shell -- nx run <project>:<target>',
      );
      return { success: false };
    }
    throw error;
  }
  if (status.code !== 0) {
    console.error(`${command}: exited with ${status.signal ?? `status ${status.code}`}`);
    return { success: false };
  }
  if (built === null) {
    const wanted = artifact.kind === 'bin' ? `binary ${artifact.name}` : 'cdylib';
    console.error(`${command}: cargo reported no ${wanted} artifact for ${options.package} at ${manifestPath}`);
    return { success: false };
  }

  // From the dep-info cargo just wrote, before the artifact is copied: a build whose inputs cannot be named leaves the
  // output as it was, and a source edited while the artifact is copied is not recorded with its new bytes.
  const cargo = await cargoWorkspace(path.dirname(manifestPath));
  if (!cargo.ok) {
    console.error(`${command}: ${describeRetentionError(cargo.error)}`);
    return { success: false };
  }
  const inputs = buildInputsOf(built, manifestPath, cargo.value.workspaceRoot, producer);
  if (!inputs.ok) {
    console.error(`${command}: ${inputs.error}`);
    return { success: false };
  }
  await publishArtifact(built, output, artifact, inputs.value);
  console.log(`${path.relative(context.root, built)} -> ${path.relative(context.root, output)}`);
  return { success: true };
}
