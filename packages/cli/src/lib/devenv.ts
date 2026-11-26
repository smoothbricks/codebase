import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { $ } from 'bun';
import { decode, mergeEnv, printCommandOutput } from './run.js';

type EnvSnapshot = Record<string, string>;

/**
 * Compiler environment variables owned by each checked-out ref's devenv shell.
 * A historical ref whose `ttsc-env.sh` exports only `TTSC_TSGO_BINARY` must not
 * inherit the dispatch ref's `TTSC_TSGO_SOURCE`, so both names are withheld
 * when entering the checked-out shell and dropped from the base environment
 * before merging its exports.
 */
const CHECKOUT_OWNED_TTSC_VARS = ['TTSC_TSGO_BINARY', 'TTSC_TSGO_SOURCE'] as const;

export async function withDevenvEnv<T>(root: string, runWithEnv: () => Promise<T>): Promise<T> {
  const snapshot = snapshotProcessEnv();
  const env = await loadDevenvEnv(root);
  replaceProcessEnv(mergeDevenvEnv(snapshot, env));
  try {
    return await runWithEnv();
  } finally {
    restoreProcessEnv(snapshot);
  }
}

async function loadDevenvEnv(root: string): Promise<EnvSnapshot> {
  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-devenv-env-'));
  const envPath = join(tempDir, 'env');
  try {
    // A login shell reloads host profiles and can replace the selected Nix toolchain in PATH.
    const result = await $`devenv shell -- bash -c ${'env -0 > "$1"'} bash ${envPath}`
      .cwd(join(root, 'tooling', 'direnv'))
      .env(mergeEnv(undefined, CHECKOUT_OWNED_TTSC_VARS))
      .quiet()
      .nothrow();
    if (result.exitCode !== 0) {
      printCommandOutput(decode(result.stdout), decode(result.stderr));
      throw new Error(
        `devenv shell failed with exit code ${result.exitCode}. Ensure devenv is installed and the tooling/direnv shell is valid.`,
      );
    }
    return parseNulEnv(await readFile(envPath));
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

export function parseNulEnv(bytes: Uint8Array): EnvSnapshot {
  const env: EnvSnapshot = {};
  let entryStart = 0;
  for (let index = 0; index <= bytes.length; index += 1) {
    if (index !== bytes.length && bytes[index] !== 0) {
      continue;
    }
    if (index === entryStart) {
      entryStart = index + 1;
      continue;
    }
    const entry = decode(bytes.subarray(entryStart, index));
    const separator = entry.indexOf('=');
    if (separator <= 0) {
      throw new Error('devenv shell produced an invalid environment entry.');
    }
    env[entry.slice(0, separator)] = entry.slice(separator + 1);
    entryStart = index + 1;
  }
  return env;
}

export function mergeDevenvEnv(baseEnv: EnvSnapshot, devenvEnv: EnvSnapshot): EnvSnapshot {
  const merged: EnvSnapshot = { ...baseEnv };
  for (const name of CHECKOUT_OWNED_TTSC_VARS) {
    delete merged[name];
  }
  return { ...merged, ...devenvEnv };
}

function snapshotProcessEnv(): EnvSnapshot {
  const snapshot: EnvSnapshot = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      snapshot[key] = value;
    }
  }
  return snapshot;
}

function replaceProcessEnv(env: EnvSnapshot): void {
  for (const key of Object.keys(process.env)) {
    delete process.env[key];
  }
  for (const [key, value] of Object.entries(env)) {
    process.env[key] = value;
  }
}

function restoreProcessEnv(snapshot: EnvSnapshot): void {
  for (const key of Object.keys(process.env)) {
    delete process.env[key];
  }
  for (const [key, value] of Object.entries(snapshot)) {
    process.env[key] = value;
  }
}
