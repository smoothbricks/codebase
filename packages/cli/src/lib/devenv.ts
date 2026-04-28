import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { $ } from 'bun';
import { decode } from './run.js';

type EnvSnapshot = Record<string, string>;

export async function withDevenvEnv<T>(root: string, runWithEnv: () => Promise<T>): Promise<T> {
  const env = await loadDevenvEnv(root);
  const snapshot = snapshotProcessEnv();
  replaceProcessEnv(env);
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
    const result = await $`devenv shell -- bash -lc ${'env -0 > "$1"'} bash ${envPath}`
      .cwd(join(root, 'tooling', 'direnv'))
      .quiet()
      .nothrow();
    if (result.exitCode !== 0) {
      throw new Error('devenv shell failed. Ensure devenv is installed and the tooling/direnv shell is valid.');
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
