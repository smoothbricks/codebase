import { $ } from 'bun';
import { isRecord } from './json.js';
import { decode, runStatus } from './run.js';

type EnvValue = string | null;
type EnvPatch = Record<string, EnvValue>;
type EnvSnapshot = Record<string, string>;

export async function direnvRun(root: string, command: string, args: string[]): Promise<void> {
  const status = await direnvRunStatus(root, command, args);
  if (status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${status}`);
  }
}

export async function direnvRunStatus(root: string, command: string, args: string[], quiet = false): Promise<number> {
  return withDirenvEnv(root, async () => runStatus(command, args, root, quiet));
}

export async function withDirenvEnv<T>(root: string, runWithEnv: () => Promise<T>): Promise<T> {
  const env = await loadDirenvEnv(root);
  const snapshot = snapshotProcessEnv();
  applyEnvPatch(env);
  try {
    return await runWithEnv();
  } finally {
    restoreProcessEnv(snapshot);
  }
}

async function loadDirenvEnv(root: string): Promise<EnvPatch> {
  const result = await $`direnv export json`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    throw new Error('direnv export json failed. Ensure direnv is installed and the repo .envrc is allowed.');
  }
  const output = decode(result.stdout).trim();
  if (!output) {
    return {};
  }
  const parsed = JSON.parse(output);
  if (!isRecord(parsed)) {
    throw new Error('direnv export json returned a non-object payload.');
  }
  const env: EnvPatch = {};
  for (const [key, value] of Object.entries(parsed)) {
    if (typeof value === 'string' || value === null) {
      env[key] = value;
      continue;
    }
    throw new Error(`direnv export json returned an unsupported value for ${key}.`);
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

function applyEnvPatch(env: EnvPatch): void {
  for (const [key, value] of Object.entries(env)) {
    if (value === null) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
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
