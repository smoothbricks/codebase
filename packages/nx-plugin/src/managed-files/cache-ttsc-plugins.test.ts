/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: GitHub Actions expressions are asserted literally. */
import { describe, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import typia from 'typia';

/**
 * The managed ttsc plugin cache has two halves that meet only through the job's environment: setup restores, and the
 * job-end hook saves. GitHub cannot run here, so these tests run the scripts the halves hand each other against fixture
 * checkouts, and pin where each half is wired in.
 */

type Scalar = string | number | boolean;

interface ActionStep {
  readonly name: string;
  readonly if?: string;
  readonly uses?: string;
  readonly run?: string;
  readonly with?: Record<string, Scalar>;
}

interface Action {
  readonly runs: { readonly steps: ActionStep[] };
}

const SAVE_KEY = 'SMOO_TTSC_PLUGINS_SAVE_KEY';
const RESTORED_AT = 'SMOO_TTSC_PLUGINS_RESTORED_AT_MS';
/** When the fixture job restored its cache, in the epoch milliseconds ttsc stamps entries with. */
const RESTORED_AT_MS = 1_792_000_000_000;
const DAY_MS = 24 * 60 * 60 * 1000;

function stepsOf(action: string): ActionStep[] {
  const manifest = new URL(`../../managed/templates/github/actions/${action}/action.yml`, import.meta.url);
  return typia.assert<Action>(Bun.YAML.parse(readFileSync(manifest, 'utf8'))).runs.steps;
}

function stepNamed(action: string, name: string): ActionStep {
  const step = stepsOf(action).find((candidate) => candidate.name === name);
  if (step === undefined) throw new Error(`${action} has no step named ${name}`);
  return step;
}

/** This suite may run in a job whose setup set the cache variables; a step sees only what a test gives it. */
function ambientEnvironment(): Record<string, string> {
  return Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined && !entry[0].startsWith('SMOO_TTSC_PLUGINS_'),
    ),
  );
}

/** Runs a step's script the way GitHub runs a bash step. */
function runScript(step: ActionStep, env: Record<string, string>) {
  if (step.run === undefined) throw new Error(`${step.name} has no script`);
  const result = Bun.spawnSync(['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', step.run], {
    env: { ...ambientEnvironment(), ...env },
  });
  return { status: result.exitCode, stdout: result.stdout.toString(), stderr: result.stderr.toString() };
}

/** A checkout whose plugin cache holds entries stamped the way ttsc stamps a hit or a build (null: never stamped). */
function checkoutWithPlugins(entries: Record<string, number | null>) {
  const workspace = mkdtempSync(join(tmpdir(), 'ttsc-plugin-cache-'));
  const plugins = join(workspace, '.cache', 'ttsc-plugins', 'plugins');
  mkdirSync(plugins, { recursive: true });
  writeFileSync(join(plugins, '.gc-last-run'), `${RESTORED_AT_MS}\n`);
  for (const [name, stamp] of Object.entries(entries)) {
    mkdirSync(join(plugins, name));
    writeFileSync(join(plugins, name, 'plugin'), 'binary');
    if (stamp !== null) writeFileSync(join(plugins, name, '.last-used'), `${stamp}\n`);
    // The persistent lock directory ttsc leaves beside every entry it built.
    mkdirSync(join(plugins, `${name}.lock.v2`, 'retired'), { recursive: true });
    writeFileSync(join(plugins, `${name}.lock.v2`, 'protocol'), 'v2\n');
  }
  return { workspace, plugins };
}

function prune(workspace: string, restoredAt: number | undefined) {
  const output = join(workspace, 'github-output');
  writeFileSync(output, '');
  const run = runScript(stepNamed('cache-ttsc-plugins', 'Drop ttsc plugin entries this job did not use'), {
    GITHUB_WORKSPACE: workspace,
    GITHUB_OUTPUT: output,
    ...(restoredAt === undefined ? {} : { [RESTORED_AT]: String(restoredAt) }),
  });
  return { ...run, output: readFileSync(output, 'utf8') };
}

describe('restoring the ttsc plugins', () => {
  test('falls back to the newest cache of this OS and architecture, and no wider', () => {
    const restore = stepNamed('cache-ttsc-plugins', 'Restore ttsc plugin binaries');
    const key = String(restore.with?.key);
    const fallback = String(restore.with?.['restore-keys']).trim();

    expect(fallback).toBe('${{ runner.os }}-${{ runner.arch }}-ttsc-plugins-v1-');
    // The fallback is the exact key up to its hash, so a prefix hit is a cache saved from the same key version.
    expect(key.startsWith(fallback)).toBe(true);
    expect(key.slice(fallback.length)).toStartWith('${{ hashFiles(');
  });

  test('restores and saves with one pinned actions/cache release', () => {
    const restore = stepNamed('cache-ttsc-plugins', 'Restore ttsc plugin binaries');
    const save = stepNamed('cache-ttsc-plugins', 'Save ttsc plugin binaries');

    expect(restore.uses).toStartWith('actions/cache/restore@');
    expect(save.uses).toStartWith('actions/cache/save@');
    const pin = restore.uses?.split('@')[1];
    expect(pin).toMatch(/^[0-9a-f]{40}$/);
    expect(save.uses?.split('@')[1]).toBe(pin);
  });
});

describe('handing the restore to the job-end save', () => {
  test('records the key the restore looked up and a millisecond stamp that is not later than the restore', () => {
    const dir = mkdtempSync(join(tmpdir(), 'ttsc-plugin-env-'));
    try {
      const envFile = join(dir, 'github-env');
      writeFileSync(envFile, '');
      const before = Date.now();
      const run = runScript(stepNamed('cache-ttsc-plugins', 'Note the key and time for the job-end save'), {
        GITHUB_ENV: envFile,
        PRIMARY_KEY: 'macOS-ARM64-ttsc-plugins-v1-0123abcd',
      });
      const after = Date.now();
      expect(run.status).toBe(0);

      const recorded: Record<string, string> = {};
      for (const line of readFileSync(envFile, 'utf8').trimEnd().split('\n')) {
        recorded[line.slice(0, line.indexOf('='))] = line.slice(line.indexOf('=') + 1);
      }
      expect(recorded[SAVE_KEY]).toBe('macOS-ARM64-ttsc-plugins-v1-0123abcd');
      // `date` counts whole seconds, so the stamp rounds down: never after the restore, less than a second before it.
      const stamp = Number(recorded[RESTORED_AT]);
      expect(stamp).toBeLessThanOrEqual(after);
      expect(stamp).toBeGreaterThan(before - 1_000);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('saves under the carried key, never one hashed again after a release step rewrote bun.lock', () => {
    const save = stepNamed('cache-ttsc-plugins', 'Save ttsc plugin binaries');

    expect(save.with?.key).toBe('${{ env.SMOO_TTSC_PLUGINS_SAVE_KEY }}');
  });

  test('setup restores before it builds the shell and never saves', () => {
    const setup = stepsOf('setup-devenv');
    const restoreAt = setup.findIndex((step) => step.uses === './.github/actions/cache-ttsc-plugins');
    const buildAt = setup.findIndex((step) => step.name === '🐚 Build devenv shell');

    expect(restoreAt).toBeGreaterThanOrEqual(0);
    expect(buildAt).toBeGreaterThan(restoreAt);
    expect(setup[restoreAt]?.with?.mode ?? 'restore').toBe('restore');
    expect(setup.filter((step) => step.uses === './.github/actions/cache-ttsc-plugins')).toHaveLength(1);
  });

  test('the job-end hook saves, so a red job still saves what it compiled', () => {
    const hook = stepsOf('save-nix-devenv').filter((step) => step.uses === './.github/actions/cache-ttsc-plugins');

    expect(hook.map((step) => step.with?.mode)).toEqual(['save']);
  });
});

describe('dropping the ttsc plugin entries a job did not use', () => {
  test('keeps what the job used and drops what an earlier generation left, lock directories included', () => {
    const { workspace, plugins } = checkoutWithPlugins({
      hit: RESTORED_AT_MS + 5_000,
      built: RESTORED_AT_MS,
      superseded: RESTORED_AT_MS - DAY_MS,
      unstamped: null,
    });
    try {
      const run = prune(workspace, RESTORED_AT_MS);

      expect(run.status).toBe(0);
      expect(readdirSync(plugins).sort()).toEqual(['.gc-last-run', 'built', 'built.lock.v2', 'hit', 'hit.lock.v2']);
      // An entry in use keeps its lock directory whole.
      expect(readFileSync(join(plugins, 'hit.lock.v2', 'protocol'), 'utf8')).toBe('v2\n');
      expect(run.output).toBe('kept=2\n');
      expect(run.stdout).toContain('dropping superseded');
      expect(run.stdout).toContain('dropping unstamped');
      expect(run.stdout).not.toContain('dropping hit');
      expect(run.stdout).not.toContain('dropping built');
    } finally {
      rmSync(workspace, { recursive: true, force: true });
    }
  });

  test('keeps nothing when the job used no entry, which is also what stops the save', () => {
    const { workspace, plugins } = checkoutWithPlugins({ earlier: RESTORED_AT_MS - 1 });
    try {
      const run = prune(workspace, RESTORED_AT_MS);

      expect(run.status).toBe(0);
      expect(run.output).toBe('kept=0\n');
      expect(readdirSync(plugins)).toEqual(['.gc-last-run']);
    } finally {
      rmSync(workspace, { recursive: true, force: true });
    }
    // The save runs only on a positive count: an emptied cache saved under the exact key would stay there.
    const save = stepNamed('cache-ttsc-plugins', 'Save ttsc plugin binaries');
    expect(save.if).toContain('steps.prune.outputs.kept > 0');
  });

  test('keeps nothing when the job never made a plugin cache', () => {
    const workspace = mkdtempSync(join(tmpdir(), 'ttsc-plugin-cache-'));
    try {
      const run = prune(workspace, RESTORED_AT_MS);

      expect(run.status).toBe(0);
      expect(run.output).toBe('kept=0\n');
    } finally {
      rmSync(workspace, { recursive: true, force: true });
    }
  });

  test('refuses to run without the restore it measures against, and deletes nothing', () => {
    const { workspace, plugins } = checkoutWithPlugins({ earlier: RESTORED_AT_MS - 1 });
    try {
      const run = prune(workspace, undefined);

      expect(run.status).not.toBe(0);
      expect(run.stderr).toContain(RESTORED_AT);
      expect(readdirSync(plugins)).toContain('earlier');
    } finally {
      rmSync(workspace, { recursive: true, force: true });
    }
  });
});
