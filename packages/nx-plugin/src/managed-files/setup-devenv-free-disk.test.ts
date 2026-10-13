import { describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** The setup action's steps in the order a runner takes them, each as its fields. */
function actionSteps(yaml: string): Map<string, unknown>[] {
  const action: unknown = Bun.YAML.parse(yaml);
  if (typeof action !== 'object' || action === null || !('runs' in action)) return [];
  const runs = action.runs;
  if (typeof runs !== 'object' || runs === null || !('steps' in runs) || !Array.isArray(runs.steps)) return [];
  return runs.steps.map(
    (step: unknown) => new Map(typeof step === 'object' && step !== null ? Object.entries(step) : []),
  );
}

const steps = actionSteps(
  readFileSync(new URL('../../managed/templates/github/actions/setup-devenv/action.yml', import.meta.url), 'utf8'),
);
const freeDiskAt = steps.findIndex((step) => String(step.get('run') ?? '').includes('sudo rm -rf'));
const freeDisk = steps[freeDiskAt];

/**
 * Runs the step's script the way GitHub runs a bash step, with df reporting `freeGiB` free on / and sudo exiting with
 * `sudoStatus`. A sudo that succeeds frees 40 GiB; nothing on disk is deleted.
 */
function runFreeDisk(freeGiB: number, sudoStatus: number) {
  if (!freeDisk) throw new Error('setup-devenv has no step that frees runner disk');
  const tools = mkdtempSync(join(tmpdir(), 'hostedtoolcache-'));
  try {
    const stubs = [
      'free=$((FREE_GIB * 1024 * 1024))',
      'df() { if [ "$1" = -k ]; then printf "Avail\\n%s\\n" "$free"; else echo "df $*"; fi; }',
      'sudo() { echo "sudo $*"; [ "$SUDO_STATUS" = 0 ] || return "$SUDO_STATUS"; free=$((free + 40 * 1024 * 1024)); }',
    ].join('\n');
    const result = Bun.spawnSync(
      ['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', `${stubs}\n${String(freeDisk.get('run'))}`],
      {
        env: {
          ...process.env,
          AGENT_TOOLSDIRECTORY: tools,
          FREE_GIB: String(freeGiB),
          SUDO_STATUS: String(sudoStatus),
        },
      },
    );
    const stdout = result.stdout.toString();
    return {
      tools,
      status: result.exitCode,
      stdout,
      deletes: stdout.split('\n').filter((line) => line.startsWith('sudo rm -rf ')),
    };
  } finally {
    rmSync(tools, { recursive: true, force: true });
  }
}

describe('freeing GitHub-hosted runner disk in setup-devenv', () => {
  test('runs only on GitHub-hosted Linux runners that take the ephemeral path', () => {
    // A self-hosted machine keeps everything, even one that takes the ephemeral branch.
    expect(String(freeDisk?.get('if')).split(' && ').sort()).toEqual([
      "runner.environment == 'github-hosted'",
      "runner.os == 'Linux'",
      "steps.runner-kind.outputs.host != 'true'",
    ]);
  });

  test('frees the space before any action restores or installs something on the disk', () => {
    expect(freeDisk).toBeDefined();
    // Every action the setup uses restores a cache or installs Nix.
    const actions = steps.flatMap((step, index) => (step.has('uses') ? [index] : []));
    expect(actions.length).toBeGreaterThan(0);
    expect(Math.min(...actions)).toBeGreaterThan(freeDiskAt);
  });

  test('deletes nothing on a runner that already has the space', () => {
    const run = runFreeDisk(90, 0);
    expect(run.status).toBe(0);
    expect(run.deletes).toEqual([]);
  });

  test('stops deleting as soon as / has the space', () => {
    const run = runFreeDisk(10, 0);
    expect(run.status).toBe(0);
    expect(run.deletes).toHaveLength(1);
  });

  test('warns and goes on when a delete fails, since freeing space is never a gate', () => {
    const run = runFreeDisk(10, 1);
    expect(run.status).toBe(0);
    expect(run.deletes).toContain(`sudo rm -rf ${run.tools}`);
    expect(run.stdout).toContain(`::warning::could not remove ${run.tools}`);
  });
});
