import { describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setupDevenvSteps } from '../__tests__/setup-devenv-steps.js';

const locate = setupDevenvSteps().find((step) =>
  String(step.get('name')).includes('Locate devenv state on the shared bind'),
);

/** act's host-executor checkout, the path a job runs from when the stable bind is refused. */
const checkout = '/home/runner/.cache/act/402397e7e5534eea/hostexecutor';

interface Host {
  readonly os: string;
  /** Binds earlier jobs left stacked at the stable path. */
  readonly staleBinds: number;
  readonly umountStatus: number;
  readonly mountStatus: number;
}

const freshLinux: Host = { os: 'Linux', staleBinds: 0, umountStatus: 0, mountStatus: 0 };

/**
 * Runs the step's script the way GitHub runs a bash step, with every command that touches the host stubbed to print
 * its argv. mountpoint answers for the binds still stacked at the stable path, a successful sudo umount removes the
 * top one, and sudo umount and sudo mount exit with the host's statuses. mountpoint answers no after ten calls, so a
 * detach loop that never ends shows as a call list instead of a hung test.
 */
function locateDevenvState(host: Host) {
  if (!locate) throw new Error('setup-devenv has no step that locates devenv state on the shared bind');
  const dir = mkdtempSync(join(tmpdir(), 'setup-devenv-stable-'));
  try {
    const githubEnv = join(dir, 'github-env');
    const stubs = [
      'mkdir() { echo "mkdir $*"; }',
      'rm() { echo "rm $*"; }',
      'ln() { echo "ln $*"; }',
      'mountpoint_calls=0',
      'mountpoint() {',
      '  echo "mountpoint $*"',
      '  mountpoint_calls=$((mountpoint_calls + 1))',
      '  [ "$mountpoint_calls" -le 10 ] && [ "$STALE_BINDS" -gt 0 ]',
      '}',
      'sudo() {',
      '  echo "sudo $*"',
      '  case "$1" in',
      '    umount) [ "$UMOUNT_STATUS" = 0 ] || return "$UMOUNT_STATUS"; STALE_BINDS=$((STALE_BINDS - 1)) ;;',
      '    mount) return "$MOUNT_STATUS" ;;',
      '  esac',
      '}',
    ].join('\n');
    const result = Bun.spawnSync(
      ['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', `${stubs}\n${String(locate.get('run'))}`],
      {
        cwd: dir,
        env: {
          ...process.env,
          DEVENV_STATE_REPO: 'example/app',
          DEVENV_STATE_LANE: 'CI',
          DEVENV_STATE_JOB: 'main',
          SCOPE_REF: 'refs/heads/main',
          GITHUB_WORKFLOW: 'CI',
          GITHUB_JOB: 'main',
          CACHE_SCOPE: 'main',
          RUNNER_OS: host.os,
          RUNNER_ARCH: 'X64',
          GITHUB_WORKSPACE: checkout,
          GITHUB_ENV: githubEnv,
          STALE_BINDS: String(host.staleBinds),
          UMOUNT_STATUS: String(host.umountStatus),
          MOUNT_STATUS: String(host.mountStatus),
        },
      },
    );
    const stdout = result.stdout.toString();
    return {
      status: result.exitCode,
      stdout,
      calls: stdout.split('\n').filter((line) => line.startsWith('mountpoint ') || line.startsWith('sudo ')),
      stable: /^mkdir -p (\/work\/\S+)$/m.exec(stdout)?.[1] ?? '',
      githubEnv: existsSync(githubEnv) ? readFileSync(githubEnv, 'utf8') : '',
    };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe('the stable devenv path in setup-devenv', () => {
  // An instance that ran an earlier job of this scope still holds that job's bind at the stable path, onto a checkout
  // the runner deleted; a bind over it fails with ENOENT.
  test("detaches an earlier job's bind before binding this checkout there", () => {
    const run = locateDevenvState({ ...freshLinux, staleBinds: 1 });
    expect(run.status).toBe(0);
    expect(run.stable).toMatch(/^\/work\/example\/app\/ci\/main\/[0-9a-f]{40}$/);
    expect(run.calls).toEqual([
      `mountpoint -q ${run.stable}`,
      `sudo umount --lazy ${run.stable}`,
      `mountpoint -q ${run.stable}`,
      `sudo mount --bind ${checkout} ${run.stable}`,
    ]);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${run.stable}\n`);
  });

  test('detaches every bind earlier jobs stacked there', () => {
    const run = locateDevenvState({ ...freshLinux, staleBinds: 2 });
    expect(run.status).toBe(0);
    expect(run.calls).toEqual([
      `mountpoint -q ${run.stable}`,
      `sudo umount --lazy ${run.stable}`,
      `mountpoint -q ${run.stable}`,
      `sudo umount --lazy ${run.stable}`,
      `mountpoint -q ${run.stable}`,
      `sudo mount --bind ${checkout} ${run.stable}`,
    ]);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${run.stable}\n`);
  });

  test("binds a fresh instance's stable path without detaching anything", () => {
    const run = locateDevenvState(freshLinux);
    expect(run.status).toBe(0);
    expect(run.calls).toEqual([`mountpoint -q ${run.stable}`, `sudo mount --bind ${checkout} ${run.stable}`]);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${run.stable}\n`);
  });

  test('goes on from the checkout, and says every task runs there, when the bind is refused', () => {
    const run = locateDevenvState({ ...freshLinux, mountStatus: 32 });
    expect(run.status).toBe(0);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${checkout}\n`);
    expect(run.stdout).toContain(
      `::warning::stable devenv path unavailable (bind mount refused); devenv and every task run from the checkout at ${checkout}`,
    );
  });

  test('leaves a bind it could not detach to the kernel, and goes on from the checkout when that refuses', () => {
    const run = locateDevenvState({ ...freshLinux, staleBinds: 1, umountStatus: 32, mountStatus: 32 });
    expect(run.status).toBe(0);
    expect(run.calls).toEqual([
      `mountpoint -q ${run.stable}`,
      `sudo umount --lazy ${run.stable}`,
      `sudo mount --bind ${checkout} ${run.stable}`,
    ]);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${checkout}\n`);
  });

  test('looks for no bind on a host that is not Linux, which has no mountpoint', () => {
    const run = locateDevenvState({ ...freshLinux, os: 'macOS', staleBinds: 1, mountStatus: 32 });
    expect(run.status).toBe(0);
    expect(run.calls).toEqual([`sudo mount --bind ${checkout} ${run.stable}`]);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${checkout}\n`);
  });
});
