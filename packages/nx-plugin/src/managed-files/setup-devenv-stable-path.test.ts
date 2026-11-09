import { describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { saveNixDevenvSteps, setupDevenvSteps } from '../__tests__/setup-devenv-steps.js';

const locate = setupDevenvSteps().find((step) =>
  String(step.get('name')).includes('Locate devenv state on the shared bind'),
);
const release = saveNixDevenvSteps().find((step) => step.get('name') === 'Release job-owned host cache mounts');

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
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${run.stable}\nCI_DEVENV_WORKSPACE_MOUNT=${run.stable}\n`);
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
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${run.stable}\nCI_DEVENV_WORKSPACE_MOUNT=${run.stable}\n`);
  });

  test("binds a fresh instance's stable path without detaching anything", () => {
    const run = locateDevenvState(freshLinux);
    expect(run.status).toBe(0);
    expect(run.calls).toEqual([`mountpoint -q ${run.stable}`, `sudo mount --bind ${checkout} ${run.stable}`]);
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${run.stable}\nCI_DEVENV_WORKSPACE_MOUNT=${run.stable}\n`);
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
    expect(run.githubEnv).toBe(`DEVENV_WORKDIR=${checkout}\n`);
  });
});

/**
 * Runs save-nix-devenv's release step with the job environment setup left behind and sudo stubbed to print its argv;
 * sudo umount exits with `umountStatus`.
 */
function releaseMounts(jobEnv: Record<string, string>, umountStatus = 0) {
  if (!release) throw new Error('save-nix-devenv has no step that releases the job-owned host cache mounts');
  const stub = ['sudo() {', '  echo "sudo $*"', '  [ "$1" != umount ] || return "$UMOUNT_STATUS"', '}'].join('\n');
  const result = Bun.spawnSync(
    ['bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', `${stub}\n${String(release.get('run'))}`],
    { env: { PATH: process.env.PATH ?? '', UMOUNT_STATUS: String(umountStatus), ...jobEnv } },
  );
  return {
    status: result.exitCode,
    calls: result.stdout
      .toString()
      .split('\n')
      .filter((line) => line.startsWith('sudo ')),
  };
}

/** The job environment a run of the locate step leaves: its GITHUB_ENV lines as variables. */
function jobEnvironment(githubEnv: string): Record<string, string> {
  return Object.fromEntries(
    githubEnv
      .split('\n')
      .filter((line) => line.includes('='))
      .map((line) => [line.slice(0, line.indexOf('=')), line.slice(line.indexOf('=') + 1)]),
  );
}

describe('the job-end release of the stable devenv bind', () => {
  test('detaches exactly the bind setup made, so the runner can remove its checkout', () => {
    const run = locateDevenvState(freshLinux);
    const released = releaseMounts(jobEnvironment(run.githubEnv));
    expect(released).toEqual({ status: 0, calls: [`sudo umount --lazy -- ${run.stable}`] });
  });

  test('detaches nothing when setup bound nothing', () => {
    const run = locateDevenvState({ ...freshLinux, mountStatus: 32 });
    expect(releaseMounts(jobEnvironment(run.githubEnv))).toEqual({ status: 0, calls: [] });
  });

  test('fails the step when a detach is refused, after trying every mount', () => {
    const released = releaseMounts(
      { CI_DEVENV_WORKSPACE_MOUNT: '/work/a', CI_CARGO_TARGET_MOUNT: '/work/a/target' },
      32,
    );
    expect(released).toEqual({
      status: 1,
      calls: ['sudo umount --lazy -- /work/a/target', 'sudo umount --lazy -- /work/a'],
    });
  });
});
