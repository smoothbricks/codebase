/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CowshedError, type ErrorCode, openHost } from '../dist/ts/index.js';

/** The taxonomy and a non-empty hint are the contract; message copy is not (see native.test.ts). */
function requireCowshedError(error: unknown, code: ErrorCode): CowshedError {
  expect(error).toBeInstanceOf(CowshedError);
  if (!(error instanceof CowshedError)) {
    // invariant throw: the assertion above proves this branch unreachable.
    throw new Error('expected CowshedError');
  }
  expect(error.code).toBe(code);
  expect(error.hint.length).toBeGreaterThan(0);
  return error;
}

const launcher = join(import.meta.dir, '..', 'bin', 'cowshed');
const recorded = { paths: [{ checkout: '.nx/cache', volume: 'nx/cache' }] };

/** A checkout whose build-volume link records one build-state path, as cowshed writes it. */
async function recordedCheckout(): Promise<{ root: string; checkout: string }> {
  const root = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-'));
  const checkout = join(root, 'checkout');
  const volume = join(root, 'volume');
  await mkdir(join(checkout, '.git'), { recursive: true });
  await mkdir(join(checkout, '.cowshed'), { recursive: true });
  await mkdir(volume);
  await writeFile(join(volume, 'cowshed-build-state.json'), JSON.stringify({ version: 1, paths: recorded.paths }));
  await symlink(volume, join(checkout, '.cowshed', 'build'));
  return { root, checkout };
}

/** This process's `cowshed host` children: the launcher execs the binary, so each is our child. */
function hostChildren(): number[] {
  const listed = spawnSync('/bin/ps', ['-ax', '-o', 'pid=,ppid=,command='], { encoding: 'utf8' });
  expect(listed.status).toBe(0);
  return listed.stdout
    .split('\n')
    .map((line) => line.trim().split(/\s+/))
    .filter(([, ppid, ...command]) => Number(ppid) === process.pid && command.join(' ').endsWith('cowshed host'))
    .map(([pid]) => Number(pid));
}

describe('openHost', () => {
  it('answers host.buildState with exactly what the build-state verb prints', async () => {
    const { root, checkout } = await recordedCheckout();
    const host = await openHost();
    try {
      const answered = await host.buildState(checkout);
      const verb = spawnSync(launcher, ['--json', '--project', checkout, 'build-state'], { encoding: 'utf8' });
      expect(verb.status).toBe(0);
      expect(answered).toEqual(JSON.parse(verb.stdout));
      expect(answered).toEqual(recorded);
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects with the verb own typed refusal', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-bare-'));
    const host = await openHost();
    try {
      const refused = await host.buildState(root).then(
        () => undefined,
        (error: unknown) => error,
      );
      requireCowshedError(refused, 'environment-missing');
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('installs the skill into the named project only, and refuses an unknown harness writing nothing', async () => {
    const project = await mkdtemp(join(tmpdir(), 'cowshed-host-napi-skill-'));
    const host = await openHost();
    try {
      const refused = await host.skillInstall({ project, harnesses: ['no-such-harness'] }).then(
        () => undefined,
        (error: unknown) => error,
      );
      requireCowshedError(refused, 'usage');
      expect(await readdir(project)).toEqual([]);
      const report = await host.skillInstall({ project, harnesses: ['claude-code'] });
      expect(report.installs.length).toBeGreaterThan(0);
      for (const install of report.installs) {
        expect(install.path.startsWith(project)).toBe(true);
      }
    } finally {
      await host.close();
      await rm(project, { recursive: true, force: true });
    }
  });

  it('every concurrent close resolves once its cowshed host process has ended, and later calls reject', async () => {
    const { root, checkout } = await recordedCheckout();
    const before = hostChildren();
    const host = await openHost();
    try {
      expect(await host.buildState(checkout)).toEqual(recorded);
      const started = hostChildren().filter((pid) => !before.includes(pid));
      expect(started).toHaveLength(1);
      // Each close's own fulfillment is checked when it happens: a close that resolved before the
      // process ended would see the pid still listed.
      const ended = await Promise.all(
        [host.close(), host.close(), host.close()].map((close) =>
          close.then(() => hostChildren().includes(started[0])),
        ),
      );
      expect(ended).toEqual([false, false, false]);
      const after = await host.buildState(checkout).then(
        () => undefined,
        (error: unknown) => error,
      );
      requireCowshedError(after, 'environment-missing');
    } finally {
      await host.close();
      await rm(root, { recursive: true, force: true });
    }
  });
});
