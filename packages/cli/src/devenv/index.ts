import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { run } from '../lib/run.js';
import { type DevenvReleaseRecording, recordDevenvRelease, recordingResult } from '../monorepo/devenv-release.js';
import { printResults } from '../monorepo/managed-files.js';

export interface DevenvCommandShell {
  run(command: string, args: string[], cwd: string): Promise<void>;
  remove(path: string): Promise<void>;
  /** Asks the remote of the devenv rev devenv.lock locks for its release verdict, and records it. */
  recordRelease(root: string): Promise<DevenvReleaseRecording>;
}

const defaultShell: DevenvCommandShell = {
  run,
  async remove(path) {
    await rm(path, { recursive: true, force: true });
  },
  recordRelease: recordDevenvRelease,
};

export async function updateDevenv(root: string, shell: DevenvCommandShell = defaultShell): Promise<void> {
  await shell.run('devenv', ['update'], direnvRoot(root));
  // The update may have moved the lock: record the verdict of the rev it locks now, asking nothing when it is recorded.
  await recordDevenvReleaseVerdict(root, shell);
}

/**
 * Records the release verdict of the devenv rev devenv.lock locks, asking its remote unless the record already holds
 * it. A rev that is no release, or a remote that cannot be listed, throws: nothing was recorded, and `smoo monorepo
 * check` fails until it is.
 */
export async function recordDevenvReleaseVerdict(
  root: string,
  shell: DevenvCommandShell = defaultShell,
): Promise<void> {
  const recording = await shell.recordRelease(root);
  if (recording.kind === 'refused') {
    throw new Error(recording.message);
  }
  const row = recordingResult(recording);
  if (row !== null) {
    printResults([row]);
  }
}

export async function reloadDevenv(root: string, shell: DevenvCommandShell = defaultShell): Promise<void> {
  const cwd = direnvRoot(root);
  await shell.remove(join(cwd, '.direnv'));
  await shell.remove(join(cwd, '.devenv'));
  await shell.run('direnv', ['reload'], cwd);
}

export async function updateNixpkgsOverlay(root: string, shell: DevenvCommandShell = defaultShell): Promise<void> {
  await shell.run('nix', ['shell', 'nixpkgs#nvfetcher', '-c', 'nvfetcher', '-o', '_sources'], nixpkgsOverlayRoot(root));
}

export function direnvRoot(root: string): string {
  return join(root, 'tooling', 'direnv');
}

export function nixpkgsOverlayRoot(root: string): string {
  return join(direnvRoot(root), 'nixpkgs-overlay');
}
