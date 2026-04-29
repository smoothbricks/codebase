import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { run } from '../lib/run.js';

export interface DevenvCommandShell {
  run(command: string, args: string[], cwd: string): Promise<void>;
  remove(path: string): Promise<void>;
}

const defaultShell: DevenvCommandShell = {
  run,
  async remove(path) {
    await rm(path, { recursive: true, force: true });
  },
};

export async function updateDevenv(root: string, shell: DevenvCommandShell = defaultShell): Promise<void> {
  await shell.run('devenv', ['update'], direnvRoot(root));
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
