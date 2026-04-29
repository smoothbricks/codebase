import { describe, expect, it } from 'bun:test';
import type { DevenvCommandShell } from './index.js';
import { reloadDevenv, updateDevenv, updateNixpkgsOverlay } from './index.js';

describe('devenv commands', () => {
  it('runs devenv update in tooling/direnv', async () => {
    const shell = new RecordingShell();

    await updateDevenv('/repo', shell);

    expect(shell.commands).toEqual([{ command: 'devenv', args: ['update'], cwd: '/repo/tooling/direnv' }]);
  });

  it('clears cached devenv directories before reloading direnv', async () => {
    const shell = new RecordingShell();

    await reloadDevenv('/repo', shell);

    expect(shell.removals).toEqual(['/repo/tooling/direnv/.direnv', '/repo/tooling/direnv/.devenv']);
    expect(shell.commands).toEqual([{ command: 'direnv', args: ['reload'], cwd: '/repo/tooling/direnv' }]);
  });

  it('runs nvfetcher through nix shell in the overlay directory', async () => {
    const shell = new RecordingShell();

    await updateNixpkgsOverlay('/repo', shell);

    expect(shell.commands).toEqual([
      {
        command: 'nix',
        args: ['shell', 'nixpkgs#nvfetcher', '-c', 'nvfetcher', '-o', '_sources'],
        cwd: '/repo/tooling/direnv/nixpkgs-overlay',
      },
    ]);
  });
});

class RecordingShell implements DevenvCommandShell {
  readonly commands: { command: string; args: string[]; cwd: string }[] = [];
  readonly removals: string[] = [];

  async run(command: string, args: string[], cwd: string): Promise<void> {
    this.commands.push({ command, args, cwd });
  }

  async remove(path: string): Promise<void> {
    this.removals.push(path);
  }
}
