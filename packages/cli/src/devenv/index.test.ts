import { describe, expect, it, spyOn } from 'bun:test';
import type { DevenvReleaseRecording } from '../monorepo/devenv-release.js';
import type { DevenvCommandShell } from './index.js';
import { recordDevenvReleaseVerdict, reloadDevenv, updateDevenv, updateNixpkgsOverlay } from './index.js';

const REV = 'b904dcb51fe48c30db250038241507f60752f222';

describe('devenv commands', () => {
  it('runs devenv update in tooling/direnv, then records the verdict of the rev it locked', async () => {
    const shell = new RecordingShell();
    const rows = spyOn(console, 'log').mockImplementation(() => {});
    try {
      await updateDevenv('/repo', shell);

      expect(shell.commands).toEqual([{ command: 'devenv', args: ['update'], cwd: '/repo/tooling/direnv' }]);
      expect(shell.recorded).toEqual(['/repo']);
      // The verdict is for the rev the update locked, so it is asked after the lock moved, never before.
      expect(shell.events).toEqual(['run devenv update', 'record /repo']);
    } finally {
      rows.mockRestore();
    }
  });

  it('refuses a bump that locks no release: the update ran, nothing is recorded, and the command fails', async () => {
    const shell = new RecordingShell({
      kind: 'refused',
      message: 'tooling/direnv/devenv.lock locks devenv at a commit no release names',
    });

    await expect(updateDevenv('/repo', shell)).rejects.toThrow('locks devenv at a commit no release names');
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

describe('devenv release verdict command', () => {
  const logged = async (recording: DevenvReleaseRecording): Promise<string[]> => {
    const rows = spyOn(console, 'log').mockImplementation(() => {});
    try {
      await recordDevenvReleaseVerdict('/repo', new RecordingShell(recording));
      return rows.mock.calls.map((call) => String(call[0]));
    } finally {
      rows.mockRestore();
    }
  };

  it('prints the row of the verdict it recorded, or found current', async () => {
    const record = { remote: 'https://github.com/cachix/devenv', rev: REV, tag: 'v2.4.0' };
    expect(await logged({ kind: 'recorded', created: true, record })).toEqual([
      `created         tooling/direnv/devenv-release.json — v2.4.0 names ${REV}`,
    ]);
    expect(await logged({ kind: 'current', record })).toEqual([
      `unchanged       tooling/direnv/devenv-release.json — v2.4.0 names ${REV}`,
    ]);
  });

  it('prints nothing for a repository with no devenv, and fails with the reason when the rev is refused', async () => {
    expect(await logged({ kind: 'no-devenv' })).toEqual([]);
    await expect(
      recordDevenvReleaseVerdict('/repo', new RecordingShell({ kind: 'refused', message: 'no release names it' })),
    ).rejects.toThrow('no release names it');
  });
});

class RecordingShell implements DevenvCommandShell {
  readonly commands: { command: string; args: string[]; cwd: string }[] = [];
  readonly removals: string[] = [];
  readonly recorded: string[] = [];
  readonly events: string[] = [];

  constructor(private readonly recording: DevenvReleaseRecording = { kind: 'no-devenv' }) {}

  async run(command: string, args: string[], cwd: string): Promise<void> {
    this.commands.push({ command, args, cwd });
    this.events.push(`run ${command} ${args.join(' ')}`);
  }

  async recordRelease(root: string): Promise<DevenvReleaseRecording> {
    this.recorded.push(root);
    this.events.push(`record ${root}`);
    return this.recording;
  }

  async remove(path: string): Promise<void> {
    this.removals.push(path);
  }
}
