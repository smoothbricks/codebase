import { describe, expect, it } from 'bun:test';
import { fixPackageHygiene, type PackageHygieneShell, validatePackageHygiene } from './package-hygiene.js';

describe('package hygiene', () => {
  it('runs sherif in autofix mode and selects highest dependency versions', async () => {
    const shell = new RecordingShell();

    await fixPackageHygiene('/repo', shell);

    expect(shell.runs).toEqual([{ command: 'sherif', args: ['-f', '--select', 'highest'], cwd: '/repo' }]);
  });

  it('validates sherif warnings as failures', async () => {
    const shell = new RecordingShell();

    await validatePackageHygiene('/repo', shell);

    expect(shell.statuses).toEqual([{ command: 'sherif', args: ['--fail-on-warnings'], cwd: '/repo' }]);
  });
});

class RecordingShell implements PackageHygieneShell {
  readonly runs: { command: string; args: string[]; cwd: string }[] = [];
  readonly statuses: { command: string; args: string[]; cwd: string }[] = [];

  async run(command: string, args: string[], cwd: string): Promise<void> {
    this.runs.push({ command, args, cwd });
  }

  async runStatus(command: string, args: string[], cwd: string): Promise<number> {
    this.statuses.push({ command, args, cwd });
    return 0;
  }
}
