import { afterEach, describe, expect, it, spyOn } from 'bun:test';
import { fixPackageHygiene, type PackageHygieneShell, validatePackageHygiene } from './package-hygiene.js';

describe('package hygiene', () => {
  afterEach(() => {
    console.log = originalConsoleLog;
    console.error = originalConsoleError;
  });

  it('runs sherif in autofix mode and selects highest dependency versions', async () => {
    const shell = new RecordingShell();

    await fixPackageHygiene('/repo', shell);

    expect(shell.statuses).toEqual([{ command: 'sherif', args: ['-f', '--select', 'highest'], cwd: '/repo' }]);
  });

  it('validates sherif warnings as failures', async () => {
    const shell = new RecordingShell();

    await validatePackageHygiene('/repo', shell);

    expect(shell.statuses).toEqual([{ command: 'sherif', args: ['--fail-on-warnings'], cwd: '/repo' }]);
  });

  it('prints captured sherif output when quiet validation fails', async () => {
    const shell = new FailingResultShell('sherif stdout', 'sherif stderr');
    const logs = captureConsoleLogs();
    const errors = captureConsoleErrors();

    const failures = await validatePackageHygiene('/repo', shell);

    expect(failures).toBe(1);
    expect(logs).toEqual(['sherif stdout']);
    expect(errors).toEqual(['sherif stderr', 'sherif package hygiene validation failed']);
  });
});

const originalConsoleLog = console.log;
const originalConsoleError = console.error;

function captureConsoleLogs(): string[] {
  const logs: string[] = [];
  spyOn(console, 'log').mockImplementation((...args: unknown[]) => {
    logs.push(args.join(' '));
  });
  return logs;
}

function captureConsoleErrors(): string[] {
  const errors: string[] = [];
  spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    errors.push(args.join(' '));
  });
  return errors;
}

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

class FailingResultShell implements PackageHygieneShell {
  constructor(
    private readonly stdout: string,
    private readonly stderr: string,
  ) {}

  async run(): Promise<void> {}

  async runResult(): Promise<{ exitCode: number; stdout: string; stderr: string }> {
    return { exitCode: 1, stdout: this.stdout, stderr: this.stderr };
  }

  async runStatus(): Promise<number> {
    return 1;
  }
}
