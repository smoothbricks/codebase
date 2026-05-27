import { printCommandOutput, run, runResult, runStatus } from '../lib/run.js';

export interface PackageHygieneShell {
  run(command: string, args: string[], cwd: string): Promise<void>;
  runResult?(
    command: string,
    args: string[],
    cwd: string,
  ): Promise<{
    exitCode: number;
    stdout: string;
    stderr: string;
  }>;
  runStatus(command: string, args: string[], cwd: string, quiet?: boolean): Promise<number>;
}

const defaultShell: PackageHygieneShell = { run, runResult, runStatus };

export async function fixPackageHygiene(
  root: string,
  verboseOrShell: boolean | PackageHygieneShell = false,
  maybeShell: PackageHygieneShell = defaultShell,
): Promise<void> {
  const verbose = typeof verboseOrShell === 'boolean' ? verboseOrShell : false;
  const shell = typeof verboseOrShell === 'boolean' ? maybeShell : verboseOrShell;
  const result = await runPackageHygieneCommand(shell, 'sherif', ['-f', '--select', 'highest'], root, verbose);
  const status = result.exitCode;
  if (status !== 0) {
    if (!verbose) {
      printCommandOutput(result.stdout, result.stderr);
    }
    throw new Error(`sherif -f --select highest failed with exit code ${status}`);
  }
}

export async function validatePackageHygiene(
  root: string,
  verboseOrShell: boolean | PackageHygieneShell = false,
  maybeShell: PackageHygieneShell = defaultShell,
): Promise<number> {
  const verbose = typeof verboseOrShell === 'boolean' ? verboseOrShell : false;
  const shell = typeof verboseOrShell === 'boolean' ? maybeShell : verboseOrShell;
  const result = await runPackageHygieneCommand(shell, 'sherif', ['--fail-on-warnings'], root, verbose);
  const status = result.exitCode;
  if (status !== 0) {
    if (!verbose) {
      printCommandOutput(result.stdout, result.stderr);
    }
    console.error('sherif package hygiene validation failed');
    return 1;
  }
  return 0;
}

async function runPackageHygieneCommand(
  shell: PackageHygieneShell,
  command: string,
  args: string[],
  root: string,
  verbose: boolean,
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  if (!verbose && shell.runResult) {
    return shell.runResult(command, args, root);
  }
  return { exitCode: await shell.runStatus(command, args, root, !verbose), stdout: '', stderr: '' };
}
