import { run, runStatus } from '../lib/run.js';

export interface PackageHygieneShell {
  run(command: string, args: string[], cwd: string): Promise<void>;
  runStatus(command: string, args: string[], cwd: string, quiet?: boolean): Promise<number>;
}

const defaultShell: PackageHygieneShell = { run, runStatus };

export async function fixPackageHygiene(
  root: string,
  verboseOrShell: boolean | PackageHygieneShell = false,
  maybeShell: PackageHygieneShell = defaultShell,
): Promise<void> {
  const verbose = typeof verboseOrShell === 'boolean' ? verboseOrShell : false;
  const shell = typeof verboseOrShell === 'boolean' ? maybeShell : verboseOrShell;
  const status = await shell.runStatus('sherif', ['-f', '--select', 'highest'], root, !verbose);
  if (status !== 0) {
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
  const status = await shell.runStatus('sherif', ['--fail-on-warnings'], root, !verbose);
  if (status !== 0) {
    console.error('sherif package hygiene validation failed');
    return 1;
  }
  return 0;
}
