import { run, runStatus } from '../lib/run.js';

export interface PackageHygieneShell {
  run(command: string, args: string[], cwd: string): Promise<void>;
  runStatus(command: string, args: string[], cwd: string): Promise<number>;
}

const defaultShell: PackageHygieneShell = { run, runStatus };

export async function fixPackageHygiene(root: string, shell: PackageHygieneShell = defaultShell): Promise<void> {
  await shell.run('sherif', ['-f', '--select', 'highest'], root);
}

export async function validatePackageHygiene(root: string, shell: PackageHygieneShell = defaultShell): Promise<number> {
  const status = await shell.runStatus('sherif', ['--fail-on-warnings'], root);
  if (status !== 0) {
    console.error('sherif package hygiene validation failed');
    return 1;
  }
  return 0;
}
