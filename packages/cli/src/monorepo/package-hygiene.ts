import { run, runStatus } from '../lib/run.js';

export async function fixPackageHygiene(root: string): Promise<void> {
  await run('sherif', ['--fix', '--select', 'highest'], root);
}

export async function validatePackageHygiene(root: string): Promise<number> {
  const status = await runStatus('sherif', ['--fail-on-warnings'], root);
  if (status !== 0) {
    console.error('sherif package hygiene validation failed');
    return 1;
  }
  return 0;
}
