import { run, runStatus } from '../lib/run.js';

export async function fixNxSync(root: string): Promise<void> {
  await run('nx', ['sync'], root);
}

export async function validateNxSync(root: string): Promise<number> {
  const status = await runStatus('nx', ['sync:check'], root);
  return status === 0 ? 0 : 1;
}
