import { runStatus } from '../lib/run.js';

export async function fixNxSync(root: string, verbose = false): Promise<void> {
  const status = await runStatus('nx', ['sync'], root, !verbose);
  if (status !== 0) {
    throw new Error(`nx sync failed with exit code ${status}`);
  }
}

export async function validateNxSync(root: string, verbose = false): Promise<number> {
  const status = await runStatus('nx', ['sync:check'], root, !verbose);
  return status === 0 ? 0 : 1;
}
