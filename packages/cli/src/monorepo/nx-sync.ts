import { runStatus } from '../lib/run.js';

export async function validateNxSync(root: string): Promise<number> {
  const status = await runStatus('nx', ['sync:check'], root);
  return status === 0 ? 0 : 1;
}
