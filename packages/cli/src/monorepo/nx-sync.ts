import { printCommandOutput, runResult, runStatus } from '../lib/run.js';

export async function fixNxSync(root: string, verbose = false): Promise<void> {
  const result = verbose
    ? { exitCode: await runStatus('nx', ['sync'], root, false), stdout: '', stderr: '' }
    : await runResult('nx', ['sync'], root);
  const status = result.exitCode;
  if (status !== 0) {
    if (!verbose) {
      printCommandOutput(result.stdout, result.stderr);
    }
    throw new Error(`nx sync failed with exit code ${status}`);
  }
}

export async function validateNxSync(root: string, verbose = false): Promise<number> {
  const result = verbose
    ? { exitCode: await runStatus('nx', ['sync:check'], root, false), stdout: '', stderr: '' }
    : await runResult('nx', ['sync:check'], root);
  if (result.exitCode === 0) {
    return 0;
  }
  if (!verbose) {
    printCommandOutput(result.stdout, result.stderr);
  }
  return 1;
}
