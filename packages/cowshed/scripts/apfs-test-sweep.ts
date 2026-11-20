/// <reference types="bun" />

import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

/** The real-image lane setup sweeps dead runs with the checkout's declared executable. */
async function main(): Promise<number> {
  if (process.platform !== 'darwin') return 0;
  const executable = fileURLToPath(new URL('../.cache/apfs-test-sweep/cowshed-test-sweep', import.meta.url));
  const sweep = Bun.spawn([executable, randomUUID()], {
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  });
  return await sweep.exited;
}

if (import.meta.main) {
  try {
    process.exitCode = await main();
  } catch (error) {
    console.error(`APFS lane setup refused: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
