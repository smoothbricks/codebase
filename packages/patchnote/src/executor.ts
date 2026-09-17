import { execa } from 'execa';
import typia from 'typia';
import type { CommandExecutor, ExecutorResult } from './types.js';

/**
 * Adapt Execa's overloaded API to the text-only command port. Missing captured
 * streams (inherit/ignore/buffer:false) are empty text. Reject known non-text
 * modes before spawning; validate custom stream results without casting them.
 */
export const executeCommand: CommandExecutor = async (file, args, options) => {
  if (options?.encoding === 'buffer' || options?.lines === true) {
    throw new TypeError('CommandExecutor requires text output; binary and line-array modes are unsupported');
  }
  const result = typia.assert<Partial<ExecutorResult>>(await execa(file, args, options));
  return { stdout: result.stdout ?? '', stderr: result.stderr ?? '', exitCode: result.exitCode };
};
