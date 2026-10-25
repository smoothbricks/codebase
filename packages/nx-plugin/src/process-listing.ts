/**
 * Running one listing of named processes, `ps -p` or `pgrep -P`, under a deadline. A listing waits on the kernel's view
 * of the processes it names; one of those caught in an uninterruptible kernel wait can hold the listing there with it.
 *
 * Node APIs only: this module is built with the library.
 */
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from './test-backstop.js';

const run = promisify(execFile);

/**
 * How long one listing may take: the leg's no-progress backstop (specs/cowshed/08_testing.md), so a listing never fails
 * a run the backstop would let finish, and one that hangs fails naming its command instead of leaving the leg to die
 * unexplained.
 */
const LISTING_DEADLINE_MS = BOUNDED_TEST_IDLE_TIMEOUT_MS;

/**
 * The output of `file args`, resolved through PATH, within {@link LISTING_DEADLINE_MS}: the deadline rejects at once,
 * whether or not the command it ends has exited yet. `ps -p` and `pgrep` both exit 1 with no output when no process
 * matches, which is an empty listing, not a failure.
 */
export async function listing(file: string, args: readonly string[]): Promise<string> {
  const command = `${file} ${args.join(' ')}`;
  const abort = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      abort.abort();
      reject(new Error(`${command} did not answer within ${LISTING_DEADLINE_MS}ms`));
    }, LISTING_DEADLINE_MS);
  });
  try {
    const listed = run(file, args, { signal: abort.signal, maxBuffer: 16 * 1024 * 1024 });
    return (await Promise.race([listed, deadline])).stdout;
  } catch (error) {
    if (
      error instanceof Error &&
      'code' in error &&
      error.code === 1 &&
      'stdout' in error &&
      'stderr' in error &&
      String(error.stdout) === '' &&
      String(error.stderr) === ''
    ) {
      return '';
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
