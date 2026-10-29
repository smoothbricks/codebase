/**
 * The process the Nx hook's automatic retention pass runs in (`startRetentionPass`): this package's published Node CLI,
 * `--apply --automatic`, started detached, so neither the caller's thread nor its exit waits on the pass. Node only, like
 * `locks-node.ts`; the compiled CLI never starts one.
 *
 * Its standard streams are `/dev/null`: an inherited pipe would keep whoever reads the run's output waiting until the
 * pass ends, the wait this exists to remove. It records its outcome in the target's receipt instead. It shares no other
 * descriptor with the caller and takes every lock itself. `node` is run by its PATH name, as the CLI's shebang runs it.
 */
import { type ChildProcess, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import type { PassLauncher } from './cargo-retention.js';
import { describeCause, type Result } from './errors.js';

/** The published CLI (`package.json#bin`), emitted beside this module. */
const CLI = fileURLToPath(new URL('./node-cli.js', import.meta.url));

export const launchNodeCli: PassLauncher = async (workspaceRoot) => {
  let child: ChildProcess;
  try {
    child = spawn('node', [CLI, '--apply', '--automatic', workspaceRoot], { detached: true, stdio: 'ignore' });
  } catch (error) {
    // Node throws, not emits 'error', for exec failures it does not class as run-time, such as EPERM from a sandbox.
    return { ok: false, error: `cannot run node: ${describeCause(error)}` };
  }
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: Node emits 'spawn' once the exec has run or 'error' once it could not, one of them for every child; nothing here waits on the pass itself
  const started = await new Promise<Result<number, string>>((resolve) => {
    child.once('error', (error) => resolve({ ok: false, error: `cannot run node: ${error.message}` }));
    child.once('spawn', () =>
      resolve(
        child.pid === undefined ? { ok: false, error: 'node ran without a pid' } : { ok: true, value: child.pid },
      ),
    );
  });
  // The caller's event loop never waits for the pass's exit.
  child.unref();
  return started;
};
