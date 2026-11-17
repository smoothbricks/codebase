import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { nxFixtureRoot } from './__tests__/fixture-nx-env.js';

/**
 * Guards the installed Nx patch's run-commands drain (`patches/README.md`, "A command's output is read to its end"):
 * a `nx:run-commands` task run without a terminal takes its result once the command's stdout and stderr have closed,
 * never at the command's 'exit', when its last chunks can still be unread. Upstream nrwl/nx 5d440a5fe6 (#37762).
 *
 * The command's last line is written by a process it leaves behind, released only once the command's own 'exit' was
 * emitted to the listeners Nx registered before this program's: whatever Nx joins in its 'exit' handler cannot hold
 * that line, and output read to the pipes' close does. No timing decides it. Both runtimes run it: Node, Nx's own,
 * and Bun, which a consumer workspace's Nx worker runs in.
 */
const nxRoot = join(import.meta.dir, '../../../node_modules/nx/dist/src');

const program = `
const fs = require('node:fs');
const { execFileSync } = require('node:child_process');
const { SeriallyRunningTasks } = require(${JSON.stringify(join(nxRoot, 'executors/run-commands/running-tasks.js'))});
execFileSync('mkfifo', ['gate']);
const runner = new SeriallyRunningTasks(
  {
    commands: [{ command: '(read go < gate; echo tail) & echo head' }],
    color: false,
    cwd: process.cwd(),
    env: {},
    streamOutput: false,
    usePty: false,
    readyWhenStatus: [],
    parallel: false,
  },
  { root: process.cwd() },
  false,
  { id: 'fixture:drain' },
  process.env,
);
const release = () => fs.writeFileSync('gate', 'go\\n');
// The runner takes the command's process right after spawning it, after Nx's own 'exit' listener is registered.
let current = null;
Object.defineProperty(runner, 'currentProcess', {
  get: () => current,
  set: (running) => {
    current = running;
    const child = running.childProcess;
    if (child.exitCode !== null || child.signalCode !== null) release();
    else child.once('exit', release);
  },
});
runner.getResults().then((result) => process.stdout.write(JSON.stringify(result)));
`;

for (const runtime of ['node', 'bun'] as const) {
  it(`reads a run-commands task's output to the close of its pipes, never only to its exit (${runtime})`, async () => {
    const root = await nxFixtureRoot('nx-run-commands-drain-');
    try {
      const child = spawnSync(runtime, ['-e', program], { cwd: root, encoding: 'utf8' });
      if (child.error) throw child.error;
      expect(child.status, child.stderr).toBe(0);
      const result: unknown = JSON.parse(child.stdout);
      expect(result).toMatchObject({ code: 0, terminalOutput: expect.stringContaining('head\ntail\n') });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
}
