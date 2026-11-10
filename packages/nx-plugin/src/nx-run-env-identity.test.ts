import { expect, it } from 'bun:test';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import type * as NxDaemonEnvironment from 'nx/src/daemon/client/daemon-environment';
import { RESERVATIONS_ENV, RESERVATIONS_UNSERVED_ENV } from './executors/bounded-exec/mbx-reservations.js';

/**
 * Guards the run-endpoint hunk of `patches/nx@24.0.3.patch` in `daemon-environment.js` (`patches/README.md`, "A run's
 * reservation endpoint is no graph identity"). This package's `preTasksExecution` publishes the run's mbx reservation
 * endpoint in the environment every task inherits: `MBX_RESERVATIONS_SOCKET`, a fresh socket directory per run, or
 * `BOUNDED_EXEC_RESERVATIONS_UNSERVED` when the endpoint did not start (`executors/bounded-exec/mbx-reservations.ts`).
 * Stock 24.0.3 counts both as project-graph identity, so the first request that carried a run's endpoint discarded the
 * daemon's graph and computed it again: 6.1 to 6.7 s of a consumer's dev boot ("Graph recompute necessary due to env
 * variable refresh. Changed keys: MBX_RESERVATIONS_SOCKET"). The daemon still receives the endpoint; only the graph
 * identity leaves it out.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const workspaceRequire = createRequire(join(repositoryRoot, 'package.json'));
const environment: typeof NxDaemonEnvironment = workspaceRequire('nx/src/daemon/client/daemon-environment');

const graph = (env: NodeJS.ProcessEnv) => environment.normalizeDaemonEnvironmentForGraph(env, 'darwin');
const shell: NodeJS.ProcessEnv = { PATH: '/usr/bin:/bin', HOME: '/home/u' };

//#region smoo!n/nx-run-endpoint-graph-identity
it("leaves a run's reservation socket out of the graph identity, whatever its path", () => {
  const without = graph(shell);
  expect(graph({ ...shell, [RESERVATIONS_ENV]: '/tmp/mbx-r-a1b2/r.sock' })).toEqual(without);
  expect(graph({ ...shell, [RESERVATIONS_ENV]: '/tmp/mbx-r-c3d4/r.sock' })).toEqual(without);
});

it("leaves the reason a run's endpoint did not start out of the graph identity too", () => {
  expect(graph({ ...shell, [RESERVATIONS_UNSERVED_ENV]: 'mbx reservations could not run: ENOENT' })).toEqual(
    graph(shell),
  );
});

it('still hands the daemon the endpoint: only the graph identity leaves it out', () => {
  const saved = process.env[RESERVATIONS_ENV];
  process.env[RESERVATIONS_ENV] = '/tmp/mbx-r-e5f6/r.sock';
  try {
    expect(environment.getDaemonEnv()[RESERVATIONS_ENV]).toBe('/tmp/mbx-r-e5f6/r.sock');
  } finally {
    if (saved === undefined) delete process.env[RESERVATIONS_ENV];
    else process.env[RESERVATIONS_ENV] = saved;
  }
});

it('keeps any other variable a client sets as graph identity', () => {
  expect(graph({ ...shell, JAVA_HOME: '/opt/jdk-21' })).not.toEqual(graph(shell));
});
//#endregion smoo!n/nx-run-endpoint-graph-identity
