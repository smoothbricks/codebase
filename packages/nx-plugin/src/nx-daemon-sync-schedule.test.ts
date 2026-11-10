import { afterEach, beforeEach, expect, it, jest, spyOn } from 'bun:test';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import type * as NxProjectGraphRecomputation from 'nx/src/daemon/server/project-graph-incremental-recomputation';
import type * as NxSyncGenerators from 'nx/src/daemon/server/sync-generators';

/**
 * Guards the sync-schedule hunk of `patches/nx@24.0.3.patch` in `daemon/server/sync-generators.js`
 * (`patches/README.md`, "Sync generators run when a client asks"). Stock 24.0.3's daemon runs every registered sync
 * generator 100 ms after each project-graph recomputation (`collectAndScheduleSyncGenerators`), ahead of any request
 * for its changes. A generator is synchronous work on the daemon's one event loop, so the client that caused the
 * recomputation waits for it: `managed-files` took 2.4 s of a consumer's cold daemon's first graph request, and no
 * client asked for its changes in any boot. The patched daemon keeps a stale generator scheduled and runs it when a
 * client asks (`getFromCacheOrRunGenerators`), against the graph current then.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const workspaceRequire = createRequire(join(repositoryRoot, 'package.json'));
const syncGenerators: typeof NxSyncGenerators = workspaceRequire('nx/src/daemon/server/sync-generators');
const recomputation: typeof NxProjectGraphRecomputation = workspaceRequire(
  'nx/src/daemon/server/project-graph-incremental-recomputation',
);

beforeEach(() => {
  jest.useFakeTimers();
});

afterEach(() => {
  jest.useRealTimers();
  jest.restoreAllMocks();
});

//#region smoo!n/nx-sync-generators-run-on-request
it('runs no generator after a graph recomputation: no client asked for its changes', () => {
  // A scheduled run's first step is to ask for the current graph; a run that never starts never asks.
  const graphRequests = spyOn(recomputation, 'getCachedSerializedProjectGraphPromise').mockRejectedValue(
    new Error('a scheduled sync run asked for the graph'),
  );
  syncGenerators.collectAndScheduleSyncGenerators({ nodes: {}, dependencies: {}, externalNodes: {} });
  // Every wait Nx's own scheduling uses, and more: the clock runs no generator.
  jest.advanceTimersByTime(60_000);
  expect(graphRequests).not.toHaveBeenCalled();
});

it('still collects the generators nx.json registers, for the client that asks for them', async () => {
  syncGenerators.collectAndScheduleSyncGenerators({ nodes: {}, dependencies: {}, externalNodes: {} });
  const registered = await syncGenerators.getCachedRegisteredSyncGenerators();
  expect(registered.globalGenerators).toContain('@smoothbricks/nx-plugin:managed-files');
});
//#endregion smoo!n/nx-sync-generators-run-on-request
