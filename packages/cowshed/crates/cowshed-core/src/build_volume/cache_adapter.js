// One version-sensitive boundary for the checkout's own Nx cache (05_gateway.md, "Publication").
// DbCache and its native cache field are INTERNAL Nx 23.2.1 APIs, not a public stable adapter.
// biome-ignore lint/correctness/noUnusedVariables: invoked by the bodies cache_adapter::script prefixes (cache_adapter.rs), not from this file.
function openNxCache(root) {
  const path = require('node:path');
  const { createRequire } = require('node:module');
  if (!root || !path.isAbsolute(root)) {
    throw new Error(`Nx cache adapter requires an absolute checkout root, got ${JSON.stringify(root)}`);
  }
  const requireNx = createRequire(path.join(root, 'package.json'));
  const version = requireNx('nx/package.json').version;
  if (version !== '23.2.1') {
    throw new Error(
      `Nx cache adapter supports exactly Nx 23.2.1, not ${version}; requalify the adapter before upgrading`,
    );
  }
  // Nx fixes these paths at module load. Every root is handled in its own existing job process.
  process.env.NX_WORKSPACE_ROOT_PATH = root;
  process.env.NX_WORKSPACE_DATA_DIRECTORY = path.join(root, '.nx', 'workspace-data');
  process.env.NX_CACHE_DIRECTORY = path.join(root, '.nx', 'cache');
  if (requireNx('nx/src/native').IS_WASM) {
    throw new Error('The Nx cache adapter requires the pinned native database cache, not WASM');
  }
  // Nx resolves its workspace-data directory itself; prove it is this checkout's before any database is opened.
  const { sharedDataDirectory } = requireNx('nx/src/utils/cache-directory');
  const workspaceData = path.join(root, '.nx', 'workspace-data');
  const actualWorkspaceData = sharedDataDirectory(root, 'workspace-data');
  if (path.resolve(actualWorkspaceData) !== path.resolve(workspaceData)) {
    throw new Error(
      `Nx 23.2.1 workspace-data directory changed: expected ${workspaceData}, actual ${actualWorkspaceData}`,
    );
  }
  const { DbCache } = requireNx('nx/src/tasks-runner/cache');
  // Construction, not init(): Nx owns all schema/metadata setup; no remote loader is contacted.
  const cache = new DbCache({ nxCloudRemoteCache: null, skipRemoteCache: true });
  const native = cache.cache;
  if (!native || typeof native.get !== 'function' || typeof native.cacheDirectory !== 'string') {
    throw new Error('Nx 23.2.1 DbCache native cache surface changed; requalify the adapter');
  }
  const expectedCacheDir = path.join(root, '.nx', 'cache');
  if (path.resolve(native.cacheDirectory) !== path.resolve(expectedCacheDir)) {
    throw new Error(`Nx 23.2.1 cache directory changed: expected ${expectedCacheDir}, actual ${native.cacheDirectory}`);
  }
  return { requireNx, cache, cacheDir: native.cacheDirectory, getLocal: (hash) => native.get(hash) };
}
