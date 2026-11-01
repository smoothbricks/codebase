// One version-sensitive boundary for the checkout's own Nx cache (05_gateway.md, "Publication").
// DbCache and its native cache field are INTERNAL Nx APIs, not a public stable adapter: each Nx version
// it was read against is declared in openNxCache with the layout it relies on, and any other is refused.
// biome-ignore lint/correctness/noUnusedVariables: invoked by the bodies cache_adapter::script prefixes (cache_adapter.rs), not from this file.
function openNxCache(root) {
  const path = require('node:path');
  const { createRequire } = require('node:module');
  if (!root || !path.isAbsolute(root)) {
    throw new Error(`Nx cache adapter requires an absolute checkout root, got ${JSON.stringify(root)}`);
  }
  // What each qualified Nx version fixes for this adapter, read in nrwl/nx at that tag: the checkout's
  // task-database and cache directories (`utils/cache-directory.ts`). In both, the task database is
  // `<machine>-v<DB_VERSION>.db` there (v3 in 23.2.1, v6 in 24.0.3: `native/db/mod.rs`, `initialize.rs`),
  // reached only through Nx's own `NxCache.get`; an entry is `<cache>/<hash>/` with its terminal output in
  // `<cache>/terminalOutputs/<hash>` (`native/cache/cache.rs`); `CachedResult` is `{ code, terminalOutput,
  // outputsPath, size }`; and `HttpRemoteCache.store` archives the entry alike (`native/cache/http_remote_cache.rs`,
  // the same source). 23.2.1's `get` answers a row whose entry directory is gone; 24.0.3's does not.
  const layouts = new Map([
    ['23.2.1', { workspaceData: '.nx/workspace-data', cache: '.nx/cache' }],
    ['24.0.3', { workspaceData: '.nx/workspace-data', cache: '.nx/cache' }],
  ]);
  const requireNx = createRequire(path.join(root, 'package.json'));
  const version = requireNx('nx/package.json').version;
  const layout = layouts.get(version);
  if (!layout) {
    throw new Error(
      `Nx cache adapter supports exactly Nx ${[...layouts.keys()].join(' or ')}, not ${version}; requalify the adapter against it`,
    );
  }
  const workspaceData = path.join(root, layout.workspaceData);
  const cacheDir = path.join(root, layout.cache);
  // Nx fixes these paths at module load. Every root is handled in its own existing job process.
  process.env.NX_WORKSPACE_ROOT_PATH = root;
  process.env.NX_WORKSPACE_DATA_DIRECTORY = workspaceData;
  process.env.NX_CACHE_DIRECTORY = cacheDir;
  if (requireNx('nx/src/native').IS_WASM) {
    throw new Error('The Nx cache adapter requires the pinned native database cache, not WASM');
  }
  // Nx resolves its workspace-data directory itself; prove it is this checkout's before any database is opened.
  const { sharedDataDirectory } = requireNx('nx/src/utils/cache-directory');
  const actualWorkspaceData = sharedDataDirectory(root, 'workspace-data');
  if (path.resolve(actualWorkspaceData) !== path.resolve(workspaceData)) {
    throw new Error(
      `Nx ${version} workspace-data directory changed: expected ${workspaceData}, actual ${actualWorkspaceData}`,
    );
  }
  const { DbCache } = requireNx('nx/src/tasks-runner/cache');
  // Construction, not init(): Nx owns all schema/metadata setup; no remote loader is contacted.
  const cache = new DbCache({ nxCloudRemoteCache: null, skipRemoteCache: true });
  const native = cache.cache;
  if (!native || typeof native.get !== 'function' || typeof native.cacheDirectory !== 'string') {
    throw new Error(`Nx ${version} DbCache native cache surface changed; requalify the adapter`);
  }
  if (path.resolve(native.cacheDirectory) !== path.resolve(cacheDir)) {
    throw new Error(`Nx ${version} cache directory changed: expected ${cacheDir}, actual ${native.cacheDirectory}`);
  }
  return { requireNx, cache, cacheDir: native.cacheDirectory, getLocal: (hash) => native.get(hash) };
}
