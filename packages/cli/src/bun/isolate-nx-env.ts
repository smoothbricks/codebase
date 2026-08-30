/**
 * CI exports NX_CACHE_DIRECTORY and NX_WORKSPACE_DATA_DIRECTORY to a per-lane
 * tree shared by every task in the run. cli tests spawn fixture `nx` through
 * `run()` / `githubCiNxRunMany`, which inherit this process env. Writing the
 * fixture's one-project graph into that shared tree is what made a concurrent
 * `nx run-many -t test` fail with "Could not find project cowshed".
 *
 * Drop both here, at bun-test preload, so every fixture nx uses its own `.nx`
 * under the fixture root. The parent orchestrator keeps the CI directories.
 */
delete process.env.NX_CACHE_DIRECTORY;
delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
