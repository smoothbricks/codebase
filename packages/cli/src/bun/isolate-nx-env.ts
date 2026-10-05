// Fixture Nx processes must own their workspace root, graph, cache, and socket locations.
// Inherited CI paths can corrupt the outer graph or name another job's sockets.
// Every Nx a test starts — `runFixtureNx`, or our own code shelling out to `nx` or
// loading a fixture's graph — runs without a daemon: a daemonless Nx leaves nothing
// running once it exits. A test whose subject is the daemon asks for one by name
// (`runFixtureNx(root, args, { daemon: true })`, or `NX_DAEMON=true` in the child's
// environment), and its fixture stops that daemon before its root is deleted.
delete process.env.NX_CACHE_DIRECTORY;
delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
delete process.env.NX_WORKSPACE_ROOT_PATH;
delete process.env.NX_SOCKET_DIR;
delete process.env.NX_DAEMON_SOCKET_DIR;
process.env.NX_DAEMON = 'false';
// As in this repository's shell: otherwise a fixture daemon asked for Nx Console's status
// pulls `nx@latest` from the registry in an `npm` child working in the fixture root.
process.env.NX_USE_LOCAL = 'true';
