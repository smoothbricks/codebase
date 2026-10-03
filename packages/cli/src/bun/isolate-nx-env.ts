// Fixture Nx processes must own their workspace root, graph, cache, and socket locations.
// Inherited CI paths can corrupt the outer graph or name another job's sockets.
// An inherited NX_DAEMON is dropped too, so Nx's own defaults decide whether a
// fixture runs a daemon; every fixture that starts one stops it before its
// root is deleted (`stopFixtureNxDaemon`).
delete process.env.NX_CACHE_DIRECTORY;
delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
delete process.env.NX_WORKSPACE_ROOT_PATH;
delete process.env.NX_SOCKET_DIR;
delete process.env.NX_DAEMON_SOCKET_DIR;
delete process.env.NX_DAEMON;
