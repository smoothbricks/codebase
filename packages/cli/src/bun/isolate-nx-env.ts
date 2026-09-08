// Fixture Nx processes must own their graph, cache, and socket locations.
// Inherited CI paths can corrupt the outer graph or name another job's sockets.
delete process.env.NX_CACHE_DIRECTORY;
delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
delete process.env.NX_SOCKET_DIR;
delete process.env.NX_DAEMON_SOCKET_DIR;
