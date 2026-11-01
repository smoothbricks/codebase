// This process is the Nx of every fixture the suite infers in-process, and a fixture is its own checkout with its
// own Nx state (specs/cowshed/04_sandbox.md, "One Nx state per checkout"). The job running the suite (CI's
// setup-devenv action, a cowshed job) exports absolute locations of its checkout's state, the suite inherits them, and
// Nx resolves them for any root, so a fixture's file index walks through that checkout's data directory. Nx 24's
// native file index shares one walk among every context of one data directory: the walk holds `nx_files.lock` there,
// and a context that waits on it loads the archive the walk wrote (`nx_files_v2.nxt`) instead of walking its own
// root, so a fixture's context gets the files of whichever root walked there last. Without these variables every root
// resolves its own `.nx`, the cache with its task database. Every other inherited NX_* variable stays: fixtures place
// their sockets beside the job's NX_SOCKET_DIR, and the Nx processes they start take fixtureNxEnv.
delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
delete process.env.NX_PROJECT_GRAPH_CACHE_DIRECTORY;
delete process.env.NX_CACHE_DIRECTORY;
