/**
 * In-process plugin tests infer targets for temporary fixture workspaces. Nx's
 * daemon client answers file-index queries for the workspace this process
 * started in, never for the fixture root a query names, so a daemon-backed
 * glob would silently describe the wrong tree. With the daemon off, Nx builds
 * the index in process for the root each query names, as a CI run does.
 * Child Nx processes the tests spawn set NX_DAEMON themselves.
 */
process.env.NX_DAEMON = 'false';
