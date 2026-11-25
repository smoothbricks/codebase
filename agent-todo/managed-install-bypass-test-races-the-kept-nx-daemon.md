# The managed-install absolute-bypass test passes the bypass chain intermittently

`cowshed-cli` `command_dispatch`
`macos::directory_managed_install_restores_developer_links_before_nx_and_cleans_absolute_bypass_substrate_lifecycle`
(tests/command_dispatch/macos.rs:9256) failed once and passed once on the same tree (spawn-off-runtime at 19927f2f77,
2026-11-25):

- **Failed** in `cargo-test-cowshed-cli-exceptions-shard5` of the second consumer run, at host load ~80, with
  `assertion left != right failed: absolute bypass is not chained-command coverage, left: 0, right: 0`. The chain
  `<bun> install --ignore-scripts && node -e '<guard>' && node_modules/.bin/nx show projects --json`, which runs bun by
  absolute path past the developer-link restorer, exited 0, when the test expects it to fail.
- **Passed** in the first consumer run's same shard, and alone on the host lane in 98.6 s.

The log of the failing run shows the workspace's Nx daemon kept by its supervisor across the earlier exec ("keeps the Nx
daemon of …/links: its supervisor runs job 4"). [INFERENCE] `nx show projects` may then be answered by that live daemon
from the graph it computed before the bypass replaced `node_modules/nx`, so the chain's last step succeeds whether or
not the replaced package works. The exit status would then depend on whether the daemon is still alive when the bypass
runs.

Owed:

- Find the actual mechanism: record which `nx` answered (daemon or in-process) in the failing case, for example with
  `NX_DAEMON=false` in the bypass chain or the daemon's own log, before changing anything.
- Then fix the test so it asserts the bypass outcome by the event that decides it, not by a race with the kept daemon.
- A red that fails deterministically on today's code comes first. One way to get it: start the bypass chain while the
  kept daemon is known to be live.
