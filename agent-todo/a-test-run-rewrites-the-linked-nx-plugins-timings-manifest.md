# A test run rewrites the linked nx-plugin's timings manifest and restarts every consumer's Nx daemon

Found 2026-11-26 while consumer workspaces ran long Nx targets. Owner: nx-plugin.

A consumer workspace's run died with:

```
The Nx daemon (pid …) stopped: LINKED_PLUGIN_CHANGED (linked plugin file
<checkout>/packages/nx-plugin/test-timings.json changed)
```

A consumer workspace links `@smoothbricks/nx-plugin` from the SmoothBricks checkout, and Nx restarts the daemon of every
workspace that links the plugin when any file under it changes. `packages/nx-plugin/test-timings.json` is rewritten by
test runs in the SmoothBricks checkout, so a test run there restarts the daemon, and drops the running command's project
graph, in every consumer workspace at once. Long runs (a land gate, a 45-minute test target) die on it.

## What must hold

- Nothing a test run writes lives under the plugin's linked package files. Timings a test run measures go to a cache or
  output location that the plugin link does not cover, and the checked-in manifest changes only by a commit.

## Red

A test run in the plugin's own checkout leaves every file of the linked package unchanged (compare hashes before and
after).
