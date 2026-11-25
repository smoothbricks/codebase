# mbx probes every stale learned-state path on each start

Observed 2026-11-25 22:18 +04 under a vnode-saturation alert (`fs_usage -w -f pathname -t 3`). Two mbx processes, one
second apart, each made 8,678 `getattrlist` calls, one per distinct `/private/tmp/<test-prefix>-XXXXXX` path. Only 6
such directories exist. The rest are learned incremental state (or managed targets) recorded for scratch workspaces that
were deleted long ago. Every mbx start checks all of them again.

What must hold: a record whose path is gone is dropped the first time it's found gone, the way the land's "removed
learned incremental state for …" lines already do for a workspace. A start then pays for live records only. A stale
record must never survive a probe that found its path missing.

Measure before and after with the same `fs_usage` window: the number of distinct missing-path probes per mbx start,
which should be 0 after the first start.
