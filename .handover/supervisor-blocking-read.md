# Workspace supervisor: blocking read inside the actor

## Contract

`specs/cowshed/05_gateway.md`, **Deadlines**, requires a round trip through the supervisor actor for each progress
heartbeat (every 5 s); a client fails `SILENT` after 15 s without one. Blocking filesystem work must not park that
actor.

## Observed evidence

- The publication-denylist invocation failed `SILENT` after 15.00176 s, naming supervisor PID 93211 and no actor
  progress. This was not a denylist finding.
- Sample `artifact://61248` captures `SupervisorActor::run → nx_daemon::read_and_probe → std::fs::read → read`
  throughout the sample. The installed supervisor was build
  `bacd3186a445052f1c103e0271176752ef87e33b90a4d38f99df931f5081a7c4`.
- The probed `<workspace>/.nx/workspace-data/d/server-process.json` was a 189-byte regular file (`-rw-r--r--`, inode
  7379314), on the workspace's APFS build volume (`/dev/disk90s1`). It was not a FIFO. The scoped descriptor census did
  not identify an open descriptor for this record; it does not establish why the kernel read stalled.
- Doctor separately reported healthy host/gateway/storage. That is not evidence that the supervisor actor answered or
  recovered. No process was signalled during this investigation.

## Disposition

The defect is the blocking file read on the actor loop. Move the probe's filesystem work off-loop, or give it a
progress-bounded owner that leaves the actor responsive; do not hide the terminal error or retry the same invocation as
a repair. Keep this repair out of the current integration batch. A future behavioral proof must stall the owned probe
while demonstrating that the actor still serves its protocol and reports the probe's actual outcome.
