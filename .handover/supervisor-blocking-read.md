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

## Repair

The keeper owns one blocking probe task, polled as a branch of the supervisor actor's select. Other actor events leave
that task intact: they never cancel, duplicate or fabricate its result. Completion is judged against the current ledger
and idle clock; a result for a replaced Nx root is discarded. A lifecycle census uses the same probe before ending a
keeper start, and a retirement transfers those waiters to the ordinary job conclusion. While the census fence stands,
the pure keeper decision holds both starts and idle stops, without advancing the idle clock; only a new job or session
resumes maintenance. No silence bound, heartbeat authority or retry policy changes.

This follows specs/cowshed/04_sandbox.md, **devenv / Nix inside the sandbox** (Nx keeper), and
specs/cowshed/05_gateway.md, **Deadlines**, and specs/cowshed/02_workspaces.md, **Holders cowshed does not run**. Proof
placement follows specs/cowshed/08_testing.md, **Unit tests**. The table injects completion times with and without the
census fence. The single shell wiring check observes a real live daemon, holds its next record read while advancing an
injected clock past idle, gets an actor-authority round trip and census acknowledgement while that read is pending, then
releases the live result and proves the fixture-owned daemon remains running. No named session or job renews its idle
bound during that interleaving.

## Later incident: evidence limits

A later supervisor of the same installed build, PID 34175, failed a wait call after 15.0025485 s without progress. That
is not a startup hello failure: the job had already been admitted. The canonical trace reader's errors/top/open queries
over the UTC 20:30–20:40 window on 2026-10-26 found the workspace's job 132 admitted at 20:36:56.452035 and ending in
error at 20:37:50.536054; job 133 was also admitted. The job's process trace shows bun nx reset --onlyDaemon (PID 8519)
starting at 20:37:01.685158, running for 47.979 s and ending on SIGTERM; the later kept-daemon script was not reached.
No retained wait segment names peer PID 34175. A later two-second process sample found that actor mostly in its event
wait, with some artifact writes; it did not capture the original stall. Therefore the exact syscall in this later
incident is not proven. The earlier sample above proves the actor-blocking read defect this repair removes; it does not
prove that every later silence had the same syscall.
