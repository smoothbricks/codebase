# Recurring-work optimization ledger

Measurements are scoped to the named operation, not total command latency. The runs below use macOS arm64, Cargo's
release profile (opt-level 3 and LTO in this repository), background scheduling, two build jobs, warm inputs, and
interleaved control/candidate arms in the same binary on a co-resident host.

## Candidate order

Paths below are relative to `packages/cowshed/crates/`; line numbers name the surveyed source before each unit. The
order uses measured recurring cost where available. Unmeasured costs are not claimed speedups.

| Rank | Crate and site                                                                               | Frequency and evidence                                                                                                                                                                                              | Byproduct level and disposition                                                                                                                                             |
| ---- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `cowshed-core/src/developer_links.rs:410`, terminal restoration                              | Every job with captured developer links; 47 links in 26 parents cost 10.128 ms of planning per job before the change                                                                                                | L2: reuse the captured path order to group adjacent siblings; L1: one verified parent handle serves all siblings. No retained cache or second store.                        |
| 2    | `cowshed-cli/src/resident.rs:154` → `cowshed-gateway/src/actor.rs:2627`, revision membership | One full status census per eligible resident admission. A pre-change live host observation had 48 sessions, 9,104 response bytes and 123.039 µs mean over 1,000 requests, including Python socket/decode overhead   | L1: the installed session map already answers membership. A point query can reuse that source without a parallel revision registry.                                         |
| 3    | `cowshed-core/src/runtime/supervisor.rs:4687`, group recording                               | Spawn and seal scan every retained job to select groups still requiring recovery. Related completion/liveness scans at 6405 and 6654 run after every actor event. Cost grows with historical jobs; not yet measured | L1 candidate: move terminal records out of the live traversal, without copying them into another inventory. A separately maintained recovery index is not the proposed fix. |
| 4    | `cowshed-core/src/runtime/commitment_feed.rs:91`, cursor paging                              | A filtered retained-prefix scan, bounded at 65,536 entries. The sole production forwarder acknowledges each page before the next read (`runtime/project.rs:8233–8260`), normally removing the prefix entirely       | L2: contiguous cursor arithmetic could seek directly, but the recurring production prefix is normally empty. No measured hot-path win; no change.                           |

Fresh admission discovery in `developer_links.rs:200` and `capabilities/build_state_discovery.rs:825` remains
authoritative for tracked manifests and installed links. A retained directory census would be a **duplicate inventory /
parallel registry** and miss installs, relinks, or index changes; it is rejected, not treated as a cache trade-off. An
unchanged admission capture measured 96.998 ms median over nine runs; that work is not included in the terminal-planning
saving below.

Job spans (`runtime/job_spans.rs:389`) and commitments (`storage/audit.rs:430`) seal durable segments in order. Batching
them changes publication/durability and health-accounting boundaries; no such change is made. Store-owner discovery
(`storage/bootstrap.rs:125`) is not a per-job warm path. The full span-directory census in
`tests/workspace_supervisor.rs:7305` is a test reader, not a production admission operation. Sandbox profiles already
reuse their rendered policy; executor planning already consumes that policy rather than rendering another copy.

## Terminal link planning: one parent traversal per sibling run

Accepted in `47f3c836c`. Production enters through `finish_terminal_restore` → `DeveloperLinks::restore` → `plan_links`.
The captured, sorted link paths are the source of sibling runs. Each run opens and verifies its parent once, then reads
every leaf through that handle. The handle and path buffer die with the plan; nothing is cached across jobs or retained
beside the filesystem. Decisions are still all completed before any mutation.

The retained `plan_per_link` test oracle runs the former per-link traversal. For 1,000 calls of each arm:

| Input                               | Before / job (ms) | After / job (ms) | Before / 1,000 (ms) | After / 1,000 (ms) |
| ----------------------------------- | ----------------: | ---------------: | ------------------: | -----------------: |
| Real checkout: 47 links, 26 parents |         10.128265 |         5.679204 |           10128.265 |           5679.204 |
| One sibling                         |          0.085285 |         0.084639 |              85.285 |             84.639 |
| Two siblings                        |          0.193674 |         0.099926 |             193.674 |             99.926 |
| Eight siblings                      |          0.733199 |         0.100800 |             733.199 |            100.800 |
| 64 siblings                         |          5.557357 |         0.192894 |            5557.357 |            192.894 |
| 64 distinct parents                 |          5.589021 |         5.591227 |            5589.021 |           5591.227 |

The real-input saving is **4.449062 ms/job, 4.449062 s/1,000 jobs (43.93%)**. Its medians were 9.419083 → 5.366666 ms;
measured syscall counts fell from 1,542 to 879 per plan, including the same counter overhead in both arms. The crossover
is two siblings. Distinct-parent input is flat (+0.0395%). An empty direct planner call costs an extra 65 ns, but
production restoration already returns before calling the planner for an empty snapshot.

Guarantees exercised: all leaf classes in all three restoration modes; current filesystem changes between calls;
symlink/non-directory parent refusal; and refusal before any mutation. The 22 developer-link tests, including the
ignored benchmark, and 10 supervisor install-observation tests passed. This is focused evidence, not a Linux run or a
replacement for the repository landing gate.

Reproduce from an allowed repository development shell:

```sh
direnv exec . cargo nextest run --cargo-profile release -p cowshed-core --lib \
  -E 'test(developer_links::)' --run-ignored all --no-capture --build-jobs 2
direnv exec . cargo nextest run --cargo-profile release -p cowshed-core --lib \
  -E 'test(runtime::supervisor::install_observation_tests::)' --no-capture --build-jobs 2
```

## Resident admission: query one installed revision

Production enters through `cowshed-cli/src/resident.rs::gateway_current` → `ControlSocket::session_current` →
`GatewayControlClient::session_current` → the gateway actor's `SessionCurrent` command. The actor looks up the existing
installed-session `HashMap` and compares the revision; it does not construct, sort, serialize, decode, or scan the full
diagnostic status. This is **L1**: the installed table serves another consumer, with no new index, cache, durable bytes,
or parallel registry. It remains an observation of what is installed, not grant authority and not a reservation against
later rotation.

The ignored `session_current_benchmark` exercises the real actor and Unix control socket with the former
`client.status().sessions.any(...)` as its control arm. Each cell alternates 1,000 calls per arm after 20 warmups, with
101-byte workspace identities shaped like production IDs. Setup, session installation and shutdown are outside the
intervals. One call is the gateway check of one eligible resident job, not a whole exec.

| Installed sessions | Census / job (µs) | Point query / job (µs) | Census / 1,000 jobs (ms) | Point query / 1,000 jobs (ms) |
| ------------------ | ----------------: | ---------------------: | -----------------------: | ----------------------------: |
| 0                  |         46.296256 |              48.727338 |                46.296256 |                     48.727338 |
| 1                  |         44.437443 |              43.769752 |                44.437443 |                     43.769752 |
| 8                  |         42.961207 |              37.508074 |                42.961207 |                     37.508074 |
| 48                 |        110.546328 |              56.609166 |               110.546328 |                     56.609166 |
| 128                |        168.802542 |              53.161057 |               168.802542 |                     53.161057 |

At the observed host's 48-session scale, this saves **53.937162 µs/job, 53.937162 ms/1,000 jobs (48.79%, 1.95×)**. The
medians there were 109.084 → 57.791 µs. At 128 sessions the reduction is 68.51% (3.18×). The first measured win is at
one session, but its 0.668 µs (1.50%) saving is near-flat; eight sessions show a clear 12.69% reduction. Empty-table
queries regress by **2.431 µs (5.25%)**, with medians 41.542 → 41.917 µs. Absolute host load varies between cells;
comparisons are only between each cell's interleaved arms. The earlier Python live-host sample is workload evidence, not
the control arm of this comparison.

Eight focused control/rotation tests passed, covering strict fields and framing, local credentials, explicit true and
false versus a missing boolean, absent workspace, mismatched revision, removal/tombstones, failed rotation, successful
rotation and stopped transport. The benchmark then passed separately. The initial new-client test caught the missing
operation in the strict parser; it was fixed at that parser, not bypassed. The initial benchmark's 64-port workspace
could not hold 48 sessions; its resource admission was corrected with the supported port grant below, not by escaping
the workspace's port allocation. No Linux execution or full repository gate is claimed by these focused runs.

The old full-status API still serves diagnostics and reconciliation. Control authentication, one-frame/EOF bounds,
monotonic revisions, tombstones and removal fences are unchanged. A missing or failed point query declines the resident
fast path exactly as the old failed status read did, leaving the existing controller route to reconcile. No failure is
converted into a successful match.

Reproduce in a dedicated benchmark workspace, granting enough capacity before running its fixtures:

```sh
cowshed grant <benchmark-workspace> --ports 257
direnv exec . cargo nextest run --cargo-profile release -p cowshed-gateway --lib --test gateway \
  -E 'test(control::tests::) | test(macos::control_socket_is_local_authenticated_and_reports_status) | test(macos::revision_tombstone_and_rotation_preserve_authority) | test(macos::session_current_benchmark)' \
  --run-ignored all --no-capture --build-jobs 2
```

## Content proof: patch identity before content

Production enters through `rm`'s gate, `ls --landing` and a land's folds → `counted` → `GitRepository::divergence` and,
for a branch whose merge into the target changes something, `GitRepository::content_not_held` (02_workspaces.md, `rm`
step 1). It was measured through gc's abandoned-ref sweep, since removed, which ran it once per kept tip. Every git call
is its own sandboxed controller process, so the phase costs what its process count costs.

`content_not_held` used to merge every file both sides changed, with one `git merge-file` and one `git cat-file blob`
each, before it asked patch identity, and then ran one `rev-list -- <file>` per unheld file. On the SB store (33 kept
namespaces, 36 measured tips) that was 2,840 merges, 2,840 blob reads and over 700 rev-lists. Patch identity now comes
first. One `git log --name-only --diff-merges=first-parent` over the range names the paths each commit changes, and only
a file that a commit outside the patch-held set changes is judged by content: 599 merges, with the target's versions
read in one `git cat-file --batch` per tip. In a range without merges that listing also names the commits changing an
unheld file, so no rev-list runs; none of the 36 ranges has a merge. `divergence` counts ahead and behind in one
`rev-list --left-right --count`. Nothing is cached or stored, and the verdict is unchanged.

The arms ran back to back from an unsandboxed host shell against the real store, both at main `314830f3c4`, through the
debug test profile:

| Arm    | `abandoned_refs` wall (s) | user (s) | sys (s) |
| ------ | ------------------------: | -------: | ------: |
| Before |                     248.4 |    109.8 |    46.1 |
| After  |                      38.1 |     24.8 |     6.5 |

Both arms serialized byte-identical `AbandonedRefs`. Two earlier before-runs took 214.0 s and 250.9 s; the whole
`gc --dry-run` of the installed build took 225.6 s the same day.

Two costs remain. `git cherry` recomputes main's patch-ids for every tip: 12.2 s summed over the 36 tips, unsandboxed.
The 599 merges cost about 30 ms each, about 18 s. Keying a stored per-file verdict by the base, head and target blob ids
would skip only the merges of files main has not touched since the previous gc. It is not built.
