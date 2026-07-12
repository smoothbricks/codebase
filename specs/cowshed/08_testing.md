# Testing & Performance

Four test tiers plus enforced performance budgets. Everything pure (unit + property) runs everywhere; the real tiers are
parametrized over the substrate (`APFS-image` on macOS, `ZFS` on Linux) and enforcement layer (Seatbelt / Landlock) and
each is explicit about where it runs.

## Unit tests (pure, all platforms)

No mounts, no root, no network — pure functions with table-driven cases:

- **Sandbox rule generation** (Seatbelt profile text and Landlock ruleset spec, from the same grant snapshot): closed
  baseline shape, grant snapshot inclusion, secret denies present regardless of grants (by omission on Landlock; on
  Seatbelt by **ordering** — see next bullet), ReadOnly drops mount writes, grant-intersects-deny refusal, path
  canonicalization/escaping (including unix-socket rule paths: the kernel matches canonical targets, so a `/tmp`-spelled
  rule silently denies — measured).
- **Profile ordering invariant (layered)**: SBPL is last-match-wins (measured — the same rules in the opposite order
  leave a secret readable), so every generated profile MUST emit its four layers in order: broad allows → the
  `~/.cowshed` volume-wide deny → scoped carve-backs (caches read, designated cache-subtree writes, own mount) → secret
  denies (04_sandbox.md). This test asserts the layer order structurally over generated grant sets AND by probe paths:
  grant file, CA key, sibling image, sibling mount must resolve to deny; own mount and designated cache subtrees to
  allow; secret paths to deny regardless of grants. The entire secret-protection model depends on it.
- **Path policy**: cwd validation, `..`/symlink-shape normalization, workspace-name validation.
- **Grant files**: schema round-trip, revision monotonicity, delta application, wildcard egress matching
  (`*.github.com`), port defaults.
- **Marker files**: round-trip, unknown-version rejection, role transitions (new/fork/restore).
- **CLI contract**: stdout shape per command (bare value vs JSON envelope), exit-code mapping from `CowshedError`,
  `next:` hints on the documented paths. Golden tests — changing them is a breaking change and must touch this spec.
- **Env wiring**: exec env allowlist filtering (no `*_TOKEN`/`*_SECRET`/`AWS_*` pass-through), cache exports match
  03_caches.md exactly.
- **Port-block rule generation**: the generated SBPL emits the block as **16 literal single-port `network-outbound`
  allows** (measured: SBPL rejects port ranges — `invalid port in network address` — and hosts other than
  `localhost`/`*`), leaves `network-bind`/`network-inbound` permissive on localhost, and contains no range syntax
  anywhere (a generation-text golden; kernel enforcement is proved by the escape tier).
- **Runner launch planning**: a workflow fixture whose first step is repository-controlled, followed by shell and
  supported action-generated commands, produces only `cowshed exec` launch plans in original order. The direct process
  launcher is a fail-on-call spy. An action type for which every process cannot be intercepted is rejected with the
  documented configuration error before its payload or any child process starts; cwd relocation and completion of the
  composite action never count as wrapping.
- **Image-format dispatch**: detached host metadata carries `imageFormat`; the complete table is `ASIF` → `.asif` and
  `SPARSE` → `.sparseimage`. Matching metadata/extension selects the corresponding attach path, while either crossed
  pair, an unknown format, or a wrong extension is rejected before an attach command is constructed. There is no
  extension alias or inference fallback.
- **Job-control encoding and summaries**: numeric job IDs round-trip without string coercion; stdout and stderr summary
  codecs are separate, versioned, deterministic for identical bytes, bounded at the specified byte limit, and redact the
  specified secret fixtures. Control-message JSON and Arrow records carry control/result metadata plus those summaries,
  never the full stream bytes. Arrow's `job_id` joins to the standard trace identity and is not copied into or
  substituted for `span_id`.
- **API capability goldens**: `Project` exposes discovery only; `WorkspaceRef` exposes inspection plus safe
  `ensure`/`attach`; `WorkspaceHandle` exposes exactly one workspace's exec/shell/jobs/quota-bound checkpoint/push/grant
  reads; only `Coordinator` exposes grant/revoke/restore/destroy/rebase/land/gc/repo-mirror and quota policy.
  Compile-fail fixtures prove forbidden methods and cross-workspace construction are unavailable.
- **Push CAS and preservation model**: pure state-machine tables cover each omitted/satisfied/mismatched combination of
  expected workspace incarnation, source head, and destination ref head (including expected-missing). A mismatch is a
  `Conflict` that preserves the old destination exactly and leaves the source live; success reports one source object ID
  and a destination ref resolving to that exact ID. DTO goldens pin the Rust, JSON, N-API, and MCP spellings.
- **MCP capability/auth codecs**: coordinator authority can be read only from the designated inherited FD/socketpair and
  never argv/environment/stderr; worker descriptors are 256-bit, 30-second, one-use, memory-only, atomic-consume,
  restart-invalidated, workspace/socket/peer-bound. Missing, expired, replayed, mismatched, or insufficient authority
  maps to dedicated JSON-RPC `-32005`, never sandbox-denied.
- **Structured stdin codecs and policy**: API/CLI/N-API/MCP discriminated inputs round-trip arbitrary binary inline
  bytes, streams, and normalized workspace-relative file paths without shell interpolation. Pure path cases reject
  absolute paths, traversal, symlinks, non-regular files, and ambiguous source combinations; job/trace DTO goldens carry
  kind, delivered bytes, completion, and optional relative path but never inline contents.
- **Shell AST redirection eligibility**: use the production shell AST parser against a matrix proving that only one
  top-level simple command with literal, in-workspace, same-filesystem, nonexistent `>` and/or `2>` targets under
  default clobber semantics is eligible. Append, fd duplication, pipelines, lists, subshells, expansions, symlinks,
  `noclobber`, existing/cross-filesystem targets, malformed input, and unknown AST forms must fall back. A spy makes
  regex/string sniffing impossible and proves parsing never feeds authorization or denial decisions.
- **Job-output quota state machine**: combined accounting includes persisted plus in-flight bytes, counts a shared
  redirection inode once, and transitions only to explicit `output-limit`; tests pin TERM→grace→KILL→drain→fsync→publish
  ordering and distinguish timeout/signal/exit.

## Property tests (proptest, pure, all platforms)

Invariants the table-driven unit cases only sample. Each is a pure function over generated inputs:

- **Path policy**: normalization is idempotent (`normalize(normalize(p)) == normalize(p)`) and containment-preserving (a
  path normalized under a root never escapes it via `..`/symlink shape).
- **Grant algebra**: delta composition is well-defined (apply then apply-inverse is a no-op), revision is strictly
  monotonic across effective mutations, a no-op mutation leaves the revision and the canonical set unchanged, and public
  vectors come out sorted + deduplicated regardless of input order.
- **Marker/schema**: round-trip (`parse(write(m)) == m`) for all roles; any unknown `version` is rejected, never
  silently coerced.
- **Egress/endpoint normalization**: host wildcard matching (`*.github.com`) and default-port fill are order- and
  duplicate-independent; macOS `portBlock` round-trips when present and is rejected on Linux. Linux endpoint generation
  always yields `http://127.0.0.1:7644` and never manufactures a block base.
- **Staged-object non-enumeration**: for any generated set of in-flight staged names (adopt/new temporaries, 01/02),
  enumeration never returns one — the "derived state" promise holds under partial writes.
- **Idempotent recovery/gc**: replaying a crash-recovery or `gc` pass over any generated interrupted-state fixture
  converges to the same result as running it once (crash points from the adopt/new/rm recovery tables, 02).

## Integration tests (real substrate)

Gated by `COWSHED_INTEGRATION=1` and parametrized over the substrate the host provides. On macOS: small `.asif` images
(1 GiB caps) under `/private/tmp/cowshed-itest-<pid>`, mirroring the apfs-workspace-bench harness, cleaned up in reverse
mount order with `-force` fallback. On Linux: a scratch ZFS pool on a loopback/file vdev (`cowshed.itest.<pid>`) with
datasets destroyed and the pool exported on teardown; the Linux leg also exercises `cowshed-helper` and the
Landlock/netns exec path. A suite-level guard reaps leaked `cowshed.itest.*` volumes/pools. The same flow table runs on
both; substrate-specific assertions (fsck step on APFS, origin-snapshot GC on ZFS) are tagged.

Covered flows:

- adopt → new → exec → push → rm (the golden path), including marker/token rewrite on new;
- **Completed-branch preservation without checkout mutation**: dirty the main workspace working tree and index and
  record its checked-out branch, HEAD, status, file bytes, and index checksum. Push a completed workspace branch and
  assert `refs/cowshed/<ws>/heads/<branch>` resolves to the exact source head with every unique commit reachable, while
  the recorded branch, HEAD, status, bytes, and index remain byte-for-byte unchanged. Assert no preservation claim is
  made for objects reachable only from a temporary ref or `FETCH_HEAD`, retire the source only after the durable ref is
  installed, and recover the exact commit graph after retirement.
- **Writable handoff and fork recovery**: checkpoint a quiesced writable workspace containing committed and uncommitted
  state, hand the same live workspace to a successive writer, and prove it can edit, execute, commit, and push. Repeat
  by creating a live CoW fork at handoff; prove the fork is independently writable with fresh identity and
  closed-baseline grants, then simulate writer failure and recover from both the retained source/checkpoint and the
  unaffected fork without losing the pre-handoff state.
- **Push compare-and-swap conflicts**: race workspace restore against expected-incarnation push, a new source commit
  against expected-source-head push, and another preservation update against expected-destination-head push (including
  expected-missing versus an existing ref). Every stale attempt must return `Conflict`, identify the moved expectation,
  leave the destination object ID and main working tree/index unchanged, and retain the source workspace. A retry with
  freshly read expectations must preserve exactly the newly selected source head.
- **runner interception from step one**: execute a workflow fixture with a hostile first command, later shell commands,
  and a supported action-generated command. Instrument the runner spawn boundary and assert that every
  repository-controlled command is launched through `cowshed exec` in the job workspace, in workflow order, with zero
  direct runner spawns. A fixture containing an uninterceptable action type must fail before that action starts and must
  not fall back to direct execution. Run the fixture after the composite action has returned and with cwd already inside
  the mount, proving neither mechanism supplies interception.
- **workspace-local job allocation across restart**: submit multiple execs, assert each submission receives a unique
  numeric ID in strictly increasing allocation order, restart the job-control/supervisor process, submit again, and
  assert the new ID is greater than every ID allocated before restart. Exercise two workspaces to prove ordering state
  is workspace-local rather than a shared or lexicographic identifier.
- **job backing files and summary surfaces**: run a below-quota command that emits distinct text, secret fixtures,
  truncation-boundary bytes, and invalid UTF-8 independently to both streams, then exits nonzero. Assert every admitted
  byte is present in `.cowshed/job/<id>/out` and `.cowshed/job/<id>/err`; stdout/stderr are never merged. Assert control
  messages and Arrow rows contain the same deterministic, bounded, versioned, redacted per-stream summaries and
  control/result metadata, while backing files remain unredacted. Redaction and summary truncation must not change
  denial, exit status, policy evaluation, or build-success classification.
- **JSON is control-only**: request JSON for the same job and parse the complete output as the documented control/result
  envelope. It contains the numeric job ID, result metadata, backing-file references, and redacted stdout/stderr
  summaries, but neither raw stream bytes nor a text/base64 embedding of them; reading the referenced files is the only
  way to recover full binary output.
- **supervisor grant-revision cutover**: start a long-running job at revision N, apply an effective filesystem grant or
  revoke, and assert the enclosing supervisor drains and is relaunched at N+1 before the next exec. The running job
  completes under N, an inner per-command profile can only narrow N and cannot broaden it, the next exec observes N+1,
  and reconnecting a named session bound to N returns the documented stale-session conflict rather than silently
  rebinding. Repeat for both grant and revoke.
- **attach `-nomount` → fsck device → mount** ordering on APFS (the clone is verified as a block device _before_ it is
  mounted, per 02) — asserts the sequence and that a structurally-bad clone is caught before mount, not after;
- **fork mid-write clone validity**: clone an image while a writer churns the volume, then verify the clone mounts and
  fsck-passes. (Measured baseline to hold: 10/10 clonefiles taken under a continuous file-writer plus a streaming 128
  MiB dd passed both `fsck_apfs -q` and a full `-n` check, mountable and readable, on both SPARSE and ASIF; a non-synced
  clone may miss the last writes — freshness, not consistency. This tier keeps that regression-pinned.)
- checkpoint/restore round-trip (restore undo image `pre-restore-…` present);
- ensure healing matrix: detached image, wrong-flag mount, missing/wrong-flag `cowshed.store` and `cowshed.caches`
  volumes (lazy recreate + canonical-flag remount, 01_storage.md), stub `.envrc`;
- lazy volume creation at adopt: both dedicated volumes created idempotently before the first image; **Time Machine
  default-inclusion check** (verification item, 01_storage.md): whether TM includes additional internal volumes by
  default, and that adopt's volume-level exclusion is applied when it does;
- rm-while-busy (open file handle → grace → force detach);
- gc: trash drain, checkpoint pruning, orphan mountpoint removal, compaction (SPARSE fallback);
- gateway: mirror hit/miss against a local fixture registry, token→policy mapping, 403 hint body, audit records, CONNECT
  allow/deny, `repo mirror` fetch into a read-only bare mirror;
- **Linux connector end to end**: for an attached workspace, assert exactly one connector exists in its private netns,
  under the dedicated controller-owned identity/cgroup, bound only to IPv4 `127.0.0.1:7644`. Run real Bun/npm install,
  Cargo sparse-registry fetch, Go module download, and a generic HTTP/HTTPS proxy client against
  `http://127.0.0.1:7644/{npm,cargo,go}` and the proxy variables; assert byte identity through the connector, gateway
  endpoint+token authentication, mirror behavior, and no direct fallback. None of these clients may use a Unix-socket
  transport. Run two workspaces with the same address/port and prove neither can reach the other's connector or socket.
  Detach must stop admission, drain connections, kill the connector cgroup, unlink the socket, and leave 7644 unbound;
  attach must create exactly one fresh connector before exec admission. Restore must drain old connections and make the
  old connector/socket/token unusable before publishing the new incarnation.
- gateway interception (05_gateway.md): an intercepted host serves a workspace-CA leaf the in-image anchor trusts,
  injects the Keychain credential, and records a **request-granular** audit line; an `--opaque` host tunnels without
  injection; an `--impersonate` connection suppresses header injection; the **upstream-health gate** fails a dead
  upstream fast with a classifiable error (not a per-request timeout) — asserting the gateway-absent / upstream-offline
  / denied trichotomy; **socket teardown**: no leaked listeners or half-closed sockets after a churn of many distinct
  intercepted hosts (the JS original's fd-leak bug class), and oversized request headers are tolerated;
- ASIF/SPARSE format/extension enforcement: attach detached fixtures whose host metadata says `ASIF` with `.asif` and
  `SPARSE` with `.sparseimage`, and assert dispatch reaches the corresponding real substrate path. Swap the extensions
  and assert a format/extension mismatch is reported before attach; fallback from unavailable ASIF creates and records
  SPARSE with `.sparseimage`, never ASIF metadata on a SPARSE file.
- **persistent multi-client supervisor socket**: runtime directory is `0700`, socket is `0600`, wrong-peer credentials
  fail before framing, and many simultaneous clients submit/query independent jobs. Disconnect one client while jobs and
  another attachment remain active, then reconnect and resume by job id/backing-file offset; assert no job stops and the
  socket remains linked. Only orderly supervisor exit unlinks it; stale-start cleanup first proves no live owner.
- **authoritative job telemetry channel**: launch the supervisor with a controller-provided writer IPC/capability FD and
  assert it is close-on-exec and absent from every job descendant. Concurrent producers write distinct exclusively
  allocated segments; seal/publish makes each immutable, recovery writes a new segment, and no shared append occurs.
  Forge, delete, and contradict workspace-local `records.arrow`; status, output-limit, denial correlation, and audit
  queries must still return controller-owned truth.
- **combined job-output quota**: configure small limits and race stdout/stderr writers so persisted and
  read-but-in-flight bytes cross from both pipes. Assert exactly one trip, no accounting overshoot/double-count for
  eligible shared-inode redirection, complete-process-group TERM then grace/KILL, both pipes drained without deadlock,
  fsync before the authoritative `output-limit` event, and no silent continuation/truncation. Repeat at exactly-limit,
  one-byte-over, client-disconnected, soft-timeout, and hard-timeout boundaries.
- **shell redirection fast path equivalence**: for every eligible AST form, assert requested destination and job stream
  are links to the supervisor-created inode, byte-identical to ordinary shell behavior, tailed once, and retained
  without unlinking the caller destination. For every ineligible/ambiguous/racy case, assert pre-exec fallback to the
  ordinary shell/capture path and identical exit/output/filesystem results. Exercise quota crossing in both optimized
  and fallback paths and require identical terminal metadata.
- **structured stdin end to end**: feed binary data containing NUL and invalid UTF-8 through inline, backpressured
  stream, and workspace-file sources over CLI/N-API/MCP into a slow reader; assert byte identity, bounded buffering, EOF
  exactly once, and consistent stdin/job/trace metadata. Cancel mid-stream (stdin closes and metadata is incomplete
  without implicit job kill), exit the job before producer EOF (producer cancels and waiters release), and race path
  replacement. Workspace-file opens must reject absolute/escaping paths, symlink ancestors/targets, devices, sockets,
  directories, and cross-workspace sources with no bytes delivered.
- **MCP authority lifecycle**: prove coordinator startup emits no authority to stdout/stderr/argv/environment, inherited
  endpoint closure and non-inheritance, descriptor TTL/replay/concurrent atomic consume/restart invalidation, peer and
  socket binding, and non-disclosure through telemetry. A worker invoking every coordinator-only tool receives `-32005`
  before dispatch; `SandboxDenied` remains reserved for authoritative sandbox evidence.

## Escape tests (cowshed-escape-tests, one corpus, both OSes, release gate)

One shared adversarial corpus (04_sandbox.md), run through the real exec pipeline: **Seatbelt on macOS, Landlock +
loopback netns on Linux**, green on **both** as a release gate (a red escape test cannot be waived). Structure uses one
shared corpus: each case is a shell payload plus an assertion that the operation was denied and the artifact untouched.

Shared categories: path escapes (traversal, symlink, hardlink), secret reads, cowshed-state tampering, cross-workspace
access, egress bypass (direct, helper-process, DNS), revocation binding, ReadOnly enforcement, **workspace-CA
isolation** (a workspace cannot read its own or a sibling's CA private key, nor obtain a leaf for an ungranted host —
the CA cert it holds is a public anchor only, 04_sandbox.md/05_gateway.md) — plus two the port-block model adds:

- **sibling-supervisor-socket**: workspace A attempts to `connect(2)` workspace B's supervisor unix socket and drive B's
  shells — must be denied (the baseline scopes unix-socket connect to the workspace's own supervisor socket, the nix
  daemon, and the gateway, 04_sandbox.md).
- **port-block escape**: connect to a _sibling's_ data-plane port, a sibling's service port, and a sibling's
  ephemeral-bound listener — all EPERM (isolation is **outbound-enforced**; bind stays permissive, so sibling binds are
  not prevented and need no case). Verify the errno signal while at it: denied connect = EPERM(1), allowed-but-unserved
  = ECONNREFUSED(61) — the in-band denial evidence of 04_sandbox.md.

Linux-specific cases (Landlock/netns): bind-mount a denied path into a granted root, `/proc/<pid>/root` and
`/proc/<pid>/cwd` reach-arounds, `unshare`/`setns` to leave the netns, abstract-namespace and filesystem unix sockets
reaching a non-gateway listener, and `connect(2)` to a non-gateway TCP port. Connector-specific cases race a malicious
workspace listener for `127.0.0.1:7644`, attempt to impersonate/replace the connector, signal or ptrace it, join its
identity/cgroup, inject traffic through a non-loopback local or peer address, redirect it to another Unix socket, and
reach a sibling workspace's connector/socket. Every attempt must fail and produce no gateway request. Policy-string
goldens (unit tier) do not substitute for this — they prove generation, not kernel enforcement. Every
production-discovered escape becomes a permanent case.

## Trace assertions (lmao-query)

The escape and integration tiers assert over **emitted traces** (13_telemetry.md), not scraped text. Because lmao is
deterministic under an injected `Clock`/`Entropy` (bit-identical trace bytes per `(build, seed, config)`), a run
produces a stable trace the tier queries with `lmao-query` selectors:

- **Escape tier** asserts denials as trace facts: `never(gateway.allow ∧ host ∉ grants)`,
  `count(egress ∧ ¬granted) == 0`, and the ordering invariant `never(secret-read ∧ granted-ancestor)` (the denies-last
  property, cross-checked against the unit-tier generation golden — one proves kernel enforcement, the other profile
  text).
- **Integration tier** asserts lifecycle causality: `every(rm ⇒ supervisor-stop precedes detach)`,
  `every(linux-detach ⇒ connector-drain precedes connector-cgroup-kill precedes socket-unlink precedes netns-release)`,
  `every(linux-restore ⇒ old-connector-drain precedes new-endpoint-publish)`, `every(new ⇒ fsck precedes mount)`, and
  the escalation loop `denial → grant(rev+1) → retry` as a single connected trace.
- **Golden trace fixtures**: the deterministic trace of `cowshed new` (and other lifecycle ops) is checked in; a diff is
  a behavior change that must touch the spec, the same contract as the CLI goldens.

This is the assertion surface `lmao-query` was built for (its `selector → count/never` shape); the tiers consume it
rather than each re-implementing trace inspection.

## Performance budgets (regression thresholds)

Measured by the integration suite with tinybench-style medians (≥ 10 samples); CI asserts with a 3× multiplier to absorb
runner noise, local `cowshed doctor --bench` reports raw numbers.

### APFS / Seatbelt (macOS)

| Operation                     | Budget (median, local)  | Basis                                                        |
| ----------------------------- | ----------------------- | ------------------------------------------------------------ |
| `cowshed new` cold            | ≤ 1 s                   | clonefile ~2 ms + attach ~235 ms + branch/marker work        |
| `cowshed ensure` healthy      | ≤ 25 ms                 | one statfs + one marker read, compiled binary                |
| `cowshed rm` (perceived)      | ≤ 100 ms to return      | rename + grant-file unlink; detach is background             |
| `cowshed fork` / `checkpoint` | ≤ 1 s                   | same physics as new                                          |
| `cowshed path` / `ls`         | ≤ 50 ms                 | readdir + getmntinfo only — proves the no-state-store design |
| exec sandbox overhead         | ≤ 50 ms over bare spawn | profile generation + sandbox-exec                            |

**Basis, and what the numbers are (and are not).** Figures come from the apfs-workspace-bench study
(`specs/cowshed/prototypes/apfs-workspace-bench/`, harness, REPORT.md, and results committed alongside): clonefile of a
populated 100k-file image ~2 ms; `hdiutil attach` **median ~235 ms**; clone-backed images beat shadow mounts 2.34× on
synchronous write throughput, which is why clones are the substrate. These are **primitive-operation evidence from a
single 20-sample run**, not end-to-end cowshed SLOs — budgets are stated as **medians** precisely because 20 samples
cannot fix a stable tail. Note the p99 correction: the ~590 ms figure sometimes quoted is the p99 of **shadow
create+attach**, not clone attach — clone _attach-only_ p99 in that run was ~273 ms and clonefile+attach ~271 ms. Do not
cite ~590 ms as a clone-attach percentile.

The format experiment (`results/2026-07-11-substrate-experiments.json`, single-run medians, 4 GiB / 2000 files) adds:
**ASIF vs SPARSE — create 479 vs 1017 ms (2.1×), direct read 3.2 vs 17.9 ms (5.6×), direct write 15.7 vs 39.6 ms (2.5×),
metadata ~102 vs ~234 ms (2.3×), clonefile equal (~1.8–2.0 ms), attach ~416 vs ~342 ms (ASIF ~75 ms slower)** — the
basis for the ASIF default (01_storage.md); budgets absorb the ~75 ms attach delta within the 1 s median. **Attach floor
verdict: not flag-reducible** — `-noverify` saves ~15 ms (noise), `-noautofsck` nothing, `diskutil image attach` exposes
no equivalent knobs; the ~235–400 ms is inherent to DiskImages + APFS mount + DiskArbitration. Halving it would need a
different attach path (DiskImages2 / diskarbitrationd private API) — a research note, and budgets must not assume it.
The bench harness is the reference methodology for any future substrate change (it validated ASIF before it became the
default; it establishes the ZFS baseline below).

### ZFS / Landlock (Linux) — separate baseline

ZFS has no `attach` and no `fsck` step, so the APFS numbers do not transfer; the same _user-visible_ operations get
their own measured baseline and regression limits on a ZFS-capable Linux host (the cowshed CI runner, 10_ci.md),
established before the Linux leg gates.

| Operation                | Budget (median, local)  | Basis                                                                |
| ------------------------ | ----------------------- | -------------------------------------------------------------------- |
| `cowshed new` cold       | ≤ 250 ms (to establish) | `zfs snapshot` + `zfs clone` + mount, tens of ms; no attach, no fsck |
| `cowshed ensure` healthy | ≤ 25 ms                 | same pure fast path as macOS                                         |
| `cowshed rm` (perceived) | ≤ 100 ms to return      | logical retire; `zfs destroy` clone+origin is background             |
| `cowshed path` / `ls`    | ≤ 50 ms                 | `zfs list` + mount table                                             |
| exec sandbox overhead    | ≤ 50 ms over bare spawn | Landlock ruleset apply + netns join                                  |

The "to establish" figures are placeholders until first measured on the runner; they are recorded here so the Linux
baseline is an explicit deliverable, not an inherited APFS number.

## CI

- Unit tier: every PR, all platforms.
- Integration + escape tiers: macOS runners **and** a Linux+ZFS runner on PRs touching `packages/cowshed`, and nightly.
  The Linux leg is the natural dogfood target — a cowshed CI runner (10_ci.md) running cowshed's own suite.
- Escape suite green on both OSes is a release gate; a red escape test cannot be waived.
- `cargo clippy --workspace -D warnings` and `cargo fmt --check` gate merges (repo rule: fix everything you see — no
  pre-existing-failure waivers).
