# Testing & Performance

Four test tiers plus enforced performance budgets. Everything pure (unit + property) runs everywhere; the real tiers are
parametrized over the substrate (`APFS-image` on macOS, `ZFS` on Linux) and enforcement layer (Seatbelt / Landlock) and
each is explicit about where it runs.

## Unit tests (pure, all platforms)

Decisions live in a pure core: event handling, ordering, timeouts, retry policy and recovery take injected events and an
injected clock, so this tier reaches them as table-driven calls with no real I/O and no sleeps. The imperative shell is
dumb, decisionless I/O: it maps OS events (FSEvents and other file watchers, sockets, signals, timers) into core inputs
and executes core outputs. It gets one simple mock integration check — a synthetic OS event reaches the real core as its
mapped input; the mock stands in for the OS, never for our code — and is proven by actual runs in practice. No test
drives core logic through real FSEvents, real timers or a load generator; the real tiers below prove substrate and
enforcement contracts, not decisions a core takes.

A suite tests only the contract of the crate or package it belongs to, in every tier: a test belongs to it only if a bug
in that suite's own code can make it fail and its assertion is about what that code promises. Behavior a dependency owns
(another crate or package, Cargo, Nx, git, Bun, the OS) is tested once, in that dependency's own suite: how Cargo reads
a jobserver is Cargo's own to test (the jobserver proof below), and what the OS's disk tools do with an image is theirs.
A dependency re-test is never written and is deleted on discovery, with its fixtures and helpers, in the unit that finds
it, never kept as "integration coverage"; a test the dependency's suite lacks is added by its owner. A consumer that
re-implements what a dependency owns is a defect: the re-implementation goes, and its tests with it. What a suite keeps
is its own contract: its wire and protocol, its recovery of its own records, one test per mapping of a dependency's
results and refusals into its own terms (the dependency faked only at its published interface, as the mock above stands
in for the OS), and exactly one smoke check that the real dependency is wired in. The real tiers below assert what
cowshed does with its real substrate, never what the substrate does.

No mounts, no root, no network — pure functions with table-driven cases:

- **Sandbox rule generation** (Seatbelt profile text and Landlock ruleset spec, from the same grant snapshot): closed
  baseline shape, grant snapshot inclusion, secret denies present regardless of grants (by omission on Landlock; on
  Seatbelt by **operation specificity and ordering** — see next bullet), ReadOnly drops mount writes,
  grant-intersects-deny refusal, path canonicalization/escaping (including unix-socket rule paths: the kernel matches
  canonical targets, so a `/tmp`-spelled rule silently denies — measured).
- **Profile specificity and ordering invariant (layered)**: an explicit broad `allow file-read-data` defeats a later
  wildcard `deny file-read*`; read denials must name `file-read-data` explicitly (04_sandbox.md). Every generated
  profile MUST also emit its four layers in order: broad allows → the `/private/cowshed/store` volume-wide deny → scoped
  carve-backs (detected capabilities' shared caches and grants, own mount) → secret denies (04_sandbox.md). This test
  asserts the layer order structurally over generated grant sets AND by probe paths: grant file, CA key, sibling image,
  sibling mount must resolve to deny; own mount and detected shared caches to allow; secret paths to deny regardless of
  grants. Real Seatbelt probes must confirm data reads are denied, not merely that deny text occurs later in the
  generated profile.
- **Path policy**: cwd validation, `..`/symlink-shape normalization, workspace-name validation.
- **Raw Git preservation**: independent real-repository witnesses cover top-level byte drift, submodule bytes and HEAD
  drift, and copy-only paths. Each witness proves configured filters would run under ordinary Git but never during
  capture or copy verification, verifies restored bytes again, and checks both repositories' config/index bytes remain
  unchanged. Each planted refusal names the step that owns it: the one component of the live comparison, the path the
  raw byte comparison reports, or the path set. A refusal for another reason fails the witness, so no verification
  between plants is needed to attribute one: each plant's bytes and modification time are asserted back in place, and
  the copy verifies once more after the last plant. A same-size `.git/description` change, which no Git probe reads, is
  the plant only the `.git` tree comparison can refuse. A byte plant over a filtered file matches neither the raw bytes
  nor the blob its clean filter stored, so no racily clean index entry can turn the byte refusal into a live-status one.
  The dirty submodule's clean driver is named by the submodule's own config alone: the filter-free status overrides the
  superproject's drivers through environment variables that Git also hands to a submodule's child status, so a driver of
  the same name would never run even from a status that descended into the submodule, and the marker would prove
  nothing. Each is an ordinary test that finishes well inside the ordinary 30s slow warning, so the run's progress bound
  sees it finish; they are not one aggregate of hundreds of serial Git probes. Staging uses the production tree copier,
  not a fixed host executable path. Allocation-only comparison cases supply distinct loose-size probe observations while
  retaining the other real Git evidence and fsck refusal; they never require compression or sparse writeback to change
  the host filesystem's allocated-block count. Companion-retention table rows share one captured real-Git source and
  reset their disposable project tree per row; every refusal still compares the whole project tree, and every converged
  row retains its unchanged-replay oracle.
- **Grant files**: schema round-trip, revision monotonicity, delta application, wildcard egress matching
  (`*.github.com`), port defaults, canonical host admission shared with exec, host:port refusal before write, verbatim
  invalid-key revoke and no-op recovery, and closed own/standing-only fork inheritance. Gateway denial hints must name
  hosts the grant parser admits.
- **Marker files**: round-trip, unknown-version rejection, role transitions (new/fork/restore).
- **CLI/error contract**: stdout shape per command, all `CowshedError` mappings including `Integrity` → own exit 7,
  exec-wrapper 106, MCP -32006, and `next:` hints. A child 7 still passes through unchanged. Golden tests make any
  taxonomy/shape change explicitly breaking. Wire-corpus acceptance also widens each object document, including each
  object in a list, and requires both generated assertion and parsing to refuse the extra field. Bootstrap tests require
  the retained Store identity, exact fstab pin and complete action-outcome roster before universal predicates. Mirror
  audit tests pin each decision's kind, status, workspace and safe path before checking credential redaction. The
  concurrent reaper fixture joins its signaller and ends its owned process group even when the pre-reap assertion
  unwinds. An independently owned stdin pipe, retained outside `Child::wait`, holds the descendant until owner cleanup
  or owner death, rather than a lifetime timer.
- **Env wiring**: exec env allowlist filtering (no `*_TOKEN`/`*_SECRET`/`AWS_*` pass-through), cache exports match
  03_caches.md exactly.
- **Repository Cargo warmth**: warm the real repository's own config in a managed shell, fork its source/build images to
  a different mount path and repeat the identical Cargo build. Require every `compiler-artifact` to be `fresh: true`; a
  fixture alone cannot catch a checkout-relative linker. The Darwin linker probe requires zero `xcrun`/`xcode-select`
  with exported `SDKROOT`, exactly one fallback `xcrun` without it, and runnable binaries from both paths.
- **Port-block rule generation**: the generated SBPL emits the block as **one literal single-port `network-outbound`
  allow per port of its recorded size** — 16 for a live pre-64 block, 64 for a new one (measured: SBPL rejects port
  ranges — `invalid port in network address` — and hosts other than `localhost`/`*`), leaves
  `network-bind`/`network-inbound` permissive on localhost, and contains no range syntax anywhere (a generation-text
  golden; kernel enforcement is the escape tests' to prove).
- **Runner launch planning**: a workflow fixture whose first step is repository-controlled, followed by shell and
  supported action-generated commands, produces only `cowshed exec` launch plans in original order. The direct process
  launcher is a fail-on-call spy. An action type for which every process cannot be intercepted is rejected with the
  documented configuration error before its payload or any child process starts; cwd relocation and completion of the
  composite action never count as wrapping.
- **Job-control encoding, storage unions, and summaries**: numeric job IDs round-trip exactly; stdout/stderr codecs
  remain separate. Goldens cover every `OutputStorage`/`ProtectedOutput` discriminant, exact counts/SHA-256, bounded
  tagged inline JSON, flattened Arrow validity, and deterministic bounded redacted summaries. Protected Arrow may carry
  bounded inline Binary; controller audit Arrow must contain counts/hashes/batch digests but no payload/path fields.
  `job_id` joins standard trace identity and never substitutes for `span_id`.
- **Job CPU sampling**: the real-process witness compares CPU share with the job's own observed CPU delta and wall-time
  interval, not a presumed share of a loaded host. A paused Tokio sampling clock keeps the interval's boundaries known
  while kernel rusage and monotonic wall time remain real. Repeated reads must keep the window; advancing one clock
  boundary opens the idle window. The child's independent CPU report pins the accounted total. Concurrent witnesses
  sharing a pipe emit each complete bounded report in one write; the reader keeps its strict record grammar rather than
  accepting interleaved fields.
- **API capability goldens**: `Project` exposes discovery only; `WorkspaceRef` exposes inspection plus safe `attach`;
  `WorkspaceHandle` exposes exactly one workspace's exec/shell/jobs/quota-bound checkpoint/push/grant reads; only
  `Coordinator` exposes grant/revoke/restore/destroy/rebase/land/gc/repo-mirror and quota policy. Compile-fail fixtures
  prove forbidden methods and cross-workspace construction are unavailable. Every declared operation, including
  `job.processes` and `job.processEvents`, has one request in `operations.corpus.json`; the codec, controller authority
  routing, and served-handle decoding tests consume that same corpus. Process-tree and process-event reply goldens cover
  omitted observations, observed exits, sparse changes, and coverage gaps through core serialization and the generated
  public validators. `cowshed:wire-fixtures` regenerates those goldens before the workspace's `cargo-test-archive`
  compiles them with `include_str!`; the archive hashes the produced JSON. Generated JSON is never edited by hand. The
  target renders and formats the corpus in a git-ignored staging directory and publishes it with one rename, so a reader
  running beside it (`cowshed:lint`) sees the old corpus or the new one, never a truncated or unformatted file;
  `cowshed:wire-fixtures-test` witnesses that.
- **Public declaration IR:** `cowshed:declaration-model-test` admits the actual owner model through its generated Typia
  Equals guards in one setup, pins `get`/`as-str`/`display` projection metadata, and refuses missing, unknown and extra
  metadata through both assertion and JSON parsing. It consumes the prebuilt owner generator, without an N-API wire
  corpus or native runtime fixture. Rust IR tests independently pin every serialized fact and the exact owner-emitted
  model/validator bytes. Like the canonical wire-generation fixture, its isolated source clone and generated validator
  project live in the package's ignored `.cache/`, preserving the owning Typia/dependency lookup while remaining outside
  strict source scanners. A live-clone inventory control retains the real bootstrap source and excludes the temporary
  copy. The focused target runs Biome and ESLint before the admission oracle.
  - **API generator source mutations:** module-collection and N-API projection fixtures copy declarations under
    std::env::temp_dir(), honoring the workspace TMPDIR contract in 04_sandbox.md. They run inside the worker sandbox
    without write authority to a world-shared temporary directory.
- **Authoritative cwd resolution and env wiring**: fake-host/router tables cover deeply nested cwd success, detached and
  cross-project refusal, malicious marker-only refusal, and overlapping active-mount ambiguity, plus macOS/Linux
  `PortBlock` presence. Capability-wire tests freeze `project.workspaceAt` and reject local CLI inference.
- **Unserved workspace serve** (02_workspaces.md, "Workspace environment"): `repo-path.test.ts` in
  `packages/nx-plugin/src/managed-files/` generates `.cowshed/env` files, inherited `TMPDIR` and `COWSHED_RUNTIME_LINK`
  values, workspace names, and what the installed cowshed's `exec <ws> -- true` does: write a generated file, or fail
  with a generated status and error. It proves the managed PATH writer leaves a served file alone and otherwise runs
  that start exactly once, from the checkout, through the installed cowshed rather than the checkout's launcher, with
  neither inherited value and no stdin; that it enters when the start serves the file, and refuses, naming the
  workspace, each missing entry or the start's status and error, when the start fails or leaves the file unserved.
  `unserved-workspace-hooks.test.ts` beside it runs a real `git commit` through the managed hooks in an interrupted-fork
  fixture whose build link names main's Nx state, from a shell bound to main. A start that serves the fork lets the hook
  past its PATH writer; a failing start refuses by name and makes no commit. Either way main's daemon record bytes and
  its live `d.sock` are untouched. The cli package's `nx-socket-dir.test.ts` proves the socket owner refuses an unserved
  workspace and a runtime link that leads to another checkout.
- **Per-workspace aggregate checkpoint quota**: substrate stats fixtures freeze active allocated bytes, total checkpoint
  bytes/count, and the pinned-byte subset across mixed pinned/automatic images. Runtime state machines prove
  `existing checkpoints + one active-image charge`, exact-boundary admission, one-byte/count overflow refusal with zero
  image/fact/metadata effect, and two-workspace isolation.
- **GC plan purity and revalidation**: preview fixtures freeze candidate identity/path/allocated-byte/reason ordering
  and byte sums, exclude every pinned checkpoint, and compare repeated previews. Dry-run preserves every image, sidecar,
  fact, and metadata byte. Pin/incarnation/size changes and a held lifecycle lock make execution return stale-plan
  `Conflict` before mutation; a fresh unchanged plan reclaims exactly its candidates. A preview and reclaim over a
  retired workspace whose 100 GiB sparse checkpoint images are mode 000 succeed, because identity is taken from `stat`
  and never opens an image; a same-size, same-time replacement image and a same-length, same-time rewritten record each
  change the identity. The unreadable-inventory fixture uses ordinary RAW `.dmg` candidates: the first candidate is
  deferred on its inventory error while the later candidate is reclaimed. A file that is not a `.dmg` is never a GC
  candidate.
- **Push CAS and preservation model**: pure state-machine tables cover each omitted/satisfied/mismatched combination of
  expected workspace incarnation, source head, and destination ref head (including expected-missing). A mismatch is a
  `Conflict` that preserves the old destination exactly and leaves the source live; success reports one source object ID
  and a destination ref resolving to that exact ID. DTO goldens pin the Rust, JSON, N-API, and MCP spellings.
- **Foreign-session isolation and atomic retirement**: directory-backed fixtures keep sibling grants missing or corrupt
  while the selected workspace remains inspectable and reconciles without evicting sibling gateway sessions. The
  controller admits and observes 100 live jobs while project enumeration refuses its foreign inventory fault. Retirement
  fixtures prove clean landed sources are committed to `sessions/.deleting` before the removal reply, none to the
  recovery trash and no byte of one freed by the removal, a real immutable-file refusal of that release preserves source
  grants/CA authority, and dirty, unlanded and only-copy hidden content survives in its live or recovery image, even
  after the recovery sidecar is removed and startup/GC runs again, while a tree whose only extra file is gitignored
  retires with its image released. Cancellation and post-rename crash replay retain the mutating intent until the
  release's owner metadata is removed; recovery-image rename fixtures still require complete grants/CA at commit. The
  directory cutover runs the real controller and gateway with corrupt/missing sibling grants, named mount/marker fences
  and held-lock recovery, plus prepared-retirement retry and post-commit recovery. Cold named and removal opens ignore
  broken sibling sidecars, preserve session-origin validation, and still refuse selected corruption; full Store/Inspect
  opens and project enumeration retain their malformed-inventory refusals. Ordinary worker and trusted Gate exec share
  the core scoped preparation boundary. A fixture restarting a bare gateway explicitly restores project sessions through
  maintenance before work; job admission never repairs port allocation. These witnesses acquire no physical disk-image
  capability.
- **MCP capability/auth codecs**: coordinator authority can be read only from the designated inherited FD/socketpair and
  never argv/environment/stderr; worker descriptors are 256-bit, 30-second, one-use, memory-only, atomic-consume,
  restart-invalidated, workspace/socket/peer-bound. Missing, expired, replayed, mismatched, or insufficient authority
  maps to dedicated JSON-RPC `-32005`, never sandbox-denied.
- **Structured stdin codecs and policy**: API/CLI/N-API/MCP discriminated inputs round-trip arbitrary binary inline
  bytes, streams, and normalized workspace-relative file paths without shell interpolation. Pure path cases reject
  absolute paths, traversal, symlinks, non-regular files, and ambiguous source combinations; job/trace DTO goldens carry
  kind, delivered bytes, completion, and optional relative path but never inline contents.
- **Built-addon scripted Node peers**: `cowshed:napi-test` keeps exact stdout, exit-code and empty-stderr oracles for
  binary delivery, typed refusals, attachment detach and job progress. Scripted Node fixtures retain `NO_COLOR` and the
  rest of their inherited environment but omit `FORCE_COLOR`, so contradictory outer Nx color settings cannot inject
  Node warnings. Nested Node peers inherit that coherent input; warnings are never filtered or suppressed.
- **Shell AST redirect eligibility**: use the production parser against a matrix proving only a simple literal
  in-workspace `>`/`2>` can classify as `Redirect`, and then only when a trusted supervisor-controlled actual writable
  descriptor interposes with exact combined-quota accounting. Append, fd duplication, pipelines, lists, subshells,
  expansions, symlinks, malformed/unknown forms, or missing interposition run ordinary shell semantics with no redirect
  capture claim. Spies forbid regex sniffing, polling/tailing, path reopen, and every hardlink operation.
- **Job-output quota state machine**: combined accounting includes protected plus in-flight bytes across memory,
  promotion, ordinary capture, and eligible redirect descriptors. It admits no byte beyond the exact boundary and
  transitions once to `output-limit`; tests pin TERM→grace→KILL→drain→seal→terminal-audit-record ordering and
  distinguish timeout/signal/exit.

Native process-metric fixtures that require a positive RSS wait for the measured child itself to report readiness after
exec and touching memory. A parent's report of the just-forked PID is not that evidence: the child can still report
measured zero before exec. One lifecycle keeps the live, exited-unreaped and reaped assertions; no sleep or retry
supplies readiness.

## Fixtures

A fixture serves a test of its suite's own contract. The fixtures and helpers that only a deleted dependency re-test
used are deleted with it, and a fixture stands in for a dependency only at that dependency's published interface: it
returns the results and refusals the interface can return and reproduces nothing behind it.

Every test fixture stays inside the effective grants of the ordinary exec it runs in. A Rust fixture whose paths are
used as spelled, such as a Unix-socket fixture, takes the one shared scratch parent (`scratch_parent` in
`cowshed-core/tests/support/temp_parent.rs`, which the gateway's fixture directory reuses): `COWSHED_RUNTIME_LINK`, the
workspace's short runtime link, when it is set, else `/tmp`. The parent is never canonicalized back to the long
`<mount>/.cowshed/run` path, and never the sandbox's `TMPDIR`, which lies under the cowshed store: either would put a
socket below the fixture past Darwin's 104-byte `sun_path`. A fixture that holds no socket and needs a canonical path,
such as a test `HOME`, takes the granted `TMPDIR`. An invalid requested alias fails loudly; it never falls through to a
global or temp directory.

RAM host fixtures own a test namespace (01_storage.md, "Host namespace") and derive everything from it: their root
`/private/var/folders/cowshed/<uid>/<ns>`, the fixture gateway's control socket `<root>/gateway.sock`, the volume paths
(`<root>/ram3`, `<root>/ram3-new`), the volume name `smoo-ram3-<uid>-<ns>`, the lock, and the exact attached registry
identities they record. No fixture path can be the installed gateway's or the user's shared RAM volume's, so no guard
compares them. Every attach and every facade or non-facade disk operation uses that real fixture gateway.
`cowshed-gateway`'s `ram_fixture <ns>` executable serves it until its stdin closes; host tests create the namespace's
root, start the prebuilt helper, wait for its socket readiness, detach only identities their own attaches recorded, then
close the helper. `disk-command --gateway-socket` routes native tools to the same fixture admission, with registry-bound
detach revalidation inside it. Size and UID distinguish fixture inventories but never authorize teardown.

A fixture's child directory is a real, non-symlink directory owned by the test process UID with mode 0700. Drop cleans
every path the fixture created. No fixture adds a grant, a host cache path, a script wrapper, or a sandbox disable. FIFO
readiness reads are nonblocking asynchronous reads: dropping a failed or cancelled wait closes its descriptor, never
leaves a blocking open behind during runtime shutdown. A fixture awaiting a helper's readiness also watches that
helper's exit, so startup failure reports the exited helper instead of waiting for a writer that no longer exists. PID
and release readiness frames are newline-terminated messages; their completion does not depend on every inherited writer
descriptor reaching EOF. Independent readiness publishers are read concurrently.

Process fixtures resolve real portable tools through the selected shell's PATH, not fixed FHS executable paths. A
version-probe fixture prepends its selected executable directory without discarding that toolchain PATH. Host API proofs
compare both successful answers and typed refusals with their CLI verbs; JSON failure diagnostics live on stdout. The
native disk-output proof in CLI `host.rs` belongs to its macOS-only `real_apfs_host` module and the existing
`cargo-test-group-real-apfs-host` lane. Portable runs still prove the disk runner's rename refusal and sccache role
isolation. Sccache's LaunchAgent status is macOS-only: Linux proves the same explicit launchctl-unavailable error
through the CLI and host API, never a fabricated running status.

An oracle on allocated bytes is judged against the same filesystem, never one filesystem's block count: a sparse-file
proof compares the file with a fresh sparse control of the same length created and synced beside it and reads zeroes at
its head and tail. Drain custody is proved directly: foreign file bytes survive, the held owned tree is removed, and the
replacement journal symlink remains. Do not rank owned and foreign trees by allocated bytes: compression and deferred
allocation can make a larger foreign file report fewer blocks even after sync. A test that names a file or directory
with bytes that are not UTF-8 requires a temp filesystem that admits such names; EILSEQ names that setup failure and its
exact root/path. No fixture invents a mount, a private parent directory or a skip for that refusal.

The shared Rust fixture guard has one constructor, `TempRoot::new_in(parent, prefix)`. Every caller selects its
already-granted parent explicitly. Independent controller-git sandbox fixtures use the shared canonical `/tmp` parent;
alias-owned fixtures keep their selected runtime alias unchanged.

A pure regression fixture runs inside an ordinary exec and covers both the HTTP and Unix-socket authentication cases. It
is not a real APFS mount or cache cutover proof.

The process-custody witness retires eight native subtrees concurrently while each retained sibling forks and execs 64
children. It joins exactly each selected root and child, observes every sibling complete its work alive, and never uses
group expansion. Controls omit all image and final-counter reads during custody and coalesced exit, preserve normal
EINVAL/credential-denial errors, and require unknown lineage and missing watches to refuse without signalling.

The real membership endpoint proof holds a same-real-user setuid ps behind an owned output pipe while two independently
admitted subtrees of one job retire concurrently. The public unique-id/parent-id record stays readable; the legacy
same-effective-uid combined BSD read must retain its actual EPERM. Both detached descendants join native exit watches,
the retained sibling completes its fork/exec work, and an unrelated process in the same checkout survives. Missing final
counters remain UnreadFinalUsage, never zeroes, and cannot replace identity, ancestry, watch or signal proof.

A whole job's membership read, exit watch and signal take the same public reads. The job-groups proof holds a
setuid-root `top` in a test-owned group: its membership record names root's effective uid, and its exact life is watched
and ended by SIGTERM. The combined read refused that member with EPERM, and with it the retirement of a job whose leader
exited while Nx's setuid `ps` ran: the job stayed draining and its client never saw its end.

A refusal that still happens must end the client, not hang it. The host-controller lane's
`host_controller_a_refused_retirement_answers_its_client_and_its_survivors_exits_conclude_it` runs a real supervisor job
whose member runs `lsof` as root through `sudo -n`. lsof's real uid is root's, so the kernel refuses retirement's signal
to it with EPERM when the leader exits; SIGTERM is ignored so sudo neither ends nor relays it before that refusal. The
parked `wait` and stdout follower both get that refusal, the job stays draining with its hold, and once the test closes
the file lsof watches, their exits re-run the retirement, which seals the job. It needs the same passwordless sudo grant
for `/usr/sbin/lsof`.

The exit-watch fallback is proven on the real kernel in the ordinary lane: registering launchd (pid 1, which no user may
SIGKILL) yields the withheld watch and the observer's own child the status watch. A scripted withheld exit folds no
status and the `UnreadExitStatus` gap. The host-controller lane's
`host_controller_a_root_command_run_through_sudo_stays_in_custody_with_its_status_withheld` runs `sudo -n lsof` under a
watched shell, which every cow sandbox denies: custody adopts the root lsof under its withheld watch without a refusal,
sudo relays SIGTERM, and both exits are joined with sudo's status read and lsof's withheld. It needs the host's
passwordless sudo grant for `/usr/sbin/lsof` and fails naming sudo's refusal without it.

Port-zero fixture children report numeric ports with explicit string writes to stdout, not console inspection. They
retain inherited color settings; the reader compares the exact undecorated decimal protocol, and never strips ANSI or
disables color to make an oracle pass.

Inside a workspace's port block, the gateway's own port `COWSHED_PORT_BASE` stays reserved: an upstream fixture binds
the first free odd-offset service port (`base+1`, `base+3`, …, `base+size-1`), never port 0 or a port outside the block.
Even-offset session ports are bindable only by the holder of their adjacent service port; every allocator follows this
same claim protocol. A fixture store is one: removing a detached workspace's gateway session unbinds its block's base
until the next attach, and every fixture store and test gateway on the host shares the fixture partition, so the store
claims the block's first service port before the removal and lets the claim go once the workspace's own session is
installed again, or hands it to a reservation of the block, which releases it at publication like its other listeners
(05_gateway.md). Inside the block, a fixture that needs a port where nothing listens holds a service port and hands out
its session port, which it leaves unbound; it never hands out a port it bound and closed. Outside a block no shared
allocator exists to claim against, and such a fixture hands out a port-0 listener's port after closing it. A block
exported without its size, or the reverse, fails loudly by name. Inside a Cow test gateway, `free_endpoint` allocates
aligned two-port sub-blocks starting at `base+2`, never the reserved real gateway `base`; each claimed service port is a
single listener, and its claim is retained for the listener's lifetime. Outside a workspace the fixture partition
(`MACOS_FIXTURE_PORT_MIN..=MACOS_PORT_MAX`, one 4096-port block aligned to its size) is the enclosing block: the test
allocators take its aligned two-port sub-blocks from `base+2` by the same claim protocol, never a host-partition port
production allocates from. The private actor fixture installer claims the adjacent service port first, then the session
port in one fork-lock creation step. The service-port claim remains held until the installed listener is quiesced. It
excludes other fixture installers across test processes throughout the session port's release/rebind handoff; requests
and the other tests remain parallel. Both claims use the existing fork-lock `Fenced` value (11_shell.md): socket
creation and release wait only for concurrent spawns in flight, so another fixture's pre-exec descriptor copy cannot
outlive the release/rebind handoff. No process-local installer mutex is needed.

An exact-count network fixture, including single-shot HTTP and TLS backends, keeps its listener owned until the test
releases its fixture handle or its joined result, even after its accept loop finishes (including a zero-count loop). A
completed task retains the listener as its result. Finishing the expected requests must not make a still-used fixture
port reusable by another test.

Host-controller tests that need the global identities of the real host (the shared port range, the real gateway socket,
the host's own store) name each such identity explicitly in the test. No blanket bypass of the workspace's block or
grants is permitted.

A fixture that waits for a file or directory to change waits on a kernel watch it armed itself, never on a timer and
never on Bun's `fs.watch`. On darwin, Bun 1.4.3's `fs.watch` is one FSEvents stream per process. The stream starts
since-now after the call returns and restarts whenever a watcher is added or removed; it missed 24 of 1000 single
creates made 0-100 ms after the wait was armed. The nx-plugin tests use `whenFound` and `whenChanged`
(`packages/nx-plugin/src/__tests__/path-change.ts`). It runs a waker process that holds a kqueue `EVFILT_VNODE` watch on
darwin or an inotify watch on linux, and writes one byte once the watch is armed and one after every event. Each byte
re-judges the whole predicate, and the first judgment comes only after the watch is armed, so no change can fall between
a read and its wait. Judgments run one at a time; a change reported during one brings another after it. A watch that
cannot be armed fails the wait with the syscall and its errno; it never falls back to another watcher. A caller that
stops waiting aborts the wait. Either way, the waker is killed and its exit awaited before the wait settles, so no waker
outlives its fixture. The leg's watchdog stays the only hang bound.

Known limitation: dead-run reclaim of the shared Nx fixture root enumerates live processes with `ps`, and the installed
cowshed workspace denies that spawn, so reclaim cannot run there. A focused hook proof may use the host's direnv only to
reclaim; that is neither a subject grant nor a cutover claim.

## Host-controller authority

Run `nx run cowshed:host-controller-test` from the owned checkout in an unsandboxed host-controller shell. This single
uncached target selects `test(/host_controller_/)` across `cowshed-core`, `cowshed-cli`, `cowshed-shell`, and
`cowshed-gateway` with `--run-ignored only`, without a second list of fixture names. Its `build` dependency, the
existing inferred host build aggregate, completes first and refreshes the JS and host-platform native addon the worker
proofs load, so they never load a stale `dist/native` addon from an earlier build. Its owning `cargo-test-archive`
dependency compiles once before the run, and the target hashes the archive output bytes. The host runner consumes that
prebuilt archive with `--archive-file` and remaps its workspace to the exact owned checkout; it never recompiles under
the test guard or lets its private runtime `TMPDIR` change compiled artifacts. An empty selection fails.
Controller-owned filesystem and kernel-profile fixtures carry that prefix and an explicit ignore reason naming this
target. Ordinary sandboxed nextest runs report them ignored: an enclosing executed-child profile cannot grant an inner
supervisor independent authority. Pure policy tests and pre-spawn refusal tests remain in the ordinary lane. The host
runner probes actual hard-link authority before running and refuses with the same actionable command if an enclosing
sandbox denies it; neither a write grant nor a nested profile can undo that denial. It pins `TMPDIR` to a unique
disposable directory under the exact owned checkout's `.cowshed/tmp` and removes it afterward. Unix-socket fixtures use
the existing short owned `ScratchRoot` so Darwin's socket-path bound is independent of that private `TMPDIR`. Fixtures
use only disposable local data and never change installed host services or checkout source. The descriptor-inheritance
proof launches only its unique one-shot agent from the generated gateway plist, holds 10000 descriptors with finite
`OPEN_MAX`, and unloads that agent on every exit path.

Workspace supervisors and children deliberately deny `file-link`; this host-controller proof cannot run inside them.

An exit-watch fixture waits for an executed-body readiness byte before its parent captures the process identity and
registers the watch. The parent holds the child's input gate until registration; returning from spawn alone is not
readiness. Its actual parent retains the child unreaped through the exit observation.

The inherited-shell equivalence fixture (`cowshed-shell` `shell_pool.rs`,
`host_controller_a_clone_adopts_its_origins_shell_and_matches_its_own_evaluation`) is the two-root test 11_shell.md
"Inherited shells" rests on. Workspaces of one tree at different roots, port blocks and runtime links: the origin
evaluates; the first clone carries its record, may not adopt it uncertified, evaluates and, agreeing, certifies the
origin's record; the adopter then carries that record and must serve its first command without evaluating; the evaluator
has no record and evaluates. The adopter's and the evaluator's job environments, with each fixture's own root and
runtime link replaced by fixed names and the per-job and per-port-block variables left out, must be equal, a credential
from `.envrc-local` and `-I`/`-L`/`-Wl,-rpath` paths included, and the record must hold no credential value. A workspace
with one changed input byte must evaluate and say why. A shell exporting a hash of `$PWD` must never certify: its later
clone evaluates, says its producer is not certified, and holds its own hash. The fixture's shell is cowshed's own
`.envrc`, not a repository's; the sandboxed lane covers the record, key, move, certification and host protocol
(`shell_inherit.rs`, `script_host.rs`) without it.

The build-state CLI probe launches the installed `cowshed` binary through private `tools/bin` beneath the executed-child
profile in both read-write and read-only modes. Its program link and resolved executable have literal read access with
metadata-only ancestors, matching the installed-binary contract in the package guide. The probe requires the checkout's
recorded build-state paths and refuses controller-data reads and listing the executable's parent directory; it grants no
controller socket or controller-state authority.

Process-observation fixtures hold a child and grandchild under their root with FIFO gates and independent PID/parent
oracles. The real served supervisor and trusted N-API embedder read the same retained birth-order records and exec/exit
events. Dropping a stream leaves the workload running; a later subscriber replays the same history. Their sole
supervisor trace sink seals three spans with the observed parent edges, one argv display per start, one terminal usage
row per life and the exact generated fourteen-column schema (07_api.md, "Process-tree observations"; 13_telemetry.md,
"Process-tree spans"). A separately counted burst of reaped grandchildren reports retained and missing births from its
fixture oracle, never from CPU, and preserves the explicit coverage gap through both public API rows and the terminal
fixed `job.unattributed` row. Its named signed differences and unavailable I/O survive decoding without clamping or zero
fill. The job-only host, volume and charged-memory boundary columns come from their one annotated record through the
same generator. Boundary round trips preserve signed deltas, absent build/charged memory and the exact `u16` core count;
positive schema controls reject a widened core column or an added process-specific column. The admitted APFS-baseline
reader control is macOS-only; the fixed-column encoding and schema controls remain in all-targets Linux cross-checks.
The supervisor's `host_controller_caller_supplied_nx_endpoint_and_read_only_flag_never_reach_the_child` exercises the
real host-side environment builder: a caller's Nx endpoint and `READ_ONLY=false` never reach the child, which gets the
workspace's own endpoint and `READ_ONLY=true`. Creating its independent global short runtime alias requires this lane.
The fixture keeps its directory guard under its selected granted parent, supplies the canonical owned workspace mount to
capability containment as the controller does, and removes its verified runtime alias on return or panic. Its success
path asserts the alias is gone; it neither changes the production detector nor adds a sandbox grant.

Sandboxed TypeScript fixtures cannot allocate the host-controller's Rust-only `ScratchRoot` under world-shared
`/private/tmp`. They use the same short-parent-link pattern in the job's already-admitted socket namespace: a short
alias points to real socket leaves inside the fixture's own root, outside every checkout. The final socket directory is
not a symlink. Workspace ownership is established by physical containment, so the foreign-workspace guard stays
unchanged. The fixture stops its daemon before unlinking its verified alias and removing its root. Bind and connect are
proved under the real sandbox's long store `TMPDIR`, not only a host runner's short temporary directory.

A sandboxed Rust fixture whose root itself must be short, because a real Nx daemon binds its sockets inside the root,
takes it under the fixture namespace of 04_sandbox.md, "Fixture roots under /tmp": `/tmp/cx<pid>-<two alphanumerics>`,
named for the pid of the process that owns it and reclaimed by pid once that process is gone, under the
`/tmp/cxsweep.lock` that serializes the reclaiming. The grant is proved at the kernel in the host-controller lane,
because a nested profile cannot add what an enclosing sandbox lacks.
`host_controller_seatbelt_admits_only_pid_named_fixture_roots_and_the_sweep_lock_in_tmp` makes, writes, renames, lists,
binds a socket in and removes such a root under the supervisor and child profiles, and refuses its lookalikes, a write
beside it and an escape from it. The probe removes every entry it made in `/tmp` on every exit path, a failed assertion
included, and leaves the shared lock, which belongs to every sweep.

Nx backend host-controller fixtures run the package's server bundle, `dist/server/nx-cache-server.js` from
`nx run nx-cache-server:bundle`, under the `bun` on the test's `PATH`, bound on an ephemeral loopback listener owned by
the fixture. The gateway's land case, `host_controller_a_landed_gate_publishes_and_another_shed_restores_it`
(`nx_land_publish.rs`), and the gateway's process case,
`host_controller_nx_backend_process_serves_and_releases_its_store`, are separate; each names one controller-owned
backend. Neither grants a broader host bypass.

Git discovery fixtures exercise the restricted discovery profile, including denied includes, alternate object stores,
and worktree metadata grants. The probe executes Git through the system-selected developer directory, not the
`/usr/bin/git` launcher: the launcher requires xcrun host-cache writes even for read-only operations. Publication
fixtures provide the existing `.cowshed` directory that workspace creation owns.

Nx-backed host-controller probes share `tests/support/nx_reads.rs`: it walks the checkout's pinned Bun-installed Nx
package and its reachable package-private namespaces, admitting only the required external Bun-store payloads and
namespaces. Patched packages remain local to the checkout; no generated release closure, binary copy, hoist or
whole-HOME grant is introduced. Both CLI volume walks and the native-file-watching walk use that same helper. The
watcher walk advances on the child's ready stdout and the allowed change's native callback, not a guessed
subscription/debounce sleep or repeated flush polling. It publishes the denied private control first, then atomically
renames the allowed source change into place, drains pending events and stops the watcher before retaining all privacy
and notification assertions. It carries no deadline of its own: a callback or line that never comes is a hang the
target's no-progress bound names, and the walk never retries or enables diagnostic flags to make the test pass.

The CLI's committed checkout templates share one immutable copy of the repository's installed Nx namespace per runner,
regardless of whether a checkout declares outputs or also declares a Rust build volume. The first writer materializes
the real Bun dependency closure under the existing template lock and publishes it by rename; later checkout writers
clone that published tree on the scratch volume, without rediscovering or recopying the repository installation.
Crossing from the repository's volume can require a byte copy, but changing fixture shape cannot repeat it. A
deterministic regression removes the source after the first materialization and requires the second checkout to preserve
the installed bytes and relative links with exactly one writer invocation.

An adoption no-progress report alone does not identify a missing lock or daemon event. In the instrumented foreign
database-opener lifecycle, namespace materialization spent 133 seconds copying before the lock holder was even started;
its ready line, both land replies, holder exit and gateway drain all subsequently arrived. The no-progress watchdog
observes the runner's test-finished event, not those internal phases. Sharing the immutable installation removes
repeated setup work; it does not turn elapsed setup time into a heartbeat or relax the watchdog's bound.

No outer sandbox installation or permission change is required for this target: the authority boundary is unchanged.
Updating the checkout's sandbox source cannot change an already-running outer supervisor; separately testing a changed
runtime policy requires the controller to install the intended release and start a fresh supervisor. Never broaden
`file-link` to make this proof run. The real-APFS tests attach real images through DiskManagement in one macOS serial
group, excluded from every ordinary and exceptional shard. A selected subset is not their full proof: run the owning
default Nx lint, test and build of `cowshed` (`nx run-many -p cowshed -t lint test build`, default parallelism, no
per-lane selection) from an unsandboxed host-controller shell. A sandboxed run cannot attach images and is not evidence
for those tests. Run the explicit `host-controller-test` target separately: it covers controller fixtures beyond the
library's real fixtures. Both are mandatory proofs; an ignored controller fixture in the ordinary chain is never
evidence that its behavior passed.

The ordinary core lane also covers port allocation's publication handoff. A deterministic regression snapshots native
inventory, lets another allocator reserve a block, publishes its workspace metadata, and releases its reservation before
the stale allocator resumes. The stale allocator must claim then re-read current inventory under its reservation guard,
reject the now published block, and choose another. Inventory errors must release the newly claimed marker; successful
reservations remain held until their owner's publication finishes.

An occupied port inside an otherwise unclaimed block is planted with a real loopback listener: the allocator must skip
that candidate, and the guard must close every probed listener before another allocation may reuse the block. A second
regression claims the published gateway base _after_ allocation and proves the real daemon rejects its bind, the
controller rotates the durable grant to another block, and the new endpoint answers with the updated revision.

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
- **Bounded image release** (16_build_volumes.md, "Garbage collection"; 05_gateway.md, "Deleting released images"): over
  generated run layouts and step bounds, every cut shortens the file and frees at most one step of runs, the cuts end at
  zero and free every run exactly once in as few cuts as the bound allows, and a release resumed after any cut plans
  exactly the cuts left. A table fixes the cleanup's timed cut size (`next_cut`): the last cut's rate carried to the
  target, at most twice the last cut and at least 64 runs, an overshoot shrinking it at once. Its real boundary is a
  fragmented file on the host's filesystem, cut a few steps, abandoned, and finished by a second release that counts
  only what was left. The deleting directory's crash points are witnessed the same way. A release returns with its entry
  whole and unlocked and frees nothing, whatever it releases (a build volume, a removed workspace's image, an orphan
  session or staging image, an expired checkpoint, a failed restore's staged clone) and whether or not a gateway takes
  its nudge: the image is off its name, every byte waits in `.deleting`, and a retry finds the commit already made,
  makes its rename durable and answers. A link in place of the deleting directory refuses. A directory store's release
  of a tree a process holds anything inside answers with the tree committed whole to its `.deleting` directory, deletes
  nothing and nudges nothing, and `finish_build_releases` leaves a build-volume tree while it is held and deletes it
  once it is not. An unsynced native entry still requires all rename syncs before its owner's retry answers. Directory
  regressions replace the deleting directory's pathname after acquisition: neither retry confirmation nor the finisher
  may follow the foreign tree. The finisher syncs both held directories while the unsynced entry is still intact, then
  deletes through those descriptors; a replaced opened child and foreign symlink targets remain untouched. A real
  self-held payload stays, but the finisher's own observation does not retain it.
- **Released images are nudged to the gateway and freed by its cleanup** (05_gateway.md, "Deleting released images"). A
  release through a store whose configuration names no gateway (a function call over a fixture store no test gave one)
  returns with its entry whole in `.deleting` and says so once per process. The nudge is a function call to a listener
  that plays the gateway: `deleter::nudge` writes one `{"op":"released"}` line with no entry and no counts, half-closes
  and reads the answer; a listener that refuses, never answers within 2 s or is absent leaves the entry whole, said once
  on stderr, and the release still answers. The startup listing is a function call over a scratch store with released
  images planted in each project's `build/.deleting` and `sessions/.deleting`, in an archived project's
  (`.retired-projects/<operation-id>/build/.deleting` and `…/sessions/.deleting`) and in the store's own `.deleting`,
  workspace and build images alike: it lists them all, leaves a legacy `.releasing` alone, names each stray and each
  unreadable directory, leaves every other file of an archive untouched, skips the receipt files beside the archives,
  and frees and syncs nothing. The pass leaves an entry another process holds
  (`the_cleanup_cuts_in_steps_and_never_cuts_a_held_entry` in release.rs), and a deletion that stops after its first cut
  is finished by the next listing from the file's length. The unbinding moves each released image of its project into
  `<store>/.deleting`, nudges the gateway for each one and frees none, an entry the cleanup holds moving with it. A
  workspace image left in `sessions/.deleting` does not delay project recovery, which reconciles the record its rename
  left imageless without waiting for the image; the gateway's next listing finds it.
- **A supervisor's opening finishes nothing and waits for no image lock** (07_api.md, "Authority model"): for any
  generated workspace and probe name, a `Supervisor` opening observes what a `Workspaces` opening naming that one
  workspace observes, replays no intent, selects no rename and has no retirement target; over generated scopes it and
  `Inspect` are the only openings that finish nothing, and an opening that finishes nothing replays no intent. Its real
  boundary is a directory-backed project whose main image lock and served workspace's lock other processes hold, beside
  a corrupt sibling sidecar the store-wide pass refuses and an unrecorded earlier generation of the served workspace in
  the trash: the supervisor opens, serves its settled mount and answers `hello` on its socket while both holders still
  hold; neither it nor an ordinary opening naming the workspace once the holders let go deletes the trash image, a
  recovery image retained without its record (02_workspaces.md, `rm` step 5). A second boundary detaches the served
  workspace and holds its lock in another process: the supervisor, a process of its own, says once on stderr that it
  waits for that lock, naming the holder's PID, and mounts and serves only after the test releases the holder, its
  socket's publication waking the test.
- **Stateless restore recovery boundaries**: interrupt after undo-sidecar publication, each image/CA-key swap and
  rename, each canonical/undo parent fsync, pending metadata rename/fsync, staged-metadata removal, and final parent
  fsync. Restart twice from every fixture. Recovery must derive rollback/forward state only from APFS image, detached
  metadata, grant/CA, and `.restore.json` facts; converge idempotently; accept only the corresponding token/incarnation;
  and leave no `.restore-fences`, runtime journal, or second mutable fact (the restore audit record may be emitted more
  than once across retries — it gates nothing).

## Integration tests (real substrate)

Parametrized over the substrate the host provides, and never gated off on it: a missing capability is a failure, not a
skip. On macOS these are the `real_apfs_*` tests, compiled only for macOS: small `.dmg` images (1 GiB caps) under
`/private/var/folders/cowshed/<uid>/<ns>`, the directory of the test namespace each scratch root mints (01_storage.md,
"Host namespace"; the label is said on stderr, not spelled, so sockets below fit `sun_path`), driven by the production
host — real `diskutil`/`hdiutil`/`fsck_apfs`/`mount_apfs`, descriptor-native `funmount` and the live kernel mount table.
The root's volumes carry a test label, never production's `[cowshed]`: a clone carries the shared test template's
`[cowshed-test]` until its workspace's supervisor relabels it with the namespace's prefix `[cowshed-test-<ns>]`. The
per-user root is host state `cowshed setup` provisions, never fixture state: a fixture creates only its namespace's
directory and `t` inside it and refuses a missing or foreign root, so a host lacking setup gets setup's same directories
from the Nx target `cowshed:apfs-test-root`, which every such test target depends on (below). A mount state a test needs
(wrong flags, an impostor volume, a busy holder) is produced on a real volume, never by a substitute mount source. Every
scratch root selects its backing image paths, then calls the same production owned-attachment cleanup used by normal
removal, recovery-marker reads, reverse-order teardown, duplicate-attachment rejection, and staging GC. Each cleanup
rereads image/whole-device/volume identity and retains each owned volume's raw pin through namespace admission and
same-root-FD `funmount`, then runs guarded `hdiutil detach` of the exact physical device under the image's lease. The
detach guard rechecks after storage admission; a cached device number, caller mount set or mountpoint never authorizes
an effect. A failed release keeps the scratch tree intact for the next run, rather than deleting a still-attached
image's backing files. Before any release, a scratch root ends every process working in its tree, selected by the
working directory the kernel reports (`proc_pidinfo` `PROC_PIDVNODEPATHINFO`, which still names a deleted directory):
`SIGTERM`, as `nx daemon --stop` sends it, then `SIGKILL` after a 10s grace. A stock Nx daemon a fixture started
detaches from its starter, so a failed test's unwinding otherwise never reached it and it kept running in the deleted
root. Successful cleanup removes the root, and every run first ends the processes working under, then reclaims the roots
and attachments of, runs whose owner pid is gone. On Linux: a scratch ZFS pool on a loopback/file vdev
(`cowshed.itest.<pid>`) with datasets destroyed and the pool exported on teardown; the Linux leg also exercises
`cowshed-helper` and the Landlock/netns exec path. A suite-level guard reaps leaked `cowshed.itest.*` volumes/pools. The
same flow table runs on both; substrate-specific assertions (fsck step on APFS, origin-snapshot GC on ZFS) are tagged.

The Nx target `cowshed:apfs-test-root` (`packages/cowshed/scripts/apfs-test-root.ts`) gives a host `cowshed setup` has
not run on, such as a CI runner that cannot answer setup's interactive authorization prompt, setup's own three
directories: `/private/var/folders/cowshed` (root:wheel 0755), the user's `<uid>` directory below it and `mnt` below
that (the user's, 0700). Every target that runs a test creating a scratch root depends on it: the cargo test shards of
the `cowshed-*` crates, both real-APFS groups, `host-controller-test` and `integration-apfs`. It creates only what is
missing, by `sudo -n` for the prefix and the per-user root and as the user for `mnt`, refuses a link, a non-directory or
a foreign owner rather than writing through it, and with a directory missing and `sudo -n` refused fails naming the
exact commands to run. It never looks at `t` or a test namespace's directory: the fixtures make them, and a
`cowshed exec` sandbox, where the shards run too, denies it. On a host setup has been through it checks and changes
nothing.

The panic/kill cleanup fixture reads only its own child's stdout pipe. Before ending that child it requires two complete
line-start image records (the mounted RAW image, then the retained RAW input) and the worker record; neither a sibling's
output nor a host inventory count substitutes for either record. The child runs libtest with `--nocapture --quiet`:
terse output prevents the single-threaded pretty harness from prefixing the first record with `test <name> ... `. This
matters under nextest's inherited `RUST_TEST_THREADS=1`, even while independent parent tests run in parallel. The strict
two-record assertion and exact-root teardown remain unchanged; no delay, retry, dropped assertion or broad sweep repairs
a framing failure.

Owned-image command-boundary controls pin current provider-tree projection, excluding a foreign/stale cached BSD
descendant before any command effect, and retain moved/reused-device refusals. Fixture injectors resolve their recorded
backing image through the typed attachment projection: its physical whole device and synthesized container are distinct,
not two attachments. A lost pre-pin attachment must refuse after one attach with zero fscks while the foreign
replacement remains mounted. The kernel does not promise the freed BSD unit to the foreign attach (another attach on a
shared host can take it, or a lower free unit can), so the injector makes the reused spelling exact: once the owner is
ejected, its recorded raw volume opens the foreign replacement's real raw volume, and the test requires that pin to have
happened; attachments, inventory and mount table stay the kernel's. Raw-pin attacks prove the deliberate
pre-fsck/pre-mount state from the kernel mount table, then require exact-physical forced detach to refuse Busy; they do
not ask an already-unmounted volume to unmount. Mount requires the exact single owned APFS volume, pinned fsck before
the first mount and that proven raw pin held across the host lease wait; a failed fsck or mount never earns release
grace or retry. Option controls retain read-only/visibility/owners/noatime and the separate event-store-repair
`nobrowse,noowners` state. Unmount controls remove the exact volume during simulated namespace admission and require a
post-grant kernel query with no native effect. Foreign/non-root held directories and stale filesystem identities refuse
before mutation. Kernel-read and postcondition failures remain distinct errors; disappearance during grace is never
reported forced. A fresh real kernel mount is immediately released through the production owner, then unmounted again
without releasing its image attachment between calls. The real boundary proves the same event-only FD's
root/source/FSID, descriptor-native naming compatibility, unforced `funmount`, retained post-FD `ENOTSUP` and
independent original-FSID absence; an errno or constructed fake state alone is not the oracle. Native exact-`EBUSY`
policy controls retain monotonic grace and intent-specific force authority, while permission and other failures never
earn grace. Exact physical-device `hdiutil detach`, post-grant guards and unchanged command named-dissenter/busy parsers
remain covered. Real image teardown still proves the complete mounted RAW image plus unmounted, read-only retained RAW
input panic/kill walk. An isolated pass after operator cleanup is not a repair verdict when the unchanged full gate
repeats the failure; preserve both historical witnesses and exercise the real command cutover. The lease overlap fixture
also drops B's returned handle before cleanup to prove exact-image recovery. A no-device cleanup control holds the
backing-file lock: unlink refuses and preserves its bytes until an exclusive no-follow open proves it unlocked, then
cleanup accepts both that file and an already-absent file without spending an image attach.

The killed-caller attach fixture captures helper PIDs from the existing I/O Registry creator walker, joined to its exact
backing image before release. After its own attach/release operations finish, two reads must find no orphan among those
captured PIDs; unrelated helpers and disk tools do not decide this owned verdict. Pure controls prove both that a
foreign orphan is excluded and that an owned helper whose user client disappeared still fails. The host-wide helper
census remains a doctor/host-limit observation, never this fixture's acceptance oracle.

The manual macOS example `cargo run -p cowshed-cli --example stable_mount_root -- <new-receipt-directory>` exercises
setup's production fixed-root authorization transaction, then the production APFS backend directly: it mounts a scratch
image beneath the fixed root, reads a payload written there back through a preferred-path symlink that points to the
canonical root, and detaches the image. No CLI parser, project controller or gateway runs: the controller's no-follow
validation refuses a symlink beneath a fixture store, so the alias lives in the receipt directory. The fixture persists
no host configuration; the root under test is the one `HostConfig` derives, and existing live host configuration is
checked byte-for-byte and never changed. The example requires operator approval and a quiet native-fixture window,
rather than a skipped test or an administrator prompt in ordinary CI. It records the provisioned directories' actual
ownership/modes and, before mount, while mounted and after detach, four-class kernel censuses, mount/device mappings,
registry IDs, and the running mds process's PID and rights. Growth in any departed-object class, or an mds restart,
fails the proof; retained raw records permit per-image attribution when another host process interferes. Teardown
releases only this fixture's images and refuses to delete a remaining mount. Passing this example proves fixed-root
placement and preferred-path alias reads for a production-backend mount, not a CLI lifecycle, operator migration of a
retired custom root or repair of historical leaked objects.

Lifecycle integration that does not assert a physical-volume property uses the same project controller and lifecycle
executor with `DirectoryExecutionHost`. Its workspaces and build volumes are real directory payloads, cloned through the
production tree-copy path; atomic filesystem bindings expose them at checkout and build paths. Git, Nx, gateway
connections, command supervision, lock contention, publication records and recovery journals remain real. These
`directory_*` tests and the clone-intent recovery binary run in ordinary Cargo test shards without creating or attaching
a disk image. Directory stores report actual tree allocation, not fabricated block-device counters; volume labels,
physical resizing and read-only mounts remain native-host tests. Their scratch roots use
`/private/tmp/cowshed-dtest-<pid>-<n>` and name their test in the root's `label` file, not its path, so every Unix
socket a store binds beneath one fits macOS's 104-byte `sun_path` whatever the test is called. They share process
cleanup with the image fixtures, but neither their setup nor their cleanup enumerates or releases disk images.

Cargo lifecycle fixtures run the real Cargo that drives the test: canonical `CARGO`, never a `cargo` found on `PATH`,
where an earlier install's link may still answer to the name. `rustc` and `rustdoc` resolve by Cargo's own rule
(`RUSTC`/`RUSTDOC`, else the first executable on an absolute `PATH` entry, canonicalized), since a Nix toolchain's Cargo
sits in another store path than its compiler. Nothing installed answers to `cargo`, `rustc` or `rustdoc` (03_caches.md,
"Cargo reads its budget from its holder"). Cargo lock fixtures still invoke ambient `cargo`: after build-script
readiness they independently report its parent PID and Cargo executable, check those against the kernel executable/argv
and the entry process's parent edge, retain its kernel process group, and match the lock refusal to that actual Cargo
process. An observed FIFO — one a holder configured — is checked as a live kernel FIFO only its user can open. Existing
profile locks, the newly-created-profile control, seed preservation and native/Directory lifecycle oracles remain
unchanged (16_build_volumes.md, "Targets and seeds"). The native build-volume holder subprocess uses
`--nocapture --quiet` so inherited single-thread libtest formatting cannot prefix its actual-PID record. Its
kernel-holder, forced-release diagnostics, checkout-label ownership and reclamation assertions remain exact.

A job's jobserver is proved against its own contract with no Cargo and no compile: the supervisor's unit tests hold what
a job's command receives — `CARGO_MAKEFLAGS`/`MAKEFLAGS` naming the job's own fifo, `BOUNDED_EXEC_CPU_TOKENS` and
`RUST_TEST_THREADS`/`NEXTEST_TEST_THREADS` at `g` unless the caller named them, a caller's own jobserver variables never
passed — and a fifo of `g − 1` bytes. How Cargo reads a jobserver is Cargo's own to test; an end-to-end proof with real
Cargo belongs in a nightly, not the gate. The `bounded-exec` executor suite clears every inherited sizing variable for
its own run, so the count of the job that runs the suite is no ceiling on the runners its cases size.

Directory build-state freshness uses observed filesystem change times, not synthetic wall-clock marks. If a listed child
or subtree disappears during a live scan, the nearest surviving ancestor records that removal; the root uses its
modification time so binding renames do not count as build writes. A missing root still refuses, and other I/O errors
name the actual path. The deterministic listing-then-removal regression checks those clocks and root refusal;
`directory_adoption_copies_nothing_from_the_target` proves an adopted volume indexes only the landing workspace's own Nx
entries and holds none of the target's.

Re-adoption has independent real walks for RAW replacement and prepublication RAW recovery. Each interrupted walk
resumes the recorded replacement through `mount main` and ordinary startup without issuing a second replacement or
retained generation, preserving the checkout and warm build-volume identity.

The Linux job-accounting fixture is a separate kernel-counter proof, not a production ZFS workspace-admission proof. It
runs from the ordinary runner through a passwordless-sudo delegated scope; only its parent prepares and cleans up a
test-owned ext4 image on a declared root-only loop device. Delegation of cgroupfs does not confer mount authority. The
fixture records actual role credentials, capabilities, namespace and kernel and consumes only the runner's narrow ext4
mount mediation. A missing device or refused mount fails explicitly, never skips or substitutes a tmpfs page-cache
claim. Its ordinary controller must be denied device control. Kernel mount ID/device/backing identity is observed even
on a mount command error; unmount is reconciled against kernel state before removing the image. Explicit cleanup returns
release and diagnostic-write failures, while destructor fallback remains non-panicking and attempts every safe owned
release step. Its fallible stderr/stdout reporting cannot guarantee delivery when both channels are unavailable.
Unmount, association absence and image/directory removal are verified after successful workloads, refused-command unwind
before/after mounting, and setup failure after actual mount acquisition. Held CPU/RSS/I/O controls require the sole
retained parent and every stated proc counter; missing observations fail rather than becoming zero. CPU comparisons use
the selected child's complete `wait4` lifetime, not a pre-exit self-report; cache/I/O assertions compare direct counters
and retain their isolation, allocation/cache-hit and kernel-terminal empty-census negative controls.

Test tasks run under `@smoothbricks/nx-plugin:bounded-exec`, which on a macOS host gives each task a `TMPDIR` lease on
one RAM-backed volume (`packages/nx-plugin/README.md`). Real-APFS scratch roots stay in their namespaces' directories
below `/private/var/folders/cowshed/<uid>`, and the user's template, the lane pools and the sweep's lock and record in
its `t`, whatever `TMPDIR` a runner has: every run's sweep must see every dead run, not only those of one task's lease,
and finds them as the per-user root's entries that parse as a test namespace whose owner is gone: its pid names no
process, or names one born at another instant than the start time the root's `.owner` record holds; a clone reaches its
template by `clonefile`, which cannot cross volumes (that root is on Data, as the template is); and Darwin's socket
bound must not depend on the runner's `TMPDIR`. The roots are beside the workspace mount root because Spotlight's `mds`
admits no volume whose mount path contains `/var/folders/`, and keeps one `AppleAPFSVolume` right for every volume it
admits until it exits: a fixture volume mounted under `/private/tmp` left one each (01a_apfs.md, "Spotlight media
references"). Production names only `mnt` and `ram<revision>` there and reads neither `t` nor a namespace's directory.
They need the per-user root `cowshed setup` makes; `t` and each namespace's directory are created 0700 and refused
unless the user owns them with that mode. The lease never detaches an image attached from below it; it keeps such a
lease and names it.

These canonical scratch roots, the `/private/tmp` directory fixtures, and the canonical `/tmp` parent of the independent
controller-git sandbox fixtures, are host-only and run in the land gate; a `cowshed exec` sandbox denies `/private/tmp`.
No granted parent replaces them there. A fixture that runs controller Git or a supervisor under a profile of its own
cannot apply that nested profile (`sandbox_apply` fails with `EPERM`). The sandbox's `TMPDIR` lies under the cowshed
store, so controller Git refuses to grant it, a directory store inside it is refused, and it is too long for the sockets
below a fixture store; the canonical runtime link, `<mount>/.cowshed/run`, is too long as well. A host shell whose
`TMPDIR` is the workspace's exported exec temp directory would meet the same refusals, so the parent stays `/tmp`, not
`TMPDIR`.

On macOS, disk-image coverage runs in the dedicated root `cargo-test-group-real-apfs` target, selected from the
`cowshed-apfs-tests` crate. Its source inputs are that crate's transitive local Cargo dependency closure:
`cowshed-apfs-tests`, `cowshed-apfs`, `cowshed-os` and `cowshed-gateway-types`. The closure includes the image fixture
pool and shared process-cleanup support. Cargo manifests and lockfile, effective Cargo configuration, nextest policy and
the run-wrapper are inputs too; unrelated crate sources and the workspace archive's content hash are not inputs of this
verdict. The test-only crate depends on the public device API; the host does not depend on the test crate. Changing a
device scenario invalidates only the device verdict, changing the APFS implementation invalidates both device and host
verdicts, and an unrelated source change preserves both cache keys.

Native-host integration whose subject combines physical images with core lifecycle state runs in
`cargo-test-group-real-apfs-host`, with its own core/CLI Cargo dependency closure. This includes actual mount and label
assertions, image resizing, re-adoption and native recovery boundaries. The host target depends on the disk-image
target: the two groups never run concurrently. Each nextest group declares `max-threads = 1`. `.config/nextest.toml`
owns their package-scoped selection filters, and the code-time wrapper rules select the same tests. Ordinary and
exceptions shards exclude those filters. The root `host-e2e` target depends on both groups and on
`cowshed:host-napi-test`; the root `gate` target depends on every project's `lint`, `test` and `build` plus `host-e2e`.
The declared land gate (`tooling/land-host.sh --gate`, `[land] gate`) runs
`nx run-many -t gate --parallel=$(sysctl -n hw.ncpu)` after rebasing and stopping the workspace's daemon. **The landing
gate is never CPU-limited**: it runs at the core count, at gate class (05_gateway.md, "Classes"), with no QoS clamp, no
I/O throttle and no outer deadline, because it is the run that moves main. Measured 2026-10-16: the same gate took 40
minutes capped at `--parallel=2` under `taskpolicy -b` and 4m44s uncapped, and two per-test timeouts that reproduced in
every capped run did not reproduce once the cap was removed - the failures were the cap, not the tests. Exploratory runs
are the ones that get limited, and they are limited by being stopped rather than by being niced. This reaches the same
checks as naming the ordinary targets, both host groups and `cowshed:host-napi-test` explicitly, but Nx owns the target
list. Ordinary `test` remains free of the expensive host groups and of that proof: Rule 2 keeps the inner loop cheap,
while the explicit land gate includes host coverage and each proof replays from cache unless its own inputs changed.
`nx run-many -t host-e2e` is the focused host proof. Every test has one runner, and non-macOS inference supplies neither
APFS runner; package-declared placeholders and explicit dependency edges may remain.

`cowshed:host-napi-test` is the built addon's host proof, run against this checkout's launcher and CLI binary, which is
why it depends on `build`. `host.napi.test.ts` runs `openHost`: each host row answers what its verb prints, refusals are
typed, and `close()` resolves once `cowshed host` has exited. `launchers.napi.test.ts` runs `spawnLock`, `spawnHold`,
`spawnSubtreeRoot` and `spawnLandTurn` through the same launcher. The disk-command case of `host.napi.test.ts` runs the
real `hdiutil`, the TypeScript twin of the CLI `host.rs` proof in `real_apfs_host`, so the target needs a Mac and is
reached only through `host-e2e`: the Linux CI job (the CI section, below) runs `test` and cannot host it.

The land gate uses `--outputStyle=static-failures-only` and must exit zero before the expected-tip fast-forward and
workspace retirement. Any failed task, timeout or infrastructure failure stops the land: there is no bound-only retry,
acceptance judge, tolerated-failure ledger or baseline-acceptance route. A separate reporter lists unique failing tasks
and deduplicated exact diagnostics, retaining the complete output for context; reporting cannot turn a failed gate
green. Both Nx output descriptors write directly to one regular file; successful gate output and the fallback when the
reporter is unavailable replay only its final 64 KiB under a 10-second bound (then a one-second kill grace), and replay
failure stops the land. The reporter reads the complete file, not the replayed suffix. The pre-rewrite ancestry cutoff
remains a separate guard before rebase.

A red names the commits to read first. The wrapper reads the run summary the gate's Nx wrote (`run.json` in Nx's cache
directory, refusing one whose `run.startTime` precedes the second the gate began in) and lists the tasks that failed; a
task Nx skipped because a task it waits for failed has no row and is not listed. For each failed task it prints the
commits of the land (`base..head`) that change a file Nx hashes for that task or for a task it waits for
(`tooling/land-attribution.ts`). The files come from Nx's hash plan (`HashPlanInspector.inspectTaskInputs`), asked for
the failed tasks alone by the workspace's `tooling/land-inputs.mts` under node, the runtime `bun nx` runs Nx's CLI in:
bun resolves some external packages to other versions, so a plan read under bun is not the one the gate ran. A commit
that only deletes input files is not seen, and a failed task no commit reaches fails on what the land shares with main
or on an input that is no workspace file. Naming is reporting only: a step that fails is said, and Nx's exit status
stays the verdict. A red land's next attempt gates whole again. Nx's cache already replays every task whose inputs did
not move, and a task Nx does not cache has a verdict its hash does not decide, so no record of earlier passes stands in
for running it. After a passing gate, task hashes that cannot be read from the run summary are said and the land goes on
without publishing them: the cache never blocks a land (05_gateway.md, "Publication").

Every test target, the groups included, is bounded by progress alone: the run ends when the time since the last finished
test reaches its configured `idleTimeoutMs` (`BOUNDED_TEST_IDLE_TIMEOUT_MS` in the plugin's test-target policy), and
names the tests still running. There is no count-multiplied outer wall-clock budget and no per-test bound kills a test:
a Bun leg deletes Bun's own 5000 ms per-test deadline with `--timeout=0`, and no test carries one of its own
(`packages/nx-plugin/README.md`, "Bounded Test Targets"). A group's runner carries `--no-tests=fail`, so a filter that
selects nothing fails rather than passing a run of no test. The RAM host walk and real-process CPU-pool
production-wiring proofs run in `test-host`, outside the four pure Bun shards, with the existing case bounds, and so do
the Nx daemon proofs that apply their own Seatbelt profile (a refused daemon stop, a stopped FSEvents stream), which
`sandbox_apply` refuses inside a cowshed job. The shards exclude `*-host.test.ts`; the host target names each owned host
file.

Drain fixtures read report publications on native directory-write events and await the serving task and resident's
completion directly. CPU-admission holders announce readiness on stdout before blocking on their open stdin; consumers
follow that stream rather than poll job PIDs. Missing events are hangs owned by the target's no-progress diagnosis,
never independent test deadlines. A test proves a descriptor's release by the kernel's grant to a waiter, never by a
non-blocking probe at the spawn's return or at the child's reaping.

Land-turn fixtures observe queued replies and executed-command readiness before asserting their queue and order
snapshots. The after-land job's records (a background job of main's supervisor, never awaited by the land) are read on a
vnode subscription armed before the first lands.log read, not on an elapsed-time poll.

An unfinished test name is not holder attribution. During the final ten seconds of an unchanged no-progress interval,
the executor takes one bounded pre-signal snapshot of its runner, descendants and retained process group. Resumed
progress discards and cancels that interval's snapshot. At expiry the executor emits the completed observation, or an
explicit incomplete-capture result, and starts termination immediately without awaiting a probe. Diagnostic capture does
not extend the configured no-progress deadline. Capture timestamps, the offset from the scheduled expiry and the actual
expiry-to-capture interval accompany the observation. Process rows retain PID, PPID, PGID, state, percent CPU, elapsed
time, start identity and command. On macOS the snapshot also holds call stacks: the runner and up to four leaf processes
of its tree (members with no live child; further leaves are named, not sampled) are each sampled for one second by
`sudo -n sample <pid> 1 -file /dev/stdout`, all at once and beside the open-resource probe, inside the same window. The
samples set the window's length: under load averages of 27-52 on 18 cores one sample took 1.1-4.6 s, and in a
five-second window two of eight real captures lost their sample at its limit. Each sample runs until the closing process
census would have less than its one second left; `-mayDie` is not passed, since it reads symbols before sampling and
measured about a second slower under load; the report is kept through its call graph, without its binary-image list, and
is written to the probe's pipe alone, never to a file in `/tmp`. A stack names the call a process waits in (`read`,
`open`, a condition wait), which the open-resource probe cannot. A sample that sudo refuses, that fails or that reaches
its limit is named, with its stderr, as no stack for that process: it never delays termination and does not change the
holder outcome, which stacks do not establish. Other platforms record that no stack is sampled. The case this answers: a
gate run's deadlock was located only from `sample` stacks taken by hand while the run hung. On macOS the open-resource
probe invokes `/usr/sbin/lsof` through noninteractive sudo, with an explicitly limited ordinary-user fallback. A missing
tool, denied privilege, timeout, truncated output, other probe stderr or changing process identity is incomplete
evidence, never an empty holder set. lsof's mount-table fallback (`can't stat() <type> file system <mount>` ...
`assuming "dev=<hex>" from mount table`) is the one stderr kept without that verdict: it names a mount lsof could not
stat(), such as one detaching under a concurrent shed, and omits no descriptor of the selected processes. Successful
observations with resources, successful observations without matches and incomplete/raced captures remain distinct. An
open file is not proof of a contested lock, and an admission token count or one CPU/state sample does not establish CPU
starvation or its absence. The snapshot also names, for each regular file the run holds on a numbered descriptor, the
processes outside the run that hold that file open. Such a file is classified as a lock wait, naming those processes as
holder evidence (open handles, not a kernel lock owner), and the verdict records it; the lock call itself is not
observed. The bound and the kill are unchanged. The case this answers: on 2026-10-21 a `cargo --frozen test` that
printed `Blocking waiting for file lock on package cache` held `~/.cargo/.package-cache` open and was killed at the
bound with only its own files in the snapshot, so its holder went unnamed. The process census resolves `ps` through the
runtime `PATH`, as fixture process tables do; NixOS has no `/bin/ps`. A probe past its limit is ended without being
awaited: an ordinary probe by SIGKILL to its process group, the privileged one by SIGTERM to `sudo` itself, which relays
it to its root command and exits only after that command. A group signal from the caller reaches `sudo` but not the
command it runs as root, so it would orphan `lsof`. While `sudo` starts it holds root's real uid and refuses every
signal from the caller; the probe result names that refusal rather than claiming the probe ended.

**Script-host fixture wedge (2026-10-16, resolved 2026-10-17).** The isolation case
`nothing_one_script_does_to_its_process_reaches_the_next` once and `scripts_forked_at_once_each_run_whole` four times
reached the unchanged 180s no-progress bound with the test and one exec host sleeping at 0% CPU on a connected
control-socket pair; in both of its gate logs with checkpoints, all 800 scripts had completed through `RELEASED`.
Reproduced in 5 of 502 local runs of the scripts case, every wedge had the same `sample` stacks: the test thread in
`wait4` for the host it had `shutdown(SHUT_RDWR)`, the host in `recvmsg` in its idle request read. A non-blocking
`MSG_PEEK` on the host's socket returned 0, end of stream, while the blocked `recvmsg` slept; interrupting it ended the
host normally. Darwin can lose a peer shutdown's wakeup for a reader already asleep in `recvmsg`. A cowshed-free C model
of 8 pairs that end with a peer shutdown wedged within 50–750 trials of 8 teardowns for `SHUT_RDWR` and `SHUT_WR`. The
same model never wedged in 3000 trials when the peer `close`s instead, with or without `SCM_RIGHTS`, nor when the reader
waits in `kevent` before reading. Fixtures therefore end a host as a supervisor does: they close the control socket's
last outside descriptor (11_shell.md, controller EOF), never `shutdown(2)` it while holding it. The supervisor's
`ExecHost` already ends a host by closing its socket.

The real process-snapshot witness keeps its child, grandchild and opened file alive through capture. Its readiness PID
is a tagged, newline-terminated string, not a console-inspected number that inherited color settings can decorate. The
reader assembles a complete line across chunks within an abortable readiness bound; EOF and expiry report the text
observed rather than treating a partial buffer as a PID.

The stack-sample witness runs, under bounded-exec, a command that ignores SIGTERM and opens a FIFO the test owns and
never opens for writing, so it waits in open(2). It runs on real timers under a progress bound five seconds longer than
the capture window, with no other deadline. Its readiness is the complete `opening` line, assembled across output
chunks; the case requires that line seen before the capture began and the capture complete at the expiry. The bound then
force-kills the command and its group empties. On macOS the snapshot carries the command's sampled stack down to
`__open`; elsewhere it records that no stack is sampled.

Re-executed libtest fixture roles that report a line protocol use `--nocapture --quiet`. With an inherited
`RUST_TEST_THREADS=1`, ordinary libtest output prefixes the first report with its unfinished test-name line; a reader
waiting for a report at the start of a line then waits forever. Quiet formatting preserves the inherited count and the
exact report parser. The held-parent CPU and listening-group regressions must also pass under a one-thread runner.

Disk-child warning tests release an owned child after observing the required warnings rather than assuming a wall-clock
sleep gave the waiter enough turns. Shell-pool tests observe release-time dirty-event processing before asserting that
no replacement warmed without demand.

**A real-APFS test's class is on its code time.** Every disk command a test's process tree runs first queues for the
host-wide disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease") behind every other cowshed process on the host:
the same warm-fork walk took 37-70s on a loaded host with 0.2-28s of it spent waiting for grants, against a 25s run when
nothing waited, and one 7.7s wait admitted a 73ms unmount. A wall-clock class measured that queue, not the test. So each
group test runs under the `nextest-code-bound.ts` run-wrapper the `@smoothbricks/nx-plugin` ships, named in
`.config/nextest.toml`. The wrapper creates a fresh lease-wait record, names it in `COWSHED_LEASE_WAIT_RECORD`, which
every process of the test inherits, and runs the test binary. Every disk-lease client — cowshed-core's `leased` and the
plugin's `takeDiskLease` — appends `start <pid> <seq> <unix-µs> <class>` before it asks and `end <pid> <seq> <unix-µs>`
when the wait ends, granted or not, unwinding included; each line is one append, so concurrent writers never interleave.
Each client asks for its phase `alone` (05_gateway.md, "Disk-lifecycle lease"): a shared member's command queues on
`storagekitd` behind every other member's, and in one gate a detach that takes 0.3 s idle took 10.6 s there, so the
queue is waited for before the grant, where the record counts it. After the test exits, the wrapper prints its wall
time, the union of its waits clipped to the run (waits of concurrent threads and processes, as a sweep beside a
teardown, count once) and their difference, its code time, and warns when that code time exceeds its class; the test
keeps its own exit status. The record cannot undercount a wait whose process died: the start is written before the wait,
so a start with no end — its process killed while waiting, or still waiting — counts to the test's end and is named. A
record that cannot be written is said once per process and its waits then count as code time; an end with no start
counts as code time and is named; a record the wrapper cannot parse fails the test.

**Class policy lives once in configuration.** `.config/nextest.toml` owns class membership and the code-time classes
passed to the wrapper; this spec does not duplicate their values. The lifecycle class is defined by what a test runs:
the raw substrate lifecycle and every genuine-Nx CLI walk, a test whose fixture installs the repository's pinned Nx and
runs it or its daemon (`Project::Nx`, or `Project::Build` with Nx), each named with the `_substrate_lifecycle` suffix.
The default class covers the rest of the groups' declared filters. A class is the fixture's idle code-time baseline
multiplied by its configured regression factor, not an envelope fitted to a loaded host's successful runs. Changes to a
fixture require its baseline and class policy to be reviewed; unexplained code-time growth is a regression to find, not
a reason to raise the class.

Historical diagnostic evidence, not class policy: the raw lifecycle's worst recorded idle measurement was 14.8s, and the
warm-fork walk took about 25s with no lease wait. A full serial lane at load 15-84 (median 37), with lease waits
excluded, measured genuine-Nx walks at 2.3-45.4s of code time: land-carries 45.4s, the foreign database opener 38.1s,
rebase-carries 36.0s, warm fork 30.9s and the foreign database holder 30.2s, with at most 5.8s of lease wait each. Those
measurements describe that run's load and work; they do not establish acceptance thresholds.

A class is a warning, never a kill or a failure. Nextest's warning cadence follows configured class policy rather than
an independent spec duration. A test that never returns is ended only by its runner's configured no-progress bound,
which names it and emits the pre-signal holder observation or explicit capture failure described above. A killed test
leaves its images attached until the next run's sweep (01a_apfs.md, "How the APFS host degrades" §2).

The genuine-Nx CLI fixtures use the normal enabled daemon (16_build_volumes.md, "The Nx daemon stays on"). Both halves
of the declared-versus-undeclared output witness require a seeded, actually adopted build volume through the typed land
report and assert its step-7 check exactly (16_build_volumes.md, Land step 7): the declared half hits all three Nx tasks
with no miss, unattributed or failed check; the undeclared half hits two and misses only `a:test` because restoring
`a:build` supplies no undeclared output, so `a:test`'s recomputed hash differs. No test runs in the target, and that
miss is sealed as its one `landAdoption` commitment. An empty findings list alone is not evidence that a declared output
restored and hit: a busy-volume skip, an unattributed run summary or a failed check leaves it empty too.

**Host limit.** The host's disk-image attach pool runs out (01a_apfs.md, "How the APFS host degrades" §2), and the suite
stops spending into a spent one. The harness owns a lane-wide fixture pool
(`packages/cowshed/crates/cowshed-apfs/tests/support/blank_image.rs`): it attaches each scenario image in its registry
once per lane run and hands tests handles to mounts of an already-attached volume, so a test cannot attach an image of
its own through the fixture API — the API hands out shared-fixture handles only, and no per-test attach helper is in
tests' reach. Seeding a store clones the blank template and attaches nothing. Tests that inherently prove attach or
detach behaviour keep their own attach through the production host; they are few and name that in their docs. Fixtures
attach once per scenario and share; tests pay the fewest attach cycles that prove their behaviour. The pool refuses to
attach past its registry, whose size — computed in code from the registry itself — is the lane's attach bound, so a new
per-test attach fails the lane instead of silently spending the pool. A test whose setup and subject match a neighbour's
becomes assertions in that neighbour rather than a separate test, and anything that does not need a real device is a
pure or unit test with no image at all: the real-device lane holds a few scenario tests per fixture, not a test per
behaviour. Fixtures attach through the production path, in the kernel (01a_apfs.md, "Mounts"), which draws nothing from
the DiskImages2 pool, and the lane creates no image with a disk tool. The lane stays `--no-fail-fast`, so its ordinary
failures still run to the end. The leaked-device delta across a lane run (`ioclasscount AppleDiskImageDevice` against
the registered devices, before and after) is the check that no disk-image command still leaks on a clean run; it must
be 0. Nothing refuses an attach on the media count: fixtures attach in the kernel, which waits on no IOMedia
notification (01a_apfs.md, "The IOMedia match-notification ceiling").
`real_apfs_making_room_detaches_the_least_recently_used_idle_session_and_warns_by_name` proves making room
(02_workspaces.md) on the real kernel: the cap is its decision input, set just short of its namespace's own count, and
every count is read from the registry, never faked. Its namespace counts only the media its store's images register
(01_storage.md, "Host namespace"), so other users' attaches move no reading, and each outcome is asserted exactly: the
workspaces detached and every reading `make_room` decided on. The decision's properties (only idle workspaces, least
recently used first, enough or none) are proptests of `plan`, and the demand each controller operation reads from its
request is a unit test of `attach_demand`.

**The suite spends a finite host budget.** Every attach→detach cycle a test makes can leak an AppleDiskImages2 device
that the host's live sheds lose until reboot (01a_apfs.md, "How the APFS host degrades" §2), so a test pays the fewest
attach cycles that prove its behavior. A run that killed a test leaves that test's images attached until recovery
reclaims its dead root. `real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end` proves panic and
SIGKILL cleanup with one mounted RAW image and one read-only retained RAW input. Its parent owns the helper roots: panic
drops the child's root, while a killed child's exact reported root is reclaimed after the parent reaps it. This uses the
same process/image/tree cleanup as root drop, without starting a host-wide sweep or waiting on unrelated dead runs.
Owned-attachment release selects the exact backing path the kernel holds, not the runtime image extension: a retained
input must be released too. Ordinary runtime enumeration and attachment remain RAW-only (02_workspaces.md,
"Re-adoption"); cleanup neither attaches nor converts an image. A scratch root's teardown and the sweep release a root's
images concurrently, one holder per image: each release is an unmount under the host's namespace lease and then a detach
under its storage lease, and those classes exclude each other (05_gateway.md, "Disk-lifecycle lease"), so releasing a
CLI fixture's main, build and workspace images one after another waited for a phase change at every step on a loaded
host. A run sweeps its dead predecessors once, on a thread beside each test process's first test: one process sweeps
while the others wait on the lock and then find the run's sweep recorded. Only the root that started its process's sweep
owns and joins that thread; dropping another root never takes its handle or waits for it. A barrier regression keeps the
starter's sweep blocked while another root finishes cleanup. An individual image-release refusal retains only the dead
root containing that image; the sweep reports every refusal and still removes other dead roots after releasing their
images. An inventory failure retains every root, since no root's image-release safety can then be proved. Root-local
refusal and continued reclamation have a pure, platform-independent regression; one unreleased image must not leave
another run's already-detached files behind. Nothing a test does depends on the predecessor sweep, since no sweep
selects a live pid's root. The starter keeps its sweep alive through completion, including on unwind. Child fixtures
whose parent owns their recovery create only their own roots; they never reset the suite run identity to force another
host-wide scan. That old coupling made the panic witness spend 44.7 seconds joining a 52-second scan of unrelated dead
runs after its own 1.6-second teardown.

Pure regressions recycle a device to a RAM image during the storage-lease wait, recycle it between dissent attempts and
before force, and repeat an image's whole-device view. They prove the guard runs after the grant, no child detaches a
recycled number, and a release acts on each whole device once without spending an attach cycle.

Coverage pays for an attachment only when the kernel behavior is the oracle. The image-growth witness also checks
normalized whole/volume device names; unheld RW/RO live captures preserve source facts on success and destination
refusal; a held RW capture also succeeds unchanged. Restart-derived facts check an idle-detach dissent before release.
Workspace grant refusal, workspace grant persistence and project-policy persistence share one adopted CLI fixture,
retaining both refused and successful restart checks before a real port race and gateway-grant reconciliation. Current
create/fork refusal, legacy no-artifact recovery, missing-revision refusal/retry and adopt/create/fork port-publication
checks share one adopted project, retaining every runtime shutdown and scoped recovery opening. An attached source's
detached-capture refusal precedes its mount and rename checks; registered-host idle-detach dissent precedes its final
release. Busy resize and defrag run their original refusal oracles on the mounted-growth fixture before its holder is
released and growth succeeds. The raw-pin witness also checks independent forced release between verification and mount;
the mount-flag truth table heals its final ownership-free mount by mountpoint. Declared first-touch and warm-fork
assertions share the build-volume naming/reclamation walk. Push preservation and checked-land refusal/success share the
plain-Git lifecycle walk, keeping the original Git-reference and no-write refusal oracles. Capacity-only refusal uses
the existing recording host over a sparse RAW file, and an invalid first-mint capacity uses the pure filesystem store:
neither needs an attached volume. Tests that share an attach are one scenario with phased assertions: adoption crash
windows one world reaches in order are phases of that world, each window's state asserted before the next step.
Registered-versus-restarted detach paths, writer-admission boundaries and durable deletion records keep their own
oracles. The non-growing build-volume resize is refused as Usage on the held-conflict fixture before the holder is
taken.

Ordered scenarios reuse a step's resulting state instead of adopting another identical store. The Nx land walk checks
ownership, seed identity, previous-volume reclamation, labels and linked mounts before main writes again, reseeds and
forks. Link displacement, doctor findings and exec relinking precede a workspace's fork/reseed walk. Database-opening
refusal precedes release of the opener, a new topic commit and database-holder refusal on the same warm main and topic;
both distinct refusal variants, process identities and unchanged target volume are asserted. Each land must seal exactly
one durable outcome for its reported landed head, with the same seed and refusal as its report; the later land retains
the earlier head's outcome rather than replacing it. The successful first-touch volume also supplies the
existing-link-wins case, whose unused competing mint is still released. These sequences remove duplicate setup, not real
transitions: each crash-injection state and source/target isolation witness keeps its own assertions.

Staging GC derives backing-image and mountpoint paths in one preallocated buffer each. Both orphan branches borrow the
parsed workspace for the deletion record rather than allocating a second name; mount GC also retains its parsed stem.

The native inventory teardown regression runs the production read-only host-storage planner over a deterministic
disk-state source that steps an unrelated image through its teardown: mounted, its mount gone while the registry still
lists it, its container left with no volume, then gone. Each stage is one fixed kernel snapshot, so the ordering is
proven by the stages rather than by overlapping wall-clock intervals, and no image is attached. At every stage the
selected home device and container remain unchanged. The regression does not require machine-global `cowshed.store`
installation on an ephemeral runner. Installation validation remains a separate contract and still refuses missing
storage.

Read-only planning reads the kernel snapshot once, with no retry (01a_apfs.md "Dedicated store volume"), while an
unrelated image may be departing. The same regression proves that a snapshot taken at any teardown stage still finds a
reserved volume in another container. An incomplete snapshot, an invalid device name or a partially populated record is
an error, never evidence that a reserved volume is absent, and mutation planning refuses it.

APFS diagnostics distinguish image-lease acquisition, a mint (`<leg>/mint`, its `clonefile` and attach inside it),
template minting (`template/mint`, with blank-image creation and formatting inside it), raw-device pinning, image/device
identity inspection, attach, and fsck. Backend disk commands also report their operation and elapsed time, including
attachment inventories and detach requests that previously appeared only as gaps between lifecycle spans. Adoption
reports binding and inventory checks, identity ownership, intent publication, secret scanning, grant reservation, staged
copy and credentials, inherited-state cleanup, Git environment wiring, and supervisor startup. Controller startup
reports runtime creation and protocol connection/open separately. Checked land reports binding, source/target
inspection, dirty checks, supervisor readiness, check dispatch/completion, and Git delivery. `gc` reports its build
volume collection, its discard deletes and its substrate pass (`gc build-volumes`, `gc discard`, `gc substrate`). The
real fixture also reports Git verb durations and elapsed phase boundaries, and each scratch root its teardown and a
run's sweep of dead runs; these diagnostics do not expose command payloads. Build-state Git failures name the exit
status or terminating signal alongside stderr, including when the command wrote nothing, so an interrupted query does
not leave an unexplained empty error. The re-adoption witness captures the real core stderr channel to assert
retained-image guidance, then replays it after restoring stderr and before asserting the command result. Unwinding also
restores and replays that capture, so a refusal or panic cannot disappear when the fixture removes its private report.

The CLI dispatch tier opens a real `ActorBridge` on caller-owned APFS images and the same persisted workspace/project
grant stores as a normal controller. It tests grant denial without a write, grant survival across runtime restart, a
real gateway's reconciliation of revised grants and raced ports, and checked landing's gateway-absent refusal followed
by a successful fast-forward. There is no test-only `CliService` answering commands. The gateway and scratch-image
teardown guards run on assertion failure as well as success; no test installs or changes the machine-global daemon.

**Existing fixture debt — owner: cowshed-core native tests.** The older `FakeInventory` and `RecordingRunner` cases in
`apfs_native_macos/macos.rs` synthesize `hdiutil` responses to test command-runner handling. They are not real
host/substrate evidence and do not count toward the `real_apfs_*` acceptance set. The real APFS cases added beside them
use `ScratchRoot`, `SystemCommandRunner`, and the kernel's attachment table; the command-runner fixtures remain a
separate, explicitly owned migration boundary rather than being presented as real dispatch tests. They attach no image,
so they run in cowshed-core's ordinary parallel shards: the host group's cowshed-core filter selects `real_apfs_*` names
only, not the whole `apfs_native_macos` binary.

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
- **Checked-out target fast-forward**: create a clean main workspace with the selected target branch checked out, land a
  reviewed workspace onto that branch, and assert the target ref, `HEAD`, index, tracked files, and worktree tree all
  advance to the exact validated source object ID. Assert no hidden or namespaced ref is accepted as a substitute for
  updating the visible checkout. Repeat with dirty tracked and staged state and prove `land` returns `Conflict` without
  changing the target, source workspace, index, or file bytes.
- **Non-checked-out target fast-forward**: check out a different branch in the main workspace, land onto another local
  target branch, and assert only `refs/heads/<target>` advances while the current branch, `HEAD`, index, status, and
  file bytes remain unchanged. Race target advancement and source/incarnation changes against the final update; every
  stale expectation must fail before retirement. A target checked out by an unmanaged linked worktree must be refused
  rather than updated behind that worktree's index.
- **runner interception from step one**: execute a workflow fixture with a hostile first command, later shell commands,
  and a supported action-generated command. Instrument the runner spawn boundary and assert that every
  repository-controlled command is launched through `cowshed exec` in the job workspace, in workflow order, with zero
  direct runner spawns. A fixture containing an uninterceptable action type must fail before that action starts and must
  not fall back to direct execution. Run the fixture after the composite action has returned and with cwd already inside
  the mount, proving neither mechanism supplies interception.
- **workspace-local job allocation across restart**: submit multiple execs, assert each accepted submission appends a
  protected allocation batch and the admission audit record before spawn, receives a unique strictly increasing numeric
  ID, and does not eagerly create `out`/`err`. Restart the supervisor, reconcile complete protected records and
  inherited spill names, then assert the next ID exceeds every prior allocation. Duplicate or contradictory allocations
  are `Integrity`, never reused.
- **lazy stream representation and summary surfaces**: run below-inline-limit commands emitting distinct invalid UTF-8
  and secret fixtures independently to stdout/stderr. Assert no per-job stream files exist, protected terminal Arrow
  columns hold separate Binary values, and both `StreamInfo`s are `Captured/Inline` with exact counts/SHA-256 and
  deterministic bounded redacted summaries. Repeat above the threshold and after forced backgrounding: assert promotion
  creates only the required protected `out`/`err` files, writes the buffered prefix once, and `Captured/File` reads are
  byte-identical. Exercise one stream inline and the other file.
- **bounded JSON and representation-transparent reads**: core/CLI/NAPI/MCP goldens pin `{storage,bytes,sha256,summary}`
  and every captured/redirect × inline/file discriminant. Valid UTF-8 serializes as `{encoding:"utf8",data}`, other
  bytes as `{encoding:"base64",data}`; both decoders enforce the decoded inline bound and preserve bytes exactly.
  Ordinary `JobInfo` JSON permits this bounded union, while controller audit records reject every payload/path field.
  File variants have no inline data and inline variants invent no path. Logs/follow/reconnect/ checkpoint readers return
  identical raw bytes without caller representation branches.
- **supervisor grant-revision advance**: start a long-running job at revision N, apply an effective filesystem grant or
  revoke, and assert the manager advances the same supervisor to N+1 and starts no second one before the next exec. The
  running job completes under N, an inner per-command profile can only narrow N and cannot broaden it, the next exec
  observes N+1, and reconnecting a named session bound to N returns the documented stale-session conflict rather than
  silently rebinding. Repeat for both grant and revoke.
- **One exact-image no-mount attach → pinned fsck → pinned `mount_apfs`** on APFS: assert attach flags/plist parsing,
  exact current image/whole/volume identity, `fsck_apfs -q` before the first mount, and the raw pin held across the host
  namespace wait, then helper options, requested root and held mount-root/source identity before publication. A
  structurally-bad clone refuses before mounting. A malformed reply, failed fsck or refused/wrong mount releases only
  the owned attachment or names its cleanup refusal. Foreign/stale descendants are neither targets nor vetoes. No
  combined `--mountPoint` attach, fallback or retry is permitted. Detached verification/resize and unformatted RAW
  retain intentional unmounted fixtures;
- **Live captures stay mounted; build seeds retain writer exclusion**: a live capture (`fork`, `checkpoint`) flushes the
  source volume and backing file, descriptor-clones the live RAW image, and asserts that the source's attachment and
  mount flags are unchanged (01a_apfs.md, "Images"). `fork` and `restore` pass a formatted clone to the one exact-image
  no-mount attach, pinned `fsck_apfs -q` and `mount_apfs` path and verify the resulting held mount identity. A failed
  operation refuses publication and cleans up only the destination, never the live source (02_workspaces.md, fork step
  4). A build seed (16_build_volumes.md, "Targets and seeds") uses the same block-atomic mounted descriptor clone with
  supervised admission closed and the Nx and existing Cargo-profile guards held through capture and publication. Tests
  require an exempt index/cache reader to survive with source identity and mounts unchanged; an Nx state holder or a
  held known Cargo profile refuses seed replacement. The new-host-profile gap remains explicit in that contract; no
  fencing of that case is claimed. RAW has no outer allocation map for a live capture to split (02_workspaces.md, fork
  step 2).
- checkpoint/restore round-trip (restore undo image `pre-restore-…` present);
- defrag: main fragmented by rewriting pages while a clone shares them refuses the rewrite while a file is open on its
  volume (image untouched), then, idle, comes back in at most a tenth of its extents with its data, marker, and mount
  unchanged (01a_apfs.md, "Clone cost follows extents, not size");
- attach healing matrix: detached image, wrong-flag mount, missing/wrong-flag `cowshed.store` volume (lazy recreate +
  canonical-flag remount, 01a_apfs.md), stub `.envrc`;
- lazy volume creation at adopt: the store volume created idempotently before the first image;
- rm-while-busy (open file handle → grace → force detach);
- gc: release of retired trash, orphan session and staging images and expired checkpoints, each nudged to the gateway's
  cleanup, orphan mountpoint removal; real root-owned `0700` event-log debris on an unmounted directory is retained by a
  no-replace, same-parent rename, never blocks image reclamation or healthy doctor info, and mixed work, symlink
  substitutions, collisions and mounted filesystems remain protected;
- gateway: mirror hit/miss against a local fixture registry, token→policy mapping, 403 hint body, audit records, CONNECT
  allow/deny, `repo mirror` fetch into a read-only bare mirror;
- **Linux connector end to end**: for an attached workspace, assert exactly one connector exists in its private netns,
  under the dedicated controller-owned identity/cgroup, bound only to IPv4 `127.0.0.1:7644`. Run real Bun/npm install,
  Cargo registry fetch, Go module download, and a generic HTTP/HTTPS proxy client through the proxy variables on
  `http://127.0.0.1:7644`; assert byte identity through the connector, gateway endpoint+token authentication, mirror
  behavior, and no direct fallback. None of these clients may use a Unix-socket transport. Run two workspaces with the
  same address/port and prove neither can reach the other's connector or socket. Detach must stop admission, drain
  connections, kill the connector cgroup, unlink the socket, and leave 7644 unbound; attach must create exactly one
  fresh connector before exec admission. Restore must drain old connections and make the old connector/socket/token
  unusable before publishing the new incarnation.
- gateway interception (05_gateway.md): an intercepted host serves a workspace-CA leaf the in-image anchor trusts,
  injects the Keychain credential, and records a **request-granular** audit line; an `--opaque` host tunnels without
  injection; the **upstream-health gate** fails a dead upstream fast with a classifiable error (not a per-request
  timeout) — asserting the gateway-absent / upstream-offline / denied trichotomy; **socket teardown**: no leaked
  listeners or half-closed sockets after a churn of many distinct intercepted hosts (the JS original's fd-leak bug
  class), and oversized request headers are tolerated;
- image creation, unprivileged: a created image mounts as case-sensitive APFS whose root the invoking user owns
  (`pathconf(_PC_CASE_SENSITIVE)` is 1 and `Foo` and `foo` coexist), no step runs as root, a clone of it mounts
  case-sensitive too, and an adopted repository's `core.ignorecase` reads `false`.
- **persistent multi-client supervisor socket**: runtime directory is `0700`, socket is `0600`, wrong-peer credentials
  fail before framing, and simultaneous clients submit/query independent jobs. Disconnect and reconnect by job id and
  stream offset before/after lazy promotion; assert no job stops, no byte repeats/disappears, and the socket remains
  linked until orderly supervisor exit.
- **child profile before repository startup**: instrument shell startup files, direnv, repository hooks, named sessions,
  one-shots, and descendants. Before any such code runs, assert the child restriction denies create/open-write/truncate/
  replace/rename/unlink/link/metadata mutation beneath `.cowshed/job/**`, inherited writable protected FDs, and symlink,
  hardlink, bind-mount, alternate-spelling, and `/proc` reach-arounds. The trusted supervisor alone can append.
- **audit sink and record channel**: prove the writer capability is close-on-exec/non-inheritable. Round-trip exact
  Admission/Terminal/Checkpoint/Fork/Restore variants through the Arrow sink (one sealed segment per record,
  writer-local order, private mode, temporary cleaned on a crash before rename, sealed segment kept on a crash after),
  through `off`, and through an injected sink whose refusals surface as `doctor` health, never as a failed act.
  Controller Arrow carries only the frozen identity/order/ lineage/state/count/hash/batch-digest fields; adding
  `inline_bytes`, `protected_path`, `source_path`, summary, or raw payload rejects. Protected Arrow round-trips exact
  Job/CheckpointManifest variants and rejects tag/null mismatches. Mutate/delete protected artifacts, forge controller
  rows, alter complete frames, contradict lineage, and remove each side in turn: status/read/restore/publication returns
  typed `Integrity`, preserves both sides, and never picks outside/newer. Only an incomplete trailing frame is
  discardable; its retained `batch_sha256` appears in recovery.
- **lineage authorizes inherited records**: open a records file containing frames from the workspace's marker lineage
  and assert it opens; a frame from an incarnation outside the lineage is `Integrity`; a marker written before lineage
  was recorded is healed once from the foreign origins already in its records and is strict afterwards; fork and restore
  write the clone source's incarnation followed by the source's own lineage into the new marker; a marker from another
  repository is never an ancestor.
- **checkpoint barrier and resident bytes**: checkpoint while two jobs have below-inline-limit bytes only in supervisor
  memory and another appends to a spill. Assert admission/artifact mutation pauses and every running prefix promotes and
  fsyncs. The exact
  `CheckpointManifestRecord { version,repo_id,origin_incarnation,barrier_id,visible_jobs, records_sha256 }` is the
  complete manifest batch; each visible stream has exact storage kind/count/hash/path and path iff file. Clone while
  held, then record the matching
  `CheckpointCommitment { version,order,repo_id,origin_incarnation,checkpoint_id,barrier_id,manifest_batch_sha256 }`
  audit record. Restore exposes exactly this boundary: terminal inline bytes resolve through prefix Job rows, running
  bytes through files, and no checkpointed byte remains only in process memory.
- **combined job-output quota**: race separate stdout/stderr writers through memory and file promotion. Assert one exact
  crossing over protected plus in-flight bytes, no post-boundary payload retention, process-group TERM/grace/KILL, both
  pipes drained without deadlock, artifact sealing before the terminal audit record, and explicit `outputLimit`. Repeat
  at exact-limit, one-byte-over, disconnected, backgrounded, soft-timeout, hard-timeout, inline/file, and redirect
  edges.
- **redirect and publication isolation**: AST-proven simple literal `>`/`2>` may become `Redirect` only with a
  supervisor-controlled actual writable descriptor and identical exact quota accounting. Assert post-terminal
  clone/reflink/copy creates an independent protected artifact, `source` mutation cannot change it, and polling/tailing/
  path reopen is never used. Ineligible forms run ordinary shell semantics and claim only captured-pipe bytes.
  Independently test API `stdout_copy`/`stderr_copy: Option<OutputPublication>` per-stream CreateNew/Replace policies
  and CLI `--stdout-copy`/`--stderr-copy` default CreateNew plus invocation-wide `--replace-output`; the latter is Usage
  with no copy option. Assert sealed-source clone/reflink/copy, no hardlink, no storage/read-authority change,
  create/replace race safety, and destination mutation/deletion cannot alter protected evidence.
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

## Escape tests

One adversarial corpus (04_sandbox.md "Escape tests"), each case a payload run under the generated profile plus an
assertion that the operation was denied and the artifact untouched. Test cases live beside the code they pin, as defined
in [04 Sandbox, Escape tests](04_sandbox.md#escape-tests). The same corpus runs on both OSes through the real exec
pipeline: Seatbelt on macOS, Landlock plus loopback netns on Linux. Escape suite green on both OSes is a release gate; a
red escape test cannot be waived. The categories below are the corpus; each category has cases.

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

## Performance budgets (regression thresholds)

Measured by the integration suite with tinybench-style medians (≥ 10 samples); CI asserts with a 3× multiplier to absorb
runner noise, local `cowshed doctor --bench` reports raw numbers.

The fork tests run from main and from a session with a tracked edit and a nonignored untracked file. Default `fork`
preserves both on `cowshed/<ws>` without changing the source branch or status. `--clean` removes both; `--ref <commit>`
implies clean and selects that exact commit. Ignored dependencies and build state remain warm in every case.
Linked-worktree carry preserves working-tree edits and untracked files, not staged-only state. `new` is rejected as a
fork argument, and the API rejects a ref with `clean: false`. Main moves past the named baseline in every fork fixture,
so a mint that ignores the ref fails its ref cell instead of passing because the baseline is the source's HEAD, and the
clean and ref cells assert the selected commit's files as well as its HEAD. A standalone mint reads its source only for
its links and identity, so from main only its carry cell runs; its clean and ref cells would repeat the session
fixture's. A linked-worktree mint from main runs all three, since it replaces the clone's `.git` directory by a
registration.

### APFS / Seatbelt (macOS)

| Operation                        | Budget (median, local)  | Basis                                                                                          |
| -------------------------------- | ----------------------- | ---------------------------------------------------------------------------------------------- |
| `cowshed fork` cold              | ≤ 1 s                   | clonefile + attach + `fsck_apfs -q` + `mount_apfs` (the mount pays the extent copy)            |
| `cowshed rm` complete retirement | No measured bound       | detach and the release's rename finish before success; the gateway thread frees it, off-budget |
| `cowshed checkpoint`             | ≤ 1 s                   | descriptor-clone path                                                                          |
| `cowshed path` / `ls`            | ≤ 50 ms                 | readdir + getmntinfo only — proves the no-state-store design                                   |
| exec sandbox overhead            | ≤ 50 ms over bare spawn | profile generation + sandbox-exec                                                              |

**The fork extent warning is a standalone first-write sensitivity bound, not a whole-clone SLO, and never refuses a
fork.** One loaded RAW observation was 60.845084042 s for 434,462 extents: 140.046964 µs/extent. At that observed rate,
**7,140 extents** take less than 1 s and 7,141 exceed it; 1,785 fit 250 ms and 1,786 do not. The cause is unattributed.
This is measured sensitivity, not causal calibration. Concurrent first writes and container-wide contention can change
the rate, so it is not a latency guarantee. Further attribution found 440,074-extent first writes taking 50.004/39.605 s
wall versus 5.981/5.633 s system CPU, reading 801–823 MB, while a 25,013-extent source took about 4.04 s (about 161
µs/extent, above the selected 140 µs sensitivity). Off-CPU work is substantial; those observations do not establish a
universal per-extent calibration.

The extent warning applies to **both the workspace image and every build seed it publishes or clones**. The proof uses
device/inode, length, allocated blocks, and nanosecond modification/change times before and after the extent walk; Git
HEAD or the build tree is not a physical-generation identity. Unknown or changed generations refuse publication;
oversized generations warn and proceed. Each admission or publication extent walk stops at the 7,141st run; proof work
is additional, never an unbounded recount concealed in the cold-fork number. Doctor explicitly counts the workspace
sources, live build sources and build seeds, reporting each image's own count and RAW sensitivity estimate.

The cold-fork row remains a primitive-path regression target, not evidence that a production whole clone meets it.
Attach, filesystem check/mount, identity work and proof collection consume additional time. Serialized first writes of
multiple images share one budget: giving each image 7,140 extents does **not** put their sum below 1 s. No images are
merged. Tests prove the 7,140/7,141 boundary, reject stale/replaced/missing generations, cover build-source and seed
diagnostics, and prove that the named `cowshed defrag <workspace>` route reaches those registered images.

**What the budgets are.** They are regression thresholds on medians of primitive-operation costs, not end-to-end cowshed
SLOs: a median is stated because a small sample cannot fix a stable tail. Clone-backed images rather than shadow mounts
are the substrate because shadows degrade synchronous writes (09_substrates.md, "Tradeoffs").

**Attach floor verdict: not flag-reducible** — `-noverify` and `-noautofsck` do not lower it, and
`diskutil image attach` exposes no equivalent knobs; the floor is inherent to DiskImages attach plus the APFS mount.
Only a different attach path (the private DiskImages2 API) could lower it, and budgets must not assume one. The
apfs-workspace-bench and image-format-bench harnesses (`specs/cowshed/prototypes/`) are the reference methodology for
any substrate change; apfs-workspace-bench also establishes the ZFS baseline below.

### ZFS / Landlock (Linux) — separate baseline

ZFS has no `attach` and no `fsck` step, so the APFS numbers do not transfer; the same _user-visible_ operations get
their own measured baseline and regression limits on a ZFS-capable Linux host (the cowshed CI runner, 10_ci.md),
established before the Linux leg gates.

| Operation                | Budget (median, local)  | Basis                                                    |
| ------------------------ | ----------------------- | -------------------------------------------------------- |
| `cowshed fork` cold      | ≤ 250 ms                | `zfs snapshot` + `zfs clone` + mount; no attach, no fsck |
| `cowshed rm` (perceived) | ≤ 100 ms to return      | logical retire; `zfs destroy` clone+origin is background |
| `cowshed path` / `ls`    | ≤ 50 ms                 | `zfs list` + mount table                                 |
| exec sandbox overhead    | ≤ 50 ms over bare spawn | Landlock ruleset apply + netns join                      |

## CI

- One job, `Validate` (`.github/workflows/ci.yml`), on every push to `main` and every pull request: Build, Lint, Unit
  Tests and Browser Tests through `smoo github-ci nx-smart`, on Linux only — the self-hosted `nixos-latest-x64` runner
  for the repository's own branches, `ubuntu-latest` for pull requests from forks. There are no macOS runners, no
  Linux+ZFS runner, and no nightly run, so every macOS-only test (APFS, Seatbelt, launchd, the escape tests, the
  built-addon host proof) runs only where a developer runs the gate: `bun nx run-many -t gate` on a Mac, with
  `bun run check:linux` cross-linting the Linux target. The check compiles test targets too; native-only methods and
  fixture helpers share their callers' platform guards, never warning suppressions. Retained-input attach construction
  remains available to portable tests; APFS supervisor mounting is macOS-only, and the macOS child-fence race keeps its
  imports inside that test.
- Lint is `cargo fmt --all --check` and `cargo clippy --workspace --all-targets --config 'build.warnings="deny"'` (repo
  rule: fix everything you see — no pre-existing-failure waivers).
