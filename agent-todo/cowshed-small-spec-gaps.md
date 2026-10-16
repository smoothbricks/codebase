# cowshed: small gaps between the specs and the code

Each item names the spec text that states the design, the code that does not do it yet, and the test that proves it
done. They are independent of each other.

## Go proxy and toolchain pins

- Spec: `specs/cowshed/03_caches.md:115-126` and `:264`, `15_capabilities.md:31`. The Go capability exports
  `GOPROXY=https://proxy.golang.org` (no `direct`), `GOSUMDB=sum.golang.org` and `GOTOOLCHAIN=local` as environment
  actions, not a GOENV file.
- Code: `packages/cowshed/crates/cowshed-core/src/capabilities/go.rs:96` sets only `GOMODCACHE`/`GOCACHE`; nothing sets
  the three variables.
- Work: add the three `EnvAction::Own` entries beside `GOMODCACHE` in the Go contribution.
- Acceptance: in a Go workspace job, `go env GOPROXY GOSUMDB GOTOOLCHAIN` prints the three values; a `go.mod` with
  `toolchain go9.9.9` fails without a download attempt; a module the public proxy serves still fetches.

## JVM truststore

- Spec: `specs/cowshed/04_sandbox.md:597`: a per-workspace PKCS12 truststore cowshed generates in-image.
- Code: no generator; `grep -rn 'PKCS12\|truststore' packages/cowshed` finds nothing. `capabilities/gradle.rs` wires
  caches only.
- Work: generate the truststore from the workspace trust bundle whenever the anchors are published (the path that writes
  `.cowshed/ca-bundle.pem`), and point the JVM at it from the Gradle capability.
- Acceptance: `keytool -list -storetype PKCS12` lists the platform roots and the workspace CA and no private key; a
  Gradle build through an intercepted host verifies; a CA rotation regenerates the file.

## Seed retirement takes the image lock as a witness

- Spec: `specs/cowshed/16_build_volumes.md:553-555`: a held lifecycle or image lock defers release; seed retirement
  takes the target's image lock as a typed witness.
- Code: `packages/cowshed/crates/cowshed-core/src/runtime/build_volumes.rs:1650` `retire_seeds` takes no lock argument;
  its callers (`freeze_seed` at `:965`, reseed at `:1862`) happen to hold the lock. `collect` (`:1264`) never checks it.
- Work: give `retire_seeds` the lock guard as a parameter, as `freeze_seed` receives `&TargetLock` (`:918`), so the type
  requires it.
- Acceptance: a reseed holding the target's image lock while `collect()` runs; the superseded seed is not released until
  the lock is.

## Open-time projectRoot check

- Spec: `specs/cowshed/02_workspaces.md:214-216`: opening a project compares the marker's `projectRoot` with the
  canonical git root and returns a conflict (exit 4) before any mount.
- Code: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:376` `open_for_adopt`, `:399` `open_existing_at`, `:408`
  `open_existing` do not compare them.
- Work: compare in the shared open path, before mounting.
- Acceptance (`crates/cowshed-cli/tests/mount_main.rs`): a checkout copied or moved outside `cowshed mv` fails to open
  with exit 4; the unmoved checkout opens.

## envHash on job records and job spans

- Spec: `specs/cowshed/11_shell.md:513-514` (`envHash` beside `grantRevision` on every job record) and
  `13_telemetry.md:32-33`, `:53` (`env_hash` on job spans).
- Code: `ExecRecord.env_hash` exists in `crates/cowshed-core/src/api/dto.rs:2085`, but no record or span carries a
  computed value; `crates/cowshed-core/src/runtime/job_spans.rs:8` says the supervisor does not compute it.
- Work: hash the spawned job's environment once at spawn (`runtime/supervisor.rs:1323` `sandbox_environment` plus the
  request's env), store it in the protected record's admission batch and the terminal span row.
- Algorithm gap: `specs/cowshed/11_shell.md:513-514` names the environment hash but defines neither its algorithm nor
  its canonical byte encoding. Define that contract before adding the missing producer; do not invent a hash or zero.
- Acceptance: two jobs with the same environment carry the same `envHash`; changing one variable changes it; the
  terminal span row and the record agree.

## Hard timeout

- Spec: `specs/cowshed/11_shell.md:414-416`: `[shell] hard_timeout` sends SIGTERM, then SIGKILL after the grace, drains
  both pipes, and records `killed:timeout`.
- Code: no `hard_timeout` setting is read anywhere in `packages/cowshed/crates/*/src`.
- Work: read `[shell] hard_timeout` from `.cowshed.toml` and arm it per job in the supervisor.
- Acceptance: a job that sleeps past the timeout ends `killed:timeout` with both streams drained; a job that handles
  SIGTERM is SIGKILLed after the grace.

## Cancellable log reads

- Spec: `specs/cowshed/07_api.md:1455` (`logs` takes `signal?: AbortSignal`) and `:1577` (cancellation covers
  `JobHandle.logs`).
- Code: `packages/cowshed/src/types.ts:159` and `src/index.ts:353` take `JobLogsArguments` with no signal, and the
  generated binding (`src/native.generated.ts:657`) has no abort path.
- Work: thread the signal through the generated binding, as `attach`/`wait` do.
- Acceptance: aborting a following read ends it promptly; aborting a non-following read rejects with an abort error; an
  unaborted read returns its full range.

## CPU budget through Cargo's jobserver

- Spec: `specs/cowshed/05_gateway.md` "Host CPU budget" (Outside `bounded-exec`, The jobserver fifo, The jobserver run,
  Where the launcher cannot budget) and `03_caches.md` "The `cargo` jobserver launcher": one jobserver fifo per client
  process. The launcher, first on `PATH`, picks one of four cases: an inherited live jobserver runs `cargo` as asked; a
  `bounded-exec` grant runs it with `CARGO_BUILD_JOBS`/`RUST_TEST_THREADS`/`NEXTEST_TEST_THREADS` set to
  `BOUNDED_EXEC_CPU_TOKENS` where unset; a subcommand that compiles nothing (`metadata`, `fmt`, `locate-project`,
  `--version`, … as listed in 05) runs directly; any other sends `{"op":"jobserver","want":<one per core>,…}`, gets a
  grant of `g` tokens plus a fifo path, spawns the next `cargo` on `PATH` with `CARGO_MAKEFLAGS`/`MAKEFLAGS` =
  `-j --jobserver-auth=fifo:<path>` and `RUST_TEST_THREADS`/`NEXTEST_TEST_THREADS`/`BOUNDED_EXEC_CPU_TOKENS` = `g` where
  unset, waits, and keeps the connection open while it lives. The gateway creates the fifo (`0600`, the client's user),
  holds its own `O_RDWR` descriptor on it until the drain, writes `g − 1` token bytes, and on connection close drains
  it, unlinks it, and returns all `g` tokens to the ledger, so tokens a killed process held are credited; it removes
  leftover fifos at startup.
- Code: `crates/cowshed-cli/src/launchd.rs:411` notes that cargo creates its own jobserver; the gateway serves none and
  has no `jobserver` control operation. No launcher exists: `crates/cowshed-core/src/runtime/supervisor.rs:901-920`
  builds the bootstrap `PATH` as the private `tools/bin`, then `.cowshed/bin/` (which holds the `xcrun` shim and runtime
  tool links, `capabilities/installations.rs:22-25`), then system directories; shell activation then owns `PATH`
  (`specs/cowshed/03_caches.md`, build wiring).
- Work: the `jobserver` operation (with `want`) in the gateway's control socket and `cpu_budget` ledger (fifo creation
  in the gateway's private runtime directory, ownership from peer credentials, the gateway's own `O_RDWR` descriptor,
  drain/unlink/return on close, the startup sweep of leftover fifos); the launcher with its four cases, the subcommand
  parse past global options and `+toolchain`, and the variables each case sets, resolving the next `cargo` by skipping
  its own directory; placing it ahead of shell activation's entries in the supervisor's spawn environment and in the
  `.envrc` wiring; the 05 jobserver and launcher tests.
- Acceptance: a host-shell `cargo build` and a `bounded-exec` task running together never hold more tokens than the host
  has cores (`{"op":"cpu-budget"}` shows both); a host-shell `cargo test` granted `g` tokens runs at most `g` test
  threads; a `SIGKILL`ed `cargo` mid-build leaves `held` back at its previous value and its fifo unlinked; a `cargo` run
  inside `bounded-exec` and a nested `cargo` from a build script take no second grant, and a `cargo build` started by a
  one-token `process` runner's script runs one job; `cargo metadata` on a saturated host returns without queueing.

## Job id in the proxy userinfo

- Spec: `specs/cowshed/03_caches.md:127-135` and `:273-275`: a job's proxy variables carry `job-<job_id>` as the
  userinfo username and the workspace token as the password. `05_gateway.md:66-70`: the gateway records that username as
  the request's job attribution and authenticates only the password. `13_telemetry.md` "Attribution tiers" tier 2: proxy
  traffic is job-exact even when a workspace's jobs overlap.
- Code: `crates/cowshed-core/src/runtime/supervisor.rs:946-951` (`gateway_proxy_url`) always writes the username
  `cowshed`, and the URL is built once per sandbox environment (`:1352`), not per job.
  `crates/cowshed-gateway/src/proxy.rs:2347-2370` decodes `Basic` and deliberately ignores the username.
- Work: build the four proxy variables per job at spawn, with `job-<job_id>` as the username; a warm shell host is
  activated before its job exists, so the job's values are applied at the spawn of its command, not at activation. In
  the gateway, parse the username, accept `job-<decimal job id>` as attribution and anything else as none, keep the
  password as the only credential, and write the job id on the gateway request span. Update the user docs that state the
  fixed `cowshed` username: `packages/cowshed/docs/gateway.md:30-36` and `:224`, and
  `packages/cowshed/docs/troubleshooting.md:105`.
- Acceptance: two overlapping jobs in one workspace each run `curl` through the proxy; each gateway span names its own
  job id. A request whose username is `job-<id>` with a wrong token is still 401. The existing
  `proxy_url_carries_the_token_as_basic_auth_userinfo` test is rewritten for the per-job URL.

## Escape-test crate and release gate

- Spec: `specs/cowshed/08_testing.md:425-429` and `00_overview.md:134`: the corpus runs in a `cowshed-escape-tests`
  crate on both OSes through the real exec pipeline, and green on both is an unwaivable release gate.
- Code: the cases live in `cowshed-core`'s sandbox tests and host-controller probes; there is no such crate, no Linux
  leg, and no gate.
- Work: move the corpus into the crate, cover every category in `08_testing.md:431-448`, and make the release workflow
  refuse to publish on a red run. The Linux leg arrives with the Linux substrate.
- Acceptance: a deliberately failing escape case blocks a release, and no flag waives it.

## cowshed-helper

- Spec: `specs/cowshed/09_substrates.md`, "Privileged helper (Linux)", `00_overview.md:137`, `08_testing.md:177`,
  `10_ci.md`, "Runner host": Linux ZFS mutations go through `cowshed-helper`, a minimal root binary with a fixed verb
  set that refuses any dataset outside `<pool>/cowshed/`. CI uses the ordinary target lifecycle, not a separate
  promotion verb.
- Code: no crate; an argument parser was started and removed (`29f212088`).
- Work: the crate, its fixed verbs, and dataset validation; part of the Linux substrate. Keep the constrained restore
  clone-swap and ordinary origin/dependent cleanup from `09_substrates.md`; no CI-refresh-only verb is required.
- Acceptance: the helper refuses a dataset outside `<pool>/cowshed/` and any verb outside its set. Removing a workspace
  destroys its clone before its origin snapshot, and live dependents still refuse reclamation. Restore retains its
  constrained clone-swap.

## Workspace-only operator quiesce and retirement

- Governing contract: specs/cowshed/11_shell.md, "Controller/supervisor protocol" (:273-284), defines drain as stopping
  admission, letting running jobs finish, then retiring; retirement releases the listener and lease before
  acknowledgement (:206-213). specs/cowshed/16_build_volumes.md, "Owned index links", names Nx and Cargo writer
  exclusion for seed capture, exempts other contributed paths, and forbids copying an index onto the source volume.
- Existing mechanism: runtime/supervisor_socket.rs:345-348 has internal Quiesce/Retire calls; :1317-1327 dispatches
  them. Its drain at :1503-1537 uses the existing cross-build request and verifies the live socket peer's kernel
  identity before retaining its exit watch. runtime/supervisor_manager.rs:205-217 exposes only Ensure/SameBuild,
  dispatched at :945-958. These paths are under packages/cowshed/crates/cowshed-core/src/; none is a public
  workspace-only operator verb.
- Contract gap: an operator cannot request that one workspace stop admission and retire through the existing control
  channel without also detaching or removing it. Define that operator contract before adding a CLI/API route; this entry
  authorizes no new wire operation. Preserve unrelated workspaces, sessions, jobs and mounts; no fleet stop, raw PID
  signal, forced detach or ad-hoc socket request substitutes for the missing route.
- Acceptance for that separate contract: the exact workspace stops admission, completes or reports its admitted jobs,
  retires its supervisor and Nx keeper, and acknowledges only after releasing its listener and lease; another workspace
  remains served. A subsequent seed capture uses the normal non-forced exclusion. Index refresh itself needs no drain:
  it leaves exact fixed links intact or discards a displaced real directory in .cowshed/discard and relinks it, never
  copying index bytes onto the source volume.

## Capture lease before a host Cargo profile exists

- Governing contract: `specs/cowshed/16_build_volumes.md`, "Targets and seeds", known new-host-profile gap. Seed capture
  retains closed supervised admission and the guards for existing Cargo profiles and Nx state, but uses a mounted
  block-atomic image clone so an exempt cache reader cannot block it.
- Current boundary: `packages/cowshed/crates/cowshed-core/src/build_volume/cargo.rs::hold` enumerates existing
  `.cargo-lock` files. Those locks cannot exclude a host Cargo process that creates a new profile after enumeration.
  Without the former detached-volume barrier, that host build can put intermediate non-transactional output into a seed.
- Required separate work: define one producer-side capture lease shared by every Cargo invocation, acquired before any
  profile creation or output write and retained for the build. Capture takes the exclusive side through snapshot and
  publication. Thread that contract through both supervised and host Cargo entry points; another holder census does not
  close the look-to-create race. This finding does not authorize an implementation or a new operator route.
- Acceptance: an event-coordinated real host Cargo build creates a previously absent profile after capture has
  enumerated its old profiles; the producer lease must block that creation or refuse the capture before any seed
  publication. A held existing profile and a live Nx holder still refuse, while an open index reader remains exempt.
