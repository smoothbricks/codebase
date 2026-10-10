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

- Spec: `specs/cowshed/11_shell.md:514-515` (`envHash` beside `grantRevision` on every job record) and
  `13_telemetry.md:32-33`, `:53` (`env_hash` on job spans).
- Code: `ExecRecord.env_hash` exists in `crates/cowshed-core/src/api/dto.rs:2051`, but no record or span carries a
  computed value; `crates/cowshed-core/src/runtime/job_spans.rs:8` says the supervisor does not compute it.
- Work: hash the spawned job's environment once at spawn (`runtime/supervisor.rs:1268` `sandbox_environment` plus the
  request's env), store it in the protected record's admission batch and the terminal span row.
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

- Spec: `specs/cowshed/05_gateway.md:443-445`: processes outside `bounded-exec` take the CPU budget through Cargo's
  make-protocol jobserver, which hands out the same tokens.
- Code: `crates/cowshed-cli/src/launchd.rs:411` notes that cargo creates its own jobserver; the gateway serves none.
- Work: the gateway serves a jobserver backed by the same token ledger, and the managed environment names it to cargo.
- Acceptance: a host-shell `cargo build` and a `bounded-exec` task running together never hold more tokens than the host
  has cores (`{"op":"cpu-budget"}` shows both).

## Escape-test crate and release gate

- Spec: `specs/cowshed/08_testing.md:425-429` and `00_overview.md:134`: the corpus runs in a `cowshed-escape-tests`
  crate on both OSes through the real exec pipeline, and green on both is an unwaivable release gate.
- Code: the cases live in `cowshed-core`'s sandbox tests and host-controller probes; there is no such crate, no Linux
  leg, and no gate.
- Work: move the corpus into the crate, cover every category in `08_testing.md:431-448`, and make the release workflow
  refuse to publish on a red run. The Linux leg arrives with the Linux substrate.
- Acceptance: a deliberately failing escape case blocks a release, and no flag waives it.

## cowshed-helper

- Spec: `specs/cowshed/09_substrates.md:277`, `00_overview.md:137`, `08_testing.md:177`, `10_ci.md:22,143`: Linux ZFS
  mutations go through `cowshed-helper`, a minimal root binary with a fixed verb set that refuses any dataset outside
  `<pool>/cowshed/`.
- Code: no crate; an argument parser was started and removed (`29f212088`).
- Work: the crate, its fixed verbs, and dataset validation; part of the Linux substrate.
- Acceptance: the helper refuses a dataset outside `<pool>/cowshed/` and any verb outside its set.
