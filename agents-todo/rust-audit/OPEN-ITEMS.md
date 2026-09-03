# Rust audit — open items and operational state

Companion to `INDEX.md`. `INDEX.md` records what the audit FOUND; this records what is still owed, the conclusions that
are not visible in any diff, and the hazards a future session will otherwise rediscover the hard way.

## Landing procedure (proven)

`cowshed rebase` and `cowshed land` MUST NOT be used while `main` has been rewritten under a live workspace. Both replay
history, and on a rewritten upstream they recompute the merge base past the rewrite and attempt to replay commits that
no longer exist. Use this instead, per workspace, with a CLEAN tree:

```sh
# in the workspace
git fetch main
git rebase --onto main/main <fork-base> cowshed/<name>
cargo test -p <crate> && cargo clippy -p <crate> --all-targets -- -D warnings
# in the host checkout
git fetch /Users/danny/Dev/.cowshed/smoothbricks/codebase/<name> cowshed/<name>
git merge --ff-only FETCH_HEAD
```

Every workspace in this batch forked at `f7519d033` (pre-rewrite hash), so that is `<fork-base>` unless the workspace
was already replayed once. Verified end to end on `col-vm`: 11 commits, zero conflicts, 252/252 and clippy clean after
the replay.

## Workspace state

| Workspace                                                                  | Branch               | State                                                                                                                                                  |
| -------------------------------------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `col-vm`                                                                   | —                    | LANDED (11 commits)                                                                                                                                    |
| `cs-storage`                                                               | `cowshed/cs-storage` | 2 commits verified, ready to land; agent yielded. Uncommitted work was destroyed and is NOT recoverable — its inventory is in the agent's final report |
| `cs-coreapi`                                                               | `cowshed/cs-coreapi` | 1 commit; agent stopped on request budget with most of its assignment undone, resumable with full context                                              |
| `cs-runtime`, `cs-cli`, `cs-gateway`, `cs-seam`, `col-data`, `lmao-rs-fix` | `cowshed/<name>`     | in flight at time of writing; all instructed to commit everything and run no history operations                                                        |

Land order for `cowshed-core` is `cs-coreapi` first — its type changes are what other agents' call sites consume — then
`cs-runtime` and `cs-storage`.

## Owed by the coordinator, not yet done

1. **nx-plugin must MERGE a declared partial into an inferred target.** Nx replaces, and so does the plugin
   (`!(CARGO_TEST_TARGET in declared)` skips inference entirely when the key is present). Consequence, measured: adding
   only `dependsOn` to `columine`'s `cargo-test` collapsed the executor to `nx:noop` with empty options — a target that
   passes having run nothing. This is why `columine:cargo-test` cannot yet depend on `cargo-wasm` from `package.json`,
   and why `columine` is legitimately RED until it can.
2. **`cowshed-core` still has its own `validate_repo_id`.** `cs-gateway` canonicalised the three gateway copies behind
   one gateway-owned validator using core's stricter lowercase-only grammar. Core cannot be the callee — `cowshed-core`
   depends on `cowshed-gateway`, so the reverse direction is a cycle. Making core call the gateway validator, or
   extracting a shared identity crate, is the follow-up.
3. **`COWSHED_CONTINUITY_AUDIT` needs a CLI flag** (see Environment variables below).
4. **trybuild needs its own nextest bound.** `lmao-macros::trybuild compile_fail_cases` is terminated at exactly 30.005
   s by the global `slow-timeout = { period = "30s", terminate-after = 1 }` in `packages/nx-plugin/nextest.toml`, while
   its 82 sibling tests each finish under 0.11 s. The bound must come from a measurement taken on an OTHERWISE IDLE
   machine; any number taken while a fleet is compiling is CPU-starved and must not be encoded. See handbook §19.9 for
   why this test cannot be hoisted into `cargo-test-compile`.
5. **Darwin linker/SDK selection is per-cargo-workspace, and only `packages/cowshed` makes it.** cowshed's own layer
   manages `DEVELOPER_DIR` and nothing else: `developer_directory()` in
   `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs` exports it into every sandboxed child, accepting a
   path only under `/Applications`, `/Library/Developer` or `/System`. Linker choice is the project's:
   `packages/cowshed/.cargo/config.toml` points `[target.aarch64-apple-darwin] linker` and its x86_64 twin at
   `scripts/macos-linker.sh`, which re-derives `DEVELOPER_DIR` and `-isysroot` from `xcode-select`/`xcrun` and execs
   Apple's clang, so the Nix cc-wrapper's `NIX_LDFLAGS` never reaches the link. Cargo discovers that config by walking
   up from the invocation cwd, so it covers exactly the cargo runs rooted under `packages/cowshed/`. `packages/columine`
   and `packages/lmao` are separate cargo workspaces whose `.cargo/config.toml` sets no linker, so their macOS artifacts
   link through the cc-wrapper against whatever SDK it resolves; a repository-root cargo workspace would inherit nothing
   either. The follow-up is a decision about ownership, not an abstraction: either every cargo workspace that ships a
   macOS artifact carries the two `[target.*-apple-darwin]` lines and a workspace-relative copy of the script, or one
   repository-root config carries them once for every workspace and accepts a single repo-wide fingerprint. It does NOT
   belong in cowshed: `-C linker` is part of every crate's fingerprint (splitting it across Nx targets cost a full
   ~160-crate rebuild per macOS artifact — measured, see that config's own comment), and a cowshed-level linker policy
   would impose one toolchain on every project cowshed hosts.

## Unassigned property tests, ranked

From the audit's duplication findings. Two were implemented (`col-vm`); one is in flight (`col-data`); the rest exist
nowhere but here.

| Property                                                                                | Catches                                                                       | Report                                                |
| --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ----------------------------------------------------- |
| Same workload through all three hash tables yields identical membership/lookup          | three independent probe implementations                                       | `columine-vm-maps-intern-aggregates.md` F2            |
| `offset_of!(field) == H_*/FB_*/ID_*` for every field                                    | two layout sources; `H_FREELIST_EXACT` has no backing field                   | `lmao-arena.md` F1, handbook §7.10g                   |
| `compact → decode == original`                                                          | whole-batch rewrite with no round-trip guard                                  | `columine-event-processor.md`                         |
| Rust table == TS table for Opcode/ErrorCode/EntryType/SizeClass, read from both sources | makes the hand-alignments permanent; the existing registry test cannot go red | `xcut-rust-vs-typescript-duplication.md` F7           |
| Both conflict machines agree on every operation pair                                    | three verb enums, two checkers, one on-disk lifecycle                         | `cowshed-core-storage-lifecycle-recovery-audit.md` F1 |
| Apply-then-undo == identity over randomised sequences, including REFUSED operations     | side tables desyncing from slot bytes                                         | `columine-vm-minroar-bitmaps.md` F1                   |

## Environment variables

Inventory: ~15 distinct names read at runtime in Rust, 80 call sites / ~30 names in TypeScript. The large majority are
INBOUND platform contracts where the environment is the calling convention and no CLI argument exists — GitHub Actions
(10 names), Nx (`NX_WORKSPACE_ROOT_PATH` and friends), cargo build scripts (`TARGET`, `CARGO`, `CARGO_FEATURE_NAPI`),
POSIX (`HOME`, `PATH`, `XDG_CONFIG_HOME`, `DEVELOPER_DIR`, `CREDENTIALS_DIRECTORY`). Those are not a smell; they are
reading someone else's ABI.

Two are the genuine antipattern:

- **`LMAO_TIMESTAMP_PROOF_SHARED_MEMORY`** — a cargo feature implemented as an environment variable
  (`crates/lmao-timestamp-proof/build.rs:11-12`, `var_os(..).is_some()` plus `rerun-if-env-changed`). Cargo features are
  enumerable, composable, lockfile-visible and built by `--all-features`, so they cannot rot unbuilt; an env gate has
  none of that. Convert to `[features]`.
- **`COWSHED_CONTINUITY_AUDIT`** — a production mode selector (`arrow | off`) read in
  `storage/audit.rs::ContinuityAudit::from_environment`, whose only discovery path is an error hint telling the user to
  set it. `cowshed-cli` has a full clap grammar; this belongs in it. Note its test has to use
  `unsafe { std::env::set_var }`, which is the compiler pointing at process-global mutable state.

Softer, behaviour-selected-by-ambient-state: `LMAO_BENCH_MODE`, `LMAO_BENCH_TRANSFORM`,
`EXPO_PUBLIC_LMAO_BENCH_TRANSFORM`, `LMAO_TEST_TRACE_VERBOSE`, `LMAO_TEST_CLEANUP_DEBUG`, `NAPI_DEBUG_ADDON`.
Defensible: `COWSHED_FLOCK_HELPER_*` (five names) is a test re-execing itself as a lock holder, where env is the
conventional channel because the harness owns argv — though one encoded variable would beat five.

Not a finding: `MODE_ENVIRONMENT_VARIABLE` appears five times but is one test file's local constant naming
`LMAO_BENCH_MODE`.

## `COWSHED_NODE_PATH` and the napi path

`COWSHED_NODE_PATH` (`packages/cowshed/src/native.ts:100`) overrides the location of the N-API addon
`cowshed.<platform>-<arch>.node`. It has nothing to do with a Node.js interpreter; `.node` is the addon extension. The
name reliably manufactures the wrong question and should be `COWSHED_ADDON_PATH` or the ecosystem-standard name below.

**An env-var addon override is the napi-rs convention, not an anomaly.** Source-verified: `NAPI_RS_NATIVE_LIBRARY_PATH`
is the FIRST branch of every generated loader (`napi-rs/cli/src/api/templates/js-binding.ts`, `requireNative()`),
present verbatim in shipped loaders of rolldown (`packages/rolldown/src/binding.cjs:64`), oxc parser
(`napi/parser/src-js/bindings.js:68`), `@node-rs/argon2` and `@node-rs/bcrypt` (`index.js`/`binding.js:64`). It is
documented on napi.rs, and rolldown instructs users to set it (`docs/guide/getting-started.md:80`). `@swc/core` adds a
second, `SWC_BINARY_PATH`. Counter-examples with NO env override: `nodejs-polars` (`polars/native-polars.js`, zero
`process.env`) and `lightningcss` — both resolve per-platform `optionalDependencies` first and fall back to a local path
convention, and both load a debug addon by overwriting the same output path rather than by variable.

So the anomalies here are the bespoke NAME, the second bespoke debug variable (`NAPI_DEBUG_ADDON`), and the bundled
multi-platform directory instead of `optionalDependencies`. The last one is explained, not accidental: `cab66c110`
records that separate platform packages "were rejected because smoo scopes release candidates to each package root and
cannot keep sidecar packages in lockstep without release-group support".

**The napi CLI predates the native binary.** `c67631547` (2026-07-14) added the Bun/Node bindings and
`COWSHED_NODE_PATH`; `cab66c110` (2026-08-14, 441 commits later) shipped the real binaries behind the exec trampoline
and explicitly "keeps runCli as the final fallback". That fallback is what forces `cowshed-napi` to link `cowshed-cli`
and therefore hyper, rustls/ring and Arrow — `cowshed-napi-workspace-manifests.md` F3.

## Latent hazards, labelled honestly

- **`cowshed rebase` recovery path.** `cowshed-core/src/runtime/project.rs:7989-7996` discards the result of
  `git rebase --abort` (`let _ = ...`) and then runs `git reset --hard` unconditionally. The code therefore cannot tell
  whether the lossless path already succeeded before it takes the destructive one. This was initially blamed for
  destroying uncommitted work in two workspaces and that attribution is RETRACTED: the reflog shows the rebase actually
  started, which git will not do on a dirty tree without autostash, and no fresh autostash exists. The mechanism of that
  loss is unexplained. The discarded-Result pattern remains worth fixing on its own merits (handbook §7.10f), and
  `cowshed rebase` should additionally refuse to start on a dirty tree — the codebase already has `is_dirty()` +
  `removal_dirty_refusal` for exactly this shape.
- **Pre-existing stash entries ride into cloned workspaces.** Every cowshed workspace inherits the image's stash list,
  including entries labelled `autostash` dated months earlier. A recovery attempt that reaches for `stash@{0}` will
  apply unrelated historical changes; one such apply left `UU bun.lock`/`UU package.json` conflicts. Check
  `git stash list --date=short` before trusting any entry.
- **`cargo-test-cowshed-escape-tests`** runs a crate whose entire source is one doc comment and zero tests — a target
  that passes having executed nothing.

## Cross-language divergences still open

- `packages/lmao/src/lib/wasm/wasmAllocator.ts` declares `SizeClass.Identity = 4` although identity has a dedicated
  freelist and `lmao-wasm` now rejects unknown class 4 with sentinel 0. `isWasmExports` should also validate every
  export `wrapWasmInstance` consumes.
- The `JobInfo.argv` CRITICAL requires the Rust `ExecRecord.argv → Vec<CommandArg>` change and the TypeScript type to
  converge on one wire shape.
