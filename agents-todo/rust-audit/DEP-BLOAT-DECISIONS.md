# DEP-BLOAT decisions

Verdict on all 25 DEP-BLOAT findings, judged against `main` at `17a95a816`.

Two agents produced this: judgment and manifest archaeology here, build evidence from a second workspace (`dep-grok`).
No dependency change is landed by either. Every CUT-SAFE row below names a build that was run and reverted; every KEEP
row names the mechanism that makes the dependency load-bearing.

## The rule this category needs

**A dependency's contract lives in its own `#[cfg]` attributes, not in our usage.** Reading our call sites cannot decide
whether a feature is dead, because a feature can gate the _existence of the type or method we already call_. Two
findings in this batch fail on exactly that, each with a compile error to prove it. A third fails on the adjacent
mistake — reasoning from an unused _writer_ to conclude an in-use _reader_ is disposable:

| dependency                 | what our code names                            | what the feature actually gates                                                |
| -------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------ |
| `toml` 1.x `serde`         | `Table`, `Value` in `sccache_client_config.rs` | the `Table`/`Value` types themselves are `#[cfg(feature = "serde")]`           |
| `rcgen` 0.14 `x509-parser` | `Issuer` (imported without the parser)         | `Issuer::from_ca_cert_pem` is `#[cfg(feature = "x509-parser")]`                |
| `arrow-ipc` (columine)     | `try_schema_from_ipc_buffer`                   | nothing — but the "unused writer" reasoning drops a FlatBuffers Schema decoder |

The first cost a broken workspace tonight. The second was refuted by `cargo check` before it could:

```
error[E0599]: no associated function or constant named `from_ca_cert_pem` found for struct `Issuer<'a, S>` in the current scope
   --> crates/cowshed-gateway/src/tls.rs:28:30
    |
 28 |         let issuer = Issuer::from_ca_cert_pem(&material.certificate_pem, key)
    |                              ^^^^^^^^^^^^^^^^ associated function or constant not found in `Issuer<'_, _>`
```

**A lockfile row is not a compiled unit.** Four findings quantify bloat by counting `Cargo.lock` package entries.
Measured with `cargo tree --target <triple> --edges normal`, unique `name vX.Y.Z`:

| workspace           | `Cargo.lock` rows | compiled units, `aarch64-apple-darwin` | rows that never compile |
| ------------------- | ----------------- | -------------------------------------- | ----------------------- |
| `packages/cowshed`  | 266               | 177                                    | 33%                     |
| `packages/lmao-rs`  | 144               | 45                                     | 69%                     |
| `packages/columine` | 102               | 40                                     | 61%                     |

`jni`, `rustls-platform-verifier-android`, `bit-vec`, `r-efi`, `windows-sys`, `windows-targets` and all nine `windows_*`
crates are **zero rows** on both `aarch64-apple-darwin` and `x86_64-unknown-linux-gnu`.

**A dependency that checks another thing is not bloat.** `lmao-query`'s deleted SQLite arm was the only independent
oracle over a hand-rolled Arrow scan; the deletion only stands because `crates/lmao-query/tests/oracle.rs` replaced it.
No verdict below proposes deleting a test, oracle, parity arm, or differential check to shrink a graph.

## Tally

| verdict           | count |
| ----------------- | ----- |
| ALREADY-RESOLVED  | 11    |
| CUT-SAFE          | 3     |
| KEEP              | 10    |
| NEEDS-MEASUREMENT | 1     |

Measurement regime for every build below: warm target directory, sibling `cargo` processes contending. A compile-fail
test measured 4.0s idle, 12.8s inside a full crate test, and 100.7s under sibling contention tonight — a 25x spread — so
no wall-clock number here is quoted without that regime, and none is used as evidence for a cut.

---

## CUT-SAFE — 3

Each was applied, built, and reverted in `dep-grok`. Nothing is landed.

### `cowshed-gateway-policy-config-platform-tls` F7 — MEDIUM — `rustls-pki-types` is unused

Correct. The crate names `rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName}` (`tls.rs:8-11`,
`interfaces.rs:5`) and never `rustls_pki_types`; `rustls` re-exports the module.

Manifest edit — `packages/cowshed/crates/cowshed-gateway/Cargo.toml`, delete one line:

```toml
rustls-pki-types = "1"
```

Build: `RUSTC_WRAPPER= cargo check -p cowshed-gateway --all-targets` → exit 0, 2.41s (warm, contended).

`Cargo.lock` changes by exactly one line: `rustls-pki-types` leaves `cowshed-gateway`'s dependency list. The **package
entry stays** — `rcgen`, `rustls` and `rustls-pemfile` still parent it. So this removes a redundant direct declaration
and one version to keep aligned by hand; it removes zero bytes from the binary. That is the whole benefit and it is
worth taking on those terms, not on a size claim.

### `xcut-dependency-bloat-sweep` F7 — LOW — unused `workspace.dependency` keys

Three dead keys, confirmed by grep across each workspace's crate manifests and sources (zero references, and none
resolves into its lockfile):

```toml
# packages/cowshed/Cargo.toml
anyhow = "1"

# packages/columine/Cargo.toml
rustc-hash = "2"
criterion = { version = "0.5", default-features = false }
```

Build: see `## Build ledger`. `Cargo.lock` unchanged in both workspaces — an unreferenced `workspace.dependencies` key
does not resolve.

The finding's own reason is the right one and it is not about size: a dead key is a loaded gun for the next crate that
writes `anyhow.workspace = true` by habit. Note `rustc-hash` is load-bearing in **`packages/lmao-rs`**
(`lmao-arrow/src/dict.rs` records the hasher measurement) — only the columine key is dead.

### `columine-wasm-exports` F5 — MEDIUM — `wasm-perf` and `wasm-s` are dead profiles

Confirmed: `wasm-perf` and `wasm-s` appear nowhere in `packages/columine` except their own definitions at
`Cargo.toml:36` and `:41` — no `justfile` recipe, no script, no CI reference.

```toml
[profile.wasm-perf]
inherits = "release"
opt-level = 3
panic = "abort"

[profile.wasm-s]
inherits = "release"
opt-level = "s"
panic = "abort"
```

Two honest qualifications the finding does not make. A cargo profile costs **nothing** until invoked, so this is dead
configuration, not compile weight — do not sell it as a build-time win. And `wasm-perf` was the handbook §4.1 escape
hatch for separating `opt-level = "z"` outlining cost from structural cost; deleting it deletes the recipe for that
attribution. Deleting the profile is right _because_ nothing invokes it; if size-vs-speed attribution is wanted back, it
returns as a `just` recipe plus the profile, not a profile alone.

`[profile.wasm-release]` and the two per-package `opt-level = 3` overrides on `columine-vm` and `columine-parsing` are
live and must stay.

---

## KEEP — 10

### `cowshed-cli-sccache-probe-skill` F1 — MEDIUM — `toml` `serde` feature is unused

**REFUTED, and it broke the workspace before it was refuted.** In `toml` 1.x, `toml::Table` and `toml::Value` are
themselves behind `#[cfg(feature = "serde")]`. `sccache_client_config.rs` reads the config through both. Dropping the
feature does not trim a feature — it deletes the types, and `cargo check -p cowshed-cli` fails with
`cannot find type Table in crate toml`. Six compile errors; every crate linking `cowshed-cli` went red.

The mechanism is now recorded in `packages/cowshed/crates/cowshed-cli/Cargo.toml` above the dependency
(`3040f0482 docs(cowshed): name why the toml serde feature is load-bearing`), and `Cargo.lock` needed `serde_core` added
to recover. The finding's evidence — "nothing in this crate serializes" — was true and irrelevant.

### `xcut-dependency-bloat-sweep` F6 — MEDIUM — gateway `rcgen` enables `x509-parser` that `tls.rs` does not call

**REFUTED by compile error.** `rcgen` 0.14 gates `Issuer::from_ca_cert_pem` behind `#[cfg(feature = "x509-parser")]`.
`tls.rs:28` calls it. The finding's evidence — "gateway `tls.rs` imports `rcgen::{CertificateParams, Issuer, KeyPair}`
only" — is evidence about the wrong side of the boundary: we do not import the parser crate, we call a method the parser
feature creates.

Build: dropping `"x509-parser"` from the `[dependencies]` `rcgen` features →
`RUSTC_WRAPPER= cargo check -p cowshed-gateway --all-targets` fails with the `E0599` quoted at the top of this document.
Reverted. The `x509-parser` feature stays; core's `x509-parser` + `verify` stays.

**One rider from this finding is separately correct and verified safe.** The gateway `[dev-dependencies]` `rcgen` line
restates the dependency with _fewer_ features, which unification ignores:

```toml
# packages/cowshed/crates/cowshed-gateway/Cargo.toml — [dev-dependencies]
rcgen = { version = "0.14", default-features = false, features = ["crypto", "pem", "ring"] }
```

Deleting that line alone: `RUSTC_WRAPPER= cargo check -p cowshed-gateway --all-targets` → exit 0 in 0.76s, **no unit
recompiled**, `Cargo.lock` unchanged. It is a no-op restatement, same class as `rustls-pki-types`. Take it as a tidy; it
is not what the finding claimed.

### `xcut-dependency-bloat-sweep` F4 — MEDIUM — `rustls-platform-verifier` drags JNI/Android/openssl-probe in

The JNI claim counted lockfile text. `cargo tree --target <triple> --edges normal` in `packages/cowshed`:

| target                     | `jni` rows | `openssl-probe` rows | `rustls-platform-verifier` direct deps                                                                    |
| -------------------------- | ---------- | -------------------- | --------------------------------------------------------------------------------------------------------- |
| `aarch64-apple-darwin`     | **0**      | **0**                | `core-foundation`, `core-foundation-sys`, `log`, `rustls`, `security-framework`, `security-framework-sys` |
| `x86_64-unknown-linux-gnu` | **0**      | 1                    | `log`, `rustls`, `rustls-native-certs`, `rustls-webpki`                                                   |

`jni` and `rustls-platform-verifier-android` are `cfg(target_os = "android")` and never enter a compile on either
shipped target. `security-framework` and `security-framework-sys` are already direct
`[target.'cfg(target_os = "macos")'.dependencies]` of `cowshed-gateway`, so the verifier's **net new** cost on macOS is
three crates: `core-foundation`, `core-foundation-sys`, `log`. On Linux it is `log`, `rustls-native-certs` and
`rustls-webpki`, with `openssl-probe` as the one genuinely extra node.

Against that, the proposed fix hand-writes platform root-trust verification for two operating systems. That is the
single highest-consequence class of change in a TLS proxy, bought for one small crate on one target. KEEP. The
`ConfigVerifierExt` call site at `interfaces.rs:6` stays as it is.

### `cowshed-napi-workspace-manifests` F8 — MEDIUM — 14 duplicate crate versions in `Cargo.lock`

Eleven of the fourteen are **zero compiled rows** on both shipped targets. `cargo tree --edges normal` per target:

| duplicated name                                  | darwin    | linux     | why                                          |
| ------------------------------------------------ | --------- | --------- | -------------------------------------------- |
| `getrandom` 0.2 / 0.3 / 0.4                      | all three | all three | three independent majors, below              |
| `hashbrown` 0.16 / 0.17                          | both      | both      | arrow graph vs std-adjacent consumers        |
| `bit-vec` 0.8 / 0.9                              | none      | none      | reachable only through `proptest`, a dev-dep |
| `r-efi` 5 / 6                                    | none      | none      | `cfg(target_os = "uefi")` inside `getrandom` |
| `windows-sys`, `windows-targets`, 9× `windows_*` | none      | none      | `cfg(windows)`                               |

The three `getrandom` majors are each demanded by a different graph node, verified by `cargo tree -i`:

```
getrandom v0.2.17 └── ring v0.17.14 ── rcgen / rustls / tokio-rustls
getrandom v0.3.4  ├── ahash v0.8.12 ── arrow-array v56.2.1
                  └── cowshed-core (direct, `getrandom::fill`)
getrandom v0.4.3  └── uuid v1.23.5
```

There is no manifest edit that unifies those. A `[patch]` would be a claim about `ring`'s and `uuid`'s MSRV graphs that
we are not entitled to make. The finding's own fix already says "leave `windows-*` alone" and "once core's `0.3` can
move" — that is correct and it is a wait, not a task. Its "14 duplicates" headline is the part to discard.

### `xcut-dependency-bloat-sweep` F8 — LOW — `getrandom` three times in every lockfile

Same mechanism as the row above, and the finding reaches the right answer itself: "no local fix that is honest… a forced
unify would be a lie about those crates' MSRV graphs." Recorded as KEEP so it is not re-discovered: three majors, three
independent parents (`ring` 0.2, `ahash`+`cowshed-core` 0.3, `uuid` 0.4), not collapsible from our manifests. Do not add
a fourth.

### `cowshed-gateway-cache-telemetry` F11 — LOW — `uuid` in `cache.rs` is a temp-name RNG

The premise is that `cache.rs:315` is the only use, making `uuid` a heavyweight temp-name generator. It is not. In
`cowshed-gateway` production code alone:

- `cache.rs:315` — temp fill name, `Uuid::new_v4().simple()`
- `telemetry.rs:140`, `:217`, `:526` — `writer_id: Uuid`, written into segment names
- `repo_mirror.rs:955` — temp directory; `:989` — the **published** `{uuid}.git` name

plus tests in `cache.rs`, `control.rs`, `platform.rs`, `sim_broker.rs`, `tests/control_brokers.rs`,
`tests/mirror_cache.rs`. A published mirror path name and a telemetry writer identity are not temp-name RNG, and `uuid`
is a crate-wide dependency of `cowshed-core` as well (`serde`, `v4`). KEEP.

### `cowshed-gateway-proxy` F15 — LOW — `base64` here is load-bearing; do not shell out

The finding's own verdict, and it is right. `proxy.rs:2374` decodes an HTTP Basic header the client already sent, in
process, per request. The `git2 → git(1)` precedent does not transfer: there is no per-request CLI, and hand-rolling
Base64 would put a second decoder next to the `STANDARD` engine used by `mirror.rs` and `tests/gateway.rs`. KEEP.

Recorded because a later sweep will re-propose it. The `uuid`-unused-by-`proxy.rs` remark in the same finding is
answered by F11 above.

### `columine-arrow` F4 — MEDIUM — `arrow-ipc` decodes Schema only; RecordBatch IPC is hand-rolled

The finding's own conclusion is KEEP and the reasoning is sound, so this is a ratification, not a reversal. The
production use is `arrow_ipc::{MessageHeader, convert::try_schema_from_ipc_buffer, root_as_message}` at
`schema.rs:7-8,199-218` — a FlatBuffers Schema decode of **untrusted bytes arriving over FFI**. The hand-rolled writer
in `record_batch.rs` and `ipc.rs` does not duplicate `arrow-ipc`'s writer (that writer is unused) and does not duplicate
`lmao-arrow` (which produces `RecordBatch` values and writes no IPC). Dropping `arrow-ipc`/`arrow-schema` requires
writing a real FlatBuffers Schema reader; getting that wrong ships invalid-schema acceptance at the processor-create
boundary. `default-features = false` is already set at the workspace root.

Verified on the shipped `wasm32-unknown-unknown` graph: `columine-arrow`'s cost through `arrow-array` is
`chrono v0.4.45 → num-traits` and nothing else — no `wasm-bindgen`, no `iana-time-zone`, no clock feature. The Arrow
dependency is already at its minimum here.

Two riders from this finding, both correct, both outside the dependency question: delete `logical_types` from the
retained schema config (dead after validation), and delete the unused `proptest` dev-dep — see `## Build ledger`.
`arrow-array` in the same `[dev-dependencies]` block is the `StreamReader` oracle in `ipc.rs` tests and stays.

### `columine-wasm-exports` F9 — LOW — lockfile duplicate versions off the shipped wasm graph

Answered by a build, which nobody had done.
`cargo tree --target wasm32-unknown-unknown -p columine-ep-wasm --edges normal`:

- `wasm-bindgen` — **absent**
- `iana-time-zone` — **absent**
- `chrono` — present, via `arrow-array v56.2.1 → chrono v0.4.45 → num-traits`, with no `clock` and no `wasmbind`
- `getrandom` — only `0.2.17`, via `ahash → const-random-macro`, itself a proc-macro. `0.3` and `0.4` are not on the
  wasm graph at all
- `r-efi` — absent

The shipped wasm graph compiles 40 of the workspace's 102 lockfile rows. The finding's conditional fix — "if
`wasm-bindgen` links, disable chrono's clock/iana features for wasm32" — is refuted: it does not link, and
`default-features = false` at the workspace root has already taken chrono to its floor. The duplicate
`getrandom`/`r-efi` rows are lockfile text that never reaches `event_processor.wasm`. No change in either wasm manifest,
which is what the finding itself recommends.

### `cowshed-core-storage-bootstrap` F7 — MEDIUM — System.keychain is driven with `security(1)` through AEWP

**Misfiled as DEP-BLOAT: the dependency graph is invariant under this change, in both directions.** Verified:

- `#[link(name = "Security", kind = "framework")]` at `storage/bootstrap/native/macos.rs:3043` already links
  Security.framework into `cowshed-core` for `AuthorizationCreate`/`AuthorizationCopyRights`. `SecItemCopyMatching`,
  `SecItemAdd` and `SecItemDelete` are in that same framework — the migration adds **no crate and no framework**.
- `cowshed-gateway/src/platform.rs` already reads keychain generic-password items in process, typed, through the
  `security-framework` crate; and `cowshed-core` depends on `cowshed-gateway`, so that crate is _already_ in
  `cowshed-core`'s macOS compiled graph.

So there is no dependency to cut and none to add, and no size or compile argument either way. KEEP on the dependency
axis.

The finding's real content is operational truth and it is correct on that axis: `security(1)` under
`AuthorizationExecuteWithPrivileges` loses stderr, and `macos.rs:954-957` treats empty stdout as "missing", which cannot
distinguish "no such item" from "the subprocess failed". `5d9dfb625 fix(cowshed): propagate AEWP child exit status`
fixed the sibling half of that. **Re-file the remainder outside DEP-BLOAT**, and note it interacts with the restructure
below: if `cowshed-core` stops linking the gateway daemon, `security-framework` leaves core's graph, and the migration
should then use raw `SecItem*` FFI against the already-linked framework rather than pulling the crate back in directly.

Its two dependency riders are ratified: `uuid::parse_str` is RFC 4122 validation of APFS volume UUIDs (`macos.rs:378`,
`:968`), not a temp-name RNG; `getrandom` + `zeroize` are the passphrase path.

---

## NEEDS-MEASUREMENT — 1

### `cowshed-core-gateway-inventory-sessions` F4 — MEDIUM — `cowshed-core` links the full gateway daemon for types + a Unix client

The finding is right about direction and right about grain, and it is the most valuable open item in this category — it
is also the true owner of the tail that `cowshed-napi` F3 could not close. **The benefit is now measured; the cost is
bounded and one structural constraint decides whether it is real.**

**Benefit, measured.** `cargo tree -p <pkg> --target aarch64-apple-darwin --edges normal`, unique `name vX.Y.Z`, against
a set-union of `cowshed-core`'s other direct dependencies plus the protocol crate's:

```
cowshed-core   154 compiled units today  ->  126 after the split   (26 third-party crates leave)
cowshed-napi   167 compiled units today  ->  ~140
```

The 26: `h2`, `hyper`, `hyper-util`, `http-body`, `http-body-util`, `httparse`, `httpdate`, `rustls`, `rustls-pemfile`,
`rustls-platform-verifier`, `rustls-webpki`, `tokio-rustls`, `security-framework`, `security-framework-sys`,
`core-foundation`, `tokio-util`, `tracing`, `tracing-core`, `futures-channel`, `futures-core`, `futures-sink`,
`atomic-waker`, `fnv`, `slab`, `try-lock`, `want`. That is the entire HTTP server+client stack and the entire TLS stack
leaving the Node addon.

`rcgen`, `x509-parser`, `ring` and `arrow-*` do **not** leave — `cowshed-core` declares them directly for
`workspace_credentials.rs` and the audit batches. Any pitch for this split that claims otherwise is wrong.

**Cost, measured — and it is smaller than the module line counts suggest.** The back-edges were enumerated
(`grep -n "crate::" config.rs policy.rs control.rs`, excluding the three peers):

| module       | lines | back-edges out of the protocol set                                                                                                                                                                                                                                                                                                                                                                |
| ------------ | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy.rs`  | 641   | **none**. Clean.                                                                                                                                                                                                                                                                                                                                                                                  |
| `config.rs`  | 692   | `cache::{CacheConfig, DEFAULT_HIGH_WATER_BYTES, DEFAULT_LOW_WATER_BYTES}`, reached only by `pub(crate) MirrorCacheConfig::cache_config()`, which builds the daemon's cache type and stays daemon-side. `GatewayConfig` itself holds `PathBuf`/`u64`. Plus `crate::validate_repo_id` (`repo_id.rs`), a small helper that moves with it.                                                            |
| `control.rs` | 1168  | **not a protocol module.** `GatewayControlClient` plus the request/response serde types — roughly the first 200 lines — is the client. The rest is the daemon control plane: `ControlServices { GatewayHandle, RepoMirrorHandle, SimBrokerHandle, Option<AuditTailHandle> }` at `:602` and the dispatch at `:866+` calling into `repo_mirror`, `sim_broker`, `telemetry`, `interfaces`. It stays. |

So the move is roughly **1500 lines**, not 2501: `policy.rs` whole, `config.rs` less the `cache_config()` bridge, the
`GatewayControlClient` client half of `control.rs`, `GatewayStatus`/`SessionStatus` out of `actor.rs`,
`validate_repo_id` out of `repo_id.rs`, and the `GatewayInstaller` trait out of `cowshed-core` — plus the re-export
surface in `cowshed-gateway/src/lib.rs` and CLI `gateway_service.rs`. Those modules import only `base64`, `subtle`,
`thiserror`, `zeroize`, `http`, `serde`, `url`, `tokio` — no `hyper`, no `rustls`, no `rcgen` — so the protocol crate is
genuinely thin. Splitting `control.rs` along the client/control-plane line is the only part that takes judgment rather
than a file move.

**The constraint that decides it.** Twelve of the thirteen items `cowshed-core` imports are plain serde data plus a
socket client. `WorkspaceCa` is `{ certificate_pem: String, private_key_pem: Zeroizing<String> }` — PEM strings, not
`rcgen` or `rustls` types. `EgressGrant` needs `http::Method`, not `hyper`. `GatewayControlClient` holds a `PathBuf` or
`SocketAddr` and uses `tokio::net` internally.

The thirteenth is `GatewayHandle { commands: mpsc::Sender<Command> }`, and `Command` carries `rustls::ServerConfig`. It
is the in-process daemon actor handle and cannot leave the daemon crate. `cowshed-core` is forced to name it today by an
orphan-rule accident: `trait GatewayInstaller` is defined at `cowshed-core/src/gateway_sessions.rs:140` and
`impl GatewayInstaller for GatewayHandle` at `:145`, so the crate that owns the trait must link the crate that owns the
type.

The cutover that resolves it: **move the trait into the protocol crate and the impl into the daemon crate.**
`cowshed-core` then holds `dyn GatewayInstaller` and never names `GatewayHandle`; `cowshed-gateway` provides
`impl GatewayInstaller for GatewayHandle`; `cowshed-cli`, which links the daemon anyway, constructs it. No cycle, no
orphan violation, and in-process install still works — which matters, because the alternative ("core talks only through
`GatewayControlClient`") would delete a capability to win a compile-graph argument.

**What is still unmeasured, and how to get it.** Two numbers, in this order:

1. Does it compile? Create `crates/cowshed-gateway-protocol` with `policy.rs`, the data half of `config.rs`, the
   `GatewayControlClient` half of `control.rs`, the two status structs and the `GatewayInstaller` trait; point
   `cowshed-core` at it; move `impl GatewayInstaller for GatewayHandle` into `cowshed-gateway`. Then
   `RUSTC_WRAPPER= cargo check --workspace --all-targets` in `packages/cowshed`. The back-edge table above says the only
   judgment call is where the `control.rs` client stops and the control plane starts.
2. Stripped byte size of the built `.node` before and after — the 26-crate figure is a compiled-unit count, not a size
   claim, and this restructure should be sold on the artifact number.

**`bun run check:linux` at the repo root is mandatory before this lands**, not optional. `policy.rs` has zero `#[cfg]`.
`config.rs` has all of it, and it is not incidental — the protocol crate would own the platform contract for every
session install. `WorkspaceSession::validate` calls `WorkspaceEndpoint::validate_for_current_platform`
(`config.rs:56-73`), which is a three-way fork:

```rust
    pub fn validate_for_current_platform(&self) -> Result<(), ConfigError> {
        #[cfg(target_os = "macos")]
        {
            self.validate_macos_port_block()
        }
        #[cfg(target_os = "linux")]
        {
            self.validate()?;
            match self {
                Self::Unix(_) => Ok(()),
                Self::Tcp(_) => Err(ConfigError::ExpectedUnixEndpoint),
            }
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(ConfigError::UnsupportedHostPlatform)
        }
    }
```

macOS admits only `Tcp`, inside the 16-port 40960–49151 block. Linux admits only `Unix` and rejects `Tcp`. The two arms
have **no overlapping accepted input**, so a macOS-only check of the protocol crate cannot see the Linux arm at all.
Three more Linux-only families move with it: `GatewayConfig::validate:478-485` (requires `data_socket_root`),
`validate_session_endpoint:532-561` (socket must live under `data_socket_root`, must not equal the control socket, must
be named `{ident}.sock` — and the non-Linux body is `let _ = session; Ok(())`), and `validate_data_socket_root:564-581`
(absolute, not a symlink, `uid == geteuid()`, `mode & 0o077 == 0`).

That last one is a permission check on a socket directory. A split that is green on `aarch64-apple-darwin` and red on
Linux is exactly the class this repo uses `check:linux` to catch, and here the failure mode is not a compile error — it
is validation that silently becomes `Ok(())` on the wrong target.

Verdict is NEEDS-MEASUREMENT rather than a go **only** because nobody has built it. The 26-crate benefit and the
~1500-line cost are both in hand and both point the same way.

---

## ALREADY-RESOLVED — 11

Closed by tonight's landed work. Each was confirmed against the current manifest, not taken on report.

| finding                                                                                                 | sev    | closed by                                                                                                                                                | current state on `17a95a816`                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lmao-macros` F2 — `syn` `full` parses an `Expr` never inspected                                        | HIGH   | `c564449b6 fix(lmao-rs): drop syn full from lmao-macros`                                                                                                 | `syn = { version = "2", default-features = false, features = ["derive", "parsing", "printing", "proc-macro"] }`. Went past the finding: `clone-impls` dropped too, and `span!` stayed a proc-macro parsing token trees to a comma instead of moving to `macro_rules!`.                                                                                                                                |
| `lmao-query` F1 — drop the DataFusion backend                                                           | HIGH   | `a48f78a48 fix(lmao-rs): drop unused datafusion and sqlite query backends`, then `9d495a77f test(lmao-rs): differential oracle for the Arrow trace scan` | `lmao-query` dependencies are `arrow-array` + `arrow-schema`. `datafusion_backend.rs` gone.                                                                                                                                                                                                                                                                                                           |
| `lmao-query` F2 — drop bundled `rusqlite`; sink is TS `bun:sqlite`                                      | HIGH   | same two commits                                                                                                                                         | `rusqlite` gone, `bundled` `cc` compile gone. **The deletion only stands because of the second commit**: the SQLite arm was the sole independent oracle over a hand-rolled Arrow scan, and it mapped a query error to `count = 0` via `unwrap_or(0)`, so it could not go red for what it existed to catch. `crates/lmao-query/tests/oracle.rs` is the model-vs-arrow differential that replaced it.   |
| `xcut-dependency-bloat-sweep` F2 — DataFusion 47 is a 224-node graph behind an optional feature         | HIGH   | `a48f78a48`                                                                                                                                              | Feature and crate gone; `packages/lmao-rs/Cargo.lock` is 144 rows / 45 darwin units. The `arrow` umbrella left with it.                                                                                                                                                                                                                                                                               |
| `xcut-dependency-bloat-sweep` F3 — cowshed `tokio features = ["full"]` at the workspace root            | HIGH   | `f2f793e14 chore(cowshed): name the tokio features used and drop the empty member`                                                                       | `tokio = { version = "1", features = ["fs", "io-std", "io-util", "macros", "net", "process", "rt", "signal", "sync", "time"] }` with a comment naming why each is reached and why `rt-multi-thread` is not. Gateway's `test-util` dev-dep kept. Same commit removed the empty `cowshed-escape-tests` member.                                                                                          |
| `xcut-dependency-bloat-sweep` F5 — `cowshed-napi` links the entire CLI into the cdylib                  | MEDIUM | `ef05910a1 refactor(cowshed): stop linking the CLI into the Node addon`                                                                                  | `cowshed-cli` moved to `[dev-dependencies]`, with the reason recorded: the parity tests must see `cowshed_cli::args`. `clap` and `toml` are out of the `.node`.                                                                                                                                                                                                                                       |
| `cowshed-napi-workspace-manifests` F3 — napi cdylib links `cowshed-cli`, hence gateway/arrow/hyper/ring | HIGH   | `ef05910a1`                                                                                                                                              | The `cowshed-cli` half is closed. **The `gateway/hyper/ring` half is not this finding's to close** — it survives through `cowshed-core`'s own `cowshed-gateway` dependency and is now owned entirely by `cowshed-core-gateway-inventory-sessions` F4 above, where it is measured at 26 crates. `arrow` and `ring` survive either way via `cowshed-core`'s direct `arrow-*` and `rcgen`/`x509-parser`. |
| `lmao-arrow` F8 — arrow default features pull chrono/chrono-tz                                          | MEDIUM | `1d28d7221 feat(lmao-rs): converge Arrow IPC on version 56`                                                                                              | `packages/lmao-rs/Cargo.toml` has all four subcrates at `{ version = "56", default-features = false }`. `chrono-tz` is gone from the lockfile; `chrono` remains because `arrow-array` 56.2.1 lists it unconditionally.                                                                                                                                                                                |
| `xcut-arrow-triplication` F5 — lmao Arrow 55 default features pull `chrono-tz` and the `arrow` umbrella | MEDIUM | `1d28d7221` (+ `a48f78a48` for the umbrella)                                                                                                             | Both halves closed. All three workspaces are now on Arrow 56.2.1 with `default-features = false`, which also closes the divergence premise of `xcut` F1.                                                                                                                                                                                                                                              |
| `lmao-timestamp-proof` F3 — separate crate + NAPI + unused `proptest` do not earn the compile unit      | HIGH   | `1d28d7221` — "delete the divergent timestamp proof crate"                                                                                               | Resolved past what the finding proposed. It asked for a fold into a `lmao-core` module; the crate was **deleted outright** — no directory, not in `members`, and zero `timestamp_proof` references anywhere in `packages/lmao-rs`. The optional `napi`/`napi-build` deps and the unused `proptest` went with it.                                                                                      |
| `lmao-core` F10 — `tokio` default features for one example                                              | LOW    | `185ade7d8 feat(lmao-rs): generate tracing ABI bindings from TypeScript`                                                                                 | `tokio = { version = "1", default-features = false, features = ["rt-multi-thread", "macros", "time", "sync"] }`. Worth stating plainly since the finding did not: this is a `[dev-dependency]` for `examples/jcode_tracer.rs`, so it never shipped in the first place — the win is example compile time, not artifact weight.                                                                         |

---

## Build ledger

Every command run in `dep-grok` (`/Users/danny/Dev/.cowshed/smoothbricks/codebase/dep-grok`), all reverted. Regime
throughout: warm target directory, sibling `cargo` contention.

| #   | finding                                                               | command                                                                                                  | result                                                                                                                             |
| --- | --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| 0   | baseline `packages/cowshed`                                           | `RUSTC_WRAPPER= cargo build --workspace --all-targets`                                                   | exit 0, 28.12s                                                                                                                     |
| 0   | baseline `packages/lmao-rs`                                           | `RUSTC_WRAPPER= cargo build --workspace --all-targets`                                                   | exit 0, 15.57s                                                                                                                     |
| 0   | baseline `packages/columine`                                          | `RUSTC_WRAPPER= cargo build --workspace --all-targets`                                                   | exit 0, 11.37s                                                                                                                     |
| 1   | gw-policy F7 — delete `rustls-pki-types`                              | `RUSTC_WRAPPER= cargo check -p cowshed-gateway --all-targets`                                            | **exit 0**, 2.41s; `Cargo.lock` −1 line, package entry retained                                                                    |
| 2   | xcut F6 — drop `"x509-parser"` from `[dependencies]` rcgen            | `RUSTC_WRAPPER= cargo check -p cowshed-gateway --all-targets`                                            | **FAIL** `E0599` on `Issuer::from_ca_cert_pem`, `tls.rs:28:30`                                                                     |
| 3   | xcut F6 rider — delete `[dev-dependencies]` rcgen line                | `RUSTC_WRAPPER= cargo check -p cowshed-gateway --all-targets`                                            | **exit 0**, 0.76s, no unit recompiled, `Cargo.lock` unchanged                                                                      |
| 4   | gw-cache F11 — `uuid`                                                 | grep of `cowshed-gateway/src`                                                                            | 6 production sites in 3 files; no cut attempted                                                                                    |
| 5   | xcut F4 — `rustls-platform-verifier`                                  | `cargo tree --target {aarch64-apple-darwin,x86_64-unknown-linux-gnu} --edges normal`                     | `jni` 0 rows both targets; no cut attempted                                                                                        |
| 6   | napi F8 / xcut F8 — duplicates                                        | `cargo tree -i getrandom@{0.2.17,0.3.4,0.4.3}` + per-target trees                                        | three independent parents; 11 of 14 duplicates 0 rows                                                                              |
| 7   | cs-core-gw-inv F4 — protocol split sizing                             | `cargo tree -p {cowshed-core,cowshed-gateway,cowshed-napi} --target aarch64-apple-darwin --edges normal` | 154 → 126 units; 26 crates enumerated                                                                                              |
| 8   | columine F9 — wasm graph                                              | `cargo tree --target wasm32-unknown-unknown -p columine-ep-wasm --edges normal`                          | exit 0; `wasm-bindgen`/`iana-time-zone` absent, `chrono` present via `arrow-array`                                                 |
| 9   | xcut F7 — delete `anyhow` / `rustc-hash` / `criterion` workspace keys | `RUSTC_WRAPPER= cargo check --workspace --all-targets`                                                   | **exit 0** in `packages/cowshed` 13.91s and `packages/columine` 1.51s; `Cargo.lock` unchanged in both                              |
| 10  | columine-arrow F4 rider — delete unused `proptest` dev-dep            | `RUSTC_WRAPPER= cargo check -p columine-arrow --all-targets`                                             | **exit 0**, 0.48s; `Cargo.lock` −1 line from `columine-arrow`'s dependency list, package entry retained (other crates use the key) |
| 11  | columine F5 — delete `wasm-perf` / `wasm-s` profiles                  | `RUSTC_WRAPPER= cargo check --workspace --all-targets`                                                   | **exit 0**, 0.27s, no unit recompiled, `Cargo.lock` unchanged                                                                      |
| 12  | cs-core-gw-inv F4 — back-edge enumeration                             | `grep -n "crate::" config.rs policy.rs control.rs`                                                       | `policy.rs` clean; `config.rs` one `pub(crate)` cache bridge + `validate_repo_id`; `control.rs` is client + daemon control plane   |
| 13  | columine F9 / columine-arrow F4 — shipped wasm graphs                 | `cargo tree --target wasm32-unknown-unknown -p {columine-ep-wasm,columine-wasm} --edges normal`          | `columine-wasm` is `columine-types` + `columine-vm` only — no chrono, no getrandom, no wasm-bindgen                                |

## Nothing here is landed

No manifest is modified in this branch. Every experiment above was applied and reverted in a separate workspace. The
three CUT-SAFE edits and the F4 restructure are proposals awaiting review.
