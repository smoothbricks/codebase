# XCUT dependency bloat sweep

Scope: doctrine (`BYPRODUCT-ENGINEERING.md`, `docs/handbook/02-measurement.md` §4.1, `04-mechanisms.md`,
`05-memory-toolkit.md`); workspace manifests `packages/cowshed/Cargo.toml` (31), `packages/columine/Cargo.toml` (62),
`packages/lmao-rs/Cargo.toml` (50); crate manifests cowshed-core (39), cowshed-cli (28), cowshed-gateway (45),
cowshed-napi (20), cowshed-escape-tests (7), columine-arrow (17), columine-ep-wasm (21), columine-event-processor (19),
columine-parsing (16), columine-types (10), columine-vm (19), columine-wasm (15), lmao-arena (19), lmao-arrow (23),
lmao-core (26), lmao-macros (19), lmao-query (25), lmao-timestamp-proof (26), lmao-wasm (13); lockfiles
`packages/cowshed/Cargo.lock` (2629, 272 entries / 256 names), `packages/columine/Cargo.lock` (921, 102 / 99),
`packages/lmao-rs/Cargo.lock` (3051, 289 / 280). Targeted greps of `packages/cowshed`, `packages/columine`,
`packages/lmao-rs` for candidate-crate use. No `cargo`/`nx`. Versions and closures are from the lockfiles as text.

Regime for this slice (PERFORMANCE-HANDBOOK §4.1): compile/link/lockfile weight, not a hot probe. An extra crate on a
once-per-boot path is a note; a workspace-wide `tokio/full` or a 224-node DataFusion graph that pins Arrow across
workspaces is a finding.

## Summary

- Arrow is 56.2.1 in cowshed+columine and 55.2.0 in lmao-rs; DataFusion 47 is the pin and the lockfile is 224 packages
  in that crate's closure.
- `tokio = { features = ["full"] }` is workspace-wide in cowshed; every production binary uses `current_thread`, never
  `rt-multi-thread`.
- `rustls-platform-verifier` pulls `jni` + `rustls-platform-verifier-android` + `openssl-probe` into the cowshed
  lockfile for a macOS-first gateway.
- `cowshed-napi` (cdylib) depends on `cowshed-cli`, so the `.node` links clap/toml and the full CLI.
- Unused workspace keys: cowshed `anyhow`; columine `rustc-hash` and `criterion`.
- Intra-lockfile duplicates: `getrandom` 0.2+0.3+0.4 in all three locks; `hashbrown` three majors in lmao-rs.
- `syn features=["full"]` is KEEP (`syn::Expr`). CLI-substitution candidates are KEEP except as noted in F-list.
- `roaring` is a columine-vm _dev_-dep only; it must not re-enter `columine-wasm`.
- `datafusion`/`rusqlite` are optional on `lmao-query` (`default = []`); they still occupy the lockfile and pin
  Arrow 55.
- No CRITICAL (no live correctness/security hole from a dep itself).

## Findings

### F1 — HIGH — SSOT — Arrow 55 vs 56 across the three lockfiles

Evidence: `packages/lmao-rs/Cargo.toml:23-27`

```
# Arrow subcrates only (not the `arrow` umbrella) to keep compile times down.
arrow-array = "55"
arrow-buffer = "55"
arrow-schema = "55"
arrow-ipc = "55"
```

`packages/columine/Cargo.toml:20-22`

```
arrow-array = { version = "56", default-features = false }
arrow-ipc = { version = "56", default-features = false }
arrow-schema = { version = "56", default-features = false }
```

`packages/cowshed/crates/cowshed-core/Cargo.toml:14-17`

```
arrow-array = "56"
arrow-buffer = "56"
arrow-ipc = "56"
arrow-schema = "56"
```

Lockfile resolved: cowshed+columine `arrow-array`/`arrow-buffer`/`arrow-data`/`arrow-ipc`/`arrow-schema`/`arrow-select`
= 56.2.1; lmao-rs the same crates = 55.2.0, plus the `arrow` umbrella 55.2.0 pulled by DataFusion.

Problem: one on-disk Arrow format, two majors. lmao-rs cannot move to 56 while `datafusion = "47"` remains a member
dependency (optional or not — cargo locks it). IPC written by columine 56 is not guaranteed to be what lmao-arrow 55
reads. This is already divergence, not just duplication.

Fix: pick 56 as the monorepo SSOT (cowshed and columine already did). Drop the `datafusion` feature of `lmao-query` or
replace DataFusion 47 with a release that takes Arrow 56; then retarget `packages/lmao-rs/Cargo.toml`
workspace.dependencies to `"56"`. Do not introduce a compatibility shim.

Cost/Risk: `lmao-query` DataFusion backend and its tests move; Arrow 55 leaves the lmao lockfile. columine/cowshed
unchanged.

Cross-lockfile version splits (every crate present in ≥2 lockfiles at disagreeing versions). Patch-level noise listed so
it is not re-discovered:

| crate                                                   | cowshed                            | columine   | lmao-rs                        |
| ------------------------------------------------------- | ---------------------------------- | ---------- | ------------------------------ |
| **arrow-array / buffer / data / ipc / schema / select** | **56.2.1**                         | **56.2.1** | **55.2.0**                     |
| clap / clap_builder                                     | 4.6.6                              | —          | 4.6.1 / 4.6.0                  |
| toml / toml_parser                                      | 1.1.4 / 1.1.3                      | —          | 1.1.2 / 1.1.2                  |
| uuid                                                    | 1.23.5                             | —          | 1.23.4                         |
| winnow                                                  | 1.0.4                              | —          | 1.0.3                          |
| zmij                                                    | 1.0.22                             | —          | 1.0.21                         |
| bit-vec                                                 | 0.8.0 **and** 0.9.1                | 0.8.0      | 0.8.0                          |
| hashbrown                                               | 0.16.1 **and** 0.17.1              | 0.16.1     | **0.14.5, 0.15.5, 0.17.1**     |
| getrandom                                               | 0.2.17 **and** 0.3.4 **and** 0.4.3 | same three | same three                     |
| r-efi                                                   | 5.3.0 and 6.0.0                    | same       | same                           |
| rand / rand_chacha / rand_core                          | 0.9.5 / 0.9.0 / 0.9.5              | same       | **also** 0.8.7 / 0.3.1 / 0.6.4 |
| windows-sys                                             | 0.52.0, 0.60.2, 0.61.2             | 0.61.2     | 0.61.2                         |

Intra-lockfile duplicates not in that table: cowshed `windows-targets` 0.52.6+0.53.5 and eight `windows_*` triples;
lmao-rs `itertools` 0.10.5+0.14.0.

### F2 — HIGH — DEP-BLOAT — DataFusion 47 is a 224-node graph behind an optional feature

Evidence: `packages/lmao-rs/crates/lmao-query/Cargo.toml:12-21`

```
rusqlite = { version = "0.37", features = ["bundled"], optional = true }
datafusion = { version = "47", default-features = false, features = ["nested_expressions"], optional = true }
tokio = { version = "1", features = ["rt"], optional = true }
[features]
default = []
sqlite = ["dep:rusqlite"]
datafusion = ["dep:datafusion", "dep:tokio"]
```

`packages/lmao-rs/Cargo.lock:584-611` — `datafusion` 47.0.0 lists 41 direct deps including `datafusion-datasource-csv`,
`datafusion-datasource-json`, `datafusion-functions-nested`, and the rest of the 27 `datafusion-*` packages. Closure
from the lockfile graph: **224** packages. lmao-rs without `datafusion`+`rusqlite`: 161 names; 119 names exist only on
that path.

`packages/lmao-rs/crates/lmao-query/src/datafusion_backend.rs:37-41` SQL is `SELECT * FROM spans WHERE …` and a
`NOT EXISTS` subquery — no array/list nested-expression functions.

Problem: `default-features = false` does not make DataFusion small. `nested_expressions` is not referenced by the SQL
this crate emits. Optional still pins the workspace to Arrow 55 (F1) and inflates `Cargo.lock` by ~119 crates.
`rusqlite` `bundled` compiles SQLite via `cc` (`libsqlite3-sys` 0.35.0 → `cc`, `pkg-config`, `vcpkg`) even though the
feature is off by default.

Fix: delete the `datafusion` feature and `datafusion_backend.rs` unless a caller outside this crate enables it in a
shipped artifact (cross-slice). If SQL-over-Arrow stays, it has to earn Arrow 56 and a much smaller crate — not
DataFusion 47 with `nested_expressions`. Keep `sqlite` optional; KEEP `bundled` if the feature stays (in-process typed
SQL, `sqlite3` CLI is not a library). Drop `nested_expressions` immediately if DataFusion is retained.

Cost/Risk: exploratory SQL surface goes away; `ArrowTraceQuery` remains. Lockfile shrinks by ~119 names; Arrow 56
becomes reachable.

### F3 — HIGH — DEP-BLOAT — cowshed `tokio` features=`["full"]` at the workspace root

Evidence: `packages/cowshed/Cargo.toml:24`

```
tokio = { version = "1", features = ["full"] }
```

`packages/cowshed/crates/cowshed-cli/src/main.rs:3`

```
#[tokio::main(flavor = "current_thread")]
```

`packages/cowshed/crates/cowshed-napi/src/lib.rs:927-929`

```
let runtime = napi::tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
```

Grep of `packages/cowshed` for `flavor = "multi_thread"`, `rt-multi-thread`, `Runtime::new`,
`Builder::new_multi_thread`: **no matches**.

Lockfile contrast: cowshed `tokio` 1.52.3 depends on
`bytes, libc, mio, parking_lot, pin-project-lite, signal-hook-registry, socket2, tokio-macros, windows-sys 0.61.2` (9).
lmao-rs `tokio` 1.52.3 with `features = ["rt"]` depends on `bytes, pin-project-lite, tokio-macros` (3).

Features actually referenced in cowshed source: `macros` (`tokio::main`, `tokio::test`, `select!`, `pin!`, `join!`),
`rt` (`spawn`, `spawn_blocking`, `JoinSet`, `JoinHandle`), `time`, `signal`, `io-util`, `io-std` (`tokio::io::stdin`),
`net` (`UnixStream`), `fs`, `process`, `sync`. `test-util` is only a gateway _dev_-dep (`cowshed-gateway/Cargo.toml:45`)
for `tokio::time::advance`.

Problem: `full` compiles `rt-multi-thread` and `parking_lot` into every cowshed crate that takes
`tokio.workspace = true` (core, cli, gateway, and via them napi). Precedent in this repo is the same as `git2`: pay only
for what the process uses.

Fix: replace the workspace key with
`tokio = { version = "1", default-features = false, features = ["macros", "rt", "time", "signal", "io-util", "io-std", "net", "fs", "process", "sync"] }`.
Keep gateway's dev-dep `features = ["test-util"]`. Do not add `rt-multi-thread`.

Cost/Risk: if a future runtime needs worker threads, it must add the feature explicitly. napi `tokio_rt` still works on
`current_thread`.

### F4 — MEDIUM — DEP-BLOAT — `rustls-platform-verifier` drags JNI/Android/openssl-probe into cowshed

Evidence: `packages/cowshed/crates/cowshed-gateway/Cargo.toml:23-24`

```
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
rustls-platform-verifier = "0.7"
```

`packages/cowshed/crates/cowshed-gateway/src/interfaces.rs:5-6`

```
use rustls::{ClientConfig, pki_types::ServerName};
use rustls_platform_verifier::ConfigVerifierExt;
```

Lockfile: `rustls-platform-verifier` 0.7.0 depends on `jni`, `rustls-platform-verifier-android`, `rustls-native-certs`
(→ `openssl-probe`, `schannel`, `security-framework`). `jni` 0.22.4 is in `packages/cowshed/Cargo.lock` solely because
of this crate.

Problem: the gateway is a macOS/Linux userspace proxy. Android JNI and `openssl-probe` are not load-bearing on that
target; they are the verifier crate's kitchen-sink target graph. `security-framework` is already a direct macOS dep of
the same crate for keychain (KEEP — F-list below).

Fix: keep in-process TLS verification (do not shell out to `security`/`openssl`). Replace `rustls-platform-verifier`
with a macos `security-framework` + linux `rustls-native-certs` (or webpki roots) verifier written against the rustls
`ClientConfig` builder already in `interfaces.rs`. Delete `jni` from the lockfile as a consequence.

Cost/Risk: must re-prove platform root trust on macOS and Linux. `ConfigVerifierExt` call site is one file.

### F5 — MEDIUM — DEP-BLOAT — `cowshed-napi` links the entire CLI into the cdylib

Evidence: `packages/cowshed/crates/cowshed-napi/Cargo.toml:10-11`

```
[dependencies]
cowshed-cli = { path = "../cowshed-cli" }
```

`packages/cowshed/crates/cowshed-napi/src/lib.rs:924-932`

```
#[napi(js_name = "runCli")]
pub async fn run_cli(argv: Vec<String>) -> napi::Result<i32> {
    napi::tokio::task::spawn_blocking(move || {
        let runtime = napi::tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok::<i32, io::Error>(runtime.block_on(cowshed_cli::run::run(
```

`cowshed-cli` direct deps (lockfile):
`async-trait, base64, bytes, clap, cowshed-core, cowshed-gateway, libc, serde, serde_json, sha2, tokio, toml`.

Problem: the shipped `.node` is not a thin core binding. `runCli` pulls clap (even with default-features off), toml,
sccache config parsing, and the CLI dispatch tree into the addon. napi already depends on `cowshed-core` for the typed
surface.

Fix: if `runCli` is required, it stays — but then the addon _is_ the CLI and the extra core-shaped napi API is the
duplicate surface (SSOT with the `cowshed` binary). If `runCli` is convenience, move it to a separate cdylib or delete
it and have JS spawn `cowshed`. Decision I would take: delete `runCli` from this addon; JS already can exec the binary;
keep napi on `cowshed-core` only.

Cost/Risk: JS callers of `runCli` must switch to the binary. clap/toml leave the `.node`.

### F6 — MEDIUM — DEP-BLOAT — gateway `rcgen` enables `x509-parser` that `tls.rs` does not call

Evidence: `packages/cowshed/crates/cowshed-gateway/Cargo.toml:22`

```
rcgen = { version = "0.14", default-features = false, features = ["crypto", "pem", "ring", "x509-parser", "zeroize"] }
```

`packages/cowshed/crates/cowshed-core/Cargo.toml:26,35`

```
rcgen = { version = "0.14", default-features = false, features = ["crypto", "pem", "ring", "zeroize"] }
x509-parser = { version = "0.18", features = ["verify"] }
```

Gateway `tls.rs` imports: `rcgen::{CertificateParams, Issuer, KeyPair}` only. Core `workspace_credentials.rs` is the
crate that parses PEM/DER (`x509_parser::pem::parse_x509_pem`, `verify_signature`). Gateway _dev_-dep restates rcgen
**without** `x509-parser` (`cowshed-gateway/Cargo.toml:44`) — feature unification with the `[dependencies]` line makes
that slimming a no-op.

Problem: `x509-parser` 0.18.1 closure is 56 packages (`asn1-rs`, `nom`, `der-parser`, `oid-registry`, `ring`, …). Core
needs `verify` (KEEP). Gateway's rcgen feature does not.

Fix: drop `"x509-parser"` from gateway's rcgen features; delete the redundant slimmer dev-dep rcgen line (unification
already ignores it). Leave core's `x509-parser` + `verify`.

Cost/Risk: none if tls.rs never used rcgen's parser APIs (read: it does not).

### F7 — LOW — DEP-BLOAT — unused workspace.dependency keys

Evidence: `packages/cowshed/Cargo.toml:18`

```
anyhow = "1"
```

Grep of `packages/cowshed` for `use anyhow` / `anyhow::`: **no matches**. `anyhow` is absent from
`packages/cowshed/Cargo.lock` package names.

`packages/columine/Cargo.toml:15,19`

```
rustc-hash = "2"
criterion = { version = "0.5", default-features = false }
```

Grep of `packages/columine` for `rustc-hash`/`rustc_hash` and `criterion`: **only those two workspace lines**. No member
crate references them. They do not appear as columine lockfile packages.

Problem: dead keys. They do not inflate the lockfile today; they will the moment a crate writes
`anyhow.workspace = true` by habit.

Fix: delete the three keys. columine already has `proptest` (used) and `roaring` (used as vm dev-dep).

Cost/Risk: none.

### F8 — LOW — DEP-BLOAT — `getrandom` three times in every lockfile

Evidence: `packages/cowshed/Cargo.lock` packages `getrandom` 0.2.17 (ring), 0.3.4 (cowshed-core direct,
`getrandom::fill` in `workspace_credentials.rs:121` and `bootstrap/native/macos.rs:912`), 0.4.3 (uuid). Same three
versions in columine and lmao-rs lockfiles.

Problem: three copies of one crate in each workspace. Not a runtime hot-path issue (§4.1). It is the same class as
hashbrown 0.14/0.15/0.17 in lmao-rs (DataFusion/Arrow). Cannot be collapsed from our manifests without waiting for
ring/uuid to share a getrandom major.

Fix: no local fix that is honest. Do not add a fourth. When uuid/ring move, re-lock. Not worth a compatibility crate.

Cost/Risk: none if left; a forced unify would be a lie about those crates' MSRV graphs.

## Cross-slice questions

- `lmao-query` / LmaoQuery: does any shipped npm/napi artifact enable `--features datafusion` or `sqlite`? If no, F2 is
  a delete. If yes, name the artifact.
- `cowshed-napi` / CsNapiWorkspace: is `runCli` part of the published contract? F5's delete depends on that.
- `columine-vm` / ColVmCore: roaring must stay `[dev-dependencies]`. If any slice moves it to `[dependencies]`,
  `reducer_vm.wasm` grows by the comment's 35K.
- `cowshed-gateway` / CsGwProxy: F4 (verifier) and F6 (rcgen feature) are that crate's manifests; this slice only read
  the manifests + the two call sites.

## Non-findings (checked, clean)

### Direct dependency ledger

Weight class = lockfile closure size of that crate's name (cowshed graph unless noted): trivial ≤5, small ≤20, medium
≤50, heavy ≤100, massive >100. Versions are the resolved lockfile version. Path deps omitted from weight.

| dep                                                                 | locked ver                      | crates that declare it                                                                                | weight                                         | notes                                                                                      |
| ------------------------------------------------------------------- | ------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------ |
| serde                                                               | 1.0.228                         | cowshed-core, cli, gateway, napi                                                                      | small                                          | `derive` KEEP                                                                              |
| serde_json                                                          | 1.0.150                         | same                                                                                                  | small                                          | KEEP                                                                                       |
| tokio                                                               | 1.52.3                          | cowshed-core, cli, gateway; lmao-query opt `rt`; lmao-core **dev** `rt-multi-thread,macros,time,sync` | medium (cowshed full=34) / trivial (lmao rt=3) | F3                                                                                         |
| clap                                                                | 4.6.6                           | cowshed-cli                                                                                           | trivial (4)                                    | already `default-features=false` + `std,error-context,usage`                               |
| napi / napi-derive / napi-build                                     | 2.16.17 / 2.16.13 / 2.3.2       | cowshed-napi; lmao-timestamp-proof **optional**                                                       | medium (49)                                    | KEEP (FFI)                                                                                 |
| anyhow                                                              | (undeclared in lock)            | cowshed workspace only                                                                                | —                                              | F7 unused                                                                                  |
| async-trait                                                         | 0.1.89                          | cowshed-cli, core, gateway                                                                            | small                                          | KEEP: `dyn` async traits                                                                   |
| base64                                                              | 0.22.1                          | cowshed-cli, core, gateway                                                                            | trivial                                        | KEEP                                                                                       |
| bytes                                                               | 1.12.1                          | cowshed-cli, core, gateway                                                                            | trivial                                        | KEEP                                                                                       |
| libc                                                                | 0.2.186                         | cowshed-cli, core, gateway, napi                                                                      | trivial                                        | KEEP                                                                                       |
| sha2                                                                | 0.10.9                          | cowshed-cli, core, gateway                                                                            | small (10)                                     | KEEP                                                                                       |
| toml                                                                | 1.1.4                           | cowshed-cli                                                                                           | small                                          | KEEP; features `parse,serde` already trimmed                                               |
| arrow-array/buffer/ipc/schema                                       | 56.2.1                          | cowshed-core, gateway; columine-arrow (+ ipc/schema); lmao-arrow/query at **55.2.0**                  | heavy (63–69)                                  | F1; columine `default-features=false` (chrono/num still in lock — unconditional in 56.2.1) |
| getrandom                                                           | 0.3.4 (direct)                  | cowshed-core                                                                                          | small                                          | KEEP; F8 for 0.2/0.4 companions                                                            |
| plist                                                               | 1.10.0                          | cowshed-core                                                                                          | small (20)                                     | KEEP                                                                                       |
| notify                                                              | 8.2.0                           | cowshed-core                                                                                          | medium (26)                                    | KEEP                                                                                       |
| walkdir                                                             | 2.5.0                           | cowshed-core                                                                                          | trivial                                        | KEEP                                                                                       |
| rcgen                                                               | 0.14.8                          | cowshed-core, gateway                                                                                 | heavy (64)                                     | KEEP crate; F6 feature                                                                     |
| x509-parser                                                         | 0.18.1                          | cowshed-core (`verify`)                                                                               | heavy (56)                                     | KEEP                                                                                       |
| thiserror                                                           | 2.0.18                          | cowshed-core, gateway                                                                                 | trivial                                        | KEEP                                                                                       |
| url                                                                 | 2.5.8                           | cowshed-core, gateway                                                                                 | medium (idna/icu)                              | KEEP                                                                                       |
| uuid                                                                | 1.23.5                          | cowshed-core (`serde,v4`), gateway (`v4`)                                                             | medium (27)                                    | KEEP                                                                                       |
| zeroize                                                             | 1.9.0                           | cowshed-core, gateway                                                                                 | trivial                                        | KEEP                                                                                       |
| http / http-body / http-body-util                                   | 1.4.2 / **=1.0.1** / **=0.1.3** | cowshed-gateway                                                                                       | small                                          | exact pins; not bloat                                                                      |
| hyper                                                               | 1.10.1                          | cowshed-gateway `http1,http2,server,client`                                                           | medium (56)                                    | all four features used (`proxy.rs` http1+http2 client+server)                              |
| hyper-util                                                          | 0.1.20                          | cowshed-gateway `tokio`                                                                               | small                                          | KEEP                                                                                       |
| idna                                                                | 1.1.0                           | cowshed-gateway                                                                                       | medium (29, ICU)                               | KEEP; also transitive via `url`                                                            |
| pin-project-lite                                                    | 0.2.17                          | cowshed-gateway                                                                                       | trivial                                        | KEEP                                                                                       |
| rustls                                                              | 0.23.40                         | cowshed-gateway `ring,std,tls12`                                                                      | medium (29)                                    | KEEP                                                                                       |
| rustls-platform-verifier                                            | 0.7.0                           | cowshed-gateway                                                                                       | medium + jni                                   | F4                                                                                         |
| rustls-pki-types / rustls-pemfile                                   | 1.15.0 / 2.2.0                  | cowshed-gateway                                                                                       | small                                          | KEEP                                                                                       |
| tokio-rustls                                                        | 0.26.4                          | cowshed-gateway `ring,tls12`                                                                          | small                                          | KEEP                                                                                       |
| subtle                                                              | 2.6.1                           | cowshed-gateway                                                                                       | trivial                                        | KEEP                                                                                       |
| time                                                                | 0.3.53                          | cowshed-gateway                                                                                       | small                                          | KEEP (`OffsetDateTime` for certs)                                                          |
| security-framework / -sys                                           | 3.7.0 / 2.17.0                  | cowshed-gateway `cfg(macos)`                                                                          | small                                          | KEEP                                                                                       |
| proptest                                                            | 1.11.0                          | cowshed-core **dev**; columine-arrow/vm/parsing/event-processor **dev**; lmao-* **dev**               | —                                              | does not ship                                                                              |
| roaring                                                             | 0.11.4                          | columine-vm **dev** only (`default-features=false`, `std`)                                            | small                                          | KEEP as oracle; must not ship                                                              |
| rustc-hash                                                          | 2 (lmao lock)                   | lmao-arrow; columine workspace unused                                                                 | small                                          | lmao KEEP; columine F7                                                                     |
| criterion                                                           | 0.5.1                           | lmao-core/arena/arrow **dev**; columine workspace unused                                              | —                                              | `default-features=false`; does not ship                                                    |
| syn                                                                 | 2.0.118                         | lmao-macros `full`                                                                                    | small                                          | KEEP (see features)                                                                        |
| quote / proc-macro2                                                 | 1.0.46 / 1.0.106                | lmao-macros                                                                                           | trivial                                        | KEEP                                                                                       |
| trybuild                                                            | 1                               | lmao-macros **dev**                                                                                   | —                                              | does not ship                                                                              |
| rusqlite                                                            | 0.37.0                          | lmao-query optional `bundled`                                                                         | small unique (~8)                              | KEEP if feature stays                                                                      |
| datafusion                                                          | 47.0.0                          | lmao-query optional `nested_expressions`                                                              | **massive (224)**                              | F2                                                                                         |
| cowshed-escape-tests                                                | 0.1.0                           | empty `[dependencies]`                                                                                | —                                              | no bloat                                                                                   |
| columine-types / lmao-core / lmao-arena / lmao-wasm / columine-wasm | —                               | no third-party prod deps                                                                              | —                                              | clean                                                                                      |

### CLI-substitution verdicts

Precedent: `git2` was removed because openssl came with it and `git` on PATH was enough. Apply that test, both
directions.

| candidate                       | verdict                                           | reason                                                                                                                                                                                                                                                                   |
| ------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| git / git2                      | already done                                      | not in any of the three manifests                                                                                                                                                                                                                                        |
| plist / `plutil`                | **KEEP**                                          | in-process parse of already-captured `diskutil`/`hdiutil` stdout (`apfs.rs`, `bootstrap/native/macos.rs`). Typed `plist::Value` errors. `plutil -convert json` is an extra process + temp file on a setup path; the crate's closure is 20, not openssl-class.            |
| rcgen + x509-parser / `openssl` | **KEEP**                                          | workspace CA mint + PEM verify with typed errors (`workspace_credentials.rs` `verify_signature`, gateway `tls.rs` leaf mint). `openssl` CLI is not guaranteed (LibreSSL on macOS, absent on some hosts) and is not a typed API. ring is the cost and it is load-bearing. |
| security-framework / `security` | **KEEP**                                          | `get_generic_password` + `errSecItemNotFound` (`platform.rs:115-123`). Keychain bytes with typed errors, no-shell, used on the credential path. The CLI is not machine-parseable the same way.                                                                           |
| notify / fsevents               | **KEEP**                                          | in-process `RecommendedWatcher` on devenv paths (`supervisor.rs:14,661`). No `fsevents` CLI; this is a long-lived watcher, not a once-per-boot spawn.                                                                                                                    |
| walkdir                         | **KEEP**                                          | small (same-file + winapi-util). Secret scan and host_config need typed walk errors (`secrets.rs:8,81`). Hand-rolling 30 lines would not delete a heavyweight crate.                                                                                                     |
| uuid / `uuidgen`                | **KEEP**                                          | in-process v4 for temp names, nonces, incarnations. `uuidgen` is a process per id. serde feature is used. Do not replace with `uuidgen`.                                                                                                                                 |
| sha2 / `shasum`                 | **KEEP**                                          | in-process Sha256/Sha512 of buffers (DTO digest, cache, artifacts, TLS fingerprint). Hot-ish relative to a process spawn. `shasum` is not a library.                                                                                                                     |
| base64                          | **KEEP**                                          | serde DTO encode/decode and HTTP Basic. In-process, no CLI.                                                                                                                                                                                                              |
| toml                            | **KEEP**                                          | sccache config the process did not write; parse-only so the never-clobber decision is honest (`cowshed-cli/Cargo.toml:23-28`, `sccache_client_config.rs:297`).                                                                                                           |
| rusqlite bundled / `sqlite3`    | **KEEP** (optional)                               | in-process SQL with typed `rusqlite::Error`. `sqlite3` CLI is not a library and is not dialect-stable. Bundled `cc` compile is the cost; it is gated on `sqlite`.                                                                                                        |
| datafusion                      | **KEEP as a crate class, DELETE as this feature** | in-process SQL over Arrow cannot be a CLI. The _crate_ is the right shape; _this_ 47/`nested_expressions` pin is F2.                                                                                                                                                     |
| roaring                         | **KEEP as dev oracle**                            | production uses `minroar`. roaring is the differential oracle (`columine-vm/Cargo.toml:16-18`). Replacing with a CLI is nonsense.                                                                                                                                        |
| napi                            | **KEEP**                                          | this _is_ the Node ABI. No CLI substitute.                                                                                                                                                                                                                               |

Wrong "just shell out" recommendations avoided: plist, rcgen, security-framework, notify, sha2, rusqlite, napi.

### Feature flags actually referenced

- **tokio `full` (cowshed workspace)** — over-provisioned. Referenced: macros, rt, time, signal, io-util, io-std, net,
  fs, process, sync. Not referenced: rt-multi-thread. test-util only via gateway dev-dep. → F3.
- **syn `full` (lmao-macros)** — KEEP. `syn::Expr` is parsed (`lmao-macros/src/lib.rs:346-363,381`). Without `full`,
  `Expr` is not in the API. `Parse`/`Punctuated`/`Ident`/`LitStr`/`Visibility`/`Token`/`braced`/`bracketed` would not by
  themselves require `full`; `Expr` does.
- **napi `napi6` + `tokio_rt`** — KEEP. `napi::tokio::spawn` / `napi::tokio::task::spawn_blocking`.
- **rcgen gateway `x509-parser`** — over-provisioned → F6. core rcgen features `crypto,pem,ring,zeroize` match
  `tls`/`workspace_credentials` mint.
- **x509-parser `verify`** — KEEP (`verify_signature`).
- **clap** — already trimmed; builder API not `derive`.
- **hyper `http1,http2,server,client`** — all four used.
- **rustls / tokio-rustls `ring,std,tls12`** — KEEP; http/1.1 and h2 over TLS 1.2.
- **uuid core `serde,v4` / gateway `v4`** — both used.
- **toml `parse,serde`** — KEEP; write/display features off.
- **roaring `default-features=false, std`** — KEEP for the oracle.
- **columine arrow `default-features=false`** — already done; chrono/num still lock because arrow-array 56.2.1 lists
  them unconditionally.
- **datafusion `nested_expressions`** — not referenced by emitted SQL → F2.
- **lmao-query `default = []`** — correct; sqlite/datafusion opt-in.
- **lmao-timestamp-proof `napi` feature** — KEEP as opt-in so wasm stays dep-free.
- **cowshed-gateway tokio `test-util`** — KEEP, dev-only, used (`tokio::time::advance`).

### Dev-dependencies vs shipped artifacts

- columine-vm `roaring` / `proptest`: `[dev-dependencies]` only. `columine-wasm` depends on `columine-vm` without those
  features. Lockfile lists roaring on `columine-vm` because cargo records workspace-member dev-deps; they do not link
  into `cdylib` unless moved. Comment at `columine-vm/Cargo.toml:16-18` is the tripwire.
- cowshed-core `proptest`: dev-only. Appears in the lockfile `cowshed-core` dep list; not linked into `cowshed` /
  `.node`.
- lmao-core `tokio` / `criterion` / `lmao-macros`: dev-only (benches/example). Not in `lmao-wasm`.
- lmao-macros `trybuild`: dev-only.
- lmao-query `lmao-arrow` / `lmao-core`: dev-only. `datafusion`/`rusqlite` are _optional prod_ deps, not dev-deps — they
  ship if the feature is on (F2), not by leak.
- cowshed-gateway slimmer dev `rcgen`: does not reduce features (unification) → F6.
- napi-build: `[build-dependencies]` on cowshed-napi and optional on lmao-timestamp-proof. Build-only; does not ship in
  the cdylib.
- No evidence of a `[dev-dependency]` that is also in `[dependencies]` except the gateway rcgen restatement.

### Other axes this slice does not own

COPIES / STRUCTURE / TESTS of crate internals: other agents. This slice grepped only enough to classify a dep as used vs
unused and to name features actually referenced.
