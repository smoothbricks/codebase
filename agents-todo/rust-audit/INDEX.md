# Rust audit — index

Read-only audit of every Rust crate in the monorepo: 19 crates across 3 cargo workspaces (`packages/cowshed`,
`packages/columine`, `packages/lmao-rs`), 133k lines of Rust. 45 slices, one report each. Rubric:
`_fork/minigraf/BYPRODUCT-ENGINEERING.md` + `PERFORMANCE-HANDBOOK.md`. Every finding carries `path:line` evidence.

509 findings: **10 CRITICAL / 154 HIGH / 234 MEDIUM / 111 LOW**.

| Axis                                            | CRIT | HIGH | MED | LOW |
| ----------------------------------------------- | ---- | ---- | --- | --- |
| SSOT (one concept, two sources of truth)        | 5    | 63   | 70  | 18  |
| COPIES (allocation / memcpy / evaporating work) | 2    | 40   | 38  | 26  |
| STRUCTURE                                       | 2    | 27   | 55  | 31  |
| DUPLICATION (two implementations)               | 1    | 14   | 39  | 16  |
| TESTS (cannot go red / asserts on strings)      | 0    | 3    | 19  | 14  |
| DEP-BLOAT                                       | 0    | 7    | 12  | 6   |

SSOT dominates at 156 findings — the audit's premise is confirmed: the recurring defect in this codebase is a concept
restated rather than owned.

## The 10 CRITICAL findings

| #   | Report                                             | Finding                                                                                                                                                                      |
| --- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `cowshed-core-secrets-credentials-sandbox-exec.md` | Workspace token escapes `Zeroizing` via `format!`/`shell_word` into a plain `String`                                                                                         |
| 2   | `cowshed-core-secrets-credentials-sandbox-exec.md` | `read_bounded_utf8` drops unzeroized secret bytes on UTF-8 failure (`mem::take` + `FromUtf8Error`)                                                                           |
| 3   | `cowshed-core-storage-bootstrap.md`                | Privileged runner fabricates `Exit(0)` — failed `security add-generic-password` / `install` / `launchctl` all report success; encrypt-in-place can run with no keychain item |
| 4   | `cowshed-napi-workspace-manifests.md`              | `types.ts` `JobInfo.argv: string[]` vs napi's `CommandArg {encoding,data}` — typia rejects every real job                                                                    |
| 5   | `xcut-rust-vs-typescript-duplication.md`           | Same defect, found independently: `parseJobInfo` rejects the wire shape                                                                                                      |
| 6   | `xcut-rust-vs-typescript-duplication.md`           | TS `Opcode` is a stale subset of the Rust VM (missing `0x14/0x24/0x25/0x32/0x48`, all nested); tests already emit `0x48` as a raw byte                                       |
| 7   | `xcut-rust-vs-typescript-duplication.md`           | TS `ErrorCode` omits `ColumnUnderrun=8`; `wasm-backend.vmErrorCode` throws on a legal VM status                                                                              |
| 8   | `columine-vm-state-growth-undo.md`                 | `Nested` `slot_data_size` is 0, so `grow_state` silently drops Nested data on any slot growth                                                                                |
| 9   | `columine-vm-maps-intern-aggregates.md`            | HASHMAP `Last`/`First` TTL upserts force `new_cmp=0` → `insert_with_ttl` records `0.0`; single upsert/remove never touch the eviction index                                  |
| 10  | `columine-parsing-msgpack.md`                      | `skip_value` map32 `n*2` wraps in `u32` (release): `n>=2^31` makes skip succeed, parsing the map body as parent keys                                                         |

Findings 4 and 5 are the same bug reached from two directions (crate-side and seam-side). Treat that as corroboration,
not noise — per PH §L9 the convergent finds are the ones the spec already implied, so the _single-source_ findings below
carry more information.

## Cross-cutting verdicts

- **Arrow is pinned at two versions** — 56 in cowshed/columine, 55 in lmao-rs (dragged by datafusion 47).
  `lmao_arrow::trace_schema` and the gateway's `event_batch` share the `01f` names but already disagree (Int64 vs
  Timestamp, dict vs Utf8, UInt32 vs UInt64 span ids): a live interop split. `StreamWriter try_new/write/finish` is
  copied at five sites. → `xcut-arrow-triplication.md`
- **lmao-query is 509 lines carrying three query backends**, including bundled SQLite (all of SQLite compiled from C)
  and datafusion 47 (a 224-package lockfile closure, feature `nested_expressions` unused). Neither is reached:
  `DEFAULT_TRACE_DB_PATH` goes through TypeScript `bun:sqlite`. → `lmao-query.md`, `xcut-dependency-bloat-sweep.md`
- **`tokio = features=["full"]`** at the cowshed workspace root while every runtime built is `current_thread`;
  `rt-multi-thread` is unreferenced. **`syn = features=["full"]`** in lmao-macros exists only to parse a `syn::Expr`
  that is never inspected.
- **`git2` removal followed through** — git is PATH-only now. The remaining CLI-substitution candidates were judged
  individually; most are KEEP with reasons (see the sweep). The real bloat is feature over-provisioning and the two dead
  query backends, not the crates.
- **The Rust↔TypeScript seam is the worst SSOT offender**: opcode numbers, entry types 1–24, column names, DTO shapes,
  CLI flags and on-disk format versions are all restated by hand, and four pairs have already diverged. →
  `xcut-rust-vs-typescript-duplication.md`
- **Copies**: 106 findings. Hot-path standouts — every JSON field costs two heap allocs before columns see bytes; every
  lmao span pays three `vec![0; capacity]`; every arena alloc zeroes its block with a per-byte `write_u8` loop (576
  stores for `SpanSystem/64`); every gateway cache _hit_ re-SHA-256s the sealed body (Byproduct L7 violation). →
  `xcut-copies-sweep-cowshed.md`, `xcut-copies-sweep-columine-lmao-rs.md`

## Reports

### cowshed (5 crates, 103k lines Rust — 22 slices)

| Report                                             | F   | CRIT | HIGH |
| -------------------------------------------------- | --- | ---- | ---- |
| `cowshed-core-runtime-project.md`                  | 13  | 0    | 4    |
| `cowshed-core-runtime-supervisor.md`               | 13  | 0    | 3    |
| `cowshed-core-storage-bootstrap.md`                | 11  | 1    | 3    |
| `cowshed-core-apfs-triad.md`                       | 17  | 0    | 4    |
| `cowshed-core-storage-job_artifact.md`             | 12  | 0    | 3    |
| `cowshed-core-git.md`                              | 8   | 0    | 2    |
| `cowshed-core-api.md`                              | 11  | 0    | 3    |
| `cowshed-core-gateway-inventory-sessions.md`       | 10  | 0    | 3    |
| `cowshed-core-metadata-repository-checkout.md`     | 10  | 0    | 3    |
| `cowshed-core-storage-lifecycle-recovery-audit.md` | 12  | 0    | 4    |
| `cowshed-core-copy-fsio-process-misc.md`           | 5   | 0    | 1    |
| `cowshed-core-secrets-credentials-sandbox-exec.md` | 10  | 2    | 1    |
| `cowshed-cli-runtime.md`                           | 9   | 0    | 3    |
| `cowshed-cli-args-help-output.md`                  | 10  | 0    | 2    |
| `cowshed-cli-services.md`                          | 8   | 0    | 1    |
| `cowshed-cli-sccache-probe-skill.md`               | 2   | 0    | 0    |
| `cowshed-gateway-proxy.md`                         | 15  | 0    | 2    |
| `cowshed-gateway-actor-control.md`                 | 12  | 0    | 4    |
| `cowshed-gateway-mirror-repo_mirror.md`            | 10  | 0    | 2    |
| `cowshed-gateway-cache-telemetry.md`               | 12  | 0    | 4    |
| `cowshed-gateway-policy-config-platform-tls.md`    | 14  | 0    | 4    |
| `cowshed-napi-workspace-manifests.md`              | 15  | 1    | 2    |

### columine (7 crates, 24.5k lines — 10 slices)

| Report                                  | F   | CRIT | HIGH |
| --------------------------------------- | --- | ---- | ---- |
| `columine-vm-vm.md`                     | 15  | 0    | 5    |
| `columine-vm-state-growth-undo.md`      | 13  | 1    | 3    |
| `columine-vm-minroar-bitmaps.md`        | 10  | 0    | 4    |
| `columine-vm-maps-intern-aggregates.md` | 12  | 1    | 3    |
| `columine-parsing-json.md`              | 10  | 0    | 5    |
| `columine-parsing-msgpack.md`           | 7   | 1    | 3    |
| `columine-arrow.md`                     | 10  | 0    | 3    |
| `columine-types.md`                     | 9   | 0    | 3    |
| `columine-event-processor.md`           | 9   | 0    | 2    |
| `columine-wasm-exports.md`              | 9   | 0    | 4    |

### lmao-rs (7 crates, 5.7k lines — 7 slices)

| Report                    | F   | CRIT | HIGH |
| ------------------------- | --- | ---- | ---- |
| `lmao-core.md`            | 10  | 0    | 3    |
| `lmao-arena.md`           | 12  | 0    | 4    |
| `lmao-arrow.md`           | 10  | 0    | 5    |
| `lmao-macros.md`          | 10  | 0    | 4    |
| `lmao-query.md`           | 10  | 0    | 4    |
| `lmao-timestamp-proof.md` | 6   | 0    | 3    |
| `lmao-wasm.md`            | 8   | 0    | 1    |

### cross-cutting (6 slices)

| Report                                   | F   | CRIT | HIGH |
| ---------------------------------------- | --- | ---- | ---- |
| `xcut-rust-vs-typescript-duplication.md` | 15  | 3    | 4    |
| `xcut-arrow-triplication.md`             | 9   | 0    | 3    |
| `xcut-dependency-bloat-sweep.md`         | 9   | 0    | 3    |
| `xcut-copies-sweep-cowshed.md`           | 26  | 0    | 10   |
| `xcut-copies-sweep-columine-lmao-rs.md`  | 30  | 0    | 14   |
| `xcut-intra-cowshed-duplication.md`      | 12  | 0    | 3    |

## Findings that were checked and rejected

Recorded so the negative results are not re-derived (PH §7.14d — a search returning zero is a positive claim):

- **The cowshed apfs "triad" is not a duplicate.** `src/apfs.rs` = disk-image primitives, `storage/apfs.rs` = lifecycle
  substrate, `storage/apfs/native.rs` = host adapter. Layered, not copied. Neither module dies.
- **`columine-arrow` is not a clone of `lmao-arrow`.** RecordBatch IPC is hand-rolled here; `arrow-ipc` is used for
  Schema decode only. The crate cannot be dropped without writing a schema decoder first.
- **`bootstrap/native/macos.rs` at 6517 lines vs `linux.rs` at 95 is not an unfinished port** — Linux is fail-closed by
  design; the bulk is tests plus unix helpers misfiled in the macOS adapter.
- **Keep `clap` in cowshed-cli** — it is the load-bearing matcher and its features are already trimmed; hand-rolled help
  is deliberate (stdout purity). The fix is to generate the clap builder from `CommandSpec`, not to drop either.
- **`skill/generated.rs` SSOT is clean** — `refresh-harnesses.ts` writes it from one upstream revision; humans edit the
  generator.
- **`Git2RepoTransport` no longer uses libgit2** despite the name — PATH git plus an HTTP preflight. Rename, do not
  unify with core's `git_command_at`.

## Method notes

- Read-only: `git status` confirms zero source files modified.
- No `cargo`/`nx` was run — a 45-agent build storm on shared target dirs would have deadlocked the machine, and
  dependency facts are all readable from manifests plus `Cargo.lock`.
- Consequently every claim is static. Nothing here is a measured performance verdict: per PH §4.1/§L8, the COPIES
  findings name a mechanism and a regime, and each still owes a cell before its fix ships.
