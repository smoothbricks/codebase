# cowshed-cli/sccache+probe+skill

Scope: owned files (all read in full) — `packages/cowshed/crates/cowshed-cli/src/sccache_client_config.rs` (794),
`packages/cowshed/crates/cowshed-cli/src/sccache_client_config/native.rs` (9),
`packages/cowshed/crates/cowshed-cli/src/sccache_client_config/native/linux.rs` (13),
`packages/cowshed/crates/cowshed-cli/src/sccache_client_config/native/macos.rs` (10),
`packages/cowshed/crates/cowshed-cli/src/sccache_service.rs` (680), `packages/cowshed/crates/cowshed-cli/src/probe.rs`
(394), `packages/cowshed/crates/cowshed-cli/src/skill.rs` (666),
`packages/cowshed/crates/cowshed-cli/src/skill/generated.rs` (436). Also read for SSOT/dep questions (not owned):
`packages/cowshed/crates/cowshed-cli/Cargo.toml` (28), `packages/cowshed/Cargo.toml` (32; no toml rationale here),
`packages/cowshed/scripts/refresh-harnesses.ts` (197), `packages/cowshed/Cargo.lock` toml stanza, targeted lines in
`launchd.rs`, `args.rs`, `sandbox.rs`, `api/dto.rs`.

## Summary

- No CRITICAL/HIGH. This slice is CLI/setup/doctor: once-per-command, not a hot loop (PERFORMANCE-HANDBOOK §4.1 regime).
- `toml` `serde` feature is unused; `parse` alone matches the code. The crate itself is load-bearing for never-clobber.
- Never-clobber _keys_ match the crate-manifest rationale; append still `trim_end()`s the user's prefix, so "never
  rewriting those bytes at all" is false.
- `skill/generated.rs` is derived from one source (upstream `src/agents.ts` via `scripts/refresh-harnesses.ts`). Humans
  edit the generator and `VERIFIED_HARNESSES`, not the output.
- Copies/`format!`/`to_owned` exist but sit on setup/doctor/skill-install, not a measured hot path — not findings.
- No `unsafe`. Production `expect` is the non-empty config-dir invariant. Operational failures go through `Result`.

## Findings

### F1 — MEDIUM — DEP-BLOAT — `toml` `serde` feature is unused

Evidence: `packages/cowshed/crates/cowshed-cli/Cargo.toml:24-28`

```
# sccache's own config file is TOML, and `setup` has to read one it did not write without
# reordering or dropping a key its author set. Parsing is what makes the never-clobber decision
# honest; preservation comes from never rewriting those bytes at all — cowshed's block is appended
# below them, and only that block is ever rewritten.
toml = { version = "1", default-features = false, features = ["parse", "serde"] }
```

and the only uses, all parse/`Value` (no `toml::from_*` / `toml::to_*` / `Deserialize` on a toml type),
`packages/cowshed/crates/cowshed-cli/src/sccache_client_config.rs:297-301`, `:376`, `:407-422`, `:431-435`:

```
let document = match text.parse::<toml::Table>() {
    Ok(document) => document,
    Err(error) => {
        return ConfigPlan::Refuse(ConfigConflict::Unparsable {
            reason: error.message().to_owned(),
```

`packages/cowshed/Cargo.lock:2033-2044` shows the `serde` feature's extra edges (`serde_core`, `serde_spanned`,
`toml_datetime`) on this `toml 1.1.4` node. Workspace-wide grep of `packages/cowshed` found no `toml::from_` /
`toml::to_` / `toml::serde`.

Problem: `default-features=false` is right (`display` is off; comments are hand-rendered in `render_block`). `parse` is
the feature the code uses. `serde` is not. The never-clobber decision is a `toml::Table` walk, not a serde DTO.

Fix: drop `serde` from the feature list: `toml = { version = "1", default-features = false, features = ["parse"] }`.
Leave the crate. Do not replace it with a hand parser or a `toml` CLI — the never-clobber cases (inline tables, dotted
keys, unmergeable `cache = { … }`) are exactly why an in-process parser with typed errors is load-bearing, and no
platform `toml` tool is guaranteed or machine-stable.

Cost/Risk: Cargo.lock refresh only. `serde`/`serde_json` stay for sccache `--show-stats` JSON in `sccache_service.rs`.
[INFERENCE] without `serde`, `toml`'s `serde_spanned` / `toml_datetime` serde edges drop; `toml_parser`+`winnow` stay
with `parse`.

### F2 — LOW — SSOT — append path `trim_end()` rewrites user bytes the rationale says are never rewritten

Evidence: rationale `packages/cowshed/crates/cowshed-cli/Cargo.toml:24-27` ("preservation comes from never rewriting
those bytes at all — cowshed's block is appended below them") vs
`packages/cowshed/crates/cowshed-cli/src/sccache_client_config.rs:321-335`:

```
        DiskDirectory::TableAbsent => {
            // A blank line between their last setting and cowshed's marker, so the boundary
            // between the two authors is visible at a glance.
            let contents = format!("{}\n\n{block}", text.trim_end());
            // The one shape check that matters, made by construction rather than by enumeration:
            // if the merged file does not parse into the directory that was wanted, it is not
            // written. An inline `cache` table lands here.
            if !directs_to(&contents, store) {
                return ConfigPlan::Refuse(ConfigConflict::UnmergeableShape);
            }
            ConfigPlan::Write {
                change: ConfigChange::Appended,
                contents,
            }
```

The test at `:556-560` only proves `starts_with(existing)` for an `existing` that already ends in a single `\n`; extra
trailing newlines in a real file are collapsed.

Problem: keys are not reordered or dropped (parse-then-append, refuse foreign `cache.disk`). That half of the rationale
matches. `trim_end()` still mutates the author's prefix (trailing whitespace/newlines) before the block is glued on, so
the "never rewriting those bytes" sentence is false.

Fix: concatenate the prefix verbatim. If it does not already end in `\n`, push one, then `\n` + `block`. Keep
`directs_to` as the merge oracle. Update the Cargo.toml sentence to "keys and byte-for-byte prefix; only a missing final
newline is inserted."

Cost/Risk: one `plan` branch + the append test. Refresh of a cowshed-owned tail (`:308-319`) already preserves the
prefix as `&text[..marker]` and is the pattern to copy.

## Cross-slice questions

- `sccache_service.rs:208-215` sums allocated bytes of **both** `ImageFormat::Asif` and `ImageFormat::Sparse` main
  images per project. `StorageLayout::main_image` is path construction, not exclusivity. If a project can have both
  files on disk (migration leftover), derived cap double-counts. Owner: cowshed-core metadata/storage/apfs triad.
- `probe.rs:47` mints finding code `"git-identity"`. `runtime.rs:2974-2975` restates the same string when the probe
  errors. If doctor findings should share one code constant, that lives in the runtime slice.
- Never-clobber rationale was asked against `packages/cowshed/Cargo.toml`; it actually lives on the cli crate manifest
  (`crates/cowshed-cli/Cargo.toml:24-28`). Workspace `Cargo.toml` has no toml comment.

## Non-findings (checked, clean)

- **generated.rs SSOT.** `scripts/refresh-harnesses.ts` fetches `vercel-labs/skills` `src/agents.ts` at one revision and
  writes only `crates/cowshed-cli/src/skill/generated.rs`. Header is `DO NOT EDIT` (`generated.rs:3-7`). Humans edit the
  generator and `VERIFIED_HARNESSES` (`skill.rs:97-107`); the omp override is absent from the snapshot and the test at
  `skill.rs:572-575` fails if upstream adds it. Not a dual-edit.
- **`toml` crate (as opposed to its `serde` feature).** In-process parse with error typing is required for never-clobber
  across TOML shapes (`inline_and_dotted_spellings_are_read_as_the_same_setting`,
  `an_inline_cache_table_is_refused_rather_than_corrupted`). Foreign `cache.disk.dir` is refused, not overwritten
  (`:339-344`, `:571-589`). `display` correctly off: comments must survive, so `render_block` is hand-written
  (`:442-467`). One `toml` version in `Cargo.lock` (1.1.4). No `directories` crate — macOS/XDG paths are ~10 lines each;
  that is the `git2` test applied and passed.
- **sccache stats JSON adapter** (`sccache_service.rs:287-323`) is a wire-format parse into
  `cowshed_core::api::SccacheStats`, not a restated DTO. `serde_json` is already a workspace dep; sccache's
  `--stats-format json` is the machine-parseable interface. Load-bearing. Do not shell-scrape the text table.
- **tokio `UnixStream` + `Command`** in start/status/stats: in-process, needs the live socket and the same resolved
  binary launchd has no `PATH` for. Load-bearing.
- **COPIES regime.** `to_owned`/`clone`/`format!`/`Vec` rebuilds of the harness table are setup/doctor/skill-install.
  Not a hot loop. `basic_string` allocates once per render. `disk_directory` clones the `dir` string once per `plan`.
  Leave them.
- **STRUCTURE.** No god file. No function over ~100 lines. No `unsafe`. `client_config_path` `expect` (`:191`) is the
  invariant that every platform returns ≥1 config dir (linux 1, macos 2). `ProbeRepo::Drop` swallowing `remove_dir_all`
  is Drop, not a `Result` path; the probe test asserts the throwaway repo is gone. `linux.rs` under
  `cfg(not(target_os = "macos"))` is the XDG arm; the crate is unix (`OpenOptionsExt`) so Windows is not a silent wrong
  path.
- **PRIVATE_CONFIG_MODE `0o600`.** Same numeric mode as `launchd::PRIVATE_PLIST_MODE`, but that constant is a plist
  mode; sharing it would couple this module to launchd for a POSIX file mode. Not an SSOT break.
- **TESTS.** sccache config tests assert rendered TOML _and_ re-parse/`directs_to` — the file is sccache's contract.
  Skill tests fold frontmatter to `(key, value)` instead of line layout (`skill.rs:368-371`). Probe tests assert typed
  `config_file` / `include_if_condition` plus the user-facing message. `GENERATED_HARNESSES.len() >= 40` is a
  generator-liveness smoke, not a path oracle; path contracts are the four representative harnesses. Substitution-weak
  description-length check (`skill.rs:413-416`) is not load-bearing enough to file.
- **`SKILL.md`.** One file, `include_str!` (`skill.rs:25`). Not copied.
- **`args.rs` already rejects unknown `--harness` names** before `skill::dispatch`'s `filter_map`; that `filter_map` is
  not a silent user-facing drop.
