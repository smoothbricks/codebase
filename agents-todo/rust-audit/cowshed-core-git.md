# cowshed-core/git.rs

Scope: `packages/cowshed/crates/cowshed-core/src/git.rs` (3246 lines: production 1–1948, tests 1949–3246). Doctrine:
`BYPRODUCT-ENGINEERING.md`; `PERFORMANCE-HANDBOOK` §4.1 (profile trap / regime) and chapters `04-mechanisms.md`,
`05-memory-toolkit.md`. Targeted greps across `packages/cowshed/crates` for git2 leftover, `git_command_at` /
`GIT_TERMINAL_PROMPT`, `MAIN_REMOTE` / `cowshed-main`, porcelain/`-z`, `GitOid`, and `.cowshed/env`. Neighbouring reads
(not audited): `cowshed-core/Cargo.toml`, `api/dto.rs` `GitOid`, `workspace_environment.rs` path constant,
`runtime/project.rs` spawn/error helpers, `cowshed-cli/src/probe.rs` git wrapper, `cowshed-gateway/src/repo_mirror.rs`
isolated git helper.

## Summary

- `git2` is gone from this crate and from `packages/cowshed/Cargo.lock`. Every git _command_ in this file is a
  PATH-`git` subprocess through `git_command_at` → `run_git_at_with_objects`. Direct FS is used only where git’s verbs
  are the wrong tool (exclude file, envrc, worktree admin dir).
- Live SSOT split: `remotes()` parses URLs as UTF-8 `String`; `remote_url()` preserves arbitrary bytes as `PathBuf`.
  Production binding reconciliation calls `remotes()`. The non-UTF-8 test never exercises `remotes()`, so the production
  path cannot go red.
- `is_git_repository` maps `try_exists` I/O failure to “not a repository”, which `owns_remote` treats as reclaimable.
  That is a write on a path whose state was not determined.
- Oids are untyped `String` here and re-validated as `GitOid` at every caller. `ensure_git_success` labels every
  non-zero git exit `conflict`. Branch minting is copied between `prepare_workspace` and `adopt_as_linked_worktree`.
- Allocations and N-subprocess chatter are once-per-user-operation, dominated by `git` spawn. Per §4.1 those are notes,
  not findings, except where two functions restate the same argv.

## Findings

### F1 — HIGH — SSOT — `remotes()` UTF-8 `String` vs `remote_url()` `PathBuf`; production listing cannot round-trip the byte contract this file tests

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:14-17`, `417-441`, `1767-1772`, `1911-1929`, `2692-2722`

```rust
pub struct RemoteUrl {
    pub name: String,
    pub url: String,
}
// ...
for url in parse_lines(&output.stdout, "remote URL")? {
    remotes.push(RemoteUrl { name: name.clone(), url });
}
```

```rust
async fn remote_url(&self, name: &str) -> Result<Option<PathBuf>> {
    // ...
    return Ok(Some(parse_one_path(&output.stdout, "remote url")?));
}
fn parse_lines(bytes: &[u8], description: &str) -> Result<Vec<String>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| CowshedError::internal(format!("{description} is not valid UTF-8")))?;
```

```rust
fn parse_one_path(bytes: &[u8], description: &str) -> Result<PathBuf> {
    let value = parse_one_line(bytes, description)?;
    Ok(PathBuf::from(OsString::from_vec(value.to_vec())))
}
```

`preserves_non_utf8_main_remote_argument` (2692–2722) writes a `/tmp/cowshed-main-\xff` remote via `prepare_workspace` /
`remote_url` and asserts `git remote get-url` bytes. It never calls `remotes()`.

Problem: one concept (a git remote URL, which on Unix is an `OsString`) has two parsers that already disagree.
`parse_lines` demands UTF-8; `parse_one_path` keeps the bytes. Production binding load uses `git.remotes()`
(`runtime/project.rs:1729`, also 2130, 6655, 7351). A mount path this file explicitly allows will fail `remotes()` with
`internal` (“remote URL is not valid UTF-8”) after `remote_url` / `configure_main_remote` succeeded. `remotes()` is also
N+1 (`git remote` plus `git remote get-url --all` per name) while `classified_merge_drivers` in the same file already
shows the one-shot `-z` config parse.

Fix: delete `RemoteUrl.url: String`. Make `url` a `PathBuf` (or `OsString`). Replace `remotes()` with one
`git config --local -z --get-regexp ^remote\..*\.url$` and the same record split `classified_merge_drivers` uses
(`split('\0')` / first newline). Parse values with `OsString::from_vec`, names as UTF-8 if git’s remote-name grammar is
the constraint — do not UTF-8 the URL. Point `preserves_non_utf8_main_remote_argument` at `remotes()` so a UTF-8-only
listing goes red.

Cost/Risk: every `RemoteUrl` consumer (binding reconciliation in `runtime/project.rs`) must compare `Path`/`OsStr`, not
`str`. That is the cutover; no shim.

### F2 — HIGH — STRUCTURE — `is_git_repository` swallows `try_exists` errors as “not a repo”; `owns_remote` will reclaim

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:1325-1332`, `1831-1838`

```rust
async fn owns_remote(&self, name: &str, url: &Path, main_mount: &Path) -> Result<bool> {
    if let Some(recorded) = self.recorded_remote_owner(name).await? {
        return Ok(recorded == url);
    }
    if url == main_mount {
        return Ok(true);
    }
    Ok(url.is_absolute() && !is_git_repository(url).await?)
}

async fn is_git_repository(path: &Path) -> Result<bool> {
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Ok(false);
    }
    Ok(run_git_at(path, ["rev-parse", "--git-dir"])
        .await?
        .status
        .success())
}
```

Problem: `tokio::fs::try_exists` returns `Err` on query failure (permission denied on a parent, interrupted I/O).
`unwrap_or(false)` turns that into “does not exist”. For a legacy remote with no `remote.<name>.cowshed` record,
`owns_remote` then returns `true` for any absolute path, and `configure_main_remote` retargets it. The comment on
`is_git_repository` says a missing path should fail the same way as “not a git dir”; an undetermined path is neither.
Spawn failure of `git` is correctly `Err` via `git_spawn_error`; filesystem query failure is not. Operational failure
must be `Err` (repo rule); this is the `/dev/null` swallow.

Fix: match `try_exists`: `Ok(false)` → `Ok(false)`; `Err(e)` → `CowshedError::integrity` / environment error naming
`path`; `Ok(true)` → `rev-parse --git-dir`. Do not reclaim unless the negative is conclusive (absent path, or
`rev-parse` non-zero). Add a test that injects a permission-denied parent (or a mock) and asserts
`configure_main_remote` returns `Err` rather than `Canonical`.

Cost/Risk: only `owns_remote` / `inspect_cowshed_upstream` (the latter already treats undetermined network URLs as
`repository: false` without writing). Fail-closed on I/O is the intended doctor behaviour.

### F3 — MEDIUM — SSOT — git.rs oids are `String`; the crate’s oid type is `GitOid` (40 or 64 lowercase hex)

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:444-446`, `695-701`, `759-761`;
`packages/cowshed/crates/cowshed-core/src/api/dto.rs:114-129`; wrap sites `runtime/project.rs:2929`, `landing.rs:69-70`

```rust
pub async fn head_oid(&self) -> Result<String> {
    self.read_one(["rev-parse", "HEAD"], "read HEAD").await
}
async fn resolve_commit(&self, revision: &str) -> Result<Option<String>> { /* rev-parse --verify --quiet */ }
pub async fn branch_tip(&self, branch: &str) -> Result<Option<String>> {
    self.resolve_commit(&format!("refs/heads/{branch}")).await
}
```

```rust
pub struct GitOid(String);
impl GitOid {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if matches!(value.len(), 40 | 64)
            && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
```

Callers immediately `GitOid::new(git.head_oid().await?)`. `discovers_repository_and_reads_head` asserts
`head_oid().len() == 40` (git.rs:2229), which is SHA-1-only and weaker than `GitOid` (40 or 64).

Problem: the validated oid type lives in `dto.rs`; this module is the producer and emits untyped strings. Validation is
re-derived at every boundary (Byproduct L0 / handbook §7.7: validate once at the parse edge).
`commits_changing_something` builds a `BTreeSet<String>` of oids the same way. `runtime/project.rs` `git_revision_oid`
(8042–8047) reimplements `rev-parse --verify` instead of calling `GitRepository::head_oid` / `resolve_commit`, because
those return `String`.

Fix: `head_oid` / `resolve_commit` / `branch_tip` / cherry and rev-list oid sets return `GitOid`. Parse with
`GitOid::new` inside `parse_one_string`’s oid path (or a `parse_oid`). Delete the `GitOid::new` wraps at the
project/landing boundary. Change the discover test to `GitOid::new(oid).is_ok()` (accepts 40 and 64). `git_revision_oid`
in `project.rs` becomes a `GitRepository` call — that deletion is the other slice’s cutover.

Cost/Risk: every `&str` oid argument in this file (`has_commit`, `commit_is_ancestor`, `bundle_commits` expected tip, …)
should take `&GitOid` or `impl AsRef<str>` consistently. `dto.rs` is the SSOT for the type; git.rs is the SSOT for
producing it from git.

### F4 — MEDIUM — DUPLICATION — workspace branch mint is written twice (`prepare_workspace` and `adopt_as_linked_worktree`)

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:1224-1259`, `1589-1604`, `1687-1697`

```rust
let branch = format!("cowshed/{name}");
let branch_ref = format!("refs/heads/{branch}");
let exists = self.run(["show-ref", "--verify", "--quiet", branch_ref.as_str()]).await?;
if exists.status.success() { return Err(CowshedError::conflict(...)); }
if exists.status.code() != Some(1) { return Err(git_internal("check workspace branch", &exists)); }
// ...
let mut args = vec![OsString::from("switch"), OsString::from("-c"), OsString::from(branch)];
if let Some(start) = start { args.push(OsString::from("--")); args.push(OsString::from(start)); }
```

The same `show-ref` / exit-1 / `switch -c` block is repeated against `main` in `adopt_as_linked_worktree` (1589–1604,
1687–1697), with a different conflict sentence.

Problem: two implementations of “create `cowshed/<name>` if absent”. A third change to start-point argv (`--` vs not,
`-c` vs `switch --create`) will land in one mint path only.

Fix: one private `fn workspace_branch(name: &str) -> (String, String)` and one
`async fn create_session_branch(&self, name: &str, start: Option<&str>) -> Result<()>` that owns show-ref + switch. Both
mint methods call it. Conflict message takes the repo that holds the ref (`cloned workspace` vs `main's repository`) as
an argument.

Cost/Risk: contained to this file. Tests already cover both mint paths.

### F5 — MEDIUM — STRUCTURE — `ensure_git_success` maps every non-zero git exit to `ErrorCode::conflict`

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:1879-1887`; contrast (not owned)
`runtime/project.rs:8118-8152`

```rust
fn ensure_git_success(operation: &str, output: Output) -> Result<()> {
    if output.status.success() {
        Ok(())
    } else {
        Err(CowshedError::conflict(
            git_message(operation, &output),
            "resolve the git conflict and retry the cowshed command",
        ))
    }
}
```

Used for `git switch`, `git remote add/remove`, `git worktree add/repair`, `git reset`, `git bundle create`,
`git config`. A missing start revision, a worktree path git refuses, or a bundle git will not write all become
`conflict` with “resolve the git conflict”.

Problem: operational failure is a value, but the _wrong_ value. `git_internal` exists for “this command should have
succeeded”; `ensure_git_success` exists for “git said no”. Collapsing the second onto `conflict` makes doctor/CLI
classification lie, and it has already diverged from `require_git_success` in `project.rs`, which at least inspects
stderr for CONFLICT / fast-forward / overwrite. Two classifiers, neither is this file’s single source, and they already
disagree on hint text (`git_spawn_error` says install CLT; `project.rs:7940-7941` says `restore /usr/bin/git`).

Fix: keep `ensure_git_success` only for verbs whose non-zero _is_ a user-resolvable repo state (`switch`, `reset` when
dirty). Remote/worktree/bundle/config failures should use `git_internal` or a dedicated
`git_failed(operation, output, hint)` that does not hard-code `conflict`. Do not copy `require_git_success`’s stderr
substring table into this file — lift one classifier (owned here, since `git_message` already lives here) and delete the
project.rs copy.

Cost/Risk: error `code` changes for some failure paths; CLI tests that assert `conflict` on those verbs must follow.
Cross-slice: `CsCoreProject` owns `require_git_success`.

### F6 — LOW — DUPLICATION — `is_dirty` and `dirty_file_count` spawn the same porcelain command

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:461-468`, `982-988`

```rust
pub async fn is_dirty(&self) -> Result<bool> {
    let output = self
        .run(["status", "--porcelain=v1", "-z", "--untracked-files=normal"])
        .await?;
    // ...
    Ok(!output.stdout.is_empty())
}
pub async fn dirty_file_count(&self) -> Result<u64> {
    let output = self
        .run(["status", "--porcelain=v1", "-z", "--untracked-files=normal"])
        .await?;
```

Problem: argv is restated, not centralized. `is_dirty` is emptiness; `dirty_file_count` walks NUL records and skips the
extra rename/copy field. Both are correct porcelain. Callers differ (`is_dirty` → removal fence in `project.rs:2933`;
`dirty_file_count` → `landing.rs:93`), so this is not a double-spawn on one path today. Regime: once per list/remove,
spawn-dominated — not a copies finding.

Fix: one `async fn porcelain_status(&self) -> Result<Output>` (or `Vec<u8>` stdout). `is_dirty` is `!bytes.is_empty()`;
`dirty_file_count` keeps the record walk.

Cost/Risk: none.

### F7 — LOW — SSOT — workspace env hook embeds `.cowshed/env`, which `workspace_environment.rs` already names

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:99-103`;
`packages/cowshed/crates/cowshed-core/src/workspace_environment.rs:7`

```rust
const WORKSPACE_ENVIRONMENT_SOURCE: &[u8] = b"source_env_if_present .cowshed/env";
const LOCAL_WORKSPACE_ENVIRONMENT_SOURCE: &[u8] =
    b"source_env_if_exists \"${local_override%/*}/.cowshed/env\"";
```

```rust
pub const WORKSPACE_ENVIRONMENT_PATH: &str = ".cowshed/env";
```

Problem: the file the hook sources is the environment blob `workspace_environment.rs` publishes. If that path moves,
git.rs will keep writing a stale source line. Tests hard-code the same bytes (2037, 2060, 2121) — that is the right
substitution-test shape for the hook _text_, but the production constant should still be derived from
`WORKSPACE_ENVIRONMENT_PATH`.

Fix: build the source line from `crate::workspace_environment::WORKSPACE_ENVIRONMENT_PATH` (once, as a `OnceLock<[u8]>`
or a `const` concat if the path stays a string literal). Leave the test literals hardcoded so they still go red on a
path change.

Cost/Risk: a dependency from `git` to `workspace_environment`. If that crate-cycle is wrong, move the path constant to a
tiny shared module both already use. Do not duplicate the string a third time.

### F8 — LOW — DUPLICATION — `commit_is_preserved` and `commit_is_remote_preserved` are one function with a ref prefix

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:711-756`

```rust
pub async fn commit_is_preserved(&self, commit: &str) -> Result<bool> {
    if !self.has_commit(commit).await? { return Ok(false); }
    let output = self.run(["for-each-ref", "--format=%(refname)", "--contains", commit, "refs/heads", "refs/cowshed"]).await?;
    // ...
    Ok(!output.stdout.is_empty())
}
pub async fn commit_is_remote_preserved(&self, commit: &str) -> Result<bool> {
    if !self.has_commit(commit).await? { return Ok(false); }
    let output = self.run(["for-each-ref", "--format=%(refname)", "--contains", commit, "refs/remotes"]).await?;
```

Problem: near-identical functions differing only in the ref prefixes. A format-string or `--contains` change will land
in one.

Fix: `async fn commit_contained_in(&self, commit: &str, refs: &[&str]) -> Result<bool>`. The two pub methods become
one-liners.

Cost/Risk: none.

## Cross-slice questions

- `runtime/project.rs` `invoke_git` (7938–7942) rebuilds a tokio command from `git_command_at` but maps spawn failure
  with a different hint (`restore /usr/bin/git`) than `git_spawn_error`
  (`install the macOS command line developer tools`). `git_spawn_error`’s comment says that split must not exist.
  `CsCoreProject` owns that file.
- `runtime/project.rs:6198-6207` `Command::new("git").arg("clone").arg("--mirror")` bypasses `git_command_at` entirely
  (no `-C`, no `GIT_TERMINAL_PROMPT=0`). Same slice.
- `runtime/project.rs` `git_revision_oid` / `git_optional_ref_oid` (8042–8064) reimplement `rev-parse --verify` instead
  of `GitRepository::{head_oid,resolve_commit}`. Blocked on F3.
- `runtime/project.rs` `require_git_success` (8118–8152) is a second git-exit classifier; see F5.
- `cowshed-cli/src/probe.rs:213-232` restates `git_message` (stderr trim / status fallback) with a third wording
  (`failed to run git {args} in {root}`). Uses `git_command_at` + `git_spawn_error` correctly for spawn.
- `cowshed-gateway/src/repo_mirror.rs:579-664` has its own `git_command` (`env_clear`, `GIT_CONFIG_*` /dev/null,
  `protocol.file/ext.allow=never`). That is a different security domain (isolated fetch, leftover from libgit2
  lockdown), not a copy of `git_command_at`. Do not unify. CsGwMirror owns it.
- `cowshed-core/tests/checkout_relocation.rs` asserts the string `"cowshed-main"` instead of `FALLBACK_MAIN_REMOTE`.
- `api/dto.rs` `GitOid` is the oid SSOT (F3). Do not invent a second oid type in git.rs.

## Non-findings (checked, clean)

- **git2 follow-through (this crate):** `cowshed-core/Cargo.toml` has no `git2`. `packages/cowshed/Cargo.lock` has no
  `git2` / `libgit2` package. Production git in this file all goes through `git_command_at` (1844–1847) →
  `Command::from` → `output()`. `GIT_TERMINAL_PROMPT=0` is set once. Alternate object stores are one env var, refused if
  the path contains `:`.
- **Direct FS is load-bearing, not a missed subprocess:** `.git/info/exclude` and `.envrc` use `O_NOFOLLOW` (`libc`);
  `git worktree remove` / `prune` are documented as the wrong unregistration. `unregister_linked_worktree` deletes the
  admin dir git itself names via `--git-path`.
- **Porcelain / `-z`:** `is_dirty` / `dirty_file_count` use `--porcelain=v1 -z` and skip the extra rename/copy field.
  `classified_merge_drivers` uses `config -z` because driver values contain spaces. `cherry` / `rev-list` / `remote`
  line-split is the format those commands emit (no interior NULs).
- **Exit codes:** production has no `unwrap` / `expect` / `panic!` / `unsafe`. git exit 1 is `Ok(None)` / `Ok(false)`
  where git defines it (symbolic-ref, show-ref, merge-base --is-ancestor, config --get, check-ignore, ls-files
  --error-unmatch, rev-parse --verify --quiet). Other non-zero is `Err`. Spawn failure is `git_spawn_error`. Exception
  is F2’s `try_exists`.
- **DEP-BLOAT (this file’s uses):** `tokio` process is the point of dropping git2 (in-process, typed `Output`, no
  shell). `libc` for `O_NOFOLLOW` cannot be a CLI. `uuid` for `.bundle-verify-*` scratch names is already a crate
  dependency (`fsio.rs` uses the same `Uuid::new_v4().simple()` temp-name pattern); recommending `uuidgen(1)` here would
  add a subprocess to a path that already runs git, and would not remove the crate. Do not drop it from git.rs in
  isolation.
- **COPIES / regime (§4.1):** `parse_one_string`/`parse_one_path` `to_vec`, `name.clone()` in `remotes()`, `format!`
  config keys, `in_progress_operation`’s five `rev-parse --git-path` spawns, `has_commit` then `merge-base` — all once
  per user operation, dwarfed by `execve(git)`. Not findings. `CowshedUpstream.remote_name: String` copies a `'static`
  that `MainRemote` already is; cosmetic.
- **STRUCTURE:** 3246 lines, ~1300 of them tests. Under the 5k–10k god-file bar. `verify_bundle` (~125) and
  `adopt_as_linked_worktree` (~123) are long because the proof/mint is sequential, not because they mixed unrelated
  jobs. No `TODO`/`FIXME`. `cfg(unix)` is implicit via `std::os::unix` (cowshed is macOS).
- **TESTS:** in-module tests hit mint, displacement, legacy `host` remote, dead-path reclaim, linked-worktree pointers,
  non-UTF-8 _write_ path, bundle self-containment, envrc symlink escape, exclude symlink, in-progress MERGE_HEAD. They
  assert typed `MainRemote` / `CowshedUpstream` / `ErrorCode` as well as bytes where bytes are the contract (envrc,
  exclude, pointer file). `expect` is test-only. Gap: F1’s listing path and F2’s I/O-error path are untested
  (substitution: those tests stay green if the bugs remain).
- **repo_mirror identity/argv:** isolated helper is intentionally not `git_command_at`. No oid/ignore-rule table is
  copied between the two files. Ignore rules in git.rs are the two exclude patterns `.cowshed/` and `.fseventsd/` plus
  `check-ignore` for `.envrc-local`; gateway mirror does not restate them.
