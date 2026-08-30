# cowshed-gateway/mirror+repo_mirror

Scope: `packages/cowshed/crates/cowshed-gateway/src/mirror.rs` (1200),
`packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs` (1962). Doctrine: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md` §4.1. Targeted reads (not owned):
`cowshed-core/src/git.rs` `git_command_at` 1844–1848; `cowshed-core/src/repository.rs` `validate_identity_component`
107–132; `cowshed-gateway/src/config.rs` `validate_repo_id` 221–229; `cowshed-gateway/src/sim_broker.rs`
`validate_repo_id` 857–866; `cowshed-gateway/src/cache.rs` `hex_decode_32`/`hex_nibble`/`unix_ms` 1497–1531;
`cowshed-gateway/src/policy.rs` `normalize_path` 561–603; `cowshed-gateway/tests/mirror_cache.rs` npm rewrite test
1001–1064; `cowshed-gateway/Cargo.toml`.

Two modules, two concepts — not a bad split of one thing:

- `mirror.rs` is an HTTP registry cache (npm/cargo/go): classify path, rewrite metadata, fill `Cache`, follow
  same-origin redirects.
- `repo_mirror.rs` is an isolated Git fetch actor: admit HTTPS remotes, spawn an env-cleared helper, publish a read-only
  bare clone.

Shared vocabulary (`Mirror*`, `MAX_REDIRECTS = 5`) is coincidental. Public types do not overlap
(`MirrorRequest`/`MirrorService` vs `RepoMirrorRequest`/`RepoMirrorHandle`/`RepoTransport`). Do not merge them.

Git: this slice already shells out to PATH `git` (git2 crate is gone). Core's `git_command_at` is a different threat
model (trusted local checkout, no `env_clear`, no protocol lockdown) and must not become the helper's command builder.

## Summary

- HIGH SSOT: `cowshed-integrity` `sha256-` is Cargo hex in `parse_protocol_expectation` and npm SRI-base64 in
  `rewrite_npm_packument`/`parse_sri` — sha256-only npm tarball URLs fail closed as `MissingIntegrity`.
- HIGH SSOT: `validate_repo_id` restates core `validate_identity_component`; sibling gateway copies already admit
  uppercase that this function (and core) refuse.
- MEDIUM STRUCTURE: `Git2RepoTransport` still names libgit2 after the crate was deleted; the implementation is PATH
  git + HTTP preflight.
- MEDIUM STRUCTURE: git-upload-pack preflight maps connect timeout to `FetchTimeout` and response timeout to
  `FetchFailed`.
- MEDIUM STRUCTURE: `fetch_with_git` returns `Result<_, ()>` — git's exit/stderr evaporate into a boolean.
- MEDIUM DUPLICATION: 32-byte hex decode is restated vs `cache.rs`, and the copies disagree on uppercase.
- MEDIUM TESTS: `audit_failure_prevents_fetch_and_non_https_never_reaches_credentials` cannot go red on the https claim
  while `auditor.fail` is set.
- MEDIUM DUPLICATION: `percent_decode` is a second copy of `policy::normalize_path`'s percent-decode loop.
- LOW STRUCTURE: three production `unsafe { libc::geteuid() }` sites have no SAFETY comment.
- LOW DUPLICATION: `unix_ms` and Content-Length parsing are each written twice.
- uuid/sha2/base64 in this slice are load-bearing; do not replace with shell-outs.

## Findings

### F1 — HIGH — SSOT — `cowshed-integrity` `sha256-` is two encodings

Evidence: `packages/cowshed/crates/cowshed-gateway/src/mirror.rs:574-575`, `923-927`, `677-690`

```
        query.append_pair("cowshed-integrity", integrity);
…
    let digest = if let Some(hex) = integrity.strip_prefix("sha256-") {
        ObjectDigest::Sha256(decode_hex_32(hex).ok_or(MirrorError::MissingIntegrity)?)
    } else {
        parse_sri(&integrity).ok_or(MirrorError::MissingIntegrity)?
    };
…
            "sha512" => return decoded.try_into().ok().map(ObjectDigest::Sha512),
            "sha256" => sha256 = decoded.try_into().ok().map(ObjectDigest::Sha256),
```

Problem: one query parameter, two codecs. Cargo `dl` templates emit `sha256-` + 64 hex chars (`mirror.rs:595`). npm
packument rewrite copies `dist.integrity` verbatim, which `parse_sri` has already accepted as SRI (standard-base64). A
sha256-only SRI string starts with `sha256-`, so `parse_protocol_expectation` never reaches `parse_sri`; `decode_hex_32`
fails (SRI payload is 44 base64 chars, not 64 hex) and the subsequent tarball request returns `MissingIntegrity`.
Fail-closed, not a hole — but the sha256 arm of `parse_sri` is dead on the rewritten URL.
`tests/mirror_cache.rs:1001-1044` only exercises `sha512-`, so the substitution test for this branch cannot go red.

Fix: parse `cowshed-integrity` only through `parse_sri` (SRI tokens) plus a distinct Cargo form that cannot collide —
e.g. require `sha256:` (colon) or a `cowshed-algo=hex` partner for hex, or hex-decode only when `decode_hex_32` succeeds
and otherwise fall through to `parse_sri`. Single decoder, one test that round-trips a sha256-only npm integrity through
rewrite then `MirrorRequest::new`.

Cost/Risk: Cargo download URLs and any stored cache keys that already contain `cowshed-integrity=sha256-<hex>` must keep
working. npm rewritten packuments in cache would start resolving sha256 SRI after the fix.

### F2 — HIGH — SSOT — `validate_repo_id` restated; gateway copies already diverged

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:1300-1321`

```
/// Mirror of `validate_identity_component` in cowshed-core's `repository.rs` — the gateway sits
/// below core and cannot import it, so the rules are restated here and must stay identical: an
/// identity core admits that the gateway refuses (or the reverse) strands a repository between
/// the two layers.
fn validate_repo_id(value: &str) -> Result<(), RepoMirrorError> {
    …
                || !bytes.iter().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte)
                })
```

Core (`repository.rs:121-127`) matches this copy: first char `[a-z0-9]`, rest `[a-z0-9._-]`. Same crate already restates
the rule twice more and those copies **do not agree**:

- `config.rs:210-227` — `is_ascii_alphanumeric()` (uppercase legal), plus a 128-char cap this function lacks.
- `sim_broker.rs:846-865` — same uppercase-legal identifier.

Problem: the comment names the exact failure (core admits / gateway refuses, or reverse). It has already happened inside
the gateway: a session `repo_id` that `config.rs` accepts (`Owner/Repo`) is stored by `bind_session` (no validation,
`repo_mirror.rs:710-722`) and then rejected here as `InvalidRepoId`. Live stranding, not hypothetical.

Fix: one `RepoId` parser in a crate both core and gateway can import (core's `RepoId::parse` is the SSOT). Delete
`validate_repo_id` here, in `config.rs`, and in `sim_broker.rs`. Until that crate exists, copy core's function
byte-for-byte into **one** gateway module and call it from the other two; reject uppercase everywhere.

Cost/Risk: any stored config/session that used uppercase `repo_id` starts failing closed. That is the intended core
rule. `bind_session` must validate too, or junk identities fill `MAX_BINDINGS`.

### F3 — MEDIUM — STRUCTURE — `Git2RepoTransport` names a crate that is gone

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:166-172`, `351-363`, `606-610`

```
pub struct Git2RepoTransport {
    connector: Arc<dyn UpstreamConnector>,
    …
    helper_executable: Option<PathBuf>,
    …
}
…
        run_fetch_helper(executable, plan).await
…
/// Isolated fetch: same lockdown as the old libgit2 helper, via PATH git.
```

Problem: the type and the comment still say git2/libgit2. The body is HTTP preflight through `UpstreamConnector` plus
`Command::new(git)` (`git_command` at 579–603: `env_clear`, `GIT_TERMINAL_PROMPT=0`, `protocol.file.allow=never`,
`protocol.ext.allow=never`, `http.followRedirects=false`). That is the right design (same test that removed `git2`: PATH
git is present; openssl/libgit2-sys is not earned). The name is a lie the next reader will trust.

Core `git.rs:1844-1847` also shells out to `git`, but only sets `GIT_TERMINAL_PROMPT=0` on a trusted checkout. Do not
unify with `git_command` here — untrusted-remote lockdown would vanish, or every local `git status` would inherit helper
isolation it does not need.

Fix: rename `Git2RepoTransport` → `GitRepoTransport` (or `PathGitRepoTransport`) and rewrite the comment to state PATH
git + preflight, not libgit2. Update `actor.rs` construction sites. Leave core's `git_command_at` alone.

Cost/Risk: public name inside the crate (`actor.rs` imports it; `lib.rs` does not re-export it). Rename is mechanical.

### F4 — MEDIUM — STRUCTURE — preflight response timeout is `FetchFailed`, not `FetchTimeout`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:208-218`, `240-275`

```
        let connection = timeout(
            self.connect_timeout,
            self.connector.connect(&AuthorizedTarget { … }),
        )
        .await
        .map_err(|_| RepoMirrorError::FetchTimeout)?
        .map_err(|_| RepoMirrorError::FetchFailed)?;
…
        let response = timeout(self.response_timeout, async move {
            match connection.transport { … }
        })
        .await
        .map_err(|_| RepoMirrorError::FetchFailed)??;
```

Problem: connect deadline → `FetchTimeout`; HTTP response deadline → `FetchFailed`. `classification()` then maps both to
`"repo-fetch-failed"` (`1336-1340`), so audit cannot tell a hung upload-pack from a 404. Operational failure is an `Err`
(good) but the wrong variant (Cantrill: the system cannot tell the truth about itself).

Fix: map the outer `timeout(self.response_timeout, …)` error to `RepoMirrorError::FetchTimeout`. Keep handshake/send
errors as `FetchFailed`.

Cost/Risk: tests that match `FetchFailed` on a slow connector would need to expect `FetchTimeout`. No such test exists
in this file.

### F5 — MEDIUM — STRUCTURE — `fetch_with_git` returns `Result<_, ()>`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:611-643`, `500-510`

```
fn fetch_with_git(mut request: HelperRequest) -> Result<Option<String>, ()> {
    …
    let init = git_command(git, extra_header.as_ref())
        .args(["init", "--bare", "--template="])
        .arg(destination)
        .status()
        .map_err(|_| ())?;
    if !init.success() {
        return Err(());
    }
```

Problem: init/fetch/for-each-ref failures collapse to `()`. The helper then emits `error_code: Some("fetch-failed")`;
the parent maps any `!ok` to `RepoMirrorError::FetchFailed` and deletes the dest. git's status and stderr are discarded
(`stderr(Stdio::null())` on the parent helper spawn at 410). A missing `git`, a denied protocol, and a 401 look
identical.

Fix: thread a small error enum through the helper JSON (`error_code` already exists on `HelperResponse`) with at least
`git-missing` / `git-denied` / `fetch-failed`, and capture a bounded stderr tail into that field. Keep `ok: false` as
the fail-closed wire flag.

Cost/Risk: helper protocol v1. Bump `version` or treat unknown `error_code` as `fetch-failed`. Parent classification
table grows one arm.

### F6 — MEDIUM — DUPLICATION — hex decode restated; uppercase already diverged

Evidence: `packages/cowshed/crates/cowshed-gateway/src/mirror.rs:931-939`, `1082-1088` vs `cache.rs:1497-1524`

```
fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 { return None; }
    …
        decoded[index] = (hex(pair[0]).ok()? << 4) | hex(pair[1]).ok()?;
…
        b'A'..=b'F' => Ok(value - b'A' + 10),
```

`cache.rs` `hex_nibble` has no `A-F` arm — uppercase is `InvalidMetadata`. Cargo `cksum` is lowercase; this copy is the
one `validate_cargo_index` uses (`mirror.rs:621`). A packument/index with uppercase hex is metadata-valid here and
cache-invalid later.

Fix: delete `decode_hex_32`/`hex` from `mirror.rs`; call `cache.rs`'s decoder (make it `pub(crate)`). Pick one case
rule: lowercase-only, matching cache and cargo.

Cost/Risk: `percent_decode` also calls `hex` (`mirror.rs:1070-1071`) and npm paths may contain `%2F`/`%2f` but not `%2F`
vs `%2f` as a case policy for _hex of hashes_. Keep a path-nibble helper that accepts A-F if npm needs it; do not share
it with digest decode.

### F7 — MEDIUM — TESTS — https claim is structurally blind

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:1825-1862`

```
    async fn audit_failure_prevents_fetch_and_non_https_never_reaches_credentials() {
        …
        auditor.fail.store(true, Ordering::SeqCst);
        …
        assert!(matches!(
            handle.mirror(RepoMirrorRequest {
                …
                remote: "http://git.example.test/org/repo.git".to_owned(),
            }).await,
            Err(RepoMirrorError::AuditUnavailable)
        ));
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 0);
```

Problem: two claims, one fixture. `auditor.fail` makes every path return `AuditUnavailable` before `fetch_redirects`
(credentials live there, `1101-1117`). If `canonical_remote` started accepting `http://`, this test still passes:
Allowed-audit fails first, credentials stay 0. §4.3b / §7.10bb — the guard is blind exactly on the https decision.

Fix: split. (1) auditor-fail + admitted **https** URL → `AuditUnavailable`, credentials 0. (2) auditor-ok + `http://` →
`InvalidRemote`, credentials 0. Neither test shares the other's knob.

Cost/Risk: one extra tokio test, same fixtures.

### F8 — MEDIUM — DUPLICATION — `percent_decode` copies `policy::normalize_path`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/mirror.rs:1061-1079` vs `policy.rs:565-585`

Both walk bytes, on `%` take two nibbles, push the decoded byte, then `String::from_utf8`. `normalize_path` additionally
rejects decoded `/`, `\`, NUL, `%`. npm classification cannot use `normalize_path` as-is (`@scope%2fpkg` must decode a
slash — `mirror.rs:1039-1058` is the documented npm exception).

Problem: two implementations of one decode; nibble helpers already disagree on case (F6). A third copy will appear the
next time a protocol needs percent-decoding.

Fix: one `percent_decode` in `policy.rs` (or a tiny bytes module). `normalize_path` = decode + reject `.`/`..` + reject
decoded separators. `classify_npm` = decode without the separator reject. Do not reimplement the loop in `mirror.rs`.

Cost/Risk: npm path tests in `tests/mirror_cache.rs` are the oracle; keep `%2f` in `@scope%2fpkg` green.

### F9 — LOW — STRUCTURE — `unsafe { libc::geteuid() }` has no SAFETY comment

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:473`, `539-540`, `1211`

```
        || metadata.uid() != unsafe { libc::geteuid() }
```

Problem: POSIX `geteuid` is not UB, but the rubric is "unsafe without a stated invariant comment". Three sites, none.
Test-only `unsafe` (`mkfifo`/`open`/`from_raw_fd`/`waitpid` at 1882–1946) is the same omission.

Fix: one `fn euid() -> u32` with `// SAFETY: geteuid is always defined and has no preconditions.` Call it from the three
production sites. Annotate the test FIFO helpers the same way (fd ≥ 0 before `from_raw_fd`).

Cost/Risk: none.

### F10 — LOW — DUPLICATION — `unix_ms` and Content-Length parsing written twice

Evidence: `mirror.rs:1127-1131` vs `cache.rs:1527-1531`; `mirror.rs:697-710` vs `740-754`

```
fn unix_ms(time: SystemTime) -> Result<u64, MirrorError> {
    let duration = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| MirrorError::Clock)?;
    u64::try_from(duration.as_millis()).map_err(|_| MirrorError::Clock)
}
```

`response_limit` re-parses `CONTENT_LENGTH` instead of calling `response_content_length_optional`. Same header, two
error mappings (`InvalidContentLength` vs `Clock`/`InvalidMetadata`).

Fix: `response_limit` calls `response_content_length_optional`. `unix_ms` lives next to `CachedResponse` in `cache.rs`;
mirror calls it (map `CacheError` or return `u64` and let the caller wrap).

Cost/Risk: none. Regime is once-per-fill, not a hot loop.

## Cross-slice questions

- `cowshed-core/src/repository.rs` (`RepoId` / `validate_identity_component`, owned by CsCoreMetadata): can `RepoId`
  move to a leaf crate gateway may import, so F2's copies die? Gateway `Cargo.toml` has no `cowshed-core` dep; the
  comment at `repo_mirror.rs:1300` treats that as a given.
- `cowshed-gateway/src/config.rs` and `sim_broker.rs`: their `validate_repo_id` copies already admit uppercase. Whoever
  owns those files should delete them in the same cutover as F2.
- `cowshed-gateway/src/cache.rs`: F6/F10 want `hex_decode_32` / `unix_ms` as the crate SSOT. Not audited here.
- `cowshed-gateway/src/policy.rs`: F8 wants the percent-decode loop owned there.
- `cowshed-core/src/git.rs` (`git_command_at`, owned by CsCoreGit): do **not** share it with this helper. Different
  threat model. The leftover `Git2*` name is this slice's problem (F3).
- `cowshed-core/src/runtime/project.rs` `MirrorParams` `{ repo_id, workspace, url }` vs this slice's `RepoMirrorRequest`
  `{ workspace_id, repo_id, remote }`: two DTOs for one coordinator verb. Owned elsewhere; field names already drifted
  (`url`/`remote`, `workspace`/`workspace_id`).

## Non-findings (checked, clean)

- Not one concept split badly. HTTP registry cache vs isolated Git fetch actor. Keep both files. `MAX_REDIRECTS = 5` in
  each is the same number for different protocols; not a shared constant.
- git2 crate is already absent from `cowshed-gateway/Cargo.toml`. Helper + PATH git is the earned replacement. Do not
  reintroduce libgit2. `uuid` (unique published dir names), `sha2` (`hash_component`), `base64` (SRI) are in-process and
  load-bearing — shelling out to `uuidgen`/`shasum`/`base64` would be the wrong call.
- Production `unwrap`/`expect`: only `response_from_hit` (`mirror.rs:467-468`) on `Response::builder()` headers, an HTTP
  invariant. Test `expect`s are fixtures.
- `mirror.rs` has no `#[cfg(test)]` module; behavior lives in `tests/mirror_cache.rs` (outside this slice).
  `repo_mirror.rs` tests (shutdown cancel, credential deadline, preflight redirect, publication fencing, helper
  kill/reap) are typed outcomes, not string-shaped goldens, except the git-upload-pack request line which **is** the
  wire contract.
- Allocations (`to_owned`, `key.clone()` in `execute`'s acquire loop, `serde_json::Value` packument rewrite,
  `cacheable`'s `collect+join+to_ascii_lowercase`, `hash_component` hex `String`) run once per HTTP fill or per Git
  publish — not a per-byte hot loop. Regime: request/publish. Not findings under §4.1.
- `make_read_only` then `directory_bytes` walks the tree twice at publish. Closed-form size is not available from git;
  once-per-publish.
- Unix-only `std::os::unix` / `libc` with no `cfg` — crate is already host-gated (`macos`/`linux` credential providers
  in `lib.rs`). Not a silent cross-compile landmine inside this slice.
- No `TODO`/`FIXME` in either file. No `panic!` on operational paths.
