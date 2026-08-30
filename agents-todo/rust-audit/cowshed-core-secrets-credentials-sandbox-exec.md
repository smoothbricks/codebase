# cowshed-core/secrets+credentials+sandbox+exec

Scope: `packages/cowshed/crates/cowshed-core/src/secrets.rs` (1260), `workspace_credentials.rs` (776),
`workspace_environment.rs` (97), `sandbox.rs` (1140), `exec.rs` (1203), `exec/macos.rs` (64), `exec/linux.rs` (34),
`exec/other.rs` (27). Doctrine: BYPRODUCT-ENGINEERING.md; PERFORMANCE-HANDBOOK §4.1, §7 (04-mechanisms.md), §7.12–7.13
(05-memory-toolkit.md).

## Summary

- CRITICAL: mint wraps the workspace token in `Zeroizing` then `write_workspace_environment` copies it into a plain
  `String` via `format!`/`shell_word`.
- CRITICAL: `read_bounded_utf8` `mem::take`s secret bytes out of `Zeroizing<Vec<u8>>` and drops `FromUtf8Error`
  unzeroized on UTF-8 failure.
- HIGH: 32-byte / 43-char unpadded base64url token is restated in this crate, the supervisor, and cowshed-gateway; they
  agree today.
- MEDIUM: every `unsafe` in `exec/*` lacks a SAFETY invariant comment (`pre_exec`, `proc_pidinfo`, `assume_init`,
  `close_range`).
- MEDIUM: `seatbelt_profile` is a 248-line generator; SBPL last-match-wins order is load-bearing and buried in one
  function.
- MEDIUM: `exec::has_traversal` restates `repository::is_lexically_canonical` instead of calling it.
- MEDIUM: `COWSHED_WORKSPACE_TOKEN` / `COWSHED_PORT_BASE` are bare string literals in the env-file writer and again in
  the supervisor.
- LOW: secret-scan findings allocate `String` rule ids and clone redacted context per rule; Nix live test can never go
  red on a host without the daemon.
- Seatbelt profile text has one generator. This slice does not talk to the keychain. `process.rs` is not a third spawn
  wrapper.

## Findings

### F1 — CRITICAL — COPIES — workspace token escapes Zeroizing via format!/shell_word

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_credentials.rs:118-128`

```
    let private_key = Zeroizing::new(signing_key.serialize_pem());
    let certificate_pem = certificate.pem();
    let mut token_bytes = Zeroizing::new([0_u8; TOKEN_BYTES]);
    getrandom::fill(&mut token_bytes[..])
        .map_err(|_| WorkspaceCredentialError::Generation("workspace token"))?;
    let token = Zeroizing::new(URL_SAFE_NO_PAD.encode(&token_bytes[..]));
    publish_asset(private_key_path, private_key.as_bytes())?;
    publish_asset(&certificate_path, certificate_pem.as_bytes())?;
    publish_asset(&token_path, token.as_bytes())?;
    write_workspace_environment(mount_point, workspace_mount, &token, platform, port_block)?;
```

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_environment.rs:54-70`

```
    let mut contents = format!(
        "export GOENV={}\nexport COWSHED_WORKSPACE_TOKEN={}\n",
        shell_word(go_env),
        shell_word(token),
    );
    if let Some(block) = port_block {
        contents.push_str(&format!("export COWSHED_PORT_BASE={}\n", block.base()));
    }
    write_atomic_bytes(
        &image_root.join(WORKSPACE_ENVIRONMENT_PATH),
        contents.as_bytes(),
    )?;
```

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_environment.rs:74-86`

```
fn shell_word(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| { /* alphanumeric + _@%+=:,./- */ })
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
```

Problem: `GatewayWorkspaceCredentials` documents that secret bytes are zeroized on drop. Minting actually does wrap the
token. Then `write_workspace_environment` takes `&str` and `shell_word` does `to_owned()` (the token alphabet is
unpadded base64url, so the safe-charset arm always wins) and `format!` copies it again into `contents: String`. After
the 0600 write, `contents` is dropped without zeroize. Two heap copies of the bearer leave the zeroizing type. Regime:
once per mint/rotation, not a hot loop — still a live secret-residue bug, not a performance note. Fix: change
`write_workspace_environment` to take `&Zeroizing<String>` (or `&[u8]`) and build `contents` as `Zeroizing<String>` /
`Zeroizing<Vec<u8>>`. For the token, skip `shell_word` (the alphabet is already the safe set; that is an invariant of
`TOKEN_ENCODED_BYTES` + URL_SAFE_NO_PAD) and `push_str` into the zeroizing buffer. `shell_word` stays for `GOENV` only.
Cost/Risk: only caller is `mint_workspace_credentials`. Env-file bytes on disk stay 0600 via `write_atomic_bytes`. Tests
that match the env-file text still pass.

### F2 — CRITICAL — COPIES — read_bounded_utf8 drops unzeroized secret bytes on UTF-8 failure

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_credentials.rs:431-441`

```
    let mut bytes = Zeroizing::new(Vec::with_capacity(capacity));
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_failure(operation, path, source))?;
    if bytes.len() as u64 > maximum {
        return Err(invalid(kind, path));
    }
    String::from_utf8(std::mem::take(&mut *bytes))
        .map(Zeroizing::new)
        .map_err(|_| invalid(kind, path))
```

Problem: `mem::take` moves the secret `Vec<u8>` out of `Zeroizing`, leaving an empty vec that zeroizes nothing.
`String::from_utf8` takes ownership. On `Err`, `FromUtf8Error` holds the original bytes and is discarded by
`map_err(|_| …)` — std Drop of that error does not zeroize. This path is used for the gateway token and private key PEM
(`read_gateway_workspace_credentials` at 212-229). Contrast `validate_private_key` at 143-144, which borrows with
`str::from_utf8(bytes.as_ref())` and keeps the `Zeroizing` owner. Regime: once per gateway credential load; UTF-8
failure is the hostile/corrupt-file path, which is exactly when leftover key bytes matter. Fix: do not take out of
`Zeroizing`. Use `str::from_utf8(bytes.as_ref())`, then `Zeroizing::new(validated.to_owned())` (still one copy, but the
source remains zeroized) — or
`Zeroizing::new(String::from_utf8(Zeroizing::take(bytes)).map_err(|e| { let mut v = e.into_bytes(); v.zeroize(); invalid(...) })?)`
so the error bytes are wiped. Prefer the borrow form; it already exists 30 lines above in `validate_private_key`.
Cost/Risk: local to `read_bounded_utf8`. Gateway read tests still see the same `&str` contents.

### F3 — HIGH — SSOT — 32-byte / 43-char token format restated in three places

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_credentials.rs:26-27,362-375`

```
const TOKEN_BYTES: usize = 32;
const TOKEN_ENCODED_BYTES: usize = 43;
…
    if encoded.len() != TOKEN_ENCODED_BYTES || encoded.contains(&b'=') {
        return Err(invalid("workspace token", path));
    }
    let decoded = Zeroizing::new(
        URL_SAFE_NO_PAD.decode(&encoded[..])
            .map_err(|_| invalid("workspace token", path))?,
    );
    if decoded.len() != TOKEN_BYTES {
        return Err(invalid("workspace token", path));
    }
```

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1084-1088` (other slice)

```
fn valid_workspace_token(token: &str) -> bool {
    token.len() == 43
        && token.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:19,104-114` (other crate)

```
pub const TOKEN_BYTES: usize = 32;
    pub fn parse(encoded: &str) -> Result<Self, ConfigError> {
        if encoded.contains('=') { return Err(ConfigError::MalformedToken); }
        let decoded = URL_SAFE_NO_PAD.decode(encoded)…;
        let bytes: [u8; TOKEN_BYTES] = decoded.try_into()…;
```

Problem: one concept (unpadded base64url of 32 random bytes, no `=`) is three validators. They agree today (`43` is
`ceil(32*4/3)` without pad). Supervisor does not decode; it only checks length+alphabet, so a 43-char alphabet-legal
string that is not valid base64url would be injected into `HTTP_PROXY` / `COWSHED_WORKSPACE_TOKEN` while mint/gateway
would reject it. That is a latent divergence, not yet a live mismatch of constants. Fix: export `TOKEN_BYTES` /
`TOKEN_ENCODED_BYTES` (and one `fn token_encoded_ok(&[u8]) -> bool` that decodes) from `workspace_credentials`.
Supervisor and gateway call it. Delete the magic `43` and the second `TOKEN_BYTES`. Cost/Risk: cowshed-gateway currently
owns `TOKEN_BYTES` as a public config constant; callers of `WorkspaceToken::parse` must keep working. One decision:
credentials crate is the minting authority, so it is the SSOT.

### F4 — MEDIUM — STRUCTURE — unsafe in exec/* has no SAFETY comments

Evidence: `packages/cowshed/crates/cowshed-core/src/exec/macos.rs:25-61`

```
    let required = unsafe { libc::proc_pidinfo(libc::getpid(), libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
    …
    let bytes = unsafe { libc::proc_pidinfo(… descriptors.as_mut_ptr().cast(), capacity as libc::c_int) };
    let count = validate_fd_listing_size(bytes, capacity)?;
    for descriptor in &descriptors[..count] {
        let descriptor = unsafe { descriptor.assume_init_ref() }.proc_fd;
        …
    }
    unsafe {
        command.pre_exec(move || {
            mark_macos_non_stdio_close_on_exec(&mut descriptors)
                .map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO))
        });
    }
```

Evidence: `packages/cowshed/crates/cowshed-core/src/exec/linux.rs:14,27-31` (`SYS_close_range` + `pre_exec`);
`exec/other.rs:20-24` (`pre_exec`); `exec.rs:139,144,176` (`assume_init` after `getrlimit`, `fcntl`). Problem:
`CommandExt::pre_exec` is unsafe because the closure runs between fork and exec in a signal-restricted child.
`assume_init_ref` is unsafe because it trusts `proc_pidinfo` filled `count` entries. None of these sites state the
invariant (post-fork single-threaded; `validate_fd_listing_size` proved `bytes` is a multiple of `proc_fdinfo` and
`<= capacity`; `getrlimit != -1` initialized the struct). Repo rule: unsafe without a stated invariant comment. Fix:
one-line SAFETY above each block citing the check that makes it sound. Do not wrap or hide the unsafe. Cost/Risk:
comments only.

### F5 — MEDIUM — STRUCTURE — seatbelt_profile is a 248-line last-match-wins generator

Evidence: `packages/cowshed/crates/cowshed-core/src/sandbox.rs:187-435` (`pub fn seatbelt_profile` through
`Ok(profile)`). Problem: function is ~248 lines (bar is ~100). It mixes path validation, grant∩deny, immutable system
roots, sockets, port literals, cache carve-backs, host cargo registry, hard denies, git-worktree hole,
protected-artifact denies, and the supervisor/child terminal narrowing. SBPL is last-match-wins; order is the
enforcement. A 248-line body is how a later edit inserts a grant after a deny and silently opens a hole. Not a 5k-line
god file; this is the natural seam. Fix: keep one public `seatbelt_profile`. Split emission into ordered stages that
return `()` and only append: `emit_header`, `emit_system_roots`, `emit_network`, `emit_grants`,
`emit_store_denies_and_ancestors`, `emit_hard_denies`, `emit_git_worktree`, `emit_protected_artifacts`,
`emit_role_terminal`. The stage order in the caller is the documented last-match-wins sequence; tests that pin substring
order stay. Cost/Risk: tests in this file assert on rendered SBPL text and relative `find` positions; they are the
oracle and must keep passing byte-for-byte.

### F6 — MEDIUM — DUPLICATION — exec::has_traversal restates repository::is_lexically_canonical

Evidence: `packages/cowshed/crates/cowshed-core/src/exec.rs:348-357,405-408`

```
    if has_traversal(workspace_mount) {
        return Err(ExecError::WrapperFailure { stage: WrapperStage::ValidateProfile, … });
    }
…
fn has_traversal(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
}
```

Evidence: `packages/cowshed/crates/cowshed-core/src/repository.rs:719-726` (neighbouring module)

```
/// The one definition of "canonical" every path validator narrows from
pub fn is_lexically_canonical(path: &Path) -> bool {
    path.is_absolute()
        && !path.components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
}
```

Evidence: `packages/cowshed/crates/cowshed-core/src/sandbox.rs:478-490` already calls `is_lexically_canonical`. Problem:
the comment on `is_lexically_canonical` says every path validator must agree with it. `contained_cwd` reimplements the
`.`/`..` half and checks `is_absolute` separately. `sandbox::validate_path` does the right thing. Two predicates can
drift (e.g. one starts treating `Prefix`/`RootDir` differently). Fix: `contained_cwd` should use
`crate::repository::is_lexically_canonical` for the mount (absolute+normalized) and, for the requested cwd, either the
same predicate after join or a shared `has_dot_dot` extracted from it. Delete `has_traversal`. Cost/Risk:
`contained_cwd` still does the real `canonicalize` + `starts_with(workspace)` containment check; this only replaces the
lexical pre-check. Exec cwd tests stay.

### F7 — MEDIUM — SSOT — env var names are untyped string literals

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_environment.rs:58-64`

```
        "export GOENV={}\nexport COWSHED_WORKSPACE_TOKEN={}\n",
        shell_word(go_env),
        shell_word(token),
    );
    if let Some(block) = port_block {
        contents.push_str(&format!("export COWSHED_PORT_BASE={}\n", block.base()));
```

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1271-1272` (other slice)

```
            .env("COWSHED_PORT_BASE", &port_base)
            .env("COWSHED_WORKSPACE_TOKEN", workspace_token)
```

Problem: the env-file contract and the sandboxed-child env injection must name the same variables. They are restated as
literals. A rename or a typo in one arm leaves direnv-sourced shells and `cowshed exec` disagreeing.
`WORKSPACE_ENVIRONMENT_PATH` is already a constant; the variable names are not. Fix:
`pub const WORKSPACE_TOKEN_ENV: &str = "COWSHED_WORKSPACE_TOKEN";` and `PORT_BASE_ENV` next to
`WORKSPACE_ENVIRONMENT_PATH`. Both writers use them. Cost/Risk: supervisor spawn env setup must switch to the constants
(CsCoreSupervisor). Tests that match `export COWSHED_WORKSPACE_TOKEN=` keep working if the value is unchanged.

### F8 — LOW — COPIES — SecretFinding allocates String rule ids and clones redacted context per rule

Evidence: `packages/cowshed/crates/cowshed-core/src/secrets.rs:283-348`

```
            rule_id: rule_id.to_owned(),
            …
            context: "[REDACTED: sensitive filename]".to_owned(),
…
        let context = redact_line(line, &matches);
        let mut rules: Vec<&str> = matches.iter().map(|matched| matched.rule_id).collect();
        …
        for rule_id in rules {
            findings.push(SecretFinding {
                path: relative.to_path_buf(),
                rule_id: rule_id.to_owned(),
                line: Some(index + 1),
                context: context.clone(),
            });
        }
```

Problem: `filename_rule` / scanners already return `&'static str`. `SecretFinding.rule_id` is `String` because of serde.
Each matching rule on a line clones the whole redacted context. Regime: once per `scan_tree` (commit/quarantine), not a
per-exec hot path — so this is LOW, not a profile-trap finding. Still evaporating work (Byproduct L0): the rule id is
static and the context is identical across rules on one line. Fix: store `rule_id: &'static str` and a custom
serializer, or an interned enum of rule ids. Share context via `Arc<str>` if multiple rules on one line remain a product
requirement; otherwise emit one finding with a list of rule ids. Cost/Risk: JSON camelCase of `SecretScan` is consumed
by project runtime error rendering (`runtime/project.rs`). Changing the DTO shape is a typed cutover there.

### F9 — LOW — TESTS — Nix-daemon Seatbelt test is a silent no-op without Nix

Evidence: `packages/cowshed/crates/cowshed-core/src/sandbox.rs:1020-1029`

```
        let Some(socket) = nix_daemon_socket() else {
            // No multi-user Nix on this host: there is no boundary to exercise.
            return;
        };
        let Ok(nix) = fs::canonicalize("/nix/var/nix/profiles/default/bin/nix") else {
            return;
        };
```

Problem: PERFORMANCE-HANDBOOK §7.10bb — a guard that cannot go red is not a guard. On a host without multi-user Nix this
test is green while proving nothing about admission. The unit test
`the_daemon_socket_is_admitted_only_by_resolving_to_a_real_socket` (973-1015) already covers the resolver with a
synthetic socket and does go red. Fix: keep the live test but `#[ignore]` it (or gate on an explicit env) so CI that
lacks Nix does not report a pass. Do not `return` on the success path of the default suite. Cost/Risk: none to
production. Operators who want the live boundary keep `cargo test -- --ignored`.

### F10 — LOW — DUPLICATION — linux.rs and other.rs prepare_child_descriptors are the same function

Evidence: `packages/cowshed/crates/cowshed-core/src/exec/linux.rs:21-33` and `exec/other.rs:14-26` — both
`descriptor_limit()`, then
`unsafe { command.pre_exec(move || mark_non_stdio_close_on_exec(descriptor_limit).map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO)) ) }`.
Problem: two copies of the CLOEXEC `pre_exec` trampoline. The mark functions differ (close_range vs fd walk); the
trampoline does not. Fix: one `fn install_cloexec_pre_exec(command, limit, mark: fn(rlim_t) -> io::Result<()>)` in
`exec.rs`. Platform modules only supply `mark_non_stdio_close_on_exec`. Cost/Risk: cfg tests that import the platform
mark functions stay.

## Cross-slice questions

- `runtime/supervisor.rs:1224-1272` (`CsCoreSupervisor`): `tokio::fs::read_to_string` loads the workspace token into a
  plain `String`, then copies it into `COWSHED_WORKSPACE_TOKEN` and `gateway_proxy_url`
  (`format!("http://cowshed:{workspace_token}@…")`). That is a third Zeroizing escape of the same secret F1 covers at
  mint time. Also `valid_workspace_token` (F3) and the env-var literals (F7).
- `runtime/supervisor.rs:1177-1315` (`CsCoreSupervisor`): production sandbox spawn is `plan_exec` + a tokio `Command` +
  `prepare_child_descriptors` + a second `pre_exec(setpgid)`. `exec::SystemSpawnRunner` is the sync inherit-stdio twin.
  `process.rs` is **not** a third spawn wrapper (it is `ProcessStatus` / `CommandOutput` diagnostics only). Two spawn
  shells share the plan and CLOEXEC prep; they do not share Command construction. If supervisor is the only production
  caller of sandboxed exec, `SystemSpawnRunner` may be test-only — confirm before deleting.
- `cowshed-gateway/src/config.rs:19,104-129` (`CsGw*`): second `TOKEN_BYTES` and a second URL_SAFE_NO_PAD parse;
  `WorkspaceToken::encode` returns a non-zeroizing `String`. Credentials should be SSOT (F3).
- `fsio.rs:101-103` (`CsCoreCopy` / metadata): `publish_private_file` writes secrets through `BufWriter<File>`. The
  buffer is not zeroized on drop. Mint of token + private key goes through this path.
- `storage/bootstrap/native/macos.rs` uses `/usr/bin/security` against System.keychain; `cowshed-gateway` uses
  `security-framework`. This slice mints file-backed workspace CAs with `rcgen` and does not touch the keychain. No
  contradiction; do not unify those with this slice.
- `Cargo.lock` has `getrandom` 0.2.17, 0.3.4, and 0.4.3. This crate depends on `getrandom = "0.3"` for token minting.
  Duplicate-version question for `XcutDeps`.

## Non-findings (checked, clean)

- Seatbelt profile text: one generator (`seatbelt_profile`). Tests restate SBPL fragments as oracles of last-match-wins
  order; that is the wire format, not a second template. `SANDBOX_EXEC` is the production path constant; sandbox tests
  hardcode `/usr/bin/sandbox-exec` only in live `Command::new` (same bytes).
- Keychain: this slice has no `security-framework` and no `security` CLI. Workspace token/CA live under `.cowshed/` as
  0600 files (`write_atomic_bytes` → `publish_private_file` mode 0600, `validate_mode_and_type` checks 0600).
- `rcgen` (`default-features = false`, `crypto,pem,ring,zeroize`) and `x509-parser` (`verify`): load-bearing in-process
  P-256 CA mint + identity-stamp check. Shelling `openssl` would lose typed errors and the
  `cowshed:<repo>:<ws>:<incarnation>` subject contract. Keep.
- `getrandom`: load-bearing; token mint cannot be a `uuidgen` shell-out (need 32 opaque bytes, no UUID variant bits,
  `Zeroizing` destination).
- `base64` URL_SAFE_NO_PAD: tiny, shared with gateway decode. Not git2/openssl-class bloat.
- `walkdir`: in-process scan needs `follow_links(false)`, `sort_by_file_name`, `filter_entry` prune of `.git`/caches. A
  `find` shell-out is not machine-typed and would follow the policy the scanner exists to enforce. Keep.
- `zeroize` crate itself is used; F1/F2 are incomplete application, not a missing dependency. `rcgen`'s `zeroize`
  feature is on. CA `certificate.pem()` is public by design (published as `ca.pem`).
- `validate_private_key` / `validate_workspace_credentials` borrow PEM via `str::from_utf8` on `Zeroizing<Vec<u8>>` —
  the correct pattern F2 should copy.
- Operational failures are `Result` (`SecretScanError`, `WorkspaceCredentialError`, `SandboxError`, `ExecError`).
  Production `expect` in secrets scanners is on `checked_add` of in-line offsets (invariant). No `unwrap` on I/O in
  non-test code.
- `process.rs` is status/output formatting, not spawn.
- SecretFinding serde DTO is not restated in TS in `packages/*/src`.
- `COWSHED_ROOT = "/private/cowshed"` is the machine-global deny covering both `STORE_ROOT` and `CACHES_ROOT`; sandbox
  uses those bootstrap constants for sccache paths. Not a second store-root spelling.
- Sandbox tests that `contains` rendered SBPL would go red if a deny/grant moved; they are substitution-capable for the
  ordering contract. Credentials tests assert typed `InvalidAsset` / token length / 0600, not just Display strings.
- `linux` vs `macos` CLOEXEC strategies are genuinely different (`close_range` vs `PROC_PIDLISTFDS`); only the
  trampoline is duplicated (F10).
- Allocations in `scan_tree` / `seatbelt_profile` / `plan_exec` are once-per-scan or once-per-spawn, not per-byte
  kernels. Not inflated under §4.1.
