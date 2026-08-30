# XCUT intra-cowshed duplication

Scope: grep-driven census of every `.rs` file under `packages/cowshed/crates/cowshed-core/src/` (~88k loc),
`cowshed-cli/src/` (~12k), `cowshed-gateway/src/` (~15k). Bodies read (not skimmed) for every quoted finding. Line
counts from `wc -l`:

cowshed-core: `lib.rs` 37, `error.rs` 177, `exec.rs` 1203, `process.rs` 191, `runtime/supervisor.rs` 4102,
`runtime/project.rs` 10762 (spawn/grant mapping ranges), `metadata.rs` 1824, `sandbox.rs` 1140, `gateway_sessions.rs`
511, `workspace_credentials.rs` 776, `workspace_environment.rs` 97, `repository.rs` 1233, `git.rs` 3246
(`git_command_at`), `fsio.rs` 191, `storage/bootstrap.rs` 1794 (constants), `storage/host_config.rs` 619, `apfs.rs` 5169
(`CommandRunner`), `api/dto.rs` 2517 (digest), `api/capability.rs` 3023 (`verify_peer`), `api/server.rs` 1128
(`verify_peer`). cowshed-cli: `lib.rs` 12, `launchd.rs` 1650, `gateway_service.rs` 1051, `probe.rs` 394, `runtime.rs`
4008 (`parse_duration`), `sccache_service.rs` 680. cowshed-gateway: `lib.rs` 61, `config.rs` 676, `policy.rs` 641,
`control.rs` 1168, `cache.rs` 1532, `mirror.rs` 1200, `repo_mirror.rs` 1962, `sim_broker.rs` 1244, `actor.rs` 2206
(`GatewayError`). Manifests: the three `Cargo.toml`. Crate graph: `cowshed-core` depends on `cowshed-gateway`; gateway
cannot import core.

Ranked SSOT table (concept / locations / proposed owner):

| concept                               | locations                                                                                | owner                                                                                            |
| ------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `owner/repo` identity grammar         | `repository.rs:107-131` vs `config.rs:210-228`                                           | **core `RepoId`** — gateway copy is wrong                                                        |
| macOS 16-port block 40960–49151       | `metadata.rs:13-16` vs `config.rs:20-22`                                                 | **gateway** (leaf); core re-export                                                               |
| 32-byte workspace token               | `workspace_credentials.rs:26-27` vs `config.rs:19` vs `supervisor.rs:1084-1088`          | **gateway `TOKEN_BYTES` + `WorkspaceToken::parse`**                                              |
| egress grant / mode                   | `metadata.rs:856-878` vs `sandbox.rs:11-21` vs `policy.rs:8-191` vs `control.rs:525-531` | **gateway `EgressGrant`**; core `EgressRule` is the durable form, sandbox drops mode             |
| `/private/cowshed` roots              | `bootstrap.rs:31-33` vs `sandbox.rs:8` vs `host_config.rs:56`                            | **core `STORE_ROOT`/`CACHES_ROOT`**; derive the parent                                           |
| `COWSHED_{PORT_BASE,WORKSPACE_TOKEN}` | `workspace_environment.rs:58-64` vs `supervisor.rs:1271-1272`                            | **`workspace_environment`**                                                                      |
| SHA-256 hex                           | `dto.rs:972-1004` vs `cache.rs:1487-1524` vs `gateway_sessions.rs:178-183`               | **gateway `hex_*`** (leaf); core keep `Sha256Digest` as wrapper                                  |
| git no-prompt / no-system config      | `git.rs:1844-1846` vs `repo_mirror.rs:579-585` vs `supervisor.rs:1256-1258`              | **core `git_command_at` for checkout git**; gateway helper stays separate but share env literals |
| `CommandOutput`                       | `process.rs:51-56` vs `launchd.rs:1099-1104`                                             | **core `process::CommandOutput`** if CLI needs status fidelity; else rename CLI type             |
| unix peer-uid handshake               | `capability.rs:887-904` vs `server.rs:1089-1107`                                         | **one `verify_peer` in `peer_credentials`**                                                      |
| sim openurl/install                   | `metadata.rs:885-889` vs `sim_broker.rs:30-32`                                           | **one enum**; serde currently disagrees                                                          |
| dir mode 0700                         | `launchd.rs:18` vs `gateway_service.rs:44`                                               | **cli `launchd::PRIVATE_DIRECTORY_MODE`**                                                        |

## Summary

- HIGH: `RepoId` grammar is restated in the gateway and **already disagrees** (uppercase, length, leading `-`/`_`).
- HIGH: macOS port-block `{min,max,size}` live in both `cowshed-core::metadata` and `cowshed-gateway::config`; they
  agree today, drift is a Seatbelt/CONNECT outage.
- HIGH: workspace token is 32 bytes / 43 chars in three validators; supervisor does not decode.
- MEDIUM: three `EgressGrant`/`EgressRule` types plus two `EgressMode` enums; sandbox mapping drops
  `mode`/`impersonate`.
- MEDIUM: `/private/cowshed` restated as `COWSHED_ROOT` and as a `host.json` path literal instead of `STORE_ROOT`.
- MEDIUM: `COWSHED_PORT_BASE` / `COWSHED_WORKSPACE_TOKEN` stringly duplicated between env-file writer and supervisor
  spawn.
- MEDIUM: SHA-256 hex encode/decode written four times; uppercase policy differs.
- LOW: git env literals, `CommandOutput` name collision, duplicated `verify_peer` with divergent error mapping,
  `SimVerb` vs `SimGrant` serde, CLI `0o700` constant.
- Spawn wrappers: **not three copies**. `exec.rs` plans + CLOEXEC; `supervisor.rs` consumes them; `process.rs` is
  status/output types.
- Crate graph forces shared constants into **gateway** (or a new leaf). Core already does this for `WorkspaceToken`;
  ports and `TOKEN_BYTES` did not follow.

## Findings

### F1 — HIGH — SSOT — `RepoId` grammar restated in gateway and already diverged

Evidence: `packages/cowshed/crates/cowshed-core/src/repository.rs:107-131`

```
fn validate_identity_component(value: &str, component: RepoIdComponent) -> Result<(), RepoIdError> {
    ...
    if value == "." { return Err(RepoIdError::TraversalComponent { component }); }
    if value == ".." { return Err(RepoIdError::TraversalComponent { component }); }
    ...
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(RepoIdError::InvalidComponent { component });
    }
    if !value.as_bytes().iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte))
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:210-228`

```
fn validate_identifier(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty() || value.len() > 128
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    { return Err(ConfigError::InvalidIdentifier { field }); }
}
fn validate_repo_id(value: &str) -> Result<(), ConfigError> {
    let (owner, name) = value.split_once('/').ok_or(ConfigError::InvalidRepoId)?;
    if name.contains('/') || matches!(owner, "." | "..") || matches!(name, "." | "..") {
        return Err(ConfigError::InvalidRepoId);
    }
    validate_identifier("repo_id", owner)...
```

Problem: one identity, two grammars. Core is `[a-z0-9][a-z0-9._-]*` with no 128 cap. Gateway is 1–128 **ASCII
alphanumeric** (uppercase allowed) and accepts a leading `-`/`_`. Live bug: a control-plane `install` can admit
`Acme/Widget` that `RepoId::parse` will never produce or match. Inventory path is safe only because core mints first.
Fix: delete `validate_repo_id`'s private grammar. Until a leaf crate exists, copy `RepoId::parse` rules byte-for-byte
into gateway and pin with a table test (`Acme/x` reject, `a/_b` reject, 129-char reject/accept per core). Owner of the
grammar is `cowshed-core::repository::RepoId` because that is the durable store key; gateway is a cache of it.
Cost/Risk: gateway session install + any raw control client. No on-disk format change if production only ever wrote core
`RepoId`s.

### F2 — HIGH — SSOT — macOS port-block constants restated across the crate boundary

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:13-16`

```
pub const PORT_BLOCK_SIZE: u16 = 16;
pub const MACOS_PORT_BLOCK_MIN: u16 = 40_960;
pub const MACOS_PORT_BLOCK_MAX: u16 = 49_151;
pub const MACOS_PORT_BLOCK_LAST_BASE: u16 = MACOS_PORT_BLOCK_MAX - PORT_BLOCK_SIZE + 1;
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:20-22` and `:75-89`

```
pub const MACOS_PORT_MIN: u16 = 40_960;
pub const MACOS_PORT_MAX: u16 = 49_151;
pub const MACOS_PORT_BLOCK_SIZE: u16 = 16;
...
if address.port() < MACOS_PORT_MIN
    || last > MACOS_PORT_MAX
    || !(address.port() - MACOS_PORT_MIN).is_multiple_of(MACOS_PORT_BLOCK_SIZE)
```

Problem: Seatbelt admits `base..base+15`; the gateway listener is `base`. Two spellings of the same interval. Values
agree today (`40960–49151`, size 16) — not yet a live numeric bug. Drift is a CONNECT blackhole, not a compile error.
Core already depends on gateway, so this copy is unnecessary. Fix: keep the constants in `cowshed-gateway::config`
(leaf). In core:
`pub use cowshed_gateway::{MACOS_PORT_MIN as MACOS_PORT_BLOCK_MIN, MACOS_PORT_MAX as MACOS_PORT_BLOCK_MAX, MACOS_PORT_BLOCK_SIZE as PORT_BLOCK_SIZE};`
and derive `MACOS_PORT_BLOCK_LAST_BASE` from those. Delete the core literals. Cost/Risk: `PortBlock::new`, slot math in
`runtime/project.rs:3376-3378`, gateway `validate_macos_port_block`. Rename-only at call sites if the aliases are
published.

### F3 — HIGH — SSOT — workspace token size/validator written three times

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_credentials.rs:26-27,362-376`

```
const TOKEN_BYTES: usize = 32;
const TOKEN_ENCODED_BYTES: usize = 43;
...
if encoded.len() != TOKEN_ENCODED_BYTES || encoded.contains(&b'=') { return Err(...) }
let decoded = URL_SAFE_NO_PAD.decode(&encoded[..])...
if decoded.len() != TOKEN_BYTES { return Err(...) }
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/config.rs:19,104-114`

```
pub const TOKEN_BYTES: usize = 32;
pub fn parse(encoded: &str) -> Result<Self, ConfigError> {
    if encoded.contains('=') { return Err(ConfigError::MalformedToken); }
    let decoded = URL_SAFE_NO_PAD.decode(encoded)...
    let bytes: [u8; TOKEN_BYTES] = decoded.try_into()...
```

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1084-1088`

```
fn valid_workspace_token(token: &str) -> bool {
    token.len() == 43
        && token.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}
```

Problem: one token, three predicates. `TOKEN_BYTES` is copied. Supervisor uses a magic `43` and **never decodes** — a
43-char `A-Za-z0-9-_` string that is not valid unpadded base64url still becomes `COWSHED_WORKSPACE_TOKEN` and HTTP proxy
userinfo; the gateway then fails closed on CONNECT. `WorkspaceToken::parse` does not pin encoded length 43. Values 32/43
agree today. Fix: mint and validate only through `cowshed_gateway::WorkspaceToken` (`TOKEN_BYTES` already public).
`validate_token` becomes `WorkspaceToken::parse`. Supervisor calls that (or the already-loaded
`GatewayWorkspaceCredentials::token`) and drops `valid_workspace_token`. Delete core `TOKEN_BYTES` /
`TOKEN_ENCODED_BYTES`. Cost/Risk: credential publish, supervisor spawn env, control `SessionWire`. Token files on disk
stay 43-char base64url.

### F4 — MEDIUM — SSOT — egress grant/mode types restated three times

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:856-878`

```
#[serde(rename_all = "lowercase")]
pub enum EgressMode { Intercept, Opaque }
pub struct EgressRule {
    pub host: String,
    pub ports: Vec<u16>,
    pub mode: EgressMode,
    pub impersonate: Option<String>,
}
```

Evidence: `packages/cowshed/crates/cowshed-core/src/sandbox.rs:11-21`

```
pub struct EgressGrant { pub host: String, pub ports: Vec<u16> }
pub struct SandboxGrants { pub read: Vec<PathBuf>, pub write: Vec<PathBuf>, pub egress: Vec<EgressGrant> }
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/policy.rs:8-13,183-191` and `control.rs:525-531`

```
#[serde(rename_all = "kebab-case")]
pub enum EgressMode { Intercept, Opaque }
pub struct EgressGrant { pub host: HostPattern, pub port: u16, pub mode: EgressMode, ... impersonate: bool }
struct GrantWire { host, port, mode: EgressMode, methods, path_prefixes, impersonate }
```

Problem: durable sidecar (`EgressRule`, ports as `Vec`), Seatbelt snapshot (`sandbox::EgressGrant`, mode dropped),
runtime policy (one port per grant, `HostPattern`). `policy_from_grants` (`gateway_sessions.rs:191-209`) is the adapter
and hard-codes empty ports → `[443, 80]`. Two `EgressMode` enums; serde `lowercase` vs `kebab-case` happens to agree for
these two variants only. `runtime/project.rs:3515-3518` clones host/ports and **drops `mode`/`impersonate`** —
documented as Seatbelt-not-enforcing-egress, but the type name `EgressGrant` collides with the gateway type. Fix: rename
sandbox field to `SandboxEgress` (or drop egress from Seatbelt grants entirely — gateway enforces it). Keep `EgressRule`
as the file format. Use gateway `EgressMode` from core via `pub use` (core already depends on gateway). Put the
default-port table next to `EgressRule`, not inline in the adapter. Cost/Risk: grants JSON (`camelCase` `egress[]`),
`policy_from_grants`, sandbox tests. Do not change on-disk `EgressRule` without a format bump.

### F5 — MEDIUM — SSOT — `/private/cowshed` restated instead of derived from `STORE_ROOT`

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/bootstrap.rs:31-33`

```
pub const STORE_ROOT: &str = "/private/cowshed/store";
pub const CACHES_ROOT: &str = "/private/cowshed/caches";
```

Evidence: `packages/cowshed/crates/cowshed-core/src/sandbox.rs:8,223-224,318`

```
const COWSHED_ROOT: &str = "/private/cowshed";
let cowshed = Path::new(COWSHED_ROOT);
push_subpath_rule(&mut profile, "deny file-read* file-write*", cowshed)?;
```

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/host_config.rs:56`

```
let default = if store_root == Path::new("/private/cowshed/store") {
```

Problem: the machine-global parent and the store path are independent literals. CLI already refuses to restate
`STORE_ROOT` (`runtime.rs:2504-2506`); `host_config` and Seatbelt did not get the memo. If `STORE_ROOT` moves and
`COWSHED_ROOT` does not, the deny no longer covers the store (or covers the wrong tree). The
`== "/private/cowshed/store"` test silently takes the non-HOME default-mount branch. Fix: `const COWSHED_ROOT` derived
as the parent of `Path::new(STORE_ROOT)` (or `STORE_ROOT` built from `COWSHED_ROOT.join("store")` — one table).
`host_config` compares to `Path::new(STORE_ROOT)`. Cost/Risk: Seatbelt deny, host.json default `~/.cowshed/mnt`. Tests
assert the deny string `/private/cowshed`.

### F6 — MEDIUM — SSOT — workspace env var names restated in the supervisor

Evidence: `packages/cowshed/crates/cowshed-core/src/workspace_environment.rs:58-64`

```
let mut contents = format!(
    "export GOENV={}\nexport COWSHED_WORKSPACE_TOKEN={}\n",
    shell_word(go_env), shell_word(token),
);
if let Some(block) = port_block {
    contents.push_str(&format!("export COWSHED_PORT_BASE={}\n", block.base()));
}
```

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1261,1271-1272`

```
.env("GOENV", private_cache.join("go/env"))
.env("COWSHED_PORT_BASE", &port_base)
.env("COWSHED_WORKSPACE_TOKEN", workspace_token)
```

Problem: direnv file and sandbox spawn must export the same names. They are string literals in two modules. A rename in
one leaves the other poisoning shells with a dead variable (and cargo/go still seeing the old one). Fix: `pub const` in
`workspace_environment.rs` (`ENV_TOKEN`, `ENV_PORT_BASE`, `ENV_GOENV`). Supervisor uses those. Same file already owns
`WORKSPACE_ENVIRONMENT_PATH`. Cost/Risk: spawn env + `.cowshed/env` contents. No format bump if the strings stay.

### F7 — MEDIUM — SSOT — SHA-256 hex encode/decode written four times

Evidence: `packages/cowshed/crates/cowshed-core/src/api/dto.rs:972-1004`

```
pub fn to_hex(self) -> String { ... write!(&mut output, "{byte:02x}") ... }
fn hex_nibble(byte: u8) -> u8 {
    match byte { b'0'..=b'9' => byte - b'0', b'a'..=b'f' => byte - b'a' + 10, _ => unreachable!(...) }
}
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/cache.rs:1487-1524`

```
fn hex_encode(bytes: &[u8]) -> String { const HEX: &[u8; 16] = b"0123456789abcdef"; ... }
fn hex_nibble(value: u8) -> Result<u8, CacheError> {
    match value { b'0'..=b'9' => Ok(...), b'a'..=b'f' => Ok(...), _ => Err(CacheError::InvalidMetadata) }
}
```

Also: `gateway_sessions.rs:178-183` (`hex_prefix`), `mirror.rs:1082-1087` and `policy.rs:596-603` (these two accept
`A-F`; digest hex does not). Problem: same 32-byte digest alphabet, four implementations. Uppercase accepted for URL
percent-decode, rejected for cache metadata — correct domains, but `hex_nibble` itself is copy-paste. Core cannot import
gateway helpers without moving them to the leaf; gateway cannot import `Sha256Digest`. Fix: one
`hex_encode`/`hex_decode_n` in `cowshed-gateway` (already the cache copy). Core `Sha256Digest` keeps the newtype and can
keep a 15-line encoder (digest display is not cache metadata). Do not unify percent-decode uppercase with digest
lowercase. Cost/Risk: cache metadata, mirror integrity, workspace id hex. Wrong unification of percent vs digest
alphabets would be worse than the copies.

### F8 — LOW — SSOT — git “no prompt / no system config” env literals

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:1844-1846`

```
pub fn git_command_at(root: &Path) -> std::process::Command {
    let mut command = std::process::Command::new("git");
    command.arg("-C").arg(root).env("GIT_TERMINAL_PROMPT", "0");
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/repo_mirror.rs:579-585`

```
fn git_command(git: &Path, extra_header: Option<&Zeroizing<String>>) -> std::process::Command {
    let mut command = std::process::Command::new(git);
    command.env_clear();
    command.env("GIT_TERMINAL_PROMPT", "0");
    command.env("GIT_CONFIG_NOSYSTEM", "1");
    command.env("GIT_CONFIG_GLOBAL", "/dev/null");
    command.env("GIT_CONFIG_SYSTEM", "/dev/null");
```

Also `supervisor.rs:1256-1258` (`GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM=1`, `GIT_ATTR_NOSYSTEM=1`). Problem:
`git_command_at` is already the SSOT for checkout git (CLI `probe.rs:205` uses it). Gateway's helper is a different job
(env_clear, pinned git binary, `core.hooksPath=/dev/null`). The shared literals can still drift (`GIT_TERMINAL_PROMPT`
omitted on one path re-enables a blocking prompt). Fix:
`pub const GIT_TERMINAL_PROMPT_OFF: (&str, &str) = ("GIT_TERMINAL_PROMPT", "0");` next to `git_command_at`. Gateway and
supervisor import it. Do not merge the two command builders. Cost/Risk: every git spawn. Leave gateway `env_clear` +
hooksPath alone.

### F9 — LOW — STRUCTURE — `CommandOutput` name collision across crates

Evidence: `packages/cowshed/crates/cowshed-core/src/process.rs:7-11,51-56`

```
pub enum ProcessStatus { Exit(i32), Signal(i32), Unknown }
pub struct CommandOutput { pub status: ProcessStatus, pub stdout: Vec<u8>, pub stderr: Vec<u8> }
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:1093-1104`

```
pub enum CommandStatus { Success, ExitCode(i32), Terminated }
pub struct CommandOutput { pub status: CommandStatus, pub stdout: Vec<u8>, pub stderr: Vec<u8> }
```

Problem: same name, different status algebra. Core preserves signals (`process.rs` comment: “without collapsing signals
into a synthetic exit code”). CLI collapses success vs exit vs terminated and is launchctl-shaped. Not a silent logic
bug today because the types do not meet, but a `use` collision waiting for the first CLI file that talks to core
`CommandOutput`. Fix: rename CLI type to `LaunchctlOutput` / `LaunchctlStatus`. Do not replace it with core
`CommandOutput` unless launchctl signal fidelity is required. Cost/Risk: `launchd.rs` + tests only.

### F10 — LOW — STRUCTURE — unix `verify_peer` copied with divergent error mapping

Evidence: `packages/cowshed/crates/cowshed-core/src/api/capability.rs:887-904`

```
PeerCredentialsError::SocketTypeSizeOverflow
| PeerCredentialsError::SocketTypeQueryFailed
| PeerCredentialsError::NotStream => {
    handshake_error("coordinator descriptor is not a stream socket")
}
```

Evidence: `packages/cowshed/crates/cowshed-core/src/api/server.rs:1089-1107`

```
PeerCredentialsError::SocketTypeSizeOverflow => {
    connection_error("socket type size does not fit socklen_t")
}
PeerCredentialsError::SocketTypeQueryFailed | PeerCredentialsError::NotStream => {
    connection_error("controller descriptor is not a stream socket")
}
```

Problem: same uid check (`peer_uid == geteuid()`). `SocketTypeSizeOverflow` is reported as “not a stream socket” on the
client handshake and as a size error on the server. Live diagnostic divergence, not an auth hole
(`PeerCredentialQueryFailed` still fail-closes). Fix: one `verify_peer(fd, role: Coordinator|Controller)` in
`api/peer_credentials`. Both call sites go through it. Cost/Risk: handshake tests only.

### F11 — LOW — SSOT — `SimVerb` vs `SimGrant` serde already disagrees

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:884-889`

```
pub enum SimVerb {
    #[serde(rename = "openurl")]
    OpenUrl,
    #[serde(rename = "install")]
    Install,
}
```

Evidence: `packages/cowshed/crates/cowshed-gateway/src/sim_broker.rs:29-32`

```
#[serde(rename_all = "kebab-case")]
pub enum SimGrant { OpenUrl, Install }
```

Problem: same two verbs. Grants sidecar serializes `openurl`; gateway config would deserialize `open-url`. No core
caller currently builds `SimProjectConfig` from `GrantSet.sim` (grep: no hits). Latent: the first wiring is a silent
grant miss, not a compile error. Fix: one enum. Owner: gateway `SimGrant` (runtime). Core `GrantSet.sim` uses it via
`pub use`, serde `open-url` or `openurl` picked once and the other spelling deleted. Do not ship both. Cost/Risk: grants
JSON `sim` array if any file already wrote `openurl`.

### F12 — LOW — SSOT — CLI `PRIVATE_DIRECTORY_MODE` defined twice

Evidence: `packages/cowshed/crates/cowshed-cli/src/launchd.rs:18`

```
pub const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/gateway_service.rs:44`

```
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
```

Problem: same mode, two constants in one crate. `launchd` already publishes it. Fix: gateway_service imports
`crate::launchd::PRIVATE_DIRECTORY_MODE`. Delete the local. Cost/Risk: none.

## Cross-slice questions

- `CsCoreMetadata` owns `metadata.rs` `PortBlock` / `EgressMode` / `SimVerb`. F2/F4/F11 want those to re-export gateway
  types; confirm no on-disk JSON change is implied beyond `sim` spelling.
- `CsCoreGit` owns `git.rs`. F8 does not want `git_command_at` merged with gateway `repo_mirror::git_command`; only the
  env literals.
- `XcutRustTs` / napi: TS or napi DTO for port blocks / tokens / `owner/repo` must follow the single owner above, not a
  fourth copy. Not verified here.
- `CsCoreCopy` / `XcutCopiesCowshed`: F5 `host_config.rs:56` path literal vs `STORE_ROOT` is SSOT, not a copy/alloc
  finding.

## Non-findings (checked, clean)

- **Spawn wrappers (assignment e):** not three implementations of one spawn. `exec.rs` is the Seatbelt argv planner +
  CLOEXEC fd hygiene (`plan_exec`, `prepare_child_descriptors`, sync `SystemSpawnRunner` used by `execute()` / tests).
  `runtime/supervisor.rs` `SystemSpawnSink` is the production async spawn: it **calls** `plan_exec` +
  `prepare_child_descriptors` then sets sandbox env and pipes stdio (`supervisor.rs:1177-1314`). `process.rs` does not
  spawn; it is `ProcessStatus`/`CommandOutput` + diagnostic formatting, consumed by `apfs.rs` `SystemCommandRunner`. All
  three survive. Do not delete `exec.rs` in favor of the supervisor.
- **`git_command_at`:** already the checkout-git SSOT; CLI probe uses it (`probe.rs:205`).
- **`control_socket_path`:** derived from `STORE_ROOT` in core (`gateway_sessions.rs:40-44`) specifically to avoid a
  cross-crate copy; CLI `GatewayPaths` calls it (`gateway_service.rs:64`).
- **sccache socket/dir:** owned by `sandbox::sccache_server_socket` / `sccache_cache_directory`; CLI imports them.
- **Atomic private publish:** `fsio::publish_private_file` is the one writer; `metadata::write_atomic_bytes` and
  `host_config::write_private_atomic` wrap it.
- **Baseline mirror routes:** `gateway_sessions::baseline_mirror_routes` uses
  `MirrorProtocol::{local_prefix,baseline_origin}` — not a second origin table.
- **CLI `STORE_ROOT`:** doctor path compares via the core constant (`runtime.rs:2504-2512`), except F5.
- Error enums (`CowshedError` vs `GatewayError` vs `ConfigError`): different domains; gateway errors map at the CLI/core
  boundary. No overlapping variant algebra to collapse.
- Tests-as-evidence and dep-bloat: out of this slice. `cowshed-core → cowshed-gateway` is load-bearing (sessions,
  policy, token type).
