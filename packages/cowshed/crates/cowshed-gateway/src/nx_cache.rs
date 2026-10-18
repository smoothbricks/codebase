//! Accepted-project Nx cache authority (05_gateway.md). Backend credentials never reach a
//! workspace. Publication is an ephemeral in-process capability, never a standing session grant.

use std::{collections::BTreeSet, net::SocketAddr};

use cowshed_gateway_types::{CanonicalHost, CanonicalTarget, TargetScheme, WorkspaceToken};

/// One controller-owned project backend, using Nx's unchanged GET/PUT protocol.
#[derive(Clone, Debug)]
pub struct NxCacheBackend {
    pub repo_id: String,
    pub address: SocketAddr,
    pub read_token: WorkspaceToken,
    pub write_token: WorkspaceToken,
    /// Staging files reclaimed by startup recovery: an observation, never authority.
    pub reclaimed_staging_files: u64,
}

impl NxCacheBackend {
    pub fn validate(&self) -> Result<(), &'static str> {
        cowshed_gateway_types::validate_repo_id(&self.repo_id)
            .map_err(|_| "Nx backend project identity is invalid")?;
        if !self.address.ip().is_loopback() || self.address.port() == 0 {
            return Err("Nx backend must have a nonzero loopback address");
        }
        if self.read_token.matches(&self.write_token) {
            return Err("Nx backend read and write credentials must differ");
        }
        Ok(())
    }

    pub(crate) fn target(&self) -> CanonicalTarget {
        CanonicalTarget {
            scheme: TargetScheme::Http,
            host: CanonicalHost::Ip(self.address.ip()),
            port: self.address.port(),
        }
    }
}

/// A hash-scoped capability installed by an in-process caller. The qualified production land
/// caller is deferred; this actor API does not itself verify a land plan or protected receipt.
#[derive(Clone, Debug)]
pub struct NxPublication {
    pub workspace_id: String,
    pub repo_id: String,
    pub expected_revision: u64,
    pub accepted_head: String,
    pub hashes: BTreeSet<String>,
    pub token: WorkspaceToken,
}

impl NxPublication {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        cowshed_gateway_types::validate_identifier("workspace_id", &self.workspace_id)
            .map_err(|_| "Nx publication workspace identity is invalid")?;
        cowshed_gateway_types::validate_repo_id(&self.repo_id)
            .map_err(|_| "Nx publication project identity is invalid")?;
        if self.accepted_head.len() != 40
            || !self
                .accepted_head
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("Nx publication must name the exact accepted source head");
        }
        if self.hashes.is_empty() || self.hashes.iter().any(|hash| !is_task_hash(hash)) {
            return Err("Nx publication must name successful current task hashes");
        }
        Ok(())
    }
}

/// One validated Nx task key (05_gateway.md): 1-128 characters of `[A-Za-z0-9_-]`, the backend's
/// own key charset. Nx sends a task's hash verbatim, stock decimal or a custom hasher's value.
pub(crate) fn is_task_hash(hash: &str) -> bool {
    (1..=128).contains(&hash.len())
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// Recognize only stock Nx's exact route; no query, escaping, or path suffix can acquire it.
pub(crate) fn route_hash(path: &str) -> Option<&str> {
    path.strip_prefix("/nx/v1/cache/")
        .filter(|hash| is_task_hash(hash))
}

/// The status an unavailable Nx backend answers with, before any response head: a read is a miss
/// and a write is refused. Nothing is staged or sent to a backend that cannot take it.
pub(crate) fn unavailable_status(method: &http::Method) -> http::StatusCode {
    if *method == http::Method::GET {
        http::StatusCode::NOT_FOUND
    } else {
        http::StatusCode::FORBIDDEN
    }
}

/// Whether stock Nx 23.2.1 reads `status` for `method` without failing the task
/// (`native/cache/http_remote_cache.rs`): `GET` 200/404, `PUT` 200/403/409. Every other status is
/// its fatal "misconfigured remote cache", so the gateway never forwards one.
pub(crate) fn client_reads(method: &http::Method, status: http::StatusCode) -> bool {
    if *method == http::Method::GET {
        matches!(status, http::StatusCode::OK | http::StatusCode::NOT_FOUND)
    } else {
        matches!(
            status,
            http::StatusCode::OK | http::StatusCode::FORBIDDEN | http::StatusCode::CONFLICT
        )
    }
}

/// The installed server bundle, the `PATH` its `bun` comes from, and a host-private artifact root.
/// A backend is started only when an Nx client first asks for its project, not for every non-Nx
/// workspace in inventory.
///
/// The server is JavaScript (`@smoothbricks/nx-cache-server`'s `dist/server` bundle) run by the
/// `bun` on `path`: cowshed carries no interpreter of its own (tg#7504). `cowshed setup` puts the
/// directory of the `bun` it found on the gateway agent's `PATH`, a nix store path included.
#[derive(Clone, Debug)]
pub struct NxCacheServiceConfig {
    pub script: std::path::PathBuf,
    pub path: std::ffi::OsString,
    pub directory: std::path::PathBuf,
    pub max_bytes: u64,
    pub cap_bytes: u64,
}

/// The first `bun` on `path`: an executable regular file (through links) in an absolute
/// directory. Relative entries are skipped, as a service's working directory names nothing.
pub fn bun_on_path(path: &std::ffi::OsStr) -> Option<std::path::PathBuf> {
    use std::os::unix::fs::PermissionsExt as _;
    std::env::split_paths(path)
        .filter(|directory| directory.is_absolute())
        .map(|directory| directory.join("bun"))
        .find(|candidate| {
            candidate.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Startup {
    Ready(ReadyStartup),
    Failed(FailedStartup),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyStartup {
    kind: ReadyKind,
    origin: String,
    port: u16,
    #[serde(rename = "reclaimedStagingFiles")]
    reclaimed_staging_files: u64,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum ReadyKind {
    Ready,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum FailedKind {
    Failed,
}

/// Every refusal the server can write, each with exactly the fields its stage emits. A frame
/// with a stage outside this set, or fields its stage does not carry, is a protocol error.
#[derive(serde::Deserialize)]
#[serde(tag = "stage", deny_unknown_fields)]
enum FailedStartup {
    #[serde(rename = "store-locked")]
    StoreLocked {
        kind: FailedKind,
        ok: bool,
        directory: String,
        path: std::path::PathBuf,
        reason: String,
    },
    #[serde(rename = "store")]
    Store {
        kind: FailedKind,
        ok: bool,
        directory: String,
        reason: String,
    },
    #[serde(rename = "gc")]
    Gc {
        kind: FailedKind,
        ok: bool,
        directory: String,
        reason: String,
    },
    #[serde(rename = "listen")]
    Listen {
        kind: FailedKind,
        ok: bool,
        directory: String,
        reason: String,
    },
    #[serde(rename = "config")]
    Config {
        kind: FailedKind,
        problems: Vec<String>,
    },
}

impl FailedStartup {
    /// The refusal message, or a protocol error when a refused startup claims `ok`.
    fn refusal(self, repo_id: &str) -> Result<String, String> {
        let refused = |stage: &str, ok: bool, detail: String| -> Result<String, String> {
            if ok {
                return Err(format!(
                    "invalid Nx cache backend {repo_id} readiness: a refused startup must report ok=false"
                ));
            }
            Ok(format!(
                "Nx cache backend {repo_id} startup refused at stage {stage}: {detail}"
            ))
        };
        match self {
            Self::StoreLocked {
                kind: FailedKind::Failed,
                ok,
                directory,
                path,
                reason,
            } => refused(
                "store-locked",
                ok,
                format!(
                    "the store at {} is locked (directory {directory}); diagnostic: {reason}",
                    path.display()
                ),
            ),
            Self::Store {
                kind: FailedKind::Failed,
                ok,
                directory,
                reason,
            } => refused(
                "store",
                ok,
                format!("directory {directory}; diagnostic: {reason}"),
            ),
            Self::Gc {
                kind: FailedKind::Failed,
                ok,
                directory,
                reason,
            } => refused(
                "gc",
                ok,
                format!("directory {directory}; diagnostic: {reason}"),
            ),
            Self::Listen {
                kind: FailedKind::Failed,
                ok,
                directory,
                reason,
            } => refused(
                "listen",
                ok,
                format!("directory {directory}; diagnostic: {reason}"),
            ),
            Self::Config {
                kind: FailedKind::Failed,
                problems,
            } => Ok(format!(
                "Nx cache backend {repo_id} startup refused at stage config: {}",
                problems.join("; ")
            )),
        }
    }
}

pub(crate) async fn start_backend(
    config: &NxCacheServiceConfig,
    repo_id: &str,
) -> Result<(NxCacheBackend, tokio::process::Child), String> {
    use cowshed_os::fork_lock::Spawn as _;
    use sha2::{Digest as _, Sha256};
    use std::{os::unix::fs::DirBuilderExt as _, process::Stdio};
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};

    cowshed_gateway_types::validate_repo_id(repo_id)
        .map_err(|error| format!("Nx cache project {repo_id}: {error}"))?;
    if !config.script.is_absolute()
        || !config.directory.is_absolute()
        || config.max_bytes == 0
        || config.max_bytes > config.cap_bytes
    {
        return Err(
            "Nx cache service requires installed absolute paths and a bounded artifact cap"
                .to_owned(),
        );
    }
    // A missing interpreter is a setup fault, said as one: nothing beside cowshed stands in.
    let bun = bun_on_path(&config.path).ok_or_else(|| {
        format!(
            "Nx cache backend {repo_id}: no bun on the gateway's PATH {}; run `cowshed setup` from a shell whose PATH has bun (devenv provides it)",
            std::path::Path::new(&config.path).display()
        )
    })?;
    let project = config
        .directory
        .join(format!("{:x}", Sha256::digest(repo_id.as_bytes())));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&project)
        .map_err(|error| format!("create project Nx cache {}: {error}", project.display()))?;
    for path in [&config.directory, &project] {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|error| format!("inspect Nx cache {}: {error}", path.display()))?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Err(format!(
                "Nx cache {} must be a host-owned private directory",
                path.display()
            ));
        }
    }
    let read_token = mint_token()?;
    let write_token = mint_token()?;
    let mut child = tokio::process::Command::new(&bun)
        .arg(&config.script)
        .arg("--exit-on-stdin-eof")
        .env_clear()
        .env("NX_CACHE_SERVER_DIR", project.join("entries"))
        .env("NX_CACHE_SERVER_HOST", "127.0.0.1")
        .env("NX_CACHE_SERVER_PORT", "0")
        .env("NX_CACHE_SERVER_MAX_BYTES", config.max_bytes.to_string())
        .env("NX_CACHE_SERVER_CAP_BYTES", config.cap_bytes.to_string())
        .env(
            "NX_CACHE_SERVER_READ_TOKENS",
            format!("workspace:{}", read_token.encode()),
        )
        .env(
            "NX_CACHE_SERVER_WRITE_TOKENS",
            format!("accepted:{}", write_token.encode()),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn_locked()
        .map_err(|error| {
            format!(
                "start Nx cache {} {}: {error}",
                bun.display(),
                config.script.display()
            )
        })?;
    let stdout = child
        .stdout
        .take()
        .ok_or("Nx cache server did not open its readiness channel")?;
    let mut ready = Vec::new();
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        BufReader::new(stdout.take(16_385)).read_until(b'\n', &mut ready),
    )
    .await
    .map_err(|_| format!("Nx cache backend {repo_id} did not become ready in 30s"))?
    .map_err(|error| format!("read Nx cache backend {repo_id} readiness: {error}"))?;
    if read == 0 || read > 16_384 || ready.last() != Some(&b'\n') {
        return Err(format!(
            "Nx cache backend {repo_id} supplied no bounded readiness frame"
        ));
    }
    let startup: Startup = serde_json::from_slice(&ready)
        .map_err(|error| format!("invalid Nx cache backend {repo_id} readiness: {error}"))?;
    match startup {
        Startup::Ready(ReadyStartup {
            kind: ReadyKind::Ready,
            origin,
            port,
            reclaimed_staging_files,
        }) => {
            if port == 0 || origin != format!("http://127.0.0.1:{port}") {
                return Err(format!(
                    "Nx cache backend {repo_id} announced an invalid origin {origin}"
                ));
            }
            let backend = NxCacheBackend {
                repo_id: repo_id.to_owned(),
                address: SocketAddr::from(([127, 0, 0, 1], port)),
                read_token,
                write_token,
                reclaimed_staging_files,
            };
            backend.validate().map_err(str::to_owned)?;
            Ok((backend, child))
        }
        Startup::Failed(failed) => Err(failed.refusal(repo_id)?),
    }
}

pub(crate) fn mint_token() -> Result<WorkspaceToken, String> {
    let mut bytes = zeroize::Zeroizing::new([0_u8; 32]);
    getrandom::fill(&mut bytes[..]).map_err(|error| format!("mint Nx cache authority: {error}"))?;
    Ok(WorkspaceToken::from_bytes(*bytes))
}

#[cfg(test)]
mod startup_wire_tests {
    use super::Startup;

    fn parse(json: &str) -> Result<Startup, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn exact_ready_frame_is_accepted() {
        let frame = r#"{"kind":"ready","origin":"http://127.0.0.1:4000","port":4000,"reclaimedStagingFiles":2}"#;
        assert!(matches!(parse(frame), Ok(Startup::Ready(_))));
    }

    #[test]
    fn exact_failure_frames_are_accepted() {
        for (label, frame) in [
            (
                "exact_store_locked_frame_is_accepted",
                r#"{"kind":"failed","ok":false,"stage":"store-locked","directory":"/d","path":"/d/.owner.lock","reason":"locked"}"#,
            ),
            (
                "exact_config_frame_is_accepted_without_ok",
                r#"{"kind":"failed","stage":"config","problems":["executable is relative"]}"#,
            ),
        ] {
            assert!(matches!(parse(frame), Ok(Startup::Failed(_))), "{label}");
        }
    }

    #[test]
    fn malformed_startup_frames_are_refused() {
        for (label, frame) in [
            (
                "store_locked_without_path_is_refused",
                r#"{"kind":"failed","ok":false,"stage":"store-locked","directory":"/d","reason":"locked"}"#,
            ),
            (
                "operation_failure_with_path_is_refused",
                r#"{"kind":"failed","ok":false,"stage":"store","directory":"/d","path":"/d/x","reason":"io"}"#,
            ),
            (
                "unknown_stage_is_refused",
                r#"{"kind":"failed","ok":false,"stage":"bogus","directory":"/d","reason":"io"}"#,
            ),
            (
                "unknown_kind_is_refused",
                r#"{"kind":"stopped","origin":"http://127.0.0.1:4000","port":4000,"reclaimedStagingFiles":0}"#,
            ),
            (
                "extra_field_is_refused",
                r#"{"kind":"ready","origin":"http://127.0.0.1:4000","port":4000,"reclaimedStagingFiles":0,"extra":1}"#,
            ),
        ] {
            assert!(parse(frame).is_err(), "{label}");
        }
    }
}
