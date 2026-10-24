//! A real, private gateway for the TypeScript RAM-disk fixtures.
//!
//! Build with cargo build -p cowshed-gateway --example ram_fixture, then run
//! ram_fixture <absolute secure-owned-root>. The caller creates that real directory with mode
//! 0700, keeps stdin open, and waits for the single stdout readiness line:
//! {"socket":"<root>/gateway.sock"}. Readiness follows the gateway's successful bind.
//!
//! The caller owns every requested disk and must detach only its recorded registry identities,
//! through this gateway's `ram-detach` of each exact whole disk and entry, before closing stdin.
//! EOF drains the gateway; the caller then removes the fixture root. This executable never
//! attaches a disk on startup or detaches one on shutdown. On Linux it serves the real disk lease
//! without a RAM runner, so a RAM attach receives the gateway's explicit refusal and a RAM detach
//! answers `not-started`. No production cache, credentials, workspace sessions or startup pass is
//! used.

use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};
use std::os::unix::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(target_os = "macos")]
use cowshed_apfs::ram_disk::SystemRamDisks;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_gateway_types::control_socket_path;
use serde::Serialize;

struct NoCredentials;

#[async_trait]
impl CredentialProvider for NoCredentials {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(None)
    }
}

struct NoConnector;

#[async_trait]
impl UpstreamConnector for NoConnector {
    async fn health(&self, _target: &cowshed_gateway::CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Unknown
    }

    async fn connect(
        &self,
        _target: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        Err(ConnectError::NoAddresses)
    }
}

struct DiscardAudit;

#[async_trait]
impl AuditSink for DiscardAudit {
    async fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Ok(())
    }
}

fn fixture_root() -> Result<PathBuf, String> {
    let mut arguments = std::env::args_os().skip(1);
    let (Some(root), None) = (arguments.next(), arguments.next()) else {
        return Err("usage: ram_fixture <absolute secure-owned-root>".to_owned());
    };
    // Strip trailing separators and dots before lstat: neither may hide a symlink leaf.
    let root: PathBuf = Path::new(&root).components().collect();
    if !root.is_absolute() {
        return Err(format!("fixture root {} must be absolute", root.display()));
    }
    let metadata = fs::symlink_metadata(&root)
        .map_err(|error| format!("lstat fixture root {}: {error}", root.display()))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(format!(
            "fixture root {} must be a real, non-symlink directory",
            root.display()
        ));
    }
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if metadata.uid() != uid {
        return Err(format!(
            "fixture root {} belongs to uid {}, not effective uid {uid}",
            root.display(),
            metadata.uid()
        ));
    }
    let mode = metadata.permissions().mode() & 0o7777;
    if mode != 0o700 {
        return Err(format!(
            "fixture root {} is mode {mode:04o}, not 0700",
            root.display()
        ));
    }
    Ok(root)
}

fn fixture_socket(root: &Path) -> Result<PathBuf, String> {
    let socket = root.join("gateway.sock");
    // Resolve only for the production identity check. Bind the caller's original short path;
    // its granted scratch parent may be an alias for a longer workspace runtime directory.
    let resolved = fs::canonicalize(root)
        .map_err(|error| format!("resolve fixture root {}: {error}", root.display()))?
        .join("gateway.sock");
    let production = control_socket_path();
    if socket == production || resolved == production {
        return Err(format!(
            "refusing production gateway socket {}",
            production.display()
        ));
    }
    SocketAddr::from_pathname(&socket)
        .map_err(|error| format!("invalid fixture socket path {}: {error}", socket.display()))?;
    if socket.to_str().is_none() {
        return Err(format!(
            "fixture socket {} must be UTF-8 for JSON readiness",
            socket.display()
        ));
    }
    match fs::symlink_metadata(&socket) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(socket),
        Err(error) => Err(format!(
            "lstat fixture socket {}: {error}",
            socket.display()
        )),
        Ok(_) => Err(format!(
            "fixture socket {} already exists; refusing to replace it",
            socket.display()
        )),
    }
}

fn ready(socket: &Path) -> Result<(), String> {
    #[derive(Serialize)]
    struct Readiness<'a> {
        socket: &'a Path,
    }

    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &Readiness { socket })
        .map_err(|error| format!("write fixture readiness: {error}"))?;
    stdout
        .write_all(b"\n")
        .and_then(|()| stdout.flush())
        .map_err(|error| format!("flush fixture readiness: {error}"))
}

async fn run() -> Result<(), String> {
    let root = fixture_root()?;
    let socket = fixture_socket(&root)?;
    let cache = root.join("cache");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&cache)
        .map_err(|error| format!("create private fixture cache {}: {error}", cache.display()))?;
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            #[cfg(target_os = "macos")]
            ram_disks: Some(Arc::new(SystemRamDisks::new(socket.clone()))),
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .map_err(|error| format!("start fixture gateway at {}: {error}", socket.display()))?;

    let lifetime = async {
        ready(&socket)?;
        tokio::io::copy(&mut tokio::io::stdin(), &mut tokio::io::sink())
            .await
            .map(|_| ())
            .map_err(|error| format!("read fixture stdin until EOF: {error}"))
    }
    .await;
    let drain = gateway
        .drain()
        .await
        .map_err(|error| format!("drain fixture gateway at {}: {error}", socket.display()));
    match (lifetime, drain) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(lifetime), Err(drain)) => Err(format!("{lifetime}; {drain}")),
    }
}

fn main() -> ExitCode {
    let result = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(run()),
        Err(error) => Err(format!("create fixture runtime: {error}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            cowshed_os::eprintln!("ram_fixture: {error}");
            ExitCode::FAILURE
        }
    }
}
