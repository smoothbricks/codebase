//! Independent falsification of the control-socket parent check.
//!
//! The production check in `ControlRuntime::start` must keep refusing a group-writable
//! parent and a parent that is a symlink. These cases are constructed here rather than
//! inferred from the happy-path fixture.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;
#[cfg(target_os = "linux")]
#[path = "control_parent_probe/linux.rs"]
mod linux;

use std::{
    io,
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, CanonicalTarget, ConnectError,
    CredentialError, CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    GatewayError, GatewayTimeouts, MirrorCacheConfig, NegotiatedTransport, UpstreamConnection,
    UpstreamConnector, UpstreamHealth, UpstreamPurpose,
};
use fixture_dir::{FixtureDir, secure_fixture_dir};
use tokio::net::TcpStream;

#[derive(Debug)]
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

#[derive(Debug)]
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

struct LocalConnector;

#[async_trait]
impl UpstreamConnector for LocalConnector {
    async fn health(&self, _target: &CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Healthy
    }

    async fn connect(&self, target: &AuthorizedTarget) -> Result<UpstreamConnection, ConnectError> {
        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, target.target.port))
            .await
            .map_err(ConnectError::Io)?;
        let transport = match target.purpose {
            UpstreamPurpose::OpaqueTcp => NegotiatedTransport::Raw,
            UpstreamPurpose::PlainHttp | UpstreamPurpose::TlsHttp => NegotiatedTransport::Http1,
        };
        Ok(UpstreamConnection {
            io: Box::new(stream),
            transport,
        })
    }
}

/// The probe's config, and the guard owning the mirror-cache root it names.
///
/// `Gateway::start` creates `repo/` under that root before the control-socket parent check
/// refuses it, so the root exists after every probe; the guard removes it however the test ends.
fn probe_config() -> (GatewayConfig, FixtureDir) {
    static NEXT_CACHE: AtomicUsize = AtomicUsize::new(0);
    let cache = secure_fixture_dir(&format!(
        "cowshed-gateway-probe-cache-{}-{}",
        std::process::id(),
        NEXT_CACHE.fetch_add(1, Ordering::Relaxed)
    ));
    let config = GatewayConfig {
        timeouts: GatewayTimeouts {
            request_headers: Duration::from_secs(2),
            connect: Duration::from_secs(1),
            tls_handshake: Duration::from_secs(2),
            response_headers: Duration::from_secs(2),
            body_idle: Duration::from_secs(2),
            request_total: Duration::from_secs(5),
            tunnel_total: Duration::from_secs(5),
            leaf_lifetime: Duration::from_secs(60 * 60),
        },
        mirror_cache: MirrorCacheConfig::new(cache.path().to_path_buf()),
        ..GatewayConfig::default()
    };
    #[cfg(target_os = "linux")]
    let config = GatewayConfig {
        data_socket_root: Some(linux::socket_root()),
        ..config
    };
    (config, cache)
}

fn secure_root(tag: &str) -> FixtureDir {
    secure_fixture_dir(&format!("cowshed-ctl-{}-{}", tag, std::process::id()))
}

async fn start_error(control: &std::path::Path) -> GatewayError {
    let (mut config, _cache) = probe_config();
    config.control_socket = Some(control.to_path_buf());
    let Err(error) = Gateway::start(
        config,
        Arc::new(NoCredentials),
        Arc::new(LocalConnector),
        Arc::new(DiscardAudit),
    )
    .await
    else {
        panic!("insecure control socket parent must refuse Gateway::start");
    };
    error
}

fn io_refusal(error: GatewayError) -> io::Error {
    let GatewayError::Io(error) = error else {
        panic!("expected an I/O refusal, got {error:?}");
    };
    error
}

#[tokio::test]
async fn group_writable_parent_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = secure_root("gw");
    let shared = root.path().join("shared");
    std::fs::create_dir(&shared).expect("create group-writable parent");
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o770))
        .expect("relax group-writable parent");
    let control = shared.join("gateway.sock");
    let error = io_refusal(start_error(&control).await);
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "control socket parent must be an owned, non-writable real directory"
    );
    assert!(
        !control.exists(),
        "refusal must happen before bind: {}",
        control.display()
    );
}

#[tokio::test]
async fn symlink_parent_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = secure_root("ln");
    let target = root.path().join("real");
    std::fs::create_dir(&target).expect("create symlink target");
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700))
        .expect("secure symlink target");
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&target, &link).expect("link a private parent");
    let control = link.join("gateway.sock");
    let error = io_refusal(start_error(&control).await);
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "control socket parent must be an owned, non-writable real directory"
    );
    assert!(
        !control.exists(),
        "refusal must happen before bind: {}",
        control.display()
    );
    assert!(
        !target.join("gateway.sock").exists(),
        "the refusal must not bind through the link either"
    );
}
