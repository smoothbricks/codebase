//! A real gateway on a scratch control socket, for the tests that drive land turns through the
//! `cowshed` binary (05_gateway.md "Land turns"): it runs on a runtime of its own, so its queues
//! are served while the test thread waits on a child, and it drains when dropped. A test crate that
//! uses it declares `temp_parent` and `temp_root` (`cowshed-core/tests/support`) at its root.

use std::fs;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use cowshed_core::api::dto::LandTurnQueue;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    GatewayControlClient, MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};

use crate::temp_parent;
use crate::temp_root::TempRoot;

/// The credential store, which these tests never reach.
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

/// Upstream connections, which these tests never make.
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

/// Audit events, which no test here reads.
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

/// One test's gateway: `--gateway-socket` names `socket` to the binary under test.
pub struct TestGateway {
    pub socket: PathBuf,
    runtime: tokio::runtime::Runtime,
    gateway: Option<Gateway>,
    /// Declared last: the socket's directory goes after the runtime that serves the socket.
    _root: TempRoot,
}

impl TestGateway {
    pub fn start() -> Self {
        // The socket's root sits under the scratch parent, short enough for a bind path.
        let root = TempRoot::new_in(&temp_parent::scratch_parent(), "csturn");
        let cache = root.join("cache");
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&cache)
            .expect("create the gateway's mirror cache");
        let socket = root.join("gateway.sock");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("start the test runtime");
        let gateway = runtime
            .block_on(Gateway::start(
                GatewayConfig {
                    control_socket: Some(socket.clone()),
                    mirror_cache: MirrorCacheConfig::new(cache),
                    ..GatewayConfig::default()
                },
                Arc::new(NoCredentials),
                Arc::new(NoConnector),
                Arc::new(DiscardAudit),
            ))
            .expect("start the test gateway");
        Self {
            socket,
            runtime,
            gateway: Some(gateway),
            _root: root,
        }
    }

    /// The gateway's land-turn queues, read through its `land-turns` op: every queue with a live
    /// ticket, and every queue with an abandonment at or after `abandoned_since_unix_ms`.
    pub fn land_turns(&self, abandoned_since_unix_ms: u64) -> Vec<LandTurnQueue> {
        let control =
            GatewayControlClient::new(self.socket.clone()).expect("an absolute socket path");
        self.runtime
            .block_on(control.land_turns(abandoned_since_unix_ms))
            .expect("read the gateway's land turns")
    }
}

impl Drop for TestGateway {
    fn drop(&mut self) {
        // The gateway drains on the runtime that serves it. The fields then drop in declaration
        // order: the runtime, then the socket's directory.
        if let Some(gateway) = self.gateway.take() {
            let drained = self.runtime.block_on(gateway.drain());
            if !thread::panicking() {
                drained.expect("drain the test gateway");
            }
        }
    }
}
