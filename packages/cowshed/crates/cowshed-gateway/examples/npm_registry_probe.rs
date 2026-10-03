//! Runs one real [`Gateway`] against the public npm registry so real package managers can be
//! pointed at it and measured: the production upstream connector, the policy `cowshed-core`
//! installs for a workspace granted `registry.npmjs.org` (one intercept grant, no mirror route),
//! and a CA minted the way workspaces mint theirs. Clients keep the public registry and reach it
//! through the gateway's proxy, trusting the workspace CA.
//!
//! ```text
//! cargo run --release -p cowshed-gateway --example npm_registry_probe -- <state-dir>
//! ```
//!
//! `<state-dir>` is created private (0700) and receives:
//!
//! - `cache/`        the mirror cache root; reuse the directory for warm-cache runs.
//! - `ca.pem`        the workspace CA, for `NODE_EXTRA_CA_CERTS`.
//! - `ready.json`    `{ pid, endpoint, gateway_http, token, ca, registry_proxy }`, written once
//!   the session is live; clients use `HTTPS_PROXY=<registry_proxy>`.
//! - `audit.jsonl`   one gateway audit event per line: kind, method, host, path, status, bytes,
//!   mirror cache status. This is the record of which requests reached the gateway, and how.
//! - `summary.json`  `{ max_rss_bytes }`, written after SIGINT/SIGTERM drains the gateway.
//!
//! The runtime is current-thread, as the host daemon's is.

#[cfg(target_os = "macos")]
fn main() -> std::process::ExitCode {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");
    match runtime.block_on(probe::run()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("npm_registry_probe: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn main() -> std::process::ExitCode {
    eprintln!("npm_registry_probe: only the macOS loopback port-block endpoint is wired");
    std::process::ExitCode::FAILURE
}

#[cfg(target_os = "macos")]
mod probe {
    use std::{
        fs::{self, File, OpenOptions},
        io::{Read as _, Write as _},
        net::{Ipv4Addr, SocketAddr, TcpListener},
        os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use cowshed_gateway::{
        AuditError, AuditEvent, AuditSink, CredentialError, CredentialProvider, CredentialQuery,
        CredentialRecord, EgressGrant, Gateway, GatewayConfig, MACOS_PORT_MAX, MACOS_PORT_MIN,
        MirrorCacheConfig, NEW_PORT_BLOCK_SIZE, SystemConnector, TOKEN_BYTES, WorkspaceCa,
        WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
    };
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair,
        KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
    };
    use tokio::signal::unix::{SignalKind, signal};

    const REGISTRY_HOST: &str = "registry.npmjs.org";

    /// The probe holds no secrets: every upstream it reaches is public.
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

    /// One JSON line per audit event, written whole so a reader never sees half an event.
    struct JsonLinesAudit {
        file: Mutex<File>,
    }

    #[async_trait]
    impl AuditSink for JsonLinesAudit {
        async fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
            let mut line =
                serde_json::to_vec(&event).map_err(|error| AuditError(error.to_string()))?;
            line.push(b'\n');
            self.file
                .lock()
                .map_err(|_| AuditError("audit log lock poisoned".to_owned()))?
                .write_all(&line)
                .map_err(|error| AuditError(error.to_string()))
        }

        async fn flush(&self) -> Result<(), AuditError> {
            self.file
                .lock()
                .map_err(|_| AuditError("audit log lock poisoned".to_owned()))?
                .sync_data()
                .map_err(|error| AuditError(error.to_string()))
        }
    }

    fn state_dir() -> Result<PathBuf, String> {
        let mut arguments = std::env::args_os().skip(1);
        let (Some(state), None) = (arguments.next(), arguments.next()) else {
            return Err("usage: npm_registry_probe <state-dir>".to_owned());
        };
        let state = PathBuf::from(state);
        std::path::absolute(&state).map_err(|error| format!("resolve {}: {error}", state.display()))
    }

    fn private_dir(path: &Path) -> Result<(), String> {
        fs::create_dir_all(path).map_err(|error| format!("create {}: {error}", path.display()))?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("restrict {}: {error}", path.display()))
    }

    fn private_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .and_then(|mut file| file.write_all(bytes))
            .map_err(|error| format!("write {}: {error}", path.display()))
    }

    /// The CA parameters `cowshed-core` mints a workspace CA with.
    fn workspace_ca() -> Result<(WorkspaceCa, String), String> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|error| format!("generate CA key: {error}"))?;
        let mut params = CertificateParams::new(Vec::<String>::new())
            .map_err(|error| format!("CA parameters: {error}"))?;
        let mut name = DistinguishedName::new();
        name.push(DnType::OrganizationName, "cowshed");
        name.push(DnType::CommonName, "cowshed npm registry probe");
        params.distinguished_name = name;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let certificate = params
            .self_signed(&key)
            .map_err(|error| format!("self-sign CA: {error}"))?;
        let pem = certificate.pem();
        let ca = WorkspaceCa::new(pem.clone(), key.serialize_pem())
            .map_err(|error| format!("workspace CA: {error}"))?;
        Ok((ca, pem))
    }

    fn workspace_token() -> Result<WorkspaceToken, String> {
        let mut bytes = [0_u8; TOKEN_BYTES];
        File::open("/dev/urandom")
            .and_then(|mut random| random.read_exact(&mut bytes))
            .map_err(|error| format!("read /dev/urandom: {error}"))?;
        Ok(WorkspaceToken::from_bytes(bytes))
    }

    /// The base of the first port block nothing else is listening on.
    fn free_block_base() -> Result<SocketAddr, String> {
        (MACOS_PORT_MIN..=MACOS_PORT_MAX)
            .step_by(usize::from(NEW_PORT_BLOCK_SIZE))
            .map(|port| SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
            .find(|address| TcpListener::bind(address).is_ok())
            .ok_or_else(|| "no free macOS port block".to_owned())
    }

    fn max_rss_bytes() -> i64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: `getrusage` fills the struct it is handed; RUSAGE_SELF is always valid.
        let usage = unsafe {
            libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
            usage.assume_init()
        };
        // macOS reports `ru_maxrss` in bytes.
        usage.ru_maxrss
    }

    pub async fn run() -> Result<(), String> {
        let state = state_dir()?;
        private_dir(&state)?;
        let cache_root = state.join("cache");
        private_dir(&cache_root)?;
        let audit_path = state.join("audit.jsonl");
        let audit = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&audit_path)
            .map_err(|error| format!("open {}: {error}", audit_path.display()))?;

        let config = GatewayConfig {
            mirror_cache: MirrorCacheConfig::new(cache_root),
            ..GatewayConfig::default()
        };
        let connector =
            SystemConnector::new(config.timeouts.connect, config.timeouts.tls_handshake)
                .map_err(|error| format!("upstream connector: {error}"))?;
        let gateway = Gateway::start(
            config,
            Arc::new(NoCredentials),
            Arc::new(connector),
            Arc::new(JsonLinesAudit {
                file: Mutex::new(audit),
            }),
        )
        .await
        .map_err(|error| format!("start gateway: {error}"))?;

        let policy = WorkspacePolicy {
            grants: vec![
                EgressGrant::intercept(REGISTRY_HOST, 443)
                    .map_err(|error| format!("registry grant: {error}"))?,
            ],
            mirrors: Vec::new(),
        };
        let (ca, ca_pem) = workspace_ca()?;
        let ca_path = state.join("ca.pem");
        private_file(&ca_path, ca_pem.as_bytes())?;
        let token = workspace_token()?;
        let encoded = token.encode();
        let address = free_block_base()?;
        gateway
            .handle()
            .install(WorkspaceSession {
                workspace_id: "npm-registry-probe".to_owned(),
                repo_id: "probe/npm-registry".to_owned(),
                revision: 1,
                endpoint: WorkspaceEndpoint::Tcp {
                    address,
                    block_size: NEW_PORT_BLOCK_SIZE,
                },
                token,
                ca,
                policy,
            })
            .await
            .map_err(|error| format!("install session: {error}"))?;

        let ready = serde_json::json!({
            "pid": std::process::id(),
            "endpoint": address.to_string(),
            "gateway_http": format!("http://{address}"),
            "token": encoded,
            "ca": ca_path,
            "registry_proxy": format!("http://cowshed:{encoded}@{address}"),
        });
        private_file(&state.join("ready.json"), ready.to_string().as_bytes())?;
        println!("{ready}");

        let mut terminate =
            signal(SignalKind::terminate()).map_err(|error| format!("SIGTERM: {error}"))?;
        let mut interrupt =
            signal(SignalKind::interrupt()).map_err(|error| format!("SIGINT: {error}"))?;
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
        tokio::time::timeout(Duration::from_secs(30), gateway.drain())
            .await
            .map_err(|_| "drain timed out".to_owned())?
            .map_err(|error| format!("drain gateway: {error}"))?;
        let summary = serde_json::json!({ "max_rss_bytes": max_rss_bytes() });
        private_file(&state.join("summary.json"), summary.to_string().as_bytes())?;
        println!("{summary}");
        Ok(())
    }
}
