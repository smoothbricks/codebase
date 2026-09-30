use super::*;
use cowshed_cli::gateway_service::{ControlSocket, reconcile_project};
use cowshed_core::gateway_sessions::{SessionInventory, policy_from_grants, stable_workspace_id};
use cowshed_core::workspace_environment::PORT_BASE_ENV;
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, AuthorizedTarget, CanonicalTarget, ConnectError,
    CredentialError, CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    GatewayError, MACOS_PORT_BLOCK_SIZE, MACOS_PORT_MAX, MACOS_PORT_MIN, MirrorCacheConfig,
    NegotiatedTransport, UpstreamConnection, UpstreamConnector, UpstreamHealth, UpstreamPurpose,
    WorkspaceCa, WorkspaceEndpoint, WorkspaceSession, WorkspaceToken,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::task::JoinHandle;

const UPSTREAM_HOST: &str = "registry.example.test";
const FETCH_CHECK: &str = "curl --noproxy '' --proxy \"$HTTP_PROXY\" --fail --silent --show-error --max-time 5 http://registry.example.test/ready";

struct NoCredentials;

#[async_trait]
impl CredentialProvider for NoCredentials {
    async fn lookup(
        &self,
        _: &CredentialQuery,
    ) -> std::result::Result<Option<CredentialRecord>, CredentialError> {
        Ok(None)
    }
}

struct LocalUpstream(PathBuf);

#[async_trait]
impl UpstreamConnector for LocalUpstream {
    async fn health(&self, _: &CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Healthy
    }

    async fn connect(
        &self,
        target: &AuthorizedTarget,
    ) -> std::result::Result<UpstreamConnection, ConnectError> {
        assert!(matches!(
            &target.target.host,
            cowshed_gateway::CanonicalHost::Dns(host) if host == UPSTREAM_HOST
        ));
        assert_eq!(target.purpose, UpstreamPurpose::PlainHttp);
        let stream = UnixStream::connect(&self.0)
            .await
            .map_err(ConnectError::Io)?;
        Ok(UpstreamConnection {
            io: Box::new(stream),
            transport: NegotiatedTransport::Http1,
        })
    }
}

// The dispatch seam substitutes only host storage: policy reconciliation, the gateway, the
// acceptance child, and git's target ref are real. No host daemon or adopted checkout is touched.
pub(super) struct Fixture {
    root: PathBuf,
    parent: PathBuf,
    workspace: PathBuf,
    control: ControlSocket,
    gateway: Option<Gateway>,
    upstream: JoinHandle<()>,
    repo: RepoId,
    identity: String,
    endpoint: SocketAddr,
    token: WorkspaceToken,
    ca: WorkspaceCa,
    grants: GrantSet,
    check_stdout: Vec<u8>,
}

impl Fixture {
    async fn new(gateway_port: Option<u16>) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let socket_root = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(format!("cs-land-{}-{nonce}", std::process::id()));
        fs::create_dir(&socket_root).unwrap();
        fs::set_permissions(&socket_root, fs::Permissions::from_mode(0o700)).unwrap();
        let root = socket_root.canonicalize().unwrap();
        let parent = root.join("parent");
        let workspace = root.join("topic");
        fs::create_dir(&parent).unwrap();
        git(&parent, ["init", "-q", "-b", "main"]).await;
        fs::write(parent.join("base.txt"), "base\n").unwrap();
        git(&parent, ["add", "base.txt"]).await;
        git(&parent, ["commit", "-q", "-m", "base"]).await;
        git(&root, ["clone", "-q", "--no-hardlinks", "parent", "topic"]).await;
        fs::write(workspace.join("feature.txt"), "feature\n").unwrap();
        git(&workspace, ["add", "feature.txt"]).await;
        git(&workspace, ["commit", "-q", "-m", "feature"]).await;

        let upstream_address = socket_root.join("upstream.sock");
        let listener = UnixListener::bind(&upstream_address).unwrap();
        let upstream = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let length = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(length, 0, "upstream request ended before its headers");
                    request.extend_from_slice(&buffer[..length]);
                    assert!(
                        request.len() <= 16 * 1024,
                        "upstream request headers exceed fixture bound"
                    );
                }
                assert!(request.starts_with(b"GET /ready HTTP/1.1\r\n"));
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ngranted\n")
                    .await
                    .unwrap();
            }
        });
        let cache = root.join("cache");
        fs::create_dir(&cache).unwrap();
        fs::set_permissions(&cache, fs::Permissions::from_mode(0o700)).unwrap();
        let control_path = socket_root.join("gateway.sock");
        let audit_root = root.join("audit");
        fs::create_dir(&audit_root).unwrap();
        fs::set_permissions(&audit_root, fs::Permissions::from_mode(0o700)).unwrap();
        let audit = ArrowAuditSink::start(ArrowAuditConfig::new(audit_root).unwrap()).unwrap();
        let gateway = Gateway::start(
            GatewayConfig {
                control_socket: Some(control_path.clone()),
                mirror_cache: MirrorCacheConfig::new(cache),
                ..GatewayConfig::default()
            },
            Arc::new(NoCredentials),
            Arc::new(LocalUpstream(upstream_address)),
            Arc::new(audit),
        )
        .await
        .unwrap();
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let certificate = params.self_signed(&key).unwrap();
        let repo = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("00000000000000000000000000000001").unwrap();
        let identity = stable_workspace_id(&repo, "topic", &incarnation);
        let mut fixture = Self {
            root,
            parent,
            workspace,
            control: ControlSocket::at(control_path).unwrap(),
            gateway: Some(gateway),
            upstream,
            repo,
            identity,
            // The enclosing sandbox's real gateway owns IPv4 at its assigned
            // base. An isolated IPv6 listener uses that same admitted port.
            endpoint: SocketAddr::new(
                if gateway_port.is_some() {
                    IpAddr::V6(Ipv6Addr::LOCALHOST)
                } else {
                    IpAddr::V4(Ipv4Addr::LOCALHOST)
                },
                gateway_port.unwrap_or(MACOS_PORT_MIN),
            ),
            token: WorkspaceToken::from_bytes([19; 32]),
            ca: WorkspaceCa::new(certificate.pem(), key.serialize_pem()).unwrap(),
            grants: GrantSet::default(),
            check_stdout: Vec::new(),
        };
        // Install atomically rather than probing then releasing a port: another test process
        // or the host gateway may already own any candidate block.
        let first = gateway_port.unwrap_or(MACOS_PORT_MIN);
        let end = gateway_port.map_or(MACOS_PORT_MAX, |port| port + 1);
        for port in (first..end).step_by(usize::from(MACOS_PORT_BLOCK_SIZE)) {
            fixture.endpoint.set_port(port);
            match fixture
                .gateway
                .as_ref()
                .unwrap()
                .handle()
                .install(fixture.session())
                .await
            {
                Ok(()) => return fixture,
                Err(GatewayError::Io(error)) if error.kind() == std::io::ErrorKind::AddrInUse => {}
                Err(error) => panic!("install fixture gateway session: {error}"),
            }
        }
        panic!("no gateway port block available for the landing fixture");
    }

    fn session(&self) -> WorkspaceSession {
        WorkspaceSession {
            workspace_id: self.identity.clone(),
            repo_id: self.repo.as_str().to_owned(),
            revision: self.grants.revision,
            endpoint: WorkspaceEndpoint::Tcp(self.endpoint),
            token: self.token.clone(),
            ca: WorkspaceCa::new(
                self.ca.certificate_pem.clone(),
                self.ca.private_key_pem.to_string(),
            )
            .unwrap(),
            policy: policy_from_grants(&self.grants).unwrap(),
        }
    }

    pub(super) async fn reconcile(&mut self, grants: &GrantSet) -> Result<()> {
        self.grants = grants.clone();
        reconcile_project(&self.control, self, &self.repo, unsafe { libc::geteuid() })
            .await
            .map(|_| ())
    }

    pub(super) async fn land(&mut self, options: LandOptions) -> Result<LandReport> {
        let target = options.target_branch.unwrap_or_else(|| "main".to_owned());
        let previous = git(&self.parent, ["rev-parse", &target]).await;
        for check in options.check.unwrap_or_default() {
            let output = Command::new("/bin/sh")
                .args(["-c", &check])
                .current_dir(&self.workspace)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env(
                    "HTTP_PROXY",
                    format!("http://workspace:{}@{}", self.token.encode(), self.endpoint),
                )
                .kill_on_drop(true)
                .output()
                .await
                .unwrap();
            self.check_stdout = output.stdout;
            if !output.status.success() {
                return Err(CowshedError::conflict(
                    format!(
                        "acceptance check failed: {}",
                        String::from_utf8_lossy(&output.stderr)
                    ),
                    "make the acceptance check pass before landing",
                ));
            }
        }
        git(&self.parent, ["fetch", "-q", "../topic", "HEAD"]).await;
        git(&self.parent, ["merge", "-q", "--ff-only", "FETCH_HEAD"]).await;
        Ok(LandReport {
            landed_head: GitOid::new(git(&self.parent, ["rev-parse", &target]).await).unwrap(),
            target_branch: target,
            previous_target_head: Some(GitOid::new(previous).unwrap()),
            target_was_checked_out: true,
            retired: false,
        })
    }

    async fn stop_gateway(&mut self) {
        if let Some(gateway) = self.gateway.take() {
            gateway.drain().await.unwrap();
        }
    }
}

#[async_trait]
impl SessionInventory for Fixture {
    async fn all_sessions(&self) -> Result<Vec<WorkspaceSession>> {
        Ok(vec![self.session()])
    }

    async fn project_sessions(&self, repo: &RepoId) -> Result<Vec<WorkspaceSession>> {
        Ok(if repo == &self.repo {
            vec![self.session()]
        } else {
            Vec::new()
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.upstream.abort();
        fs::remove_dir_all(&self.root).unwrap();
    }
}

async fn git<const N: usize>(root: &Path, args: [&str; N]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

#[tokio::test]
async fn checked_land_uses_new_egress_grants_without_an_intervening_exec() {
    let gateway_port = std::env::var(PORT_BASE_ENV)
        .ok()
        .map(|port| port.parse().expect("assigned gateway port"));
    let fixture = Fixture::new(gateway_port).await;
    let expected_head = git(&fixture.workspace, ["rev-parse", "HEAD"]).await;
    let mut service = FakeService {
        checked_landing: Some(fixture),
        ..FakeService::default()
    };
    run(&mut service, ["grant", "topic", "--egress", UPSTREAM_HOST]).await;
    let cli = parse_args(["land", "topic", "--no-retire", "--check", FETCH_CHECK]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let result = dispatch(&mut service, cli, tokio::io::empty(), &mut output).await;
    let fixture = service.checked_landing.as_mut().unwrap();
    let actual_head = git(&fixture.parent, ["rev-parse", "main"]).await;
    fixture.stop_gateway().await;

    assert_eq!(
        result
            .expect("a newly granted upstream must be reachable during the check")
            .code,
        0
    );
    assert_eq!(fixture.check_stdout, b"granted\n");
    assert_eq!(actual_head, expected_head);
}

#[tokio::test]
async fn checked_land_keeps_target_ref_unchanged_when_gateway_reconciliation_fails() {
    let mut fixture = Fixture::new(None).await;
    let original_head = git(&fixture.parent, ["rev-parse", "main"]).await;
    fixture.stop_gateway().await;
    let mut service = FakeService {
        checked_landing: Some(fixture),
        ..FakeService::default()
    };
    let cli = parse_args(["land", "topic", "--no-retire", "--check", "printf checked"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let result = dispatch(&mut service, cli, tokio::io::empty(), &mut output).await;
    let fixture = service.checked_landing.as_ref().unwrap();
    let actual_head = git(&fixture.parent, ["rev-parse", "main"]).await;

    assert_eq!(
        actual_head, original_head,
        "unreconciled landing must not advance the target"
    );
    assert_eq!(
        result
            .expect_err("gateway unavailability must refuse checked land")
            .code,
        ErrorCode::EnvironmentMissing
    );
    assert!(
        fixture.check_stdout.is_empty(),
        "acceptance checks must not run with stale authority"
    );
}

#[tokio::test]
async fn checkless_land_does_not_require_gateway_reconciliation() {
    let mut fixture = Fixture::new(None).await;
    let expected_head = git(&fixture.workspace, ["rev-parse", "HEAD"]).await;
    fixture.stop_gateway().await;
    let mut service = FakeService {
        checked_landing: Some(fixture),
        ..FakeService::default()
    };
    let cli = parse_args(["land", "topic", "--no-retire"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let result = dispatch(&mut service, cli, tokio::io::empty(), &mut output).await;
    let fixture = service.checked_landing.as_ref().unwrap();
    let actual_head = git(&fixture.parent, ["rev-parse", "main"]).await;

    assert_eq!(
        result
            .expect("landing without checks must not require the gateway preflight")
            .code,
        0
    );
    assert_eq!(actual_head, expected_head);
}
