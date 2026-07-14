use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use git2::{AutotagOption, FetchOptions, RemoteCallbacks, RemoteRedirect};
use http::{Method, Request, header};
use http_body_util::Empty;
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant, timeout, timeout_at};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use url::Url;
use uuid::Uuid;
use zeroize::{Zeroize as _, Zeroizing};

use crate::{
    actor::{BrokerAuditEvent, BrokerAuditKind, BrokerAuditStatus, BrokerAuditor},
    interfaces::{
        AuthorizedTarget, CredentialProtocol, CredentialProvider, CredentialQuery,
        NegotiatedTransport, UpstreamConnector, UpstreamPurpose,
    },
    policy::{
        CanonicalHost, CanonicalTarget, EgressMode, HostPattern, TargetScheme, WorkspacePolicy,
        normalize_path,
    },
};
pub const GATEWAY_GIT_FETCH_HELPER_ARG: &str = "__cowshed-gateway-git-fetch";
const MAX_HELPER_FRAME: u64 = 1024 * 1024;

const MAX_BINDINGS: usize = 4096;
const MAX_MIRRORS: usize = 4096;
const MAX_REDIRECTS: usize = 5;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepoMirrorRequest {
    pub workspace_id: String,
    pub repo_id: String,
    pub remote: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MirrorInfo {
    pub repo_id: String,
    pub canonical_remote: String,
    pub path: PathBuf,
    pub head: Option<String>,
    pub bytes: u64,
}

pub struct RepoCredential {
    pub header_name: String,
    pub header_value: Zeroizing<String>,
}

impl fmt::Debug for RepoCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepoCredential")
            .field("header_name", &self.header_name)
            .field("header_value", &"[REDACTED]")
            .finish()
    }
}

struct FetchCancellation {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

impl FetchCancellation {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    async fn cancelled(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_cancelled() {
            notified.await;
        }
    }
}

pub struct RepoFetchPlan {
    pub remote: String,
    pub destination: PathBuf,
    pub credential: Option<RepoCredential>,
    cancellation: Arc<FetchCancellation>,
}

impl RepoFetchPlan {
    fn new(
        remote: String,
        destination: PathBuf,
        credential: Option<RepoCredential>,
        cancellation: Arc<FetchCancellation>,
    ) -> Self {
        Self {
            remote,
            destination,
            credential,
            cancellation,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

impl fmt::Debug for RepoFetchPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepoFetchPlan")
            .field("remote", &self.remote)
            .field("destination", &self.destination)
            .field("credential", &self.credential)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepoFetchOutcome {
    Fetched { head: Option<String> },
    Redirect { location: String },
}

#[async_trait]
pub trait RepoTransport: Send + Sync + 'static {
    async fn fetch(&self, plan: RepoFetchPlan) -> Result<RepoFetchOutcome, RepoMirrorError>;
    fn owns_cancellation(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct Git2RepoTransport {
    connector: Arc<dyn UpstreamConnector>,
    connect_timeout: Duration,
    helper_executable: Option<PathBuf>,
    response_timeout: Duration,
}

impl fmt::Debug for Git2RepoTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Git2RepoTransport")
            .field("connect_timeout", &self.connect_timeout)
            .field("response_timeout", &self.response_timeout)
            .finish_non_exhaustive()
    }
}

impl Git2RepoTransport {
    pub fn new(
        helper_executable: Option<PathBuf>,
        connector: Arc<dyn UpstreamConnector>,
        connect_timeout: Duration,
        response_timeout: Duration,
    ) -> Self {
        Self {
            helper_executable,
            connector,
            connect_timeout,
            response_timeout,
        }
    }

    async fn preflight(&self, plan: &RepoFetchPlan) -> Result<Option<String>, RepoMirrorError> {
        let mut url = Url::parse(&plan.remote).map_err(|_| RepoMirrorError::InvalidRemote)?;
        let path = format!("{}/info/refs", url.path().trim_end_matches('/'));
        url.set_path(&path);
        url.set_query(Some("service=git-upload-pack"));
        let target = CanonicalTarget::from_url(&url).map_err(|_| RepoMirrorError::InvalidRemote)?;
        if plan.is_cancelled() {
            return Err(RepoMirrorError::Cancelled);
        }
        let connection = timeout(
            self.connect_timeout,
            self.connector.connect(&AuthorizedTarget {
                target: target.clone(),
                purpose: UpstreamPurpose::TlsHttp,
                private_network_authorized: true,
            }),
        )
        .await
        .map_err(|_| RepoMirrorError::FetchTimeout)?
        .map_err(|_| RepoMirrorError::FetchFailed)?;
        let request_target = format!(
            "{}?{}",
            url.path(),
            url.query().ok_or(RepoMirrorError::InvalidRemote)?
        );
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(request_target)
            .header(header::HOST, target.authority())
            .header(header::USER_AGENT, "cowshed-gateway/1")
            .header("git-protocol", "version=2");
        if let Some(credential) = plan.credential.as_ref() {
            let name = http::HeaderName::from_bytes(credential.header_name.as_bytes())
                .map_err(|_| RepoMirrorError::CredentialScopeMismatch)?;
            let value = http::HeaderValue::from_str(credential.header_value.as_str())
                .map_err(|_| RepoMirrorError::CredentialScopeMismatch)?;
            request = request.header(name, value);
        }
        let request = request
            .body(Empty::<Bytes>::new())
            .map_err(|_| RepoMirrorError::FetchFailed)?;
        let response = timeout(self.response_timeout, async move {
            match connection.transport {
                NegotiatedTransport::Http1 => {
                    let (mut sender, connection) = http1::handshake(TokioIo::new(connection.io))
                        .await
                        .map_err(|_| RepoMirrorError::FetchFailed)?;
                    let driver = tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    let response = sender
                        .send_request(request)
                        .await
                        .map_err(|_| RepoMirrorError::FetchFailed);
                    driver.abort();
                    response
                }
                NegotiatedTransport::Http2 => {
                    let (mut sender, connection) = http2::Builder::new(TokioExecutor::new())
                        .handshake(TokioIo::new(connection.io))
                        .await
                        .map_err(|_| RepoMirrorError::FetchFailed)?;
                    let driver = tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    let response = sender
                        .send_request(request)
                        .await
                        .map_err(|_| RepoMirrorError::FetchFailed);
                    driver.abort();
                    response
                }
                NegotiatedTransport::Raw => Err(RepoMirrorError::FetchFailed),
            }
        })
        .await
        .map_err(|_| RepoMirrorError::FetchFailed)??;
        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .ok_or(RepoMirrorError::InvalidRedirect)?;
            return Ok(Some(location.to_owned()));
        }
        if !response.status().is_success() {
            return Err(RepoMirrorError::FetchFailed);
        }
        Ok(None)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HelperRequestOut<'a> {
    version: u16,
    remote: &'a str,
    destination: &'a Path,
    allowed_root: &'a Path,
    credential: Option<HelperCredentialOut<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HelperCredentialOut<'a> {
    header_name: &'a str,
    header_value: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HelperRequest {
    version: u16,
    remote: String,
    destination: PathBuf,
    allowed_root: PathBuf,
    credential: Option<HelperCredential>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HelperCredential {
    header_name: String,
    header_value: String,
}

impl Drop for HelperCredential {
    fn drop(&mut self) {
        self.header_value.zeroize();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HelperResponse {
    ok: bool,
    head: Option<String>,
    error_code: Option<String>,
}

#[derive(Debug, Error)]
pub enum GitFetchHelperError {
    #[error("gateway Git fetch helper protocol is invalid")]
    InvalidProtocol,
    #[error("gateway Git fetch helper I/O failed")]
    Io(#[from] std::io::Error),
}

#[async_trait]
impl RepoTransport for Git2RepoTransport {
    async fn fetch(&self, plan: RepoFetchPlan) -> Result<RepoFetchOutcome, RepoMirrorError> {
        if plan.is_cancelled() {
            return Err(RepoMirrorError::Cancelled);
        }
        if let Some(location) = self.preflight(&plan).await? {
            return Ok(RepoFetchOutcome::Redirect { location });
        }
        let executable = self
            .helper_executable
            .as_deref()
            .ok_or(RepoMirrorError::HelperUnavailable)?;
        run_fetch_helper(executable, plan).await
    }

    fn owns_cancellation(&self) -> bool {
        true
    }
}

async fn run_fetch_helper(
    executable: &Path,
    plan: RepoFetchPlan,
) -> Result<RepoFetchOutcome, RepoMirrorError> {
    validate_helper_executable(executable)?;
    let allowed_root = plan
        .destination
        .parent()
        .ok_or(RepoMirrorError::HelperUnavailable)?;
    let credential = plan
        .credential
        .as_ref()
        .map(|credential| HelperCredentialOut {
            header_name: &credential.header_name,
            header_value: credential.header_value.as_str(),
        });
    let request = HelperRequestOut {
        version: 1,
        remote: &plan.remote,
        destination: &plan.destination,
        allowed_root,
        credential,
    };
    let encoded =
        Zeroizing::new(serde_json::to_vec(&request).map_err(|_| RepoMirrorError::HelperProtocol)?);
    if encoded.len() > MAX_HELPER_FRAME as usize {
        return Err(RepoMirrorError::HelperProtocol);
    }
    let mut child = Command::new(executable)
        .arg(GATEWAY_GIT_FETCH_HELPER_ARG)
        .current_dir(allowed_root)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| RepoMirrorError::HelperUnavailable)?;
    let mut stdin = child.stdin.take().ok_or(RepoMirrorError::HelperProtocol)?;
    let stdout = child.stdout.take().ok_or(RepoMirrorError::HelperProtocol)?;
    let writer = tokio::spawn(async move {
        stdin.write_all(&encoded).await?;
        stdin.shutdown().await
    });
    let reader = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout
            .take(MAX_HELPER_FRAME + 1)
            .read_to_end(&mut bytes)
            .await?;
        Ok::<_, std::io::Error>(bytes)
    });
    let status = tokio::select! {
        status = child.wait() => status.map_err(|_| RepoMirrorError::FetchFailed)?,
        _ = plan.cancellation.cancelled() => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            writer.abort();
            reader.abort();
            remove_tree(&plan.destination);
            return Err(RepoMirrorError::Cancelled);
        }
    };
    writer
        .await
        .map_err(|_| RepoMirrorError::FetchFailed)?
        .map_err(|_| RepoMirrorError::FetchFailed)?;
    let response = reader
        .await
        .map_err(|_| RepoMirrorError::FetchFailed)?
        .map_err(|_| RepoMirrorError::FetchFailed)?;
    if !status.success() || response.is_empty() || response.len() > MAX_HELPER_FRAME as usize {
        remove_tree(&plan.destination);
        return Err(RepoMirrorError::FetchFailed);
    }
    let response: HelperResponse =
        serde_json::from_slice(&response).map_err(|_| RepoMirrorError::HelperProtocol)?;
    if !response.ok {
        remove_tree(&plan.destination);
        return Err(RepoMirrorError::FetchFailed);
    }
    Ok(RepoFetchOutcome::Fetched {
        head: response.head,
    })
}

fn validate_helper_executable(path: &Path) -> Result<(), RepoMirrorError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if !path.is_absolute() {
        return Err(RepoMirrorError::HelperUnavailable);
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| RepoMirrorError::HelperUnavailable)?;
    let mode = metadata.permissions().mode();
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || mode & 0o022 != 0
        || mode & 0o100 == 0
    {
        return Err(RepoMirrorError::HelperUnavailable);
    }
    Ok(())
}

pub fn run_gateway_git_fetch_helper() -> Result<(), GitFetchHelperError> {
    use std::io::{Read as _, Write as _};

    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take(MAX_HELPER_FRAME + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > MAX_HELPER_FRAME as usize {
        return Err(GitFetchHelperError::InvalidProtocol);
    }
    let request: HelperRequest =
        serde_json::from_slice(&bytes).map_err(|_| GitFetchHelperError::InvalidProtocol)?;
    if request.version != 1 || !validate_helper_destination(&request) {
        return Err(GitFetchHelperError::InvalidProtocol);
    }
    let response = match fetch_git2_helper(request) {
        Ok(head) => HelperResponse {
            ok: true,
            head,
            error_code: None,
        },
        Err(()) => HelperResponse {
            ok: false,
            head: None,
            error_code: Some("fetch-failed".to_owned()),
        },
    };
    let encoded = Zeroizing::new(
        serde_json::to_vec(&response).map_err(|_| GitFetchHelperError::InvalidProtocol)?,
    );
    if encoded.len() > MAX_HELPER_FRAME as usize {
        return Err(GitFetchHelperError::InvalidProtocol);
    }
    std::io::stdout().write_all(&encoded)?;
    std::io::stdout().flush()?;
    Ok(())
}

fn validate_helper_destination(request: &HelperRequest) -> bool {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if !request.allowed_root.is_absolute() || !request.destination.is_absolute() {
        return false;
    }
    let Ok(root_metadata) = std::fs::symlink_metadata(&request.allowed_root) else {
        return false;
    };
    let Ok(destination_metadata) = std::fs::symlink_metadata(&request.destination) else {
        return false;
    };
    if !root_metadata.is_dir()
        || root_metadata.file_type().is_symlink()
        || !destination_metadata.is_dir()
        || destination_metadata.file_type().is_symlink()
        || root_metadata.uid() != unsafe { libc::geteuid() }
        || destination_metadata.uid() != unsafe { libc::geteuid() }
        || root_metadata.permissions().mode() & 0o022 != 0
    {
        return false;
    }
    let Ok(root) = std::fs::canonicalize(&request.allowed_root) else {
        return false;
    };
    let Ok(destination) = std::fs::canonicalize(&request.destination) else {
        return false;
    };
    let name_ok = destination
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"));
    destination.parent() == Some(root.as_path())
        && name_ok
        && std::fs::read_dir(&destination).is_ok_and(|mut entries| entries.next().is_none())
}

fn fetch_git2_helper(mut request: HelperRequest) -> Result<Option<String>, ()> {
    let repository = git2::Repository::init_bare(&request.destination).map_err(|_| ())?;
    {
        let mut config = repository.config().map_err(|_| ())?;
        config.set_bool("core.bare", true).map_err(|_| ())?;
        config
            .set_str("core.hooksPath", "/dev/null")
            .map_err(|_| ())?;
        config
            .set_str("protocol.file.allow", "never")
            .map_err(|_| ())?;
        config
            .set_str("protocol.ext.allow", "never")
            .map_err(|_| ())?;
        config
            .set_bool("http.followRedirects", false)
            .map_err(|_| ())?;
    }
    let mut remote = repository
        .remote_anonymous(&request.remote)
        .map_err(|_| ())?;
    let mut callbacks = RemoteCallbacks::new();
    callbacks.credentials(|_, _, _| Err(git2::Error::from_str("interactive credentials disabled")));
    let mut fetch = FetchOptions::new();
    fetch.remote_callbacks(callbacks);
    fetch.follow_redirects(RemoteRedirect::None);
    fetch.download_tags(AutotagOption::All);
    fetch.prune(git2::FetchPrune::On);
    let header = request.credential.take().map(|credential| {
        Zeroizing::new(format!(
            "{}: {}",
            credential.header_name, credential.header_value
        ))
    });
    if let Some(header) = header.as_ref() {
        fetch.custom_headers(&[header.as_str()]);
    }
    remote
        .fetch(
            &["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"],
            Some(&mut fetch),
            None,
        )
        .map_err(|_| ())?;
    Ok(repository.references().ok().and_then(|mut references| {
        references.find_map(|reference| {
            let reference = reference.ok()?;
            if !reference.is_branch() {
                return None;
            }
            reference.target().map(|oid| oid.to_string())
        })
    }))
}

#[derive(Clone)]
pub(crate) struct RepoMirrorHandle {
    sender: mpsc::Sender<Message>,
    shutdown: watch::Sender<bool>,
}

impl fmt::Debug for RepoMirrorHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepoMirrorHandle")
            .finish_non_exhaustive()
    }
}

impl RepoMirrorHandle {
    pub(crate) fn start(
        root: PathBuf,
        capacity: usize,
        credentials: Arc<dyn CredentialProvider>,
        transport: Arc<dyn RepoTransport>,
        auditor: Arc<dyn BrokerAuditor>,
        credential_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), RepoMirrorError> {
        prepare_root(&root)?;
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (shutdown, stopped) = watch::channel(false);
        let handle = Self { sender, shutdown };
        let task = tokio::spawn(run(
            receiver,
            stopped,
            root,
            credentials,
            transport,
            auditor,
            RepoDeadlines {
                credential: credential_timeout,
                total: total_timeout,
            },
        ));
        Ok((handle, task))
    }

    pub(crate) async fn bind_session(
        &self,
        workspace_id: String,
        repo_id: String,
        policy: WorkspacePolicy,
    ) -> Result<(), RepoMirrorError> {
        self.call(|reply| Message::Bind {
            workspace_id,
            repo_id,
            policy,
            reply,
        })
        .await
    }

    pub(crate) async fn unbind_session(&self, workspace_id: String) -> Result<(), RepoMirrorError> {
        self.call(|reply| Message::Unbind {
            workspace_id,
            reply,
        })
        .await
    }

    pub(crate) async fn mirror(
        &self,
        request: RepoMirrorRequest,
    ) -> Result<MirrorInfo, RepoMirrorError> {
        self.call(|reply| Message::Mirror { request, reply }).await
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, RepoMirrorError>>) -> Message,
    ) -> Result<T, RepoMirrorError> {
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| RepoMirrorError::Stopped)?;
        receiver.await.map_err(|_| RepoMirrorError::Stopped)?
    }
}

enum Message {
    Bind {
        workspace_id: String,
        repo_id: String,
        policy: WorkspacePolicy,
        reply: oneshot::Sender<Result<(), RepoMirrorError>>,
    },
    Unbind {
        workspace_id: String,
        reply: oneshot::Sender<Result<(), RepoMirrorError>>,
    },
    Mirror {
        request: RepoMirrorRequest,
        reply: oneshot::Sender<Result<MirrorInfo, RepoMirrorError>>,
    },
}

#[derive(Clone)]
struct Binding {
    repo_id: String,
    policy: WorkspacePolicy,
}

#[derive(Clone, Copy)]
struct RepoDeadlines {
    credential: Duration,
    total: Duration,
}

async fn run(
    mut receiver: mpsc::Receiver<Message>,
    mut shutdown: watch::Receiver<bool>,
    root: PathBuf,
    credentials: Arc<dyn CredentialProvider>,
    transport: Arc<dyn RepoTransport>,
    auditor: Arc<dyn BrokerAuditor>,
    deadlines: RepoDeadlines,
) {
    let mut bindings = HashMap::<String, Binding>::new();
    let mut mirrors = HashMap::<(String, String), MirrorInfo>::new();
    loop {
        let message = tokio::select! {
            message = receiver.recv() => {
                let Some(message) = message else { break };
                message
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };
        match message {
            Message::Bind {
                workspace_id,
                repo_id,
                policy,
                reply,
            } => {
                let result =
                    if bindings.len() >= MAX_BINDINGS && !bindings.contains_key(&workspace_id) {
                        Err(RepoMirrorError::Capacity)
                    } else {
                        bindings.insert(workspace_id, Binding { repo_id, policy });
                        Ok(())
                    };
                let _ = reply.send(result);
            }
            Message::Unbind {
                workspace_id,
                reply,
            } => {
                bindings.remove(&workspace_id);
                let _ = reply.send(Ok(()));
            }
            Message::Mirror { request, reply } => {
                let cancellation = Arc::new(FetchCancellation::new());
                let operation = mirror(
                    request,
                    MirrorContext {
                        root: &root,
                        bindings: &bindings,
                        mirrors: &mut mirrors,
                        credentials: credentials.as_ref(),
                        transport: transport.as_ref(),
                        auditor: auditor.as_ref(),
                        deadlines,
                        cancellation: Arc::clone(&cancellation),
                    },
                );
                tokio::pin!(operation);
                let result = tokio::select! {
                    result = &mut operation => result,
                    changed = shutdown.changed() => {
                        cancellation.cancel();
                        let _ = timeout(Duration::from_secs(2), &mut operation).await;
                        let _ = reply.send(Err(RepoMirrorError::Cancelled));
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                        continue;
                    }
                };
                let _ = reply.send(result);
            }
        }
    }
}

struct MirrorContext<'a> {
    root: &'a Path,
    bindings: &'a HashMap<String, Binding>,
    mirrors: &'a mut HashMap<(String, String), MirrorInfo>,
    credentials: &'a dyn CredentialProvider,
    transport: &'a dyn RepoTransport,
    auditor: &'a dyn BrokerAuditor,
    deadlines: RepoDeadlines,
    cancellation: Arc<FetchCancellation>,
}

async fn mirror(
    request: RepoMirrorRequest,
    context: MirrorContext<'_>,
) -> Result<MirrorInfo, RepoMirrorError> {
    let MirrorContext {
        root,
        bindings,
        mirrors,
        credentials,
        transport,
        auditor,
        deadlines,
        cancellation,
    } = context;
    let deadline = Instant::now() + deadlines.total;
    validate_repo_id(&request.repo_id)?;
    let binding = bindings
        .get(&request.workspace_id)
        .ok_or(RepoMirrorError::UnknownWorkspace)?;
    if binding.repo_id != request.repo_id {
        let mut scoped = request.clone();
        scoped.repo_id.clone_from(&binding.repo_id);
        audit(
            auditor,
            &scoped,
            None,
            BrokerAuditStatus::Denied,
            Some("repo-scope-mismatch"),
            0,
        )
        .await?;
        return Err(RepoMirrorError::ScopeMismatch);
    }
    let original = match canonical_remote(&request.remote) {
        Ok(remote) => remote,
        Err(error) => {
            audit(
                auditor,
                &request,
                None,
                BrokerAuditStatus::Denied,
                Some(error.classification()),
                0,
            )
            .await?;
            return Err(error);
        }
    };
    if let Err(error) = admit_remote(binding, &original) {
        audit(
            auditor,
            &request,
            Some(&original.canonical),
            BrokerAuditStatus::Denied,
            Some(error.classification()),
            0,
        )
        .await?;
        return Err(error);
    }
    audit(
        auditor,
        &request,
        Some(&original.canonical),
        BrokerAuditStatus::Allowed,
        None,
        0,
    )
    .await?;
    let key = (request.repo_id.clone(), original.canonical.clone());
    if mirrors.len() >= MAX_MIRRORS && !mirrors.contains_key(&key) {
        return Err(RepoMirrorError::Capacity);
    }
    let project = root.join(hash_component(&request.repo_id));
    ensure_private_directory(&project)?;
    let remote_root = project.join(hash_component(&original.canonical));
    ensure_private_directory(&remote_root)?;
    let temp = remote_root.join(format!(".{}.tmp", Uuid::new_v4()));
    std::fs::create_dir(&temp).map_err(|_| RepoMirrorError::PublicationFailed)?;

    let result = fetch_redirects(
        original,
        FetchContext {
            request: &request,
            binding,
            destination: temp.clone(),
            credentials,
            transport,
            deadlines,
            deadline,
            cancellation,
        },
    )
    .await;
    let (final_remote, head) = match result {
        Ok(result) => result,
        Err(error) => {
            remove_tree(&temp);
            audit(
                auditor,
                &request,
                None,
                BrokerAuditStatus::Failed,
                Some(error.classification()),
                0,
            )
            .await?;
            return Err(error);
        }
    };
    make_read_only(&temp)?;
    let published = remote_root.join(format!("{}.git", Uuid::new_v4()));
    std::fs::rename(&temp, &published).map_err(|_| RepoMirrorError::PublicationFailed)?;
    sync_directory(&remote_root)?;
    let bytes = directory_bytes(&published)?;
    let info = MirrorInfo {
        repo_id: request.repo_id.clone(),
        canonical_remote: final_remote.canonical,
        path: published,
        head,
        bytes,
    };
    if let Some(previous) = mirrors.insert(key, info.clone())
        && previous.path != info.path
    {
        remove_tree_read_only(&previous.path);
    }
    audit(
        auditor,
        &request,
        Some(&info.canonical_remote),
        BrokerAuditStatus::Completed,
        None,
        bytes,
    )
    .await?;
    Ok(info)
}

struct CanonicalRemote {
    canonical: String,
    target: CanonicalTarget,
    path: String,
}

fn canonical_remote(value: &str) -> Result<CanonicalRemote, RepoMirrorError> {
    if value.len() > 8192 {
        return Err(RepoMirrorError::InvalidRemote);
    }
    let url = Url::parse(value).map_err(|_| RepoMirrorError::InvalidRemote)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(RepoMirrorError::InvalidRemote);
    }
    let target = CanonicalTarget::from_url(&url).map_err(|_| RepoMirrorError::InvalidRemote)?;
    if target.scheme != TargetScheme::Https || !matches!(target.host, CanonicalHost::Dns(_)) {
        return Err(RepoMirrorError::InvalidRemote);
    }
    let path = normalize_path(url.path()).map_err(|_| RepoMirrorError::InvalidRemote)?;
    if path == "/" || path.ends_with('/') || path != url.path() {
        return Err(RepoMirrorError::InvalidRemote);
    }
    let port = if target.port == 443 {
        String::new()
    } else {
        format!(":{}", target.port)
    };
    let canonical = format!("https://{}{}{}", target.host, port, path);
    if canonical != value {
        return Err(RepoMirrorError::NonCanonicalRemote);
    }
    Ok(CanonicalRemote {
        canonical,
        target,
        path,
    })
}

fn admit_remote(binding: &Binding, remote: &CanonicalRemote) -> Result<(), RepoMirrorError> {
    let grant = binding
        .policy
        .authorize(&remote.target, &Method::GET, &remote.path)
        .map_err(|_| RepoMirrorError::NotAdmitted)?;
    if grant.mode != EgressMode::Intercept || !matches!(grant.host, HostPattern::Exact(_)) {
        return Err(RepoMirrorError::NotAdmitted);
    }
    Ok(())
}

struct FetchContext<'a> {
    request: &'a RepoMirrorRequest,
    binding: &'a Binding,
    destination: PathBuf,
    credentials: &'a dyn CredentialProvider,
    transport: &'a dyn RepoTransport,
    deadlines: RepoDeadlines,
    deadline: Instant,
    cancellation: Arc<FetchCancellation>,
}

async fn fetch_redirects(
    mut remote: CanonicalRemote,
    context: FetchContext<'_>,
) -> Result<(CanonicalRemote, Option<String>), RepoMirrorError> {
    let FetchContext {
        request,
        binding,
        destination,
        credentials,
        transport,
        deadlines,
        deadline,
        cancellation,
    } = context;
    for redirect_count in 0..=MAX_REDIRECTS {
        if Instant::now() >= deadline {
            return Err(RepoMirrorError::FetchTimeout);
        }
        admit_remote(binding, &remote)?;
        let query = CredentialQuery {
            workspace_id: request.workspace_id.clone(),
            repo_id: request.repo_id.clone(),
            protocol: CredentialProtocol::Generic,
            origin: remote.target.origin(),
            method: Method::GET,
            path: remote.path.clone(),
        };
        let credential_deadline = deadline.min(Instant::now() + deadlines.credential);
        let credential = tokio::select! {
            result = timeout_at(credential_deadline, credentials.lookup(&query)) => {
                result
                    .map_err(|_| RepoMirrorError::CredentialTimeout)?
                    .map_err(|_| RepoMirrorError::CredentialUnavailable)?
            }
            _ = cancellation.cancelled() => return Err(RepoMirrorError::Cancelled),
        };
        let credential = match credential {
            Some(record) if record.validate_for(&query) => Some(RepoCredential {
                header_name: record.header_name.as_str().to_owned(),
                header_value: record.header_value,
            }),
            Some(_) => return Err(RepoMirrorError::CredentialScopeMismatch),
            None => None,
        };
        let operation = transport.fetch(RepoFetchPlan::new(
            remote.canonical.clone(),
            destination.clone(),
            credential,
            Arc::clone(&cancellation),
        ));
        tokio::pin!(operation);
        let outcome = tokio::select! {
            result = &mut operation => result?,
            _ = cancellation.cancelled() => {
                if transport.owns_cancellation() {
                    let _ = operation.await;
                }
                return Err(RepoMirrorError::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline) => {
                cancellation.cancel();
                let _ = timeout(Duration::from_secs(2), &mut operation).await;
                return Err(RepoMirrorError::FetchTimeout);
            }
        };
        match outcome {
            RepoFetchOutcome::Fetched { head } => return Ok((remote, head)),
            RepoFetchOutcome::Redirect { location } if redirect_count < MAX_REDIRECTS => {
                remove_tree(&destination);
                std::fs::create_dir(&destination)
                    .map_err(|_| RepoMirrorError::PublicationFailed)?;
                remote = resolve_redirect(&remote, &location)?;
            }
            RepoFetchOutcome::Redirect { .. } => return Err(RepoMirrorError::TooManyRedirects),
        }
    }
    Err(RepoMirrorError::TooManyRedirects)
}

fn resolve_redirect(
    base: &CanonicalRemote,
    location: &str,
) -> Result<CanonicalRemote, RepoMirrorError> {
    let base = Url::parse(&base.canonical).map_err(|_| RepoMirrorError::InvalidRedirect)?;
    let resolved = base
        .join(location)
        .map_err(|_| RepoMirrorError::InvalidRedirect)?;
    canonical_remote(resolved.as_str()).map_err(|_| RepoMirrorError::InvalidRedirect)
}

async fn audit(
    auditor: &dyn BrokerAuditor,
    request: &RepoMirrorRequest,
    remote: Option<&str>,
    status: BrokerAuditStatus,
    classification: Option<&str>,
    bytes: u64,
) -> Result<(), RepoMirrorError> {
    auditor
        .record_broker(BrokerAuditEvent {
            workspace_id: request.workspace_id.clone(),
            kind: BrokerAuditKind::RepoMirror,
            method: Some("GET".to_owned()),
            path: remote.map(str::to_owned),
            status,
            classification: classification.map(str::to_owned),
            bytes,
        })
        .await
        .map_err(|_| RepoMirrorError::AuditUnavailable)
}

fn prepare_root(root: &Path) -> Result<(), RepoMirrorError> {
    if !root.is_absolute() {
        return Err(RepoMirrorError::InsecureRoot);
    }
    ensure_private_directory(root)
}

fn ensure_private_directory(path: &Path) -> Result<(), RepoMirrorError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    if !path.exists() {
        std::fs::create_dir(path).map_err(|_| RepoMirrorError::InsecureRoot)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| RepoMirrorError::InsecureRoot)?;
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| RepoMirrorError::InsecureRoot)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(RepoMirrorError::InsecureRoot);
    }
    Ok(())
}

fn make_read_only(path: &Path) -> Result<(), RepoMirrorError> {
    use std::os::unix::fs::PermissionsExt as _;
    for entry in std::fs::read_dir(path).map_err(|_| RepoMirrorError::PublicationFailed)? {
        let entry = entry.map_err(|_| RepoMirrorError::PublicationFailed)?;
        let ty = entry
            .file_type()
            .map_err(|_| RepoMirrorError::PublicationFailed)?;
        if ty.is_symlink() {
            return Err(RepoMirrorError::PublicationFailed);
        }
        if ty.is_dir() {
            make_read_only(&entry.path())?;
        } else if !ty.is_file() {
            return Err(RepoMirrorError::PublicationFailed);
        }
        let mode = if ty.is_dir() { 0o555 } else { 0o444 };
        std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(mode))
            .map_err(|_| RepoMirrorError::PublicationFailed)?;
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555))
        .map_err(|_| RepoMirrorError::PublicationFailed)
}

fn directory_bytes(path: &Path) -> Result<u64, RepoMirrorError> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path).map_err(|_| RepoMirrorError::PublicationFailed)? {
        let entry = entry.map_err(|_| RepoMirrorError::PublicationFailed)?;
        let ty = entry
            .file_type()
            .map_err(|_| RepoMirrorError::PublicationFailed)?;
        if ty.is_symlink() {
            return Err(RepoMirrorError::PublicationFailed);
        }
        if ty.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        } else if ty.is_file() {
            total = total.saturating_add(
                entry
                    .metadata()
                    .map_err(|_| RepoMirrorError::PublicationFailed)?
                    .len(),
            );
        } else {
            return Err(RepoMirrorError::PublicationFailed);
        }
    }
    Ok(total)
}

fn sync_directory(path: &Path) -> Result<(), RepoMirrorError> {
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| RepoMirrorError::PublicationFailed)
}

fn remove_tree(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

fn remove_tree_read_only(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    remove_tree_read_only(&entry.path());
                }
            }
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
            let _ = std::fs::remove_dir(path);
        } else if metadata.is_file() {
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            let _ = std::fs::remove_file(path);
        }
    }
}

fn hash_component(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn validate_repo_id(value: &str) -> Result<(), RepoMirrorError> {
    let (owner, name) = value
        .split_once('/')
        .ok_or(RepoMirrorError::InvalidRepoId)?;
    if name.contains('/')
        || matches!(owner, "." | "..")
        || matches!(name, "." | "..")
        || [owner, name].into_iter().any(|component| {
            component.is_empty()
                || component.len() > 128
                || !component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        return Err(RepoMirrorError::InvalidRepoId);
    }
    Ok(())
}

impl RepoMirrorError {
    fn classification(&self) -> &'static str {
        match self {
            Self::ScopeMismatch => "repo-scope-mismatch",
            Self::NotAdmitted => "repo-not-admitted",
            Self::InvalidRemote | Self::NonCanonicalRemote => "repo-invalid-remote",
            Self::InvalidRedirect | Self::TooManyRedirects => "repo-redirect-denied",
            Self::CredentialUnavailable
            | Self::CredentialScopeMismatch
            | Self::CredentialTimeout => "repo-credential-denied",
            Self::FetchFailed
            | Self::FetchTimeout
            | Self::Cancelled
            | Self::HelperUnavailable
            | Self::HelperProtocol => "repo-fetch-failed",
            Self::PublicationFailed => "repo-publication-failed",
            _ => "repo-request-rejected",
        }
    }
}

#[derive(Debug, Error)]
pub enum RepoMirrorError {
    #[error("repository id is invalid")]
    InvalidRepoId,
    #[error("repository remote must be a canonical HTTPS URL without embedded credentials")]
    InvalidRemote,
    #[error("repository remote URL is not canonical")]
    NonCanonicalRemote,
    #[error("repository mirror workspace is not installed")]
    UnknownWorkspace,
    #[error("repository mirror project scope does not match")]
    ScopeMismatch,
    #[error("repository remote is not admitted by trusted project policy")]
    NotAdmitted,
    #[error("repository redirect is invalid or not admitted")]
    InvalidRedirect,
    #[error("repository redirect limit exceeded")]
    TooManyRedirects,
    #[error("repository credential store is unavailable")]
    CredentialUnavailable,
    #[error("repository credential lookup timed out")]
    CredentialTimeout,
    #[error("repository credential scope does not match")]
    CredentialScopeMismatch,
    #[error("repository transport fetch failed")]
    FetchFailed,
    #[error("repository transport fetch timed out")]
    FetchTimeout,
    #[error("repository transport fetch was cancelled")]
    Cancelled,
    #[error("repository Git fetch helper is unavailable")]
    HelperUnavailable,
    #[error("repository Git fetch helper protocol failed")]
    HelperProtocol,
    #[error("repository mirror publication failed")]
    PublicationFailed,
    #[error("repository mirror root is insecure")]
    InsecureRoot,
    #[error("repository mirror actor capacity is exhausted")]
    Capacity,
    #[error("repository mirror audit is unavailable")]
    AuditUnavailable,
    #[error("repository mirror actor stopped")]
    Stopped,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, VecDeque},
        os::unix::fs::PermissionsExt as _,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        sync::Notify,
    };

    use super::*;

    struct CountingCredentials {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl CredentialProvider for CountingCredentials {
        async fn lookup(
            &self,
            _query: &CredentialQuery,
        ) -> Result<Option<crate::CredentialRecord>, crate::CredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    struct NeverCredentials;

    #[async_trait]
    impl CredentialProvider for NeverCredentials {
        async fn lookup(
            &self,
            _query: &CredentialQuery,
        ) -> Result<Option<crate::CredentialRecord>, crate::CredentialError> {
            std::future::pending().await
        }
    }

    struct FixtureTransport {
        calls: Mutex<Vec<String>>,
        outcomes: Mutex<VecDeque<RepoFetchOutcome>>,
    }

    #[async_trait]
    impl RepoTransport for FixtureTransport {
        async fn fetch(&self, plan: RepoFetchPlan) -> Result<RepoFetchOutcome, RepoMirrorError> {
            self.calls.lock().expect("calls").push(plan.remote);
            let outcome = self
                .outcomes
                .lock()
                .expect("outcomes")
                .pop_front()
                .expect("fixture outcome");
            if matches!(outcome, RepoFetchOutcome::Fetched { .. }) {
                std::fs::create_dir_all(plan.destination.join("objects")).expect("create objects");
                std::fs::write(plan.destination.join("HEAD"), b"ref: refs/heads/main\n")
                    .expect("write HEAD");
                std::fs::write(plan.destination.join("objects").join("pack"), b"pack")
                    .expect("write pack");
            }
            Ok(outcome)
        }
    }
    struct NeverTransport {
        started: Arc<Notify>,
    }

    #[async_trait]
    impl RepoTransport for NeverTransport {
        async fn fetch(&self, _plan: RepoFetchPlan) -> Result<RepoFetchOutcome, RepoMirrorError> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    struct RedirectConnector {
        request: Arc<Mutex<Vec<u8>>>,
    }

    #[async_trait]
    impl UpstreamConnector for RedirectConnector {
        async fn health(&self, _target: &CanonicalTarget) -> crate::UpstreamHealth {
            crate::UpstreamHealth::Healthy
        }

        async fn connect(
            &self,
            _target: &AuthorizedTarget,
        ) -> Result<crate::UpstreamConnection, crate::ConnectError> {
            let (client, mut server) = tokio::io::duplex(4096);
            let request = Arc::clone(&self.request);
            tokio::spawn(async move {
                let mut captured = Vec::new();
                let mut buffer = [0_u8; 512];
                while !captured.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = server.read(&mut buffer).await.expect("read request");
                    if read == 0 {
                        break;
                    }
                    captured.extend_from_slice(&buffer[..read]);
                }
                *request.lock().expect("request") = captured;
                server
                    .write_all(
                        b"HTTP/1.1 302 Found\r\nLocation: /org/final.git\r\nContent-Length: 0\r\n\r\n",
                    )
                    .await
                    .expect("write redirect");
            });
            Ok(crate::UpstreamConnection {
                io: Box::new(client),
                transport: NegotiatedTransport::Http1,
            })
        }
    }

    #[derive(Default)]
    struct FixtureAuditor {
        fail: AtomicBool,
        events: Mutex<Vec<BrokerAuditEvent>>,
    }

    #[async_trait]
    impl BrokerAuditor for FixtureAuditor {
        async fn record_broker(&self, event: BrokerAuditEvent) -> Result<(), crate::AuditError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(crate::AuditError("unavailable".to_owned()));
            }
            self.events.lock().expect("events").push(event);
            Ok(())
        }
    }

    fn fixture_root(label: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowshed-repo-mirror-{label}-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).expect("create root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure root");
        root
    }

    fn policy(prefix: &str) -> WorkspacePolicy {
        let policy = WorkspacePolicy {
            grants: vec![crate::EgressGrant {
                host: HostPattern::parse("git.example.test").expect("host"),
                port: 443,
                mode: EgressMode::Intercept,
                methods: BTreeSet::from(["GET".to_owned(), "HEAD".to_owned()]),
                path_prefixes: vec![prefix.to_owned()],
                impersonate: false,
            }],
            mirrors: Vec::new(),
        };
        policy.validate().expect("policy");
        policy
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_fetch_without_waiting_for_total_deadline() {
        let root = fixture_root("drain");
        let started = Arc::new(Notify::new());
        let transport = Arc::new(NeverTransport {
            started: Arc::clone(&started),
        });
        let auditor = Arc::new(FixtureAuditor::default());
        let credentials = Arc::new(CountingCredentials {
            calls: AtomicUsize::new(0),
        });
        let (handle, task) = RepoMirrorHandle::start(
            root.clone(),
            4,
            credentials,
            transport,
            auditor,
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .expect("start");
        handle
            .bind_session("ws".to_owned(), "owner/repo".to_owned(), policy("/org"))
            .await
            .expect("bind");
        let started_wait = started.notified();
        let request_handle = handle.clone();
        let request = tokio::spawn(async move {
            request_handle
                .mirror(RepoMirrorRequest {
                    workspace_id: "ws".to_owned(),
                    repo_id: "owner/repo".to_owned(),
                    remote: "https://git.example.test/org/repo.git".to_owned(),
                })
                .await
        });
        timeout(Duration::from_secs(1), started_wait)
            .await
            .expect("fetch started");
        timeout(Duration::from_millis(100), handle.shutdown())
            .await
            .expect("shutdown signal");
        timeout(Duration::from_millis(100), task)
            .await
            .expect("actor stopped")
            .expect("actor joined");
        assert!(matches!(
            request.await.expect("request joined"),
            Err(RepoMirrorError::Cancelled | RepoMirrorError::Stopped)
        ));
        remove_tree_read_only(&root);
    }

    #[tokio::test]
    async fn credential_lookup_obeys_stage_deadline() {
        let root = fixture_root("credential-timeout");
        let transport = Arc::new(FixtureTransport {
            calls: Mutex::new(Vec::new()),
            outcomes: Mutex::new(VecDeque::new()),
        });
        let auditor = Arc::new(FixtureAuditor::default());
        let (handle, task) = RepoMirrorHandle::start(
            root.clone(),
            4,
            Arc::new(NeverCredentials),
            transport,
            auditor,
            Duration::from_millis(10),
            Duration::from_secs(1),
        )
        .expect("start");
        handle
            .bind_session("ws".to_owned(), "owner/repo".to_owned(), policy("/org"))
            .await
            .expect("bind");
        assert!(matches!(
            handle
                .mirror(RepoMirrorRequest {
                    workspace_id: "ws".to_owned(),
                    repo_id: "owner/repo".to_owned(),
                    remote: "https://git.example.test/org/repo.git".to_owned(),
                })
                .await,
            Err(RepoMirrorError::CredentialTimeout)
        ));
        handle.shutdown().await;
        task.await.expect("join");
        remove_tree_read_only(&root);
    }

    #[tokio::test]
    async fn production_transport_discovers_redirect_before_git_fetch() {
        let root = fixture_root("preflight");
        let request = Arc::new(Mutex::new(Vec::new()));
        let transport = Git2RepoTransport::new(
            None,
            Arc::new(RedirectConnector {
                request: Arc::clone(&request),
            }),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let outcome = transport
            .fetch(RepoFetchPlan::new(
                "https://git.example.test/org/repo.git".to_owned(),
                root.join("unused.git"),
                Some(RepoCredential {
                    header_name: "authorization".to_owned(),
                    header_value: Zeroizing::new("Bearer scoped-secret".to_owned()),
                }),
                Arc::new(FetchCancellation::new()),
            ))
            .await
            .expect("redirect");
        assert_eq!(
            outcome,
            RepoFetchOutcome::Redirect {
                location: "/org/final.git".to_owned()
            }
        );
        let captured =
            String::from_utf8(request.lock().expect("request").clone()).expect("UTF-8 request");
        assert!(
            captured
                .starts_with("GET /org/repo.git/info/refs?service=git-upload-pack HTTP/1.1\r\n")
        );
        assert!(
            captured
                .to_ascii_lowercase()
                .contains("authorization: bearer scoped-secret\r\n")
        );
        remove_tree_read_only(&root);
    }

    #[tokio::test]
    async fn scope_redirect_credentials_and_publication_are_fenced() {
        let root = fixture_root("flow");
        let credentials = Arc::new(CountingCredentials {
            calls: AtomicUsize::new(0),
        });
        let transport = Arc::new(FixtureTransport {
            calls: Mutex::new(Vec::new()),
            outcomes: Mutex::new(VecDeque::from([
                RepoFetchOutcome::Redirect {
                    location: "https://git.example.test/org/final.git".to_owned(),
                },
                RepoFetchOutcome::Fetched {
                    head: Some("a".repeat(40)),
                },
                RepoFetchOutcome::Fetched {
                    head: Some("b".repeat(40)),
                },
                RepoFetchOutcome::Redirect {
                    location: "https://git.example.test/outside/final.git".to_owned(),
                },
            ])),
        });
        let auditor = Arc::new(FixtureAuditor::default());
        let (handle, task) = RepoMirrorHandle::start(
            root.clone(),
            8,
            credentials.clone(),
            transport.clone(),
            auditor.clone(),
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("start");
        handle
            .bind_session("ws-a".to_owned(), "owner/repo-a".to_owned(), policy("/org"))
            .await
            .expect("bind a");
        handle
            .bind_session("ws-b".to_owned(), "owner/repo-b".to_owned(), policy("/org"))
            .await
            .expect("bind b");

        let outside = handle
            .mirror(RepoMirrorRequest {
                workspace_id: "ws-a".to_owned(),
                repo_id: "owner/repo-a".to_owned(),
                remote: "https://git.example.test/outside/repo.git".to_owned(),
            })
            .await;
        assert!(matches!(outside, Err(RepoMirrorError::NotAdmitted)));
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 0);
        assert!(transport.calls.lock().expect("calls").is_empty());

        let first = handle
            .mirror(RepoMirrorRequest {
                workspace_id: "ws-a".to_owned(),
                repo_id: "owner/repo-a".to_owned(),
                remote: "https://git.example.test/org/repo.git".to_owned(),
            })
            .await
            .expect("mirror a");
        assert_eq!(
            first.canonical_remote,
            "https://git.example.test/org/final.git"
        );
        assert_ne!(
            std::fs::metadata(&first.path)
                .expect("published")
                .permissions()
                .mode()
                & 0o222,
            0o222
        );
        assert_eq!(
            std::fs::metadata(&first.path)
                .expect("published")
                .permissions()
                .mode()
                & 0o222,
            0
        );
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 2);

        let second = handle
            .mirror(RepoMirrorRequest {
                workspace_id: "ws-b".to_owned(),
                repo_id: "owner/repo-b".to_owned(),
                remote: "https://git.example.test/org/repo.git".to_owned(),
            })
            .await
            .expect("mirror b");
        assert_ne!(first.path, second.path);
        assert!(
            !first
                .path
                .starts_with(second.path.parent().expect("parent"))
        );

        let before = transport.calls.lock().expect("calls").len();
        assert!(matches!(
            handle
                .mirror(RepoMirrorRequest {
                    workspace_id: "ws-a".to_owned(),
                    repo_id: "owner/repo-b".to_owned(),
                    remote: "https://git.example.test/org/repo.git".to_owned(),
                })
                .await,
            Err(RepoMirrorError::ScopeMismatch)
        ));
        assert_eq!(transport.calls.lock().expect("calls").len(), before);

        let redirect_denied = handle
            .mirror(RepoMirrorRequest {
                workspace_id: "ws-a".to_owned(),
                repo_id: "owner/repo-a".to_owned(),
                remote: "https://git.example.test/org/redirect.git".to_owned(),
            })
            .await;
        assert!(matches!(
            redirect_denied,
            Err(RepoMirrorError::InvalidRedirect | RepoMirrorError::NotAdmitted)
        ));
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 4);
        assert!(
            auditor
                .events
                .lock()
                .expect("events")
                .iter()
                .all(|event| event.path.as_deref().is_none_or(|path| !path.contains('@')))
        );

        handle.shutdown().await;
        task.await.expect("join");
        remove_tree_read_only(&root);
    }

    #[tokio::test]
    async fn audit_failure_prevents_fetch_and_non_https_never_reaches_credentials() {
        let root = fixture_root("audit");
        let credentials = Arc::new(CountingCredentials {
            calls: AtomicUsize::new(0),
        });
        let transport = Arc::new(FixtureTransport {
            calls: Mutex::new(Vec::new()),
            outcomes: Mutex::new(VecDeque::new()),
        });
        let auditor = Arc::new(FixtureAuditor::default());
        auditor.fail.store(true, Ordering::SeqCst);
        let (handle, task) = RepoMirrorHandle::start(
            root.clone(),
            4,
            credentials.clone(),
            transport.clone(),
            auditor,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("start");
        handle
            .bind_session("ws".to_owned(), "owner/repo".to_owned(), policy("/org"))
            .await
            .expect("bind");
        assert!(matches!(
            handle
                .mirror(RepoMirrorRequest {
                    workspace_id: "ws".to_owned(),
                    repo_id: "owner/repo".to_owned(),
                    remote: "http://git.example.test/org/repo.git".to_owned(),
                })
                .await,
            Err(RepoMirrorError::AuditUnavailable)
        ));
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 0);
        assert!(transport.calls.lock().expect("calls").is_empty());
        handle.shutdown().await;
        task.await.expect("join");
        remove_tree_read_only(&root);
    }
    #[tokio::test]
    async fn helper_process_is_killed_reaped_and_cleaned_on_repeated_cancellation() {
        let root = fixture_root("helper-cancel");
        let helper = root.join("blocking-helper");
        std::fs::copy("/bin/cat", &helper).expect("copy blocking helper");
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700))
            .expect("secure blocking helper");
        let fifo = root.join(GATEWAY_GIT_FETCH_HELPER_ARG);
        let fifo_path =
            std::ffi::CString::new(std::os::unix::ffi::OsStrExt::as_bytes(fifo.as_os_str()))
                .expect("FIFO path");
        let created = unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) };
        assert_eq!(
            created,
            0,
            "create blocking FIFO: {}",
            std::io::Error::last_os_error()
        );

        for attempt in 0..8 {
            let destination = root.join(format!("attempt-{attempt}"));
            std::fs::create_dir(&destination).expect("create unpublished destination");
            let cancellation = Arc::new(FetchCancellation::new());
            let plan = RepoFetchPlan::new(
                "https://git.example.test/org/repo.git".to_owned(),
                destination.clone(),
                None,
                Arc::clone(&cancellation),
            );
            let fetch = tokio::spawn({
                let helper = helper.clone();
                async move { run_fetch_helper(&helper, plan).await }
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
            cancellation.cancel();
            let result = timeout(Duration::from_secs(1), fetch)
                .await
                .expect("helper cancellation deadline")
                .expect("helper task joined");
            assert!(matches!(result, Err(RepoMirrorError::Cancelled)));
            assert!(
                !destination.exists(),
                "cancelled helper left unpublished destination"
            );
        }

        remove_tree_read_only(&root);
    }
}
