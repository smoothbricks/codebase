use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{
    process::Command,
    sync::{mpsc, oneshot},
    time::{Duration, timeout},
};
use url::Url;

use crate::actor::{BrokerAuditEvent, BrokerAuditKind, BrokerAuditStatus, BrokerAuditor};

const MAX_PROJECTS: usize = 4096;
const MAX_SESSIONS: usize = 4096;
const MAX_APPROVALS: usize = 4096;
const MAX_SCHEMES: usize = 64;
const MAX_APP_ENTRIES: usize = 100_000;
const MAX_DEVICE_OUTPUT: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SimGrant {
    OpenUrl,
    Install,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "verb", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SimRequest {
    OpenUrl { device: String, url: String },
    Install { device: String, digest: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SimResult {
    pub operation: String,
    pub device: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SimProjectConfig {
    pub repo_id: String,
    pub grants: HashSet<SimGrant>,
    pub registered_schemes: HashSet<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SimInstallApproval {
    pub repo_id: String,
    pub device: String,
    pub artifact_digest: String,
    pub human_receipt: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SimDevice {
    pub identifier: String,
    pub name: String,
    pub state: String,
}

#[derive(Clone, Debug)]
pub enum SimCommand {
    OpenUrl { device: String, url: String },
    Install { device: String, app: PathBuf },
    List,
    Boot { device: String },
}

#[derive(Clone, Debug)]
pub struct SimCommandOutput {
    pub stdout: Vec<u8>,
}

#[async_trait]
pub trait SimRunner: Send + Sync + 'static {
    async fn run(&self, command: SimCommand) -> Result<SimCommandOutput, SimBrokerError>;
}

#[derive(Clone, Debug, Default)]
pub struct XcrunSimRunner;

#[async_trait]
impl SimRunner for XcrunSimRunner {
    async fn run(&self, command: SimCommand) -> Result<SimCommandOutput, SimBrokerError> {
        let mut process = Command::new("/usr/bin/xcrun");
        process.env_clear().arg("simctl");
        match command {
            SimCommand::OpenUrl { device, url } => {
                process.arg("openurl").arg(device).arg(url);
            }
            SimCommand::Install { device, app } => {
                process.arg("install").arg(device).arg(app);
            }
            SimCommand::List => {
                process.args(["list", "devices", "--json"]);
            }
            SimCommand::Boot { device } => {
                process.arg("boot").arg(device);
            }
        }
        process.kill_on_drop(true);
        let output = timeout(Duration::from_secs(120), process.output())
            .await
            .map_err(|_| SimBrokerError::RunnerTimeout)?
            .map_err(|_| SimBrokerError::RunnerFailed)?;
        if !output.status.success() || output.stdout.len() > MAX_DEVICE_OUTPUT {
            return Err(SimBrokerError::RunnerFailed);
        }
        Ok(SimCommandOutput {
            stdout: output.stdout,
        })
    }
}

#[derive(Clone)]
pub(crate) struct SimBrokerHandle {
    sender: mpsc::Sender<Message>,
}

impl fmt::Debug for SimBrokerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SimBrokerHandle")
            .finish_non_exhaustive()
    }
}

impl SimBrokerHandle {
    pub(crate) fn start(
        drop_root: Option<PathBuf>,
        capacity: usize,
        runner: Arc<dyn SimRunner>,
        auditor: Arc<dyn BrokerAuditor>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), SimBrokerError> {
        if let Some(root) = drop_root.as_deref() {
            validate_drop_root(root)?;
        }
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let handle = Self { sender };
        let task = tokio::spawn(run(receiver, drop_root, runner, auditor));
        Ok((handle, task))
    }

    pub(crate) async fn bind_session(
        &self,
        workspace_id: String,
        repo_id: String,
    ) -> Result<(), SimBrokerError> {
        self.call(|reply| Message::Bind {
            workspace_id,
            repo_id,
            reply,
        })
        .await
    }

    pub(crate) async fn unbind_session(&self, workspace_id: String) -> Result<(), SimBrokerError> {
        self.call(|reply| Message::Unbind {
            workspace_id,
            reply,
        })
        .await
    }

    pub(crate) async fn configure(&self, config: SimProjectConfig) -> Result<(), SimBrokerError> {
        self.call(|reply| Message::Configure { config, reply })
            .await
    }

    pub(crate) async fn approve(&self, approval: SimInstallApproval) -> Result<(), SimBrokerError> {
        self.call(|reply| Message::Approve { approval, reply })
            .await
    }

    pub(crate) async fn request(
        &self,
        workspace_id: String,
        request: SimRequest,
    ) -> Result<SimResult, SimBrokerError> {
        self.call(|reply| Message::Request {
            workspace_id,
            request,
            reply,
        })
        .await
    }

    pub(crate) async fn list_devices(
        &self,
        repo_id: String,
    ) -> Result<Vec<SimDevice>, SimBrokerError> {
        self.call(|reply| Message::List { repo_id, reply }).await
    }

    pub(crate) async fn boot_device(
        &self,
        repo_id: String,
        device: String,
    ) -> Result<(), SimBrokerError> {
        self.call(|reply| Message::Boot {
            repo_id,
            device,
            reply,
        })
        .await
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.call(|reply| Message::Shutdown { reply }).await;
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, SimBrokerError>>) -> Message,
    ) -> Result<T, SimBrokerError> {
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| SimBrokerError::Stopped)?;
        receiver.await.map_err(|_| SimBrokerError::Stopped)?
    }
}

enum Message {
    Bind {
        workspace_id: String,
        repo_id: String,
        reply: oneshot::Sender<Result<(), SimBrokerError>>,
    },
    Unbind {
        workspace_id: String,
        reply: oneshot::Sender<Result<(), SimBrokerError>>,
    },
    Configure {
        config: SimProjectConfig,
        reply: oneshot::Sender<Result<(), SimBrokerError>>,
    },
    Approve {
        approval: SimInstallApproval,
        reply: oneshot::Sender<Result<(), SimBrokerError>>,
    },
    Request {
        workspace_id: String,
        request: SimRequest,
        reply: oneshot::Sender<Result<SimResult, SimBrokerError>>,
    },
    List {
        repo_id: String,
        reply: oneshot::Sender<Result<Vec<SimDevice>, SimBrokerError>>,
    },
    Boot {
        repo_id: String,
        device: String,
        reply: oneshot::Sender<Result<(), SimBrokerError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), SimBrokerError>>,
    },
}

#[derive(Clone)]
struct ProjectState {
    grants: HashSet<SimGrant>,
    schemes: HashSet<String>,
}

#[derive(Clone, Eq, PartialEq)]
struct ApprovalKey {
    repo_id: String,
    device: String,
    digest: String,
    receipt_hash: [u8; 32],
}

async fn run(
    mut receiver: mpsc::Receiver<Message>,
    drop_root: Option<PathBuf>,
    runner: Arc<dyn SimRunner>,
    auditor: Arc<dyn BrokerAuditor>,
) {
    let mut projects = HashMap::<String, ProjectState>::new();
    let mut sessions = HashMap::<String, String>::new();
    let mut approvals = VecDeque::<ApprovalKey>::new();
    let mut used_receipts = VecDeque::<[u8; 32]>::new();
    while let Some(message) = receiver.recv().await {
        match message {
            Message::Bind {
                workspace_id,
                repo_id,
                reply,
            } => {
                let result =
                    if sessions.len() >= MAX_SESSIONS && !sessions.contains_key(&workspace_id) {
                        Err(SimBrokerError::Capacity)
                    } else {
                        sessions.insert(workspace_id, repo_id);
                        Ok(())
                    };
                let _ = reply.send(result);
            }
            Message::Unbind {
                workspace_id,
                reply,
            } => {
                sessions.remove(&workspace_id);
                let _ = reply.send(Ok(()));
            }
            Message::Configure { config, reply } => {
                let result = configure_project(&mut projects, config);
                let _ = reply.send(result);
            }
            Message::Approve { approval, reply } => {
                let result =
                    approve_install(&projects, &mut approvals, &mut used_receipts, approval);
                let _ = reply.send(result);
            }
            Message::Request {
                workspace_id,
                request,
                reply,
            } => {
                let result = execute_request(
                    &workspace_id,
                    request,
                    ExecutionContext {
                        sessions: &sessions,
                        projects: &projects,
                        approvals: &mut approvals,
                        drop_root: drop_root.as_deref(),
                        runner: runner.as_ref(),
                        auditor: auditor.as_ref(),
                    },
                )
                .await;
                let _ = reply.send(result);
            }
            Message::List { repo_id, reply } => {
                let result = control_list(&repo_id, &projects, runner.as_ref()).await;
                let _ = reply.send(result);
            }
            Message::Boot {
                repo_id,
                device,
                reply,
            } => {
                let result = control_boot(&repo_id, &device, &projects, runner.as_ref()).await;
                let _ = reply.send(result);
            }
            Message::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
                break;
            }
        }
    }
}

fn configure_project(
    projects: &mut HashMap<String, ProjectState>,
    config: SimProjectConfig,
) -> Result<(), SimBrokerError> {
    validate_identifier(&config.repo_id)?;
    if config.registered_schemes.len() > MAX_SCHEMES
        || (projects.len() >= MAX_PROJECTS && !projects.contains_key(&config.repo_id))
    {
        return Err(SimBrokerError::Capacity);
    }
    let mut schemes = HashSet::with_capacity(config.registered_schemes.len());
    for scheme in config.registered_schemes {
        let normalized = scheme.to_ascii_lowercase();
        if normalized != scheme || !valid_scheme(&normalized) {
            return Err(SimBrokerError::InvalidScheme);
        }
        schemes.insert(normalized);
    }
    projects.insert(
        config.repo_id,
        ProjectState {
            grants: config.grants,
            schemes,
        },
    );
    Ok(())
}

fn approve_install(
    projects: &HashMap<String, ProjectState>,
    approvals: &mut VecDeque<ApprovalKey>,
    used_receipts: &mut VecDeque<[u8; 32]>,
    mut approval: SimInstallApproval,
) -> Result<(), SimBrokerError> {
    validate_identifier(&approval.repo_id)?;
    validate_device(&approval.device)?;
    validate_digest(&approval.artifact_digest)?;
    if approval.human_receipt.len() < 16 || approval.human_receipt.len() > 4096 {
        return Err(SimBrokerError::InvalidReceipt);
    }
    let project = projects
        .get(&approval.repo_id)
        .ok_or(SimBrokerError::ProjectNotConfigured)?;
    if !project.grants.contains(&SimGrant::Install) {
        return Err(SimBrokerError::NotGranted);
    }
    let receipt_hash = Sha256::digest(approval.human_receipt.as_bytes()).into();
    approval.human_receipt.clear();
    if used_receipts.contains(&receipt_hash) {
        return Err(SimBrokerError::ReceiptReplay);
    }
    if used_receipts.len() >= MAX_APPROVALS {
        used_receipts.pop_front();
    }
    used_receipts.push_back(receipt_hash);
    if approvals.len() >= MAX_APPROVALS {
        approvals.pop_front();
    }
    approvals.push_back(ApprovalKey {
        repo_id: approval.repo_id,
        device: approval.device,
        digest: approval.artifact_digest,
        receipt_hash,
    });
    Ok(())
}

struct ExecutionContext<'a> {
    sessions: &'a HashMap<String, String>,
    projects: &'a HashMap<String, ProjectState>,
    approvals: &'a mut VecDeque<ApprovalKey>,
    drop_root: Option<&'a Path>,
    runner: &'a dyn SimRunner,
    auditor: &'a dyn BrokerAuditor,
}

async fn execute_request(
    workspace_id: &str,
    request: SimRequest,
    context: ExecutionContext<'_>,
) -> Result<SimResult, SimBrokerError> {
    let ExecutionContext {
        sessions,
        projects,
        approvals,
        drop_root,
        runner,
        auditor,
    } = context;
    let repo_id = sessions
        .get(workspace_id)
        .ok_or(SimBrokerError::UnknownWorkspace)?;
    let project = projects
        .get(repo_id)
        .ok_or(SimBrokerError::ProjectNotConfigured)?;
    match request {
        SimRequest::OpenUrl { device, url } => {
            let classification = validate_openurl(project, &device, &url);
            if let Err(error) = classification {
                audit(
                    auditor,
                    workspace_id,
                    "openurl",
                    None,
                    BrokerAuditStatus::Denied,
                    Some(error.classification()),
                )
                .await?;
                return Err(error);
            }
            let scheme = Url::parse(&url)
                .map(|parsed| format!("{}:", parsed.scheme()))
                .map_err(|_| SimBrokerError::InvalidUrl)?;
            run_audited(
                auditor,
                runner,
                workspace_id,
                "openurl",
                Some(&scheme),
                SimCommand::OpenUrl {
                    device: device.clone(),
                    url,
                },
            )
            .await?;
            Ok(SimResult {
                operation: "openurl".to_owned(),
                device,
            })
        }
        SimRequest::Install { device, digest } => {
            if let Err(error) = validate_device(&device).and_then(|()| validate_digest(&digest)) {
                audit(
                    auditor,
                    workspace_id,
                    "install",
                    None,
                    BrokerAuditStatus::Denied,
                    Some(error.classification()),
                )
                .await?;
                return Err(error);
            }
            if !project.grants.contains(&SimGrant::Install) {
                audit(
                    auditor,
                    workspace_id,
                    "install",
                    None,
                    BrokerAuditStatus::Denied,
                    Some("sim-install-not-granted"),
                )
                .await?;
                return Err(SimBrokerError::NotGranted);
            }
            let Some(approval_index) = approvals.iter().position(|approval| {
                approval.repo_id == *repo_id
                    && approval.device == device
                    && approval.digest == digest
            }) else {
                audit(
                    auditor,
                    workspace_id,
                    "install",
                    None,
                    BrokerAuditStatus::Denied,
                    Some("sim-human-approval-required"),
                )
                .await?;
                return Err(SimBrokerError::ApprovalRequired);
            };
            let _consumed = approvals
                .remove(approval_index)
                .expect("approval index exists");
            let Some(root) = drop_root else {
                audit(
                    auditor,
                    workspace_id,
                    "install",
                    None,
                    BrokerAuditStatus::Denied,
                    Some("sim-install-source-denied"),
                )
                .await?;
                return Err(SimBrokerError::InstallDisabled);
            };
            let app = root.join(repo_id).join(format!("{digest}.app"));
            let expected = digest.clone();
            let checked =
                match tokio::task::spawn_blocking(move || verify_app(&app, &expected).map(|_| app))
                    .await
                {
                    Ok(Ok(app)) => app,
                    Ok(Err(error)) => {
                        audit(
                            auditor,
                            workspace_id,
                            "install",
                            None,
                            BrokerAuditStatus::Denied,
                            Some(error.classification()),
                        )
                        .await?;
                        return Err(error);
                    }
                    Err(_) => {
                        audit(
                            auditor,
                            workspace_id,
                            "install",
                            None,
                            BrokerAuditStatus::Failed,
                            Some("sim-install-source-denied"),
                        )
                        .await?;
                        return Err(SimBrokerError::InvalidApp);
                    }
                };
            run_audited(
                auditor,
                runner,
                workspace_id,
                "install",
                None,
                SimCommand::Install {
                    device: device.clone(),
                    app: checked,
                },
            )
            .await?;
            Ok(SimResult {
                operation: "install".to_owned(),
                device,
            })
        }
    }
}

fn validate_openurl(
    project: &ProjectState,
    device: &str,
    value: &str,
) -> Result<(), SimBrokerError> {
    if !project.grants.contains(&SimGrant::OpenUrl) {
        return Err(SimBrokerError::NotGranted);
    }
    validate_device(device)?;
    if value.len() > 8192 {
        return Err(SimBrokerError::InvalidUrl);
    }
    let url = Url::parse(value).map_err(|_| SimBrokerError::InvalidUrl)?;
    if url.scheme() == "file" || !project.schemes.contains(url.scheme()) {
        return Err(SimBrokerError::InvalidScheme);
    }
    Ok(())
}

async fn run_audited(
    auditor: &dyn BrokerAuditor,
    runner: &dyn SimRunner,
    workspace_id: &str,
    method: &str,
    path: Option<&str>,
    command: SimCommand,
) -> Result<(), SimBrokerError> {
    audit(
        auditor,
        workspace_id,
        method,
        path,
        BrokerAuditStatus::Allowed,
        Some("sim-admitted"),
    )
    .await?;
    match runner.run(command).await {
        Ok(_) => {
            audit(
                auditor,
                workspace_id,
                method,
                path,
                BrokerAuditStatus::Completed,
                None,
            )
            .await
        }
        Err(error) => {
            let status = if matches!(error, SimBrokerError::RunnerTimeout) {
                BrokerAuditStatus::TimedOut
            } else {
                BrokerAuditStatus::Failed
            };
            audit(
                auditor,
                workspace_id,
                method,
                path,
                status,
                Some(error.classification()),
            )
            .await?;
            Err(error)
        }
    }
}

async fn audit(
    auditor: &dyn BrokerAuditor,
    workspace_id: &str,
    method: &str,
    path: Option<&str>,
    status: BrokerAuditStatus,
    classification: Option<&str>,
) -> Result<(), SimBrokerError> {
    auditor
        .record_broker(BrokerAuditEvent {
            workspace_id: workspace_id.to_owned(),
            kind: BrokerAuditKind::Sim,
            method: Some(method.to_owned()),
            path: path.map(str::to_owned),
            status,
            classification: classification.map(str::to_owned),
            bytes: 0,
        })
        .await
        .map_err(|_| SimBrokerError::AuditUnavailable)
}

async fn control_list(
    repo_id: &str,
    projects: &HashMap<String, ProjectState>,
    runner: &dyn SimRunner,
) -> Result<Vec<SimDevice>, SimBrokerError> {
    validate_identifier(repo_id)?;
    if !projects.contains_key(repo_id) {
        return Err(SimBrokerError::ProjectNotConfigured);
    }
    let output = runner.run(SimCommand::List).await?;
    #[derive(Deserialize)]
    struct DeviceList {
        devices: HashMap<String, Vec<RawDevice>>,
    }
    #[derive(Deserialize)]
    struct RawDevice {
        name: String,
        udid: String,
        state: String,
    }
    let parsed: DeviceList =
        serde_json::from_slice(&output.stdout).map_err(|_| SimBrokerError::InvalidRunnerOutput)?;
    let mut devices = parsed
        .devices
        .into_values()
        .flatten()
        .map(|device| SimDevice {
            identifier: device.udid,
            name: device.name,
            state: device.state,
        })
        .collect::<Vec<_>>();
    devices.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    Ok(devices)
}

async fn control_boot(
    repo_id: &str,
    device: &str,
    projects: &HashMap<String, ProjectState>,
    runner: &dyn SimRunner,
) -> Result<(), SimBrokerError> {
    validate_identifier(repo_id)?;
    validate_device(device)?;
    if !projects.contains_key(repo_id) {
        return Err(SimBrokerError::ProjectNotConfigured);
    }
    runner
        .run(SimCommand::Boot {
            device: device.to_owned(),
        })
        .await?;
    Ok(())
}

fn validate_drop_root(root: &Path) -> Result<(), SimBrokerError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    if !root.is_absolute() {
        return Err(SimBrokerError::InsecureDropRoot);
    }
    let metadata = std::fs::symlink_metadata(root).map_err(|_| SimBrokerError::InsecureDropRoot)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(SimBrokerError::InsecureDropRoot);
    }
    Ok(())
}

fn verify_app(path: &Path, expected_digest: &str) -> Result<(), SimBrokerError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| SimBrokerError::InvalidApp)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || path.extension().and_then(|v| v.to_str()) != Some("app")
    {
        return Err(SimBrokerError::InvalidApp);
    }
    if !path.join("Info.plist").is_file() {
        return Err(SimBrokerError::InvalidApp);
    }
    let mut entries = Vec::new();
    collect_entries(path, path, &mut entries)?;
    entries.sort();
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    for relative in entries {
        let full = path.join(&relative);
        let metadata = std::fs::symlink_metadata(&full).map_err(|_| SimBrokerError::InvalidApp)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(SimBrokerError::InvalidApp);
        }
        digest.update(relative.as_os_str().as_encoded_bytes());
        digest.update([0]);
        let mut file = std::fs::File::open(full).map_err(|_| SimBrokerError::InvalidApp)?;
        loop {
            use std::io::Read as _;
            let read = file
                .read(&mut buffer)
                .map_err(|_| SimBrokerError::InvalidApp)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
    }
    let actual = format!("{:x}", digest.finalize());
    if actual != expected_digest {
        return Err(SimBrokerError::DigestMismatch);
    }
    Ok(())
}

fn collect_entries(
    root: &Path,
    current: &Path,
    entries: &mut Vec<PathBuf>,
) -> Result<(), SimBrokerError> {
    if entries.len() >= MAX_APP_ENTRIES {
        return Err(SimBrokerError::InvalidApp);
    }
    for entry in std::fs::read_dir(current).map_err(|_| SimBrokerError::InvalidApp)? {
        let entry = entry.map_err(|_| SimBrokerError::InvalidApp)?;
        let metadata = entry.file_type().map_err(|_| SimBrokerError::InvalidApp)?;
        let path = entry.path();
        if metadata.is_symlink() {
            return Err(SimBrokerError::InvalidApp);
        }
        if metadata.is_dir() {
            collect_entries(root, &path, entries)?;
        } else if metadata.is_file() {
            entries.push(
                path.strip_prefix(root)
                    .map_err(|_| SimBrokerError::InvalidApp)?
                    .to_path_buf(),
            );
            if entries.len() > MAX_APP_ENTRIES {
                return Err(SimBrokerError::InvalidApp);
            }
        } else {
            return Err(SimBrokerError::InvalidApp);
        }
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), SimBrokerError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(SimBrokerError::InvalidIdentifier);
    }
    Ok(())
}

fn validate_device(value: &str) -> Result<(), SimBrokerError> {
    if value == "booted" {
        return Ok(());
    }
    validate_identifier(value)
}

fn validate_digest(value: &str) -> Result<(), SimBrokerError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(SimBrokerError::InvalidDigest);
    }
    Ok(())
}

fn valid_scheme(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')
        })
}

impl SimBrokerError {
    fn classification(&self) -> &'static str {
        match self {
            Self::NotGranted => "sim-not-granted",
            Self::InvalidScheme => "sim-scheme-denied",
            Self::InvalidUrl => "sim-invalid-url",
            Self::ApprovalRequired | Self::ReceiptReplay | Self::InvalidReceipt => {
                "sim-human-approval-required"
            }
            Self::InstallDisabled
            | Self::InsecureDropRoot
            | Self::InvalidApp
            | Self::DigestMismatch => "sim-install-source-denied",
            _ => "sim-request-rejected",
        }
    }
}

#[derive(Debug, Error)]
pub enum SimBrokerError {
    #[error("simulator broker is not configured for this project")]
    ProjectNotConfigured,
    #[error("simulator operation is not granted")]
    NotGranted,
    #[error("simulator URL scheme is not registered for this project")]
    InvalidScheme,
    #[error("simulator URL is invalid")]
    InvalidUrl,
    #[error("simulator install requires a one-use human approval")]
    ApprovalRequired,
    #[error("simulator approval receipt was already used")]
    ReceiptReplay,
    #[error("simulator approval receipt is invalid")]
    InvalidReceipt,
    #[error("simulator install is disabled")]
    InstallDisabled,
    #[error("simulator drop root is insecure")]
    InsecureDropRoot,
    #[error("simulator application bundle is invalid")]
    InvalidApp,
    #[error("simulator application digest does not match")]
    DigestMismatch,
    #[error("simulator artifact digest is invalid")]
    InvalidDigest,
    #[error("simulator device identifier is invalid")]
    InvalidIdentifier,
    #[error("simulator broker capacity is exhausted")]
    Capacity,
    #[error("simulator workspace is not installed")]
    UnknownWorkspace,
    #[error("simulator runner failed")]
    RunnerFailed,
    #[error("simulator runner timed out")]
    RunnerTimeout,
    #[error("simulator runner returned invalid output")]
    InvalidRunnerOutput,
    #[error("simulator audit is unavailable")]
    AuditUnavailable,
    #[error("simulator broker stopped")]
    Stopped,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        os::unix::fs::PermissionsExt as _,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::*;
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingRunner {
        commands: Mutex<Vec<SimCommand>>,
    }

    #[async_trait]
    impl SimRunner for RecordingRunner {
        async fn run(&self, command: SimCommand) -> Result<SimCommandOutput, SimBrokerError> {
            self.commands.lock().expect("commands").push(command);
            Ok(SimCommandOutput {
                stdout: br#"{"devices":{}}"#.to_vec(),
            })
        }
    }
    struct FailingRunner {
        timeout: bool,
    }

    #[async_trait]
    impl SimRunner for FailingRunner {
        async fn run(&self, _command: SimCommand) -> Result<SimCommandOutput, SimBrokerError> {
            if self.timeout {
                Err(SimBrokerError::RunnerTimeout)
            } else {
                Err(SimBrokerError::RunnerFailed)
            }
        }
    }

    #[derive(Default)]
    struct RecordingAuditor {
        failed: AtomicBool,
        events: Mutex<Vec<BrokerAuditEvent>>,
    }

    #[async_trait]
    impl BrokerAuditor for RecordingAuditor {
        async fn record_broker(&self, event: BrokerAuditEvent) -> Result<(), crate::AuditError> {
            if self.failed.load(Ordering::SeqCst) {
                return Err(crate::AuditError("unavailable".to_owned()));
            }
            self.events.lock().expect("events").push(event);
            Ok(())
        }
    }

    fn fixture_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("cowshed-sim-{label}-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).expect("create root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure root");
        root
    }

    fn project(grants: impl IntoIterator<Item = SimGrant>) -> SimProjectConfig {
        SimProjectConfig {
            repo_id: "repo-a".to_owned(),
            grants: grants.into_iter().collect(),
            registered_schemes: HashSet::from(["cowshed-demo".to_owned()]),
        }
    }

    fn app_digest(contents: &[u8]) -> String {
        let mut digest = Sha256::new();
        digest.update(b"Info.plist");
        digest.update([0]);
        digest.update(contents);
        format!("{:x}", digest.finalize())
    }

    #[tokio::test]
    async fn scheme_drop_receipt_and_replay_are_fenced() {
        let root = fixture_root("gates");
        let runner = Arc::new(RecordingRunner::default());
        let auditor = Arc::new(RecordingAuditor::default());
        let (handle, task) =
            SimBrokerHandle::start(Some(root.clone()), 8, runner.clone(), auditor.clone())
                .expect("start broker");
        handle
            .configure(project([SimGrant::OpenUrl, SimGrant::Install]))
            .await
            .expect("configure");
        handle
            .bind_session("ws-a".to_owned(), "repo-a".to_owned())
            .await
            .expect("bind");

        let denied = handle
            .request(
                "ws-a".to_owned(),
                SimRequest::OpenUrl {
                    device: "booted".to_owned(),
                    url: "https://example.test/private".to_owned(),
                },
            )
            .await;
        assert!(matches!(denied, Err(SimBrokerError::InvalidScheme)));
        handle
            .request(
                "ws-a".to_owned(),
                SimRequest::OpenUrl {
                    device: "booted".to_owned(),
                    url: "cowshed-demo://open/item".to_owned(),
                },
            )
            .await
            .expect("allowed openurl");

        let contents = b"verified bundle";
        let digest = app_digest(contents);
        let app = root.join("repo-a").join(format!("{digest}.app"));
        std::fs::create_dir_all(&app).expect("create app");
        std::fs::write(app.join("Info.plist"), contents).expect("write app");
        let approval = SimInstallApproval {
            repo_id: "repo-a".to_owned(),
            device: "booted".to_owned(),
            artifact_digest: digest.clone(),
            human_receipt: "human-confirmation-0001".to_owned(),
        };
        handle.approve(approval.clone()).await.expect("approve");
        assert!(matches!(
            handle.approve(approval).await,
            Err(SimBrokerError::ReceiptReplay)
        ));
        handle
            .request(
                "ws-a".to_owned(),
                SimRequest::Install {
                    device: "booted".to_owned(),
                    digest: digest.clone(),
                },
            )
            .await
            .expect("install");
        assert!(matches!(
            handle
                .request(
                    "ws-a".to_owned(),
                    SimRequest::Install {
                        device: "booted".to_owned(),
                        digest,
                    },
                )
                .await,
            Err(SimBrokerError::ApprovalRequired)
        ));

        {
            let commands = runner.commands.lock().expect("commands");
            assert!(matches!(
                &commands[0],
                SimCommand::OpenUrl { device, url }
                    if device == "booted" && url == "cowshed-demo://open/item"
            ));
            assert!(matches!(
                &commands[1],
                SimCommand::Install { device, app }
                    if device == "booted" && app.starts_with(root.join("repo-a"))
            ));
        }
        {
            let events = auditor.events.lock().expect("events");
            assert!(events.iter().all(|event| {
                event
                    .path
                    .as_deref()
                    .is_none_or(|path| path == "cowshed-demo:")
            }));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.status == BrokerAuditStatus::Completed)
                    .count(),
                2
            );
        }
        handle.shutdown().await;
        task.await.expect("join");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn unavailable_audit_prevents_runner_side_effect() {
        let root = fixture_root("audit");
        let runner = Arc::new(RecordingRunner::default());
        let auditor = Arc::new(RecordingAuditor::default());
        auditor.failed.store(true, Ordering::SeqCst);
        let (handle, task) = SimBrokerHandle::start(Some(root.clone()), 4, runner.clone(), auditor)
            .expect("start broker");
        handle
            .configure(project([SimGrant::OpenUrl]))
            .await
            .expect("configure");
        handle
            .bind_session("ws-a".to_owned(), "repo-a".to_owned())
            .await
            .expect("bind");
        assert!(matches!(
            handle
                .request(
                    "ws-a".to_owned(),
                    SimRequest::OpenUrl {
                        device: "booted".to_owned(),
                        url: "cowshed-demo://open".to_owned(),
                    },
                )
                .await,
            Err(SimBrokerError::AuditUnavailable)
        ));
        {
            let commands = runner.commands.lock().expect("commands");
            assert!(commands.is_empty());
        }
        handle.shutdown().await;
        task.await.expect("join");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runner_failure_and_timeout_emit_one_terminal_outcome() {
        for (timeout, terminal) in [
            (false, BrokerAuditStatus::Failed),
            (true, BrokerAuditStatus::TimedOut),
        ] {
            let root = fixture_root(if timeout { "timeout" } else { "failure" });
            let runner = Arc::new(FailingRunner { timeout });
            let auditor = Arc::new(RecordingAuditor::default());
            let (handle, task) =
                SimBrokerHandle::start(Some(root.clone()), 4, runner, auditor.clone())
                    .expect("start broker");
            handle
                .configure(project([SimGrant::OpenUrl]))
                .await
                .expect("configure");
            handle
                .bind_session("ws-a".to_owned(), "repo-a".to_owned())
                .await
                .expect("bind");
            let result = handle
                .request(
                    "ws-a".to_owned(),
                    SimRequest::OpenUrl {
                        device: "booted".to_owned(),
                        url: "cowshed-demo://open".to_owned(),
                    },
                )
                .await;
            if timeout {
                assert!(matches!(result, Err(SimBrokerError::RunnerTimeout)));
            } else {
                assert!(matches!(result, Err(SimBrokerError::RunnerFailed)));
            }
            {
                let events = auditor.events.lock().expect("events");
                let statuses = events.iter().map(|event| event.status).collect::<Vec<_>>();
                assert_eq!(statuses, vec![BrokerAuditStatus::Allowed, terminal]);
            }
            handle.shutdown().await;
            task.await.expect("join");
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[test]
    fn data_plane_schema_rejects_controller_and_unknown_verbs() {
        for request in [
            r#"{"verb":"list"}"#,
            r#"{"verb":"boot","device":"booted"}"#,
            r#"{"verb":"unknown"}"#,
            r#"{"verb":"open-url","device":"booted","url":"x://y","extra":true}"#,
        ] {
            assert!(serde_json::from_str::<SimRequest>(request).is_err());
        }
    }
}
