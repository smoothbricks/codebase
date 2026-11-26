//! Host-side scratch provider hook (04_sandbox.md, 06_cli.md).
//!
//! When main's `.cowshed.toml` declares `[scratch] provider`, the supervisor spawns one provider
//! process on the host (outside every sandbox) per exec host and per sandboxed one-shot child.
//! The provider answers one JSON line naming a 0700 directory it holds until EOF on its stdin; on
//! any refusal, timeout, protocol error or invalid directory, the host falls back to the store
//! `TMPDIR` and the reason is printed once per supervisor per distinct reason.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use cowshed_os::fork_lock::Spawn as _;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, ChildStdin, Command};

use crate::exec::prepare_child_descriptors;
use crate::repository::is_lexically_canonical;
use crate::sandbox::{
    SandboxConfig, cowshed_root, sandbox_runtime_dir, sandbox_runtime_link,
    shared_daemon_runtime_dir, shared_daemon_runtime_link,
};

/// Bound on the provider's first JSON line on stdout. A provider may create its storage before it
/// answers: the RAM provider's cold create (`diskutil apfs createContainer`, then `addVolume`)
/// answered after 7.4, 7.4, 7.4 and 13.6 s at load 11-13 (2026-11-26). A bound below that abandons
/// every cold start, whose provider then creates the volume and detaches it again on EOF, so the
/// bound is 30 s: twice the slowest cold create measured, while a warm provider answers in ~0.1 s.
pub const SCRATCH_PROVIDER_BOUND: Duration = Duration::from_secs(30);

/// Bound on the provider settling and exiting after stdin EOF.
pub const SCRATCH_SETTLE_BOUND: Duration = Duration::from_secs(5);

/// Maximum bytes accepted on the provider's first stdout line so a broken provider cannot exhaust
/// supervisor memory.
const MAX_PROVIDER_LINE_BYTES: usize = 64 * 1024;

/// Maximum bytes of one provider stderr line relayed to the supervisor log.
const MAX_STDERR_LINE_BYTES: usize = 8 * 1024;

/// Host identity variables forwarded to the provider when set in the supervisor's environment.
const FORWARDED_ENV: [&str; 5] = ["HOME", "USER", "LOGNAME", "TERM", "LANG"];

/// Environment variable naming the workspace whose host asks for a scratch directory.
pub const SCRATCH_WORKSPACE_ENV: &str = "COWSHED_SCRATCH_WORKSPACE";

/// Environment variable naming the serving supervisor's socket.
pub const SUPERVISOR_SOCKET_ENV: &str = "COWSHED_SUPERVISOR_SOCKET";

/// Broad shared roots a scratch parent must never name: the profile emits a subpath deny over the
/// parent before carving back the scratch directory, so denying any of these would close the host
/// or shared user trees.
const FORBIDDEN_PARENTS: [&str; 10] = [
    "/",
    "/tmp",
    "/private",
    "/private/tmp",
    "/var",
    "/private/var",
    "/var/folders",
    "/private/var/folders",
    "/Volumes",
    "/Users",
];

/// System roots a scratch parent must never enclose.
const SYSTEM_ROOTS: [&str; 15] = [
    "/Applications",
    "/Library",
    "/System",
    "/bin",
    "/dev",
    "/etc",
    "/lib",
    "/lib64",
    "/nix",
    "/nix/store",
    "/opt",
    "/private/var/select",
    "/sbin",
    "/usr",
    "/var/select",
];

/// Configuration for one workspace supervisor's scratch provider, derived strictly from main's
/// `.cowshed.toml`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScratchProviderConfig {
    /// Literal argv of the provider command; never empty.
    pub provider: Vec<String>,
    /// Main's checkout root, used as the provider's working directory.
    pub main_root: PathBuf,
    /// Workspace name reported in `COWSHED_SCRATCH_WORKSPACE`.
    pub workspace_name: String,
    /// Supervisor socket reported in `COWSHED_SUPERVISOR_SOCKET` when served on a socket.
    pub supervisor_socket: Option<PathBuf>,
    /// Project root in the store (forbidden for scratch directories), when known.
    pub project_root: Option<PathBuf>,
}

impl ScratchProviderConfig {
    pub fn new(
        provider: Vec<String>,
        main_root: PathBuf,
        workspace_name: String,
        supervisor_socket: Option<PathBuf>,
        project_root: Option<PathBuf>,
    ) -> Self {
        Self {
            provider,
            main_root,
            workspace_name,
            supervisor_socket,
            project_root,
        }
    }
}

/// Time bounds for provider handshake and settlement; overridden in tests so no test sleeps for
/// the production bounds.
#[derive(Clone, Copy, Debug)]
struct Bounds {
    handshake: Duration,
    settle: Duration,
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            handshake: SCRATCH_PROVIDER_BOUND,
            settle: SCRATCH_SETTLE_BOUND,
        }
    }
}

/// Per-supervisor scratch provider client. Never global: each supervisor owns its own instance
/// and deduplicates fallback reasons once per supervisor per distinct reason.
pub(super) struct ScratchClient {
    config: ScratchProviderConfig,
    bounds: Bounds,
    reported_reasons: Mutex<BTreeSet<String>>,
}

/// Outcome of asking the provider for one host's or one-shot child's scratch directory.
pub(super) enum ScratchOutcome {
    Lease(ScratchLease),
    Fallback { reason: String },
}

/// One live scratch directory and the provider process holding it until the host or one-shot
/// child has exited and retired its processes.
pub(super) struct ScratchLease {
    directory: PathBuf,
    child: Option<Box<ProviderChild>>,
    settle_bound: Duration,
}

struct ProviderChild {
    stdin: Option<ChildStdin>,
    child: Child,
}

/// Ensures an in-flight provider child is killed and reaped off-actor if `acquire` is cancelled
/// mid-handshake.
struct InFlightProvider(Option<ProviderChild>);

impl InFlightProvider {
    fn take(&mut self) -> Option<ProviderChild> {
        self.0.take()
    }
}

impl Drop for InFlightProvider {
    fn drop(&mut self) {
        if let Some(child) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(child.kill_and_reap());
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderReply {
    directory: Option<String>,
    unavailable: Option<String>,
}

enum ParsedReply {
    Directory(PathBuf),
    Unavailable(String),
}

impl ScratchLease {
    pub(super) fn directory(&self) -> &Path {
        &self.directory
    }

    /// Close the provider's stdin and reap it within `settle_bound`. Called off the supervisor's
    /// actor, only after the host or one-shot child using this directory has retired its
    /// processes.
    pub(super) async fn settle(mut self) {
        if let Some(child) = self.child.take() {
            child.settle(self.settle_bound).await;
        }
    }
}

impl Drop for ScratchLease {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return;
        };
        let bound = self.settle_bound;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(child.settle(bound));
        }
    }
}

impl ProviderChild {
    async fn settle(mut self, bound: Duration) {
        // EOF on stdin signals the provider to settle the directory and exit.
        drop(self.stdin.take());
        let deadline = cowshed_os::deadline::start(
            "settle scratch provider",
            cowshed_os::deadline::Bound::new(cowshed_os::deadline::LatencyClass::Reap, bound),
        );
        match cowshed_os::deadline::reap_async(
            "scratch provider",
            &mut self.child,
            &deadline,
            cowshed_os::deadline::Progress::Nothing,
        )
        .await
        {
            Ok(_status) => {}
            Err(cowshed_os::deadline::ReapError::Wait { source, .. }) => {
                cowshed_os::eprintln!("cowshed: scratch provider wait failed: {source}");
            }
            Err(cowshed_os::deadline::ReapError::Expired(_)) => {
                cowshed_os::eprintln!(
                    "cowshed: scratch provider did not exit within {}ms after stdin EOF; killing",
                    bound.as_millis()
                );
                let _ = self.child.start_kill();
                let reap = cowshed_os::deadline::start(
                    "reap killed scratch provider",
                    cowshed_os::deadline::REAP,
                );
                let _ = cowshed_os::deadline::reap_async(
                    "scratch provider",
                    &mut self.child,
                    &reap,
                    cowshed_os::deadline::Progress::Nothing,
                )
                .await;
            }
        }
    }

    async fn kill_and_reap(mut self) {
        drop(self.stdin.take());
        let _ = self.child.start_kill();
        let reap = cowshed_os::deadline::start("reap scratch provider", cowshed_os::deadline::REAP);
        let _ = cowshed_os::deadline::reap_async(
            "scratch provider",
            &mut self.child,
            &reap,
            cowshed_os::deadline::Progress::Nothing,
        )
        .await;
    }
}

impl ScratchClient {
    pub(super) fn new(config: ScratchProviderConfig) -> Self {
        Self {
            config,
            bounds: Bounds::default(),
            reported_reasons: Mutex::new(BTreeSet::new()),
        }
    }

    #[cfg(test)]
    fn with_bounds(config: ScratchProviderConfig, bounds: Bounds) -> Self {
        Self {
            config,
            bounds,
            reported_reasons: Mutex::new(BTreeSet::new()),
        }
    }

    /// Format the fallback notice if this supervisor has not yet reported `reason` to any job's
    /// stderr. Calling this marks `reason` reported for the remainder of this supervisor's life.
    pub(super) fn take_notice(&self, reason: &str, store_temp_dir: &Path) -> Option<String> {
        let mut reported = self
            .reported_reasons
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !reported.insert(reason.to_owned()) {
            return None;
        }
        Some(format!(
            "cowshed: scratch provider {reason}; TMPDIR stays {}",
            store_temp_dir.display()
        ))
    }

    /// Spawn the provider on the host (outside every sandbox), read its one JSON line within
    /// `SCRATCH_PROVIDER_BOUND`, and validate the returned directory against `sandbox`. Never
    /// blocks the supervisor actor and never fails a job.
    pub(super) async fn acquire(&self, sandbox: &SandboxConfig) -> ScratchOutcome {
        let Some((program, arguments)) = self.config.provider.split_first() else {
            return ScratchOutcome::Fallback {
                reason: "has an empty command".to_owned(),
            };
        };

        let host_home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| sandbox.home.clone());
        let path_dirs = crate::declared_land::bootstrap_path(&host_home);
        let bootstrap_path = match std::env::join_paths(path_dirs) {
            Ok(path) => path,
            Err(error) => {
                return ScratchOutcome::Fallback {
                    reason: format!("cannot construct host bootstrap PATH: {error}"),
                };
            }
        };

        let mut command = Command::new(program);
        command
            .args(arguments)
            .current_dir(&self.config.main_root)
            .env_clear()
            .env("PATH", &bootstrap_path)
            .env(SCRATCH_WORKSPACE_ENV, &self.config.workspace_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        for name in FORWARDED_ENV {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        if let Some(socket) = &self.config.supervisor_socket {
            command.env(SUPERVISOR_SOCKET_ENV, socket);
        }
        if let Err(error) = prepare_child_descriptors(command.as_std_mut()) {
            return ScratchOutcome::Fallback {
                reason: format!("cannot prepare provider descriptors: {}", error.source),
            };
        }

        let handle = tokio::runtime::Handle::current();
        let program_label = program.clone();
        let spawned = tokio::task::spawn_blocking(move || {
            let _enter = handle.enter();
            command.spawn_locked()
        })
        .await;
        let mut child = match spawned {
            Ok(Ok(child)) => child,
            Ok(Err(error)) => {
                return ScratchOutcome::Fallback {
                    reason: format!("failed to start ({program_label}): {error}"),
                };
            }
            Err(error) => {
                return ScratchOutcome::Fallback {
                    reason: format!("provider spawn task failed ({program_label}): {error}"),
                };
            }
        };

        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(relay_stderr(stderr));
        }
        let mut in_flight = InFlightProvider(Some(ProviderChild { stdin, child }));
        let Some(stdout) = stdout else {
            if let Some(holder) = in_flight.take() {
                holder.kill_and_reap().await;
            }
            return ScratchOutcome::Fallback {
                reason: "has no stdout pipe".to_owned(),
            };
        };

        let line = match tokio::time::timeout(
            self.bounds.handshake,
            read_bounded_line(stdout, MAX_PROVIDER_LINE_BYTES),
        )
        .await
        {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None)) => {
                let Some(mut holder) = in_flight.take() else {
                    return ScratchOutcome::Fallback {
                        reason: "closed stdout without answering".to_owned(),
                    };
                };
                let deadline = cowshed_os::deadline::start(
                    "reap exited scratch provider",
                    cowshed_os::deadline::Bound::new(
                        cowshed_os::deadline::LatencyClass::Reap,
                        self.bounds.settle,
                    ),
                );
                let reason = match cowshed_os::deadline::reap_async(
                    "scratch provider",
                    &mut holder.child,
                    &deadline,
                    cowshed_os::deadline::Progress::Nothing,
                )
                .await
                {
                    Ok(status) => format!("exited ({status}) without answering"),
                    Err(cowshed_os::deadline::ReapError::Wait { source, .. }) => {
                        format!("closed stdout without answering ({source})")
                    }
                    Err(cowshed_os::deadline::ReapError::Expired(_)) => {
                        holder.kill_and_reap().await;
                        "closed stdout without answering".to_owned()
                    }
                };
                return ScratchOutcome::Fallback { reason };
            }
            Ok(Err(error)) => {
                if let Some(holder) = in_flight.take() {
                    holder.kill_and_reap().await;
                }
                return ScratchOutcome::Fallback {
                    reason: format!("failed to read reply: {error}"),
                };
            }
            Err(_) => {
                if let Some(holder) = in_flight.take() {
                    holder.kill_and_reap().await;
                }
                return ScratchOutcome::Fallback {
                    reason: format!(
                        "timed out after {}ms waiting for reply",
                        self.bounds.handshake.as_millis()
                    ),
                };
            }
        };

        let directory = match parse_reply(&line) {
            Ok(ParsedReply::Directory(directory)) => directory,
            Ok(ParsedReply::Unavailable(reason)) => {
                if let Some(holder) = in_flight.take() {
                    tokio::spawn(holder.settle(self.bounds.settle));
                }
                return ScratchOutcome::Fallback {
                    reason: format!("unavailable: {reason}"),
                };
            }
            Err(reason) => {
                if let Some(holder) = in_flight.take() {
                    tokio::spawn(holder.kill_and_reap());
                }
                return ScratchOutcome::Fallback { reason };
            }
        };

        let sandbox_snapshot = sandbox.clone();
        let project_root = self.config.project_root.clone();
        let validated_dir = directory.clone();
        let validation = tokio::task::spawn_blocking(move || {
            validate_scratch_directory(&validated_dir, &sandbox_snapshot, project_root.as_deref())
        })
        .await;

        match validation {
            Ok(Ok(())) => ScratchOutcome::Lease(ScratchLease {
                directory,
                child: in_flight.take().map(Box::new),
                settle_bound: self.bounds.settle,
            }),
            Ok(Err(reason)) => {
                if let Some(holder) = in_flight.take() {
                    tokio::spawn(holder.settle(self.bounds.settle));
                }
                ScratchOutcome::Fallback { reason }
            }
            Err(error) => {
                if let Some(holder) = in_flight.take() {
                    tokio::spawn(holder.settle(self.bounds.settle));
                }
                ScratchOutcome::Fallback {
                    reason: format!("directory validation task failed: {error}"),
                }
            }
        }
    }
}

async fn read_bounded_line(
    stdout: tokio::process::ChildStdout,
    limit: usize,
) -> io::Result<Option<String>> {
    let mut reader = BufReader::new(stdout);
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }
        if let Some(newline) = available.iter().position(|&byte| byte == b'\n') {
            if bytes.len() + newline > limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("reply exceeds {limit} bytes"),
                ));
            }
            bytes.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            break;
        }
        let take = available.len();
        if bytes.len() + take > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("reply exceeds {limit} bytes"),
            ));
        }
        bytes.extend_from_slice(available);
        reader.consume(take);
    }
    if bytes.ends_with(b"\r") {
        bytes.pop();
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn relay_stderr(stderr: tokio::process::ChildStderr) {
    let mut reader = BufReader::new(stderr);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let mut exceeded = false;
        loop {
            let available = match reader.fill_buf().await {
                Ok(available) => available,
                Err(_) => return,
            };
            if available.is_empty() {
                if !buffer.is_empty() {
                    let line = String::from_utf8_lossy(&buffer);
                    cowshed_os::eprintln!("cowshed: scratch provider stderr: {line}");
                }
                return;
            }
            if let Some(newline) = available.iter().position(|&byte| byte == b'\n') {
                if !exceeded {
                    let room = MAX_STDERR_LINE_BYTES.saturating_sub(buffer.len());
                    let copy = newline.min(room);
                    buffer.extend_from_slice(&available[..copy]);
                }
                reader.consume(newline + 1);
                break;
            }
            let take = available.len();
            if !exceeded {
                let room = MAX_STDERR_LINE_BYTES.saturating_sub(buffer.len());
                let copy = take.min(room);
                buffer.extend_from_slice(&available[..copy]);
                if copy < take {
                    exceeded = true;
                }
            }
            reader.consume(take);
        }
        if buffer.ends_with(b"\r") {
            buffer.pop();
        }
        let line = String::from_utf8_lossy(&buffer);
        cowshed_os::eprintln!("cowshed: scratch provider stderr: {line}");
    }
}

fn parse_reply(line: &str) -> Result<ParsedReply, String> {
    let reply: ProviderReply =
        serde_json::from_str(line).map_err(|error| format!("returned malformed JSON: {error}"))?;
    match (reply.directory, reply.unavailable) {
        (Some(directory), None) if !directory.is_empty() => {
            Ok(ParsedReply::Directory(PathBuf::from(directory)))
        }
        (None, Some(reason)) if !reason.trim().is_empty() => {
            Ok(ParsedReply::Unavailable(reason.trim().to_owned()))
        }
        _ => Err(
            "returned invalid JSON: expected {\"directory\":\"/...\"} or {\"unavailable\":\"...\"}"
                .to_owned(),
        ),
    }
}

/// Validate a provider-supplied scratch directory against the contract and sandbox invariants:
/// - absolute, lexically canonical, no NUL, has a parent;
/// - no symlink in any component;
/// - leaf is a directory owned by the supervisor's effective uid with exact mode 0700;
/// - not equal to or beneath workspace mount, mount root, store root, project root, exec temp dir
///   or `/private/cowshed`;
/// - parent is not a forbidden broad root and is not equal to or an ancestor of any path the
///   sandbox profile grants or relies on (so denying `(subpath "<parent>")` never closes a grant).
fn validate_scratch_directory(
    directory: &Path,
    sandbox: &SandboxConfig,
    project_root: Option<&Path>,
) -> Result<(), String> {
    let os_bytes = directory.as_os_str().as_encoded_bytes();
    if os_bytes.contains(&0) {
        return Err("returned directory containing NUL".to_owned());
    }
    if !is_lexically_canonical(directory) {
        return Err(format!(
            "returned non-canonical directory {}",
            directory.display()
        ));
    }
    let Some(parent) = directory.parent() else {
        return Err(format!(
            "returned directory without parent: {}",
            directory.display()
        ));
    };

    // Walk every component from `/` down to `directory`, rejecting any symlink.
    let mut prefix = PathBuf::new();
    let mut leaf_metadata = None;
    for component in directory.components() {
        match component {
            Component::RootDir => {
                prefix.push(Component::RootDir.as_os_str());
                continue;
            }
            Component::Normal(part) => prefix.push(part),
            _ => {
                return Err(format!(
                    "returned non-canonical directory {}",
                    directory.display()
                ));
            }
        }
        let metadata = std::fs::symlink_metadata(&prefix).map_err(|error| {
            format!(
                "cannot stat {} in scratch directory {}: {error}",
                prefix.display(),
                directory.display()
            )
        })?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "returned directory with symlink component {}",
                prefix.display()
            ));
        }
        leaf_metadata = Some(metadata);
    }

    let metadata =
        leaf_metadata.ok_or_else(|| format!("returned root directory {}", directory.display()))?;
    if !metadata.is_dir() {
        return Err(format!(
            "returned path is not a directory: {}",
            directory.display()
        ));
    }
    // SAFETY: geteuid is a pure kernel query with no preconditions.
    let expected_uid = unsafe { libc::geteuid() };
    if metadata.uid() != expected_uid {
        return Err(format!(
            "returned directory {} owned by uid {} (expected {expected_uid})",
            directory.display(),
            metadata.uid()
        ));
    }
    let mode = metadata.mode() & 0o7777;
    if mode != 0o700 {
        return Err(format!(
            "returned directory {} with mode {mode:04o} (expected 0700)",
            directory.display()
        ));
    }

    validate_scratch_paths(directory, parent, sandbox, project_root)
}

/// Pure path-relationship checks between `directory`, `parent`, and `sandbox`.
fn validate_scratch_paths(
    directory: &Path,
    parent: &Path,
    sandbox: &SandboxConfig,
    project_root: Option<&Path>,
) -> Result<(), String> {
    // Directory must not lie at or below protected roots.
    for (label, root) in [
        ("workspace mount", sandbox.workspace_mount.as_path()),
        ("mount root", sandbox.mount_root.as_path()),
        ("store root", sandbox.store_root.as_path()),
        ("exec temp dir", sandbox.exec_temp_dir.as_path()),
        ("cowshed root", cowshed_root()),
    ]
    .into_iter()
    .chain(project_root.map(|root| ("project root", root)))
    {
        if directory.starts_with(root) || root.starts_with(directory) {
            return Err(format!(
                "returned directory {} intersecting {label} {}",
                directory.display(),
                root.display()
            ));
        }
    }

    // Parent must not be a broad shared root or HOME.
    for forbidden in FORBIDDEN_PARENTS
        .into_iter()
        .map(Path::new)
        .chain(std::iter::once(sandbox.home.as_path()))
    {
        if parent == forbidden {
            return Err(format!(
                "returned directory {} under forbidden parent {}",
                directory.display(),
                parent.display()
            ));
        }
    }

    // Because Seatbelt emits `(deny file-read* file-write* (subpath "<parent>"))`, the parent
    // must not equal or enclose any path the profile grants or depends on.
    let runtime_link = sandbox_runtime_link(sandbox);
    let private_runtime_link =
        PathBuf::from("/private").join(runtime_link.strip_prefix("/").unwrap_or(&runtime_link));
    let shared_runtime_link = shared_daemon_runtime_link(sandbox);
    let private_shared_runtime_link = PathBuf::from("/private").join(
        shared_runtime_link
            .strip_prefix("/")
            .unwrap_or(&shared_runtime_link),
    );
    let runtime_dir = sandbox_runtime_dir(sandbox);
    let shared_runtime_dir = shared_daemon_runtime_dir(sandbox);
    let installed_bin = crate::host_dirs::installed_binary_directory(&sandbox.home);

    let mut protected_targets: Vec<&Path> = vec![
        &sandbox.workspace_mount,
        &sandbox.exec_temp_dir,
        &sandbox.store_root,
        &sandbox.mount_root,
        &sandbox.home,
        cowshed_root(),
        &runtime_dir,
        &shared_runtime_dir,
        &runtime_link,
        &private_runtime_link,
        &shared_runtime_link,
        &private_shared_runtime_link,
        &installed_bin,
    ];
    if let Some(root) = project_root {
        protected_targets.push(root);
    }
    if let Some(volume) = &sandbox.build_volume_mount {
        protected_targets.push(volume);
    }
    if let Some(repo) = &sandbox.git_worktree_repository {
        protected_targets.push(repo);
    }
    if let Some(main) = &sandbox.main_mount {
        protected_targets.push(main);
    }
    if let Some(cpu_pool) = &sandbox.cpu_pool_directory {
        protected_targets.push(cpu_pool);
    }
    for path in &sandbox.program_directories {
        protected_targets.push(path);
    }
    for path in &sandbox.grants.read {
        protected_targets.push(path);
    }
    for path in &sandbox.grants.write {
        protected_targets.push(path);
    }
    for path in &sandbox.allowed_unix_sockets {
        protected_targets.push(path);
    }
    for grant in &sandbox.capabilities.contribution.grants {
        protected_targets.push(&grant.path);
    }
    for cache in &sandbox.capabilities.contribution.shared_caches {
        protected_targets.push(&cache.path);
    }
    for root in SYSTEM_ROOTS {
        protected_targets.push(Path::new(root));
    }

    for target in protected_targets {
        if target.starts_with(parent) {
            return Err(format!(
                "returned directory {} whose parent {} encloses {}",
                directory.display(),
                parent.display(),
                target.display()
            ));
        }
    }

    Ok(())
}

/// Overlay `TMPDIR` for a warm command when its host holds a scratch lease.
pub(super) fn push_tmpdir_overlay(
    overlay: &mut Vec<(OsString, OsString)>,
    lease: Option<&ScratchLease>,
) {
    if let Some(lease) = lease {
        overlay.push((
            OsString::from("TMPDIR"),
            lease.directory().as_os_str().to_owned(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::RunSandboxMode;
    use std::os::unix::fs::PermissionsExt as _;

    fn test_sandbox(root: &Path) -> SandboxConfig {
        let mut config = crate::sandbox::tests::config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.mount_root = root.join("mounts");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.store_root = root.join("store");
        config
    }

    fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[test]
    fn parse_reply_accepts_only_exact_directory_or_unavailable() {
        assert!(matches!(
            parse_reply(r#"{"directory":"/Volumes/scratch/h00001"}"#),
            Ok(ParsedReply::Directory(path)) if path == Path::new("/Volumes/scratch/h00001")
        ));
        assert!(matches!(
            parse_reply(r#"{"unavailable":"no volume"}"#),
            Ok(ParsedReply::Unavailable(reason)) if reason == "no volume"
        ));
        for bad in [
            "{}",
            r#"{"directory":""}"#,
            r#"{"unavailable":"   "}"#,
            r#"{"directory":"/a","unavailable":"b"}"#,
            r#"{"directory":"/a","extra":1}"#,
            "not json",
        ] {
            assert!(parse_reply(bad).is_err(), "expected error for {bad}");
        }
    }

    #[test]
    fn validate_rejects_symlinks_bad_mode_and_forbidden_parents() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-sv");
        let root_path = std::fs::canonicalize(&*root).unwrap();
        let sandbox = test_sandbox(&root_path);

        let parent =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-sp");
        let parent_path = std::fs::canonicalize(&*parent).unwrap();

        let good = parent_path.join("h00001");
        std::fs::create_dir(&good).unwrap();
        std::fs::set_permissions(&good, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            validate_scratch_directory(&good, &sandbox, None),
            Ok(()),
            "valid 0700 directory under isolated parent must pass"
        );

        // Mode 0755 refused.
        let wide = parent_path.join("h00002");
        std::fs::create_dir(&wide).unwrap();
        std::fs::set_permissions(&wide, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            validate_scratch_directory(&wide, &sandbox, None)
                .unwrap_err()
                .contains("0700")
        );

        // Symlink component refused.
        let link = parent_path.join("h00003");
        std::os::unix::fs::symlink(&good, &link).unwrap();
        assert!(
            validate_scratch_directory(&link, &sandbox, None)
                .unwrap_err()
                .contains("symlink")
        );

        // Relative path refused.
        assert!(validate_scratch_directory(Path::new("relative/dir"), &sandbox, None).is_err());

        // Parent enclosing workspace mount or forbidden broad root refused.
        assert!(
            validate_scratch_paths(
                &PathBuf::from("/private/tmp/h00001"),
                Path::new("/private/tmp"),
                &sandbox,
                None
            )
            .is_err()
        );
        assert!(
            validate_scratch_paths(&sandbox.home.join("h00001"), &sandbox.home, &sandbox, None)
                .is_err()
        );
        assert!(
            validate_scratch_paths(&root_path.join("h00001"), &root_path, &sandbox, None)
                .unwrap_err()
                .contains("encloses")
        );
        assert!(
            validate_scratch_paths(
                &sandbox.workspace_mount.join("h00001"),
                &sandbox.workspace_mount,
                &sandbox,
                None
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn provider_lifecycle_acquires_settles_on_eof_and_dedups_fallback_reasons() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-sl");
        let root_path = std::fs::canonicalize(&*root).unwrap();
        let main = root_path.join("main");
        std::fs::create_dir_all(&main).unwrap();
        let sandbox = test_sandbox(&root_path);

        let parent =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-sp");
        let parent_path = std::fs::canonicalize(&*parent).unwrap();
        let lease_dir = parent_path.join("h00001");
        let settled = parent_path.join("settled");
        let env_dump = parent_path.join("env");

        write_script(
            &main,
            "provider.sh",
            &format!(
                "#!/bin/sh\nset -eu\nprintf 'WS=%s SOCK=%s\\n' \"$COWSHED_SCRATCH_WORKSPACE\" \"$COWSHED_SUPERVISOR_SOCKET\" > \"{}\"\nmkdir -m 0700 \"{}\"\nprintf '{{\"directory\":\"%s\"}}\\n' \"{}\"\n/bin/cat >/dev/null\n/bin/rm -rf \"{}\"\nprintf 'ok' > \"{}\"\n",
                env_dump.display(),
                lease_dir.display(),
                lease_dir.display(),
                lease_dir.display(),
                settled.display(),
            ),
        );

        let client = ScratchClient::with_bounds(
            ScratchProviderConfig::new(
                vec!["/bin/sh".into(), "provider.sh".into()],
                main.clone(),
                "job-scratch".into(),
                Some(PathBuf::from("/tmp/sup.sock")),
                None,
            ),
            Bounds {
                handshake: Duration::from_secs(5),
                settle: Duration::from_secs(5),
            },
        );

        let lease = match client.acquire(&sandbox).await {
            ScratchOutcome::Lease(lease) => lease,
            ScratchOutcome::Fallback { reason } => panic!("unexpected fallback: {reason}"),
        };
        assert_eq!(lease.directory(), lease_dir.as_path());
        assert!(lease_dir.is_dir());
        assert_eq!(
            std::fs::read_to_string(&env_dump).unwrap(),
            "WS=job-scratch SOCK=/tmp/sup.sock\n"
        );
        lease.settle().await;
        assert!(!lease_dir.exists(), "provider must clean up on stdin EOF");
        assert_eq!(std::fs::read_to_string(&settled).unwrap(), "ok");

        // Notice deduplication: reported once per distinct reason.
        let first = client.take_notice("unavailable: no volume", &sandbox.exec_temp_dir);
        assert!(first.is_some());
        assert!(
            client
                .take_notice("unavailable: no volume", &sandbox.exec_temp_dir)
                .is_none()
        );
        assert!(
            client
                .take_notice("exited (1)", &sandbox.exec_temp_dir)
                .is_some()
        );
    }

    /// The production handshake bound admits a provider whose first answer waits on creating its
    /// storage: the RAM provider's cold create (`diskutil apfs createContainer` then `addVolume`)
    /// answered after 7.4-13.6 s at load 11-13. A provider answering after 6 s, past the first
    /// guessed 5 s bound, is used; at 5 s every cold host start abandoned it.
    #[tokio::test]
    async fn a_provider_answering_after_a_cold_create_is_used_under_the_production_bound() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-sc");
        let root_path = std::fs::canonicalize(&*root).unwrap();
        let main = root_path.join("main");
        std::fs::create_dir_all(&main).unwrap();
        let sandbox = test_sandbox(&root_path);
        let parent =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-sd");
        let parent_path = std::fs::canonicalize(&*parent).unwrap();
        let lease_dir = parent_path.join("h00002");
        write_script(
            &main,
            "cold.sh",
            &format!(
                "#!/bin/sh\nset -eu\n/bin/sleep 6\nmkdir -m 0700 \"{0}\"\nprintf '{{\"directory\":\"%s\"}}\\n' \"{0}\"\n/bin/cat >/dev/null\n/bin/rm -rf \"{0}\"\n",
                lease_dir.display(),
            ),
        );
        let client = ScratchClient::new(ScratchProviderConfig::new(
            vec!["/bin/sh".into(), "cold.sh".into()],
            main.clone(),
            "job-scratch".into(),
            None,
            None,
        ));
        let lease = match client.acquire(&sandbox).await {
            ScratchOutcome::Lease(lease) => lease,
            ScratchOutcome::Fallback { reason } => {
                panic!("a provider answering after a cold create must be used: {reason}")
            }
        };
        assert_eq!(lease.directory(), lease_dir.as_path());
        lease.settle().await;
        assert!(!lease_dir.exists());
    }

    #[tokio::test]
    async fn provider_handshake_timeout_and_stuck_settle_are_bounded() {
        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cowshed-st");
        let root_path = std::fs::canonicalize(&*root).unwrap();
        let main = root_path.join("main");
        std::fs::create_dir_all(&main).unwrap();
        let sandbox = test_sandbox(&root_path);

        // Provider that blocks without printing anything.
        write_script(&main, "hang.sh", "#!/bin/sh\n/bin/cat >/dev/null\n");
        let client = ScratchClient::with_bounds(
            ScratchProviderConfig::new(
                vec!["/bin/sh".into(), "hang.sh".into()],
                main.clone(),
                "job-scratch".into(),
                None,
                None,
            ),
            Bounds {
                handshake: Duration::from_millis(100),
                settle: Duration::from_millis(100),
            },
        );
        match client.acquire(&sandbox).await {
            ScratchOutcome::Fallback { reason } => {
                assert!(reason.contains("timed out"), "{reason}");
            }
            ScratchOutcome::Lease(_) => panic!("hanging provider must time out"),
        }
    }
}
