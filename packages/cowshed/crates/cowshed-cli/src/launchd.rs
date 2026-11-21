//! The service manager: immutable service definitions, filesystem plans, and native executors.
//!
//! This is the one interface to the host's per-user service manager, launchd on macOS and
//! systemd's user manager on Linux. The module and its types keep launchd's names
//! (`LaunchAgentSpec`, `LaunchAgentTarget`, `ControlPlan`, `LaunchctlCommand`, `LaunchdExecutor`,
//! `LaunchdServiceStatus`, `plist_path`, …) and mean the neutral thing on every platform: a
//! service definition, its control target, one call of the manager's control tool, the manager's
//! word on whether it holds the service. Everything here is the same on both platforms — the
//! host-stable executables, the install plans, the filesystem adapter, the executor — and the
//! platform file supplies only the mechanism: where a definition lives and its bytes
//! (`macos.rs`: a LaunchAgent plist; `linux.rs`: a systemd user unit), the control tool's argv
//! for each lifecycle step (`launchctl`; `systemctl --user`), how its status answer reads, and
//! the commands a hint names. No caller branches on the platform.
//!
//! The planner is pure. Execution is isolated behind injectable filesystem and
//! command adapters so callers can keep one mutable, actor-owned executor while
//! tests remain entirely host-independent.

use crate::agent_cycle::{self, LoadedAgent};
use cowshed_os::deadline::{self, Bound, Deadline, LatencyClass, RunError};
use cowshed_os::namespace::HostNamespace;
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// The manager's control tool: `/bin/launchctl`, or `systemctl` as `PATH` finds it.
pub use platform::MANAGER_EXECUTABLE as LAUNCHCTL_EXECUTABLE;
/// What a test's recording runner reads of a control argv and answers a status read with.
#[cfg(test)]
pub(crate) use platform::recorded;
/// What messages call the service manager, its control tool, a service it runs, that service's
/// definition file, and the two together.
pub use platform::{AGENT, AGENT_DEFINITION, DEFINITION, MANAGER, TOOL};
/// The commands a hint names, and how the control tool names a label's service.
pub use platform::{bootstrap_hint, kickstart_hint, print_hint, service_target};

pub const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
/// The mode of a definition file, whatever the platform calls it.
pub const PRIVATE_PLIST_MODE: u32 = 0o600;
/// Runnable, and writable only by the user the service manager runs the agent as.
pub const STABLE_BINARY_MODE: u32 = 0o755;
pub const COWSHED_BINARY_NAME: &str = "cowshed";

/// The gateway host's name in the host-stable directory (05_gateway.md "The gateway host"): the
/// program launchd runs for the gateway agent, a signed copy of the build at a path no install
/// moves, so the path and designated requirement a Developer Tools entry holds never change.
pub const GATEWAY_HOST_NAME: &str = "cowshed-gateway-host";

/// `cargo` in the host-stable directory. Earlier installs linked it beside the stable `cowshed`
/// name, and that binary answered as `cargo`; cowshed no longer does — a job's jobserver reaches
/// Cargo as the make jobserver configuration Cargo reads itself (05_gateway.md, "Parallelism
/// inside a job") — so
/// an install removes the link an earlier one left and nothing creates it.
pub const CARGO_LINK_NAME: &str = "cargo";

pub const LIBRARY_DIRECTORY: &str = "Library";
pub const APPLICATION_SUPPORT: &str = "Application Support";
pub const STABLE_SUPPORT_DIRECTORY: &str = "dev.cowshed";
pub const STABLE_BINARY_DIRECTORY: &str = "bin";

/// The directory of the `bun` the gateway runs its Nx cache server with, named as a place nix or
/// devenv keeps alive and re-points, never as a `/nix/store` path. A plist outlives the shell that
/// wrote it, and nothing roots a store path for it: after a devenv update and a nix GC that path is
/// gone and every backend start fails. So this is the first `PATH` entry holding `bun`, spelt as
/// `PATH` spells it — a devenv `.devenv/profile/bin`, a nix profile's `bin` such as
/// `/etc/profiles/per-user/<user>/bin`, or any plain directory — with one translation: a devenv
/// shell puts its own profile on `PATH` as the immutable store path it evaluated
/// (`DEVENV_PROFILE`), and that entry becomes the profile link devenv re-points on activation and
/// roots with the shell (`$DEVENV_DOTFILE/profile`, see [`DevenvProfile`]). Any other store entry
/// is refused by name. cowshed carries no interpreter of its own (the owner's rulings, 2026-10-17
/// and 2026-10-18).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BunDirectory(PathBuf);

/// Where nix keeps immutable paths. A store path is never a [`BunDirectory`].
const NIX_STORE: &str = "/nix/store";

/// The invoking shell's devenv profile: `store`, the immutable profile the shell evaluated
/// (`DEVENV_PROFILE`), and `link`, `$DEVENV_DOTFILE/profile`, which devenv points at the profile it
/// last activated and which the `.devenv/gc` root it registers keeps alive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DevenvProfile {
    pub store: PathBuf,
    pub link: PathBuf,
}

impl DevenvProfile {
    /// The profile devenv's own variables name, when this process runs in a devenv shell.
    pub fn of_environment() -> Option<Self> {
        let store = std::env::var_os("DEVENV_PROFILE")?;
        let dotfile = std::env::var_os("DEVENV_DOTFILE")?;
        Some(Self {
            store: store.into(),
            link: PathBuf::from(dotfile).join("profile"),
        })
    }
}

impl BunDirectory {
    /// A directory said to hold `bun`, held to the shape a plist may name: absolute, in its
    /// canonical lexical spelling, and outside `/nix/store`. It states a path rather than probing
    /// it; [`Self::on_path`] is the constructor that looks.
    pub fn new(directory: PathBuf) -> Result<Self, LaunchdError> {
        validate_canonical_absolute_path("bun-directory", &directory)?;
        if directory.starts_with(NIX_STORE) {
            return Err(LaunchdError::InvalidPath {
                field: "bun-directory",
                reason: platform::STORE_PATH_REFUSAL,
            });
        }
        Ok(Self(directory))
    }

    /// The directory of the first `bun` on `path`, as [`BunDirectory`] states, or why there is
    /// none to use. `devenv` is the invoking shell's profile, when it is a devenv shell.
    pub fn on_path(path: &std::ffi::OsStr, devenv: Option<&DevenvProfile>) -> Result<Self, String> {
        let found = cowshed_gateway::bun_on_path(path)
            .ok_or_else(|| format!("no bun on PATH {}", Path::new(path).display()))?;
        let entry = found.parent().unwrap_or(Path::new("/"));
        let directory = match devenv {
            Some(profile) if entry == profile.store.join("bin") => {
                let linked = profile.link.join("bin");
                cowshed_gateway::bun_on_path(linked.as_os_str()).ok_or_else(|| {
                    format!(
                        "bun {} is devenv's profile, but its link {} holds no bun",
                        found.display(),
                        linked.display()
                    )
                })?;
                linked
            }
            _ => entry.to_path_buf(),
        };
        Self::new(directory).map_err(|error| format!("bun {}: {error}", found.display()))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// A binary path launchd can still reach after a reboot.
///
/// `~/Library/Application Support/dev.cowshed/bin/<name>` is on the volume that also carries
/// `~/Library/LaunchAgents`: if launchd can read the plist, the binary that plist names is there
/// too. Every other candidate can be absent when the agent first runs — a checkout is rebuilt or
/// deleted, a nix store path is garbage collected, and a workspace image is not mounted until
/// cowshed mounts it, which is the gateway's own job. launchd answers a missing program with
/// exit 78 and, under `KeepAlive`, retries it forever: the healer must not live inside what it
/// heals.
///
/// This is the only way to name a service executable, so no caller can put a disappearing path
/// into a plist. It carries the home it was derived from, which is also what keeps one home's
/// plist from naming another home's binary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostStableExecutable {
    home: PathBuf,
    name: String,
    path: PathBuf,
}

impl HostStableExecutable {
    pub fn new(home: &Path, name: &str) -> Result<Self, LaunchdError> {
        validate_canonical_absolute_path("home", home)?;
        validate_binary_name(name)?;
        let path = home
            .join(LIBRARY_DIRECTORY)
            .join(APPLICATION_SUPPORT)
            .join(STABLE_SUPPORT_DIRECTORY)
            .join(STABLE_BINARY_DIRECTORY)
            .join(name);
        Ok(Self {
            home: home.to_path_buf(),
            name: name.to_owned(),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    /// The directory holding every host-stable cowshed binary.
    pub fn directory(&self) -> &Path {
        self.path
            .parent()
            .expect("derived paths always have a parent")
    }

    /// The `dev.cowshed` directory holding [`Self::directory`].
    pub fn support_directory(&self) -> &Path {
        self.directory()
            .parent()
            .expect("derived paths always have a parent")
    }

    /// The gateway host beside this stable name: [`GATEWAY_HOST_NAME`] in [`Self::directory`].
    pub fn gateway_host(&self) -> PathBuf {
        self.directory().join(GATEWAY_HOST_NAME)
    }
    /// The content-addressed copy of the build whose bytes hash to `digest`: `<name>-<digest>`,
    /// beside the stable name. It is written once, by the install that first meets those bytes,
    /// and never rewritten, so whatever runs from it runs exactly that build for as long as it
    /// exists.
    pub fn stored(&self, digest: &ExecutableDigest) -> PathBuf {
        self.directory().join(self.stored_name(digest))
    }

    /// What the stable name's link says when it names [`Self::stored`]: the copy's file name,
    /// relative to the directory they share, so the pair means the same thing wherever the home
    /// is.
    pub fn stored_link(&self, digest: &ExecutableDigest) -> PathBuf {
        PathBuf::from(self.stored_name(digest))
    }

    fn stored_name(&self, digest: &ExecutableDigest) -> String {
        format!("{}-{digest}", self.name)
    }

    /// The stable executable `path` is, when [`Self::new`] derives it from some home; `None` for
    /// any other path.
    pub fn at(path: &Path) -> Option<Self> {
        let name = path.file_name()?.to_str()?;
        // `<home>/Library/Application Support/dev.cowshed/bin/<name>`
        let home = path.ancestors().nth(5)?;
        Self::new(home, name)
            .ok()
            .filter(|derived| derived.path == path)
    }

    /// The build a file name in [`Self::directory`] is the stored copy of, or `None` for any
    /// other name: the stable name itself, a retained or temporary file, another service's
    /// binary.
    pub fn stored_digest(&self, file_name: &std::ffi::OsStr) -> Option<ExecutableDigest> {
        file_name
            .to_str()?
            .strip_prefix(self.name.as_str())?
            .strip_prefix('-')
            .and_then(ExecutableDigest::parse)
    }
}

/// The SHA-256 of an executable's bytes: 64 lowercase hex digits.
///
/// The one spelling a stored copy's name can take, so a string that is not a digest — a path
/// separator, a `..`, an uppercase twin of a copy already stored — can never become part of a
/// path under the host-stable directory.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExecutableDigest(String);

impl ExecutableDigest {
    /// The digest a hasher finished with.
    pub fn from_bytes(digest: &[u8; 32]) -> Self {
        use std::fmt::Write as _;
        let mut hex = String::with_capacity(64);
        for byte in digest {
            write!(hex, "{byte:02x}").expect("writing into a String cannot fail");
        }
        Self(hex)
    }

    /// `hex` when it is a digest as [`Self::from_bytes`] spells one.
    pub fn parse(hex: &str) -> Option<Self> {
        (hex.len() == 64
            && hex
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
        .then(|| Self(hex.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExecutableDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A control target: the label, and the path of the definition file that defines it
/// (`~/Library/LaunchAgents/<label>.plist`, or `~/.config/systemd/user/<label>.service`).
///
/// Split out of [`LaunchAgentSpec`] because `stop` and `status` are not definitions. They have to
/// reach the agent `start` installed even when the program it names is gone, a state `doctor` must
/// be able to report rather than one that makes the control verbs unconstructible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchAgentTarget {
    label: String,
    plist_path: PathBuf,
}

impl LaunchAgentTarget {
    pub fn new(home: &Path, label: &str) -> Result<Self, LaunchdError> {
        validate_canonical_absolute_path("home", home)?;
        validate_label(label)?;
        Ok(Self {
            label: label.to_owned(),
            plist_path: platform::definition_path(home, label),
        })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// The definition file, a plist only on macOS.
    pub fn plist_path(&self) -> &Path {
        &self.plist_path
    }

    /// The directory the manager reads definitions from, `~/Library/LaunchAgents` only on macOS.
    pub fn launch_agents_directory(&self) -> &Path {
        self.plist_path
            .parent()
            .expect("validated plist paths always have a parent")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceLifecycle {
    /// Start at login and restart whenever the service exits.
    KeepAlive,
    /// Run once when the agent is loaded.
    RunAtLoad,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchAgentSpec {
    target: LaunchAgentTarget,
    executable: PathBuf,
    arguments: Vec<String>,
    environment: Vec<(String, String)>,
    lifecycle: ServiceLifecycle,
    standard_error_path: PathBuf,
    /// A finite soft limit with unlimited hard authority, inherited by descendants.
    soft_number_of_files: Option<u64>,
    /// How long the manager lets the program exit after the SIGTERM of a stop before it SIGKILLs
    /// it: launchd `ExitTimeOut` (a bootout or `kickstart -k`), systemd `TimeoutStopSec=`. `None`
    /// leaves the manager's default: launchd's is 5 s on macOS 26 (`launchctl print`:
    /// "exit timeout = 5"), systemd's 90 s.
    exit_timeout: Option<std::time::Duration>,
}

/// Everything that varies between one agent definition and another.
///
/// A value rather than a positional argument list: the two constructors below differ in five of
/// these fields at once, and five adjacent positional parameters of overlapping types is how a
/// definition ends up with the wrong log file.
struct AgentDefinition {
    label: String,
    arguments: Vec<String>,
    environment: Vec<(String, String)>,
    lifecycle: ServiceLifecycle,
    standard_error_file: &'static str,
}

impl LaunchAgentSpec {
    pub fn new_user(
        executable: &HostStableExecutable,
        label: impl Into<String>,
        arguments: Vec<String>,
        lifecycle: ServiceLifecycle,
    ) -> Result<Self, LaunchdError> {
        validate_arguments(&arguments)?;
        Self::assemble(
            executable.home(),
            executable.path(),
            AgentDefinition {
                label: label.into(),
                arguments,
                environment: Vec::new(),
                lifecycle,
                standard_error_file: "daemon-stderr.log",
            },
        )
    }

    /// `home` and `program` are primitives here, but every public constructor above reaches this
    /// through a type that has already proved its program outlives the build that installed it, a
    /// [`HostStableExecutable`] for a cowshed-owned copy. Nothing else can name a service executable.
    fn assemble(
        home: &Path,
        program: &Path,
        definition: AgentDefinition,
    ) -> Result<Self, LaunchdError> {
        let AgentDefinition {
            label,
            arguments,
            environment,
            lifecycle,
            standard_error_file,
        } = definition;
        validate_environment(&environment)?;
        let target = LaunchAgentTarget::new(home, &label)?;
        // launchd creates StandardErrorPath before the gateway can remount
        // ~/.cowshed. Keep this on Data so reboot cannot mask the store volume.
        let standard_error_path = home
            .join(LIBRARY_DIRECTORY)
            .join("Logs")
            .join("cowshed")
            .join(standard_error_file);

        Ok(Self {
            target,
            executable: program.to_path_buf(),
            arguments,
            environment,
            lifecycle,
            standard_error_path,
            soft_number_of_files: None,
            exit_timeout: None,
        })
    }

    /// The gateway agent. Its `PATH` is the directory of the `bun` setup found ([`BunDirectory`])
    /// ahead of the manager's own default, so the daemon runs its Nx cache server with that `bun`
    /// and every other child it names resolves exactly as it did under the manager's default.
    ///
    /// The label is `namespace`'s ([`HostNamespace::launchd_label`], 01_storage.md "Host
    /// namespace"): production's `dev.cowshed.gateway`, a test namespace's
    /// `dev.cowshed-test-<ns>.gateway`.
    ///
    /// The manager runs the gateway host ([`crate::gateway_host`]) from its fixed path, and the
    /// host runs the stable name's `gateway run` as its child: the daemon runs its own
    /// content-addressed copy, and on macOS every process it starts, every workspace job included,
    /// has the host as its responsible process.
    pub fn gateway(
        namespace: HostNamespace,
        executable: &HostStableExecutable,
        bun: &BunDirectory,
    ) -> Result<Self, LaunchdError> {
        let environment = platform::gateway_environment(executable.home(), bun);
        let mut spec = Self::assemble(
            executable.home(),
            &executable.gateway_host(),
            AgentDefinition {
                label: namespace.launchd_label("gateway"),
                arguments: vec![
                    crate::gateway_host::VERB.into(),
                    executable
                        .path()
                        .to_str()
                        .expect("validated paths are UTF-8")
                        .into(),
                    "gateway".into(),
                    "run".into(),
                ],
                environment,
                lifecycle: ServiceLifecycle::KeepAlive,
                standard_error_file: "daemon-stderr.log",
            },
        )?;
        // A finite kernel-sized soft limit keeps sysconf(_SC_OPEN_MAX) usable by fd-closing tools.
        // Hard authority remains all the manager has (Darwin's RLIM_INFINITY, a systemd user
        // manager's own hard limit), owned by the service, not child spawns.
        spec.soft_number_of_files = Some(245_760);
        // launchd's 5 s default SIGKILLed a draining gateway mid-drain (measured 2026-10-22), so
        // the agent states the bound the daemon's own drain is sized within.
        spec.exit_timeout = Some(cowshed_gateway_types::GATEWAY_EXIT_TIMEOUT);
        Ok(spec)
    }

    pub fn label(&self) -> &str {
        self.target.label()
    }

    /// The launchctl control target this definition installs, for `stop` and `status`.
    pub fn target(&self) -> &LaunchAgentTarget {
        &self.target
    }

    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    /// The variables the definition states, each value as it is, unescaped. `PATH` holds what the
    /// platform's definition leads with: a plist states it whole, launchd's default included,
    /// and a unit writes the user manager's own default behind it (`linux.rs`).
    pub fn environment(&self) -> &[(String, String)] {
        &self.environment
    }

    pub fn lifecycle(&self) -> ServiceLifecycle {
        self.lifecycle
    }

    pub fn plist_path(&self) -> &Path {
        self.target.plist_path()
    }

    pub fn launch_agents_directory(&self) -> &Path {
        self.target.launch_agents_directory()
    }

    pub fn standard_error_path(&self) -> &Path {
        &self.standard_error_path
    }

    pub fn program_arguments(&self) -> impl Iterator<Item = &str> {
        std::iter::once(
            self.executable
                .to_str()
                .expect("validated executable paths are UTF-8"),
        )
        .chain(self.arguments.iter().map(String::as_str))
    }

    /// The definition file's bytes, a plist only on macOS.
    pub fn plist_bytes(&self) -> Vec<u8> {
        platform::definition_bytes(self)
    }

    /// The directories besides the definition's own that must exist before the manager can
    /// start this definition, each with the mode it has to have. None where the manager makes
    /// what a definition names (launchd).
    pub fn required_directories(&self) -> Vec<RequiredDirectory> {
        platform::required_directories(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExistingPlist<'a> {
    pub bytes: &'a [u8],
    pub mode: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InstallState<'a> {
    /// `None` means the definition directory (`~/Library/LaunchAgents`,
    /// `~/.config/systemd/user`) does not exist.
    pub launch_agents_directory_mode: Option<u32>,
    pub plist: Option<ExistingPlist<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallPlan {
    operations: Vec<Mutation>,
}

impl InstallPlan {
    pub fn operations(&self) -> &[Mutation] {
        &self.operations
    }

    pub fn is_noop(&self) -> bool {
        self.operations.is_empty()
    }
}

/// An ordered, filesystem-only mutation plan.
///
/// `CreateExclusiveTemporaryFile` and `CopyToTemporaryFile` produce the temporary file consumed
/// by the immediately following temporary-file operations. The executor must choose a
/// unique suffix, open with exclusive creation and no symlink following, and
/// clean up that file if a later operation fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    EnsureDirectory {
        path: PathBuf,
        mode: u32,
    },
    SetPermissions {
        path: PathBuf,
        mode: u32,
    },
    CreateExclusiveTemporaryFile {
        directory: PathBuf,
        name_prefix: String,
        bytes: Vec<u8>,
        mode: u32,
    },
    /// Stream `source` into a fresh temporary file. Service binaries are tens of megabytes: they
    /// are copied through the filesystem, never carried in a plan.
    CopyToTemporaryFile {
        directory: PathBuf,
        name_prefix: String,
        source: PathBuf,
        mode: u32,
    },
    /// A fresh symbolic link to `target` in `directory`, made the active temporary file so the
    /// `RenameTemporaryFile` that follows swaps it over its destination atomically. A link has no
    /// contents to sync: the directory sync that ends the plan makes it durable.
    CreateTemporarySymlink {
        directory: PathBuf,
        name_prefix: String,
        target: PathBuf,
    },
    /// Sign the active temporary file in place as the gateway host with `identity`
    /// ([`crate::code_signing`]), through the executor's command runner, so the bytes renamed
    /// into place are the signed ones.
    SignTemporaryFile {
        identity: crate::code_signing::SigningIdentity,
    },
    SyncTemporaryFile,
    RenameTemporaryFile {
        destination: PathBuf,
    },
    RemoveFile {
        path: PathBuf,
    },
    SyncDirectory {
        path: PathBuf,
    },
}

/// Plan a directory that has to exist at exactly `wanted`.
fn plan_directory(path: &Path, mode: Option<u32>, wanted: u32) -> Option<Mutation> {
    match mode {
        None => Some(Mutation::EnsureDirectory {
            path: path.to_path_buf(),
            mode: wanted,
        }),
        Some(mode) if mode != wanted => Some(Mutation::SetPermissions {
            path: path.to_path_buf(),
            mode: wanted,
        }),
        Some(_) => None,
    }
}

fn plan_private_directory(path: &Path, mode: Option<u32>) -> Option<Mutation> {
    plan_directory(path, mode, PRIVATE_DIRECTORY_MODE)
}

fn plan_remove_file(path: &Path, directory: &Path, installed: bool) -> Vec<Mutation> {
    if installed {
        vec![
            Mutation::RemoveFile {
                path: path.to_path_buf(),
            },
            Mutation::SyncDirectory {
                path: directory.to_path_buf(),
            },
        ]
    } else {
        Vec::new()
    }
}

/// Plan the definition's install: its directory, as the platform keeps it (launchd's private to
/// the user, systemd's made when missing and otherwise left alone), and the definition file,
/// published whole through a temporary file beside it. A definition that is already there with
/// these bytes and this mode plans nothing.
pub fn plan_install(spec: &LaunchAgentSpec, state: InstallState<'_>) -> InstallPlan {
    let directory = spec.launch_agents_directory().to_path_buf();
    let mut operations = Vec::new();

    if let Some(operation) =
        platform::plan_definition_directory(&directory, state.launch_agents_directory_mode)
    {
        operations.push(operation);
    }

    let desired = spec.plist_bytes();
    let plist_is_current = state
        .plist
        .is_some_and(|plist| plist.mode == PRIVATE_PLIST_MODE && plist.bytes == desired);

    if !plist_is_current {
        operations.push(Mutation::CreateExclusiveTemporaryFile {
            directory: directory.clone(),
            name_prefix: format!(".{}.{}.", spec.label(), platform::DEFINITION_EXTENSION),
            bytes: desired,
            mode: PRIVATE_PLIST_MODE,
        });
        operations.push(Mutation::SyncTemporaryFile);
        operations.push(Mutation::RenameTemporaryFile {
            destination: spec.plist_path().to_path_buf(),
        });
    }

    if !operations.is_empty() {
        operations.push(Mutation::SyncDirectory { path: directory });
    }

    InstallPlan { operations }
}

/// A directory a definition needs besides its own before the manager can start it
/// ([`LaunchAgentSpec::required_directories`]), and the mode it has to have.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequiredDirectory {
    pub path: PathBuf,
    pub mode: u32,
}

/// Plan the directories a definition requires: each with the mode observed for it, `None` for
/// one that does not exist. One that is missing is made, with its parents; one at another mode
/// is set to its own; one that is as required plans nothing, so a current host plans nothing.
/// Planned apart from [`plan_install`] because making one never calls for a restart: the manager
/// retries a service whose directory was missing, and finds it at its next start.
pub fn plan_required_directories(observed: &[(RequiredDirectory, Option<u32>)]) -> InstallPlan {
    InstallPlan {
        operations: observed
            .iter()
            .filter_map(|(directory, mode)| plan_directory(&directory.path, *mode, directory.mode))
            .collect(),
    }
}

pub fn plan_remove(target: &LaunchAgentTarget, installed: bool) -> InstallPlan {
    let operations = plan_remove_file(
        target.plist_path(),
        target.launch_agents_directory(),
        installed,
    );

    InstallPlan { operations }
}

/// What the host currently has under a [`HostStableExecutable`]'s name and directory.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutableInstallState {
    /// Mode of `~/Library/Application Support/dev.cowshed`; `None` when it does not exist.
    pub support_directory_mode: Option<u32>,
    /// Mode of that directory's `bin`; `None` when it does not exist.
    pub binary_directory_mode: Option<u32>,
    /// The stored copy of the source, when a regular file is there.
    pub stored: Option<InstalledExecutable>,
    /// What the stable name is.
    pub link: StableLink,
}

/// What a [`HostStableExecutable`]'s stable name is on the host.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum StableLink {
    #[default]
    Absent,
    /// Anything but a symbolic link: before copies were content-addressed, the installed binary
    /// itself.
    Foreign,
    /// A symbolic link saying this.
    To(PathBuf),
}

impl ExecutableInstallState {
    /// The stored copy holds the source's bytes at [`STABLE_BINARY_MODE`].
    pub fn stored_is_current(&self) -> bool {
        self.stored
            .is_some_and(|stored| stored.mode == STABLE_BINARY_MODE && stored.matches_source)
    }

    /// The stable name already runs the stored copy of `digest`, and that copy is current.
    pub fn is_current(&self, executable: &HostStableExecutable, digest: &ExecutableDigest) -> bool {
        self.stored_is_current() && self.link == StableLink::To(executable.stored_link(digest))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstalledExecutable {
    pub mode: u32,
    /// Whether its bytes are already the bytes being installed.
    pub matches_source: bool,
}

/// Plan the copy that makes `source` — the build whose bytes hash to `digest` — what the stable
/// name runs.
///
/// Two publications, each a temporary beside its destination renamed over it: the copy of the
/// bytes, under the name its digest gives it, and then the stable name as a link to that copy.
/// launchd restarts a `KeepAlive` service the moment it exits, so it must never observe a
/// half-written binary or a missing name at the path it runs, and the name moves from one whole
/// build to the next in one rename. A copy that already matches plans nothing — the source is
/// tens of megabytes — and a stable name that already names it plans nothing either, so an
/// installed build is touched by nothing but the install of another.
pub fn plan_executable_install(
    executable: &HostStableExecutable,
    digest: &ExecutableDigest,
    source: &Path,
    state: &ExecutableInstallState,
) -> InstallPlan {
    let mut operations = Vec::new();
    for (directory, mode) in [
        (executable.support_directory(), state.support_directory_mode),
        (executable.directory(), state.binary_directory_mode),
    ] {
        if let Some(operation) = plan_private_directory(directory, mode) {
            operations.push(operation);
        }
    }

    if !state.stored_is_current() {
        operations.push(Mutation::CopyToTemporaryFile {
            directory: executable.directory().to_path_buf(),
            name_prefix: format!(".{}.", executable.name()),
            source: source.to_path_buf(),
            mode: STABLE_BINARY_MODE,
        });
        operations.push(Mutation::SyncTemporaryFile);
        operations.push(Mutation::RenameTemporaryFile {
            destination: executable.stored(digest),
        });
    }

    let link = executable.stored_link(digest);
    if state.link != StableLink::To(link.clone()) {
        operations.push(Mutation::CreateTemporarySymlink {
            directory: executable.directory().to_path_buf(),
            name_prefix: format!(".{}.link.", executable.name()),
            target: link,
        });
        operations.push(Mutation::RenameTemporaryFile {
            destination: executable.path().to_path_buf(),
        });
    }

    if !operations.is_empty() {
        operations.push(Mutation::SyncDirectory {
            path: executable.directory().to_path_buf(),
        });
    }

    InstallPlan { operations }
}

/// Plan the gateway host's install (05_gateway.md "The gateway host"): `source` copied into a
/// temporary file beside [`HostStableExecutable::gateway_host`], signed there with `identity`,
/// synced, and renamed over the host. The host is one file at a path no install moves, so
/// launchd and a Developer Tools entry always name the same one; each rewrite replaces it whole
/// in one rename, and a host still running keeps the file it started from. The directory exists:
/// the stored copy's install, which made it, runs first.
pub fn plan_gateway_host_install(
    executable: &HostStableExecutable,
    source: &Path,
    identity: crate::code_signing::SigningIdentity,
) -> InstallPlan {
    gateway_host_install(executable, source, Some(identity))
}

/// [`plan_gateway_host_install`] on a host that signs nothing
/// ([`crate::code_signing::gateway_host_identity`]): the same copy, sync and rename, with no
/// sign step between them.
pub fn plan_unsigned_gateway_host_install(
    executable: &HostStableExecutable,
    source: &Path,
) -> InstallPlan {
    gateway_host_install(executable, source, None)
}

fn gateway_host_install(
    executable: &HostStableExecutable,
    source: &Path,
    identity: Option<crate::code_signing::SigningIdentity>,
) -> InstallPlan {
    let directory = executable.directory().to_path_buf();
    let mut operations = vec![Mutation::CopyToTemporaryFile {
        directory: directory.clone(),
        name_prefix: format!(".{GATEWAY_HOST_NAME}."),
        source: source.to_path_buf(),
        mode: STABLE_BINARY_MODE,
    }];
    operations.extend(identity.map(|identity| Mutation::SignTemporaryFile { identity }));
    operations.extend([
        Mutation::SyncTemporaryFile,
        Mutation::RenameTemporaryFile {
            destination: executable.gateway_host(),
        },
        Mutation::SyncDirectory { path: directory },
    ]);
    InstallPlan { operations }
}

/// Plan the removal of the `cargo` link an earlier install left in the host-stable `directory`.
/// Left there, `cargo` on a `PATH` holding that directory would run the cowshed CLI. The caller
/// has judged the link one a cowshed install planted; anything else there is not cowshed's to
/// remove.
pub fn plan_cargo_link_removal(directory: &Path) -> InstallPlan {
    InstallPlan {
        operations: plan_remove_file(&directory.join(CARGO_LINK_NAME), directory, true),
    }
}

/// Plan the removal of `present`: files of the host-stable directory, in the order given.
///
/// Serves the uninstall, which removes the stable name before the copies it names so nothing
/// launchd could start dangles, and the pruning of copies an install has replaced. Only those
/// files: the enclosing directories are removed by nothing, because
/// `~/Library/Application Support/dev.cowshed` is shared by every service and an empty directory
/// costs nothing. No file to remove plans nothing, which is what makes a second uninstall a
/// no-op rather than a failure.
pub fn plan_executable_remove(
    executable: &HostStableExecutable,
    present: &[PathBuf],
) -> InstallPlan {
    let mut operations: Vec<Mutation> = present
        .iter()
        .map(|path| Mutation::RemoveFile { path: path.clone() })
        .collect();
    if !operations.is_empty() {
        operations.push(Mutation::SyncDirectory {
            path: executable.directory().to_path_buf(),
        });
    }

    InstallPlan { operations }
}

/// Filesystem operations required to execute an [`InstallPlan`].
///
/// `ensure_directory` creates missing parents with `mode` and guarantees exactly `mode` on the
/// named directory: a home is not always furnished, and `~/Library/Application Support` does not
/// exist on a freshly created account until something writes there.
///
/// Implementations of `create_exclusive_no_follow` and `copy_exclusive_no_follow` must either
/// return a fully written file with exactly `mode`, or remove any file they created before
/// returning an error.
pub trait LaunchdFilesystem {
    fn ensure_directory(&mut self, path: &Path, mode: u32) -> io::Result<()>;
    fn set_permissions(&mut self, path: &Path, mode: u32) -> io::Result<()>;
    fn create_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        bytes: &[u8],
        mode: u32,
    ) -> io::Result<PathBuf>;
    fn copy_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        source: &Path,
        mode: u32,
    ) -> io::Result<PathBuf>;
    /// Create a symbolic link to `target` at a fresh exclusive name in `directory`, and answer
    /// that name.
    fn symlink_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        target: &Path,
    ) -> io::Result<PathBuf>;
    fn sync_file(&mut self, path: &Path) -> io::Result<()>;
    fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()>;
    fn remove_file(&mut self, path: &Path) -> io::Result<()>;
    fn sync_directory(&mut self, path: &Path) -> io::Result<()>;
}

/// Native, no-shell filesystem adapter for per-user LaunchAgent files.
#[derive(Debug, Default)]
pub struct NativeFilesystem {
    next_temporary_id: u64,
}

impl NativeFilesystem {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a symbolic link to `target` at a fresh name in `directory`. `symlink(2)` fails with
    /// `EEXIST` rather than replace or follow whatever holds the name, which is the exclusivity
    /// the temporary file's `O_CREAT | O_EXCL` gives a file.
    fn create_temporary_symlink(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        target: &Path,
    ) -> io::Result<PathBuf> {
        for _ in 0..128 {
            let id = self.next_temporary_id;
            self.next_temporary_id = self.next_temporary_id.wrapping_add(1);
            let path = directory.join(format!("{name_prefix}{}.{}", std::process::id(), id));
            match std::os::unix::fs::symlink(target, &path) {
                Ok(()) => return Ok(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("could not allocate an exclusive {MANAGER} temporary symlink"),
        ))
    }

    /// Open a fresh exclusive temporary file in `directory` with exactly `mode`.
    ///
    /// The mode is set explicitly after creation as well: `O_CREAT` mode is masked by the
    /// process umask, and a plist at 0644 or a binary without its exec bit are both wrong.
    fn create_temporary_file(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        mode: u32,
    ) -> io::Result<(PathBuf, File)> {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        for _ in 0..128 {
            let id = self.next_temporary_id;
            self.next_temporary_id = self.next_temporary_id.wrapping_add(1);
            let path = directory.join(format!("{name_prefix}{}.{}", std::process::id(), id));
            let opened = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(mode)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path);
            let file = match opened {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            };

            let prepared = (|| {
                if !file.metadata()?.file_type().is_file() {
                    return Err(wrong_file_kind("temporary path", FileKind::RegularFile));
                }
                file.set_permissions(fs::Permissions::from_mode(mode))
            })();
            return match prepared {
                Ok(()) => Ok((path, file)),
                Err(error) => {
                    drop(file);
                    let _ = fs::remove_file(&path);
                    Err(error)
                }
            };
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("could not allocate an exclusive {MANAGER} temporary file"),
        ))
    }

    /// Keep the temporary file only when it was written in full: a partial plist or binary must
    /// never survive to be renamed over the path launchd reads.
    fn published(path: PathBuf, file: File, written: io::Result<()>) -> io::Result<PathBuf> {
        drop(file);
        match written {
            Ok(()) => Ok(path),
            Err(error) => {
                let _ = fs::remove_file(&path);
                Err(error)
            }
        }
    }
}

impl LaunchdFilesystem for NativeFilesystem {
    fn ensure_directory(&mut self, path: &Path, mode: u32) -> io::Result<()> {
        use std::os::unix::fs::DirBuilderExt;

        // Recursive so a home missing `Library/Application Support` still gets the agent: only
        // directories this call creates take `mode`, and existing ancestors keep theirs.
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(mode);
        match builder.create(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        set_exact_permissions_no_follow(path, mode, FileKind::Directory)
    }

    fn set_permissions(&mut self, path: &Path, mode: u32) -> io::Result<()> {
        set_exact_permissions_no_follow(path, mode, FileKind::Directory)
    }

    fn create_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        bytes: &[u8],
        mode: u32,
    ) -> io::Result<PathBuf> {
        let (path, mut file) = self.create_temporary_file(directory, name_prefix, mode)?;
        let written = file.write_all(bytes);
        Self::published(path, file, written)
    }

    fn copy_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        source: &Path,
        mode: u32,
    ) -> io::Result<PathBuf> {
        let (path, mut file) = self.create_temporary_file(directory, name_prefix, mode)?;
        // Streamed, not buffered: io::copy takes the platform's fast path and a service binary
        // never has to be resident in this process.
        let written = File::open(source)
            .and_then(|mut source| io::copy(&mut source, &mut file))
            .map(|_| ());
        Self::published(path, file, written)
    }

    fn symlink_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        target: &Path,
    ) -> io::Result<PathBuf> {
        self.create_temporary_symlink(directory, name_prefix, target)
    }

    fn sync_file(&mut self, path: &Path) -> io::Result<()> {
        let file = open_existing_no_follow(path, true)?;
        require_file_kind(&file, "temporary path", FileKind::RegularFile)?;
        file.sync_all()
    }

    fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()> {
        fs::rename(source, destination)
    }

    fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        let directory = open_existing_no_follow(path, false)?;
        require_file_kind(&directory, "directory path", FileKind::Directory)?;
        directory.sync_all()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileKind {
    Directory,
    RegularFile,
}

fn set_exact_permissions_no_follow(path: &Path, mode: u32, kind: FileKind) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let file = open_existing_no_follow(path, false)?;
    require_file_kind(&file, format_args!("{MANAGER} path"), kind)?;
    file.set_permissions(fs::Permissions::from_mode(mode))
}

fn open_existing_no_follow(path: &Path, write: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW);
    options.open(path)
}

fn require_file_kind(
    file: &File,
    subject: impl fmt::Display,
    expected: FileKind,
) -> io::Result<()> {
    let actual = file.metadata()?.file_type();
    let matches = match expected {
        FileKind::Directory => actual.is_dir(),
        FileKind::RegularFile => actual.is_file(),
    };
    if matches {
        Ok(())
    } else {
        Err(wrong_file_kind(subject, expected))
    }
}

fn wrong_file_kind(subject: impl fmt::Display, expected: FileKind) -> io::Error {
    let expected = match expected {
        FileKind::Directory => "directory",
        FileKind::RegularFile => "regular file",
    };
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{subject} is not a {expected}"),
    )
}

/// Make `destination` a symbolic link saying `target`, in one rename: a temporary link beside it
/// replaces whatever is there — a link to another build, a regular file — so a reader of
/// `destination` meets the old answer or the new one and never an absence.
pub fn replace_symlink(target: &Path, destination: &Path) -> io::Result<()> {
    let (Some(directory), Some(name)) = (
        destination.parent(),
        destination.file_name().and_then(|name| name.to_str()),
    ) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no file in a directory", destination.display()),
        ));
    };
    let temporary = NativeFilesystem::new().create_temporary_symlink(
        directory,
        &format!(".{name}.link."),
        target,
    )?;
    fs::rename(&temporary, destination).inspect_err(|_| {
        let _ = fs::remove_file(&temporary);
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemOperation {
    EnsureDirectory,
    SetPermissions,
    CreateTemporaryFile,
    CreateTemporarySymlink,
    CopyTemporaryFile,
    SignTemporaryFile,
    SyncTemporaryFile,
    RenameTemporaryFile,
    RemoveFile,
    SyncDirectory,
}

#[derive(Debug)]
pub struct CleanupFailure {
    path: PathBuf,
    source: io::Error,
}

impl CleanupFailure {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn source_error(&self) -> &io::Error {
        &self.source
    }
}

#[derive(Debug)]
pub enum InstallExecutionError {
    Filesystem {
        operation: FilesystemOperation,
        path: PathBuf,
        source: io::Error,
        cleanup_failure: Option<CleanupFailure>,
    },
    InvalidPlan {
        operation: FilesystemOperation,
        reason: &'static str,
    },
}

impl InstallExecutionError {
    pub fn operation(&self) -> FilesystemOperation {
        match self {
            Self::Filesystem { operation, .. } | Self::InvalidPlan { operation, .. } => *operation,
        }
    }

    pub fn cleanup_failure(&self) -> Option<&CleanupFailure> {
        match self {
            Self::Filesystem {
                cleanup_failure, ..
            } => cleanup_failure.as_ref(),
            Self::InvalidPlan { .. } => None,
        }
    }
}

impl fmt::Display for InstallExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filesystem {
                operation,
                path,
                source,
                cleanup_failure,
            } => {
                write!(
                    formatter,
                    "{MANAGER} filesystem operation {operation:?} failed for {}: {source}",
                    path.display()
                )?;
                if let Some(cleanup) = cleanup_failure {
                    write!(
                        formatter,
                        "; cleanup of {} also failed: {}",
                        cleanup.path.display(),
                        cleanup.source
                    )?;
                }
                Ok(())
            }
            Self::InvalidPlan { operation, reason } => {
                write!(
                    formatter,
                    "invalid {MANAGER} plan at {operation:?}: {reason}"
                )
            }
        }
    }
}

impl Error for InstallExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Filesystem { source, .. } => Some(source),
            Self::InvalidPlan { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    NoChange,
    Changed,
}

/// Whether a removal found anything to remove.
///
/// Kept distinct from [`InstallOutcome`] because it answers a different operational question: an
/// install reports whether the host changed, a removal reports whether the artifact was there at
/// all. Both are reportable outcomes rather than errors — a second uninstall is a no-op, and
/// saying "nothing to remove" is how the caller learns the host was already clean.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalOutcome {
    Removed,
    AlreadyAbsent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlAction {
    Bootstrap,
    Bootout,
    Kickstart,
    Print,
}

impl ControlAction {
    /// The op a call of this action is bounded and named under (05_gateway.md "Deadlines"):
    /// the control tool and its verb, `launchctl bootstrap` or `systemctl enable`.
    pub const fn op(self) -> &'static str {
        platform::op(self)
    }
}

/// One call of the manager's control tool (`launchctl`, `systemctl --user`) and the bound it
/// runs to its end within (05_gateway.md "Deadlines", ffi). The platform gives the argv; the
/// bounds are the same for both.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlan {
    action: ControlAction,
    arguments: Vec<OsString>,
    bound: Bound,
}

impl ControlPlan {
    /// Load the agent from its definition and start it. The manager answers from its own state,
    /// within [`deadline::LAUNCHCTL`].
    pub fn bootstrap(uid: u32, target: &LaunchAgentTarget) -> Self {
        Self {
            action: ControlAction::Bootstrap,
            arguments: platform::control_arguments(ControlAction::Bootstrap, uid, target),
            bound: deadline::LAUNCHCTL,
        }
    }

    /// A bootout of `agent`, as the manager's status read it. The call may wait for the agent's
    /// process up to its exit timeout, so it is bounded by the manager's own deadline for the
    /// bootout ([`agent_cycle::bootout_deadline`]), never by a number of its own.
    pub fn bootout(uid: u32, target: &LaunchAgentTarget, agent: LoadedAgent) -> Self {
        Self {
            action: ControlAction::Bootout,
            arguments: platform::control_arguments(ControlAction::Bootout, uid, target),
            bound: Bound::new(LatencyClass::Ffi, agent_cycle::bootout_deadline(agent)),
        }
    }

    /// A restart of `agent`, as the manager's status read it: `kickstart -k`, or `restart`. It
    /// returns only once the old process has gone and the new one has started: under launchd
    /// 9.03 s at an exit timeout of 2 s (its 10 s respawn throttle from the old start) and
    /// 12.02 s at one of 12 s. So it is bounded as the bootout of the old process and then one
    /// more call.
    pub fn kickstart(uid: u32, target: &LaunchAgentTarget, agent: LoadedAgent) -> Self {
        Self {
            action: ControlAction::Kickstart,
            arguments: platform::control_arguments(ControlAction::Kickstart, uid, target),
            bound: Bound::new(
                LatencyClass::Ffi,
                agent_cycle::bootout_deadline(agent).saturating_add(deadline::LAUNCHCTL.within),
            ),
        }
    }

    /// The manager answers a status read from its own state, within [`deadline::LAUNCHCTL`].
    pub fn print(uid: u32, target: &LaunchAgentTarget) -> Self {
        Self {
            action: ControlAction::Print,
            arguments: platform::control_arguments(ControlAction::Print, uid, target),
            bound: deadline::LAUNCHCTL,
        }
    }

    pub fn action(&self) -> ControlAction {
        self.action
    }

    /// The bound this call runs to its end within.
    pub fn bound(&self) -> Bound {
        self.bound
    }

    pub fn executable(&self) -> &Path {
        Path::new(LAUNCHCTL_EXECUTABLE)
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Success,
    ExitCode(i32),
    Terminated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchctlOutput {
    pub status: CommandStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl LaunchctlOutput {
    pub fn success() -> Self {
        Self {
            status: CommandStatus::Success,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }
}

/// Runs one system tool (`launchctl` or `systemctl`, `codesign`) to its end within `deadline`,
/// its output collected (05_gateway.md "Deadlines", ffi). The deadline's op names the call:
/// `<tool> <verb>`.
pub trait LaunchctlCommand {
    fn run(
        &mut self,
        executable: &Path,
        arguments: &[OsString],
        deadline: &Deadline,
    ) -> Result<LaunchctlOutput, RunError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NativeLaunchctlCommand;

impl LaunchctlCommand for NativeLaunchctlCommand {
    /// Past its bound the call fails `TIMEOUT` naming the tool's pid and the state the kernel
    /// shows it in. The tool is left as it is: a launchctl blocked on launchd is not ended by
    /// giving up on it.
    fn run(
        &mut self,
        executable: &Path,
        arguments: &[OsString],
        deadline: &Deadline,
    ) -> Result<LaunchctlOutput, RunError> {
        // The child's role is its tool, the op's first word.
        let op = deadline.op();
        let role = op.split_once(' ').map_or(op, |(tool, _)| tool);
        let output =
            deadline::output_within(role, Command::new(executable).args(arguments), deadline)?;
        let status = if output.status.success() {
            CommandStatus::Success
        } else if let Some(code) = output.status.code() {
            CommandStatus::ExitCode(code)
        } else {
            CommandStatus::Terminated
        };
        Ok(LaunchctlOutput {
            status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlOutcome {
    pub action: ControlAction,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// The manager's word on whether it holds a service: `Loaded` while it runs it or will start it
/// (launchd: bootstrapped; systemd: active, in transition, or enabled). How its answer says so
/// is the platform's to read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchdServiceStatus {
    Loaded {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    NotLoaded {
        /// What the tool exited with: launchctl's non-zero "no such service", 0 from a tool
        /// that says it in its output.
        exit_code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum ControlExecutionError {
    /// The control tool did not run to its end: it could not start, outlived its bound, or its
    /// output could not be read. Its message names the call.
    Unavailable {
        action: ControlAction,
        source: RunError,
    },
    Rejected {
        action: ControlAction,
        status: CommandStatus,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    InvalidStatusPlan {
        action: ControlAction,
    },
    /// The control tool answered a status plan and its answer states no status.
    UnreadableStatus {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
}

impl fmt::Display for ControlExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { source, .. } => {
                write!(formatter, "{source}")
            }
            Self::Rejected { action, status, .. } => {
                write!(formatter, "{TOOL} {action:?} failed with {status:?}")
            }
            Self::InvalidStatusPlan { action } => {
                write!(formatter, "{TOOL} {action:?} is not a service status plan")
            }
            Self::UnreadableStatus { stdout, .. } => {
                write!(
                    formatter,
                    "{TOOL} answered a status read with no state: {}",
                    String::from_utf8_lossy(stdout).trim()
                )
            }
        }
    }
}

impl Error for ControlExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable { source, .. } => Some(source),
            Self::Rejected { .. }
            | Self::InvalidStatusPlan { .. }
            | Self::UnreadableStatus { .. } => None,
        }
    }
}

/// Single-owner executor for install plans and control plans.
///
/// All methods require `&mut self`; callers should keep this value inside their
/// coordinator actor rather than sharing it through a lock.
#[derive(Debug)]
pub struct LaunchdExecutor<F, C> {
    filesystem: F,
    command: C,
}

impl<F, C> LaunchdExecutor<F, C> {
    pub fn new(filesystem: F, command: C) -> Self {
        Self {
            filesystem,
            command,
        }
    }

    pub fn into_parts(self) -> (F, C) {
        (self.filesystem, self.command)
    }
}

impl<F, C: LaunchctlCommand> LaunchdExecutor<F, C> {
    /// Whether `path` carries `identity`'s signature intact, its designated requirement
    /// satisfied, as `codesign --verify` judges it through the executor's command runner.
    pub fn satisfies(
        &mut self,
        path: &Path,
        identity: crate::code_signing::SigningIdentity,
    ) -> io::Result<bool> {
        let output = self
            .command
            .run(
                Path::new(crate::code_signing::CODESIGN),
                &identity.verify_arguments(path),
                &deadline::start("codesign --verify", deadline::CODESIGN),
            )
            .map_err(io::Error::other)?;
        crate::code_signing::verification(output.status, &output.stderr).map_err(io::Error::other)
    }
}

impl LaunchdExecutor<NativeFilesystem, NativeLaunchctlCommand> {
    pub fn native() -> Self {
        Self::new(NativeFilesystem::new(), NativeLaunchctlCommand)
    }
}

impl<F: LaunchdFilesystem, C: LaunchctlCommand> LaunchdExecutor<F, C> {
    pub fn execute_install(
        &mut self,
        plan: &InstallPlan,
    ) -> Result<InstallOutcome, InstallExecutionError> {
        if plan.is_noop() {
            return Ok(InstallOutcome::NoChange);
        }

        let mut temporary_file: Option<PathBuf> = None;
        for mutation in plan.operations() {
            let (operation, path, result) = match mutation {
                Mutation::EnsureDirectory { path, mode } => (
                    FilesystemOperation::EnsureDirectory,
                    path.as_path(),
                    self.filesystem.ensure_directory(path, *mode),
                ),
                Mutation::SetPermissions { path, mode } => (
                    FilesystemOperation::SetPermissions,
                    path.as_path(),
                    self.filesystem.set_permissions(path, *mode),
                ),
                Mutation::CreateExclusiveTemporaryFile {
                    directory,
                    name_prefix,
                    bytes,
                    mode,
                } => {
                    if temporary_file.is_some() {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::CreateTemporaryFile,
                            reason: "a temporary file is already active",
                        });
                    }
                    match self.filesystem.create_exclusive_no_follow(
                        directory,
                        name_prefix,
                        bytes,
                        *mode,
                    ) {
                        Ok(path) => {
                            temporary_file = Some(path);
                            continue;
                        }
                        Err(source) => (
                            FilesystemOperation::CreateTemporaryFile,
                            directory.as_path(),
                            Err(source),
                        ),
                    }
                }
                Mutation::CreateTemporarySymlink {
                    directory,
                    name_prefix,
                    target,
                } => {
                    if temporary_file.is_some() {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::CreateTemporarySymlink,
                            reason: "a temporary file is already active",
                        });
                    }
                    match self.filesystem.symlink_exclusive_no_follow(
                        directory,
                        name_prefix,
                        target,
                    ) {
                        Ok(path) => {
                            temporary_file = Some(path);
                            continue;
                        }
                        Err(source) => (
                            FilesystemOperation::CreateTemporarySymlink,
                            directory.as_path(),
                            Err(source),
                        ),
                    }
                }
                Mutation::CopyToTemporaryFile {
                    directory,
                    name_prefix,
                    source,
                    mode,
                } => {
                    if temporary_file.is_some() {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::CopyTemporaryFile,
                            reason: "a temporary file is already active",
                        });
                    }
                    match self.filesystem.copy_exclusive_no_follow(
                        directory,
                        name_prefix,
                        source,
                        *mode,
                    ) {
                        Ok(path) => {
                            temporary_file = Some(path);
                            continue;
                        }
                        Err(error) => (
                            FilesystemOperation::CopyTemporaryFile,
                            directory.as_path(),
                            Err(error),
                        ),
                    }
                }
                Mutation::SignTemporaryFile { identity } => {
                    let Some(path) = temporary_file.as_deref() else {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::SignTemporaryFile,
                            reason: "there is no active temporary file",
                        });
                    };
                    let signed = self
                        .command
                        .run(
                            Path::new(crate::code_signing::CODESIGN),
                            &identity.sign_arguments(path),
                            &deadline::start("codesign --sign", deadline::CODESIGN),
                        )
                        .map_err(io::Error::other)
                        .and_then(|output| match output.status {
                            CommandStatus::Success => Ok(()),
                            status => Err(io::Error::other(format!(
                                "{} ended {status:?}: {}",
                                crate::code_signing::CODESIGN,
                                String::from_utf8_lossy(&output.stderr).trim()
                            ))),
                        });
                    (FilesystemOperation::SignTemporaryFile, path, signed)
                }
                Mutation::SyncTemporaryFile => {
                    let Some(path) = temporary_file.as_deref() else {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::SyncTemporaryFile,
                            reason: "there is no active temporary file",
                        });
                    };
                    (
                        FilesystemOperation::SyncTemporaryFile,
                        path,
                        self.filesystem.sync_file(path),
                    )
                }
                Mutation::RenameTemporaryFile { destination } => {
                    let Some(path) = temporary_file.as_deref() else {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::RenameTemporaryFile,
                            reason: "there is no active temporary file",
                        });
                    };
                    let result = self.filesystem.rename(path, destination);
                    if result.is_ok() {
                        temporary_file = None;
                    }
                    (
                        FilesystemOperation::RenameTemporaryFile,
                        destination.as_path(),
                        result,
                    )
                }
                Mutation::RemoveFile { path } => (
                    FilesystemOperation::RemoveFile,
                    path.as_path(),
                    self.filesystem.remove_file(path),
                ),
                Mutation::SyncDirectory { path } => (
                    FilesystemOperation::SyncDirectory,
                    path.as_path(),
                    self.filesystem.sync_directory(path),
                ),
            };

            if let Err(source) = result {
                let failed_path = path.to_path_buf();
                let cleanup_failure = temporary_file.take().and_then(|path| {
                    self.filesystem
                        .remove_file(&path)
                        .err()
                        .map(|source| CleanupFailure { path, source })
                });
                return Err(InstallExecutionError::Filesystem {
                    operation,
                    path: failed_path,
                    source,
                    cleanup_failure,
                });
            }
        }

        if temporary_file.is_some() {
            return Err(InstallExecutionError::InvalidPlan {
                operation: FilesystemOperation::RenameTemporaryFile,
                reason: "the plan left a temporary file active",
            });
        }
        Ok(InstallOutcome::Changed)
    }
}

impl<F, C: LaunchctlCommand> LaunchdExecutor<F, C> {
    pub fn execute_control(
        &mut self,
        plan: &ControlPlan,
    ) -> Result<ControlOutcome, ControlExecutionError> {
        let output = self
            .command
            .run(
                plan.executable(),
                plan.arguments(),
                &deadline::start(plan.action().op(), plan.bound()),
            )
            .map_err(|source| ControlExecutionError::Unavailable {
                action: plan.action(),
                source,
            })?;
        match output.status {
            CommandStatus::Success => Ok(ControlOutcome {
                action: plan.action(),
                stdout: output.stdout,
                stderr: output.stderr,
            }),
            status => Err(ControlExecutionError::Rejected {
                action: plan.action(),
                status,
                stdout: output.stdout,
                stderr: output.stderr,
            }),
        }
    }

    /// Executes a status plan and classifies the idempotent loaded state, as the platform
    /// reads the tool's answer (launchctl: a normal non-zero exit means the service is absent;
    /// systemctl: the properties say). Failure to spawn or signal termination remains an
    /// operational error.
    pub fn execute_status(
        &mut self,
        plan: &ControlPlan,
    ) -> Result<LaunchdServiceStatus, ControlExecutionError> {
        if plan.action() != ControlAction::Print {
            return Err(ControlExecutionError::InvalidStatusPlan {
                action: plan.action(),
            });
        }
        let output = self
            .command
            .run(
                plan.executable(),
                plan.arguments(),
                &deadline::start(plan.action().op(), plan.bound()),
            )
            .map_err(|source| ControlExecutionError::Unavailable {
                action: ControlAction::Print,
                source,
            })?;
        platform::service_status(output)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchdError {
    InvalidPath {
        field: &'static str,
        reason: &'static str,
    },
    InvalidLabel,
    InvalidArgument {
        index: usize,
        reason: &'static str,
    },
    InvalidEnvironmentVariable {
        name: String,
        reason: &'static str,
    },
    PrivilegedProvisioning,
}

impl fmt::Display for LaunchdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath { field, reason } => {
                write!(formatter, "invalid {field} path: {reason}")
            }
            Self::InvalidLabel => write!(formatter, "invalid {MANAGER} label"),
            Self::InvalidArgument { index, reason } => {
                write!(formatter, "invalid service argument {index}: {reason}")
            }
            Self::InvalidEnvironmentVariable { name, reason } => {
                write!(formatter, "invalid environment variable {name:?}: {reason}")
            }
            Self::PrivilegedProvisioning => write!(
                formatter,
                "{MANAGER} services may not create host storage in the foreground"
            ),
        }
    }
}

impl Error for LaunchdError {}

fn validate_canonical_absolute_path(field: &'static str, path: &Path) -> Result<(), LaunchdError> {
    let value = path.to_str().ok_or(LaunchdError::InvalidPath {
        field,
        reason: "must be UTF-8",
    })?;
    if value.is_empty() || !path.is_absolute() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must be absolute",
        });
    }
    if path.parent().is_none() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must not be the filesystem root",
        });
    }
    if value.chars().any(platform::is_unsafe_in_path) {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "contains a control character",
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must be lexically normalized",
        });
    }
    let normalized: PathBuf = path.components().collect();
    if normalized.as_os_str() != path.as_os_str() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must use its canonical lexical spelling",
        });
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), LaunchdError> {
    let valid = !label.is_empty()
        && !label.starts_with('.')
        && !label.ends_with('.')
        && label.split('.').all(|component| !component.is_empty())
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(LaunchdError::InvalidLabel)
    }
}

/// A binary name is one path component, so the derived path stays inside the host-stable
/// directory: a name carrying a separator or a dot form would name something else entirely.
fn validate_binary_name(name: &str) -> Result<(), LaunchdError> {
    let valid = !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.chars().any(is_unsafe_xml_control);
    if valid {
        Ok(())
    } else {
        Err(LaunchdError::InvalidPath {
            field: "executable-name",
            reason: "must be a single path component",
        })
    }
}

fn validate_arguments(arguments: &[String]) -> Result<(), LaunchdError> {
    if arguments.is_empty() {
        return Err(LaunchdError::InvalidArgument {
            index: 0,
            reason: "at least one service argument is required",
        });
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "adopt")
    {
        return Err(LaunchdError::PrivilegedProvisioning);
    }
    for (index, argument) in arguments.iter().enumerate() {
        if argument.is_empty() {
            return Err(LaunchdError::InvalidArgument {
                index,
                reason: "must not be empty",
            });
        }
        if argument.chars().any(is_unsafe_xml_control) {
            return Err(LaunchdError::InvalidArgument {
                index,
                reason: "contains a control character",
            });
        }
    }
    Ok(())
}

fn validate_environment(environment: &[(String, String)]) -> Result<(), LaunchdError> {
    for (name, value) in environment {
        if name.is_empty() {
            return Err(LaunchdError::InvalidEnvironmentVariable {
                name: name.clone(),
                reason: "name must not be empty",
            });
        }
        if name.contains('=') || name.chars().any(is_unsafe_xml_control) {
            return Err(LaunchdError::InvalidEnvironmentVariable {
                name: name.clone(),
                reason: "name contains an unsafe character",
            });
        }
        if value.chars().any(is_unsafe_xml_control) {
            return Err(LaunchdError::InvalidEnvironmentVariable {
                name: name.clone(),
                reason: "value contains a control character",
            });
        }
    }
    Ok(())
}

fn is_unsafe_xml_control(character: char) -> bool {
    matches!(character, '\0'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' | '\u{7f}')
}
