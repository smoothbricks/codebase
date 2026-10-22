use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, ArgAction, ArgMatches, Command as ClapCommand, value_parser};
use cowshed_apfs::DiskTool;
use cowshed_core::api::{
    AbandonedRefSupersession, ExecClass, ExecScope, GitOid, GrantDelta, SupersededReason,
};
use cowshed_core::metadata::{EgressMode, EgressRule, MetadataError, WorkspaceName};
use cowshed_core::repository::RepoId;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::num::{NonZeroU16, NonZeroUsize};
use std::path::PathBuf;
use std::sync::LazyLock;

use crate::help::{self, CommandSpec, EXPECTED_SOURCE_HEAD, EXPECTED_WORKSPACE_INCARNATION, Opt};

/// Every command, in the order the command map lists them.
///
/// This is the list `cowshed --help` prints and the list an unknown command is corrected against,
/// so a verb the parser dispatches is a verb the help knows about.
pub static COMMANDS: &[&CommandSpec] = &[
    &ADOPT,
    &SETUP,
    &FORK,
    &MOVE,
    &CHECKPOINT,
    &RESTORE,
    &LIST,
    &PATH,
    &BUILD_STATE,
    &EXEC,
    &GRANT,
    &REVOKE,
    &REMOVE,
    &FORGET,
    &ATTACH,
    &DETACH,
    &MOUNT,
    &RESIZE,
    &DEFRAG,
    &RESEED,
    &CACHE,
    &REKEY,
    &GC,
    &PUSH,
    &REBASE,
    &LAND,
    &DOCTOR,
    &DISK_COMMAND,
    &LAND_TURN,
    &SUBTREE_ROOT,
    &RETIRE_DESCENDANTS,
    &LOCK,
    &LOCK_HOLDERS,
    &HOLD,
    &TREE_HOLDERS,
    &GATEWAY,
    &CONTROLLER,
    &CREDENTIAL,
    &IDENTITY,
    &SCCACHE,
    &SKILL,
];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GlobalOptions {
    pub json: bool,
    pub project: Option<PathBuf>,
    pub quiet: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cli {
    pub global: GlobalOptions,
    pub command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Adopt(AdoptArgs),
    /// Host storage setup, repair, and teardown. Its subject is the host, so it takes no
    /// project and no workspace — neither is something a stranded machine can name.
    Setup(SetupArgs),
    Fork(ForkArgs),
    Move(MoveArgs),
    Checkpoint(CheckpointArgs),
    Restore(RestoreArgs),
    List(ListArgs),
    Path(PathArgs),
    /// Read the selected checkout's recorded build-state paths without opening its controller.
    BuildState,
    Exec(ExecArgs),
    Grant(GrantArgs),
    Revoke(RevokeArgs),
    Remove(RemoveArgs),
    /// Retire one project's registration on the operator's explicit `--abandon`, from store
    /// records alone. Its subject is the project named `<owner/repo>`, so it takes no checkout
    /// and no `--project`: the checkout is what an abandoned project no longer has.
    Forget(ForgetArgs),
    Attach(AttachArgs),
    Detach(DetachArgs),
    Mount(MountArgs),
    Resize(ResizeArgs),
    Defrag(DefragArgs),
    Reseed(ReseedArgs),
    Rekey(RekeyArgs),
    Cache(CacheCommand),
    Gc(GcArgs),
    Push(PushArgs),
    Rebase(RebaseArgs),
    Land(LandArgs),
    Doctor(DoctorArgs),
    /// One `diskutil` or `hdiutil` invocation through cowshed's disk runner, for a caller that is
    /// not cowshed: no project, no daemon, the tool's own bytes and exit status.
    DiskCommand(DiskCommandArgs),
    /// `land-turn <repo> -- <cmd...>`: one command run as its repository's land turn, in the order
    /// the lands asked, for a caller that is not cowshed: no project, no daemon, no runtime.
    LandTurn(LandTurnArgs),
    /// Admit this live wrapper, report its handle, then replace it with the child command.
    SubtreeRoot(SubtreeRootArgs),
    /// Retire only the retained subtree identified by one handle on standard input.
    RetireDescendants(RetireDescendantsArgs),
    /// `lock <path> -- <cmd...>`: one command run holding a lock file's exclusive `flock(2)`,
    /// which it inherits: no project, no daemon, no runtime.
    Lock(LockArgs),
    /// `lock-holders <path>`: who the kernel shows holding a lock file, as one JSON object.
    LockHolders(LockHoldersArgs),
    /// `hold --control-fd <n> --state-fd <m> -- <cmd...>`: one command held unreaped as the leader
    /// of its own session and group, signalled only through its holder: no project, no daemon.
    Hold(HoldArgs),
    /// `tree-holders <dir>`: every process holding something inside a directory tree, as JSON lines.
    TreeHolders(TreeHoldersArgs),
    Gateway(GatewayCommand),
    /// One coordinator controller connection, served on standard input for an embedding process.
    Controller,
    Credential(CredentialCommand),
    Identity(IdentityCommand),
    Sccache(SccacheCommand),
    Skill(SkillArgs),
    /// `--version` or `-V`: the npm package version.
    Version,
    /// `--help`, `-h`, or `help`: the command map, or one command's page.
    Help(Option<&'static CommandSpec>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectDiscovery {
    Required,
    Optional,
    NotUsed,
}

impl Command {
    pub const fn project_discovery(&self) -> ProjectDiscovery {
        match self {
            Self::Attach(args) if args.all => ProjectDiscovery::NotUsed,
            Self::Detach(_) => ProjectDiscovery::NotUsed,
            Self::Mount(_) => ProjectDiscovery::NotUsed,
            Self::Forget(_) => ProjectDiscovery::NotUsed,
            Self::Adopt(_)
            | Self::Fork(_)
            | Self::Move(_)
            | Self::Checkpoint(_)
            | Self::Restore(_)
            | Self::Path(_)
            | Self::BuildState
            | Self::Exec(_)
            | Self::Grant(_)
            | Self::Revoke(_)
            | Self::Remove(_)
            | Self::Attach(_)
            | Self::Resize(_)
            | Self::Defrag(_)
            | Self::Reseed(_)
            | Self::Rekey(_)
            | Self::Gc(_)
            | Self::Push(_)
            | Self::Rebase(_)
            | Self::Land(_)
            | Self::Credential(_)
            | Self::Cache(_)
            | Self::Controller
            | Self::Identity(_) => ProjectDiscovery::Required,
            Self::List(args) if !args.all => ProjectDiscovery::Optional,
            Self::Doctor(_) => ProjectDiscovery::Optional,
            Self::List(_)
            | Self::Setup(_)
            | Self::Gateway(_)
            | Self::DiskCommand(_)
            | Self::LandTurn(_)
            | Self::SubtreeRoot(_)
            | Self::RetireDescendants(_)
            | Self::Lock(_)
            | Self::LockHolders(_)
            | Self::Hold(_)
            | Self::TreeHolders(_)
            | Self::Sccache(_)
            | Self::Skill(_)
            | Self::Version
            | Self::Help(_) => ProjectDiscovery::NotUsed,
        }
    }
}

/// The descriptor is only a number while parsing: ownership passes at dispatch, after every
/// argument has been validated. The command and its arguments retain their native bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubtreeRootArgs {
    pub handle_fd: std::os::fd::RawFd,
    pub program: OsString,
    pub args: Vec<OsString>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetireDescendantsArgs {
    pub grace: std::time::Duration,
}
/// `disk-command <diskutil|hdiutil> -- <args…>`: the tool and its argv, bytes untouched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskCommandArgs {
    pub tool: DiskTool,
    pub deadline: std::time::Duration,
    pub args: Vec<OsString>,
}

/// `land-turn <repo> [--workspace <ws>] [--outside-lock <path>] -- <cmd...>`: the repository whose
/// land queue the command joins, the workspace the land is for, and the command run under its turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandTurnArgs {
    pub repo: String,
    pub workspace: String,
    pub outside_lock: Option<PathBuf>,
    pub argv: Vec<OsString>,
}

/// `lock <path> [--state-fd <n>] [--owned-pid <pid>]... -- <cmd...>`: the lock file, the descriptor
/// its state lines go to, the lives a cancellation may reach, and the command run holding it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockArgs {
    pub path: PathBuf,
    pub state_fd: Option<i32>,
    /// The pids whose lives the caller owns: captured at the start, the only lives a signal may reach.
    pub owned_pids: Vec<i32>,
    pub argv: Vec<OsString>,
}

/// `hold --control-fd <n> --state-fd <m> -- <cmd...>`: the descriptor requests are read from, the
/// one events are written to, and the command held.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HoldArgs {
    pub control_fd: i32,
    pub state_fd: i32,
    pub argv: Vec<OsString>,
}

/// `lock-holders <path>`: the lock file whose holders the kernel names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockHoldersArgs {
    pub path: PathBuf,
}

/// `tree-holders <dir>`: the tree whose holders the census names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeHoldersArgs {
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkillCommand {
    Install,
}

/// `--harness` names are validated at parse time, so an unknown harness is a
/// usage error before any directory is touched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillArgs {
    pub action: SkillCommand,
    pub harnesses: Vec<String>,
}

/// `stop` takes `--purge` because deactivating an agent and deleting the binary it ran are two
/// different intentions: the ordinary stop leaves the installed copy so the next `start` is a
/// plist write, and `--purge` is for a host that is done with cowshed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayCommand {
    Start,
    Stop { purge: bool },
    Status,
    Run,
}

/// Enrolment is per project, because a credential record is bound to one `repo_id`. The
/// repository is discovered from the checkout like every other project verb, so an operator
/// never types an identity the store already knows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialCommand {
    Add(CredentialAddArgs),
    Status,
    Remove { origin: String },
}

/// The binding's identities are controller authority: an operator binds a remote the project's
/// main checkout already configures, and nothing a workspace can write ever adds one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityCommand {
    Add { remote: String },
}

/// The secret is named, never written on the command line: `--secret-env` names a variable in
/// the operator's own environment, `--secret-command` names a helper to run, `--secret-stdin`
/// reads a pipe. There is deliberately no flag that takes the credential itself — argv is
/// visible to every process on the host and lands in shell history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialAddArgs {
    pub origin: String,
    pub path_prefixes: Vec<String>,
    pub methods: Vec<String>,
    pub secret_env: Option<String>,
    pub secret_command: Option<Vec<String>>,
    pub secret_stdin: bool,
}

/// `setup` runs in one of two directions and never both, so they are one flag apiece rather than a
/// subcommand: the verb's whole promise is that a stranded host can type `cowshed setup`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SetupArgs {
    /// Remove cowshed's host presence — fstab pins, service agents, installed binaries — while
    /// leaving every volume, image, and workspace exactly where it is.
    pub uninstall: bool,
    /// Proceed with `--uninstall` although the volumes still hold workspaces, or although their
    /// occupancy could not be established at all.
    pub force: bool,
    /// Build the patched sccache from cowshed's own nix flake, root the result, and start its
    /// LaunchAgent. Opt-in because not every cowshed user writes Rust and it requires nix.
    pub sccache: bool,
    /// Delete the retired caches volume inside setup's one authorization, once the migration has
    /// left it holding nothing but its marker. A plain setup never escalates for it.
    pub retire_caches_volume: bool,
    /// Install the invoking build without judging it, even when it does not contain the installed
    /// cowshed. Without it such a build is refused, naming the binary and the commits it lacks.
    pub downgrade: bool,
}

/// `start` takes the cache cap because the cap is the one thing a host operator has to be able to
/// override: the derived default is sized from what the store already holds, and a host that
/// builds more graphs than it stores needs a bigger number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SccacheCommand {
    Start { capacity: Option<OsString> },
    Stop,
    Status,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdoptArgs {
    pub path: Option<PathBuf>,
    pub capacity: Option<OsString>,
    pub repo_id: Option<OsString>,
    pub quarantine: bool,
    pub re_adopt: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkArgs {
    pub source: Option<String>,
    pub destination: String,
    pub reference: Option<OsString>,
    pub clean: bool,
    pub browse: bool,
    pub slot: Option<u32>,
    pub register: bool,
    pub git_worktree: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MoveArgs {
    pub source: String,
    pub destination: MoveDestination,
}

/// What `mv`'s second argument means, which the first argument decides.
///
/// `main` is not a renameable workspace — its name is fixed by the project layout — so `mv main`
/// can only mean "move the checkout", and its destination is a path. Every other source names a
/// workspace whose destination is a new workspace name. One verb, two grammars, and the source
/// disambiguates them before either is validated, so a path is never rejected for failing the
/// workspace-name charset and a workspace name is never resolved against the filesystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MoveDestination {
    Workspace(String),
    Checkout(PathBuf),
    RepoId(RepoId),
}

impl std::fmt::Display for MoveDestination {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Workspace(name) => formatter.write_str(name),
            Self::Checkout(path) => write!(formatter, "{}", path.display()),
            Self::RepoId(repo_id) => write!(formatter, "{repo_id}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointArgs {
    pub workspace: Option<String>,
    pub label: Option<OsString>,
    pub keep: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreArgs {
    pub workspace: String,
    pub label: OsString,
}

/// `landing` selects the view; `landed` selects the rows and implies the measurement. They are
/// separate because their consumers are: a human triaging a project wants every row with its
/// counts, and a script retiring landed workspaces wants nothing but the names it may safely pass
/// to `rm`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ListArgs {
    pub all: bool,
    pub landing: bool,
    pub landed: bool,
}

/// `cowshed path` resolves either a workspace by name or a build slot by number. A slot is a
/// stable mount path shared by successive tenants, so `--slot` answers "what absolute path do I
/// build slot n through" without the caller knowing which workspace currently holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathArgs {
    pub workspace: Option<String>,
    pub slot: Option<u32>,
    pub no_attach: bool,
}

/// Parse-stage spelling of core's `StdinSource`: base64 still encoded, the path not yet
/// workspace-validated, and `Empty` spelled as the absence of any flag. `exec_command` in
/// runtime.rs owns the mapping into core; its `cli_stdin_spelling` seam pins variant coverage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StdinSource {
    Stream,
    WorkspaceFile(PathBuf),
    InlineBase64(OsString),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecArgs {
    pub workspace: String,
    pub argv: Vec<OsString>,
    pub stdin: Option<StdinSource>,
    pub read_only: bool,
    pub cwd: Option<PathBuf>,
    pub session: Option<String>,
    pub timeout: Option<OsString>,
    pub background: bool,
    pub stdout_copy: Option<PathBuf>,
    pub stderr_copy: Option<PathBuf>,
    pub replace_output: bool,
    /// The class the command is admitted under (`--class`): background unless the caller names
    /// interactive work it waits on, the operator's gate, or a resident.
    pub class: ExecClass,
    /// The most CPU tokens the command can use at once (`--want`); its grant is between one and
    /// this. `None` asks for the class's own: the background share of the host for background
    /// and interactive work, one token for a gate.
    pub want: Option<NonZeroUsize>,
    /// Where the command runs: the workspace sandbox, or `--host`, the host boundary outside it.
    pub scope: ExecScope,
}

/// Whose grants `grant` reads or changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantTarget {
    /// One workspace's own grants (`cowshed grant <ws>`).
    Workspace(String),
    /// The project's standing grants every workspace runs under (`cowshed grant --project-wide`),
    /// held in the trusted project policy.
    Project,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrantArgs {
    pub target: GrantTarget,
    pub read: Vec<PathBuf>,
    pub write: Vec<PathBuf>,
    pub deny_write: Vec<PathBuf>,
    pub deny: Vec<PathBuf>,
    /// Hosts this workspace may reach through the gateway. Network reach is a separate decision
    /// from filesystem reach, and separately auditable: the gateway logs every admission.
    pub egress: Vec<String>,
    /// The mode every `egress` host is granted in: intercepted by default, or `--opaque` for a
    /// client that cannot trust the workspace CA (a pinned client, or Go on macOS).
    pub egress_mode: EgressMode,
    /// `--ports N`: the minimum number of service ports this workspace's port block must hold,
    /// excluding the gateway listener. Zero is not a request, so it is unrepresentable here.
    pub service_ports: Option<NonZeroU16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevokeArgs {
    pub target: GrantTarget,
    pub selection: RevokeSelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RevokeSelection {
    Items(GrantDelta),
    All,
}

/// `rm <ws>` — retire one workspace.
///
/// The two overrides are separate on purpose. `force` overrides *transient* state: a dirty tree, a
/// half-finished merge, a mount that is still busy. `abandon` is the only flag that authorizes
/// destroying commits `main` does not contain, and it has no short spelling — a script that carries
/// `--force` to get past a stuck workspace must not thereby acquire the power to delete work that
/// has no other home.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveArgs {
    pub workspace: String,
    pub force: bool,
    pub restore: bool,
    pub abandon: bool,
    /// With `main --restore`: remove the whole project first (every session, its images, and
    /// under `abandon` its abandon bundles) — `Coordinator::remove_project`.
    pub purge: bool,
}

/// `forget <owner/repo> --abandon` — the one project to retire.
///
/// The parser refuses a `forget` without `--abandon`, so this type carries no flag for it: a
/// `Command::Forget` that exists was explicitly authorized by the operator who typed it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForgetArgs {
    pub repo_id: RepoId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachArgs {
    pub workspace: Option<String>,
    pub all: bool,
    pub browse: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetachArgs {
    pub workspace: Option<String>,
    pub all: bool,
}

/// `mount main --repo-id <owner/repo>` — mount main from store records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountArgs {
    pub target: MountTarget,
    pub repo_id: RepoId,
}

/// The mount target. Only `main` exists yet; siblings arrive later.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountTarget {
    Main,
}

/// `resize <ws|main> <size>` — grow one workspace's image; `resize <ws|main> --build <size>` —
/// grow its build volume and seed instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResizeArgs {
    pub workspace: String,
    pub capacity: OsString,
    pub volume: cowshed_core::api::dto::ResizeVolume,
}

/// `defrag <ws|main>` — rewrite one workspace's image contiguously.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefragArgs {
    pub workspace: String,
}

/// `cache clear [<ws>]` — delete a workspace's Nx state and the project's Nx store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheCommand {
    Clear { workspace: Option<String> },
}

/// `reseed <ws|main> [--from <target>] [--dry-run]` — discard live build state for a seed clone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReseedArgs {
    pub workspace: String,
    pub from: String,
    pub dry_run: bool,
}

/// `rekey <ws|main>` — rebuild one keyless workspace's CA identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RekeyArgs {
    pub workspace: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcArgs {
    pub dry_run: bool,
    pub abandoned: bool,
    pub supersede: Option<AbandonedRefSupersession>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PushArgs {
    pub workspace: Option<String>,
    pub branch: Option<OsString>,
    pub expected_workspace_incarnation: Option<OsString>,
    pub expected_source_head: Option<OsString>,
    pub expected_destination_head: Option<OsString>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseArgs {
    pub workspace: Option<String>,
    pub into: Option<String>,
    pub onto: Option<OsString>,
    pub expected_workspace_incarnation: Option<OsString>,
    pub expected_source_head: Option<OsString>,
    pub expected_onto_head: Option<OsString>,
    pub gate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandArgs {
    pub workspace: String,
    pub into: Option<String>,
    pub target: Option<OsString>,
    pub checks: Vec<OsString>,
    pub retire: bool,
    pub push_only: bool,
    pub expected_workspace_incarnation: Option<OsString>,
    pub expected_source_head: Option<OsString>,
    pub expected_target_head: Option<OsString>,
    pub ref_lock: Option<OsString>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DoctorArgs {
    pub repair: bool,
    pub clear_attach_budget: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UsageErrorKind {
    MissingCommand,
    InvalidArguments,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageError {
    pub kind: UsageErrorKind,
    pub message: String,
    pub hint: String,
}

impl UsageError {
    /// A refusal from inside one command, hinting that command's own grammar.
    fn new(message: impl Into<String>, spec: &'static CommandSpec) -> Self {
        Self {
            kind: UsageErrorKind::InvalidArguments,
            message: message.into(),
            hint: spec.hint(),
        }
    }

    /// A refusal from outside any command, which has to spell its own next step.
    fn with_hint(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            kind: UsageErrorKind::InvalidArguments,
            message: message.into(),
            hint: hint.into(),
        }
    }

    fn missing_command() -> Self {
        Self {
            kind: UsageErrorKind::MissingCommand,
            message: "a command is required".to_owned(),
            hint: "cowshed --help".to_owned(),
        }
    }

    pub const fn exit_code(&self) -> i32 {
        cowshed_core::ErrorCode::Usage.exit_code() as i32
    }

    pub fn command_map(&self) -> Option<&'static str> {
        match self.kind {
            UsageErrorKind::MissingCommand => Some(help::bare_invocation()),
            UsageErrorKind::InvalidArguments => None,
        }
    }
}

impl fmt::Display for UsageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} — run: {}", self.message, self.hint)
    }
}

impl std::error::Error for UsageError {}

pub fn parse_args<I, T>(args: I) -> Result<Cli, UsageError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    if let Some(cli) = parse_help_request(&args)? {
        return Ok(cli);
    }
    match cli_command().try_get_matches_from(&args) {
        Ok(matches) => cli_from_matches(matches),
        Err(error) if error.kind() == ErrorKind::DisplayVersion => Ok(Cli {
            global: GlobalOptions::default(),
            command: Command::Version,
        }),
        Err(error) => Err(translate_clap(error, &args)),
    }
}

/// `--help` is an answer, not a grammar check: it wins even on a half-typed line,
/// and clap never sees it because its help flag is disabled (stdout purity).
fn parse_help_request(args: &[OsString]) -> Result<Option<Cli>, UsageError> {
    let mut global = GlobalOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].to_str() {
            Some("--json") => {
                global.json = true;
                index += 1;
            }
            Some("-q" | "--quiet") => {
                global.quiet = true;
                index += 1;
            }
            Some("--project") => {
                index += 1;
                let value = args.get(index).ok_or_else(|| {
                    UsageError::with_hint(
                        "--project requires a git root",
                        "cowshed --project <git-root> <command>",
                    )
                })?;
                global.project = Some(PathBuf::from(value));
                index += 1;
            }
            Some("--help" | "-h" | "help") => {
                return Ok(Some(Cli {
                    global,
                    command: parse_help(args, index + 1)?,
                }));
            }
            Some(name) if !name.starts_with('-') => {
                if wants_help(&args[index + 1..]) {
                    let spec = help::command_named(name).ok_or_else(|| unknown_command(name))?;
                    return Ok(Some(Cli {
                        global,
                        command: Command::Help(Some(spec)),
                    }));
                }
                return Ok(None);
            }
            _ => return Ok(None),
        }
    }
    Ok(None)
}

/// `help [<command>]`, and the `--help`/`-h` spellings of the same request.
fn parse_help(args: &[OsString], mut index: usize) -> Result<Command, UsageError> {
    const HINT: &str = "cowshed help [<command>]";
    let mut topic = None;
    while index < args.len() {
        match args[index].to_str() {
            Some("--help" | "-h") => {}
            Some(name) if !name.starts_with('-') && topic.is_none() => {
                topic = Some(help::command_named(name).ok_or_else(|| unknown_command(name))?);
            }
            _ => {
                let argument = args[index].to_string_lossy();
                return Err(UsageError::with_hint(
                    format!("help describes one command at a time, not `{argument}`"),
                    HINT,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Help(topic))
}

/// Whether `--help` appears before the `--` that ends cowshed's own arguments.
fn wants_help(arguments: &[OsString]) -> bool {
    arguments
        .iter()
        .map(|argument| argument.to_str())
        .take_while(|argument| *argument != Some("--"))
        .any(|argument| argument == Some("--help") || argument == Some("-h"))
}

/// A verb nobody has, corrected against the ones that exist.
///
/// The correction matters more than the refusal: an agent that mistypes a verb otherwise retries
/// the same spelling, and a near miss is the overwhelmingly common way a command line is wrong.
fn unknown_command(name: &str) -> UsageError {
    let nearest = help::nearest_commands(name);
    let message = if nearest.is_empty() {
        format!("unknown command `{name}`")
    } else {
        format!(
            "unknown command `{name}`; did you mean: {}",
            nearest.join(", ")
        )
    };
    UsageError::with_hint(message, "cowshed --help")
}

/// The package manifest is embedded in every native CLI artifact, so `--version` reports the npm
/// release version even when no package tree exists beside the extracted binary.
pub fn package_version() -> &'static str {
    #[derive(serde::Deserialize)]
    struct PackageManifest {
        version: String,
    }

    static VERSION: LazyLock<String> = LazyLock::new(|| {
        serde_json::from_str::<PackageManifest>(include_str!("../../../package.json"))
            .expect("cowshed package.json must contain a string version")
            .version
    });
    VERSION.as_str()
}

fn cli_command() -> ClapCommand {
    ClapCommand::new("cowshed")
        .no_binary_name(true)
        .disable_help_flag(true)
        .disable_help_subcommand(true)
        .version(package_version())
        .args(global_args())
        .subcommand(leaf("adopt").arg(positional("path", 0..=1)).args([
            value("capacity"),
            value("repo-id"),
            flag("quarantine"),
            flag("re-adopt"),
        ]))
        .subcommand(leaf("setup").args([
            flag("uninstall"),
            flag("force"),
            flag("sccache"),
            flag("retire-caches-volume"),
            flag("downgrade"),
        ]))
        .subcommand(
            leaf("fork")
                .arg(positional("workspaces", 0..=2).num_args(0..=2))
                .args([
                    value("ref"),
                    flag("clean"),
                    flag("browse"),
                    value("slot"),
                    flag("register"),
                    flag("git-worktree"),
                    flag("markdown-only"),
                ]),
        )
        .subcommand(
            leaf("mv")
                .arg(positional("src", 0..=1))
                .arg(positional("dst", 0..=1))
                .arg(value("repo-id")),
        )
        .subcommand(
            leaf("checkpoint")
                .arg(positional("workspace", 0..=1))
                .arg(positional("label", 0..=1))
                .arg(flag("keep")),
        )
        .subcommand(
            leaf("restore")
                .arg(positional("workspace", 0..=1))
                .arg(positional("label", 0..=1)),
        )
        .subcommand(leaf("ls").args([flag("all"), flag("landing"), flag("landed")]))
        .subcommand(
            leaf("path")
                .arg(positional("workspace", 0..=1))
                .args([value("slot"), flag("no-attach")]),
        )
        .subcommand(leaf("build-state"))
        .subcommand(
            leaf("exec")
                .arg(positional("workspace", 0..=1))
                .args([
                    flag("stdin"),
                    value("stdin-file"),
                    value("stdin-base64"),
                    flag("ro"),
                    value("cwd"),
                    value("session"),
                    value("timeout"),
                    flag("background"),
                    value_once("stdout-copy"),
                    value_once("stderr-copy"),
                    flag("replace-output"),
                    value("class"),
                    value("want"),
                    flag("host"),
                ])
                .arg(
                    Arg::new("argv")
                        .value_parser(value_parser!(OsString))
                        .num_args(0..)
                        .last(true)
                        .allow_hyphen_values(true),
                ),
        )
        .subcommand(
            leaf("grant")
                .arg(positional("workspace", 0..=1))
                .arg(flag("project-wide"))
                .args([
                    path_values("read"),
                    path_values("write"),
                    path_values("deny-write"),
                    path_values("deny"),
                ])
                .arg(append_value("egress"))
                .arg(flag("opaque"))
                .arg(value("ports")),
        )
        .subcommand(
            leaf("revoke")
                .arg(positional("workspace", 0..=1))
                .arg(flag("project-wide"))
                .args([
                    path_values("read"),
                    path_values("write"),
                    path_values("deny-write"),
                    path_values("deny"),
                ])
                .arg(append_value("egress"))
                .arg(flag("all")),
        )
        .subcommand(leaf("rm").arg(positional("workspace", 0..=1)).args([
            flag("force"),
            flag("restore"),
            flag("abandon"),
            flag("purge"),
        ]))
        .subcommand(
            leaf("forget")
                .arg(positional("repo", 0..=1))
                .arg(flag("abandon")),
        )
        .subcommand(
            leaf("attach")
                .arg(positional("workspace", 0..=1))
                .args([flag("all"), flag("browse")]),
        )
        .subcommand(
            leaf("detach")
                .arg(positional("workspace", 0..=1))
                .arg(flag("all")),
        )
        .subcommand(
            leaf("mount")
                .arg(positional("target", 0..=1))
                .arg(value("repo-id")),
        )
        .subcommand(
            leaf("resize")
                .arg(positional("workspace", 0..=1))
                .arg(positional("size", 0..=1))
                .arg(value_once("build")),
        )
        .subcommand(leaf("defrag").arg(positional("workspace", 0..=1)))
        .subcommand(
            leaf("reseed")
                .arg(positional("workspace", 0..=1))
                .arg(value_once("from"))
                .arg(flag("dry-run")),
        )
        .subcommand(leaf("rekey").arg(positional("workspace", 0..=1)))
        .subcommand(
            leaf("cache")
                .subcommand_required(true)
                .subcommand(leaf("clear").arg(positional("workspace", 0..=1))),
        )
        .subcommand(leaf("gc").args([
            flag("dry-run"),
            flag("abandoned"),
            value("supersede"),
            value("reason"),
        ]))
        .subcommand(leaf("push").arg(positional("workspace", 0..=1)).args([
            value("branch"),
            value("expected-workspace-incarnation"),
            value("expected-source-head"),
            value("expected-destination-head"),
        ]))
        .subcommand(leaf("rebase").arg(positional("workspace", 0..=1)).args([
            value("into"),
            value("onto"),
            value("expected-workspace-incarnation"),
            value("expected-source-head"),
            value("expected-onto-head"),
            flag("gate"),
        ]))
        .subcommand(leaf("land").arg(positional("workspace", 0..=1)).args([
            value("into"),
            value("target"),
            append_value("check"),
            flag("no-retire"),
            flag("push-only"),
            value("expected-workspace-incarnation"),
            value("expected-source-head"),
            value("expected-target-head"),
            value("ref-lock"),
        ]))
        .subcommand(leaf("doctor").args([flag("repair"), flag("clear-attach-budget")]))
        .subcommand(
            leaf("disk-command")
                .arg(positional("tool", 0..=1))
                .arg(value("deadline-ms"))
                .arg(
                    Arg::new("argv")
                        .value_parser(value_parser!(OsString))
                        .num_args(0..)
                        .last(true)
                        .allow_hyphen_values(true),
                ),
        )
        .subcommand(
            leaf("land-turn")
                .arg(positional("repo", 0..=1))
                .arg(value("workspace"))
                .arg(value("outside-lock"))
                .arg(
                    Arg::new("argv")
                        .value_parser(value_parser!(OsString))
                        .num_args(0..)
                        .last(true)
                        .allow_hyphen_values(true),
                ),
        )
        .subcommand(
            leaf("subtree-root").arg(value_once("handle-fd")).arg(
                Arg::new("argv")
                    .value_parser(value_parser!(OsString))
                    .num_args(0..)
                    .last(true)
                    .allow_hyphen_values(true),
            ),
        )
        .subcommand(leaf("retire-descendants").arg(value_once("grace-ms")))
        .subcommand(
            leaf("lock")
                .arg(positional("path", 0..=1))
                .arg(value("state-fd"))
                .arg(append_value("owned-pid"))
                .arg(
                    Arg::new("argv")
                        .value_parser(value_parser!(OsString))
                        .num_args(0..)
                        .last(true)
                        .allow_hyphen_values(true),
                ),
        )
        .subcommand(leaf("lock-holders").arg(positional("path", 0..=1)))
        .subcommand(
            leaf("hold")
                .arg(value("control-fd"))
                .arg(value("state-fd"))
                .arg(
                    Arg::new("argv")
                        .value_parser(value_parser!(OsString))
                        .num_args(0..)
                        .last(true)
                        .allow_hyphen_values(true),
                ),
        )
        .subcommand(leaf("tree-holders").arg(positional("path", 0..=1)))
        .subcommand(
            leaf("gateway")
                .subcommand_required(true)
                .subcommand(leaf("start"))
                .subcommand(leaf("stop").arg(flag("purge")))
                .subcommand(leaf("status"))
                .subcommand(leaf("run")),
        )
        .subcommand(leaf("controller"))
        .subcommand(
            leaf("credential")
                .subcommand_required(true)
                .subcommand(leaf("add").args([
                    value("origin"),
                    append_value("path-prefix"),
                    append_value("method"),
                    value("secret-env"),
                    value("secret-command"),
                    flag("secret-stdin"),
                ]))
                .subcommand(leaf("ls"))
                .subcommand(leaf("status"))
                .subcommand(leaf("rm").arg(value("origin"))),
        )
        .subcommand(
            leaf("identity")
                .subcommand_required(true)
                .subcommand(leaf("add").arg(positional("remote", 1..=1))),
        )
        .subcommand(
            leaf("sccache")
                .subcommand_required(true)
                .subcommand(leaf("start").arg(value("capacity")))
                .subcommand(leaf("stop"))
                .subcommand(leaf("status")),
        )
        .subcommand(
            leaf("skill")
                .subcommand_required(true)
                .subcommand(leaf("install").arg(append_value("harness"))),
        )
}

fn global_args() -> [Arg; 3] {
    [
        Arg::new("json")
            .long("json")
            .action(ArgAction::SetTrue)
            .global(true),
        Arg::new("quiet")
            .short('q')
            .long("quiet")
            .action(ArgAction::SetTrue)
            .global(true),
        Arg::new("project")
            .long("project")
            .value_name("git-root")
            .value_parser(value_parser!(PathBuf))
            .allow_hyphen_values(true)
            .global(true),
    ]
}

fn leaf(name: &'static str) -> ClapCommand {
    ClapCommand::new(name)
        .disable_help_flag(true)
        .disable_version_flag(true)
}

fn flag(name: &'static str) -> Arg {
    Arg::new(name).long(name).action(ArgAction::SetTrue)
}

fn value(name: &'static str) -> Arg {
    Arg::new(name)
        .long(name)
        .num_args(1)
        .value_parser(value_parser!(OsString))
        .allow_hyphen_values(true)
        .overrides_with(name)
}

fn value_once(name: &'static str) -> Arg {
    Arg::new(name)
        .long(name)
        .num_args(1)
        .value_parser(value_parser!(OsString))
        .allow_hyphen_values(true)
}

fn append_value(name: &'static str) -> Arg {
    Arg::new(name)
        .long(name)
        .num_args(1)
        .value_parser(value_parser!(OsString))
        .allow_hyphen_values(true)
        .action(ArgAction::Append)
}

fn path_values(name: &'static str) -> Arg {
    Arg::new(name)
        .long(name)
        .num_args(1..)
        .value_parser(value_parser!(PathBuf))
        .action(ArgAction::Append)
}

fn positional(name: &'static str, _range: std::ops::RangeInclusive<usize>) -> Arg {
    Arg::new(name)
        .value_parser(value_parser!(OsString))
        .required(false)
        .num_args(1)
}

fn cli_from_matches(matches: ArgMatches) -> Result<Cli, UsageError> {
    let Some((name, leaf)) = matches.subcommand() else {
        return Err(UsageError::missing_command());
    };
    let global = merge_globals(&matches, leaf);
    let command = match name {
        "adopt" => parse_adopt(leaf)?,
        "setup" => parse_setup(leaf, &global)?,
        "fork" => parse_fork(leaf)?,
        "mv" => parse_move(leaf)?,
        "checkpoint" => parse_checkpoint(leaf)?,
        "restore" => parse_restore(leaf)?,
        "ls" => parse_list(leaf)?,
        "path" => parse_path(leaf)?,
        "build-state" => Command::BuildState,
        "exec" => parse_exec(leaf)?,
        "grant" => parse_grant(leaf)?,
        "revoke" => parse_revoke(leaf)?,
        "rm" => parse_remove(leaf)?,
        "forget" => parse_forget(leaf, &global)?,
        "attach" => parse_attach(leaf)?,
        "detach" => parse_detach(leaf)?,
        "mount" => parse_mount(leaf)?,
        "resize" => parse_resize(leaf)?,
        "defrag" => parse_defrag(leaf)?,
        "reseed" => parse_reseed(leaf)?,
        "cache" => parse_cache(leaf)?,
        "rekey" => parse_rekey(leaf)?,
        "gc" => parse_gc(leaf)?,
        "push" => parse_push(leaf)?,
        "rebase" => parse_rebase(leaf)?,
        "land" => parse_land(leaf)?,
        "doctor" => parse_doctor(leaf)?,
        "disk-command" => parse_disk_command(leaf, &global)?,
        "land-turn" => parse_land_turn(leaf, &global)?,
        "subtree-root" => parse_subtree_root(leaf, &global)?,
        "retire-descendants" => parse_retire_descendants(leaf, &global)?,
        "lock" => parse_lock(leaf, &global)?,
        "lock-holders" => parse_lock_holders(leaf, &global)?,
        "hold" => parse_hold(leaf, &global)?,
        "tree-holders" => parse_tree_holders(leaf, &global)?,
        "controller" => Command::Controller,
        "gateway" => parse_gateway(leaf, &global)?,
        "credential" => parse_credential(leaf)?,
        "identity" => parse_identity(leaf)?,
        "sccache" => parse_sccache(leaf, &global)?,
        "skill" => parse_skill(leaf, &global)?,
        other => return Err(unknown_command(other)),
    };
    Ok(Cli { global, command })
}

fn merge_globals(root: &ArgMatches, leaf: &ArgMatches) -> GlobalOptions {
    let mut global = globals_from(root);
    overlay_globals(&mut global, leaf);
    if let Some((_, child)) = leaf.subcommand() {
        overlay_globals(&mut global, child);
        if let Some((_, inner)) = child.subcommand() {
            overlay_globals(&mut global, inner);
        }
    }
    global
}

fn overlay_globals(global: &mut GlobalOptions, matches: &ArgMatches) {
    let nested = globals_from(matches);
    if nested.json {
        global.json = true;
    }
    if nested.quiet {
        global.quiet = true;
    }
    if nested.project.is_some() {
        global.project = nested.project;
    }
}

fn globals_from(matches: &ArgMatches) -> GlobalOptions {
    GlobalOptions {
        json: matches.get_flag("json"),
        quiet: matches.get_flag("quiet"),
        project: matches.get_one::<PathBuf>("project").cloned(),
    }
}

fn translate_clap(error: clap::Error, args: &[OsString]) -> UsageError {
    let spec = spec_from_argv(args);
    match error.kind() {
        ErrorKind::DisplayHelp
        | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        | ErrorKind::MissingSubcommand => match spec {
            None => UsageError::missing_command(),
            Some(spec) => UsageError::new(spec.missing, spec),
        },
        ErrorKind::InvalidSubcommand => {
            let name = context_string(&error, ContextKind::InvalidSubcommand)
                .or_else(|| first_unrecognized(args).map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned());
            match spec {
                Some(spec)
                    if matches!(spec.name, "gateway" | "credential" | "sccache" | "skill") =>
                {
                    UsageError::new(format!("unknown {} action `{name}`", spec.name), spec)
                }
                _ => unknown_command(&name),
            }
        }
        ErrorKind::UnknownArgument => {
            let token = context_string(&error, ContextKind::InvalidArg)
                .or_else(|| first_unrecognized(args).map(str::to_owned))
                .unwrap_or_else(|| "flag".to_owned());
            match spec {
                None => unknown_command(&token),
                Some(spec) if spec.name == "setup" && !token.starts_with('-') => UsageError::new(
                    format!("setup takes no arguments, only flags; got `{token}`"),
                    spec,
                ),
                Some(spec) if spec.name == "exec" && !token.starts_with('-') => {
                    UsageError::new("exec requires `--` before the child argv", spec)
                }
                Some(spec) if spec.name == "forget" && !token.starts_with('-') => UsageError::new(
                    format!("forget names exactly one owner/repo; got an extra `{token}`"),
                    spec,
                ),
                Some(spec) if spec.name == "disk-command" && !token.starts_with('-') => {
                    UsageError::new(
                        "disk-command requires `--` before the tool's arguments",
                        spec,
                    )
                }
                Some(spec)
                    if matches!(spec.name, "land-turn" | "lock" | "hold")
                        && !token.starts_with('-') =>
                {
                    UsageError::new(
                        format!("{} requires `--` before the command", spec.name),
                        spec,
                    )
                }
                Some(spec) if token.starts_with('-') => unknown_flag(&token, spec),
                Some(spec) => UsageError::new(
                    format!("{} accepts no positional arguments", spec.name),
                    spec,
                ),
            }
        }
        ErrorKind::TooManyValues | ErrorKind::ArgumentConflict => {
            let option = context_string(&error, ContextKind::InvalidArg)
                .unwrap_or_else(|| "option".to_owned());
            if let Some(spec) = spec {
                UsageError::new(format!("{option} may only be specified once"), spec)
            } else {
                UsageError::with_hint(
                    format!("{option} may only be specified once"),
                    "cowshed --help",
                )
            }
        }

        _ => {
            let head = error_head(&error);
            let message = if head.contains("multiple times") {
                let option = context_string(&error, ContextKind::InvalidArg)
                    .unwrap_or_else(|| "option".to_owned());
                format!("{option} may only be specified once")
            } else {
                head
            };
            if let Some(spec) = spec {
                UsageError::new(message, spec)
            } else {
                UsageError::with_hint(message, "cowshed --help")
            }
        }
    }
}

fn error_head(error: &clap::Error) -> String {
    error
        .to_string()
        .lines()
        .next()
        .unwrap_or("invalid arguments")
        .trim()
        .trim_start_matches("error: ")
        .to_owned()
}

fn context_string(error: &clap::Error, wanted: ContextKind) -> Option<String> {
    error.context().find_map(|(kind, value)| {
        if kind != wanted {
            return None;
        }
        match value {
            ContextValue::String(text) => Some(text.clone()),
            ContextValue::Strings(texts) => texts.first().cloned(),
            _ => None,
        }
    })
}

fn spec_from_argv(args: &[OsString]) -> Option<&'static CommandSpec> {
    verb_from_argv(args).and_then(help::command_named)
}

fn verb_from_argv(args: &[OsString]) -> Option<&str> {
    let mut index = 0;
    while index < args.len() {
        match args[index].to_str() {
            Some("--") => return None,
            Some("--project") => index += 2,
            Some(flag) if flag.starts_with('-') => index += 1,
            Some(name) => return Some(name),
            None => return None,
        }
    }
    None
}

/// The output-shaping globals a refused line asked for.
///
/// Only `--json` and `--quiet`, because only they decide how a refusal is written. A walk cannot
/// answer `--project` honestly — it cannot tell a missing value from an absent flag the way the
/// parser can — so it does not claim to.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutputGlobals {
    pub json: bool,
    pub quiet: bool,
}

/// Read the output globals by walking a line instead of parsing it.
///
/// A usage error is by definition a line the parser refused, so there is no [`Cli`] to take
/// `--json` and `--quiet` from, and the refusal still has to be written in the shape the caller
/// asked for. This is [`verb_from_argv`]'s walk: it stops at `--`, because everything after that
/// is a child's argv and none of it is addressed to cowshed, and it steps over `--project`'s
/// value, because a git root spelled `--json` is a path. A scan that reads that path as a flag
/// answers `cowshed --project --json <bogus>` with a JSON envelope nobody requested.
#[must_use]
pub fn globals_before_child_argv(args: &[OsString]) -> OutputGlobals {
    let mut globals = OutputGlobals::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].to_str() {
            Some("--") => break,
            Some("--project") => index += 2,
            Some("--json") => {
                globals.json = true;
                index += 1;
            }
            Some("-q" | "--quiet") => {
                globals.quiet = true;
                index += 1;
            }
            _ => index += 1,
        }
    }
    globals
}

fn first_unrecognized(args: &[OsString]) -> Option<&str> {
    args.iter()
        .filter_map(|argument| argument.to_str())
        .find(|argument| {
            *argument != "--"
                && help::command_named(argument).is_none()
                && !matches!(
                    *argument,
                    "--json"
                        | "-q"
                        | "--quiet"
                        | "--project"
                        | "start"
                        | "stop"
                        | "status"
                        | "run"
                        | "install"
                )
        })
}

fn os(matches: &ArgMatches, name: &str) -> Option<OsString> {
    matches.get_one::<OsString>(name).cloned()
}

fn flagged(matches: &ArgMatches, name: &str) -> bool {
    matches.get_flag(name)
}

/// A required `--flag <value>` that must be UTF-8. Origins, paths and methods are protocol text,
/// so a non-UTF-8 spelling is a usage error rather than something to carry as bytes.
fn required_value(
    matches: &ArgMatches,
    name: &str,
    usage: &'static CommandSpec,
    missing: &str,
) -> Result<String, UsageError> {
    let value = os(matches, name).ok_or_else(|| UsageError::new(missing, usage))?;
    value
        .into_string()
        .map_err(|_| UsageError::new(format!("--{name} must be valid UTF-8"), usage))
}

/// Every occurrence of a repeatable `--flag <value>`, in the order they were typed.
///
/// A value that is not UTF-8 is refused rather than skipped: origins, prefixes and methods are
/// protocol text, and dropping one silently would enrol a narrower scope than the operator
/// wrote — or grant one host fewer than they asked for — while reporting success.
fn appended_values(
    matches: &ArgMatches,
    name: &str,
    usage: &'static CommandSpec,
) -> Result<Vec<String>, UsageError> {
    let Some(values) = matches.get_many::<OsString>(name) else {
        return Ok(Vec::new());
    };
    values
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| UsageError::new(format!("--{name} must be valid UTF-8"), usage))
        })
        .collect()
}

fn require_workspace(
    matches: &ArgMatches,
    field: &str,
    reserve_main: bool,
    usage: &'static CommandSpec,
    missing: &str,
) -> Result<String, UsageError> {
    let value = os(matches, field).ok_or_else(|| UsageError::new(missing, usage))?;
    workspace_name(&value, reserve_main, usage)
}

fn optional_workspace(
    matches: &ArgMatches,
    field: &str,
    reserve_main: bool,
    usage: &'static CommandSpec,
) -> Result<Option<String>, UsageError> {
    os(matches, field)
        .map(|value| workspace_name(&value, reserve_main, usage))
        .transpose()
}

fn parse_slot_value(
    matches: &ArgMatches,
    usage: &'static CommandSpec,
) -> Result<Option<u32>, UsageError> {
    let Some(value) = os(matches, "slot") else {
        return Ok(None);
    };
    value
        .to_str()
        .and_then(|text| text.parse().ok())
        .map(Some)
        .ok_or_else(|| UsageError::new("--slot must be an unsigned integer", usage))
}

fn reject_project(
    global: &GlobalOptions,
    usage: &'static CommandSpec,
    message: &str,
) -> Result<(), UsageError> {
    if global.project.is_some() {
        return Err(UsageError::new(message, usage));
    }
    Ok(())
}

const DISK_COMMAND: CommandSpec = CommandSpec {
    name: "disk-command",
    missing: "disk-command requires a tool: diskutil or hdiutil",
    args: "<diskutil|hdiutil>",
    trailing: "-- <args...>",
    summary: "run disk tools through cowshed",
    about: &[
        "Runs one `diskutil` or `hdiutil` argv — never a shell string — exactly as cowshed runs its own disk tools: under the host's disk-lifecycle lease, in its own session, with the tool's stdout and stderr passed through as its own bytes and its exit status as this command's (a signal ends it with 128 + the signal). The command's standard input is forwarded to the tool, so a passphrase can be piped to `diskutil apfs unlockVolume … -stdinpassphrase`. It needs no project and asks no daemon: scripts and other programs call it from any directory.",
        "A command that creates or releases a disk-image device — `diskutil image …`, `diskutil eject` and every `hdiutil` verb — is never killed: past 120 s it is waited for, with a `cowshed:` warning on stderr every 30 s, because one killed mid-operation can leak its kernel device until reboot. It also ignores hangup, interrupt and terminate and writes its output to files the runner reads back, so a caller that gives up and kills this command leaves the operation to finish. Every other verb has one total monotonic budget for lease admission and child execution, 120 s by default or --deadline-ms before --. An exhausted budget never executes the native tool; an overrun kills the owned group and waits for the child's actual termination before reporting a wrapper timeout.",
        "Failures of the runner itself — the tool could not be started or waited for, or an ordinary verb hit its deadline — exit 100–106 with a `cowshed:` line on stderr and nothing on stdout, as `exec` reports its wrapper failures. `--json` is refused: the answer is the tool's own output, so ask the tool for structure (`-plist`).",
    ],
    options: &[Opt {
        spelling: "--deadline-ms <positive integer>",
        meaning: "before -- only: total SAFE admission-and-child budget in milliseconds; default 120000. Image commands retain their 120 s warning-only, never-kill policy",
    }],
};

fn parse_disk_command(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &DISK_COMMAND;
    reject_project(
        global,
        USAGE,
        "--project is not valid for disk-command: it names no project",
    )?;
    if global.json {
        return Err(UsageError::new(
            "disk-command answers with the tool's own bytes, never a JSON envelope; drop --json and ask the tool for structure (`-plist`)",
            USAGE,
        ));
    }
    let name = os(matches, "tool").ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let tool = name.to_str().and_then(DiskTool::named).ok_or_else(|| {
        UsageError::new(
            format!(
                "disk-command runs diskutil or hdiutil, named without a path; got `{}`",
                name.to_string_lossy()
            ),
            USAGE,
        )
    })?;
    let args: Vec<OsString> = matches
        .get_many::<OsString>("argv")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    if args.is_empty() {
        return Err(UsageError::new(
            if matches.contains_id("argv") {
                "disk-command requires the tool's arguments after `--`"
            } else {
                "disk-command requires `--` before the tool's arguments"
            },
            USAGE,
        ));
    }
    let deadline = match os(matches, "deadline-ms") {
        None => cowshed_apfs::DISK_CHILD_DEADLINE,
        Some(value) => {
            let millis = value
                .to_str()
                .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
                .and_then(|text| text.parse::<std::num::NonZeroU64>().ok())
                .ok_or_else(|| {
                    UsageError::new(
                        "--deadline-ms must be a positive integer number of milliseconds",
                        USAGE,
                    )
                })?;
            std::time::Duration::from_millis(millis.get())
        }
    };
    Ok(Command::DiskCommand(DiskCommandArgs {
        tool,
        deadline,
        args,
    }))
}

const LAND_TURN: CommandSpec = CommandSpec {
    name: "land-turn",
    missing: "land-turn requires a repository name before `--`",
    args: "<repo>",
    trailing: "-- <cmd...>",
    summary: "run a command as a land turn",
    about: &[
        "Takes this repository's land turn and runs `<cmd...>` under it. The command is argv, never a shell string, and it inherits cowshed's standard streams and environment. Turns are granted first come, first served per repository: a land that asks while another holds the turn waits behind it and says on stderr who holds it, how many are ahead, and when the holder asked. The report repeats every 90 s, the land watchdog's cadence, and `taken after N s` says how long the wait was.",
        "The turn is released only after the command ends, so an interrupted land still waits for the command's real end. The command inherits the descriptors of its ticket and of the outside lock, so the kernel keeps the turn until the command ends even if cowshed is killed first. SIGINT, SIGHUP and SIGTERM that cowshed receives are forwarded to the command; a signal that arrives while the turn is still queued ends cowshed and gives up its place. The command's exit status is cowshed's, and a signal ends it with 128 + the signal.",
        "It needs no project and asks no daemon: a land script calls it from any directory. `--json` is refused, since its output is the command's own bytes and cowshed's stderr lines.",
    ],
    options: &[
        Opt {
            spelling: "--workspace <ws>",
            meaning: "the workspace this land is for, shown to the lands queued behind it; defaults to `-`",
        },
        Opt {
            spelling: "--outside-lock <path>",
            meaning: "the lock file that lands which do not ask for turns hold; once the turn reaches the head of the queue it waits for that lock, then holds it exclusively until the command ends",
        },
    ],
};

fn parse_land_turn(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &LAND_TURN;
    reject_project(
        global,
        USAGE,
        "--project is not valid for land-turn: it names no project",
    )?;
    if global.json {
        return Err(UsageError::new(
            "land-turn writes no JSON envelope: its output is the command's own bytes and cowshed's stderr lines; drop --json",
            USAGE,
        ));
    }
    let repo = os(matches, "repo").ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let repo = repo
        .to_str()
        .filter(|name| valid_repo_name(name))
        .map(str::to_owned)
        .ok_or_else(|| {
            UsageError::new(
                format!(
                    "land-turn names a repository as 1 to 64 of A-Z a-z 0-9 . _ -, not starting with `.`; got `{}`",
                    repo.to_string_lossy()
                ),
                USAGE,
            )
        })?;
    let workspace = match os(matches, "workspace") {
        None => "-".to_owned(),
        Some(value) => value
            .into_string()
            .ok()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                UsageError::new(
                    "--workspace must be a non-empty UTF-8 workspace name",
                    USAGE,
                )
            })?,
    };
    let argv = command_argv(matches, USAGE)?;
    Ok(Command::LandTurn(LandTurnArgs {
        repo,
        workspace,
        outside_lock: os(matches, "outside-lock").map(PathBuf::from),
        argv,
    }))
}

const LOCK: CommandSpec = CommandSpec {
    name: "lock",
    missing: "lock requires the lock file's path before `--`",
    args: "<path>",
    trailing: "-- <cmd...>",
    summary: "run a command holding a lock file",
    about: &[
        "Takes an exclusive flock(2) on the lock file at <path>, creating it if no holder has yet and never removing it, records this process on it once the kernel grants the lock, and runs `<cmd...>` holding it. The command is argv, never a shell string, and it inherits cowshed's standard streams and environment.",
        "The command inherits the lock's descriptor, so the kernel releases the lock only once cowshed, the command and every process the command handed the descriptor to have closed it: killing cowshed alone never lets another holder in while its command runs. The command runs with NX_DAEMON_START=never, honored by cowshed's managed Nx, and with git's automatic gc, maintenance and fsmonitor off (appended to any GIT_CONFIG_COUNT entries it was given). Other descendants, including detached processes, keep the lock until they close its descriptor. An empty GIT_CONFIG_COUNT means zero; an invalid count is refused before taking or queueing for the lock. A waiter killed before the grant takes its pending request with it.",
        "SIGINT, SIGHUP and SIGTERM that cowshed receives once it holds the lock are forwarded to the command; one that arrives while it still waits ends cowshed and gives up its place. The command's exit status is cowshed's, and a signal ends it with 128 + the signal. It needs no project and asks no daemon; `--json` is refused, since its output is the command's own bytes.",
    ],
    options: &[
        Opt {
            spelling: "--state-fd <n>",
            meaning: "a caller-opened descriptor (3 or above), adopted before runtime allocation, for one JSON LockWaiterEvent per line: `{\"kind\":\"queued\",\"inode\":…}` while another holds the lock, `{\"kind\":\"acquired\",\"inode\":…}` only after the command successfully spawns holding it, or `{\"kind\":\"failed\",\"reason\":…}` if setup, locking or spawn fails without running the command. Before `acquired`, the controller relinquishes its copies of command stdin/stdout/stderr. After a successful handoff, the state channel remains open until the command ends. A handoff fault emits `commandFailed` and closes the channel before waiting, with child and lock custody retained; a later wrapper fault also emits `commandFailed`, never a claim that no command ran. The command never inherits this channel. If its reader closes, a nonzero wrapper exit still reports the failure; command stderr is not retained as a fallback. A descriptor that cannot be adopted is reported on stderr. While queued, each SIGUSR1 adds one event: `notSignalled` with its reason, and the wait goes on, or `signalled` with the pid, the last event: lock then refuses its command and exits with the conflict code",
        },
        Opt {
            spelling: "--owned-pid <pid>",
            meaning: "repeatable: a process the caller owns. Its life is captured when lock starts (a pidfd on Linux, its pid version on Darwin) and held for the whole run. While queued, SIGUSR1 sends SIGTERM to the owner of the inode lock is queued on only if the kernel proves it one of these captured lives, still running: a later process given the same pid, whatever its uid and argv, or the holder of a file renamed over the path, is never signalled",
        },
    ],
};

fn parse_lock(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &LOCK;
    reject_project(
        global,
        USAGE,
        "--project is not valid for lock: it names no project",
    )?;
    if global.json {
        return Err(UsageError::new(
            "lock writes no JSON envelope: its output is the command's own bytes; drop --json",
            USAGE,
        ));
    }
    let path = os(matches, "path")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let state_fd = caller_descriptor(matches, "state-fd", USAGE)?;
    let argv = command_argv(matches, USAGE)?;
    let owned_pids = appended_values(matches, "owned-pid", USAGE)?
        .into_iter()
        .map(|value| {
            value
                .parse::<i32>()
                .ok()
                .filter(|pid| *pid >= 1 && value.bytes().all(|byte| byte.is_ascii_digit()))
                .ok_or_else(|| {
                    UsageError::new("--owned-pid must be a process id of 1 or above", USAGE)
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Command::Lock(LockArgs {
        path,
        state_fd,
        owned_pids,
        argv,
    }))
}

const HOLD: CommandSpec = CommandSpec {
    name: "hold",
    missing: "hold requires --control-fd and --state-fd before `--`",
    args: "--control-fd <n> --state-fd <m>",
    trailing: "-- <cmd...>",
    summary: "run a command held unreaped, signalled only as its group",
    about: &[
        "Runs `<cmd...>` as the leader of a new session and process group that cowshed stays outside of, so a stop or kill of that group never reaches cowshed, and holds the leader unreaped until it is released: while it is held its pid cannot be given to another process and its group id names its group alone, so a signal sent through cowshed reaches that group or nothing. The command is argv, never a shell string; its arguments, environment, working directory, standard streams and every inherited descriptor but <n> and <m> are cowshed's own, byte for byte. Once the command runs cowshed keeps no copy of its standard streams, so they end with the command's group.",
        "--control-fd <n> and --state-fd <m> are two caller-opened descriptors, 3 or above, adopted before anything else runs and never inherited by the command. cowshed reads one request per line from <n>: `signal <SIG>` sends SIG (HUP INT QUIT ABRT KILL USR1 USR2 ALRM TERM STOP CONT TSTP TTIN TTOU WINCH) to the leader's whole group while the leader is held, and `release` reaps the leader once it has exited. The end of <n> is a release, so a caller that dies releases. Any other line fails the hold.",
        "cowshed writes one JSON HoldEvent per line to <m>: `started` with the leader's pid, its birth identity, and the kernel's names of the descriptors it was handed as stdout and stderr in the form `lsof -F di` prints (`device` 0x… for a pipe or socket on Darwin, `inode` otherwise); `exited` with its status, observed without reaping it; `signalled` or `notSignalled` with its reason, answering each signal; `failed` with its reason when no command runs, always a last line; `commandFailed` with the leader's pid and the reason once cowshed fails after the leader ran: it releases the leader as the end of <n> does, keeps answering on <m>, and ends with `exited` once the leader is reaped.",
        "Once the leader is released and reaped, cowshed exits with its status: its code, or 128 + the signal that ended it; a failure of cowshed itself uses the wrapper codes `exec` uses. SIGINT, SIGHUP and SIGTERM that cowshed receives are sent to the group as a `signal` request is, and answered on <m>. It needs no project and asks no daemon; `--json` is refused, since its output is the command's own bytes.",
    ],
    options: &[],
};

fn parse_hold(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &HOLD;
    reject_project(
        global,
        USAGE,
        "--project is not valid for hold: it names no project",
    )?;
    if global.json {
        return Err(UsageError::new(
            "hold writes no JSON envelope: its output is the command's own bytes; drop --json",
            USAGE,
        ));
    }
    let (Some(control_fd), Some(state_fd)) = (
        caller_descriptor(matches, "control-fd", USAGE)?,
        caller_descriptor(matches, "state-fd", USAGE)?,
    ) else {
        return Err(UsageError::new(USAGE.missing, USAGE));
    };
    if control_fd == state_fd {
        return Err(UsageError::new(
            "--control-fd and --state-fd must be two descriptors: requests and events never share one",
            USAGE,
        ));
    }
    Ok(Command::Hold(HoldArgs {
        control_fd,
        state_fd,
        argv: command_argv(matches, USAGE)?,
    }))
}

/// The caller-opened descriptor `--<name>` numbers: 3 or above, never a standard stream.
fn caller_descriptor(
    matches: &ArgMatches,
    name: &str,
    usage: &'static CommandSpec,
) -> Result<Option<i32>, UsageError> {
    let Some(value) = os(matches, name) else {
        return Ok(None);
    };
    value
        .to_str()
        .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|text| text.parse::<i32>().ok())
        .filter(|descriptor| *descriptor >= 3)
        .map(Some)
        .ok_or_else(|| {
            UsageError::new(
                format!(
                    "--{name} must be an open descriptor number of 3 or above, never a standard stream"
                ),
                usage,
            )
        })
}

/// The command after `--`: refused when absent, naming whether `--` or the command is missing.
fn command_argv(
    matches: &ArgMatches,
    usage: &'static CommandSpec,
) -> Result<Vec<OsString>, UsageError> {
    let argv: Vec<OsString> = matches
        .get_many::<OsString>("argv")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    if argv.is_empty() {
        return Err(UsageError::new(
            if matches.contains_id("argv") {
                format!("{} requires a command after `--`", usage.name)
            } else {
                format!("{} requires `--` before the command", usage.name)
            },
            usage,
        ));
    }
    Ok(argv)
}
const LOCK_HOLDERS: CommandSpec = CommandSpec {
    name: "lock-holders",
    missing: "lock-holders requires the lock file's path",
    args: "<path>",
    trailing: "",
    summary: "show who holds a lock file",
    about: &[
        "Prints one JSON object: the lock file's inode, whether the kernel shows it exclusively locked, the holder record on it, the owner that record names once the kernel proves it, and every other process with a descriptor on that inode with its custody. On Linux a descriptor's custody is the kernel's own: `holds` (its fdinfo shows the flock) or `open` (a waiter blocked in flock(2), or an open that never asked). Darwin exposes no lock state per descriptor: the life the record names, proven by its kernel unique id and a descriptor on the inode, is `recorded`, and every other descriptor is `unexposed`, never counted a holder.",
        "It needs no project and asks no daemon; `--json` is refused, since the object is its whole output.",
    ],
    options: &[],
};

fn parse_lock_holders(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &LOCK_HOLDERS;
    reject_project(
        global,
        USAGE,
        "--project is not valid for lock-holders: it names no project",
    )?;
    if global.json {
        return Err(UsageError::new(
            "lock-holders always writes one JSON object, never an envelope; drop --json",
            USAGE,
        ));
    }
    let path = os(matches, "path")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    Ok(Command::LockHolders(LockHoldersArgs { path }))
}

const TREE_HOLDERS: CommandSpec = CommandSpec {
    name: "tree-holders",
    missing: "tree-holders requires the directory whose holders it names",
    args: "<dir>",
    trailing: "",
    summary: "show who holds a directory tree",
    about: &[
        "Prints one JSON object per line for every process of this user holding something inside the directory tree at <dir>, as the census `rm` and `detach` refuse with reads it (02_workspaces.md \"Holders cowshed does not run\"): `{\"pid\":…,\"hold\":…,\"path\":…,\"command\":…}`, where `hold` is the first hold the census found, cheapest first: `working-directory`, `root-directory`, `executable`, `open-file` or `mapped-file`. A missing directory has no holder.",
        "It needs no project and asks no daemon; `--json` is refused, since its lines are its whole output.",
    ],
    options: &[],
};

fn parse_tree_holders(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &TREE_HOLDERS;
    reject_project(
        global,
        USAGE,
        "--project is not valid for tree-holders: it names no project",
    )?;
    if global.json {
        return Err(UsageError::new(
            "tree-holders always writes JSON lines, never an envelope; drop --json",
            USAGE,
        ));
    }
    let path = os(matches, "path")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    Ok(Command::TreeHolders(TreeHoldersArgs { path }))
}

/// A land queue is keyed by one directory name: 1 to 64 of `[A-Za-z0-9._-]`, not starting with a
/// dot, so a name can never reach the queue's parent or a hidden file.
fn valid_repo_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

const SUBTREE_ROOT: CommandSpec = CommandSpec {
    name: "subtree-root",
    missing: "subtree-root requires --handle-fd N and a command after `--`",
    args: "",
    trailing: "-- <cmd...>",
    summary: "admit as a subtree root, then exec a command",
    about: &[
        "Connects directly to COWSHED_SUPERVISOR_SOCKET as this live wrapper, with no project discovery or gateway bootstrap. Writes one JobSubtreeAdmission JSON record and newline to the inherited --handle-fd, closes it, then execs the command in this same process life. It creates no child process, process group, session, sandbox or cgroup. Command arguments after -- retain their exact native bytes, including cowshed flags.",
        "The caller transfers exclusive ownership of a writable inherited descriptor numbered at least 3. Closed, read-only and close-on-exec descriptors are refused without taking ownership; malformed arguments never close a descriptor. The descriptor must not be used concurrently by an embedding host. Admission goes only to this descriptor; stdout, stderr, stdin, the environment and the working directory pass to the command.",
        "A typed refused admission still execs the command, unleased. An absent COWSHED_SUPERVISOR_SOCKET produces the foldUnavailable refusal. Transport, admission-write and exec failures are loud nonzero errors, never an unleased fallback. The library-hosted CLI also intentionally replaces its entire host process on successful exec, without shutting down the host runtime first.",
    ],
    options: &[Opt {
        spelling: "--handle-fd <N>",
        meaning: "required before --: exclusively owned, inherited writable descriptor, N >= 3",
    }],
};

fn parse_subtree_root(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    use std::os::unix::ffi::OsStrExt as _;

    const USAGE: &CommandSpec = &SUBTREE_ROOT;
    reject_project(
        global,
        USAGE,
        "--project is not valid for subtree-root: it names no project",
    )?;
    let handle_fd = matches
        .get_one::<OsString>("handle-fd")
        .and_then(|value| value.to_str())
        .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|text| text.parse::<std::os::fd::RawFd>().ok())
        .filter(|fd| *fd >= 3)
        .ok_or_else(|| {
            UsageError::new(
                "--handle-fd must name an inherited writable descriptor numbered at least 3",
                USAGE,
            )
        })?;
    let mut argv = matches.get_many::<OsString>("argv").into_iter().flatten();
    let program = argv
        .next()
        .filter(|program| !program.is_empty())
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let args: Vec<OsString> = argv.cloned().collect();
    if program.as_bytes().contains(&0) || args.iter().any(|arg| arg.as_bytes().contains(&0)) {
        return Err(UsageError::new(
            "subtree-root command arguments cannot contain NUL bytes",
            USAGE,
        ));
    }
    Ok(Command::SubtreeRoot(SubtreeRootArgs {
        handle_fd,
        program: program.clone(),
        args,
    }))
}

const RETIRE_DESCENDANTS: CommandSpec = CommandSpec {
    name: "retire-descendants",
    missing: "retire-descendants requires --grace-ms N",
    args: "",
    trailing: "",
    summary: "retire the retained subtree named by a handle on stdin",
    about: &[
        "Reads exactly one JobSubtreeHandle JSON value from stdin through EOF, at most 65536 bytes including whitespace. Pass the handle from an admitted subtree-root response, not the surrounding admission record. It asks COWSHED_SUPERVISOR_SOCKET to retire that retained root and its descendants, not siblings or the whole job. No project discovery or gateway bootstrap runs.",
        "Writes one bare JobSubtreeRetirement JSON record and newline on stdout, including typed refusals. An absent socket environment produces foldUnavailable. A refusal exits zero because the response carries the outcome; malformed input and operational or transport failures exit nonzero. --grace-ms is required, nonnegative milliseconds; zero requests immediate retirement without a grace interval.",
    ],
    options: &[Opt {
        spelling: "--grace-ms <N>",
        meaning: "required: nonnegative integer grace interval in milliseconds",
    }],
};

fn parse_retire_descendants(
    matches: &ArgMatches,
    global: &GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &RETIRE_DESCENDANTS;
    reject_project(
        global,
        USAGE,
        "--project is not valid for retire-descendants: it names no project",
    )?;
    let millis = matches
        .get_one::<OsString>("grace-ms")
        .and_then(|value| value.to_str())
        .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| {
            UsageError::new(
                "--grace-ms must be a nonnegative integer number of milliseconds",
                USAGE,
            )
        })?;
    Ok(Command::RetireDescendants(RetireDescendantsArgs {
        grace: std::time::Duration::from_millis(millis),
    }))
}
const CONTROLLER: CommandSpec = CommandSpec {
    name: "controller",
    missing: "controller requires an argument",
    args: "",
    trailing: "",
    summary: "serve a controller to an embedding process",
    about: &[
        "Serves one coordinator controller connection for the project selected by the cwd or `--project`, on standard input, which must be a Unix socket: an embedding process makes a socketpair, starts this command with one end as its standard input, and speaks the controller protocol on the other end (`Cowshed::connect` in Rust, `coordinatorEndpoint` in N-API). Nothing is written to stdout; the command exits 0 when the embedding process closes its end.",
        "A program that links cowshed as a library is another cowshed build, and the daemon starts workspace supervisors only for its own build, so such a program runs its controller as this command of the host's `cowshed` instead of opening the project in-process. The project opens here exactly as for any other verb, and the project's gateway sessions are reconciled before each exec, shell and checked land, as `cowshed exec` and `cowshed land --check` reconcile them.",
    ],
    options: &[],
};

const GATEWAY: CommandSpec = CommandSpec {
    name: "gateway",
    missing: "gateway action is required",
    args: "<start|stop|status|run>",
    trailing: "",
    summary: "manage the host gateway",
    about: &[
        "The gateway is the one trusted process outside every sandbox: workspaces reach the network, main's repository, and each other only through its authenticated Unix socket. `start` installs and loads the per-user LaunchAgent and waits until that socket answers; `stop` boots it out; `status` reports health without starting anything. Both mutations are idempotent.",
        "`run` is the LaunchAgent's own foreground entrypoint. It validates already-mounted storage and never creates any, so a background start can report missing setup but can never raise an authorization prompt.",
        "An ordinary `stop` leaves the installed cowshed the agent ran — the content-addressed copies and the stable name linking to one of them: that is host state rather than agent state, and keeping it makes the next `start` a plist write instead of a file copy. `stop --purge` deletes them, and the `cowshed` links on your `PATH` that name them, for a host that is done with the gateway rather than pausing it. `start` installs the running build as the installed cowshed the way `setup` does, and refuses a build that does not contain the installed one.",
    ],
    options: &[Opt {
        spelling: "--purge",
        meaning: "`stop` only: also delete the installed cowshed the agent ran and the PATH links that name it, not just the plist",
    }],
};

fn parse_gateway(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &GATEWAY;
    reject_project(global, USAGE, "--project is not valid for gateway commands")?;
    let (action, child) = matches
        .subcommand()
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let command = match action {
        "start" => GatewayCommand::Start,
        "stop" => GatewayCommand::Stop {
            purge: flagged(child, "purge"),
        },
        "status" => GatewayCommand::Status,
        "run" => GatewayCommand::Run,
        other => {
            return Err(UsageError::new(
                format!("unknown gateway action `{other}`"),
                USAGE,
            ));
        }
    };
    Ok(Command::Gateway(command))
}

const CREDENTIAL: CommandSpec = CommandSpec {
    name: "credential",
    missing: "credential action is required",
    args: "<add|ls|status|rm>",
    trailing: "",
    summary: "enrol the registry credentials the gateway holds",
    about: &[
        "A workspace reaches a private registry through the gateway, which attaches a credential the host holds and strips whatever the client sent. Nothing inside a sandbox ever receives the secret, so this command is how the host comes to hold it: `add` writes one scoped record to the platform credential store, `ls`/`status` report which routes exist, the scope each was enrolled with, and whether its credential is installed, and `rm` forgets one.",
        "A record is bound to this project's repository, one exact HTTPS origin with its port, the methods an intercept grant admits, and at least one registry path prefix. The origin is host and port only — the registry's namespace path belongs in `--path-prefix`, which is what keeps the credential from being usable for the whole host.",
        "The secret is named, never typed: `--secret-env` reads a variable from this shell, `--secret-command` runs a helper whose argv comes from the flag, `--secret-stdin` reads a pipe. A non-empty named variable wins and the helper is not run. There is no flag that takes the credential itself, because argv is visible to every process on the host.",
        "Enrolment records the variable NAME it read, and every job in this project then has that name withheld from its environment: a credential the gateway holds has no reason to also travel into a sandbox. Network reach is still a separate, auditable decision — grant it per workspace with `cowshed grant <workspace> --egress <host>`.",
    ],
    options: &[
        Opt {
            spelling: "--origin <https://host[:port]>",
            meaning: "the registry origin, host and port only; the port defaults to 443",
        },
        Opt {
            spelling: "--path-prefix <path>",
            meaning: "registry namespace the credential is limited to; repeatable, at least one required",
        },
        Opt {
            spelling: "--method <GET|HEAD>",
            meaning: "methods the credential may be attached to; defaults to GET and HEAD",
        },
        Opt {
            spelling: "--secret-env <NAME>",
            meaning: "read the credential from this environment variable, and withhold that name from every child",
        },
        Opt {
            spelling: "--secret-command <json-argv>",
            meaning: "run this argv array to obtain the credential; not run when --secret-env is set and non-empty",
        },
        Opt {
            spelling: "--secret-stdin",
            meaning: "read the credential from stdin",
        },
    ],
};

/// `GET` and `HEAD` are what an intercept grant admits, so they are the default and the only
/// methods worth spelling out; anything else is refused by the record's own validation.
const DEFAULT_CREDENTIAL_METHODS: [&str; 2] = ["GET", "HEAD"];

fn parse_credential(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &CREDENTIAL;
    let (action, child) = matches
        .subcommand()
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let command = match action {
        "add" => {
            let origin = required_value(
                child,
                "origin",
                USAGE,
                "--origin <https://host> is required",
            )?;
            let path_prefixes = appended_values(child, "path-prefix", USAGE)?;
            if path_prefixes.is_empty() {
                return Err(UsageError::new(
                    "at least one --path-prefix is required: a credential without a scope would be usable for the whole host",
                    USAGE,
                ));
            }
            let mut methods = appended_values(child, "method", USAGE)?;
            if methods.is_empty() {
                methods = DEFAULT_CREDENTIAL_METHODS
                    .iter()
                    .map(|method| (*method).to_owned())
                    .collect();
            }
            let secret_env = match os(child, "secret-env") {
                Some(name) => Some(
                    name.into_string()
                        .map_err(|_| UsageError::new("--secret-env must be valid UTF-8", USAGE))?,
                ),
                None => None,
            };
            let secret_command = match os(child, "secret-command") {
                Some(text) => {
                    let text = text.into_string().map_err(|_| {
                        UsageError::new("--secret-command must be valid UTF-8", USAGE)
                    })?;
                    Some(parse_secret_command(&text, USAGE)?)
                }
                None => None,
            };
            let secret_stdin = flagged(child, "secret-stdin");
            if secret_stdin && (secret_env.is_some() || secret_command.is_some()) {
                return Err(UsageError::new(
                    "--secret-stdin cannot be combined with another credential source",
                    USAGE,
                ));
            }
            if !secret_stdin && secret_env.is_none() && secret_command.is_none() {
                return Err(UsageError::new(
                    "name a credential source: --secret-env <NAME>, --secret-command <json-argv>, or --secret-stdin",
                    USAGE,
                ));
            }
            CredentialCommand::Add(CredentialAddArgs {
                origin,
                path_prefixes,
                methods,
                secret_env,
                secret_command,
                secret_stdin,
            })
        }
        "ls" | "status" => CredentialCommand::Status,
        "rm" => CredentialCommand::Remove {
            origin: required_value(
                child,
                "origin",
                USAGE,
                "--origin <https://host> is required to name the route to forget",
            )?,
        },
        other => {
            return Err(UsageError::new(
                format!("unknown credential action `{other}`"),
                USAGE,
            ));
        }
    };
    Ok(Command::Credential(command))
}

const IDENTITY: CommandSpec = CommandSpec {
    name: "identity",
    missing: "identity action is required",
    args: "add <remote>",
    trailing: "",
    summary: "bind another checkout remote as an identity",
    about: &[
        "A dependency can name this repository by more than one URL — a forge it is pushed to as well as the host it was adopted from. `add` records the named remote of the project's main checkout as an additional, non-primary identity of this project, and from the next exec every workspace with read access to the checkout fetches that URL (and its SSH form without the login) from the local clone instead of the network.",
        "Identities are controller-owned: only this command adds one, and a workspace's own Git configuration never creates a route. A URL another adopted project already binds is refused, because a fetch URL has exactly one local clone. The remote must stay configured under the same name, as the adopted one must; a remote that is already bound reports the binding unchanged.",
    ],
    options: &[],
};

fn parse_identity(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &IDENTITY;
    let (action, child) = matches
        .subcommand()
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let command = match action {
        "add" => IdentityCommand::Add {
            remote: os(child, "remote")
                .ok_or_else(|| {
                    UsageError::new("name the remote to bind: identity add <remote>", USAGE)
                })?
                .into_string()
                .map_err(|_| UsageError::new("the remote name must be valid UTF-8", USAGE))?,
        },
        other => {
            return Err(UsageError::new(
                format!("unknown identity action `{other}`"),
                USAGE,
            ));
        }
    };
    Ok(Command::Identity(command))
}

/// The helper is an argv ARRAY, not a shell string: a command line assembled by a shell is a
/// second language between the operator and the process that reads the secret.
fn parse_secret_command(
    text: &str,
    usage: &'static CommandSpec,
) -> Result<Vec<String>, UsageError> {
    let argv: Vec<String> = serde_json::from_str(text).map_err(|error| {
        UsageError::new(
            format!("--secret-command must be a JSON array of strings: {error}"),
            usage,
        )
    })?;
    if argv.is_empty() || argv.iter().any(|word| word.is_empty()) {
        return Err(UsageError::new(
            "--secret-command needs a program and no empty arguments",
            usage,
        ));
    }
    Ok(argv)
}

const SCCACHE: CommandSpec = CommandSpec {
    name: "sccache",
    missing: "sccache action is required",
    args: "<start|stop|status>",
    trailing: "",
    summary: "manage the host sccache daemon",
    about: &[
        "Runs the shared compile cache as a supervised LaunchAgent, so its configuration is pinned before any client speaks to it. sccache reads its store path, its cache cap, and its base directories once, at server start, and never again. A first client that spawned a server implicitly would therefore freeze its own environment into the daemon every later workspace shares; starting it deliberately keeps the cap and the store where the host meant them.",
        "The gateway daemon starts this agent itself, so a healthy host already has it; these verbs are for repair, inspection, and resizing. `status` reports launchd and socket health without starting anything, and surfaces the daemon's own statistics whenever it answers. Hits are reported per language on purpose: cross-workspace C and C++ reuse needs no build slot, so a healthy aggregate hit rate routinely hides a Rust hit rate of zero.",
    ],
    options: &[Opt {
        spelling: "--capacity <size>",
        meaning: "`start` only: cache cap (100g, 1t). The default is the summed size of every adopted project's main image, floored at 40 GiB, because sccache's own 10 GiB default is smaller than one debug graph and evicts what the next slot tenant came for",
    }],
};

fn parse_sccache(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &SCCACHE;
    reject_project(global, USAGE, "--project is not valid for sccache commands")?;
    let (action, child) = matches
        .subcommand()
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let command = match action {
        "start" => SccacheCommand::Start {
            capacity: os(child, "capacity"),
        },
        "stop" => SccacheCommand::Stop,
        "status" => SccacheCommand::Status,
        other => {
            return Err(UsageError::new(
                format!("unknown sccache action `{other}`"),
                USAGE,
            ));
        }
    };
    Ok(Command::Sccache(command))
}

const SKILL: CommandSpec = CommandSpec {
    name: "skill",
    missing: "skill action is required",
    args: "install",
    trailing: "",
    summary: "install the agent skill",
    about: &[
        "Writes cowshed's agent skill into every harness already present on the host, so an agent working in a workspace knows the verbs without being told them. A repeat install that finds the shipped bytes already there reports `unchanged` and leaves mtimes alone.",
        "`--project` decides the scope: with it the skill is installed into the project's own harness directories, without it into the user's.",
    ],
    options: &[Opt {
        spelling: "--harness <name>",
        meaning: "install into this harness whether or not it is detected; repeatable",
    }],
};

fn parse_skill(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &SKILL;
    let (action, child) = matches
        .subcommand()
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    if action != "install" {
        return Err(UsageError::new(
            format!("unknown skill action `{action}`"),
            USAGE,
        ));
    }
    let mut harnesses = Vec::new();
    if let Some(values) = child.get_many::<OsString>("harness") {
        for value in values {
            let name = value
                .to_str()
                .ok_or_else(|| UsageError::new("--harness requires a UTF-8 harness name", USAGE))?;
            if !harnesses.iter().any(|existing| existing == name) {
                harnesses.push(name.to_owned());
            }
        }
    }
    let scope = if global.project.is_some() {
        crate::skill::Scope::Project
    } else {
        crate::skill::Scope::Global
    };
    for name in &harnesses {
        if crate::skill::harness_named(scope, name).is_none() {
            let suggestions = crate::skill::harness_suggestions(scope, name);
            let detail = if suggestions.is_empty() {
                format!(
                    "known harnesses include {}",
                    crate::skill::harness_names(scope)
                )
            } else {
                format!("did you mean {}?", suggestions.join(" or "))
            };
            return Err(UsageError::new(
                format!("unknown harness `{name}`; {detail}"),
                USAGE,
            ));
        }
    }
    Ok(Command::Skill(SkillArgs {
        action: SkillCommand::Install,
        harnesses,
    }))
}

fn parse_adopt(matches: &ArgMatches) -> Result<Command, UsageError> {
    Ok(Command::Adopt(AdoptArgs {
        path: os(matches, "path").map(PathBuf::from),
        capacity: os(matches, "capacity"),
        repo_id: os(matches, "repo-id"),
        quarantine: flagged(matches, "quarantine"),
        re_adopt: flagged(matches, "re-adopt"),
    }))
}

const ADOPT: CommandSpec = CommandSpec {
    name: "adopt",
    missing: "adopt requires an argument",
    args: "[path]",
    trailing: "",
    summary: "adopt a checkout",
    about: &[
        "Converts an existing checkout into this repository's image-backed main workspace, at the same path. Run it once per repository; every other verb finds its project from the cwd or `--project`. Adoption is the only operation that copies a source tree into an image, and one of only two commands that may create host storage — so the first adopt on a host may raise one administrator prompt while the cowshed volumes are created, and no ordinary command ever can.",
        "`cowshed setup` is the other, and the one every storage error points at: it repairs a host without needing a checkout to adopt. Reach for adopt when you have a repository to bring in, and for setup when the machine itself is wrong.",
        "The secret gate runs before anything changes. A refusal names every offending file and prints the exact controller-owned `waivers.json` path plus a valid entry example. Entries carry an exact repository-relative `path` and a non-empty `reason`. Only intentionally committed synthetic or public detector fixtures that can never hold live credentials may be waived — never live, temporary, copied, developer-local, deployment, or recoverable credentials. A waiver unblocks adoption while every waived finding stays retained for audit.",
        "Default adopt is idempotent. `--re-adopt` explicitly replaces an adopted main through a durable, verified copy into a fresh sparse RAW image. It retains the old backing image and the original `.pre-cowshed` tree, carries the entire checkout and Git state, and preserves the warm build volume's identity. Use it for a format transition or guest-space reclamation; run outside the checkout with no source or build-volume holders.",
    ],
    options: &[
        Opt {
            spelling: "--capacity <size>",
            meaning: "capacity of the images this project creates (100g, 1t); `cowshed resize` grows them later",
        },
        Opt {
            spelling: "--repo-id <owner/repo>",
            meaning: "identity for a repository whose remotes cannot supply one unambiguously",
        },
        Opt {
            spelling: "--quarantine",
            meaning: "move detected secret files into the project's quarantine instead of refusing the adopt",
        },
        Opt {
            spelling: "--re-adopt",
            meaning: "replace an adopted main by verified re-adoption, retaining its old backing image and full Git/build state",
        },
    ],
};

const SETUP: CommandSpec = CommandSpec {
    name: "setup",
    missing: "setup requires an argument",
    args: "",
    trailing: "",
    summary: "create or repair host storage",
    about: &[
        "Brings this host's store volume to its canonical state and pins it in `/etc/fstab`: an absent store is created; no volume is ever deleted unasked. A detached or mis-mounted one is remounted where it belongs, its marker is validated, and the fstab line that survives a reboot is written. It is idempotent — on a healthy host it changes nothing and says so — and it needs no repository, because its subject is the machine rather than a checkout.",
        "On a host that still has the retired `cowshed.caches` volume, setup moves everything it holds to its home in the host HOME — each tool's cache to the tool's own default, the gateway's mirrors to cowshed's cache directory, sccache's store to sccache's default, a repository's cache to the `[caches] home` path its main declares — merging where both sides hold a cache and naming, with its size, anything it cannot place. That migration needs no authorization. Once the volume holds nothing but its marker, setup names `cowshed setup --retire-caches-volume` as the attended step that deletes it.",
        "Everything that can require elevation happens inside one authorization session, and every volume's exact intent is printed before the dialog appears; a run with nothing to escalate raises no prompt at all. A volume that exists but is not this host's — a `cowshed.store` in another container — is reported with its device and left exactly as it is, never adopted and never re-created, because re-creating means deleting a volume. `cowshed doctor` explains a host; this repairs one.",
        "Setup installs the invoking build as the host's cowshed — the CLI and the daemon alike, as one artifact. The build is copied to a content-addressed file beside the plists' volume, `~/Library/Application Support/dev.cowshed/bin/cowshed-<sha256>`; the stable name `cowshed` there becomes a link to it; the gateway agent runs that name; and the first `cowshed` on your `PATH`, when it is a symbolic link — what `bun link` of the package or a link made by hand leaves — is pointed at it too. The `cowshed` you type and the daemon are then the same bytes, and only `cowshed setup` changes them: rebuilding a checkout changes nothing on `PATH` until a setup runs from that build. A setup run from the installed copy therefore installs nothing; install another build by running that build's own `cowshed setup` — a checkout's `packages/cowshed/bin/cowshed`, or its `dist/bin/<platform>/cowshed`. A gateway that answers while running other bytes than the installed copy is restarted onto it, and a stale installed copy is replaced through the same atomic plan `gateway start` uses, with the previous build put back if the agent cannot be activated. A service left running a binary from before this build is host drift a repair ends — setup never reports such a host as set up. Run setup from a release build — the platform `cli-*` target's `production` configuration (`nx run cowshed:cli-arm64-macos:production` on Apple silicon) or `cargo build --release -p cowshed-cli`; `nx run cowshed:build` builds a debug one, and a debug build is refused as the supervised binary.",
        "Release builds record the commit they were made from, and the installed cowshed keeps the record of its build. One install runs at a time: setup holds the install slot, `dev.cowshed/install.lock`, for its whole run, and a second setup or `gateway start` waits for it and then judges its own build against whatever was installed meanwhile. Setup refuses to replace the installed cowshed with a build that does not contain it: the build's commit must be on an adopted project's `main`, and that main must show the build holding every installed commit, by ancestry or patch identity. The refusal names the binary, both builds and each commit the install would remove; a build that records nothing is refused too. Setup repairs everything else and exits non-zero. `--downgrade` installs the invoking build without judging it.",
        "After successful storage repair, setup refreshes every adopted main's build state through the same path jobs use. It announces rebuild-only migration before discarding contributed incremental directories, protects tracked source files, and links an empty build volume for the next build to repopulate. No build state is copied. One refusing project does not hide later projects: each is reported on stderr, then setup exits with the first typed failure and a count. Uninstall never migrates build state.",
        "The host session workspace mount root is fixed, and setup has no option or setting to change it. On macOS it is `/private/var/folders/cowshed/<uid>/mnt` (also spelled `/var/folders/cowshed/<uid>/mnt`), using the invoking numeric uid, not HOME or a random temporary directory; on Linux it is `~/.cowshed/mnt`. Setup creates or repairs the stable macOS prefix root:wheel 0755 and its user directories uid:gid 0700 through the same single authorization, refusing symlinks, non-directories and foreign ownership. Session workspaces mount at `<root>/<owner>/<repo>/<ws>`, and build volumes also derive from that root; mains stay mounted directly at their checkout paths.",
        "To reach workspaces through a preferred path, make a symbolic link at that path pointing to the root, for example `ln -s /private/var/folders/cowshed/$(id -u)/mnt ~/workspaces` (on Linux, point it at `~/.cowshed/mnt`). The link runs from the preferred path to the root, never the other way round: the root stays a real directory, because the kernel-resolved physical mount path is what Spotlight's indexer admits. Cowshed's identity probe and workspace jobs use canonical mount paths; identity rules anchored only at an alias are not reliable.",
        "A `host.json` that still records a custom `mountRoot` is refused when read, naming that file, the recorded root and the required root. The refusal does not ignore the value, rewrite the file, move a mount or attach at the new path, and there is no automatic migration: with the previously installed build, stop workspace jobs and daemons, detach every session and build volume at the old root, resolve busy holders (Spotlight included), and audit path-bearing detached metadata and sandbox grants before reattaching. Only then remove `mountRoot` from `host.json`, keeping credential routes and mode 0600, and run this build's ordinary `cowshed setup`. A recorded root equal to the fixed root is harmless and is omitted the next time the file is saved.",
        "`--uninstall` is the same transaction backwards, and deliberately narrower: it removes the machine presence — the cowshed-tagged `/etc/fstab` pins, the gateway and sccache LaunchAgents, and the installed binaries they ran — and touches no volume, no image, and no workspace. Nothing it removes holds data; everything it leaves does. It refuses while the volumes still hold workspaces, or while their occupancy cannot be established, until `--force` says the caller means it anyway.",
        "`--sccache` builds the patched sccache from the flake this package ships at `nix/sccache`, registers a nix GC root for the result under cowshed's support directory, and starts a LaunchAgent running that exact store path. It is opt-in: not every cowshed user writes Rust, and it needs nix. A host without nix is told so and the rest of the run still repairs storage. The plist names the resolved `/nix/store` path rather than the GC root symlink, so a rebuild is a plist change — and therefore an explicit restart — instead of a different binary appearing under a loaded agent.",
        "`--retire-caches-volume` deletes the retired caches volume, removes its `/etc/fstab` pin and drops it from the storage mount service, inside the same single authorization. It refuses while the volume holds anything but its marker.",
    ],
    options: &[
        Opt {
            spelling: "--uninstall",
            meaning: "remove cowshed's host presence — fstab pins, service agents, installed binaries — leaving every volume and workspace untouched",
        },
        Opt {
            spelling: "--force",
            meaning: "`--uninstall` only: proceed although workspaces remain, or although occupancy could not be established",
        },
        Opt {
            spelling: "--sccache",
            meaning: "build the patched sccache from the shipped nix flake, root it, and run it as a LaunchAgent (requires nix)",
        },
        Opt {
            spelling: "--retire-caches-volume",
            meaning: "delete the retired caches volume once setup has moved everything it held (one administrator authorization)",
        },
        Opt {
            spelling: "--downgrade",
            meaning: "install the invoking build without judging whether it contains the installed cowshed",
        },
    ],
};

/// `setup` takes no project: a host whose volumes are missing has no adopted checkout to select,
/// so silently accepting `--project` would promise a scope the verb does not have. `--force`
/// without `--uninstall` is refused rather than ignored — it confirms a refusal that the forward
/// direction never makes, so accepting it would answer a question nobody asked. `--sccache`
/// cannot combine with teardown: teardown releases the GC root an install registers, so asking
/// for both in one command line names two opposite intents.
fn parse_setup(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &SETUP;
    reject_project(global, USAGE, "--project is not valid for setup")?;
    let uninstall = flagged(matches, "uninstall");
    let force = flagged(matches, "force");
    let sccache = flagged(matches, "sccache");
    let retire_caches_volume = flagged(matches, "retire-caches-volume");
    let downgrade = flagged(matches, "downgrade");
    if sccache && (uninstall || force) {
        return Err(UsageError::new(
            "--sccache installs sccache; --uninstall removes it",
            USAGE,
        ));
    }
    if retire_caches_volume && (uninstall || force) {
        return Err(UsageError::new(
            "--retire-caches-volume repairs a host; --uninstall tears it down",
            USAGE,
        ));
    }
    if downgrade && (uninstall || force) {
        return Err(UsageError::new(
            "--downgrade installs a build; --uninstall installs none",
            USAGE,
        ));
    }
    if force && !uninstall {
        return Err(UsageError::new(
            "--force only confirms --uninstall; setup never refuses to repair a host",
            USAGE,
        ));
    }
    Ok(Command::Setup(SetupArgs {
        uninstall,
        force,
        sccache,
        retire_caches_volume,
        downgrade,
    }))
}

const FORK: CommandSpec = CommandSpec {
    name: "fork",
    missing: "fork requires a destination workspace",
    args: "[<src>] <ws>",
    trailing: "",
    summary: "create a warm workspace",
    about: &[
        "Clones main by default, or any named source, carrying tracked edits, untracked files, dependencies and build state warm. Every fork mints its own cowshed/<ws> branch and fresh closed grants.",
        "--clean resets tracked edits and removes nonignored untracked files. --ref implies clean and selects a commit held by the source. Ignored build state stays warm.",
    ],
    options: &[
        Opt {
            spelling: "--clean",
            meaning: "discard source edits and nonignored untracked files in the clone",
        },
        Opt {
            spelling: "--ref <rev>",
            meaning: "start at the source's revision; implies --clean",
        },
        Opt {
            spelling: "--browse",
            meaning: "show the volume in Finder",
        },
        Opt {
            spelling: "--slot <n>",
            meaning: "mount at the stable build-slot path",
        },
        Opt {
            spelling: "--register",
            meaning: "also register the workspace as a remote in main",
        },
        Opt {
            spelling: "--git-worktree",
            meaning: "share main's object store through a clone-owned linked-worktree registration",
        },
        Opt {
            spelling: "--markdown-only",
            meaning: "pure-Markdown edit disjoint from code: refuse to mint a shed and edit in place",
        },
    ],
};

fn parse_fork(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &FORK;
    if flagged(matches, "markdown-only") {
        return Err(UsageError::new(
            "pure-Markdown edit disjoint from code edits in place — no shed minted (drop --markdown-only to mint a workspace)",
            USAGE,
        ));
    }
    let mut workspaces = matches
        .get_many::<OsString>("workspaces")
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let (source, destination) = match (workspaces.next(), workspaces.next()) {
        (Some(destination), None) => (None, destination),
        (Some(source), Some(destination)) => {
            let source = source
                .to_str()
                .ok_or_else(|| UsageError::new("source must be UTF-8", USAGE))?;
            WorkspaceName::new(source)
                .map_err(|error| UsageError::new(error.to_string(), USAGE))?;
            (Some(source.to_owned()), destination)
        }
        _ => return Err(UsageError::new(USAGE.missing, USAGE)),
    };
    let destination = destination
        .to_str()
        .ok_or_else(|| UsageError::new("destination must be UTF-8", USAGE))?;
    WorkspaceName::session(destination)
        .map_err(|error| UsageError::new(error.to_string(), USAGE))?;
    let reference = os(matches, "ref");
    let clean = flagged(matches, "clean") || reference.is_some();
    Ok(Command::Fork(ForkArgs {
        source,
        destination: destination.to_owned(),
        reference,
        clean,
        browse: flagged(matches, "browse"),
        slot: parse_slot_value(matches, USAGE)?,
        register: flagged(matches, "register"),
        git_worktree: flagged(matches, "git-worktree"),
    }))
}

const MOVE: CommandSpec = CommandSpec {
    name: "mv",
    missing: "mv requires a workspace",
    args: "<ws> <new-name> | main <path>",
    trailing: "",
    summary: "rename a workspace, move/re-identify main",
    about: &[
        "Three jobs, and the source decides which. `mv <ws> <new-name>` renames a session workspace (the new name follows the ordinary name grammar and cannot be `main`). `mv main <path>` moves the adopted checkout to an absolute path and keeps every record of where it lives in step. `mv main --repo-id <owner/repo>` changes the adopted repository identity, without consulting or changing Git remotes.",
        "Re-identifying main — and moving a main that mounts directly at its checkout — detaches its volume to rename the identity-scoped store paths, so it refuses while anything holds the mount. Run it from outside the checkout with editors and daemons off the volume; main must be attached, and session workspaces must be detached first (the refusal lists the exact detach commands). Renaming a session workspace touches only that workspace's volume and works fine from inside main.",
        "The old identity is kept as a former one, so markers, certificates and artifact stamps minted under it stay valid. Nothing before the volume comes down is destructive: a refused or interrupted attempt leaves the project as it was.",
    ],
    options: &[Opt {
        spelling: "--repo-id <owner/repo>",
        meaning: "`main` only: replace the adopted repository identity in every cowshed record, keeping the old one as a former identity; detaches and remounts main's volume, so the mount must be quiesced",
    }],
};

fn parse_move(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &MOVE;
    let source = require_workspace(matches, "src", false, USAGE, USAGE.missing)?;
    let repo_id = os(matches, "repo-id");
    let destination = os(matches, "dst");
    let destination = match (source.as_str(), repo_id, destination) {
        ("main", Some(repo_id), None) => {
            let value = repo_id
                .to_str()
                .ok_or_else(|| UsageError::new("--repo-id requires a UTF-8 owner/repo", USAGE))?;
            MoveDestination::RepoId(RepoId::parse(value).map_err(|error| {
                UsageError::new(format!("invalid --repo-id `{value}`: {error}"), USAGE)
            })?)
        }
        ("main", Some(_), Some(_)) => {
            return Err(UsageError::new(
                "`main --repo-id` cannot also name a checkout destination",
                USAGE,
            ));
        }
        (_, Some(_), _) => {
            return Err(UsageError::new(
                "--repo-id is only valid when the source workspace is `main`",
                USAGE,
            ));
        }
        ("main", None, Some(destination)) => {
            MoveDestination::Checkout(checkout_destination(&destination, USAGE)?)
        }
        (_, None, Some(destination)) => {
            MoveDestination::Workspace(workspace_name(&destination, true, USAGE)?)
        }
        (_, None, None) => {
            return Err(UsageError::new("mv requires a destination", USAGE));
        }
    };
    Ok(Command::Move(MoveArgs {
        source,
        destination,
    }))
}

/// A checkout destination is a path, and the only thing the parser can decide about it is that it
/// is spelt absolutely. Whether it exists, is occupied, or overlaps cowshed storage is the
/// coordinator's to refuse, with the project state in hand to say why.
fn checkout_destination(value: &OsStr, usage: &'static CommandSpec) -> Result<PathBuf, UsageError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(UsageError::new(
            "the checkout destination must be an absolute path",
            usage,
        ));
    }
    Ok(path)
}

const CHECKPOINT: CommandSpec = CommandSpec {
    name: "checkpoint",
    missing: "checkpoint requires an argument",
    args: "[<ws>] [label]",
    trailing: "",
    summary: "create a checkpoint",
    about: &[
        "Snapshots a workspace by clonefiling its image under a label (a UTC timestamp when you give none). The copy is taken after a supervisor barrier seals complete job output, so it is crash-consistent rather than merely recent. Omit the workspace to checkpoint the one you are standing in.",
    ],
    options: &[Opt {
        spelling: "--keep",
        meaning: "pin the checkpoint so expiry pruning never deletes it; an explicit label pins it too",
    }],
};

fn parse_checkpoint(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &CHECKPOINT;
    Ok(Command::Checkpoint(CheckpointArgs {
        workspace: optional_workspace(matches, "workspace", false, USAGE)?,
        label: os(matches, "label"),
        keep: flagged(matches, "keep"),
    }))
}

const RESTORE: CommandSpec = CommandSpec {
    name: "restore",
    missing: "restore requires a workspace",
    args: "<ws> <label>",
    trailing: "",
    summary: "restore a checkpoint",
    about: &[
        "Swaps the workspace's image for the checkpoint and mints a new workspace incarnation. The displaced image is kept as a `pre-restore-<timestamp>` checkpoint, so a restore is itself undoable; a restore over unsaved work is refused.",
    ],
    options: &[],
};

fn parse_restore(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &RESTORE;
    Ok(Command::Restore(RestoreArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
        label: os(matches, "label")
            .ok_or_else(|| UsageError::new("restore requires a label", USAGE))?,
    }))
}

const LIST: CommandSpec = CommandSpec {
    name: "ls",
    missing: "ls requires an argument",
    args: "",
    trailing: "",
    summary: "list workspaces",
    about: &[
        "One line per workspace of the project selected by the cwd or `--project`: name, state, branch, and mountpoint (empty when detached).",
        "`--landing` and `--landed` answer a different question: is this workspace's work already in main? Both compare each workspace's HEAD — not its recorded branch, which is a label that can drift — against main's branch tip as it stands right now in main's own repository, and then every other local branch the same way, because `rm` destroys them all. Landed is content: a head main contains is landed, and otherwise every file it changed since its merge base with main must already carry that change in main, so work that reached main by squash-merge, cherry-pick, a history rewrite or a pick whose conflict was resolved counts even though no commit of main is its own. The unlanded column counts the commits that change a file main lacks the change of; `rm` names those files. A branch other than HEAD also counts as landed when main's repository preserves it under its own name or as the ref `cowshed push --branch` writes. A workspace whose target cannot be resolved reports `indeterminate` with the reason on stderr and is never treated as landed.",
        "Neither flag is on by default because the measurement costs git processes per workspace, and `ls` is used interactively and in scripts.",
    ],
    options: &[
        Opt {
            spelling: "--all",
            meaning: "every adopted project on the host, with its repository id as the first column",
        },
        Opt {
            spelling: "--landing",
            meaning: "add a header row and five columns — unlanded, landed, behind, dirty, unlanded-branches — measured against main; `?` where the measurement could not be made. unlanded-branches counts the local branches besides HEAD that `rm` would refuse for",
        },
        Opt {
            spelling: "--landed",
            meaning: "list only workspaces with nothing unlanded, as bare names for `cowshed ls --landed | xargs -n1 cowshed rm`; main is never listed, and an indeterminate workspace is never listed; add --landing for the table form instead of names",
        },
    ],
};

fn parse_list(matches: &ArgMatches) -> Result<Command, UsageError> {
    Ok(Command::List(ListArgs {
        all: flagged(matches, "all"),
        landing: flagged(matches, "landing"),
        landed: flagged(matches, "landed"),
    }))
}

const PATH: CommandSpec = CommandSpec {
    name: "path",
    missing: "path requires an argument",
    args: "[<ws>]",
    trailing: "",
    summary: "print a workspace mount",
    about: &[
        "The mountpoint, bare on stdout, for `cd $(cowshed path raven)`. A detached workspace is attached first, so the path printed is always live. Naming no workspace answers for the one you are standing in.",
    ],
    options: &[
        Opt {
            spelling: "--slot <n>",
            meaning: "answer for build slot <n>'s current tenant instead of a named workspace, without the caller knowing which workspace holds it; the two are exclusive",
        },
        Opt {
            spelling: "--no-attach",
            meaning: "skip the remount and print the would-be path of a detached workspace",
        },
    ],
};

fn parse_path(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &PATH;
    let workspace = optional_workspace(matches, "workspace", false, USAGE)?;
    let slot = parse_slot_value(matches, USAGE)?;
    if workspace.is_some() && slot.is_some() {
        return Err(UsageError::new(
            "path takes a workspace or --slot, not both",
            USAGE,
        ));
    }
    Ok(Command::Path(PathArgs {
        workspace,
        slot,
        no_attach: flagged(matches, "no-attach"),
    }))
}

const EXEC: CommandSpec = CommandSpec {
    name: "exec",
    missing: "exec requires a workspace",
    args: "<ws>",
    trailing: "-- <cmd...>",
    summary: "run an argv command",
    about: &[
        "Runs one argv — never a shell string — inside the workspace's sandbox, with the cwd at the workspace root. Child stdout and stderr pass through as opaque bytes and the child's exit code passes through untouched; only a denial cowshed has authoritative evidence for is reported as one.",
        "Waits for the command however long it runs and exits with its status. With `--timeout`, a command still running then keeps running under the workspace supervisor and exec exits 103 naming its job; `--background` prints the job id and returns at once. No CLI verb reattaches to a job: the cowshed API does. `--session` keeps a named session's cwd and variables across calls.",
        "Every job but a resident takes host CPU tokens from the gateway before its command starts. An ordinary command is `background` and asks for the background share of the host, the half of its tokens background work may hold, so the runners it starts run side by side; `--want <n>` names the tokens a command can use at once instead, and its grant is between one and `n`. Background work never holds more than half the host. `--class interactive` names a command someone is waiting on: it takes its tokens as background work does and runs at nice 0, ahead of a gate at +5 and background at +10 whenever cores are contended, and a workspace may ask for it. `--class gate` admits the command as the operator's gate: it starts at once on the half background leaves free while no other gate work holds it, and otherwise waits for whole returned tokens ahead of queued background work, however long that takes; it runs at nice +5. Only this trusted route may ask for a gate; a workspace's own capability is refused. A foreground job lives as long as this exec does: if exec ends first, however it ends, the job is killed and its tokens return, so a gate whose lander is gone never keeps its place ahead of the next and a caller's timeout leaves no orphan; `--timeout` lets it run on, and `--background` never holds it. `--host` runs the command once at the host boundary, outside the workspace sandbox, at the class it names: host work at the default class takes a background token like any other, and only `--class gate` takes the repository's gate order.",
        "`--class resident` admits a process that stays up to serve requests and is idle between them — a daemon, a worker. It holds no token and starts at once: its descendants ask its pool for their own tokens, each as background work for the workspace, so it costs the host tokens only while a request of its runs. A resident wants none, so it takes no `--want`. It runs at nice +10; `--class interactive-resident` is the same resident at nice 0, for a worker whose requests someone waits on, and differs in nothing else.",
    ],
    options: &[
        Opt {
            spelling: "--stdin",
            meaning: "forward this process's stdin to the child",
        },
        Opt {
            spelling: "--stdin-file <rel>",
            meaning: "read the child's stdin from a workspace-relative file",
        },
        Opt {
            spelling: "--stdin-base64 <data>",
            meaning: "decode the child's stdin from inline base64, instead of interpolating input into command text",
        },
        Opt {
            spelling: "--ro",
            meaning: "run against a read-only view of the workspace",
        },
        Opt {
            spelling: "--cwd <rel>",
            meaning: "run in a workspace-relative directory instead of the workspace root",
        },
        Opt {
            spelling: "--session <name>",
            meaning: "run in a persistent named shell session whose cwd, variables, and jobs survive across calls",
        },
        Opt {
            spelling: "--timeout <dur>",
            meaning: "stop waiting after this long; a command still running keeps running and exec exits 103 naming its job",
        },
        Opt {
            spelling: "--background",
            meaning: "print the job id and return without waiting",
        },
        Opt {
            spelling: "--stdout-copy <rel>",
            meaning: "also publish stdout to this workspace-relative file",
        },
        Opt {
            spelling: "--stderr-copy <rel>",
            meaning: "also publish stderr to this workspace-relative file",
        },
        Opt {
            spelling: "--replace-output",
            meaning: "overwrite an existing publication target; without it a copy refuses to clobber",
        },
        Opt {
            spelling: "--want <n>",
            meaning: "the CPU tokens the command can use at once (default: the background share, half the host's tokens; one for a gate); its grant is between one and n",
        },
        Opt {
            spelling: "--class <background|interactive|gate|resident|interactive-resident>",
            meaning: "admit the command as background work (default), as work someone waits on, as the operator's gate, or as a resident that holds no token, at background or interactive nice",
        },
        Opt {
            spelling: "--host",
            meaning: "run the argv once at the host boundary, outside the workspace sandbox, with the operator's environment and no session, at its own --class (background by default)",
        },
    ],
};

fn parse_exec(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &EXEC;
    let mut stdin = None;
    if flagged(matches, "stdin") {
        set_stdin(&mut stdin, StdinSource::Stream, USAGE)?;
    }
    if let Some(value) = os(matches, "stdin-file") {
        set_stdin(
            &mut stdin,
            StdinSource::WorkspaceFile(PathBuf::from(value)),
            USAGE,
        )?;
    }
    if let Some(value) = os(matches, "stdin-base64") {
        set_stdin(&mut stdin, StdinSource::InlineBase64(value), USAGE)?;
    }
    let stdout_copy = os(matches, "stdout-copy").map(PathBuf::from);
    let stderr_copy = os(matches, "stderr-copy").map(PathBuf::from);
    let replace_output = flagged(matches, "replace-output");
    if replace_output && stdout_copy.is_none() && stderr_copy.is_none() {
        return Err(UsageError::new(
            "--replace-output requires --stdout-copy or --stderr-copy",
            USAGE,
        ));
    }
    let argv: Vec<OsString> = matches
        .get_many::<OsString>("argv")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    if argv.is_empty() {
        return Err(UsageError::new(
            if matches.contains_id("argv") {
                "exec requires a child command after `--`"
            } else {
                "exec requires `--` before the child argv"
            },
            USAGE,
        ));
    }
    let session = os(matches, "session")
        .map(|value| workspace_name(&value, false, USAGE))
        .transpose()?;
    let class = match os(matches, "class") {
        None => ExecClass::Background,
        Some(value) => match value.to_str() {
            Some("background") => ExecClass::Background,
            Some("interactive") => ExecClass::Interactive,
            Some("gate") => ExecClass::Gate,
            Some("resident") => ExecClass::Resident,
            Some("interactive-resident") => ExecClass::InteractiveResident,
            _ => {
                return Err(UsageError::new(
                    format!(
                        "--class must be background, interactive, gate, resident or interactive-resident, got `{}`",
                        value.to_string_lossy()
                    ),
                    USAGE,
                ));
            }
        },
    };
    let want = positive_count(matches, "want", USAGE)?
        .map(|count| {
            usize::try_from(count)
                .ok()
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| UsageError::new("--want is too large for this platform", USAGE))
        })
        .transpose()?;
    let host = flagged(matches, "host");
    if host && session.is_some() {
        return Err(UsageError::new(
            "--host runs outside any named session",
            USAGE,
        ));
    }
    if class.is_resident() && want.is_some() {
        return Err(UsageError::new(
            "--class resident takes no --want: a resident holds no token, and its descendants ask its pool for their own",
            USAGE,
        ));
    }
    if class == ExecClass::Gate && session.is_some() {
        return Err(UsageError::new(
            "--class gate runs outside any named session",
            USAGE,
        ));
    }
    let scope = if host {
        ExecScope::HostGate
    } else {
        ExecScope::Workspace
    };
    Ok(Command::Exec(ExecArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
        argv,
        stdin,
        read_only: flagged(matches, "ro"),
        cwd: os(matches, "cwd").map(PathBuf::from),
        session,
        timeout: os(matches, "timeout"),
        background: flagged(matches, "background"),
        stdout_copy,
        stderr_copy,
        replace_output,
        class,
        want,
        scope,
    }))
}

/// `--<flag> n`: a positive whole number, refused as usage when it is zero or not a number.
fn positive_count(
    matches: &ArgMatches,
    flag: &str,
    usage: &'static CommandSpec,
) -> Result<Option<u64>, UsageError> {
    let Some(value) = os(matches, flag) else {
        return Ok(None);
    };
    value
        .to_str()
        .and_then(|text| text.parse::<u64>().ok())
        .filter(|count| *count > 0)
        .map(Some)
        .ok_or_else(|| {
            UsageError::new(
                format!(
                    "--{flag} must be a positive whole number, got `{}`",
                    value.to_string_lossy()
                ),
                usage,
            )
        })
}

const GRANT: CommandSpec = CommandSpec {
    name: "grant",
    missing: "grant requires a workspace",
    args: "<ws>",
    trailing: "",
    summary: "grant paths and network access",
    about: &[
        "Adds read-only or writable host paths to one workspace's sandbox grant snapshot. Paths are normalized, deduplicated, sorted, and recorded outside the workspace image; they apply from the next exec or shell. With no flags, prints the current filesystem grants.",
        "A grant cannot cover the workspace mount, another cowshed mount, controller state, project policy roots, or credential-bearing paths.",
        "A path is recorded under its resolved spelling. A symlink planted beside the workspace so `../<name>` resolves (for example `<shed>/<org>/<project>/<name>` pointing at a sibling repository) is readable through the link exactly when its target is granted; grant the target, not the link.",
        "`--project-wide` changes the project's standing policy instead: read paths, egress hosts and workspace-relative write and read denies every workspace — main and forked workspaces — runs under. A workspace grant cannot remove a project deny. A write allow stays per workspace. With no other flags, prints the project's standing policy.",
        "`--deny` hides workspace-relative paths from every job: reads and writes beneath them fail with EPERM. The same denies can be declared durably in main's `.cowshed.toml` as `[sandbox] deny = [\"<relative-path>\", …]`; cowshed reads that section only from main's checkout, never from a workspace's copy, and adds it to every workspace's grants from its next supervisor launch.",
        "An egress host is intercepted by default: the gateway terminates its TLS under the workspace CA, audits each request, and can attach a credential the host holds. `--opaque` grants the hosts named in the same invocation as opaque tunnels instead, for a client that verifies the real certificate — a pinned client, or Go on macOS, whose platform verifier never trusts the workspace CA. A host holds one rule, so granting it again restates its mode.",
        "On macOS a workspace's port block starts at 64 ports: the gateway listener at `$COWSHED_PORT_BASE` and 63 service ports above it, and a sandboxed process can connect only to its own blocks. `--ports <N>` grows the block to hold at least N service ports, not counting the gateway port — `cowshed grant raven --ports 80` yields a 128-port block. A block never shrinks, and a count it already holds changes nothing. The block grows into a larger block that contains it when one is free, and otherwise moves to a free larger block; a block it moved from stays reserved to the workspace, and connectable by its jobs, until the workspace is removed. Growth needs an idle workspace: while any of its jobs is running the grant is refused at once rather than queued, so grant ports before launching services. The next exec gets the new `COWSHED_PORT_BASE` and `COWSHED_PORT_BLOCK_SIZE` and the matching sandbox profile. The ports come from the host's finite reserved range, so a block that cannot be placed is refused. Linux has no port block — each workspace's private loopback already holds every port — and refuses `--ports`.",
    ],
    options: &[
        Opt {
            spelling: "--project-wide",
            meaning: "change or print the project's standing policy; takes --read, --deny-write, --deny, --egress and --opaque",
        },
        Opt {
            spelling: "--read <path...>",
            meaning: "allow reads beneath one or more absolute host paths; repeat the flag to add more",
        },
        Opt {
            spelling: "--write <path...>",
            meaning: "allow reads and writes beneath one or more absolute host paths; repeat the flag to add more",
        },
        Opt {
            spelling: "--deny-write <relative-path...>",
            meaning: "deny writes at workspace-relative paths and beneath directories; project-wide policy applies to main and all workspaces",
        },
        Opt {
            spelling: "--deny <relative-path...>",
            meaning: "deny reads and writes at workspace-relative paths and beneath directories; project-wide policy applies to main and all workspaces",
        },
        Opt {
            spelling: "--egress <host>",
            meaning: "allow a bare DNS name, IP address or single-label *. wildcard; no :port or URL; repeat to add more",
        },
        Opt {
            spelling: "--opaque",
            meaning: "grant this invocation's --egress hosts as opaque tunnels instead of intercepted ones",
        },
        Opt {
            spelling: "--ports <N>",
            meaning: "macOS, one workspace only: grow the port block to hold at least N service ports, a positive integer that excludes the gateway port; never shrinks it",
        },
    ],
};

fn parse_grant(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &GRANT;
    let target = if flagged(matches, "project-wide") {
        if os(matches, "workspace").is_some() {
            return Err(UsageError::new(
                "grant names one workspace or the whole project, not both",
                USAGE,
            ));
        }
        if matches.get_many::<PathBuf>("write").is_some() {
            return Err(UsageError::new(
                "a project-wide grant carries reads and egress; a write grant is a per-workspace decision",
                USAGE,
            ));
        }
        if os(matches, "ports").is_some() {
            return Err(UsageError::new(
                "a project-wide grant carries reads and egress; --ports sizes one workspace's port block",
                USAGE,
            ));
        }
        GrantTarget::Project
    } else {
        GrantTarget::Workspace(require_workspace(
            matches,
            "workspace",
            false,
            USAGE,
            USAGE.missing,
        )?)
    };
    let mut egress = appended_values(matches, "egress", USAGE)?;
    for host in &mut egress {
        let canonical = cowshed_gateway::HostPattern::parse(host).map_err(|error| {
            UsageError::new(
                format!(
                    "invalid egress host `{host}`: {error}; use a bare host without a port or URL scheme"
                ),
                USAGE,
            )
        })?;
        *host = canonical.to_string();
    }
    let egress_mode = if flagged(matches, "opaque") {
        if egress.is_empty() {
            return Err(UsageError::new(
                "--opaque sets the mode of the hosts --egress grants; name at least one --egress <host>",
                USAGE,
            ));
        }
        EgressMode::Opaque
    } else {
        EgressMode::Intercept
    };
    Ok(Command::Grant(GrantArgs {
        target,
        read: matches
            .get_many::<PathBuf>("read")
            .map(|paths| paths.cloned().collect())
            .unwrap_or_default(),
        write: matches
            .get_many::<PathBuf>("write")
            .map(|paths| paths.cloned().collect())
            .unwrap_or_default(),
        deny_write: matches
            .get_many::<PathBuf>("deny-write")
            .map(|paths| paths.cloned().collect())
            .unwrap_or_default(),
        deny: matches
            .get_many::<PathBuf>("deny")
            .map(|paths| paths.cloned().collect())
            .unwrap_or_default(),
        egress,
        egress_mode,
        service_ports: parse_service_ports(matches, USAGE)?,
    }))
}

/// `--ports <N>`: a positive count of service ports, the gateway port not included. A zero or a
/// malformed count is refused, never read as "no change": a request the operator typed must
/// either take effect or say why it cannot.
fn parse_service_ports(
    matches: &ArgMatches,
    usage: &'static CommandSpec,
) -> Result<Option<NonZeroU16>, UsageError> {
    let Some(value) = os(matches, "ports") else {
        return Ok(None);
    };
    let count = value
        .to_str()
        .and_then(|text| text.parse::<u16>().ok())
        .ok_or_else(|| {
            UsageError::new(
                format!(
                    "--ports must be a whole number of service ports from 1 to {}, got `{}`",
                    u16::MAX,
                    value.to_string_lossy()
                ),
                usage,
            )
        })?;
    NonZeroU16::new(count).map(Some).ok_or_else(|| {
        UsageError::new(
            "--ports 0 requests no service ports; a port block only grows, so name at least 1",
            usage,
        )
    })
}

const REVOKE: CommandSpec = CommandSpec {
    name: "revoke",
    missing: "revoke requires a workspace",
    args: "<ws>",
    trailing: "",
    summary: "remove paths and network access",
    about: &[
        "Removes the selected workspace's own grants; --project-wide selects standing project grants instead. Requires selectors or --all, never both. Absent selectors are no-ops. Port ownership never shrinks, and project checkpoint quotas are unchanged.",
        "--egress removes every rule for the host regardless of mode or ports. Invalid recorded keys are accepted verbatim for repair: cowshed revoke raven --egress api.example.com:443, then grant the bare api.example.com if reach is still wanted. No gateway reconciliation precedes recovery. Workspace revoke cannot remove project policy.",
    ],
    options: &[
        Opt {
            spelling: "--project-wide",
            meaning: "remove standing project grants instead; no workspace or --write",
        },
        Opt {
            spelling: "--read <path...>",
            meaning: "remove read grants; repeat for more",
        },
        Opt {
            spelling: "--write <path...>",
            meaning: "remove workspace write grants; repeat for more",
        },
        Opt {
            spelling: "--deny-write <relative-path...>",
            meaning: "remove own workspace-relative write denies",
        },
        Opt {
            spelling: "--deny <relative-path...>",
            meaning: "remove own workspace-relative read and write denies",
        },
        Opt {
            spelling: "--egress <host>",
            meaning: "remove a host or exact invalid recorded key; repeat for more",
        },
        Opt {
            spelling: "--all",
            meaning: "remove all own grant vectors under the observed revision's CAS; no selectors",
        },
    ],
};

fn parse_revoke(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REVOKE;
    let target = if flagged(matches, "project-wide") {
        if os(matches, "workspace").is_some() || matches.get_many::<PathBuf>("write").is_some() {
            return Err(UsageError::new(
                "project-wide revoke takes no workspace or write allow",
                USAGE,
            ));
        }
        GrantTarget::Project
    } else {
        GrantTarget::Workspace(require_workspace(
            matches,
            "workspace",
            false,
            USAGE,
            USAGE.missing,
        )?)
    };
    let paths = |name| {
        matches
            .get_many::<PathBuf>(name)
            .map(|paths| paths.cloned().collect())
            .unwrap_or_default()
    };
    let delta = GrantDelta {
        read: paths("read"),
        write: paths("write"),
        deny_write: paths("deny-write"),
        deny: paths("deny"),
        egress: appended_values(matches, "egress", USAGE)?
            .into_iter()
            .map(|host| EgressRule {
                host,
                ports: Vec::new(),
                mode: EgressMode::Intercept,
            })
            .collect(),
        ..GrantDelta::default()
    };
    let has_items = !delta.read.is_empty()
        || !delta.write.is_empty()
        || !delta.deny_write.is_empty()
        || !delta.deny.is_empty()
        || !delta.egress.is_empty();
    let selection = match (flagged(matches, "all"), has_items) {
        (true, false) => RevokeSelection::All,
        (false, true) => RevokeSelection::Items(delta),
        _ => {
            return Err(UsageError::new(
                "revoke requires selectors or --all, never both",
                USAGE,
            ));
        }
    };
    Ok(Command::Revoke(RevokeArgs { target, selection }))
}

const REMOVE: CommandSpec = CommandSpec {
    name: "rm",
    missing: "rm requires a workspace",
    args: "<ws>",
    trailing: "",
    summary: "remove a workspace",
    about: &[
        "Retires one workspace, deleting the image its commits live in. The gate is containment, not preservation: `rm` refuses unless every change the workspace carries — on HEAD and on every other local branch, since the image takes them all — is already in the project's main branch. A branch other than HEAD also passes when main's repository preserves it: its tip equals or is an ancestor of main's branch of that name or of the ref `cowshed push --branch` writes. A refusal names each branch that did not pass, and each file whose change main lacks with why: missing, conflict, added, deleted, changed (main deleted it), mode, or unmergeable. The workspace is marked deleted immediately; detach and image deletion finish in the background.",
        "Build volumes are collected before rm returns: a volume with no checkout link and no seed owner is detached and deleted unless a running job's hold or an unfinished create/fork in the lifecycle intent journal owns it. Release tries an unforced unmount with bounded grace, then forces it if needed, naming each initial holder's pid and command and whether the release needed force. Collection says why a volume is retained; an editor or indexer alone cannot strand an unlinked volume.",
        "Containment is content, not commit identity. A head main contains is landed outright; otherwise every file the workspace changed since its merge base with main must already carry that change in main, beside whatever main changed since. Work that reached main by squash-merge, cherry-pick, a history rewrite, or a pick whose conflict was resolved on the way in is therefore landed, so retiring it needs no flag — passing a commit-destroying flag to remove work main already has is exactly the habit this gate exists to prevent. The target tip is read live from main's own repository, never from the workspace's `main` remote, which is a clone-time snapshot; when it cannot be read the answer is `cannot determine`, which is refused exactly as unlanded work is.",
        "The two overrides authorize different losses and neither substitutes for the other, so a script carrying one has not acquired the other.",
        "A process cowshed does not run that holds the workspace — a host shell's `direnv exec <ws> nx …`, a working directory in it, a file of it open — refuses the removal before anything stops, `--force` or not, named by pid, command and what it holds where. Cowshed stops none of them.",
        "The workspace is found in cwd's project or the one `--project` names. Run from outside any project, `rm <ws>` finds it the way `detach <ws>` does — in the store, by name — and refuses when more than one project holds that name; the gate is the same either way.",
    ],
    options: &[
        Opt {
            spelling: "--force",
            meaning: "waive transient state only — a dirty tree, an in-progress merge, the workspace's own running jobs — and the ten-minute floor on a workspace's age; the deletion log records which of them it waived and which process asked. It does not waive the containment gate or a process cowshed does not run holding the mount, and it does not preserve what it waives: an uncommitted tree is destroyed with no bundle and no record, so commit it or `cowshed checkpoint` first if it is the only copy",
        },
        Opt {
            spelling: "--restore",
            meaning: "main only: put the pre-adoption checkout back and unbind the project, the reverse of adopt. Refused while main's head is preserved neither by the retained checkout nor by a remote ref, unless --abandon is given",
        },
        Opt {
            spelling: "--abandon",
            meaning: "the sole authorization for destroying work main does not hold, and needed only for that: a workspace whose changes main holds passes without it. Before deleting, HEAD and every local branch that did not pass are bundled into sessions/.trash/<ws>-<tip>.bundle, then kept in main's repository as refs/cowshed/<ws>/abandoned/<head>/HEAD and .../heads/<branch>, and the abandonment reported branch by branch with each kept ref, so the commits stay recoverable from main itself after gc --abandoned empties the trash — the uncommitted tree is not bundled and does not survive. The project's deletion log records the head, every branch tip and the ref that keeps each. With --restore on main, it authorizes restoring past commits nothing else preserves: they are bundled into .git/cowshed/abandoned-main-<tip>.bundle of the restored checkout first",
        },
        Opt {
            spelling: "--purge",
            meaning: "main --restore only: remove the whole project in one call — every session workspace (listed or never published, under the --force and --abandon given), the collection of their images, and with --abandon the abandon bundles in sessions/.trash once main's repository keeps their tips — and then restore main, which first carries every refs/cowshed ref (abandoned tips, pushed branches) into the retained checkout it puts back, where they outlive the project. A store another process changed under the collection is refused as retryable (`retry.reason` is `gcPlanStale` in --json); run the same command again",
        },
    ],
};

fn parse_remove(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REMOVE;
    let workspace = require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?;
    let restore = flagged(matches, "restore");
    let abandon = flagged(matches, "abandon");
    if restore && workspace != "main" {
        return Err(UsageError::new(
            "--restore is only valid for the main workspace",
            USAGE,
        ));
    }
    if abandon && workspace == "main" && !restore {
        return Err(UsageError::new(
            "--abandon on main needs --restore: it keeps main's unpreserved commits in a bundle \
             inside the restored checkout",
            USAGE,
        ));
    }
    let purge = flagged(matches, "purge");
    if purge && !restore {
        return Err(UsageError::new(
            "--purge removes the whole project and needs main --restore",
            USAGE,
        ));
    }
    Ok(Command::Remove(RemoveArgs {
        workspace,
        force: flagged(matches, "force"),
        restore,
        abandon,
        purge,
    }))
}

const FORGET: CommandSpec = CommandSpec {
    name: "forget",
    missing: "forget requires an owner/repo",
    args: "<owner/repo> --abandon",
    trailing: "",
    summary: "retire an abandoned project, keeping its tree",
    about: &[
        "Retires one project's registration from the host's own store records, for a project the ordinary verbs can no longer open: its checkout is gone, or its marker, lease or repository record is corrupt. It needs no checkout, no cwd and no `--project` — the project is named only by `<owner/repo>`, so the answer is the same from any directory — and it neither opens nor remounts the project.",
        "`--abandon` is required, and it is the operator's own authorization. cowshed never infers it from a missing checkout, a missing marker or lease, a dead pid, a path prefix or an age, and nothing retires a project automatically. The authorization is recorded durably before anything else happens, outside the project tree, in `.retirements/<owner>/<repo>.json` in the store; from then on nothing opens, heals or re-adopts that project.",
        "Then, in recorded phases — fenced, drained, detached, archived — it asks every running supervisor of the project to drain and requires each one's positive receipt, detaches each of the project's backing images without force, and moves the whole project tree into `.retired-projects/<operation-id>` in the store. That tree holds the Git repositories inside the images, the grants and the resource evidence, and it is preserved as it was: nothing is deleted or rewritten. stdout is the archive path; `--json` reports `repoId`, `archive` and `images`, the number of images detached.",
        "A supervisor that has not finished draining, or a holder that keeps an image busy — an open file, a working directory, a running job — refuses that step with exit 4 and nothing is forced: the project stays fenced, so stop or wait for what holds it and run the same command again. A run that was refused or interrupted resumes from its recorded phase, and an archive that already exists is left intact: it is reserved host storage that no verb discovers, heals or deletes.",
        "The gateway daemon finishes a recorded retirement at its next start, before it heals any project, so one interrupted by a crash or a reboot completes without being typed again; it never begins one.",
    ],
    options: &[],
};

/// `forget` names no checkout, so `--project` would promise a scope the verb does not have, and
/// the identity is validated here rather than discovered: a malformed `<owner/repo>` is refused
/// before any store record is read. The explicit `--abandon` is checked last so the refusal's
/// hint can be the completed command line, built from an identity already known to be valid.
fn parse_forget(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &FORGET;
    reject_project(
        global,
        USAGE,
        "--project is not valid for forget: it retires the project named by <owner/repo>, which needs no checkout",
    )?;
    let named = os(matches, "repo").ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let text = named
        .to_str()
        .ok_or_else(|| UsageError::new("forget requires a UTF-8 owner/repo", USAGE))?;
    let repo_id = RepoId::parse(text)
        .map_err(|error| UsageError::new(format!("invalid owner/repo `{text}`: {error}"), USAGE))?;
    if !flagged(matches, "abandon") {
        return Err(UsageError::with_hint(
            format!(
                "forget retires {repo_id} only with an explicit --abandon; a missing checkout, marker, pid or path prefix never authorizes it"
            ),
            format!("cowshed forget {repo_id} --abandon"),
        ));
    }
    Ok(Command::Forget(ForgetArgs { repo_id }))
}

const ATTACH: CommandSpec = CommandSpec {
    name: "attach",
    missing: "attach requires an argument",
    args: "[ws]",
    trailing: "",
    summary: "attach session workspace(s)",
    about: &[
        "Mounts detached session workspaces. `attach <ws>` mounts one; with no argument, standing in a project attaches every detached session of that project; `--all` attaches every detached session store-wide. Mains are never attach targets, because a main is always mounted at its checkout.",
    ],
    options: &[
        Opt {
            spelling: "--all",
            meaning: "every detached session workspace store-wide",
        },
        Opt {
            spelling: "--browse",
            meaning: "show the volume in Finder; the default mount is nobrowse",
        },
    ],
};

fn parse_attach(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &ATTACH;
    let workspace = optional_workspace(matches, "workspace", false, USAGE)?;
    let all = flagged(matches, "all");
    if all && workspace.is_some() {
        return Err(UsageError::new(
            "attach --all cannot name a workspace",
            USAGE,
        ));
    }
    if workspace.as_deref() == Some("main") {
        return Err(UsageError::new(
            "mains are always mounted; attach targets session workspaces",
            USAGE,
        ));
    }
    Ok(Command::Attach(AttachArgs {
        workspace,
        all,
        browse: flagged(matches, "browse"),
    }))
}

const DETACH: CommandSpec = CommandSpec {
    name: "detach",
    missing: "detach requires a workspace",
    args: "[ws]",
    trailing: "",
    summary: "detach session workspace(s)",
    about: &[
        "Unmounts session workspaces. `detach <ws>` names one, resolved from the store directly so it works from any directory; with no argument, the project from the cwd or `--project` scopes it; `--all` detaches every attached session store-wide. Mains are never detach targets.",
        "Before anything stops, a process cowshed does not run that holds the workspace — a host shell's `direnv exec <ws> nx …`, a working directory in it, a file of it open — refuses the detach, named by pid, command and what it holds where. Cowshed stops none of them.",
    ],
    options: &[Opt {
        spelling: "--all",
        meaning: "every attached session workspace store-wide",
    }],
};

fn parse_detach(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &DETACH;
    let workspace = optional_workspace(matches, "workspace", false, USAGE)?;
    let all = flagged(matches, "all");
    if all && workspace.is_some() {
        return Err(UsageError::new(
            "detach --all cannot name a workspace",
            USAGE,
        ));
    }
    if workspace.as_deref() == Some("main") {
        return Err(UsageError::new(
            "mains are always mounted; detach targets session workspaces",
            USAGE,
        ));
    }
    if !all && workspace.is_none() {
        return Err(UsageError::new(USAGE.missing, USAGE));
    }
    Ok(Command::Detach(DetachArgs { workspace, all }))
}

const MOUNT: CommandSpec = CommandSpec {
    name: "mount",
    missing: "mount requires a target",
    args: "main",
    trailing: "",
    summary: "mount main from store records",
    about: &[
        "Mounts main for the project named by `--repo-id`, resolving the project from store records — the repository binding and the checkout-path record — rather than from a live git checkout, so an empty stub directory left by a broken workspace still mounts. The mount uses the gateway-canonical flags; a volume mounted with other flags is remounted rather than refused.",
    ],
    options: &[Opt {
        spelling: "--repo-id <owner/repo>",
        meaning: "the adopted repository whose main to mount",
    }],
};

fn parse_mount(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &MOUNT;
    let target = os(matches, "target");
    match target.as_ref().and_then(|value| value.to_str()) {
        None => {
            return Err(UsageError::new(USAGE.missing, USAGE));
        }
        Some("main") => {}
        Some(other) => {
            return Err(UsageError::new(
                format!("unknown mount target `{other}`: only `main` is supported"),
                USAGE,
            ));
        }
    }
    let repo_id = os(matches, "repo-id")
        .ok_or_else(|| UsageError::new("mount main requires --repo-id <owner/repo>", USAGE))?;
    let value = repo_id
        .to_str()
        .ok_or_else(|| UsageError::new("--repo-id requires a UTF-8 owner/repo", USAGE))?;
    let repo_id = RepoId::parse(value)
        .map_err(|error| UsageError::new(format!("invalid --repo-id `{value}`: {error}"), USAGE))?;
    Ok(Command::Mount(MountArgs {
        target: MountTarget::Main,
        repo_id,
    }))
}

const RESIZE: CommandSpec = CommandSpec {
    name: "resize",
    missing: "resize requires a workspace",
    args: "<ws|main> <size>",
    trailing: "",
    summary: "grow a workspace image or build volume",
    about: &[
        "Grows one workspace's image. Sizes are binary units — 100g, 200g, 1t — at least a mebibyte and a whole number of the 4 KiB blocks the image tools resize in. The supervisor is stopped for the resize and restarted after, because the image has to leave the kernel.",
        "With `--build <size>` instead of a positional size, grows the workspace's build volume and its seed, so every later fork of the workspace inherits the capacity. The workspace's jobs and Nx daemon stop first; any other holder of the volume refuses the resize before anything changes. The workspace must be attached, because its build link names the volume.",
    ],
    options: &[Opt {
        spelling: "--build <size>",
        meaning: "grow the workspace's build volume and seed to this size instead of its image",
    }],
};

fn parse_resize(matches: &ArgMatches) -> Result<Command, UsageError> {
    use cowshed_core::api::dto::ResizeVolume;
    const USAGE: &CommandSpec = &RESIZE;
    let workspace = require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?;
    let (capacity, volume) = match (os(matches, "size"), os(matches, "build")) {
        (Some(size), None) => (size, ResizeVolume::Workspace),
        (None, Some(size)) => (size, ResizeVolume::Build),
        (None, None) => return Err(UsageError::new("resize requires a size", USAGE)),
        (Some(_), Some(_)) => {
            return Err(UsageError::new(
                "resize takes one size: <size> for the image or --build <size> for the build volume",
                USAGE,
            ));
        }
    };
    Ok(Command::Resize(ResizeArgs {
        workspace,
        capacity,
        volume,
    }))
}

const DEFRAG: CommandSpec = CommandSpec {
    name: "defrag",
    missing: "defrag requires a workspace",
    args: "<ws|main>",
    trailing: "",
    summary: "rewrite an image contiguously",
    about: &[
        "Rewrites one workspace's image so its data is contiguous again. A clone shares its source's extent map and the first write to it copies that map, at about 12 µs per extent, so a main that has fragmented into millions of extents makes every `fork` wait tens of seconds; `cowshed doctor` reports main's extent count and the cost it predicts.",
        "The workspace is detached the way `resize` detaches it — a volume with a file open or a job running refuses before the image is touched — then its data is copied with plain reads and writes into a sibling, flushed, renamed over the image, verified by attaching it, and mounted again if it was mounted. The copy needs as much free space as the image has allocated, and keeps it while older clones and checkpoints still share the old blocks.",
    ],
    options: &[],
};

fn parse_defrag(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &DEFRAG;
    Ok(Command::Defrag(DefragArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
    }))
}

const CACHE: CommandSpec = CommandSpec {
    name: "cache",
    missing: "cache action is required",
    args: "clear [<ws>]",
    trailing: "",
    summary: "clear a workspace's Nx state and the project's Nx store",
    about: &[
        "`clear` deletes both Nx caches cowshed keeps and says how many entries each held: the workspace's own Nx state on its build volume — every entry in `.nx/cache` and `.nx/workspace-data`, the cached outputs together with the task database that indexes them — and the project's accepted-task store, which the gateway serves to every workspace of the project. With no `<ws>` it clears the workspace the command runs inside.",
        "`nx reset` does not do this: in a cowshed checkout `.nx/cache` and `.nx/workspace-data` are links into the build volume, and `nx reset` removes the links and leaves every entry on the volume, where the next refresh links them back.",
        "Nothing running is stopped except the checkout's own Nx daemon and the project's Nx cache backend, and each starts again on its next request. The workspace is refused while a job or land holds its build volume, another lifecycle operation holds the workspace, or a process other than that daemon has its Nx task database open; the store is refused while any of the project's Nx requests is in flight or queued, or a land is publishing to it. Each refusal names what is running and clears nothing of that cache.",
        "The workspace is cleared first, then the store. When the store refuses after that, the refusal says the workspace was cleared; run the command again once the named work has ended. Prints one line per cache with its entry count; --json returns CacheClearReport.",
    ],
    options: &[],
};

fn parse_cache(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &CACHE;
    let (action, child) = matches
        .subcommand()
        .ok_or_else(|| UsageError::new(USAGE.missing, USAGE))?;
    let command = match action {
        "clear" => CacheCommand::Clear {
            workspace: optional_workspace(child, "workspace", false, USAGE)?,
        },
        other => {
            return Err(UsageError::new(
                format!("unknown cache action `{other}`"),
                USAGE,
            ));
        }
    };
    Ok(Command::Cache(command))
}

const RESEED: CommandSpec = CommandSpec {
    name: "reseed",
    missing: "reseed requires a workspace",
    args: "<ws|main>",
    trailing: "",
    summary: "replace live build state from an existing seed",
    about: &[
        "Discards the workspace's live build-volume contents and replaces them with a new clone of the target's latest existing seed in its current incarnation (default main). The source tree, HEAD, index, dirty and untracked files stay untouched; only the excluded .cowshed/build link changes. Missing incremental work is rebuilt cold next time.",
        "Never refreshes, writes or deletes any seed, including the workspace's own seed. A stale seed is used as-is; an absent seed refuses. Running jobs, land holds, filesystem holders and unfinished create/fork/adopt work refuse before publication; holders are named by PID and command, never killed or forcibly unmounted.",
        "Prints reclaimed allocated image bytes (0 for --dry-run); --json returns ReseedResult. Preview reports the old allocation, seed-based replacement estimate and signed estimated reduction on stderr, without changing links, mounts, images or seeds. APFS shared extents mean allocation accounting is not a promise of physical free space.",
    ],
    options: &[
        Opt {
            spelling: "--from <target>",
            meaning: "clone this target's latest existing seed instead of main's; never refresh it",
        },
        Opt {
            spelling: "--dry-run",
            meaning: "report volume identities and allocated-byte estimates without changing build state",
        },
    ],
};

fn parse_reseed(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &RESEED;
    Ok(Command::Reseed(ReseedArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
        from: optional_workspace(matches, "from", false, USAGE)?
            .unwrap_or_else(|| "main".to_owned()),
        dry_run: flagged(matches, "dry-run"),
    }))
}

const REKEY: CommandSpec = CommandSpec {
    name: "rekey",
    missing: "rekey requires a workspace",
    args: "<ws|main>",
    trailing: "",
    summary: "rekey a keyless workspace",
    about: &[
        "Rebuilds the CA identity of a workspace whose companion is missing or invalid: the quarantined grants sidecar is republished beside the still-in-place image (revision bumped by one), fresh credentials are minted into the live mount, and the quarantine entry is consumed. The revision is preserved when the sidecar never left. Rotation invalidates in-flight job certificates.",
    ],
    options: &[],
};

fn parse_rekey(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REKEY;
    Ok(Command::Rekey(RekeyArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
    }))
}

const GC: CommandSpec = CommandSpec {
    name: "gc",
    missing: "gc requires an argument",
    args: "",
    trailing: "",
    summary: "free storage",
    about: &[
        "Reclaims seven kinds of garbage in the project selected by the cwd or `--project`, and reports `examined`, `reclaimed`, `retainedPinned`, `retainedActive` and `freedBytes`. Six of them need no flag; the seventh, `abandonedBundle`, is only ever collected under `--abandoned`. `rm`, `land`, and `restore` run it opportunistically, so most of the time it finds nothing left to do. Every category below is named in the `reason` field of each candidate, and `--dry-run` prints the candidates without touching them — run that first if you want to know what a run would cost you.",
        "`retiredWorkspace` — the image of a workspace already retired by `rm` or `land --retire`, sitting in `sessions/.trash/` with its sidecars, checkpoints and empty mountpoint. Retirement already ran the containment gate, so these commits are in main or were deliberately abandoned — and an abandonment kept them in main's repository under `refs/cowshed/<ws>/abandoned/<head>/`. Safe. The `<ws>-<tip>.bundle` files `rm --abandon` writes into the same trash directory are *not* this category and are never reclaimed without `--abandoned`: they are the recovery files of the commits that removal gave up, so they stay fetchable and their disk use accumulates until you reclaim them with `--abandoned` or delete them yourself.",
        "`orphanStagingImage` and `orphanStagingMetadata` — a staged image, with or without its grants sidecar, or a sidecar with no image, under the project's `.staging/` in the store. Each is a lifecycle transaction — create, fork, restore — that died between writing the image and publishing it. The one sign a transaction is still running is its workspace lifecycle lock: an entry whose lock is held is retained and counted in `retainedActive`; an entry whose lock is free was never published under a workspace name, so nothing uses it. Safe.",
        "`orphanStagingMount` — a mountpoint under the mount root's `.staging/` whose image is gone or was never published: an empty directory, or a volume an interrupted operation left attached. Attached volumes are detached before the directory goes, so an orphan that held tens of gigabytes stops charging for them. Safe; the same lock rule retains a mountpoint an operation still owns.",
        "`expiredCheckpoint` — an automatic checkpoint that is neither one of the five most recent for its workspace nor younger than fourteen days. This is the category that can delete something you still want: an automatic checkpoint is a real crash-consistent copy of that workspace, and past those two thresholds it goes. Pinned checkpoints are never candidates, and `cowshed checkpoint --keep` or any explicitly labelled checkpoint is pinned; `retainedPinned` in the report counts what was spared for that reason.",
        "`orphanMountpoint` — a directory under the project's mount root that is no workspace's mountpoint: nothing is mounted at it and no image or retirement names it. Hidden files and files the project ignores are what late writers left after a volume went away, and go with the directory; any other file stops the removal and is named, so you move or delete it yourself.",
        "`abandonedBundle` — under `--abandoned` only, the `<ws>-<tip>.bundle` files in `sessions/.trash/` that `rm --abandon` verified and left behind, holding commits main does not contain, which is why no plain `gc` ever deletes one. A plain `gc` still says how many the trash keeps and their allocated bytes, as `doctor` and `ls --all` do, so they are never out of sight. Before deleting any, `--abandoned` fetches each bundle's tips into main's repository as `refs/cowshed/<ws>/abandoned/<head>/HEAD` and `.../heads/<branch>` — the refs `rm --abandon` already wrote, written again for a bundle an older cowshed left without them — and records them in the project's deletion log; a bundle it cannot keep that way stops the whole sweep, naming it, and nothing is deleted. `--dry-run --abandoned` lists them with their real allocated bytes first.",
        "A bare mountpoint whose only entry is root's `0700` `.fseventsd` is retained aside by an atomic, same-parent `.root-held-<uuid>` rename instead of deleting privileged debris. A mounted filesystem, symlink substitution, rename collision or another entry (hidden included) is never overwritten or moved. The retained log is named with a quoted privileged cleanup hint in `deferred`; it does not fail collection or make doctor unhealthy. Dry-run and doctor report it as information without renaming.",
        "Also collects build volumes no checkout links and superseded seeds. A running job's shared hold or an unfinished create/fork in the lifecycle intent journal defers release; otherwise a refused unforced unmount gets bounded grace, then is forced if needed, naming each initial holder's pid and command. Both gc and opportunistic collection in rm/land say why they skip volumes, and one failed release does not hide later candidates. Dry-run reports these reasons without changing any mount or image.",
        "Every gc also retires the refs `rm --abandon` keeps in main's repository, `refs/cowshed/<ws>/abandoned/<head>/`, once main holds every tip by ancestry or by content — the measurement `rm` uses, so a cherry-pick, squash or conflict-resolved pick counts — and journals each in the project's deletion log as `retire-abandoned-ref`. Every other abandonment is kept and named with how many commits main does not hold, here, in `doctor` and in `ls --all`. Its owner lets one go with `--supersede <ws>/<head> --reason <one line>`, and the reason is journaled; nothing else ever deletes those refs.",
    ],
    options: &[
        Opt {
            spelling: "--dry-run",
            meaning: "report every candidate with its path, byte count, and reason, and delete nothing",
        },
        Opt {
            spelling: "--abandoned",
            meaning: "also reclaim the `sessions/.trash/<ws>-<tip>.bundle` files `rm --abandon` kept, after keeping each one's tips in main's repository under `refs/cowshed/<ws>/abandoned/<head>/`; they are reclaimed only when you ask",
        },
        Opt {
            spelling: "--supersede <ws>/<head>",
            meaning: "retire the abandoned refs `refs/cowshed/<ws>/abandoned/<head>/` though main does not hold them, recording why in the project's deletion log; requires --reason",
        },
        Opt {
            spelling: "--reason <one line>",
            meaning: "why the work --supersede names is no longer needed: one line, not blank",
        },
    ],
};

fn parse_gc(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &GC;
    let supersede = match (os(matches, "supersede"), os(matches, "reason")) {
        (None, None) => None,
        (Some(_), None) => {
            return Err(UsageError::new(
                "--supersede needs --reason: say in one line why the work is no longer needed",
                USAGE,
            ));
        }
        (None, Some(_)) => {
            return Err(UsageError::new(
                "--reason only explains a --supersede",
                USAGE,
            ));
        }
        (Some(namespace), Some(reason)) => {
            let namespace = namespace
                .to_str()
                .ok_or_else(|| UsageError::new("--supersede names <ws>/<head> in UTF-8", USAGE))?;
            let (workspace, head) = namespace.split_once('/').ok_or_else(|| {
                UsageError::new(
                    format!("--supersede names <ws>/<head>, got `{namespace}`"),
                    USAGE,
                )
            })?;
            let reason = reason
                .into_string()
                .map_err(|_| UsageError::new("--reason must be UTF-8", USAGE))
                .and_then(|reason| {
                    SupersededReason::new(reason)
                        .map_err(|error| UsageError::new(error.to_string(), USAGE))
                })?;
            Some(AbandonedRefSupersession {
                workspace: WorkspaceName::new(workspace)
                    .map_err(|error| UsageError::new(error.to_string(), USAGE))?,
                head: GitOid::new(head.to_owned())
                    .map_err(|error| UsageError::new(error.to_string(), USAGE))?,
                reason,
            })
        }
    };
    Ok(Command::Gc(GcArgs {
        dry_run: flagged(matches, "dry-run"),
        abandoned: flagged(matches, "abandoned"),
        supersede,
    }))
}

const PUSH: CommandSpec = CommandSpec {
    name: "push",
    missing: "push requires an argument",
    args: "[<ws>]",
    trailing: "",
    summary: "preserve a workspace ref",
    about: &[
        "Preserves a workspace branch in main's repository as `refs/cowshed/<ws>/heads/<branch>`, without touching main's checked-out branch, index, or working tree. Under the hood it is a host-side fetch from the workspace mount, so nothing inside the sandbox — hooks, `.git/config`, its remotes — ever runs outside it. Naming no workspace pushes the one you are standing in.",
        "The `--expected-*` preconditions are for a coordinator driving several workspaces at once: each is checked before anything moves, so a stale plan is refused rather than applied to a workspace that changed underneath it.",
    ],
    options: &[
        Opt {
            spelling: "--branch <name>",
            meaning: "workspace branch to preserve instead of `cowshed/<ws>`",
        },
        EXPECTED_WORKSPACE_INCARNATION,
        EXPECTED_SOURCE_HEAD,
        Opt {
            spelling: "--expected-destination-head <oid|missing>",
            meaning: "refuse unless the preservation ref is still this commit, or `missing` for one that must not exist yet",
        },
    ],
};

fn parse_push(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &PUSH;
    Ok(Command::Push(PushArgs {
        workspace: optional_workspace(matches, "workspace", false, USAGE)?,
        branch: os(matches, "branch"),
        expected_workspace_incarnation: os(matches, "expected-workspace-incarnation"),
        expected_source_head: os(matches, "expected-source-head"),
        expected_destination_head: os(matches, "expected-destination-head"),
    }))
}

const REBASE: CommandSpec = CommandSpec {
    name: "rebase",
    missing: "rebase requires an argument",
    args: "[<ws>]",
    trailing: "",
    summary: "rebase a workspace",
    about: &[
        "Brings the workspace branch up to current main: fetches main's checkout into the workspace and rebases onto it. A dirty tree is refused before anything moves; commit or discard the uncommitted work first. A conflict aborts cleanly and names the conflicted paths, leaving the workspace as it was. Naming no workspace rebases the one you are standing in.",
        "With `--into <lane>`, the destination is what the unit lands into instead: the branch the lane base <lane> has checked out, fetched from its mount, so a unit forked from a lane base catches up with what its lane-mates have landed there.",
        "Then stock Nx computes task hashes at the rebased tree, and the workspace's build volume takes only missing target cache entries with those hashes, not the target's entire historical cache. A failed hash computation carries nothing and names the root and reason. Row, byte and free-space bounds stop a carry before it fills the volume, reporting what was kept and why it stopped; the rebase still stands. The new head prints on stdout and what was carried on stderr. The carry stops each side's idle Nx daemon, which restarts on its next client; a side whose task database another process holds carries nothing and says which process.",
    ],
    options: &[
        Opt {
            spelling: "--into <ws>",
            meaning: "rebase onto the branch this workspace has checked out — the lane base the unit lands into; conflicts with --onto",
        },
        Opt {
            spelling: "--onto <rev>",
            meaning: "rebase onto this revision instead of main; conflicts with --into",
        },
        EXPECTED_WORKSPACE_INCARNATION,
        EXPECTED_SOURCE_HEAD,
        Opt {
            spelling: "--expected-onto-head <oid>",
            meaning: "refuse unless the revision being rebased onto is still this commit",
        },
        Opt {
            spelling: "--gate",
            meaning: "run the carry's jobs as gate-class work, ahead of background: a land's rebase, which its lander runs under the land lock",
        },
    ],
};

fn parse_rebase(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REBASE;
    let into = optional_workspace(matches, "into", false, USAGE)?;
    let onto = os(matches, "onto");
    if into.is_some() && onto.is_some() {
        return Err(UsageError::new("--into conflicts with --onto", USAGE));
    }
    Ok(Command::Rebase(RebaseArgs {
        workspace: optional_workspace(matches, "workspace", false, USAGE)?,
        into,
        onto,
        expected_workspace_incarnation: os(matches, "expected-workspace-incarnation"),
        expected_source_head: os(matches, "expected-source-head"),
        expected_onto_head: os(matches, "expected-onto-head"),
        gate: flagged(matches, "gate"),
    }))
}

const LAND: CommandSpec = CommandSpec {
    name: "land",
    missing: "land requires a workspace",
    args: "<ws>",
    trailing: "",
    summary: "land a workspace",
    about: &[
        "The whole close-out as one primitive: run the checks inside the sandbox, fast-forward main's checkout to the workspace's branch, retire the workspace. The target must be the branch main's checkout has checked out, and a dirty tree is refused before the checks run, since they see the working tree but only the commit lands. Land does not rebase: when main has moved past the workspace's base, the fast-forward is refused and `cowshed rebase <ws>` is the next step. Any step that fails before the fast-forward leaves the workspace intact; a retire refused after it says what landed, and the next step is `cowshed rm`.",
        "Before running any --check command, land synchronizes the gateway with current workspace grants. If synchronization fails, no check runs and the target branch is unchanged. Landing without --check does not require this preflight.",
        "Landing is also what `rm` measures against: the ancestry gate a removal enforces is satisfied by the branch this command delivers to.",
        "An adopted build volume is renamed `[cowshed] <project> — build <target>` in the background. The target's previous volume is released before land returns, including with --no-retire, unless a running job's hold owns it. Release uses an unforced unmount with bounded grace, then force if needed, naming initial holders and every collection deferral. Seeds are never mounted or named: mounting one just to rename it would advance the image mtime used for seed freshness.",
        "With `--into <lane>`, the unit lands into the lane base <lane> instead of main: the lane base's checked-out branch is fast-forwarded, and the unit retires once the lane base holds its commits. The lane itself reaches main with `cowshed rebase <lane>` and `cowshed land <lane>`.",
    ],
    options: &[
        Opt {
            spelling: "--into <ws>",
            meaning: "land into this workspace — the lane base the unit was forked from — instead of main",
        },
        Opt {
            spelling: "--target <branch>",
            meaning: "land onto this branch instead of the one the target has checked out; the target's checkout must have it checked out",
        },
        Opt {
            spelling: "--check <cmd>",
            meaning: "validation command run inside the sandbox after synchronizing gateway grants; repeatable",
        },
        Opt {
            spelling: "--no-retire",
            meaning: "keep the workspace after a successful landing",
        },
        Opt {
            spelling: "--push-only",
            meaning: "stop after validation and delivery, for review-gated flows",
        },
        EXPECTED_WORKSPACE_INCARNATION,
        EXPECTED_SOURCE_HEAD,
        Opt {
            spelling: "--expected-target-head <oid|missing>",
            meaning: "refuse unless the target branch is still this commit, or `missing` for one that must not exist yet",
        },
        Opt {
            spelling: "--ref-lock <file>",
            meaning: "the lock file every other writer of the target branch holds while it moves it (lockf(1)); land holds it only from re-reading the target head through the fast-forward, and says who holds it while it waits",
        },
    ],
};

fn parse_land(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &LAND;
    let checks = matches
        .get_many::<OsString>("check")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    Ok(Command::Land(LandArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
        into: optional_workspace(matches, "into", false, USAGE)?,
        target: os(matches, "target"),
        checks,
        retire: !flagged(matches, "no-retire"),
        push_only: flagged(matches, "push-only"),
        expected_workspace_incarnation: os(matches, "expected-workspace-incarnation"),
        expected_source_head: os(matches, "expected-source-head"),
        expected_target_head: os(matches, "expected-target-head"),
        ref_lock: os(matches, "ref-lock"),
    }))
}

const BUILD_STATE: CommandSpec = CommandSpec {
    name: "build-state",
    missing: "build-state takes no arguments",
    args: "",
    trailing: "",
    summary: "read build-volume paths for lint",
    about: &[
        "Reads this checkout's .cowshed/build volume-state record without opening its controller or store and without discovering tools. With --json, prints {paths:[{checkout,volume}]} for the consuming repository's Nx output lint.",
        "A checkout with no build volume or no recorded state refuses with environment-missing (exit 5): run `cowshed setup` on the host or any `cowshed exec` in the checkout to migrate.",
    ],
    options: &[],
};

const DOCTOR: CommandSpec = CommandSpec {
    name: "doctor",
    missing: "doctor requires an argument",
    args: "",
    trailing: "",
    summary: "check invariants",
    about: &[
        "Checks the invariants a healthy host holds: every image has a marker, every mount matches an image, grants files parse, the caches volume and the gateway answer, autosave is fresh, and nothing reclaimable is stranded — retired trash, and staging entries (images, sidecars, mountpoints, attached volumes) that no running operation owns are each a finding with their byte count. Exit 0 when healthy, otherwise 5.",
        "With `--repair`, first validates every mounted workspace artifact frame. Duplicate or regressed sequences are refused because resequencing would change store identity and make the rewritten log attest to itself; create a fresh store instead. Repair then runs the same sweep `gc` runs, so stranded trash and orphaned staging are retired under gc's rules and reported.",
    ],
    options: &[
        Opt {
            spelling: "--repair",
            meaning: "validate artifact ordering, refuse identity-changing sequence rewrites, then reclaim what gc would",
        },
        Opt {
            spelling: "--clear-attach-budget",
            meaning: "end this boot's recorded disk-image attach-pool exhaustion and forget its recorded ceiling, once what spent it is freed",
        },
    ],
};

fn parse_doctor(matches: &ArgMatches) -> Result<Command, UsageError> {
    Ok(Command::Doctor(DoctorArgs {
        repair: flagged(matches, "repair"),
        clear_attach_budget: flagged(matches, "clear-attach-budget"),
    }))
}

fn set_stdin(
    target: &mut Option<StdinSource>,
    value: StdinSource,
    usage: &'static CommandSpec,
) -> Result<(), UsageError> {
    if target.is_some() {
        return Err(UsageError::new(
            "--stdin, --stdin-file, and --stdin-base64 conflict",
            usage,
        ));
    }
    *target = Some(value);
    Ok(())
}

fn workspace_name(
    value: &OsStr,
    reserve_main: bool,
    usage: &'static CommandSpec,
) -> Result<String, UsageError> {
    let Some(value) = value.to_str() else {
        return Err(UsageError::new("workspace names must be UTF-8", usage));
    };
    let parsed = if reserve_main {
        WorkspaceName::session(value)
    } else {
        WorkspaceName::new(value)
    };
    match parsed {
        Ok(name) => Ok(name.as_str().to_owned()),
        Err(MetadataError::ReservedSessionName) => {
            Err(UsageError::new("workspace name `main` is reserved", usage))
        }
        Err(MetadataError::InvalidWorkspaceName(_)) => {
            Err(UsageError::new(WorkspaceName::USAGE, usage))
        }
        Err(error) => Err(UsageError::new(error.to_string(), usage)),
    }
}

fn unknown_flag(flag: &str, usage: &'static CommandSpec) -> UsageError {
    UsageError::new(format!("unknown flag `{flag}`"), usage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reseed_parses_existing_seed_selection_and_nonmutating_preview() {
        assert_eq!(
            parse_args(["reseed", "raven"]).unwrap().command,
            Command::Reseed(ReseedArgs {
                workspace: "raven".into(),
                from: "main".into(),
                dry_run: false,
            })
        );
        let cli = parse_args([
            "reseed",
            "main",
            "--from",
            "lane-base",
            "--dry-run",
            "--json",
        ])
        .unwrap();
        assert!(cli.global.json);
        assert_eq!(
            cli.command,
            Command::Reseed(ReseedArgs {
                workspace: "main".into(),
                from: "lane-base".into(),
                dry_run: true,
            })
        );
        for refused in [
            &["reseed"][..],
            &["reseed", "raven", "--from"],
            &["reseed", "raven", "--from", "not/a/name"],
            &["reseed", "not/a/name"],
            &["reseed", "raven", "extra"],
            &["reseed", "raven", "--from", "main", "--from", "lane"],
            &["reseed", "raven", "--refresh"],
        ] {
            assert!(parse_args(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn cache_clear_names_one_workspace_or_infers_it() {
        assert_eq!(
            parse_args(["cache", "clear", "raven"]).unwrap().command,
            Command::Cache(CacheCommand::Clear {
                workspace: Some("raven".into())
            })
        );
        assert_eq!(
            parse_args(["cache", "clear", "main", "--json"])
                .unwrap()
                .command,
            Command::Cache(CacheCommand::Clear {
                workspace: Some("main".into())
            })
        );
        assert_eq!(
            parse_args(["cache", "clear"]).unwrap().command,
            Command::Cache(CacheCommand::Clear { workspace: None })
        );
        for refused in [
            &["cache"][..],
            &["cache", "reset"],
            &["cache", "clear", "not/a/name"],
            &["cache", "clear", "raven", "extra"],
            &["cache", "clear", "raven", "--force"],
        ] {
            assert!(parse_args(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn cache_clear_help_says_nx_reset_leaves_the_entries() {
        for argv in [
            &["help", "cache"][..],
            &["cache", "--help"],
            &["cache", "clear", "--help"],
        ] {
            assert_eq!(
                parse_args(argv.iter().copied()).unwrap().command,
                Command::Help(Some(&CACHE))
            );
        }
        let page = CACHE
            .page()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        for fact in [
            "cache clear [<ws>]",
            "`nx reset` removes the links and leaves every entry on the volume",
            "`.nx/cache` and `.nx/workspace-data`",
            "accepted-task store",
            "names what is running",
            "CacheClearReport",
        ] {
            assert!(page.contains(fact), "missing {fact}: {page}");
        }
    }

    #[test]
    fn gc_supersedes_one_named_abandonment_only_with_a_one_line_reason() {
        let head = "1".repeat(40);
        let namespace = format!("raven/{head}");
        assert_eq!(
            parse_args([
                "gc",
                "--supersede",
                &namespace,
                "--reason",
                "replaced by the rewrite"
            ])
            .unwrap()
            .command,
            Command::Gc(GcArgs {
                supersede: Some(AbandonedRefSupersession {
                    workspace: WorkspaceName::new("raven").unwrap(),
                    head: GitOid::new(head.clone()).unwrap(),
                    reason: SupersededReason::new("replaced by the rewrite").unwrap(),
                }),
                ..GcArgs::default()
            })
        );
        for refused in [
            &["gc", "--supersede", &namespace][..],
            &["gc", "--reason", "replaced"],
            &["gc", "--supersede", &namespace, "--reason", ""],
            &["gc", "--supersede", &namespace, "--reason", "  \t"],
            &["gc", "--supersede", &namespace, "--reason", "two\nlines"],
            &["gc", "--supersede", "raven", "--reason", "replaced"],
            &[
                "gc",
                "--supersede",
                "raven/not-an-oid",
                "--reason",
                "replaced",
            ],
            &[
                "gc",
                "--supersede",
                &format!("not/a/name/{head}"),
                "--reason",
                "replaced",
            ],
        ] {
            assert!(parse_args(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn reseed_help_explains_discard_without_manual_seed_refresh() {
        for argv in [
            &["help", "reseed"][..],
            &["reseed", "--help"],
            &["reseed", "--from", "--help"],
        ] {
            assert_eq!(
                parse_args(argv.iter().copied()).unwrap().command,
                Command::Help(Some(&RESEED))
            );
        }
        let page = RESEED
            .page()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        for fact in [
            "--from <target>",
            "--dry-run",
            "rebuilt cold",
            "absent seed refuses",
            "Never refreshes",
            "source tree, HEAD, index",
            "not a promise of physical free space",
        ] {
            assert!(page.contains(fact), "missing {fact}: {page}");
        }
    }

    #[test]
    fn build_state_is_read_only_checkout_scoped_and_rejects_positionals() {
        let cli = parse_args(["build-state", "--json", "--project", "/checkout"]).unwrap();
        assert_eq!(cli.command, Command::BuildState);
        assert_eq!(cli.command.project_discovery(), ProjectDiscovery::Required);
        assert!(cli.global.json);
        assert_eq!(cli.global.project, Some(PathBuf::from("/checkout")));
        assert!(parse_args(["build-state", "main"]).is_err());
        let page = BUILD_STATE.page();
        assert!(page.contains("environment-missing (exit 5)"));
        assert!(page.contains("without discovering tools"));
    }

    #[test]
    fn global_aliases_are_identical_and_can_follow_the_command() {
        let short = parse_args(["-q", "ls", "--json"]).unwrap();
        let long = parse_args(["ls", "--quiet", "--json"]).unwrap();
        assert_eq!(short, long);
        assert!(short.global.quiet);
        assert!(short.global.json);
    }

    #[test]
    fn egress_grant_rejects_authorities_before_project_open() {
        for host in [
            "api.example.test:443",
            "*.example.test:443",
            "https://api.example.test",
        ] {
            for target in ["raven", "--project-wide"] {
                let error = parse_args(["grant", target, "--egress", host])
                    .expect_err("a grant takes a host, not an authority");
                assert_eq!(error.kind, UsageErrorKind::InvalidArguments);
                assert_eq!(error.exit_code(), 2);
                assert!(error.message.contains(host), "{}", error.message);
            }
        }
    }

    #[test]
    fn egress_grant_canonicalizes_the_gateway_host_grammar() {
        for (input, canonical) in [
            ("API.Example.Test", "api.example.test"),
            ("*.EXAMPLE.TEST", "*.example.test"),
            ("bücher.example", "xn--bcher-kva.example"),
            ("[2001:db8::1]", "2001:db8::1"),
        ] {
            let Command::Grant(grant) = parse_args(["grant", "raven", "--egress", input])
                .expect("host grant parses")
                .command
            else {
                panic!("expected grant")
            };
            assert_eq!(grant.egress, [canonical]);
        }
    }

    #[test]
    fn revoke_accepts_recorded_invalid_egress_keys_without_gateway_preflight() {
        for argv in [
            vec!["revoke", "raven", "--egress", "api.example.test:443"],
            vec![
                "revoke",
                "--project-wide",
                "--egress",
                "api.example.test:443",
            ],
            vec!["revoke", "raven", "--all"],
            vec!["revoke", "--project-wide", "--all"],
        ] {
            let cli = parse_args(argv.clone()).expect("supported grant recovery parses");
            assert_eq!(cli.command.project_discovery(), ProjectDiscovery::Required);
        }
        for argv in [
            vec!["revoke", "raven"],
            vec!["revoke", "raven", "--ports", "80"],
            vec![
                "revoke",
                "raven",
                "--opaque",
                "--egress",
                "api.example.test",
            ],
            vec!["revoke", "raven", "--all", "--egress", "api.example.test"],
            vec!["revoke", "--project-wide", "--write", "/opt/output"],
        ] {
            assert!(parse_args(argv.clone()).is_err(), "{argv:?}");
        }
    }

    #[test]
    fn grant_accepts_listing_and_repeatable_path_groups() {
        let listed = parse_args(["grant", "raven"]).expect("grant listing parses");
        let Command::Grant(listed) = listed.command else {
            panic!("expected grant")
        };
        assert!(listed.read.is_empty());
        assert!(listed.write.is_empty());

        let parsed = parse_args([
            "grant", "raven", "--read", "/tmp/z", "/tmp/a", "--read", "/tmp/b", "--write",
            "/tmp/out", "--json",
        ])
        .expect("grant mutation parses");
        assert!(parsed.global.json);
        let Command::Grant(grant) = parsed.command else {
            panic!("expected grant")
        };
        assert_eq!(grant.target, GrantTarget::Workspace("raven".to_owned()));
        assert_eq!(
            grant.read,
            [
                PathBuf::from("/tmp/z"),
                PathBuf::from("/tmp/a"),
                PathBuf::from("/tmp/b")
            ]
        );
        assert_eq!(grant.write, [PathBuf::from("/tmp/out")]);
        assert_eq!(
            Command::Grant(grant).project_discovery(),
            ProjectDiscovery::Required
        );
    }

    /// `--project-wide` names the project's standing grants instead of one workspace's, from the
    /// same project discovery every verb uses. It takes reads and egress; a write grant stays a
    /// per-workspace decision, and naming a workspace alongside it is refused, not guessed.
    #[test]
    fn grant_project_wide_targets_the_project_and_refuses_a_workspace_or_a_write() {
        let listed = parse_args(["grant", "--project-wide"]).expect("project listing parses");
        let Command::Grant(listed) = listed.command else {
            panic!("expected grant")
        };
        assert_eq!(listed.target, GrantTarget::Project);
        assert!(listed.read.is_empty() && listed.egress.is_empty());

        let parsed = parse_args([
            "grant",
            "--project-wide",
            "--read",
            "/opt/shared",
            "--egress",
            "github.com",
            "--egress",
            "index.crates.io",
        ])
        .expect("project grant parses");
        let Command::Grant(grant) = parsed.command else {
            panic!("expected grant")
        };
        assert_eq!(grant.target, GrantTarget::Project);
        assert_eq!(grant.read, [PathBuf::from("/opt/shared")]);
        assert_eq!(grant.egress, ["github.com", "index.crates.io"]);
        assert_eq!(
            Command::Grant(grant).project_discovery(),
            ProjectDiscovery::Required
        );

        for refused in [
            vec!["grant", "raven", "--project-wide", "--read", "/opt/shared"],
            vec!["grant", "--project-wide", "--write", "/opt/output"],
        ] {
            let error = parse_args(refused.clone()).expect_err("refused");
            assert_eq!(error.kind, UsageErrorKind::InvalidArguments, "{refused:?}");
        }
        // Without a target the verb still asks for the workspace, as before.
        let missing = parse_args(["grant", "--read", "/opt/shared"]).expect_err("no target");
        assert!(missing.message.contains("grant requires a workspace"));
    }

    /// `--deny` takes workspace-relative paths for one workspace or the whole project, repeatably,
    /// beside `--deny-write`; the two lists stay separate because they deny different things.
    #[test]
    fn grant_deny_takes_relative_paths_for_a_workspace_or_the_project() {
        for (argv, target) in [
            (
                vec![
                    "grant",
                    "raven",
                    "--deny",
                    ".runtime",
                    "secrets/vault",
                    "--deny-write",
                    ".git/hooks",
                ],
                GrantTarget::Workspace("raven".to_owned()),
            ),
            (
                vec![
                    "grant",
                    "--project-wide",
                    "--deny",
                    ".runtime",
                    "--deny",
                    "secrets/vault",
                    "--deny-write",
                    ".git/hooks",
                ],
                GrantTarget::Project,
            ),
        ] {
            let Command::Grant(grant) = parse_args(argv.clone()).expect("parses").command else {
                panic!("expected grant")
            };
            assert_eq!(grant.target, target, "{argv:?}");
            assert_eq!(
                grant.deny,
                [PathBuf::from(".runtime"), PathBuf::from("secrets/vault")]
            );
            assert_eq!(grant.deny_write, [PathBuf::from(".git/hooks")]);
        }
    }

    /// `--opaque` is the mode of the hosts the same invocation grants, for the workspace or the
    /// whole project alike; without a host to apply to it names nothing and is refused.
    #[test]
    fn opaque_grants_the_invocations_egress_hosts_as_tunnels() {
        for (argv, target) in [
            (
                vec!["grant", "raven", "--egress", "proxy.golang.org", "--opaque"],
                GrantTarget::Workspace("raven".to_owned()),
            ),
            (
                vec![
                    "grant",
                    "--project-wide",
                    "--opaque",
                    "--egress",
                    "proxy.golang.org",
                    "--egress",
                    "sum.golang.org",
                ],
                GrantTarget::Project,
            ),
        ] {
            let Command::Grant(grant) = parse_args(argv.clone()).expect("parses").command else {
                panic!("expected grant")
            };
            assert_eq!(grant.target, target, "{argv:?}");
            assert_eq!(grant.egress_mode, EgressMode::Opaque, "{argv:?}");
        }
        let Command::Grant(intercepted) = parse_args(["grant", "raven", "--egress", "github.com"])
            .expect("parses")
            .command
        else {
            panic!("expected grant")
        };
        assert_eq!(intercepted.egress_mode, EgressMode::Intercept);

        for refused in [
            vec!["grant", "raven", "--opaque"],
            vec![
                "grant",
                "--project-wide",
                "--opaque",
                "--read",
                "/opt/shared",
            ],
        ] {
            let error = parse_args(refused.clone()).expect_err("refused");
            assert!(
                error.message.contains("--egress"),
                "{refused:?}: {}",
                error.message
            );
        }
    }

    /// `--ports N` is a workspace grant of at least N service ports, gateway excluded. It is not a
    /// project-wide policy, and a zero or malformed count is refused rather than read as "no
    /// change" — the operator asked for ports and must hear why none were granted.
    #[test]
    fn ports_grants_a_minimum_service_port_count_to_one_workspace() {
        let listed = parse_args(["grant", "raven"]).expect("listing parses");
        let Command::Grant(listed) = listed.command else {
            panic!("expected grant")
        };
        assert_eq!(listed.service_ports, None);

        let Command::Grant(grant) = parse_args(["grant", "raven", "--ports", "80"])
            .expect("ports grant parses")
            .command
        else {
            panic!("expected grant")
        };
        assert_eq!(grant.target, GrantTarget::Workspace("raven".to_owned()));
        assert_eq!(grant.service_ports, NonZeroU16::new(80));
        assert!(grant.read.is_empty() && grant.write.is_empty() && grant.egress.is_empty());

        let Command::Grant(combined) = parse_args([
            "grant",
            "raven",
            "--egress",
            "github.com",
            "--ports",
            "65535",
        ])
        .expect("ports combine with other grants")
        .command
        else {
            panic!("expected grant")
        };
        assert_eq!(combined.service_ports, NonZeroU16::new(u16::MAX));
        assert_eq!(combined.egress, ["github.com"]);

        let project = parse_args(["grant", "--project-wide", "--ports", "80"])
            .expect_err("project-wide ports refused");
        assert_eq!(project.kind, UsageErrorKind::InvalidArguments);
        assert!(project.message.contains("--ports"), "{}", project.message);

        let zero = parse_args(["grant", "raven", "--ports", "0"]).expect_err("zero refused");
        assert_eq!(zero.kind, UsageErrorKind::InvalidArguments);
        assert!(zero.message.contains("at least 1"), "{}", zero.message);

        for malformed in ["-1", "80.5", "eighty", "0x50", "65536"] {
            let error = parse_args(["grant", "raven", "--ports", malformed])
                .expect_err("malformed refused");
            assert_eq!(
                error.kind,
                UsageErrorKind::InvalidArguments,
                "{malformed:?}"
            );
            assert!(
                error.message.contains("--ports must be a whole number"),
                "{malformed:?}: {}",
                error.message
            );
        }

        let missing = parse_args(["grant", "--ports", "80"]).expect_err("no workspace");
        assert!(missing.message.contains("grant requires a workspace"));
    }

    /// The pre-parse walk answers about output shape exactly as the parser would, and it does not
    /// mistake a value for a flag.
    ///
    /// `globals_before_child_argv` exists only because a refused line has no parsed `Cli` to read
    /// `--json` from. Two ways for that to go wrong, both of which put a refusal in a shape nobody
    /// asked for: disagreeing with the parser on an accepted line, and reading `--project`'s value
    /// — a git root, which may be spelled anything at all — as a global.
    #[test]
    fn the_pre_parse_scan_reads_the_same_output_globals_the_parser_does() {
        let argv = |words: &[&str]| -> Vec<OsString> {
            words.iter().copied().map(OsString::from).collect()
        };

        for words in [
            vec!["ls"],
            vec!["--json", "ls"],
            vec!["ls", "--json"],
            vec!["-q", "ls", "--json"],
            vec!["ls", "--quiet"],
            vec!["--project", "/repo", "--json", "ls"],
            vec!["gateway", "status", "--json"],
        ] {
            let parsed = parse_args(words.iter().copied()).expect("accepted line");
            let walked = globals_before_child_argv(&argv(&words));
            assert_eq!(
                (walked.json, walked.quiet),
                (parsed.global.json, parsed.global.quiet),
                "{words:?}"
            );
        }

        // A git root spelled like a flag is still a git root.
        let walked = globals_before_child_argv(&argv(&["--project", "--json", "bogusverb"]));
        assert!(
            !walked.json,
            "--project's value is a path, not a request for JSON"
        );
        let walked = globals_before_child_argv(&argv(&["--project", "-q", "bogusverb"]));
        assert!(
            !walked.quiet,
            "--project's value is a path, not a request for silence"
        );

        // Everything after `--` is the child's argv and is addressed to the child.
        let walked =
            globals_before_child_argv(&argv(&["exec", "raven", "--", "printf", "--json", "-q"]));
        assert_eq!((walked.json, walked.quiet), (false, false));
    }

    #[test]
    fn version_is_a_root_clap_flag_backed_by_the_npm_package_manifest() {
        for flag in ["--version", "-V"] {
            let cli = parse_args([flag]).expect("version flag parses");
            assert_eq!(cli.command, Command::Version);
            assert_eq!(cli.command.project_discovery(), ProjectDiscovery::NotUsed);
        }
    }

    #[test]
    fn doctor_repair_is_an_explicit_project_scoped_flag() {
        let Command::Doctor(default) = parse_args(["doctor"]).unwrap().command else {
            panic!("expected doctor")
        };
        assert!(!default.repair);

        let parsed = parse_args(["doctor", "--repair", "--project", "/repo"]).unwrap();
        let Command::Doctor(repair) = parsed.command else {
            panic!("expected doctor repair")
        };
        assert!(repair.repair);
        assert_eq!(parsed.global.project, Some(PathBuf::from("/repo")));
        assert_eq!(
            Command::Doctor(repair).project_discovery(),
            ProjectDiscovery::Optional
        );
    }

    #[test]
    fn doctor_clear_attach_budget_is_an_explicit_flag() {
        let Command::Doctor(clear) = parse_args(["doctor", "--clear-attach-budget"])
            .unwrap()
            .command
        else {
            panic!("expected doctor")
        };
        assert!(clear.clear_attach_budget);
        assert!(!clear.repair);
    }

    /// Every `cowshed …` command an attach-budget hint or next step prints parses as a
    /// valid invocation: a hint that names a command the CLI refuses is a dead end at the
    /// exact moment the operator is out of attach headroom.
    #[test]
    fn attach_budget_hint_commands_parse_as_cowshed_invocations() {
        use cowshed_apfs::attach_budget::{CLEAR_COMMAND, ORPHANS_FIRST_HINT};
        for hint in [CLEAR_COMMAND, ORPHANS_FIRST_HINT] {
            let mut spans = 0;
            let mut candidates: Vec<&str> = hint.split('`').skip(1).step_by(2).collect();
            if candidates.is_empty() {
                candidates.push(hint);
            }
            for span in candidates {
                let Some(argv) = span.strip_prefix("cowshed ") else {
                    continue;
                };
                spans += 1;
                let argv: Vec<&str> = argv.split_whitespace().collect();
                parse_args(argv.clone()).unwrap_or_else(|error| {
                    panic!("hint command `cowshed {argv:?}` does not parse: {error}")
                });
            }
            assert!(spans >= 1, "no cowshed command in hint: {hint}");
        }
        assert_eq!(CLEAR_COMMAND, "cowshed doctor --clear-attach-budget");
    }

    #[test]
    fn list_all_is_an_explicit_flag_and_list_rejects_every_other_argument() {
        let Command::List(scoped) = parse_args(["ls"]).unwrap().command else {
            panic!("expected list")
        };
        assert!(!scoped.all);

        let cli = parse_args(["ls", "--all", "--json"]).unwrap();
        let Command::List(all) = cli.command else {
            panic!("expected list")
        };
        assert!(all.all);
        assert!(cli.global.json);

        for invalid in [["ls", "project"], ["ls", "--unknown"]] {
            let error = parse_args(invalid).unwrap_err();
            assert!(error.hint.contains("cowshed ls [--all]"));
        }
    }

    #[test]
    fn attach_accepts_one_workspace_project_scope_or_store_wide() {
        let Command::Attach(named) = parse_args(["attach", "raven"]).unwrap().command else {
            panic!("expected attach")
        };
        assert_eq!(named.workspace.as_deref(), Some("raven"));
        assert!(!named.all);
        assert!(!named.browse);

        let Command::Attach(project) = parse_args(["attach"]).unwrap().command else {
            panic!("expected attach")
        };
        assert_eq!(project.workspace, None);
        assert!(!project.all);

        let Command::Attach(all) = parse_args(["attach", "--all", "--browse"]).unwrap().command
        else {
            panic!("expected attach")
        };
        assert_eq!(all.workspace, None);
        assert!(all.all);
        assert!(all.browse);

        let named_all = parse_args(["attach", "raven", "--all"]).unwrap_err();
        assert_eq!(named_all.message, "attach --all cannot name a workspace");
        assert!(named_all.hint.contains("cowshed attach [ws]"));

        let main = parse_args(["attach", "main"]).unwrap_err();
        assert_eq!(
            main.message,
            "mains are always mounted; attach targets session workspaces"
        );
    }

    #[test]
    fn detach_accepts_one_workspace_or_store_wide() {
        let Command::Detach(named) = parse_args(["detach", "raven"]).unwrap().command else {
            panic!("expected detach")
        };
        assert_eq!(named.workspace.as_deref(), Some("raven"));
        assert!(!named.all);

        let Command::Detach(all) = parse_args(["detach", "--all"]).unwrap().command else {
            panic!("expected detach")
        };
        assert_eq!(all.workspace, None);
        assert!(all.all);

        let named_all = parse_args(["detach", "raven", "--all"]).unwrap_err();
        assert_eq!(named_all.message, "detach --all cannot name a workspace");
        assert!(named_all.hint.contains("cowshed detach [ws]"));

        let main = parse_args(["detach", "main"]).unwrap_err();
        assert_eq!(
            main.message,
            "mains are always mounted; detach targets session workspaces"
        );

        let missing = parse_args(["detach"]).unwrap_err();
        assert_eq!(missing.message, "detach requires a workspace");

        assert_eq!(
            parse_args(["detach", "--all"])
                .unwrap()
                .command
                .project_discovery(),
            ProjectDiscovery::NotUsed
        );
        assert_eq!(
            parse_args(["detach", "raven"])
                .unwrap()
                .command
                .project_discovery(),
            ProjectDiscovery::NotUsed
        );
    }

    #[test]
    fn rekey_accepts_a_session_or_main_workspace() {
        let Command::Rekey(named) = parse_args(["rekey", "raven"]).unwrap().command else {
            panic!("expected rekey")
        };
        assert_eq!(named.workspace, "raven");

        let Command::Rekey(main) = parse_args(["rekey", "main"]).unwrap().command else {
            panic!("expected rekey")
        };
        assert_eq!(main.workspace, "main");
        assert_eq!(
            parse_args(["rekey", "main"])
                .unwrap()
                .command
                .project_discovery(),
            ProjectDiscovery::Required
        );

        let missing = parse_args(["rekey"]).unwrap_err();
        assert_eq!(missing.message, "rekey requires a workspace");
        assert!(missing.hint.contains("cowshed rekey <ws|main>"));
    }

    #[test]
    fn exec_preserves_child_argv_bytes_after_separator() {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::{OsStrExt, OsStringExt};
            let opaque = OsString::from_vec(vec![b'f', 0x80, b'o']);
            let cli = parse_args(vec![
                OsString::from("exec"),
                OsString::from("raven"),
                OsString::from("--"),
                opaque.clone(),
                OsString::from("--json"),
            ])
            .unwrap();
            let Command::Exec(exec) = cli.command else {
                panic!("expected exec")
            };
            assert_eq!(exec.argv[0].as_bytes(), opaque.as_bytes());
            assert_eq!(exec.argv[1], "--json");
            assert!(!cli.global.json);
        }
    }

    #[test]
    fn exec_rejects_conflicting_stdin_sources_with_resolving_hint() {
        let error = parse_args([
            "exec",
            "raven",
            "--stdin",
            "--stdin-file",
            "input",
            "--",
            "cat",
        ])
        .unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert!(error.message.contains("conflict"));
        assert!(error.hint.starts_with("cowshed exec <ws>"));
    }

    #[test]
    fn exec_parses_explicit_output_publication_policy() {
        let cli = parse_args([
            "exec",
            "raven",
            "--stdout-copy",
            "artifacts/stdout.log",
            "--stderr-copy",
            "artifacts/stderr.log",
            "--replace-output",
            "--",
            "build",
        ])
        .unwrap();
        let Command::Exec(exec) = cli.command else {
            panic!("expected exec")
        };

        assert_eq!(
            exec.stdout_copy.as_deref(),
            Some(std::path::Path::new("artifacts/stdout.log"))
        );
        assert_eq!(
            exec.stderr_copy.as_deref(),
            Some(std::path::Path::new("artifacts/stderr.log"))
        );
        assert!(exec.replace_output);
    }

    #[test]
    fn output_publication_defaults_to_create_new_and_rejects_duplicates() {
        let cli = parse_args([
            "exec",
            "raven",
            "--stdout-copy",
            "artifacts/stdout.log",
            "--",
            "build",
        ])
        .unwrap();
        let Command::Exec(exec) = cli.command else {
            panic!("expected exec")
        };
        assert!(!exec.replace_output);

        let duplicate = parse_args([
            "exec",
            "raven",
            "--stdout-copy",
            "one",
            "--stdout-copy",
            "two",
            "--",
            "build",
        ])
        .unwrap_err();
        assert!(duplicate.message.contains("only be specified once"));
    }

    #[test]
    fn exec_names_its_scheduling_and_defaults_to_ordinary_background_work() {
        let exec = |argv: &[&str]| {
            let Command::Exec(exec) = parse_args(argv.iter().copied()).unwrap().command else {
                panic!("expected exec")
            };
            exec
        };
        let ordinary = exec(&["exec", "raven", "--", "build"]);
        assert_eq!(ordinary.class, ExecClass::Background);
        assert_eq!(ordinary.want, None, "an unnamed want is the class's own");
        assert_eq!(ordinary.scope, ExecScope::Workspace);

        let heavy = exec(&["exec", "raven", "--want", "12", "--", "nextest"]);
        assert_eq!(
            (heavy.class, heavy.want.map(NonZeroUsize::get)),
            (ExecClass::Background, Some(12))
        );
        let one = exec(&["exec", "raven", "--want", "1", "--", "build"]);
        assert_eq!(
            one.want,
            Some(NonZeroUsize::MIN),
            "a named want is taken as named"
        );

        let gate = exec(&["exec", "raven", "--class", "gate", "--", "gate"]);
        assert_eq!(gate.class, ExecClass::Gate);
        assert_eq!(gate.scope, ExecScope::Workspace);

        let host = exec(&[
            "exec", "raven", "--class", "gate", "--host", "--want", "18", "--", "gate",
        ]);
        assert_eq!(host.scope, ExecScope::HostGate);
        assert_eq!(host.want.map(NonZeroUsize::get), Some(18));

        let host_background = exec(&["exec", "raven", "--host", "--want", "4", "--", "test"]);
        assert_eq!(
            (
                host_background.class,
                host_background.want.map(NonZeroUsize::get),
                host_background.scope
            ),
            (ExecClass::Background, Some(4), ExecScope::HostGate)
        );
        for (class, spelled) in [
            (ExecClass::Interactive, "interactive"),
            (ExecClass::Resident, "resident"),
            (ExecClass::InteractiveResident, "interactive-resident"),
        ] {
            let host = exec(&["exec", "raven", "--class", spelled, "--host", "--", "x"]);
            assert_eq!((host.class, host.scope), (class, ExecScope::HostGate));
        }

        let resident = exec(&["exec", "raven", "--class", "resident", "--", "sleep", "600"]);
        assert_eq!(
            (resident.class, resident.want, resident.scope),
            (ExecClass::Resident, None, ExecScope::Workspace)
        );

        let interactive_resident = exec(&[
            "exec",
            "raven",
            "--class",
            "interactive-resident",
            "--",
            "sleep",
            "600",
        ]);
        assert_eq!(
            (
                interactive_resident.class,
                interactive_resident.want,
                interactive_resident.scope
            ),
            (ExecClass::InteractiveResident, None, ExecScope::Workspace)
        );

        let interactive = exec(&["exec", "raven", "--class", "interactive", "--", "nextest"]);
        assert_eq!(
            (interactive.class, interactive.want, interactive.scope),
            (ExecClass::Interactive, None, ExecScope::Workspace)
        );
    }

    #[test]
    fn exec_refuses_scheduling_that_cannot_be_admitted() {
        for (argv, mention) in [
            (
                vec!["exec", "raven", "--class", "foreground", "--", "x"],
                "--class must be background, interactive, gate, resident or interactive-resident",
            ),
            (
                vec![
                    "exec", "raven", "--class", "resident", "--want", "4", "--", "x",
                ],
                "--class resident takes no --want",
            ),
            (
                vec![
                    "exec", "raven", "--class", "resident", "--want", "1", "--", "x",
                ],
                "--class resident takes no --want",
            ),
            (
                vec![
                    "exec",
                    "raven",
                    "--class",
                    "interactive-resident",
                    "--want",
                    "4",
                    "--",
                    "x",
                ],
                "--class resident takes no --want",
            ),
            (
                vec!["exec", "raven", "--want", "0", "--", "x"],
                "--want must be a positive whole number",
            ),
            (
                vec![
                    "exec",
                    "raven",
                    "--class",
                    "gate",
                    "--session",
                    "build",
                    "--",
                    "x",
                ],
                "--class gate runs outside any named session",
            ),
            (
                vec!["exec", "raven", "--host", "--session", "build", "--", "x"],
                "--host runs outside any named session",
            ),
        ] {
            let error = parse_args(argv.clone()).unwrap_err();
            assert!(
                error.message.contains(mention),
                "{argv:?}: {}",
                error.message
            );
            assert_eq!(error.exit_code(), 2, "{argv:?}");
        }
    }

    #[test]
    fn replace_output_requires_one_publication_destination() {
        let error = parse_args(["exec", "raven", "--replace-output", "--", "build"]).unwrap_err();

        assert!(error.message.contains("requires"));
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn adopt_parses_explicit_repository_identity_and_quarantine() {
        let cli = parse_args([
            "adopt",
            "/repo",
            "--capacity",
            "100g",
            "--repo-id",
            "local/widget",
            "--quarantine",
            "--json",
        ])
        .unwrap();
        let Command::Adopt(args) = cli.command else {
            panic!("expected adopt")
        };
        assert_eq!(args.path, Some(PathBuf::from("/repo")));
        assert_eq!(args.capacity, Some(OsString::from("100g")));
        assert_eq!(args.repo_id, Some(OsString::from("local/widget")));
        assert!(args.quarantine);
        assert!(cli.global.json);
        assert!(parse_args(["adopt", "--repo-id"]).is_err());
    }

    #[test]
    fn fork_validates_names_and_rejects_the_removed_new_verb() {
        assert!(parse_args(["fork", "Bad_Name"]).is_err());
        assert!(parse_args(["fork", "main"]).is_err());
        assert!(parse_args(["new", "raven"]).is_err());
        assert!(parse_args(["path", "main"]).is_ok());
    }

    #[test]
    fn fork_defaults_to_main_and_ref_implies_clean() {
        for argv in [
            vec!["fork", "raven"],
            vec!["fork", "main", "raven"],
            vec!["fork", "sibling", "raven", "--clean"],
            vec!["fork", "sibling", "raven", "--ref", "main"],
        ] {
            let cli = parse_args(argv.clone()).unwrap();
            let Command::Fork(args) = cli.command else {
                panic!("expected fork")
            };
            assert_eq!(args.destination, "raven");
            assert_eq!(
                args.source.as_deref(),
                if argv.len() == 2 { None } else { Some(argv[1]) }
            );
            assert_eq!(
                args.clean,
                argv.contains(&"--clean") || argv.contains(&"--ref")
            );
        }
        assert!(parse_args(["fork", "main", "raven", "extra"]).is_err());
        assert!(parse_args(["fork", "raven", "--from", "main"]).is_err());
        assert!(
            parse_args(["new", "raven"])
                .unwrap_err()
                .message
                .contains("unknown command")
        );
    }

    #[test]
    fn fork_carries_the_git_worktree_request_through_to_the_command() {
        let cli = parse_args(["fork", "raven", "--git-worktree"]).unwrap();
        let Command::Fork(args) = cli.command else {
            panic!("expected fork")
        };
        assert!(args.git_worktree);
        assert!(!args.register);

        let cli = parse_args(["fork", "raven"]).unwrap();
        let Command::Fork(args) = cli.command else {
            panic!("expected fork")
        };
        assert!(!args.git_worktree);
    }

    #[test]
    fn slots_parse_on_fork_and_path_and_refuse_a_workspace_alongside() {
        let cli = parse_args(["fork", "raven", "--slot", "3"]).unwrap();
        let Command::Fork(args) = cli.command else {
            panic!("expected fork")
        };
        assert_eq!(args.slot, Some(3));

        let cli = parse_args(["path", "--slot", "3"]).unwrap();
        let Command::Path(args) = cli.command else {
            panic!("expected path")
        };
        assert_eq!(args.slot, Some(3));
        assert_eq!(args.workspace, None);

        let cli = parse_args(["path", "raven"]).unwrap();
        let Command::Path(args) = cli.command else {
            panic!("expected path")
        };
        assert_eq!(args.slot, None);
        assert_eq!(args.workspace.as_deref(), Some("raven"));

        // A slot and a name are two different questions; answering both at once would let the
        // caller believe it asked the one it did not.
        assert!(parse_args(["path", "raven", "--slot", "3"]).is_err());
        assert!(parse_args(["path", "--slot", "three"]).is_err());
        assert!(parse_args(["path", "--slot"]).is_err());
    }

    #[test]
    fn sccache_start_takes_a_capacity_and_the_other_verbs_do_not() {
        let cli = parse_args(["sccache", "start", "--capacity", "80g"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Sccache(SccacheCommand::Start {
                capacity: Some(OsString::from("80g"))
            })
        );
        assert!(parse_args(["sccache", "status", "--capacity", "80g"]).is_err());
        assert!(parse_args(["sccache", "stop", "--capacity", "80g"]).is_err());
    }

    /// `setup` is host-level repair: no positionals, no project, and the flags that pick its
    /// direction. A stranded host has no adopted checkout to name, so accepting `--project` would
    /// advertise a context the verb cannot use.
    #[test]
    fn setup_takes_only_its_own_flags_and_never_a_project() {
        const REPAIR: SetupArgs = SetupArgs {
            uninstall: false,
            force: false,
            sccache: false,
            retire_caches_volume: false,
            downgrade: false,
        };
        assert_eq!(
            parse_args(["setup"]).unwrap().command,
            Command::Setup(REPAIR)
        );

        let cli = parse_args(["setup", "--json"]).unwrap();
        assert_eq!(cli.command, Command::Setup(REPAIR));
        assert!(cli.global.json);

        let cli = parse_args(["--quiet", "setup"]).unwrap();
        assert_eq!(cli.command, Command::Setup(REPAIR));
        assert!(cli.global.quiet);

        assert_eq!(
            parse_args(["setup", "--uninstall"]).unwrap().command,
            Command::Setup(SetupArgs {
                uninstall: true,
                force: false,
                sccache: false,
                retire_caches_volume: false,
                downgrade: false,
            })
        );
        assert_eq!(
            parse_args(["setup", "--uninstall", "--force"])
                .unwrap()
                .command,
            Command::Setup(SetupArgs {
                uninstall: true,
                force: true,
                sccache: false,
                retire_caches_volume: false,
                downgrade: false,
            })
        );

        // sccache is opt-in: absent by default, and it cannot ride along with the teardown that
        // releases the very GC root it registers.
        assert_eq!(
            parse_args(["setup", "--sccache"]).unwrap().command,
            Command::Setup(SetupArgs {
                uninstall: false,
                force: false,
                sccache: true,
                retire_caches_volume: false,
                downgrade: false,
            })
        );
        let error = parse_args(["setup", "--sccache", "--uninstall"]).unwrap_err();
        assert_eq!(
            error.message,
            "--sccache installs sccache; --uninstall removes it"
        );

        // Deleting the retired caches volume is opt-in too, and never part of a teardown.
        assert_eq!(
            parse_args(["setup", "--retire-caches-volume"])
                .unwrap()
                .command,
            Command::Setup(SetupArgs {
                retire_caches_volume: true,
                ..REPAIR
            })
        );
        let error = parse_args(["setup", "--retire-caches-volume", "--uninstall"]).unwrap_err();
        assert_eq!(
            error.message,
            "--retire-caches-volume repairs a host; --uninstall tears it down"
        );

        // Installing a build that does not contain the installed one is asked for by name, and
        // only by a run that installs a build at all.
        assert_eq!(
            parse_args(["setup", "--downgrade"]).unwrap().command,
            Command::Setup(SetupArgs {
                downgrade: true,
                ..REPAIR
            })
        );
        let error = parse_args(["setup", "--downgrade", "--uninstall"]).unwrap_err();
        assert_eq!(
            error.message,
            "--downgrade installs a build; --uninstall installs none"
        );

        let error = parse_args(["setup", "--project", "/repo"]).unwrap_err();
        assert_eq!(error.message, "--project is not valid for setup");
        assert!(error.hint.contains("cowshed setup"));
        assert!(error.hint.contains("--downgrade"));

        let error = parse_args(["setup", "--force"]).unwrap_err();
        assert_eq!(
            error.message,
            "--force only confirms --uninstall; setup never refuses to repair a host"
        );

        assert!(parse_args(["setup", "extra"]).is_err());
        assert!(parse_args(["setup", "--purge"]).is_err());
    }

    /// The workspace mount root is derived (01a_apfs.md, "Stable workspace mount root"), so
    /// `--mount-root` is not an option of `setup`: the parser refuses it as it refuses any unknown
    /// flag, whatever value it carries and whatever flag rides with it, and no usage line or help
    /// surface names it. Only the parser runs; nothing here can reach a live host.
    #[test]
    fn setup_has_no_mount_root_option() {
        let spec = help::command_named("setup").expect("setup has a help page");
        let refused: &[&[&str]] = &[
            &["setup", "--mount-root", "/Users/dev/.cowshed/mnt"],
            &["setup", "--mount-root", "relative/mnt"],
            &["setup", "--mount-root=/Users/dev/.cowshed/mnt"],
            &["setup", "--mount-root"],
            &["setup", "--uninstall", "--mount-root", "/tmp/mnt"],
            &[
                "setup",
                "--uninstall",
                "--force",
                "--mount-root",
                "/tmp/mnt",
            ],
            &["setup", "--mount-root", "/tmp/mnt", "--downgrade"],
            &["setup", "--sccache", "--mount-root", "/tmp/mnt"],
            &[
                "setup",
                "--retire-caches-volume",
                "--mount-root",
                "/tmp/mnt",
            ],
            &["--json", "setup", "--mount-root", "/tmp/mnt"],
        ];
        for argv in refused {
            let error = parse_args(argv.iter().copied()).unwrap_err();
            assert_eq!(error.kind, UsageErrorKind::InvalidArguments, "{argv:?}");
            assert_eq!(error.message, "unknown flag `--mount-root`", "{argv:?}");
            assert_eq!(error.hint, spec.hint(), "{argv:?}");
        }

        assert!(!spec.hint().contains("mount-root"));
        assert!(
            spec.options
                .iter()
                .all(|option| !option.spelling.contains("mount-root"))
        );
        assert!(!spec.page().contains("mount-root"));
        assert!(!help::command_map().contains("mount-root"));
    }

    /// `--purge` belongs to `gateway stop` alone: it names bytes to delete, and deleting them
    /// while starting or querying the service is not a thing anybody meant.
    #[test]
    fn gateway_stop_takes_purge_and_the_other_actions_do_not() {
        assert_eq!(
            parse_args(["gateway", "stop"]).unwrap().command,
            Command::Gateway(GatewayCommand::Stop { purge: false })
        );
        assert_eq!(
            parse_args(["gateway", "stop", "--purge"]).unwrap().command,
            Command::Gateway(GatewayCommand::Stop { purge: true })
        );

        let cli = parse_args(["gateway", "stop", "--purge", "--json"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Gateway(GatewayCommand::Stop { purge: true })
        );
        assert!(cli.global.json);

        for argv in [
            vec!["gateway", "start", "--purge"],
            vec!["gateway", "status", "--purge"],
            vec!["gateway", "run", "--purge"],
            vec!["gateway", "--purge", "stop"],
        ] {
            assert!(parse_args(argv.clone()).is_err(), "accepted {argv:?}");
        }
    }

    /// The credential the gateway will hold is named, never typed: there is no flag carrying the
    /// secret, and a source has to be chosen explicitly.
    #[test]
    fn credential_add_requires_a_scope_and_a_named_secret_source() {
        let cli = parse_args([
            "credential",
            "add",
            "--origin",
            "https://registry.test",
            "--path-prefix",
            "/api/packages/owner/npm/",
            "--secret-env",
            "REGISTRY_READ_TOKEN",
        ])
        .unwrap();
        assert_eq!(
            cli.command,
            Command::Credential(CredentialCommand::Add(CredentialAddArgs {
                origin: "https://registry.test".to_owned(),
                path_prefixes: vec!["/api/packages/owner/npm/".to_owned()],
                methods: vec!["GET".to_owned(), "HEAD".to_owned()],
                secret_env: Some("REGISTRY_READ_TOKEN".to_owned()),
                secret_command: None,
                secret_stdin: false,
            }))
        );

        // A scope is mandatory: a credential admitted for the whole host is not a scope.
        assert!(
            parse_args([
                "credential",
                "add",
                "--origin",
                "https://registry.test",
                "--secret-env",
                "TOKEN",
            ])
            .is_err()
        );
        // So is a source.
        assert!(
            parse_args([
                "credential",
                "add",
                "--origin",
                "https://registry.test",
                "--path-prefix",
                "/api/packages/owner/npm/",
            ])
            .is_err()
        );
        // And there is no flag that takes the secret itself.
        assert!(
            parse_args([
                "credential",
                "add",
                "--origin",
                "https://registry.test",
                "--path-prefix",
                "/x/",
                "--secret",
                "hunter2",
            ])
            .is_err()
        );
    }

    /// `--secret-env` and `--secret-command` travel together — the variable wins at resolution
    /// time and the helper is the fallback — while stdin is exclusive with both.
    #[test]
    fn credential_secret_sources_compose_except_stdin() {
        let cli = parse_args([
            "credential",
            "add",
            "--origin",
            "https://registry.test:8443",
            "--path-prefix",
            "/api/packages/owner/npm/",
            "--method",
            "GET",
            "--secret-env",
            "REGISTRY_READ_TOKEN",
            "--secret-command",
            r#"["credential-helper","read","registry"]"#,
        ])
        .unwrap();
        assert_eq!(
            cli.command,
            Command::Credential(CredentialCommand::Add(CredentialAddArgs {
                origin: "https://registry.test:8443".to_owned(),
                path_prefixes: vec!["/api/packages/owner/npm/".to_owned()],
                methods: vec!["GET".to_owned()],
                secret_env: Some("REGISTRY_READ_TOKEN".to_owned()),
                secret_command: Some(vec![
                    "credential-helper".to_owned(),
                    "read".to_owned(),
                    "registry".to_owned(),
                ]),
                secret_stdin: false,
            }))
        );

        for argv in [
            vec![
                "credential",
                "add",
                "--origin",
                "https://registry.test",
                "--path-prefix",
                "/x/",
                "--secret-stdin",
                "--secret-env",
                "TOKEN",
            ],
            // A shell string is a second language between the operator and the helper.
            vec![
                "credential",
                "add",
                "--origin",
                "https://registry.test",
                "--path-prefix",
                "/x/",
                "--secret-command",
                "credential-helper read registry",
            ],
            vec![
                "credential",
                "add",
                "--origin",
                "https://registry.test",
                "--path-prefix",
                "/x/",
                "--secret-command",
                "[]",
            ],
        ] {
            assert!(parse_args(argv.clone()).is_err(), "accepted {argv:?}");
        }
    }

    #[test]
    fn credential_reports_and_removals_name_their_route() {
        assert_eq!(
            parse_args(["credential", "ls"]).unwrap().command,
            Command::Credential(CredentialCommand::Status)
        );
        assert_eq!(
            parse_args(["credential", "status"]).unwrap().command,
            Command::Credential(CredentialCommand::Status)
        );
        assert_eq!(
            parse_args(["credential", "rm", "--origin", "https://registry.test"])
                .unwrap()
                .command,
            Command::Credential(CredentialCommand::Remove {
                origin: "https://registry.test".to_owned()
            })
        );
        assert!(parse_args(["credential", "rm"]).is_err());
        assert!(parse_args(["credential"]).is_err());
        assert!(parse_args(["credential", "forget"]).is_err());
        // Enrolment is per project, so the project selector is honoured like any project verb.
        let cli = parse_args(["credential", "status", "--project", "/tmp/checkout"]).unwrap();
        assert_eq!(
            cli.global.project.as_deref(),
            Some(std::path::Path::new("/tmp/checkout"))
        );
        assert_eq!(cli.command.project_discovery(), ProjectDiscovery::Required);
    }

    #[test]
    fn identity_add_names_exactly_one_remote_of_a_discovered_project() {
        let cli = parse_args(["identity", "add", "forge", "--project", "/tmp/checkout"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Identity(IdentityCommand::Add {
                remote: "forge".to_owned()
            })
        );
        assert_eq!(cli.command.project_discovery(), ProjectDiscovery::Required);
        assert!(parse_args(["identity", "add"]).is_err());
        assert!(parse_args(["identity", "add", "forge", "origin"]).is_err());
        assert!(parse_args(["identity"]).is_err());
        assert!(parse_args(["identity", "rm", "forge"]).is_err());
    }

    #[test]
    fn resize_grows_the_image_or_with_build_the_build_volume() {
        use cowshed_core::api::dto::ResizeVolume;
        let resize = |args: &[&str]| match parse_args(args).unwrap().command {
            Command::Resize(args) => args,
            other => panic!("expected resize, got {other:?}"),
        };
        assert_eq!(
            resize(&["resize", "raven", "200g"]),
            ResizeArgs {
                workspace: "raven".to_owned(),
                capacity: OsString::from("200g"),
                volume: ResizeVolume::Workspace,
            }
        );
        assert_eq!(
            resize(&["resize", "main", "--build", "300g"]),
            ResizeArgs {
                workspace: "main".to_owned(),
                capacity: OsString::from("300g"),
                volume: ResizeVolume::Build,
            }
        );
        for refused in [
            &["resize", "raven"][..],
            &["resize", "raven", "200g", "--build", "300g"],
            &["resize", "raven", "--build", "1g", "--build", "2g"],
        ] {
            assert!(parse_args(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn lifecycle_options_parse_with_last_value_precedence() {
        let cli = parse_args([
            "land",
            "raven",
            "--target",
            "release/one",
            "--check",
            "cargo test",
            "--target",
            "release/two",
            "--check",
            "cargo clippy",
            "--no-retire",
            "--push-only",
            "--expected-workspace-incarnation",
            "0198f2c0b7e34dc795f17b238b331c80",
            "--expected-source-head",
            "1111111111111111111111111111111111111111",
            "--expected-target-head",
            "missing",
            "--ref-lock",
            "/tmp/ref.lock",
            "--json",
        ])
        .unwrap();
        let Command::Land(args) = cli.command else {
            panic!("expected land")
        };
        assert_eq!(args.target, Some(OsString::from("release/two")));
        assert_eq!(
            args.checks,
            [OsString::from("cargo test"), OsString::from("cargo clippy")]
        );
        assert!(!args.retire);
        assert!(args.push_only);
        assert_eq!(args.ref_lock, Some(OsString::from("/tmp/ref.lock")));
        assert!(cli.global.json);

        let Command::Remove(remove) = parse_args(["rm", "main", "--restore"]).unwrap().command
        else {
            panic!("expected remove")
        };
        assert!(remove.restore);
        assert!(!remove.force);
    }

    /// `--force` and `--abandon` are separate flags carrying separate authorizations, and the
    /// destructive one is long-form only so no short spelling can be typed by accident.
    #[test]
    fn rm_parses_force_and_abandon_as_independent_authorizations() {
        let Command::Remove(plain) = parse_args(["rm", "raven"]).unwrap().command else {
            panic!("expected remove")
        };
        assert!(!plain.force && !plain.restore && !plain.abandon);

        let Command::Remove(forced) = parse_args(["rm", "raven", "--force"]).unwrap().command
        else {
            panic!("expected remove")
        };
        assert!(forced.force && !forced.abandon);

        let Command::Remove(abandoned) = parse_args(["rm", "raven", "--abandon"]).unwrap().command
        else {
            panic!("expected remove")
        };
        assert!(abandoned.abandon && !abandoned.force);

        let Command::Remove(both) = parse_args(["rm", "raven", "--force", "--abandon"])
            .unwrap()
            .command
        else {
            panic!("expected remove")
        };
        assert!(both.force && both.abandon);

        // No short spelling. On main, `--abandon` authorizes only what a restore would otherwise
        // refuse to lose, so it is refused without `--restore`.
        assert!(parse_args(["rm", "raven", "-a"]).is_err());
        let error = parse_args(["rm", "main", "--abandon"]).expect_err("main has no landed gate");
        assert!(error.message.contains("--restore"), "{}", error.message);
        // Usage text is where the flags are documented deliberately.
        assert_eq!(
            error.hint,
            "cowshed rm <ws> [--force] [--restore] [--abandon] [--purge]"
        );
        let Command::Remove(discard) = parse_args(["rm", "main", "--restore", "--abandon"])
            .unwrap()
            .command
        else {
            panic!("expected remove")
        };
        assert!(discard.restore && discard.abandon && !discard.force);
        // `--purge` removes the whole project, so it rides only on main's restore and carries the
        // session authorizations it is given, never more.
        let Command::Remove(purge) =
            parse_args(["rm", "main", "--restore", "--purge", "--force", "--abandon"])
                .unwrap()
                .command
        else {
            panic!("expected remove")
        };
        assert!(purge.purge && purge.restore && purge.force && purge.abandon);
        assert!(parse_args(["rm", "main", "--purge"]).is_err());
        assert!(parse_args(["rm", "raven", "--purge"]).is_err());
    }

    /// Which verbs may omit `<ws>` is a parser-level fact, and the split is deliberate: acting on
    /// the workspace you are standing in is inferable, losing it is not.
    #[test]
    fn only_in_place_verbs_accept_an_omitted_workspace() {
        for verb in ["rebase", "push", "checkpoint", "path"] {
            let cli = parse_args([verb]).unwrap_or_else(|_| panic!("{verb} may infer its cwd"));
            let named = parse_args([verb, "raven"])
                .unwrap_or_else(|_| panic!("{verb} still accepts a name"));
            assert_ne!(
                format!("{:?}", cli.command),
                format!("{:?}", named.command),
                "{verb} must carry the explicit name through rather than discarding it"
            );
        }
        // Retire, replace, rename, unmount: the workspace has to be named.
        // `attach` without a name is a different scope (the project's detached
        // sessions), not cwd inference of one workspace.
        for verb in ["rm", "land", "restore", "mv", "detach", "exec"] {
            assert!(
                parse_args([verb]).is_err(),
                "{verb} must not infer a workspace from the cwd"
            );
        }
    }

    #[test]
    fn mv_reads_its_destination_according_to_its_source() {
        let Command::Move(args) = parse_args(["mv", "raven", "kestrel"])
            .expect("rename")
            .command
        else {
            panic!("mv <ws> <name> parses as a rename");
        };
        assert_eq!(
            args.destination,
            MoveDestination::Workspace("kestrel".to_owned())
        );

        let Command::Move(args) = parse_args(["mv", "main", "/Users/dev/moved"])
            .expect("checkout move")
            .command
        else {
            panic!("mv main <path> parses as a checkout move");
        };
        assert_eq!(args.source, "main");
        assert_eq!(
            args.destination,
            MoveDestination::Checkout(PathBuf::from("/Users/dev/moved"))
        );
        let Command::Move(args) = parse_args(["mv", "main", "--repo-id", "acme/renamed"])
            .expect("identity move")
            .command
        else {
            panic!("mv main --repo-id parses as an identity move");
        };
        assert_eq!(args.source, "main");
        assert_eq!(
            args.destination,
            MoveDestination::RepoId(RepoId::parse("acme/renamed").expect("repo identity"))
        );
        assert!(parse_args(["mv", "main", "--repo-id", "not-valid"]).is_err());
        assert!(parse_args(["mv", "raven", "--repo-id", "acme/renamed"]).is_err());
        assert!(
            parse_args([
                "mv",
                "main",
                "/Users/dev/moved",
                "--repo-id",
                "acme/renamed"
            ])
            .is_err()
        );

        // A path where a workspace name belongs, and a workspace name where a path belongs, are
        // each rejected by the grammar the source selected — never silently reinterpreted.
        assert!(parse_args(["mv", "raven", "/Users/dev/moved"]).is_err());
        assert!(parse_args(["mv", "main", "relative/path"]).is_err());
        assert!(parse_args(["mv", "main", "kestrel"]).is_err());
        // `main` remains reserved as a rename destination.
        assert!(parse_args(["mv", "raven", "main"]).is_err());
    }

    #[test]
    fn lifecycle_parsers_enforce_required_values_and_preserve_revision_bytes() {
        assert!(parse_args(["fork"]).is_err());
        assert!(parse_args(["restore", "raven"]).is_err());
        assert!(parse_args(["push", "raven", "--branch"]).is_err());
        assert!(parse_args(["land", "raven", "--check"]).is_err());
        assert!(parse_args(["rm", "raven", "--restore"]).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::ffi::{OsStrExt, OsStringExt};
            let opaque = OsString::from_vec(vec![b'm', 0x80, b'a', b'i', b'n']);
            let cli = parse_args(vec![
                OsString::from("rebase"),
                OsString::from("raven"),
                OsString::from("--onto"),
                opaque.clone(),
            ])
            .unwrap();
            let Command::Rebase(args) = cli.command else {
                panic!("expected rebase")
            };
            assert_eq!(args.onto.unwrap().as_bytes(), opaque.as_bytes());
        }
    }

    #[test]
    fn only_a_rebase_that_names_gate_runs_its_carry_as_gate_work() {
        let rebase = |argv: &[&str]| {
            let Command::Rebase(args) = parse_args(argv.iter().copied()).unwrap().command else {
                panic!("expected rebase")
            };
            args.gate
        };
        assert!(!rebase(&["rebase", "raven"]));
        assert!(rebase(&["rebase", "raven", "--gate"]));
    }

    #[test]
    fn no_args_is_usage_and_command_map_is_complete() {
        let error = parse_args(Vec::<OsString>::new()).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert_eq!(error.kind, UsageErrorKind::MissingCommand);
        let map = error.command_map().unwrap();
        // Every verb the parser dispatches, including the ones a hand-written map forgot.
        for spec in COMMANDS {
            assert!(
                map.lines()
                    .any(|line| line.trim_start().starts_with(spec.name)),
                "missing {}",
                spec.name
            );
        }
        assert!(map.lines().any(|line| line.trim_start().starts_with("mv")));
    }

    /// `--help` is answered wherever it is typed, including inside a command line the parser would
    /// otherwise refuse — which is exactly when it gets typed.
    #[test]
    fn help_is_answered_before_a_verb_grammar_is_enforced() {
        for spelling in [["--help"], ["-h"], ["help"]] {
            assert_eq!(parse_args(spelling).unwrap().command, Command::Help(None));
        }

        let fork = help::command_named("fork").unwrap();
        assert_eq!(
            parse_args(["fork", "--help"]).unwrap().command,
            Command::Help(Some(fork))
        );
        assert_eq!(
            parse_args(["help", "fork"]).unwrap().command,
            Command::Help(Some(fork))
        );
        // A half-typed line answers the question rather than refusing the grammar it is missing.
        assert_eq!(
            parse_args(["fork", "--slot", "--help"]).unwrap().command,
            Command::Help(Some(fork))
        );

        // Globals still parse around it.
        let cli = parse_args(["--json", "--help"]).unwrap();
        assert_eq!(cli.command, Command::Help(None));
        assert!(cli.global.json);

        // Past `--` the argument belongs to the child, not to cowshed.
        let Command::Exec(exec) = parse_args(["exec", "raven", "--", "cargo", "--help"])
            .unwrap()
            .command
        else {
            panic!("expected exec")
        };
        assert_eq!(exec.argv, ["cargo", "--help"]);
        assert_eq!(
            parse_args(["exec", "raven", "--help", "--", "cargo"])
                .unwrap()
                .command,
            Command::Help(help::command_named("exec"))
        );

        assert!(parse_args(["help", "new", "rm"]).is_err());
    }

    /// A mistyped verb is corrected, because an agent that only learns "unknown command" retries
    /// the same spelling.
    #[test]
    fn an_unknown_command_names_the_command_that_was_meant() {
        let error = parse_args(["sscache"]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert_eq!(
            error.message,
            "unknown command `sscache`; did you mean: sccache"
        );
        assert_eq!(error.hint, "cowshed --help");

        assert_eq!(
            parse_args(["help", "sscache"]).unwrap_err().message,
            "unknown command `sscache`; did you mean: sccache"
        );

        // Nothing within two edits: no invented suggestion.
        let unrelated = parse_args(["frobnicate"]).unwrap_err();
        assert_eq!(unrelated.message, "unknown command `frobnicate`");
    }

    #[test]
    fn every_command_declares_its_project_discovery_requirement() {
        use ProjectDiscovery::{NotUsed, Optional, Required};

        let cases: &[(&[&str], ProjectDiscovery)] = &[
            (&["adopt", "/repo"], Required),
            (&["setup"], NotUsed),
            (&["fork", "raven"], Required),
            (&["fork", "raven", "falcon"], Required),
            (&["mv", "raven", "falcon"], Required),
            (&["checkpoint", "raven"], Required),
            (&["restore", "raven", "saved"], Required),
            (&["attach"], Required),
            (&["attach", "--all"], NotUsed),
            (&["ls"], Optional),
            (&["ls", "--all"], NotUsed),
            (&["path", "raven"], Required),
            (&["exec", "raven", "--", "true"], Required),
            (&["rm", "raven"], Required),
            (&["forget", "acme/widget", "--abandon"], NotUsed),
            (&["attach", "raven"], Required),
            (&["detach", "raven"], NotUsed),
            (&["detach", "--all"], NotUsed),
            (&["resize", "raven", "32GiB"], Required),
            (&["defrag", "main"], Required),
            (&["reseed", "raven"], Required),
            (&["reseed", "main", "--from", "lane", "--dry-run"], Required),
            (&["rekey", "raven"], Required),
            (&["gc"], Required),
            (&["push", "raven"], Required),
            (&["rebase", "raven"], Required),
            (&["land", "raven"], Required),
            (&["doctor"], Optional),
            (&["gateway", "status"], NotUsed),
            (&["controller"], Required),
            (&["sccache", "status"], NotUsed),
            (&["skill", "install"], NotUsed),
            (&["land-turn", "repo", "--", "true"], NotUsed),
            (&["lock", "/tmp/x.lock", "--", "true"], NotUsed),
            (&["lock-holders", "/tmp/x.lock"], NotUsed),
            (
                &["hold", "--control-fd", "3", "--state-fd", "4", "--", "true"],
                NotUsed,
            ),
            (&["tree-holders", "/tmp/tree"], NotUsed),
            (&["help"], NotUsed),
        ];

        for (arguments, expected) in cases {
            let cli = parse_args(arguments.iter().copied()).expect("representative command parses");
            assert_eq!(cli.command.project_discovery(), *expected, "{arguments:?}");
        }
    }

    /// The grammar a usage error hints is the option table printed, so the flags a verb accepts
    /// and the flags its usage line advertises cannot drift apart.
    #[test]
    fn usage_hints_come_from_the_same_table_as_the_help() {
        let error = parse_args(["fork", "raven", "--unknown"]).unwrap_err();
        assert_eq!(error.message, "unknown flag `--unknown`");
        assert_eq!(
            error.hint,
            "cowshed fork [<src>] <ws> [--clean] [--ref <rev>] [--browse] [--slot <n>] [--register] [--git-worktree] [--markdown-only]"
        );

        for spec in COMMANDS {
            let hint = spec.hint();
            for option in spec.options {
                assert!(
                    hint.contains(option.spelling),
                    "{} hints without {}",
                    spec.name,
                    option.spelling
                );
            }
        }
    }

    /// `hold` takes two distinct caller descriptors, 3 or above, and a command after `--`.
    #[test]
    fn hold_takes_two_distinct_caller_descriptors_and_a_command() {
        let Command::Hold(hold) = parse_args([
            "hold",
            "--control-fd",
            "3",
            "--state-fd",
            "4",
            "--",
            "sh",
            "-c",
            "true",
        ])
        .unwrap()
        .command
        else {
            panic!("hold parses as hold");
        };
        assert_eq!(
            hold,
            HoldArgs {
                control_fd: 3,
                state_fd: 4,
                argv: vec!["sh".into(), "-c".into(), "true".into()],
            }
        );
        let refused: [(&[&str], &str); 5] = [
            (
                &["hold", "--control-fd", "3", "--state-fd", "3", "--", "true"],
                "--control-fd and --state-fd must be two descriptors: requests and events never share one",
            ),
            (
                &["hold", "--control-fd", "2", "--state-fd", "4", "--", "true"],
                "--control-fd must be an open descriptor number of 3 or above, never a standard stream",
            ),
            (
                &["hold", "--state-fd", "4", "--", "true"],
                "hold requires --control-fd and --state-fd before `--`",
            ),
            (
                &["hold", "--control-fd", "3", "--state-fd", "4"],
                "hold requires `--` before the command",
            ),
            (
                &["hold", "--control-fd", "3", "--state-fd", "4", "true"],
                "hold requires `--` before the command",
            ),
        ];
        for (arguments, message) in refused {
            assert_eq!(
                parse_args(arguments.iter().copied()).unwrap_err().message,
                message,
                "{arguments:?}"
            );
        }
    }

    #[test]
    fn missing_positionals_use_the_same_words_as_require_helpers() {
        let cases = [
            (["fork"].as_slice(), "fork requires a destination workspace"),
            (&["mv"], "mv requires a workspace"),
            (&["restore"], "restore requires a workspace"),
            (&["exec", "--", "true"], "exec requires a workspace"),
            (&["rm"], "rm requires a workspace"),
            (&["forget"], "forget requires an owner/repo"),
            (&["detach"], "detach requires a workspace"),
            (&["resize"], "resize requires a workspace"),
            (&["defrag"], "defrag requires a workspace"),
            (&["reseed"], "reseed requires a workspace"),
            (&["rekey"], "rekey requires a workspace"),
            (&["land"], "land requires a workspace"),
            (&["gateway"], "gateway action is required"),
            (&["sccache"], "sccache action is required"),
            (&["skill"], "skill action is required"),
        ];
        for (arguments, message) in cases {
            let error = parse_args(arguments.iter().copied()).unwrap_err();
            assert_eq!(error.message, message, "{arguments:?}");
            assert_eq!(
                help::command_named(arguments[0]).unwrap().missing,
                message,
                "{arguments:?} clap fallback drifted"
            );
        }
    }

    #[test]
    fn no_help_page_contains_provision_or_a_phantom_job_verb() {
        for spec in COMMANDS {
            let page = spec.page();
            assert!(
                !page.contains("provision"),
                "{} help must not say provision:\n{page}",
                spec.name
            );
            assert!(
                !page.contains("cowshed job"),
                "{} help must not name a job verb:\n{page}",
                spec.name
            );
        }
        let exec = help::command_named("exec").unwrap().page();
        assert!(exec.contains("exec --session") || exec.contains("`--session`"));
        assert!(
            help::command_named("setup")
                .unwrap()
                .summary
                .contains("create or repair")
        );
    }

    #[test]
    fn disk_command_deadline_is_positive_and_only_before_the_separator() {
        let Command::DiskCommand(args) = parse_args([
            "disk-command",
            "diskutil",
            "--deadline-ms",
            "37",
            "--",
            "info",
            "--deadline-ms",
            "0",
        ])
        .unwrap()
        .command
        else {
            panic!("disk command")
        };
        assert_eq!(args.deadline, std::time::Duration::from_millis(37));
        assert_eq!(
            args.args,
            ["info", "--deadline-ms", "0"].map(OsString::from)
        );
        let Command::DiskCommand(default) = parse_args([
            "disk-command",
            "diskutil",
            "--",
            "info",
            "--deadline-ms",
            "0",
        ])
        .unwrap()
        .command
        else {
            panic!("disk command")
        };
        assert_eq!(default.deadline, cowshed_apfs::DISK_CHILD_DEADLINE);
        assert_eq!(
            default.args,
            ["info", "--deadline-ms", "0"].map(OsString::from)
        );
        for value in ["0", "-1", "+1", "1.5", "no", "18446744073709551616"] {
            let error = parse_args([
                "disk-command",
                "diskutil",
                "--deadline-ms",
                value,
                "--",
                "info",
            ])
            .expect_err("invalid deadline");
            assert_eq!(error.exit_code(), 2, "{value}");
        }
    }

    #[test]
    fn disk_command_takes_a_named_tool_and_its_argv_after_the_separator() {
        let cli = parse_args(["disk-command", "hdiutil", "--", "info"]).unwrap();
        assert_eq!(
            cli.command,
            Command::DiskCommand(DiskCommandArgs {
                tool: DiskTool::Hdiutil,
                deadline: cowshed_apfs::DISK_CHILD_DEADLINE,
                args: vec![OsString::from("info")],
            })
        );
        assert_eq!(cli.command.project_discovery(), ProjectDiscovery::NotUsed);

        // Past `--` every word is the tool's, cowshed's own flags included.
        let Command::DiskCommand(disk) = parse_args([
            "disk-command",
            "diskutil",
            "--",
            "image",
            "attach",
            "--json",
            "-q",
            "--help",
        ])
        .unwrap()
        .command
        else {
            panic!("disk-command parses");
        };
        assert_eq!(disk.tool, DiskTool::Diskutil);
        assert_eq!(
            disk.args,
            ["image", "attach", "--json", "-q", "--help"].map(OsString::from)
        );

        for (argv, said) in [
            (&["disk-command", "hdiutil", "info"][..], "requires `--`"),
            (&["disk-command", "hdiutil", "--"][..], "`--`"),
            (
                &["disk-command", "fsck_apfs", "--", "-n"][..],
                "diskutil or hdiutil",
            ),
            (
                &["disk-command", "/usr/sbin/diskutil", "--", "list"][..],
                "diskutil or hdiutil",
            ),
            (&["disk-command"][..], "requires a tool"),
            (
                &["--json", "disk-command", "hdiutil", "--", "info"][..],
                "--json",
            ),
            (
                &["disk-command", "--project", "/w", "hdiutil", "--", "info"][..],
                "--project",
            ),
        ] {
            let error = parse_args(argv.iter().copied()).expect_err("refused");
            assert!(error.message.contains(said), "{argv:?}: {}", error.message);
            assert_eq!(error.exit_code(), 2, "{argv:?}");
        }
        assert_eq!(
            parse_args(["disk-command", "--help"]).unwrap().command,
            Command::Help(help::command_named("disk-command"))
        );
    }
}
