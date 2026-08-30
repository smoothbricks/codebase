use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, ArgAction, ArgMatches, Command as ClapCommand, value_parser};
use cowshed_core::metadata::{MetadataError, WorkspaceName};
use cowshed_core::repository::RepoId;
use std::ffi::{OsStr, OsString};
use std::fmt;
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
    &NEW,
    &FORK,
    &MOVE,
    &CHECKPOINT,
    &RESTORE,
    &LIST,
    &PATH,
    &EXEC,
    &REMOVE,
    &ATTACH,
    &DETACH,
    &RESIZE,
    &GC,
    &PUSH,
    &REBASE,
    &LAND,
    &DOCTOR,
    &GATEWAY,
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
    New(NewArgs),
    Fork(ForkArgs),
    Move(MoveArgs),
    Checkpoint(CheckpointArgs),
    Restore(RestoreArgs),
    List(ListArgs),
    Path(PathArgs),
    Exec(ExecArgs),
    Remove(RemoveArgs),
    Attach(AttachArgs),
    Detach(DetachArgs),
    Resize(ResizeArgs),
    Gc(GcArgs),
    Push(PushArgs),
    Rebase(RebaseArgs),
    Land(LandArgs),
    Doctor(DoctorArgs),
    Gateway(GatewayCommand),
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
            Self::Adopt(_)
            | Self::New(_)
            | Self::Fork(_)
            | Self::Move(_)
            | Self::Checkpoint(_)
            | Self::Restore(_)
            | Self::Path(_)
            | Self::Exec(_)
            | Self::Remove(_)
            | Self::Attach(_)
            | Self::Resize(_)
            | Self::Gc(_)
            | Self::Push(_)
            | Self::Rebase(_)
            | Self::Land(_) => ProjectDiscovery::Required,
            Self::List(args) if !args.all => ProjectDiscovery::Optional,
            Self::Doctor(_) => ProjectDiscovery::Optional,
            Self::List(_)
            | Self::Setup(_)
            | Self::Gateway(_)
            | Self::Sccache(_)
            | Self::Skill(_)
            | Self::Version
            | Self::Help(_) => ProjectDiscovery::NotUsed,
        }
    }
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
    /// Host-configured session mount root (`<mount-root>/<owner>/<repo>/<ws>`). Settable only
    /// when every session workspace is detached; direct-mounted mains are unaffected.
    pub mount_root: Option<PathBuf>,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewArgs {
    pub name: String,
    pub reference: Option<OsString>,
    pub from: Option<String>,
    pub browse: bool,
    pub slot: Option<u32>,
    pub register: bool,
    pub git_worktree: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkArgs {
    pub source: String,
    pub destination: String,
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

/// `resize <ws|main> <size>` — grow one workspace's image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResizeArgs {
    pub workspace: String,
    pub capacity: OsString,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcArgs {
    pub dry_run: bool,
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
    pub onto: Option<OsString>,
    pub expected_workspace_incarnation: Option<OsString>,
    pub expected_source_head: Option<OsString>,
    pub expected_onto_head: Option<OsString>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandArgs {
    pub workspace: String,
    pub target: Option<OsString>,
    pub checks: Vec<OsString>,
    pub retire: bool,
    pub push_only: bool,
    pub expected_workspace_incarnation: Option<OsString>,
    pub expected_source_head: Option<OsString>,
    pub expected_target_head: Option<OsString>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DoctorArgs {
    pub repair: bool,
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
        ]))
        .subcommand(leaf("setup").args([flag("uninstall"), flag("force"), value("mount-root")]))
        .subcommand(leaf("new").arg(positional("name", 0..=1)).args([
            value("ref"),
            value("from"),
            flag("browse"),
            value("slot"),
            flag("register"),
            flag("git-worktree"),
        ]))
        .subcommand(
            leaf("fork")
                .arg(positional("src", 0..=1))
                .arg(positional("dst", 0..=1)),
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
                ])
                .arg(
                    Arg::new("argv")
                        .value_parser(value_parser!(OsString))
                        .num_args(0..)
                        .last(true)
                        .allow_hyphen_values(true),
                ),
        )
        .subcommand(leaf("rm").arg(positional("workspace", 0..=1)).args([
            flag("force"),
            flag("restore"),
            flag("abandon"),
        ]))
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
            leaf("resize")
                .arg(positional("workspace", 0..=1))
                .arg(positional("size", 0..=1)),
        )
        .subcommand(leaf("gc").arg(flag("dry-run")))
        .subcommand(leaf("push").arg(positional("workspace", 0..=1)).args([
            value("branch"),
            value("expected-workspace-incarnation"),
            value("expected-source-head"),
            value("expected-destination-head"),
        ]))
        .subcommand(leaf("rebase").arg(positional("workspace", 0..=1)).args([
            value("onto"),
            value("expected-workspace-incarnation"),
            value("expected-source-head"),
            value("expected-onto-head"),
        ]))
        .subcommand(leaf("land").arg(positional("workspace", 0..=1)).args([
            value("target"),
            append_value("check"),
            flag("no-retire"),
            flag("push-only"),
            value("expected-workspace-incarnation"),
            value("expected-source-head"),
            value("expected-target-head"),
        ]))
        .subcommand(leaf("doctor").arg(flag("repair")))
        .subcommand(
            leaf("gateway")
                .subcommand_required(true)
                .subcommand(leaf("start"))
                .subcommand(leaf("stop").arg(flag("purge")))
                .subcommand(leaf("status"))
                .subcommand(leaf("run")),
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
        "new" => parse_new(leaf)?,
        "fork" => parse_fork(leaf)?,
        "mv" => parse_move(leaf)?,
        "checkpoint" => parse_checkpoint(leaf)?,
        "restore" => parse_restore(leaf)?,
        "ls" => parse_list(leaf)?,
        "path" => parse_path(leaf)?,
        "exec" => parse_exec(leaf)?,
        "rm" => parse_remove(leaf)?,
        "attach" => parse_attach(leaf)?,
        "detach" => parse_detach(leaf)?,
        "resize" => parse_resize(leaf)?,
        "gc" => parse_gc(leaf)?,
        "push" => parse_push(leaf)?,
        "rebase" => parse_rebase(leaf)?,
        "land" => parse_land(leaf)?,
        "doctor" => parse_doctor(leaf)?,
        "gateway" => parse_gateway(leaf, &global)?,
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
                Some(spec) if matches!(spec.name, "gateway" | "sccache" | "skill") => {
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

const GATEWAY: CommandSpec = CommandSpec {
    name: "gateway",
    missing: "gateway action is required",
    args: "<start|stop|status|run>",
    trailing: "",
    summary: "manage the host gateway",
    about: &[
        "The gateway is the one trusted process outside every sandbox: workspaces reach the network, main's repository, and each other only through its authenticated Unix socket. `start` installs and loads the per-user LaunchAgent and waits until that socket answers; `stop` boots it out; `status` reports health without starting anything. Both mutations are idempotent.",
        "`run` is the LaunchAgent's own foreground entrypoint. It validates already-mounted storage and never creates any, so a background start can report missing setup but can never raise an authorization prompt.",
        "An ordinary `stop` leaves the host-stable binary copy the agent ran: that copy is host state rather than agent state, and keeping it makes the next `start` a plist write instead of a file copy. `stop --purge` deletes it, for a host that is done with the gateway rather than pausing it.",
    ],
    options: &[Opt {
        spelling: "--purge",
        meaning: "`stop` only: also delete the installed cowshed binary the agent ran, not just its plist",
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
    ],
};

const SETUP: CommandSpec = CommandSpec {
    name: "setup",
    missing: "setup requires an argument",
    args: "",
    trailing: "",
    summary: "create or repair host storage",
    about: &[
        "Brings this host's two dedicated volumes to their canonical state and pins them in `/etc/fstab`: absent volumes are created; existing volumes are never deleted. Detached or mis-mounted ones are remounted where they belong, markers are validated, and the fstab lines that survive a reboot are written. It is idempotent — on a healthy host it changes nothing and says so — and it needs no repository, because its subject is the machine rather than a checkout.",
        "Everything that can require elevation happens inside one authorization session, and every volume's exact intent is printed before the dialog appears; a run with nothing to escalate raises no prompt at all. A volume that exists but is not this host's — a `cowshed.store` in another container — is reported with its device and left exactly as it is, never adopted and never re-created, because re-creating means deleting a volume. `cowshed doctor` explains a host; this repairs one.",
        "`--mount-root <dir>` sets the host session workspace mount root (default `~/.cowshed/mnt`). Session workspaces mount at `<mount-root>/<owner>/<repo>/<ws>`. The root can change only while every session workspace is detached; mains stay mounted directly at their checkout paths.",
        "`--uninstall` is the same transaction backwards, and deliberately narrower: it removes the machine presence — the cowshed-tagged `/etc/fstab` pins, the gateway and sccache LaunchAgents, and the installed binaries they ran — and touches no volume, no image, and no workspace. Nothing it removes holds data; everything it leaves does. It refuses while the volumes still hold workspaces, or while their occupancy cannot be established, until `--force` says the caller means it anyway.",
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
            spelling: "--mount-root <dir>",
            meaning: "set the host session mount root; refused while any session workspace is attached",
        },
    ],
};

/// `setup` takes no project: a host whose volumes are missing has no adopted checkout to select,
/// so silently accepting `--project` would promise a scope the verb does not have. `--force`
/// without `--uninstall` is refused rather than ignored — it confirms a refusal that the forward
/// direction never makes, so accepting it would answer a question nobody asked. `--mount-root`
/// is a third direction and cannot combine with teardown.
fn parse_setup(matches: &ArgMatches, global: &GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &SETUP;
    reject_project(global, USAGE, "--project is not valid for setup")?;
    let uninstall = flagged(matches, "uninstall");
    let force = flagged(matches, "force");
    let mount_root = match os(matches, "mount-root") {
        Some(value) => Some(absolute_mount_root(&value, USAGE)?),
        None => None,
    };
    if mount_root.is_some() && (uninstall || force) {
        return Err(UsageError::new(
            "--mount-root cannot be combined with --uninstall",
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
        mount_root,
    }))
}

const NEW: CommandSpec = CommandSpec {
    name: "new",
    missing: "new requires a workspace name",
    args: "<name>",
    trailing: "",
    summary: "create a workspace",
    about: &[
        "Clones a live image of the project's main workspace and mounts it. The clone is copy-on-write, so a workspace costs the writes it makes rather than a copy of the tree, and it inherits main's source, dependencies, and build state warm.",
        "A build slot is a stable mount path, `mnt/<owner>/<repo>/slot@<n>`, held by one workspace at a time and released when that workspace is removed or renamed, so the next tenant of slot n builds through byte-identical absolute paths. That path identity is the whole feature: cargo derives `-C metadata` from a package id carrying the absolute manifest directory, and sccache hashes the compiler's physical working directory, so the same sources built at two paths are two different compilations sharing no compile cache. A slot tenant is also given `RUSTC_WRAPPER=sccache` and `CARGO_INCREMENTAL=0`, trading local incrementality for a cache its successors can hit. Main cannot take a slot: its mount is fixed by the checkout layout.",
    ],
    options: &[
        Opt {
            spelling: "--ref <rev>",
            meaning: "start the branch at <rev> instead of main's tip; conflicts with --from",
        },
        Opt {
            spelling: "--from <ws>",
            meaning: "clone another workspace of this project instead of main; conflicts with --ref",
        },
        Opt {
            spelling: "--browse",
            meaning: "show the volume in Finder; the default mount is nobrowse",
        },
        Opt {
            spelling: "--slot <n>",
            meaning: "mount at build slot <n>'s stable path instead of one named after the workspace, so compiler caches keyed on absolute paths survive the tenant",
        },
        Opt {
            spelling: "--register",
            meaning: "also add the workspace as a remote in main's repository; off by default because it is host-side state that outlives an interrupted retire",
        },
        Opt {
            spelling: "--git-worktree",
            meaning: "mint a linked worktree of main's repository instead of a standalone clone: one object store and one ref namespace, at the cost of requiring main mounted and giving up checkpoint and restore",
        },
    ],
};

fn parse_new(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &NEW;
    let reference = os(matches, "ref");
    let from = optional_workspace(matches, "from", false, USAGE)?;
    if reference.is_some() && from.is_some() {
        return Err(UsageError::new("--ref conflicts with --from", USAGE));
    }
    Ok(Command::New(NewArgs {
        name: require_workspace(matches, "name", true, USAGE, USAGE.missing)?,
        reference,
        from,
        browse: flagged(matches, "browse"),
        slot: parse_slot_value(matches, USAGE)?,
        register: flagged(matches, "register"),
        git_worktree: flagged(matches, "git-worktree"),
    }))
}

const FORK: CommandSpec = CommandSpec {
    name: "fork",
    missing: "fork requires a source workspace",
    args: "<src> <dst>",
    trailing: "",
    summary: "fork a workspace",
    about: &[
        "Clones a running workspace: two divergent futures from the same mid-flight state, in milliseconds. Grants are not inherited — a fork starts closed, like any new workspace.",
    ],
    options: &[],
};

fn parse_fork(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &FORK;
    Ok(Command::Fork(ForkArgs {
        source: require_workspace(matches, "src", false, USAGE, USAGE.missing)?,
        destination: require_workspace(
            matches,
            "dst",
            true,
            USAGE,
            "fork requires a destination workspace",
        )?,
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

fn absolute_mount_root(value: &OsStr, usage: &'static CommandSpec) -> Result<PathBuf, UsageError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(UsageError::new(
            "the workspace mount root must be an absolute path",
            usage,
        ));
    }
    Ok(path)
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
        "`--landing` and `--landed` answer a different question: is this workspace's work already in main? Both compare each workspace's HEAD — not its recorded branch, which is a label that can drift — against main's branch tip as it stands right now in main's own repository. A commit counts as landed when main holds its patch, so work that reached main by squash-merge, cherry-pick, or a history rewrite counts even though it is not an ancestor. A commit whose equivalence cannot be computed — a merge, or a commit with an empty diff — counts as unlanded, because the only safe error is to refuse. A workspace whose target cannot be resolved reports `indeterminate` with the reason on stderr and is never treated as landed.",
        "Neither flag is on by default because the measurement costs git processes per workspace, and `ls` is used interactively and in scripts.",
    ],
    options: &[
        Opt {
            spelling: "--all",
            meaning: "every adopted project on the host, with its repository id as the first column",
        },
        Opt {
            spelling: "--landing",
            meaning: "add a header row and four columns — unlanded, landed, behind, dirty — measured against main; `?` where the measurement could not be made",
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
        "Long commands auto-background at the soft timeout and keep running under the workspace supervisor. Reattach with `cowshed exec --session` or `--background`.",
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
            meaning: "soft timeout before the command auto-backgrounds (default 120s)",
        },
        Opt {
            spelling: "--background",
            meaning: "background the command immediately instead of waiting for the soft timeout",
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
    }))
}

/// Usage text is where the destructive flags are documented: a human reads options here
/// deliberately, whereas a refusal message is what an agent pattern-matches into a retry — which
/// is why no refusal names the flag that would override it.
const REMOVE: CommandSpec = CommandSpec {
    name: "rm",
    missing: "rm requires a workspace",
    args: "<ws>",
    trailing: "",
    summary: "remove a workspace",
    about: &[
        "Retires one workspace, deleting the image its commits live in. The gate is containment, not preservation: `rm` refuses unless every commit the workspace's HEAD carries is already in the project's main branch. The workspace is marked deleted immediately; detach and image deletion finish in the background.",
        "Containment means ancestry *or* patch equivalence. Work that reached main by squash-merge, cherry-pick, or a history rewrite is not an ancestor of main and is still landed, so retiring it needs no flag — passing a commit-destroying flag to remove work main already has is exactly the habit this gate exists to prevent. A commit whose equivalence cannot be computed — a merge, or one with an empty diff — is treated as not contained. The target tip is read live from main's own repository, never from the workspace's `main` remote, which is a clone-time snapshot; when it cannot be read the answer is `cannot determine`, which is refused exactly as unlanded work is.",
        "The two overrides authorize different losses and neither substitutes for the other, so a script carrying one has not acquired the other.",
    ],
    options: &[
        Opt {
            spelling: "--force",
            meaning: "waive transient state only — a dirty tree, an in-progress merge, a busy mount. It does not waive the containment gate, and it does not preserve what it waives: an uncommitted tree is destroyed with no bundle and no record, so commit it or `cowshed checkpoint` first if it is the only copy",
        },
        Opt {
            spelling: "--restore",
            meaning: "main only: put the pre-adoption checkout back and unbind the project, the reverse of adopt",
        },
        Opt {
            spelling: "--abandon",
            meaning: "the sole authorization for destroying commits main does not contain, and needed only for those: a workspace whose work is upstream by patch equivalence passes without it. Before deleting, main..HEAD is bundled into sessions/.trash/<ws>-<tip>.bundle and the abandonment reported, so the commits stay recoverable by fetching that bundle — the uncommitted tree is not bundled and does not survive",
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
    if abandon && workspace == "main" {
        return Err(UsageError::new(
            "--abandon applies to session workspaces, whose commits main can contain",
            USAGE,
        ));
    }
    Ok(Command::Remove(RemoveArgs {
        workspace,
        force: flagged(matches, "force"),
        restore,
        abandon,
    }))
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

const RESIZE: CommandSpec = CommandSpec {
    name: "resize",
    missing: "resize requires a workspace",
    args: "<ws|main> <size>",
    trailing: "",
    summary: "grow a workspace image",
    about: &[
        "Grows one workspace's image. Sizes are binary units — 100g, 200g, 1t — at least a mebibyte and a whole number of the 4 KiB blocks the image tools resize in. The supervisor is stopped for the resize and restarted after, because the image has to leave the kernel.",
    ],
    options: &[],
};

fn parse_resize(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &RESIZE;
    Ok(Command::Resize(ResizeArgs {
        workspace: require_workspace(matches, "workspace", false, USAGE, USAGE.missing)?,
        capacity: os(matches, "size")
            .ok_or_else(|| UsageError::new("resize requires a size", USAGE))?,
    }))
}

const GC: CommandSpec = CommandSpec {
    name: "gc",
    missing: "gc requires an argument",
    args: "",
    trailing: "",
    summary: "free storage",
    about: &[
        "Reclaims five kinds of garbage in the project selected by the cwd or `--project`, and reports `examined`, `reclaimed`, `retainedPinned` and `freedBytes`. `rm`, `land`, and `restore` run it opportunistically, so most of the time it finds nothing left to do. Every category below is named in the `reason` field of each candidate, and `--dry-run` prints the candidates without touching them — run that first if you want to know what a run would cost you.",
        "`retiredWorkspace` — the image of a workspace already retired by `rm` or `land --retire`, sitting in `sessions/.trash/` with its sidecars, checkpoints and empty mountpoint. Retirement already ran the containment gate, so these commits are in main or were deliberately abandoned. Safe. Note what is *not* in this category: the `<ws>-<tip>.bundle` files `rm --abandon` writes into the same trash directory are never reclaimed, so abandoned commits stay fetchable and their disk use accumulates until you delete them yourself.",
        "`orphanStagingImage` and `orphanStagingMetadata` — an image with no metadata sidecar, or a sidecar with no image, under `sessions/.staging/`. Each is half of a lifecycle transaction — create, fork, restore — that died between writing the image and publishing it. Neither half was ever published under a workspace name, so nothing ever used it. Safe.",
        "`expiredCheckpoint` — an automatic checkpoint that is neither one of the five most recent for its workspace nor younger than fourteen days. This is the category that can delete something you still want: an automatic checkpoint is a real crash-consistent copy of that workspace, and past those two thresholds it goes. Pinned checkpoints are never candidates, and `cowshed checkpoint --keep` or any explicitly labelled checkpoint is pinned; `retainedPinned` in the report counts what was spared for that reason.",
        "`detachedImageCompaction` — punches holes in an unmounted sparse image so the filesystem stops charging for blocks the image no longer uses. This deletes no data: the image's contents are unchanged and it stays fully usable. Only unmounted sparse images qualify.",
    ],
    options: &[Opt {
        spelling: "--dry-run",
        meaning: "report every candidate with its path, byte count, and reason, and delete nothing",
    }],
};

fn parse_gc(matches: &ArgMatches) -> Result<Command, UsageError> {
    Ok(Command::Gc(GcArgs {
        dry_run: flagged(matches, "dry-run"),
    }))
}

const PUSH: CommandSpec = CommandSpec {
    name: "push",
    missing: "push requires an argument",
    args: "[<ws>]",
    trailing: "",
    summary: "preserve a workspace ref",
    about: &[
        "Delivers the workspace branch into main's repository. Under the hood it is a host-side fetch from the workspace mount, so nothing inside the sandbox — hooks, `.git/config` — ever runs outside it. Naming no workspace pushes the one you are standing in.",
        "The `--expected-*` preconditions are for a coordinator driving several workspaces at once: each is checked before anything moves, so a stale plan is refused rather than applied to a workspace that changed underneath it.",
    ],
    options: &[
        Opt {
            spelling: "--branch <name>",
            meaning: "destination branch in main's repository instead of the workspace's own",
        },
        EXPECTED_WORKSPACE_INCARNATION,
        EXPECTED_SOURCE_HEAD,
        Opt {
            spelling: "--expected-destination-head <oid|missing>",
            meaning: "refuse unless the destination branch is still this commit, or `missing` for one that must not exist yet",
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
        "Brings the workspace branch up to current main, run inside the sandbox. A conflict aborts cleanly and names the conflicted paths, leaving the workspace as it was. Naming no workspace rebases the one you are standing in.",
    ],
    options: &[
        Opt {
            spelling: "--onto <rev>",
            meaning: "rebase onto this revision instead of main",
        },
        EXPECTED_WORKSPACE_INCARNATION,
        EXPECTED_SOURCE_HEAD,
        Opt {
            spelling: "--expected-onto-head <oid>",
            meaning: "refuse unless the revision being rebased onto is still this commit",
        },
    ],
};

fn parse_rebase(matches: &ArgMatches) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REBASE;
    Ok(Command::Rebase(RebaseArgs {
        workspace: optional_workspace(matches, "workspace", false, USAGE)?,
        onto: os(matches, "onto"),
        expected_workspace_incarnation: os(matches, "expected-workspace-incarnation"),
        expected_source_head: os(matches, "expected-source-head"),
        expected_onto_head: os(matches, "expected-onto-head"),
    }))
}

const LAND: CommandSpec = CommandSpec {
    name: "land",
    missing: "land requires a workspace",
    args: "<ws>",
    trailing: "",
    summary: "land a workspace",
    about: &[
        "The whole close-out as one primitive: rebase onto the target branch, run the checks inside the sandbox, fast-forward main's repository from the workspace, retire the workspace. Any failing step stops there and leaves the workspace intact, so a landing is all-or-nothing without being a long-lived transaction.",
        "Landing is also what `rm` measures against: the ancestry gate a removal enforces is satisfied by the branch this command delivers to.",
    ],
    options: &[
        Opt {
            spelling: "--target <branch>",
            meaning: "land onto this branch of main's repository instead of main",
        },
        Opt {
            spelling: "--check <cmd>",
            meaning: "validation command run inside the sandbox, repeatable; the default comes from .cowshed.toml [land] check",
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
        target: os(matches, "target"),
        checks,
        retire: !flagged(matches, "no-retire"),
        push_only: flagged(matches, "push-only"),
        expected_workspace_incarnation: os(matches, "expected-workspace-incarnation"),
        expected_source_head: os(matches, "expected-source-head"),
        expected_target_head: os(matches, "expected-target-head"),
    }))
}

const DOCTOR: CommandSpec = CommandSpec {
    name: "doctor",
    missing: "doctor requires an argument",
    args: "",
    trailing: "",
    summary: "check invariants",
    about: &[
        "Checks the invariants a healthy host holds: every image has a marker, every mount matches an image, grants files parse, the caches volume and the gateway answer, autosave is fresh. Exit 0 when healthy, otherwise 5.",
        "With `--repair`, first validates every mounted workspace artifact frame. Duplicate or regressed sequences are refused because resequencing would change store identity and make the rewritten log attest to itself; create a fresh store instead.",
    ],
    options: &[Opt {
        spelling: "--repair",
        meaning: "validate artifact ordering and refuse identity-changing sequence rewrites",
    }],
};

fn parse_doctor(matches: &ArgMatches) -> Result<Command, UsageError> {
    Ok(Command::Doctor(DoctorArgs {
        repair: flagged(matches, "repair"),
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
    fn global_aliases_are_identical_and_can_follow_the_command() {
        let short = parse_args(["-q", "ls", "--json"]).unwrap();
        let long = parse_args(["ls", "--quiet", "--json"]).unwrap();
        assert_eq!(short, long);
        assert!(short.global.quiet);
        assert!(short.global.json);
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
    fn validates_names_and_new_option_conflicts() {
        assert!(parse_args(["new", "Bad_Name"]).is_err());
        assert!(parse_args(["new", "main"]).is_err());
        assert!(parse_args(["new", "raven", "--ref", "HEAD", "--from", "main"]).is_err());
        assert!(parse_args(["path", "main"]).is_ok());
    }

    #[test]
    fn new_carries_the_git_worktree_request_through_to_the_command() {
        let cli = parse_args(["new", "raven", "--git-worktree"]).unwrap();
        let Command::New(args) = cli.command else {
            panic!("expected new")
        };
        assert!(args.git_worktree);
        assert!(!args.register);

        let cli = parse_args(["new", "raven"]).unwrap();
        let Command::New(args) = cli.command else {
            panic!("expected new")
        };
        assert!(!args.git_worktree);
    }

    #[test]
    fn slots_parse_on_new_and_path_and_refuse_a_workspace_alongside() {
        let cli = parse_args(["new", "raven", "--slot", "3"]).unwrap();
        let Command::New(args) = cli.command else {
            panic!("expected new")
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
            mount_root: None,
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
                mount_root: None,
            })
        );
        assert_eq!(
            parse_args(["setup", "--uninstall", "--force"])
                .unwrap()
                .command,
            Command::Setup(SetupArgs {
                uninstall: true,
                force: true,
                mount_root: None,
            })
        );

        let Command::Setup(configured) =
            parse_args(["setup", "--mount-root", "/Users/dev/.cowshed/mnt"])
                .unwrap()
                .command
        else {
            panic!("expected setup")
        };
        assert_eq!(
            configured.mount_root.as_deref(),
            Some(std::path::Path::new("/Users/dev/.cowshed/mnt"))
        );
        assert!(!configured.uninstall);

        let error = parse_args(["setup", "--project", "/repo"]).unwrap_err();
        assert_eq!(error.message, "--project is not valid for setup");
        assert!(error.hint.contains("cowshed setup"));
        assert!(error.hint.contains("--mount-root"));

        let error = parse_args(["setup", "--force"]).unwrap_err();
        assert_eq!(
            error.message,
            "--force only confirms --uninstall; setup never refuses to repair a host"
        );

        let error = parse_args(["setup", "--uninstall", "--mount-root", "/tmp/mnt"]).unwrap_err();
        assert_eq!(
            error.message,
            "--mount-root cannot be combined with --uninstall"
        );

        let error = parse_args(["setup", "--mount-root", "relative/mnt"]).unwrap_err();
        assert_eq!(
            error.message,
            "the workspace mount root must be an absolute path"
        );

        assert!(parse_args(["setup", "extra"]).is_err());
        assert!(parse_args(["setup", "--purge"]).is_err());
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

        // No short spelling, and nothing on main to abandon: main *is* the branch.
        assert!(parse_args(["rm", "raven", "-a"]).is_err());
        let error = parse_args(["rm", "main", "--abandon"]).expect_err("main has no landed gate");
        assert!(error.message.contains("--abandon"));
        // Usage text is where the flags are documented deliberately.
        assert_eq!(
            error.hint,
            "cowshed rm <ws> [--force] [--restore] [--abandon]"
        );
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
        assert!(parse_args(["fork", "raven"]).is_err());
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

        let new = help::command_named("new").unwrap();
        assert_eq!(
            parse_args(["new", "--help"]).unwrap().command,
            Command::Help(Some(new))
        );
        assert_eq!(
            parse_args(["help", "new"]).unwrap().command,
            Command::Help(Some(new))
        );
        // A half-typed line answers the question rather than refusing the grammar it is missing.
        assert_eq!(
            parse_args(["new", "--slot", "--help"]).unwrap().command,
            Command::Help(Some(new))
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
            (&["new", "raven"], Required),
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
            (&["attach", "raven"], Required),
            (&["detach", "raven"], NotUsed),
            (&["detach", "--all"], NotUsed),
            (&["resize", "raven", "32GiB"], Required),
            (&["gc"], Required),
            (&["push", "raven"], Required),
            (&["rebase", "raven"], Required),
            (&["land", "raven"], Required),
            (&["doctor"], Optional),
            (&["gateway", "status"], NotUsed),
            (&["sccache", "status"], NotUsed),
            (&["skill", "install"], NotUsed),
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
        let error = parse_args(["new", "raven", "--unknown"]).unwrap_err();
        assert_eq!(error.message, "unknown flag `--unknown`");
        assert_eq!(
            error.hint,
            "cowshed new <name> [--ref <rev>] [--from <ws>] [--browse] [--slot <n>] [--register] [--git-worktree]"
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

    #[test]
    fn missing_positionals_use_the_same_words_as_require_helpers() {
        let cases = [
            (["new"].as_slice(), "new requires a workspace name"),
            (&["fork"], "fork requires a source workspace"),
            (&["mv"], "mv requires a workspace"),
            (&["restore"], "restore requires a workspace"),
            (&["exec", "--", "true"], "exec requires a workspace"),
            (&["rm"], "rm requires a workspace"),
            (&["detach"], "detach requires a workspace"),
            (&["resize"], "resize requires a workspace"),
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
}
