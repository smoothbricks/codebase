use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;

use crate::help::{self, CommandSpec, Opt};

/// Every command, in the order the command map lists them.
///
/// This is the list `cowshed --help` prints and the list an unknown command is corrected against,
/// so a verb the parser dispatches is a verb the help knows about.
pub static COMMANDS: &[&CommandSpec] = &[
    &ADOPT,
    &NEW,
    &FORK,
    &MOVE,
    &CHECKPOINT,
    &RESTORE,
    &ENSURE,
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
    New(NewArgs),
    Fork(ForkArgs),
    Move(MoveArgs),
    Checkpoint(CheckpointArgs),
    Restore(RestoreArgs),
    Ensure(EnsureArgs),
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
    Doctor,
    Gateway(GatewayCommand),
    Sccache(SccacheCommand),
    Skill(SkillArgs),
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
            Self::Adopt(_)
            | Self::New(_)
            | Self::Fork(_)
            | Self::Move(_)
            | Self::Checkpoint(_)
            | Self::Restore(_)
            | Self::Ensure(_)
            | Self::Path(_)
            | Self::Exec(_)
            | Self::Remove(_)
            | Self::Attach(_)
            | Self::Detach(_)
            | Self::Resize(_)
            | Self::Gc(_)
            | Self::Push(_)
            | Self::Rebase(_)
            | Self::Land(_) => ProjectDiscovery::Required,
            Self::List(args) if !args.all => ProjectDiscovery::Optional,
            Self::Doctor => ProjectDiscovery::Optional,
            Self::List(_)
            | Self::Gateway(_)
            | Self::Sccache(_)
            | Self::Skill(_)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayCommand {
    Start,
    Stop,
    Status,
    Run,
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
}

impl std::fmt::Display for MoveDestination {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Workspace(name) => formatter.write_str(name),
            Self::Checkout(path) => write!(formatter, "{}", path.display()),
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EnsureArgs {
    pub envrc: bool,
    pub attach: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ListArgs {
    pub all: bool,
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
    pub workspace: String,
    pub browse: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetachArgs {
    pub workspace: String,
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
    pub fresh: bool,
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
        2
    }

    pub fn command_map(&self) -> Option<&'static str> {
        match self.kind {
            UsageErrorKind::MissingCommand => Some(help::command_map()),
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

#[derive(Clone, Copy)]
enum CommandName {
    Adopt,
    New,
    Fork,
    Move,
    Checkpoint,
    Restore,
    Ensure,
    List,
    Path,
    Exec,
    Remove,
    Attach,
    Detach,
    Resize,
    Gc,
    Push,
    Rebase,
    Land,
    Doctor,
    Gateway,
    Sccache,
    Skill,
}

impl CommandName {
    /// The dispatched verb's entry in the table, which is where its help and its grammar live.
    fn spec(self) -> &'static CommandSpec {
        match self {
            Self::Adopt => &ADOPT,
            Self::New => &NEW,
            Self::Fork => &FORK,
            Self::Move => &MOVE,
            Self::Checkpoint => &CHECKPOINT,
            Self::Restore => &RESTORE,
            Self::Ensure => &ENSURE,
            Self::List => &LIST,
            Self::Path => &PATH,
            Self::Exec => &EXEC,
            Self::Remove => &REMOVE,
            Self::Attach => &ATTACH,
            Self::Detach => &DETACH,
            Self::Resize => &RESIZE,
            Self::Gc => &GC,
            Self::Push => &PUSH,
            Self::Rebase => &REBASE,
            Self::Land => &LAND,
            Self::Doctor => &DOCTOR,
            Self::Gateway => &GATEWAY,
            Self::Sccache => &SCCACHE,
            Self::Skill => &SKILL,
        }
    }
}

pub fn parse_args<I, T>(args: I) -> Result<Cli, UsageError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let mut args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let mut global = GlobalOptions::default();
    let mut index = 0;
    while index < args.len() && parse_global(&args, &mut index, &mut global)? {}

    let command = match args.get(index).and_then(|arg| arg.to_str()) {
        Some("adopt") => CommandName::Adopt,
        Some("new") => CommandName::New,
        Some("fork") => CommandName::Fork,
        Some("mv") => CommandName::Move,
        Some("checkpoint") => CommandName::Checkpoint,
        Some("restore") => CommandName::Restore,
        Some("ensure") => CommandName::Ensure,
        Some("ls") => CommandName::List,
        Some("path") => CommandName::Path,
        Some("exec") => CommandName::Exec,
        Some("rm") => CommandName::Remove,
        Some("attach") => CommandName::Attach,
        Some("detach") => CommandName::Detach,
        Some("resize") => CommandName::Resize,
        Some("gc") => CommandName::Gc,
        Some("push") => CommandName::Push,
        Some("rebase") => CommandName::Rebase,
        Some("land") => CommandName::Land,
        Some("doctor") => CommandName::Doctor,
        Some("gateway") => CommandName::Gateway,
        Some("sccache") => CommandName::Sccache,
        Some("skill") => CommandName::Skill,
        Some("--help" | "-h" | "help") => {
            let command = parse_help(&args, index + 1)?;
            return Ok(Cli { global, command });
        }
        Some(other) => return Err(unknown_command(other)),
        None => return Err(UsageError::missing_command()),
    };
    index += 1;

    // `--help` among a verb's own arguments answers the question the command line was about to get
    // wrong, rather than refusing the half-typed grammar it appears in. The scan stops at `--`,
    // where cowshed's arguments end and the child's begin.
    if wants_help(&args[index..]) {
        return Ok(Cli {
            global,
            command: Command::Help(Some(command.spec())),
        });
    }

    let command = match command {
        CommandName::Adopt => parse_adopt(&args, index, &mut global)?,
        CommandName::New => parse_new(&args, index, &mut global)?,
        CommandName::Fork => parse_fork(&args, index, &mut global)?,
        CommandName::Move => parse_move(&args, index, &mut global)?,
        CommandName::Checkpoint => parse_checkpoint(&args, index, &mut global)?,
        CommandName::Restore => parse_restore(&args, index, &mut global)?,
        CommandName::Ensure => parse_ensure(&args, index, &mut global)?,
        CommandName::List => parse_list(&args, index, &mut global)?,
        CommandName::Path => parse_path(&args, index, &mut global)?,
        CommandName::Exec => parse_exec(&mut args, index, &mut global)?,
        CommandName::Remove => parse_remove(&args, index, &mut global)?,
        CommandName::Attach => parse_attach(&args, index, &mut global)?,
        CommandName::Detach => parse_detach(&args, index, &mut global)?,
        CommandName::Resize => parse_resize(&args, index, &mut global)?,
        CommandName::Gc => parse_gc(&args, index, &mut global)?,
        CommandName::Push => parse_push(&args, index, &mut global)?,
        CommandName::Rebase => parse_rebase(&args, index, &mut global)?,
        CommandName::Land => parse_land(&args, index, &mut global)?,
        CommandName::Doctor => parse_empty(&args, index, &mut global, &DOCTOR, Command::Doctor)?,
        CommandName::Gateway => parse_gateway(&args, index, &mut global)?,
        CommandName::Sccache => parse_sccache(&args, index, &mut global)?,
        CommandName::Skill => parse_skill(&args, index, &mut global)?,
    };
    Ok(Cli { global, command })
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

const GATEWAY: CommandSpec = CommandSpec {
    name: "gateway",
    args: "<start|stop|status|run>",
    trailing: "",
    summary: "manage the host gateway",
    about: &[
        "The gateway is the one trusted process outside every sandbox: workspaces reach the network, main's repository, and each other only through its authenticated Unix socket. `start` installs and loads the per-user LaunchAgent and waits until that socket answers; `stop` boots it out; `status` reports health without starting anything. Both mutations are idempotent.",
        "`run` is the LaunchAgent's own foreground entrypoint. It validates already-mounted storage and never provisions any, so a background start can report missing setup but can never raise an authorization prompt.",
    ],
    options: &[],
};

fn parse_gateway(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    let usage: &'static CommandSpec = &GATEWAY;
    let action = match args.get(index).and_then(|argument| argument.to_str()) {
        Some("start") => GatewayCommand::Start,
        Some("stop") => GatewayCommand::Stop,
        Some("status") => GatewayCommand::Status,
        Some("run") => GatewayCommand::Run,
        Some(other) => {
            return Err(UsageError::new(
                format!("unknown gateway action `{other}`"),
                usage,
            ));
        }
        None => return Err(UsageError::new("gateway action is required", usage)),
    };
    index += 1;
    while index < args.len() && parse_global(args, &mut index, global)? {}
    if index != args.len() {
        let argument = args[index].to_string_lossy();
        return Err(UsageError::new(
            format!("unexpected gateway argument `{argument}`"),
            usage,
        ));
    }
    if global.project.is_some() {
        return Err(UsageError::new(
            "--project is not valid for gateway commands",
            usage,
        ));
    }
    Ok(Command::Gateway(action))
}

const SCCACHE: CommandSpec = CommandSpec {
    name: "sccache",
    args: "<start|stop|status>",
    trailing: "",
    summary: "manage the host sccache daemon",
    about: &[
        "Runs the shared compile cache as a supervised LaunchAgent, so its configuration is pinned before any client speaks to it. sccache reads its store path, its cache cap, and its base directories once, at server start, and never again — so the first client to need a server and spawn one implicitly would freeze its own environment into the daemon every later workspace then shares. Starting the daemon deliberately is what keeps the cap and the store where the host meant them.",
        "The gateway daemon starts this agent itself, so a healthy host already has it; these verbs are for repair, inspection, and resizing. `status` reports launchd and socket health without starting anything, and surfaces the daemon's own statistics whenever it answers. Hits are reported per language on purpose: cross-workspace C and C++ reuse needs no build slot, so a healthy aggregate hit rate routinely hides a Rust hit rate of zero.",
    ],
    options: &[Opt {
        spelling: "--capacity <size>",
        meaning: "`start` only: cache cap (100g, 1t). The default is the summed size of every adopted project's main image, floored at 40 GiB, because sccache's own 10 GiB default is smaller than one debug graph and evicts what the next slot tenant came for",
    }],
};

fn parse_sccache(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    let usage: &'static CommandSpec = &SCCACHE;
    let mut action = match args.get(index).and_then(|argument| argument.to_str()) {
        Some("start") => SccacheCommand::Start { capacity: None },
        Some("stop") => SccacheCommand::Stop,
        Some("status") => SccacheCommand::Status,
        Some(other) => {
            return Err(UsageError::new(
                format!("unknown sccache action `{other}`"),
                usage,
            ));
        }
        None => return Err(UsageError::new("sccache action is required", usage)),
    };
    index += 1;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match (args[index].to_str(), &mut action) {
            (Some("--capacity"), SccacheCommand::Start { capacity }) => {
                *capacity = Some(take_value(args, &mut index, "--capacity", usage)?);
            }
            _ => {
                let argument = args[index].to_string_lossy();
                return Err(UsageError::new(
                    format!("unexpected sccache argument `{argument}`"),
                    usage,
                ));
            }
        }
        index += 1;
    }
    if global.project.is_some() {
        return Err(UsageError::new(
            "--project is not valid for sccache commands",
            usage,
        ));
    }
    Ok(Command::Sccache(action))
}

const SKILL: CommandSpec = CommandSpec {
    name: "skill",
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

fn parse_skill(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &SKILL;
    let action = match args.get(index).and_then(|argument| argument.to_str()) {
        Some("install") => SkillCommand::Install,
        Some(other) => {
            return Err(UsageError::new(
                format!("unknown skill action `{other}`"),
                USAGE,
            ));
        }
        None => return Err(UsageError::new("skill action is required", USAGE)),
    };
    index += 1;

    let mut harnesses = Vec::new();
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--harness") => {
                let value = take_value(args, &mut index, "--harness", USAGE)?;
                let name = value
                    .to_str()
                    .ok_or_else(|| {
                        UsageError::new("--harness requires a UTF-8 harness name", USAGE)
                    })?
                    .to_owned();
                if !harnesses.contains(&name) {
                    harnesses.push(name);
                }
            }
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ => {
                let argument = args[index].to_string_lossy();
                return Err(UsageError::new(
                    format!("unexpected skill argument `{argument}`"),
                    USAGE,
                ));
            }
        }
        index += 1;
    }

    // The scope decides which harness names exist, so validation waits until
    // --project has been seen wherever it appears in the argument list.
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

    Ok(Command::Skill(SkillArgs { action, harnesses }))
}

fn parse_global(
    args: &[OsString],
    index: &mut usize,
    global: &mut GlobalOptions,
) -> Result<bool, UsageError> {
    match args[*index].to_str() {
        Some("--json") => global.json = true,
        Some("-q" | "--quiet") => global.quiet = true,
        Some("--project") => {
            *index += 1;
            let value = args.get(*index).ok_or_else(|| {
                UsageError::with_hint(
                    "--project requires a git root",
                    "cowshed --project <git-root> <command>",
                )
            })?;
            global.project = Some(PathBuf::from(value));
        }
        _ => return Ok(false),
    }
    *index += 1;
    Ok(true)
}

const ADOPT: CommandSpec = CommandSpec {
    name: "adopt",
    args: "[path]",
    trailing: "",
    summary: "adopt a checkout",
    about: &[
        "Converts an existing checkout into this repository's image-backed main workspace, at the same path. Run it once per repository; every other verb finds its project from the cwd or `--project`. Adoption is the only operation that copies a source tree into an image, and on macOS the only command allowed to provision storage — so the first adopt on a host may raise one administrator prompt while the cowshed volumes are created, and no later command ever can.",
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

fn parse_adopt(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &ADOPT;
    let mut parsed = AdoptArgs::default();
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--capacity") => {
                parsed.capacity = Some(take_value(args, &mut index, "--capacity", USAGE)?);
            }
            Some("--repo-id") => {
                parsed.repo_id = Some(take_value(args, &mut index, "--repo-id", USAGE)?);
            }
            Some("--quarantine") => parsed.quarantine = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if parsed.path.is_none() => parsed.path = Some(PathBuf::from(&args[index])),
            _ => return Err(UsageError::new("adopt accepts at most one path", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Adopt(parsed))
}

const NEW: CommandSpec = CommandSpec {
    name: "new",
    args: "<name>",
    trailing: "",
    summary: "create a workspace",
    about: &[
        "Clones a live image of the project's main workspace and mounts it. The clone is copy-on-write, so a workspace costs the writes it makes rather than a copy of the tree, and it inherits main's source, dependencies, and build state warm.",
        "A build slot is a stable mount path — `mnt/<owner>/<repo>/slot@<n>` — held by one workspace at a time and released when that workspace is removed or renamed, so the next tenant of slot n builds through byte-identical absolute paths. That path identity is the whole feature: cargo derives `-C metadata` from a package id carrying the absolute manifest directory, and sccache hashes the compiler's physical working directory, so the same sources built at two paths are two different compilations that share no compile cache. A slot tenant is therefore also given `RUSTC_WRAPPER=sccache` and `CARGO_INCREMENTAL=0`, trading local incrementality for a cache its successors can hit; main cannot take a slot, because its mount is fixed by the checkout layout.",
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

fn parse_new(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &NEW;
    let mut name = None;
    let mut reference = None;
    let mut from = None;
    let mut browse = false;
    let mut slot = None;
    let mut register = false;
    let mut git_worktree = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--ref") => reference = Some(take_value(args, &mut index, "--ref", USAGE)?),
            Some("--from") => {
                let value = take_value(args, &mut index, "--from", USAGE)?;
                from = Some(workspace_name(&value, false, USAGE)?);
            }
            Some("--browse") => browse = true,
            Some("--register") => register = true,
            Some("--git-worktree") => git_worktree = true,
            Some("--slot") => slot = Some(parse_slot(args, &mut index, USAGE)?),
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if name.is_none() => name = Some(workspace_name(&args[index], true, USAGE)?),
            _ => {
                return Err(UsageError::new(
                    "new accepts exactly one workspace name",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    if reference.is_some() && from.is_some() {
        return Err(UsageError::new("--ref conflicts with --from", USAGE));
    }
    Ok(Command::New(NewArgs {
        name: name.ok_or_else(|| UsageError::new("new requires a workspace name", USAGE))?,
        reference,
        from,
        browse,
        slot,
        register,
        git_worktree,
    }))
}

const FORK: CommandSpec = CommandSpec {
    name: "fork",
    args: "<src> <dst>",
    trailing: "",
    summary: "fork a workspace",
    about: &[
        "Clones a running workspace: two divergent futures from the same mid-flight state, in milliseconds. Grants are not inherited — a fork starts closed, like any new workspace.",
    ],
    options: &[],
};

fn parse_fork(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &FORK;
    let mut source = None;
    let mut destination = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if source.is_none() => {
                source = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ if destination.is_none() => {
                destination = Some(workspace_name(&args[index], true, USAGE)?);
            }
            _ => {
                return Err(UsageError::new(
                    "fork accepts exactly two workspaces",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Fork(ForkArgs {
        source: source.ok_or_else(|| UsageError::new("fork requires a source workspace", USAGE))?,
        destination: destination
            .ok_or_else(|| UsageError::new("fork requires a destination workspace", USAGE))?,
    }))
}

const MOVE: CommandSpec = CommandSpec {
    name: "mv",
    args: "<ws> <new-name> | main <new-checkout-path>",
    trailing: "",
    summary: "rename a workspace or move the checkout",
    about: &[
        "The source decides what the destination means. `mv main <path>` moves the adopted checkout to an absolute path and keeps every record of where it lives in step; every other source renames a workspace, whose new name is subject to the ordinary name grammar and cannot be `main`.",
    ],
    options: &[],
};

fn parse_move(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &MOVE;
    let mut source: Option<String> = None;
    let mut destination: Option<OsString> = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            // `main` is accepted here and means the checkout move; every other source is a
            // workspace the coordinator renames.
            _ if source.is_none() => {
                source = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ if destination.is_none() => {
                destination = Some(args[index].clone());
            }
            _ => {
                return Err(UsageError::new("mv accepts exactly two arguments", USAGE));
            }
        }
        index += 1;
    }
    let source = source.ok_or_else(|| UsageError::new("mv requires a workspace", USAGE))?;
    let destination =
        destination.ok_or_else(|| UsageError::new("mv requires a destination", USAGE))?;
    let destination = if source == "main" {
        MoveDestination::Checkout(checkout_destination(&destination, USAGE)?)
    } else {
        MoveDestination::Workspace(workspace_name(&destination, true, USAGE)?)
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
    args: "[<ws>] [label]",
    trailing: "",
    summary: "create a checkpoint",
    about: &[
        "Clonefiles the workspace image under a label — generated from the UTC timestamp when you do not give one — after a supervisor barrier seals complete job output, so the snapshot is crash-consistent rather than merely recent. Omit the workspace to checkpoint the one you are standing in.",
    ],
    options: &[Opt {
        spelling: "--keep",
        meaning: "pin the checkpoint so expiry pruning never reclaims it; an explicit label pins it too",
    }],
};

fn parse_checkpoint(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &CHECKPOINT;
    let mut workspace = None;
    let mut label = None;
    let mut keep = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--keep") => keep = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ if label.is_none() => label = Some(args[index].clone()),
            _ => {
                return Err(UsageError::new(
                    "checkpoint accepts one workspace and at most one label",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Checkpoint(CheckpointArgs {
        workspace,
        label,
        keep,
    }))
}

const RESTORE: CommandSpec = CommandSpec {
    name: "restore",
    args: "<ws> <label>",
    trailing: "",
    summary: "restore a checkpoint",
    about: &[
        "Swaps the workspace's image for the checkpoint and mints a new workspace incarnation. The displaced image is kept as a `pre-restore-<timestamp>` checkpoint, so a restore is itself undoable; a restore over unsaved work is refused.",
    ],
    options: &[],
};

fn parse_restore(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &RESTORE;
    let mut workspace = None;
    let mut label = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ if label.is_none() => label = Some(args[index].clone()),
            _ => {
                return Err(UsageError::new(
                    "restore accepts exactly one workspace and one label",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Restore(RestoreArgs {
        workspace: workspace
            .ok_or_else(|| UsageError::new("restore requires a workspace", USAGE))?,
        label: label.ok_or_else(|| UsageError::new("restore requires a label", USAGE))?,
    }))
}

const ENSURE: CommandSpec = CommandSpec {
    name: "ensure",
    args: "",
    trailing: "",
    summary: "heal the current workspace",
    about: &[
        "The fast auto-fix, and the one command safe to run on every prompt: a healthy workspace costs a marker read and a statfs, and says nothing. Otherwise it reattaches images after a reboot or a Finder eject, repairs mount flags, re-arms the autosave agent, and reconciles whatever drifted — synchronously, so when it returns you are standing in a valid workspace.",
    ],
    options: &[
        Opt {
            spelling: "--envrc",
            meaning: "also print the POSIX shell exports for the current workspace, for `eval` or an .envrc",
        },
        Opt {
            spelling: "--attach",
            meaning: "the explicit remount spelling, for devenv-native repositories",
        },
    ],
};

fn parse_ensure(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &ENSURE;
    let mut parsed = EnsureArgs::default();
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--envrc") => parsed.envrc = true,
            Some("--attach") => parsed.attach = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ => {
                return Err(UsageError::new(
                    "ensure accepts no positional arguments",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Ensure(parsed))
}

const LIST: CommandSpec = CommandSpec {
    name: "ls",
    args: "",
    trailing: "",
    summary: "list workspaces",
    about: &[
        "One line per workspace of the project selected by the cwd or `--project`: name, state, branch, and mountpoint (empty when detached).",
    ],
    options: &[Opt {
        spelling: "--all",
        meaning: "every adopted project on the host, with its repository id as the first column",
    }],
};

fn parse_list(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &LIST;
    let mut parsed = ListArgs::default();
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--all") => parsed.all = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ => {
                return Err(UsageError::new("ls accepts no positional arguments", USAGE));
            }
        }
        index += 1;
    }
    Ok(Command::List(parsed))
}

/// `--slot <n>` in every verb that takes one.
fn parse_slot(
    args: &[OsString],
    index: &mut usize,
    usage: &'static CommandSpec,
) -> Result<u32, UsageError> {
    let value = take_value(args, index, "--slot", usage)?;
    value
        .to_str()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| UsageError::new("--slot must be an unsigned integer", usage))
}

const PATH: CommandSpec = CommandSpec {
    name: "path",
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
            meaning: "skip the healing and print the would-be path of a detached workspace",
        },
    ],
};

fn parse_path(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &PATH;
    let mut workspace = None;
    let mut slot = None;
    let mut no_attach = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--no-attach") => no_attach = true,
            Some("--slot") => slot = Some(parse_slot(args, &mut index, USAGE)?),
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?)
            }
            _ => return Err(UsageError::new("path accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    if workspace.is_some() && slot.is_some() {
        return Err(UsageError::new(
            "path takes a workspace or --slot, not both",
            USAGE,
        ));
    }
    Ok(Command::Path(PathArgs {
        workspace,
        slot,
        no_attach,
    }))
}

const EXEC: CommandSpec = CommandSpec {
    name: "exec",
    args: "<ws>",
    trailing: "-- <cmd...>",
    summary: "run an argv command",
    about: &[
        "Runs one argv — never a shell string — inside the workspace's sandbox, with the cwd at the workspace root. Child stdout and stderr pass through as opaque bytes and the child's exit code passes through untouched; only a denial cowshed has authoritative evidence for is reported as one.",
        "Long commands auto-background at the soft timeout and keep running under the workspace supervisor, where `cowshed job` reaches them.",
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

fn parse_exec(
    args: &mut Vec<OsString>,
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &EXEC;
    let mut workspace = None;
    let mut stdin = None;
    let mut read_only = false;
    let mut cwd = None;
    let mut session = None;
    let mut timeout = None;
    let mut background = false;
    let mut stdout_copy = None;
    let mut stderr_copy = None;
    let mut replace_output = false;

    while index < args.len() {
        if args[index] == OsStr::new("--") {
            index += 1;
            break;
        }
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--stdin") => set_stdin(&mut stdin, StdinSource::Stream, USAGE)?,
            Some("--stdin-file") => {
                let value = take_value(args, &mut index, "--stdin-file", USAGE)?;
                set_stdin(
                    &mut stdin,
                    StdinSource::WorkspaceFile(PathBuf::from(value)),
                    USAGE,
                )?;
            }
            Some("--stdin-base64") => {
                let value = take_value(args, &mut index, "--stdin-base64", USAGE)?;
                set_stdin(&mut stdin, StdinSource::InlineBase64(value), USAGE)?;
            }
            Some("--ro") => read_only = true,
            Some("--cwd") => {
                cwd = Some(PathBuf::from(take_value(args, &mut index, "--cwd", USAGE)?))
            }
            Some("--session") => {
                let value = take_value(args, &mut index, "--session", USAGE)?;
                session = Some(workspace_name(&value, false, USAGE)?);
            }
            Some("--timeout") => timeout = Some(take_value(args, &mut index, "--timeout", USAGE)?),
            Some("--background") => background = true,
            Some("--stdout-copy") => {
                let value = PathBuf::from(take_value(args, &mut index, "--stdout-copy", USAGE)?);
                set_output_copy(&mut stdout_copy, value, "--stdout-copy", USAGE)?;
            }
            Some("--stderr-copy") => {
                let value = PathBuf::from(take_value(args, &mut index, "--stderr-copy", USAGE)?);
                set_output_copy(&mut stderr_copy, value, "--stderr-copy", USAGE)?;
            }
            Some("--replace-output") => replace_output = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?)
            }
            _ => {
                return Err(UsageError::new(
                    "exec requires `--` before the child argv",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    let workspace = workspace.ok_or_else(|| UsageError::new("exec requires a workspace", USAGE))?;
    if index == 0 || args.get(index.wrapping_sub(1)) != Some(&OsString::from("--")) {
        return Err(UsageError::new(
            "exec requires `--` before the child argv",
            USAGE,
        ));
    }
    if replace_output && stdout_copy.is_none() && stderr_copy.is_none() {
        return Err(UsageError::new(
            "--replace-output requires --stdout-copy or --stderr-copy",
            USAGE,
        ));
    }
    args.drain(..index);
    let argv = std::mem::take(args);
    if argv.is_empty() {
        return Err(UsageError::new(
            "exec requires a child command after `--`",
            USAGE,
        ));
    }
    Ok(Command::Exec(ExecArgs {
        workspace,
        argv,
        stdin,
        read_only,
        cwd,
        session,
        timeout,
        background,
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
    args: "<ws>",
    trailing: "",
    summary: "retire a workspace",
    about: &[
        "Retires one workspace, deleting the image its commits live in. The gate is therefore ancestry, not preservation: `rm` refuses unless the project's main branch already contains the workspace's HEAD, read out of main's own repository. The workspace is marked deleted immediately; detach and image deletion finish in the background.",
        "The two overrides authorize different losses and neither substitutes for the other, so a script carrying one has not acquired the other.",
    ],
    options: &[
        Opt {
            spelling: "--force",
            meaning: "waive transient state only — a dirty tree, an in-progress merge, a busy mount; it does not reach the landed-ancestry gate",
        },
        Opt {
            spelling: "--restore",
            meaning: "main only: put the pre-adoption checkout back and unbind the project, the reverse of adopt",
        },
        Opt {
            spelling: "--abandon",
            meaning: "the sole authorization for destroying commits main does not contain; before deleting, main..HEAD is bundled into sessions/.trash/<ws>-<tip>.bundle and the abandonment reported, so it stays recoverable by fetching that bundle",
        },
    ],
};

fn parse_remove(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REMOVE;
    let mut workspace = None;
    let mut force = false;
    let mut restore = false;
    let mut abandon = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--force") => force = true,
            Some("--restore") => restore = true,
            Some("--abandon") => abandon = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?)
            }
            _ => return Err(UsageError::new("rm accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    let workspace = workspace.ok_or_else(|| UsageError::new("rm requires a workspace", USAGE))?;
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
        force,
        restore,
        abandon,
    }))
}

const ATTACH: CommandSpec = CommandSpec {
    name: "attach",
    args: "<ws>",
    trailing: "",
    summary: "attach a workspace",
    about: &[
        "Mounts a detached workspace again. A detached workspace costs one closed file, so detaching is how a workspace waits without being deleted.",
    ],
    options: &[Opt {
        spelling: "--browse",
        meaning: "show the volume in Finder; the default mount is nobrowse",
    }],
};

fn parse_attach(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &ATTACH;
    let mut workspace = None;
    let mut browse = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--browse") => browse = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?)
            }
            _ => {
                return Err(UsageError::new(
                    "attach accepts exactly one workspace",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Attach(AttachArgs {
        workspace: workspace
            .ok_or_else(|| UsageError::new("attach requires a workspace", USAGE))?,
        browse,
    }))
}

const DETACH: CommandSpec = CommandSpec {
    name: "detach",
    args: "<ws>",
    trailing: "",
    summary: "detach a workspace",
    about: &[
        "Unmounts the workspace and stops its supervisor without destroying anything. `attach`, `path`, and `ensure` bring it back.",
    ],
    options: &[],
};

fn parse_detach(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &DETACH;
    let mut workspace = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?)
            }
            _ => {
                return Err(UsageError::new(
                    "detach accepts exactly one workspace",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Detach(DetachArgs {
        workspace: workspace
            .ok_or_else(|| UsageError::new("detach requires a workspace", USAGE))?,
    }))
}

const RESIZE: CommandSpec = CommandSpec {
    name: "resize",
    args: "<ws|main> <size>",
    trailing: "",
    summary: "grow a workspace image",
    about: &[
        "Grows one workspace's image. Sizes are binary units — 100g, 200g, 1t — at least a mebibyte and a whole number of the 4 KiB blocks the image tools resize in. The supervisor is stopped for the resize and restarted after, because the image has to leave the kernel.",
    ],
    options: &[],
};

fn parse_resize(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &RESIZE;
    let mut workspace = None;
    let mut capacity: Option<OsString> = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            // `main` resizes too, so the name is not reserved here the way it is for verbs that
            // would create or replace a workspace.
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ if capacity.is_none() => capacity = Some(args[index].clone()),
            _ => {
                return Err(UsageError::new(
                    "resize accepts exactly one workspace and one size",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Resize(ResizeArgs {
        workspace: workspace
            .ok_or_else(|| UsageError::new("resize requires a workspace", USAGE))?,
        capacity: capacity.ok_or_else(|| UsageError::new("resize requires a size", USAGE))?,
    }))
}

const GC: CommandSpec = CommandSpec {
    name: "gc",
    args: "",
    trailing: "",
    summary: "reclaim storage",
    about: &[
        "Deletes orphaned images and stale mountpoint directories, prunes expired checkpoints, compacts detached images, and reports what it reclaimed. Safe at any time; other commands run it opportunistically.",
    ],
    options: &[Opt {
        spelling: "--dry-run",
        meaning: "report what would be reclaimed without deleting anything",
    }],
};

fn parse_gc(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &GC;
    let mut parsed = GcArgs::default();
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--dry-run") => parsed.dry_run = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ => return Err(UsageError::new("gc accepts no positional arguments", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Gc(parsed))
}

const PUSH: CommandSpec = CommandSpec {
    name: "push",
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
        Opt {
            spelling: "--expected-workspace-incarnation <id>",
            meaning: "refuse unless the workspace is still this incarnation",
        },
        Opt {
            spelling: "--expected-source-head <oid>",
            meaning: "refuse unless the workspace HEAD is still this commit",
        },
        Opt {
            spelling: "--expected-destination-head <oid|missing>",
            meaning: "refuse unless the destination branch is still this commit, or `missing` for one that must not exist yet",
        },
    ],
};

fn parse_push(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &PUSH;
    let mut workspace = None;
    let mut branch = None;
    let mut expected_workspace_incarnation = None;
    let mut expected_source_head = None;
    let mut expected_destination_head = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--branch") => branch = Some(take_value(args, &mut index, "--branch", USAGE)?),
            Some("--expected-workspace-incarnation") => {
                expected_workspace_incarnation = Some(take_value(
                    args,
                    &mut index,
                    "--expected-workspace-incarnation",
                    USAGE,
                )?);
            }
            Some("--expected-source-head") => {
                expected_source_head = Some(take_value(
                    args,
                    &mut index,
                    "--expected-source-head",
                    USAGE,
                )?);
            }
            Some("--expected-destination-head") => {
                expected_destination_head = Some(take_value(
                    args,
                    &mut index,
                    "--expected-destination-head",
                    USAGE,
                )?);
            }
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ => return Err(UsageError::new("push accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Push(PushArgs {
        workspace,
        branch,
        expected_workspace_incarnation,
        expected_source_head,
        expected_destination_head,
    }))
}

const REBASE: CommandSpec = CommandSpec {
    name: "rebase",
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
        Opt {
            spelling: "--fresh",
            meaning: "shed accumulated image divergence: replay the branch onto a brand-new clone of current main and transplant the workspace's identity onto it; refused on a dirty tree",
        },
        Opt {
            spelling: "--expected-workspace-incarnation <id>",
            meaning: "refuse unless the workspace is still this incarnation",
        },
        Opt {
            spelling: "--expected-source-head <oid>",
            meaning: "refuse unless the workspace HEAD is still this commit",
        },
        Opt {
            spelling: "--expected-onto-head <oid>",
            meaning: "refuse unless the revision being rebased onto is still this commit",
        },
    ],
};

fn parse_rebase(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &REBASE;
    let mut workspace = None;
    let mut onto = None;
    let mut fresh = false;
    let mut expected_workspace_incarnation = None;
    let mut expected_source_head = None;
    let mut expected_onto_head = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--onto") => onto = Some(take_value(args, &mut index, "--onto", USAGE)?),
            Some("--fresh") => fresh = true,
            Some("--expected-workspace-incarnation") => {
                expected_workspace_incarnation = Some(take_value(
                    args,
                    &mut index,
                    "--expected-workspace-incarnation",
                    USAGE,
                )?);
            }
            Some("--expected-source-head") => {
                expected_source_head = Some(take_value(
                    args,
                    &mut index,
                    "--expected-source-head",
                    USAGE,
                )?);
            }
            Some("--expected-onto-head") => {
                expected_onto_head =
                    Some(take_value(args, &mut index, "--expected-onto-head", USAGE)?);
            }
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ => {
                return Err(UsageError::new(
                    "rebase accepts exactly one workspace",
                    USAGE,
                ));
            }
        }
        index += 1;
    }
    Ok(Command::Rebase(RebaseArgs {
        workspace,
        onto,
        fresh,
        expected_workspace_incarnation,
        expected_source_head,
        expected_onto_head,
    }))
}

const LAND: CommandSpec = CommandSpec {
    name: "land",
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
        Opt {
            spelling: "--expected-workspace-incarnation <id>",
            meaning: "refuse unless the workspace is still this incarnation",
        },
        Opt {
            spelling: "--expected-source-head <oid>",
            meaning: "refuse unless the workspace HEAD is still this commit",
        },
        Opt {
            spelling: "--expected-target-head <oid|missing>",
            meaning: "refuse unless the target branch is still this commit, or `missing` for one that must not exist yet",
        },
    ],
};

fn parse_land(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &CommandSpec = &LAND;
    let mut workspace = None;
    let mut target = None;
    let mut checks = Vec::new();
    let mut retire = true;
    let mut push_only = false;
    let mut expected_workspace_incarnation = None;
    let mut expected_source_head = None;
    let mut expected_target_head = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--target") => target = Some(take_value(args, &mut index, "--target", USAGE)?),
            Some("--check") => checks.push(take_value(args, &mut index, "--check", USAGE)?),
            Some("--no-retire") => retire = false,
            Some("--push-only") => push_only = true,
            Some("--expected-workspace-incarnation") => {
                expected_workspace_incarnation = Some(take_value(
                    args,
                    &mut index,
                    "--expected-workspace-incarnation",
                    USAGE,
                )?);
            }
            Some("--expected-source-head") => {
                expected_source_head = Some(take_value(
                    args,
                    &mut index,
                    "--expected-source-head",
                    USAGE,
                )?);
            }
            Some("--expected-target-head") => {
                expected_target_head = Some(take_value(
                    args,
                    &mut index,
                    "--expected-target-head",
                    USAGE,
                )?);
            }
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?);
            }
            _ => return Err(UsageError::new("land accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Land(LandArgs {
        workspace: workspace.ok_or_else(|| UsageError::new("land requires a workspace", USAGE))?,
        target,
        checks,
        retire,
        push_only,
        expected_workspace_incarnation,
        expected_source_head,
        expected_target_head,
    }))
}

const DOCTOR: CommandSpec = CommandSpec {
    name: "doctor",
    args: "",
    trailing: "",
    summary: "check invariants",
    about: &[
        "Checks the invariants a healthy host holds: every image has a marker, every mount matches an image, grants files parse, the caches volume and the gateway answer, autosave is fresh. Exit 0 when healthy, otherwise the code of the most severe finding.",
    ],
    options: &[],
};

fn parse_empty(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
    spec: &'static CommandSpec,
    parsed: Command,
) -> Result<Command, UsageError> {
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        return Err(UsageError::new(
            format!("{} accepts no arguments", spec.name),
            spec,
        ));
    }
    Ok(parsed)
}

fn take_value(
    args: &[OsString],
    index: &mut usize,
    option: &str,
    usage: &'static CommandSpec,
) -> Result<OsString, UsageError> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| UsageError::new(format!("{option} requires a value"), usage))
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
fn set_output_copy(
    target: &mut Option<PathBuf>,
    value: PathBuf,
    option: &str,
    usage: &'static CommandSpec,
) -> Result<(), UsageError> {
    if target.replace(value).is_some() {
        return Err(UsageError::new(
            format!("{option} may only be specified once"),
            usage,
        ));
    }
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
    let valid = !value.is_empty()
        && value.len() <= 64
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid || (reserve_main && value == "main") {
        return Err(UsageError::new(
            if reserve_main && value == "main" {
                "workspace name `main` is reserved"
            } else {
                "workspace names must match [a-z0-9][a-z0-9-]{0,63}"
            },
            usage,
        ));
    }
    Ok(value.to_owned())
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

        let cli = parse_args(["sccache", "start"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Sccache(SccacheCommand::Start { capacity: None })
        );

        let cli = parse_args(["sccache", "status", "--json"]).unwrap();
        assert_eq!(cli.command, Command::Sccache(SccacheCommand::Status));
        assert!(cli.global.json);

        assert!(parse_args(["sccache", "stop", "--capacity", "80g"]).is_err());
        assert!(parse_args(["sccache", "start", "--capacity"]).is_err());
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
        for verb in ["rm", "land", "restore", "mv", "detach", "attach", "exec"] {
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
            (&["new", "raven"], Required),
            (&["fork", "raven", "falcon"], Required),
            (&["mv", "raven", "falcon"], Required),
            (&["checkpoint", "raven"], Required),
            (&["restore", "raven", "saved"], Required),
            (&["ensure"], Required),
            (&["ls"], Optional),
            (&["ls", "--all"], NotUsed),
            (&["path", "raven"], Required),
            (&["exec", "raven", "--", "true"], Required),
            (&["rm", "raven"], Required),
            (&["attach", "raven"], Required),
            (&["detach", "raven"], Required),
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
            assert_eq!(
                cli.command.project_discovery(),
                *expected,
                "{arguments:?}"
            );
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
}
