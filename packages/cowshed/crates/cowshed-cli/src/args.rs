use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;

pub const COMMAND_MAP: &str = "commands:\n  adopt [path]       adopt a checkout\n  new <name>         create a workspace\n  fork <src> <dst>   fork a workspace\n  checkpoint <ws>    create a checkpoint\n  restore <ws> <id>  restore a checkpoint\n  ensure             heal the current workspace\n  ls                 list workspaces\n  path <ws>          print a workspace mount\n  exec <ws> -- <cmd> run an argv command\n  rm <ws>            remove a workspace\n  attach <ws>        attach a workspace\n  detach <ws>        detach a workspace\n  gc                 reclaim storage\n  push <ws>          preserve a workspace ref\n  rebase <ws>        rebase a workspace\n  land <ws>          land a workspace\n  doctor             check invariants\n  gateway <action>   manage the host gateway\n  skill install      install the agent skill";

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
    List,
    Path(PathArgs),
    Exec(ExecArgs),
    Remove(RemoveArgs),
    Attach(AttachArgs),
    Detach(DetachArgs),
    Gc(GcArgs),
    Push(PushArgs),
    Rebase(RebaseArgs),
    Land(LandArgs),
    Doctor,
    Gateway(GatewayCommand),
    Skill(SkillArgs),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathArgs {
    pub workspace: Option<String>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveArgs {
    pub workspace: String,
    pub force: bool,
    pub restore: bool,
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
    fn new(message: impl Into<String>, usage: &'static str) -> Self {
        Self {
            kind: UsageErrorKind::InvalidArguments,
            message: message.into(),
            hint: format!("cowshed {usage}"),
        }
    }

    fn missing_command() -> Self {
        Self {
            kind: UsageErrorKind::MissingCommand,
            message: "a command is required".to_owned(),
            hint: "choose a command from the command map".to_owned(),
        }
    }

    pub const fn exit_code(&self) -> i32 {
        2
    }

    pub const fn command_map(&self) -> Option<&'static str> {
        match self.kind {
            UsageErrorKind::MissingCommand => Some(COMMAND_MAP),
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
    Gc,
    Push,
    Rebase,
    Land,
    Doctor,
    Gateway,
    Skill,
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
        Some("gc") => CommandName::Gc,
        Some("push") => CommandName::Push,
        Some("rebase") => CommandName::Rebase,
        Some("land") => CommandName::Land,
        Some("doctor") => CommandName::Doctor,
        Some("gateway") => CommandName::Gateway,
        Some("skill") => CommandName::Skill,
        Some(other) => {
            return Err(UsageError::new(
                format!("unknown command `{other}`"),
                "<command>",
            ));
        }
        None => return Err(UsageError::missing_command()),
    };
    index += 1;

    let command = match command {
        CommandName::Adopt => parse_adopt(&args, index, &mut global)?,
        CommandName::New => parse_new(&args, index, &mut global)?,
        CommandName::Fork => parse_fork(&args, index, &mut global)?,
        CommandName::Move => parse_move(&args, index, &mut global)?,
        CommandName::Checkpoint => parse_checkpoint(&args, index, &mut global)?,
        CommandName::Restore => parse_restore(&args, index, &mut global)?,
        CommandName::Ensure => parse_ensure(&args, index, &mut global)?,
        CommandName::List => parse_empty(&args, index, &mut global, "ls", Command::List)?,
        CommandName::Path => parse_path(&args, index, &mut global)?,
        CommandName::Exec => parse_exec(&mut args, index, &mut global)?,
        CommandName::Remove => parse_remove(&args, index, &mut global)?,
        CommandName::Attach => parse_attach(&args, index, &mut global)?,
        CommandName::Detach => parse_detach(&args, index, &mut global)?,
        CommandName::Gc => parse_gc(&args, index, &mut global)?,
        CommandName::Push => parse_push(&args, index, &mut global)?,
        CommandName::Rebase => parse_rebase(&args, index, &mut global)?,
        CommandName::Land => parse_land(&args, index, &mut global)?,
        CommandName::Doctor => parse_empty(&args, index, &mut global, "doctor", Command::Doctor)?,
        CommandName::Gateway => parse_gateway(&args, index, &mut global)?,
        CommandName::Skill => parse_skill(&args, index, &mut global)?,
    };
    Ok(Cli { global, command })
}

fn parse_gateway(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    let usage = "gateway <start|stop|status|run>";
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

fn parse_skill(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "skill install [--harness <name>] [--project <path>]";
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
                UsageError::new(
                    "--project requires a git root",
                    "--project <git-root> <command>",
                )
            })?;
            global.project = Some(PathBuf::from(value));
        }
        _ => return Ok(false),
    }
    *index += 1;
    Ok(true)
}

fn parse_adopt(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "adopt [path] [--capacity <size>] [--repo-id <owner/repo>] [--quarantine]";
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

fn parse_new(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "new <name> [--ref <rev> | --from <ws>] [--browse] [--slot <n>] [--register] [--git-worktree]";
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
            Some("--slot") => {
                let value = take_value(args, &mut index, "--slot", USAGE)?;
                let text = value
                    .to_str()
                    .ok_or_else(|| UsageError::new("--slot must be an unsigned integer", USAGE))?;
                slot =
                    Some(text.parse().map_err(|_| {
                        UsageError::new("--slot must be an unsigned integer", USAGE)
                    })?);
            }
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

fn parse_fork(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "fork <src> <dst>";
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

fn parse_move(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "mv <ws> <new-name> | mv main <new-checkout-path>";
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
fn checkout_destination(value: &OsStr, usage: &'static str) -> Result<PathBuf, UsageError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(UsageError::new(
            "the checkout destination must be an absolute path",
            usage,
        ));
    }
    Ok(path)
}

fn parse_checkpoint(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "checkpoint <ws> [label] [--keep]";
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

fn parse_restore(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "restore <ws> <label>";
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

fn parse_ensure(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "ensure [--envrc] [--attach]";
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

fn parse_path(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "path <ws> [--no-attach]";
    let mut workspace = None;
    let mut no_attach = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--no-attach") => no_attach = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => {
                workspace = Some(workspace_name(&args[index], false, USAGE)?)
            }
            _ => return Err(UsageError::new("path accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Path(PathArgs {
        workspace,
        no_attach,
    }))
}

fn parse_exec(
    args: &mut Vec<OsString>,
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "exec <ws> [--stdin | --stdin-file <rel> | --stdin-base64 <data>] [--ro] [--cwd <rel>] [--session <name>] [--timeout <dur>] [--background] [--stdout-copy <rel>] [--stderr-copy <rel>] [--replace-output] -- <cmd...>";
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

fn parse_remove(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "rm <ws> [--force] [--restore]";
    let mut workspace = None;
    let mut force = false;
    let mut restore = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--force") => force = true,
            Some("--restore") => restore = true,
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
    Ok(Command::Remove(RemoveArgs {
        workspace,
        force,
        restore,
    }))
}

fn parse_attach(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "attach <ws> [--browse]";
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

fn parse_detach(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "detach <ws>";
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

fn parse_gc(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "gc [--dry-run]";
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

fn parse_push(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "push <ws> [--branch <name>] [--expected-workspace-incarnation <id>] [--expected-source-head <oid>] [--expected-destination-head <oid|missing>]";
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

fn parse_rebase(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "rebase <ws> [--onto <rev>] [--fresh] [--expected-workspace-incarnation <id>] [--expected-source-head <oid>] [--expected-onto-head <oid>]";
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

fn parse_land(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "land <ws> [--target <branch>] [--check <cmd>] [--no-retire] [--push-only] [--expected-workspace-incarnation <id>] [--expected-source-head <oid>] [--expected-target-head <oid|missing>]";
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

fn parse_empty(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
    command: &'static str,
    parsed: Command,
) -> Result<Command, UsageError> {
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        return Err(UsageError::new(
            format!("{command} accepts no arguments"),
            command,
        ));
    }
    Ok(parsed)
}

fn take_value(
    args: &[OsString],
    index: &mut usize,
    option: &str,
    usage: &'static str,
) -> Result<OsString, UsageError> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| UsageError::new(format!("{option} requires a value"), usage))
}

fn set_stdin(
    target: &mut Option<StdinSource>,
    value: StdinSource,
    usage: &'static str,
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
    usage: &'static str,
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
    usage: &'static str,
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

fn unknown_flag(flag: &str, usage: &'static str) -> UsageError {
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
        for command in [
            "adopt",
            "new",
            "fork",
            "checkpoint",
            "restore",
            "ensure",
            "ls",
            "path",
            "exec",
            "rm",
            "attach",
            "detach",
            "gc",
            "push",
            "rebase",
            "land",
            "doctor",
        ] {
            assert!(
                map.lines()
                    .any(|line| line.trim_start().starts_with(command)),
                "missing {command}"
            );
        }
    }
}
