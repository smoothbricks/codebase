use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;

pub const COMMAND_MAP: &str = "commands:\n  adopt [path]       adopt a checkout\n  new <name>         create a workspace\n  fork <src> <dst>   fork a workspace\n  checkpoint <ws>    create a checkpoint\n  restore <ws> <id>  restore a checkpoint\n  ensure             heal the current workspace\n  ls                 list workspaces\n  path <ws>          print a workspace mount\n  exec <ws> -- <cmd> run an argv command\n  rm <ws>            remove a workspace\n  attach <ws>        attach a workspace\n  detach <ws>        detach a workspace\n  gc                 reclaim storage\n  push <ws>          preserve a workspace ref\n  rebase <ws>        rebase a workspace\n  land <ws>          land a workspace\n  doctor             check invariants";

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkArgs {
    pub source: String,
    pub destination: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointArgs {
    pub workspace: String,
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
    pub workspace: String,
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
    pub workspace: String,
    pub branch: Option<OsString>,
    pub expected_workspace_incarnation: Option<OsString>,
    pub expected_source_head: Option<OsString>,
    pub expected_destination_head: Option<OsString>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseArgs {
    pub workspace: String,
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
    };
    Ok(Cli { global, command })
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
    const USAGE: &str = "new <name> [--ref <rev> | --from <ws>] [--browse] [--slot <n>]";
    let mut name = None;
    let mut reference = None;
    let mut from = None;
    let mut browse = false;
    let mut slot = None;
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
        workspace: workspace
            .ok_or_else(|| UsageError::new("checkpoint requires a workspace", USAGE))?,
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
        workspace: workspace.ok_or_else(|| UsageError::new("path requires a workspace", USAGE))?,
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
        workspace: workspace.ok_or_else(|| UsageError::new("push requires a workspace", USAGE))?,
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
        workspace: workspace
            .ok_or_else(|| UsageError::new("rebase requires a workspace", USAGE))?,
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

    #[test]
    fn lifecycle_parsers_enforce_required_values_and_preserve_revision_bytes() {
        assert!(parse_args(["fork", "raven"]).is_err());
        assert!(parse_args(["checkpoint"]).is_err());
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
