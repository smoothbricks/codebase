use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;

pub const COMMAND_MAP: &str = "commands:\n  adopt [path]       adopt a checkout\n  new <name>         create a workspace\n  ls                 list workspaces\n  path <ws>          print a workspace mount\n  exec <ws> -- <cmd> run an argv command\n  rm <ws>            remove a workspace\n  attach <ws>        attach a workspace\n  detach <ws>        detach a workspace\n  doctor             check invariants";

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
    List,
    Path(PathArgs),
    Exec(ExecArgs),
    Remove(RemoveArgs),
    Attach(AttachArgs),
    Detach(DetachArgs),
    Doctor,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdoptArgs {
    pub path: Option<PathBuf>,
    pub capacity: Option<OsString>,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveArgs {
    pub workspace: String,
    pub force: bool,
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

pub fn parse_args<I, T>(args: I) -> Result<Cli, UsageError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let mut global = GlobalOptions::default();
    let mut index = 0;
    while index < args.len() && parse_global(&args, &mut index, &mut global)? {}

    let Some(command) = args.get(index).and_then(|arg| arg.to_str()) else {
        return Err(UsageError::missing_command());
    };
    index += 1;

    let command = match command {
        "adopt" => parse_adopt(&args, index, &mut global)?,
        "new" => parse_new(&args, index, &mut global)?,
        "ls" => parse_empty(&args, index, &mut global, "ls", Command::List)?,
        "path" => parse_path(&args, index, &mut global)?,
        "exec" => parse_exec(&args, index, &mut global)?,
        "rm" => parse_remove(&args, index, &mut global)?,
        "attach" => parse_attach(&args, index, &mut global)?,
        "detach" => parse_detach(&args, index, &mut global)?,
        "doctor" => parse_empty(&args, index, &mut global, "doctor", Command::Doctor)?,
        other => return Err(UsageError::new(format!("unknown command `{other}`"), "<command>")),
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
                UsageError::new("--project requires a git root", "--project <git-root> <command>")
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
    const USAGE: &str = "adopt [path] [--capacity <size>]";
    let mut parsed = AdoptArgs::default();
    while index < args.len() {
        if parse_global(args, &mut index, global)? {
            continue;
        }
        match args[index].to_str() {
            Some("--capacity") => {
                parsed.capacity = Some(take_value(args, &mut index, "--capacity", USAGE)?);
            }
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
                let text = value.to_str().ok_or_else(|| UsageError::new("--slot must be an unsigned integer", USAGE))?;
                slot = Some(text.parse().map_err(|_| UsageError::new("--slot must be an unsigned integer", USAGE))?);
            }
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if name.is_none() => name = Some(workspace_name(&args[index], true, USAGE)?),
            _ => return Err(UsageError::new("new accepts exactly one workspace name", USAGE)),
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

fn parse_path(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "path <ws> [--no-attach]";
    let mut workspace = None;
    let mut no_attach = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? { continue; }
        match args[index].to_str() {
            Some("--no-attach") => no_attach = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => workspace = Some(workspace_name(&args[index], false, USAGE)?),
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
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
) -> Result<Command, UsageError> {
    const USAGE: &str = "exec <ws> [--stdin | --stdin-file <rel> | --stdin-base64 <data>] [--ro] [--cwd <rel>] [--session <name>] [--timeout <dur>] [--background] -- <cmd…>";
    let mut workspace = None;
    let mut stdin = None;
    let mut read_only = false;
    let mut cwd = None;
    let mut session = None;
    let mut timeout = None;
    let mut background = false;

    while index < args.len() {
        if args[index] == OsStr::new("--") {
            index += 1;
            break;
        }
        if parse_global(args, &mut index, global)? { continue; }
        match args[index].to_str() {
            Some("--stdin") => set_stdin(&mut stdin, StdinSource::Stream, USAGE)?,
            Some("--stdin-file") => {
                let value = take_value(args, &mut index, "--stdin-file", USAGE)?;
                set_stdin(&mut stdin, StdinSource::WorkspaceFile(PathBuf::from(value)), USAGE)?;
            }
            Some("--stdin-base64") => {
                let value = take_value(args, &mut index, "--stdin-base64", USAGE)?;
                set_stdin(&mut stdin, StdinSource::InlineBase64(value), USAGE)?;
            }
            Some("--ro") => read_only = true,
            Some("--cwd") => cwd = Some(PathBuf::from(take_value(args, &mut index, "--cwd", USAGE)?)),
            Some("--session") => {
                let value = take_value(args, &mut index, "--session", USAGE)?;
                session = Some(workspace_name(&value, false, USAGE)?);
            }
            Some("--timeout") => timeout = Some(take_value(args, &mut index, "--timeout", USAGE)?),
            Some("--background") => background = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => workspace = Some(workspace_name(&args[index], false, USAGE)?),
            _ => return Err(UsageError::new("exec requires `--` before the child argv", USAGE)),
        }
        index += 1;
    }
    let workspace = workspace.ok_or_else(|| UsageError::new("exec requires a workspace", USAGE))?;
    if index == 0 || args.get(index.wrapping_sub(1)) != Some(&OsString::from("--")) {
        return Err(UsageError::new("exec requires `--` before the child argv", USAGE));
    }
    let argv = args[index..].to_vec();
    if argv.is_empty() {
        return Err(UsageError::new("exec requires a child command after `--`", USAGE));
    }
    Ok(Command::Exec(ExecArgs { workspace, argv, stdin, read_only, cwd, session, timeout, background }))
}

fn parse_remove(args: &[OsString], mut index: usize, global: &mut GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &str = "rm <ws> [--force]";
    let mut workspace = None;
    let mut force = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? { continue; }
        match args[index].to_str() {
            Some("--force") => force = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => workspace = Some(workspace_name(&args[index], false, USAGE)?),
            _ => return Err(UsageError::new("rm accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Remove(RemoveArgs { workspace: workspace.ok_or_else(|| UsageError::new("rm requires a workspace", USAGE))?, force }))
}

fn parse_attach(args: &[OsString], mut index: usize, global: &mut GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &str = "attach <ws> [--browse]";
    let mut workspace = None;
    let mut browse = false;
    while index < args.len() {
        if parse_global(args, &mut index, global)? { continue; }
        match args[index].to_str() {
            Some("--browse") => browse = true,
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => workspace = Some(workspace_name(&args[index], false, USAGE)?),
            _ => return Err(UsageError::new("attach accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Attach(AttachArgs { workspace: workspace.ok_or_else(|| UsageError::new("attach requires a workspace", USAGE))?, browse }))
}

fn parse_detach(args: &[OsString], mut index: usize, global: &mut GlobalOptions) -> Result<Command, UsageError> {
    const USAGE: &str = "detach <ws>";
    let mut workspace = None;
    while index < args.len() {
        if parse_global(args, &mut index, global)? { continue; }
        match args[index].to_str() {
            Some(flag) if flag.starts_with('-') => return Err(unknown_flag(flag, USAGE)),
            _ if workspace.is_none() => workspace = Some(workspace_name(&args[index], false, USAGE)?),
            _ => return Err(UsageError::new("detach accepts exactly one workspace", USAGE)),
        }
        index += 1;
    }
    Ok(Command::Detach(DetachArgs { workspace: workspace.ok_or_else(|| UsageError::new("detach requires a workspace", USAGE))? }))
}

fn parse_empty(
    args: &[OsString],
    mut index: usize,
    global: &mut GlobalOptions,
    command: &'static str,
    parsed: Command,
) -> Result<Command, UsageError> {
    while index < args.len() {
        if parse_global(args, &mut index, global)? { continue; }
        return Err(UsageError::new(format!("{command} accepts no arguments"), command));
    }
    Ok(parsed)
}

fn take_value(args: &[OsString], index: &mut usize, option: &str, usage: &'static str) -> Result<OsString, UsageError> {
    *index += 1;
    args.get(*index).cloned().ok_or_else(|| UsageError::new(format!("{option} requires a value"), usage))
}

fn set_stdin(target: &mut Option<StdinSource>, value: StdinSource, usage: &'static str) -> Result<(), UsageError> {
    if target.is_some() {
        return Err(UsageError::new("--stdin, --stdin-file, and --stdin-base64 conflict", usage));
    }
    *target = Some(value);
    Ok(())
}

fn workspace_name(value: &OsStr, reserve_main: bool, usage: &'static str) -> Result<String, UsageError> {
    let Some(value) = value.to_str() else {
        return Err(UsageError::new("workspace names must be UTF-8", usage));
    };
    let valid = !value.is_empty()
        && value.len() <= 64
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid || (reserve_main && value == "main") {
        return Err(UsageError::new(
            if reserve_main && value == "main" { "workspace name `main` is reserved" } else { "workspace names must match [a-z0-9][a-z0-9-]{0,63}" },
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
            let cli = parse_args(vec![OsString::from("exec"), OsString::from("raven"), OsString::from("--"), opaque.clone(), OsString::from("--json")]).unwrap();
            let Command::Exec(exec) = cli.command else { panic!("expected exec") };
            assert_eq!(exec.argv[0].as_bytes(), opaque.as_bytes());
            assert_eq!(exec.argv[1], "--json");
            assert!(!cli.global.json);
        }
    }

    #[test]
    fn exec_rejects_conflicting_stdin_sources_with_resolving_hint() {
        let error = parse_args(["exec", "raven", "--stdin", "--stdin-file", "input", "--", "cat"]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert!(error.message.contains("conflict"));
        assert!(error.hint.starts_with("cowshed exec <ws>"));
    }

    #[test]
    fn validates_names_and_new_option_conflicts() {
        assert!(parse_args(["new", "Bad_Name"]).is_err());
        assert!(parse_args(["new", "main"]).is_err());
        assert!(parse_args(["new", "raven", "--ref", "HEAD", "--from", "main"]).is_err());
        assert!(parse_args(["path", "main"]).is_ok());
    }

    #[test]
    fn no_args_is_usage_and_command_map_is_complete() {
        let error = parse_args(Vec::<OsString>::new()).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert_eq!(error.kind, UsageErrorKind::MissingCommand);
        let map = error.command_map().unwrap();
        for command in ["adopt", "new", "ls", "path", "exec", "rm", "attach", "detach", "doctor"] {
            assert!(map.lines().any(|line| line.trim_start().starts_with(command)), "missing {command}");
        }
    }
}
