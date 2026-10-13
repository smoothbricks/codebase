//! The `cargo` jobserver launcher (03_caches.md, "The `cargo` jobserver
//! launcher"; 05_gateway.md, "Host CPU budget").
//!
//! The installed cowshed binary is also the managed environment's `cargo`:
//! invoked under that name it runs the next `cargo` on `PATH` sized to the
//! host CPU budget. It reads its environment and its subcommand, in order:
//!
//! - an inherited jobserver (`CARGO_MAKEFLAGS` names a live one) already
//!   counts this run: `cargo` runs as asked;
//! - a `bounded-exec` grant (`BOUNDED_EXEC_CPU_TOKENS` set) already sized
//!   this run: no second grant, and the cargo sizing variables are set to
//!   that count where unset;
//! - a subcommand that compiles nothing runs directly, so rust-analyzer's
//!   frequent `cargo metadata` never queues behind gates;
//! - any other subcommand asks the gateway for a jobserver grant and runs
//!   under its fifo, with the test-thread sizing set to the grant where
//!   unset. With no gateway listening, `cargo` runs unbudgeted.

use cowshed_os::fork_lock::Run as _;

#[cfg(test)]
use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

/// The subcommands that compile nothing and so never queue for a grant.
const NONCOMPILING: &[&str] = &[
    "metadata",
    "fmt",
    "locate-project",
    "tree",
    "pkgid",
    "read-manifest",
    "verify-project",
    "version",
    "help",
    "config",
    "search",
    "info",
    "new",
    "init",
    "add",
    "remove",
    "update",
    "fetch",
    "vendor",
    "generate-lockfile",
    "clean",
    "login",
    "logout",
    "owner",
    "yank",
    "report",
];

/// What the launcher decided to do with this invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Plan {
    /// Run the real `cargo` as asked.
    Direct,
    /// Run it with the cargo sizing variables set to this count where unset.
    Sized(usize),
    /// Ask the gateway for a jobserver grant of this many tokens.
    Ask { want: usize },
}

/// Whether this process was invoked as the managed environment's `cargo`.
pub fn invoked_as_cargo() -> bool {
    env::args_os().next().is_some_and(|argv0| {
        Path::new(&argv0)
            .file_name()
            .is_some_and(|name| name == OsStr::new("cargo"))
    })
}

/// Run as `cargo`: the process exit code.
pub fn run(arguments: &[OsString]) -> i32 {
    let socket = PathBuf::from(cowshed_core::storage::bootstrap::STORE_ROOT).join("gateway.sock");
    run_at(arguments, &socket)
}

/// Run as `cargo`, asking the gateway at `socket` for jobserver grants.
pub fn run_at(arguments: &[OsString], socket: &Path) -> i32 {
    let get = |name: &str| env::var(name).ok();
    match plan(&get, arguments) {
        Plan::Direct => exec(arguments, &[]),
        Plan::Sized(tokens) => exec(arguments, &sizing(tokens, &get)),
        Plan::Ask { want } => match ask(arguments, want, socket) {
            Ok(code) => code,
            Err(reason) => {
                eprintln!("cowshed: cargo runs unbudgeted: {reason}");
                exec(arguments, &[])
            }
        },
    }
}

/// The plan for one invocation, from its environment and its arguments.
fn plan(get: &impl Fn(&str) -> Option<String>, arguments: &[OsString]) -> Plan {
    if inherited_jobserver_live(get) {
        return Plan::Direct;
    }
    if let Some(tokens) = get("BOUNDED_EXEC_CPU_TOKENS")
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|tokens| *tokens > 0)
    {
        return Plan::Sized(tokens);
    }
    match subcommand(arguments) {
        None => Plan::Direct,
        Some(name) if NONCOMPILING.contains(&name.as_str()) => Plan::Direct,
        Some(_) => Plan::Ask {
            want: std::thread::available_parallelism().map_or(1, usize::from),
        },
    }
}

/// `CARGO_MAKEFLAGS` names a jobserver that is still there: a `fifo:` one
/// whose path exists, or a pipe one, which only exists while it is live.
fn inherited_jobserver_live(get: &impl Fn(&str) -> Option<String>) -> bool {
    let Some(flags) = get("CARGO_MAKEFLAGS") else {
        return false;
    };
    for token in flags.split_whitespace() {
        let Some(auth) = token.strip_prefix("--jobserver-auth=") else {
            continue;
        };
        return match auth.strip_prefix("fifo:") {
            Some(path) => Path::new(path).exists(),
            None => true,
        };
    }
    false
}

/// The invocation's subcommand: the first argument after cargo's global
/// options and a `+toolchain` prefix, or `None` when there is none
/// (`--version`, `--list`, a bare `cargo`).
fn subcommand(arguments: &[OsString]) -> Option<String> {
    let mut index = 0;
    if arguments
        .first()
        .is_some_and(|first| first.to_string_lossy().starts_with('+'))
    {
        index = 1;
    }
    while let Some(argument) = arguments.get(index) {
        let text = argument.to_string_lossy();
        match text.as_ref() {
            "--config" | "-Z" | "--color" => index += 2,
            _ if text.starts_with('-') => index += 1,
            _ => return Some(text.into_owned()),
        }
    }
    None
}

/// The environment a grant of `tokens` sets where the caller left it unset:
/// test threads take no jobserver token, so without these a granted
/// `cargo test` still runs one test thread per core.
fn sizing(tokens: usize, get: &impl Fn(&str) -> Option<String>) -> Vec<(String, String)> {
    [
        "CARGO_BUILD_JOBS",
        "RUST_TEST_THREADS",
        "NEXTEST_TEST_THREADS",
        "BOUNDED_EXEC_CPU_TOKENS",
    ]
    .into_iter()
    .filter(|name| get(name).is_none())
    .map(|name| (name.to_owned(), tokens.to_string()))
    .collect()
}

/// Ask the gateway for a jobserver grant and run under it. `Err` names why
/// there is no grant; the caller then runs `cargo` unbudgeted.
fn ask(arguments: &[OsString], want: usize, socket: &Path) -> Result<i32, String> {
    let mut stream = UnixStream::connect(socket).map_err(|error| {
        format!(
            "the gateway's control socket {} does not answer ({error})",
            socket.display()
        )
    })?;
    let checkout = env::current_dir()
        .map_err(|error| format!("cannot name the working directory: {error}"))?;
    let command = std::iter::once("cargo".to_owned())
        .chain(
            arguments
                .iter()
                .map(|argument| argument.to_string_lossy().into_owned()),
        )
        .collect::<Vec<_>>()
        .join(" ");
    let request = serde_json::json!({
        "op": "jobserver",
        "want": want,
        "checkout": checkout.to_string_lossy(),
        "command": command,
    });
    stream
        .write_all(format!("{request}\n").as_bytes())
        .map_err(|error| format!("cannot ask the gateway for a jobserver grant: {error}"))?;

    // The gateway answers `queued` at once, then the grant when it is made.
    // The connection stays open for the run's whole life: its close is what
    // returns the grant. It is not inherited by the child.
    let mut lines = BufReader::new(
        stream
            .try_clone()
            .map_err(|error| format!("cannot keep the jobserver connection: {error}"))?,
    );
    let granted = loop {
        let mut line = String::new();
        let read = lines
            .read_line(&mut line)
            .map_err(|error| format!("the gateway stopped answering: {error}"))?;
        if read == 0 {
            return Err("the gateway closed the jobserver request without granting it".to_owned());
        }
        let answer: serde_json::Value = serde_json::from_str(&line).map_err(|error| {
            format!("the gateway answered a jobserver request with non-JSON: {error}")
        })?;
        if answer["lease"] == "queued" {
            continue;
        }
        if answer["ok"] != true || answer["lease"] != "granted" {
            let reason = answer["error"].as_str().unwrap_or("refused");
            return Err(format!("the gateway refused the jobserver grant: {reason}"));
        }
        break answer;
    };
    let tokens = granted["tokens"]
        .as_u64()
        .and_then(|tokens| usize::try_from(tokens).ok())
        .filter(|tokens| *tokens > 0)
        .ok_or_else(|| "the gateway granted a jobserver without a token count".to_owned())?;
    let fifo = granted["fifo"]
        .as_str()
        .ok_or_else(|| "the gateway granted a jobserver without a fifo".to_owned())?;

    let get = |name: &str| env::var(name).ok();
    let mut environment = sizing(tokens, &get);
    environment.retain(|(name, _)| name != "CARGO_BUILD_JOBS");
    environment.push((
        "CARGO_MAKEFLAGS".to_owned(),
        format!("-j --jobserver-auth=fifo:{fifo}"),
    ));
    environment.push((
        "MAKEFLAGS".to_owned(),
        format!("-j --jobserver-auth=fifo:{fifo}"),
    ));
    let code = exec_with(arguments, &environment);
    drop(lines);
    drop(stream);
    Ok(code)
}

/// Run the real `cargo` with exactly the given extra environment.
fn exec(arguments: &[OsString], environment: &[(String, String)]) -> i32 {
    exec_with(arguments, environment)
}

fn exec_with(arguments: &[OsString], environment: &[(String, String)]) -> i32 {
    let Some(cargo) = real_cargo() else {
        eprintln!("cowshed: no cargo on PATH beyond the jobserver launcher");
        return 127;
    };
    match Command::new(&cargo)
        .args(arguments)
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .status_locked()
    {
        Ok(status) => exit_code(status),
        Err(error) => {
            eprintln!("cowshed: cannot run {}: {error}", cargo.display());
            127
        }
    }
}

/// The child's exit code, or `128 + signal` when it died by one.
fn exit_code(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    1
}

/// The next `cargo` on `PATH`: the first executable by that name that is not
/// this binary, reached through whatever links name it.
fn real_cargo() -> Option<PathBuf> {
    let own = env::current_exe()
        .ok()
        .and_then(|path| path.canonicalize().ok());
    let path = env::var_os("PATH")?;
    for directory in env::split_paths(&path) {
        if !directory.is_absolute() {
            continue;
        }
        let candidate = directory.join("cargo");
        let Ok(metadata) = candidate.metadata() else {
            continue;
        };
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            continue;
        }
        if own.is_some() && candidate.canonicalize().ok() == own {
            continue;
        }
        return Some(candidate);
    }
    None
}

/// The launcher's view of an environment, for tests.
#[cfg(test)]
fn environment(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: BTreeMap<String, String> = pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    move |name: &str| map.get(name).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(arguments: &[&str]) -> Vec<OsString> {
        arguments.iter().map(OsString::from).collect()
    }

    #[test]
    fn an_inherited_jobserver_and_a_bounded_exec_grant_run_as_asked_or_sized() {
        // A fifo that exists is a live inherited jobserver.
        let fifo = env::temp_dir().join(format!("cowshed-launcher-test-{}", std::process::id()));
        std::fs::write(&fifo, []).unwrap();
        let flags = format!("-j --jobserver-auth=fifo:{}", fifo.display());
        let pairs = [("CARGO_MAKEFLAGS", flags.as_str())];
        let get = environment(&pairs);
        assert_eq!(plan(&get, &args(&["build"])), Plan::Direct);
        let _ = std::fs::remove_file(&fifo);

        // A fifo that is gone counts as no jobserver; the grant then sizes.
        let get = environment(&[
            (
                "CARGO_MAKEFLAGS",
                "-j --jobserver-auth=fifo:/nonexistent/fifo",
            ),
            ("BOUNDED_EXEC_CPU_TOKENS", "3"),
        ]);
        assert_eq!(plan(&get, &args(&["build"])), Plan::Sized(3));
    }

    #[test]
    fn subcommands_that_compile_nothing_run_directly_and_builds_ask() {
        let get = environment(&[]);
        for name in ["metadata", "fmt", "tree", "version", "help", "clean"] {
            assert_eq!(plan(&get, &args(&[name])), Plan::Direct, "{name}");
        }
        assert_eq!(plan(&get, &args(&[])), Plan::Direct);
        assert_eq!(plan(&get, &args(&["--version"])), Plan::Direct);
        assert!(matches!(plan(&get, &args(&["build"])), Plan::Ask { .. }));
        assert!(matches!(plan(&get, &args(&["test"])), Plan::Ask { .. }));
        assert!(matches!(
            plan(&get, &args(&["nextest", "run"])),
            Plan::Ask { .. }
        ));
        assert!(matches!(plan(&get, &args(&["clippy"])), Plan::Ask { .. }));
        // Third-party subcommands are builds.
        assert!(matches!(plan(&get, &args(&["machete"])), Plan::Ask { .. }));
    }

    #[test]
    fn the_subcommand_follows_toolchains_and_global_options() {
        assert_eq!(
            subcommand(&args(&["+nightly", "build"])).as_deref(),
            Some("build")
        );
        assert_eq!(
            subcommand(&args(&["--config", "a.b=1", "metadata"])).as_deref(),
            Some("metadata")
        );
        assert_eq!(
            subcommand(&args(&["-Z", "unstable-options", "build"])).as_deref(),
            Some("build")
        );
        assert_eq!(
            subcommand(&args(&["--frozen", "test"])).as_deref(),
            Some("test")
        );
        assert_eq!(subcommand(&args(&["--version"])), None);
        assert_eq!(subcommand(&args(&[])), None);
    }

    #[test]
    fn sizing_sets_only_what_is_unset() {
        let get = environment(&[("RUST_TEST_THREADS", "2")]);
        assert_eq!(
            sizing(5, &get),
            [
                ("CARGO_BUILD_JOBS".to_owned(), "5".to_owned()),
                ("NEXTEST_TEST_THREADS".to_owned(), "5".to_owned()),
                ("BOUNDED_EXEC_CPU_TOKENS".to_owned(), "5".to_owned()),
            ]
        );
        let granted = environment(&[]);
        assert_eq!(sizing(1, &granted).len(), 4);
    }

    #[test]
    fn a_grant_ask_wants_one_per_core() {
        let get = environment(&[]);
        let Plan::Ask { want } = plan(&get, &args(&["build"])) else {
            panic!("a build asks");
        };
        assert_eq!(
            want,
            std::thread::available_parallelism().map_or(1, usize::from)
        );
    }
}
