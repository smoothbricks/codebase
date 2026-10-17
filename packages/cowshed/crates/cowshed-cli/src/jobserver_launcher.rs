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
//! - a subcommand that compiles nothing, or any subcommand asked only for
//!   its version or usage, runs directly, so rust-analyzer's frequent
//!   `cargo metadata` and Nx's sandboxed `cargo nextest --version` toolchain
//!   input never queue behind gates or need the gateway;
//! - any other subcommand asks the gateway for a jobserver grant and runs
//!   under its fifo, with the test-thread sizing set to the grant where
//!   unset. A managed checkout refuses every failed admission. Only gateway absence in a
//!   positively nonmanaged host/CI checkout permits an unbudgeted run.

use cowshed_os::fork_lock::{Run as _, Spawn as _};

#[cfg(test)]
use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::io::{self, BufRead, BufReader, Write};
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

/// The flags that ask any subcommand for its version or usage instead of running it.
const QUERIES: &[&str] = &["-V", "--version", "-h", "--help"];

/// Absence is the only admission failure an explicitly nonmanaged host may bypass.
#[derive(Debug)]
enum GrantFailure {
    NoGateway(String),
    Admission(String),
}
impl From<String> for GrantFailure {
    fn from(reason: String) -> Self {
        Self::Admission(reason)
    }
}
impl std::fmt::Display for GrantFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoGateway(reason) | Self::Admission(reason) => formatter.write_str(reason),
        }
    }
}

/// Presence only tightens admission, never grants authority. Cargo can run below the checkout
/// root, so inspect the same protected workspace marker as bounded-exec along its ancestors.
fn managed_checkout(cwd: &Path) -> Result<bool, String> {
    for root in cwd.ancestors() {
        let marker = root.join(".cowshed/workspace.json");
        match std::fs::symlink_metadata(&marker) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "cannot inspect CPU admission marker {}: {error}",
                    marker.display()
                ));
            }
        }
    }
    Ok(false)
}
/// What the launcher decided to do with this invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Plan {
    /// Run the real `cargo` as asked.
    Direct,
    /// Run it with the cargo sizing variables set to this count where unset.
    Sized(usize),
    /// Ask the gateway for a jobserver grant of this many tokens.
    Ask { want: usize },
    /// A live immediate parent owns the upstream grant; borrow only its local allocation.
    Parent { socket: PathBuf, want: usize },
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
    let socket = cowshed_gateway_types::control_socket_path();
    run_at(arguments, &socket)
}

/// Run as `cargo`, asking the gateway at `socket` for jobserver grants.
pub fn run_at(arguments: &[OsString], socket: &Path) -> i32 {
    let get = |name: &str| env::var(name).ok();
    match plan(&get, arguments) {
        Plan::Direct => exec(arguments, &[]),
        Plan::Sized(tokens) => exec(arguments, &sizing(tokens, &get)),

        Plan::Parent {
            socket: parent,
            want,
        } => match ask_parent(arguments, want, &parent) {
            Ok(code) => code,
            Err(reason) => {
                eprintln!("cowshed: cargo parent CPU grant refused: {reason}");
                1
            }
        },
        Plan::Ask { want } => {
            let checkout = match env::current_dir() {
                Ok(checkout) => checkout,
                Err(error) => {
                    eprintln!(
                        "cowshed: cannot name Cargo checkout: {error}; refusing to run without a grant"
                    );
                    return 1;
                }
            };
            let managed = match managed_checkout(&checkout) {
                Ok(managed) => managed,
                Err(reason) => {
                    eprintln!(
                        "cowshed: cargo CPU admission failed: {reason}; refusing to run without a grant"
                    );
                    return 1;
                }
            };
            match ask(arguments, want, socket, &checkout) {
                Ok(code) => code,
                Err(GrantFailure::NoGateway(reason)) if !managed => {
                    eprintln!("cowshed: cargo runs unbudgeted: {reason}");
                    exec(arguments, &[])
                }
                Err(reason) => {
                    eprintln!(
                        "cowshed: cargo CPU admission failed: {reason}; refusing to run without a grant"
                    );
                    1
                }
            }
        }
    }
}

/// The plan for one invocation, from its environment and its arguments.
fn plan(get: &impl Fn(&str) -> Option<String>, arguments: &[OsString]) -> Plan {
    if inherited_jobserver_live(get) {
        return Plan::Direct;
    }

    if let Some(socket) =
        get("BOUNDED_EXEC_CPU_POOL").filter(|value| !value.is_empty() && !value.contains('\0'))
        && let Some(tokens) = get("BOUNDED_EXEC_CPU_TOKENS")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
    {
        if compiles_nothing(arguments) {
            return Plan::Direct;
        }
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        return Plan::Parent {
            socket: PathBuf::from(socket),
            want: tokens.min(cores.div_ceil(2)),
        };
    }
    if let Some(tokens) = get("BOUNDED_EXEC_CPU_TOKENS")
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|tokens| *tokens > 0)
    {
        return Plan::Sized(tokens);
    }
    if compiles_nothing(arguments) {
        return Plan::Direct;
    }
    Plan::Ask {
        want: std::thread::available_parallelism().map_or(1, usize::from),
    }
}

/// The invocation compiles nothing: it names no subcommand, a noncompiling one, or any
/// subcommand with a query flag as its whole argument list. The query is recognised by shape,
/// not by subcommand, so a third-party subcommand's `--version` needs no list entry; a query
/// flag among other arguments may reach a program that is built first (`cargo run -- -V`).
fn compiles_nothing(arguments: &[OsString]) -> bool {
    match subcommand(arguments) {
        None => true,
        Some((_, [query])) if query.to_str().is_some_and(|flag| QUERIES.contains(&flag)) => true,
        Some((name, _)) => NONCOMPILING.contains(&name.as_str()),
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

/// The invocation's subcommand and the arguments after it: the first argument after cargo's
/// global options and a `+toolchain` prefix, or `None` when there is none (`--version`,
/// `--list`, a bare `cargo`).
fn subcommand(arguments: &[OsString]) -> Option<(String, &[OsString])> {
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
            _ => return Some((text.into_owned(), &arguments[index + 1..])),
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

/// Ask the gateway and run only after a real grant. Connected refusal/protocol failures never
/// become absence; the caller may bypass only NoGateway in a positively nonmanaged checkout.
fn ask(
    arguments: &[OsString],
    want: usize,
    socket: &Path,
    checkout: &Path,
) -> Result<i32, GrantFailure> {
    let mut stream = UnixStream::connect(socket).map_err(|error| {
        let absent = error.kind() == io::ErrorKind::NotFound;
        let reason = format!(
            "the gateway's control socket {} does not answer ({error})",
            socket.display()
        );
        if absent {
            GrantFailure::NoGateway(reason)
        } else {
            GrantFailure::Admission(reason)
        }
    })?;
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
            return Err(
                "the gateway closed the jobserver request without granting it"
                    .to_owned()
                    .into(),
            );
        }
        let answer: serde_json::Value = serde_json::from_str(&line).map_err(|error| {
            format!("the gateway answered a jobserver request with non-JSON: {error}")
        })?;
        if answer["lease"] == "queued" {
            continue;
        }
        if answer["ok"] != true || answer["lease"] != "granted" {
            let reason = answer["error"].as_str().unwrap_or("refused");
            return Err(format!("the gateway refused the jobserver grant: {reason}").into());
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
    fn a_version_or_help_query_compiles_nothing_whatever_the_subcommand() {
        // Nx's Rust toolchain runtime input runs `cargo nextest --version` inside the shed
        // sandbox, which cannot reach the gateway: a query must never ask for a grant.
        let get = environment(&[]);
        for query in [
            ["nextest", "--version"],
            ["clippy", "-V"],
            ["machete", "--help"],
            ["build", "-h"],
        ] {
            assert_eq!(plan(&get, &args(&query)), Plan::Direct, "{query:?}");
        }
        assert_eq!(
            plan(
                &get,
                &args(&["+nightly", "--frozen", "nextest", "--version"])
            ),
            Plan::Direct
        );
        let pool = environment(&[
            ("BOUNDED_EXEC_CPU_POOL", "/nonexistent/pool.sock"),
            ("BOUNDED_EXEC_CPU_TOKENS", "2"),
        ]);
        assert_eq!(plan(&pool, &args(&["nextest", "--version"])), Plan::Direct);
        // A query flag that is not the subcommand's whole argument list may reach a program
        // that compiles first: the binary `cargo run` builds, or the test binaries `cargo test`
        // builds before passing them `--help`.
        for build in [["run", "--", "--version"], ["test", "--", "--help"]] {
            assert!(
                matches!(plan(&get, &args(&build)), Plan::Ask { .. }),
                "{build:?}"
            );
        }
    }

    #[test]
    fn the_subcommand_follows_toolchains_and_global_options() {
        let name = |arguments: &[&str]| subcommand(&args(arguments)).map(|(name, _)| name);
        assert_eq!(name(&["+nightly", "build"]).as_deref(), Some("build"));
        assert_eq!(
            name(&["--config", "a.b=1", "metadata"]).as_deref(),
            Some("metadata")
        );
        assert_eq!(
            name(&["-Z", "unstable-options", "build"]).as_deref(),
            Some("build")
        );
        assert_eq!(name(&["--frozen", "test"]).as_deref(), Some("test"));
        assert_eq!(name(&["--version"]), None);
        assert_eq!(name(&[]), None);
        // The arguments after the subcommand are its own.
        let arguments = args(&["+nightly", "--frozen", "nextest", "--version"]);
        assert_eq!(
            subcommand(&arguments),
            Some(("nextest".to_owned(), &arguments[3..]))
        );
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

fn ask_parent(arguments: &[OsString], want: usize, socket: &Path) -> Result<i32, String> {
    let mut stream = UnixStream::connect(socket).map_err(|error| {
        format!(
            "the inherited CPU pool {} is unavailable: {error}; no second root grant is taken",
            socket.display()
        )
    })?;
    stream
        .write_all(
            format!(
                "{}\n",
                serde_json::json!({ "op": "cpu-pool", "want": want, "children": true })
            )
            .as_bytes(),
        )
        .map_err(|error| format!("cannot ask the parent CPU pool: {error}"))?;
    let mut lines = BufReader::new(&mut stream);
    let granted = loop {
        let mut line = String::new();
        if lines
            .read_line(&mut line)
            .map_err(|error| error.to_string())?
            == 0
        {
            return Err("parent closed without granting tokens".to_owned());
        }
        let answer: serde_json::Value =
            serde_json::from_str(&line).map_err(|error| error.to_string())?;
        if answer["ok"] != true {
            return Err(format!("parent refused: {answer}"));
        }
        if answer["lease"] == "queued" {
            cowshed_os::timing::event("jobserver", || {
                format!(
                    "cargo parent-pool queued want={want} pool={}",
                    socket.display()
                )
            });
            continue;
        }
        if answer["lease"] != "granted" {
            return Err(format!("parent answered out of turn: {answer}"));
        }
        break answer;
    };
    let tokens = granted["tokens"]
        .as_u64()
        .and_then(|tokens| usize::try_from(tokens).ok())
        .filter(|tokens| *tokens > 0 && *tokens <= want)
        .ok_or_else(|| format!("invalid parent allocation: {granted}"))?;
    let subpool = granted["pool"]
        .as_str()
        .filter(|pool| !pool.is_empty() && !pool.contains('\0'))
        .ok_or_else(|| format!("parent supplied no live child pool: {granted}"))?;
    let mut environment = sizing(tokens, &|_| None);
    environment.push(("BOUNDED_EXEC_CPU_POOL".to_owned(), subpool.to_owned()));
    cowshed_os::timing::event("jobserver", || {
        format!(
            "cargo parent-pool granted tokens={tokens}/{want} pool={}",
            socket.display()
        )
    });
    drop(lines);
    stream
        .set_nonblocking(true)
        .map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        use tokio::io::AsyncReadExt as _;
        use std::os::unix::process::CommandExt as _;
        let mut pool = tokio::net::UnixStream::from_std(stream).map_err(|error| error.to_string())?;
        let cargo = real_cargo().ok_or_else(|| "no cargo on PATH beyond the launcher".to_owned())?;
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|error| error.to_string())?;
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).map_err(|error| error.to_string())?;
        // A std Child is never reaped behind our back. The existing controller primitive waits
        // with WNOWAIT, keeping its process-group identity owned until all signals are finished.
        let mut child = Command::new(cargo).args(arguments).envs(environment).process_group(0).spawn_locked().map_err(|error| error.to_string())?;
        let pid = child.id();
        let group = i32::try_from(pid).expect("a process id fits pid_t");
        let mut exits = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
            .map_err(|error| error.to_string())?;
        let mut exited = std::pin::pin!(async {
            loop {
                if cowshed_core::runtime::job_groups::exit_unreaped(group).map_err(|error| error.to_string())?.is_some() {
                    return Ok::<(), String>(());
                }
                if exits.recv().await.is_none() { return Err("child-exit notifications ended".to_owned()); }
            }
        });
        let mut byte = [0_u8; 1];
        let ending = async {
            tokio::select! {
                end = pool.read(&mut byte) => (true, libc::SIGTERM, format!("parent CPU grant lost: {end:?}")),
                interrupt = tokio::signal::ctrl_c() => (false, libc::SIGINT, format!("SIGINT: {interrupt:?}")),
                _ = terminate.recv() => (false, libc::SIGTERM, "SIGTERM".to_owned()),
                _ = hangup.recv() => (false, libc::SIGHUP, "SIGHUP".to_owned()),
            }
        };
        let (observed, lost) = tokio::select! {
            observed = &mut exited => (observed, false),
            (lost, signal, reason) = ending => {
                eprintln!("cowshed: cargo {reason}; stopping owned Cargo process group {pid}");
                if let Err(error) = signal_borrowed_group(group, signal) {
                    eprintln!("cowshed: {error}; retaining the allocation until the owned child exits");
                }
                let observed = match tokio::time::timeout(std::time::Duration::from_secs(10), &mut exited).await {
                    Ok(observed) => observed,
                    Err(_) => {
                        if let Err(error) = signal_borrowed_group(group, libc::SIGKILL) {
                            eprintln!("cowshed: {error}; retaining the allocation until the owned child exits");
                        }
                        exited.await
                    }
                };
                (observed, lost)
            }
        };
        observed?;
        // The leader remains unreaped here. Never signal a numeric group after its id can be reused.
        signal_borrowed_group(group, libc::SIGKILL)?;
        let status = child.wait().map_err(|error| error.to_string())?;
        Ok(if lost { 1 } else { exit_code(status) })
    })
}

fn signal_borrowed_group(group: i32, signal: i32) -> Result<(), String> {
    cowshed_core::runtime::job_groups::signal_unreaped_group(group, signal).map_err(|error| {
        format!("cannot signal owned unreaped Cargo process group {group}: {error}")
    })
}

#[cfg(test)]
mod parent_pool_tests {
    use super::*;
    #[test]
    fn a_gate_string_is_no_grant_and_compiling_cargo_takes_a_live_local_pool() {
        let arguments = vec![OsString::from("build")];
        assert!(matches!(
            plan(&environment(&[("COWSHED_QOS_CLASS", "gate")]), &arguments),
            Plan::Ask { .. }
        ));
        let get = environment(&[
            ("BOUNDED_EXEC_CPU_POOL", "/private/live-parent.sock"),
            ("BOUNDED_EXEC_CPU_TOKENS", "3"),
        ]);
        assert!(matches!(
            plan(&get, &arguments),
            Plan::Parent { want: 1..=3, .. }
        ));
        assert_eq!(plan(&get, &[OsString::from("metadata")]), Plan::Direct);
    }
}
