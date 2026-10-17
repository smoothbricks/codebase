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
//!
//! A build that only compiles asks to grow: a token returned after it started is handed to its
//! jobserver as one more rustc, up to what it asked for, and each growth is said on stderr. A
//! command that also runs what it built sizes its test threads once and never grows.

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
        && get("BOUNDED_EXEC_CPU_TOKENS")
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|value| value > 0)
    {
        if compiles_nothing(arguments) {
            return Plan::Direct;
        }
        // The count proves a whole descriptor; neither it nor half the host bounds the ask, as
        // for a root build, one per core. A lent pool grants at most what it holds, and a gate's
        // relay pool asks the host for each child at gate class.
        return Plan::Parent {
            socket: PathBuf::from(socket),
            want: std::thread::available_parallelism().map_or(1, usize::from),
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

/// The invocation only compiles: a jobserver token handed to it after it starts is one more
/// rustc it can run. A command that also runs what it built (tests, benches, `run`) sizes its
/// test threads once at its start, so a token it was given later would sit idle in its fifo.
fn compiles_only(arguments: &[OsString]) -> bool {
    let Some((name, rest)) = subcommand(arguments) else {
        return false;
    };
    match name.as_str() {
        "build" | "b" | "check" | "c" | "clippy" | "doc" | "d" | "rustc" | "rustdoc" | "fix" => {
            true
        }
        "test" | "t" | "bench" => rest.iter().any(|argument| argument == "--no-run"),
        "nextest" => rest.first().is_some_and(|argument| argument == "archive"),
        _ => false,
    }
}

/// A growing grant's `grown` answer: its new total, which must exceed `held` and stay within
/// `want`; `None` for any other line.
fn grown_total(line: &str, held: usize, want: usize) -> Option<usize> {
    let answer: serde_json::Value = serde_json::from_str(line).ok()?;
    (answer["ok"] == true && answer["lease"] == "grown")
        .then(|| answer["tokens"].as_u64())
        .flatten()
        .and_then(|tokens| usize::try_from(tokens).ok())
        .filter(|tokens| *tokens > held && *tokens <= want)
}

/// The line a growing build prints for each growth, beside the grant it started with.
fn say_grown(from: usize, to: usize, want: usize) {
    eprintln!("cowshed: cargo jobserver grew tokens={from}/{want} -> {to}/{want}");
}

/// The make jobserver fifo a launcher owns for one growing build under a parent pool: it holds
/// the grant's tokens beyond Cargo's implicit first and takes each grown token. The launcher's
/// own descriptor keeps the bytes alive; dropping it unlinks the path. The pool connection, not
/// this fifo, is what returns the tokens.
struct OwnedFifo {
    path: PathBuf,
    file: std::fs::File,
}

impl OwnedFifo {
    fn create(tokens: usize) -> Result<Self, String> {
        use std::os::unix::ffi::OsStrExt as _;
        // A process id is unique among live processes, so a fifo at this path is a dead one's.
        let path = env::temp_dir().join(format!("cowshed-cargo-{}.fifo", std::process::id()));
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("cannot clear {}: {error}", path.display())),
        }
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| format!("jobserver fifo path {} has a NUL", path.display()))?;
        // SAFETY: c_path is a valid NUL-terminated path for the duration of the call.
        if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
            return Err(format!(
                "cannot create jobserver fifo {}: {}",
                path.display(),
                io::Error::last_os_error()
            ));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| format!("cannot open jobserver fifo {}: {error}", path.display()));
        let mut fifo = Self {
            file: match file {
                Ok(file) => file,
                Err(error) => {
                    let _ = std::fs::remove_file(&path);
                    return Err(error);
                }
            },
            path,
        };
        fifo.fill(tokens - 1)
            .map_err(|error| format!("cannot fill jobserver fifo: {error}"))?;
        Ok(fifo)
    }

    /// Hand the build `tokens` more token bytes.
    fn fill(&mut self, tokens: usize) -> io::Result<()> {
        self.file.write_all(&vec![b'+'; tokens])
    }

    /// `CARGO_MAKEFLAGS`/`MAKEFLAGS` naming this fifo.
    fn flags(&self) -> String {
        format!("-j --jobserver-auth=fifo:{}", self.path.display())
    }
}

impl Drop for OwnedFifo {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!(
                "cowshed: cannot remove jobserver fifo {}: {error}",
                self.path.display()
            );
        }
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
    let grow = compiles_only(arguments);
    let request = serde_json::json!({
        "op": "jobserver",
        "want": want,
        "checkout": checkout.to_string_lossy(),
        "command": command,
        "grow": grow,
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
    if grow {
        eprintln!("cowshed: cargo jobserver granted tokens={tokens}/{want}");
    }
    // The gateway fills the fifo itself as the grant grows; it says so on this connection.
    let growth = grow.then(|| {
        std::thread::spawn(move || {
            let mut held = tokens;
            for line in lines.lines() {
                let Ok(line) = line else { break };
                if let Some(to) = grown_total(&line, held, want) {
                    say_grown(held, to, want);
                    held = to;
                }
            }
        })
    });
    let code = exec_with(arguments, &environment);
    // Closing both directions ends the reader and returns the whole grant.
    let _ = stream.shutdown(std::net::Shutdown::Both);
    if let Some(growth) = growth {
        let _ = growth.join();
    }
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

/// Borrow from the parent CPU pool at `socket` and run Cargo in its own process group under that
/// grant. The pool's connection is a ledger entry, not the build's owner: its close (the gateway
/// restarting, or the lending parent exiting) is said and stops nothing, so Cargo runs on over
/// budget and its status is the launcher's (05_gateway.md "Host CPU budget"). Only a signal sent
/// to the launcher ends the group.
fn ask_parent(arguments: &[OsString], want: usize, socket: &Path) -> Result<i32, String> {
    let mut stream = UnixStream::connect(socket).map_err(|error| {
        format!(
            "the inherited CPU pool {} is unavailable: {error}; no second root grant is taken",
            socket.display()
        )
    })?;
    let grow = compiles_only(arguments);
    stream
        .write_all(
            format!(
                "{}\n",
                serde_json::json!({ "op": "cpu-pool", "want": want, "children": true, "grow": grow })
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
    // A build that only compiles runs under a fifo this launcher owns, so a token the pool
    // grows it by is one more rustc; anything else keeps its fixed sizing.
    let mut fifo = if grow {
        let fifo = OwnedFifo::create(tokens)?;
        environment.retain(|(name, _)| name != "CARGO_BUILD_JOBS");
        environment.push(("CARGO_MAKEFLAGS".to_owned(), fifo.flags()));
        environment.push(("MAKEFLAGS".to_owned(), fifo.flags()));
        eprintln!("cowshed: cargo jobserver granted tokens={tokens}/{want}");
        Some(fifo)
    } else {
        None
    };
    cowshed_os::timing::event("jobserver", || {
        format!(
            "cargo parent-pool granted tokens={tokens}/{want} pool={}",
            socket.display()
        )
    });
    // Bytes the handshake read past the grant are the pool's next lines.
    let mut carry = String::from_utf8_lossy(lines.buffer()).into_owned();
    drop(lines);
    stream
        .set_nonblocking(true)
        .map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        use tokio::io::AsyncBufReadExt as _;
        use std::os::unix::process::CommandExt as _;
        let mut pool = tokio::io::BufReader::new(tokio::net::UnixStream::from_std(stream).map_err(|error| error.to_string())?);
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
        let mut released = std::pin::pin!(async {
            let mut held = tokens;
            // The pool says nothing after the grant but a growth. Anything else ends its ledger as
            // its close does: said once, and Cargo keeps what it holds.
            let end = 'ledger: loop {
                while let Some(newline) = carry.find('\n') {
                    let line: String = carry.drain(..=newline).collect();
                    let (Some(fifo), Some(to)) = (fifo.as_mut(), grown_total(&line, held, want)) else {
                        break 'ledger format!("answered out of turn ({})", line.trim_end());
                    };
                    if let Err(error) = fifo.fill(to - held) {
                        break 'ledger format!("grew past what the jobserver could take ({error})");
                    }
                    say_grown(held, to, want);
                    held = to;
                }
                match pool.read_line(&mut carry).await {
                    Ok(0) => break "closed".to_owned(),
                    Ok(_) => {}
                    Err(error) => break format!("failed ({error})"),
                }
            };
            eprintln!(
                "cowshed: cargo's CPU pool {} {end}; its grant is gone, and Cargo process group {pid} runs on over budget until it ends",
                socket.display()
            );
            std::future::pending::<std::convert::Infallible>().await
        });
        let signalled = async {
            tokio::select! {
                interrupt = tokio::signal::ctrl_c() => (libc::SIGINT, format!("SIGINT: {interrupt:?}")),
                _ = terminate.recv() => (libc::SIGTERM, "SIGTERM".to_owned()),
                _ = hangup.recv() => (libc::SIGHUP, "SIGHUP".to_owned()),
            }
        };
        let observed = tokio::select! {
            observed = &mut exited => observed,
            never = &mut released => match never {},
            (signal, reason) = signalled => {
                eprintln!("cowshed: cargo {reason}; stopping owned Cargo process group {pid}");
                if let Err(error) = signal_borrowed_group(group, signal) {
                    eprintln!("cowshed: {error}; retaining the allocation until the owned child exits");
                }
                match tokio::time::timeout(std::time::Duration::from_secs(10), &mut exited).await {
                    Ok(observed) => observed,
                    Err(_) => {
                        if let Err(error) = signal_borrowed_group(group, libc::SIGKILL) {
                            eprintln!("cowshed: {error}; retaining the allocation until the owned child exits");
                        }
                        exited.await
                    }
                }
            }
        };
        observed?;
        // The leader remains unreaped here. Never signal a numeric group after its id can be reused.
        signal_borrowed_group(group, libc::SIGKILL)?;
        let status = child.wait().map_err(|error| error.to_string())?;
        Ok(exit_code(status))
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
        assert_eq!(
            plan(&get, &arguments),
            Plan::Parent {
                socket: PathBuf::from("/private/live-parent.sock"),
                want: std::thread::available_parallelism().map_or(1, usize::from),
            }
        );
        assert_eq!(plan(&get, &[OsString::from("metadata")]), Plan::Direct);
    }

    fn words(arguments: &[&str]) -> Vec<OsString> {
        arguments.iter().map(OsString::from).collect()
    }

    #[test]
    fn only_a_build_that_runs_nothing_it_built_asks_to_grow() {
        for growing in [
            &["build"][..],
            &["+nightly", "--frozen", "check", "--workspace"],
            &["clippy", "--all-targets"],
            &["test", "--workspace", "--no-run"],
            &["nextest", "archive", "--workspace"],
        ] {
            assert!(compiles_only(&words(growing)), "{growing:?}");
        }
        for fixed in [
            &["test"][..],
            &["nextest", "run"],
            &["bench"],
            &["run"],
            &["machete"],
            &[],
        ] {
            assert!(!compiles_only(&words(fixed)), "{fixed:?}");
        }
    }

    #[test]
    fn a_growth_line_names_a_larger_total_within_the_want() {
        let line = r#"{"ok":true,"lease":"grown","tokens":5}"#;
        assert_eq!(grown_total(line, 1, 18), Some(5));
        assert_eq!(grown_total(line, 5, 18), None, "not larger than held");
        assert_eq!(grown_total(line, 1, 4), None, "past the want");
        assert_eq!(
            grown_total(r#"{"ok":true,"lease":"granted","tokens":5}"#, 1, 18),
            None
        );
    }

    #[test]
    fn an_owned_fifo_holds_every_token_beyond_the_first_and_takes_grown_ones() {
        let mut fifo = OwnedFifo::create(3).expect("create");
        let path = fifo.path.clone();
        fifo.fill(2).expect("grow");
        let mut reader = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open");
        let mut bytes = [0_u8; 4];
        std::io::Read::read_exact(&mut reader, &mut bytes).expect("four tokens");
        assert_eq!(&bytes, b"++++");
        drop(fifo);
        assert!(!path.exists(), "dropping the fifo unlinks it");
    }
}
