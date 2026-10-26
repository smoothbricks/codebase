//! `cowshed land-turn`, run as the binary a land runs (05_gateway.md "Land turns"). The queue
//! policy (ticket verdicts from the kernel lock alone, a land's place, report cadence) is
//! cowshed-core `land_turn`'s, table-tested there; these keep one real killed-holder handoff, the
//! command's exit status and the shell boundaries around it. Each gated land's command appends its
//! workspace to an order file, says `started` on stdout, then blocks reading its release gate, so
//! the test decides when each land ends. The test advances only on what a land says: its
//! command's `started` line once it holds the turn, or its waiting report once it has its ticket;
//! then it reads the queue and the order file once.
#![cfg(unix)]

use cowshed_os::test_backstop::ChildBackstop as _;
use std::fs::{self, File, TryLockError};
use std::io::{self, BufRead as _, BufReader, ErrorKind};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;

use cowshed_core::api::dto::ExitStatus;
use cowshed_core::host_dirs::support_directory;
use cowshed_core::land_turn::{TurnEventKind, land_turn_root, observe, observe_all};
use cowshed_core::runtime::job_groups::{await_exit_unreaped, signal_unreaped_group};

const REPO: &str = "testrepo";

#[path = "support/release_gate.rs"]
mod release_gate;
use release_gate::Gate;

/// The `cowshed` binary under test, read when the test runs (see `disk_command.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A fresh HOME for one test: a directory under the granted temporary directory, removed when the
/// test ends. It holds no socket, so its length does not matter.
struct Home {
    path: PathBuf,
}

impl Home {
    fn new() -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("cowshed-land-turn-{}-{n}", std::process::id()));
        fs::create_dir_all(&path).expect("create the test HOME");
        // cowshed requires HOME to be canonical; the temporary directory may be spelled through a
        // link (macOS's /tmp is one to /private/tmp).
        let path = fs::canonicalize(&path).expect("canonicalize the test HOME");
        Self { path }
    }

    fn order(&self) -> PathBuf {
        self.path.join("order")
    }

    /// The repository's directory, where its tickets live.
    fn repo_dir(&self) -> PathBuf {
        land_turn_root(&support_directory(&self.path)).join(REPO)
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        let removed = fs::remove_dir_all(&self.path);
        if !thread::panicking() {
            removed.expect("remove the test HOME");
        }
    }
}

/// `kill(2)` to a process this test spawned. The status is checked: a signal that did not land
/// would let a test pass for the wrong reason.
fn send(pid: u32, signal: i32) {
    let pid = i32::try_from(pid).expect("a pid fits in pid_t");
    // SAFETY: kill(2) takes no pointers; `pid` is a child this test spawned.
    let rc = unsafe { libc::kill(pid, signal) };
    assert_eq!(
        rc,
        0,
        "kill({pid}, {signal}): {}",
        io::Error::last_os_error()
    );
}

/// One `cowshed land-turn` process as a test drives it. It leads its own process group and keeps
/// its leader unreaped until Drop, so cleanup cannot signal a reused process-group id.
struct Land {
    workspace: String,
    child: Child,
    /// Releases the land's command; `None` for a command the test does not gate.
    gate: Option<Gate>,
    /// The command's stdout, where a gated command says `started` once it holds the turn.
    stdout: BufReader<ChildStdout>,
    stderr: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Land {
    /// A land whose command appends `workspace` to the order file, says `started`, then blocks
    /// until released.
    fn gated(home: &Home, workspace: &str) -> Self {
        Self::gated_in(home, REPO, workspace, None)
    }

    /// [`Land::gated`] in `repo`'s queue, its head taking `outside` too when given.
    fn gated_in(home: &Home, repo: &str, workspace: &str, outside: Option<&Path>) -> Self {
        let gate = Gate::new(home.path.join(format!("{workspace}.fifo")));
        let script = format!(
            "echo {workspace} >> {}; echo started; read x < {}",
            home.order().display(),
            gate.path().display()
        );
        let mut land = Self::spawn_in(home, repo, workspace, outside, &script);
        land.gate = Some(gate);
        land
    }

    fn spawn(home: &Home, workspace: &str, script: &str) -> Self {
        Self::spawn_in(home, REPO, workspace, None, script)
    }

    fn spawn_in(
        home: &Home,
        repo: &str,
        workspace: &str,
        outside: Option<&Path>,
        script: &str,
    ) -> Self {
        Self::start(
            workspace,
            Self::command(home, repo, workspace, outside).args(["--", "sh", "-c", script]),
        )
    }

    fn command(home: &Home, repo: &str, workspace: &str, outside: Option<&Path>) -> Command {
        let mut command = Command::new(cowshed());
        command
            .env("HOME", &home.path)
            .args(["land-turn", repo, "--workspace", workspace]);
        if let Some(lock) = outside {
            command.arg("--outside-lock").arg(lock);
        }
        command
    }

    fn start(workspace: &str, command: &mut Command) -> Self {
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("spawn cowshed land-turn");
        let stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
        let stderr = child.stderr.take().expect("stderr is piped");
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            workspace: workspace.to_owned(),
            child,
            gate: None,
            stdout,
            stderr: lines,
            seen: Vec::new(),
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Lets the land's command run on: its `read` takes the line and the command exits.
    fn release(&self) {
        self.gate
            .as_ref()
            .expect("a gated land has a gate to release")
            .release();
    }

    /// Waits for the gated command to say it started, which it does only holding the turn, then
    /// requires the order file to be exactly `expected`, ending with this land: a land that ran out
    /// of turn shows there.
    fn ran(&mut self, home: &Home, expected: &[&str]) {
        assert_eq!(expected.last(), Some(&self.workspace.as_str()));
        let mut line = String::new();
        let read = self
            .stdout
            .read_line(&mut line)
            .expect("read the command's stdout");
        assert!(
            read > 0,
            "{}: the command ended before it started; stderr {:?}",
            self.workspace,
            self.seen
        );
        assert_eq!(line, "started\n", "{}", self.workspace);
        assert_eq!(order(home), expected, "turns out of order");
    }

    /// Waits for this land to report its place behind `expected`'s head, which it does once its
    /// ticket is taken and again whenever the live ticket just ahead of it ends, then requires the
    /// queue to be exactly `expected`.
    fn queued(&mut self, home: &Home, expected: &[&str]) {
        let ahead = expected
            .iter()
            .position(|workspace| *workspace == self.workspace)
            .expect("the land is in the expected queue");
        self.stderr_line(&format!(
            "land turn {REPO}: {ahead} ahead; {} holds it",
            expected[0]
        ));
        assert_eq!(queue(home), expected);
    }

    /// Kill the owned group without reaping its leader; Drop alone relinquishes the group id.
    fn kill_hard(&mut self) {
        signal_unreaped_group(
            i32::try_from(self.pid()).expect("a pid fits in pid_t"),
            libc::SIGKILL,
        )
        .expect("kill the land's process group");
        self.wait_exit();
    }

    /// Observe the exit while retaining the leader and therefore ownership of its group id.
    fn wait_exit(&self) -> ExitStatus {
        await_exit_unreaped(i32::try_from(self.pid()).expect("a pid fits in pid_t"))
            .expect("observe cowshed exit without reaping")
    }

    fn wait_success(&mut self) {
        let status = self.wait_exit();
        self.finish_stderr();
        assert_eq!(
            status,
            ExitStatus::Exited { code: 0 },
            "{} ended with {status:?}; stderr: {:?}",
            self.workspace,
            self.seen
        );
    }

    /// Reads what the land wrote to stderr, to the end of the pipe. Call only once the land has
    /// ended: the pipe then closes, and the reader thread disconnects the channel. A process the
    /// land left behind holding the pipe keeps this waiting, which the runner reports as a leak.
    fn finish_stderr(&mut self) {
        while let Ok(line) = self.stderr.recv() {
            self.seen.push(line);
        }
    }

    /// The first stderr line containing `needle`, waiting for it.
    fn stderr_line(&mut self, needle: &str) -> String {
        loop {
            if let Some(line) = self.seen.iter().find(|line| line.contains(needle)) {
                return line.clone();
            }
            match self.stderr.recv() {
                Ok(line) => self.seen.push(line),
                Err(error) => panic!(
                    "{}: stderr ended with no line containing {needle:?} ({error}); stderr was {:?}",
                    self.workspace, self.seen
                ),
            }
        }
    }
}

impl Drop for Land {
    fn drop(&mut self) {
        let pid = i32::try_from(self.pid()).expect("a pid fits in pid_t");
        let result = signal_unreaped_group(pid, libc::SIGKILL)
            .and_then(|()| self.child.wait_bounded().map(|_| ()));
        if let Err(error) = result {
            if thread::panicking() {
                cowshed_os::eprintln!("land fixture process group {pid} cleanup failed: {error}");
            } else {
                panic!("land fixture process group {pid} cleanup failed: {error}");
            }
        }
    }
}

/// The workspaces that ran, in order, one per line of the order file.
fn order(home: &Home) -> Vec<String> {
    match fs::read_to_string(home.order()) {
        Ok(text) => text.lines().map(str::to_owned).collect(),
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read the order file: {error}"),
    }
}

/// The workspaces of the live tickets in the repository's queue, in turn order.
fn queue(home: &Home) -> Vec<String> {
    observe(&home.repo_dir())
        .expect("observe the queue")
        .into_iter()
        .map(|ticket| ticket.workspace)
        .collect()
}

/// A holder killed mid-land never drops its ticket, but its lock dies with it: the ticket is dead,
/// and the next land in line takes the turn.
#[test]
fn a_killed_holder_passes_the_turn_on() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    holder.ran(&home, &["H"]);
    let holder_pid = holder.pid();
    let mut waiter = Land::gated(&home, "W1");
    waiter.queued(&home, &["H", "W1"]);
    holder.kill_hard();
    waiter.ran(&home, &["H", "W1"]);
    assert_eq!(queue(&home), ["W1"], "the killed holder's ticket is dead");
    waiter.release();
    waiter.wait_success();

    // The handoff's account, as `gateway status` reads it: the successor found the killed
    // holder's ticket unlocked and unmarked and recorded it abandoned, and its own turn ran from
    // its place in line to its command's status.
    let root = home.repo_dir();
    let turns = observe_all(root.parent().expect("the land-turn root"), Some(0))
        .expect("observe the queues");
    let repo = turns
        .iter()
        .find(|turns| turns.repo == REPO)
        .expect("the repository is listed for its abandoned ticket");
    assert!(repo.tickets.is_empty(), "no live ticket: {repo:?}");
    let [abandoned] = repo.abandoned.as_slice() else {
        panic!("one abandoned ticket: {repo:?}");
    };
    assert_eq!(abandoned.event, TurnEventKind::Abandoned);
    assert_eq!(
        (abandoned.ticket.workspace.as_str(), abandoned.ticket.pid),
        ("H", holder_pid)
    );
    let log = fs::read_to_string(home.repo_dir().join("turns.log")).expect("the turn log");
    let successor: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("a JSON line"))
        .filter(|event: &serde_json::Value| event["ticket"]["workspace"] == "W1")
        .collect();
    let kinds: Vec<&str> = successor
        .iter()
        .map(|event| event["event"].as_str().expect("an event kind"))
        .collect();
    assert_eq!(kinds, ["queued", "taken", "finished"], "{log}");
    assert_eq!(successor[2]["outcome"], "exited 0", "{log}");
}

/// Measured 2026-10-21: a land waited 1002 s for the outside lock having said once that "a land
/// that did not ask for a turn" held it, naming no process. The head waiting for the outside lock
/// names the process holding it, by pid and command. The holder is a real process outside this
/// queue: the head of another repository's queue, which takes the same outside lock.
#[test]
fn a_head_waiting_for_the_outside_lock_names_the_process_holding_it() {
    let home = Home::new();
    let lock = home.path.join("outside.lock");
    let mut holder = Land::gated_in(&home, "otherrepo", "H", Some(&lock));
    holder.ran(&home, &["H"]);
    let mut waiter = Land::gated_in(&home, REPO, "W", Some(&lock));
    let line = waiter.stderr_line("cowshed: land turn testrepo: next, waiting for ");
    let named = format!(
        "cowshed: land turn testrepo: next, waiting for {}, held outside the queue; open in pid {} (",
        lock.display(),
        holder.pid()
    );
    assert!(
        line.starts_with(&named),
        "the outside wait names the holder's pid: {line}"
    );
    assert!(
        line.contains("land-turn otherrepo --workspace H --outside-lock"),
        "the holder's command is named: {line}"
    );
    holder.release();
    holder.wait_success();
    waiter.ran(&home, &["H", "W"]);
    waiter.release();
    waiter.wait_success();
}

/// A land ends with its command's status.
#[test]
fn a_land_exits_with_its_commands_status() {
    let home = Home::new();
    let mut land = Land::spawn(&home, "W", "exit 7");
    let status = land.wait_exit();
    land.finish_stderr();
    assert_eq!(
        status,
        ExitStatus::Exited { code: 7 },
        "stderr: {:?}",
        land.seen
    );
}

/// A land whose cowshed alone is killed keeps its turn and its outside lock while its command
/// runs: the command inherited both descriptors, so the kernel releases neither before the command
/// ends, and no later land runs beside it.
#[test]
fn a_land_killed_without_its_command_keeps_its_turn_and_outside_lock_until_the_command_ends() {
    let home = Home::new();
    let outside = home.path.join("outside.lock");
    let mut holder = Land::gated_in(&home, REPO, "H", Some(&outside));
    holder.ran(&home, &["H"]);
    let mut waiter = Land::gated(&home, "W1");
    waiter.queued(&home, &["H", "W1"]);
    send(holder.pid(), libc::SIGKILL);
    holder.wait_exit();
    let outside_held = File::open(&outside)
        .expect("open the outside lock")
        .try_lock();
    assert!(
        matches!(outside_held, Err(TryLockError::WouldBlock)),
        "the outside lock went with the killed cowshed while its command ran"
    );
    assert_eq!(
        queue(&home),
        ["H", "W1"],
        "the turn went with the killed cowshed while its command ran"
    );
    assert_eq!(order(&home), ["H"], "a land ran beside the held turn");
    holder.release();
    waiter.ran(&home, &["H", "W1"]);
    waiter.release();
    waiter.wait_success();
}

/// A land receives the same automatic Git background-work settings and managed Nx no-start
/// policy as every other held command.
#[test]
fn a_land_holding_its_turn_receives_the_git_and_managed_nx_policies() {
    let home = Home::new();
    let seen = home.path.join("seen");
    let script = format!(
        "{{ git config --get gc.auto; git config --get maintenance.auto; git config --get core.fsmonitor; \
         echo \"$NX_DAEMON_START\"; }} > {}",
        seen.display()
    );
    let mut land = Land::spawn(&home, "W", &script);
    let status = land.wait_exit();
    land.finish_stderr();
    assert_eq!(
        status,
        ExitStatus::Exited { code: 0 },
        "stderr: {:?}",
        land.seen
    );
    assert_eq!(
        fs::read_to_string(&seen).expect("read what the land saw"),
        "0\nfalse\nfalse\nnever\n"
    );
}

#[test]
fn land_turn_with_an_outside_lock_preserves_non_utf8_child_arguments() {
    let home = Home::new();
    let outside = home.path.join("outside.lock");
    let seen = home.path.join("raw-argv");
    let bytes = b"raw-\xff-\xfe";
    let mut land = Land::start(
        "raw",
        Land::command(&home, REPO, "raw", Some(&outside))
            .args(["--", "sh", "-c", r#"printf '%s' "$1" > "$2""#, "step"])
            .arg(std::ffi::OsStr::from_bytes(bytes))
            .arg(&seen),
    );
    land.wait_success();
    assert_eq!(fs::read(&seen).expect("read child argument"), bytes);
    assert!(
        File::open(&outside)
            .expect("open the outside lock")
            .try_lock()
            .is_ok(),
        "the ended command retained the outside lock"
    );
}

#[test]
fn a_land_refuses_invalid_git_config_before_asking_for_a_turn() {
    let home = Home::new();
    let outside = home.path.join("outside.lock");
    let mut holder = Land::gated_in(&home, REPO, "H", Some(&outside));
    holder.ran(&home, &["H"]);
    let ran = home.path.join("invalid-ran");
    let mut refused = Land::start(
        "invalid",
        Land::command(&home, REPO, "invalid", Some(&outside))
            .env("GIT_CONFIG_COUNT", "not-a-count")
            .args(["--", "sh", "-c", r#": > "$1""#, "step"])
            .arg(&ran),
    );
    assert_eq!(refused.wait_exit(), ExitStatus::Exited { code: 104 });
    refused.finish_stderr();
    assert!(
        refused
            .seen
            .iter()
            .any(|line| line.contains("GIT_CONFIG_COUNT")),
        "{:?}",
        refused.seen
    );
    assert_eq!(queue(&home), ["H"], "invalid command took a queue place");
    assert!(!ran.exists(), "invalid configuration started the command");
    let log = fs::read_to_string(home.repo_dir().join("turns.log")).expect("turn log");
    assert!(
        log.lines().all(|line| {
            let event: serde_json::Value = serde_json::from_str(line).expect("turn event");
            event["ticket"]["workspace"] != "invalid"
        }),
        "invalid configuration entered the turn history: {log}"
    );
    holder.release();
    holder.wait_success();
}

#[test]
fn a_land_spawn_failure_records_that_no_command_started() {
    let home = Home::new();
    let missing = home.path.join("no-such-executable");
    let mut land = Land::start(
        "missing",
        Land::command(&home, REPO, "missing", None)
            .arg("--")
            .arg(&missing),
    );
    assert_eq!(land.wait_exit(), ExitStatus::Exited { code: 104 });
    land.finish_stderr();
    assert!(queue(&home).is_empty(), "failed spawn retained its ticket");
    let log = fs::read_to_string(home.repo_dir().join("turns.log")).expect("turn log");
    let events: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("turn event"))
        .collect();
    let finished = events.last().expect("recorded turn outcome");
    assert_eq!(finished["event"], "finished", "{log}");
    assert!(
        finished["outcome"]
            .as_str()
            .expect("recorded outcome")
            .starts_with("did not start: cannot run "),
        "{log}"
    );
}
