//! `cowshed land-turn`, run as the binary a land runs: the lands of one repository take turns in
//! the order they asked, a waiter killed in line leaves the queue, and a holder killed mid-land
//! hands the turn on. Each land's command appends its workspace to an order file, announces readiness, then blocks
//! until the test writes to its fifo, so the test decides when each land ends. The queue is the
//! gateway's state: each test runs its own gateway and reads the queue with `land-turns` while
//! the lands wait.
#![cfg(unix)]

use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
use std::ffi::CString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufRead as _, BufReader, ErrorKind, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use cowshed_core::api::dto::{ExitStatus, LandTurnQueue};
use cowshed_core::runtime::job_groups::{await_exit_unreaped, signal_unreaped_group};

#[path = "../../cowshed-core/tests/support/temp_parent.rs"]
mod temp_parent;
#[path = "../../cowshed-core/tests/support/temp_root.rs"]
mod temp_root;

#[path = "support/test_gateway.rs"]
mod test_gateway;

use test_gateway::TestGateway;

const REPO: &str = "testrepo";
/// How long a test waits for any one step before it fails naming that step: the run's no-progress
/// backstop, the one bound that decides a hang (`cowshed_os::test_backstop`), never a deadline of
/// the test's own.
const BACKSTOP: Duration = cowshed_os::test_backstop::NO_PROGRESS;

/// The `cowshed` binary under test, read when the test runs (see `disk_command.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A fresh HOME for one test: a directory under the granted temporary directory, removed when the
/// test ends, and the gateway whose land-turn queue its lands join.
struct Home {
    path: PathBuf,
    gateway: TestGateway,
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
        Self {
            path,
            gateway: TestGateway::start(),
        }
    }

    fn order(&self) -> PathBuf {
        self.path.join("order")
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
    /// Writing to this releases the land's command; `None` for a command the test does not gate.
    fifo: Option<PathBuf>,
    stderr: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Land {
    /// A land whose command appends `workspace` to the order file, then blocks until released.
    fn gated(home: &Home, workspace: &str) -> Self {
        Self::gated_in(home, REPO, workspace, None)
    }

    /// [`Land::gated`] in `repo`'s queue, its head taking `outside` too when given.
    fn gated_in(home: &Home, repo: &str, workspace: &str, outside: Option<&Path>) -> Self {
        let fifo = home.path.join(format!("{workspace}.fifo"));
        let c_path = CString::new(fifo.as_os_str().as_bytes()).expect("a fifo path has no NUL");
        // SAFETY: mkfifo(3) on a path inside this test's own HOME.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(
            rc,
            0,
            "mkfifo {}: {}",
            fifo.display(),
            io::Error::last_os_error()
        );
        let script = format!(
            "echo {workspace} >> {}; echo command-started >&2; read x < {}",
            home.order().display(),
            fifo.display()
        );
        let mut land = Self::spawn_in(home, repo, workspace, outside, &script);
        land.fifo = Some(fifo);
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
            .args(["land-turn", repo, "--workspace", workspace])
            .arg("--gateway-socket")
            .arg(&home.gateway.socket);
        if let Some(lock) = outside {
            command.arg("--outside-lock").arg(lock);
        }
        command
    }

    fn start(workspace: &str, command: &mut Command) -> Self {
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("spawn cowshed land-turn");
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
            fifo: None,
            stderr: lines,
            seen: Vec::new(),
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Lets the land's command run on: its `read` takes the line and the command exits.
    fn release(&self) {
        let fifo = self
            .fifo
            .as_ref()
            .expect("a gated land has a fifo to release");
        OpenOptions::new()
            .write(true)
            .open(fifo)
            .and_then(|mut writer| writer.write_all(b"\n"))
            .expect("release the command once it opens its fifo");
    }

    /// SIGSTOP: the process keeps every lock it holds and runs no code until resumed.
    fn stop(&self) {
        send(self.pid(), libc::SIGSTOP);
    }

    fn resume(&self) {
        send(self.pid(), libc::SIGCONT);
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

    /// The first stderr line starting with `needle`, waiting up to [`BACKSTOP`] for it.
    fn stderr_line(&mut self, needle: &str) -> String {
        loop {
            if let Some(line) = self.seen.iter().find(|line| line.starts_with(needle)) {
                return line.clone();
            }
            match self.stderr.recv_timeout(BACKSTOP) {
                Ok(line) => self.seen.push(line),
                Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                    "{}: no stderr line starting with {needle:?} in {BACKSTOP:?}; stderr was {:?}",
                    self.workspace, self.seen
                ),
                Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                    "{}: stderr ended with no line starting with {needle:?}; stderr was {:?}",
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

/// The land queues the host gateway holds, read through its `land-turns` op: every queue with a
/// live ticket, and every queue with an abandonment at or after `abandoned_since_unix_ms`.
fn queues(home: &Home, abandoned_since_unix_ms: u64) -> Vec<LandTurnQueue> {
    home.gateway.land_turns(abandoned_since_unix_ms)
}

/// The workspaces of the live tickets in the repository's queue, in turn order.
fn queue(home: &Home) -> Vec<String> {
    queues(home, u64::MAX)
        .into_iter()
        .find(|turns| turns.repo.as_str() == REPO)
        .map_or_else(Vec::new, |turns| {
            turns
                .tickets
                .into_iter()
                .map(|ticket| ticket.workspace)
                .collect()
        })
}

/// The waiter's queued reply acknowledges its place before the queue snapshot is asserted.
fn wait_for_queue(home: &Home, land: &mut Land, expected: &[&str]) {
    let ahead = expected
        .iter()
        .position(|workspace| *workspace == land.workspace.as_str())
        .expect("the waiter is in the expected queue");
    let holder = expected.first().expect("a queued land has a holder");
    land.stderr_line(&format!(
        "cowshed: land turn {REPO}: {ahead} ahead; {holder} holds it"
    ));
    assert_eq!(queue(home), expected, "the acknowledged queue is in order");
}

/// The command announces readiness after appending its workspace to the order file.
fn wait_for_order(home: &Home, land: &mut Land, expected: &[&str]) {
    land.stderr_line("command-started");
    assert_eq!(order(home), expected, "turns ran out of order");
}

/// Three lands in line: the first two ask and are then stopped, holding their tickets. The third
/// asks after the holder has gone, behind them. Stopped lands keep their tickets, so the
/// third cannot pass them however soon the holder releases: the turns follow the order of asking.
#[test]
fn a_land_that_asks_as_the_head_releases_waits_for_the_earlier_waiters() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &mut holder, &["H"]);
    let mut first = Land::gated(&home, "W1");
    wait_for_queue(&home, &mut first, &["H", "W1"]);
    let mut second = Land::gated(&home, "W2");
    wait_for_queue(&home, &mut second, &["H", "W1", "W2"]);
    first.stop();
    second.stop();
    holder.release();
    holder.wait_success();
    let mut third = Land::gated(&home, "W3");
    wait_for_queue(&home, &mut third, &["W1", "W2", "W3"]);
    first.resume();
    second.resume();
    wait_for_order(&home, &mut first, &["H", "W1"]);
    first.release();
    first.wait_success();
    wait_for_order(&home, &mut second, &["H", "W1", "W2"]);
    second.release();
    second.wait_success();
    wait_for_order(&home, &mut third, &["H", "W1", "W2", "W3"]);
    third.release();
    third.wait_success();
}

/// A holder killed mid-land never drops its ticket, but its lock dies with it: the ticket is dead,
/// and the next land in line takes the turn.
#[test]
fn a_killed_holder_passes_the_turn_on() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &mut holder, &["H"]);
    let mut waiter = Land::gated(&home, "W1");
    wait_for_queue(&home, &mut waiter, &["H", "W1"]);
    holder.kill_hard();
    wait_for_order(&home, &mut waiter, &["H", "W1"]);
    assert_eq!(queue(&home), ["W1"], "the waiter holds the turn");
    waiter.release();
    waiter.wait_success();
}

/// A waiter killed in line leaves a dead ticket that no later land waits behind: the next land
/// takes the turn when the holder releases, and a land that asks afterwards runs after it.
#[test]
fn a_waiter_killed_in_line_blocks_no_later_land() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &mut holder, &["H"]);
    let mut killed = Land::gated(&home, "W1");
    wait_for_queue(&home, &mut killed, &["H", "W1"]);
    let mut next = Land::gated(&home, "W2");
    wait_for_queue(&home, &mut next, &["H", "W1", "W2"]);
    killed.kill_hard();
    wait_for_queue(&home, &mut next, &["H", "W2"]);
    holder.release();
    holder.wait_success();
    wait_for_order(&home, &mut next, &["H", "W2"]);
    let mut last = Land::gated(&home, "W3");
    wait_for_queue(&home, &mut last, &["W2", "W3"]);
    next.release();
    next.wait_success();
    wait_for_order(&home, &mut last, &["H", "W2", "W3"]);
    last.release();
    last.wait_success();
    assert!(queue(&home).is_empty(), "tickets left in the queue");
}

/// Measured 2026-10-21: a land queued for 635 s vanished with its agent's session and nothing
/// recorded it, so it read as running while it did not exist. A waiter killed in line is
/// recorded as abandoned, and `gateway status` lists it.
#[test]
fn a_waiter_killed_in_line_is_recorded_as_abandoned() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &mut holder, &["H"]);
    let mut killed = Land::gated(&home, "W1");
    wait_for_queue(&home, &mut killed, &["H", "W1"]);
    let killed_pid = killed.pid();
    killed.kill_hard();
    holder.release();
    holder.wait_success();

    let mut probe = Land::gated(&home, "probe");
    wait_for_order(&home, &mut probe, &["H", "probe"]);
    let turns = queues(&home, 0);
    let repo = turns
        .iter()
        .find(|turns| turns.repo.as_str() == REPO)
        .expect("the repository is listed for its abandoned ticket");
    let [ticket] = repo.tickets.as_slice() else {
        panic!("only the probe holds a live ticket: {repo:?}");
    };
    assert_eq!(ticket.workspace, "probe");
    let [abandoned] = repo.abandoned.as_slice() else {
        panic!("one abandoned ticket: {repo:?}");
    };
    assert_eq!(
        (abandoned.ticket.workspace.as_str(), abandoned.ticket.pid),
        ("W1", killed_pid)
    );
    assert_eq!(abandoned.reason, "it left the queue before its turn");
    probe.release();
    probe.wait_success();
}

/// The line a waiting land prints names the holder, its pid, its bound command's pid, how many
/// lands are ahead, and when the holder asked.
#[test]
fn the_waiting_line_names_the_holder() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &mut holder, &["H"]);
    let mut waiter = Land::gated(&home, "W1");
    let line = waiter.stderr_line(&format!(
        "cowshed: land turn testrepo: 1 ahead; H holds it (pid {}, command pid ",
        holder.pid()
    ));
    assert!(
        line.contains(", asked ") && line.ends_with(')'),
        "the line ends with the asked time: {line}"
    );
    holder.release();
    holder.wait_success();
    wait_for_order(&home, &mut waiter, &["H", "W1"]);
    waiter.release();
    waiter.wait_success();
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
    wait_for_order(&home, &mut holder, &["H"]);
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
        line.contains("land-turn otherrepo --workspace H ")
            && line.contains(&format!("--outside-lock {} -- sh -c", lock.display())),
        "the holder's command is named: {line}"
    );
    holder.release();
    holder.wait_success();
    wait_for_order(&home, &mut waiter, &["H", "W"]);
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
    wait_for_order(&home, &mut holder, &["H"]);
    let mut waiter = Land::gated(&home, "W1");
    wait_for_queue(&home, &mut waiter, &["H", "W1"]);
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
    wait_for_order(&home, &mut waiter, &["H", "W1"]);
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
    let output = Land::command(&home, REPO, "W", None)
        .args(["--", "sh", "-c", &script])
        .output_bounded()
        .expect("run and reap the policy command with its output drained");
    assert!(output.status.success(), "{output:?}");
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
    wait_for_order(&home, &mut holder, &["H"]);
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
    let entered = queues(&home, 0).iter().any(|turns| {
        turns
            .tickets
            .iter()
            .any(|ticket| ticket.workspace == "invalid")
            || turns
                .abandoned
                .iter()
                .any(|abandoned| abandoned.ticket.workspace == "invalid")
    });
    assert!(!entered, "invalid configuration entered the land queue");
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
    let was_abandoned = queues(&home, 0).iter().any(|turns| {
        turns
            .abandoned
            .iter()
            .any(|abandoned| abandoned.ticket.workspace == "missing")
    });
    assert!(!was_abandoned, "a failed spawn was abandoned, not ended");
}

/// A process the land's command leaves running holds no turn: the command exits, its descendant
/// keeps running, and the next land takes the turn at once.
#[test]
fn a_process_the_command_leaves_running_holds_no_turn() {
    let home = Home::new();
    let descendant = home.path.join("descendant");
    let script = format!(
        "sleep 600 </dev/null >/dev/null 2>&1 & echo $! > {}; exit 0",
        descendant.display()
    );
    let mut first = Land::spawn(&home, "first", &script);
    first.wait_success();
    let pid = fs::read_to_string(&descendant)
        .expect("read the descendant's pid")
        .trim()
        .parse::<u32>()
        .expect("the descendant's pid");
    let exited = cowshed_os::process::ExitWatch::of(pid)
        .expect("watch the owned descendant's exit")
        .expect("the command left its descendant running");
    let mut second = Land::spawn(&home, "second", "true");
    second.wait_success();
    let alive = !exited
        .within(Duration::ZERO)
        .expect("read the descendant's exit event");
    first.kill_hard();
    assert!(
        exited
            .within(BACKSTOP)
            .expect("wait for the descendant's exit"),
        "the owned descendant did not exit before the no-progress backstop"
    );
    assert!(alive, "the first land's command left no process running");
    assert!(
        queue(&home).is_empty(),
        "a finished land left a ticket in the queue"
    );
}
