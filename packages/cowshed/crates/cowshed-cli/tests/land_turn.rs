//! `cowshed land-turn`, run as the binary a land runs: the lands of one repository take turns in
//! the order they asked, a waiter killed in line leaves the queue, and a holder killed mid-land
//! hands the turn on. Each land's command appends its workspace to an order file, then blocks
//! until the test writes to its fifo, so the test decides when each land ends and reads the queue
//! with `observe` while they wait.
#![cfg(unix)]

use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead as _, BufReader, ErrorKind, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use cowshed_core::host_dirs::support_directory;
use cowshed_core::land_turn::{TurnEventKind, land_turn_root, observe, observe_all};

const REPO: &str = "testrepo";
const POLL: Duration = Duration::from_millis(20);

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

/// `killpg(2)` to a land's process group: its cowshed and the `sh` it spawned. A group with no
/// members left is the normal end of a land, so ESRCH is not a failure.
fn send_group(pgid: u32, signal: i32) -> io::Result<()> {
    let pgid = i32::try_from(pgid).expect("a pid fits in pid_t");
    // SAFETY: killpg(2) takes no pointers.
    if unsafe { libc::killpg(pgid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

/// One `cowshed land-turn` process as a test drives it. It leads its own process group, so the
/// `sh` it spawns can be killed with it.
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
            "echo {workspace} >> {}; read x < {}",
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
        let mut command = Command::new(cowshed());
        command
            .env("HOME", &home.path)
            .args(["land-turn", repo, "--workspace", workspace]);
        if let Some(lock) = outside {
            command.arg("--outside-lock").arg(lock);
        }
        let mut child = command
            .args(["--", "sh", "-c", script])
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
        loop {
            // Non-blocking, so a fifo with no reader yet fails with ENXIO instead of hanging; the
            // command reaches it, and a land that never does is the runner's timeout to report.
            match OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(fifo)
            {
                Ok(mut writer) => {
                    writer.write_all(b"\n").expect("write the release line");
                    return;
                }
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => thread::sleep(POLL),
                Err(error) => panic!("open {}: {error}", fifo.display()),
            }
        }
    }

    /// SIGSTOP: the process keeps every lock it holds and runs no code until resumed.
    fn stop(&self) {
        send(self.pid(), libc::SIGSTOP);
    }

    fn resume(&self) {
        send(self.pid(), libc::SIGCONT);
    }

    /// `kill -9` of the cowshed process, then of its group, so the `sh` it spawned (blocked on
    /// the fifo) goes too. Reaped before returning.
    fn kill_hard(&mut self) {
        send(self.pid(), libc::SIGKILL);
        send_group(self.pid(), libc::SIGKILL).expect("kill the land's process group");
        self.child.wait().expect("reap the killed cowshed");
    }

    /// Waits for the land to end and returns its status.
    fn wait_exit(&mut self) -> ExitStatus {
        self.child.wait().expect("wait for cowshed")
    }

    fn wait_success(&mut self) {
        let status = self.wait_exit();
        self.finish_stderr();
        assert_eq!(
            status.code(),
            Some(0),
            "{} ended with {status}; stderr: {:?}",
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
        // Tears down a land the test left running, a stopped one included: SIGKILL reaches a
        // stopped process. Failures are raised unless the test is already panicking.
        let torn_down = send_group(self.pid(), libc::SIGKILL);
        if !thread::panicking() {
            torn_down.expect("kill the land's process group");
            self.child.wait().expect("reap the land");
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

/// Every `*.ticket` file left in the repository's directory.
fn ticket_files(home: &Home) -> Vec<String> {
    fs::read_dir(home.repo_dir())
        .expect("read the repository directory")
        .map(|entry| {
            let entry = entry.expect("read a directory entry");
            entry.file_name().to_string_lossy().into_owned()
        })
        .filter(|name| name.ends_with(".ticket"))
        .collect()
}

/// Waits until the queue's workspaces, in turn order, are exactly `expected`.
fn wait_for_queue(home: &Home, expected: &[&str]) {
    while queue(home) != expected {
        thread::sleep(POLL);
    }
}

/// Waits until the order file holds exactly `expected`. A line that is not the next one expected
/// is a wrong turn, and fails at once.
fn wait_for_order(home: &Home, expected: &[&str]) {
    loop {
        let ran = order(home);
        assert!(
            ran.len() <= expected.len() && ran == expected[..ran.len()],
            "turns out of order: expected {expected:?}, ran {ran:?}"
        );
        if ran.len() == expected.len() {
            return;
        }
        thread::sleep(POLL);
    }
}

/// Three lands in line: the first two ask and are then stopped, holding their tickets. The third
/// asks after the holder has gone, behind them. Stopped lands still hold their locks, so the
/// third cannot pass them however soon the holder releases: the turns follow the order of asking.
#[test]
fn a_land_that_asks_as_the_head_releases_waits_for_the_earlier_waiters() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &["H"]);
    let mut first = Land::gated(&home, "W1");
    wait_for_queue(&home, &["H", "W1"]);
    let mut second = Land::gated(&home, "W2");
    wait_for_queue(&home, &["H", "W1", "W2"]);
    first.stop();
    second.stop();
    holder.release();
    holder.wait_success();
    let mut third = Land::gated(&home, "W3");
    wait_for_queue(&home, &["W1", "W2", "W3"]);
    first.resume();
    second.resume();
    wait_for_order(&home, &["H", "W1"]);
    first.release();
    first.wait_success();
    wait_for_order(&home, &["H", "W1", "W2"]);
    second.release();
    second.wait_success();
    wait_for_order(&home, &["H", "W1", "W2", "W3"]);
    third.release();
    third.wait_success();
}

/// A holder killed mid-land never drops its ticket, but its lock dies with it: the ticket is dead,
/// and the next land in line takes the turn.
#[test]
fn a_killed_holder_passes_the_turn_on() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &["H"]);
    let mut waiter = Land::gated(&home, "W1");
    wait_for_queue(&home, &["H", "W1"]);
    holder.kill_hard();
    wait_for_order(&home, &["H", "W1"]);
    wait_for_queue(&home, &["W1"]);
    waiter.release();
    waiter.wait_success();
}

/// A waiter killed in line leaves a dead ticket that no later land waits behind: the next land
/// takes the turn when the holder releases, and a land that asks afterwards runs after it.
#[test]
fn a_waiter_killed_in_line_blocks_no_later_land() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &["H"]);
    let mut killed = Land::gated(&home, "W1");
    wait_for_queue(&home, &["H", "W1"]);
    let mut next = Land::gated(&home, "W2");
    wait_for_queue(&home, &["H", "W1", "W2"]);
    killed.kill_hard();
    wait_for_queue(&home, &["H", "W2"]);
    holder.release();
    holder.wait_success();
    wait_for_order(&home, &["H", "W2"]);
    let mut last = Land::gated(&home, "W3");
    wait_for_queue(&home, &["W2", "W3"]);
    next.release();
    next.wait_success();
    wait_for_order(&home, &["H", "W2", "W3"]);
    last.release();
    last.wait_success();
    assert_eq!(
        ticket_files(&home),
        Vec::<String>::new(),
        "tickets left behind"
    );
}

/// Measured 2026-10-21: a land queued for 635 s vanished with its agent's session and nothing
/// recorded it, so it read as running while it did not exist. A waiter killed in line is
/// recorded as abandoned by the next reader of the queue, and `gateway status` lists it; the
/// holder's turn is recorded from its place in line to how its command ended.
#[test]
fn a_waiter_killed_in_line_is_recorded_as_abandoned() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &["H"]);
    let mut killed = Land::gated(&home, "W1");
    wait_for_queue(&home, &["H", "W1"]);
    let killed_pid = killed.pid();
    killed.kill_hard();
    holder.release();
    holder.wait_success();

    // No land asked after W1, so the status read is the first to find its dead ticket.
    let root = home.repo_dir();
    let root = root.parent().expect("the land-turn root");
    let turns = observe_all(root, 0).expect("observe the queues");
    let repo = turns
        .iter()
        .find(|turns| turns.repo == REPO)
        .expect("the repository is listed for its abandoned ticket");
    assert!(repo.tickets.is_empty(), "no live ticket: {repo:?}");
    let [abandoned] = repo.abandoned.as_slice() else {
        panic!("one abandoned ticket: {repo:?}");
    };
    assert_eq!(
        (abandoned.ticket.workspace.as_str(), abandoned.ticket.pid),
        ("W1", killed_pid)
    );
    assert_eq!(abandoned.event, TurnEventKind::Abandoned);

    let log = fs::read_to_string(home.repo_dir().join("turns.log")).expect("the turn log");
    let holder_events: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("a JSON line"))
        .filter(|event: &serde_json::Value| event["ticket"]["workspace"] == "H")
        .collect();
    let kinds: Vec<&str> = holder_events
        .iter()
        .map(|event| event["event"].as_str().expect("an event kind"))
        .collect();
    assert_eq!(kinds, ["queued", "taken", "finished"], "{log}");
    assert_eq!(holder_events[2]["outcome"], "exited 0", "{log}");
}

/// The line a waiting land prints names the holder, its pid, and how many lands are ahead.
#[test]
fn the_waiting_line_names_the_holder() {
    let home = Home::new();
    let mut holder = Land::gated(&home, "H");
    wait_for_order(&home, &["H"]);
    let mut waiter = Land::gated(&home, "W1");
    let line = waiter.stderr_line(&format!(
        "cowshed: land turn testrepo: 1 ahead; H holds it (pid {}, asked ",
        holder.pid()
    ));
    assert!(
        line.ends_with(')'),
        "the line ends with the asked time: {line}"
    );
    holder.release();
    holder.wait_success();
    wait_for_order(&home, &["H", "W1"]);
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
    wait_for_order(&home, &["H"]);
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
    wait_for_order(&home, &["H", "W"]);
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
    assert_eq!(status.code(), Some(7), "stderr: {:?}", land.seen);
}
