//! Script jobs against the real exec host binary, driven over its own protocol.
//!
//! The host runs unsandboxed here: these tests are about what brush and the fork do with a
//! job's process group, streams and status, which no sandbox changes. The workspace has no
//! `.envrc`, so the host holds exactly the environment it was started with.

use std::io::Read as _;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use cowshed_core::api::{ExitStatus, ScriptCommand, ScriptValue};
use cowshed_core::runtime::job_groups::Birth;
use cowshed_core::runtime::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, CONTROL_DESCRIPTOR, FrameReader, FrameWriter, REPLY_EXITED,
    REPLY_HOST_FAILED, REPLY_RELEASED, REPLY_SCRIPT_SYNTAX, REPLY_STARTED, REQUEST_APPROVE,
    REQUEST_RELEASE, REQUEST_SCRIPT, REQUEST_SIGNAL, read_frame, send_with_descriptors,
};
use cowshed_core::script::{Binding, RenderedScript, render};

/// The exec host binary under test, read when the test runs. `env!` would bake in the path of
/// the checkout that compiled this test, but the test runs from a cached nextest archive that
/// is relocated into every checkout, and nextest re-points `CARGO_BIN_EXE_cowshed-shell-host`
/// at the extracted binary only at runtime.
fn shell_host() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed-shell-host").expect(
            "cargo and nextest set CARGO_BIN_EXE_cowshed-shell-host for an integration test",
        ),
    )
}

struct Host {
    child: std::process::Child,
    control: std::os::unix::net::UnixStream,
    directory: PathBuf,
}

impl Host {
    /// A host with this test's own PATH, as a supervisor gives it the tools it found.
    fn start(label: &str) -> Self {
        Self::start_with_path(label, &std::env::var_os("PATH").expect("PATH"))
    }

    fn start_with_path(label: &str, path: &std::ffi::OsStr) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-script-host-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&directory).expect("scratch directory");
        let directory = std::fs::canonicalize(directory).expect("canonical scratch");
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let theirs_raw = theirs.as_raw_fd();
        let mut command = std::process::Command::new(shell_host());
        command
            .env_clear()
            .env("PATH", path)
            .env("HOME", &directory)
            .current_dir(&directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        // SAFETY: dup2/fcntl between fork and exec touch only the descriptor table.
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(theirs_raw, CONTROL_DESCRIPTOR) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().expect("start the exec host");
        drop(theirs);
        Self {
            child,
            control: ours,
            directory,
        }
    }

    fn run(&mut self, script: &RenderedScript) -> Ran {
        run_on(&self.control, &self.directory, script, None)
    }

    /// Run `script`, sending the host `signal` for it once it has started.
    fn run_signalled(&mut self, script: &RenderedScript, signal: i32) -> Ran {
        run_on(&self.control, &self.directory, script, Some(signal))
    }
}

/// Ask the host to signal the command it holds.
fn send_signal(control: &std::os::unix::net::UnixStream, signal: i32) {
    let frame = FrameWriter::new(REQUEST_SIGNAL)
        .i32(signal)
        .finish()
        .unwrap();
    std::io::Write::write_all(&mut &*control, &frame).expect("send the signal");
}

/// The command's exit, which the host reports once its leader exits.
fn read_exit(control: &std::os::unix::net::UnixStream) -> ExitStatus {
    let (last, _) = read_frame(control).unwrap().expect("an exit");
    let (tag, mut fields) = FrameReader::new(&last).unwrap();
    match tag {
        REPLY_EXITED => {
            let exit = fields.exit().unwrap();
            fields.finish().unwrap();
            exit
        }
        REPLY_HOST_FAILED => panic!(
            "the host failed instead of reporting the exit: {}",
            String::from_utf8_lossy(fields.bytes().unwrap())
        ),
        other => panic!("unexpected reply {other} instead of the exit"),
    }
}

/// A script whose leader starts a descendant that records its pid in `marker` and outlives the
/// leader, which exits with `code` once the descendant runs: a leader that exited first could end
/// before the interpreter started its background command at all.
fn outliving_descendant(marker: &Path, code: i32) -> RenderedScript {
    text(&format!(
        "sh -c 'printf \"%s\" \"$$\" > {marker}; exec sleep 300' & \
         while [ ! -s {marker} ]; do sleep 0.01; done; exit {code}",
        marker = marker.display()
    ))
}

/// Release the held command, as a supervisor does once the job concluded, and see it reaped.
fn release(control: &std::os::unix::net::UnixStream) {
    let frame = FrameWriter::new(REQUEST_RELEASE).finish().unwrap();
    std::io::Write::write_all(&mut &*control, &frame).expect("send the release");
    let (reply, _) = read_frame(control).unwrap().expect("a release reply");
    let (tag, fields) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_RELEASED);
    fields.finish().unwrap();
}

/// A script request on its way: the host's first reply and the readers of the job's streams.
struct Submitted {
    reply: Vec<u8>,
    readers: [std::thread::JoinHandle<String>; 2],
}

/// Send a rendered script to a host's control socket and read the host's first reply.
fn submit(
    control: &std::os::unix::net::UnixStream,
    directory: &Path,
    script: &RenderedScript,
) -> Submitted {
    let (stdout_read, stdout_write) = pipe();
    let (stderr_read, stderr_write) = pipe();
    let stdin = OwnedFd::from(std::fs::File::open("/dev/null").expect("null device"));
    let mut frame = FrameWriter::new(REQUEST_SCRIPT)
        .bytes(script.text.as_bytes())
        .unwrap()
        .u32(u32::try_from(script.bindings.len()).unwrap());
    for binding in &script.bindings {
        frame = match binding {
            Binding::Scalar { name, value } => frame
                .bytes(name.as_bytes())
                .unwrap()
                .u32(u32::from(BINDING_SCALAR))
                .list(std::iter::once(value.as_bytes()))
                .unwrap(),
            Binding::Array { name, values } => frame
                .bytes(name.as_bytes())
                .unwrap()
                .u32(u32::from(BINDING_ARRAY))
                .list(values.iter().map(String::as_bytes))
                .unwrap(),
        };
    }
    let frame = frame
        .bytes(directory.as_os_str().as_encoded_bytes())
        .unwrap()
        .list(std::iter::empty())
        .unwrap()
        .finish()
        .unwrap();
    let sent = send_with_descriptors(
        control.as_raw_fd(),
        &frame,
        &[
            stdin.as_raw_fd(),
            stdout_write.as_raw_fd(),
            stderr_write.as_raw_fd(),
        ],
    )
    .expect("send the request");
    std::io::Write::write_all(&mut &*control, &frame[sent..]).expect("send the rest");
    drop((stdin, stdout_write, stderr_write));
    let readers = [stdout_read, stderr_read].map(|reader| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::fs::File::from(reader)
                .read_to_end(&mut bytes)
                .expect("read a job stream");
            String::from_utf8_lossy(&bytes).into_owned()
        })
    });
    let (reply, _) = read_frame(control).unwrap().expect("a reply");
    Submitted { reply, readers }
}

/// Run a rendered script on a host's control socket as a supervisor does: its exit, then its
/// streams to their end, then its release. Returns the job's exit and streams.
fn run_on(
    control: &std::os::unix::net::UnixStream,
    directory: &Path,
    script: &RenderedScript,
    signal: Option<i32>,
) -> Ran {
    let Submitted { reply, readers } = submit(control, directory, script);
    let (tag, mut fields) = FrameReader::new(&reply).unwrap();
    let (birth, status) = match tag {
        REPLY_SCRIPT_SYNTAX => (None, None),
        REPLY_STARTED => {
            let birth = fields.birth().unwrap();
            fields.finish().unwrap();
            if let Some(signal) = signal {
                send_signal(control, signal);
            }
            (Some(birth), Some(read_exit(control)))
        }
        REPLY_HOST_FAILED => panic!(
            "the host failed: {}",
            String::from_utf8_lossy(fields.bytes().unwrap())
        ),
        other => panic!("unexpected reply {other}"),
    };
    let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
    if birth.is_some() {
        release(control);
    }
    Ran {
        pid: birth.as_ref().map(Birth::pid),
        birth,
        status,
        stdout,
        stderr,
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct Ran {
    pid: Option<u32>,
    /// The job's group leader as the host observed it before reaping it.
    birth: Option<Birth>,
    /// `None` when the script did not parse.
    status: Option<ExitStatus>,
    stdout: String,
    stderr: String,
}

fn exited(status: Option<&ExitStatus>) -> Option<i32> {
    match status {
        Some(ExitStatus::Exited { code }) => Some(*code),
        Some(ExitStatus::Signaled { .. }) | None => None,
    }
}

fn signaled(status: Option<&ExitStatus>) -> Option<i32> {
    match status {
        Some(ExitStatus::Signaled { signal, .. }) => Some(*signal),
        Some(ExitStatus::Exited { .. }) | None => None,
    }
}

fn pipe() -> (OwnedFd, OwnedFd) {
    let mut ends = [0; 2];
    // SAFETY: room for the two descriptors pipe(2) writes.
    assert_eq!(unsafe { libc::pipe(ends.as_mut_ptr()) }, 0);
    // SAFETY: both descriptors were just created and are owned here alone.
    unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) }
}

fn script(parts: &[&str], values: Vec<ScriptValue>) -> RenderedScript {
    render(
        &ScriptCommand::new(
            parts.iter().map(|part| (*part).to_owned()).collect(),
            values,
        )
        .expect("a well-shaped script"),
    )
    .expect("renders")
}

fn text(source: &str) -> RenderedScript {
    script(&[source], Vec::new())
}

fn alive(pid: i32) -> bool {
    // SAFETY: signal 0 only checks existence.
    unsafe { libc::kill(pid, 0) == 0 }
}

fn stop_fixture_jobs(host_pid: u32, leader: Option<i32>, descendant: Option<i32>) {
    let host_pid = i32::try_from(host_pid).expect("host PID");
    let host_group = unsafe { libc::getpgid(host_pid) };
    let our_group = unsafe { libc::getpgrp() };
    if let Some(pid) =
        leader.filter(|pid| *pid > 0 && *pid != host_pid && *pid != host_group && *pid != our_group)
    {
        // SAFETY: never signal our group or the host's group.
        unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
    if let Some(pid) =
        descendant.filter(|pid| *pid > 0 && *pid != host_pid && *pid != std::process::id() as i32)
    {
        // A background child may have escaped its leader's group.
        unsafe { libc::kill(pid, libc::SIGKILL) };
    }
}

struct ScriptJobGuard {
    host_pid: u32,
    leader: i32,
    descendant: i32,
    armed: bool,
}

impl Drop for ScriptJobGuard {
    fn drop(&mut self) {
        if self.armed {
            stop_fixture_jobs(self.host_pid, Some(self.leader), Some(self.descendant));
        }
    }
}

#[test]
fn a_script_has_its_own_streams_and_exact_status() {
    let mut host = Host::start("streams");
    let ran = host.run(&text("printf out; printf err >&2; exit 7"));
    assert_eq!((ran.stdout.as_str(), ran.stderr.as_str()), ("out", "err"));
    assert_eq!(exited(ran.status.as_ref()), Some(7));

    let ran = host.run(&text("kill -SEGV $$"));
    assert_eq!(
        signaled(ran.status.as_ref()),
        Some(libc::SIGSEGV),
        "a script process that dies by a signal reports that signal, not 128+N"
    );

    let ran = host.run(&text("/bin/sh -c 'kill -TERM $$'"));
    assert_eq!(
        exited(ran.status.as_ref()),
        Some(128 + libc::SIGTERM),
        "a script whose last command died by a signal exits 128+N, as bash does"
    );
}

/// The host is the job's parent, so it observes the job's group leader before it reaps it and
/// reports that identity with the start, under the pid the host reports -- even for a job whose
/// leader exits at once. No later observer could identify the leader once the host has reaped
/// the job and its pid is free for another process.
#[test]
fn the_host_reports_the_leader_it_observed_before_reaping_it() {
    let mut host = Host::start("birth");
    for source in ["sleep 0.2", "exit 0"] {
        let ran = host.run(&text(source));
        assert_eq!(exited(ran.status.as_ref()), Some(0));
        let leader = ran
            .birth
            .as_ref()
            .and_then(Birth::leader)
            .expect("the host identified the job's group leader");
        assert_eq!(Some(leader.pgid().unsigned_abs()), ran.pid);
    }
}

/// A job's signal reaches its command's group through the host, the command's parent, which holds
/// the leader unreaped until the job is released -- after the leader exited too, so descendants
/// that outlive it and hold the job's output open are still reached. The released host serves
/// the next command.
#[test]
fn the_host_signals_its_command_s_group_until_it_is_released() {
    let mut host = Host::start("signal");
    let ran = host.run_signalled(&text("sleep 30"), libc::SIGKILL);
    assert_eq!(signaled(ran.status.as_ref()), Some(libc::SIGKILL));

    let marker = host.directory.join("descendant");
    let Submitted { reply, readers } = submit(
        &host.control,
        &host.directory,
        &outliving_descendant(&marker, 3),
    );
    let (tag, _) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_STARTED);
    assert_eq!(read_exit(&host.control), ExitStatus::Exited { code: 3 });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let descendant = loop {
        if let Some(pid) = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|pid| pid.trim().parse::<i32>().ok())
        {
            break pid;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the descendant never started"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut job = ScriptJobGuard {
        host_pid: host.child.id(),
        leader: -1,
        descendant,
        armed: true,
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(alive(descendant), "the descendant outlives the leader");
    assert!(
        readers.iter().all(|reader| !reader.is_finished()),
        "the descendant holds the job's output open"
    );

    send_signal(&host.control, libc::SIGKILL);
    let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
    assert_eq!((stdout.as_str(), stderr.as_str()), ("", ""));
    assert!(gone(descendant), "the signal reached the descendant");
    job.armed = false;
    release(&host.control);

    let ran = host.run(&text("printf after"));
    assert_eq!(
        (exited(ran.status.as_ref()), ran.stdout.as_str()),
        (Some(0), "after")
    );
}

/// A supervisor that goes away while its command's leader has exited and descendants run on
/// leaves nothing to signal them: the host ends the command's group itself, reaps the leader and
/// exits.
#[test]
fn a_host_whose_supervisor_goes_away_ends_the_command_it_holds() {
    let mut host = Host::start("orphan");
    let marker = host.directory.join("descendant");
    let Submitted { reply, readers } = submit(
        &host.control,
        &host.directory,
        &outliving_descendant(&marker, 0),
    );
    let (tag, _) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_STARTED);
    assert_eq!(read_exit(&host.control), ExitStatus::Exited { code: 0 });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let descendant = loop {
        if let Some(pid) = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|pid| pid.trim().parse::<i32>().ok())
        {
            break pid;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the descendant never started"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut job = ScriptJobGuard {
        host_pid: host.child.id(),
        leader: -1,
        descendant,
        armed: true,
    };
    host.control
        .shutdown(std::net::Shutdown::Both)
        .expect("close the supervisor's end");
    let [stdout, _] = readers.map(|reader| reader.join().expect("stream reader"));
    assert_eq!(stdout, "");
    let host_status = loop {
        if let Some(status) = host.child.try_wait().expect("host status") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the host never ended its command"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(host_status.code(), Some(0));
    assert!(gone(descendant), "the host ended the descendant");
    job.armed = false;
}

/// Whether `pid` stops existing within a bound: a killed descendant whose parent already exited
/// is collected by init, not by this test.
fn gone(pid: i32) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while alive(pid) {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

#[test]
fn a_script_that_does_not_parse_never_runs() {
    let mut host = Host::start("syntax");
    let marker = host.directory.join("ran");
    let ran = host.run(&text(&format!("touch {}; if true; then", marker.display())));
    assert_eq!(ran.status, None);
    assert!(ran.stderr.contains("does not parse"), "{}", ran.stderr);
    assert!(
        !marker.exists(),
        "nothing of a script that does not parse runs"
    );
}

#[test]
fn killing_a_script_job_group_reaches_its_grandchildren_and_spares_the_host() {
    let mut host = Host::start("group");
    let leader = host.directory.join("leader");
    let descendant = host.directory.join("descendant");
    let hold = host.directory.join("hold");
    for path in [&leader, &descendant, &hold] {
        let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("FIFO path");
        // SAFETY: a NUL-terminated path in this fixture's own directory.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0, "{}", std::io::Error::last_os_error());
    }
    let rendered = text(&format!(
        "sh -c 'printf \"%s\\n\" \"$$\" > {}; read -r _ < {}' & printf '%s\\n' \"$$\" > {}; wait",
        descendant.display(), hold.display(), leader.display()
    ));
    let Submitted { reply, readers } = submit(&host.control, &host.directory, &rendered);
    let (tag, mut fields) = FrameReader::new(&reply).expect("start reply");
    assert_eq!(tag, REPLY_STARTED);
    let birth = fields.birth().expect("the real parent observed its unreaped leader");
    fields.finish().expect("start frame");
    // Each FIFO reports only after that exact process runs. No elapsed delay supplies readiness.
    let pids = [&leader, &descendant].map(|path| {
        let value = std::fs::read_to_string(path).expect("read the actual process's FIFO");
        value.trim().parse::<i32>().expect("published process PID")
    });
    assert_eq!(birth.pid(), pids[0].cast_unsigned());
    let mut job = ScriptJobGuard {
        host_pid: host.child.id(), leader: pids[0], descendant: pids[1], armed: true,
    };
    let members = cowshed_core::runtime::job_groups::job_members(&birth).expect("identity-proven live group");
    let exits = pids.map(|pid| {
        let member = members.iter().find(|member| member.pid() == pid).expect("published process is in the actual owned group");
        let exit = member.watch_exit().expect("watch this retained life before killing it");
        assert!(!exit.within(Duration::ZERO).expect("current exit readiness"), "process {pid} already exited");
        exit
    });
    // A real group signal must end both retained lives, not just the leader or its output pipes.
    send_signal(&host.control, libc::SIGTERM);
    let status = read_exit(&host.control);
    assert_eq!(signaled(Some(&status)), Some(libc::SIGTERM));
    for (pid, exit) in pids.into_iter().zip(exits) {
        assert!(exit.within(Duration::from_secs(10)).expect("native exit notification"),
            "retained process {pid} of the killed script still runs");
    }
    let [stdout, stderr] = readers.map(|reader| reader.join().expect("owned output reader"));
    assert_eq!((stdout.as_str(), stderr.as_str()), ("", ""));
    release(&host.control); // Only its actual parent reaps the script's leader.
    job.armed = false;
    assert!(host.child.try_wait().expect("host status").is_none(), "the host is not in the job's group");
    assert_eq!(host.run(&text("printf again")).stdout, "again");
}

/// The old null-signal oracle called an exited, unreaped child alive. The exit watch must not.
#[test]
fn an_unreaped_zombie_is_exited_even_while_the_null_signal_reaches_it() {
    let mut child = std::process::Command::new("/bin/sh")
        .args(["-c", "read -r _"]).stdin(Stdio::piped()).process_group(0)
        .spawn().expect("owned pipe-held child");
    let pid = i32::try_from(child.id()).expect("child PID");
    let birth = Birth::of(child.id());
    let members = cowshed_core::runtime::job_groups::job_members(&birth).expect("owned group");
    let retained = members.iter().find(|member| member.pid() == pid).expect("the owned leader");
    let exit = retained.watch_exit().expect("watch the actual running child");
    let running = exit.within(Duration::ZERO).expect("live readiness");
    drop(child.stdin.take());
    let status = cowshed_core::runtime::job_groups::await_exit_unreaped(pid).expect("real exit, no reap");
    let null_signal_reaches = alive(pid);
    let exited = exit.within(Duration::ZERO).expect("zombie exit notification");
    #[cfg(target_os = "linux")]
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
    child.wait().expect("only the owner reaps this child, even when an oracle below fails");
    assert!(!running, "the held child was running before EOF");
    assert_eq!(status, ExitStatus::Exited { code: 1 });
    assert!(null_signal_reaches, "POSIX null signal succeeds on the owned unreaped zombie");
    assert!(exited, "positive native exit evidence while the child remained unreaped");
    #[cfg(target_os = "linux")]
    {
        let stat = stat.expect("unreaped child's real kernel state");
        let closing = stat.rfind(')').expect("stat command delimiter");
        assert_eq!(stat[closing + 1..].split_whitespace().next(), Some("Z"), "actual Linux zombie state: {stat}");
        eprintln!("owned-zombie pid={pid} kernel_state=Z null_signal_reaches=true exit_watch_ready=true reaped_by_owner=true");
    }
}

/// Scripts forked at once each run whole. Two creations of one process group race, and macOS
/// refuses the loser with `EPERM`: were the host to create the child's group too, a refused
/// child would end its script with 126 before it ran and a refused host would end itself.
/// Several hosts forking back to back make the race likely.
#[test]
fn scripts_forked_at_once_each_run_whole() {
    // Every host starts before any job pipe exists, so no host inherits another's pipe end.
    let hosts: Vec<Host> = (0..8)
        .map(|index| Host::start(&format!("burst-{index}")))
        .collect();
    let workers: Vec<_> = hosts
        .into_iter()
        .map(|mut host| {
            std::thread::spawn(move || {
                (0..100)
                    .map(|_| host.run(&text("printf x")))
                    .filter(|ran| ran.stdout != "x" || exited(ran.status.as_ref()) != Some(0))
                    .map(|ran| {
                        format!(
                            "stdout {:?}, status {:?}, stderr {:?}",
                            ran.stdout, ran.status, ran.stderr
                        )
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let failures: Vec<String> = workers
        .into_iter()
        .flat_map(|worker| worker.join().expect("the host replied to every script"))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of 800 scripts did not run:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn nothing_one_script_does_to_its_process_reaches_the_next() {
    let mut host = Host::start("isolation");
    let first = host.run(&text(
        "umask 077; ulimit -n 64; cd /; export LEAK=1; trap 'echo trapped' EXIT",
    ));
    assert_eq!(exited(first.status.as_ref()), Some(0));
    let second = host.run(&text(
        "umask; ulimit -n; printf '%s|%s\\n' \"$PWD\" \"${LEAK-}\"",
    ));
    let lines: Vec<&str> = second.stdout.lines().collect();
    assert_ne!(lines[0], "0077", "umask leaked: {}", second.stdout);
    assert_ne!(lines[1], "64", "ulimit leaked: {}", second.stdout);
    assert_eq!(lines[2], format!("{}|", host.directory.display()));
    assert!(!second.stdout.contains("trapped"));
}

#[test]
fn values_reach_the_script_as_data_in_every_quoting_context() {
    let mut host = Host::start("values");
    let hostile = r#"a "b" 'c' $(touch pwned) `id` ; rm -rf x *"#;
    for parts in [
        ["printf '[%s]' ", ""],
        ["printf '[%s]' \"", "\""],
        ["printf '[%s]' '", "'"],
        ["printf '[%s]' \"$(printf '%s' ", ")\""],
    ] {
        let ran = host.run(&script(&parts, vec![ScriptValue::Word(hostile.into())]));
        assert_eq!(
            ran.stdout,
            format!("[{hostile}]"),
            "{parts:?}: stderr {:?}",
            ran.stderr
        );
    }
    assert!(!host.directory.join("pwned").exists());
    let spread = host.run(&script(
        &["printf '[%s]' ", ""],
        vec![ScriptValue::Words(vec!["a b".into(), "*".into()])],
    ));
    assert_eq!(spread.stdout, "[a b][*]");
}

#[derive(serde::Deserialize)]
struct Corpus {
    schema: String,
    rows: Vec<Row>,
}

#[derive(serde::Deserialize)]
struct Row {
    name: String,
    parts: Vec<String>,
    values: Vec<ScriptValue>,
    verdict: String,
    #[serde(default)]
    stdout: Option<String>,
}

fn corpus() -> Corpus {
    let path = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR")
            .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs"),
    )
    .join("testdata/shell-script-corpus.json");
    serde_json::from_slice(&std::fs::read(path).expect("the shared corpus")).expect("corpus JSON")
}

/// The corpus is shared byte for byte with the compile-time checker on the other side of the
/// script interface: both must agree which scripts parse, and what an accepted one prints.
#[test]
fn the_shared_corpus_parses_and_prints_what_bash_does() {
    let corpus = corpus();
    assert_eq!(corpus.schema, "cowshed.shell-script-corpus/v1");
    let mut host = Host::start("corpus");
    let mut mismatches = Vec::new();
    for row in &corpus.rows {
        let command = ScriptCommand::new(row.parts.clone(), row.values.clone())
            .unwrap_or_else(|error| panic!("{}: {error}", row.name));
        let rendered = render(&command);
        match row.verdict.as_str() {
            "ok" => {
                let rendered = rendered.unwrap_or_else(|error| panic!("{}: {error}", row.name));
                let ran = host.run(&rendered);
                let expected = row.stdout.as_deref().unwrap_or_default();
                if ran.status.is_none() || ran.stdout != expected {
                    mismatches.push(format!(
                        "{}: stdout {:?} (want {expected:?}), status {:?}, stderr {:?}",
                        row.name, ran.stdout, ran.status, ran.stderr
                    ));
                }
            }
            "syntax" => {
                if let Ok(rendered) = rendered {
                    let ran = host.run(&rendered);
                    if ran.status.is_some() {
                        mismatches.push(format!("{}: parsed, but bash refuses it", row.name));
                    }
                }
            }
            // Placement is refused before a script reaches cowshed; brush only has to keep the
            // value out of the shell text, which rendering guarantees for every row that renders.
            "placement" => {}
            other => panic!("{}: unknown verdict {other}", row.name),
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// A host that cannot serve a request says why before it exits, instead of leaving the
/// supervisor a closed socket and nothing else.
#[test]
fn a_host_that_cannot_run_direnv_says_so() {
    let host = Host::start_with_path("no-direnv", std::ffi::OsStr::new("/nonexistent/bin"));
    let envrc = host.directory.join(".envrc");
    std::fs::write(&envrc, b"").expect("envrc");
    let (_stdout_read, stdout_write) = pipe();
    let (_stderr_read, stderr_write) = pipe();
    let frame = FrameWriter::new(REQUEST_APPROVE)
        .bytes(envrc.as_os_str().as_encoded_bytes())
        .unwrap()
        .finish()
        .unwrap();
    let sent = send_with_descriptors(
        host.control.as_raw_fd(),
        &frame,
        &[stdout_write.as_raw_fd(), stderr_write.as_raw_fd()],
    )
    .expect("send the request");
    std::io::Write::write_all(&mut &host.control, &frame[sent..]).expect("send the rest");
    drop((stdout_write, stderr_write));
    let (reply, _) = read_frame(&host.control)
        .expect("a readable reply")
        .expect("a reply, not a closed socket");
    let (tag, mut fields) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_HOST_FAILED);
    let reason = String::from_utf8_lossy(fields.bytes().unwrap()).into_owned();
    assert!(
        reason.contains("direnv") && reason.contains("/nonexistent/bin"),
        "the reason names the tool and where it was looked for: {reason}"
    );
}
