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

use cowshed_core::api::{ScriptCommand, ScriptValue};
use cowshed_core::runtime::job_groups::Birth;
use cowshed_core::runtime::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, CONTROL_DESCRIPTOR, FrameReader, FrameWriter, REPLY_EXITED,
    REPLY_HOST_FAILED, REPLY_SCRIPT_SYNTAX, REPLY_STARTED, REQUEST_APPROVE, REQUEST_SCRIPT,
    REQUEST_SIGNAL, read_frame, send_with_descriptors,
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

/// Ask the host to signal the command it runs.
fn send_signal(control: &std::os::unix::net::UnixStream, signal: i32) {
    let frame = FrameWriter::new(REQUEST_SIGNAL)
        .i32(signal)
        .finish()
        .unwrap();
    std::io::Write::write_all(&mut &*control, &frame).expect("send the signal");
}

/// Run a rendered script on a host's control socket; returns the job's raw status and streams.
fn run_on(
    control: &std::os::unix::net::UnixStream,
    directory: &Path,
    script: &RenderedScript,
    signal: Option<i32>,
) -> Ran {
    {
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
        let (first, _) = read_frame(control).unwrap().expect("a reply");
        let (tag, mut fields) = FrameReader::new(&first).unwrap();
        let (birth, status) = match tag {
            REPLY_SCRIPT_SYNTAX => (None, None),
            REPLY_STARTED => {
                let birth = fields.birth().unwrap();
                fields.finish().unwrap();
                if let Some(signal) = signal {
                    send_signal(control, signal);
                }
                let (last, _) = read_frame(control).unwrap().expect("an exit");
                let (tag, mut fields) = FrameReader::new(&last).unwrap();
                assert_eq!(tag, REPLY_EXITED);
                (Some(birth), Some(fields.i32().unwrap()))
            }
            REPLY_HOST_FAILED => panic!(
                "the host failed: {}",
                String::from_utf8_lossy(fields.bytes().unwrap())
            ),
            other => panic!("unexpected reply {other}"),
        };
        let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
        Ran {
            pid: birth.as_ref().map(Birth::pid),
            birth,
            status,
            stdout,
            stderr,
        }
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
    /// Raw `waitpid` status; `None` when the script did not parse.
    status: Option<i32>,
    stdout: String,
    stderr: String,
}

fn exited(status: Option<i32>) -> Option<i32> {
    let status = status?;
    libc::WIFEXITED(status).then(|| libc::WEXITSTATUS(status))
}

fn signaled(status: Option<i32>) -> Option<i32> {
    let status = status?;
    libc::WIFSIGNALED(status).then(|| libc::WTERMSIG(status))
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
    assert_eq!(exited(ran.status), Some(7));

    let ran = host.run(&text("kill -SEGV $$"));
    assert_eq!(
        signaled(ran.status),
        Some(libc::SIGSEGV),
        "a script process that dies by a signal reports that signal, not 128+N"
    );

    let ran = host.run(&text("/bin/sh -c 'kill -TERM $$'"));
    assert_eq!(
        exited(ran.status),
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
        assert_eq!(exited(ran.status), Some(0));
        let leader = ran
            .birth
            .as_ref()
            .and_then(Birth::leader)
            .expect("the host identified the job's group leader");
        assert_eq!(Some(leader.pgid().unsigned_abs()), ran.pid);
    }
}

/// A job's signal reaches its command through the host, the command's parent, which applies it
/// while it still holds the command unreaped. One that arrives after the command ended and was
/// reaped reaches nothing, and the host goes on to serve the next request.
#[test]
fn the_host_signals_its_running_command_and_ignores_a_late_signal() {
    let mut host = Host::start("signal");
    let ran = host.run_signalled(&text("sleep 30"), libc::SIGKILL);
    assert_eq!(signaled(ran.status), Some(libc::SIGKILL));
    send_signal(&host.control, libc::SIGKILL);
    let ran = host.run(&text("printf after"));
    assert_eq!(
        (exited(ran.status), ran.stdout.as_str()),
        (Some(0), "after")
    );
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
    let rendered = text(&format!(
        "sh -c 'printf \"%s\\n\" \"$$\" > {}; exec sleep 300' & printf '%s\\n' \"$$\" > {}; wait",
        descendant.display(),
        leader.display()
    ));
    let (sender, receiver) = std::sync::mpsc::channel();
    let control = host.control.try_clone().expect("control clone");
    let directory = host.directory.clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let runner = std::thread::spawn(move || {
        let ran = run_on(&control, &directory, &rendered, None);
        sender.send((ran.pid, ran.status)).unwrap();
    });
    // Wait until the descendant itself has written its PID. `$!` in the parent
    // may still be unset when its background command first starts.
    let pids = loop {
        let parent = std::fs::read_to_string(&leader)
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok());
        let child = std::fs::read_to_string(&descendant)
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok());
        if let (Some(parent), Some(child)) = (parent, child) {
            break [parent, child];
        }
        if std::time::Instant::now() >= deadline {
            let parent = std::fs::read_to_string(&leader);
            let child = std::fs::read_to_string(&descendant);
            let parent_pid = parent
                .as_ref()
                .ok()
                .and_then(|value| value.trim().parse::<i32>().ok());
            let child_pid = child
                .as_ref()
                .ok()
                .and_then(|value| value.trim().parse::<i32>().ok());
            stop_fixture_jobs(host.child.id(), parent_pid, child_pid);
            let host_status = host.child.try_wait().expect("host status");
            panic!(
                "the background job did not publish both PIDs: leader={parent:?}, descendant={child:?}, host={host_status:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut job = ScriptJobGuard {
        host_pid: host.child.id(),
        leader: pids[0],
        descendant: pids[1],
        armed: true,
    };
    // The background child holds the script's output pipes. If brush moves it to a
    // different process group, killing the script leaves both those pipes open.
    let script_group = unsafe { libc::getpgid(pids[0]) };
    let grandchild_group = unsafe { libc::getpgid(pids[1]) };
    assert_eq!(script_group, pids[0], "the script must lead its own group");
    assert_eq!(
        grandchild_group, script_group,
        "a background child must remain in the script's group"
    );
    // The job's group is the script process's own pid.
    // SAFETY: a group signal to the job's group.
    assert_eq!(unsafe { libc::kill(-pids[0], libc::SIGTERM) }, 0);
    let outcome =
        receiver.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()));
    let (_, status) = outcome.unwrap_or_else(|error| {
        let script_group = unsafe { libc::getpgid(pids[0]) };
        let grandchild_group = unsafe { libc::getpgid(pids[1]) };
        let host = host.child.try_wait().expect("host status");
        panic!(
            "the job did not end: {error}; script group={script_group}, grandchild group={grandchild_group}, host={host:?}"
        );
    });
    runner.join().unwrap();
    assert_eq!(signaled(status), Some(libc::SIGTERM));
    std::thread::sleep(Duration::from_millis(100));
    for pid in pids {
        assert!(!alive(pid), "process {pid} of the killed script survived");
    }
    job.armed = false;
    assert!(
        host.child.try_wait().unwrap().is_none(),
        "the host is not in the job's group"
    );
    assert_eq!(host.run(&text("printf again")).stdout, "again");
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
                    .filter(|ran| ran.stdout != "x" || exited(ran.status) != Some(0))
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
    assert_eq!(exited(first.status), Some(0));
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
