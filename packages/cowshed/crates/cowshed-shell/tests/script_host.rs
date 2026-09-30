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
use cowshed_core::runtime::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, CONTROL_DESCRIPTOR, FrameReader, FrameWriter, REPLY_EXITED,
    REPLY_SCRIPT_SYNTAX, REPLY_STARTED, REQUEST_SCRIPT, read_frame, send_with_descriptors,
};
use cowshed_core::script::{Binding, RenderedScript, render};

struct Host {
    child: std::process::Child,
    control: std::os::unix::net::UnixStream,
    directory: PathBuf,
}

impl Host {
    fn start(label: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-script-host-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&directory).expect("scratch directory");
        let directory = std::fs::canonicalize(directory).expect("canonical scratch");
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let theirs_raw = theirs.as_raw_fd();
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_cowshed-shell-host"));
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
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
        run_on(&self.control, &self.directory, script)
    }
}

/// Run a rendered script on a host's control socket; returns the job's raw status and streams.
fn run_on(
    control: &std::os::unix::net::UnixStream,
    directory: &Path,
    script: &RenderedScript,
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
        let (pid, status) = match tag {
            REPLY_SCRIPT_SYNTAX => (None, None),
            REPLY_STARTED => {
                let pid = fields.u32().unwrap();
                let (last, _) = read_frame(control).unwrap().expect("an exit");
                let (tag, mut fields) = FrameReader::new(&last).unwrap();
                assert_eq!(tag, REPLY_EXITED);
                (Some(pid), Some(fields.i32().unwrap()))
            }
            other => panic!("unexpected reply {other}"),
        };
        let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
        Ran {
            pid,
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
    let pids = host.directory.join("pids");
    let rendered = text(&format!(
        "sleep 300 & printf '%s %s\\n' $$ $! > {}; wait",
        pids.display()
    ));
    let (sender, receiver) = std::sync::mpsc::channel();
    let control = host.control.try_clone().expect("control clone");
    let directory = host.directory.clone();
    let runner = std::thread::spawn(move || {
        let ran = run_on(&control, &directory, &rendered);
        sender.send((ran.pid, ran.status)).unwrap();
    });
    let recorded = loop {
        if let Ok(text) = std::fs::read_to_string(&pids)
            && text.ends_with('\n')
        {
            break text;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let pids: Vec<i32> = recorded
        .split_whitespace()
        .map(|pid| pid.parse().unwrap())
        .collect();
    // The job's group is the script process's own pid.
    // SAFETY: a group signal to the job's group.
    assert_eq!(unsafe { libc::kill(-pids[0], libc::SIGTERM) }, 0);
    let (_, status) = receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("the job ends");
    runner.join().unwrap();
    assert_eq!(signaled(status), Some(libc::SIGTERM));
    std::thread::sleep(Duration::from_millis(100));
    for pid in pids {
        assert!(!alive(pid), "process {pid} of the killed script survived");
    }
    assert!(
        host.child.try_wait().unwrap().is_none(),
        "the host is not in the job's group"
    );
    assert_eq!(host.run(&text("printf again")).stdout, "again");
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
