//! Script jobs against the real exec host binary, driven over its own protocol.
//!
//! The host runs unsandboxed here: these tests are about what brush and the fork do with a
//! job's process group, streams and status, which no sandbox changes. The workspace has no
//! `.envrc`, so the host holds exactly the environment it was started with.
//! On Linux each host-using test re-executes its exact case through
//! `sudo -n systemd-run --scope -p Delegate=yes`, as the native cgroup harness does. A root
//! setup process delegates that fresh scope, then runs the test as the original unprivileged
//! user. No user session bus or externally delegated test runner is required. Missing sudo,
//! systemd, or delegated controllers fails the test; nothing skips or substitutes a regular
//! file. Each request gets a fresh job cgroup, retired after `cgroup.kill` and the empty event.
//!
//! A fixture ends a host as a supervisor does: it closes its end of the control socket, never
//! `shutdown(2)`s it. Darwin can lose a peer shutdown's wakeup for a host already asleep in
//! `recvmsg`; `specs/cowshed/08_testing.md`, CI, records the measurement. Protocol-position
//! checkpoints stay as diagnostics.

use cowshed_os::test_backstop::ChildBackstop as _;
use std::io::Read as _;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use cowshed_core::api::{ExitStatus, ScriptCommand, ScriptValue};
#[cfg(target_os = "linux")]
use cowshed_core::runtime::job_cgroup::{CgroupAuthority, IncarnationCgroups, JobCgroup};
use cowshed_core::runtime::job_groups::Birth;
use cowshed_core::runtime::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, CONTROL_DESCRIPTOR, FrameReader, FrameWriter, REPLY_ACTIVATED,
    REPLY_ADOPTION_REFUSED, REPLY_EXITED, REPLY_HOST_FAILED, REPLY_RELEASED, REPLY_SCRIPT_SYNTAX,
    REPLY_STARTED, REQUEST_ADOPT, REQUEST_APPROVE, REQUEST_RELEASE, REQUEST_SCRIPT, REQUEST_SIGNAL,
    read_frame, send_with_descriptors,
};
use cowshed_core::script::{Binding, RenderedScript, render};
#[cfg(target_os = "linux")]
use cowshed_os::test_backstop::CommandBackstop as _;

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
    /// The supervisor's end of the control socket, the only descriptor for it outside the host;
    /// `None` once closed. Closing it is the host's end of stream.
    control: Option<std::os::unix::net::UnixStream>,
    directory: PathBuf,
    finished: bool,
    #[cfg(target_os = "linux")]
    cgroups: IncarnationCgroups,
    #[cfg(target_os = "linux")]
    jobs: std::cell::RefCell<Vec<JobCgroup>>,
}

#[cfg(target_os = "linux")]
fn cgroup_authority() -> &'static CgroupAuthority {
    static AUTHORITY: std::sync::LazyLock<CgroupAuthority> = std::sync::LazyLock::new(|| {
        CgroupAuthority::delegated().expect("the setup role delegated a real cgroup v2 scope")
    });
    &AUTHORITY
}

#[cfg(target_os = "linux")]
const DELEGATED_CASE_ROLE: &str = "COWSHED_SCRIPT_HOST_ROLE";

#[cfg(target_os = "linux")]
#[derive(serde::Deserialize, serde::Serialize)]
enum DelegatedCaseRole {
    Delegate { uid: u32, gid: u32 },
    Run { uid: u32, gid: u32 },
}

#[cfg(target_os = "linux")]
fn fixture_program(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("the delegated script fixture requires {name} on PATH"))
}

/// Return only after this exact case passed in the delegated subprocess. The `Run` role alone
/// returns false and executes the original body; the other roles are harnesses, not skipped tests.
#[cfg(target_os = "linux")]
fn delegated_case(case: &str) -> bool {
    let role = std::env::var_os(DELEGATED_CASE_ROLE).map(|role| {
        serde_json::from_slice::<DelegatedCaseRole>(role.as_encoded_bytes())
            .expect("the script fixture's delegated role")
    });
    let mut command = match role {
        Some(DelegatedCaseRole::Run { uid, gid }) => {
            // SAFETY: these calls only read this process's credentials.
            assert_eq!(unsafe { libc::geteuid() }, uid, "the test is unprivileged");
            // SAFETY: as above.
            assert_eq!(unsafe { libc::getegid() }, gid, "the runner's group");
            assert_ne!(uid, 0, "root only prepares delegation, never runs the test");
            cgroup_authority();
            println!("COWSHED_SCRIPT_HOST_CASE {case}");
            return false;
        }
        Some(DelegatedCaseRole::Delegate { uid, gid }) => {
            // SAFETY: geteuid only reads this process's credentials.
            assert_eq!(
                unsafe { libc::geteuid() },
                0,
                "the scope setup requires root"
            );
            let membership =
                std::fs::read_to_string("/proc/self/cgroup").expect("the setup role's cgroup");
            let relative = membership
                .lines()
                .find_map(|line| line.strip_prefix("0::"))
                .filter(|path| path.starts_with('/'))
                .expect("the setup role runs in cgroup v2");
            let scope = Path::new(cowshed_core::runtime::job_cgroup::CGROUP_MOUNT)
                .join(relative.trim_start_matches('/'));
            for file in [
                "",
                "cgroup.procs",
                "cgroup.threads",
                "cgroup.subtree_control",
            ] {
                let path = scope.join(file);
                std::os::unix::fs::chown(&path, Some(uid), Some(gid))
                    .unwrap_or_else(|error| panic!("delegate {}: {error}", path.display()));
            }
            // The waiting setup parent cannot populate the distributing root. Both it and the
            // ordinary test controller stay outside every admitted job in this controller leaf.
            let leaf = scope.join(cowshed_core::runtime::job_cgroup::CONTROLLER_LEAF);
            std::fs::create_dir(&leaf).expect("create the fresh scope's controller leaf");
            for file in ["", "cgroup.procs", "cgroup.threads"] {
                let path = leaf.join(file);
                std::os::unix::fs::chown(&path, Some(uid), Some(gid))
                    .unwrap_or_else(|error| panic!("delegate {}: {error}", path.display()));
            }
            std::fs::write(leaf.join("cgroup.procs"), "0")
                .expect("move the setup parent out of the distributing scope");
            let mut command = std::process::Command::new(fixture_program("setpriv"));
            command
                .arg("--reuid")
                .arg(uid.to_string())
                .arg("--regid")
                .arg(gid.to_string())
                .args(["--clear-groups", "--"])
                .env(
                    DELEGATED_CASE_ROLE,
                    serde_json::to_string(&DelegatedCaseRole::Run { uid, gid })
                        .expect("serialize the unprivileged role"),
                );
            command
        }
        None => {
            // SAFETY: getuid/getgid only read this process's credentials.
            let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
            assert_ne!(uid, 0, "start the script tests as the ordinary runner");
            let mut command = std::process::Command::new(fixture_program("sudo"));
            command.arg("-n").arg(fixture_program("env")).arg(format!(
                "{DELEGATED_CASE_ROLE}={}",
                serde_json::to_string(&DelegatedCaseRole::Delegate { uid, gid })
                    .expect("serialize the delegation role"),
            ));
            // sudo resets its environment. Preserve nextest's relocated binary and corpus
            // paths, plus the runner's declared tools; never fall back to a compiled-in path.
            for name in [
                "PATH",
                "CARGO_BIN_EXE_cowshed-shell-host",
                "CARGO_MANIFEST_DIR",
            ] {
                let mut assignment = std::ffi::OsString::from(format!("{name}="));
                assignment.push(
                    std::env::var_os(name)
                        .unwrap_or_else(|| panic!("the test runner must set {name}")),
                );
                command.arg(assignment);
            }
            for name in ["TMPDIR", "LD_LIBRARY_PATH", "RUST_BACKTRACE"] {
                if let Some(value) = std::env::var_os(name) {
                    let mut assignment = std::ffi::OsString::from(format!("{name}="));
                    assignment.push(value);
                    command.arg(assignment);
                }
            }
            command.arg(fixture_program("systemd-run")).args([
                "--scope",
                "--quiet",
                "--collect",
                "--property=Delegate=yes",
                "--",
            ]);
            command
        }
    };
    let output = command
        .arg(std::env::current_exe().expect("the running script_host test binary"))
        .args(["--exact", case, "--nocapture", "--quiet"])
        .output_bounded()
        .unwrap_or_else(|error| panic!("start delegated script case {case}: {error}"));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("{stdout}");
    cowshed_os::eprintln!("{stderr}");
    assert!(
        output.status.success(),
        "delegated script case {case} failed: {}",
        output.status,
    );
    assert!(
        stdout.contains(&format!("COWSHED_SCRIPT_HOST_CASE {case}")),
        "the delegated subprocess never entered case {case}",
    );
    true
}

impl Host {
    /// A host with this test's own PATH, as a supervisor gives it the tools it found.
    fn start(label: &str) -> Self {
        Self::start_with_path(label, &std::env::var_os("PATH").expect("PATH"))
    }

    fn start_with_path(label: &str, path: &std::ffi::OsStr) -> Self {
        Self::start_with(label, path, &[])
    }

    /// A host whose environment also holds `ambient`, as a sandbox environment may.
    fn start_with(label: &str, path: &std::ffi::OsStr, ambient: &[(&str, &str)]) -> Self {
        // Move the fixture controller before spawning the reusable host: neither belongs to a
        // job. Each host gets its own incarnation, even when tests run concurrently.
        #[cfg(target_os = "linux")]
        let cgroups = cgroup_authority()
            .incarnation(
                &cowshed_core::metadata::WorkspaceIncarnation::new(
                    uuid::Uuid::new_v4().simple().to_string(),
                )
                .expect("incarnation"),
            )
            .expect("create this host's job cgroups");
        let program = shell_host();
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let directory = std::env::temp_dir().join(format!(
            "cowshed-script-host-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&directory).expect("create this fixture's unique directory");
        let started = (|| -> std::io::Result<Self> {
            let canonical = std::fs::canonicalize(&directory)?;
            let theirs_raw = theirs.as_raw_fd();
            let mut command = std::process::Command::new(program);
            command
                .env_clear()
                .env("PATH", path)
                .env("HOME", &canonical)
                .current_dir(&canonical)
                .envs(ambient.iter().copied())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .process_group(0);
            // SAFETY: this prepared dup2 touches only the forked child's descriptor table.
            unsafe {
                command.pre_exec(move || {
                    if libc::dup2(theirs_raw, CONTROL_DESCRIPTOR) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let child = command.spawn()?;
            cowshed_os::eprintln!("script-fixture host={} phase=spawned", child.id());
            drop(theirs);
            Ok(Self {
                child,
                control: Some(ours),
                directory: canonical,
                finished: false,
                #[cfg(target_os = "linux")]
                cgroups,
                #[cfg(target_os = "linux")]
                jobs: std::cell::RefCell::new(Vec::new()),
            })
        })();
        match started {
            Ok(host) => host,
            Err(cause) => {
                let cleanup = std::fs::remove_dir_all(&directory);
                panic!("fixture host startup failed: {cause}; owned directory cleanup={cleanup:?}");
            }
        }
    }

    fn control(&self) -> &std::os::unix::net::UnixStream {
        self.control
            .as_ref()
            .expect("the control socket is open until the fixture closes it")
    }

    fn run(&mut self, script: &RenderedScript) -> Ran {
        run_on(self, script, None)
    }

    /// Run `script`, sending the host `signal` for it once it has started.
    fn run_signalled(&mut self, script: &RenderedScript, signal: i32) -> Ran {
        run_on(self, script, Some(signal))
    }

    #[cfg(target_os = "linux")]
    fn placement(&self) -> OwnedFd {
        let mut jobs = self.jobs.borrow_mut();
        let id =
            cowshed_core::api::JobId::new(u64::try_from(jobs.len()).unwrap() + 1).expect("job id");
        let job = self
            .cgroups
            .admit(id, cowshed_core::api::ExecClass::Background)
            .expect("admit the protocol job");
        let placement = job.placement().expect("open the cgroup placement");
        jobs.push(job);
        placement.into_descriptor()
    }

    #[cfg(target_os = "linux")]
    fn retire_jobs(&mut self) -> std::io::Result<()> {
        for job in self.jobs.get_mut().drain(..) {
            job.kill_and_wait(cowshed_os::deadline::REAP)
                .map_err(std::io::Error::other)?;
            job.terminal()
                .map_err(|refused| std::io::Error::other(refused.error))?
                .retire()
                .map_err(std::io::Error::other)?;
        }
        // Each job's cgroup is the incarnation's only one: jobs sit in the shared class cgroups
        // (`<root>/<class>/job-<incarnation>-<id>`), which outlive every incarnation.
        Ok(())
    }

    /// Go away as a supervisor does: close the control socket, which the host reads as its end
    /// of stream, retiring any command it holds and exiting. Not `shutdown(2)`: on Darwin a
    /// peer's shutdown sometimes never wakes a host already asleep in `recvmsg`, which then
    /// sleeps forever; closing the socket's last descriptor wakes it.
    fn close_control(&mut self) {
        drop(self.control.take());
    }

    fn finish(&mut self) -> std::io::Result<()> {
        if self.finished {
            return Ok(());
        }
        // The actual parent host must retire its held group before it is joined.
        self.close_control();
        #[cfg(target_os = "linux")]
        self.retire_jobs()?;
        cowshed_os::eprintln!(
            "script-fixture host={} phase=control-closed",
            self.child.id()
        );
        let joined = self.child.wait_bounded();
        cowshed_os::eprintln!(
            "script-fixture host={} phase=host-joined result={joined:?}",
            self.child.id()
        );
        let removed = std::fs::remove_dir_all(&self.directory);
        self.finished = joined.is_ok() && removed.is_ok();
        match (&joined, &removed) {
            (Ok(_), Ok(())) => Ok(()),
            _ => Err(std::io::Error::other(format!(
                "fixture teardown: host join={joined:?}; directory removal={removed:?}"
            ))),
        }
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
fn submit(host: &Host, script: &RenderedScript) -> Submitted {
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
    submit_command(host, frame)
}

fn submit_command(host: &Host, frame: FrameWriter) -> Submitted {
    submit_request(
        host,
        frame,
        #[cfg(target_os = "linux")]
        Some(host.placement()),
    )
}

fn submit_request(
    host: &Host,
    frame: FrameWriter,
    #[cfg(target_os = "linux")] placement: Option<OwnedFd>,
) -> Submitted {
    let control = host.control();
    let directory = &host.directory;
    let (stdout_read, stdout_write) = pipe();
    let (stderr_read, stderr_write) = pipe();
    let stdin = OwnedFd::from(std::fs::File::open("/dev/null").expect("null device"));
    let frame = frame
        .bytes(directory.as_os_str().as_encoded_bytes())
        .unwrap()
        .list(std::iter::empty())
        .unwrap()
        .finish()
        .unwrap();
    #[cfg(target_os = "macos")]
    let (ready_read, ready_write) = pipe();
    #[cfg(target_os = "macos")]
    let (go_read, go_write) = pipe();
    #[cfg(target_os = "macos")]
    let descriptors = [
        stdin.as_raw_fd(),
        stdout_write.as_raw_fd(),
        stderr_write.as_raw_fd(),
        ready_write.as_raw_fd(),
        go_read.as_raw_fd(),
    ];
    #[cfg(target_os = "linux")]
    let descriptors = {
        let mut descriptors = vec![
            stdin.as_raw_fd(),
            stdout_write.as_raw_fd(),
            stderr_write.as_raw_fd(),
        ];
        descriptors.extend(placement.as_ref().map(|placement| placement.as_raw_fd()));
        descriptors
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let descriptors = [
        stdin.as_raw_fd(),
        stdout_write.as_raw_fd(),
        stderr_write.as_raw_fd(),
    ];
    let sent =
        send_with_descriptors(control.as_raw_fd(), &frame, &descriptors).expect("send the request");
    std::io::Write::write_all(&mut &*control, &frame[sent..]).expect("send the rest");
    drop((stdin, stdout_write, stderr_write));
    #[cfg(target_os = "linux")]
    drop(placement);
    cowshed_os::eprintln!(
        "script-fixture control={} phase=request-sent",
        control.as_raw_fd()
    );
    let readers = [stdout_read, stderr_read].map(|reader| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::fs::File::from(reader)
                .read_to_end(&mut bytes)
                .expect("read a job stream");
            String::from_utf8_lossy(&bytes).into_owned()
        })
    });
    #[cfg(target_os = "macos")]
    let _observation = {
        drop((ready_write, go_read));
        let mut ready = std::fs::File::from(ready_read);
        cowshed_os::eprintln!(
            "script-fixture control={} phase=waiting-observation-ready",
            control.as_raw_fd()
        );
        let mut pid = [0_u8; 4];
        let first = loop {
            match ready.read(&mut pid[..1]) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                read => break read.expect("read the held leader's report"),
            }
        };
        if first == 0 {
            None // Syntax or spawn failed before a leader existed.
        } else {
            ready
                .read_exact(&mut pid[1..])
                .expect("the complete leader report");
            let pid = u32::try_from(i32::from_ne_bytes(pid)).expect("leader pid");
            cowshed_os::eprintln!(
                "script-fixture control={} phase=leader-reported pid={pid}",
                control.as_raw_fd()
            );
            let observer = cowshed_core::runtime::process_events::KqueueObserver::watch(pid)
                .expect("attach before the script can run");
            assert_eq!(observer.fold().live(pid).map(|life| life.pid), Some(pid));
            std::io::Write::write_all(&mut std::fs::File::from(go_write), b"x")
                .expect("release the observed leader");
            cowshed_os::eprintln!(
                "script-fixture control={} phase=observer-released pid={pid}",
                control.as_raw_fd()
            );
            Some(observer)
        }
    };
    cowshed_os::eprintln!(
        "script-fixture control={} phase=waiting-start-reply",
        control.as_raw_fd()
    );
    let (reply, _) = read_frame(control).unwrap().expect("a reply");
    Submitted { reply, readers }
}

/// Run a rendered script on a host's control socket as a supervisor does: its exit, then its
/// streams to their end, then its release. Returns the job's exit and streams.
fn run_on(host: &Host, script: &RenderedScript, signal: Option<i32>) -> Ran {
    let control = host.control();
    let Submitted { reply, readers } = submit(host, script);
    let (tag, mut fields) = FrameReader::new(&reply).unwrap();
    let (birth, status) = match tag {
        REPLY_SCRIPT_SYNTAX => (None, None),
        REPLY_STARTED => {
            let birth = fields.birth().unwrap();
            cowshed_os::eprintln!(
                "script-fixture control={} phase=waiting-exit pid={}",
                control.as_raw_fd(),
                birth.pid()
            );
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
    cowshed_os::eprintln!(
        "script-fixture control={} phase=waiting-stream-eof",
        control.as_raw_fd()
    );
    let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
    if birth.is_some() {
        cowshed_os::eprintln!(
            "script-fixture control={} phase=waiting-release-reply",
            control.as_raw_fd()
        );
        release(control);
    }
    cowshed_os::eprintln!(
        "script-fixture control={} phase=run-completed",
        control.as_raw_fd()
    );
    Ran {
        pid: birth.as_ref().map(Birth::pid),
        birth,
        status,
        stdout,
        stderr,
    }
}

fn report_fixture_cleanup(message: std::fmt::Arguments<'_>) {
    use std::io::Write as _;
    match writeln!(std::io::stderr(), "{message}") {
        Ok(()) => {}
        Err(stderr_error) => match writeln!(
            std::io::stdout(),
            "{message}; stderr diagnostic unavailable: {stderr_error}"
        ) {
            Ok(()) => {}
            Err(_stdout_error) => {
                // Both existing channels are unavailable. A destructor cannot return their
                // errors; failed diagnostic delivery must not prevent later owned releases.
            }
        },
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            report_fixture_cleanup(format_args!("host fixture cleanup failed: {error}"));
        }
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

struct ScriptJobGuard {
    /// Only this direct child may be killed and reaped here, never a script grandchild.
    child: Option<std::process::Child>,
}

impl ScriptJobGuard {
    fn finish_child(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let Some(mut child) = self.child.take() else {
            return Ok(None);
        };
        match child.wait_bounded() {
            Ok(status) => Ok(Some(status)),
            Err(error) => {
                self.child = Some(child);
                Err(error)
            }
        }
    }
}

impl Drop for ScriptJobGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            drop(child.stdin.take());
            if let Err(cause) = child.kill() {
                report_fixture_cleanup(format_args!(
                    "script fixture owned-child stop failed: {cause}"
                ));
            }
        }
        if let Err(error) = self.finish_child() {
            report_fixture_cleanup(format_args!(
                "script fixture owned-child reap failed: {error}"
            ));
        }
    }
}

#[test]
fn a_script_has_its_own_streams_and_exact_status() {
    #[cfg(target_os = "linux")]
    if delegated_case("a_script_has_its_own_streams_and_exact_status") {
        return;
    }
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
    #[cfg(target_os = "linux")]
    if delegated_case("the_host_reports_the_leader_it_observed_before_reaping_it") {
        return;
    }
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
    #[cfg(target_os = "linux")]
    if delegated_case("the_host_signals_its_command_s_group_until_it_is_released") {
        return;
    }
    let mut host = Host::start("signal");
    let ran = host.run_signalled(&text("sleep 30"), libc::SIGKILL);
    assert_eq!(signaled(ran.status.as_ref()), Some(libc::SIGKILL));

    let marker = host.directory.join("descendant");
    let Submitted { reply, readers } = submit(&host, &outliving_descendant(&marker, 3));
    let (tag, _) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_STARTED);
    assert_eq!(read_exit(host.control()), ExitStatus::Exited { code: 3 });
    let descendant = loop {
        if let Some(pid) = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|pid| pid.trim().parse::<i32>().ok())
        {
            break pid;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    std::thread::sleep(Duration::from_millis(100));
    assert!(alive(descendant), "the descendant outlives the leader");
    assert!(
        readers.iter().all(|reader| !reader.is_finished()),
        "the descendant holds the job's output open"
    );

    send_signal(host.control(), libc::SIGKILL);
    let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
    assert_eq!((stdout.as_str(), stderr.as_str()), ("", ""));
    assert!(gone(descendant), "the signal reached the descendant");
    release(host.control());

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
    #[cfg(target_os = "linux")]
    if delegated_case("a_host_whose_supervisor_goes_away_ends_the_command_it_holds") {
        return;
    }
    let mut host = Host::start("orphan");
    let marker = host.directory.join("descendant");
    let Submitted { reply, readers } = submit(&host, &outliving_descendant(&marker, 0));
    let (tag, _) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_STARTED);
    assert_eq!(read_exit(host.control()), ExitStatus::Exited { code: 0 });
    let descendant = loop {
        if let Some(pid) = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|pid| pid.trim().parse::<i32>().ok())
        {
            break pid;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    host.close_control();
    let [stdout, _] = readers.map(|reader| reader.join().expect("stream reader"));
    assert_eq!(stdout, "");
    let host_status = loop {
        if let Some(status) = host.child.try_wait().expect("host status") {
            break status;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(host_status.code(), Some(0));
    assert!(gone(descendant), "the host ended the descendant");
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
    #[cfg(target_os = "linux")]
    if delegated_case("a_script_that_does_not_parse_never_runs") {
        return;
    }
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
    #[cfg(target_os = "linux")]
    if delegated_case("killing_a_script_job_group_reaches_its_grandchildren_and_spares_the_host") {
        return;
    }
    let mut host = Host::start("group");
    cowshed_os::eprintln!(
        "script-group phase=host-started host_pid={} control_fd={} fixture={}",
        host.child.id(),
        host.control().as_raw_fd(),
        host.directory.display()
    );
    let leader = host.directory.join("leader");
    let descendant = host.directory.join("descendant");
    let hold = host.directory.join("hold");
    for path in [&leader, &descendant, &hold] {
        let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("FIFO path");
        // SAFETY: a NUL-terminated path in this fixture's own directory.
        assert_eq!(
            unsafe { libc::mkfifo(path.as_ptr(), 0o600) },
            0,
            "{}",
            std::io::Error::last_os_error()
        );
    }
    let rendered = text(&format!(
        "sh -c 'printf \"%s\\n\" \"$$\" > {}; read -r _ < {}' & printf '%s\\n' \"$$\" > {}; wait",
        descendant.display(),
        hold.display(),
        leader.display()
    ));
    cowshed_os::eprintln!("script-group phase=submitting");
    let Submitted { reply, readers } = submit(&host, &rendered);
    cowshed_os::eprintln!("script-group phase=start-reply");
    let (tag, mut fields) = FrameReader::new(&reply).expect("start reply");
    assert_eq!(tag, REPLY_STARTED);
    let birth = fields
        .birth()
        .expect("the real parent observed its unreaped leader");
    fields.finish().expect("start frame");
    cowshed_os::eprintln!("script-group phase=started birth={birth:?}");
    // Each FIFO reports only after that exact process runs. No elapsed delay supplies readiness.
    let pids = [&leader, &descendant].map(|path| {
        cowshed_os::eprintln!("script-group phase=reading-pid fifo={}", path.display());
        let value = std::fs::read_to_string(path).expect("read the actual process's FIFO");
        let pid = value.trim().parse::<i32>().expect("published process PID");
        cowshed_os::eprintln!(
            "script-group phase=pid-read fifo={} pid={pid}",
            path.display()
        );
        pid
    });
    assert_eq!(birth.pid(), pids[0].cast_unsigned());
    cowshed_os::eprintln!("script-group phase=reading-members birth={birth:?} pids={pids:?}");
    let members =
        cowshed_core::runtime::job_groups::job_members(&birth).expect("identity-proven live group");
    for member in &members {
        cowshed_os::eprintln!("script-group phase=member-read pid={}", member.pid());
    }
    let exits = pids.map(|pid| {
        let member = members
            .iter()
            .find(|member| member.pid() == pid)
            .expect("published process is in the actual owned group");
        cowshed_os::eprintln!("script-group phase=watching-exit pid={pid}");
        let exit = member
            .watch_exit()
            .expect("watch this retained life before killing it");
        assert!(
            !exit.within(Duration::ZERO).expect("current exit readiness"),
            "process {pid} already exited"
        );
        cowshed_os::eprintln!("script-group phase=exit-watched-running pid={pid}");
        exit
    });
    // A real group signal must end both retained lives, not just the leader or its output pipes.
    cowshed_os::eprintln!("script-group phase=signalling signal={}", libc::SIGTERM);
    send_signal(host.control(), libc::SIGTERM);
    cowshed_os::eprintln!("script-group phase=reading-exit");
    let status = read_exit(host.control());
    cowshed_os::eprintln!("script-group phase=exit-read status={status:?}");
    assert_eq!(signaled(Some(&status)), Some(libc::SIGTERM));
    for (pid, exit) in pids.into_iter().zip(exits) {
        cowshed_os::eprintln!("script-group phase=awaiting-native-exit pid={pid}");
        assert!(
            exit.within(Duration::from_secs(10))
                .expect("native exit notification"),
            "retained process {pid} of the killed script still runs"
        );
        cowshed_os::eprintln!("script-group phase=native-exit-observed pid={pid}");
    }
    cowshed_os::eprintln!("script-group phase=joining-output-readers");
    let [stdout, stderr] = readers.map(|reader| reader.join().expect("owned output reader"));
    cowshed_os::eprintln!("script-group phase=outputs-ended");
    assert_eq!((stdout.as_str(), stderr.as_str()), ("", ""));
    cowshed_os::eprintln!("script-group phase=releasing-leader");
    release(host.control()); // Only its actual parent reaps the script's leader.
    cowshed_os::eprintln!("script-group phase=leader-reaped");
    assert!(
        host.child.try_wait().expect("host status").is_none(),
        "the host is not in the job's group"
    );
    cowshed_os::eprintln!("script-group phase=reusing-host");
    assert_eq!(host.run(&text("printf again")).stdout, "again");
    cowshed_os::eprintln!("script-group phase=finishing-fixture");
    host.finish().expect("finish the successful host fixture");
    cowshed_os::eprintln!("script-group phase=fixture-finished");
}

/// The old null-signal oracle called an exited, unreaped child alive. The exit watch must not.
#[test]
fn an_unreaped_zombie_is_exited_even_while_the_null_signal_reaches_it() {
    let child = std::process::Command::new("/bin/sh")
        .args(["-c", "printf r; read -r _"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .process_group(0)
        .spawn()
        .expect("owned pipe-held child");
    let pid = i32::try_from(child.id()).expect("child PID");
    let mut owner = ScriptJobGuard { child: Some(child) };
    let child = owner
        .child
        .as_mut()
        .expect("the guard owns its direct child");
    // Spawn returning is not executed-body readiness. Keep stdin held until
    // the shell itself has reached the gate and the exit watch is registered.
    let mut ready = [0];
    child
        .stdout
        .take()
        .expect("piped readiness")
        .read_exact(&mut ready)
        .expect("the shell reached its input gate");
    assert_eq!(&ready, b"r", "the held shell's readiness byte");
    let birth = Birth::of(child.id());
    let members = cowshed_core::runtime::job_groups::job_members(&birth).expect("owned group");
    let retained = members
        .iter()
        .find(|member| member.pid() == pid)
        .expect("the owned leader");
    let exit = retained
        .watch_exit()
        .expect("watch the actual running child");
    let running = exit.within(Duration::ZERO).expect("live readiness");
    drop(child.stdin.take());
    let status =
        cowshed_core::runtime::job_groups::await_exit_unreaped(pid).expect("real exit, no reap");
    let null_signal_reaches = alive(pid);
    let exited = exit
        .within(Duration::ZERO)
        .expect("zombie exit notification");
    #[cfg(target_os = "linux")]
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
    owner
        .finish_child()
        .expect("join and consume the guard's owned child exactly once");
    assert!(!running, "the held child was running before EOF");
    assert!(
        matches!(status, ExitStatus::Exited { .. }),
        "the owned child actually exited: {status:?}"
    );
    assert!(
        null_signal_reaches,
        "POSIX null signal succeeds on the owned unreaped zombie"
    );
    assert!(
        exited,
        "positive native exit evidence while the child remained unreaped"
    );
    #[cfg(target_os = "linux")]
    {
        let stat = stat.expect("unreaped child's real kernel state");
        let closing = stat.rfind(')').expect("stat command delimiter");
        assert_eq!(
            stat[closing + 1..].split_whitespace().next(),
            Some("Z"),
            "actual Linux zombie state: {stat}"
        );
        cowshed_os::eprintln!(
            "owned-zombie pid={pid} kernel_state=Z null_signal_reaches=true exit_watch_ready=true reaped_by_owner=true"
        );
    }
}

/// Scripts forked at once each run whole. Two creations of one process group race, and macOS
/// refuses the loser with `EPERM`: were the host to create the child's group too, a refused
/// child would end its script with 126 before it ran and a refused host would end itself.
/// Several hosts forking back to back make the race likely.
#[test]
fn scripts_forked_at_once_each_run_whole() {
    #[cfg(target_os = "linux")]
    if delegated_case("scripts_forked_at_once_each_run_whole") {
        return;
    }
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
    #[cfg(target_os = "linux")]
    if delegated_case("nothing_one_script_does_to_its_process_reaches_the_next") {
        return;
    }
    cowshed_os::eprintln!("script-isolation phase=starting-host");
    let mut host = Host::start("isolation");
    cowshed_os::eprintln!("script-isolation phase=first-command");
    let first = host.run(&text(
        "umask 077; ulimit -n 64; cd /; export LEAK=1; trap 'echo trapped' EXIT",
    ));
    cowshed_os::eprintln!("script-isolation phase=first-completed");
    assert_eq!(exited(first.status.as_ref()), Some(0));
    let second = host.run(&text(
        "umask; ulimit -n; printf '%s|%s\\n' \"$PWD\" \"${LEAK-}\"",
    ));
    cowshed_os::eprintln!("script-isolation phase=second-completed");
    let lines: Vec<&str> = second.stdout.lines().collect();
    assert_ne!(lines[0], "0077", "umask leaked: {}", second.stdout);
    assert_ne!(lines[1], "64", "ulimit leaked: {}", second.stdout);
    assert_eq!(lines[2], format!("{}|", host.directory.display()));
    assert!(!second.stdout.contains("trapped"));
    cowshed_os::eprintln!("script-isolation phase=assertions-completed");
}

#[test]
fn values_reach_the_script_as_data_in_every_quoting_context() {
    #[cfg(target_os = "linux")]
    if delegated_case("values_reach_the_script_as_data_in_every_quoting_context") {
        return;
    }
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
    #[cfg(target_os = "linux")]
    if delegated_case("the_shared_corpus_parses_and_prints_what_bash_does") {
        return;
    }
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
    #[cfg(target_os = "linux")]
    if delegated_case("a_host_that_cannot_run_direnv_says_so") {
        return;
    }
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
        host.control().as_raw_fd(),
        &frame,
        &[stdout_write.as_raw_fd(), stderr_write.as_raw_fd()],
    )
    .expect("send the request");
    std::io::Write::write_all(&mut host.control(), &frame[sent..]).expect("send the rest");
    drop((stdout_write, stderr_write));
    let (reply, _) = read_frame(host.control())
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

#[cfg(target_os = "linux")]
fn linux_request(tag: u8, source: &str) -> FrameWriter {
    match tag {
        REQUEST_SCRIPT => FrameWriter::new(tag)
            .bytes(source.as_bytes())
            .unwrap()
            .u32(0),
        cowshed_core::runtime::shell_host::REQUEST_RUN => FrameWriter::new(tag)
            .list([b"/bin/sh".as_slice(), b"-c", source.as_bytes()].into_iter())
            .unwrap(),
        _ => panic!("not a command request"),
    }
}

#[cfg(target_os = "linux")]
#[test]
fn linux_argv_and_scripts_enter_the_job_cgroup_without_leaking_placement() {
    if delegated_case("linux_argv_and_scripts_enter_the_job_cgroup_without_leaking_placement") {
        return;
    }
    let host = Host::start("cgroup-placement");
    let host_membership = std::fs::read_to_string(format!("/proc/{}/cgroup", host.child.id()))
        .expect("the host's initial cgroup");
    for tag in [
        cowshed_core::runtime::shell_host::REQUEST_RUN,
        REQUEST_SCRIPT,
    ] {
        let ready = host.directory.join(format!("ready-{tag}"));
        let hold = host.directory.join(format!("hold-{tag}"));
        for path in [&ready, &hold] {
            let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            // SAFETY: the path is NUL-terminated and belongs to this fixture.
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        }
        // Only builtins read and report the interpreter's own membership before it can run an
        // external program. The FIFO holds that same process alive for descriptor inspection.
        let source = format!(
            "while IFS= read -r line; do printf '%s\\n' \"$line\"; done < /proc/self/cgroup; \
             printf ready > \"{}\"; read -r _ < \"{}\"",
            ready.display(),
            hold.display(),
        );
        let Submitted { reply, readers } = submit_command(&host, linux_request(tag, &source));
        let (tag, mut fields) = FrameReader::new(&reply).expect("start reply");
        assert_eq!(tag, REPLY_STARTED);
        let birth = fields.birth().expect("the held command");
        fields.finish().expect("complete start reply");
        assert_eq!(std::fs::read_to_string(&ready).unwrap(), "ready");
        let jobs = host.jobs.borrow();
        let job = jobs.last().expect("the request's admitted cgroup");
        let membership = format!(
            "0::/{}\n",
            job.path()
                .strip_prefix(cowshed_core::runtime::job_cgroup::CGROUP_MOUNT)
                .expect("cgroup beneath the unified mount")
                .display(),
        );
        assert_eq!(
            std::fs::read_to_string(format!("/proc/{}/cgroup", birth.pid())).unwrap(),
            membership,
        );
        for descriptor in
            std::fs::read_dir(format!("/proc/{}/fd", birth.pid())).expect("the command's fds")
        {
            let path = descriptor.expect("a descriptor").path();
            let target = match std::fs::read_link(&path) {
                Ok(target) => target,
                // The ready FIFO can close between readdir and readlink.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => panic!("readlink {}: {error}", path.display()),
            };
            assert_ne!(
                target,
                job.path().join("cgroup.procs"),
                "placement authority leaked into job code",
            );
        }
        assert_eq!(
            std::fs::read_to_string(format!("/proc/{}/cgroup", host.child.id())).unwrap(),
            host_membership,
            "the reusable host never enters a job cgroup",
        );
        assert_ne!(membership, host_membership);
        job.kill().expect("kill through the job cgroup");
        assert_eq!(
            signaled(Some(&read_exit(host.control()))),
            Some(libc::SIGKILL)
        );
        job.kill_and_wait(cowshed_os::deadline::REAP)
            .expect("kernel confirms the job is empty");
        let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
        assert_eq!(
            stdout, membership,
            "the first job code observed its own cgroup"
        );
        assert_eq!(stderr, "");
        release(host.control());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn linux_commands_without_a_placement_descriptor_are_refused() {
    if delegated_case("linux_commands_without_a_placement_descriptor_are_refused") {
        return;
    }
    for tag in [
        cowshed_core::runtime::shell_host::REQUEST_RUN,
        REQUEST_SCRIPT,
    ] {
        let host = Host::start("missing-placement");
        let marker = host.directory.join("ran");
        let source = format!("printf ran > \"{}\"", marker.display());
        let Submitted { reply, readers } = submit_request(&host, linux_request(tag, &source), None);
        let (tag, mut fields) = FrameReader::new(&reply).expect("refusal reply");
        assert_eq!(tag, REPLY_HOST_FAILED);
        let diagnostic = String::from_utf8_lossy(fields.bytes().unwrap());
        assert!(diagnostic.contains("cgroup placement"), "{diagnostic}");
        fields.finish().unwrap();
        let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
        assert_eq!((stdout.as_str(), stderr.as_str()), ("", ""));
        assert!(!marker.exists(), "nothing runs without cgroup placement");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn linux_refused_cgroup_placement_never_runs_the_command() {
    if delegated_case("linux_refused_cgroup_placement_never_runs_the_command") {
        return;
    }
    for tag in [
        cowshed_core::runtime::shell_host::REQUEST_RUN,
        REQUEST_SCRIPT,
    ] {
        let mut host = Host::start("refused-placement");
        drop(host.placement());
        let path = host
            .jobs
            .borrow()
            .last()
            .unwrap()
            .path()
            .join("cgroup.procs");
        // A real cgroup interface, deliberately opened read-only: entering it must fail. No
        // ordinary file stands in for a cgroup, even in the negative case.
        let placement = OwnedFd::from(std::fs::File::open(path).expect("read-only cgroup.procs"));
        let marker = host.directory.join("ran");
        let source = format!("printf ran > \"{}\"", marker.display());
        let Submitted { reply, readers } =
            submit_request(&host, linux_request(tag, &source), Some(placement));
        let (tag, mut fields) = FrameReader::new(&reply).expect("placement refusal");
        assert_eq!(tag, REPLY_EXITED, "a refused child was never started");
        assert_eq!(fields.exit().unwrap(), ExitStatus::Exited { code: 126 });
        fields.finish().unwrap();
        let [stdout, stderr] = readers.map(|reader| reader.join().expect("stream reader"));
        assert_eq!(stdout, "");
        assert!(stderr.contains("cgroup placement"), "{stderr}");
        assert!(!marker.exists(), "a placement failure never runs job code");
        let ran = host.run(&text("printf after"));
        assert_eq!(exited(ran.status.as_ref()), Some(0));
        assert_eq!(
            ran.stdout, "after",
            "placement failure did not enter the host"
        );
    }
}

/// A checkout tree the adoption tests record at `origin` and adopt at `clone`, inside the host's
/// own directory so the host's baseline (its PATH and HOME) is the same for both. The recorded
/// export carries `LOCAL_TOKEN`; `tail_supplies_token` decides whether the checkouts' own
/// `.envrc-local` sets it. Returns the clone and the adoption request.
fn adopt_in_fixture(host: &Host, tail_supplies_token: bool) -> (PathBuf, Vec<u8>) {
    use cowshed_core::runtime::shell_host::{Inherited, SHELL_HOST_DIRECTORY};
    use cowshed_core::runtime::shell_inherit::{Placement, record_path, verify, write_record};
    use cowshed_core::runtime::shell_watch::{WatchEntry, certify};

    let origin = host.directory.join("origin");
    let clone = host.directory.join("clone");
    let local = if tail_supplies_token {
        "export LOCAL_TOKEN=from-local\n"
    } else {
        "export LOCAL_OTHER=1\n"
    };
    for root in [&origin, &clone] {
        std::fs::create_dir_all(root.join(SHELL_HOST_DIRECTORY)).unwrap();
        std::fs::write(root.join(".envrc"), "use devenv\n").unwrap();
        let name = root.file_name().unwrap().to_string_lossy().into_owned();
        std::fs::write(
            root.join(".cowshed/env"),
            format!("export COWSHED_RUNTIME_LINK=/tmp/cs-{name}\n"),
        )
        .unwrap();
        std::fs::write(root.join(".envrc-local"), local).unwrap();
    }
    let baseline: std::collections::BTreeMap<std::ffi::OsString, std::ffi::OsString> = [
        ("PATH", std::env::var_os("PATH").expect("PATH")),
        ("HOME", host.directory.clone().into_os_string()),
    ]
    .into_iter()
    .map(|(name, value)| (name.into(), value))
    .collect();
    // The origin's own evaluation, as recorded; the clone then carries its record.
    let evaluate = |root: &Path| {
        let entries: Vec<WatchEntry> = [".envrc", ".cowshed/env", ".envrc-local"]
            .iter()
            .map(|input| WatchEntry {
                path: root.join(input),
                exists: true,
            })
            .collect();
        let (_, contents) = certify(entries.iter().map(|entry| entry.path.as_path())).unwrap();
        let name = root.file_name().unwrap().to_string_lossy().into_owned();
        let export = [
            ("AT_ROOT", Some(format!("{}/tool", root.display()))),
            ("LOCAL_TOKEN", Some("from-local".to_owned())),
            ("COWSHED_RUNTIME_LINK", Some(format!("/tmp/cs-{name}"))),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
        write_record(
            &Placement::of(root, &baseline, &[]),
            &baseline,
            &entries,
            &contents,
            &export,
        )
        .unwrap()
    };
    evaluate(&origin);
    let written = std::fs::read_to_string(record_path(&origin)).unwrap();
    assert!(
        !written.contains("from-local"),
        "the record holds no credential value"
    );
    std::fs::copy(record_path(&origin), record_path(&clone)).unwrap();
    let verified =
        verify(&Placement::of(&clone, &baseline, &[]), &baseline).expect("the record verifies");
    let inherited = serde_json::to_vec(&Inherited::of(&verified)).unwrap();
    let frame = FrameWriter::new(REQUEST_ADOPT)
        .tree_owner(cowshed_core::runtime::process_tree::TreeOwner::Job(
            cowshed_core::api::JobId::new(1).expect("job id"),
        ))
        .bytes(clone.as_os_str().as_encoded_bytes())
        .unwrap()
        .bytes(&inherited)
        .unwrap()
        .finish()
        .unwrap();
    (clone, frame)
}

/// An adopted shell lays the moved export over the host's environment and takes the values the
/// record withholds from the checkout's own tail files, read through direnv's stdlib.
#[test]
fn an_adopted_shell_takes_its_credentials_from_its_own_tail_files() {
    #[cfg(target_os = "linux")]
    if delegated_case("an_adopted_shell_takes_its_credentials_from_its_own_tail_files") {
        return;
    }
    let mut host = Host::start("adopt");
    let (clone, request) = adopt_in_fixture(&host, true);
    let _diagnostics = submit_adoption(&host, &request);
    let (reply, _) = read_frame(host.control())
        .unwrap()
        .expect("an adoption reply");
    let (tag, _) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_ACTIVATED, "{}", String::from_utf8_lossy(&reply));
    let ran = host.run(&text(
        "printf '%s|%s|%s' \"$AT_ROOT\" \"$LOCAL_TOKEN\" \"$COWSHED_RUNTIME_LINK\"",
    ));
    assert_eq!(
        ran.stdout,
        format!("{}/tool|from-local|/tmp/cs-clone", clone.display())
    );
}

/// A tail that does not supply a withheld value refuses the adoption and leaves the host as it
/// was, so the supervisor evaluates on the same host -- even when the host already holds a value
/// of that name of its own, which must never stand in for the one the activation exported.
#[test]
fn a_tail_that_lacks_a_withheld_value_refuses_and_changes_nothing() {
    #[cfg(target_os = "linux")]
    if delegated_case("a_tail_that_lacks_a_withheld_value_refuses_and_changes_nothing") {
        return;
    }
    let mut host = Host::start_with(
        "adopt-refused",
        &std::env::var_os("PATH").expect("PATH"),
        &[("LOCAL_TOKEN", "ambient")],
    );
    let (_, request) = adopt_in_fixture(&host, false);
    let _diagnostics = submit_adoption(&host, &request);
    let (reply, _) = read_frame(host.control())
        .unwrap()
        .expect("an adoption reply");
    let (tag, mut fields) = FrameReader::new(&reply).unwrap();
    assert_eq!(tag, REPLY_ADOPTION_REFUSED);
    let reason = String::from_utf8_lossy(fields.bytes().unwrap()).into_owned();
    assert!(reason.contains("LOCAL_TOKEN"), "{reason}");
    let ran = host.run(&text(
        "printf '%s|%s' \"${AT_ROOT-unset}\" \"$LOCAL_TOKEN\"",
    ));
    assert_eq!(ran.stdout, "unset|ambient");
}

fn submit_adoption(host: &Host, request: &[u8]) -> OwnedFd {
    let (read, write) = pipe();
    send_with_descriptors(host.control().as_raw_fd(), request, &[write.as_raw_fd()]).unwrap();
    drop(write);
    read
}

/// An inherited tail has the same activation-root custody as direnv export: its escaped child
/// must exit before the host ends after control EOF (11_shell.md, "Inherited shells").
#[cfg(target_os = "macos")]
#[test]
fn an_adopted_tail_s_escaped_descendant_ends_when_control_closes() {
    use cowshed_os::process::ExitWatch;
    use std::os::unix::process::ExitStatusExt as _;

    let mut host = Host::start("adopt-orphan");
    let (clone, request) = adopt_in_fixture(&host, true);
    for name in ["adopt-ready", "adopt-hold"] {
        let path = std::ffi::CString::new(host.directory.join(name).as_os_str().as_encoded_bytes())
            .expect("FIFO path");
        // SAFETY: a NUL-terminated path in this fixture's own directory.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    }
    std::fs::write(
        clone.join(".envrc-local"),
        r#"export LOCAL_TOKEN=from-local
/usr/bin/perl -MPOSIX -e 'POSIX::setsid() > 0 or die "setsid: $!"; open(my $f, ">", "$ENV{HOME}/adopt-ready") or die "ready: $!"; print $f $$; close $f; exec "/bin/sleep", "600" or die "exec: $!"' &
read _ < "$HOME/adopt-hold"
"#,
    )
    .expect("write the held tail");
    let mut diagnostics = std::fs::File::from(submit_adoption(&host, &request));
    let escaped: u32 = std::fs::read_to_string(host.directory.join("adopt-ready"))
        .expect("the escaped child reports readiness")
        .parse()
        .expect("child pid");
    // SAFETY: getpgid reads the two live process groups.
    assert_ne!(
        unsafe { libc::getpgid(i32::try_from(escaped).unwrap()) },
        unsafe { libc::getpgid(i32::try_from(host.child.id()).unwrap()) },
        "the tail child left the host's process group"
    );
    let escaped_exit = ExitWatch::of(escaped)
        .expect("watch the escaped child")
        .expect("the child is still running");
    let host_exit = ExitWatch::of(host.child.id())
        .expect("watch the host")
        .expect("the host still waits on the tail");
    host.close_control();
    assert!(
        host_exit
            .within(Duration::from_secs(60))
            .expect("host exit"),
        "control EOF ends the host after retiring the tail"
    );
    assert_eq!(
        host.child.wait_bounded().expect("reap host").signal(),
        Some(libc::SIGKILL)
    );
    assert!(
        escaped_exit.within(Duration::ZERO).expect("escaped exit"),
        "the escaped tail child exited before the host"
    );
    let mut reported = String::new();
    diagnostics
        .read_to_string(&mut reported)
        .expect("diagnostic EOF");
    assert!(!reported.contains("retirement failed"), "{reported}");
}
