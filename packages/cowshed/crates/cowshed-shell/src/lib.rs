//! The warm exec host: one process that holds an activated workspace shell and runs each
//! command from it (`cowshed_core::runtime::shell_host` is the protocol, 11_shell.md the design).
//!
//! # Why a host process and not a live bash
//!
//! A job owns anonymous stdin/stdout/stderr pipes that only the supervisor holds the other end
//! of, a process group the supervisor can signal, and an exact wait status (exit code, or
//! terminating signal and core flag). A long-lived bash can receive none of these per command:
//! it cannot accept descriptors over a socket, named FIFOs are reachable by sibling jobs, and
//! its `wait` folds a signal death into `128+N`. The host therefore is a small program that
//! holds the activated environment and does what bash cannot: it receives the job's descriptors
//! with `SCM_RIGHTS`, starts the job in its own process group, and reports the raw `waitpid`
//! status. It runs under the executed-child Seatbelt profile like every command, so it adds no
//! authority, and activation still runs repository code only inside that profile.
//!
//! Activation is direnv's own: `direnv export json` evaluated from the host's sandbox
//! environment, its diff applied once. The resulting `DIRENV_WATCHES` is reported to the
//! supervisor, which decides freshness from it; the host never re-evaluates.
//!
//! An argv job is forked and exec'd directly. A script job is forked without exec: the child
//! leads the job's process group and runs the parsed program in an upstream brush interpreter
//! built from the already-activated environment ([`script`]).

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::net::UnixStream;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use cowshed_core::runtime::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, CONTROL_DESCRIPTOR, FrameReader, FrameWriter, REPLY_ACTIVATED,
    REPLY_ACTIVATION_EXITED, REPLY_ACTIVATION_UNUSABLE, REPLY_APPROVED, REPLY_EXITED,
    REPLY_STARTED, REQUEST_ACTIVATE, REQUEST_APPROVE, REQUEST_RUN, REQUEST_SCRIPT, RawWaitStatus,
    SHELL_HOST_ARGUMENT, ShellHostProgram, read_frame,
};
use cowshed_core::script::Binding;

mod script;

/// Upper bound on `direnv export json` output kept in memory.
const MAX_EXPORT_BYTES: usize = 64 * 1024 * 1024;

/// Serve as an exec host if this process was started as one, and never return in that case.
/// Otherwise register this same binary as the program that starts exec hosts, so workspace
/// supervisors this process starts run commands in warm shells.
///
/// A binary that hosts workspace supervisors calls this first in `main`, before any runtime or
/// thread exists: the host forks, and a single-threaded process is the one in which that is
/// unconditionally sound.
pub fn dispatch() -> io::Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next().as_deref() == Some(OsStr::new(SHELL_HOST_ARGUMENT)) {
        std::process::exit(serve());
    }
    cowshed_core::runtime::shell_host::register(ShellHostProgram::with_arguments(
        std::env::current_exe()?,
        vec![SHELL_HOST_ARGUMENT.into()],
    ));
    Ok(())
}

/// The wait status a shell reports for a command it could not execute: 127 when the program
/// does not exist, 126 when it exists but cannot run.
fn unexecutable_status(error: &io::Error) -> RawWaitStatus {
    let code = if error.kind() == io::ErrorKind::NotFound {
        127
    } else {
        126
    };
    code << 8
}

/// Run the exec host on [`CONTROL_DESCRIPTOR`] until the supervisor closes it. Returns the
/// process exit code.
pub fn serve() -> i32 {
    // SAFETY: fcntl(F_GETFD) only queries the descriptor table.
    if unsafe { libc::fcntl(CONTROL_DESCRIPTOR, libc::F_GETFD) } < 0 {
        return 64;
    }
    // SAFETY: descriptor 3 was installed by the supervisor for this process alone and nothing
    // else in this process owns it.
    let socket = unsafe { UnixStream::from_raw_fd(CONTROL_DESCRIPTOR) };
    // SAFETY: plain fcntl on an owned descriptor; commands must not inherit the control socket.
    unsafe { libc::fcntl(CONTROL_DESCRIPTOR, libc::F_SETFD, libc::FD_CLOEXEC) };
    let mut host = Host {
        environment: std::env::vars_os().collect(),
    };
    loop {
        match read_frame(&socket) {
            Ok(Some((payload, descriptors))) => {
                if let Err(error) = host.handle(&socket, &payload, descriptors) {
                    // The host's own stderr is /dev/null; the supervisor observes the closed
                    // socket and the exit status.
                    let _ = error;
                    return 70;
                }
            }
            Ok(None) => return 0,
            Err(_) => return 65,
        }
    }
}

struct Host {
    environment: BTreeMap<OsString, OsString>,
}

fn one_descriptor(descriptors: &mut Vec<OwnedFd>, what: &str) -> io::Result<OwnedFd> {
    if descriptors.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("request carried no {what} descriptor"),
        ));
    }
    Ok(descriptors.remove(0))
}

fn reply(socket: &UnixStream, frame: FrameWriter) -> io::Result<()> {
    let mut socket = socket;
    socket.write_all(&frame.finish()?)
}

impl Host {
    fn handle(
        &mut self,
        socket: &UnixStream,
        payload: &[u8],
        mut descriptors: Vec<OwnedFd>,
    ) -> io::Result<()> {
        let (tag, mut fields) = FrameReader::new(payload)?;
        match tag {
            REQUEST_APPROVE => {
                let envrc = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                fields.finish()?;
                let stdout = one_descriptor(&mut descriptors, "stdout")?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                let status = self.approve(&envrc, stdout, stderr)?;
                reply(socket, FrameWriter::new(REPLY_APPROVED).i32(status))
            }
            REQUEST_ACTIVATE => {
                let directory = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                fields.finish()?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                self.activate(socket, &directory, stderr)
            }
            REQUEST_SCRIPT => {
                let text = fields.bytes()?;
                let count = usize::try_from(fields.u32()?).map_err(io::Error::other)?;
                let mut bindings = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    let name = utf8(fields.bytes()?)?.to_owned();
                    let kind = fields.u32()?;
                    let values = fields
                        .list()?
                        .into_iter()
                        .map(|value| utf8(value).map(str::to_owned))
                        .collect::<io::Result<Vec<_>>>()?;
                    bindings.push(match (u8::try_from(kind), values.as_slice()) {
                        (Ok(BINDING_SCALAR), [value]) => Binding::Scalar {
                            name,
                            value: value.clone(),
                        },
                        (Ok(BINDING_ARRAY), _) => Binding::Array { name, values },
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "malformed script binding",
                            ));
                        }
                    });
                }
                let cwd = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                let overlay = fields.list()?;
                fields.finish()?;
                let stdin = one_descriptor(&mut descriptors, "stdin")?;
                let stdout = one_descriptor(&mut descriptors, "stdout")?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                let environment = self.command_environment(&cwd, &overlay)?;
                script::run(
                    socket,
                    utf8(text)?,
                    &bindings,
                    &cwd,
                    &environment,
                    [stdin, stdout, stderr],
                )
            }
            REQUEST_RUN => {
                let argv = fields.list()?;
                let cwd = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                let overlay = fields.list()?;
                fields.finish()?;
                let stdin = one_descriptor(&mut descriptors, "stdin")?;
                let stdout = one_descriptor(&mut descriptors, "stdout")?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                self.run(socket, &argv, &cwd, &overlay, [stdin, stdout, stderr])
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown control request {other}"),
            )),
        }
    }

    fn tool(&self, directory: &Path) -> Command {
        let mut command = Command::new("direnv");
        command
            .env_clear()
            .envs(&self.environment)
            .env("PWD", directory)
            .current_dir(directory);
        command
    }

    /// Approve the `.envrc` in the workspace's private trust store, unless it already is.
    ///
    /// `direnv allow` rewrites its allow file on every call, and that file is one of the inputs
    /// direnv records: approving an approved file would mark every generation stale.
    fn approve(&self, envrc: &Path, stdout: OwnedFd, stderr: OwnedFd) -> io::Result<RawWaitStatus> {
        let directory = envrc.parent().unwrap_or(Path::new("/"));
        let status = self
            .tool(directory)
            .args(["status", "--json"])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output();
        if let Ok(output) = status
            && output.status.success()
            && approved(&output.stdout, envrc)
        {
            return Ok(0);
        }
        let status = self
            .tool(directory)
            .arg("allow")
            .arg(envrc)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()?;
        Ok(status.into_raw())
    }

    /// Evaluate the `.envrc` once and apply direnv's exported diff to this host's environment.
    fn activate(
        &mut self,
        socket: &UnixStream,
        directory: &Path,
        stderr: OwnedFd,
    ) -> io::Result<()> {
        // An activation can run for minutes (a Nix evaluation, an install). If the supervisor
        // that asked for it goes away — its process exited, or it dropped this host — nothing
        // will ever use the result, so the evaluation must not keep running unobserved.
        let _orphan_guard = OrphanGuard::watch(socket)?;
        let mut child = self
            .tool(directory)
            .args(["export", "json"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()?;
        let mut exported = Vec::new();
        let read = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("direnv export has no stdout pipe"))?
            .take(u64::try_from(MAX_EXPORT_BYTES + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut exported);
        let status = child.wait()?;
        read?;
        if !status.success() {
            return reply(
                socket,
                FrameWriter::new(REPLY_ACTIVATION_EXITED).i32(status.into_raw()),
            );
        }
        if exported.len() > MAX_EXPORT_BYTES {
            return reply(
                socket,
                FrameWriter::new(REPLY_ACTIVATION_UNUSABLE)
                    .bytes(b"direnv export json output exceeds its bound")?,
            );
        }
        let diff: BTreeMap<String, Option<String>> = match serde_json::from_slice(&exported) {
            Ok(diff) => diff,
            Err(error) => {
                return reply(
                    socket,
                    FrameWriter::new(REPLY_ACTIVATION_UNUSABLE).bytes(
                        format!("direnv export json is not an environment diff: {error}")
                            .as_bytes(),
                    )?,
                );
            }
        };
        for (name, value) in diff {
            match value {
                Some(value) => {
                    self.environment.insert(name.into(), value.into());
                }
                None => {
                    self.environment.remove(OsStr::new(&name));
                }
            }
        }
        let watches = self
            .environment
            .get(OsStr::new("DIRENV_WATCHES"))
            .map(|value| value.as_bytes().to_vec());
        let frame = FrameWriter::new(REPLY_ACTIVATED).u32(u32::from(watches.is_some()));
        let frame = match &watches {
            Some(watches) => frame.bytes(watches)?,
            None => frame,
        };
        reply(socket, frame)
    }

    /// The activated environment with one command's overlay and cwd laid over it.
    fn command_environment(
        &self,
        cwd: &Path,
        overlay: &[&[u8]],
    ) -> io::Result<BTreeMap<OsString, OsString>> {
        let (pairs, rest) = overlay.as_chunks::<2>();
        if !rest.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "environment overlay is not name/value pairs",
            ));
        }
        let mut environment = self.environment.clone();
        for [name, value] in pairs {
            environment.insert(
                OsString::from_vec(name.to_vec()),
                OsString::from_vec(value.to_vec()),
            );
        }
        environment.insert("PWD".into(), cwd.as_os_str().to_owned());
        Ok(environment)
    }

    fn run(
        &self,
        socket: &UnixStream,
        argv: &[&[u8]],
        cwd: &Path,
        overlay: &[&[u8]],
        [stdin, stdout, stderr]: [OwnedFd; 3],
    ) -> io::Result<()> {
        let Some((program, arguments)) = argv.split_first() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty argv"));
        };
        let environment = self.command_environment(cwd, overlay)?;
        let diagnostics = stderr.try_clone()?;
        let spawned = Command::new(OsStr::from_bytes(program))
            .args(arguments.iter().map(|argument| OsStr::from_bytes(argument)))
            .env_clear()
            .envs(&environment)
            .current_dir(cwd)
            .process_group(0)
            .stdin(Stdio::from(stdin))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn();
        let status = match spawned {
            Ok(mut child) => {
                reply(socket, FrameWriter::new(REPLY_STARTED).u32(child.id()))?;
                child.wait()?.into_raw()
            }
            Err(error) => {
                let mut diagnostics = std::fs::File::from(diagnostics);
                let _ = writeln!(
                    diagnostics,
                    "cowshed: cannot run {}: {error}",
                    String::from_utf8_lossy(program)
                );
                unexecutable_status(&error)
            }
        };
        reply(socket, FrameWriter::new(REPLY_EXITED).i32(status))
    }
}

/// Ends this host's whole process group — the host and everything its activation started —
/// when the control socket reaches end of stream while the guard lives.
///
/// A thread polls the socket and a cancel pipe. The supervisor never writes during an
/// activation, so readable data is not expected; end of stream is recognized by a zero-byte
/// `MSG_PEEK`, which consumes nothing. Dropping the guard cancels the watch.
struct OrphanGuard {
    cancel: Option<OwnedFd>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl OrphanGuard {
    fn watch(socket: &UnixStream) -> io::Result<Self> {
        let mut ends = [0; 2];
        // SAFETY: `ends` has room for the two descriptors pipe(2) writes.
        if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both descriptors were just created and are owned here alone.
        let (cancelled, cancel) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        for end in [&cancelled, &cancel] {
            // SAFETY: plain fcntl on an owned descriptor; activation children must not inherit it.
            unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        }
        let control = socket.as_raw_fd();
        let thread = std::thread::Builder::new()
            .name("cowshed-shell-host-orphan-guard".into())
            .spawn(move || {
                let mut watched = [
                    libc::pollfd {
                        fd: control,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: cancelled.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                loop {
                    // SAFETY: `watched` holds two initialized pollfd entries.
                    let ready = unsafe { libc::poll(watched.as_mut_ptr(), 2, -1) };
                    if ready < 0 {
                        if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        return;
                    }
                    if watched[1].revents != 0 {
                        return;
                    }
                    if watched[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
                        continue;
                    }
                    let mut byte = 0_u8;
                    // SAFETY: one writable byte; MSG_PEEK leaves the stream untouched.
                    let peeked = unsafe {
                        libc::recv(
                            control,
                            (&raw mut byte).cast(),
                            1,
                            libc::MSG_PEEK | libc::MSG_DONTWAIT,
                        )
                    };
                    if peeked == 0 || watched[0].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
                        // SAFETY: signals this host's own process group, which it leads; the
                        // commands it forked lead groups of their own and are untouched.
                        unsafe { libc::kill(0, libc::SIGKILL) };
                    }
                    return;
                }
            })?;
        Ok(Self {
            cancel: Some(cancel),
            thread: Some(thread),
        })
    }
}

impl Drop for OrphanGuard {
    fn drop(&mut self) {
        drop(self.cancel.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Whether `direnv status --json` reports `envrc` itself as the found, allowed rc.
fn approved(status: &[u8], envrc: &Path) -> bool {
    #[derive(serde::Deserialize)]
    struct Status {
        state: State,
    }
    #[derive(serde::Deserialize)]
    struct State {
        #[serde(rename = "foundRC")]
        found_rc: Option<FoundRc>,
    }
    #[derive(serde::Deserialize)]
    struct FoundRc {
        /// direnv's `AllowStatus`: 0 allowed, 1 not allowed, 2 denied.
        allowed: i64,
        path: PathBuf,
    }
    serde_json::from_slice::<Status>(status).is_ok_and(|status| {
        status
            .state
            .found_rc
            .is_some_and(|found| found.allowed == 0 && found.path == envrc)
    })
}

fn utf8(bytes: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_exact_allowed_rc_counts_as_approved() {
        let status = br#"{"config":{},"state":{"foundRC":{"allowed":0,"path":"/w/.envrc"}}}"#;
        assert!(approved(status, Path::new("/w/.envrc")));
        assert!(!approved(status, Path::new("/other/.envrc")));
        let denied = br#"{"state":{"foundRC":{"allowed":1,"path":"/w/.envrc"}}}"#;
        assert!(!approved(denied, Path::new("/w/.envrc")));
        assert!(!approved(br#"{"state":{}}"#, Path::new("/w/.envrc")));
    }
}
