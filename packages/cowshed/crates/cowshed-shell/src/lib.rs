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
//! with `SCM_RIGHTS`, starts the job in its own process group, reports the exit `waitid` reads
//! without reaping, and holds the leader unreaped until the job is released, so the job's group
//! stays signallable while descendants outlive the leader. It runs under the executed-child
//! Seatbelt profile like every command, so it adds no authority, and activation still runs
//! repository code only inside that profile.
//!
//! Activation is direnv's own: `direnv export json` evaluated from the host's sandbox
//! environment, its diff applied once. The resulting `DIRENV_WATCHES` is reported to the
//! supervisor, which decides freshness from it; the host never re-evaluates.
//!
//! An argv job is forked and exec'd directly. A script job is forked without exec: the child
//! leads the job's process group and runs the parsed program in an upstream brush interpreter
//! built from the already-activated environment ([`script`]).
//!
//! Linux commands receive a mandatory controller-opened cgroup placement descriptor with their
//! streams. Only the child enters it, before argv exec or script interpretation; the descriptor
//! closes before job code runs. The reusable host stays outside every job cgroup.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::OpenOptionsExt as _;
use std::os::unix::net::UnixStream;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use cowshed_core::api::dto::ExitStatus;
use cowshed_core::developer_links;
#[cfg(target_os = "linux")]
use cowshed_core::runtime::job_cgroup::Placement;
use cowshed_core::runtime::job_groups::{self, Birth};
#[cfg(target_os = "macos")]
use cowshed_core::runtime::process_monitor::ProcessMonitor;
#[cfg(target_os = "macos")]
use cowshed_core::runtime::process_tree::ProcessIdentity;
use cowshed_core::runtime::process_tree::TreeOwner;
use cowshed_core::runtime::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, CONTROL_DESCRIPTOR, FrameReader, FrameWriter, Inherited,
    REPLY_ACTIVATED, REPLY_ACTIVATION_EXITED, REPLY_ACTIVATION_UNUSABLE, REPLY_ADOPTION_REFUSED,
    REPLY_APPROVED, REPLY_EXITED, REPLY_HOST_FAILED, REPLY_RELEASED, REPLY_SIGNAL_FAILED,
    REPLY_STARTED, REQUEST_ACTIVATE, REQUEST_ADOPT, REQUEST_APPROVE, REQUEST_RELEASE, REQUEST_RUN,
    REQUEST_SCRIPT, REQUEST_SIGNAL, RawWaitStatus, SHELL_HOST_ARGUMENT, ShellHostProgram,
    read_frame,
};
use cowshed_core::script::Binding;
use cowshed_os::deadline::{self, Progress};

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
    match cowshed_core::developer_links::dispatch() {
        Ok(Some(status)) => {
            if let Some(signal) = status.signal() {
                // SAFETY: default signal disposition followed by delivery terminates this wrapper
                // with the package manager's actual signal, rather than claiming a normal exit.
                unsafe {
                    libc::signal(signal, libc::SIG_DFL);
                    libc::raise(signal);
                }
                std::process::exit(128 + signal);
            }
            std::process::exit(status.code().expect("the manager exited or was signaled"));
        }
        Ok(None) => {}
        Err(error) => {
            cowshed_os::eprintln!("cowshed: managed package manager failed: {error}");
            std::process::exit(126);
        }
    }
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

/// The exit a shell reports for a command it could not execute: 127 when the program does not
/// exist, 126 when it exists but cannot run.
fn unexecutable_exit(error: &io::Error) -> ExitStatus {
    ExitStatus::Exited {
        code: if error.kind() == io::ErrorKind::NotFound {
            127
        } else {
            126
        },
    }
}

/// Run the exec host on [`CONTROL_DESCRIPTOR`] until the supervisor closes it. Returns the
/// process exit code (71 when it cannot put SIGBUS back to its default disposition and so
/// refuses to serve); a page of its own program it cannot be given ends it by SIGBUS instead.
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
    // The host runs its program from the workspace mount (11_shell.md, "Exec hosts"), so once
    // that mount is gone -- a forced unmount, DiskArbitration restarting -- the next page-in of
    // code not yet resident fails with SIGBUS. The Rust runtime's stack-overflow handler catches
    // that SIGBUS, and its own code lies on the same unreadable pages: measured 2026-10-22, a
    // host woken by its controller's end of stream after its mount was forced off spun in R state,
    // nearly all of it system time, until killed. At the default disposition the fault ends it,
    // and its supervisor names that end (`DroppedHostEnd::PageUnavailable`).
    // SAFETY: setting a signal's disposition touches no memory; the runtime's handler only
    // prints a stack-overflow message before the same signal ends the process.
    if unsafe { libc::signal(libc::SIGBUS, libc::SIG_DFL) } == libc::SIG_ERR {
        cowshed_os::eprintln!(
            "cowshed: exec host refuses to serve: cannot restore SIGBUS's default disposition: {}",
            io::Error::last_os_error()
        );
        return 71;
    }
    let mut host = Host {
        environment: std::env::vars_os().collect(),
    };
    loop {
        match read_frame(&socket) {
            Ok(Some((payload, descriptors))) => {
                if let Err(error) = host.handle(&socket, &payload, descriptors) {
                    // The controller may already have closed the socket. Report the preserved
                    // native cause before trying that channel, never relying on a later Drop.
                    let report = match writeln!(
                        io::stderr(),
                        "cowshed: exec host request failed: {error}"
                    ) {
                        Ok(()) => error.to_string(),
                        Err(cause) => format!("{error}; stderr diagnostic unavailable: {cause}"),
                    };
                    if let Err(reply_error) = FrameWriter::new(REPLY_HOST_FAILED)
                        .bytes(report.as_bytes())
                        .and_then(|frame| reply(&socket, frame))
                    {
                        match writeln!(
                            io::stderr(),
                            "cowshed: {report}; failed host reply: {reply_error}"
                        ) {
                            Ok(()) => {}
                            Err(stderr_error) => match writeln!(
                                io::stdout(),
                                "cowshed: {report}; failed host reply: {reply_error}; stderr diagnostic unavailable: {stderr_error}"
                            ) {
                                Ok(()) => {}
                                Err(_stdout_error) => {
                                    // The controller and both standard streams are unavailable.
                                    // No delivery channel remains; the host still returns failure.
                                }
                            },
                        }
                    }
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

/// Lay a direnv export over an environment: a value sets its variable, `None` removes it.
fn apply_to(
    environment: &mut BTreeMap<OsString, OsString>,
    diff: &BTreeMap<String, Option<String>>,
) {
    for (name, value) in diff {
        match value {
            Some(value) => {
                environment.insert(name.into(), value.into());
            }
            None => {
                environment.remove(OsStr::new(name));
            }
        }
    }
}

/// The first executable `name` on `path`, as the kernel's `execvp` would find it.
fn resolve_program(name: &str, path: Option<&OsString>) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt as _;
    std::env::split_paths(path?)
        .map(|directory| directory.join(name))
        .find(|candidate| {
            std::fs::metadata(candidate).is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
}

/// The command's platform placement, passed with its streams. Linux requires the
/// controller-opened cgroup descriptor; macOS requires the observation fence. A script child
/// closes these explicitly because it does not exec.
pub(crate) struct CommandPlacement {
    #[cfg(target_os = "linux")]
    cgroup: Placement,
    #[cfg(target_os = "macos")]
    hold: ObservationHold,
}

impl CommandPlacement {
    fn take(descriptors: &mut Vec<OwnedFd>) -> io::Result<Self> {
        Ok(Self {
            #[cfg(target_os = "linux")]
            cgroup: Placement::from_descriptor(one_descriptor(descriptors, "cgroup placement")?),
            #[cfg(target_os = "macos")]
            hold: ObservationHold::take(descriptors)?,
        })
    }
}

#[cfg(target_os = "macos")]
pub(crate) struct ObservationHold {
    ready: OwnedFd,
    go: OwnedFd,
}

#[cfg(target_os = "macos")]
impl ObservationHold {
    fn take(descriptors: &mut Vec<OwnedFd>) -> io::Result<Self> {
        Ok(Self {
            ready: one_descriptor(descriptors, "observation ready")?,
            go: one_descriptor(descriptors, "observation release")?,
        })
    }

    pub(crate) fn raw(&self) -> [i32; 2] {
        [self.ready.as_raw_fd(), self.go.as_raw_fd()]
    }
}

fn reply(socket: &UnixStream, frame: FrameWriter) -> io::Result<()> {
    let mut socket = socket;
    socket.write_all(&frame.finish()?)
}

/// Opens the job's jobserver fifo the command's environment names, once, as Cargo will: read
/// and write, without blocking. This host is the first process inside the sandbox, so a fifo it
/// cannot open is one the command's Cargo cannot either, and that Cargo runs without the job's
/// jobserver (05_gateway.md "Cargo reads its holder's jobserver"). That is said on the job's
/// stderr before the command runs, and the command still runs.
fn report_unopenable_jobserver(
    environment: &BTreeMap<OsString, OsString>,
    stderr: &OwnedFd,
) -> io::Result<()> {
    let Some(fifo) = cowshed_core::runtime::job_cpu::granted_jobserver_fifo(environment) else {
        return Ok(());
    };
    let opened = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo);
    if let Err(error) = opened {
        writeln!(
            std::fs::File::from(stderr.try_clone()?),
            "cowshed: the job's CPU jobserver fifo {} cannot be opened in the sandbox ({error}); \
             Cargo will run without the job's jobserver",
            fifo.display()
        )?;
    }
    Ok(())
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
                let owner = fields.tree_owner()?;
                let directory = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                fields.finish()?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                self.activate(socket, &directory, owner, stderr)
            }
            REQUEST_ADOPT => {
                let owner = fields.tree_owner()?;
                let directory = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                let inherited: Inherited = serde_json::from_slice(fields.bytes()?)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                fields.finish()?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                self.adopt(socket, &directory, owner, inherited, stderr)
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
                let placement = CommandPlacement::take(&mut descriptors)?;
                let environment = self.command_environment(&cwd, &overlay)?;
                report_unopenable_jobserver(&environment, &stderr)?;
                script::run(
                    socket,
                    utf8(text)?,
                    &bindings,
                    &cwd,
                    &environment,
                    placement,
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
                let placement = CommandPlacement::take(&mut descriptors)?;
                self.run(
                    socket,
                    &argv,
                    &cwd,
                    &overlay,
                    placement,
                    [stdin, stdout, stderr],
                )
            }
            // Only a held command is signalled or released, inside its own exchange.
            REQUEST_SIGNAL | REQUEST_RELEASE => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("control request {tag} while no command is held"),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown control request {other}"),
            )),
        }
    }

    /// Why `direnv` could not be started, naming the PATH it was looked up on.
    fn tool_error(&self, error: io::Error) -> io::Error {
        let path = self
            .environment
            .get(OsStr::new("PATH"))
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        io::Error::new(
            error.kind(),
            format!("cannot run direnv from PATH {path}: {error}"),
        )
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
    // WHY unbounded: direnv's own bookkeeping (`status` reads its trust store, `allow` writes it),
    // which waits on no other process; the orphan guard retires this host once the supervisor that
    // asked is gone (11_shell.md).
    #[allow(clippy::disallowed_methods)]
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
            .status()
            .map_err(|error| self.tool_error(error))?;
        Ok(status.into_raw())
    }

    /// Evaluate the `.envrc` once and apply direnv's exported diff to this host's environment.
    fn activate(
        &mut self,
        socket: &UnixStream,
        directory: &Path,
        owner: TreeOwner,
        stderr: OwnedFd,
    ) -> io::Result<()> {
        // An activation can run for minutes (a Nix evaluation, an install). If the supervisor
        // that asked for it goes away — its process exited, or it dropped this host — nothing
        // will ever use the result, so the evaluation must not keep running unobserved.
        let diagnostics = stderr.try_clone()?;
        let mut command = self.tool(directory);
        command
            .args(["export", "json"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr));
        let (mut child, _orphan_guard) = OrphanGuard::spawn(socket, owner, command, diagnostics)
            .map_err(|error| self.tool_error(error))?;
        let mut exported = Vec::new();
        let read = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("direnv export has no stdout pipe"))?
            .take(u64::try_from(MAX_EXPORT_BYTES + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut exported);
        // WHY unbounded: its export pipe is closed (read to its end, or dropped at the byte cap),
        // so direnv is ending; a stalled one is retired by the orphan guard once the supervisor
        // that asked is gone (11_shell.md).
        #[allow(clippy::disallowed_methods)]
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
        self.apply(&diff);
        self.reply_activated(socket, &exported)
    }

    /// Lay an export over this host's environment, then probe the developer-link wrappers once
    /// for the shell: one version probe per activation answers every command of this warm
    /// shell; the wrapper asks again only for another program file, and itself reports any
    /// failure dropped here (15_capabilities.md, "Bootstrap executables").
    fn apply(&mut self, diff: &BTreeMap<String, Option<String>>) {
        apply_to(&mut self.environment, diff);
        let report = developer_links::report(
            self.environment
                .get(OsStr::new("PATH"))
                .map(OsString::as_os_str),
        )
        .ok()
        .flatten();
        match report {
            Some(report) => {
                self.environment
                    .insert(developer_links::REPORTED_ENV.into(), report);
            }
            None => {
                self.environment
                    .remove(OsStr::new(developer_links::REPORTED_ENV));
            }
        }
    }

    /// Report the activated shell: its watch list, and the export it applied (empty for an
    /// adopted one, whose export the supervisor already holds), so the supervisor can leave it
    /// for the checkouts cloned from this one.
    fn reply_activated(&self, socket: &UnixStream, export: &[u8]) -> io::Result<()> {
        let watches = self
            .environment
            .get(OsStr::new("DIRENV_WATCHES"))
            .map(|value| value.as_bytes().to_vec());
        let frame = FrameWriter::new(REPLY_ACTIVATED).u32(u32::from(watches.is_some()));
        let frame = match &watches {
            Some(watches) => frame.bytes(watches)?,
            None => frame,
        };
        reply(socket, frame.bytes(export)?)
    }

    /// Take an inherited export the supervisor verified for this checkout instead of evaluating
    /// (11_shell.md, "Shell activation and process reuse"). The values the record withholds —
    /// credentials and what cowshed publishes per workspace — are read from this checkout's own
    /// tail files through direnv's stdlib and `direnv dump`, exactly as an activation reads
    /// them. A tail that does not supply one of them refuses the adoption with the host's
    /// environment untouched, and the supervisor evaluates instead.
    fn adopt(
        &mut self,
        socket: &UnixStream,
        directory: &Path,
        owner: TreeOwner,
        inherited: Inherited,
        stderr: OwnedFd,
    ) -> io::Result<()> {
        let refuse = |reason: String| {
            reply(
                socket,
                FrameWriter::new(REPLY_ADOPTION_REFUSED).bytes(reason.as_bytes())?,
            )
        };
        let (environment, from_tail, tail) = inherited.into_parts();
        let baseline = self.environment.clone();
        let mut staged = baseline.clone();
        apply_to(&mut staged, &environment);
        // The withheld names start unset, so one present in the dump is one the tail wrote: an
        // ambient value of the host's own never stands in for what the activation exported.
        for name in &from_tail {
            staged.remove(OsStr::new(name));
        }
        let dump = match self.source_tail(socket, directory, owner, &staged, &tail, stderr) {
            Ok(Ok(dump)) => dump,
            Ok(Err(reason)) => return refuse(reason),
            Err(error) => return refuse(error.to_string()),
        };
        let mut diff = environment;
        for name in from_tail {
            match dump.get(&name) {
                Some(value) => {
                    diff.insert(name, Some(value.clone()));
                }
                None => {
                    return refuse(format!(
                        "the inherited shell had {name}, and this checkout's own files do not set it"
                    ));
                }
            }
        }
        apply_to(&mut staged, &diff);
        diff.insert(
            "DIRENV_DIFF".to_owned(),
            Some(cowshed_core::runtime::shell_watch::encode_direnv_diff(
                &baseline, &staged,
            )),
        );
        self.apply(&diff);
        self.reply_activated(socket, b"")
    }

    /// Source `tail` over `environment` the way direnv sources a file from an `.envrc`: its own
    /// bash, its stdlib, `source_env_if_exists`, and `direnv dump json` for the result.
    fn source_tail(
        &self,
        socket: &UnixStream,
        directory: &Path,
        owner: TreeOwner,
        environment: &BTreeMap<OsString, OsString>,
        tail: &[PathBuf],
        stderr: OwnedFd,
    ) -> io::Result<Result<BTreeMap<String, String>, String>> {
        let direnv = match resolve_program("direnv", self.environment.get(OsStr::new("PATH"))) {
            Some(direnv) => direnv,
            None => {
                return Ok(Err(self
                    .tool_error(io::ErrorKind::NotFound.into())
                    .to_string()));
            }
        };
        // WHY unbounded: `direnv status` reads direnv's own trust store and waits on no other
        // process; the orphan guard retires this host once the supervisor that asked is gone.
        #[allow(clippy::disallowed_methods)]
        let status = self
            .tool(directory)
            .arg("status")
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .map_err(|error| self.tool_error(error))?;
        let Some(bash) = String::from_utf8_lossy(&status.stdout)
            .lines()
            .find_map(|line| line.strip_prefix("bash_path ").map(PathBuf::from))
        else {
            return Ok(Err("direnv status names no bash_path".into()));
        };
        let diagnostics = stderr.try_clone()?;
        let mut command = Command::new(bash);
        command
            .env_clear()
            .envs(environment)
            .env("PWD", directory)
            .current_dir(directory)
            .arg("-c")
            .arg(r#"eval "$("$0" stdlib)" || exit; for rc; do source_env_if_exists "$rc" || exit; done; "$0" dump json"#)
            .arg(&direnv)
            .args(tail)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr));
        let (child, _orphan_guard) = OrphanGuard::spawn(socket, owner, command, diagnostics)?;
        // WHY unbounded: an activation runs as long as its evaluation; the orphan guard retires it
        // once the supervisor that asked is gone (11_shell.md), which ends this wait.
        #[allow(clippy::disallowed_methods)]
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Ok(Err(format!(
                "sourcing this checkout's own shell files failed: {}",
                output.status
            )));
        }
        Ok(serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("direnv dump json is not an environment: {error}")))
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
        // Only the package-manager wrapper route leads the activated PATH; every other entry,
        // the shell's declared toolchain included, reaches the command as activation left it.
        if let Some(wrappers) = self
            .environment
            .get(OsStr::new(cowshed_core::developer_links::MANAGER_BIN_ENV))
        {
            let mut paths = vec![PathBuf::from(wrappers)];
            if let Some(path) = environment.get(OsStr::new("PATH")) {
                paths.extend(
                    std::env::split_paths(path).filter(|entry| entry.as_os_str() != wrappers),
                );
            }
            environment.insert(
                "PATH".into(),
                std::env::join_paths(paths).map_err(io::Error::other)?,
            );
        }
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
        placement: CommandPlacement,
        [stdin, stdout, stderr]: [OwnedFd; 3],
    ) -> io::Result<()> {
        let CommandPlacement {
            #[cfg(target_os = "linux")]
            cgroup,
            #[cfg(target_os = "macos")]
            hold,
        } = placement;
        let Some((program, arguments)) = argv.split_first() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty argv"));
        };
        let environment = self.command_environment(cwd, overlay)?;
        report_unopenable_jobserver(&environment, &stderr)?;
        let diagnostics = stderr.try_clone()?;
        let mut command = Command::new(OsStr::from_bytes(program));
        command
            .args(arguments.iter().map(|argument| OsStr::from_bytes(argument)))
            .env_clear()
            .envs(&environment)
            .current_dir(cwd)
            .process_group(0)
            .stdin(Stdio::from(stdin))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        #[cfg(target_os = "linux")]
        cgroup.on_exec(&mut command);
        #[cfg(target_os = "macos")]
        {
            let [ready, go] = hold.raw();
            // SAFETY: only async-signal-safe getpid/write/read run between fork and exec.
            unsafe {
                command.pre_exec(move || {
                    cowshed_core::runtime::process_fence::hold_before_exec(ready, go)
                });
            }
        }
        let spawned = command.spawn();
        // spawn borrows the builder's stdio and dup2s the raw numbers. The
        // owned ends stay in `command` until it drops. Release waits for those
        // ends, so the builder closes them before that wait. The child handle
        // does not own them and stays unreaped.
        drop(command);
        #[cfg(target_os = "macos")]
        drop(hold);
        let exit = match spawned {
            Ok(child) => {
                drop(diagnostics);
                let held = Held::new(child.id().cast_signed());
                let birth = Birth::of(child.id());
                reply(socket, FrameWriter::new(REPLY_STARTED).birth(&birth)?)?;
                return serve_command(socket, held);
            }
            Err(error) => {
                let mut diagnostics = std::fs::File::from(diagnostics);
                let _ = writeln!(
                    diagnostics,
                    "cowshed: cannot run {}: {error}",
                    String::from_utf8_lossy(program)
                );
                unexecutable_exit(&error)
            }
        };
        reply(socket, FrameWriter::new(REPLY_EXITED).exit(&exit))
    }
}

/// A command this host forked and holds unreaped. Until it is reaped its pid -- the id of the
/// group it leads -- names nothing else, so the group can be signalled, descendants that outlive
/// the leader included. Signals and the reap happen in this one thread, so no signal follows the
/// reap.
///
/// One dropped unreleased -- its supervisor went away, or serving it failed -- leaves nothing
/// that could observe or cancel it, so it ends here as a dropped supervisor's own jobs do: its
/// group is asked first, killed after the grace, and its leader reaped.
pub(crate) struct Held {
    pid: libc::pid_t,
    reaped: bool,
}

impl Held {
    /// Hold `pid`, this host's own child, which leads its own group and is not reaped.
    pub(crate) fn new(pid: libc::pid_t) -> Self {
        Self { pid, reaped: false }
    }

    fn signal(&self, signal: libc::c_int) -> io::Result<()> {
        job_groups::signal_unreaped_group(self.pid, signal)
    }

    /// Retire the actual parent's unreaped group as a group whose controller is gone is ended
    /// ([`job_groups::end_held_group`]), then reap the leader, retaining every failed native
    /// operation.
    fn retire(&mut self) -> io::Result<()> {
        if self.reaped {
            return Ok(());
        }
        let ending = job_groups::end_held_group(self.pid, |signal| self.signal(signal));
        let reap = self.reap(Progress::Signalled {
            signal: libc::SIGKILL,
        });
        match (ending.is_clean(), &reap) {
            (true, Ok(())) => Ok(()),
            _ => Err(io::Error::other(format!(
                "retiring unreaped command group {}: {ending}; reap={reap:?}",
                self.pid,
            ))),
        }
    }

    /// Collect the leader, exited or killed (`last`), within the reap bound. A leader collected,
    /// or handed to a collector at the bound, is no longer this host's to signal: its id may name
    /// another process from here on.
    fn reap(&mut self, last: Progress) -> io::Result<()> {
        let reaped = reap_child("command leader", self.pid, last);
        self.reaped = !matches!(reaped, Err(deadline::ReapError::Wait { .. }));
        reaped.map_err(io::Error::from)
    }
}

/// Collect this host's own child `pid`, `role` to it, exited or killed, within the reap bound
/// (specs/cowshed/05_gateway.md, "Deadlines"), `last` being what this host last did to it. One still
/// running or still in its exit at the bound (an uninterruptible kernel call) is named and handed
/// to a collector thread (`deadline::collect_pid_later`), never waited for here.
pub(crate) fn reap_child(
    role: &'static str,
    pid: libc::pid_t,
    last: Progress,
) -> Result<(), deadline::ReapError> {
    let id = u32::try_from(pid).map_err(|error| deadline::ReapError::Wait {
        op: "reap child",
        role,
        pid: 0,
        elapsed: std::time::Duration::ZERO,
        source: io::Error::new(io::ErrorKind::InvalidInput, error),
    })?;
    let budget = deadline::start("reap child", deadline::REAP);
    let reaped = deadline::await_exit(role, id, &budget, last).and_then(|()| {
        deadline::collect_exited(role, id, &budget, last, || {
            // WHY unbounded: `WNOHANG` returns at once.
            #[allow(clippy::disallowed_methods)]
            // SAFETY: `pid` is this host's own child; a null status pointer is allowed.
            let waited = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
            match waited {
                0 => Ok(None),
                waited if waited == pid => Ok(Some(())),
                _ => Err(io::Error::last_os_error()),
            }
        })
    });
    if matches!(reaped, Err(deadline::ReapError::Expired(_))) {
        deadline::collect_pid_later(role, pid);
    }
    reaped
}

impl Drop for Held {
    fn drop(&mut self) {
        if let Err(cause) = self.retire() {
            match writeln!(
                io::stderr(),
                "cowshed: owned command retirement failed: {cause}"
            ) {
                Ok(()) => {}
                Err(stderr_error) => match writeln!(
                    io::stdout(),
                    "cowshed: owned command retirement failed: {cause}; stderr diagnostic unavailable: {stderr_error}"
                ) {
                    Ok(()) => {}
                    Err(_stdout_error) => {
                        // Both existing channels are unavailable. Drop cannot return their
                        // errors; the native retirement steps have already been attempted.
                    }
                },
            }
        }
    }
}

/// Serve the held command's exchange until the supervisor releases it ([`REQUEST_RELEASE`]):
/// its signals while it runs, its exit once its leader exits, and its signals while descendants
/// run on. A supervisor that goes away, and any failure here, ends the command ([`Held`]).
pub(crate) fn serve_command(socket: &UnixStream, mut held: Held) -> io::Result<()> {
    let watch = ExitWatch::new(held.pid, socket)?;
    loop {
        match watch.next()? {
            Ready::Exited => break,
            Ready::Request => match read_frame(socket)? {
                // A release before the exit is refused, so this request leaves the command held.
                Some((payload, _)) => {
                    serve_request(socket, &mut held, &payload, false)?;
                }
                None => return held.retire(),
            },
        }
    }
    drop(watch);
    let exit = job_groups::await_exit_unreaped(held.pid)?;
    reply(socket, FrameWriter::new(REPLY_EXITED).exit(&exit))?;
    loop {
        match read_frame(socket)? {
            Some((payload, _)) => {
                if serve_request(socket, &mut held, &payload, true)? {
                    return Ok(());
                }
            }
            None => return held.retire(),
        }
    }
}

/// Serve one request about the held command; `true` once it is released.
fn serve_request(
    socket: &UnixStream,
    held: &mut Held,
    payload: &[u8],
    exited: bool,
) -> io::Result<bool> {
    let (tag, mut fields) = FrameReader::new(payload)?;
    match tag {
        REQUEST_SIGNAL => {
            let signal = fields.i32()?;
            fields.finish()?;
            if let Err(error) = held.signal(signal) {
                reply(
                    socket,
                    FrameWriter::new(REPLY_SIGNAL_FAILED)
                        .i32(signal)
                        .bytes(error.to_string().as_bytes())?,
                )?;
            }
            Ok(false)
        }
        REQUEST_RELEASE if exited => {
            fields.finish()?;
            held.reap(Progress::State("exited"))?;
            reply(socket, FrameWriter::new(REPLY_RELEASED))?;
            Ok(true)
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("control request {other} while a command is held"),
        )),
    }
}

enum Ready {
    /// The child exited; it is not reaped yet.
    Exited,
    /// A request, or the end of the stream, is readable on the control socket.
    Request,
}

/// The child's exit and the control socket's requests, waited for together without reaping.
#[cfg(target_os = "macos")]
struct ExitWatch {
    queue: OwnedFd,
    /// The child had already exited when its exit was to be watched; it is not reaped yet.
    exited: bool,
}

#[cfg(target_os = "macos")]
impl ExitWatch {
    fn new(pid: libc::pid_t, socket: &UnixStream) -> io::Result<Self> {
        // SAFETY: kqueue takes no arguments; the descriptor is owned from here on.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let queue = unsafe { OwnedFd::from_raw_fd(queue) };
        let mut watch = Self {
            queue,
            exited: false,
        };
        let requests = libc::kevent {
            ident: usize::try_from(socket.as_raw_fd()).map_err(io::Error::other)?,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        watch.change(&[requests])?;
        let exit = libc::kevent {
            ident: usize::try_from(pid).map_err(io::Error::other)?,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // Darwin refuses to watch a child that already exited (measured: ESRCH on registering
        // EVFILT_PROC for an unreaped zombie); it is the child's exit, not a failure.
        match watch.change(&[exit]) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => watch.exited = true,
            Err(error) => return Err(error),
        }
        Ok(watch)
    }

    fn change(&self, changes: &[libc::kevent]) -> io::Result<()> {
        let count = libc::c_int::try_from(changes.len()).map_err(io::Error::other)?;
        // SAFETY: `changes` is a live slice of `count` events; no events are returned.
        let registered = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                changes.as_ptr(),
                count,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if registered < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn next(&self) -> io::Result<Ready> {
        if self.exited {
            return Ok(Ready::Exited);
        }
        loop {
            // SAFETY: `kevent` is plain storage the call fills.
            let mut event: libc::kevent = unsafe { std::mem::zeroed() };
            // SAFETY: room for exactly one returned event; no changes are passed; no timeout.
            let ready = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &mut event,
                    1,
                    std::ptr::null(),
                )
            };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if ready == 0 {
                continue;
            }
            return Ok(if event.filter == libc::EVFILT_PROC {
                Ready::Exited
            } else {
                Ready::Request
            });
        }
    }
}

/// The child's exit and the control socket's requests, waited for together without reaping.
#[cfg(target_os = "linux")]
struct ExitWatch {
    process: OwnedFd,
    socket: libc::c_int,
}

#[cfg(target_os = "linux")]
impl ExitWatch {
    fn new(pid: libc::pid_t, socket: &UnixStream) -> io::Result<Self> {
        // SAFETY: pidfd_open takes plain integers; the descriptor is owned from here on.
        let process = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if process < 0 {
            return Err(io::Error::last_os_error());
        }
        let process = libc::c_int::try_from(process).map_err(io::Error::other)?;
        Ok(Self {
            // SAFETY: a fresh descriptor this function alone owns.
            process: unsafe { OwnedFd::from_raw_fd(process) },
            socket: socket.as_raw_fd(),
        })
    }

    fn next(&self) -> io::Result<Ready> {
        loop {
            let mut descriptors = [
                libc::pollfd {
                    fd: self.process.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.socket,
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // SAFETY: `descriptors` is a live array of two pollfd entries.
            let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if descriptors[0].revents != 0 {
                return Ok(Ready::Exited);
            }
            if descriptors[1].revents != 0 {
                return Ok(Ready::Request);
            }
        }
    }
}

/// On control EOF, retires the activation's observed subtree before ending this host's group.
/// Darwin retains escaped and reparented descendants; Linux still has only group authority.
///
/// A thread polls the socket and a cancel pipe. The supervisor never writes during an
/// activation, so readable data is not expected; end of stream is recognized by a zero-byte
/// `MSG_PEEK`, which consumes nothing. Dropping the guard cancels the watch.
struct OrphanGuard {
    cancel: Option<OwnedFd>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl OrphanGuard {
    fn spawn(
        socket: &UnixStream,
        owner: TreeOwner,
        command: Command,
        diagnostics: OwnedFd,
    ) -> io::Result<(std::process::Child, Self)> {
        #[cfg(target_os = "macos")]
        let monitor = ProcessMonitor::prepare(owner).map_err(io::Error::other)?;
        #[cfg(target_os = "macos")]
        let (mut child, root) = monitor.spawn_root(command)?;
        // Linux retains only the host's process-group authority for activation.
        #[cfg(target_os = "linux")]
        let mut child = {
            let _ = owner;
            let mut command = command;
            command.spawn()?
        };
        let guard = match Self::watch(
            socket,
            diagnostics,
            #[cfg(target_os = "macos")]
            monitor.clone(),
            #[cfg(target_os = "macos")]
            root,
        ) {
            Ok(guard) => guard,
            Err(error) => {
                #[cfg(target_os = "macos")]
                let retired = monitor.retire_root(root, std::time::Duration::ZERO);
                #[cfg(target_os = "linux")]
                let retired = child.kill();
                let waited = deadline::reap(
                    "activation",
                    &mut child,
                    &deadline::start("reap activation", deadline::REAP),
                    Progress::Signalled {
                        signal: libc::SIGKILL,
                    },
                );
                if waited.is_err() {
                    deadline::collect_later("activation", child);
                }
                return Err(io::Error::other(format!(
                    "cannot watch activation control: {error}; retirement: {retired:?}; reap: {waited:?}"
                )));
            }
        };
        Ok((child, guard))
    }

    fn watch(
        socket: &UnixStream,
        diagnostics: OwnedFd,
        #[cfg(target_os = "macos")] monitor: ProcessMonitor,
        #[cfg(target_os = "macos")] root: ProcessIdentity,
    ) -> io::Result<Self> {
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
                        #[cfg(target_os = "macos")]
                        if let Err(error) = monitor.retire_root(root, std::time::Duration::ZERO)
                            && let Err(write) = writeln!(
                                std::fs::File::from(diagnostics),
                                "cowshed: activation subtree retirement failed: {error}"
                            )
                        {
                            cowshed_os::eprintln!(
                                "cowshed: cannot report activation retirement: {write}; {error}"
                            );
                        }
                        #[cfg(target_os = "linux")]
                        drop(diagnostics);
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
    use std::fs;

    #[test]
    fn only_the_exact_allowed_rc_counts_as_approved() {
        let status = br#"{"config":{},"state":{"foundRC":{"allowed":0,"path":"/w/.envrc"}}}"#;
        assert!(approved(status, Path::new("/w/.envrc")));
        assert!(!approved(status, Path::new("/other/.envrc")));
        let denied = br#"{"state":{"foundRC":{"allowed":1,"path":"/w/.envrc"}}}"#;
        assert!(!approved(denied, Path::new("/w/.envrc")));
        assert!(!approved(br#"{"state":{}}"#, Path::new("/w/.envrc")));
    }

    /// The activated shell's PATH reaches the command unchanged (04_sandbox.md, direnv): only
    /// the package-manager wrapper route leads it, so the toolchain the repository's shell
    /// declares wins over the bootstrap link of the same name.
    #[test]
    fn only_the_wrapper_route_leads_the_activated_path() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = std::env::temp_dir().join(format!("cowshed-shell-path-{}", std::process::id()));
        let devenv = root.join("devenv/bin");
        let tools = root.join("tools/bin");
        let wrappers = root.join("tools/manager-bin");
        for (directory, program) in [(&devenv, "cargo"), (&tools, "cargo"), (&wrappers, "bun")] {
            fs::create_dir_all(directory).unwrap();
            let file = directory.join(program);
            fs::write(&file, "#!/bin/sh\n").unwrap();
            fs::set_permissions(&file, fs::Permissions::from_mode(0o755)).unwrap();
        }
        // direnv puts the shell's additions ahead of the inherited bootstrap PATH.
        let activated =
            std::env::join_paths([&devenv, &wrappers, &tools, &PathBuf::from("/usr/bin")]).unwrap();
        let host = Host {
            environment: BTreeMap::from([
                ("PATH".into(), activated),
                (
                    cowshed_core::developer_links::MANAGER_BIN_ENV.into(),
                    wrappers.clone().into_os_string(),
                ),
            ]),
        };
        let environment = host.command_environment(&root, &[]).unwrap();
        let path: Vec<PathBuf> = std::env::split_paths(&environment[OsStr::new("PATH")]).collect();
        let found = |program: &str| {
            path.iter()
                .map(|directory| directory.join(program))
                .find(|file| file.is_file())
        };
        let resolved = (found("cargo"), found("bun"));
        fs::remove_dir_all(&root).unwrap();
        assert_eq!(
            resolved,
            (Some(devenv.join("cargo")), Some(wrappers.join("bun"))),
            "{path:?}"
        );
    }
}
