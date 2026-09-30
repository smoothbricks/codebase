//! Script jobs: a rendered program run by an upstream brush interpreter in a forked child.
//!
//! The host parses the text first, so a script that does not parse fails before anything runs.
//! It then forks without exec, the way bash runs a subshell. The child:
//!
//! - leads a new process group, the job's: brush runs with job control off, so every external
//!   command and every descendant joins that group, and the supervisor's TERM → grace → KILL
//!   reaches all of them while the host, in a group of its own, is untouched. The child alone
//!   creates the group, and the host gives the supervisor the pid only once the child reports
//!   that it exists;
//! - holds the job's stdin, stdout and stderr as its own 0, 1 and 2, and nothing else the host
//!   had open;
//! - builds a brush shell from the host's already-activated environment — no activation, no rc
//!   files — binds the script's values as shell variables, runs the program and exits with the
//!   program's status.
//!
//! The host reports the child's raw `waitpid` status: a killed script dies by the signal, a
//! script whose last command died by a signal exits `128+N` as bash's own scripts do. Anything
//! a script does to its process — `cd`, `umask`, `ulimit`, `trap`, `exec` — ends with the child.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;

use brush_builtins::ShellBuilderExt as _;
use cowshed_core::runtime::shell_host::{
    CONTROL_DESCRIPTOR, FrameWriter, REPLY_EXITED, REPLY_SCRIPT_SYNTAX, REPLY_STARTED,
};
use cowshed_core::script::Binding;

use crate::reply;

/// Parse `text`, then fork and run it as the job; replies as for an argv job, or with
/// [`REPLY_SCRIPT_SYNTAX`] when the text does not parse.
pub(crate) fn run(
    socket: &UnixStream,
    text: &str,
    bindings: &[Binding],
    cwd: &Path,
    environment: &BTreeMap<OsString, OsString>,
    [stdin, stdout, stderr]: [OwnedFd; 3],
) -> io::Result<()> {
    let program = match brush_parser::Parser::new(
        io::Cursor::new(text.as_bytes()),
        &brush_parser::ParserOptions::default(),
    )
    .parse_program()
    {
        Ok(program) => program,
        Err(error) => {
            let diagnostic = format!("cowshed: the script does not parse: {error}");
            let mut stderr = std::fs::File::from(stderr);
            let _ = writeln!(stderr, "{diagnostic}");
            return reply(
                socket,
                FrameWriter::new(REPLY_SCRIPT_SYNTAX).bytes(diagnostic.as_bytes())?,
            );
        }
    };
    // Everything the child needs is prepared before the fork: the child only moves descriptors
    // and runs the interpreter.
    let variables: Vec<(String, brush_core::ShellVariable)> = environment
        .iter()
        .map(|(name, value)| {
            let mut variable = brush_core::ShellVariable::new(brush_core::ShellValue::String(
                value.to_string_lossy().into_owned(),
            ));
            variable.export();
            (name.to_string_lossy().into_owned(), variable)
        })
        .chain(bindings.iter().map(|binding| {
            let value = match binding {
                Binding::Scalar { value, .. } => brush_core::ShellValue::String(value.clone()),
                Binding::Array { values, .. } => {
                    brush_core::ShellValue::indexed_array_from_strings(values.clone())
                }
            };
            (
                binding.name().to_owned(),
                brush_core::ShellVariable::new(value),
            )
        }))
        .collect();
    let job_descriptors = [stdin.as_raw_fd(), stdout.as_raw_fd(), stderr.as_raw_fd()];
    let (ready_read, ready_write) = close_on_exec_pipe()?;
    let ready = [ready_read.as_raw_fd(), ready_write.as_raw_fd()];

    // SAFETY: the host is single-threaded here (no activation guard thread exists outside an
    // activation), so the child starts with a consistent heap and no lock held by another
    // thread. The child never returns into the host's loop: it exits through `_exit`.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(io::Error::last_os_error());
    }
    if pid == 0 {
        child(job_descriptors, ready, cwd, variables, program);
    }
    drop((stdin, stdout, stderr, ready_write));
    // The supervisor signals the job's group the moment it learns the pid, so the group must
    // exist first. Only the child creates it: two creations of one group race, and macOS
    // refuses the loser with EPERM. The child reports once its group exists; an end of file
    // means it ended before it could.
    let mut ready = std::fs::File::from(ready_read);
    loop {
        match io::Read::read(&mut ready, &mut [0_u8]) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                // SAFETY: killing and reaping this host's own child, which nothing else waits
                // for.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::waitpid(pid, std::ptr::null_mut(), 0);
                }
                return Err(error);
            }
        }
    }
    drop(ready);
    let pid_u32 = u32::try_from(pid).map_err(io::Error::other)?;
    reply(socket, FrameWriter::new(REPLY_STARTED).u32(pid_u32))?;
    let mut status = 0;
    loop {
        // SAFETY: `pid` is this host's own child and `status` a valid out-pointer.
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        if waited == pid {
            break;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    reply(socket, FrameWriter::new(REPLY_EXITED).i32(status))
}

/// A pipe whose ends close in any program the job execs.
fn close_on_exec_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut ends = [0; 2];
    // SAFETY: room for the two descriptors pipe(2) writes; each is owned from here on.
    let (read, write) = unsafe {
        if libc::pipe(ends.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }
        (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1]))
    };
    for end in [&read, &write] {
        // SAFETY: plain fcntl on a descriptor this function owns.
        if unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((read, write))
}

/// Tell the job, on its stderr, why its script cannot run, and end the child with the status
/// a shell gives a command it cannot execute.
fn refuse(stderr: i32, what: &str, error: io::Error) -> ! {
    // SAFETY: `stderr` is the job's stderr, open in this child; `ManuallyDrop` leaves it open,
    // and `_exit` ends the child without the host's destructors.
    unsafe {
        let file = std::mem::ManuallyDrop::new(std::fs::File::from_raw_fd(stderr));
        let _ = writeln!(&*file, "cowshed: the script {what}: {error}");
        libc::_exit(126)
    }
}

/// The forked child: never returns.
fn child(
    [stdin, stdout, stderr]: [i32; 3],
    [ready_read, ready_write]: [i32; 2],
    cwd: &Path,
    variables: Vec<(String, brush_core::ShellVariable)>,
    program: brush_parser::ast::Program,
) -> ! {
    // SAFETY: plain descriptor, process-group and signal-disposition calls on this process's
    // own tables. The control socket belongs to the host; the job's descriptors become 0, 1
    // and 2, and the originals close, so the script holds exactly the job's streams. Each
    // `sigaction` query writes one zeroed `sigaction` it is handed.
    unsafe {
        libc::close(CONTROL_DESCRIPTOR);
        libc::close(ready_read);
        if libc::setpgid(0, 0) != 0 {
            refuse(
                stderr,
                "cannot lead a process group of its own",
                io::Error::last_os_error(),
            );
        }
        // The group exists: the host may give the supervisor this pid. A host that stopped
        // reading has nothing to learn, so a failed write changes nothing here.
        libc::write(ready_write, [1_u8].as_ptr().cast(), 1);
        libc::close(ready_write);
        for (target, source) in [(0, stdin), (1, stdout), (2, stderr)] {
            if libc::dup2(source, target) < 0 {
                refuse(
                    stderr,
                    "cannot take the job's streams",
                    io::Error::last_os_error(),
                );
            }
        }
        for source in [stdin, stdout, stderr] {
            if source > 2 {
                libc::close(source);
            }
        }
        // No exec clears the host's signal handlers here, so the child clears them as an exec
        // would: every handled signal goes back to its default. The Rust runtime's SIGSEGV and
        // SIGBUS stack-overflow handlers would otherwise swallow a SIGSEGV sent to the script
        // and let it carry on. SIGPIPE, which the runtime ignores on the program's behalf rather
        // than the user's, goes back to its default too.
        for signal in 1..=64 {
            let mut current = std::mem::MaybeUninit::<libc::sigaction>::zeroed();
            if libc::sigaction(signal, std::ptr::null(), current.as_mut_ptr()) == 0 {
                let handler = current.assume_init().sa_sigaction;
                if handler != libc::SIG_DFL && handler != libc::SIG_IGN {
                    libc::signal(signal, libc::SIG_DFL);
                }
            }
        }
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    let status = match interpret(cwd, variables, program) {
        Ok(status) => status,
        Err(error) => {
            let _ = writeln!(io::stderr(), "cowshed: the script could not run: {error}");
            126
        }
    };
    let _ = io::stdout().flush();
    let _ = io::stderr().flush();
    // SAFETY: `_exit` ends the child without running the host's destructors or atexit handlers,
    // which belong to the host's copy of this state.
    unsafe { libc::_exit(i32::from(status)) }
}

fn interpret(
    cwd: &Path,
    variables: Vec<(String, brush_core::ShellVariable)>,
    program: brush_parser::ast::Program,
) -> Result<u8, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut builder = brush_core::Shell::builder()
            .default_builtins(brush_builtins::BuiltinSet::BashMode)
            .do_not_inherit_env(true)
            .profile(brush_core::ProfileLoadBehavior::Skip)
            .rc(brush_core::RcLoadBehavior::Skip)
            .working_dir(cwd.to_path_buf())
            .shell_name("cowshed".to_owned());
        for (name, variable) in variables {
            builder = builder.var(name, variable);
        }
        let mut shell = builder.build().await?;
        let parameters = shell.default_exec_params();
        let result = shell.run_program(program, &parameters).await?;
        Ok(u8::from(result.exit_code))
    })
}
