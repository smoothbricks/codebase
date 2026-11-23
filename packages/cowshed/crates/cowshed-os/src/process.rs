use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::process::{ExitStatus, Output};
use std::time::Duration;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

// What Linux alone has, which only Linux's own callers name: its cgroup v2 readings.
#[cfg(target_os = "linux")]
pub use linux::{
    cgroup_cpu, cgroup_members, group_cpu, group_members, group_pids, own_cgroup, signal_cgroup,
};

/// A prepared replacement for a controller's standard streams after its command inherits them.
/// Preparation happens before spawn. Release happens before acknowledging the handoff on a
/// separate diagnostic channel: from that point the controller must never write to stdio.
/// Keeping inert descriptors in 0..=2 prevents later opens from reusing those reserved numbers.
#[cfg(unix)]
pub struct StdioRelease(std::fs::File);

#[cfg(unix)]
impl StdioRelease {
    pub fn prepare() -> io::Result<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?;
        // A caller may already have closed a standard stream. Keep the backing descriptor
        // outside the reserved range, so dropping it cannot undo the replacement below.
        let file = if file.as_raw_fd() < 3 {
            // SAFETY: duplicate the open descriptor into an unused number at least 3.
            let descriptor = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
            if descriptor == -1 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fcntl returned a new descriptor owned exclusively by this value.
            unsafe { std::fs::File::from_raw_fd(descriptor) }
        } else {
            file
        };
        Ok(Self(file))
    }

    /// Relinquish this process's copies, not the copies its spawned command inherited.
    /// The caller must retain and report an error through its separate diagnostic channel.
    pub fn release(self) -> io::Result<()> {
        use std::os::fd::AsRawFd as _;
        for descriptor in 0..=2 {
            // SAFETY: the backing descriptor is open and outside the standard-stream range.
            if unsafe { libc::dup2(self.0.as_raw_fd(), descriptor) } == -1 {
                return Err(io::Error::other(format!(
                    "cannot relinquish standard descriptor {descriptor}: {}",
                    io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }
}

/// A process's exit, watched from a moment it ran. The kernel reports it only once the process
/// has released what it held, every descriptor included (measured on Darwin: a listener it alone
/// held is detached from its socket file by NOTE_EXIT; Linux makes a pidfd readable in
/// `exit_notify`, after `exit_files` and the deferred closes it queued). On Darwin it is a kqueue
/// with the process's `NOTE_EXIT` registered once; on Linux, the process's pidfd.
#[derive(Debug)]
pub struct ExitWatch(std::os::fd::OwnedFd);

impl ExitWatch {
    /// Watch the exit of the process `pid` names now, from this moment, so a later process given
    /// the same pid is never mistaken for it; `None` when no process holds `pid`, or (Darwin)
    /// the one that does has begun to exit. For a process this one does not own, such as a
    /// launchd agent's: taken before it is signalled, its exit is never missed.
    pub fn of(pid: u32) -> io::Result<Option<Self>> {
        let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
        match Self::register(pid) {
            Ok(watch) => Ok(Some(watch)),
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Block until the process has exited.
    pub fn wait(&self) -> io::Result<()> {
        self.exited(None).map(|_| ())
    }

    /// Block until the process has exited or `within` passes; whether it exited.
    pub fn within(&self, within: Duration) -> io::Result<bool> {
        self.exited(Some(within))
    }
}

impl std::os::fd::AsRawFd for ExitWatch {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.0)
    }
}

/// How a child process terminated, without collapsing signals into a synthetic exit code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStatus {
    Exit(i32),
    Signal(i32),
    Unknown,
}

impl ProcessStatus {
    pub fn succeeded(self) -> bool {
        self == Self::Exit(0)
    }
}

impl Default for ProcessStatus {
    fn default() -> Self {
        Self::Exit(0)
    }
}

impl From<ExitStatus> for ProcessStatus {
    fn from(status: ExitStatus) -> Self {
        if let Some(code) = status.code() {
            return Self::Exit(code);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return Self::Signal(signal);
            }
        }
        Self::Unknown
    }
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exit(code) => write!(f, "exit status {code}"),
            Self::Signal(signal) => write!(f, "signal {signal}"),
            Self::Unknown => f.write_str("unknown termination status"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommandOutput {
    pub status: ProcessStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    pub fn success(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            status: ProcessStatus::Exit(0),
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    pub fn failure(status: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self::failure_with_streams(ProcessStatus::Exit(status), Vec::new(), stderr)
    }

    pub fn failure_with_streams(
        status: ProcessStatus,
        stdout: impl Into<Vec<u8>>,
        stderr: impl Into<Vec<u8>>,
    ) -> Self {
        debug_assert!(!status.succeeded());
        Self {
            status,
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    pub fn succeeded(&self) -> bool {
        self.status.succeeded()
    }
}

impl From<Output> for CommandOutput {
    fn from(output: Output) -> Self {
        Self {
            status: output.status.into(),
            stdout: output.stdout,
            stderr: output.stderr,
        }
    }
}

/// Write a subprocess failure without decoding arbitrary output bytes or allocating a joined
/// command string. Valid UTF-8 stays readable; invalid bytes are escaped individually, so the
/// diagnostic retains every byte instead of replacing it with U+FFFD.
pub fn fmt_command_failure<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    operation: &str,
    program: &OsStr,
    args: &[A],
    output: &CommandOutput,
) -> fmt::Result {
    write!(f, "{operation} failed: ")?;
    fmt_command(f, program, args)?;
    write!(
        f,
        ", {}; stdout: {}; stderr: {}",
        output.status,
        DiagnosticBytes(&output.stdout),
        DiagnosticBytes(&output.stderr)
    )
}

pub fn fmt_command_spawn<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    program: &OsStr,
    args: &[A],
    source: &std::io::Error,
) -> fmt::Result {
    f.write_str("could not run ")?;
    fmt_command(f, program, args)?;
    write!(f, ": {source}")
}

/// `executable "<program>", argv [<args>]`: the command as every diagnostic names it.
pub fn fmt_command<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    program: &OsStr,
    args: &[A],
) -> fmt::Result {
    write!(f, "executable {program:?}, argv [")?;
    fmt_argv(f, args)?;
    f.write_str("]")
}

fn fmt_argv<A: AsRef<OsStr>>(f: &mut fmt::Formatter<'_>, args: &[A]) -> fmt::Result {
    for (index, arg) in args.iter().enumerate() {
        if index != 0 {
            f.write_str(", ")?;
        }
        write!(f, "{:?}", arg.as_ref())?;
    }
    Ok(())
}

struct DiagnosticBytes<'a>(&'a [u8]);

impl fmt::Display for DiagnosticBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.trim_ascii();
        if bytes.is_empty() {
            return f.write_str("<empty>");
        }

        let mut remaining = bytes;
        while !remaining.is_empty() {
            match std::str::from_utf8(remaining) {
                Ok(text) => {
                    f.write_str(text)?;
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid != 0 {
                        let prefix = std::str::from_utf8(&remaining[..valid])
                            .expect("Utf8Error valid prefix must decode");
                        f.write_str(prefix)?;
                    }
                    let invalid = error
                        .error_len()
                        .unwrap_or_else(|| remaining.len().saturating_sub(valid));
                    for byte in &remaining[valid..valid + invalid] {
                        write!(f, "\\x{byte:02x}")?;
                    }
                    remaining = &remaining[valid + invalid..];
                }
            }
        }
        Ok(())
    }
}

/// A process of this user that holds a file open: a task database, a volume a release was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Holder {
    pub pid: i32,
    pub command: String,
}

impl fmt::Display for Holder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "pid {} ({})", self.pid, self.command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_bytes_trim_edges_and_preserve_invalid_bytes() {
        assert_eq!(DiagnosticBytes(b"  readable\n").to_string(), "readable");
        assert_eq!(
            DiagnosticBytes(b"\nleft\xffright\x80\n").to_string(),
            "left\\xffright\\x80"
        );
        assert_eq!(DiagnosticBytes(b" \t\n").to_string(), "<empty>");
    }

    #[test]
    fn command_failure_and_spawn_share_argv_rendering() {
        let args = ["--flag", "value with space"];
        struct Failure<'a>(&'a [&'a str]);
        impl fmt::Display for Failure<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_command_failure(
                    f,
                    "copy",
                    OsStr::new("diskutil"),
                    self.0,
                    &CommandOutput::failure(1, "denied"),
                )
            }
        }
        struct Spawn<'a>(&'a [&'a str]);
        impl fmt::Display for Spawn<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_command_spawn(
                    f,
                    OsStr::new("diskutil"),
                    self.0,
                    &std::io::Error::from_raw_os_error(2),
                )
            }
        }
        let argv = r#"argv ["--flag", "value with space"]"#;
        assert!(Failure(&args).to_string().contains(argv));
        assert!(Spawn(&args).to_string().contains(argv));
    }
}
