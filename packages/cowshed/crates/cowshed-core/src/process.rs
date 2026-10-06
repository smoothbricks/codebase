use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::process::{ExitStatus, Output};

/// Whether the process `pid` names has not exited. A zombie -- exited, not yet reaped by its
/// parent -- has: the null signal is never asked, because `kill(pid, 0)` succeeds on a zombie
/// (POSIX requires it), and a daemon reparented to launchd stays one until launchd gets round to
/// reaping it. A process this one may not inspect still runs, as far as anyone here can tell.
#[cfg(target_os = "macos")]
pub(crate) fn running(pid: libc::pid_t) -> io::Result<bool> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(io::Error::other)?;
    // Argument 0 asks for live processes only: the kernel answers `ESRCH` for a zombie exactly
    // as for a reaped pid.
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written == size {
        // SAFETY: proc_pidinfo filled all `size` bytes.
        return Ok(unsafe { info.assume_init() }.pbi_status != libc::SZOMB);
    }
    if written > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("proc_pidinfo returned {written} bytes, expected {size}"),
        ));
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error),
    }
}

/// Whether the process `pid` names may run under a sandbox profile: `sandbox_check` asked about
/// no operation answers 0 only for a live process with no profile (libsystem_sandbox, the query
/// `sandbox-exec`'s profiles are checked by), and 1 for a sandboxed process and for a pid that
/// names none (measured), so `false` proves a live, unsandboxed process.
#[cfg(target_os = "macos")]
pub(crate) fn sandboxed(pid: libc::pid_t) -> io::Result<bool> {
    unsafe extern "C" {
        fn sandbox_check(
            pid: libc::pid_t,
            operation: *const libc::c_char,
            filter: libc::c_int,
            ...
        ) -> libc::c_int;
    }
    /// `SANDBOX_FILTER_NONE`: the operation takes no argument.
    const NO_FILTER: libc::c_int = 0;
    // SAFETY: a null operation with no filter reads no variadic argument.
    match unsafe { sandbox_check(pid, std::ptr::null(), NO_FILTER) } {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(io::Error::last_os_error()),
    }
}

/// [`running`] from procfs: a pid without a stat has been reaped, and state `Z` (zombie) or `X`
/// (dead) has exited.
#[cfg(target_os = "linux")]
pub(crate) fn running(pid: libc::pid_t) -> io::Result<bool> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // An unmounted procfs proves nothing about `pid`.
            std::fs::metadata("/proc/self/stat")?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    // The command name is parenthesized and may hold anything; the state follows the last ')'.
    let state = stat
        .rfind(')')
        .and_then(|closing| stat[closing + 1..].split_whitespace().next())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process stat has no state"))?;
    Ok(!matches!(state, "Z" | "X"))
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
pub(crate) fn fmt_command_failure<A: AsRef<OsStr>>(
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

pub(crate) fn fmt_command_spawn<A: AsRef<OsStr>>(
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
pub(crate) fn fmt_command<A: AsRef<OsStr>>(
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

#[cfg(test)]
mod tests {
    use super::*;

    /// An exited child its parent has not reaped is a zombie: the null signal still reaches it,
    /// and it has exited all the same. Reaped, it is gone. This process itself runs.
    #[test]
    fn a_zombie_has_exited_though_the_null_signal_still_reaches_it() {
        let mut child =
            crate::fork_lock::Spawn::spawn_locked(&mut std::process::Command::new("/usr/bin/true"))
                .expect("spawn");
        let pid = libc::pid_t::try_from(child.id()).expect("pid");
        // SAFETY: an all-zero `siginfo_t` is a valid value of the plain C struct.
        let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
        // Block until the child has exited, leaving it unreaped.
        // SAFETY: `info` is a valid out-pointer; WNOWAIT leaves the child waitable.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                pid.cast_unsigned(),
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        assert_eq!(waited, 0, "{}", io::Error::last_os_error());
        // SAFETY: signal 0 only checks existence.
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "the zombie answers the null signal"
        );
        assert!(!running(pid).unwrap(), "a zombie has exited");
        child.wait().expect("reap");
        assert!(!running(pid).unwrap(), "a reaped pid has exited");
        assert!(running(libc::pid_t::try_from(std::process::id()).unwrap()).unwrap());
        // Launchd's: not this user's to inspect, and running.
        assert!(running(1).unwrap());
    }

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
