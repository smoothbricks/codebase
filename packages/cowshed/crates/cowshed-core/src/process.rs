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

/// A process's executable path and its byte-exact argument vector, as `KERN_PROCARGS2` reports
/// them. Fails for a process that has exited, reaped or not.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessArguments {
    pub executable: Vec<u8>,
    pub argv: Vec<Vec<u8>>,
}

#[cfg(target_os = "macos")]
pub(crate) fn process_arguments(pid: libc::pid_t) -> io::Result<ProcessArguments> {
    let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
    let mut argmax: libc::c_int = 0;
    let mut argmax_size = size_of::<libc::c_int>();
    // SAFETY: `mib` names KERN_ARGMAX, whose value is one c_int written into `argmax`.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&raw mut argmax).cast(),
            &mut argmax_size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let capacity = usize::try_from(argmax).map_err(io::Error::other)?;
    let mut buffer = vec![0u8; capacity];
    let mut size: libc::size_t = capacity;
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
    // SAFETY: `buffer` is writable for `size` bytes; the kernel writes at most that many.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buffer.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    parse_procargs(&buffer[..size]).map_err(|unreadable| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("KERN_PROCARGS2 of process {pid} is {unreadable}"),
        )
    })
}

/// Why `KERN_PROCARGS2`'s bytes yield no argv.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum UnreadableProcargs {
    #[error("malformed")]
    Malformed,
    /// The kernel's stand-in for arguments it hides: the saved path, not the exec's argv.
    #[error("the kernel's saved-path stand-in, not the process's arguments")]
    SavedPath,
}

/// The kernel's `executable_path=` key, saved before the path and stripped from the answer.
#[cfg(target_os = "macos")]
const EXECUTABLE_KEY: usize = "executable_path=".len();

/// The pointer size of a 64-bit image, to which the strings before argv are padded.
#[cfg(target_os = "macos")]
const ARGV_ALIGNMENT: usize = 8;

/// `KERN_PROCARGS2`: a native-endian `argc`, the executable path and its NUL, NUL padding, then
/// `argc` NUL-terminated arguments (then the environment, which is never read).
///
/// The kernel saves the path after the 16-byte `executable_path=` key and pads key, path and
/// NUL to the pointer size of a 64-bit image before argv (xnu `exec_save_path`,
/// `exec_extract_strings`); `sysctl_procargsx` strips the key. So argv starts at an offset the
/// path's length alone sets, as measured on Darwin 25.6 for paths of 8 to 23 bytes. Skipping
/// every NUL after the path instead would take an empty `argv[0]` for padding and hand out the
/// environment's first entry as an argument.
///
/// A process holding the `no-read-procargs` entitlement is answered from its saved path instead
/// (`sysctl_procargs_no_read`): `argc` 1, the path, two NULs, and the path again as `argv[0]`.
/// Those are not the arguments it was exec'd with, so that shape is refused rather than read as
/// argv, even where the padded layout could also produce it.
#[cfg(target_os = "macos")]
fn parse_procargs(buffer: &[u8]) -> Result<ProcessArguments, UnreadableProcargs> {
    let (argc, strings) = buffer
        .split_first_chunk::<4>()
        .ok_or(UnreadableProcargs::Malformed)?;
    let argc =
        usize::try_from(i32::from_ne_bytes(*argc)).map_err(|_| UnreadableProcargs::Malformed)?;
    let path_end = strings
        .iter()
        .position(|&byte| byte == 0)
        .ok_or(UnreadableProcargs::Malformed)?;
    let executable = &strings[..path_end];
    if argc == 1
        && strings[path_end..]
            .strip_prefix(b"\0\0")
            .and_then(|rest| rest.strip_prefix(executable))
            == Some(&b"\0"[..])
    {
        return Err(UnreadableProcargs::SavedPath);
    }
    let argv = padded_argv(strings, path_end, argc).ok_or(UnreadableProcargs::Malformed)?;
    Ok(ProcessArguments {
        executable: executable.to_vec(),
        argv,
    })
}

/// The `argc` arguments of the padded layout, whose path ends at `path_end`; `None` when the
/// padding holds anything but NULs or an argument is cut short.
#[cfg(target_os = "macos")]
fn padded_argv(strings: &[u8], path_end: usize, argc: usize) -> Option<Vec<Vec<u8>>> {
    let start = (EXECUTABLE_KEY + path_end + 1).next_multiple_of(ARGV_ALIGNMENT) - EXECUTABLE_KEY;
    if strings.get(path_end..start)?.iter().any(|&byte| byte != 0) {
        return None;
    }
    let mut rest = &strings[start..];
    let mut argv = Vec::with_capacity(argc.min(rest.len()));
    for _ in 0..argc {
        let end = rest.iter().position(|&byte| byte == 0)?;
        argv.push(rest[..end].to_vec());
        rest = &rest[end + 1..];
    }
    Some(argv)
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

    #[cfg(target_os = "macos")]
    fn procargs(argc: i32, strings: &[u8]) -> Vec<u8> {
        [&argc.to_ne_bytes()[..], strings].concat()
    }

    #[cfg(target_os = "macos")]
    fn arguments(executable: &[u8], argv: &[&[u8]]) -> ProcessArguments {
        ProcessArguments {
            executable: executable.to_vec(),
            argv: argv.iter().map(|argument| argument.to_vec()).collect(),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn procargs_yield_the_executable_and_byte_exact_argv() {
        let buffer = procargs(4, b"/usr/bin/node\0\0\0node\0nx.js\0\0\xff\xfe\0HOME=/x\0");
        assert_eq!(
            parse_procargs(&buffer),
            Ok(arguments(
                b"/usr/bin/node",
                &[b"node", b"nx.js", b"", b"\xff\xfe"]
            ))
        );
        let malformed = Err(UnreadableProcargs::Malformed);
        assert_eq!(parse_procargs(&[1, 0]), malformed);
        assert_eq!(parse_procargs(&[1, 0, 0, 0, b'/', 0, 0, b'a']), malformed);
        // Padding is NULs only.
        assert_eq!(
            parse_procargs(&procargs(1, b"/bin/cat\0\0\0x\0\0\0\0\0\0a\0")),
            malformed
        );
    }

    /// An empty `argv[0]` is an argument, not padding (bytes as measured on Darwin 25.6).
    #[cfg(target_os = "macos")]
    #[test]
    fn procargs_keep_an_empty_leading_argument() {
        let empty = procargs(1, b"/bin/cat\0\0\0\0\0\0\0\0\0HOME=/x\0");
        assert_eq!(parse_procargs(&empty), Ok(arguments(b"/bin/cat", &[b""])));
        let longer = procargs(2, b"/bin//////////cat\0\0\0\0\0\0\0\0-\0HOME=/x\0");
        assert_eq!(
            parse_procargs(&longer),
            Ok(arguments(b"/bin//////////cat", &[b"", b"-"]))
        );
    }

    /// The saved-path stand-in for hidden arguments (its path twice) is never read as argv,
    /// whatever padding the path's length leaves (7, 1 and 0 bytes here).
    #[cfg(target_os = "macos")]
    #[test]
    fn procargs_refuse_the_saved_path_stand_in() {
        for path in [&b"/bin/cat"[..], b"/bin////////ls", b"/bin/////////ls"] {
            let saved = procargs(1, &[path, b"\0\0", path, b"\0"].concat());
            assert_eq!(parse_procargs(&saved), Err(UnreadableProcargs::SavedPath));
        }
    }

    /// A process exec'd with an empty `argv[0]` reads back with it, whatever padding its path
    /// length leaves.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_running_process_s_empty_leading_argument_is_read_back() {
        use std::io::{BufRead, Write};
        use std::os::unix::process::CommandExt;
        for path in ["/bin/cat", "/bin////////cat", "/bin/////////cat"] {
            let mut child = crate::fork_lock::Spawn::spawn_locked(
                std::process::Command::new(path)
                    .arg0("")
                    .arg("-")
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped()),
            )
            .expect("spawn");
            // An echoed line proves cat runs: its exec is complete.
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(b"x\n").unwrap();
            let mut line = String::new();
            std::io::BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(line, "x\n");
            let pid = libc::pid_t::try_from(child.id()).unwrap();
            assert_eq!(
                process_arguments(pid).unwrap(),
                arguments(path.as_bytes(), &[b"", b"-"]),
                "{path}"
            );
            drop(stdin);
            assert!(child.wait().unwrap().success());
        }
    }

    /// An exited child its parent has not reaped is a zombie: the null signal still reaches it,
    /// and it has exited all the same. Reaped, it is gone. This process itself runs.
    #[test]
    fn a_zombie_has_exited_though_the_null_signal_still_reaches_it() {
        // `/bin/sh` is the one program path every supported host has: NixOS keeps no `/usr/bin/true`.
        let mut child = crate::fork_lock::Spawn::spawn_locked(
            std::process::Command::new("/bin/sh").args(["-c", ":"]),
        )
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
