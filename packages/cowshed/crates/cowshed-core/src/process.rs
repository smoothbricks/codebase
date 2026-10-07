use std::collections::BTreeSet;
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

/// The bytes the process `pid` names holds resident now; `None` once the kernel says no such
/// process runs. An exited process never counts as holding memory: this reads
/// `pti_resident_size` of `proc_pidinfo(PROC_PIDTASKINFO)`, which answers `ESRCH` once the
/// process has no task, not `ri_resident_size` of `proc_pid_rusage`, which still answers for an
/// exited, unreaped process with the size it last held (measured: 1.1 MB for a zombie).
#[cfg(target_os = "macos")]
pub(crate) fn resident_bytes(pid: libc::pid_t) -> io::Result<Option<u64>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_taskinfo>())
        .map_err(io::Error::other)?;
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written == size {
        // SAFETY: proc_pidinfo filled all `size` bytes.
        return Ok(Some(unsafe { info.assume_init() }.pti_resident_size));
    }
    if written > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "proc_pidinfo returned {written} bytes of process {pid}'s task, expected {size}"
            ),
        ));
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(None),
        _ => Err(io::Error::new(
            error.kind(),
            format!("the resident memory of process {pid} cannot be read: {error}"),
        )),
    }
}

/// [`resident_bytes`] from procfs: `statm`'s resident pages times the page size. A pid without
/// a `statm` (`ENOENT`), or one reaped while it is read (`ESRCH`), names no process; an exited,
/// unreaped one has no memory left to count, and its `statm` reads zero pages.
#[cfg(target_os = "linux")]
pub(crate) fn resident_bytes(pid: libc::pid_t) -> io::Result<Option<u64>> {
    let statm = match std::fs::read_to_string(format!("/proc/{pid}/statm")) {
        Ok(statm) => statm,
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Ok(None);
        }
        Err(error) => {
            return Err(io::Error::new(
                error.kind(),
                format!("the resident memory of process {pid} cannot be read: {error}"),
            ));
        }
    };
    let invalid = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid}'s statm {statm:?} {what}"),
        )
    };
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| invalid("has no resident field"))?
        .parse()
        .map_err(|_| invalid("has a resident field that is no page count"))?;
    // SAFETY: sysconf takes a plain integer and touches no memory of ours.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = u64::try_from(page).map_err(|_| {
        io::Error::other(format!(
            "the page size cannot be read: {}",
            io::Error::last_os_error()
        ))
    })?;
    pages
        .checked_mul(page)
        .map(Some)
        .ok_or_else(|| invalid("holds more bytes than a u64 counts"))
}

/// The local ports of the TCP sockets in `LISTEN` that the descriptors of process `pid` hold,
/// IPv4 and IPv6 alike, each port once. A socket that is merely bound, or connected, or of
/// another protocol is no listener. The read names only the pid and never decides that the
/// process exited: a read that fails is an error, which the caller judges by the identity of the
/// process it means, read afterwards (`Process::listening_ports`) -- a process shown gone held
/// nothing, a process shown running left its sockets unread.
///
/// Darwin reads each socket descriptor's `socket_fdinfo` (`proc_pidfdinfo`), whose TCP record
/// carries the connection's state, as `lsof` and `netstat` read it.
#[cfg(target_os = "macos")]
pub(crate) fn listening_ports(pid: libc::pid_t) -> io::Result<BTreeSet<u16>> {
    let descriptors = descriptors(pid)?;
    let size =
        libc::c_int::try_from(std::mem::size_of::<SocketFdInfo>()).map_err(io::Error::other)?;
    let socket = u32::try_from(libc::PROX_FDTYPE_SOCKET).map_err(io::Error::other)?;
    let mut ports = BTreeSet::new();
    for descriptor in descriptors
        .iter()
        .filter(|descriptor| descriptor.proc_fdtype == socket)
    {
        let mut info = std::mem::MaybeUninit::<SocketFdInfo>::zeroed();
        // SAFETY: `info` is writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidfdinfo(
                pid,
                descriptor.proc_fd,
                PROC_PIDFDSOCKETINFO,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        if written == size {
            // SAFETY: proc_pidfdinfo filled all `size` bytes.
            let info = unsafe { info.assume_init() };
            if info.psi.soi_kind != SOCKINFO_TCP {
                continue;
            }
            // SAFETY: the kernel fills `pri_tcp` for a socket of kind `SOCKINFO_TCP`.
            let tcp = unsafe { info.psi.soi_proto.pri_tcp };
            if tcp.tcpsi_state == TSI_S_LISTEN {
                ports.insert(listening_port(pid, tcp.tcpsi_ini.insi_lport)?);
            }
            continue;
        }
        if written > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "proc_pidfdinfo returned {written} bytes of process {pid}'s descriptor {}, \
                     expected {size}",
                    descriptor.proc_fd
                ),
            ));
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::ESRCH) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("process {pid} was gone before its sockets were read: {error}"),
                ));
            }
            // Closed since the descriptors were listed: it holds no listener now.
            Some(libc::EBADF) => {}
            _ => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "socket descriptor {} of process {pid} cannot be read: {error}",
                        descriptor.proc_fd
                    ),
                ));
            }
        }
    }
    Ok(ports)
}

/// The port a `LISTEN` socket's `insi_lport` holds: the kernel copies `inp_lport`, a port in
/// network byte order, into that `int`.
#[cfg(target_os = "macos")]
fn listening_port(pid: libc::pid_t, local_port: libc::c_int) -> io::Result<u16> {
    u16::try_from(local_port).map(u16::from_be).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid} listens on local port {local_port}, which no u16 holds"),
        )
    })
}

/// Every descriptor of process `pid` (`PROC_PIDLISTFDS`).
#[cfg(target_os = "macos")]
fn descriptors(pid: libc::pid_t) -> io::Result<Vec<libc::proc_fdinfo>> {
    let empty = libc::proc_fdinfo {
        proc_fd: 0,
        proc_fdtype: 0,
    };
    let mut descriptors = vec![empty; 64];
    loop {
        let bytes = libc::c_int::try_from(std::mem::size_of_val(descriptors.as_slice()))
            .map_err(io::Error::other)?;
        // SAFETY: errno is thread-local; libproc writes at most `bytes` into this live slice.
        let written = unsafe {
            *libc::__error() = 0;
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                descriptors.as_mut_ptr().cast(),
                bytes,
            )
        };
        if written <= 0 {
            let error = io::Error::last_os_error();
            return if written == 0 && error.raw_os_error() == Some(0) {
                Ok(Vec::new())
            } else {
                Err(io::Error::new(
                    error.kind(),
                    format!("the descriptors of process {pid} cannot be listed: {error}"),
                ))
            };
        }
        let written = usize::try_from(written).map_err(io::Error::other)?;
        let entry = std::mem::size_of::<libc::proc_fdinfo>();
        if written % entry != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("proc_pidinfo listed {written} bytes of process {pid}'s descriptors"),
            ));
        }
        let count = written / entry;
        // A full buffer is never evidence that every descriptor was listed.
        if count < descriptors.len() {
            descriptors.truncate(count);
            return Ok(descriptors);
        }
        let length = descriptors.len().checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the descriptors of process {pid} overflowed"),
            )
        })?;
        descriptors.resize(length, empty);
    }
}

/// `PROC_PIDFDSOCKETINFO` (`<sys/proc_info.h>`), which the `libc` crate does not declare.
#[cfg(target_os = "macos")]
const PROC_PIDFDSOCKETINFO: libc::c_int = 3;
/// `SOCKINFO_TCP`: `soi_kind` of a TCP socket, IPv4 or IPv6.
#[cfg(target_os = "macos")]
const SOCKINFO_TCP: libc::c_int = 2;
/// `TSI_S_LISTEN`: `tcpsi_state` of a listening socket.
#[cfg(target_os = "macos")]
const TSI_S_LISTEN: libc::c_int = 1;

// The records `proc_pidfdinfo(PROC_PIDFDSOCKETINFO)` writes, as `<sys/proc_info.h>` declares
// them and the `libc` crate does not. Field for field, in the header's order and types; a field
// nothing here reads carries its header name behind `_`. The sizes and offsets below were
// measured with the SDK's own header (clang `sizeof`/`offsetof`, Darwin 25.6 arm64) and are
// asserted at compile time.

/// `struct proc_fileinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct ProcFileInfo {
    _fi_openflags: u32,
    _fi_status: u32,
    _fi_offset: libc::off_t,
    _fi_type: i32,
    _fi_guardflags: u32,
}

/// `struct sockbuf_info`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct SockbufInfo {
    _sbi_cc: u32,
    _sbi_hiwat: u32,
    _sbi_mbcnt: u32,
    _sbi_mbmax: u32,
    _sbi_lowat: u32,
    _sbi_flags: libc::c_short,
    _sbi_timeo: libc::c_short,
}

/// `struct in4in6_addr`: an IPv4 address in the last word of an IPv6-sized one.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct In4In6Addr {
    _i46a_pad32: [u32; 3],
    _i46a_addr4: libc::in_addr,
}

/// The `insi_faddr`/`insi_laddr` union of `struct in_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
union InSockAddress {
    _ina_46: In4In6Addr,
    _ina_6: libc::in6_addr,
}

/// The `insi_v4` member of `struct in_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct InSockV4 {
    _in4_tos: libc::c_uchar,
}

/// The `insi_v6` member of `struct in_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct InSockV6 {
    _in6_hlim: u8,
    _in6_cksum: libc::c_int,
    _in6_ifindex: libc::c_ushort,
    _in6_hops: libc::c_short,
}

/// `struct in_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct InSockInfo {
    _insi_fport: libc::c_int,
    insi_lport: libc::c_int,
    _insi_gencnt: u64,
    _insi_flags: u32,
    _insi_flow: u32,
    _insi_vflag: u8,
    _insi_ip_ttl: u8,
    _rfu_1: u32,
    _insi_faddr: InSockAddress,
    _insi_laddr: InSockAddress,
    _insi_v4: InSockV4,
    _insi_v6: InSockV6,
}

/// `struct tcp_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct TcpSockInfo {
    tcpsi_ini: InSockInfo,
    tcpsi_state: libc::c_int,
    /// `TSI_T_NTIMERS` timers.
    _tcpsi_timer: [libc::c_int; 4],
    _tcpsi_mss: libc::c_int,
    _tcpsi_flags: u32,
    _rfu_1: u32,
    _tcpsi_tp: u64,
}

/// The `unsi_addr`/`unsi_caddr` union of `struct un_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
union UnSockAddress {
    _ua_sun: libc::sockaddr_un,
    /// `SOCK_MAXADDRLEN` bytes.
    _ua_dummy: [libc::c_char; 255],
}

/// `struct un_sockinfo`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct UnSockInfo {
    _unsi_conn_so: u64,
    _unsi_conn_pcb: u64,
    _unsi_addr: UnSockAddress,
    _unsi_caddr: UnSockAddress,
}

/// The `soi_proto` union of `struct socket_info`. Its other members -- `ndrv_info`,
/// `kern_event_info`, `kern_ctl_info`, `vsock_sockinfo` -- are smaller than `un_sockinfo` and
/// never read here, so leaving them out changes neither its size nor its alignment.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
union SocketProtocolInfo {
    _pri_in: InSockInfo,
    pri_tcp: TcpSockInfo,
    _pri_un: UnSockInfo,
}

/// `struct socket_info`.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct SocketInfo {
    _soi_stat: libc::vinfo_stat,
    _soi_so: u64,
    _soi_pcb: u64,
    _soi_type: libc::c_int,
    _soi_protocol: libc::c_int,
    _soi_family: libc::c_int,
    _soi_options: libc::c_short,
    _soi_linger: libc::c_short,
    _soi_state: libc::c_short,
    _soi_qlen: libc::c_short,
    _soi_incqlen: libc::c_short,
    _soi_qlimit: libc::c_short,
    _soi_timeo: libc::c_short,
    _soi_error: libc::c_ushort,
    _soi_oobmark: u32,
    _soi_rcv: SockbufInfo,
    _soi_snd: SockbufInfo,
    soi_kind: libc::c_int,
    _rfu_1: u32,
    soi_proto: SocketProtocolInfo,
}

/// `struct socket_fdinfo`: what `PROC_PIDFDSOCKETINFO` writes.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
struct SocketFdInfo {
    _pfi: ProcFileInfo,
    psi: SocketInfo,
}

#[cfg(target_os = "macos")]
const _: () = {
    use std::mem::{offset_of, size_of};
    assert!(size_of::<ProcFileInfo>() == 24);
    assert!(size_of::<libc::vinfo_stat>() == 136);
    assert!(size_of::<SockbufInfo>() == 24);
    assert!(size_of::<InSockInfo>() == 80);
    assert!(offset_of!(InSockInfo, insi_lport) == 4);
    assert!(size_of::<TcpSockInfo>() == 120);
    assert!(offset_of!(TcpSockInfo, tcpsi_state) == 80);
    assert!(size_of::<UnSockInfo>() == 528);
    assert!(size_of::<SocketInfo>() == 768);
    assert!(size_of::<SocketFdInfo>() == 792);
    assert!(offset_of!(SocketFdInfo, psi) == 24);
    assert!(offset_of!(SocketFdInfo, psi._soi_family) == 184);
    assert!(offset_of!(SocketFdInfo, psi.soi_kind) == 256);
    assert!(offset_of!(SocketFdInfo, psi.soi_proto) == 264);
};

/// [`listening_ports`] from procfs: the socket inodes of `/proc/<pid>/fd`, matched against the
/// `LISTEN` rows of `/proc/<pid>/net/tcp` and `tcp6`, the tables of the network namespace the
/// process is in.
///
/// Those tables list only that namespace's sockets, so this answer is complete because a job
/// lives in one namespace: its workspace's private network namespace, which its processes can
/// neither leave nor exchange for another (`unshare`/`setns` are refused, 04_sandbox.md, Linux
/// "Egress"). A process that held a socket made in another namespace would have it missing here.
#[cfg(target_os = "linux")]
pub(crate) fn listening_ports(pid: libc::pid_t) -> io::Result<BTreeSet<u16>> {
    use std::os::unix::ffi::OsStrExt as _;

    let unreadable = |what: &str, error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("the {what} of process {pid} cannot be read: {error}"),
        )
    };
    let mut sockets = std::collections::HashSet::new();
    for descriptor in std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map_err(|error| unreadable("descriptors", error))?
    {
        let descriptor = descriptor.map_err(|error| unreadable("descriptors", error))?;
        let target = match std::fs::read_link(descriptor.path()) {
            Ok(target) => target,
            // Closed since the directory was read: it holds no listener now.
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(unreadable("descriptors", error)),
        };
        if let Some(inode) = socket_inode(target.as_os_str().as_bytes()) {
            sockets.insert(inode);
        }
    }
    let mut ports = BTreeSet::new();
    if sockets.is_empty() {
        return Ok(ports);
    }
    for table in ["tcp", "tcp6"] {
        let rows = match std::fs::read_to_string(format!("/proc/{pid}/net/{table}")) {
            Ok(rows) => rows,
            // A kernel built without IPv6 has no tcp6 table, and so no IPv6 listener: the
            // process's tcp table was just read, so the process was there to have one.
            Err(error) if table == "tcp6" && error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(unreadable(table, error)),
        };
        tcp_listeners(&rows, &sockets, &mut ports).map_err(|reason| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid}'s /proc/{pid}/net/{table} {reason}"),
            )
        })?;
    }
    Ok(ports)
}

/// The inode of a descriptor link that names a socket: `socket:[<inode>]`.
#[cfg(target_os = "linux")]
fn socket_inode(target: &[u8]) -> Option<u64> {
    let inode = target.strip_prefix(b"socket:[")?.strip_suffix(b"]")?;
    std::str::from_utf8(inode).ok()?.parse().ok()
}

/// Adds to `ports` the local port of each row of a procfs TCP table (`net/tcp`, `net/tcp6`) in
/// state `LISTEN` whose socket inode is one of `sockets`. Each row is `sl local rem st
/// tx_queue:rx_queue tr:tm->when retrnsmt uid timeout inode …`, an address being
/// `<hex address>:<hex port>` (`tcp4_seq_show`, `tcp6_seq_show`); a row of another shape is
/// refused, never skipped.
#[cfg(any(target_os = "linux", test))]
fn tcp_listeners(
    table: &str,
    sockets: &std::collections::HashSet<u64>,
    ports: &mut BTreeSet<u16>,
) -> Result<(), String> {
    /// `TCP_LISTEN` in `include/net/tcp_states.h`, as the table prints it.
    const LISTEN: &str = "0A";
    let mut rows = table.lines();
    match rows.next() {
        Some(header) if header.trim_start().starts_with("sl") => {}
        header => return Err(format!("has no table header: {header:?}")),
    }
    for row in rows {
        let mut fields = row.split_whitespace();
        let (Some(local), Some(state), Some(inode)) = (fields.nth(1), fields.nth(1), fields.nth(5))
        else {
            return Err(format!("has a row of no socket: {row:?}"));
        };
        let inode: u64 = inode
            .parse()
            .map_err(|_| format!("has a row whose inode is no number: {row:?}"))?;
        if state != LISTEN || !sockets.contains(&inode) {
            continue;
        }
        let port = local
            .rsplit_once(':')
            .and_then(|(_, port)| u16::from_str_radix(port, 16).ok())
            .ok_or_else(|| format!("has a row whose local address has no port: {row:?}"))?;
        ports.insert(port);
    }
    Ok(())
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

    /// A procfs TCP table names a port only for a `LISTEN` row whose socket the process holds:
    /// a connected socket on the same port, another process's listener and a time-wait row are
    /// none, IPv4 and IPv6 listeners on one port are one port, and a row of another shape is
    /// refused rather than skipped.
    #[test]
    fn a_procfs_tcp_table_names_only_the_listeners_a_process_holds() {
        let tcp = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n\
            \x20  0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 12345 1 0000000000000000 100 0 0 10 0\n\
            \x20  1: 0100007F:1F90 0100007F:C350 01 00000000:00000000 00:00000000 00000000  1000        0 12346 1 0000000000000000 20 4 30 10 -1\n\
            \x20  2: 0100007F:0050 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 999 1 0000000000000000 100 0 0 10 0\n\
            \x20  3: 0100007F:1F91 0100007F:C351 06 00000000:00000000 03:00001770 00000000     0        0 0 3 0000000000000000\n";
        let tcp6 = "  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n\
            \x20  0: 00000000000000000000000001000000:1F90 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 22222 1 0000000000000000 100 0 0 10 0\n\
            \x20  1: 00000000000000000000000001000000:0BB8 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 33333 1 0000000000000000 100 0 0 10 0\n";
        let held = std::collections::HashSet::from([12345, 12346, 22222, 33333]);
        let mut ports = BTreeSet::new();
        tcp_listeners(tcp, &held, &mut ports).expect("tcp");
        tcp_listeners(tcp6, &held, &mut ports).expect("tcp6");
        assert_eq!(ports, BTreeSet::from([3000, 8080]));

        let header = tcp.lines().next().expect("header");
        for refused in [
            String::new(),
            "   0: 0100007F:1F90 00000000:0000 0A\n".to_owned(),
            format!("{header}\n   0: 0100007F:1F90 00000000:0000 0A 0:0 0:0 0 1000 0\n"),
            format!("{header}\n   0: 0100007F:1F90 00000000:0000 0A 0:0 0:0 0 1000 0 inode\n"),
            format!("{header}\n   0: 0100007F 00000000:0000 0A 0:0 0:0 0 1000 0 12345\n"),
        ] {
            assert!(
                tcp_listeners(&refused, &held, &mut BTreeSet::new()).is_err(),
                "{refused:?}"
            );
        }
    }

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
