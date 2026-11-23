//! Darwin's readings of a process: libproc (`proc_pidinfo`, `proc_pidfdinfo`,
//! `proc_listpgrppids`, `proc_pid_rusage`), `sysctl(KERN_PROCARGS2)` and libsystem_sandbox's
//! `sandbox_check`.

use std::collections::BTreeSet;
use std::io;

use super::{
    DescriptorName, GroupMember, NameKinds, ProcessArguments, ProcessLineage, RunState, Unread,
};
/// Whether the process `pid` names has not exited. A zombie -- exited, not yet reaped by its
/// parent -- has: the null signal is never asked, because `kill(pid, 0)` succeeds on a zombie
/// (POSIX requires it), and a daemon reparented to launchd stays one until launchd gets round to
/// reaping it. A process this one may not inspect still runs, as far as anyone here can tell.
pub fn running(pid: libc::pid_t) -> io::Result<bool> {
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
pub fn sandboxed(pid: libc::pid_t) -> io::Result<bool> {
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

pub fn process_arguments(pid: libc::pid_t) -> io::Result<ProcessArguments> {
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum UnreadableProcargs {
    #[error("malformed")]
    Malformed,
    /// The kernel's stand-in for arguments it hides: the saved path, not the exec's argv.
    #[error("the kernel's saved-path stand-in, not the process's arguments")]
    SavedPath,
}

/// The kernel's `executable_path=` key, saved before the path and stripped from the answer.
const EXECUTABLE_KEY: usize = "executable_path=".len();

/// The pointer size of a 64-bit image, to which the strings before argv are padded.
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

/// [`RunState`] of the process `pid` names, read as `ps` reads it on Darwin: a zombie or a
/// stopped process by its status; otherwise by its threads, where any running thread makes it
/// run and else any thread in an uninterruptible wait puts it in one.
pub fn run_state(pid: libc::pid_t) -> RunState {
    darwin_run_state(pid).unwrap_or_else(|error| RunState::Unread(error.to_string()))
}

fn darwin_run_state(pid: libc::pid_t) -> io::Result<RunState> {
    // xnu `bsd/sys/proc_info.h`: the thread handles of a process, one `u64` each.
    const PROC_PIDLISTTHREADS: libc::c_int = 6;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(io::Error::other)?;
    // Argument 1 answers for a zombie too (xnu `proc_pidinfo`: `findzomb`), which argument 0
    // reports as `ESRCH` like a reaped pid.
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            1,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        if written > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("proc_pidinfo returned {written} bytes, expected {size}"),
            ));
        }
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(RunState::Gone),
            _ => Err(error),
        };
    }
    // SAFETY: proc_pidinfo filled all `size` bytes.
    match unsafe { info.assume_init() }.pbi_status {
        libc::SZOMB => return Ok(RunState::Zombie),
        libc::SSTOP => return Ok(RunState::Stopped),
        _ => {}
    }
    // The kernel copies as many thread handles as fit: a full buffer may have left some out.
    let mut threads = vec![0_u64; 64];
    let count = loop {
        let capacity = libc::c_int::try_from(std::mem::size_of_val(threads.as_slice()))
            .map_err(io::Error::other)?;
        // SAFETY: `threads` is writable storage of exactly `capacity` bytes.
        let listed = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDLISTTHREADS,
                0,
                threads.as_mut_ptr().cast(),
                capacity,
            )
        };
        if listed <= 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ESRCH) => Ok(RunState::Gone),
                _ => Err(error),
            };
        }
        if listed < capacity {
            break usize::try_from(listed).map_err(io::Error::other)? / std::mem::size_of::<u64>();
        }
        threads.resize(threads.len() * 2, 0);
    };
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_threadinfo>())
        .map_err(io::Error::other)?;
    let mut uninterruptible = false;
    for &thread in threads.iter().take(count) {
        let mut info = std::mem::MaybeUninit::<libc::proc_threadinfo>::zeroed();
        // SAFETY: `info` is writable storage of exactly `size` bytes.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTHREADINFO,
                thread,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        // A thread that ended since it was listed says nothing.
        if written != size {
            continue;
        }
        // SAFETY: proc_pidinfo filled all `size` bytes.
        match unsafe { info.assume_init() }.pth_run_state {
            libc::TH_STATE_RUNNING => return Ok(RunState::Running),
            libc::TH_STATE_UNINTERRUPTIBLE => uninterruptible = true,
            _ => {}
        }
    }
    Ok(if uninterruptible {
        RunState::Uninterruptible
    } else {
        RunState::Sleeping
    })
}

/// [`ProcessLineage`] of `pid`, or `None` when no process holds it. One system call for this pid
/// alone: nothing walks the process table, spawns a process or waits on the process's task, so a
/// process stalled in the kernel cannot stall the reader, as it stalls `ps`.
pub fn lineage(pid: libc::pid_t) -> io::Result<Option<ProcessLineage>> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(io::Error::other)?;
    // Argument 1 answers for a zombie too (xnu `proc_pidinfo`: `findzomb`), which argument 0
    // reports as `ESRCH` like a reaped pid.
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            1,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        if written > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("proc_pidinfo of pid {pid} returned {written} bytes, expected {size}"),
            ));
        }
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(None),
            _ => Err(io::Error::new(
                error.kind(),
                format!("proc_pidinfo of pid {pid}: {error}"),
            )),
        };
    }
    // SAFETY: proc_pidinfo filled all `size` bytes.
    let info = unsafe { info.assume_init() };
    Ok(Some(ProcessLineage {
        pid,
        ppid: libc::pid_t::try_from(info.pbi_ppid).map_err(io::Error::other)?,
        pgid: libc::pid_t::try_from(info.pbi_pgid).map_err(io::Error::other)?,
        zombie: info.pbi_status == libc::SZOMB,
    }))
}

/// The pids holding the group id `pgid`, exited processes included, as `proc_listpgrppids` lists
/// them. A full buffer is never taken as the whole group: it is doubled and read again.
pub fn group_pids(pgid: libc::pid_t) -> io::Result<Vec<libc::pid_t>> {
    let mut pids = vec![0; 32];
    loop {
        let bytes =
            i32::try_from(std::mem::size_of_val(pids.as_slice())).map_err(io::Error::other)?;
        // SAFETY: errno is thread-local; libproc writes at most `bytes` into this live slice.
        let count = unsafe {
            *libc::__error() = 0;
            libc::proc_listpgrppids(pgid, pids.as_mut_ptr().cast(), bytes)
        };
        if count <= 0 {
            let error = io::Error::last_os_error();
            return if count == 0 && error.raw_os_error() == Some(0) {
                Ok(Vec::new())
            } else {
                Err(error)
            };
        }
        let count = usize::try_from(count).map_err(io::Error::other)?;
        if count < pids.len() {
            pids.truncate(count);
            if pids.iter().any(|pid| *pid <= 0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "process group membership contains an invalid pid",
                ));
            }
            return Ok(pids);
        }
        let length = pids.len().checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process group membership overflowed",
            )
        })?;
        pids.resize(length, 0);
    }
}

/// The CPU the processes of group `pgid` have used, each with its reaped children's, in the
/// kernel's own unit (Mach ticks on Darwin): comparable only to another reading of this group,
/// reaps it, so work done anywhere in the group advances the reading. A member that exits
/// between the listing and its read is left out, and so is one the kernel will not read for this
/// process (`EPERM`: a setuid program), whose work the reading then does not see; a run that goes
/// silent names it among its group's members. Any other failed read is the reading's error.
/// between the listing and its read is left out; any other failed read is the reading's error.
pub fn group_cpu(pgid: libc::pid_t) -> io::Result<u64> {
    let mut total = 0_u64;
    for pid in group_pids(pgid)? {
        let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
        // SAFETY: `info` is writable storage of the size the V4 flavor writes.
        let read =
            unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V4, info.as_mut_ptr().cast()) };
        if read != 0 {
            let error = io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(libc::ESRCH | libc::EPERM)) {
                continue;
            }
            return Err(io::Error::new(
                error.kind(),
                format!("read the CPU of process {pid} in group {pgid}: {error}"),
            ));
        }
        // SAFETY: a successful call filled the V4 record.
        let info = unsafe { info.assume_init() };
        total = total
            .saturating_add(info.ri_user_time)
            .saturating_add(info.ri_system_time)
            .saturating_add(info.ri_child_user_time)
            .saturating_add(info.ri_child_system_time);
    }
    Ok(total)
}

/// The running processes of group `pgid`, each named by its command line (its executable when the
/// arguments cannot be read) and its state.
pub fn group_members(pgid: libc::pid_t) -> io::Result<Vec<GroupMember>> {
    Ok(group_pids(pgid)?
        .into_iter()
        .filter_map(|pid| {
            let state = run_state(pid);
            if matches!(state, RunState::Gone | RunState::Zombie) {
                return None;
            }
            let command = process_arguments(pid).map_or_else(
                |error| format!("command unread: {error}"),
                |arguments| {
                    arguments
                        .argv
                        .iter()
                        .map(|argument| String::from_utf8_lossy(argument))
                        .collect::<Vec<_>>()
                        .join(" ")
                },
            );
            Some(GroupMember {
                pid,
                command,
                state,
            })
        })
        .collect())
}

/// The bytes the process `pid` names holds resident now; `None` once the kernel says no such
/// process runs. An exited process never counts as holding memory: this reads
/// `pti_resident_size` of `proc_pidinfo(PROC_PIDTASKINFO)`, which answers `ESRCH` once the
/// process has no task, not `ri_resident_size` of `proc_pid_rusage`, which still answers for an
/// exited, unreaped process with the size it last held (measured: 1.1 MB for a zombie).
pub fn resident_bytes(pid: libc::pid_t) -> io::Result<Option<u64>> {
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

/// The local ports of the TCP sockets in `LISTEN` that the descriptors of process `pid` hold,
/// IPv4 and IPv6 alike, each port once. A socket that is merely bound, or connected, or of
/// another protocol is no listener. The read names only the pid and never decides that the
/// process exited: a read that fails is an error, which the caller judges by the identity of the
/// process it means, read afterwards (`Process::listening_ports`) -- a process shown gone held
/// nothing, a process shown running left its sockets unread.
///
/// Darwin reads each socket descriptor's `socket_fdinfo` (`proc_pidfdinfo`), whose TCP record
/// carries the connection's state, as `lsof` and `netstat` read it.
pub fn listening_ports(pid: libc::pid_t) -> io::Result<BTreeSet<u16>> {
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
fn listening_port(pid: libc::pid_t, local_port: libc::c_int) -> io::Result<u16> {
    u16::try_from(local_port).map(u16::from_be).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid} listens on local port {local_port}, which no u16 holds"),
        )
    })
}

/// Every descriptor of process `pid` (`PROC_PIDLISTFDS`).
fn descriptors(pid: libc::pid_t) -> io::Result<Vec<libc::proc_fdinfo>> {
    listing(pid)?.map_err(|unread| {
        let error = io::Error::from_raw_os_error(unread.errno());
        io::Error::new(
            error.kind(),
            format!("the descriptors of process {pid} cannot be listed: {error}"),
        )
    })
}

/// Every descriptor of process `pid` (`PROC_PIDLISTFDS`), or why the kernel listed none: the
/// process exited (`ESRCH`) or is withheld from this user (`EPERM`). Any other refusal is an
/// error.
fn listing(pid: libc::pid_t) -> io::Result<Result<Vec<libc::proc_fdinfo>, Unread>> {
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
            return match (written, error.raw_os_error()) {
                (0, Some(0)) => Ok(Ok(Vec::new())),
                (_, Some(libc::ESRCH)) => Ok(Err(Unread::Gone)),
                (_, Some(libc::EPERM)) => Ok(Err(Unread::Withheld)),
                _ => Err(io::Error::new(
                    error.kind(),
                    format!("the descriptors of process {pid} cannot be listed: {error}"),
                )),
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
            return Ok(Ok(descriptors));
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
const PROC_PIDFDSOCKETINFO: libc::c_int = 3;
/// `SOCKINFO_TCP`: `soi_kind` of a TCP socket, IPv4 or IPv6.
const SOCKINFO_TCP: libc::c_int = 2;
/// `TSI_S_LISTEN`: `tcpsi_state` of a listening socket.
const TSI_S_LISTEN: libc::c_int = 1;

// The records `proc_pidfdinfo(PROC_PIDFDSOCKETINFO)` writes, as `<sys/proc_info.h>` declares
// them and the `libc` crate does not. Field for field, in the header's order and types; a field
// nothing here reads carries its header name behind `_`. The sizes and offsets below were
// measured with the SDK's own header (clang `sizeof`/`offsetof`, Darwin 25.6 arm64) and are
// asserted at compile time.

/// `struct proc_fileinfo`.
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
#[derive(Clone, Copy)]
#[repr(C)]
struct In4In6Addr {
    _i46a_pad32: [u32; 3],
    _i46a_addr4: libc::in_addr,
}

/// The `insi_faddr`/`insi_laddr` union of `struct in_sockinfo`.
#[derive(Clone, Copy)]
#[repr(C)]
union InSockAddress {
    _ina_46: In4In6Addr,
    _ina_6: libc::in6_addr,
}

/// The `insi_v4` member of `struct in_sockinfo`.
#[derive(Clone, Copy)]
#[repr(C)]
struct InSockV4 {
    _in4_tos: libc::c_uchar,
}

/// The `insi_v6` member of `struct in_sockinfo`.
#[derive(Clone, Copy)]
#[repr(C)]
struct InSockV6 {
    _in6_hlim: u8,
    _in6_cksum: libc::c_int,
    _in6_ifindex: libc::c_ushort,
    _in6_hops: libc::c_short,
}

/// `struct in_sockinfo`.
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
    tcpsi_tp: u64,
}

/// The `unsi_addr`/`unsi_caddr` union of `struct un_sockinfo`.
#[derive(Clone, Copy)]
#[repr(C)]
union UnSockAddress {
    _ua_sun: libc::sockaddr_un,
    /// `SOCK_MAXADDRLEN` bytes.
    _ua_dummy: [libc::c_char; 255],
}

/// `struct un_sockinfo`.
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
#[derive(Clone, Copy)]
#[repr(C)]
union SocketProtocolInfo {
    _pri_in: InSockInfo,
    pri_tcp: TcpSockInfo,
    _pri_un: UnSockInfo,
}

/// `struct socket_info`.
#[derive(Clone, Copy)]
#[repr(C)]
struct SocketInfo {
    _soi_stat: libc::vinfo_stat,
    _soi_so: u64,
    soi_pcb: u64,
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
#[derive(Clone, Copy)]
#[repr(C)]
struct SocketFdInfo {
    _pfi: ProcFileInfo,
    psi: SocketInfo,
}

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

impl Unread {
    /// The kernel's error for it: `ESRCH` or `EPERM`.
    fn errno(self) -> i32 {
        match self {
            Self::Gone => libc::ESRCH,
            Self::Withheld => libc::EPERM,
        }
    }
}

/// The name `lsof -F di` gives this process's `descriptor` ([`DescriptorName`]), read from the
/// kernel as [`descriptor_names`] reads another process's: the descriptor list, then the one
/// `proc_pidfdinfo` record of its kind.
pub fn descriptor_name(descriptor: std::os::fd::RawFd) -> io::Result<DescriptorName> {
    // SAFETY: getpid has no failure and touches no memory.
    let pid = unsafe { libc::getpid() };
    let Some(listed) = descriptors(pid)?
        .into_iter()
        .find(|listed| listed.proc_fd == descriptor)
    else {
        return Ok(DescriptorName::Closed);
    };
    name_of(pid, listed)?.map_err(|unread| {
        let error = io::Error::from_raw_os_error(unread.errno());
        io::Error::new(
            error.kind(),
            format!("descriptor {descriptor} cannot be read: {error}"),
        )
    })
}

/// Every open descriptor of process `pid` of the `kinds` asked for, in descriptor order, each by
/// number with the name [`descriptor_name`] gives it in that process; or why none were read. A
/// descriptor closed while it is read is left out.
pub fn descriptor_names(
    pid: libc::pid_t,
    kinds: NameKinds,
) -> io::Result<Result<Vec<(std::os::fd::RawFd, DescriptorName)>, Unread>> {
    let listed = match listing(pid)? {
        Ok(listed) => listed,
        Err(unread) => return Ok(Err(unread)),
    };
    let mut named = Vec::new();
    for listed in listed {
        let wanted = match libc::c_int::try_from(listed.proc_fdtype) {
            Ok(libc::PROX_FDTYPE_PIPE | libc::PROX_FDTYPE_SOCKET) => kinds.devices,
            Ok(libc::PROX_FDTYPE_VNODE) => kinds.inodes,
            _ => false,
        };
        if !wanted {
            continue;
        }
        match name_of(pid, listed)? {
            Ok(DescriptorName::Closed) => {}
            Ok(name) => named.push((listed.proc_fd, name)),
            Err(unread) => return Ok(Err(unread)),
        }
    }
    Ok(Ok(named))
}

/// The name of `pid`'s listed descriptor ([`DescriptorName`]) from the one record
/// `proc_pidfdinfo` writes for its kind: a pipe end's handle, a TCP socket's control block or
/// another socket's protocol control block, a file's inode. A descriptor closed since its listing
/// is closed; a revoked vnode is unnamed.
fn name_of(
    pid: libc::pid_t,
    listed: libc::proc_fdinfo,
) -> io::Result<Result<DescriptorName, Unread>> {
    let descriptor = listed.proc_fd;
    let named = match libc::c_int::try_from(listed.proc_fdtype).map_err(io::Error::other)? {
        libc::PROX_FDTYPE_PIPE => {
            descriptor_info::<PipeFdInfo>(pid, descriptor, PROC_PIDFDPIPEINFO)?
                .map(|info| DescriptorName::Device(info.ppi.pipe_handle))
        }
        libc::PROX_FDTYPE_SOCKET => {
            descriptor_info::<SocketFdInfo>(pid, descriptor, PROC_PIDFDSOCKETINFO)?.map(|info| {
                DescriptorName::Device(if info.psi.soi_kind == SOCKINFO_TCP {
                    // SAFETY: the kernel fills `pri_tcp` for a socket of kind `SOCKINFO_TCP`.
                    unsafe { info.psi.soi_proto.pri_tcp }.tcpsi_tp
                } else {
                    info.psi.soi_pcb
                })
            })
        }
        libc::PROX_FDTYPE_VNODE => {
            match descriptor_info::<VnodeFdInfo>(pid, descriptor, PROC_PIDFDVNODEINFO)? {
                Ok(info) => Ok(DescriptorName::Inode(info.pvi.vi_stat.vst_ino)),
                // A revoked vnode (a terminal `revoke(2)` took away, which lsof shows as
                // `(revoked)` with no node) has no inode to give: open, named by neither field.
                Err(libc::ENOENT) => Ok(DescriptorName::Unnamed {
                    fd_type: listed.proc_fdtype,
                }),
                Err(errno) => Err(errno),
            }
        }
        _ => {
            return Ok(Ok(DescriptorName::Unnamed {
                fd_type: listed.proc_fdtype,
            }));
        }
    };
    match named {
        Ok(name) => Ok(Ok(name)),
        Err(libc::EBADF) => Ok(Ok(DescriptorName::Closed)),
        Err(libc::ESRCH) => Ok(Err(Unread::Gone)),
        Err(libc::EPERM) => Ok(Err(Unread::Withheld)),
        Err(errno) => {
            let error = io::Error::from_raw_os_error(errno);
            Err(io::Error::new(
                error.kind(),
                format!("descriptor {descriptor} of process {pid} cannot be read: {error}"),
            ))
        }
    }
}

/// The record `proc_pidfdinfo` writes for `flavor` of process `pid`'s `descriptor`, whole, or
/// the errno the kernel refused it with.
fn descriptor_info<T: Copy>(
    pid: libc::pid_t,
    descriptor: std::os::fd::RawFd,
    flavor: libc::c_int,
) -> io::Result<Result<T, i32>> {
    let size = libc::c_int::try_from(std::mem::size_of::<T>()).map_err(io::Error::other)?;
    let mut info = std::mem::MaybeUninit::<T>::zeroed();
    // SAFETY: `info` is writable storage of exactly `size` bytes.
    let written =
        unsafe { libc::proc_pidfdinfo(pid, descriptor, flavor, info.as_mut_ptr().cast(), size) };
    if written == size {
        // SAFETY: proc_pidfdinfo filled all `size` bytes.
        return Ok(Ok(unsafe { info.assume_init() }));
    }
    if written > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "proc_pidfdinfo returned {written} bytes of process {pid}'s descriptor \
                 {descriptor}, expected {size}"
            ),
        ));
    }
    Ok(Err(io::Error::last_os_error().raw_os_error().unwrap_or(0)))
}

/// `PROC_PIDFDPIPEINFO` (`<sys/proc_info.h>`), which the `libc` crate does not declare.
const PROC_PIDFDPIPEINFO: libc::c_int = 6;

/// `struct pipe_info`.
#[derive(Clone, Copy)]
#[repr(C)]
struct PipeInfo {
    _pipe_stat: libc::vinfo_stat,
    pipe_handle: u64,
    _pipe_peerhandle: u64,
    _pipe_status: libc::c_int,
    _rfu_1: libc::c_int,
}

/// `struct pipe_fdinfo`: what `PROC_PIDFDPIPEINFO` writes.
#[derive(Clone, Copy)]
#[repr(C)]
struct PipeFdInfo {
    _pfi: ProcFileInfo,
    ppi: PipeInfo,
}

const _: () = {
    use std::mem::{offset_of, size_of};
    assert!(size_of::<PipeFdInfo>() == 184);
    assert!(offset_of!(PipeFdInfo, ppi.pipe_handle) == 160);
    assert!(offset_of!(SocketFdInfo, psi.soi_pcb) == 168);
    assert!(offset_of!(TcpSockInfo, tcpsi_tp) == 112);
};

/// `PROC_PIDFDVNODEINFO` (`<sys/proc_info.h>`), which the `libc` crate does not declare.
const PROC_PIDFDVNODEINFO: libc::c_int = 1;

/// `struct vnode_fdinfo`: what `PROC_PIDFDVNODEINFO` writes.
#[derive(Clone, Copy)]
#[repr(C)]
struct VnodeFdInfo {
    _pfi: ProcFileInfo,
    pvi: libc::vnode_info,
}

const _: () = {
    use std::mem::{offset_of, size_of};
    assert!(size_of::<VnodeFdInfo>() == 176);
    assert!(offset_of!(VnodeFdInfo, pvi) == 24);
};

#[cfg(test)]
mod tests {
    use super::*;

    /// What `lsof -a -p <this process> -d <descriptor> -F di` prints of `descriptor`: its `d` and
    /// `i` fields, `None` where lsof prints none.
    fn lsof_name(descriptor: std::os::fd::RawFd) -> (Option<String>, Option<String>) {
        let listed = std::process::Command::new("/usr/sbin/lsof")
            .args(["-a", "-p", &std::process::id().to_string()])
            .args(["-d", &descriptor.to_string(), "-F", "di"])
            .output()
            .expect("run lsof");
        let text = String::from_utf8(listed.stdout).expect("lsof prints ASCII");
        let field = |tag: char| {
            text.lines()
                .find_map(|line| line.strip_prefix(tag))
                .map(str::to_owned)
        };
        (field('d'), field('i'))
    }

    /// A pipe end, a Unix socket, a TCP listener and a regular file are each named exactly as
    /// lsof names them: by the kernel address in `d` for the pipe and the sockets, by the inode in
    /// `i` for the file, and a descriptor that is not open is closed.
    #[test]
    fn a_descriptor_is_named_as_lsof_names_it() {
        use std::os::fd::AsRawFd as _;

        let (read, write) = std::io::pipe().expect("pipe");
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let path = std::env::temp_dir().join(format!("cowshed-os-name-{}", std::process::id()));
        let file = std::fs::File::create(&path).expect("create a file");
        std::fs::remove_file(&path).expect("unlink the file; its descriptor keeps its inode");
        for descriptor in [
            read.as_raw_fd(),
            write.as_raw_fd(),
            socket.as_raw_fd(),
            listener.as_raw_fd(),
        ] {
            let DescriptorName::Device(address) = descriptor_name(descriptor).expect("read") else {
                panic!("descriptor {descriptor} is named by its address");
            };
            assert_eq!(
                lsof_name(descriptor),
                (Some(format!("0x{address:x}")), None),
                "descriptor {descriptor}"
            );
        }
        let DescriptorName::Inode(inode) = descriptor_name(file.as_raw_fd()).expect("read") else {
            panic!("a regular file is named by its inode");
        };
        assert_eq!(lsof_name(file.as_raw_fd()).1, Some(inode.to_string()));
        let closed = write.as_raw_fd();
        drop(write);
        assert_eq!(
            descriptor_name(closed).expect("read"),
            DescriptorName::Closed
        );
    }

    fn procargs(argc: i32, strings: &[u8]) -> Vec<u8> {
        [&argc.to_ne_bytes()[..], strings].concat()
    }

    fn arguments(executable: &[u8], argv: &[&[u8]]) -> ProcessArguments {
        ProcessArguments {
            executable: executable.to_vec(),
            argv: argv.iter().map(|argument| argument.to_vec()).collect(),
        }
    }

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
    #[test]
    fn procargs_refuse_the_saved_path_stand_in() {
        for path in [&b"/bin/cat"[..], b"/bin////////ls", b"/bin/////////ls"] {
            let saved = procargs(1, &[path, b"\0\0", path, b"\0"].concat());
            assert_eq!(parse_procargs(&saved), Err(UnreadableProcargs::SavedPath));
        }
    }

    /// A process exec'd with an empty `argv[0]` reads back with it, whatever padding its path
    /// length leaves.
    #[test]
    fn a_running_process_s_empty_leading_argument_is_read_back() {
        use std::io::{BufRead, Write};
        use std::os::unix::process::CommandExt;
        for path in ["/bin/cat", "/bin////////cat", "/bin/////////cat"] {
            let mut child = crate::test_support::SpawnReaped::spawn_reaped(
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
}
