//! Linux's readings of a process: procfs (`/proc/<pid>/stat`, `status`, `statm`, `exe`,
//! `cmdline`, `fd` and `net/tcp`).

use std::collections::BTreeSet;
use std::io;

use super::{
    DescriptorName, GroupMember, NameKinds, ProcessArguments, ProcessLineage, RunState, Unread,
};
/// [`running`] from procfs: a pid without a stat has been reaped, and state `Z` (zombie) or `X`
/// (dead) has exited.
pub fn running(pid: libc::pid_t) -> io::Result<bool> {
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

/// [`sandboxed`] from procfs: `false` only for a live process whose `/proc/<pid>/status` shows
/// `NoNewPrivs: 0` and no seccomp mode. A process without `CAP_SYS_ADMIN` confines itself only
/// once it has set `no_new_privs`: `landlock_restrict_self(2)` and a seccomp filter are both
/// refused until then, and the bit is never cleared and passes to every child and across every
/// exec. So that reading proves no Landlock domain and no filter of an unprivileged launcher's.
/// The kernel shows no process's Landlock domain to another, so the set bit is all that can be
/// read of one: it answers `true`, may be sandboxed, as do a seccomp mode, an exited process and
/// a pid that names none.
pub fn sandboxed(pid: libc::pid_t) -> io::Result<bool> {
    let path = format!("/proc/{pid}/status");
    let status = match std::fs::read(&path) {
        Ok(status) => status,
        // procfs answers ESRCH for a task that exited after its status file was opened.
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Ok(true);
        }
        Err(error) => return Err(io::Error::new(error.kind(), format!("{path}: {error}"))),
    };
    may_be_sandboxed(&status)
        .map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, format!("{path} {reason}")))
}

/// What a `/proc/<pid>/status` shows of [`sandboxed`], or why it shows nothing.
fn may_be_sandboxed(status: &[u8]) -> Result<bool, String> {
    let unreadable = |field: &str, value: Option<&[u8]>| {
        format!("has {field} {:?}", value.map(String::from_utf8_lossy))
    };
    match status_field(status, "State") {
        // Exited: the lines below say what it had, and prove no live process.
        Some([b'Z' | b'X', ..]) => return Ok(true),
        Some(_) => {}
        None => return Err(unreadable("State", None)),
    }
    let no_new_privs = match status_field(status, "NoNewPrivs") {
        Some(b"0") => false,
        Some(b"1") => true,
        value => return Err(unreadable("NoNewPrivs", value)),
    };
    let seccomp = match status_field(status, "Seccomp") {
        // A kernel built without seccomp prints no such line, and filters no process.
        None | Some(b"0") => false,
        // `SECCOMP_MODE_STRICT` and `SECCOMP_MODE_FILTER`.
        Some(b"1" | b"2") => true,
        value => return Err(unreadable("Seccomp", value)),
    };
    Ok(no_new_privs || seccomp)
}

/// The value of the `<field>:\t<value>` line of a `/proc/<pid>/status`. Its first line carries
/// the command name, any bytes, with each newline escaped (`proc_task_name`): the file is read
/// as bytes, and no name passes for another line.
fn status_field<'a>(status: &'a [u8], field: &str) -> Option<&'a [u8]> {
    status.split(|byte| *byte == b'\n').find_map(|line| {
        line.strip_prefix(field.as_bytes())?
            .strip_prefix(b":")
            .map(<[u8]>::trim_ascii)
    })
}

/// [`process_arguments`] from procfs: the executable is the image the kernel records
/// (`/proc/<pid>/exe`: the resolved path of the file exec'd, with the kernel's ` (deleted)` after
/// one since unlinked), and the argument vector is `/proc/<pid>/cmdline`. Fails for a process
/// that has exited, reaped or not, which has neither, and for one whose image this user may not
/// read (another user's, or one that made itself undumpable).
pub fn process_arguments(pid: libc::pid_t) -> io::Result<ProcessArguments> {
    use std::os::unix::ffi::OsStringExt as _;

    let unreadable = |what: &str, error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("the {what} of process {pid} cannot be read: {error}"),
        )
    };
    let image = || {
        std::fs::read_link(format!("/proc/{pid}/exe"))
            .map_err(|error| unreadable("executable", error))
    };
    let mut executable = image()?;
    loop {
        let line = std::fs::read(format!("/proc/{pid}/cmdline"))
            .map_err(|error| unreadable("arguments", error))?;
        // Two files are two moments. An exec between them pairs one image with another's
        // arguments, and an exit leaves an empty line: the image read once more tells either.
        let after = image()?;
        if after == executable {
            return Ok(ProcessArguments {
                executable: executable.into_os_string().into_vec(),
                argv: arguments(&line),
            });
        }
        executable = after;
    }
}

/// The arguments of a `cmdline`: each ends at its NUL, so an empty one is an argument. A line
/// that ends in no NUL is one its process wrote over (`setproctitle`), and its last piece is an
/// argument as well.
fn arguments(line: &[u8]) -> Vec<Vec<u8>> {
    if line.is_empty() {
        return Vec::new();
    }
    line.strip_suffix(b"\0")
        .unwrap_or(line)
        .split(|byte| *byte == 0)
        .map(<[u8]>::to_vec)
        .collect()
}

/// [`run_state`] from procfs: the state letter after the command name's closing parenthesis.
pub fn run_state(pid: libc::pid_t) -> RunState {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return RunState::Gone,
        Err(error) => return RunState::Unread(error.to_string()),
    };
    match stat
        .rfind(')')
        .and_then(|closing| stat[closing + 1..].split_whitespace().next())
    {
        Some("R") => RunState::Running,
        Some("S" | "I") => RunState::Sleeping,
        Some("D") => RunState::Uninterruptible,
        Some("T" | "t") => RunState::Stopped,
        Some("Z" | "X") => RunState::Zombie,
        other => RunState::Unread(format!("process stat state {other:?}")),
    }
}

/// [`lineage`] from procfs: the state, parent and group fields of `/proc/<pid>/stat`
/// ([`stat_lineage`]).
pub fn lineage(pid: libc::pid_t) -> io::Result<Option<ProcessLineage>> {
    Ok(stat_lineage(pid)?.map(|(lineage, _)| lineage))
}

/// The [`lineage`] of `pid` with the fields of the `/proc/<pid>/stat` line it was read from, after
/// the command name, the state first; `None` when no process holds `pid`. For a reader that needs
/// more of the line than the lineage: cowshed's Linux process-group CPU reads its times.
pub fn stat_lineage(pid: libc::pid_t) -> io::Result<Option<(ProcessLineage, String)>> {
    let Some(fields) = stat_fields(pid)? else {
        return Ok(None);
    };
    let lineage =
        lineage_of(pid, &fields).ok_or_else(|| not_a_stat_line(pid, fields.as_bytes()))?;
    Ok(Some((lineage, fields)))
}

/// The fields of `/proc/<pid>/stat` after the command name, the state first; `None` when no
/// process holds `pid`.
fn stat_fields(pid: libc::pid_t) -> io::Result<Option<String>> {
    let path = format!("/proc/{pid}/stat");
    let stat = match std::fs::read(&path) {
        Ok(stat) => stat,
        // procfs answers ESRCH for a task that exited after its stat file was opened.
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(io::Error::new(error.kind(), format!("{path}: {error}"))),
    };
    after_command(stat)
        .map(Some)
        .map_err(|stat| not_a_stat_line(pid, &stat))
}

/// The fields of a stat line after its command name, or the line when it has none. The name is
/// the process's own bytes, any bytes, emitted unescaped between parentheses, so the line is
/// read as bytes and only the ASCII fields after its last `)` are decoded.
fn after_command(mut stat: Vec<u8>) -> Result<String, Vec<u8>> {
    let Some(closing) = stat.iter().rposition(|byte| *byte == b')') else {
        return Err(stat);
    };
    stat.drain(..=closing);
    String::from_utf8(stat).map_err(std::string::FromUtf8Error::into_bytes)
}

/// The lineage the fields of a stat line hold ([`stat_fields`]): the state, the parent and the
/// group, in that order.
fn lineage_of(pid: libc::pid_t, fields: &str) -> Option<ProcessLineage> {
    let mut fields = fields.split_ascii_whitespace();
    let state = fields.next()?;
    let ppid = fields.next()?.parse().ok()?;
    let pgid = fields.next()?.parse().ok()?;
    Some(ProcessLineage {
        pid,
        ppid,
        pgid,
        zombie: matches!(state, "Z" | "X"),
    })
}

fn not_a_stat_line(pid: libc::pid_t, read: &[u8]) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "/proc/{pid}/stat is not a process stat line: {:?}",
            String::from_utf8_lossy(read)
        ),
    )
}

/// The running processes among `pids`, each named by its command line (`cmdline`) and its state.
pub fn running_members(pids: Vec<libc::pid_t>) -> Vec<GroupMember> {
    pids.into_iter()
        .filter_map(|pid| {
            let state = run_state(pid);
            if matches!(state, RunState::Gone | RunState::Zombie) {
                return None;
            }
            let command = std::fs::read(format!("/proc/{pid}/cmdline")).map_or_else(
                |error| format!("command unread: {error}"),
                |line| {
                    line.split(|byte| *byte == 0)
                        .filter(|argument| !argument.is_empty())
                        .map(String::from_utf8_lossy)
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
        .collect()
}

/// [`resident_bytes`] from procfs: `statm`'s resident pages times the page size. A pid without
/// a `statm` (`ENOENT`), or one reaped while it is read (`ESRCH`), names no process; an exited,
/// unreaped one has no memory left to count, and its `statm` reads zero pages.
pub fn resident_bytes(pid: libc::pid_t) -> io::Result<Option<u64>> {
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

/// [`listening_ports`] from procfs: the socket inodes of `/proc/<pid>/fd`, matched against the
/// `LISTEN` rows of `/proc/<pid>/net/tcp` and `tcp6`, the tables of the network namespace the
/// process is in.
///
/// Those tables list only that namespace's sockets, so this answer is complete because a job
/// lives in one namespace: its workspace's private network namespace, which its processes can
/// neither leave nor exchange for another (`unshare`/`setns` are refused, 04_sandbox.md, Linux
/// "Egress"). A process that held a socket made in another namespace would have it missing here.
pub fn listening_ports(pid: libc::pid_t) -> io::Result<BTreeSet<u16>> {
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
fn socket_inode(target: &[u8]) -> Option<u64> {
    let inode = target.strip_prefix(b"socket:[")?.strip_suffix(b"]")?;
    std::str::from_utf8(inode).ok()?.parse().ok()
}

/// [`descriptor_name`] on Linux: every descriptor is named by the inode `fstat(2)` reports, a
/// pipe's and a socket's included.
pub fn descriptor_name(descriptor: std::os::fd::RawFd) -> io::Result<DescriptorName> {
    Ok(inode(descriptor)?.map_or(DescriptorName::Closed, DescriptorName::Inode))
}

/// [`descriptor_names`] on Linux, from procfs: each `/proc/<pid>/fd` entry followed to its file,
/// whose inode is the one `fstat(2)` in that process reports.
pub fn descriptor_names(
    pid: libc::pid_t,
    kinds: NameKinds,
) -> io::Result<Result<Vec<(std::os::fd::RawFd, DescriptorName)>, Unread>> {
    use std::os::unix::fs::MetadataExt as _;

    let unread = |error: io::Error| match error.kind() {
        io::ErrorKind::NotFound => Ok(Err(Unread::Gone)),
        io::ErrorKind::PermissionDenied => Ok(Err(Unread::Withheld)),
        _ => Err(io::Error::new(
            error.kind(),
            format!("the descriptors of process {pid} cannot be read: {error}"),
        )),
    };
    if !kinds.inodes {
        return Ok(Ok(Vec::new()));
    }
    let entries = match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => entries,
        Err(error) => return unread(error),
    };
    let mut named = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => return unread(error),
        };
        let number = entry.file_name();
        let descriptor = number
            .to_str()
            .and_then(|number| number.parse().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("/proc/{pid}/fd lists {number:?}, which is no descriptor number"),
                )
            })?;
        match std::fs::metadata(entry.path()) {
            Ok(file) => named.push((descriptor, DescriptorName::Inode(file.ino()))),
            // Closed since the directory was read: it names nothing now.
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return unread(error),
        }
    }
    named.sort_unstable_by_key(|(descriptor, _)| *descriptor);
    Ok(Ok(named))
}

/// The inode `fstat(2)` reports for `descriptor`; `None` when it is not open.
fn inode(descriptor: std::os::fd::RawFd) -> io::Result<Option<u64>> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `status` is writable storage for one `stat`.
    if unsafe { libc::fstat(descriptor, status.as_mut_ptr()) } == -1 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::EBADF) => Ok(None),
            _ => Err(io::Error::new(
                error.kind(),
                format!("descriptor {descriptor} cannot be read: {error}"),
            )),
        };
    }
    // SAFETY: fstat succeeded, so it filled `status`.
    Ok(Some(unsafe { status.assume_init() }.st_ino))
}

/// Adds to `ports` the local port of each row of a procfs TCP table (`net/tcp`, `net/tcp6`) in
/// state `LISTEN` whose socket inode is one of `sockets`. Each row is `sl local rem st
/// tx_queue:rx_queue tr:tm->when retrnsmt uid timeout inode …`, an address being
/// `<hex address>:<hex port>` (`tcp4_seq_show`, `tcp6_seq_show`); a row of another shape is
/// refused, never skipped.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::SpawnReaped as _;
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

    /// A pipe end and a socket are named by the inode procfs shows for them, the field lsof
    /// prints as `i`, and a descriptor that is not open is closed.
    #[test]
    fn a_descriptor_is_named_by_the_inode_procfs_shows() {
        use std::os::fd::AsRawFd as _;

        let (read, write) = std::io::pipe().expect("pipe");
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        for (descriptor, kind) in [
            (read.as_raw_fd(), "pipe"),
            (write.as_raw_fd(), "pipe"),
            (socket.as_raw_fd(), "socket"),
        ] {
            let link = std::fs::read_link(format!("/proc/self/fd/{descriptor}")).expect("link");
            assert_eq!(
                descriptor_name(descriptor).expect("read"),
                DescriptorName::Inode(
                    link.to_str()
                        .and_then(|link| link.strip_prefix(kind))
                        .and_then(|link| link.strip_prefix(":["))
                        .and_then(|link| link.strip_suffix(']'))
                        .and_then(|inode| inode.parse().ok())
                        .expect("procfs names the inode")
                ),
                "descriptor {descriptor}"
            );
        }
        let closed = write.as_raw_fd();
        drop(write);
        assert_eq!(
            descriptor_name(closed).expect("read"),
            DescriptorName::Closed
        );
    }

    fn argv(arguments: &[&[u8]]) -> Vec<Vec<u8>> {
        arguments.iter().map(|argument| argument.to_vec()).collect()
    }

    /// A process may be sandboxed by its `no_new_privs` bit or by a seccomp mode, and always once
    /// it has exited; a kernel without seccomp filters nothing; a command name that spells a
    /// field is no field; and a status without the lines read is refused rather than taken as
    /// unsandboxed.
    #[test]
    fn a_status_shows_a_sandbox_by_no_new_privs_or_a_seccomp_mode() {
        let status = |state: &str, no_new_privs: &str, seccomp: &str| {
            format!(
                "Name:\tsh\nUmask:\t0022\nState:\t{state}\nTgid:\t7\nNoNewPrivs:\t{no_new_privs}\n\
                 Seccomp:\t{seccomp}\nSeccomp_filters:\t3\n"
            )
            .into_bytes()
        };
        let cases = [
            ("S (sleeping)", "0", "0", false),
            ("R (running)", "1", "0", true),
            ("S (sleeping)", "0", "1", true),
            ("S (sleeping)", "0", "2", true),
            ("S (sleeping)", "1", "2", true),
            ("Z (zombie)", "0", "0", true),
            ("X (dead)", "0", "0", true),
        ];
        for (state, no_new_privs, seccomp, expected) in cases {
            assert_eq!(
                may_be_sandboxed(&status(state, no_new_privs, seccomp)),
                Ok(expected),
                "{state}, NoNewPrivs {no_new_privs}, Seccomp {seccomp}"
            );
        }
        assert_eq!(
            may_be_sandboxed(b"State:\tS (sleeping)\nNoNewPrivs:\t0\n"),
            Ok(false)
        );
        assert_eq!(
            may_be_sandboxed(b"Name:\tNoNewPrivs:\t0\nState:\tS (sleeping)\nNoNewPrivs:\t1\n"),
            Ok(true)
        );
        for refused in [
            &b""[..],
            b"NoNewPrivs:\t0\nSeccomp:\t0\n",
            b"State:\tS (sleeping)\nSeccomp:\t0\n",
            b"State:\tS (sleeping)\nNoNewPrivs:\tyes\n",
            b"State:\tS (sleeping)\nNoNewPrivs:\t0\nSeccomp:\t9\n",
        ] {
            assert!(
                may_be_sandboxed(refused).is_err(),
                "{:?}",
                String::from_utf8_lossy(refused)
            );
        }
    }

    /// A child that set `no_new_privs` before its exec may be sandboxed; one that did not reads
    /// as this process does, whose bit and filters it inherits; and a reaped pid proves no live
    /// process.
    #[test]
    fn a_child_that_set_no_new_privs_may_be_sandboxed() {
        use std::os::unix::process::CommandExt as _;

        const SET: libc::c_ulong = 1;
        const UNUSED: libc::c_ulong = 0;
        let waiting = || {
            let mut command = std::process::Command::new("/bin/sh");
            command
                .args(["-c", "read _"])
                .stdin(std::process::Stdio::piped());
            command
        };
        let pid = |child: &std::process::Child| libc::pid_t::try_from(child.id()).expect("a pid");
        let mut plain = waiting().spawn_reaped().expect("spawn a child");
        let mut confined = waiting();
        // SAFETY: prctl is async-signal-safe and touches no memory of the forked child.
        unsafe {
            confined.pre_exec(|| {
                if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, SET, UNUSED, UNUSED, UNUSED) == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
        let mut confined = confined
            .spawn_reaped()
            .expect("spawn a child that sets no_new_privs");
        // Every state is read before any is asserted, so a failed assertion leaves no child.
        let own = sandboxed(libc::pid_t::try_from(std::process::id()).expect("a pid"));
        let unconfined = sandboxed(pid(&plain));
        let set = sandboxed(pid(&confined));
        drop(plain.stdin.take());
        drop(confined.stdin.take());
        plain.wait().expect("reap the child");
        confined.wait().expect("reap the confined child");
        let reaped = sandboxed(pid(&confined));
        assert_eq!(
            unconfined.expect("read the child"),
            own.expect("read this process")
        );
        assert!(set.expect("read the confined child"));
        assert!(reaped.expect("read the reaped pid"));
    }

    /// Each argument of a `cmdline` ends at its NUL, so an empty one is kept wherever it stands;
    /// no line is no argument; and a line its process wrote over is read to its end.
    #[test]
    fn a_cmdline_yields_each_argument_byte_for_byte() {
        assert_eq!(arguments(b""), argv(&[]));
        assert_eq!(arguments(b"\0"), argv(&[b""]));
        assert_eq!(arguments(b"\0-\0"), argv(&[b"", b"-"]));
        assert_eq!(
            arguments(b"node\0nx.js\0\0\xff\xfe\0"),
            argv(&[b"node", b"nx.js", b"", b"\xff\xfe"])
        );
        assert_eq!(arguments(b"nx daemon"), argv(&[b"nx daemon"]));
    }

    /// A running process's arguments read back byte for byte, an empty one included, beside the
    /// resolved path of the file it executes; reaped, it has neither.
    #[test]
    fn a_running_process_s_arguments_and_image_are_read_back() {
        use std::io::BufRead as _;
        use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};

        let mut child = crate::test_support::SpawnReaped::spawn_reaped(
            std::process::Command::new("/bin/sh")
                .args(["-c", "echo x; read _", ""])
                .arg(std::ffi::OsStr::from_bytes(b"\xff\xfe"))
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped()),
        )
        .expect("spawn");
        // An echoed line proves sh runs: its exec is complete.
        let mut line = String::new();
        let said = std::io::BufReader::new(child.stdout.take().expect("piped stdout"))
            .read_line(&mut line);
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        let read = process_arguments(pid);
        drop(child.stdin.take());
        child.wait().expect("reap the child");
        let reaped = process_arguments(pid);
        said.expect("read the child's line");
        assert_eq!(line, "x\n");
        assert_eq!(
            read.expect("read the running child"),
            ProcessArguments {
                executable: std::fs::canonicalize("/bin/sh")
                    .expect("resolve /bin/sh")
                    .into_os_string()
                    .into_vec(),
                argv: argv(&[b"/bin/sh", b"-c", b"echo x; read _", b"", b"\xff\xfe"]),
            }
        );
        assert!(reaped.is_err(), "{reaped:?}");
    }

    /// A stat line is read after the last `)` of its command name, whatever the name holds: a
    /// name of spaces, parentheses, a state letter and bytes that are no text hides no field. A
    /// line with no name, or with too few fields, yields none.
    #[test]
    fn a_stat_line_is_read_after_its_command_name() {
        let line = b"7 (a b) R 1 1 (\xff) S 3 9 9 0 -1 4194304 10 20 0 0 5 6 7 8 20 0 1 0\n";
        let fields = after_command(line.to_vec()).expect("the fields after the name");
        assert_eq!(
            lineage_of(7, &fields),
            Some(ProcessLineage {
                pid: 7,
                ppid: 3,
                pgid: 9,
                zombie: false
            })
        );
        let exited = after_command(b"7 (sh) Z 3 9 9".to_vec()).expect("the fields after the name");
        assert_eq!(
            lineage_of(7, &exited),
            Some(ProcessLineage {
                pid: 7,
                ppid: 3,
                pgid: 9,
                zombie: true
            })
        );
        assert_eq!(
            after_command(b"7 sh S 3 9".to_vec()),
            Err(b"7 sh S 3 9".to_vec())
        );
        assert_eq!(lineage_of(7, " S 3"), None);
        assert_eq!(lineage_of(7, " S 3 group"), None);
    }
}
