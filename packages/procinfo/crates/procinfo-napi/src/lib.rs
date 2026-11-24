//! The N-API face of procinfo (specs/procinfo/01_procinfo.md, "JavaScript"): each export reads one
//! fact through the procinfo crate and answers it as plain JavaScript values. A pid or a process
//! group that names no process is refused as `usage`; a kernel record that cannot be read is
//! refused as `environment-missing`; each refusal carries its message and a `hint`.

use std::io;

use napi::bindgen_prelude::{BigInt, Buffer};
use napi::{Env, JsError};
use napi_derive::napi;

/// A refusal JavaScript reads by its `code`, with a `hint` that says what to do next.
fn refusal(env: Env, code: &str, message: String, hint: &str) -> napi::Error {
    let build = move || -> napi::Result<napi::Error> {
        let mut error = JsError::from(napi::Error::new(code, message))
            .into_unknown(env)
            .coerce_to_object()?;
        error.set_named_property("hint", env.create_string(hint)?)?;
        Ok(napi::Error::from(error.into_unknown()))
    };
    match build() {
        Ok(error) | Err(error) => error,
    }
}

/// `pid` as the kernel's process id, or a `usage` refusal for a number that names no process.
fn process_id(env: Env, pid: i32, what: &str) -> napi::Result<i32> {
    if pid < 1 {
        return Err(refusal(
            env,
            "usage",
            format!("{pid} is not a {what}"),
            "pass a positive id",
        ));
    }
    Ok(pid)
}

/// A kernel record that could not be read, as an `environment-missing` refusal.
fn unread(env: Env, what: String, error: &io::Error) -> napi::Error {
    refusal(
        env,
        "environment-missing",
        format!("cannot read {what}: {error}"),
        "read it again; the process may be changing state",
    )
}

/// Milliseconds since this process first asked, on a clock that stops while the host sleeps.
#[napi(js_name = "awakeTimeMs")]
pub fn awake_time_ms() -> f64 {
    procinfo::awake_time_ms()
}

/// Where a process descends from: its parent, its group, and whether it has exited unreaped.
#[napi(object)]
pub struct ProcessLineage {
    pub pid: i32,
    pub ppid: i32,
    pub pgid: i32,
    pub zombie: bool,
}

/// The lineage of each listed pid a process holds, in the order asked; a pid none holds is left
/// out, as `ps -p` leaves it out. Each pid is read for itself.
#[napi(js_name = "processLineages")]
pub fn process_lineages(env: Env, pids: Vec<i32>) -> napi::Result<Vec<ProcessLineage>> {
    let mut lineages = Vec::with_capacity(pids.len());
    for pid in pids {
        let pid = process_id(env, pid, "process id")?;
        match procinfo::lineage(pid) {
            Ok(Some(read)) => lineages.push(ProcessLineage {
                pid: read.pid,
                ppid: read.ppid,
                pgid: read.pgid,
                zombie: read.zombie,
            }),
            Ok(None) => {}
            Err(error) => return Err(unread(env, format!("the lineage of process {pid}"), &error)),
        }
    }
    Ok(lineages)
}

/// Whether the process `pid` names has not exited: a zombie has.
#[napi]
pub fn running(env: Env, pid: i32) -> napi::Result<bool> {
    let pid = process_id(env, pid, "process id")?;
    procinfo::running(pid)
        .map_err(|error| unread(env, format!("whether process {pid} runs"), &error))
}

/// What the kernel shows a process doing, as `ps` names its state; `reason` says why an
/// `unread` state could not be read.
#[napi(object)]
pub struct RunState {
    pub state: String,
    pub reason: Option<String>,
}

impl From<procinfo::RunState> for RunState {
    fn from(state: procinfo::RunState) -> Self {
        let (state, reason) = match state {
            procinfo::RunState::Running => ("running", None),
            procinfo::RunState::Sleeping => ("sleeping", None),
            procinfo::RunState::Uninterruptible => ("uninterruptible", None),
            procinfo::RunState::Stopped => ("stopped", None),
            procinfo::RunState::Zombie => ("zombie", None),
            procinfo::RunState::Gone => ("gone", None),
            procinfo::RunState::Unread(why) => ("unread", Some(why)),
        };
        Self {
            state: state.to_owned(),
            reason,
        }
    }
}

/// The state the kernel shows the process `pid` in.
#[napi(js_name = "runState")]
pub fn run_state(env: Env, pid: i32) -> napi::Result<RunState> {
    let pid = process_id(env, pid, "process id")?;
    Ok(procinfo::run_state(pid).into())
}

/// A process's executable path and its byte-exact argument vector, as the kernel records them;
/// `nameOnly` when the vector could not be read within its bound and `argv` is the command name.
#[napi(object)]
pub struct ProcessArguments {
    pub executable: Buffer,
    pub argv: Vec<Buffer>,
    pub name_only: bool,
}

/// The executable and arguments of the process `pid`, byte for byte.
#[napi(js_name = "processArguments")]
pub fn process_arguments(env: Env, pid: i32) -> napi::Result<ProcessArguments> {
    let pid = process_id(env, pid, "process id")?;
    let read = procinfo::process_arguments(pid)
        .map_err(|error| unread(env, format!("the arguments of process {pid}"), &error))?;
    Ok(ProcessArguments {
        executable: read.executable.into(),
        argv: read.argv.into_iter().map(Buffer::from).collect(),
        name_only: read.name_only,
    })
}

#[cfg(target_os = "macos")]
/// The pids holding the process group id `pgid`, exited processes included. Darwin alone lists a
/// group's members (`proc_listpgrppids`); Linux would need a census of `/proc`.
#[napi(js_name = "groupPids")]
pub fn group_pids(env: Env, pgid: i32) -> napi::Result<Vec<i32>> {
    let pgid = process_id(env, pgid, "process group id")?;
    procinfo::group_pids(pgid)
        .map_err(|error| unread(env, format!("the processes of group {pgid}"), &error))
}

#[cfg(target_os = "macos")]
/// One running process of a group: its pid, its command line and what the kernel shows it doing.
#[napi(object)]
pub struct GroupMember {
    pub pid: i32,
    pub command: String,
    pub state: RunState,
}

#[cfg(target_os = "macos")]
/// The running processes of group `pgid`, each named by its command line and its state.
#[napi(js_name = "groupMembers")]
pub fn group_members(env: Env, pgid: i32) -> napi::Result<Vec<GroupMember>> {
    let pgid = process_id(env, pgid, "process group id")?;
    let members = procinfo::group_members(pgid)
        .map_err(|error| unread(env, format!("the members of group {pgid}"), &error))?;
    Ok(members
        .into_iter()
        .map(|member| GroupMember {
            pid: member.pid,
            command: member.command,
            state: member.state.into(),
        })
        .collect())
}

#[cfg(target_os = "macos")]
/// The CPU the processes of group `pgid` have used, with their reaped children's, in the kernel's
/// own unit (Mach ticks): comparable only to another reading of
/// the same group.
#[napi(js_name = "groupCpu")]
pub fn group_cpu(env: Env, pgid: i32) -> napi::Result<BigInt> {
    let pgid = process_id(env, pgid, "process group id")?;
    procinfo::group_cpu(pgid)
        .map(BigInt::from)
        .map_err(|error| unread(env, format!("the CPU of group {pgid}"), &error))
}

/// The bytes the process `pid` holds resident now, or `null` once no such process runs.
#[napi(js_name = "residentBytes")]
pub fn resident_bytes(env: Env, pid: i32) -> napi::Result<Option<BigInt>> {
    let pid = process_id(env, pid, "process id")?;
    procinfo::resident_bytes(pid)
        .map(|bytes| bytes.map(BigInt::from))
        .map_err(|error| unread(env, format!("the resident memory of process {pid}"), &error))
}

/// The pids whose parent is the process `pid`, as the kernel links them now; none for a pid no
/// process holds.
#[napi(js_name = "childPids")]
pub fn child_pids(env: Env, pid: i32) -> napi::Result<Vec<i32>> {
    let pid = process_id(env, pid, "process id")?;
    procinfo::child_pids(pid)
        .map_err(|error| unread(env, format!("the children of process {pid}"), &error))
}

/// The CPU time the process `pid` has used, user and system, in nanoseconds; `null` once it has
/// exited or no process holds the pid.
#[napi(js_name = "cpuTimeNs")]
pub fn cpu_time_ns(env: Env, pid: i32) -> napi::Result<Option<BigInt>> {
    let pid = process_id(env, pid, "process id")?;
    procinfo::cpu_time(pid)
        .map(|time| time.map(|time| BigInt::from(time.as_nanos())))
        .map_err(|error| unread(env, format!("the CPU time of process {pid}"), &error))
}

/// When the process `pid` started, in microseconds since the Unix epoch; `null` once no process
/// holds the pid.
#[napi(js_name = "startedUs")]
pub fn started_us(env: Env, pid: i32) -> napi::Result<Option<i64>> {
    let pid = process_id(env, pid, "process id")?;
    let started = procinfo::started(pid)
        .map_err(|error| unread(env, format!("the start of process {pid}"), &error))?;
    started
        .map(|started| {
            started
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(io::Error::other)
                .and_then(|since| i64::try_from(since.as_micros()).map_err(io::Error::other))
                .map_err(|error| unread(env, format!("the start of process {pid}"), &error))
        })
        .transpose()
}
/// The TCP ports the process `pid` listens on, IPv4 and IPv6 alike, each once, ascending.
#[napi(js_name = "listeningPorts")]
pub fn listening_ports(env: Env, pid: i32) -> napi::Result<Vec<u32>> {
    let pid = process_id(env, pid, "process id")?;
    procinfo::listening_ports(pid)
        .map(|ports| ports.into_iter().map(u32::from).collect())
        .map_err(|error| unread(env, format!("the listening ports of process {pid}"), &error))
}

/// What `lsof -F di` names a descriptor by: `kind` is `device` (the kernel address of a pipe or
/// socket, Darwin), `inode`, `closed`, or `unnamed` (a Darwin kind neither field names, by its
/// `PROX_FDTYPE_*` number in `fdType`).
#[napi(object)]
pub struct DescriptorName {
    pub kind: String,
    pub value: Option<BigInt>,
    pub fd_type: Option<u32>,
}

impl From<procinfo::DescriptorName> for DescriptorName {
    fn from(name: procinfo::DescriptorName) -> Self {
        let (kind, value, fd_type) = match name {
            procinfo::DescriptorName::Device(address) => {
                ("device", Some(BigInt::from(address)), None)
            }
            procinfo::DescriptorName::Inode(inode) => ("inode", Some(BigInt::from(inode)), None),
            procinfo::DescriptorName::Closed => ("closed", None, None),
            procinfo::DescriptorName::Unnamed { fd_type } => ("unnamed", None, Some(fd_type)),
        };
        Self {
            kind: kind.to_owned(),
            value,
            fd_type,
        }
    }
}

/// The name of this process's own descriptor `fd`.
#[napi(js_name = "descriptorName")]
pub fn descriptor_name(env: Env, fd: i32) -> napi::Result<DescriptorName> {
    if fd < 0 {
        return Err(refusal(
            env,
            "usage",
            format!("{fd} is not a file descriptor"),
            "pass a descriptor of this process, 0 or above",
        ));
    }
    procinfo::descriptor_name(fd)
        .map(DescriptorName::from)
        .map_err(|error| unread(env, format!("descriptor {fd} of this process"), &error))
}

/// One descriptor of another process and its name.
#[napi(object)]
pub struct NamedDescriptor {
    pub fd: i32,
    pub name: DescriptorName,
}

/// Another process's descriptors, or why they were not read: `unread` is `gone` (the process
/// exited, or none has the pid) or `withheld` (the kernel withholds them from this user).
#[napi(object)]
pub struct DescriptorNames {
    pub descriptors: Vec<NamedDescriptor>,
    pub unread: Option<String>,
}

/// The names of the open descriptors of the process `pid`. `devices` asks for pipes and sockets
/// (named by address on Darwin), `inodes` for everything named by an inode.
#[napi(js_name = "descriptorNames")]
pub fn descriptor_names(
    env: Env,
    pid: i32,
    devices: bool,
    inodes: bool,
) -> napi::Result<DescriptorNames> {
    let pid = process_id(env, pid, "process id")?;
    let read = procinfo::descriptor_names(pid, procinfo::NameKinds { devices, inodes })
        .map_err(|error| unread(env, format!("the descriptors of process {pid}"), &error))?;
    Ok(match read {
        Ok(named) => DescriptorNames {
            descriptors: named
                .into_iter()
                .map(|(fd, name)| NamedDescriptor {
                    fd,
                    name: name.into(),
                })
                .collect(),
            unread: None,
        },
        Err(why) => DescriptorNames {
            descriptors: Vec::new(),
            unread: Some(
                match why {
                    procinfo::Unread::Gone => "gone",
                    procinfo::Unread::Withheld => "withheld",
                }
                .to_owned(),
            ),
        },
    })
}

/// One lock the kernel records on a file: the process it names (`null` where it names none), its
/// kind (`null` on Darwin), whether it is exclusive, and whether that process waits for it.
#[napi(object)]
pub struct FileLock {
    pub pid: Option<i32>,
    pub kind: Option<String>,
    pub exclusive: bool,
    pub waiting: bool,
}

/// What the kernel's lock record says of a file: `state` is `unlocked`, `locked` (with `locks`)
/// or `unread` (with `reason`).
#[napi(object)]
pub struct LockHolders {
    pub state: String,
    pub locks: Vec<FileLock>,
    pub reason: Option<String>,
}

/// Who holds the locks of the file at `path`, as the kernel's lock record names them.
#[napi(js_name = "lockHolders")]
pub fn lock_holders(env: Env, path: String) -> napi::Result<LockHolders> {
    let read = procinfo::lock_holders(std::path::Path::new(&path))
        .map_err(|error| unread(env, format!("the locks of {path}"), &error))?;
    Ok(match read {
        procinfo::LockHolders::Unlocked => LockHolders {
            state: "unlocked".to_owned(),
            locks: Vec::new(),
            reason: None,
        },
        procinfo::LockHolders::Locked(locks) => LockHolders {
            state: "locked".to_owned(),
            locks: locks
                .into_iter()
                .map(|lock| FileLock {
                    pid: lock.holder,
                    kind: lock.kind.map(|kind| match kind {
                        procinfo::LockKind::Flock => "flock".to_owned(),
                        procinfo::LockKind::Posix => "posix".to_owned(),
                        procinfo::LockKind::OpenFileDescription => "ofd".to_owned(),
                        procinfo::LockKind::Lease => "lease".to_owned(),
                        procinfo::LockKind::Other(name) => name.to_lowercase(),
                    }),
                    exclusive: lock.exclusive,
                    waiting: lock.waiting,
                })
                .collect(),
            reason: None,
        },
        procinfo::LockHolders::Unread(reason) => LockHolders {
            state: "unread".to_owned(),
            locks: Vec::new(),
            reason: Some(reason),
        },
    })
}
/// Whether the process `pid` may run sandboxed; `false` proves a live, unsandboxed process.
#[napi]
pub fn sandboxed(env: Env, pid: i32) -> napi::Result<bool> {
    let pid = process_id(env, pid, "process id")?;
    procinfo::sandboxed(pid)
        .map_err(|error| unread(env, format!("the sandbox of process {pid}"), &error))
}

/// `getloadavg(3)`: the average run-queue length over the last 1, 5 and 15 minutes.
#[napi(object)]
pub struct HostLoad {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

/// The host's load now, or `null` when the kernel will not report all three averages.
#[napi(js_name = "hostLoad")]
pub fn host_load() -> Option<HostLoad> {
    procinfo::HostLoad::read().map(|load| HostLoad {
        one: load.one,
        five: load.five,
        fifteen: load.fifteen,
    })
}
