//! A Linux job's own cgroup v2 (07_api.md, "Complete job accounting and observation
//! reconciliation"): created fresh for exactly one job, entered by the job's first process before
//! that process executes anything, inherited by every descendant, and removed only once the job
//! holds no process and its counters are final.
//!
//! # Authority
//!
//! Cgroups belong to the controller. It works only inside the cgroup it was started in, and only
//! when that cgroup was delegated to it -- the directory, `cgroup.procs` and
//! `cgroup.subtree_control` writable by its effective uid (cgroup-v2.rst, "Delegation"). The
//! controller moves itself into the leaf [`CONTROLLER_LEAF`], because a cgroup that distributes
//! controllers to children may hold no process of its own, then enables the accounting
//! controllers below. The tree under that root is:
//!
//! ```text
//! <delegated>/controller            the controller process itself
//! <delegated>/ws-<incarnation>/     one workspace incarnation's jobs
//! <delegated>/ws-<incarnation>/job-<id>
//! ```
//!
//! A job is never given a name or a path to choose: its first process enters its cgroup through a
//! write end of `cgroup.procs` the controller opened ([`Placement`]), close-on-exec from birth, so
//! the command that runs never holds it. Everything the job then starts is born in the same
//! cgroup.
//!
//! # Identity
//!
//! A job cgroup is named by its workspace incarnation and job id, and identified by its cgroup id
//! (the directory's inode number on cgroup2). Admission creates it exclusively, so a name that
//! already exists is refused rather than reused. A lookup -- a restarted controller finding the
//! job its predecessor admitted -- reaches only cgroups of its own incarnation and accepts one only
//! if it is still the cgroup that admission recorded.
//!
//! # Counters
//!
//! The job's totals are its cgroup's own: CPU from `cpu.stat`, charged memory from
//! `memory.current` and `memory.peak`, storage I/O from `io.stat`. They count every process that
//! ever ran in the job, so a descendant reaped before any observer looked still counts. A counter
//! the kernel does not provide is an error that names it, never a zero. Retirement first collects
//! the final counters, then removes the cgroup.
//!
//! # Kernel contract
//!
//! The supervisor requires Linux 6.5 for its peer pidfd. Membership termination additionally
//! requires `cgroup.kill` (Linux 5.14): admission opens it before a command can run, and refuses
//! a missing or unwritable interface rather than falling back to process groups. Empty membership
//! is proved by `cgroup.events`, read on the same descriptor watched with `poll(POLLPRI)`. After a
//! kill, that proof and the join of each signalled member's pidfd share one reap-class deadline
//! (`cowshed_os::deadline::REAP`): a member still alive past it fails the wait as
//! [`CgroupError::Unreaped`], named with its pid, image and kernel state, and is left as it is.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString, OsStr};
use std::fmt;
use std::fs;
use std::io::{self, Read as _, Seek as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use cowshed_os::deadline::{self, Bound, Deadline, Expired, Progress};
use cowshed_os::process::ExitWatch;

use crate::api::dto::JobId;
use crate::api::resources::{
    ChargedMemoryBytes, ChargedMemoryUsage, CpuMicros, ResourceUnitError, StorageIoBytes,
    StorageIoTotals,
};
use crate::metadata::WorkspaceIncarnation;
use crate::runtime::live_membership::{LiveMember, LiveMembership};

/// Where the unified cgroup hierarchy is mounted.
pub const CGROUP_MOUNT: &str = "/sys/fs/cgroup";

/// The leaf of the delegated cgroup that holds the controller process itself.
pub const CONTROLLER_LEAF: &str = "controller";

/// `CGROUP2_SUPER_MAGIC` from `include/uapi/linux/magic.h`. A 32-bit kernel constant: glibc types
/// `statfs.f_type` as signed `__fsword_t` and musl as `c_ulong`, so it is compared by value.
const CGROUP2_SUPER_MAGIC: u32 = 0x6367_7270;

const INCARNATION_PREFIX: &str = "ws-";
const JOB_PREFIX: &str = "job-";

/// A cgroup v2 controller a job's accounting reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Controller {
    Cpu,
    Memory,
    Io,
}

impl Controller {
    /// Every controller job accounting needs: `cpu.stat`, `memory.current`/`memory.peak` and
    /// `io.stat` exist only where these are enabled.
    pub const ACCOUNTING: [Self; 3] = [Self::Cpu, Self::Memory, Self::Io];

    /// The fixed request for [`Self::ACCOUNTING`], kept in step by the controller-request test.
    const ACCOUNTING_REQUEST: &str = "+cpu +memory +io";

    pub const fn name(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::Io => "io",
        }
    }
}

impl fmt::Display for Controller {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Why a cgroup operation failed, naming the call or the file and the cgroup it concerned.
#[derive(Debug, thiserror::Error)]
pub enum CgroupError {
    #[error("{call} {path}: {source}")]
    Io {
        call: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{path} is not a cgroup v2 mount (statfs f_type {f_type:#x})")]
    NotUnified { path: PathBuf, f_type: i128 },
    #[error("/proc/self/cgroup names no cgroup v2 membership: {content:?}")]
    NoUnifiedMembership { content: String },
    #[error(
        "cgroup {path} is not delegated to this controller: {file} is not writable by effective \
         uid {euid} ({source})"
    )]
    NotDelegated {
        path: PathBuf,
        file: &'static str,
        euid: u32,
        #[source]
        source: io::Error,
    },
    #[error(
        "cgroup {path} does not offer the {controller} controller (cgroup.controllers: {available:?})"
    )]
    ControllerUnavailable {
        path: PathBuf,
        controller: Controller,
        available: String,
    },
    #[error("job cgroup {path} already exists: a job's cgroup is created once and never reused")]
    AlreadyExists { path: PathBuf },
    #[error("{admission}; removing the refused job cgroup {path} also failed: {source}")]
    AdmissionCleanup {
        path: PathBuf,
        admission: Box<CgroupError>,
        #[source]
        source: io::Error,
    },
    #[error("job cgroup {path} does not exist")]
    Missing { path: PathBuf },
    #[error("job cgroup {path} is cgroup {found}, not cgroup {recorded} that admission recorded")]
    Replaced {
        path: PathBuf,
        recorded: u64,
        found: u64,
    },
    #[error("a job cgroup of incarnation {found} is not reachable from incarnation {fence}")]
    ForeignIncarnation {
        fence: WorkspaceIncarnation,
        found: WorkspaceIncarnation,
    },
    #[error(
        "job cgroup {path} is unavailable: job containment requires writable cgroup.kill (Linux 5.14+); the supervisor requires Linux 6.5+ for peer pidfds ({source})"
    )]
    KillUnavailable {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("job cgroup {path} still holds processes: its counters are not final")]
    Populated { path: PathBuf },
    /// The membership did not end within its reap bound: code TIMEOUT, terminal. The holder
    /// names every member still alive with its pid, image and kernel state.
    #[error("{0}")]
    Unreaped(Box<Expired<LiveMembership>>),
    #[error("{path}: {detail}")]
    Malformed { path: PathBuf, detail: String },
    #[error("{path} does not exist: this kernel or this cgroup's controllers do not provide it")]
    CounterUnavailable { path: PathBuf },
    #[error("{path} has no {counter} counter")]
    MissingCounter {
        path: PathBuf,
        counter: &'static str,
    },
    #[error("{path} {counter}: {source}")]
    OutOfRange {
        path: PathBuf,
        counter: &'static str,
        #[source]
        source: ResourceUnitError,
    },
}

type Result<T, E = CgroupError> = std::result::Result<T, E>;

/// The error of `call` on `path`, which is copied only once the call has failed.
fn failed(call: &'static str, path: &Path) -> impl FnOnce(io::Error) -> CgroupError {
    move |source| CgroupError::Io {
        call,
        path: path.to_path_buf(),
        source,
    }
}

/// The error of `call` on `file` in the cgroup `directory`, joined only once the call has
/// failed.
fn failed_in(
    call: &'static str,
    directory: &Path,
    file: &'static str,
) -> impl FnOnce(io::Error) -> CgroupError {
    move |source| CgroupError::Io {
        call,
        path: directory.join(file),
        source,
    }
}

fn c_path(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| CgroupError::Malformed {
        path: path.to_path_buf(),
        detail: "the path holds a NUL byte".to_owned(),
    })
}

/// Open `name` beneath the directory `at` with `flags`, close-on-exec from birth.
fn open_at(at: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: `name` is NUL-terminated; a non-negative answer is a new descriptor owned here.
    let fd = unsafe { libc::openat(at, name.as_ptr(), flags | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat just returned this descriptor and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// A cgroup interface file, read whole through the cgroup's directory descriptor.
fn read_at(directory: &OwnedFd, name: &CStr) -> io::Result<String> {
    let file = open_at(directory.as_raw_fd(), name, libc::O_RDONLY)?;
    io::read_to_string(fs::File::from(file))
}

/// The cgroup id of the cgroup whose directory `directory` is: its inode number on cgroup2.
fn cgroup_id(directory: &OwnedFd) -> io::Result<u64> {
    // SAFETY: an all-zero `stat` is a valid output buffer for fstat.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: a live descriptor and a buffer of the right type.
    if unsafe { libc::fstat(directory.as_raw_fd(), &mut stat) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(stat.st_ino)
}

fn open_directory(path: &Path) -> Result<OwnedFd> {
    let name = c_path(path)?;
    open_at(libc::AT_FDCWD, &name, libc::O_RDONLY | libc::O_DIRECTORY).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            CgroupError::Missing {
                path: path.to_path_buf(),
            }
        } else {
            CgroupError::Io {
                call: "open",
                path: path.to_path_buf(),
                source,
            }
        }
    })
}

/// The cgroup v2 path in `/proc/self/cgroup` content: the `0::<path>` line.
fn unified_membership(content: &str) -> Result<&str> {
    content
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .filter(|path| path.starts_with('/'))
        .ok_or_else(|| CgroupError::NoUnifiedMembership {
            content: content.to_owned(),
        })
}

/// Every controller of `wanted` that `cgroup.controllers` content lists, or the first it lacks.
fn require_controllers(path: &Path, available: &str, wanted: &[Controller]) -> Result<()> {
    for controller in wanted {
        if !available
            .split_whitespace()
            .any(|name| name == controller.name())
        {
            return Err(CgroupError::ControllerUnavailable {
                path: path.to_path_buf(),
                controller: *controller,
                available: available.trim().to_owned(),
            });
        }
    }
    Ok(())
}

/// Whether `cgroup.events` content says the cgroup or a descendant holds a live process.
fn populated(path: &Path, events: &str) -> Result<bool> {
    let malformed = || CgroupError::Malformed {
        path: path.join("cgroup.events"),
        detail: format!("no populated field in {events:?}"),
    };
    match events
        .lines()
        .find_map(|line| line.strip_prefix("populated "))
        .ok_or_else(malformed)?
    {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(malformed()),
    }
}

/// Whether this process's effective uid may write `file` in `directory` (`""` for the
/// directory itself), as delegation requires.
fn writable(directory: &Path, file: &'static str) -> Result<()> {
    let target = if file.is_empty() {
        directory.to_path_buf()
    } else {
        directory.join(file)
    };
    let name = c_path(&target)?;
    // SAFETY: a NUL-terminated path and plain flags.
    let answer =
        unsafe { libc::faccessat(libc::AT_FDCWD, name.as_ptr(), libc::W_OK, libc::AT_EACCESS) };
    if answer == 0 {
        return Ok(());
    }
    let source = io::Error::last_os_error();
    Err(CgroupError::NotDelegated {
        path: directory.to_path_buf(),
        file: if file.is_empty() {
            "its directory"
        } else {
            file
        },
        // SAFETY: geteuid cannot fail.
        euid: unsafe { libc::geteuid() },
        source,
    })
}

/// Create the directory `path`; `Ok(false)` when it already existed.
fn make_directory(path: &Path) -> Result<bool> {
    match fs::create_dir(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(source) => Err(CgroupError::Io {
            call: "mkdir",
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Distribute the accounting controllers from `path` to its children.
fn enable_accounting(path: &Path) -> Result<()> {
    let available = fs::read_to_string(path.join("cgroup.controllers")).map_err(failed_in(
        "read",
        path,
        "cgroup.controllers",
    ))?;
    require_controllers(path, &available, &Controller::ACCOUNTING)?;
    let control = path.join("cgroup.subtree_control");
    fs::write(&control, Controller::ACCOUNTING_REQUEST).map_err(failed("write", &control))
}

/// The controller's authority over the cgroup subtree delegated to it.
#[derive(Debug)]
pub struct CgroupAuthority {
    root: PathBuf,
}

impl CgroupAuthority {
    /// Take authority over the delegated cgroup this process runs in: move this process into
    /// [`CONTROLLER_LEAF`] and enable the accounting controllers below the root. A process
    /// already in that leaf of a delegated cgroup keeps its place. A hierarchy that is not cgroup
    /// v2, a cgroup not delegated to this effective uid, or one lacking an accounting controller
    /// is refused before anything moves.
    pub fn delegated() -> Result<Self> {
        let mount = Path::new(CGROUP_MOUNT);
        let name = c_path(mount)?;
        // SAFETY: an all-zero `statfs` is a valid output buffer.
        let mut statfs: libc::statfs = unsafe { std::mem::zeroed() };
        // SAFETY: a NUL-terminated path and a buffer of the right type.
        if unsafe { libc::statfs(name.as_ptr(), &mut statfs) } != 0 {
            return Err(CgroupError::Io {
                call: "statfs",
                path: mount.to_path_buf(),
                source: io::Error::last_os_error(),
            });
        }
        if u32::try_from(statfs.f_type) != Ok(CGROUP2_SUPER_MAGIC) {
            return Err(CgroupError::NotUnified {
                path: mount.to_path_buf(),
                f_type: i128::from(statfs.f_type),
            });
        }
        let membership = fs::read_to_string("/proc/self/cgroup")
            .map_err(failed("read", Path::new("/proc/self/cgroup")))?;
        let own = mount.join(unified_membership(&membership)?.trim_start_matches('/'));
        let root = match own.file_name() {
            Some(leaf) if leaf == OsStr::new(CONTROLLER_LEAF) => own
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| own.clone()),
            _ => own,
        };
        Self::delegated_at(root)
    }

    fn delegated_at(root: PathBuf) -> Result<Self> {
        for file in ["", "cgroup.procs", "cgroup.subtree_control"] {
            writable(&root, file)?;
        }
        let available = fs::read_to_string(root.join("cgroup.controllers")).map_err(failed_in(
            "read",
            &root,
            "cgroup.controllers",
        ))?;
        require_controllers(&root, &available, &Controller::ACCOUNTING)?;
        let leaf = root.join(CONTROLLER_LEAF);
        make_directory(&leaf)?;
        let procs = leaf.join("cgroup.procs");
        // "0" names the writing process: all of its threads move together.
        fs::write(&procs, "0").map_err(failed("write", &procs))?;
        enable_accounting(&root)?;
        Ok(Self { root })
    }

    /// The delegated cgroup this authority governs.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The cgroups of one workspace incarnation's jobs, created on first use.
    pub fn incarnation(&self, incarnation: &WorkspaceIncarnation) -> Result<IncarnationCgroups> {
        let path = self
            .root
            .join(format!("{INCARNATION_PREFIX}{}", incarnation.as_str()));
        make_directory(&path)?;
        enable_accounting(&path)?;
        Ok(IncarnationCgroups {
            incarnation: incarnation.clone(),
            path,
        })
    }
}

/// One workspace incarnation's job cgroups: the fence every admission and lookup passes.
#[derive(Debug)]
pub struct IncarnationCgroups {
    incarnation: WorkspaceIncarnation,
    path: PathBuf,
}

/// What names one job's cgroup durably: its workspace incarnation, its job id, and the cgroup
/// id admission created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobCgroupIdentity {
    incarnation: WorkspaceIncarnation,
    job_id: JobId,
    cgroup_id: u64,
}

impl JobCgroupIdentity {
    /// An identity as a record of an earlier admission holds it.
    pub fn recorded(incarnation: WorkspaceIncarnation, job_id: JobId, cgroup_id: u64) -> Self {
        Self {
            incarnation,
            job_id,
            cgroup_id,
        }
    }

    pub fn incarnation(&self) -> &WorkspaceIncarnation {
        &self.incarnation
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn cgroup_id(&self) -> u64 {
        self.cgroup_id
    }
}

impl IncarnationCgroups {
    pub fn incarnation(&self) -> &WorkspaceIncarnation {
        &self.incarnation
    }

    fn job_path(&self, job_id: JobId) -> PathBuf {
        self.path.join(format!("{JOB_PREFIX}{}", job_id.get()))
    }

    /// Create `job_id`'s cgroup. It must not exist: a cgroup that does belongs to an earlier
    /// admission of the same id, and joining it would charge this job with that one's usage.
    pub fn admit(&self, job_id: JobId) -> Result<JobCgroup> {
        let path = self.job_path(job_id);
        if !make_directory(&path)? {
            return Err(CgroupError::AlreadyExists { path });
        }
        let admitted = (|| {
            let directory = open_directory(&path)?;
            let cgroup_id = cgroup_id(&directory).map_err(failed("fstat", &path))?;
            JobCgroup::open(
                JobCgroupIdentity {
                    incarnation: self.incarnation.clone(),
                    job_id,
                    cgroup_id,
                },
                path,
                directory,
            )
        })();
        admitted.map_err(|admission| {
            // Only this attempt created this directory and no placement was issued. A missing
            // kernel interface must not leave a reused-name refusal hiding the original cause.
            let path = self.job_path(job_id);
            match fs::remove_dir(&path) {
                Ok(()) => admission,
                Err(source) => CgroupError::AdmissionCleanup {
                    path,
                    admission: Box::new(admission),
                    source,
                },
            }
        })
    }

    /// Reopen the job cgroup `identity` names: only one of this incarnation, and only while it
    /// is still the cgroup its admission created.
    pub fn lookup(&self, identity: &JobCgroupIdentity) -> Result<JobCgroup> {
        if identity.incarnation != self.incarnation {
            return Err(CgroupError::ForeignIncarnation {
                fence: self.incarnation.clone(),
                found: identity.incarnation.clone(),
            });
        }
        let path = self.job_path(identity.job_id);
        let directory = open_directory(&path)?;
        let found = cgroup_id(&directory).map_err(failed("fstat", &path))?;
        if found != identity.cgroup_id {
            return Err(CgroupError::Replaced {
                path,
                recorded: identity.cgroup_id,
                found,
            });
        }
        JobCgroup::open(identity.clone(), path, directory)
    }

    /// Every job cgroup of this incarnation not yet retired, in job id order: what a restarted
    /// controller finds its predecessor left. Anything else in the incarnation's cgroup is an
    /// integrity failure, never skipped.
    pub fn outstanding(&self) -> Result<Vec<JobCgroupIdentity>> {
        let entries = fs::read_dir(&self.path).map_err(failed("readdir", &self.path))?;
        let mut identities = Vec::new();
        for entry in entries {
            let entry = entry.map_err(failed("readdir", &self.path))?;
            let file_type = entry.file_type().map_err(|source| CgroupError::Io {
                call: "stat",
                path: entry.path(),
                source,
            })?;
            if !file_type.is_dir() {
                continue;
            }
            let path = entry.path();
            let job_id = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix(JOB_PREFIX))
                .and_then(|id| id.parse::<u64>().ok())
                .and_then(|id| JobId::new(id).ok())
                .ok_or_else(|| CgroupError::Malformed {
                    path: path.clone(),
                    detail: "a child cgroup of an incarnation that names no job".to_owned(),
                })?;
            let directory = open_directory(&path)?;
            identities.push(JobCgroupIdentity {
                incarnation: self.incarnation.clone(),
                job_id,
                cgroup_id: cgroup_id(&directory).map_err(failed("fstat", &path))?,
            });
        }
        identities.sort_by_key(|identity| identity.job_id);
        Ok(identities)
    }
}

/// A member observed alive during retirement and the last signal sent to its membership.
/// Linux has no observation-fold index; the pid is diagnostic, never signal authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CgroupMemberSignal {
    pub pid: u32,
    pub image: Option<String>,
    pub signal: i32,
}

/// Kernel membership is empty; diagnostic rows are not an exhaustive killed-member census.
#[derive(Debug, Eq, PartialEq)]
pub struct CgroupRetirement {
    pub members: Vec<CgroupMemberSignal>,
    pub metadata_gap: bool,
}

#[derive(Debug)]
struct Member {
    pid: u32,
    watch: ExitWatch,
    image: Option<String>,
    signal: Option<i32>,
}

impl Member {
    fn exited(&self) -> io::Result<bool> {
        let mut event = libc::pollfd {
            fd: self.watch.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: one live pidfd; zero timeout is a readiness query, not a polling loop.
            if unsafe { libc::poll(&raw mut event, 1, 0) } >= 0 {
                return Ok(event.revents & libc::POLLIN != 0);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    fn terminate(&mut self) -> io::Result<()> {
        // SAFETY: a live pidfd and a plain signal, no siginfo payload. A reused numeric pid
        // can never redirect this signal; the handle remains bound to the opened process.
        if unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.watch.as_raw_fd(),
                libc::SIGTERM,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        } == 0
        {
            self.signal = Some(libc::SIGTERM);
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        Err(error)
    }

    fn receipt(self) -> Option<CgroupMemberSignal> {
        self.signal.map(|signal| CgroupMemberSignal {
            pid: self.pid,
            image: self.image,
            signal,
        })
    }
}

#[derive(Debug, Default)]
struct SignalledMembers {
    pinned: BTreeMap<u32, Member>,
    exited: Vec<CgroupMemberSignal>,
    metadata_gap: bool,
}

impl SignalledMembers {
    fn observe(&mut self, members: Vec<Member>) -> io::Result<()> {
        for member in members {
            match self.pinned.entry(member.pid) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(member);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().exited()?
                        && let Some(receipt) = entry.insert(member).receipt()
                    {
                        self.exited.push(receipt);
                    }
                }
            }
        }
        Ok(())
    }

    fn proof(&mut self) -> Arc<CgroupRetirement> {
        let Self {
            pinned,
            mut exited,
            metadata_gap,
        } = std::mem::take(self);
        exited.extend(pinned.into_values().filter_map(Member::receipt));
        Arc::new(CgroupRetirement {
            members: exited,
            metadata_gap,
        })
    }
}

/// Successful retirement and finalization are shared with late Drop and explicit retry callers.
#[derive(Debug, Default)]
struct Retirement {
    proof: Option<Arc<CgroupRetirement>>,
    totals: Option<JobCgroupTotals>,
}

/// One job's cgroup, open for placing its first process and reading its counters.
#[derive(Debug)]
pub struct JobCgroup {
    identity: JobCgroupIdentity,
    path: PathBuf,
    directory: OwnedFd,
    procs: OwnedFd,
    kill: OwnedFd,
    signalled: Mutex<SignalledMembers>,
    retirement: Mutex<Retirement>,
}

impl JobCgroup {
    fn open(identity: JobCgroupIdentity, path: PathBuf, directory: OwnedFd) -> Result<Self> {
        let available = read_at(&directory, c"cgroup.controllers").map_err(failed_in(
            "read",
            &path,
            "cgroup.controllers",
        ))?;
        require_controllers(&path, &available, &Controller::ACCOUNTING)?;
        let procs = open_at(directory.as_raw_fd(), c"cgroup.procs", libc::O_WRONLY)
            .map_err(failed_in("open", &path, "cgroup.procs"))?;
        let kill =
            open_at(directory.as_raw_fd(), c"cgroup.kill", libc::O_WRONLY).map_err(|source| {
                CgroupError::KillUnavailable {
                    path: path.join("cgroup.kill"),
                    source,
                }
            })?;
        Ok(Self {
            identity,
            path,
            directory,
            procs,
            kill,
            signalled: Mutex::new(SignalledMembers::default()),
            retirement: Mutex::new(Retirement::default()),
        })
    }

    pub fn identity(&self) -> &JobCgroupIdentity {
        &self.identity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the cgroup or any descendant holds a live process.
    pub fn populated(&self) -> Result<bool> {
        let events = read_at(&self.directory, c"cgroup.events").map_err(failed_in(
            "read",
            &self.path,
            "cgroup.events",
        ))?;
        populated(&self.path, &events)
    }

    /// The job's CPU so far: every process that has run in it, live or reaped.
    pub fn cpu(&self) -> Result<CgroupCpu> {
        read_cpu(&self.directory, &self.path)
    }

    /// What the job is charged now and the most it was ever charged.
    pub fn charged_memory(&self) -> Result<ChargedMemoryUsage> {
        read_charged_memory(&self.directory, &self.path)
    }

    /// The bytes the job has moved to and from storage so far ([`read_storage_io`]).
    pub fn storage_io(&self) -> Result<StorageIoTotals> {
        read_storage_io(&self.directory, &self.path)
    }

    /// The means for one process to enter this cgroup before it executes anything.
    pub fn placement(&self) -> Result<Placement> {
        // SAFETY: F_DUPFD_CLOEXEC on a live descriptor; a non-negative answer is a new one.
        let fd = unsafe { libc::fcntl(self.procs.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(CgroupError::Io {
                call: "fcntl(F_DUPFD_CLOEXEC)",
                path: self.path.join("cgroup.procs"),
                source: io::Error::last_os_error(),
            });
        }
        // SAFETY: fcntl just returned this descriptor and nothing else owns it.
        Ok(Placement(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    /// Kill the complete membership atomically with respect to forks and migrations. A process
    /// in a different session or a descendant cgroup is still a member. This is not an exit proof.
    pub fn kill(&self) -> Result<()> {
        self.issue_kill()?.1
    }

    /// Freeze or thaw the complete membership through `cgroup.freeze`: a job whose probation
    /// ended without a grant waits frozen until its grant (05_gateway.md "A one-token job starts
    /// on probation"). A process that enters a frozen cgroup is frozen with it.
    pub fn freeze(&self, frozen: bool) -> Result<()> {
        let freeze = open_at(self.directory.as_raw_fd(), c"cgroup.freeze", libc::O_WRONLY)
            .map_err(failed_in("open", &self.path, "cgroup.freeze"))?;
        write_one(freeze.as_raw_fd(), if frozen { b"1" } else { b"0" }).map_err(failed_in(
            "write",
            &self.path,
            "cgroup.freeze",
        ))
    }

    /// [`Self::kill`] with its refusal kept apart from what failed beside it: `Err` when
    /// cgroup.kill was not written, so nothing is known to have ended. Otherwise what this did to
    /// the membership (SIGKILL, or nothing when it was already empty), with `Err` beside it when
    /// a membership snapshot or population read failed around it.
    fn issue_kill(&self) -> Result<(Progress, Result<()>)> {
        let members = self.members();
        let mut signalled = self
            .signalled
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let recording = members.and_then(|members| {
            signalled
                .observe(members)
                .map_err(failed("poll(pidfd)", &self.path))?;
            signalled
                .pinned
                .values()
                .filter_map(|member| match member.exited() {
                    Ok(false) => Some(Ok(member.pid)),
                    Ok(true) => None,
                    Err(error) => Some(Err(failed("poll(pidfd)", &self.path)(error))),
                })
                .collect::<Result<Vec<_>>>()
        });
        let populated = self.populated();
        if matches!(populated, Ok(false)) {
            // Empty membership cannot fork: do not issue a kill that would need a receipt.
            return Ok((Progress::Nothing, recording.map(|_| ())));
        }
        // Diagnostic snapshot failures do not revoke the controller's authority to end the
        // complete membership. Report them after attempting the atomic kernel operation.
        write_one(self.kill.as_raw_fd(), b"1").map_err(failed_in(
            "write",
            &self.path,
            "cgroup.kill",
        ))?;
        // cgroup.kill exposes no killed-member list. A fork can occur after even a nonempty
        // snapshot, so every atomic kill of populated membership marks the rows non-exhaustive.
        signalled.metadata_gap = true;
        let recording = match recording {
            Ok(recording) => recording,
            Err(error) => {
                return Ok((
                    Progress::Signalled {
                        signal: libc::SIGKILL,
                    },
                    Err(error),
                ));
            }
        };
        for pid in recording {
            signalled
                .pinned
                .get_mut(&pid)
                .expect("the pinned member remains held")
                .signal = Some(libc::SIGKILL);
        }
        Ok((
            Progress::Signalled {
                signal: libc::SIGKILL,
            },
            populated.map(|_| ()),
        ))
    }

    /// Ask every currently live member to end, addressing each through a pidfd. KILL later
    /// uses cgroup.kill, so children born after this finite TERM snapshot cannot escape.
    pub fn terminate(&self) -> Result<()> {
        let members = self.members()?;
        let mut signalled = self
            .signalled
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        signalled
            .observe(members)
            .map_err(failed("poll(pidfd)", &self.path))?;
        for member in signalled.pinned.values_mut() {
            if member.signal != Some(libc::SIGKILL) {
                member
                    .terminate()
                    .map_err(failed("pidfd_send_signal(SIGTERM)", &self.path))?;
            }
        }
        Ok(())
    }

    /// TERM the current members, grant one grace deadline, then atomically KILL any remaining
    /// membership and join the kernel's empty notification and each signalled member's exit,
    /// both within `reap` (production: `cowshed_os::deadline::REAP`). Successful proof is cached
    /// and shared across explicit retries and late Drop; only one retirement runs at a time. A
    /// member that outlives `reap` fails it as [`CgroupError::Unreaped`] and is left as it is;
    /// a retry runs the whole retirement again. This method does not reap a leader, remove the
    /// hierarchy, or publish a terminal record.
    pub fn retire(&self, grace: Duration, reap: Bound) -> Result<Arc<CgroupRetirement>> {
        let mut retirement = self
            .retirement
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(proof) = &retirement.proof {
            return Ok(Arc::clone(proof));
        }
        if let Err(term) = self.terminate() {
            let ended = self.kill_and_wait(reap);
            return Err(CgroupError::Malformed {
                path: self.path.clone(),
                detail: format!("TERM refused: {term}; KILL and empty-membership join: {ended:?}"),
            });
        }
        let grace_ends =
            Instant::now()
                .checked_add(grace)
                .ok_or_else(|| CgroupError::Malformed {
                    path: self.path.clone(),
                    detail: "TERM grace exceeds the monotonic clock's range".to_owned(),
                })?;
        let graced = self.wait_empty_within(|| {
            Some(grace_ends.saturating_duration_since(Instant::now()))
                .filter(|left| !left.is_zero())
        })?;
        if graced {
            self.join_signalled(
                &deadline::start("job cgroup retire", reap),
                Progress::Signalled {
                    signal: libc::SIGTERM,
                },
            )?;
        } else {
            self.kill_and_wait(reap)?;
        }
        let mut signalled = self
            .signalled
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let proof = signalled.proof();
        retirement.proof = Some(Arc::clone(&proof));
        Ok(proof)
    }

    fn members(&self) -> Result<Vec<Member>> {
        let mut pids = BTreeSet::new();
        collect_members(&self.directory, &self.path, &mut pids)?;
        let mut members = Vec::with_capacity(pids.len());
        for pid in pids {
            let Some(watch) = ExitWatch::of(pid).map_err(failed("pidfd_open", &self.path))? else {
                continue;
            };
            let process = PathBuf::from(format!("/proc/{pid}"));
            let membership = match fs::read_to_string(process.join("cgroup")) {
                Ok(membership) => membership,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(failed_in("read", &process, "cgroup")(error)),
            };
            let current = Path::new(CGROUP_MOUNT)
                .join(unified_membership(&membership)?.trim_start_matches('/'));
            if !current.starts_with(&self.path) {
                continue;
            }
            let image = match fs::read_link(process.join("exe")) {
                Ok(image) => Some(image.to_string_lossy().into_owned()),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    None
                }
                Err(error) => return Err(failed_in("readlink", &process, "exe")(error)),
            };
            let member = Member {
                pid,
                watch,
                image,
                signal: None,
            };
            if !member.exited().map_err(failed("poll(pidfd)", &process))? {
                members.push(member);
            }
        }
        Ok(members)
    }

    /// cgroup_exit precedes exit_notify in Linux do_exit. After populated 0, join the retained
    /// pidfds too: membership is empty, but the final pidfd notification may still be pending.
    /// The join ends with `deadline`: a member whose exit has not come by then is named in
    /// [`CgroupError::Unreaped`], with `last` the last thing this wait did to the membership.
    fn join_signalled(&self, deadline: &Deadline, last: Progress) -> Result<()> {
        let signalled = self
            .signalled
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for member in signalled.pinned.values() {
            loop {
                let Some(remaining) = deadline.remaining(deadline::clock()) else {
                    let live = signalled
                        .pinned
                        .values()
                        .filter(|pinned| !matches!(pinned.exited(), Ok(true)))
                        .map(|pinned| LiveMember::observed(pinned.pid, pinned.image.clone()))
                        .collect();
                    return Err(self.unreaped(deadline, Ok(live), last));
                };
                match member.watch.within(remaining) {
                    Ok(true) => break,
                    // A wake short of the exit, or a sub-millisecond rest the watch rounds to
                    // zero, asks the deadline again.
                    Ok(false) => {}
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(failed("poll(pidfd exit)", &self.path)(error)),
                }
            }
        }
        Ok(())
    }

    /// End every member and retain the cgroup until the kernel reports that every one exited,
    /// within `reap` (production: `cowshed_os::deadline::REAP`). A refused cgroup.kill write
    /// ended nothing, so it is answered at once: joining would wait on members still running,
    /// and the caller keeps its claim for an explicit retry. A member the kill has not ended by
    /// then, such as one in an uninterruptible kernel wait, fails the wait as
    /// [`CgroupError::Unreaped`] naming it; it is left as it is.
    pub fn kill_and_wait(&self, reap: Bound) -> Result<()> {
        let (last, killed) = self.issue_kill()?;
        let waited = self.reap_membership(&deadline::start("job cgroup kill", reap), last);
        match (killed, waited) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(kill), Err(wait)) => Err(CgroupError::Malformed {
                path: self.path.clone(),
                detail: format!("KILL failed: {kill}; empty-membership join failed: {wait}"),
            }),
        }
    }

    /// Wait for empty membership, then join every signalled member's exit, both by `deadline`.
    fn reap_membership(&self, deadline: &Deadline, last: Progress) -> Result<()> {
        if !self.wait_empty_within(|| deadline.remaining(deadline::clock()))? {
            let live = self
                .members()
                .map(|members| {
                    members
                        .into_iter()
                        .map(|member| LiveMember::observed(member.pid, member.image))
                        .collect()
                })
                .map_err(|census| census.to_string());
            return Err(self.unreaped(deadline, live, last));
        }
        self.join_signalled(deadline, last)
    }

    /// The expiry of `deadline` on this cgroup, holding `live`: the members still alive, or
    /// why they could not be read.
    fn unreaped(
        &self,
        deadline: &Deadline,
        live: Result<Vec<LiveMember>, String>,
        last: Progress,
    ) -> CgroupError {
        CgroupError::Unreaped(Box::new(deadline.expired(
            LiveMembership {
                cgroup: self.path.clone(),
                members: live,
            },
            last,
            deadline::clock(),
        )))
    }

    /// Wait until the kernel reports no live member, for as long as `remaining` grants (`None`
    /// once the wait's deadline has passed); whether membership emptied. No clock samples the
    /// membership: cgroup.events changes wake ppoll(POLLPRI), clipped to what is left, and each
    /// wake rereads that descriptor.
    fn wait_empty_within(&self, remaining: impl Fn() -> Option<Duration>) -> Result<bool> {
        let fd = open_at(self.directory.as_raw_fd(), c"cgroup.events", libc::O_RDONLY)
            .map_err(failed_in("open", &self.path, "cgroup.events"))?;
        let mut file = fs::File::from(fd);
        let mut text = String::with_capacity(64);
        loop {
            file.rewind()
                .map_err(failed_in("seek", &self.path, "cgroup.events"))?;
            text.clear();
            file.read_to_string(&mut text).map_err(failed_in(
                "read",
                &self.path,
                "cgroup.events",
            ))?;
            if !populated(&self.path, &text)? {
                return Ok(true);
            }
            let Some(remaining) = remaining() else {
                return Ok(false);
            };
            let timeout = libc::timespec {
                tv_sec: libc::time_t::try_from(remaining.as_secs()).unwrap_or(libc::time_t::MAX),
                tv_nsec: libc::c_long::from(remaining.subsec_nanos()),
            };
            let mut event = libc::pollfd {
                fd: file.as_raw_fd(),
                events: libc::POLLPRI,
                revents: 0,
            };
            // SAFETY: one live descriptor, a valid timeout, and no signal-mask change. The timeout
            // is what is left of the wait's one deadline, never a sampling period.
            let answer =
                unsafe { libc::ppoll(&raw mut event, 1, &raw const timeout, std::ptr::null()) };
            if answer < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(failed_in("ppoll(POLLPRI)", &self.path, "cgroup.events")(
                    error,
                ));
            }
            if event.revents & (libc::POLLNVAL | libc::POLLHUP) != 0 {
                return Err(CgroupError::Malformed {
                    path: self.path.join("cgroup.events"),
                    detail: format!("membership watch closed (poll revents {})", event.revents),
                });
            }
            // A timeout also rereads the kernel's state before it decides the deadline passed.
            // POLLERR accompanies POLLPRI on cgroup.events and is not a failed observation.
        }
    }

    /// Collect final counters and remove an empty job's hierarchy without requiring the exec
    /// host's last Arc to have dropped. The actor calls this after its terminal sample, before
    /// releasing the claim. Descriptors retained by a late host release then name a removed
    /// cgroup, never a newly admitted job that happens to reuse the path. The join of signalled
    /// members' exits ends within `reap` (production: `cowshed_os::deadline::REAP`).
    pub fn finish(&self, reap: Bound) -> Result<JobCgroupTotals> {
        let mut retirement = self
            .retirement
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(totals) = retirement.totals {
            return Ok(totals);
        }
        if self.populated()? {
            return Err(CgroupError::Populated {
                path: self.path.clone(),
            });
        }
        self.join_signalled(
            &deadline::start("job cgroup finish", reap),
            Progress::Nothing,
        )?;
        if retirement.proof.is_none() {
            let proof = self
                .signalled
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .proof();
            retirement.proof = Some(proof);
        }
        let totals = finish_cgroup(&self.identity, &self.path, &self.directory)?;
        retirement.totals = Some(totals);
        Ok(totals)
    }

    /// The job holds no process any more: its counters are final, and only now may it be
    /// retired. A populated cgroup is handed back with the reason.
    pub fn terminal(self) -> Result<TerminalJobCgroup, Box<NotTerminal>> {
        match self.populated() {
            Ok(false) => Ok(TerminalJobCgroup {
                identity: self.identity,
                path: self.path,
                directory: self.directory,
            }),
            Ok(true) => {
                let path = self.path.clone();
                Err(Box::new(NotTerminal {
                    cgroup: self,
                    error: CgroupError::Populated { path },
                }))
            }
            Err(error) => Err(Box::new(NotTerminal {
                cgroup: self,
                error,
            })),
        }
    }
}

/// Read only the owned cgroup subtree, never a process-table census.
fn collect_members(directory: &OwnedFd, path: &Path, pids: &mut BTreeSet<u32>) -> Result<()> {
    let procs =
        read_at(directory, c"cgroup.procs").map_err(failed_in("read", path, "cgroup.procs"))?;
    for line in procs.lines() {
        let pid = line
            .parse::<u32>()
            .ok()
            .filter(|pid| *pid != 0)
            .ok_or_else(|| CgroupError::Malformed {
                path: path.join("cgroup.procs"),
                detail: format!("invalid member pid {line:?}"),
            })?;
        pids.insert(pid);
    }
    for entry in fs::read_dir(path).map_err(failed("readdir", path))? {
        let entry = entry.map_err(failed("readdir", path))?;
        if entry.file_type().map_err(failed("stat", path))?.is_dir() {
            let child = entry.path();
            let opened = open_directory(&child)?;
            collect_members(&opened, &child, pids)?;
        }
    }
    Ok(())
}

/// One control write; cgroup interface files do not accept partial writes.
fn write_one(fd: RawFd, byte: &[u8; 1]) -> io::Result<()> {
    loop {
        // SAFETY: the caller holds fd open and the byte is readable for the duration of write.
        match unsafe { libc::write(fd, byte.as_ptr().cast(), 1) } {
            1 => return Ok(()),
            -1 => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
            _ => return Err(io::Error::from(io::ErrorKind::WriteZero)),
        }
    }
}

/// A job cgroup that could not be proven empty, and why.
#[derive(Debug)]
pub struct NotTerminal {
    pub cgroup: JobCgroup,
    pub error: CgroupError,
}

/// A controller-opened write end of one job's `cgroup.procs`, close-on-exec from birth.
#[derive(Debug)]
pub struct Placement(OwnedFd);

impl Placement {
    /// Receive the controller-opened descriptor over the exec-host's authenticated control lane.
    pub fn from_descriptor(descriptor: OwnedFd) -> Self {
        Self(descriptor)
    }

    /// Transfer the placement authority to the exec host, never a pathname chosen by the job.
    pub fn into_descriptor(self) -> OwnedFd {
        self.0
    }

    /// Enter in the forked child, before exec or interpreting a script. Only write(2) runs; the
    /// caller must close this descriptor before running a script that does not exec.
    pub fn enter(&self) -> io::Result<()> {
        write_one(self.0.as_raw_fd(), b"0")
    }

    /// Have the process `command` spawns enter the job's cgroup after it forks and before it
    /// executes its program, so nothing the program runs is charged anywhere else. A failed
    /// entry fails the spawn.
    pub fn on_exec(self, command: &mut std::process::Command) {
        // SAFETY: enter and the failure diagnostic make only async-signal-safe write calls.
        unsafe {
            command.pre_exec(move || {
                self.enter().inspect_err(|_| {
                    let diagnostic = b"cowshed: cgroup placement failed before exec\n";
                    libc::write(
                        libc::STDERR_FILENO,
                        diagnostic.as_ptr().cast(),
                        diagnostic.len(),
                    );
                })
            });
        }
    }
}

/// A job cgroup that holds no process: its counters are final.
#[derive(Debug)]
pub struct TerminalJobCgroup {
    identity: JobCgroupIdentity,
    path: PathBuf,
    directory: OwnedFd,
}

impl TerminalJobCgroup {
    pub fn identity(&self) -> &JobCgroupIdentity {
        &self.identity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The job's final CPU.
    pub fn cpu(&self) -> Result<CgroupCpu> {
        read_cpu(&self.directory, &self.path)
    }

    /// What the job is charged after its last process ended -- the cache it filled stays
    /// charged -- and the most it was ever charged.
    pub fn charged_memory(&self) -> Result<ChargedMemoryUsage> {
        read_charged_memory(&self.directory, &self.path)
    }

    /// The bytes the job moved to and from storage over its life ([`read_storage_io`]).
    pub fn storage_io(&self) -> Result<StorageIoTotals> {
        read_storage_io(&self.directory, &self.path)
    }

    /// Collect the job's final counters, then remove the cgroup while its name still holds the
    /// cgroup admission created. A counter that cannot be read leaves the cgroup in place: once
    /// it is gone, nothing could ever read it again.
    pub fn retire(self) -> Result<JobCgroupTotals> {
        finish_cgroup(&self.identity, &self.path, &self.directory)
    }
}

fn finish_cgroup(
    identity: &JobCgroupIdentity,
    path: &Path,
    directory: &OwnedFd,
) -> Result<JobCgroupTotals> {
    let totals = JobCgroupTotals {
        cpu: read_cpu(directory, path)?,
        charged_memory: read_charged_memory(directory, path)?,
        storage_io: read_storage_io(directory, path)?,
    };
    let current = open_directory(path)?;
    let found = cgroup_id(&current).map_err(failed("fstat", path))?;
    if found != identity.cgroup_id {
        return Err(CgroupError::Replaced {
            path: path.to_path_buf(),
            recorded: identity.cgroup_id,
            found,
        });
    }
    remove_empty_hierarchy(path)?;
    Ok(totals)
}

/// Population was proved empty recursively; the owned hierarchy can contain empty child groups.
fn remove_empty_hierarchy(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path).map_err(failed("readdir", path))? {
        let entry = entry.map_err(failed("readdir", path))?;
        if entry.file_type().map_err(failed("stat", path))?.is_dir() {
            remove_empty_hierarchy(&entry.path())?;
        }
    }
    fs::remove_dir(path).map_err(failed("rmdir", path))
}

/// What a job cgroup counted over its whole life, read after its last process ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobCgroupTotals {
    pub cpu: CgroupCpu,
    pub charged_memory: ChargedMemoryUsage,
    pub storage_io: StorageIoTotals,
}

/// A job cgroup's CPU as its `cpu.stat` counts it: every process that ran in the cgroup or
/// beneath it, those reaped long before any observer looked included. `usage` is the kernel's
/// exact runtime; `user` and `system` split it by the tick-sampled ratio.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CgroupCpu {
    pub usage_us: CpuMicros,
    pub user_us: CpuMicros,
    pub system_us: CpuMicros,
}

fn read_cpu(directory: &OwnedFd, cgroup: &Path) -> Result<CgroupCpu> {
    let path = cgroup.join("cpu.stat");
    let text = read_at(directory, c"cpu.stat").map_err(failed("read", &path))?;
    parse_cpu_stat(&path, &text)
}

/// A block device as the kernel numbers it.
type DeviceNumber = (u32, u32);

/// One device line of `io.stat`: what the cgroup moved through that device.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeviceIo {
    device: DeviceNumber,
    read_bytes: u64,
    write_bytes: u64,
}

/// The job's storage I/O from its `io.stat`: the bytes its cgroup and every cgroup beneath it
/// submitted to block devices, read and written. The file is already hierarchical, so no
/// descendant cgroup is ever added to it. A bio passed down a device stack (device-mapper, md)
/// is counted on every device it passes, so each transfer is counted once, at the bottom of its
/// stack: a listed device that sits on another listed device is left out. Writes count when
/// they reach a device -- a page the job dirtied is charged to it at writeback -- and a read
/// the page cache served reaches none. An empty `io.stat` is the kernel's statement that the
/// cgroup moved no byte; a missing one is unavailable.
fn read_storage_io(directory: &OwnedFd, cgroup: &Path) -> Result<StorageIoTotals> {
    let path = cgroup.join("io.stat");
    let text = read_at(directory, c"io.stat").map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            CgroupError::CounterUnavailable { path: path.clone() }
        } else {
            CgroupError::Io {
                call: "read",
                path: path.clone(),
                source,
            }
        }
    })?;
    let devices = parse_io_stat(&path, &text)?;
    aggregate_storage_io(&path, &devices, lower_devices)
}

fn parse_io_stat(path: &Path, text: &str) -> Result<Vec<DeviceIo>> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let malformed = |detail: String| CgroupError::Malformed {
                path: path.to_path_buf(),
                detail,
            };
            let mut fields = line.split_whitespace();
            let device = fields
                .next()
                .and_then(|device| device.split_once(':'))
                .and_then(|(major, minor)| Some((major.parse().ok()?, minor.parse().ok()?)))
                .ok_or_else(|| malformed(format!("{line:?} names no MAJ:MIN device")))?;
            let counter = |name: &'static str| -> Result<u64> {
                let value = fields
                    .clone()
                    .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
                    .ok_or_else(|| CgroupError::MissingCounter {
                        path: path.to_path_buf(),
                        counter: name,
                    })?;
                value
                    .parse()
                    .map_err(|_| malformed(format!("{name}={value:?} is not a count")))
            };
            Ok(DeviceIo {
                device,
                read_bytes: counter("rbytes")?,
                write_bytes: counter("wbytes")?,
            })
        })
        .collect()
}

/// Sum the devices of `devices` that no other listed device sits beneath, `lower` naming the
/// devices one device is stacked directly on.
fn aggregate_storage_io(
    path: &Path,
    devices: &[DeviceIo],
    lower: impl Fn(DeviceNumber) -> Result<Vec<DeviceNumber>>,
) -> Result<StorageIoTotals> {
    let listed: BTreeSet<DeviceNumber> = devices.iter().map(|device| device.device).collect();
    let (mut read, mut written) = (0_u128, 0_u128);
    for device in devices {
        let mut beneath = lower(device.device)?;
        let mut seen = BTreeSet::new();
        let mut stacked = false;
        while let Some(next) = beneath.pop() {
            if !seen.insert(next) {
                continue;
            }
            if listed.contains(&next) {
                stacked = true;
                break;
            }
            beneath.extend(lower(next)?);
        }
        if !stacked {
            read += u128::from(device.read_bytes);
            written += u128::from(device.write_bytes);
        }
    }
    let bytes = |counter: &'static str, value: u128| {
        u64::try_from(value)
            .map_err(|_| ResourceUnitError::Inexact {
                unit: "ioBytes",
                value,
            })
            .and_then(StorageIoBytes::new)
            .map_err(|source| CgroupError::OutOfRange {
                path: path.to_path_buf(),
                counter,
                source,
            })
    };
    Ok(StorageIoTotals {
        read_bytes: bytes("rbytes", read)?,
        write_bytes: bytes("wbytes", written)?,
    })
}

/// The devices `device` is stacked directly on, from its sysfs `slaves` directory: none for a
/// device that sits on no other.
fn lower_devices((major, minor): DeviceNumber) -> Result<Vec<DeviceNumber>> {
    let slaves = PathBuf::from(format!("/sys/dev/block/{major}:{minor}/slaves"));
    let entries = match fs::read_dir(&slaves) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A device sysfs does not list at all is gone, which is not the same as a device on
            // nothing: only an existing device without a `slaves` directory is the latter.
            let device = slaves.parent().unwrap_or(&slaves);
            return match fs::metadata(device) {
                Ok(_) => Ok(Vec::new()),
                Err(source) => Err(CgroupError::Io {
                    call: "stat",
                    path: device.to_path_buf(),
                    source,
                }),
            };
        }
        Err(source) => {
            return Err(CgroupError::Io {
                call: "readdir",
                path: slaves,
                source,
            });
        }
    };
    let mut lower = Vec::new();
    for entry in entries {
        let entry = entry.map_err(failed("readdir", &slaves))?;
        let dev = entry.path().join("dev");
        let text = fs::read_to_string(&dev).map_err(failed("read", &dev))?;
        let number = text
            .trim()
            .split_once(':')
            .and_then(|(major, minor)| Some((major.parse().ok()?, minor.parse().ok()?)))
            .ok_or_else(|| CgroupError::Malformed {
                path: dev.clone(),
                detail: format!("{text:?} is no MAJ:MIN device"),
            })?;
        lower.push(number);
    }
    Ok(lower)
}

/// `memory.current`, then `memory.peak`: read in that order the peak, a high watermark that only
/// rises, holds at least the current charge. Both are opened read-only, since a write to a
/// `memory.peak` descriptor resets the watermark that descriptor reports.
fn read_charged_memory(directory: &OwnedFd, cgroup: &Path) -> Result<ChargedMemoryUsage> {
    let current = single_value(directory, cgroup, c"memory.current")?;
    let peak = single_value(directory, cgroup, c"memory.peak")?;
    if peak < current {
        return Err(CgroupError::Malformed {
            path: cgroup.join("memory.peak"),
            detail: format!("peak {peak} lies below the current charge {current} read before it"),
        });
    }
    let bytes = |name: &'static str, value: u64| {
        ChargedMemoryBytes::new(value).map_err(|source| CgroupError::OutOfRange {
            path: cgroup.join(name),
            counter: name,
            source,
        })
    };
    Ok(ChargedMemoryUsage {
        current_bytes: bytes("memory.current", current)?,
        peak_bytes: bytes("memory.peak", peak)?,
    })
}

/// A cgroup file that holds one count. One the kernel does not provide is unavailable, never
/// zero.
fn single_value(directory: &OwnedFd, cgroup: &Path, name: &CStr) -> Result<u64> {
    let path = cgroup.join(OsStr::from_bytes(name.to_bytes()));
    let text = read_at(directory, name).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            CgroupError::CounterUnavailable { path: path.clone() }
        } else {
            CgroupError::Io {
                call: "read",
                path: path.clone(),
                source,
            }
        }
    })?;
    parse_single_value(&path, &text)
}

fn parse_single_value(path: &Path, text: &str) -> Result<u64> {
    text.trim_end_matches('\n')
        .parse()
        .map_err(|_| CgroupError::Malformed {
            path: path.to_path_buf(),
            detail: format!("{text:?} is not a count"),
        })
}

fn parse_cpu_stat(path: &Path, text: &str) -> Result<CgroupCpu> {
    let micros = |counter: &'static str| -> Result<CpuMicros> {
        CpuMicros::new(keyed_counter(path, text, counter)?).map_err(|source| {
            CgroupError::OutOfRange {
                path: path.to_path_buf(),
                counter,
                source,
            }
        })
    };
    Ok(CgroupCpu {
        usage_us: micros("usage_usec")?,
        user_us: micros("user_usec")?,
        system_us: micros("system_usec")?,
    })
}

/// The value of the `<counter> <value>` line of a flat-keyed cgroup file.
fn keyed_counter(path: &Path, text: &str, counter: &'static str) -> Result<u64> {
    let value = text
        .lines()
        .find_map(|line| {
            line.split_once(' ')
                .filter(|(key, _)| *key == counter)
                .map(|(_, value)| value)
        })
        .ok_or_else(|| CgroupError::MissingCounter {
            path: path.to_path_buf(),
            counter,
        })?;
    value.parse().map_err(|_| CgroupError::Malformed {
        path: path.to_path_buf(),
        detail: format!("{counter} {value:?} is not a count"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unified_line_names_the_membership() {
        let content = "12:pids:/legacy\n0::/system.slice/run-1.scope/controller\n";
        assert_eq!(
            unified_membership(content).expect("a v2 line"),
            "/system.slice/run-1.scope/controller"
        );
        assert!(matches!(
            unified_membership("12:pids:/legacy\n"),
            Err(CgroupError::NoUnifiedMembership { .. })
        ));
    }

    #[test]
    fn the_accounting_request_enables_exactly_the_accounting_controllers() {
        let expected = Controller::ACCOUNTING
            .iter()
            .map(|controller| format!("+{controller}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(Controller::ACCOUNTING_REQUEST, expected);
    }

    #[test]
    fn failed_calls_retain_the_call_path_and_source() {
        let path = Path::new("/sys/fs/cgroup/job-1");
        let error = failed("open", path)(io::Error::from_raw_os_error(libc::EACCES));
        assert!(matches!(
            error,
            CgroupError::Io { call: "open", path: failed_path, source }
                if failed_path == path && source.raw_os_error() == Some(libc::EACCES)
        ));
        let error =
            failed_in("read", path, "cgroup.events")(io::Error::from_raw_os_error(libc::ENOENT));
        assert!(matches!(
            error,
            CgroupError::Io { call: "read", path: failed_path, source }
                if failed_path == path.join("cgroup.events")
                    && source.raw_os_error() == Some(libc::ENOENT)
        ));
    }

    #[test]
    fn a_missing_controller_is_named_never_assumed() {
        let path = Path::new("/sys/fs/cgroup/x");
        require_controllers(path, "cpuset cpu io memory pids\n", &Controller::ACCOUNTING)
            .expect("all three offered");
        let missing = require_controllers(path, "io memory pids\n", &Controller::ACCOUNTING);
        assert!(
            matches!(
                missing,
                Err(CgroupError::ControllerUnavailable {
                    controller: Controller::Cpu,
                    ref available,
                    ..
                }) if available == "io memory pids"
            ),
            "{missing:?}"
        );
        // A name that merely contains another is not it.
        assert!(require_controllers(path, "cpuset iocost memory", &[Controller::Cpu]).is_err());
    }

    #[test]
    fn population_is_read_not_guessed() {
        let path = Path::new("/sys/fs/cgroup/x");
        assert!(!populated(path, "populated 0\nfrozen 0\n").expect("empty"));
        assert!(populated(path, "populated 1\nfrozen 0\n").expect("live"));
        assert!(matches!(
            populated(path, "frozen 0\n"),
            Err(CgroupError::Malformed { .. })
        ));
        assert!(matches!(
            populated(path, "populated 2\n"),
            Err(CgroupError::Malformed { .. })
        ));
    }

    #[test]
    fn every_device_line_names_both_byte_counters() {
        let path = Path::new("/sys/fs/cgroup/x/io.stat");
        let stat = "259:3 rbytes=811008 wbytes=0 rios=61 wios=0 dbytes=0 dios=0\n\
                    259:0 rbytes=995328 wbytes=4096 rios=76 wios=1 dbytes=0 dios=0\n";
        assert_eq!(
            parse_io_stat(path, stat).expect("two devices"),
            [
                DeviceIo {
                    device: (259, 3),
                    read_bytes: 811_008,
                    write_bytes: 0
                },
                DeviceIo {
                    device: (259, 0),
                    read_bytes: 995_328,
                    write_bytes: 4096
                },
            ]
        );
        assert_eq!(parse_io_stat(path, "").expect("no I/O yet"), []);
        // Operation counts never stand in for a byte counter.
        assert!(matches!(
            parse_io_stat(path, "8:0 rbytes=1 rios=1 wios=1\n"),
            Err(CgroupError::MissingCounter {
                counter: "wbytes",
                ..
            })
        ));
        assert_eq!(
            parse_io_stat(path, "8:0 rbytesx=1 rbytes=2 wbytes=3\n")
                .expect("a longer key is another counter")[0]
                .read_bytes,
            2
        );
        assert!(matches!(
            parse_io_stat(path, "sda rbytes=1 wbytes=1\n"),
            Err(CgroupError::Malformed { .. })
        ));
    }

    #[test]
    fn a_stacked_transfer_counts_once_at_the_bottom() {
        let path = Path::new("/sys/fs/cgroup/x/io.stat");
        let device = |device, read_bytes, write_bytes| DeviceIo {
            device,
            read_bytes,
            write_bytes,
        };
        // dm-0 (253:0) sits on md0 (9:0), which sits on two disks; dm-1 (253:1) on nvme (259:0).
        let lower = |number: DeviceNumber| -> Result<Vec<DeviceNumber>> {
            Ok(match number {
                (253, 0) => vec![(9, 0)],
                (9, 0) => vec![(8, 0), (8, 16)],
                (253, 1) => vec![(259, 0)],
                _ => Vec::new(),
            })
        };
        let totals = |devices: &[DeviceIo]| {
            let totals = aggregate_storage_io(path, devices, lower).expect("totals");
            (totals.read_bytes.get(), totals.write_bytes.get())
        };
        // The whole stack is listed: only the two disks count.
        assert_eq!(
            totals(&[
                device((253, 0), 100, 10),
                device((9, 0), 100, 10),
                device((8, 0), 50, 5),
                device((8, 16), 50, 5),
            ]),
            (100, 10)
        );
        // An upper device whose lower device is not listed is the bottom of what was seen.
        assert_eq!(totals(&[device((253, 1), 7, 3)]), (7, 3));
        // Unrelated disks add up.
        assert_eq!(
            totals(&[device((8, 0), 1, 2), device((259, 0), 3, 4)]),
            (4, 6)
        );
        let failing = |_: DeviceNumber| -> Result<Vec<DeviceNumber>> {
            Err(CgroupError::Malformed {
                path: PathBuf::from("/sys/dev/block/8:0/slaves"),
                detail: "unreadable".to_owned(),
            })
        };
        assert!(aggregate_storage_io(path, &[device((8, 0), 1, 1)], failing).is_err());
    }

    #[test]
    fn a_charge_is_one_count_and_nothing_else() {
        let path = Path::new("/sys/fs/cgroup/x/memory.current");
        assert_eq!(
            parse_single_value(path, "7329304576\n").expect("a count"),
            7_329_304_576
        );
        for text in ["", "\n", "max\n", "-1\n", "1 2\n"] {
            assert!(
                matches!(
                    parse_single_value(path, text),
                    Err(CgroupError::Malformed { .. })
                ),
                "{text:?}"
            );
        }
    }

    /// A directory without the memory controller's files -- here an ordinary one -- reports the
    /// counter unavailable by name rather than a zero charge.
    #[test]
    fn a_missing_memory_counter_is_unavailable_not_zero() {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-job-cgroup-no-memory-{}",
            std::process::id()
        ));
        fs::create_dir(&directory).expect("scratch directory");
        let opened = open_directory(&directory).expect("open");
        let missing = read_charged_memory(&opened, &directory);
        fs::remove_dir(&directory).expect("remove scratch directory");
        assert!(
            matches!(
                missing,
                Err(CgroupError::CounterUnavailable { ref path })
                    if path == &directory.join("memory.current")
            ),
            "{missing:?}"
        );
    }

    #[test]
    fn every_cpu_counter_is_read_and_none_is_assumed() {
        let path = Path::new("/sys/fs/cgroup/x/cpu.stat");
        let stat = "usage_usec 1055017791\nuser_usec 889772499\nsystem_usec 165245292\n\
                    nice_usec 0\ncore_sched.force_idle_usec 0\nnr_periods 0\n";
        let cpu = parse_cpu_stat(path, stat).expect("all three counters");
        assert_eq!(cpu.usage_us.get(), 1_055_017_791);
        assert_eq!(cpu.user_us.get(), 889_772_499);
        assert_eq!(cpu.system_us.get(), 165_245_292);
        // A missing counter is named, never read as zero.
        let missing = parse_cpu_stat(path, "usage_usec 10\nuser_usec 7\n");
        assert!(
            matches!(
                missing,
                Err(CgroupError::MissingCounter {
                    counter: "system_usec",
                    ..
                })
            ),
            "{missing:?}"
        );
        // A key that only starts like the counter is not it.
        assert!(matches!(
            parse_cpu_stat(path, "usage_usec_x 1\nuser_usec 1\nsystem_usec 1\n"),
            Err(CgroupError::MissingCounter {
                counter: "usage_usec",
                ..
            })
        ));
        assert!(matches!(
            parse_cpu_stat(path, "usage_usec -1\nuser_usec 1\nsystem_usec 1\n"),
            Err(CgroupError::Malformed { .. })
        ));
        assert!(matches!(
            parse_cpu_stat(
                path,
                "usage_usec 9007199254740992\nuser_usec 1\nsystem_usec 1\n"
            ),
            Err(CgroupError::OutOfRange {
                counter: "usage_usec",
                ..
            })
        ));
    }

    /// The hierarchy root belongs to root: an unprivileged controller is refused authority there,
    /// by name, before it moves or enables anything.
    #[test]
    fn an_undelegated_cgroup_grants_no_authority() {
        // SAFETY: geteuid cannot fail.
        assert_ne!(unsafe { libc::geteuid() }, 0, "run this test unprivileged");
        let refused = CgroupAuthority::delegated_at(PathBuf::from(CGROUP_MOUNT));
        assert!(
            matches!(refused, Err(CgroupError::NotDelegated { .. })),
            "{refused:?}"
        );
    }
}
