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

use std::ffi::{CStr, CString, OsStr};
use std::fmt;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};

use crate::api::dto::JobId;
use crate::api::resources::{CpuMicros, ResourceUnitError};
use crate::metadata::WorkspaceIncarnation;

/// Where the unified cgroup hierarchy is mounted.
pub const CGROUP_MOUNT: &str = "/sys/fs/cgroup";

/// The leaf of the delegated cgroup that holds the controller process itself.
pub const CONTROLLER_LEAF: &str = "controller";

/// `CGROUP2_SUPER_MAGIC` from `include/uapi/linux/magic.h`.
const CGROUP2_SUPER_MAGIC: libc::c_long = 0x6367_7270;

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
    NotUnified { path: PathBuf, f_type: libc::c_long },
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
    #[error("job cgroup {path} still holds processes: its counters are not final")]
    Populated { path: PathBuf },
    #[error("{path}: {detail}")]
    Malformed { path: PathBuf, detail: String },
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

fn failed(call: &'static str, path: &Path) -> impl FnOnce(io::Error) -> CgroupError {
    let path = path.to_path_buf();
    move |source| CgroupError::Io { call, path, source }
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
    let available = fs::read_to_string(path.join("cgroup.controllers"))
        .map_err(failed("read", &path.join("cgroup.controllers")))?;
    require_controllers(path, &available, &Controller::ACCOUNTING)?;
    let request = Controller::ACCOUNTING
        .iter()
        .map(|controller| format!("+{controller}"))
        .collect::<Vec<_>>()
        .join(" ");
    let control = path.join("cgroup.subtree_control");
    fs::write(&control, request).map_err(failed("write", &control))
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
        if statfs.f_type != CGROUP2_SUPER_MAGIC {
            return Err(CgroupError::NotUnified {
                path: mount.to_path_buf(),
                f_type: statfs.f_type,
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
        let available = fs::read_to_string(root.join("cgroup.controllers"))
            .map_err(failed("read", &root.join("cgroup.controllers")))?;
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
            let file_type = entry.file_type().map_err(failed("stat", &entry.path()))?;
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

/// One job's cgroup, open for placing its first process and reading its counters.
#[derive(Debug)]
pub struct JobCgroup {
    identity: JobCgroupIdentity,
    path: PathBuf,
    directory: OwnedFd,
    procs: OwnedFd,
}

impl JobCgroup {
    fn open(identity: JobCgroupIdentity, path: PathBuf, directory: OwnedFd) -> Result<Self> {
        let available = read_at(&directory, c"cgroup.controllers")
            .map_err(failed("read", &path.join("cgroup.controllers")))?;
        require_controllers(&path, &available, &Controller::ACCOUNTING)?;
        let procs = open_at(directory.as_raw_fd(), c"cgroup.procs", libc::O_WRONLY)
            .map_err(failed("open", &path.join("cgroup.procs")))?;
        Ok(Self {
            identity,
            path,
            directory,
            procs,
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
        let events = read_at(&self.directory, c"cgroup.events")
            .map_err(failed("read", &self.path.join("cgroup.events")))?;
        populated(&self.path, &events)
    }

    /// The job's CPU so far: every process that has run in it, live or reaped.
    pub fn cpu(&self) -> Result<CgroupCpu> {
        read_cpu(&self.directory, &self.path)
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
    /// Have the process `command` spawns enter the job's cgroup after it forks and before it
    /// executes its program, so nothing the program runs is charged anywhere else. A failed
    /// entry fails the spawn.
    pub fn on_exec(self, command: &mut std::process::Command) {
        let procs = self.0;
        // SAFETY: the closure runs in the forked child before exec and makes one write(2), which
        // is async-signal-safe, on a descriptor the command owns until it is dropped. "0" names
        // the writing process, so the child moves itself and nothing else.
        unsafe {
            command.pre_exec(move || {
                let written = libc::write(procs.as_raw_fd(), c"0".as_ptr().cast(), 1);
                match written {
                    1 => Ok(()),
                    -1 => Err(io::Error::last_os_error()),
                    _ => Err(io::Error::from(io::ErrorKind::WriteZero)),
                }
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

    /// Collect the job's final counters, then remove the cgroup while its name still holds the
    /// cgroup admission created. A counter that cannot be read leaves the cgroup in place: once
    /// it is gone, nothing could ever read it again.
    pub fn retire(self) -> Result<JobCgroupTotals> {
        let totals = JobCgroupTotals { cpu: self.cpu()? };
        let current = open_directory(&self.path)?;
        let found = cgroup_id(&current).map_err(failed("fstat", &self.path))?;
        if found != self.identity.cgroup_id {
            return Err(CgroupError::Replaced {
                path: self.path,
                recorded: self.identity.cgroup_id,
                found,
            });
        }
        drop((current, self.directory));
        fs::remove_dir(&self.path).map_err(failed("rmdir", &self.path))?;
        Ok(totals)
    }
}

/// What a job cgroup counted over its whole life, read after its last process ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobCgroupTotals {
    pub cpu: CgroupCpu,
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
