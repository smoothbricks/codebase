//! Scratch roots for tests that drive the host's real APFS stack, or start processes that outlive
//! their starter (a stock Nx daemon).
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Once};
use std::thread::JoinHandle;
use std::time::Instant;

use cowshed_os::namespace::{HostNamespace, TestNamespace};
use cowshed_os::test_backstop::{CommandBackstop as _, FileBackstop as _};

use super::{
    CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend, SystemCommandRunner,
    verify_no_symlinks,
};

#[path = "scratch_reclaim.rs"]
mod reclaim;

#[path = "scratch_ram.rs"]
pub(crate) mod ram;

#[path = "../../../cowshed-os/tests/support/scratch_processes.rs"]
pub(crate) mod processes;

use processes::end_processes_in;

/// The directory every test run of this user shares, `/private/var/folders/cowshed/<euid>/t`
/// ([`cowshed_os::scratch::macos_scratch_root`]): shared state only, never a run's own tree. The
/// sweep's lock and record, the user's blank template and the lane pools are in it, and none of
/// them belongs to one test namespace.
pub(crate) static SCRATCH_PARENT: LazyLock<PathBuf> = LazyLock::new(|| {
    cowshed_os::scratch::macos_scratch_root()
        .unwrap_or_else(|error| panic!("real-APFS scratch parent: {error}"))
});

/// The per-user root `cowshed setup` makes, `/private/var/folders/cowshed/<euid>`: the parent of
/// [`SCRATCH_PARENT`] and of every test namespace's directory, beside production's own entries
/// (`mnt`, `ram<revision>`). Spotlight's `mds` admits no volume mounted below it, where one
/// mounted below `/private/tmp` left `mds` one `AppleAPFSVolume` right it never releases
/// (01a_apfs.md, "Spotlight media references"). It is production's directory too: nothing in it
/// is a run's unless its name parses as a test namespace ([`owner_namespace`]).
static USER_ROOT: LazyLock<PathBuf> =
    LazyLock::new(|| HostNamespace::Production.macos_user_root(euid()));

/// This process's effective uid, whose per-user root holds the test namespaces' directories.
fn euid() -> u32 {
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    unsafe { libc::geteuid() }
}

/// Held while a sweep runs. Unserialized, concurrent sweeps raced each other for the same
/// abandoned images: every sweeper waited on the image lease another held for its detach (11.5 s
/// measured), then found the volume already unmounted under it and failed. It lies in `t`, which
/// is no test namespace, and its name spells no pid, so no sweep ever reclaims it. Other
/// checkouts' sweeps share it and write their own records into it, so it records nothing of
/// this one's.
fn sweep_lock() -> PathBuf {
    SCRATCH_PARENT.join("sweep.lock")
}

/// The run whose sweep last finished ([`run_id`]), written under [`sweep_lock`]: every run
/// sweeps its dead predecessors once, not once per test process. Its name spells no pid either.
fn sweep_record() -> PathBuf {
    SCRATCH_PARENT.join("sweep.run")
}

/// One test's disposable root: the directory of the test namespace it mints,
/// `/private/var/folders/cowshed/<euid>/<ns>` ([`HostNamespace::macos_user_root`]), beside `t`
/// ([`SCRATCH_PARENT`]), `mnt` and `ram<revision>` (01_storage.md, "Host namespace"; 08_testing.md,
/// "Integration tests (real substrate)"). The namespace is the root's whole identity. Its first
/// five characters spell the pid of the run that owns it, so a later run can tell a live root from
/// an abandoned one without any shared state, and a fixture whose store lives in the root passes
/// [`ScratchRoot::namespace`] to every constructor it builds, so every name that store mints
/// derives from it and none can be one production makes or reads. The label is said on stderr when
/// the root is made and torn down, never spelled in the path: a fixture store's
/// `store/run/<32 hex>.sock` must still fit `sockaddr_un.sun_path` (104 bytes) below it. The root
/// holds `.owner`, the pid and start time of the process that made it, published whole: the kernel
/// gives a pid to a new process once its holder is reaped, so a root is abandoned when nobody holds
/// its pid or a process that started at another instant does.
///
/// Dropping it ends every process working in its tree, detaches every image attached from below
/// it, then removes the tree. Only the root that started a predecessor sweep joins that sweep;
/// another root's teardown never inherits that unrelated work.
pub struct ScratchRoot {
    path: PathBuf,
    namespace: TestNamespace,
    label: String,
    sweep: Option<JoinHandle<()>>,
}

/// The reserved plist path enrolls a root for its namespace-derived gateway label.
pub(crate) const LAUNCH_AGENT_PLIST: &str = "launch-agent.plist";
impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static SWEEP: Once = Once::new();
        Self::new_with_sweep(label, &SWEEP, sweep_dead_runs)
    }

    /// A child fixture whose parent owns recovery if the child is killed. It cleans its own
    /// root on drop, but never starts or waits for a sweep of unrelated host runs.
    pub(crate) fn new_owned(label: &str) -> std::io::Result<Self> {
        let namespace = TestNamespace::mint().map_err(|error| {
            std::io::Error::other(format!(
                "mint the test namespace of scratch root {label}: {error}"
            ))
        })?;
        let path = create_root(namespace)?;
        cowshed_os::eprintln!("scratch root {}: {label}", path.display());
        Ok(Self {
            path,
            namespace,
            label: label.to_owned(),
            sweep: None,
        })
    }

    fn new_with_sweep(label: &str, once: &Once, sweep: fn(TestNamespace)) -> std::io::Result<Self> {
        let mut root = Self::new_owned(label)?;
        let namespace = root.namespace;
        // The starter owns this work through completion. Other roots can come and go without
        // waiting on its handle, while the live starter keeps its process's sweep from being
        // silently cut off on the normal path.
        once.call_once(|| {
            match std::thread::Builder::new()
                .name("scratch-sweep".to_owned())
                .spawn(move || sweep(namespace))
            {
                Ok(sweep) => root.sweep = Some(sweep),
                Err(error) => {
                    cowshed_os::eprintln!(
                        "scratch root: no sweep thread ({error}); sweeping dead runs now"
                    );
                    sweep(namespace);
                }
            }
        });
        Ok(root)
    }

    pub(crate) fn launch_agent_plist(&self) -> PathBuf {
        self.path.join(LAUNCH_AGENT_PLIST)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The test namespace whose directory this root is: what a fixture whose store lives below it
    /// passes to every constructor it builds, so every name that store mints derives from it.
    pub fn namespace(&self) -> TestNamespace {
        self.namespace
    }
}

/// Make `namespace`'s directory in the per-user root, 0700 and the user's alone, record this
/// process as its owner ([`record_owner`]) and return its path. The per-user root is `cowshed
/// setup`'s and never a fixture's (the Nx target `cowshed:apfs-test-root` makes it on a host setup
/// has not run on): one that is missing, or that this user cannot write, refuses here, by name.
fn create_root(namespace: TestNamespace) -> std::io::Result<PathBuf> {
    let path = HostNamespace::Test(namespace).macos_user_root(euid());
    let make = || {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "create the scratch root {} (its parent is the per-user root `cowshed setup` \
                         makes, or the Nx target `cowshed:apfs-test-root` on a host setup has not run on): {error}",
                        path.display()
                    ),
                )
            })
    };
    match make() {
        Ok(()) => {}
        // This process never made this name: its ordinals are its own. So the root is a dead
        // earlier holder's of the same pid, which the sweep takes for live; release what it left
        // through the owned cleanup, attached images included, then take the name.
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            tear_down_root(namespace, &path)?;
            make()?;
        }
        Err(error) => return Err(error),
    }
    let uid = euid();
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
        return Err(std::io::Error::other(format!(
            "the scratch root {} must be a 0700 directory owned by uid {uid}",
            path.display()
        )));
    }
    record_owner(&path)?;
    Ok(path)
}

/// Recover exactly the scratch root an owned child reported, after that child has been reaped.
/// This does not take the host-wide sweep lock, enumerate unrelated dead roots, or rewrite the
/// suite's sweep record. The parent's panic/kill witness uses the same cleanup as root drop. Only
/// a test namespace's directory in the per-user root whose owner is gone ([`dead_owner`]: nobody
/// holds its pid, or a process that started at another instant does) qualifies: never `t`,
/// production's `mnt` and `ram<revision>`, or anything else there. Its images are released as that
/// namespace.
pub fn reclaim_dead_root(path: &Path) -> std::io::Result<()> {
    match dead_owner(path).filter(|_| path.parent() == Some(USER_ROOT.as_path())) {
        Some(namespace) => tear_down_root(namespace, path),
        None => Err(std::io::Error::other(format!(
            "refusing to reclaim a scratch root without a dead owner: {}",
            path.display()
        ))),
    }
}

/// Unload only the gateway label derived from an enrolled fixture namespace.
/// An absent job is already settled; any other failure retains the owner root.
fn boot_out_enrolled_agent(namespace: TestNamespace, root: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(root.join(LAUNCH_AGENT_PLIST)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    let target = format!(
        "gui/{}/{}",
        euid(),
        HostNamespace::Test(namespace).launchd_label("gateway")
    );
    let output = std::process::Command::new("/bin/launchctl")
        .args(["bootout", &target])
        .output_bounded()?;
    // bootout may return before teardown finishes, and an already-removed job
    // has no successful bootout to repeat. The recorded job, not that exit code,
    // decides whether it is safe to remove the enrolled plist. The caller has
    // already ended and watched every process working in the fixture root.
    let printed = std::process::Command::new("/bin/launchctl")
        .args(["print", &target])
        .output_bounded()?;
    match printed.status.code() {
        Some(113) => Ok(()),
        _ => Err(std::io::Error::other(format!(
            "fixture agent {target} is not proved unloaded: launchctl print {}: {}; bootout {}: {}",
            printed.status,
            String::from_utf8_lossy(&printed.stderr).trim(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))),
    }
}

fn tear_down_root(namespace: TestNamespace, path: &Path) -> std::io::Result<()> {
    // End the root's workers before releasing any image they may hold.
    end_processes_in(|cwd| cwd.starts_with(path))?;
    boot_out_enrolled_agent(namespace, path)?;
    ram::release(namespace, path)?;
    let unreleased = detach_images(namespace, |image| {
        image.starts_with(path).then_some(namespace)
    })?;
    if !unreleased.is_empty() {
        return Err(std::io::Error::other(format!(
            "scratch root {} remains intact because its images could not be released: {unreleased:?}",
            path.display()
        )));
    }
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        let started = Instant::now();
        if let Err(error) = tear_down_root(self.namespace, &self.path) {
            cowshed_os::eprintln!(
                "scratch root {}: {error}; a later sweep reclaims it",
                self.path.display()
            );
        }
        let torn_down = started.elapsed();
        let swept = Instant::now();
        if let Some(sweep) = self.sweep.take()
            && sweep.join().is_err()
        {
            cowshed_os::eprintln!(
                "scratch root: its sweep of dead runs panicked before recording it; the next run sweeps"
            );
        }
        cowshed_os::eprintln!(
            "scratch root {} ({}): torn down in {torn_down:?}, then waited {:?} for its own sweep",
            self.path.display(),
            self.label,
            swept.elapsed()
        );
    }
}

/// Reclaim what runs that can no longer clean up after themselves left behind. Nothing runs after
/// a SIGKILL — a harness timeout, a bounded-exec force-kill, a `cargo test` killed mid-mount — so
/// the only protocol that always converges is that every run sweeps its dead predecessors. One
/// process of a run sweeps while the others wait on the lock, then find the run's sweep recorded
/// and return. A sweep killed with its process records nothing, so the next process to take the
/// lock, of this run or the next, sweeps.
///
/// A dead run's root is the directory of a test namespace: an entry of the per-user root that
/// parses as one and whose owner is gone ([`dead_owner`]): nobody holds its minting pid, or a
/// process that started at another instant than its `.owner` records does. Nothing else in the
/// per-user root is ever a run's, so nothing else is selected, ended in, released or removed:
/// not `t`, not
/// production's `mnt` (below it the workspaces mount, the one a gate runs in among them) or
/// `ram<revision>`, not a name no run made. Each dead run's images are released as its own
/// namespace; `namespace`, the starter's, only reads the kernel's attachment table.
///
/// The sweep is driven off the process and attachment tables rather than the directory listing,
/// because the residues outlive each other independently: a root directory can be deleted (by
/// hand, or by a tmp reaper) while its images stay attached, and an attached image keeps working
/// from a deleted backing file, as a process keeps working in a deleted directory. Ending and
/// detaching therefore select on the path the kernel still holds, not on what is on disk now.
/// Processes go first: one working in a dead run's volume would hold that volume attached.
fn sweep_dead_runs(namespace: TestNamespace) {
    let run = run_id();
    let (lock, record) = (sweep_lock(), sweep_record());
    let _sweeping = match lock_exclusive(&lock) {
        Ok(lock) => lock,
        Err(error) => {
            cowshed_os::eprintln!(
                "abandoned scratch roots stay until a later run: lock {}: {error}",
                lock.display()
            );
            return;
        }
    };
    if fs::read(&record).is_ok_and(|recorded| recorded == run.as_bytes()) {
        return;
    }
    let started = Instant::now();
    if let Err(error) = end_processes_in(|path| dead_owner(path).is_some()) {
        cowshed_os::eprintln!("abandoned scratch roots: {error}");
        return;
    }
    let unreleased = match detach_images(namespace, dead_owner) {
        Ok(unreleased) => unreleased,
        Err(error) => {
            cowshed_os::eprintln!(
                "abandoned scratch roots remain intact because image release could not be inventoried: {error}"
            );
            return;
        }
    };
    let entries = match fs::read_dir(USER_ROOT.as_path()) {
        Ok(entries) => entries,
        Err(error) => {
            cowshed_os::eprintln!(
                "abandoned scratch roots stay until a later run: list {}: {error}",
                USER_ROOT.display()
            );
            return;
        }
    };
    reclaim::reclaim_dead_roots(
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| dead_owner(path).is_some()),
        &unreleased,
        |path| {
            let namespace = dead_owner(path).ok_or_else(|| {
                std::io::Error::other(format!(
                    "scratch owner is no longer proved dead: {}",
                    path.display()
                ))
            })?;
            boot_out_enrolled_agent(namespace, path)?;
            ram::release(namespace, path)?;
            fs::remove_dir_all(path)
        },
        |path, error| {
            cowshed_os::eprintln!(
                "abandoned scratch root {} was not removed: {error}",
                path.display()
            );
        },
    );
    if let Err(error) = fs::write(&record, run.as_bytes()) {
        cowshed_os::eprintln!(
            "this run sweeps again: record its sweep in {}: {error}",
            record.display()
        );
    }
    cowshed_os::eprintln!("scratch root: swept dead runs in {:?}", started.elapsed());
}

/// This run's identity, as [`sweep_record`] records it: nextest gives every test process of one
/// run the same `NEXTEST_RUN_ID`; `cargo test` runs a binary's tests in one process, which sweeps
/// once, so its pid and the instant it swept name its run, where a pid alone could name an
/// earlier run that had the pid before.
fn run_id() -> String {
    std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        format!("pid {} at {now}", std::process::id())
    })
}

/// `path` opened (created if absent) and `flock`ed exclusively, waiting for any holder: the lock
/// lives as long as the returned file, and dies with its process however that process ends.
pub(crate) fn lock_exclusive(path: &Path) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock_bounded()?;
    Ok(file)
}

/// The test namespace whose directory in the per-user root holds `path`, given that directory
/// or any path below it. Only a name that parses as a test namespace (`TestNamespace::parse`:
/// seven base-36 characters, the first a digit) is one, so `t`, production's `mnt` and
/// `ram<revision>` and any name no run made name none, and the sweep never takes them for a run's.
fn owner_namespace(path: &Path) -> Option<TestNamespace> {
    let name = path
        .strip_prefix(USER_ROOT.as_path())
        .ok()?
        .components()
        .next()?
        .as_os_str()
        .to_str()?;
    TestNamespace::parse(name)
}

/// The pid of the run that owns a scratch root, given the root itself or any path below it: the
/// process that minted its test namespace.
fn owner_pid(path: &Path) -> Option<i32> {
    owner_namespace(path).and_then(|namespace| i32::try_from(namespace.owner_pid()).ok())
}

/// When a process started, as `proc_pidinfo(PROC_PIDTBSDINFO)` reports it (`pbi_start_tvsec` and
/// `pbi_start_tvusec`). A pid names a process only together with it: the kernel hands the pid to a
/// new process once its holder is reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Born {
    seconds: u64,
    microseconds: u64,
}

/// The process that made a root: its pid, which is its namespace's, and when it started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Owner {
    pid: i32,
    born: Born,
}

impl std::fmt::Display for Owner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} {} {}",
            self.pid, self.born.seconds, self.born.microseconds
        )
    }
}

/// What a root's [`OWNER_FILE`] says of its owner.
#[derive(Debug, Eq, PartialEq)]
enum OwnerRecord {
    /// No file: its maker was killed between `mkdir` and publishing it, or has not published it
    /// yet.
    Missing,
    Recorded(Owner),
    /// A file that cannot be read or is not an owner record, and why.
    Unreadable(String),
}

/// What the kernel says of whoever holds an owner's pid now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Holder {
    /// No process: `kill(pid, 0)` answers `ESRCH`.
    Nobody,
    /// A process that started at this instant.
    Born(Born),
    /// A process whose start the kernel will not say (it exited and is not reaped yet, or this
    /// process may not inspect it): not proved gone.
    Unreadable,
}

/// A root's owner record, published whole: written to [`OWNER_STAGING`], then renamed to this.
const OWNER_FILE: &str = ".owner";
const OWNER_STAGING: &str = ".owner.tmp";

/// Whether the run that minted a root's namespace, whose pid is `namespace_pid`, is gone, given
/// what the root records of its owner and who holds the pid now. Nobody holding the pid proves it;
/// so does a holder that started at another instant than the record says (the pid was reused). A
/// root with no record is judged by the pid alone, as every root was before records. A record that
/// names another pid than its namespace's, or cannot be read, is not judged: the root is kept.
fn owner_is_gone(namespace_pid: i32, record: &OwnerRecord, holder: Holder) -> bool {
    match (record, holder) {
        (OwnerRecord::Unreadable(_), _) => false,
        (OwnerRecord::Recorded(owner), _) if owner.pid != namespace_pid => false,
        (_, Holder::Nobody) => true,
        (OwnerRecord::Recorded(owner), Holder::Born(now)) => now != owner.born,
        (OwnerRecord::Missing, Holder::Born(_)) | (_, Holder::Unreadable) => false,
    }
}

/// The owner record `text` is: pid, start seconds and start microseconds, in decimal, and nothing
/// else.
fn parse_owner(text: &str) -> Option<Owner> {
    let mut fields = text.split_whitespace();
    let pid = fields.next()?.parse().ok()?;
    let seconds = fields.next()?.parse().ok()?;
    let microseconds = fields.next()?.parse().ok()?;
    fields.next().is_none().then_some(Owner {
        pid,
        born: Born {
            seconds,
            microseconds,
        },
    })
}

/// What the root `root` records of its owner.
fn read_owner(root: &Path) -> OwnerRecord {
    match fs::read_to_string(root.join(OWNER_FILE)) {
        Ok(text) => parse_owner(&text).map_or_else(
            || OwnerRecord::Unreadable("not a pid and a start time".to_owned()),
            OwnerRecord::Recorded,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => OwnerRecord::Missing,
        Err(error) => OwnerRecord::Unreadable(error.to_string()),
    }
}

/// Record this process as the owner of `root`: its pid and when it started, in one file that
/// appears whole (written aside, then renamed), so a reader never sees half a record. A run killed
/// before the rename leaves none, and its root is judged by its namespace's pid alone.
fn record_owner(root: &Path) -> std::io::Result<()> {
    // SAFETY: `getpid` has no preconditions and cannot fail.
    let pid = unsafe { libc::getpid() };
    let owner = Owner {
        pid,
        born: born_at(pid)?,
    };
    let (staging, published) = (root.join(OWNER_STAGING), root.join(OWNER_FILE));
    fs::write(&staging, format!("{owner}\n"))
        .and_then(|()| fs::rename(&staging, &published))
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!(
                    "record the owner of scratch root {}: {error}",
                    root.display()
                ),
            )
        })
}

/// When the process `pid` started, as `proc_pidinfo(PROC_PIDTBSDINFO)` reads it; an error when the
/// kernel will not say: the process is gone or a zombie, or this one may not inspect it.
fn born_at(pid: i32) -> std::io::Result<Born> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(std::io::Error::other)?;
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
    if written != size {
        let error = if written > 0 {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("returned {written} bytes, expected {size}"),
            )
        } else {
            std::io::Error::last_os_error()
        };
        return Err(std::io::Error::new(
            error.kind(),
            format!("proc_pidinfo({pid}, PROC_PIDTBSDINFO): {error}"),
        ));
    }
    // SAFETY: `proc_pidinfo` filled all `size` bytes.
    let info = unsafe { info.assume_init() };
    Ok(Born {
        seconds: info.pbi_start_tvsec,
        microseconds: info.pbi_start_tvusec,
    })
}

/// Whoever holds `pid` now: nobody when `kill(pid, 0)` answers `ESRCH`, else the process whose
/// start the kernel reads, or one it will not say anything of.
fn holder_of(pid: i32) -> Holder {
    if process_is_gone(pid) {
        Holder::Nobody
    } else {
        born_at(pid).map_or(Holder::Unreadable, Holder::Born)
    }
}

/// The test namespace of the dead run that owns `path`, given its root or any path below it: what
/// a sweep or a reclaim selects by, and the namespace it releases that run's images as. Its owner
/// is gone by [`owner_is_gone`]; a root whose record is unusable is kept, and says so.
fn dead_owner(path: &Path) -> Option<TestNamespace> {
    let namespace = owner_namespace(path)?;
    let pid = owner_pid(path)?;
    let root = HostNamespace::Test(namespace).macos_user_root(euid());
    let record = read_owner(&root);
    match &record {
        OwnerRecord::Unreadable(why) => cowshed_os::eprintln!(
            "scratch root {} is kept: its {OWNER_FILE} is unusable: {why}",
            root.display()
        ),
        OwnerRecord::Recorded(owner) if owner.pid != pid => cowshed_os::eprintln!(
            "scratch root {} is kept: its {OWNER_FILE} names pid {} and its namespace pid {pid}",
            root.display(),
            owner.pid
        ),
        OwnerRecord::Missing | OwnerRecord::Recorded(_) => {}
    }
    owner_is_gone(pid, &record, holder_of(pid)).then_some(namespace)
}

/// Whether no process holds `pid` any more. Signal 0 runs the existence and permission checks
/// without delivering anything, and `ESRCH` is the single answer that proves the owner is gone —
/// `EPERM` means it is alive under another user, so a run must not reclaim its root.
fn process_is_gone(pid: i32) -> bool {
    // SAFETY: `kill` with signal 0 only probes; it delivers nothing and touches no memory.
    let probe = unsafe { libc::kill(pid, 0) };
    probe != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// Select images by the backing path the kernel's I/O Registry holds, then let the production
/// backend reread and release every attachment of each one under its lease/pin/native-unmount
/// protocol. Never act on cached diskN, and never select from `hdiutil info`, which omits attached
/// images while another image attaches or detaches. A path the kernel holds twice is one image
/// with two attachments: it is released once, both detached, never refused as ambiguous.
///
/// The images are released concurrently, one thread each. Every release is an unmount under the
/// host's namespace lease and then a detach under its storage lease, and the two classes exclude
/// each other while a phase admits up to eight of one class (05_gateway.md, "Disk-lifecycle
/// lease"). One after another, every unmount and every detach of a root waited for a phase of its
/// own class on a loaded host; released together, their unmounts can share a namespace phase and
/// their detaches a storage phase. Each image still has exactly one holder.
///
/// Each image is released as the namespace `select` names for it, that of the root it lies in:
/// its disk tools take that namespace's lease, as its own fixture took them. `inventory` only
/// reads the kernel's attachment table.
///
/// Returns the backing paths whose individual release failed; an inventory failure is an error
/// because no root can then be proved safe to remove.
fn detach_images(
    inventory: TestNamespace,
    select: impl Fn(&Path) -> Option<TestNamespace>,
) -> std::io::Result<BTreeSet<PathBuf>> {
    let attached =
        SystemCommandRunner::new(HostNamespace::Test(inventory)).attached_disk_images()?;
    let images = attached
        .iter()
        .filter_map(|attached| match &attached.source {
            DiskImageSource::File(path) => Some(path.as_path()),
            DiskImageSource::Url(_) => None,
        })
        .filter_map(|image| select(image).map(|namespace| (image, namespace)))
        .collect::<BTreeMap<_, _>>();
    let release = release_image;
    let mut unreleased = BTreeSet::new();
    let mut report = |image: &Path, released: std::thread::Result<std::io::Result<()>>| {
        let error = match released {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error,
            Err(_) => std::io::Error::other("its release thread panicked"),
        };
        cowshed_os::eprintln!(
            "scratch image {} could not be released: {error}",
            image.display()
        );
        unreleased.insert(image.to_owned());
    };
    std::thread::scope(|scope| {
        let mut releasing = Vec::with_capacity(images.len());
        for (image, namespace) in images {
            match std::thread::Builder::new()
                .name("scratch-release".to_owned())
                .spawn_scoped(scope, move || release(namespace, image))
            {
                Ok(thread) => releasing.push((image, thread)),
                Err(error) => {
                    cowshed_os::eprintln!(
                        "scratch image {}: no thread to release it beside the others ({error}); \
                     releasing it in turn",
                        image.display()
                    );
                    report(image, Ok(release(namespace, image)));
                }
            }
        }
        for (image, thread) in releasing {
            report(image, thread.join());
        }
    });
    Ok(unreleased)
}

/// Release every attachment the kernel holds for `image` as the store host of `namespace` releases
/// an image under its root: refuse a path a symlink redirects, then the production backend ejects
/// each exact mounted volume through its owned root FD, then its physical whole device through
/// hdiutil.
pub(crate) fn release_image(namespace: TestNamespace, image: &Path) -> std::io::Result<()> {
    let root = USER_ROOT.as_path();
    if image.starts_with(root) {
        verify_no_symlinks(root, image).map_err(std::io::Error::other)?;
    }
    MacOsApfsBackend::new(SystemCommandRunner::new(HostNamespace::Test(namespace)))
        .release_image(image, DetachIntent::Release)
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod sweep_ownership_tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::mpsc::{self, Receiver, Sender};

    static RENDEZVOUS: Mutex<Option<(Sender<()>, Receiver<()>)>> = Mutex::new(None);

    fn blocked_sweep(_: TestNamespace) {
        let (started, release) = RENDEZVOUS.lock().unwrap().take().unwrap();
        started.send(()).unwrap();
        release.recv().unwrap();
    }

    /// A namespace whose minting pid no process can hold: macOS numbers processes below 100000,
    /// and a namespace's first character is a digit, so its pid is below `10 * 36^4`.
    fn dead_namespace() -> TestNamespace {
        TestNamespace::of(16_000_000, 0).unwrap()
    }

    fn born(seconds: u64, microseconds: u64) -> Born {
        Born {
            seconds,
            microseconds,
        }
    }

    /// A root's owner is gone when nobody holds its pid, or a process that started at another
    /// instant than its record says does: the kernel gives a pid to a new process once its holder
    /// is reaped, so a pid alone proves a live run only while no one else has been given it. No
    /// real process is needed: the decision takes what the kernel would say.
    #[test]
    fn an_owner_is_gone_when_nobody_or_a_process_born_at_another_time_holds_its_pid() {
        let pid = 4242;
        let started = born(1_761_000_000, 250_000);
        let record = OwnerRecord::Recorded(Owner { pid, born: started });

        assert!(owner_is_gone(pid, &record, Holder::Nobody), "dead");
        assert!(
            !owner_is_gone(pid, &record, Holder::Born(started)),
            "alive: the recorded process holds it"
        );
        for reused in [
            born(1_761_000_001, 250_000),
            born(1_761_000_000, 250_001),
            born(1_760_999_999, 250_000),
        ] {
            assert!(
                owner_is_gone(pid, &record, Holder::Born(reused)),
                "reused: {reused:?}"
            );
        }
        assert!(
            !owner_is_gone(pid, &record, Holder::Unreadable),
            "a holder this process may not inspect is not proved gone"
        );
    }

    /// A root with no record yet (its maker was killed between making it and publishing the
    /// record) is judged by its namespace's pid alone, as every root was before records.
    #[test]
    fn a_root_with_no_owner_record_is_judged_by_its_pid_alone() {
        let pid = 4242;
        let holder = Holder::Born(born(1_761_000_000, 0));
        assert!(owner_is_gone(pid, &OwnerRecord::Missing, Holder::Nobody));
        assert!(!owner_is_gone(pid, &OwnerRecord::Missing, holder));
        assert!(!owner_is_gone(
            pid,
            &OwnerRecord::Missing,
            Holder::Unreadable
        ));
    }

    /// A record that names another pid than its root's namespace, or cannot be read, is not
    /// judged: the root is kept, whatever holds either pid.
    #[test]
    fn a_record_that_names_another_pid_or_cannot_be_read_keeps_its_root() {
        let pid = 4242;
        let started = born(1_761_000_000, 250_000);
        let foreign = OwnerRecord::Recorded(Owner {
            pid: pid + 1,
            born: started,
        });
        let garbled = OwnerRecord::Unreadable("not a pid and a start time".to_owned());
        for holder in [
            Holder::Nobody,
            Holder::Born(started),
            Holder::Born(born(1, 1)),
            Holder::Unreadable,
        ] {
            assert!(!owner_is_gone(pid, &foreign, holder), "{holder:?}");
            assert!(!owner_is_gone(pid, &garbled, holder), "{holder:?}");
        }
    }

    /// An owner record is one line, pid then start seconds then microseconds, that reads back
    /// whole; anything else is no record.
    #[test]
    fn an_owner_record_reads_back_whole_and_nothing_else_is_one() {
        let owner = Owner {
            pid: 94_769,
            born: born(1_761_000_000, 7),
        };
        assert_eq!(owner.to_string(), "94769 1761000000 7");
        assert_eq!(parse_owner(&format!("{owner}\n")), Some(owner));
        for text in [
            "",
            "94769",
            "94769 1761000000",
            "94769 1761000000 7 9",
            "a 1761000000 7",
            "94769 1.5 7",
            "94769 -1 7",
        ] {
            assert_eq!(parse_owner(text), None, "{text:?}");
        }
    }

    #[test]
    fn scoped_recovery_refuses_a_live_owner_and_non_root_paths() {
        let root = ScratchRoot::new_owned("live-recovery-refusal").unwrap();
        assert_eq!(
            owner_namespace(root.path()),
            Some(root.namespace()),
            "a root names the namespace it was minted for"
        );
        let own = i32::try_from(std::process::id()).unwrap();
        assert_eq!(
            read_owner(root.path()),
            OwnerRecord::Recorded(Owner {
                pid: own,
                born: born_at(own).unwrap()
            }),
            "a root records the process that made it and when it started"
        );
        assert!(
            !root.path().join(OWNER_STAGING).exists(),
            "the record is published whole"
        );
        assert_eq!(dead_owner(root.path()), None, "its owner is alive");
        let sentinel = root.path().join("keep");
        fs::write(&sentinel, b"live owner's bytes").unwrap();
        let dead = HostNamespace::Test(dead_namespace()).macos_user_root(euid());
        for path in [
            root.path().to_owned(),
            sentinel.clone(),
            // Not a run's root, whatever the process table says of its name.
            SCRATCH_PARENT.to_path_buf(),
            USER_ROOT.to_path_buf(),
            USER_ROOT.join("mnt"),
            USER_ROOT.join("ram3"),
            // A dead owner's tree but not its root, and a root of the layout before test
            // namespaces, which nothing reclaims.
            dead.join("store"),
            SCRATCH_PARENT.join("16000000-0"),
        ] {
            assert!(
                reclaim_dead_root(&path).is_err(),
                "refuse {}",
                path.display()
            );
        }
        assert_eq!(fs::read(sentinel).unwrap(), b"live owner's bytes");
    }

    /// The sweep takes a path for a dead run's by the namespace its first component below the
    /// per-user root parses as, and only a test namespace's directory parses as one. Everything
    /// else there has none, so the sweep never ends a process in it, releases an image under it or
    /// removes it: production's `mnt` (below it the workspaces mount, the one this suite runs in
    /// among them) and `ram<revision>`, `t` and what it holds, and any name that is not a
    /// namespace's.
    #[test]
    fn only_a_test_namespaces_directory_has_an_owner() {
        let (user, shared) = (USER_ROOT.as_path(), SCRATCH_PARENT.as_path());
        let namespace = TestNamespace::of(94_769, 3).unwrap();
        let root = HostNamespace::Test(namespace).macos_user_root(euid());
        assert_eq!(root, user.join(namespace.as_str()));
        for path in [root.clone(), root.join("store/main.dmg")] {
            assert_eq!(
                owner_namespace(&path),
                Some(namespace),
                "{}",
                path.display()
            );
            assert_eq!(owner_pid(&path), Some(94_769), "{}", path.display());
        }

        let dead = dead_namespace();
        let dead_root = HostNamespace::Test(dead).macos_user_root(euid());
        assert_eq!(
            dead_owner(&dead_root),
            Some(dead),
            "no process holds its pid"
        );
        assert_eq!(dead_owner(&dead_root.join("store/main.dmg")), Some(dead));
        let live = HostNamespace::Test(TestNamespace::mint().unwrap()).macos_user_root(euid());
        assert_eq!(dead_owner(&live), None, "this process holds its pid");

        for path in [
            user.to_owned(),
            user.join("mnt"),
            user.join("mnt/smoothbricks/codebase/test-namespace"),
            user.join("ram3"),
            user.join("ram3-new"),
            user.join("ram3/12345-aBcDeF"),
            shared.to_owned(),
            shared.join("sweep.lock"),
            shared.join("templates-uid502-cowshed-test"),
            shared.join("pool-cargo-4242"),
            // A namespace is a root only directly in the per-user root.
            shared.join(namespace.as_str()),
            // Not a namespace: it starts with a letter, is not seven characters, or is not
            // lowercase.
            user.join("abcdefg"),
            user.join("0214h0"),
            user.join("0214H03"),
            // The layout before test namespaces, `<pid>-<n>`, which nothing reclaims.
            shared.join("4242-0"),
            Path::new("/private/tmp").join(namespace.as_str()),
        ] {
            assert_eq!(owner_namespace(&path), None, "{}", path.display());
            assert_eq!(dead_owner(&path), None, "{}", path.display());
        }
    }

    #[test]
    fn dropping_another_root_does_not_join_the_starters_sweep() {
        let (started, running) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        *RENDEZVOUS.lock().unwrap() = Some((started, blocked));
        let once = Once::new();
        let starter = ScratchRoot::new_with_sweep("sweep-owner", &once, blocked_sweep).unwrap();
        running.recv().unwrap();
        let other =
            ScratchRoot::new_with_sweep("not-sweep-owner", &once, |_| panic!("one sweep per run"))
                .unwrap();
        let other_path = other.path().to_owned();
        let (done, finished) = mpsc::channel();
        let dropping = std::thread::spawn(move || {
            drop(other);
            done.send(()).unwrap();
        });
        // The other root's drop is the event: one that waited for the blocked sweep it did not
        // start never returns, and the run's no-progress bound names this test.
        let independent = finished.recv();
        release.send(()).unwrap();
        dropping.join().unwrap();
        drop(starter);
        assert!(
            independent.is_ok(),
            "another root's teardown waited for the blocked sweep it did not start"
        );
        assert!(!other_path.exists(), "the root still cleans its own tree");
    }
}
