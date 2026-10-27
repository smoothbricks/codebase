//! Shared test-namespace ownership and scoped reclamation.
//!
//! Global dead-run scanning stays with the APFS guard; these are its existing root and image rules.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use cowshed_apfs::store_lock::verify_no_symlinks;
use cowshed_apfs::{
    CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend, SystemCommandRunner,
};
use cowshed_os::namespace::{HostNamespace, TestNamespace};

use processes::end_processes_in;

#[path = "../../../cowshed-os/tests/support/scratch_processes.rs"]
pub(crate) mod processes;
/// The per-user root `cowshed setup` makes, `/private/var/folders/cowshed/<euid>`: the parent of
/// the shared t directory and every test namespace's directory, beside production's own entries
/// (`mnt`, `ram<revision>`). Spotlight's `mds` admits no volume mounted below it, where one
/// mounted below `/private/tmp` left `mds` one `AppleAPFSVolume` right it never releases
/// (01a_apfs.md, "Spotlight media references"). It is production's directory too: nothing in it
/// is a run's unless its name parses as a test namespace ([`owner_namespace`]).
pub(super) static USER_ROOT: LazyLock<PathBuf> =
    LazyLock::new(|| HostNamespace::Production.macos_user_root(euid()));

/// This process's effective uid, whose per-user root holds the test namespaces' directories.
pub(super) fn euid() -> u32 {
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    unsafe { libc::geteuid() }
}
/// Mint and publish one owned namespace root for either a live image fixture or directory retirement.
pub(crate) fn create_owned_root(label: &str) -> std::io::Result<(TestNamespace, PathBuf)> {
    let namespace = TestNamespace::mint().map_err(|error| {
        std::io::Error::other(format!(
            "mint the test namespace of scratch root {label}: {error}"
        ))
    })?;
    let path = create_root(namespace)?;
    cowshed_os::eprintln!("scratch root {}: {label}", path.display());
    Ok((namespace, path))
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

pub(super) fn tear_down_root(namespace: TestNamespace, path: &Path) -> std::io::Result<()> {
    // End the root's workers before releasing any image they may hold.
    if let Err(error) = end_processes_in(|cwd| cwd.starts_with(path)) {
        cowshed_os::eprintln!("scratch root {}: {error}", path.display());
    }
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
/// The test namespace whose directory in the per-user root holds `path`, given that directory
/// or any path below it. Only a name that parses as a test namespace (`TestNamespace::parse`:
/// seven base-36 characters, the first a digit) is one, so `t`, production's `mnt` and
/// `ram<revision>` and any name no run made name none, and the sweep never takes them for a run's.
pub(super) fn owner_namespace(path: &Path) -> Option<TestNamespace> {
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
pub(super) fn owner_pid(path: &Path) -> Option<i32> {
    owner_namespace(path).and_then(|namespace| i32::try_from(namespace.owner_pid()).ok())
}

/// When a process started, as `proc_pidinfo(PROC_PIDTBSDINFO)` reports it (`pbi_start_tvsec` and
/// `pbi_start_tvusec`). A pid names a process only together with it: the kernel hands the pid to a
/// new process once its holder is reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Born {
    seconds: u64,
    microseconds: u64,
}

/// The process that made a root: its pid, which is its namespace's, and when it started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Owner {
    pub(super) pid: i32,
    pub(super) born: Born,
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
pub(super) enum OwnerRecord {
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
pub(super) const OWNER_STAGING: &str = ".owner.tmp";

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
pub(super) fn read_owner(root: &Path) -> OwnerRecord {
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
pub(super) fn born_at(pid: i32) -> std::io::Result<Born> {
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
pub(super) fn dead_owner(path: &Path) -> Option<TestNamespace> {
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
pub(super) fn detach_images(
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
mod ownership_tests {
    use super::*;
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
}
