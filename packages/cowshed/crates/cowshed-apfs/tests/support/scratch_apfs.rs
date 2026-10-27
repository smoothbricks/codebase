//! Scratch roots for tests that drive the host's real APFS stack, or start processes that outlive
//! their starter (a stock Nx daemon).
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Once};
use std::thread::JoinHandle;
use std::time::Instant;

use cowshed_os::namespace::TestNamespace;
use cowshed_os::test_backstop::FileBackstop as _;

use super::scratch_namespace::{
    USER_ROOT, create_owned_root, dead_owner, detach_images, tear_down_root,
};

#[path = "scratch_reclaim.rs"]
mod reclaim;

use super::scratch_namespace::processes::end_processes_in;

/// The directory every test run of this user shares, `/private/var/folders/cowshed/<euid>/t`
/// ([`cowshed_os::scratch::macos_scratch_root`]): shared state only, never a run's own tree. The
/// sweep's lock and record, the user's blank template and the lane pools are in it, and none of
/// them belongs to one test namespace.
pub(crate) static SCRATCH_PARENT: LazyLock<PathBuf> = LazyLock::new(|| {
    cowshed_os::scratch::macos_scratch_root()
        .unwrap_or_else(|error| panic!("real-APFS scratch parent: {error}"))
});

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
/// `/private/var/folders/cowshed/<euid>/<ns>` ([`cowshed_os::namespace::HostNamespace::macos_user_root`]), beside `t`
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

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static SWEEP: Once = Once::new();
        Self::new_with_sweep(label, &SWEEP, sweep_dead_runs)
    }

    /// A child fixture whose parent owns recovery if the child is killed. It cleans its own
    /// root on drop, but never starts or waits for a sweep of unrelated host runs.
    pub(crate) fn new_owned(label: &str) -> std::io::Result<Self> {
        let (namespace, path) = create_owned_root(label)?;
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The test namespace whose directory this root is: what a fixture whose store lives below it
    /// passes to every constructor it builds, so every name that store mints derives from it.
    pub fn namespace(&self) -> TestNamespace {
        self.namespace
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
        |path| fs::remove_dir_all(path),
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

#[cfg(test)]
mod sweep_ownership_tests {
    use super::*;

    use super::super::scratch_namespace::{
        OWNER_STAGING, Owner, OwnerRecord, born_at, euid, owner_namespace, owner_pid, read_owner,
        reclaim_dead_root,
    };
    use cowshed_os::namespace::HostNamespace;
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
