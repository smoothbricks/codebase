//! Scratch roots for tests that drive the host's real APFS stack, or start processes that outlive
//! their starter (a stock Nx daemon).
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

#[cfg(test)]
use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Instant;

use cowshed_os::fork_lock::Fenced;
use cowshed_os::namespace::TestNamespace;
use cowshed_os::test_backstop::ChildBackstop as _;

use super::scratch_namespace::{create_owned_root, tear_down_root};

/// One test's disposable root: the directory of the test namespace it mints,
/// `/private/var/folders/cowshed/<euid>/<ns>` ([`cowshed_os::namespace::HostNamespace::macos_user_root`]), beside `t`
/// ([`cowshed_os::scratch::macos_scratch_root`]), `mnt` and `ram<revision>` (01_storage.md, "Host namespace"; 08_testing.md,
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
/// it, then removes the tree. A run killed before its Drop leaves that work to the root's
/// [`Keeper`], which does it the moment the run dies, so no mount outlives its owner until the next
/// lane's sweep. The lane setup owns the predecessor sweep before fixtures start; no fixture starts,
/// retains or joins unrelated cleanup.
pub struct ScratchRoot {
    path: PathBuf,
    namespace: TestNamespace,
    label: String,
    keeper: Keeper,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        let (namespace, path) = create_owned_root(label)?;
        let keeper = match Keeper::start(&path) {
            Ok(keeper) => keeper,
            Err(error) => {
                // A root no keeper watches is not handed out: a kill would leak what it holds.
                if let Err(teardown) = tear_down_root(namespace, &path) {
                    cowshed_os::eprintln!(
                        "scratch root {}: {teardown}; a later sweep reclaims it",
                        path.display()
                    );
                }
                return Err(error);
            }
        };
        Ok(Self {
            path,
            namespace,
            label: label.to_owned(),
            keeper,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The test namespace whose directory this root is: what a fixture whose store lives below it
    /// passes to every constructor it builds, so every name that store mints derives from it.
    pub fn namespace(&self) -> TestNamespace {
        self.namespace
    }

    /// The pid of the root's [`Keeper`], which outlives a killed owner.
    pub fn keeper(&self) -> u32 {
        self.keeper.child.id()
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
        // Torn down, or left to the sweep while this owner lives: either way the keeper leaves
        // the root alone.
        let kept_by = self.keeper();
        let keeper = match self.keeper.release(&self.path) {
            Ok(status) => status.to_string(),
            Err(error) => format!("not reaped: {error}"),
        };
        cowshed_os::eprintln!(
            "scratch root {} ({}): torn down in {:?}; keeper {kept_by} released, {keeper}",
            self.path.display(),
            self.label,
            started.elapsed()
        );
    }
}

/// The argument that starts `cowshed-test-sweep` keeping one root ([`Keeper`]).
pub(crate) const KEEP_ARGUMENT: &str = "--keep";
/// What an owner writes its keeper once it has torn its root down itself, before it closes the
/// pipe. The pipe's end without it means the owner died: its descriptor closed with it.
pub(crate) const KEEPER_RELEASED: &[u8] = b"released\n";
/// What a keeper writes its owner once it has checked the root and keeps it: the owner hands
/// the root out only after reading it.
pub(crate) const KEEPER_KEEPING: &[u8] = b"keeping\n";

/// The process that reclaims a root whose owner dies without dropping it: the lane's own
/// `cowshed-test-sweep --keep <root>`, in a process group of its own, so the runner's kill of the
/// test's group (its no-progress bound) never reaches it, working in `/`, so the root's own
/// teardown never ends it. It reads a pipe only the owner writes: [`KEEPER_RELEASED`] means the
/// owner tore the root down; end of input without it means the owner died, and the keeper then
/// ends the root's processes, releases its images and removes it, as Drop would have. A stop that
/// ends the keeper too leaves the root to the next lane's sweep: on Darwin a job's conclusion or
/// cancellation retires the descendants the job retained, the keeper among them (11_shell.md). It
/// holds no output of the test: what it says goes to `keeper.log` in the shared scratch
/// directory, never to a runner that is waiting for the test's own streams to close.
struct Keeper {
    child: Child,
    /// The owner's end of the keeper's input, closed under the fork lock as every descriptor whose
    /// release a peer observes.
    release: Option<Fenced<std::io::PipeWriter>>,
}

impl Keeper {
    fn start(root: &Path) -> std::io::Result<Self> {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;

        let executable = keeper_executable()?;
        let log_path = cowshed_os::scratch::macos_scratch_root()?.join("keeper.log");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "open the scratch keepers' log {}: {error}",
                        log_path.display()
                    ),
                )
            })?;
        // Darwin has no `pipe2`: each pipe is made, then marked close-on-exec. Made under the
        // exclusive fork lock, no concurrent spawn carries an end past its `exec`, so the owner
        // stays the only writer of the keeper's input (01c_macos.md, "Darwin has no `pipe2` and
        // no `SOCK_CLOEXEC`").
        let ((input, release), (word, said)) =
            cowshed_os::fork_lock::create(|| Ok((std::io::pipe()?, std::io::pipe()?)))?;
        // The command, and with it this process's copy of the keeper's ends, is gone once spawned.
        let child = std::process::Command::new(&executable)
            .arg(KEEP_ARGUMENT)
            .arg(root)
            .current_dir("/")
            .stdin(input)
            .stdout(said)
            .stderr(log)
            .process_group(0)
            .spawn_locked()
            .map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!(
                        "start {} keeping scratch root {}: {error}",
                        executable.display(),
                        root.display()
                    ),
                )
            })?;
        let mut keeper = Self {
            child,
            release: Some(Fenced::new(release)),
        };
        let mut heard = [0; KEEPER_KEEPING.len()];
        match hear(word, keeper.child.id(), &mut heard) {
            Ok(length) if heard[..length] == *KEEPER_KEEPING => Ok(keeper),
            outcome => {
                // It never kept the root: release it, so it cannot reclaim the root the caller
                // tears down itself, and say how it ended.
                let ended = keeper.release(root);
                // A keeper that said nothing in time keeps its expiry's kind; any other refusal is
                // the keeper's, said in its log.
                let kind = outcome
                    .as_ref()
                    .map_or_else(std::io::Error::kind, |_| std::io::ErrorKind::Other);
                Err(std::io::Error::new(
                    kind,
                    format!(
                        "{} did not keep scratch root {} ({}; it {}); its reason is in {}",
                        executable.display(),
                        root.display(),
                        match outcome {
                            Ok(length) =>
                                format!("it said {:?}", String::from_utf8_lossy(&heard[..length])),
                            Err(error) => error.to_string(),
                        },
                        match ended {
                            Ok(status) => status.to_string(),
                            Err(error) => format!("could not be reaped: {error}"),
                        },
                        log_path.display()
                    ),
                ))
            }
        }
    }

    /// Tell the keeper the owner tears the root down itself, then reap it: on reading this it
    /// exits without touching the root. A keeper already gone fails the write, which is said here;
    /// its exit status is the caller's to report.
    fn release(&mut self, root: &Path) -> std::io::Result<std::process::ExitStatus> {
        if let Some(release) = self.release.take()
            && let Err(error) = (&*release).write_all(KEEPER_RELEASED)
        {
            cowshed_os::eprintln!(
                "scratch root {}: cannot release its keeper {}: {error}",
                root.display(),
                self.child.id()
            );
        }
        self.child.wait_bounded()
    }
}

/// Read what the keeper `pid` says on `word` into `heard`, within the handshake bound
/// (05_gateway.md, "Deadlines"): [`KEEPER_KEEPING`] once it keeps the root, or less when it exits
/// first and its end closes. Answers how much it said; the bound expiring is an error that names
/// the keeper and what it had said.
fn hear(
    mut word: std::io::PipeReader,
    pid: u32,
    heard: &mut [u8; KEEPER_KEEPING.len()],
) -> std::io::Result<usize> {
    use cowshed_os::deadline;
    use std::os::fd::AsRawFd as _;

    let bound = deadline::start("hear a scratch root's keeper", deadline::HANDSHAKE);
    let mut length = 0;
    while length < heard.len() {
        let Some(left) = bound.remaining(deadline::clock()) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "{}: keeper {pid} said {:?} in {:?}, past the {} handshake bound of {:?}",
                    bound.op(),
                    String::from_utf8_lossy(&heard[..length]),
                    bound.elapsed(deadline::clock()),
                    deadline::HANDSHAKE.class,
                    deadline::HANDSHAKE.within
                ),
            ));
        };
        let mut ready = libc::pollfd {
            fd: word.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialized pollfd, exclusively borrowed for the call, naming the
        // descriptor `word` keeps open.
        match unsafe { libc::poll(&raw mut ready, 1, deadline::poll_timeout(left)) } {
            -1 => {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
            // Expired: the next turn says so.
            0 => {}
            _ => match word.read(&mut heard[length..]) {
                Ok(0) => break,
                Ok(read) => length += read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            },
        }
    }
    Ok(length)
}

/// The lane's `cowshed-test-sweep`, which keeps roots: the nearest one among the directories
/// holding this executable, itself included when it is that program. Cargo puts it in the target
/// profile directory (`target/<profile>/`), an ancestor of every test and example binary of that
/// build whatever its layout (`deps/`, `examples/`, `build/<package>/<hash>/out/`), and the
/// lane's nextest archive keeps that tree. None is an error that names where it looked and the
/// remedy.
fn keeper_executable() -> std::io::Result<PathBuf> {
    const NAME: &str = "cowshed-test-sweep";
    let current = std::env::current_exe()?;
    current
        .ancestors()
        .skip(1)
        .map(|directory| directory.join(NAME))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            std::io::Error::other(format!(
                "no {NAME} can keep this scratch root: none is in a directory holding {}; build \
                 it with the workspace (the lane's nextest archive carries it), or `cargo build \
                 -p cowshed-apfs --bin {NAME}`",
                current.display()
            ))
        })
}

#[cfg(test)]
mod sweep_ownership_tests {
    use super::*;

    use super::super::scratch_namespace::{
        OWNER_STAGING, Owner, OwnerRecord, USER_ROOT, born_at, dead_owner, euid, owner_namespace,
        owner_pid, read_owner, reclaim_kept_root, root_namespace,
    };
    use cowshed_os::namespace::HostNamespace;

    /// A namespace whose minting pid no process can hold: macOS numbers processes below 100000,
    /// and a namespace's first character is a digit, so its pid is below `10 * 36^4`.
    fn dead_namespace() -> TestNamespace {
        TestNamespace::of(16_000_000, 0).unwrap()
    }

    /// A root records the process that made it, published whole, and is kept by a keeper that
    /// process started in a process group of its own, outside the group a runner kills. A keeper
    /// keeps and reclaims exactly a test namespace's directory in the per-user root; anything else
    /// it refuses, touching nothing.
    #[test]
    fn a_root_records_its_owner_and_is_kept_from_outside_its_group() {
        let root = ScratchRoot::new("owner-and-keeper").unwrap();
        let shared = cowshed_os::scratch::macos_scratch_root().unwrap();
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
        let keeper = i32::try_from(root.keeper()).unwrap();
        let lineage = cowshed_os::process::lineage(keeper)
            .unwrap()
            .expect("the keeper runs while its root is held");
        assert_eq!(lineage.ppid, own, "the root's owner started its keeper");
        assert_eq!(
            lineage.pgid, keeper,
            "the keeper leads a process group of its own"
        );
        // SAFETY: `getpgrp` reads this process's group; it takes no pointers and cannot fail.
        let group = unsafe { libc::getpgrp() };
        assert_ne!(
            lineage.pgid, group,
            "the keeper is outside its owner's group"
        );
        assert_eq!(
            root_namespace(root.path()),
            Some(root.namespace()),
            "a keeper keeps a namespace's root"
        );
        let sentinel = root.path().join("keep");
        fs::write(&sentinel, b"live owner's bytes").unwrap();
        let dead = HostNamespace::Test(dead_namespace()).macos_user_root(euid());
        for path in [
            sentinel.clone(),
            // Not a run's root, whatever the process table says of its name.
            shared.clone(),
            USER_ROOT.to_path_buf(),
            USER_ROOT.join("mnt"),
            USER_ROOT.join("ram3"),
            // A dead owner's tree but not its root, and a root of the layout before test
            // namespaces, which nothing reclaims.
            dead.join("store"),
            shared.join("16000000-0"),
        ] {
            assert_eq!(root_namespace(&path), None, "keep {}", path.display());
            assert!(
                reclaim_kept_root(&path).is_err(),
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
        let shared_root = cowshed_os::scratch::macos_scratch_root().unwrap();
        let (user, shared) = (USER_ROOT.as_path(), shared_root.as_path());
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
}
