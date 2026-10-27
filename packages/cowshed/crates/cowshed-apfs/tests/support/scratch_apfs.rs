//! Scratch roots for tests that drive the host's real APFS stack, or start processes that outlive
//! their starter (a stock Nx daemon).
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

#[cfg(test)]
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use cowshed_os::namespace::TestNamespace;

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
/// it, then removes the tree. The lane setup owns the predecessor sweep before fixtures start;
/// no fixture starts, retains or joins unrelated cleanup.
pub struct ScratchRoot {
    path: PathBuf,
    namespace: TestNamespace,
    label: String,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        let (namespace, path) = create_owned_root(label)?;
        Ok(Self {
            path,
            namespace,
            label: label.to_owned(),
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
        cowshed_os::eprintln!(
            "scratch root {} ({}): torn down in {:?}",
            self.path.display(),
            self.label,
            started.elapsed()
        );
    }
}

#[cfg(test)]
mod sweep_ownership_tests {
    use super::*;

    use super::super::scratch_namespace::{
        OWNER_STAGING, Owner, OwnerRecord, USER_ROOT, born_at, dead_owner, euid, owner_namespace,
        owner_pid, read_owner, reclaim_dead_root,
    };
    use cowshed_os::namespace::HostNamespace;

    /// A namespace whose minting pid no process can hold: macOS numbers processes below 100000,
    /// and a namespace's first character is a digit, so its pid is below `10 * 36^4`.
    fn dead_namespace() -> TestNamespace {
        TestNamespace::of(16_000_000, 0).unwrap()
    }

    #[test]
    fn scoped_recovery_refuses_a_live_owner_and_non_root_paths() {
        let root = ScratchRoot::new("live-recovery-refusal").unwrap();
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
        let sentinel = root.path().join("keep");
        fs::write(&sentinel, b"live owner's bytes").unwrap();
        let dead = HostNamespace::Test(dead_namespace()).macos_user_root(euid());
        for path in [
            root.path().to_owned(),
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
