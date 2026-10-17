//! The installed cowshed is one slot, and this is what decides who may fill it
//! (05_gateway.md "The installed cowshed": "The install slot" and "Build records and the basis").
//!
//! Two installers once ran within the hour, neither aware of the other: one installed a gc fix,
//! the other had a full-width build running from a commit that predated it, and setup's guard
//! compared commit *times*. A sibling committed later sorts as newer while carrying none of the
//! other's fixes, so the second install would have taken the fix back out and said nothing.
//!
//! Two rules close that, each the only way past its own failure:
//!
//! * **One install at a time.** [`InstallSlot`] is an exclusive `flock(2)` on
//!   `dev.cowshed/install.lock` — the lock `lockf(1)` takes, so a shell `lockf <file> …` waits
//!   in the same line — held for the whole install. A second installer waits, and judges against
//!   whatever was installed while it waited, because the judgement runs after the lock is taken.
//! * **Contain what is installed.** [`judge`] admits a build only when an adopted project's own
//!   `main` holds its commit, and that main shows the build holding every change of the installed
//!   commit, by ancestry or patch identity — `cowshed rm`'s containment
//!   (`GitRepository::commits_not_held`). Main is the authority because it is the one repository
//!   that outlives every shed: the commit a shed built from may be rebased away when it lands,
//!   and the shed itself removed, while main only moves forward.

use crate::build_record::{BuildRecord, lost_commits_refusal, unproven_refusal};
use cowshed_core::git::GitRepository;
use cowshed_core::runtime::project::DEFAULT_LANDING_BRANCH;
use cowshed_core::{CowshedError, Result};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The install slot's lock file, beside `bin` in `dev.cowshed`. Never removed: uninstall removes
/// what the slot holds, not the line to fill it.
pub const INSTALL_LOCK_NAME: &str = "install.lock";

/// The install slot, held: an exclusive lock on [`INSTALL_LOCK_NAME`], which the kernel drops
/// when this value is dropped or the process dies, so no crash leaves the slot held.
#[derive(Debug)]
pub struct InstallSlot {
    _lock: File,
}

impl InstallSlot {
    /// Take the slot, waiting in line behind any install that holds it.
    ///
    /// The wait is said on stderr before it starts: an install that sits silent behind another
    /// one reads as a hang.
    pub async fn take(support_directory: &Path) -> Result<Self> {
        let path = support_directory.join(INSTALL_LOCK_NAME);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|error| slot_error(&path, "open", &error))?;
        match flock(&lock, libc::LOCK_EX | libc::LOCK_NB) {
            Ok(()) => return Ok(Self { _lock: lock }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(slot_error(&path, "lock", &error)),
        }
        eprintln!(
            "cowshed: another install holds {}; waiting for it to finish",
            path.display()
        );
        let waited = path.clone();
        tokio::task::spawn_blocking(move || flock(&lock, libc::LOCK_EX).map(|()| lock))
            .await
            .map_err(|error| {
                CowshedError::internal(format!("waiting for {} failed: {error}", waited.display()))
            })?
            .map(|lock| Self { _lock: lock })
            .map_err(|error| slot_error(&path, "lock", &error))
    }
}

fn flock(file: &File, operation: libc::c_int) -> io::Result<()> {
    loop {
        // SAFETY: `file` owns a live descriptor for the duration of the call; `flock` does not
        // consume it.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn slot_error(path: &Path, operation: &str, error: &io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "could not {operation} the install slot {}: {error}",
            path.display()
        ),
        "repair ~/Library/Application Support/dev.cowshed, then rerun the install",
    )
}

/// The adopted mains that judge an install, with the store whose controller Git reads them: the
/// CLI process serves the host store, a test its own (04_sandbox.md, "Exec pipeline" step 4).
#[derive(Clone)]
pub struct Mains {
    pub store: Arc<Path>,
    pub roots: Vec<PathBuf>,
}

/// Whether the build at `source`, recording `candidate`, may replace the installed build
/// recording `installed`, judged in the adopted mains of `mains`.
///
/// The main whose `main` branch holds the candidate's commit is the one that judges it. No such
/// main is a refusal: a commit only a shed holds may be rebased away when it lands, so what it
/// contains is not yet a question with a lasting answer. In that main, every commit the installed
/// build carries must be held by the candidate; one that is not is named in the refusal. A
/// comparison git cannot make — the installed commit absent from that main — is a refusal too:
/// not knowing is not permission, and installing on it is the silent revert this replaces.
pub async fn judge(
    source: &Path,
    candidate: &BuildRecord,
    installed: &BuildRecord,
    mains: &Mains,
) -> Result<()> {
    let main_ref = format!("refs/heads/{DEFAULT_LANDING_BRANCH}");
    let mut searched = Vec::with_capacity(mains.roots.len());
    for main in &mains.roots {
        let repository = GitRepository::from_root(&mains.store, main);
        match repository
            .commit_is_ancestor(&candidate.commit, &main_ref)
            .await
        {
            Ok(true) => {
                let lost = repository
                    .commits_not_held(&candidate.commit, &installed.commit)
                    .await
                    .map_err(|error| {
                        unproven_refusal(
                            source,
                            candidate,
                            installed,
                            &format!(
                                "{} holds the build's commit on {DEFAULT_LANDING_BRANCH} but \
                                 cannot compare it with the installed one: {}",
                                main.display(),
                                error.message
                            ),
                        )
                    })?;
                return match lost.is_empty() {
                    true => Ok(()),
                    false => Err(lost_commits_refusal(source, candidate, installed, &lost)),
                };
            }
            Ok(false) => searched.push(format!(
                "{}'s {DEFAULT_LANDING_BRANCH} does not hold it",
                main.display()
            )),
            Err(error) => searched.push(format!(
                "{} could not be read: {}",
                main.display(),
                error.message
            )),
        }
    }
    let reason = match searched.is_empty() {
        true => String::from("no project is adopted on this host to judge it against"),
        false => format!(
            "its commit is on no adopted project's {DEFAULT_LANDING_BRANCH} ({}); a build's \
             contents are judged once its commit is on {DEFAULT_LANDING_BRANCH}, so land it and \
             install the build of {DEFAULT_LANDING_BRANCH}",
            searched.join("; ")
        ),
    };
    Err(unproven_refusal(source, candidate, installed, &reason))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cowshed_os::fork_lock::Run as _;
    use std::ffi::OsStr;
    use std::fs;
    use std::process::Command;

    /// A scratch repository standing in for an adopted main: a real git repository on a real
    /// filesystem, so the containment it answers is git's own. What it is not is the host's
    /// adopted-project registry; [`judge`] is handed its path the way the registry would.
    pub(crate) struct Main {
        pub root: PathBuf,
        /// The fixture's own store under the canonical `/tmp` parent, where its controller Git
        /// writes sandbox profiles (08_testing.md, "Fixtures").
        store: Arc<Path>,
    }

    impl Main {
        pub(crate) fn new(label: &str) -> Self {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "cowshed-basis-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("main root");
            let store = fs::canonicalize("/tmp")
                .expect("canonical /tmp")
                .join(format!(
                    "cowshed-basis-store-{label}-{}-{nonce}",
                    std::process::id()
                ));
            fs::create_dir_all(&store).expect("fixture store");
            let main = Self {
                root,
                store: Arc::from(store),
            };
            main.git(["init", "-q", "-b", DEFAULT_LANDING_BRANCH, "."]);
            main.commit("base.txt", "base\n", "base", 1_000);
            main
        }

        pub(crate) fn git<I, S>(&self, args: I) -> String
        where
            I: IntoIterator<Item = S>,
            S: AsRef<OsStr>,
        {
            git_at(&self.root, args, None)
        }

        /// Commit `contents` to `file` on the current branch at commit time `time`, and return
        /// the build record a release build of that commit would carry.
        pub(crate) fn commit(
            &self,
            file: &str,
            contents: &str,
            message: &str,
            time: u64,
        ) -> BuildRecord {
            fs::write(self.root.join(file), contents).expect("write file");
            self.git(["add", "-A"]);
            git_at(&self.root, ["commit", "-q", "-m", message], Some(time));
            self.head(time)
        }

        pub(crate) fn head(&self, time: u64) -> BuildRecord {
            BuildRecord {
                commit: self.git(["rev-parse", "HEAD"]).trim().to_owned(),
                commit_time: time,
            }
        }

        /// This main alone, as the adopted-project registry would hand it to [`judge`].
        pub(crate) fn mains(&self) -> Mains {
            Mains {
                store: Arc::clone(&self.store),
                roots: vec![self.root.clone()],
            }
        }
    }

    impl Drop for Main {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
            let _ = fs::remove_dir_all(&self.store);
        }
    }

    fn git_at<I, S>(root: &Path, args: I, time: Option<u64>) -> String
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(root)
            .args(args)
            .env("GIT_AUTHOR_NAME", "fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
            .env("GIT_COMMITTER_NAME", "fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null");
        if let Some(time) = time {
            let date = format!("@{time} +0000");
            command
                .env("GIT_AUTHOR_DATE", &date)
                .env("GIT_COMMITTER_DATE", &date);
        }
        let output = command.output_locked().expect("run git");
        assert!(
            output.status.success(),
            "git failed in {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("git output is UTF-8")
    }

    const SOURCE: &str = "/Users/dev/checkout/dist/bin/darwin-arm64/cowshed";

    async fn judged(main: &Main, candidate: &BuildRecord, installed: &BuildRecord) -> Result<()> {
        judge(Path::new(SOURCE), candidate, installed, &main.mains()).await
    }

    /// Today's collision, reproduced: two siblings of one parent on main, the later-committed one
    /// lacking the earlier one's gc fix. The clock calls it newer; containment names the fix it
    /// would remove, with both builds and the override.
    #[tokio::test]
    async fn a_later_sibling_that_lacks_the_installed_fix_is_refused_naming_the_fix() {
        let main = Main::new("sibling");
        main.git(["checkout", "-q", "-b", "gc-fix"]);
        let installed = main.commit(
            "gc.rs",
            "judge strays by the ignore rules\n",
            "fix(cowshed): judge strays by the project ignore rules",
            2_000,
        );
        main.git(["checkout", "-q", DEFAULT_LANDING_BRANCH]);
        let sibling = main.commit(
            "release.txt",
            "next prerelease\n",
            "chore(release): prepare next prerelease",
            3_000,
        );

        let refusal = judged(&main, &sibling, &installed)
            .await
            .expect_err("a sibling lacking the installed fix is refused");
        assert_eq!(refusal.code.as_str(), "conflict");
        assert!(refusal.message.contains(SOURCE), "{}", refusal.message);
        assert!(
            refusal.message.contains(sibling.short_commit()),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.contains(installed.short_commit()),
            "{}",
            refusal.message
        );
        assert!(
            refusal
                .message
                .contains("fix(cowshed): judge strays by the project ignore rules"),
            "the refusal names the fix it would remove: {}",
            refusal.message
        );
        assert!(refusal.hint.contains("--downgrade"), "{}", refusal.hint);
        assert!(
            refusal.hint.contains(installed.short_commit()),
            "{}",
            refusal.hint
        );
        eprintln!("refusal: {}\nhint: {}", refusal.message, refusal.hint);
    }

    /// A build of main that descends from the installed commit holds everything it carries.
    #[tokio::test]
    async fn a_descendant_on_main_is_admitted() {
        let main = Main::new("descendant");
        let installed = main.commit("gc.rs", "fixed\n", "fix(cowshed): the gc fix", 2_000);
        let newer = main.commit("next.rs", "next\n", "feat(cowshed): the next thing", 1_500);
        judged(&main, &newer, &installed)
            .await
            .expect("a descendant holds the installed commit, whatever the clock says");
    }

    /// Work that reached main under another commit — rebased when it landed, cherry-picked, or
    /// squashed from a single commit — is held by patch identity, so the build of main that
    /// carries it is admitted although the installed commit is no ancestor of it.
    #[tokio::test]
    async fn a_fix_that_landed_under_another_commit_is_held_by_patch_identity() {
        let main = Main::new("rebased");
        main.git(["checkout", "-q", "-b", "shed"]);
        let installed = main.commit("gc.rs", "fixed\n", "fix(cowshed): the gc fix", 2_000);
        main.git(["checkout", "-q", DEFAULT_LANDING_BRANCH]);
        main.commit(
            "other.rs",
            "other\n",
            "feat(cowshed): landed meanwhile",
            2_500,
        );
        git_at(
            &main.root,
            ["cherry-pick", "--allow-empty", &installed.commit],
            Some(3_000),
        );
        let landed = main.head(3_000);
        assert_ne!(landed.commit, installed.commit);
        judged(&main, &landed, &installed)
            .await
            .expect("the rebased fix is held by patch identity");
    }

    /// The limit of patch identity, measured rather than assumed: two installed commits squashed
    /// into one on main have no single patch-identical commit, so the squash does not count as
    /// holding them. The refusal names both, which is how an operator sees why.
    #[tokio::test]
    async fn a_multi_commit_squash_is_not_recognised_and_says_so() {
        let main = Main::new("squash");
        main.git(["checkout", "-q", "-b", "shed"]);
        main.commit("a.rs", "a\n", "fix(cowshed): first half", 2_000);
        let installed = main.commit("b.rs", "b\n", "fix(cowshed): second half", 2_100);
        main.git(["checkout", "-q", DEFAULT_LANDING_BRANCH]);
        main.git(["merge", "-q", "--squash", "shed"]);
        git_at(
            &main.root,
            ["commit", "-q", "-m", "fix(cowshed): both halves"],
            Some(3_000),
        );
        let squashed = main.head(3_000);
        let refusal = judged(&main, &squashed, &installed)
            .await
            .expect_err("a multi-commit squash is not patch-identical to either half");
        assert!(refusal.message.contains("2 commits"), "{}", refusal.message);
        assert!(
            refusal.message.contains("fix(cowshed): first half")
                && refusal.message.contains("fix(cowshed): second half"),
            "{}",
            refusal.message
        );
    }

    /// A build whose commit only a shed holds is refused: what it contains is not yet a question
    /// main can answer.
    #[tokio::test]
    async fn a_build_whose_commit_is_not_on_main_is_refused() {
        let main = Main::new("shed-only");
        let installed = main.head(1_000);
        main.git(["checkout", "-q", "-b", "shed"]);
        let shed = main.commit("gc.rs", "fixed\n", "fix(cowshed): unlanded", 2_000);
        main.git(["checkout", "-q", DEFAULT_LANDING_BRANCH]);
        let refusal = judged(&main, &shed, &installed)
            .await
            .expect_err("a shed-only commit is refused");
        assert!(
            refusal.message.contains("is on no adopted project's main"),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.contains(&main.root.display().to_string()),
            "the refusal names the main it looked in: {}",
            refusal.message
        );
    }

    /// The installed commit absent from main — a build installed from a shed whose commit was
    /// rebased away — cannot be compared, and is refused rather than guessed.
    #[tokio::test]
    async fn an_installed_commit_main_cannot_see_is_refused_not_guessed() {
        let main = Main::new("unseen");
        let candidate = main.commit("next.rs", "next\n", "feat(cowshed): next", 2_000);
        let installed = BuildRecord {
            commit: "f0bfb52defe2357afc492f6619789607acbc70ca".into(),
            commit_time: 1_500,
        };
        let refusal = judged(&main, &candidate, &installed)
            .await
            .expect_err("an unseen installed commit is refused");
        assert!(
            refusal
                .message
                .contains("cannot compare it with the installed one"),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.contains("f0bfb52defe2"),
            "{}",
            refusal.message
        );
    }

    /// The slot is exclusive across descriptors, as two installers' are: while one holds it, a
    /// second `flock` on the same file would block, and once it is dropped the next one takes it.
    #[tokio::test]
    async fn the_install_slot_is_exclusive_until_dropped() {
        let main = Main::new("slot");
        let held = InstallSlot::take(&main.root).await.expect("first install");
        let probe = File::open(main.root.join(INSTALL_LOCK_NAME)).expect("lock file");
        assert_eq!(
            flock(&probe, libc::LOCK_EX | libc::LOCK_NB)
                .expect_err("the slot is held")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let waiter = tokio::spawn({
            let root = main.root.clone();
            async move { InstallSlot::take(&root).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!waiter.is_finished(), "a second installer waits in line");
        drop(held);
        waiter
            .await
            .expect("waiter")
            .expect("the second installer takes the slot once the first lets go");
    }
}
