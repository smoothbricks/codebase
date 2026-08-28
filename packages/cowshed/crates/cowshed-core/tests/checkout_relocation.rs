//! Repairing a project whose checkout moved.
//!
//! A project records where its checkout lives in four independent places per workspace, and moving
//! the checkout invalidates all four at once: the in-image marker's `projectRoot`, the detached
//! sidecar's copy of it, the `main` remote's URL, and any `merge.*.driver` whose program is spelt
//! as an absolute path. Rewriting one of the four and reporting success is what left an entire
//! project answering questions about a directory that had stopped being a repository — `doctor`
//! reporting a marker identity mismatch, `git fetch main` failing forever, and every rebase dying
//! with `merge-ledger.py: No such file or directory`.
//!
//! These tests drive the repair primitives against real git repositories and real files, in the
//! order and combination `NativeProjectRuntimeHost::repair_one_workspace_record` composes them, so
//! the composition is exercised on a platform that does not need APFS images to reach it.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::checkout::CheckoutRecord;
use cowshed_core::git::{GitRepository, MainRemote, MergeDriverState};
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, ImageFormat, METADATA_VERSION, Platform, PortBlock,
    PublicationState, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceMarker, WorkspaceName,
    WorkspaceRole, write_json,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::WORKSPACE_MARKER_PATH;

const GIT: &str = "/usr/bin/git";
const INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c80";

/// A relocated project: main at its new path, one session workspace, and a dead old checkout path
/// that every record in the session still names.
struct Relocated {
    root: PathBuf,
    /// The path the project was adopted at and every stale record still names. It does not exist.
    dead_root: PathBuf,
    /// Where the checkout lives now, which is also main's mount under direct mount.
    live_root: PathBuf,
    /// The session workspace's mount.
    session: PathBuf,
    /// The session's canonical image; its detached sidecar sits beside it.
    image: PathBuf,
}

impl Relocated {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-relocation-{label}-{}-{nonce}",
            std::process::id()
        ));
        let live_root = root.join("dev/projects/example-app");
        let session = root.join("mnt/example-org/example-app/relocated-session");
        let store = root.join("store/example-org/example-app");
        let image = store.join("sessions/relocated-session.sparseimage");
        for path in [&live_root, &session] {
            fs::create_dir_all(path).expect("fixture tree");
        }
        fs::create_dir_all(image.parent().expect("image parent")).expect("image parent");
        fs::write(&image, b"fixture image").expect("image bytes");

        // Main, at the path the checkout moved to.
        git(&live_root, ["init", "-q", "-b", "main", "."]);
        write(
            &live_root,
            "scripts/merge-ledger.py",
            "#!/usr/bin/env python3\n",
        );
        write(&live_root, "LEDGER-CLAIMS.tsv", "g1\tclaimed\n");
        git(&live_root, ["add", "-A"]);
        commit(&live_root, "base");

        // The session workspace: a clone of main, minted while the checkout was still at the path
        // that is now dead, so every record in it names that path.
        git(
            &root,
            [
                OsStr::new("clone"),
                OsStr::new("-q"),
                live_root.as_os_str(),
                session.as_os_str(),
            ],
        );
        git(
            &session,
            ["checkout", "-q", "-b", "cowshed/relocated-session"],
        );

        let fixture = Self {
            dead_root: root.join(".cowshed/mnt/example-org/example-app/main"),
            root,
            live_root,
            session,
            image,
        };
        assert!(
            !fixture.dead_root.exists(),
            "the old checkout path must genuinely be gone"
        );
        fixture.record_stale_state();
        fixture
    }

    /// Plant exactly the four stale facts observed on the host.
    fn record_stale_state(&self) {
        // 1 and 2: the marker and the detached sidecar, both naming the dead root.
        let marker = WorkspaceMarker {
            version: METADATA_VERSION,
            repo_id: repo(),
            project_root: self.dead_root.clone(),
            workspace: workspace(),
            workspace_incarnation: incarnation(),
            role: WorkspaceRole::Workspace,
            image_format: ImageFormat::Sparse,
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            created_at: "2026-08-01T00:00:00Z".to_owned(),
            forked_from: None,
            created_trace: "00000000000000000000000000000001".to_owned(),
            lineage: None,
        };
        let marker_path = self.session.join(WORKSPACE_MARKER_PATH);
        fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("marker parent");
        write_json(&marker_path, &marker).expect("marker");
        self.metadata(&self.dead_root)
            .write_for_image(&self.image)
            .expect("sidecar");

        // 3: the `main` remote, pointing into the dead root. Cowshed created it, so cowshed owns it
        // — but nothing recorded that, which is why it survived every repair attempt.
        git(
            &self.session,
            [
                OsStr::new("remote"),
                OsStr::new("add"),
                OsStr::new("main"),
                self.dead_root.as_os_str(),
            ],
        );

        // 4: merge drivers whose program is an absolute path under the dead root.
        for (name, program) in [
            ("ledger-union", "scripts/merge-ledger.py"),
            ("appenddoc-union", "scripts/merge-append-doc.py"),
        ] {
            git(
                &self.session,
                [
                    format!("config"),
                    format!("merge.{name}.driver"),
                    format!("{}/{program} %O %A %B", self.dead_root.display()),
                ],
            );
        }
        // One driver the project spelt relatively already: it must come out byte-identical.
        git(
            &self.session,
            [
                "config",
                "merge.already-relative.driver",
                "scripts/merge-ledger.py %O %A %B",
            ],
        );
    }

    fn metadata(&self, project_root: &Path) -> DetachedWorkspaceMetadata {
        DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: repo(),
            workspace: workspace(),
            workspace_incarnation: incarnation(),
            image_format: ImageFormat::Sparse,
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-08-28T00:00:00Z".to_owned(),
            grants: cowshed_core::metadata::GrantSet::closed_baseline(Some(
                PortBlock::new(40_960, 16).expect("port block"),
            ))
            .expect("grants"),
            info_snapshot: Some(WorkspaceInfoSnapshot {
                project_root: project_root.to_owned(),
                role: WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: Some("cowshed/relocated-session".to_owned()),
                created_at: "2026-08-01T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-08-01T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            }),
        }
    }

    fn record(&self) -> CheckoutRecord {
        CheckoutRecord {
            mount_point: self.session.clone(),
            image: self.image.clone(),
        }
    }

    fn recorded_roots(&self) -> (PathBuf, PathBuf) {
        let marker = WorkspaceMarker::read_from(&self.session.join(WORKSPACE_MARKER_PATH))
            .expect("marker reads");
        let sidecar =
            DetachedWorkspaceMetadata::read_for_image(&self.image).expect("sidecar reads");
        (
            marker.project_root,
            sidecar
                .info_snapshot
                .expect("info snapshot")
                .project_root
                .clone(),
        )
    }

    fn driver(&self, name: &str) -> String {
        git(&self.session, ["config", &format!("merge.{name}.driver")])
            .trim_end()
            .to_owned()
    }

    fn remote_url(&self, name: &str) -> Option<String> {
        let output = Command::new(GIT)
            .arg("-C")
            .arg(&self.session)
            .args(["config", "--get", &format!("remote.{name}.url")])
            .output()
            .expect("run git config");
        output.status.success().then(|| {
            String::from_utf8_lossy(&output.stdout)
                .trim_end()
                .to_owned()
        })
    }

    /// Everything `repair_one_workspace_record` does to a mounted session, in its order.
    async fn repair(&self) -> MainRemote {
        self.record()
            .rewrite_project_root(&self.live_root)
            .expect("record rewrite");
        let repository = GitRepository::from_root(&self.session);
        repository
            .repair_merge_drivers()
            .await
            .expect("merge driver repair");
        repository
            .configure_main_remote(&self.live_root)
            .await
            .expect("main remote repair")
    }
}

impl Drop for Relocated {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn repo() -> RepoId {
    RepoId::parse("example-org/example-app").expect("repo id")
}

fn workspace() -> WorkspaceName {
    WorkspaceName::new("relocated-session").expect("workspace name")
}

fn incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(INCARNATION).expect("incarnation")
}

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("file parent")).expect("file parent");
    fs::write(path, contents).expect("write file");
}

fn git<I, S>(root: impl AsRef<Path>, args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(GIT)
        .arg("-C")
        .arg(root.as_ref())
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git failed in {}: {}",
        root.as_ref().display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("git output is UTF-8")
}

fn commit(root: &Path, message: &str) {
    let output = Command::new(GIT)
        .arg("-C")
        .arg(root)
        .args(["commit", "-q", "-m", message])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git commit");
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The whole repair, on the state the host was actually found in.
#[tokio::test]
async fn a_relocated_project_has_every_recorded_path_repaired() {
    let fixture = Relocated::new("end-to-end");

    // Before: all four facts name the dead root, and git agrees they are unusable.
    assert_eq!(
        fixture.recorded_roots(),
        (fixture.dead_root.clone(), fixture.dead_root.clone())
    );
    assert_eq!(
        fixture.remote_url("main").as_deref(),
        Some(fixture.dead_root.display().to_string().as_str())
    );
    assert!(
        Command::new(GIT)
            .arg("-C")
            .arg(&fixture.session)
            .args(["ls-remote", "--exit-code", "main", "refs/heads/main"])
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .output()
            .expect("run git ls-remote")
            .status
            .code()
            != Some(0),
        "the dead remote must genuinely be unresolvable before the repair"
    );

    let outcome = fixture.repair().await;

    // After: the marker and the sidecar name the live checkout.
    assert_eq!(
        fixture.recorded_roots(),
        (fixture.live_root.clone(), fixture.live_root.clone()),
        "both copies of projectRoot move, not just the marker"
    );

    // The `main` remote was cowshed's own stale record, so it is retargeted rather than displaced.
    assert_eq!(outcome, MainRemote::Canonical);
    assert_eq!(
        fixture.remote_url("main").as_deref(),
        Some(fixture.live_root.display().to_string().as_str())
    );
    assert_eq!(
        fixture.remote_url("cowshed-main"),
        None,
        "cowshed must not stand beside a corpse it wrote itself"
    );
    git(
        &fixture.session,
        ["ls-remote", "--exit-code", "main", "refs/heads/main"],
    );

    // Merge drivers are relocation-proof afterwards, and the one already spelt relatively is
    // untouched.
    assert_eq!(
        fixture.driver("ledger-union"),
        "scripts/merge-ledger.py %O %A %B"
    );
    assert_eq!(
        fixture.driver("already-relative"),
        "scripts/merge-ledger.py %O %A %B"
    );
    assert_eq!(
        fixture.driver("appenddoc-union"),
        format!(
            "{}/scripts/merge-append-doc.py %O %A %B",
            fixture.dead_root.display()
        ),
        "a driver naming a program this repository does not have is reported, never guessed at"
    );
}

/// Re-running the repair is a no-op, because the operator will re-run it: the same command is both
/// the fix and the way to check the fix took.
#[tokio::test]
async fn repairing_a_repaired_project_changes_nothing() {
    let fixture = Relocated::new("idempotent");
    assert_eq!(fixture.repair().await, MainRemote::Canonical);
    let after_first = (
        fixture.recorded_roots(),
        fixture.remote_url("main"),
        fixture.driver("ledger-union"),
    );

    assert_eq!(fixture.repair().await, MainRemote::Canonical);

    assert_eq!(
        (
            fixture.recorded_roots(),
            fixture.remote_url("main"),
            fixture.driver("ledger-union")
        ),
        after_first
    );
}

/// The one thing the repair must never do. A remote named `main` that points at a live repository
/// the user chose is theirs, so cowshed stands beside it under `cowshed-main` — the behaviour the
/// ownership marker exists to preserve while still letting cowshed fix its own dead records.
#[tokio::test]
async fn a_live_foreign_main_remote_is_never_retargeted() {
    let fixture = Relocated::new("foreign-remote");
    let upstream = fixture.root.join("upstream");
    fs::create_dir_all(&upstream).expect("upstream directory");
    git(&upstream, ["init", "-q", "-b", "main", "."]);
    write(&upstream, "README.md", "upstream\n");
    git(&upstream, ["add", "-A"]);
    commit(&upstream, "upstream base");
    git(
        &fixture.session,
        [
            OsStr::new("remote"),
            OsStr::new("set-url"),
            OsStr::new("main"),
            upstream.as_os_str(),
        ],
    );

    let outcome = GitRepository::from_root(&fixture.session)
        .configure_main_remote(&fixture.live_root)
        .await
        .expect("configure");

    assert_eq!(outcome, MainRemote::Displaced);
    assert_eq!(
        fixture.remote_url("main").as_deref(),
        Some(upstream.display().to_string().as_str()),
        "a live repository the user pointed at is untouched"
    );
    assert_eq!(
        fixture.remote_url("cowshed-main").as_deref(),
        Some(fixture.live_root.display().to_string().as_str())
    );
}

/// Ownership, once recorded, survives the URL going stale — which is the case a URL comparison
/// cannot see. Here the remote is cowshed's *and* points at a path that still holds a perfectly
/// good repository, just not this project's main.
#[tokio::test]
async fn a_recorded_owner_lets_cowshed_retarget_a_remote_whose_path_still_resolves() {
    let fixture = Relocated::new("owned-live-path");
    let elsewhere = fixture.root.join("elsewhere");
    fs::create_dir_all(&elsewhere).expect("elsewhere directory");
    git(&elsewhere, ["init", "-q", "-b", "main", "."]);
    write(&elsewhere, "README.md", "elsewhere\n");
    git(&elsewhere, ["add", "-A"]);
    commit(&elsewhere, "elsewhere base");

    // Cowshed configures the remote, so ownership is recorded, and then the mount moves.
    let repository = GitRepository::from_root(&fixture.session);
    assert_eq!(
        repository
            .configure_main_remote(&elsewhere)
            .await
            .expect("first configure"),
        MainRemote::Canonical
    );
    assert_eq!(
        repository
            .configure_main_remote(&fixture.live_root)
            .await
            .expect("reconfigure after the mount moved"),
        MainRemote::Canonical
    );

    assert_eq!(
        fixture.remote_url("main").as_deref(),
        Some(fixture.live_root.display().to_string().as_str()),
        "a remote cowshed owns follows main's mount even when its old path still resolves"
    );
    assert_eq!(fixture.remote_url("cowshed-main"), None);
}

/// `inspect_merge_drivers` is what `doctor` reads, and `doctor` never mutates.
#[tokio::test]
async fn inspecting_merge_drivers_reports_without_repairing() {
    let fixture = Relocated::new("inspect-only");
    let before = fixture.driver("ledger-union");

    let drivers = GitRepository::from_root(&fixture.session)
        .inspect_merge_drivers()
        .await
        .expect("inspect");

    let mut states: Vec<_> = drivers
        .iter()
        .map(|driver| (driver.name.as_str(), &driver.state))
        .collect();
    states.sort_by_key(|(name, _)| *name);
    assert_eq!(
        states,
        vec![
            ("already-relative", &MergeDriverState::Relative),
            (
                "appenddoc-union",
                &MergeDriverState::Unresolvable {
                    program: format!(
                        "{}/scripts/merge-append-doc.py",
                        fixture.dead_root.display()
                    ),
                }
            ),
            (
                "ledger-union",
                &MergeDriverState::Relativized {
                    to: String::from("scripts/merge-ledger.py %O %A %B"),
                }
            ),
        ]
    );
    assert_eq!(
        fixture.driver("ledger-union"),
        before,
        "inspection must leave the configuration exactly as it found it"
    );
}
