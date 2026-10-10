//! Read-only Git evidence for `specs/cowshed/02_workspaces.md`, "Re-adoption:
//! format transition and space reclamation", steps 2, 4 and 5. The surrounding
//! transaction preserves the complete filesystem, including Git's administrative
//! files; these probes independently check Git's interpretation of that copy.
//! `01_storage.md` "Images" keeps the retained source until verification passes.
//!
//! Capture requires a quiescent checkout. Every probe disables optional locks,
//! retains stdout/stderr bytes, and uses the existing controller Git runner.
//! Only the current checkout's worktree-list entry is mapped to the caller's
//! canonical publication path; registered peers are never rewritten.

#[cfg(any(target_os = "macos", test))]
use std::ffi::OsString;
#[cfg(any(target_os = "macos", test))]
use std::fmt;
#[cfg(any(target_os = "macos", test))]
use std::fs;
#[cfg(any(target_os = "macos", test))]
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
#[cfg(any(target_os = "macos", test))]
use std::os::unix::process::ExitStatusExt as _;
#[cfg(any(target_os = "macos", test))]
use std::path::{Path, PathBuf};
#[cfg(any(target_os = "macos", test))]
use std::process::Output;

use serde::{Deserialize, Serialize};

use crate::api::{GitOid, Sha256Digest};
#[cfg(any(target_os = "macos", test))]
use crate::error::{CowshedError, Result};
#[cfg(any(target_os = "macos", test))]
use crate::fork_lock::Run as _;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    code: Option<i32>,
    signal: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[cfg(any(target_os = "macos", test))]
impl Probe {
    fn succeeded(&self) -> bool {
        self.code == Some(0) && self.signal.is_none()
    }
}

#[cfg(any(target_os = "macos", test))]
impl From<Output> for Probe {
    fn from(output: Output) -> Self {
        Self {
            code: output.status.code(),
            signal: output.status.signal(),
            stdout: output.stdout,
            stderr: output.stderr,
        }
    }
}

/// Durable before-state, including raw evidence rather than lossy text parsing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GitPreservationSnapshot {
    head: GitOid,
    head_probe: Probe,
    symbolic_head: Probe,
    porcelain: Probe,
    porcelain_sha256: Sha256Digest,
    count_objects: Probe,
    refs: Probe,
    reflogs: Probe,
    local_config_raw: Vec<u8>,
    local_config_resolved: Probe,
    worktrees: Probe,
    observed_root: Vec<u8>,
    canonical_checkout: Vec<u8>,
    fsck: Probe,
}

#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GitPreservationComponent {
    Head,
    Porcelain,
    CountObjects,
    Refs,
    Reflogs,
    LocalConfigRaw,
    LocalConfigResolved,
    WorktreeRegistrations,
    Fsck,
}

#[cfg(any(target_os = "macos", test))]
impl fmt::Display for GitPreservationComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Head => "HEAD",
            Self::Porcelain => "porcelain",
            Self::CountObjects => "count-objects",
            Self::Refs => "refs",
            Self::Reflogs => "reflogs/stash",
            Self::LocalConfigRaw => "local-config-raw",
            Self::LocalConfigResolved => "local-config-resolved",
            Self::WorktreeRegistrations => "worktree-registrations",
            Self::Fsck => "fsck",
        })
    }
}

/// All mismatches, in probe order; no first-mismatch-only verdict.
#[cfg(any(target_os = "macos", test))]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct GitPreservationDiff {
    pub(crate) components: Vec<GitPreservationComponent>,
}

#[cfg(any(target_os = "macos", test))]
impl fmt::Display for GitPreservationDiff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Git preservation mismatch: ")?;
        for (index, component) in self.components.iter().enumerate() {
            if index != 0 {
                formatter.write_str(", ")?;
            }
            component.fmt(formatter)?;
        }
        Ok(())
    }
}

#[cfg(any(target_os = "macos", test))]
impl std::error::Error for GitPreservationDiff {}

#[cfg(any(target_os = "macos", test))]
impl GitPreservationSnapshot {
    pub(crate) fn head(&self) -> &GitOid {
        &self.head
    }

    pub(crate) fn fsck_succeeded(&self) -> bool {
        self.fsck.succeeded()
    }

    /// Compare the exact probes, mapping only each checkout's own root entry.
    pub(crate) fn compare(&self, actual: &Self) -> std::result::Result<(), GitPreservationDiff> {
        use GitPreservationComponent as Component;
        let predicates = [
            (
                Component::Head,
                self.head != actual.head
                    || self.head_probe != actual.head_probe
                    || self.symbolic_head != actual.symbolic_head,
            ),
            (
                Component::Porcelain,
                self.porcelain_sha256 != actual.porcelain_sha256
                    || self.porcelain != actual.porcelain,
            ),
            (
                Component::CountObjects,
                self.count_objects != actual.count_objects,
            ),
            (Component::Refs, self.refs != actual.refs),
            (Component::Reflogs, self.reflogs != actual.reflogs),
            (
                Component::LocalConfigRaw,
                self.local_config_raw != actual.local_config_raw,
            ),
            (
                Component::LocalConfigResolved,
                self.local_config_resolved != actual.local_config_resolved,
            ),
            (
                Component::WorktreeRegistrations,
                !worktrees_equal(self, actual),
            ),
            (Component::Fsck, self.fsck != actual.fsck),
        ];
        let mut components = Vec::new();
        for (component, mismatched) in predicates {
            if mismatched {
                components.push(component);
            }
        }
        if components.is_empty() {
            Ok(())
        } else {
            Err(GitPreservationDiff { components })
        }
    }
}

#[cfg(any(target_os = "macos", test))]
fn absolute(path: &Path, name: &str) -> Result<()> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(CowshedError::usage(
            format!(
                "Git preservation {name} is not absolute: {}",
                path.display()
            ),
            "supply absolute checkout and canonical publication paths",
        ))
    }
}

/// Run without index refreshes or ambient Git-directory redirection.
#[cfg(any(target_os = "macos", test))]
fn probe(root: &Path, args: &[&str], allowed_codes: &[i32]) -> Result<Probe> {
    let output = crate::git::git_command_at(root)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_NAMESPACE")
        .args(args)
        .output_locked()
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot run Git preservation probe {args:?} at {}: {error}",
                    root.display()
                ),
                "make git executable and the retained checkout readable, then retry",
            )
        })?;
    if !allowed_codes.is_empty()
        && !output
            .status
            .code()
            .is_some_and(|code| allowed_codes.contains(&code))
    {
        return Err(CowshedError::integrity(
            format!(
                "Git preservation probe {args:?} at {} failed: {}",
                root.display(),
                crate::git::git_ended(&output)
            ),
            "repair access to the retained Git state before retrying; do not publish the replacement",
        ));
    }
    Ok(output.into())
}

#[cfg(any(target_os = "macos", test))]
pub(crate) fn capture(root: &Path, canonical_checkout: &Path) -> Result<GitPreservationSnapshot> {
    absolute(root, "root")?;
    absolute(canonical_checkout, "canonical checkout")?;
    let observed_root = fs::canonicalize(root).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot resolve Git preservation root {}: {error}",
                root.display()
            ),
            "make the retained checkout readable before retrying",
        )
    })?;
    let head_probe = probe(root, &["rev-parse", "--verify", "HEAD"], &[0])?;
    let head_bytes = head_probe
        .stdout
        .strip_suffix(b"\n")
        .unwrap_or(&head_probe.stdout);
    let head_text = std::str::from_utf8(head_bytes).map_err(|error| {
        CowshedError::integrity(
            format!(
                "Git preservation HEAD at {} is not an object ID: {error}",
                root.display()
            ),
            "repair the retained repository before retrying",
        )
    })?;
    let head = GitOid::new(head_text).map_err(|error| {
        CowshedError::integrity(
            format!(
                "Git preservation HEAD at {} is invalid: {error}",
                root.display()
            ),
            "repair the retained repository before retrying",
        )
    })?;
    // Exit 1 is Git's documented detached-HEAD answer, not a failed probe.
    let symbolic_head = probe(root, &["symbolic-ref", "-q", "HEAD"], &[0, 1])?;
    let porcelain = probe(root, &["status", "--porcelain=v2", "-z"], &[0])?;
    let porcelain_sha256 = Sha256Digest::compute(&porcelain.stdout);
    let count_objects = probe(root, &["count-objects", "-v"], &[0])?;
    let refs = probe(
        root,
        &[
            "for-each-ref",
            "--sort=refname",
            "--format=%(refname)%00%(objectname)%00%(symref)%00",
        ],
        &[0],
    )?;
    let reflogs = probe(
        root,
        &[
            "reflog",
            "show",
            "--all",
            "--date=raw",
            "--format=%H%x00%gD%x00%gn%x00%ge%x00%gs%x00",
        ],
        &[0],
    )?;
    let config_path = probe(
        root,
        &[
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "config",
        ],
        &[0],
    )?;
    let path_bytes = config_path
        .stdout
        .strip_suffix(b"\n")
        .unwrap_or(&config_path.stdout);
    let config_path = PathBuf::from(OsString::from_vec(path_bytes.to_vec()));
    absolute(&config_path, "local config path")?;
    let local_config_raw = fs::read(&config_path).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot read Git preservation local config {}: {error}",
                config_path.display()
            ),
            "make the retained local Git config readable before retrying",
        )
    })?;
    let local_config_resolved = probe(
        root,
        &["config", "--local", "--includes", "--list", "-z"],
        &[0],
    )?;
    let worktrees = probe(root, &["worktree", "list", "--porcelain", "-z"], &[0])?;
    // A nonzero fsck is evidence. Source admission and verify own the refusal.
    let fsck = probe(root, &["fsck", "--full"], &[])?;
    Ok(GitPreservationSnapshot {
        head,
        head_probe,
        symbolic_head,
        porcelain,
        porcelain_sha256,
        count_objects,
        refs,
        reflogs,
        local_config_raw,
        local_config_resolved,
        worktrees,
        observed_root: observed_root.into_os_string().into_vec(),
        canonical_checkout: canonical_checkout.as_os_str().as_bytes().to_vec(),
        fsck,
    })
}

#[cfg(any(target_os = "macos", test))]
fn worktree_field<'a>(
    snapshot: &'a GitPreservationSnapshot,
    field: &'a [u8],
) -> (&'a [u8], &'a [u8]) {
    const PREFIX: &[u8] = b"worktree ";
    if field.strip_prefix(PREFIX) == Some(snapshot.observed_root.as_slice()) {
        (PREFIX, &snapshot.canonical_checkout)
    } else {
        (b"", field)
    }
}

#[cfg(any(target_os = "macos", test))]
fn worktrees_equal(expected: &GitPreservationSnapshot, actual: &GitPreservationSnapshot) -> bool {
    if expected.worktrees.code != actual.worktrees.code
        || expected.worktrees.signal != actual.worktrees.signal
        || expected.worktrees.stderr != actual.worktrees.stderr
    {
        return false;
    }
    let mut expected_fields = expected.worktrees.stdout.split(|byte| *byte == 0);
    let mut actual_fields = actual.worktrees.stdout.split(|byte| *byte == 0);
    loop {
        match (expected_fields.next(), actual_fields.next()) {
            (Some(left), Some(right)) => {
                let (left_prefix, left_path) = worktree_field(expected, left);
                let (right_prefix, right_path) = worktree_field(actual, right);
                if !left_prefix
                    .iter()
                    .chain(left_path)
                    .eq(right_prefix.iter().chain(right_path))
                {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(any(target_os = "macos", test))]
pub(crate) fn verify(
    root: &Path,
    canonical_checkout: &Path,
    expected: &GitPreservationSnapshot,
) -> Result<()> {
    let actual = capture(root, canonical_checkout)?;
    let comparison = expected.compare(&actual);
    if let Err(diff) = comparison {
        return Err(CowshedError::integrity(
            format!(
                "{diff} at {}; copied fsck: code {:?}, signal {:?}; stderr: {}",
                root.display(),
                actual.fsck.code,
                actual.fsck.signal,
                String::from_utf8_lossy(&actual.fsck.stderr)
            ),
            "do not publish the replacement; retain the source and inspect the named Git components",
        ));
    }
    if !actual.fsck_succeeded() {
        return Err(CowshedError::integrity(
            format!(
                "Git preservation fsck failed at {}: code {:?}, signal {:?}; stdout: {}; stderr: {}",
                root.display(),
                actual.fsck.code,
                actual.fsck.signal,
                String::from_utf8_lossy(&actual.fsck.stdout),
                String::from_utf8_lossy(&actual.fsck.stderr)
            ),
            "do not publish a repository with failing fsck; retain and repair the source",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_root::TempRoot;

    struct Repository {
        scratch: TempRoot,
        root: PathBuf,
    }

    impl Repository {
        fn new() -> Self {
            let scratch = TempRoot::new("cowshed-git-preservation");
            let root = scratch.join("repository");
            fs::create_dir(&root).expect("create test checkout");
            let repository = Self { scratch, root };
            repository.git(&["init", "-q", "-b", "main"]);
            repository.git(&["config", "user.name", "Cowshed Test"]);
            repository.git(&["config", "user.email", "test@example.invalid"]);
            repository.git(&[
                "config",
                "remote.origin.url",
                "https://example.invalid/original.git",
            ]);
            fs::write(repository.root.join("README"), b"first\n").expect("write fixture");
            repository.git(&["add", "README"]);
            repository.git(&["commit", "-qm", "first"]);
            fs::write(repository.root.join("README"), b"second\n").expect("change fixture");
            repository.git(&["commit", "-qam", "second"]);
            repository.git(&["tag", "held", "HEAD^"]);
            repository
        }

        fn git(&self, args: &[&str]) -> Vec<u8> {
            let output = crate::git::git_command_at(&self.root)
                .env("GIT_OPTIONAL_LOCKS", "0")
                .args(args)
                .output_locked()
                .expect("run real git fixture command");
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                crate::git::git_ended(&output)
            );
            output.stdout
        }

        fn snapshot(&self) -> GitPreservationSnapshot {
            capture(&self.root, &self.root).expect("capture real Git state")
        }

        fn assert_mismatch(
            &self,
            before: &GitPreservationSnapshot,
            component: GitPreservationComponent,
        ) -> GitPreservationSnapshot {
            let after = self.snapshot();
            let diff = before
                .compare(&after)
                .expect_err("planted mutation must fail comparison");
            assert!(
                diff.components.contains(&component),
                "{component} missing from {diff:?}"
            );
            assert!(
                diff.to_string().contains(&component.to_string()),
                "diagnostic must name {component}"
            );
            let error = verify(&self.root, &self.root, before)
                .expect_err("planted mutation must refuse verification");
            assert_eq!(error.code, crate::ErrorCode::Integrity);
            assert!(
                error.message.contains(&component.to_string()),
                "{}",
                error.message
            );
            after
        }
    }

    #[test]
    fn identical_snapshots_and_serialized_evidence_compare_equal() {
        let repository = Repository::new();
        let before = repository.snapshot();
        assert!(before.fsck_succeeded());
        assert_eq!(
            before.head().as_str().as_bytes(),
            before.head_probe.stdout.strip_suffix(b"\n").unwrap()
        );
        before
            .compare(&repository.snapshot())
            .expect("identical state");
        verify(&repository.root, &repository.root, &before).expect("identical state verifies");
        let encoded = serde_json::to_vec(&before).expect("serialize raw evidence");
        let decoded: GitPreservationSnapshot =
            serde_json::from_slice(&encoded).expect("deserialize raw evidence");
        assert_eq!(before, decoded);
    }

    #[test]
    fn changed_ref_is_named() {
        let repository = Repository::new();
        let before = repository.snapshot();
        repository.git(&["update-ref", "refs/tags/held", "HEAD"]);
        repository.assert_mismatch(&before, GitPreservationComponent::Refs);
    }

    #[test]
    fn lost_reflog_entry_is_named() {
        let repository = Repository::new();
        let before = repository.snapshot();
        repository.git(&["reflog", "delete", "--rewrite", "refs/heads/main@{1}"]);
        let after = repository.assert_mismatch(&before, GitPreservationComponent::Reflogs);
        assert_eq!(before.refs, after.refs, "ref tips did not move");
    }

    #[test]
    fn lost_stash_reflog_entry_is_named_without_moving_stash_tip() {
        let repository = Repository::new();
        for content in [b"stash one\n", b"stash two\n"] {
            fs::write(repository.root.join("README"), content).expect("write stash work");
            repository.git(&["stash", "push", "-qm", "held stash"]);
        }
        let before = repository.snapshot();
        repository.git(&["reflog", "delete", "--rewrite", "refs/stash@{1}"]);
        let after = repository.assert_mismatch(&before, GitPreservationComponent::Reflogs);
        assert_eq!(before.refs, after.refs, "stash tip did not move");
    }

    #[test]
    fn changed_remote_url_names_raw_and_resolved_config() {
        let repository = Repository::new();
        let before = repository.snapshot();
        repository.git(&[
            "config",
            "remote.origin.url",
            "https://example.invalid/changed.git",
        ]);
        let after =
            repository.assert_mismatch(&before, GitPreservationComponent::LocalConfigResolved);
        assert!(
            before
                .compare(&after)
                .unwrap_err()
                .components
                .contains(&GitPreservationComponent::LocalConfigRaw)
        );
    }

    #[test]
    fn changed_included_remote_is_named() {
        let repository = Repository::new();
        repository.git(&["config", "include.path", "../included.gitconfig"]);
        let included = repository.root.join("included.gitconfig");
        fs::write(
            &included,
            b"[remote \"included\"]\n\turl = https://example.invalid/first.git\n",
        )
        .expect("write included config");
        let before = repository.snapshot();
        fs::write(
            &included,
            b"[remote \"included\"]\n\turl = https://example.invalid/other.git\n",
        )
        .expect("change included config");
        let after =
            repository.assert_mismatch(&before, GitPreservationComponent::LocalConfigResolved);
        assert_eq!(
            before.local_config_raw, after.local_config_raw,
            "include directive unchanged"
        );
    }

    #[test]
    fn config_formatting_loss_is_named_even_if_resolved_values_match() {
        let repository = Repository::new();
        let before = repository.snapshot();
        let config = repository.root.join(".git/config");
        let mut bytes = fs::read(&config).expect("read local config");
        bytes.extend_from_slice(b"\n# retained comment\n");
        fs::write(&config, bytes).expect("change raw local config");
        let after = repository.assert_mismatch(&before, GitPreservationComponent::LocalConfigRaw);
        assert_eq!(before.local_config_resolved, after.local_config_resolved);
    }

    #[test]
    fn missing_worktree_registration_is_named() {
        let repository = Repository::new();
        let peer = repository.scratch.join("peer");
        repository.git(&["worktree", "add", "-q", "--detach", peer.to_str().unwrap()]);
        let before = repository.snapshot();
        repository.git(&["worktree", "remove", peer.to_str().unwrap()]);
        repository.assert_mismatch(&before, GitPreservationComponent::WorktreeRegistrations);
    }

    #[test]
    fn corrupt_object_is_named_and_equal_failing_fsck_is_refused() {
        let repository = Repository::new();
        let payload = repository.scratch.join("payload");
        fs::write(&payload, b"unreachable blob\n").expect("write blob input");
        let oid_bytes = repository.git(&["hash-object", "-w", payload.to_str().unwrap()]);
        let oid = std::str::from_utf8(&oid_bytes).unwrap().trim_end();
        let before = repository.snapshot();
        let object = repository
            .root
            .join(".git/objects")
            .join(&oid[..2])
            .join(&oid[2..]);
        fs::remove_file(&object).expect("remove read-only loose object");
        fs::write(&object, b"corrupt object\n").expect("plant corrupt object");
        let after = repository.assert_mismatch(&before, GitPreservationComponent::Fsck);
        assert!(!after.fsck_succeeded());
        assert!(after.fsck.code.is_some_and(|code| code != 0));
        assert!(
            !after.fsck.stderr.is_empty(),
            "actual corruption diagnostic is retained"
        );
        after
            .compare(&repository.snapshot())
            .expect("same corrupt state compares equal");
        let error = verify(&repository.root, &repository.root, &after)
            .expect_err("matching corruption must still refuse");
        assert!(error.message.contains("fsck failed"));
    }

    #[test]
    fn changed_symbolic_head_is_named_without_moving_object_id() {
        let repository = Repository::new();
        let before = repository.snapshot();
        repository.git(&["checkout", "-q", "--detach"]);
        let after = repository.assert_mismatch(&before, GitPreservationComponent::Head);
        assert_eq!(before.head(), after.head());
    }

    #[test]
    fn dirty_porcelain_preserves_nul_framing_path_bytes_and_hash() {
        let repository = Repository::new();
        let before = repository.snapshot();
        let name = OsString::from_vec(b"raw-\nrecord".to_vec());
        fs::write(repository.root.join(name), b"untracked\n").expect("write newline-named file");
        let after = repository.assert_mismatch(&before, GitPreservationComponent::Porcelain);
        assert_eq!(after.porcelain.stdout, b"? raw-\nrecord\0");
        assert_eq!(
            after.porcelain_sha256,
            Sha256Digest::compute(&after.porcelain.stdout)
        );
    }

    #[test]
    fn changed_object_count_is_named() {
        let repository = Repository::new();
        let before = repository.snapshot();
        let payload = repository.scratch.join("payload");
        fs::write(&payload, b"unreachable blob\n").expect("write object input");
        repository.git(&["hash-object", "-w", payload.to_str().unwrap()]);
        repository.assert_mismatch(&before, GitPreservationComponent::CountObjects);
    }

    #[test]
    fn copied_repo_maps_only_current_root_not_peer_registrations() {
        let repository = Repository::new();
        let peer = repository.root.join("registered-peer");
        repository.git(&["worktree", "add", "-q", "--detach", peer.to_str().unwrap()]);
        let before = repository.snapshot();
        let staging = repository.scratch.join("stage");
        let status = std::process::Command::new("/bin/cp")
            .arg("-R")
            .arg(&repository.root)
            .arg(&staging)
            .status_locked()
            .expect("copy real repository to staging");
        assert!(status.success());
        let staged = capture(&staging, &repository.root).expect("capture staged copy");
        assert_ne!(
            before.worktrees.stdout, staged.worktrees.stdout,
            "raw current-root evidence differs"
        );
        before
            .compare(&staged)
            .expect("mapped staging copy is equal");
        verify(&staging, &repository.root, &before).expect("mapped staging copy verifies");
        let registration = staging.join(".git/worktrees/registered-peer/gitdir");
        fs::write(
            &registration,
            format!("{}\n", staging.join("registered-peer/.git").display()),
        )
        .expect("plant peer path rewrite");
        let changed =
            capture(&staging, &repository.root).expect("capture changed peer registration");
        assert!(
            before
                .compare(&changed)
                .unwrap_err()
                .components
                .contains(&GitPreservationComponent::WorktreeRegistrations)
        );
    }

    #[test]
    fn multiple_mismatches_are_all_named() {
        let repository = Repository::new();
        let before = repository.snapshot();
        repository.git(&["update-ref", "refs/tags/held", "HEAD"]);
        repository.git(&[
            "config",
            "remote.origin.url",
            "https://example.invalid/changed.git",
        ]);
        fs::write(repository.root.join("README"), b"dirty\n").expect("plant dirty tracked file");
        let diff = before.compare(&repository.snapshot()).unwrap_err();
        assert_eq!(
            diff.components,
            vec![
                GitPreservationComponent::Porcelain,
                GitPreservationComponent::Refs,
                GitPreservationComponent::LocalConfigRaw,
                GitPreservationComponent::LocalConfigResolved
            ]
        );
    }

    #[test]
    fn capture_does_not_refresh_or_rewrite_the_index() {
        let repository = Repository::new();
        let readme = repository.root.join("README");
        fs::File::open(&readme)
            .expect("open tracked fixture")
            .set_modified(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2))
            .expect("make tracked stat data stale");
        let index = repository.root.join(".git/index");
        let before = fs::read(&index).expect("read original index");
        let modified = fs::metadata(&index)
            .expect("stat original index")
            .modified()
            .unwrap();
        let snapshot = repository.snapshot();
        assert!(
            snapshot.porcelain.stdout.is_empty(),
            "tracked contents are unchanged"
        );
        assert_eq!(before, fs::read(&index).expect("read probed index"));
        assert_eq!(
            modified,
            fs::metadata(&index)
                .expect("stat probed index")
                .modified()
                .unwrap()
        );
    }

    #[test]
    fn newline_checkout_roots_roundtrip_without_a_path_codec() {
        let mut repository = Repository::new();
        let root = repository
            .scratch
            .join(OsString::from_vec(b"checkout-\nroot".to_vec()));
        fs::rename(&repository.root, &root).expect("move fixture to newline-named root");
        repository.root = root;
        let before = repository.snapshot();
        let encoded = serde_json::to_vec(&before).expect("serialize newline-named root evidence");
        let decoded: GitPreservationSnapshot =
            serde_json::from_slice(&encoded).expect("decode evidence");
        assert_eq!(before, decoded);
        verify(&repository.root, &repository.root, &decoded).expect("newline-named root verifies");
    }

    #[test]
    fn non_utf8_git_output_roundtrips_without_lossy_decoding() {
        let repository = Repository::new();
        let config = repository.root.join(".git/config");
        let mut bytes = fs::read(&config).expect("read local config");
        bytes.extend_from_slice(b"\n[preservation]\n\tbytes = raw-\xff\n");
        fs::write(&config, bytes).expect("plant byte-valued local config");
        let before = repository.snapshot();
        assert!(
            before
                .local_config_resolved
                .stdout
                .windows(5)
                .any(|bytes| bytes == b"raw-\xff")
        );
        assert!(
            before
                .local_config_raw
                .windows(5)
                .any(|bytes| bytes == b"raw-\xff")
        );
        let encoded = serde_json::to_vec(&before).expect("serialize invalid UTF-8 Git output");
        let decoded: GitPreservationSnapshot =
            serde_json::from_slice(&encoded).expect("decode raw evidence");
        assert_eq!(before, decoded);
        verify(&repository.root, &repository.root, &decoded).expect("raw Git output verifies");
    }

    #[test]
    fn git_probe_failure_returns_an_actionable_typed_error() {
        let root = TempRoot::new("cowshed-git-preservation-not-repository");
        let error = capture(&root, &root).expect_err("not a Git repository");
        assert_eq!(error.code, crate::ErrorCode::Integrity);
        assert!(error.message.contains("HEAD"));
        assert!(error.message.contains("not a git repository"));
        assert!(error.hint.contains("do not publish"));
    }

    #[test]
    fn relative_or_missing_roots_return_typed_errors() {
        let error = capture(Path::new("relative"), Path::new("/canonical")).unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Usage);
        let repository = Repository::new();
        let error = capture(&repository.root, Path::new("relative")).unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Usage);
        let error = capture(&repository.scratch.join("missing"), &repository.root).unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Integrity);
    }
}
