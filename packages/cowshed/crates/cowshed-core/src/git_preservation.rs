//! Read-only Git evidence for `specs/cowshed/02_workspaces.md`, "Re-adoption:
//! format transition and space reclamation", steps 2, 4 and 5. The surrounding
//! transaction preserves the complete filesystem, including Git's administrative
//! files; these probes independently check Git's interpretation of that copy.
//! `01_storage.md` "Images" keeps the retained source until verification passes.
//!
//! Capture requires a quiescent checkout. Every probe disables optional locks,
//! retains stdout/stderr bytes, and uses the existing controller Git runner.
//! Status runs with every configured filter driver disabled, so no probe executes
//! a clean, smudge or process command. Only the current checkout's worktree-list
//! entry is mapped to the caller's canonical publication path; registered peers
//! are never rewritten.
//!
//! `verify_copy` judges a copy against its retained source: every recorded
//! non-status probe must match the copy, filter-free status must match between
//! source and copy, and the `.git` directory, or a linked worktree's `.git` file and its
//! shared index, and every tracked or untracked-not-ignored path must match the source.

#[cfg(any(target_os = "macos", test))]
use std::collections::BTreeMap;
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
use cowshed_os::fork_lock::Run as _;

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

    #[cfg(test)]
    /// Compare the exact probes, mapping only each checkout's own root entry.
    pub(crate) fn compare(&self, actual: &Self) -> std::result::Result<(), GitPreservationDiff> {
        self.compare_live(
            actual,
            Live {
                status_differs: self.porcelain_sha256 != actual.porcelain_sha256
                    || self.porcelain != actual.porcelain,
                resolved_config_differs: self.local_config_resolved != actual.local_config_resolved,
            },
        )
    }

    /// Every probe except the two whose verdicts the caller passes in `live`. Recorded status
    /// and resolved config are capture-time evidence: a copy is judged by live filter-free
    /// status and live resolved config, never by replaying those bytes as if they were its
    /// current output.
    fn compare_live(
        &self,
        actual: &Self,
        live: Live,
    ) -> std::result::Result<(), GitPreservationDiff> {
        use GitPreservationComponent as Component;
        let predicates = [
            (
                Component::Head,
                self.head != actual.head
                    || self.head_probe != actual.head_probe
                    || self.symbolic_head != actual.symbolic_head,
            ),
            (Component::Porcelain, live.status_differs),
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
            (Component::LocalConfigResolved, live.resolved_config_differs),
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

/// The verdicts a copy is judged on live, against its retained source as both read now, rather
/// than against what was recorded. Status depends on the worktree and its filters. Resolved config
/// depends on included files, which can live outside `.git`, even at an absolute path inside the
/// canonical checkout, which is an empty mountpoint until the copy is published there.
#[cfg(any(target_os = "macos", test))]
struct Live {
    status_differs: bool,
    resolved_config_differs: bool,
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
    probe_with(root, &[], args, allowed_codes)
}

/// [`probe`] with `env` added to this one invocation's environment, as command-line config.
#[cfg(any(target_os = "macos", test))]
fn probe_with(
    root: &Path,
    env: &[(OsString, OsString)],
    args: &[&str],
    allowed_codes: &[i32],
) -> Result<Probe> {
    let mut command = crate::git::git_command_at(root);
    command
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_NAMESPACE")
        .args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output_locked().map_err(|error| {
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

/// Resolves `path` to its real location; a failure names the checkout role it was asked for.
#[cfg(any(target_os = "macos", test))]
fn canonical(path: &Path, name: &str) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot resolve Git preservation {name} {}: {error}",
                path.display()
            ),
            "make the retained checkout readable before retrying",
        )
    })
}

/// `git status` with every configured filter driver disabled, so no clean, smudge or process
/// command runs. Driver keys come from a query of this repository's effective config: each
/// `clean`, `smudge` and `process` is emptied and each `required` set to false. The overrides
/// are command-line configuration for this one invocation; no config file is written.
/// `core.checkStat=minimal` judges stat data by the fields a copy keeps, mtime and size. Without
/// it, the inode and ctime that copying changes make Git rehash every file on the copy alone, so
/// a filtered file the source reports clean would appear modified only in the copy.
/// Submodule worktrees are not statused (`--ignore-submodules=dirty`): git runs a child status in
/// each populated submodule under that submodule's own config, which would run drivers the keys
/// above cannot reach. Commit-level submodule changes stay visible; submodule bytes are compared
/// as whole subtrees by `copy::verify_relative_paths_blocking`.
#[cfg(any(target_os = "macos", test))]
fn filter_free_status(root: &Path) -> Result<Probe> {
    // Exit 1 means no key matched: no filter driver is configured.
    let keys = probe(
        root,
        &[
            "config",
            "--null",
            "--name-only",
            "--get-regexp",
            r"^filter\.",
        ],
        &[0, 1],
    )?;
    let mut settings: BTreeMap<Vec<u8>, &str> = BTreeMap::new();
    for key in keys
        .stdout
        .split(|byte| *byte == 0)
        .filter(|key| !key.is_empty())
    {
        let value = match key.rsplit(|byte| *byte == b'.').next() {
            Some(b"clean" | b"smudge" | b"process") => "",
            Some(b"required") => "false",
            _ => continue,
        };
        settings.insert(key.to_vec(), value);
    }
    settings.insert(b"core.checkstat".to_vec(), "minimal");
    let mut env = vec![(
        OsString::from("GIT_CONFIG_COUNT"),
        OsString::from(settings.len().to_string()),
    )];
    for (index, (key, value)) in settings.into_iter().enumerate() {
        env.push((
            OsString::from(format!("GIT_CONFIG_KEY_{index}")),
            OsString::from_vec(key),
        ));
        env.push((
            OsString::from(format!("GIT_CONFIG_VALUE_{index}")),
            OsString::from(value),
        ));
    }
    probe_with(
        root,
        &env,
        &[
            "status",
            "--porcelain=v2",
            "-z",
            "--ignore-submodules=dirty",
        ],
        &[0],
    )
}

#[cfg(any(target_os = "macos", test))]
pub(crate) fn capture(root: &Path, canonical_checkout: &Path) -> Result<GitPreservationSnapshot> {
    absolute(root, "root")?;
    absolute(canonical_checkout, "canonical checkout")?;
    let observed_root = canonical(root, "root")?;
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
    let porcelain = filter_free_status(root)?;
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
    let local_config_resolved = resolved_local_config(root)?;
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

/// The repository's local config at `root` with its includes resolved, as Git reads it now.
#[cfg(any(target_os = "macos", test))]
fn resolved_local_config(root: &Path) -> Result<Probe> {
    probe(
        root,
        &["config", "--local", "--includes", "--list", "-z"],
        &[0],
    )
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

#[cfg(test)]
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
    require_fsck(root, &actual)?;
    Ok(())
}

/// Every raw path Git tracks, plus every untracked path no ignore rule excludes, sorted without
/// duplicates (unmerged index entries repeat). A submodule or nested repository is listed as its
/// directory without the trailing slash, so the copy verifier reads it as a whole subtree.
#[cfg(any(target_os = "macos", test))]
fn worktree_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for args in [
        &["ls-files", "-z", "--cached"][..],
        &["ls-files", "-z", "--others", "--exclude-standard"][..],
    ] {
        let listed = probe(root, args, &[0])?;
        paths.extend(
            listed
                .stdout
                .split(|byte| *byte == 0)
                .filter(|field| !field.is_empty())
                .map(|field| {
                    let name = field.strip_suffix(b"/").unwrap_or(field);
                    PathBuf::from(OsString::from_vec(name.to_vec()))
                }),
        );
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// How a checkout holds its Git state. A standalone `.git` directory is copied and compared as a
/// tree. A `.git` file is a linked worktree: its administrative directory lives outside the
/// checkout, is not copied, and is shared by the source and the copy.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GitEntry {
    Directory,
    File,
}

#[cfg(any(target_os = "macos", test))]
fn git_entry(checkout: &Path) -> Result<GitEntry> {
    let git = checkout.join(".git");
    let file_type = fs::symlink_metadata(&git)
        .map_err(|error| {
            CowshedError::integrity(
                format!("cannot inspect Git entry {}: {error}", git.display()),
                "make the retained checkout readable before retrying",
            )
        })?
        .file_type();
    if file_type.is_dir() {
        Ok(GitEntry::Directory)
    } else if file_type.is_file() {
        Ok(GitEntry::File)
    } else {
        Err(CowshedError::integrity(
            format!(
                "{} is neither a directory nor a regular gitdir file",
                git.display()
            ),
            "verify a standalone repository or a linked worktree; a symlinked .git is refused",
        ))
    }
}

/// [`worktree_paths`], plus a `.git` file: it is not in the index, so its bytes are named here.
#[cfg(any(target_os = "macos", test))]
fn checkout_paths(checkout: &Path, entry: GitEntry) -> Result<Vec<PathBuf>> {
    let mut paths = worktree_paths(checkout)?;
    if entry == GitEntry::File {
        paths.push(PathBuf::from(".git"));
        paths.sort();
    }
    Ok(paths)
}

/// One Git-resolved path of `checkout`, absolute and without the newline `rev-parse` appends.
#[cfg(any(target_os = "macos", test))]
fn absolute_git_path(checkout: &Path, selector: &[&str]) -> Result<PathBuf> {
    let mut args = vec!["rev-parse", "--path-format=absolute"];
    args.extend_from_slice(selector);
    let output = probe(checkout, &args, &[0])?;
    let bytes = output.stdout.strip_suffix(b"\n").unwrap_or(&output.stdout);
    let path = PathBuf::from(OsString::from_vec(bytes.to_vec()));
    absolute(&path, "Git path")?;
    Ok(path)
}

/// The index bytes at `path`, or `None` when no index was ever written there.
#[cfg(any(target_os = "macos", test))]
fn index_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::integrity(
            format!("cannot read Git index {}: {error}", path.display()),
            "make the retained index readable before retrying",
        )),
    }
}

/// A linked worktree's copy must reach the same administrative directory and index as its
/// retained source. The index is read in place; no Git command runs against it, so nothing is
/// refreshed or written.
#[cfg(any(target_os = "macos", test))]
fn verify_linked_admin(root: &Path, retained_source: &Path) -> Result<()> {
    let copy_dir = absolute_git_path(root, &["--git-dir"])?;
    let source_dir = absolute_git_path(retained_source, &["--git-dir"])?;
    if copy_dir != source_dir {
        return Err(CowshedError::integrity(
            format!(
                "linked worktree admin differs: copy reaches {}, source reaches {}",
                copy_dir.display(),
                source_dir.display()
            ),
            "do not publish the replacement; keep the source and repair its .git file",
        ));
    }
    let copy_index = absolute_git_path(root, &["--git-path", "index"])?;
    let source_index = absolute_git_path(retained_source, &["--git-path", "index"])?;
    let same_bytes = index_bytes(&copy_index)? == index_bytes(&source_index)?;
    if copy_index != source_index || !same_bytes {
        return Err(CowshedError::integrity(
            format!(
                "linked worktree index differs: copy {}, source {}",
                copy_index.display(),
                source_index.display()
            ),
            "do not publish the replacement; the index must be the source's exact bytes",
        ));
    }
    Ok(())
}

/// Refuses a copy whose own `git fsck` failed, naming the exit status and both output streams.
#[cfg(any(target_os = "macos", test))]
fn require_fsck(root: &Path, actual: &GitPreservationSnapshot) -> Result<()> {
    if actual.fsck_succeeded() {
        return Ok(());
    }
    Err(CowshedError::integrity(
        format!(
            "Git preservation fsck failed at {}: code {:?}, signal {:?}; stdout: {}; stderr: {}",
            root.display(),
            actual.fsck.code,
            actual.fsck.signal,
            String::from_utf8_lossy(&actual.fsck.stdout),
            String::from_utf8_lossy(&actual.fsck.stderr)
        ),
        "do not publish a repository with failing fsck; retain and repair the source",
    ))
}

/// Verifies the copy at `root`, published at `canonical_checkout`, against `expected` and the
/// source still retained at `retained_source`. Every recorded probe but status and resolved
/// config must match the copy's live capture. Those two must match between source and copy as
/// both read now ([`Live`]); the recorded ones are never replayed as current output. The raw
/// local config still matches the record byte for byte, and an included file inside the tree is
/// compared byte for byte with every other path. A standalone
/// `.git` directory must match the source byte for byte. A linked worktree's `.git` file must
/// match byte for byte and reach the same administrative directory and index, which are read in
/// place and never written. Every tracked and untracked-not-ignored path must match too.
#[cfg(any(target_os = "macos", test))]
pub(crate) fn verify_copy(
    root: &Path,
    canonical_checkout: &Path,
    expected: &GitPreservationSnapshot,
    retained_source: &Path,
) -> Result<()> {
    absolute(retained_source, "retained source")?;
    if canonical(root, "copy")? == canonical(retained_source, "retained source")? {
        return Err(CowshedError::usage(
            format!(
                "Git preservation copy and retained source are the same checkout: {}",
                root.display()
            ),
            "verify a copy against its own retained source, not against itself",
        ));
    }
    let entry = git_entry(retained_source)?;
    if git_entry(root)? != entry {
        return Err(CowshedError::integrity(
            format!(
                "Git state at {} differs in kind from the retained source at {}",
                root.display(),
                retained_source.display()
            ),
            "do not publish the replacement; keep the source and repair the copy",
        ));
    }
    let actual = capture(root, canonical_checkout)?;
    let live = Live {
        status_differs: actual.porcelain != filter_free_status(retained_source)?,
        resolved_config_differs: actual.local_config_resolved
            != resolved_local_config(retained_source)?,
    };
    if let Err(diff) = expected.compare_live(&actual, live) {
        return Err(CowshedError::integrity(
            format!(
                "{diff} at {}, compared with retained source {}",
                root.display(),
                retained_source.display()
            ),
            "do not publish the replacement; retain the source and inspect the named Git components",
        ));
    }
    require_fsck(root, &actual)?;
    match entry {
        GitEntry::Directory => {
            crate::copy::verify_tree_blocking(&retained_source.join(".git"), &root.join(".git"))?;
        }
        GitEntry::File => verify_linked_admin(root, retained_source)?,
    }
    let paths = checkout_paths(retained_source, entry)?;
    if paths != checkout_paths(root, entry)? {
        return Err(CowshedError::integrity(
            format!(
                "tracked and untracked-not-ignored paths differ between {} and {}",
                root.display(),
                retained_source.display()
            ),
            "do not publish the replacement; keep the source and repair the copy",
        ));
    }
    crate::copy::verify_relative_paths_blocking(retained_source, root, &paths)
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

    /// Replace a copied file's bytes and restore its modification time, so only content differs.
    /// Returns the bytes replaced, for the exact restore.
    fn plant(path: &Path, bytes: &[u8]) -> Vec<u8> {
        let original = fs::read(path).expect("read copied bytes");
        let modified = fs::metadata(path)
            .expect("stat copied file")
            .modified()
            .expect("copied modification time");
        fs::write(path, bytes).expect("plant copied bytes");
        fs::File::open(path)
            .expect("open planted file")
            .set_modified(modified)
            .expect("restore planted modification time");
        original
    }

    #[test]
    fn configured_filters_never_run_and_copies_verify_their_raw_state() {
        let repository = Repository::new();
        // A clean filter stores the index blob, so raw worktree bytes differ from it: the shape
        // git-crypt and LFS checkouts take. The copy must verify without running that filter.
        repository.git(&["config", "filter.enc.clean", "tr a-z A-Z"]);
        fs::write(
            repository.root.join(".gitattributes"),
            b"secret.txt filter=enc\n",
        )
        .expect("write attributes");
        fs::write(repository.root.join("secret.txt"), b"raw secret\n")
            .expect("write filtered file");
        repository.git(&["add", ".gitattributes", "secret.txt"]);
        repository.git(&["commit", "-qm", "filtered"]);
        // A populated submodule with a filtered README. Its clone is made before any driver is
        // configured, so checkout never runs one.
        let submodule_source = Repository::new();
        fs::write(
            submodule_source.root.join(".gitattributes"),
            b"README filter=enc\n",
        )
        .expect("write submodule attributes");
        submodule_source.git(&["add", ".gitattributes"]);
        submodule_source.git(&["commit", "-qm", "attributes"]);
        repository.git(&[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            submodule_source.root.to_str().expect("UTF-8 fixture path"),
            "subdir",
        ]);
        repository.git(&["commit", "-qm", "submodule"]);
        let text = |bytes: Vec<u8>| {
            String::from_utf8(bytes)
                .expect("Git output is text")
                .trim_end()
                .to_owned()
        };
        // Both drivers fail after leaving a marker, so any run is observable and refused.
        let marker = repository.scratch.join("filter-ran");
        let driver = format!("sh -c 'printf ran >> \"{}\"; exit 1'", marker.display());
        repository.git(&["config", "filter.enc.clean", driver.as_str()]);
        repository.git(&["config", "filter.enc.process", driver.as_str()]);
        repository.git(&["config", "filter.enc.required", "true"]);
        // The submodule runs the same failing drivers from its own config, which no probe may run.
        repository.git(&[
            "-C",
            "subdir",
            "config",
            "filter.enc.clean",
            driver.as_str(),
        ]);
        repository.git(&[
            "-C",
            "subdir",
            "config",
            "filter.enc.process",
            driver.as_str(),
        ]);
        repository.git(&["-C", "subdir", "config", "filter.enc.required", "true"]);
        // Same-size bytes with a new modification time force a child status to hash the file, so
        // an ordinary superproject status would run the submodule's driver.
        let submodule_readme = repository.root.join("subdir/README");
        fs::write(&submodule_readme, b"SECOND\n").expect("dirty submodule file");
        fs::File::open(&submodule_readme)
            .expect("open submodule file")
            .set_modified(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2))
            .expect("make submodule stat data stale");
        // An ignored file inside the submodule: the copy verifier must still compare its bytes.
        let exclude = repository.root.join(".git/modules/subdir/info/exclude");
        fs::create_dir_all(exclude.parent().expect("exclude directory")).expect("submodule info");
        fs::write(&exclude, b"ignored.log\n").expect("ignore submodule log");
        fs::write(repository.root.join("subdir/ignored.log"), b"ignored\n")
            .expect("write ignored submodule file");
        // Commit-level drift: an unrecorded commit the copy's submodule HEAD will move onto.
        let tree = text(repository.git(&["-C", "subdir", "rev-parse", "HEAD^{tree}"]));
        let head = text(repository.git(&["-C", "subdir", "rev-parse", "HEAD"]));
        let advanced = text(repository.git(&[
            "-C",
            "subdir",
            "-c",
            "user.name=Cowshed Test",
            "-c",
            "user.email=test@example.invalid",
            "commit-tree",
            tree.as_str(),
            "-p",
            head.as_str(),
            "-m",
            "advance",
        ]));
        // Ordinary Git runs the driver; the marker is what a probe run would leave behind.
        let ordinary = crate::git::git_command_at(&repository.root)
            .args(["hash-object", "--path=secret.txt", "secret.txt"])
            .output_locked()
            .expect("run ordinary Git conversion");
        assert!(
            !ordinary.status.success(),
            "a failing required filter refuses ordinary Git"
        );
        assert!(marker.exists(), "ordinary Git runs the configured filter");
        fs::remove_file(&marker).expect("clear the control marker");
        // Control: an ordinary status descends into the dirty submodule and runs its driver, so the
        // dirty-submodule witness below is not vacuous.
        crate::git::git_command_at(&repository.root)
            .env("GIT_OPTIONAL_LOCKS", "0")
            .args(["status", "--porcelain=v2", "-z", "--ignore-submodules=none"])
            .output_locked()
            .expect("run ordinary superproject status");
        assert!(
            marker.exists(),
            "ordinary status runs the submodule's configured driver"
        );
        fs::remove_file(&marker).expect("clear the submodule control marker");

        fs::write(repository.root.join("README"), b"dirty\n").expect("dirty tracked file");
        fs::write(repository.root.join("scratch.txt"), b"untracked\n").expect("untracked file");
        let config = repository.root.join(".git/config");
        let index = repository.root.join(".git/index");
        let config_before = fs::read(&config).expect("read local config");
        let index_before = fs::read(&index).expect("read index");
        let submodule_config = repository.root.join(".git/modules/subdir/config");
        let submodule_index = repository.root.join(".git/modules/subdir/index");
        let submodule_config_before = fs::read(&submodule_config).expect("read submodule config");
        let submodule_index_before = fs::read(&submodule_index).expect("read submodule index");
        let before = repository.snapshot();
        let status = String::from_utf8_lossy(&before.porcelain.stdout).into_owned();
        assert!(
            status.contains(" README\0") && status.contains("? scratch.txt\0"),
            "{status}"
        );
        assert!(!marker.exists(), "capture never runs a configured filter");

        let staging = repository.scratch.join("stage");
        fs::create_dir(&staging).expect("create copy destination");
        crate::copy::copy_until_quiescent_blocking(&repository.root, &staging)
            .expect("copy checkout");
        let verify = || verify_copy(&staging, &repository.root, &before, &repository.root);
        verify().expect("filter-free copy verifies against its retained source");

        // Each planted difference must refuse, and the exact original bytes must verify again.
        let mut flipped_index = index_before.clone();
        *flipped_index.last_mut().expect("index checksum byte") ^= 0xff;
        let mut planted_config = config_before.clone();
        planted_config.extend_from_slice(b"\n# planted\n");
        let planted = [
            (staging.join(".git/index"), flipped_index),
            (staging.join(".git/config"), planted_config),
            (staging.join("README"), b"DIRTY\n".to_vec()),
            (staging.join("scratch.txt"), b"UNTRACKED\n".to_vec()),
            (staging.join("secret.txt"), b"RAW SECRET\n".to_vec()),
            (staging.join("subdir/README"), b"SECONX\n".to_vec()),
            (staging.join("subdir/ignored.log"), b"IGNORED\n".to_vec()),
        ];
        for (path, bytes) in planted {
            let original = plant(&path, &bytes);
            let error = verify().expect_err(&format!("planted bytes in {}", path.display()));
            assert_eq!(error.code, crate::ErrorCode::Integrity, "{}", error.message);
            plant(&path, &original);
            verify().expect("restored bytes verify again");
        }
        // A file only the copy holds is a path the source never had, never a silent omission.
        fs::write(staging.join("extra.txt"), b"only in the copy\n").expect("plant extra file");
        let error = verify().expect_err("a copy-only untracked file must refuse");
        assert_eq!(error.code, crate::ErrorCode::Integrity, "{}", error.message);
        fs::remove_file(staging.join("extra.txt")).expect("remove extra file");
        verify().expect("removed extra file verifies again");
        // A file only the copy holds inside the submodule is refused by the subtree compare.
        let subdir = staging.join("subdir");
        let subdir_modified = fs::metadata(&subdir)
            .expect("stat copied submodule directory")
            .modified()
            .expect("copied submodule modification time");
        fs::write(staging.join("subdir/extra.txt"), b"only in the copy\n")
            .expect("plant submodule extra file");
        let error = verify().expect_err("a copy-only file inside the submodule must refuse");
        assert_eq!(error.code, crate::ErrorCode::Integrity, "{}", error.message);
        fs::remove_file(staging.join("subdir/extra.txt")).expect("remove submodule extra file");
        fs::File::open(&subdir)
            .expect("open copied submodule directory")
            .set_modified(subdir_modified)
            .expect("restore the directory metadata changed by the planted extra file");
        verify().expect("removed submodule extra file verifies again");
        // Commit-level pointer drift: the copy's submodule HEAD moves onto the unrecorded commit.
        // Status names it, so the refusal must name porcelain.
        let submodule_head = staging.join(".git/modules/subdir/HEAD");
        let recorded_head = plant(&submodule_head, format!("{advanced}\n").as_bytes());
        let error = verify().expect_err("a drifted submodule pointer must refuse");
        assert_eq!(error.code, crate::ErrorCode::Integrity, "{}", error.message);
        assert!(error.message.contains("porcelain"), "{}", error.message);
        plant(&submodule_head, &recorded_head);
        verify().expect("restored submodule pointer verifies again");

        assert!(
            !marker.exists(),
            "no verification step ran a configured filter"
        );
        assert_eq!(
            fs::read(&config).expect("reread config"),
            config_before,
            "config unchanged"
        );
        assert_eq!(
            fs::read(&index).expect("reread index"),
            index_before,
            "index unchanged"
        );
        assert_eq!(
            fs::read(&submodule_config).expect("reread submodule config"),
            submodule_config_before,
            "submodule config unchanged"
        );
        assert_eq!(
            fs::read(&submodule_index).expect("reread submodule index"),
            submodule_index_before,
            "submodule index unchanged"
        );
    }

    #[test]
    fn linked_worktree_copy_reaches_the_same_external_admin() {
        let repository = Repository::new();
        let linked_path = repository.scratch.join("linked");
        repository.git(&[
            "worktree",
            "add",
            "-q",
            "--detach",
            linked_path.to_str().unwrap(),
        ]);
        // Git writes the real path into the worktree's administrative back-pointer; the
        // canonical publication path must name that same path for the registration to match.
        let linked = fs::canonicalize(&linked_path).expect("resolve linked worktree");
        fs::write(linked.join("README"), b"dirty\n").expect("dirty linked file");
        fs::write(linked.join("scratch.txt"), b"untracked\n").expect("untracked linked file");
        let before = capture(&linked, &linked).expect("capture linked worktree");
        let status = String::from_utf8_lossy(&before.porcelain.stdout).into_owned();
        assert!(
            status.contains(" README\0") && status.contains("? scratch.txt\0"),
            "{status}"
        );
        let gitfile = linked.join(".git");
        let gitfile_before = fs::read(&gitfile).expect("read linked gitfile");
        let admin_index = repository.root.join(".git/worktrees/linked/index");
        let admin_index_before = fs::read(&admin_index).expect("read linked admin index");

        let staging = repository.scratch.join("stage");
        fs::create_dir(&staging).expect("create copy destination");
        crate::copy::copy_until_quiescent_blocking(&linked, &staging)
            .expect("copy linked worktree");
        let verify = || verify_copy(&staging, &linked, &before, &linked);
        verify().expect("linked copy verifies against the same external admin");

        // The gitfile is compared byte for byte; git parsing it is not evidence that it matches.
        let original = plant(&staging.join(".git"), b"gitdir: /nonexistent/admin\n");
        let error = verify().expect_err("a retargeted gitfile must refuse");
        assert_eq!(error.code, crate::ErrorCode::Integrity, "{}", error.message);
        plant(&staging.join(".git"), &original);
        verify().expect("restored gitfile verifies again");

        assert_eq!(
            fs::read(&gitfile).expect("reread linked gitfile"),
            gitfile_before
        );
        assert_eq!(
            fs::read(&admin_index).expect("reread linked admin index"),
            admin_index_before,
            "linked admin index unchanged"
        );
    }

    /// A local config that includes a file at an absolute path inside its own checkout resolves
    /// that include to nothing while the checkout is an empty mountpoint, as it is when a
    /// re-adoption verifies its staged copy. The copy verifies against its retained source as
    /// both resolve now, and the included file's bytes are still compared with the tree.
    #[test]
    fn a_config_including_a_file_in_its_own_checkout_verifies_from_a_staged_copy() {
        let repository = Repository::new();
        let included = repository.root.join("tooling/workspace.gitconfig");
        fs::create_dir_all(included.parent().expect("tooling directory")).expect("tooling");
        fs::write(&included, b"[core]\n\tabbrev = 12\n").expect("write the included config");
        repository.git(&["add", "tooling/workspace.gitconfig"]);
        repository.git(&["commit", "-qm", "workspace config"]);
        repository.git(&[
            "config",
            "include.path",
            included.to_str().expect("a UTF-8 include path"),
        ]);
        let before = repository.snapshot();
        assert!(
            before
                .local_config_resolved
                .stdout
                .windows(b"core.abbrev\n12".len())
                .any(|bytes| bytes == b"core.abbrev\n12"),
            "the include resolves while the checkout holds it"
        );

        // The checkout's tree moves to its retained source, leaving the checkout path empty.
        let retained = repository.scratch.join("retained");
        fs::rename(&repository.root, &retained).expect("retain the source");
        fs::create_dir(&repository.root).expect("the empty checkout mountpoint");
        let staging = repository.scratch.join("stage");
        fs::create_dir(&staging).expect("create copy destination");
        crate::copy::copy_until_quiescent_blocking(&retained, &staging).expect("copy checkout");
        let verify = || verify_copy(&staging, &repository.root, &before, &retained);
        verify().expect("the copy verifies while its include cannot resolve");

        let original = plant(
            &staging.join("tooling/workspace.gitconfig"),
            b"[core]\n\tabbrev = 7\n",
        );
        let error = verify().expect_err("the included file's bytes are verified");
        assert_eq!(error.code, crate::ErrorCode::Integrity, "{}", error.message);
        plant(&staging.join("tooling/workspace.gitconfig"), &original);
        verify().expect("restored bytes verify again");
    }
}
