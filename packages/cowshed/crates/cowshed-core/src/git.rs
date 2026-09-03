use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;

use tokio::process::Command;

use crate::api::dto::GitOid;
use crate::error::{CowshedError, Result};
use crate::workspace_environment::WORKSPACE_ENVIRONMENT_PATH;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteUrl {
    pub name: String,
    pub url: PathBuf,
}

/// The name a workspace's upstream carries: the main workspace, so `main`.
///
/// `git fetch main` then reads as what it does, and an agent that has never seen this codebase
/// guesses it on the first try — which the previous name, `host`, did not deliver: it named a
/// machine rather than something you fetch from.
pub const MAIN_REMOTE: &str = "main";

/// Where cowshed's upstream goes when the workspace already has a remote named `main` that is not
/// it. Cowshed adds and lets the user remove; it never retargets a remote it did not create.
pub const FALLBACK_MAIN_REMOTE: &str = "cowshed-main";

/// The name this remote carried before it was named for what it is. Cowshed created it, so cowshed
/// removes it: it is not a user remote, and it names the recorded checkout rather than the mount.
const LEGACY_MAIN_REMOTE: &str = "host";

/// The config key that records "cowshed created this remote and may retarget it".
///
/// Ownership has to be a written-down fact rather than one inferred from the URL. Inferring it
/// failed in exactly one direction and it was the expensive one: after a checkout moved, the `main`
/// remote still held the *old* mount path, so a URL comparison read it as somebody else's remote,
/// cowshed stood beside it under `cowshed-main`, and every workspace kept a `main` remote pointing
/// at a directory that had stopped being a repository. `git fetch main` then failed forever and
/// `mv`/`attach` both reported success. A remote git namespaces under `remote.<name>.*` is the
/// natural home for the fact, so the answer travels with the thing it describes.
const REMOTE_OWNER_KEY: &str = "cowshed";

/// The name a workspace's `main` remote is actually registered under, once configuration has run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MainRemote {
    /// The remote is `main`, either freshly created or already correct.
    Canonical,
    /// A foreign remote holds the name `main`, so cowshed's is `cowshed-main`. `next:` hints must
    /// print this name rather than the canonical one — the guidance has to name what exists.
    Displaced,
}

impl MainRemote {
    pub const fn remote_name(self) -> &'static str {
        match self {
            Self::Canonical => MAIN_REMOTE,
            Self::Displaced => FALLBACK_MAIN_REMOTE,
        }
    }
}

/// One `merge.<name>.driver` entry and whether its program survives a moved checkout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeDriver {
    pub name: String,
    pub state: MergeDriverState,
}

/// How a merge driver's program is spelt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeDriverState {
    /// Already a repository-relative path, which is the only relocation-proof spelling.
    Relative,
    /// An absolute path that named a file inside this repository, rewritten to `to`.
    Relativized { to: String },
    /// An absolute path with no counterpart inside this repository. Cowshed will not invent one:
    /// the driver is the project's, and a wrong guess would silently merge with the wrong program.
    Unresolvable { program: String },
}

/// Cowshed's upstream in this workspace, read without writing.
///
/// Displacement is accounted for: a user-owned `main` is never treated as cowshed's, even when
/// that URL is not a local path. Doctor reports `repository == false`; it never retargets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CowshedUpstream {
    pub remote_name: String,
    pub url: Option<PathBuf>,
    pub repository: bool,
}

/// Where a linked-worktree registration is taken out before its pointer is moved to the mount
/// root. Inside the image, so it is on the workspace's own volume and gone before the mount is
/// published; under `.cowshed/` because that subtree is already cowshed's and not the repository's.
const WORKTREE_STAGING: &str = ".cowshed/worktree-staging";

const WORKSPACE_ENVIRONMENT_MARKER: &[u8] = b"# cowshed: workspace environment";
const LOCAL_ENVIRONMENT_LOADER: &[u8] = b"source_env_if_exists \"$local_override\"";

fn workspace_environment_source() -> String {
    format!("source_env_if_present {WORKSPACE_ENVIRONMENT_PATH}")
}

fn local_workspace_environment_source() -> String {
    format!("source_env_if_exists \"${{local_override%/*}}/{WORKSPACE_ENVIRONMENT_PATH}\"")
}

/// The name of the remote main registers for a workspace under `--register`.
pub fn workspace_remote_name(workspace: &str) -> String {
    format!("cowshed/{workspace}")
}

fn workspace_branch(name: &str) -> (String, String) {
    let branch = format!("cowshed/{name}");
    let branch_ref = format!("refs/heads/{branch}");
    (branch, branch_ref)
}

#[derive(Clone, Debug)]
pub struct GitRepository {
    root: PathBuf,
    /// One extra object store, attached read-only for the lifetime of this handle.
    ///
    /// This is how a workspace is compared against main's *current* branch tip without fetching.
    /// Do not "simplify" it back into a `git fetch main`: the workspace's `main` remote is a
    /// clone-time artifact whose URL has been observed pointing at a directory that is no longer a
    /// repository, and a fetch would write FETCH_HEAD and pack objects into every workspace during
    /// what a caller reasonably expects to be a read-only listing. Attaching main's object store
    /// instead reads the same objects with no writes anywhere and no dependence on a remote URL.
    /// A fetch is also no use as a fallback: if main's object store cannot be read then its objects
    /// cannot be obtained by any route, so the honest answer is still "cannot determine".
    alternate_objects: Option<PathBuf>,
}

#[derive(Debug)]
struct WorkspaceEnvironmentHook {
    path: PathBuf,
    relative: PathBuf,
    exists: bool,
}

/// Resolve a repository-owned hook without replacing a relocatable link or writing outside its tree.
fn workspace_environment_hook(root: &Path) -> Result<WorkspaceEnvironmentHook> {
    let hook = root.join(".envrc");
    let metadata = match fs::symlink_metadata(&hook) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WorkspaceEnvironmentHook {
                path: hook,
                relative: PathBuf::from(".envrc"),
                exists: false,
            });
        }
        Err(error) => {
            return Err(CowshedError::integrity(
                format!(
                    "cannot inspect workspace environment hook {}: {error}",
                    hook.display()
                ),
                "repair the repository .envrc and retry",
            ));
        }
    };
    if !metadata.file_type().is_symlink() {
        return Ok(WorkspaceEnvironmentHook {
            path: hook,
            relative: PathBuf::from(".envrc"),
            exists: true,
        });
    }

    let link_target = fs::read_link(&hook).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot read workspace environment hook {}: {error}",
                hook.display()
            ),
            "repair the repository .envrc and retry",
        )
    })?;
    if link_target.is_absolute() {
        return Err(CowshedError::integrity(
            format!(
                "workspace environment hook {} has a non-relocatable absolute target {}",
                hook.display(),
                link_target.display()
            ),
            "replace .envrc with a relative symlink to a file inside the repository",
        ));
    }

    let staged_root = fs::canonicalize(root).map_err(|error| {
        CowshedError::integrity(
            format!("cannot resolve workspace root {}: {error}", root.display()),
            "repair the repository directory and retry",
        )
    })?;
    let resolved = fs::canonicalize(&hook).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot resolve workspace environment hook {}: {error}",
                hook.display()
            ),
            "repair the repository .envrc and retry",
        )
    })?;
    let Ok(relative) = resolved.strip_prefix(&staged_root) else {
        return Err(CowshedError::integrity(
            format!(
                "workspace environment hook {} resolves outside workspace {} to {}",
                hook.display(),
                root.display(),
                resolved.display()
            ),
            "replace .envrc with a regular file or a symlink to a file inside the repository",
        ));
    };
    let relative = relative.to_owned();
    Ok(WorkspaceEnvironmentHook {
        path: resolved,
        relative,
        exists: true,
    })
}

fn environment_hook_contains(bytes: &[u8], line: &[u8]) -> bool {
    bytes
        .split(|byte| *byte == b'\n')
        .any(|candidate| candidate == line)
}

fn read_environment_hook(path: &Path) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| {
            CowshedError::integrity(
                format!(
                    "cannot open workspace environment hook {}: {error}",
                    path.display()
                ),
                "repair the repository environment hook and retry",
            )
        })?;
    let mut existing = Vec::new();
    file.read_to_end(&mut existing).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot read workspace environment hook {}: {error}",
                path.display()
            ),
            "repair the repository environment hook and retry",
        )
    })?;
    Ok(existing)
}

fn append_environment_hook(root: &Path, path: &Path, source: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| {
            CowshedError::integrity(
                format!(
                    "cannot open workspace environment hook {}: {error}",
                    path.display()
                ),
                "repair the repository environment hook and retry",
            )
        })?;
    let mut existing = Vec::new();
    file.read_to_end(&mut existing).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot read workspace environment hook {}: {error}",
                path.display()
            ),
            "repair the repository environment hook and retry",
        )
    })?;
    if environment_hook_contains(&existing, source) {
        return Ok(());
    }

    let mut addition = Vec::with_capacity(WORKSPACE_ENVIRONMENT_MARKER.len() + source.len() + 3);
    if !existing.is_empty() && !existing.ends_with(b"\n") {
        addition.push(b'\n');
    }
    addition.extend_from_slice(WORKSPACE_ENVIRONMENT_MARKER);
    addition.push(b'\n');
    addition.extend_from_slice(source);
    addition.push(b'\n');
    file.write_all(&addition).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot update workspace environment hook {}: {error}",
                path.display()
            ),
            "repair the repository environment hook and retry",
        )
    })?;
    file.sync_all().map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot sync workspace environment hook {}: {error}",
                path.display()
            ),
            "repair the repository environment hook and retry",
        )
    })?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            CowshedError::integrity(
                format!("cannot sync workspace root {}: {error}", root.display()),
                "repair the repository directory and retry",
            )
        })
}

impl GitRepository {
    /// Resolve the standalone repository containing `path`.
    pub async fn discover(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let output = run_git_at(path, ["rev-parse", "--show-toplevel"]).await?;
        if output.status.success() {
            return Ok(Self::from_root(parse_one_path(&output.stdout, "git root")?));
        }

        let git_dir_output =
            run_git_at(path, ["rev-parse", "--path-format=absolute", "--git-dir"]).await?;
        if git_dir_output.status.success() {
            let git_dir = parse_one_path(&git_dir_output.stdout, "git directory")?;
            if git_dir.file_name() == Some(OsStr::new(".git"))
                && let Some(root) = git_dir.parent()
            {
                return Ok(Self::from_root(root));
            }
        }

        Err(CowshedError::environment_missing(
            format!(
                "{} is not inside a standalone git repository",
                path.display()
            ),
            "cowshed adopt <git-root>",
        ))
    }

    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            alternate_objects: None,
        }
    }

    /// Attach `objects` as an additional read-only object store for every command run through this
    /// handle, so revisions living only in that store resolve here.
    ///
    /// Refused rather than silently mangled for a path git cannot express in
    /// `GIT_ALTERNATE_OBJECT_DIRECTORIES`, whose entries are colon-separated: a store under such a
    /// path would be dropped, and a dropped object store reads as "these commits do not exist".
    pub fn with_alternate_objects(mut self, objects: impl Into<PathBuf>) -> Result<Self> {
        let objects = objects.into();
        if objects.as_os_str().as_encoded_bytes().contains(&b':') {
            return Err(CowshedError::conflict(
                format!(
                    "git cannot attach an object store whose path contains a colon: {}",
                    objects.display()
                ),
                "move the cowshed store to a path without a colon",
            ));
        }
        self.alternate_objects = Some(objects);
        Ok(self)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the first in-progress repository operation, if any.
    pub async fn in_progress_operation(&self) -> Result<Option<String>> {
        for state in [
            "MERGE_HEAD",
            "rebase-merge",
            "rebase-apply",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
        ] {
            let output = self.run(["rev-parse", "--git-path", state]).await?;
            if !output.status.success() {
                return Err(git_internal("resolve repository operation state", &output));
            }
            let state_path = parse_one_path(&output.stdout, "git operation path")?;
            let absolute = if state_path.is_absolute() {
                state_path
            } else {
                self.root.join(state_path)
            };
            if absolute.exists() {
                return Ok(Some(state.to_owned()));
            }
        }
        Ok(None)
    }

    /// Reject in-progress repository operations; ordinary dirty work is intentionally allowed.
    pub async fn ensure_adoptable(&self) -> Result<()> {
        if let Some(state) = self.in_progress_operation().await? {
            return Err(CowshedError::conflict(
                format!("repository has an in-progress {state} operation"),
                format!(
                    "finish or abort the git operation, then run: cowshed adopt {}",
                    self.root.display()
                ),
            ));
        }
        Ok(())
    }

    pub async fn remotes(&self) -> Result<Vec<RemoteUrl>> {
        let output = self
            .run([
                "config",
                "--local",
                "-z",
                "--get-regexp",
                r"^remote\..*\.url$",
            ])
            .await?;
        if output.status.code() == Some(1) {
            return Ok(Vec::new());
        }
        if !output.status.success() {
            return Err(git_internal("list git remotes", &output));
        }

        let mut remotes = Vec::new();
        for record in output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|r| !r.is_empty())
        {
            let separator = record
                .iter()
                .position(|byte| *byte == b'\n')
                .ok_or_else(|| {
                    CowshedError::integrity(
                        "git reported a remote URL with no value",
                        "repair the git configuration",
                    )
                })?;
            let key = std::str::from_utf8(&record[..separator]).map_err(|_| {
                CowshedError::integrity(
                    "git reported a non-UTF-8 remote name",
                    "repair the git configuration",
                )
            })?;
            let name = key
                .strip_prefix("remote.")
                .and_then(|key| key.strip_suffix(".url"))
                .ok_or_else(|| {
                    CowshedError::integrity(
                        format!("git reported an unexpected remote URL key: {key}"),
                        "repair the git configuration",
                    )
                })?;
            remotes.push(RemoteUrl {
                name: name.to_owned(),
                url: PathBuf::from(OsString::from_vec(record[separator + 1..].to_vec())),
            });
        }
        remotes.sort_by(|left, right| (&left.name, &left.url).cmp(&(&right.name, &right.url)));
        remotes.dedup();
        Ok(remotes)
    }

    pub async fn head_oid(&self) -> Result<GitOid> {
        let value = self.read_one(["rev-parse", "HEAD"], "read HEAD").await?;
        parse_oid(value, "HEAD")
    }

    pub async fn current_branch(&self) -> Result<Option<String>> {
        let output = self
            .run(["symbolic-ref", "--quiet", "--short", "HEAD"])
            .await?;
        if output.status.success() {
            return Ok(Some(parse_one_string(&output.stdout, "branch name")?));
        }
        if output.status.code() == Some(1) {
            return Ok(None);
        }
        Err(git_internal("read current branch", &output))
    }

    /// Whether the working tree holds work: a tracked change, or an untracked file that is
    /// not junk. `git status` already omits ignored files; an untracked HIDDEN path - a
    /// `.nx/` daemon log, a `.cache/`, a tool's dot-directory - is junk by the same reading,
    /// and treating it as work made every landed workspace unremovable until someone deleted
    /// a log by hand.
    pub async fn is_dirty(&self) -> Result<bool> {
        let output = self.porcelain_status("read repository status").await?;
        Ok(
            porcelain_records(&output.stdout)
                .any(|(status, path)| !is_untracked_junk(status, path)),
        )
    }

    pub async fn ensure_cowshed_excludes(&self) -> Result<()> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let git_directory = root.join(".git");
            let info_directory = git_directory.join("info");
            for directory in [&git_directory, &info_directory] {
                let metadata = fs::symlink_metadata(directory).map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "cannot inspect Git metadata directory {}: {error}",
                            directory.display()
                        ),
                        "restore the standalone workspace Git metadata and retry",
                    )
                })?;
                if !metadata.file_type().is_dir() {
                    return Err(CowshedError::integrity(
                        format!(
                            "Git metadata path is not a real directory: {}",
                            directory.display()
                        ),
                        "restore the standalone workspace Git metadata and retry",
                    ));
                }
            }
            let exclude = info_directory.join("exclude");
            let mut file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&exclude)
                .map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "cannot open Git exclude file {}: {error}",
                            exclude.display()
                        ),
                        "restore the standalone workspace Git metadata and retry",
                    )
                })?;
            let mut existing = Vec::new();
            file.read_to_end(&mut existing).map_err(|error| {
                CowshedError::integrity(
                    format!(
                        "cannot read Git exclude file {}: {error}",
                        exclude.display()
                    ),
                    "repair .git/info/exclude and retry",
                )
            })?;
            let mut addition = Vec::new();
            for pattern in [b".cowshed/".as_slice(), b".fseventsd/".as_slice()] {
                if !existing
                    .split(|byte| *byte == b'\n')
                    .any(|line| line == pattern)
                {
                    if !existing.is_empty() && !existing.ends_with(b"\n") && addition.is_empty() {
                        addition.push(b'\n');
                    }
                    addition.extend_from_slice(pattern);
                    addition.push(b'\n');
                }
            }
            if !addition.is_empty() {
                file.write_all(&addition).map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "cannot update Git exclude file {}: {error}",
                            exclude.display()
                        ),
                        "repair .git/info/exclude and retry",
                    )
                })?;
                file.sync_all().map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "cannot sync Git exclude file {}: {error}",
                            exclude.display()
                        ),
                        "repair .git/info/exclude and retry",
                    )
                })?;
                File::open(&info_directory)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|error| {
                        CowshedError::integrity(
                            format!(
                                "cannot sync Git info directory {}: {error}",
                                info_directory.display()
                            ),
                            "repair .git/info and retry",
                        )
                    })?;
            }
            Ok(())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("Git exclude task failed: {error}")))?
    }

    async fn path_is_tracked(&self, path: &Path) -> Result<bool> {
        let args = [
            OsStr::new("ls-files"),
            OsStr::new("--error-unmatch"),
            OsStr::new("--"),
            path.as_os_str(),
        ];
        let output = self.run(args).await?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(git_internal("inspect tracked environment hook", &output)),
        }
    }

    async fn path_is_ignored(&self, path: &Path) -> Result<bool> {
        let args = [
            OsStr::new("check-ignore"),
            OsStr::new("--quiet"),
            OsStr::new("--"),
            path.as_os_str(),
        ];
        let output = self.run(args).await?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(git_internal("inspect ignored environment hook", &output)),
        }
    }

    /// Add the one repository-visible hook that loads cowshed's in-image environment.
    ///
    /// A tracked hook is immutable workspace input. When it exposes the repository's ignored local
    /// override, cowshed writes there instead; otherwise publication fails rather than making every
    /// new workspace dirty. Untracked hooks retain the direct append behavior.
    pub async fn ensure_workspace_environment_wiring(&self) -> Result<()> {
        let root = self.root.clone();
        let (hook, existing) = tokio::task::spawn_blocking(move || {
            let hook = workspace_environment_hook(&root)?;
            let existing = if hook.exists {
                read_environment_hook(&hook.path)?
            } else {
                Vec::new()
            };
            Ok::<_, CowshedError>((hook, existing))
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!(
                "workspace environment inspection task failed: {error}"
            ))
        })??;
        if environment_hook_contains(&existing, workspace_environment_source().as_bytes()) {
            return Ok(());
        }

        let tracked = self.path_is_tracked(&hook.relative).await?;
        let (path, source) = if tracked {
            if !environment_hook_contains(&existing, LOCAL_ENVIRONMENT_LOADER) {
                return Err(CowshedError::integrity(
                    format!(
                        "tracked workspace environment hook {} has no local override loader",
                        hook.relative.display()
                    ),
                    "add an ignored local environment hook before creating a workspace",
                ));
            }
            let local_relative = Path::new(".envrc-local");
            let local_path = self.root.join(local_relative);
            if self.path_is_tracked(local_relative).await? {
                let local_path_for_read = local_path.clone();
                let local_existing = tokio::task::spawn_blocking(move || {
                    read_environment_hook(&local_path_for_read)
                })
                .await
                .map_err(|error| {
                    CowshedError::internal(format!(
                        "workspace local environment inspection task failed: {error}"
                    ))
                })??;
                if environment_hook_contains(
                    &local_existing,
                    local_workspace_environment_source().as_bytes(),
                ) {
                    return Ok(());
                }
                return Err(CowshedError::integrity(
                    "tracked .envrc-local does not load the cowshed workspace environment",
                    "make .envrc-local untracked and ignored before creating a workspace",
                ));
            }
            if !self.path_is_ignored(local_relative).await? {
                return Err(CowshedError::integrity(
                    "repository .envrc-local is not ignored",
                    "add .envrc-local to the repository ignore rules before creating a workspace",
                ));
            }
            (local_path, local_workspace_environment_source())
        } else {
            if !hook.exists && !self.path_is_ignored(&hook.relative).await? {
                return Err(CowshedError::integrity(
                    "creating .envrc would make the new workspace dirty",
                    "track a workspace environment hook or ignore .envrc before creating a workspace",
                ));
            }
            (hook.path, workspace_environment_source())
        };

        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            append_environment_hook(&root, &path, source.as_bytes())
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("workspace environment wiring task failed: {error}"))
        })?
    }

    /// Resolve `revision` to the commit object this repository actually holds for it.
    ///
    /// `None` — no such ref, or an oid whose object is not here — is an ordinary negative result
    /// rather than an error: session repositories can contain unpublished objects that the
    /// controller-side host repository has never seen, and every proof built on top of this one
    /// reads that as "not held here".
    ///
    /// `rev-parse --verify --quiet` is the only spelling that answers this in one call for both
    /// refs and oids. `show-ref --verify` is fatal (exit 128) on a ref that is merely absent, and
    /// `cat-file -e` cannot take a ref name.
    async fn resolve_commit(&self, revision: &str) -> Result<Option<GitOid>> {
        let peeled = format!("{revision}^{{commit}}");
        let output = self
            .run(["rev-parse", "--verify", "--quiet", peeled.as_str()])
            .await?;
        match output.status.code() {
            Some(0) => parse_one_string(&output.stdout, "commit revision")
                .and_then(|value| parse_oid(value, "commit revision"))
                .map(Some),
            Some(1) => Ok(None),
            _ => Err(git_internal("resolve commit revision", &output)),
        }
    }

    async fn has_commit(&self, commit: &str) -> Result<bool> {
        Ok(self.resolve_commit(commit).await?.is_some())
    }

    /// Whether `commit` is contained by a host branch or a Cowshed preservation ref.
    pub async fn commit_is_preserved(&self, commit: &str) -> Result<bool> {
        self.commit_contained_in(
            commit,
            &["refs/heads", "refs/cowshed"],
            "check host commit preservation refs",
        )
        .await
    }

    /// Whether `commit` is contained by a remote-tracking ref in this repository.
    ///
    /// This is the conservative, offline proof used before deleting an adopted main image:
    /// local heads disappear with that image, while a remote-tracking ref records a push/fetch
    /// boundary whose remote retains the commit.
    pub async fn commit_is_remote_preserved(&self, commit: &str) -> Result<bool> {
        self.commit_contained_in(
            commit,
            &["refs/remotes"],
            "check remote commit preservation refs",
        )
        .await
    }

    async fn commit_contained_in(
        &self,
        commit: &str,
        refs: &[&str],
        operation: &str,
    ) -> Result<bool> {
        if !self.has_commit(commit).await? {
            return Ok(false);
        }
        let mut args = vec![
            OsString::from("for-each-ref"),
            OsString::from("--format=%(refname)"),
            OsString::from("--contains"),
            OsString::from(commit),
        ];
        args.extend(refs.iter().map(OsString::from));
        let output = self.run(args).await?;
        if !output.status.success() {
            return Err(git_internal(operation, &output));
        }
        Ok(!output.stdout.is_empty())
    }

    /// The tip of local branch `branch`, or `None` when this repository has no such branch.
    pub async fn branch_tip(&self, branch: &str) -> Result<Option<GitOid>> {
        self.resolve_commit(&format!("refs/heads/{branch}")).await
    }

    /// Whether `commit` is reachable from `descendant` in *this* repository.
    ///
    /// This is the proof that destroying the object store `commit` lives in loses nothing:
    /// `descendant` already contains it. Either object being absent here is a conclusive negative
    /// — an object this repository has never seen is not held by anything in it.
    pub async fn commit_is_ancestor(&self, commit: &str, descendant: &str) -> Result<bool> {
        if !self.has_commit(commit).await? || !self.has_commit(descendant).await? {
            return Ok(false);
        }
        let output = self
            .run(["merge-base", "--is-ancestor", commit, descendant])
            .await?;
        match output.status.code() {
            Some(0) => Ok(true),
            // Exit 1 is git's answer, not a failure: not an ancestor.
            Some(1) => Ok(false),
            _ => Err(git_internal("compare commit ancestry", &output)),
        }
    }

    /// How many commits are reachable from `head` but not from `exclude`.
    ///
    /// `exclude` absent — no such branch, or an object this repository does not hold — counts the
    /// whole history reachable from `head`: nothing here proves any of it is held elsewhere.
    pub async fn commits_ahead(&self, exclude: Option<&str>, head: &str) -> Result<u64> {
        let range = match self.usable_exclude(exclude).await? {
            Some(exclude) => format!("{exclude}..{head}"),
            None => head.to_owned(),
        };
        self.count_range(&range).await
    }

    /// Write and verify a self-contained bundle of `head` and the live target it was measured
    /// against, returning the number of commits in that exact range.
    ///
    /// A target is included as a positive revision, never an exclusion. This deliberately makes
    /// the bundle larger than a thin `target..head` bundle: both histories travel in the artifact,
    /// so a rewritten target cannot leave recovery dependent on an orphaned clone-time snapshot.
    /// Including both endpoints also lets recovery reconstruct and audit the same `target..head`
    /// range that the removal report counted.
    ///
    /// `head` MUST be a ref spelling — `HEAD` or a branch — never a raw oid. `git bundle create`
    /// names a bundle's fetchable tip after refs in its revision arguments, so a raw oid alone
    /// produces a bundle with no advertised ref and git rejects it as empty.
    pub async fn bundle_commits(
        &self,
        destination: &Path,
        target: Option<&str>,
        head: &str,
    ) -> Result<u64> {
        let head_commit = format!("{head}^{{commit}}");
        let expected_head = self
            .read_one(["rev-parse", head_commit.as_str()], "read bundle tip")
            .await?;
        let commit_count = match target {
            Some(target) => self.commits_ahead_of(target, head).await?,
            None => self.commits_ahead(None, head).await?,
        };
        let mut args = vec![
            OsString::from("bundle"),
            OsString::from("create"),
            destination.as_os_str().to_owned(),
            OsString::from(head),
        ];
        if let Some(target) = target {
            args.push(OsString::from(target));
        }
        let output = self.run(args).await?;
        ensure_git_success("write commit bundle", output)?;
        self.verify_bundle(destination, &expected_head, target, commit_count)
            .await?;
        Ok(commit_count)
    }

    /// Prove the artifact works in the environment its promise names: a repository with no
    /// prerequisite objects. Verification alone is insufficient when run in the source repository,
    /// because that repository can satisfy a thin bundle's prerequisites immediately before it is
    /// destroyed. Fetching into a fresh bare repository proves both self-containment and that the
    /// advertised tip resolves to the fenced commit.
    async fn verify_bundle(
        &self,
        bundle: &Path,
        expected_head: &str,
        target: Option<&str>,
        expected_count: u64,
    ) -> Result<()> {
        let parent = bundle.parent().ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "abandonment bundle has no parent directory: {}",
                    bundle.display()
                ),
                "repair the cowshed store and retry removal",
            )
        })?;
        let scratch = parent.join(format!(".bundle-verify-{}", uuid::Uuid::new_v4().simple()));
        tokio::fs::create_dir(&scratch).await.map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot create abandonment bundle verification repository {}: {error}",
                    scratch.display()
                ),
                "repair the cowshed store and retry removal",
            )
        })?;

        let verification = async {
            let output = run_git_at(&scratch, ["init", "--bare", "."]).await?;
            if !output.status.success() {
                return Err(bundle_artifact_error(
                    "initialize abandonment bundle verification repository",
                    &output,
                ));
            }
            let recovery = Self::from_root(&scratch);
            let output = recovery
                .run([
                    OsStr::new("bundle"),
                    OsStr::new("verify"),
                    bundle.as_os_str(),
                ])
                .await?;
            if !output.status.success() {
                return Err(bundle_artifact_error(
                    "verify abandonment bundle",
                    &output,
                ));
            }
            let output = recovery
                .run([
                    OsStr::new("fetch"),
                    OsStr::new("--quiet"),
                    OsStr::new("--no-tags"),
                    bundle.as_os_str(),
                    OsStr::new("HEAD"),
                ])
                .await?;
            if !output.status.success() {
                return Err(bundle_artifact_error(
                    "fetch abandonment bundle into an empty repository",
                    &output,
                ));
            }
            let recovered_head = recovery
                .read_one(["rev-parse", "FETCH_HEAD^{commit}"], "read recovered bundle tip")
                .await
                .map_err(|error| {
                    CowshedError::integrity(
                        format!("abandonment bundle tip is not retrievable: {}", error.message),
                        "inspect the bundle and cowshed store, then retry removal",
                    )
                })?;
            if recovered_head != expected_head {
                return Err(CowshedError::integrity(
                    format!(
                        "abandonment bundle recovered tip {recovered_head}, expected {expected_head}"
                    ),
                    "inspect the bundle and cowshed store, then retry removal",
                ));
            }
            let recovered_count = match target {
                Some(target) => recovery.commits_ahead_of(target, "FETCH_HEAD").await,
                None => recovery.commits_ahead(None, "FETCH_HEAD").await,
            }
            .map_err(|error| {
                CowshedError::integrity(
                    format!(
                        "abandonment bundle cannot reconstruct its reported commit range: {}",
                        error.message
                    ),
                    "inspect the bundle and cowshed store, then retry removal",
                )
            })?;
            if recovered_count != expected_count {
                return Err(CowshedError::integrity(
                    format!(
                        "abandonment bundle recovered {recovered_count} commits, expected \
                         {expected_count}"
                    ),
                    "inspect the bundle and cowshed store, then retry removal",
                ));
            }
            Ok(())
        }
        .await;

        let cleanup = tokio::fs::remove_dir_all(&scratch).await.map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot remove abandonment bundle verification repository {}: {error}",
                    scratch.display()
                ),
                "repair the cowshed store and retry removal",
            )
        });
        match (verification, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(primary), Err(cleanup)) => Err(CowshedError::integrity(
                format!("{primary}; verification cleanup also failed: {cleanup}"),
                "inspect the bundle and cowshed store, then retry removal",
            )),
        }
    }

    /// The `exclude` endpoint of a range, reduced to an oid this repository holds.
    async fn usable_exclude(&self, exclude: Option<&str>) -> Result<Option<GitOid>> {
        match exclude {
            Some(exclude) => self.resolve_commit(exclude).await,
            None => Ok(None),
        }
    }

    /// How many paths the working tree reports as added, changed, deleted, or untracked.
    ///
    /// Counted by record rather than by NUL, because a rename or copy record carries two paths in
    /// two NUL-separated fields and would otherwise be reported as two changes.
    pub async fn dirty_file_count(&self) -> Result<u64> {
        let output = self.porcelain_status("count working tree changes").await?;
        let mut fields = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty());
        let mut count = 0_u64;
        while let Some(record) = fields.next() {
            count = count.checked_add(1).ok_or_else(|| {
                CowshedError::integrity(
                    "working tree change count overflow",
                    "repair the git repository",
                )
            })?;
            // `XY` status pair, then the path; a rename or copy adds the original path as its own
            // field, which belongs to the record already counted.
            if record
                .get(..2)
                .is_some_and(|status| status.iter().any(|code| *code == b'R' || *code == b'C'))
            {
                fields.next();
            }
        }
        Ok(count)
    }

    /// Untracked junk the working tree holds: every untracked hidden path. What `is_dirty`
    /// disregards, listed so a removal can delete it rather than reason about it.
    pub async fn untracked_junk(&self) -> Result<Vec<PathBuf>> {
        let output = self.porcelain_status("list untracked junk").await?;
        Ok(porcelain_records(&output.stdout)
            .filter(|(status, path)| is_untracked_junk(status, path))
            .map(|(_, path)| self.root.join(std::ffi::OsStr::from_bytes(path)))
            .collect())
    }

    async fn porcelain_status(&self, operation: &str) -> Result<Output> {
        let output = self
            .run(["status", "--porcelain=v1", "-z", "--untracked-files=normal"])
            .await?;
        if !output.status.success() {
            return Err(git_internal(operation, &output));
        }
        Ok(output)
    }

    /// The absolute path of this repository's own object store.
    ///
    /// `--path-format=absolute` is load-bearing: the bare form answers relative to the repository
    /// root, and this path is handed to a *different* repository as an alternate object store,
    /// where a relative path resolves against the wrong directory.
    pub async fn object_directory(&self) -> Result<PathBuf> {
        let output = self
            .run([
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "objects",
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal("locate the git object store", &output));
        }
        parse_one_path(&output.stdout, "git object store")
    }

    /// How many commits `head` has that `revision` does not.
    ///
    /// Strict where [`Self::commits_ahead`] is lenient: an endpoint this repository cannot see is
    /// refused rather than dropped from the range. Dropping it silently would turn "I cannot see
    /// the target" into a plausible-looking count, which is the one failure mode a landing check
    /// must not have.
    pub async fn commits_ahead_of(&self, revision: &str, head: &str) -> Result<u64> {
        for endpoint in [revision, head] {
            if !self.has_commit(endpoint).await? {
                return Err(CowshedError::conflict(
                    format!("{endpoint} is not a commit this repository can see"),
                    "attach the object store that holds it, then retry",
                ));
            }
        }
        self.count_range(&format!("{revision}..{head}")).await
    }

    /// How many of `revision..head`'s commits contribute no content `revision` does not already
    /// hold.
    ///
    /// `git cherry` semantics for ordinary commits, and the semantics matter: equivalence is decided
    /// by patch-id, so a commit that reached `revision` by squash-merge, cherry-pick, or a history
    /// rewrite counts as held even though it is not an ancestor. A rebase that resolved a conflict
    /// produces a different patch and correctly does not count.
    ///
    /// Callers derive `unlanded` by subtracting this from the total ahead count, so anything this
    /// does not count blocks a no-flag removal. Three kinds need care, because git treats them
    /// differently and only one of them is safe to wave through:
    ///
    /// * **Merges with a non-empty combined diff** — *evil merges*, whose conflict resolution exists
    ///   in no parent. `git cherry` omits every merge from its output, so a check built on cherry
    ///   alone cannot see them at all: measured on one real workspace, three of its four merges
    ///   carried 6, 5 and 2 files that neither parent had. That is unlanded work a naive count
    ///   destroys, so it is never counted here.
    /// * **Merges with an empty combined diff** are counted, because they author nothing. They are
    ///   topology, and retiring the workspace loses no content. Refusing on them would mean a branch
    ///   of already-landed commits joined by clean merges could never be reported landed — exactly
    ///   the false positive the landed filter exists to remove.
    /// * **Ordinary commits with an empty diff** are *not* counted, and this asymmetry is
    ///   deliberate. Git does not omit them from `cherry`, and their patch-id is the identity of
    ///   nothing, which matches the identity of any *other* empty commit — so git reports one
    ///   workspace's marker commit as equivalent to an unrelated upstream one. A clean merge's
    ///   emptiness is structural and means the same thing every time; an empty commit is a
    ///   deliberate marker whose message is its whole content, and a false equivalence between two
    ///   unrelated markers is not a proof of anything.
    pub async fn commits_already_held(&self, revision: &str, head: &str) -> Result<u64> {
        let output = self.run(["cherry", revision, head]).await?;
        if !output.status.success() {
            return Err(git_internal("compare commit patch identities", &output));
        }
        let mut equivalent = Vec::new();
        for line in parse_lines(&output.stdout, "patch identity comparison")? {
            let (marker, oid) = line.split_at_checked(2).ok_or_else(|| {
                CowshedError::integrity(
                    format!("git reported an unreadable patch identity line: {line}"),
                    "repair the git installation",
                )
            })?;
            match marker {
                "- " => equivalent.push(oid.to_owned()),
                "+ " => {}
                // Anything else means git changed the format this proof is read out of, and a
                // miscount here authorizes a deletion. Refuse instead of guessing.
                _ => {
                    return Err(CowshedError::integrity(
                        format!("git reported an unreadable patch identity line: {line}"),
                        "repair the git installation",
                    ));
                }
            }
        }
        let mut held = self.content_free_merge_count(revision, head).await?;
        if !equivalent.is_empty() {
            let substantive = self.commits_changing_something(revision, head).await?;
            for oid in &equivalent {
                if substantive.contains(oid.as_str()) {
                    held = held.checked_add(1).ok_or_else(|| {
                        CowshedError::integrity(
                            "held commit count overflow",
                            "repair the git repository",
                        )
                    })?;
                }
            }
        }
        Ok(held)
    }

    /// Merges in `revision..head` that authored nothing of their own.
    ///
    /// The combined diff (`--diff-merges=combined`) shows only content present in *no* parent, which
    /// is exactly what a merge commit contributes over its parents. Empty means the merge is pure
    /// topology; non-empty means it carries a conflict resolution that exists nowhere else.
    ///
    /// The record separator is a NUL, because the alternative is ambiguous: a header line has to be
    /// distinguishable from a pathname, and a pathname can contain anything except NUL.
    async fn content_free_merge_count(&self, revision: &str, head: &str) -> Result<u64> {
        let output = self
            .run([
                "log",
                "--merges",
                "--diff-merges=combined",
                "--name-only",
                "--pretty=format:%x00%H",
                &format!("{revision}..{head}"),
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal("inspect merge combined diffs", &output));
        }
        let mut count = 0_u64;
        for record in output.stdout.split(|byte| *byte == 0) {
            // The first field is whatever preceded the first header, which is nothing.
            let mut lines = record
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty());
            if lines.next().is_none() {
                continue;
            }
            if lines.next().is_none() {
                count = count.checked_add(1).ok_or_else(|| {
                    CowshedError::integrity(
                        "content-free merge count overflow",
                        "repair the git repository",
                    )
                })?;
            }
        }
        Ok(count)
    }

    /// The commits of `revision..head` whose diff is not empty.
    ///
    /// A pathspec of `.` makes git drop every commit that changes nothing, which is exactly the
    /// "no patch to identify" set. `--no-merges` keeps the answer to commits patch-id is defined
    /// for, and `--full-history` suppresses the parent rewriting that pathspec filtering would
    /// otherwise apply, so the surviving oids are the real commits and not simplified stand-ins.
    async fn commits_changing_something(
        &self,
        revision: &str,
        head: &str,
    ) -> Result<std::collections::BTreeSet<String>> {
        let output = self
            .run([
                "rev-list",
                "--no-merges",
                "--full-history",
                &format!("{revision}..{head}"),
                "--",
                ".",
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal("list commits that change something", &output));
        }
        Ok(parse_lines(&output.stdout, "commit identity")?
            .into_iter()
            .collect())
    }

    async fn count_range(&self, range: &str) -> Result<u64> {
        let count = self
            .read_one(["rev-list", "--count", range], "count commits")
            .await?;
        count.parse().map_err(|_| {
            CowshedError::integrity(
                format!("git reported an unparseable commit count: {count}"),
                "repair the git repository",
            )
        })
    }

    async fn ensure_workspace_branch_absent(
        &self,
        name: &str,
        holder: &str,
        retry: String,
    ) -> Result<String> {
        let (branch, branch_ref) = workspace_branch(name);
        let exists = self
            .run(["show-ref", "--verify", "--quiet", branch_ref.as_str()])
            .await?;
        if exists.status.success() {
            return Err(CowshedError::conflict(
                format!("branch {branch} already exists in {holder}"),
                retry,
            ));
        }
        if exists.status.code() != Some(1) {
            return Err(git_internal("check workspace branch", &exists));
        }
        Ok(branch)
    }

    async fn switch_to_workspace_branch(&self, branch: &str, start: Option<&str>) -> Result<()> {
        let mut args = vec![
            OsString::from("switch"),
            OsString::from("-c"),
            OsString::from(branch),
        ];
        if let Some(start) = start {
            args.push(OsString::from("--"));
            args.push(OsString::from(start));
        }
        let output = self.run(args).await?;
        ensure_git_success("create workspace branch", output)
    }

    /// Configure local-only workspace Git and create its session branch.
    ///
    /// `main_mount` is main's *canonical mount*, never the recorded checkout path. Under the
    /// symlink layout those differ, and the checkout path is a symlink outside the workspace's read
    /// grants that dangles the moment the checkout moves; the canonical mount is the path the
    /// substrate owns, the grants cover, and `cowshed mv` maintains.
    pub async fn prepare_workspace(
        &self,
        name: &str,
        main_mount: &Path,
        start: Option<&str>,
    ) -> Result<MainRemote> {
        if !main_mount.is_absolute() {
            return Err(CowshedError::usage(
                "workspace main remote must be an absolute local path",
                "retry from a resolved repository root",
            ));
        }
        let branch = self
            .ensure_workspace_branch_absent(
                name,
                "the cloned workspace",
                format!("remove or rename cowshed/{name}, then retry: cowshed new {name}"),
            )
            .await?;

        // The `.git` directory arrived by CoW carrying every remote main had, including network
        // URLs. Sandboxed git speaks only local paths, so a fresh mint drops the lot before
        // configuring its own upstream — this is the "no remote URL ever exists inside a sandbox"
        // invariant, not a clobber of user intent: nothing in this repository is the user's yet.
        for remote in self.remote_names().await? {
            let output = self.run(["remote", "remove", remote.as_str()]).await?;
            ensure_git_success("remove inherited remote", output)?;
        }

        // The working tree arrived by CoW for the same reason, and carries the same hazard one
        // layer out: a symlink whose target climbs above the tree root was computed against
        // main's depth, so here it lands somewhere else entirely — dangling, or silently
        // resolving onto a directory main never meant. Restoring those is the same repair as
        // the remotes above, and it is confined to links that escape: an in-tree link is
        // correct at any depth already ([`crate::inherited_links`]).
        self.restore_inherited_links(main_mount).await?;
        let main_remote = self.configure_main_remote(main_mount).await?;

        self.switch_to_workspace_branch(&branch, start).await?;
        Ok(main_remote)
    }

    /// Re-resolve this tree's symlinks that point outside it, against the tree they came from.
    ///
    /// Runs at mint, where the workspace is still cowshed's: a link that escapes the root is
    /// generated state carrying main's depth, in the same way an inherited remote carries
    /// main's URLs, and neither is the user's yet.
    ///
    /// Only escaping links are rewritten. That predicate is what keeps the step affordable on
    /// a tree whose whole value is being ready in seconds — the walk reads directory entries
    /// and rewrites the handful of links that need it, rather than reinstalling anything.
    ///
    /// An escaping link whose source-tree target does not exist is refused by name rather than
    /// repointed at a guess. It is broken in main too, and a workspace that silently resolves
    /// it to whatever happens to sit at that path in the new tree is the failure being fixed,
    /// not an acceptable outcome.
    pub async fn restore_inherited_links(
        &self,
        source_root: &Path,
    ) -> Result<crate::inherited_links::LinkPlan> {
        let root = self.root.clone();
        let source = source_root.to_path_buf();
        let plan =
            tokio::task::spawn_blocking(move || crate::inherited_links::restore(&root, &source))
                .await
                .map_err(|source| {
                    CowshedError::integrity(
                        format!("restoring inherited links panicked: {source}"),
                        "retry the operation and report the failure if it repeats",
                    )
                })??;
        if !plan.refusals.is_empty() {
            return Err(CowshedError::integrity(
                format!(
                    "this workspace inherited {} symlink(s) that leave the tree and cannot be re-resolved: {}",
                    plan.refusals.len(),
                    plan.refusal_report()
                ),
                "repair the link in the source checkout — for a package manager `link:` dependency, re-run its install there — then retry",
            ));
        }
        Ok(plan)
    }

    /// Point this workspace at main's canonical mount, without ever clobbering a remote cowshed
    /// did not create.
    ///
    /// Idempotent, and that is the point: it runs at mint against a repository whose remotes were
    /// just stripped, and again on any later reconciliation against one an agent has been working
    /// in — where `cowshed repo` mirrors and hand-added remotes are the user's, and a remote named
    /// `main` may well be one of them.
    ///
    /// Ownership decides, and ownership is read from [`REMOTE_OWNER_KEY`] rather than guessed from
    /// the URL. A remote cowshed owns is retargeted whatever it currently says — that is the whole
    /// repair after a checkout moves. A remote cowshed does not own is never touched, and cowshed
    /// stands beside it under [`FALLBACK_MAIN_REMOTE`].
    pub async fn configure_main_remote(&self, main_mount: &Path) -> Result<MainRemote> {
        if !main_mount.is_absolute() {
            return Err(CowshedError::usage(
                "workspace main remote must be an absolute local path",
                "retry from a resolved repository root",
            ));
        }
        // Retire the name this remote used to carry. `host` was never the user's — mint strips
        // every inherited remote and then creates exactly one — so cowshed may remove its own
        // former spelling, and must: left alone it points at the recorded checkout path, which is
        // the wrong path under the symlink layout and stale after any `cowshed mv`.
        if self.remote_url(LEGACY_MAIN_REMOTE).await?.is_some() {
            let output = self.run(["remote", "remove", LEGACY_MAIN_REMOTE]).await?;
            ensure_git_success("remove superseded host remote", output)?;
        }
        match self.remote_url(MAIN_REMOTE).await? {
            None => {
                self.set_owned_remote(MAIN_REMOTE, main_mount).await?;
                Ok(MainRemote::Canonical)
            }
            Some(url) if self.owns_remote(MAIN_REMOTE, &url, main_mount).await? => {
                self.set_owned_remote(MAIN_REMOTE, main_mount).await?;
                Ok(MainRemote::Canonical)
            }
            Some(_) => {
                self.set_owned_remote(FALLBACK_MAIN_REMOTE, main_mount)
                    .await?;
                Ok(MainRemote::Displaced)
            }
        }
    }

    /// Is the remote named `name`, currently pointing at `url`, cowshed's to retarget?
    ///
    /// The recorded fact is *the URL cowshed last wrote*, not a boolean "cowshed made this". A
    /// boolean cannot survive the one thing users actually do — `git remote set-url main <their
    /// upstream>` inside a workspace — because the flag would still say "cowshed's" over a URL the
    /// user chose, and the next reconciliation would clobber it. A recorded value answers both
    /// questions at once: equal means untouched since cowshed wrote it, so retargeting is cowshed
    /// correcting itself; different means somebody edited it, so hands off.
    ///
    /// With no record at all — every workspace minted before this was written down — two things
    /// still establish ownership without guessing:
    ///
    /// * the URL already names `main_mount`, so claiming it changes nothing and the record is
    ///   adopted on this first reconciliation;
    /// * the URL is an absolute local path holding no repository. Nothing can be fetched from a
    ///   directory with no object store, so this is not a working remote being displaced, it is a
    ///   record whose subject has gone. A *live* foreign repository is left alone, and a URL that
    ///   is not a local path is never a candidate.
    async fn owns_remote(&self, name: &str, url: &Path, main_mount: &Path) -> Result<bool> {
        if let Some(recorded) = self.recorded_remote_owner(name).await? {
            return Ok(recorded == url);
        }
        if url == main_mount {
            return Ok(true);
        }
        Ok(url.is_absolute() && !is_git_repository(url).await?)
    }

    /// The URL cowshed recorded for `name`, or `None` if cowshed never wrote one.
    async fn recorded_remote_owner(&self, name: &str) -> Result<Option<PathBuf>> {
        let output = self
            .run([
                "config",
                "--get",
                &format!("remote.{name}.{REMOTE_OWNER_KEY}"),
            ])
            .await?;
        if output.status.success() {
            return parse_one_path(&output.stdout, "recorded remote owner").map(Some);
        }
        // git-config exits 1 for an absent key; anything else is a real failure.
        if output.status.code() == Some(1) {
            return Ok(None);
        }
        Err(git_internal("read remote ownership", &output))
    }

    /// Point `name` at `url` and record the URL just written, so the next reconciliation can tell
    /// its own record from an edit somebody made after it.
    async fn set_owned_remote(&self, name: &str, url: &Path) -> Result<()> {
        self.set_remote(name, url).await?;
        let output = self
            .run([
                OsStr::new("config"),
                OsStr::new(&format!("remote.{name}.{REMOTE_OWNER_KEY}")),
                url.as_os_str(),
            ])
            .await?;
        ensure_git_success("record remote ownership", output)
    }

    /// What every `merge.*.driver` in this repository's local config looks like, without changing
    /// any of them. `doctor` reads this; it never writes.
    pub async fn inspect_merge_drivers(&self) -> Result<Vec<MergeDriver>> {
        self.classified_merge_drivers().await
    }

    /// Which remote cowshed uses as this workspace's upstream, and whether that URL is a repository.
    ///
    /// Doctor reads this; it never writes. Ownership uses the same rules as
    /// [`Self::configure_main_remote`]: a recorded `remote.<name>.cowshed` value, else a URL that
    /// already names `main_mount`, else a dead local path. A live foreign `main` is not cowshed's.
    pub async fn inspect_cowshed_upstream(&self, main_mount: &Path) -> Result<CowshedUpstream> {
        let main_url = self.remote_url(MAIN_REMOTE).await?;
        let cowshed_owns_main = match &main_url {
            Some(url) => self.owns_remote(MAIN_REMOTE, url, main_mount).await?,
            None => false,
        };
        let remote_name = if cowshed_owns_main {
            MAIN_REMOTE
        } else {
            FALLBACK_MAIN_REMOTE
        };
        let url = if cowshed_owns_main {
            main_url
        } else {
            self.remote_url(remote_name).await?
        };
        let repository = match &url {
            Some(path) if path.is_absolute() => is_git_repository(path).await?,
            _ => false,
        };
        Ok(CowshedUpstream {
            remote_name: remote_name.to_owned(),
            url,
            repository,
        })
    }

    /// Rewrite every `merge.*.driver` whose program is an absolute path into the
    /// repository-relative spelling, and report the state of all of them afterwards.
    ///
    /// Relative is not a preference here, it is the only correct form. Git runs a merge driver with
    /// its working directory at the top of the work tree (gitattributes(5), "Defining a custom merge
    /// driver"), so `scripts/merge-ledger.py %O %A %B` resolves for every checkout of the
    /// repository forever, while an absolute path buys nothing and dies the first time the checkout
    /// moves — which is how every rebase in a relocated project came to fail with
    /// `merge-ledger.py: No such file or directory`.
    ///
    /// Idempotent: a driver that is already relative is left byte-for-byte alone, and one whose
    /// program has no counterpart in this repository is reported rather than guessed at.
    pub async fn repair_merge_drivers(&self) -> Result<Vec<MergeDriver>> {
        let drivers = self.classified_merge_drivers().await?;
        for driver in &drivers {
            let MergeDriverState::Relativized { to } = &driver.state else {
                continue;
            };
            let output = self
                .run(["config", &format!("merge.{}.driver", driver.name), to])
                .await?;
            ensure_git_success("rewrite merge driver", output)?;
        }
        Ok(drivers)
    }

    async fn classified_merge_drivers(&self) -> Result<Vec<MergeDriver>> {
        // `-z` because a driver command is free-form text: records are NUL-separated and the key is
        // separated from its value by the first newline, so a value containing spaces — every real
        // driver does, it carries `%O %A %B` — survives intact.
        let output = self
            .run([
                "config",
                "--local",
                "-z",
                "--get-regexp",
                r"^merge\..*\.driver$",
            ])
            .await?;
        // Exit 1 is git's answer for "no key matched", which is most repositories.
        if output.status.code() == Some(1) {
            return Ok(Vec::new());
        }
        if !output.status.success() {
            return Err(git_internal("list merge drivers", &output));
        }
        let text = String::from_utf8(output.stdout).map_err(|error| {
            CowshedError::integrity(error.to_string(), "repair the git configuration")
        })?;
        let mut drivers = Vec::new();
        for record in text.split('\0').filter(|record| !record.is_empty()) {
            let Some((key, command)) = record.split_once('\n') else {
                return Err(CowshedError::integrity(
                    format!(
                        "git reported a merge driver with no value: {key}",
                        key = record
                    ),
                    "repair the git configuration",
                ));
            };
            let name = key
                .strip_prefix("merge.")
                .and_then(|rest| rest.strip_suffix(".driver"))
                .ok_or_else(|| {
                    CowshedError::integrity(
                        format!("git reported an unexpected merge driver key: {key}"),
                        "repair the git configuration",
                    )
                })?;
            drivers.push(MergeDriver {
                name: name.to_owned(),
                state: self.classify_driver_command(command),
            });
        }
        Ok(drivers)
    }

    fn classify_driver_command(&self, command: &str) -> MergeDriverState {
        // Only the program is a path; everything after it is git's placeholder arguments and is
        // carried through untouched.
        let (program, arguments) = match command.split_once(char::is_whitespace) {
            Some((program, arguments)) => (program, Some(arguments)),
            None => (command, None),
        };
        let program_path = Path::new(program);
        if !program_path.is_absolute() {
            return MergeDriverState::Relative;
        }
        let Some(relative) = self.repository_relative_program(program_path) else {
            return MergeDriverState::Unresolvable {
                program: program.to_owned(),
            };
        };
        let mut to = relative.to_string_lossy().into_owned();
        if let Some(arguments) = arguments {
            to.push(' ');
            to.push_str(arguments);
        }
        MergeDriverState::Relativized { to }
    }

    /// The same program, named relative to this repository's root — or `None` if it is not in it.
    ///
    /// Two rules, exact first. A program already under this root is simply stripped. Otherwise the
    /// longest tail of the absolute path that names an existing file under the root is the answer:
    /// that is what recovers a driver still spelt against a checkout path that no longer exists,
    /// where no prefix arithmetic is possible because the old prefix was never recorded anywhere.
    /// Longest rather than shortest because it agrees with more of what the operator wrote.
    fn repository_relative_program(&self, program: &Path) -> Option<PathBuf> {
        if let Ok(stripped) = program.strip_prefix(&self.root)
            && self.root.join(stripped).is_file()
        {
            return Some(stripped.to_owned());
        }
        let components: Vec<_> = program
            .components()
            .filter_map(|component| match component {
                std::path::Component::Normal(part) => Some(part),
                _ => None,
            })
            .collect();
        (0..components.len())
            .map(|skip| components[skip..].iter().collect::<PathBuf>())
            .find(|candidate| self.root.join(candidate).is_file())
    }

    /// Register `workspace`'s mount as a remote in *this* repository, which is main's.
    ///
    /// The direction is the safe one: main fetches from a workspace, and a workspace never pushes
    /// into main — the same pull-based hand-back `push`, autosave, and `land` use.
    pub async fn register_workspace_remote(&self, workspace: &str, mount: &Path) -> Result<()> {
        if !mount.is_absolute() {
            return Err(CowshedError::usage(
                "workspace remote must be an absolute local path",
                "retry once the workspace is mounted",
            ));
        }
        self.set_remote(&workspace_remote_name(workspace), mount)
            .await
    }

    /// Drop `workspace`'s registration from main. Absent is success: retirement is idempotent and
    /// `gc` re-runs this from the same revalidated metadata that authorizes the rest of cleanup.
    pub async fn unregister_workspace_remote(&self, workspace: &str) -> Result<()> {
        let name = workspace_remote_name(workspace);
        if self.remote_url(&name).await?.is_none() {
            return Ok(());
        }
        let output = self.run(["remote", "remove", name.as_str()]).await?;
        ensure_git_success("remove workspace remote", output)
    }

    /// Turn this freshly cloned image into a registered linked worktree of the repository at
    /// `main_mount`, then create the session branch — `cowshed new --git-worktree`.
    ///
    /// `self.root` is the workspace mount; `main_mount` is main's *canonical mount*, never the
    /// recorded checkout path, for the same reason the `main` remote uses it: a registration
    /// recorded through the checkout symlink breaks the moment the checkout moves, and
    /// `git worktree repair` would then have two plausible paths and no way to choose.
    ///
    /// The cloned `.git` **directory** is discarded first. It is a complete copy of main's
    /// repository, and a linked worktree carrying one would mean two registrations claiming a
    /// single worktree id.
    ///
    /// `git worktree add` insists on creating the path it registers, and the tree is already here
    /// from the clone, so the registration is taken out through a staging directory inside the
    /// mount and the pointer file is then relocated onto the mount root. The staging directory's
    /// name is the worktree id git derives, which is why it is the workspace name rather than
    /// anything mount-derived: `--slot` mounts do not carry the name, and retirement has to find
    /// the registration from the name alone.
    pub async fn adopt_as_linked_worktree(
        &self,
        name: &str,
        main_mount: &Path,
        start: Option<&str>,
    ) -> Result<()> {
        if !main_mount.is_absolute() {
            return Err(CowshedError::usage(
                "linked worktree repository must be an absolute local path",
                "retry from a resolved repository root",
            ));
        }
        let main = Self::from_root(main_mount);
        // The branch is created in main's ref namespace, so the collision to check is main's, not
        // this image's — the image is about to stop having a ref namespace of its own.
        let branch = main
            .ensure_workspace_branch_absent(
                name,
                "main's repository",
                format!(
                    "remove or rename cowshed/{name}, then retry: cowshed new {name} --git-worktree"
                ),
            )
            .await?;
        let admin = main.worktree_admin_dir(name).await?;
        if admin.exists() {
            return Err(CowshedError::conflict(
                format!("main already registers a linked worktree named {name}"),
                format!("cowshed rm {name}"),
            ));
        }

        let head = main.head_oid().await?;
        let mount = self.root.clone();
        let staging = mount.join(WORKTREE_STAGING);
        let staged = staging.join(name);
        let dot_git = mount.join(".git");
        tokio::task::spawn_blocking({
            let dot_git = dot_git.clone();
            let staging = staging.clone();
            move || -> std::io::Result<()> {
                if dot_git.is_dir() {
                    fs::remove_dir_all(&dot_git)?;
                } else if dot_git.exists() {
                    fs::remove_file(&dot_git)?;
                }
                if staging.exists() {
                    fs::remove_dir_all(&staging)?;
                }
                fs::create_dir_all(&staging)
            }
        })
        .await
        .map_err(|error| CowshedError::internal(format!("prepare worktree staging: {error}")))?
        .map_err(|error| {
            CowshedError::internal(format!(
                "prepare worktree staging at {}: {error}",
                staging.display()
            ))
        })?;

        let output = main
            .run([
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--no-checkout"),
                OsStr::new("--detach"),
                staged.as_os_str(),
                OsStr::new(head.as_str()),
            ])
            .await?;
        ensure_git_success("register linked worktree", output)?;
        if !admin.is_dir() {
            return Err(CowshedError::integrity(
                format!("git registered the linked worktree for {name} under another id"),
                "cowshed doctor --json",
            ));
        }

        tokio::task::spawn_blocking({
            let staged = staged.clone();
            let staging = staging.clone();
            move || -> std::io::Result<()> {
                fs::rename(staged.join(".git"), &dot_git)?;
                fs::remove_dir_all(&staging)
            }
        })
        .await
        .map_err(|error| CowshedError::internal(format!("relocate worktree pointer: {error}")))?
        .map_err(|error| {
            CowshedError::internal(format!(
                "relocate worktree pointer onto {}: {error}",
                mount.display()
            ))
        })?;

        // Reconcile the other direction: the admin directory still records the staging path.
        main.repair_linked_worktree(&mount).await?;

        // `--no-checkout` left the index empty, so every file the clone brought would read as
        // deleted. A mixed reset refills the index from HEAD without touching the tree, which
        // leaves main's uncommitted edits showing as modified — exactly what a standalone clone of
        // the same image shows.
        let reset = self.run(["reset", "-q"]).await?;
        ensure_git_success("populate linked worktree index", reset)?;

        self.switch_to_workspace_branch(&branch, start).await?;
        Ok(())
    }

    /// Point this repository's registration for `mount` back at the mount, both directions.
    ///
    /// The primitive git provides for exactly this two-way pointer fixup, and the one `cowshed mv`
    /// runs after moving either end.
    pub async fn repair_linked_worktree(&self, mount: &Path) -> Result<()> {
        let output = self
            .run([
                OsStr::new("worktree"),
                OsStr::new("repair"),
                mount.as_os_str(),
            ])
            .await?;
        ensure_git_success("repair linked worktree registration", output)
    }

    /// Drop `workspace`'s linked-worktree registration from this repository, which is main's.
    /// Absent is success: retirement is idempotent and `gc` re-runs it from the same revalidated
    /// metadata that authorizes the rest of cleanup.
    ///
    /// Removing the admin directory *is* the unregistration, and the two commands that look more
    /// obvious are both wrong here. `git worktree remove` deletes the working tree at the
    /// registered path — the workspace's own files, which retirement trashes as an image and `gc`
    /// may find already gone — and refuses outright on dirty state that `--force` retirement has
    /// already accepted. `git worktree prune` unregisters *every* worktree whose path is missing,
    /// and a merely detached cowshed workspace looks exactly like that; the registration to drop
    /// is the one named by revalidated metadata, never the one that happens to look absent.
    pub async fn unregister_linked_worktree(&self, workspace: &str) -> Result<()> {
        let admin = self.worktree_admin_dir(workspace).await?;
        if !admin.exists() {
            return Ok(());
        }
        tokio::task::spawn_blocking(move || fs::remove_dir_all(admin))
            .await
            .map_err(|error| {
                CowshedError::internal(format!("unregister linked worktree: {error}"))
            })?
            .map_err(|error| CowshedError::internal(format!("unregister linked worktree: {error}")))
    }

    /// Where this repository keeps the administrative state for the linked worktree `id`.
    ///
    /// Resolved through git rather than assembled as `.git/worktrees/<id>`, so it stays correct
    /// for a repository whose git directory is not where the naive spelling would put it.
    async fn worktree_admin_dir(&self, id: &str) -> Result<PathBuf> {
        let output = self
            .run([
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                &format!("worktrees/{id}"),
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal("resolve linked worktree state", &output));
        }
        parse_one_path(&output.stdout, "linked worktree state")
    }

    async fn remote_names(&self) -> Result<Vec<String>> {
        let output = self.run(["remote"]).await?;
        if !output.status.success() {
            return Err(git_internal("list workspace remotes", &output));
        }
        parse_lines(&output.stdout, "remote name")
    }

    async fn remote_url(&self, name: &str) -> Result<Option<PathBuf>> {
        let output = self
            .run(["config", "--get", &format!("remote.{name}.url")])
            .await?;
        if output.status.success() {
            return Ok(Some(parse_one_path(&output.stdout, "remote url")?));
        }
        // git-config exits 1 for an absent key; anything else is a real failure.
        if output.status.code() == Some(1) {
            return Ok(None);
        }
        Err(git_internal("read remote url", &output))
    }

    /// Create the remote, or retarget one cowshed already owns. Callers decide ownership first.
    async fn set_remote(&self, name: &str, url: &Path) -> Result<()> {
        let existing = self.remote_url(name).await?;
        let verb = if existing.is_some() { "set-url" } else { "add" };
        let output = self
            .run([
                OsStr::new("remote"),
                OsStr::new(verb),
                OsStr::new(name),
                url.as_os_str(),
            ])
            .await?;
        ensure_git_success("configure remote", output)
    }

    async fn read_one<I, S>(&self, args: I, operation: &str) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.run(args).await?;
        if !output.status.success() {
            return Err(git_internal(operation, &output));
        }
        parse_one_string(&output.stdout, operation)
    }

    async fn run<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        run_git_at_with_objects(&self.root, self.alternate_objects.as_deref(), args).await
    }
}

async fn run_git_at<I, S>(root: &Path, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_git_at_with_objects(root, None, args).await
}

/// Does `path` currently hold a git repository?
///
/// Asked of a remote URL, this separates "somebody else's working remote" from "a record whose
/// subject has gone". `rev-parse --git-dir` is git's own answer and covers every shape — worktree,
/// bare, submodule pointer file — where a `.git` existence check covers one. A path that is not a
/// directory at all fails the same way, which is the answer we want.
async fn is_git_repository(path: &Path) -> Result<bool> {
    match tokio::fs::try_exists(path).await {
        Ok(false) => return Ok(false),
        Ok(true) => {}
        Err(error) => {
            return Err(CowshedError::integrity(
                format!(
                    "could not inspect remote repository path {}: {error}",
                    path.display()
                ),
                "repair the remote path or its parent permissions, then retry",
            ));
        }
    }
    Ok(run_git_at(path, ["rev-parse", "--git-dir"])
        .await?
        .status
        .success())
}

/// The one way to point a `git` invocation at a checkout: `-C <root>` plus a disabled terminal
/// prompt, as a `std::process::Command` so both the async runners here and the CLI's blocking
/// probe build on the same argv and environment.
pub fn git_command_at(root: &Path) -> std::process::Command {
    let mut command = std::process::Command::new("git");
    command.arg("-C").arg(root).env("GIT_TERMINAL_PROMPT", "0");
    command
}

/// The one spelling of "git itself could not run", so the same failure never carries two
/// different instructions depending on which runner hit it.
pub fn git_spawn_error(error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot execute git: {error}"),
        "install the macOS command line developer tools, then retry",
    )
}

async fn run_git_at_with_objects<I, S>(
    root: &Path,
    alternate_objects: Option<&Path>,
    args: I,
) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::from(git_command_at(root));
    command.args(args);
    if let Some(objects) = alternate_objects {
        command.env("GIT_ALTERNATE_OBJECT_DIRECTORIES", objects);
    }
    command
        .output()
        .await
        .map_err(|error| git_spawn_error(&error))
}

fn ensure_git_success(operation: &str, output: Output) -> Result<()> {
    if output.status.success() {
        Ok(())
    } else {
        Err(CowshedError::conflict(
            git_message(operation, &output),
            "resolve the git conflict and retry the cowshed command",
        ))
    }
}

/// `(XY, path)` per `--porcelain=v1 -z` record; a rename's second path field is consumed
/// with its record.
fn porcelain_records(stdout: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    let mut fields = stdout
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    std::iter::from_fn(move || {
        let record = fields.next()?;
        let status = record.get(..2).unwrap_or(record);
        if status.iter().any(|code| *code == b'R' || *code == b'C') {
            fields.next();
        }
        Some((status, record.get(3..).unwrap_or(&[])))
    })
}

/// An untracked path with a hidden component: junk, never work.
fn is_untracked_junk(status: &[u8], path: &[u8]) -> bool {
    status == b"??" && is_hidden_path(path)
}

/// True when any component of a relative path starts with `.`.
pub fn is_hidden_path(path: &[u8]) -> bool {
    path.split(|byte| *byte == b'/')
        .any(|component| component.first() == Some(&b'.'))
}

fn git_internal(operation: &str, output: &Output) -> CowshedError {
    CowshedError::internal(git_message(operation, output))
}

fn bundle_artifact_error(operation: &str, output: &Output) -> CowshedError {
    CowshedError::integrity(
        git_message(operation, output),
        "inspect the bundle and cowshed store, then retry removal",
    )
}

fn git_message(operation: &str, output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    if detail.is_empty() {
        format!("failed to {operation} (git status {})", output.status)
    } else {
        format!("failed to {operation}: {detail}")
    }
}

fn parse_lines(bytes: &[u8], description: &str) -> Result<Vec<String>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| CowshedError::internal(format!("{description} is not valid UTF-8")))?;
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn parse_one_string(bytes: &[u8], description: &str) -> Result<String> {
    let value = parse_one_line(bytes, description)?;
    String::from_utf8(value.to_vec())
        .map_err(|_| CowshedError::internal(format!("{description} is not valid UTF-8")))
}

fn parse_oid(value: String, description: &str) -> Result<GitOid> {
    GitOid::new(value).map_err(|error| {
        CowshedError::integrity(
            format!("{description} is not a git object id: {error}"),
            "repair the git repository",
        )
    })
}

fn parse_one_path(bytes: &[u8], description: &str) -> Result<PathBuf> {
    let value = parse_one_line(bytes, description)?;
    Ok(PathBuf::from(OsString::from_vec(value.to_vec())))
}

fn parse_one_line<'a>(bytes: &'a [u8], description: &str) -> Result<&'a [u8]> {
    let mut lines = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty());
    let Some(value) = lines.next() else {
        return Err(CowshedError::internal(format!(
            "expected exactly one {description}, received 0"
        )));
    };
    if lines.next().is_some() {
        return Err(CowshedError::internal(format!(
            "expected exactly one {description}, received multiple"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::symlink;
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus, Output};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        CowshedUpstream, FALLBACK_MAIN_REMOTE, GitRepository, MAIN_REMOTE, MainRemote, RemoteUrl,
        ensure_git_success, git_message, is_git_repository, parse_lines, workspace_remote_name,
    };
    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    fn repository() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "cowshed-git-test-{}-{suffix}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create test repository");
        let status = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(&root)
            .status()
            .expect("run git init");
        assert!(status.success());
        fs::write(root.join("README"), "test\n").expect("write fixture");
        let status = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "add",
                ".",
            ])
            .status()
            .expect("run git add");
        assert!(status.success());
        let status = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "initial",
            ])
            .status()
            .expect("run git commit");
        assert!(status.success());
        root
    }

    fn command_output(exit_code: i32, stderr: &[u8]) -> Output {
        Output {
            status: ExitStatus::from_raw(exit_code << 8),
            stdout: Vec::new(),
            stderr: stderr.to_vec(),
        }
    }

    #[tokio::test]
    async fn untracked_hidden_paths_are_junk_and_do_not_make_a_tree_dirty() {
        let root = repository();
        let git = GitRepository::from_root(&root);
        assert!(!git.is_dirty().await.expect("clean fixture"));

        // An nx daemon log and a tool cache: hidden, untracked, junk.
        fs::create_dir_all(root.join(".nx/workspace-data/d")).expect("nx dir");
        fs::write(root.join(".nx/workspace-data/d/daemon.log"), "log\n").expect("log");
        fs::write(root.join(".cache-marker"), "x\n").expect("marker");
        assert!(!git.is_dirty().await.expect("status"));
        let junk = git.untracked_junk().await.expect("junk");
        assert!(junk.contains(&root.join(".cache-marker")));
        assert!(junk.iter().any(|path| path.starts_with(root.join(".nx"))));

        // A visible untracked file is work.
        fs::write(root.join("notes.rs"), "fn main() {}\n").expect("source");
        assert!(git.is_dirty().await.expect("status"));
        fs::remove_file(root.join("notes.rs")).expect("remove source");

        // A tracked modification is work, hidden or not.
        fs::write(root.join("README"), "changed\n").expect("modify tracked");
        assert!(git.is_dirty().await.expect("status"));
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn workspace_environment_wiring_creates_an_absent_envrc() {
        let root = repository();
        let envrc = root.join(".envrc");
        fs::write(root.join(".git/info/exclude"), ".envrc\n").expect("ignore owned envrc");

        GitRepository::from_root(&root)
            .ensure_workspace_environment_wiring()
            .await
            .expect("wire environment");

        assert_eq!(
            fs::read(&envrc).expect("read envrc"),
            b"# cowshed: workspace environment\nsource_env_if_present .cowshed/env\n"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn workspace_environment_wiring_appends_once_without_modifying_existing_content() {
        let root = repository();
        let envrc = root.join(".envrc");
        fs::write(&envrc, b"use flake\n# project-owned tail").expect("seed envrc");
        let repository = GitRepository::from_root(&root);

        repository
            .ensure_workspace_environment_wiring()
            .await
            .expect("first wiring");
        repository
            .ensure_workspace_environment_wiring()
            .await
            .expect("idempotent wiring");

        assert_eq!(
            fs::read(&envrc).expect("read envrc"),
            b"use flake\n# project-owned tail\n# cowshed: workspace environment\nsource_env_if_present .cowshed/env\n"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn workspace_environment_wiring_preserves_an_in_tree_relative_symlink() {
        let root = repository();
        fs::write(root.join(".gitignore"), ".envrc-local\n").expect("ignore local envrc");

        let managed = root.join("managed");
        fs::create_dir(&managed).expect("create managed directory");
        let target = managed.join("envrc");
        let managed_contents = concat!(
            "local_override=\"$PWD/.envrc-local\"\n",
            "source_env_if_exists \"$local_override\"\n",
        );
        fs::write(&target, managed_contents).expect("seed managed envrc");
        let envrc = root.join(".envrc");
        std::os::unix::fs::symlink("managed/envrc", &envrc).expect("link envrc");
        git(&root, &["add", "."]);
        git(
            &root,
            &[
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "managed environment hook",
            ],
        );
        let resolved_before = fs::canonicalize(&envrc).expect("resolve envrc before wiring");
        let repository = GitRepository::from_root(&root);

        repository
            .ensure_workspace_environment_wiring()
            .await
            .expect("wire environment through relative symlink");
        repository
            .ensure_workspace_environment_wiring()
            .await
            .expect("wiring is idempotent");

        assert_eq!(
            fs::read_link(&envrc).expect("read envrc link"),
            Path::new("managed/envrc"),
            "the repository-relative link is preserved verbatim"
        );
        assert_eq!(
            fs::canonicalize(&envrc).expect("resolve envrc after wiring"),
            resolved_before,
            "the staged link still names the same in-tree file"
        );
        assert_eq!(
            fs::read(&target).expect("read managed envrc"),
            managed_contents.as_bytes(),
            "tracked hook content remains unchanged"
        );
        assert_eq!(
            fs::read(root.join(".envrc-local")).expect("read local envrc"),
            b"# cowshed: workspace environment\nsource_env_if_exists \"${local_override%/*}/.cowshed/env\"\n"
        );
        assert_eq!(
            git_stdout(&root, &["status", "--porcelain"]),
            "",
            "wiring leaves the new workspace clean"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn workspace_environment_wiring_rejects_a_symlink_outside_the_tree() {
        let root = repository();
        let outside = root.with_extension("outside-envrc");
        fs::write(&outside, b"outside\n").expect("seed outside envrc");
        let target = Path::new("..").join(outside.file_name().expect("outside file name"));
        std::os::unix::fs::symlink(target, root.join(".envrc")).expect("link outside envrc");

        let error = GitRepository::from_root(&root)
            .ensure_workspace_environment_wiring()
            .await
            .expect_err("outside symlink must be rejected");

        assert!(
            error.message.contains("resolves outside workspace"),
            "{error:?}"
        );
        assert_eq!(
            fs::read(&outside).expect("read outside envrc"),
            b"outside\n",
            "the external target is never modified"
        );
        fs::remove_file(outside).expect("remove outside envrc");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn detached_head_has_no_current_branch() {
        let root = repository();
        let status = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["switch", "--detach", "--quiet", "HEAD"])
            .status()
            .expect("detach HEAD");
        assert!(status.success());

        let branch = GitRepository::from_root(&root)
            .current_branch()
            .await
            .expect("detached HEAD is not an error");
        assert_eq!(branch, None);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn current_branch_propagates_unexpected_git_failure() {
        let root = repository();
        let missing_root = root.join("missing");
        let error = GitRepository::from_root(&missing_root)
            .current_branch()
            .await
            .expect_err("invalid repository root must fail");

        assert_eq!(error.code.as_str(), "internal");
        assert!(error.message.starts_with("failed to read current branch:"));
        assert!(!error.message.ends_with(':'));
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn ensure_git_success_propagates_failure_message_and_hint() {
        ensure_git_success("update reference", command_output(0, b""))
            .expect("successful git command");

        let error = ensure_git_success("update reference", command_output(7, b"  locked ref\n"))
            .expect_err("failed git command");
        assert_eq!(error.code.as_str(), "conflict");
        assert_eq!(error.message, "failed to update reference: locked ref");
        assert_eq!(
            error.hint,
            "resolve the git conflict and retry the cowshed command"
        );
    }

    #[test]
    fn git_failure_message_uses_status_when_stderr_is_empty() {
        let output = command_output(9, b" \n\t");
        assert_eq!(
            git_message("read object", &output),
            "failed to read object (git status exit status: 9)"
        );
    }

    #[tokio::test]
    async fn discovers_repository_and_reads_head() {
        let root = repository();
        let repo = GitRepository::discover(root.join(".git"))
            .await
            .expect("discover repository");
        assert_eq!(
            repo.root(),
            root.canonicalize().expect("canonical repository root")
        );
        assert_eq!(
            repo.current_branch().await.expect("read branch").as_deref(),
            Some("main")
        );
        assert_eq!(repo.head_oid().await.expect("read head").as_str().len(), 40);
        assert!(!repo.is_dirty().await.expect("read clean status"));
        fs::write(root.join("untracked"), b"dirty\n").expect("write untracked file");
        assert!(repo.is_dirty().await.expect("read dirty status"));
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn cowshed_excludes_are_idempotent_preserve_user_patterns_and_hide_runtime_state() {
        let root = repository();
        let exclude = root.join(".git/info/exclude");
        fs::write(&exclude, b"user-pattern").expect("seed user exclude");
        let repo = GitRepository::from_root(&root);
        repo.ensure_cowshed_excludes().await.expect("first wiring");
        repo.ensure_cowshed_excludes()
            .await
            .expect("idempotent wiring");
        assert_eq!(
            fs::read(&exclude).expect("read excludes"),
            b"user-pattern\n.cowshed/\n.fseventsd/\n"
        );
        fs::create_dir(root.join(".cowshed")).expect("runtime metadata");
        fs::create_dir(root.join(".fseventsd")).expect("APFS metadata");
        assert!(!repo.is_dirty().await.expect("runtime state is ignored"));
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn cowshed_exclude_wiring_rejects_a_symlink_target() {
        let root = repository();
        let exclude = root.join(".git/info/exclude");
        let unrelated = root.join("unrelated");
        fs::write(&unrelated, b"preserve\n").expect("unrelated file");
        fs::remove_file(&exclude).expect("remove real exclude");
        std::os::unix::fs::symlink(&unrelated, &exclude).expect("exclude symlink");

        let error = GitRepository::from_root(&root)
            .ensure_cowshed_excludes()
            .await
            .expect_err("symlink must fail closed");
        assert_eq!(error.code.as_str(), "integrity");
        assert_eq!(
            fs::read(&unrelated).expect("unrelated bytes"),
            b"preserve\n"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn prepares_standalone_workspace_branch_and_only_the_local_main_remote() {
        let root = repository();
        let status = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args([
                "remote",
                "add",
                "origin",
                "https://example.invalid/private.git",
            ])
            .status()
            .expect("add inherited network remote");
        assert!(status.success());

        let repo = GitRepository::from_root(&root);
        assert_eq!(
            repo.prepare_workspace("raven", &root, Some("main"))
                .await
                .expect("prepare workspace"),
            MainRemote::Canonical
        );
        assert_eq!(
            repo.current_branch().await.expect("read branch").as_deref(),
            Some("cowshed/raven")
        );
        // The inherited network remote is gone and exactly one local remote stands in its place.
        let remotes = repo.remotes().await.expect("read remotes");
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].name, MAIN_REMOTE);
        assert_eq!(Path::new(&remotes[0].url), root);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// Clone main's tree the way the substrate does — files and all, `.git` included — so the
    /// mint step under test starts from exactly what a CoW clone hands it.
    fn clone_image(main: &Path) -> PathBuf {
        let mount = main.with_extension("workspace");
        let status = Command::new("cp")
            .args(["-R".as_ref(), main.as_os_str(), mount.as_os_str()])
            .status()
            .expect("clone image");
        assert!(status.success());
        mount
    }

    fn git_stdout(root: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(output.status.success(), "{args:?}");
        String::from_utf8(output.stdout).expect("git output is utf-8")
    }

    /// The whole mint sequence, witnessed by what main and the workspace actually hold: one
    /// registration, no second repository, the branch in main's namespace, and no remote at all.
    #[tokio::test]
    async fn adopting_a_clone_as_a_linked_worktree_leaves_one_registration_and_no_remote() {
        let main = repository();
        // Uncommitted work in main is carried by the clone, and must read as modified rather than
        // as the wholesale deletion an empty index would report.
        fs::write(main.join("README"), "test\nlocal edit\n").expect("dirty main");
        let mount = clone_image(&main);

        GitRepository::from_root(&mount)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect("adopt as linked worktree");

        let workspace = GitRepository::from_root(&mount);
        assert_eq!(
            workspace
                .current_branch()
                .await
                .expect("read branch")
                .as_deref(),
            Some("cowshed/raven")
        );
        // A linked worktree carries a pointer file, never a second copy of the repository.
        assert!(mount.join(".git").is_file());
        assert!(!mount.join(".cowshed/worktree-staging").exists());
        // Nothing to fetch from when the object store is shared.
        assert!(workspace.remotes().await.expect("read remotes").is_empty());
        // The branch is in main's ref namespace immediately, with no fetch.
        assert!(
            git_stdout(&main, &["branch", "--list", "cowshed/raven"]).contains("cowshed/raven")
        );
        // Registered under the workspace name, pointing at the mount, and reconciled both ways.
        let resolved = mount.canonicalize().expect("resolve mount");
        let listed = git_stdout(&main, &["worktree", "list"]);
        assert!(
            listed.contains(resolved.to_str().expect("utf-8 mount")),
            "{listed}"
        );
        assert_eq!(
            PathBuf::from(
                fs::read_to_string(main.join(".git/worktrees/raven/gitdir"))
                    .expect("admin gitdir")
                    .trim()
            ),
            resolved.join(".git")
        );
        // The index came from HEAD, so main's uncommitted edit reads as a modification.
        assert_eq!(
            git_stdout(&mount, &["status", "--short"]).trim(),
            "M README"
        );

        GitRepository::from_root(&main)
            .unregister_linked_worktree("raven")
            .await
            .expect("unregister");
        assert!(!main.join(".git/worktrees/raven").exists());
        // Absent is success, so an interrupted retire can be finished by `gc`.
        GitRepository::from_root(&main)
            .unregister_linked_worktree("raven")
            .await
            .expect("second unregister is a no-op");

        fs::remove_dir_all(main).expect("remove fixture");
        fs::remove_dir_all(mount).expect("remove clone");
    }

    /// Unregistering one workspace must leave every other registration alone — including one whose
    /// path is missing, which is what a merely detached workspace looks like and what a bare
    /// `git worktree prune` would silently take with it.
    #[tokio::test]
    async fn unregistering_one_worktree_spares_a_detached_sibling() {
        let main = repository();
        let raven = clone_image(&main);
        let detached = main.with_extension("detached");
        fs::create_dir_all(&detached).expect("sibling mount");

        GitRepository::from_root(&raven)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect("adopt raven");
        GitRepository::from_root(&detached)
            .adopt_as_linked_worktree("heron", &main, None)
            .await
            .expect("adopt heron");
        // Detaching a workspace removes its volume from the filesystem; the registration stays.
        fs::remove_dir_all(&detached).expect("detach heron");

        GitRepository::from_root(&main)
            .unregister_linked_worktree("raven")
            .await
            .expect("unregister raven");

        assert!(!main.join(".git/worktrees/raven").exists());
        assert!(
            main.join(".git/worktrees/heron").exists(),
            "a detached workspace's registration must survive another workspace's retirement"
        );
        fs::remove_dir_all(main).expect("remove fixture");
        fs::remove_dir_all(raven).expect("remove clone");
    }

    /// `cowshed mv` moves main under direct mount, invalidating every gitdir pointer at once. The
    /// repair runs from main's new path and has to fix both directions.
    #[tokio::test]
    async fn repair_reconciles_both_pointers_after_main_moves() {
        let main = repository();
        let mount = clone_image(&main);
        GitRepository::from_root(&mount)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect("adopt as linked worktree");

        let moved = main.with_extension("moved");
        fs::rename(&main, &moved).expect("move main");
        assert!(
            GitRepository::from_root(&mount).head_oid().await.is_err(),
            "a stale pointer must fail loudly rather than resolve somewhere else"
        );

        GitRepository::from_root(&moved)
            .repair_linked_worktree(&mount)
            .await
            .expect("repair after main moved");

        assert_eq!(
            fs::read_to_string(mount.join(".git"))
                .expect("pointer file")
                .trim(),
            format!(
                "gitdir: {}",
                moved
                    .canonicalize()
                    .expect("resolve main")
                    .join(".git/worktrees/raven")
                    .display()
            )
        );
        GitRepository::from_root(&mount)
            .head_oid()
            .await
            .expect("workspace git works again");
        fs::remove_dir_all(moved).expect("remove fixture");
        fs::remove_dir_all(mount).expect("remove clone");
    }

    /// A branch main already holds is main's, and the registration id is the workspace name, so
    /// both collisions have to be refused rather than silently renamed by git.
    #[tokio::test]
    async fn adoption_refuses_a_branch_or_registration_main_already_holds() {
        let main = repository();
        let mount = clone_image(&main);
        let status = Command::new("git")
            .arg("-C")
            .arg(&main)
            .args(["branch", "cowshed/raven"])
            .status()
            .expect("create colliding branch");
        assert!(status.success());

        let error = GitRepository::from_root(&mount)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect_err("colliding branch must refuse");
        assert_eq!(error.code.as_str(), "conflict");
        // Refused before anything was discarded: the image is still a repository.
        assert!(mount.join(".git").is_dir());

        fs::remove_dir_all(main).expect("remove fixture");
        fs::remove_dir_all(mount).expect("remove clone");
    }

    /// The three cases of `configure_main_remote`, witnessed by what the config actually holds
    /// afterwards rather than by whether a command was issued.
    #[tokio::test]
    async fn main_remote_configuration_is_idempotent_and_never_clobbers_a_foreign_remote() {
        let root = repository();
        let mount = PathBuf::from("/tmp/cowshed-canonical-mount");
        let repo = GitRepository::from_root(&root);

        // Absent: created.
        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("create main remote"),
            MainRemote::Canonical
        );
        assert_eq!(
            repo.remote_url(MAIN_REMOTE).await.expect("read url"),
            Some(mount.clone())
        );

        // Already ours and already correct: re-running changes nothing and adds no fallback.
        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("idempotent re-run"),
            MainRemote::Canonical
        );
        assert_eq!(
            repo.remote_url(FALLBACK_MAIN_REMOTE)
                .await
                .expect("read fallback"),
            None
        );

        // Foreign: the user's remote keeps both its name and its URL, byte for byte, and cowshed's
        // upstream stands beside it under the fallback name. A network URL is the unambiguous case
        // — it can never be a path cowshed recorded — and it is what a fork workflow actually puts
        // there.
        let foreign = PathBuf::from("https://github.com/example/upstream.git");
        repo.set_remote(MAIN_REMOTE, &foreign)
            .await
            .expect("user retargets main");
        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("configure beside a foreign remote"),
            MainRemote::Displaced
        );
        assert_eq!(
            repo.remote_url(MAIN_REMOTE).await.expect("read url"),
            Some(foreign),
            "cowshed never retargets a remote it did not create"
        );
        assert_eq!(
            repo.remote_url(FALLBACK_MAIN_REMOTE)
                .await
                .expect("read fallback"),
            Some(mount.clone())
        );
        assert_eq!(MainRemote::Displaced.remote_name(), FALLBACK_MAIN_REMOTE);

        // Ownership is the URL cowshed wrote, so an edit invalidates it and keeps invalidating it:
        // once the user holds the name, cowshed stays beside them however that URL later changes.
        let edited_again = root.join("wherever-the-user-went-next");
        repo.set_remote(MAIN_REMOTE, &edited_again)
            .await
            .expect("user retargets main again");
        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("still displaced"),
            MainRemote::Displaced
        );
        assert_eq!(
            repo.remote_url(MAIN_REMOTE).await.expect("read url"),
            Some(edited_again),
            "a remote the user has edited is never reclaimed, dead path or not"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// The state a moved checkout leaves in every workspace minted before ownership was recorded:
    /// `main` naming a directory that has stopped being a repository. Nothing can be fetched from
    /// it, so it is cowshed's own record with its subject gone rather than a working remote being
    /// displaced — and leaving it is what made `git fetch main` fail forever across a whole project
    /// while `mv` and `attach` both reported success.
    #[tokio::test]
    async fn a_legacy_main_remote_naming_a_dead_path_is_reclaimed_rather_than_stood_beside() {
        let root = repository();
        let mount = root.join("main-mount");
        fs::create_dir_all(&mount).expect("main mount");
        let dead = root.join("checkout-that-moved-away");
        assert!(!dead.exists(), "the fixture path must genuinely be absent");
        let repo = GitRepository::from_root(&root);
        // `set_remote` and not `set_owned_remote`: this workspace predates the ownership record.
        repo.set_remote(MAIN_REMOTE, &dead)
            .await
            .expect("plant a stale record");

        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("reclaim a dead record"),
            MainRemote::Canonical
        );
        assert_eq!(
            repo.remote_url(MAIN_REMOTE).await.expect("read url"),
            Some(mount)
        );
        assert_eq!(
            repo.remote_url(FALLBACK_MAIN_REMOTE)
                .await
                .expect("read fallback"),
            None,
            "no second remote is left behind"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// A workspace minted before the rename carries `host`. Configuration retires it rather than
    /// leaving a second remote aimed at the recorded checkout — the wrong path under the symlink
    /// layout, and stale after any `cowshed mv`.
    #[tokio::test]
    async fn configuration_retires_the_superseded_host_remote() {
        let root = repository();
        let stale = PathBuf::from("/tmp/cowshed-recorded-checkout");
        let mount = PathBuf::from("/tmp/cowshed-canonical-mount");
        let repo = GitRepository::from_root(&root);
        repo.set_remote("host", &stale)
            .await
            .expect("legacy remote");

        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("configure over a legacy remote"),
            MainRemote::Canonical
        );
        let remotes = repo.remotes().await.expect("read remotes");
        assert_eq!(remotes.len(), 1, "exactly one upstream survives");
        assert_eq!(remotes[0].name, MAIN_REMOTE);
        assert_eq!(Path::new(&remotes[0].url), mount);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// Reverse registration lives in main's repository and disappears with the workspace.
    #[tokio::test]
    async fn workspace_registration_round_trips_and_absent_removal_succeeds() {
        let root = repository();
        let mount = PathBuf::from("/tmp/cowshed-raven-mount");
        let main = GitRepository::from_root(&root);

        // Removing what was never registered is success: retirement re-runs idempotently.
        main.unregister_workspace_remote("raven")
            .await
            .expect("absent registration is not an error");

        main.register_workspace_remote("raven", &mount)
            .await
            .expect("register workspace");
        assert_eq!(
            main.remote_url(&workspace_remote_name("raven"))
                .await
                .expect("read registration"),
            Some(mount)
        );

        main.unregister_workspace_remote("raven")
            .await
            .expect("drop registration");
        assert_eq!(
            main.remote_url(&workspace_remote_name("raven"))
                .await
                .expect("read registration"),
            None
        );

        main.register_workspace_remote("raven", Path::new("relative/path"))
            .await
            .expect_err("a registration must name an absolute mount");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn preserves_non_utf8_main_remote_argument() {
        let root = repository();
        let main_mount = PathBuf::from(OsString::from_vec(b"/tmp/cowshed-main-\xff".to_vec()));
        let repo = GitRepository::from_root(&root);
        repo.prepare_workspace("raven", &main_mount, None)
            .await
            .expect("prepare workspace");

        let output = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["remote", "get-url", MAIN_REMOTE])
            .output()
            .expect("read raw main remote");
        assert!(output.status.success());
        assert_eq!(
            output
                .stdout
                .strip_suffix(b"\n")
                .expect("git output newline"),
            main_mount.as_os_str().as_bytes()
        );
        assert_eq!(
            repo.remotes().await.expect("list non-UTF-8 remote"),
            vec![RemoteUrl {
                name: MAIN_REMOTE.to_owned(),
                url: main_mount.clone(),
            }]
        );
        // The idempotent path compares the stored URL against the mount, so it has to survive the
        // round trip through git config as bytes rather than as lossy UTF-8.
        assert_eq!(
            repo.configure_main_remote(&main_mount)
                .await
                .expect("idempotent re-run over a non-UTF-8 mount"),
            MainRemote::Canonical
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn repository_probe_propagates_filesystem_query_errors() {
        let root = repository();
        let loop_path = root.join("filesystem-loop");
        symlink(&loop_path, &loop_path).expect("create symlink loop");

        let error = is_git_repository(&loop_path)
            .await
            .expect_err("an undetermined path is not an absent repository");
        assert!(
            error.to_string().contains("filesystem-loop"),
            "error must identify the path whose state could not be read: {error}"
        );

        fs::remove_file(&loop_path).expect("remove symlink loop");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn preservation_requires_a_host_branch_or_cowshed_ref_containing_the_commit() {
        let host = repository();
        let host_repo = GitRepository::from_root(&host);
        let host_head = host_repo.head_oid().await.expect("read host head");
        assert!(
            host_repo
                .commit_is_preserved(host_head.as_str())
                .await
                .expect("main preserves its head")
        );
        assert!(
            !host_repo
                .commit_is_remote_preserved(host_head.as_str())
                .await
                .expect("local head is not remotely preserved")
        );
        let status = Command::new("git")
            .arg("-C")
            .arg(&host)
            .args(["update-ref", "refs/remotes/origin/main", host_head.as_str()])
            .status()
            .expect("write remote-tracking ref");
        assert!(status.success());
        assert!(
            host_repo
                .commit_is_remote_preserved(host_head.as_str())
                .await
                .expect("remote-tracking ref preserves head")
        );

        let session = host.with_extension("session");
        let status = Command::new("git")
            .args(["clone", "-q"])
            .arg(&host)
            .arg(&session)
            .status()
            .expect("clone session");
        assert!(status.success());
        fs::write(session.join("session-only"), "unpublished\n").expect("write session change");
        let status = Command::new("git")
            .arg("-C")
            .arg(&session)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "add",
                ".",
            ])
            .status()
            .expect("stage session change");
        assert!(status.success());
        let status = Command::new("git")
            .arg("-C")
            .arg(&session)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "session-only",
            ])
            .status()
            .expect("commit session change");
        assert!(status.success());
        let session_head = GitRepository::from_root(&session)
            .head_oid()
            .await
            .expect("read session head");
        assert!(
            !host_repo
                .commit_is_preserved(session_head.as_str())
                .await
                .expect("absent session object is not preserved")
        );

        let status = Command::new("git")
            .arg("-C")
            .arg(&session)
            .args(["push", "-q", "origin", "HEAD:refs/cowshed/raven/heads/main"])
            .status()
            .expect("publish preservation ref");
        assert!(status.success());
        assert!(
            host_repo
                .commit_is_preserved(session_head.as_str())
                .await
                .expect("preservation ref contains session commit")
        );

        fs::remove_dir_all(session).expect("remove session fixture");
        fs::remove_dir_all(host).expect("remove host fixture");
    }

    #[tokio::test]
    async fn rejects_in_progress_operation() {
        let root = repository();
        fs::write(root.join(".git/MERGE_HEAD"), "deadbeef\n").expect("write merge marker");
        let error = GitRepository::from_root(&root)
            .ensure_adoptable()
            .await
            .expect_err("must reject merge");
        assert_eq!(error.code.as_str(), "conflict");

        assert_eq!(
            GitRepository::from_root(&root)
                .in_progress_operation()
                .await
                .expect("read operation state")
                .as_deref(),
            Some("MERGE_HEAD")
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// Commit `label` on top of whatever is checked out in `root`, and answer the new head.
    fn commit_on(root: &Path, label: &str) -> String {
        fs::write(root.join(label), format!("{label}\n")).expect("write fixture change");
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["add", "."])
            .status()
            .expect("run git add");
        assert!(status.success());
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                label,
            ])
            .status()
            .expect("run git commit");
        assert!(status.success());
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("read head");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("utf-8 oid")
            .trim_end()
            .to_owned()
    }

    fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?}");
    }

    #[tokio::test]
    async fn ancestry_answers_landed_and_treats_an_unknown_object_as_not_held() {
        let root = repository();
        let repo = GitRepository::from_root(&root);
        let base = repo.head_oid().await.expect("read base");
        git(&root, &["switch", "-qc", "cowshed/raven"]);
        let work = commit_on(&root, "session-work");

        assert_eq!(
            repo.branch_tip("main").await.expect("resolve main tip"),
            Some(base.clone())
        );
        assert_eq!(
            repo.branch_tip("release").await.expect("absent branch"),
            None
        );
        // Not landed: main does not contain the session commit.
        assert!(
            !repo
                .commit_is_ancestor(&work, base.as_str())
                .await
                .expect("compare ancestry")
        );
        // The base is landed by definition, and a commit is its own ancestor.
        assert!(
            repo.commit_is_ancestor(base.as_str(), &work)
                .await
                .expect("compare")
        );
        assert!(
            repo.commit_is_ancestor(&work, &work)
                .await
                .expect("compare")
        );
        // An object this repository has never seen is a conclusive negative, not an error.
        assert!(
            !repo
                .commit_is_ancestor(&work, &"9".repeat(40))
                .await
                .expect("unknown descendant")
        );
        assert!(
            !repo
                .commit_is_ancestor(&"9".repeat(40), &work)
                .await
                .expect("unknown commit")
        );

        git(&root, &["switch", "-q", "main"]);
        git(&root, &["merge", "-q", "--ff-only", "cowshed/raven"]);
        let landed_tip = repo.branch_tip("main").await.expect("tip").expect("branch");
        assert_eq!(landed_tip.as_str(), work);
        assert!(
            repo.commit_is_ancestor(&work, landed_tip.as_str())
                .await
                .expect("landed work is contained by main")
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn unlanded_count_falls_back_to_the_whole_history_without_a_usable_exclude() {
        let root = repository();
        let repo = GitRepository::from_root(&root);
        let base = repo.head_oid().await.expect("read base");
        git(&root, &["switch", "-qc", "cowshed/raven"]);
        commit_on(&root, "one");
        commit_on(&root, "two");

        assert_eq!(
            repo.commits_ahead(Some(base.as_str()), "HEAD")
                .await
                .expect("count ahead of main"),
            2
        );
        // An exclude this repository does not hold cannot prove anything is held: count it all.
        assert_eq!(
            repo.commits_ahead(Some(&"9".repeat(40)), "HEAD")
                .await
                .expect("count without a usable exclude"),
            3
        );
        assert_eq!(
            repo.commits_ahead(None, "HEAD").await.expect("count all"),
            3
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn rewritten_main_bundle_matches_the_live_range_and_fetches_into_an_empty_repository() {
        let main = repository();
        let main_repo = GitRepository::from_root(&main);
        commit_on(&main, "old-one");
        let clone_time_main = commit_on(&main, "old-two");

        let workspace = std::env::temp_dir().join(format!(
            "cowshed-git-test-history-diverged-{}-{}",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let status = Command::new("git")
            .args(["clone", "-q"])
            .arg(&main)
            .arg(&workspace)
            .status()
            .expect("clone workspace");
        assert!(status.success());
        git(&workspace, &["config", "user.name", "Cowshed Test"]);
        git(
            &workspace,
            &["config", "user.email", "test@example.invalid"],
        );
        git(&workspace, &["switch", "-qc", "cowshed/history-diverged"]);
        commit_on(&workspace, "workspace-one");
        let tip = commit_on(&workspace, "workspace-two");

        // Deliberately make the clone-time `main` snapshot unreachable from every surviving ref.
        // This is the condition the old thin bundle silently depended on never happening.
        git(&main, &["switch", "-q", "--orphan", "rewritten-main"]);
        commit_on(&main, "replacement");
        git(&main, &["branch", "-M", "main"]);
        let live_main = main_repo.head_oid().await.expect("read rewritten main");
        assert!(
            !main_repo
                .commit_is_preserved(clone_time_main.as_str())
                .await
                .expect("check clone-time tip reachability"),
            "the test must orphan the clone-time main snapshot deliberately"
        );

        let main_objects = main_repo
            .object_directory()
            .await
            .expect("locate main object store");
        let workspace_repo = GitRepository::from_root(&workspace)
            .with_alternate_objects(main_objects)
            .expect("attach live main objects");
        let bundle = workspace.join("history-diverged.bundle");
        let reported_count = workspace_repo
            .bundle_commits(&bundle, Some(live_main.as_str()), "HEAD")
            .await
            .expect("write and verify self-contained abandonment bundle");
        let claimed = workspace_repo
            .run(["rev-list", "--reverse", &format!("{live_main}..HEAD")])
            .await
            .expect("list the live-target range");
        assert!(claimed.status.success());
        assert_eq!(
            parse_lines(&claimed.stdout, "claimed abandoned oid")
                .expect("parse claimed oids")
                .len() as u64,
            reported_count,
            "the reported abandoned count must come from the bundle's live-target range"
        );

        // A raw-oid tip advertises no fetchable ref, even though it names the right object.
        let refless = workspace_repo
            .bundle_commits(
                &workspace.join("refless.bundle"),
                Some(live_main.as_str()),
                &tip,
            )
            .await
            .expect_err("an oid tip produces a ref-less bundle git will not write");
        assert!(refless.message.contains("empty bundle"), "{refless:?}");

        let recovery = std::env::temp_dir().join(format!(
            "cowshed-git-test-empty-recovery-{}-{}",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&recovery).expect("create empty recovery");
        git(&recovery, &["init", "-q", "--bare", "."]);
        let verify = Command::new("git")
            .arg("-C")
            .arg(&recovery)
            .args(["bundle", "verify"])
            .arg(&bundle)
            .output()
            .expect("verify bundle in empty repository");
        assert!(
            verify.status.success(),
            "self-contained bundle must verify without the orphaned prerequisite: {}",
            String::from_utf8_lossy(&verify.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&verify.stderr).contains("requires this ref"),
            "git must report no prerequisites: {}",
            String::from_utf8_lossy(&verify.stderr)
        );
        git(
            &recovery,
            &[
                "fetch",
                "-q",
                bundle.to_str().expect("utf-8 bundle"),
                "HEAD:refs/heads/recovered",
            ],
        );
        let recovery_repo = GitRepository::from_root(&recovery);
        assert!(
            recovery_repo
                .has_commit(live_main.as_str())
                .await
                .expect("bundle carries the live target"),
            "the live target must travel in the self-contained artifact"
        );
        let restored = recovery_repo
            .run([
                "rev-list",
                "--reverse",
                &format!("{live_main}..refs/heads/recovered"),
            ])
            .await
            .expect("list restored abandoned oids");
        assert!(restored.status.success());
        assert_eq!(
            restored.stdout, claimed.stdout,
            "bundle must yield exactly the oids claimed against live main after history rewrite"
        );

        fs::remove_dir_all(recovery).expect("remove recovery fixture");
        fs::remove_dir_all(workspace).expect("remove workspace fixture");
        fs::remove_dir_all(main).expect("remove main fixture");
    }

    #[tokio::test]
    async fn failed_empty_repository_verification_leaves_the_workspace_intact() {
        let workspace = repository();
        let repo = GitRepository::from_root(&workspace);
        let base = repo.head_oid().await.expect("read base");
        git(&workspace, &["switch", "-qc", "cowshed/history-diverged"]);
        commit_on(&workspace, "one");
        let tip = commit_on(&workspace, "two");
        let thin = workspace.join("thin.bundle");
        git(
            &workspace,
            &[
                "bundle",
                "create",
                thin.to_str().expect("utf-8 bundle"),
                "main..HEAD",
            ],
        );

        let error = repo
            .verify_bundle(&thin, &tip, Some(base.as_str()), 2)
            .await
            .expect_err("a prerequisite-dependent artifact must abort abandonment");
        assert!(
            error.message.contains("verify abandonment bundle"),
            "{error:?}"
        );
        assert!(
            workspace.is_dir(),
            "verification must not destroy the workspace"
        );
        assert_eq!(
            repo.head_oid()
                .await
                .expect("workspace remains readable")
                .as_str(),
            tip,
            "verification failure must leave the workspace tip untouched"
        );
        assert_eq!(
            repo.current_branch()
                .await
                .expect("workspace branch remains"),
            Some("cowshed/history-diverged".to_owned()),
            "verification failure must leave workspace refs untouched"
        );

        fs::remove_dir_all(workspace).expect("remove fixture");
    }

    #[tokio::test]
    async fn inspect_cowshed_upstream_does_not_write_and_skips_a_foreign_main() {
        let workspace = repository();
        let main_mount = repository();
        let repo = GitRepository::from_root(&workspace);

        git(
            &workspace,
            &[
                "remote",
                "add",
                "main",
                "https://example.invalid/acme/widget.git",
            ],
        );
        let inspected = repo
            .inspect_cowshed_upstream(&main_mount)
            .await
            .expect("inspect");
        assert_eq!(
            inspected,
            CowshedUpstream {
                remote_name: FALLBACK_MAIN_REMOTE.to_owned(),
                url: None,
                repository: false,
            }
        );
        let remotes = repo.remotes().await.expect("list remotes");
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].name, "main");

        fs::remove_dir_all(workspace).expect("remove workspace");
        fs::remove_dir_all(main_mount).expect("remove main");
    }

    #[tokio::test]
    async fn inspect_cowshed_upstream_reports_a_dead_owned_remote_and_configure_clears_it() {
        let workspace = repository();
        let main_mount = repository();
        let dead = workspace.join("gone-checkout");
        let repo = GitRepository::from_root(&workspace);

        git(
            &workspace,
            &["remote", "add", "main", dead.to_str().expect("utf-8")],
        );
        git(
            &workspace,
            &[
                "config",
                "remote.main.cowshed",
                dead.to_str().expect("utf-8"),
            ],
        );

        let before = repo
            .inspect_cowshed_upstream(&main_mount)
            .await
            .expect("inspect");
        assert_eq!(before.remote_name, MAIN_REMOTE);
        assert_eq!(before.url.as_deref(), Some(dead.as_path()));
        assert!(
            !before.repository,
            "a recorded cowshed remote pointing at a missing path is not a repository"
        );

        repo.configure_main_remote(&main_mount)
            .await
            .expect("repair");
        let after = repo
            .inspect_cowshed_upstream(&main_mount)
            .await
            .expect("inspect after repair");
        assert_eq!(after.remote_name, MAIN_REMOTE);
        assert_eq!(after.url.as_deref(), Some(main_mount.as_path()));
        assert!(
            after.repository,
            "configure_main_remote must retarget cowshed's remote"
        );

        fs::remove_dir_all(workspace).expect("remove workspace");
        fs::remove_dir_all(main_mount).expect("remove main");
    }
}
