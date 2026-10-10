use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::Output;

use tokio::process::Command;

use crate::api::dto::GitOid;
#[cfg(any(target_os = "macos", test))]
use crate::api::dto::Sha256Digest;
use crate::error::{CowshedError, Result};
use cowshed_os::fork_lock::{Run as _, RunAsync as _, Spawn as _};
use cowshed_os::timing::timed_async;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteUrl {
    pub name: String,
    pub url: PathBuf,
}

/// The working-state digest a rename compares before and after handing a linked worktree across.
/// A value only: it carries no authority and grants nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(any(target_os = "macos", test))]
pub(crate) struct RenameWorkState {
    /// SHA-256 of the length-framed status, index-vs-HEAD diff, working-vs-index diff, untracked
    /// and restored-link sections; see `GitRepository::rename_work_state`.
    pub work: [u8; 32],
    /// SHA-256 of a linked worktree's index file bytes; `None` for a standalone clone.
    pub index: Option<[u8; 32]>,
}

/// The name a workspace's upstream carries: the main workspace, so `main`.
///
/// `git fetch main` then reads as what it does, and an agent that has never seen this codebase
/// guesses it on the first try.
pub const MAIN_REMOTE: &str = "main";

/// Where cowshed's upstream goes when the workspace already has a remote named `main` that is not
/// it. Cowshed adds and lets the user remove; it never retargets a remote it did not create.
pub const FALLBACK_MAIN_REMOTE: &str = "cowshed-main";

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

/// The two trees a freshly cloned workspace answers to at mint.
///
/// They are one tree only when a fork's source is main. With an explicit session source the image came from a
/// sibling mounted at another depth than main, and every byte the clone inherited was written
/// there: resolving those bytes against main instead names paths nobody wrote. Named fields
/// rather than two positional paths, because swapping them is exactly that failure.
#[derive(Clone, Copy, Debug)]
pub struct CloneOrigin<'a> {
    /// The mount whose image was cloned — main, or the named source workspace. Escaping symlinks
    /// and the Git identity are re-resolved against it.
    pub source: &'a Path,
    /// Main's canonical mount. The `main` remote and a linked-worktree registration name it
    /// whichever tree the image came from.
    pub main: &'a Path,
}

/// How a minted workspace holds its repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceRepository {
    /// The cloned `.git` directory stays, its remotes replaced by main's canonical mount.
    Standalone,
    /// The clone is re-registered as a linked worktree of main's repository.
    LinkedWorktree,
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

/// Private identity-only global Git config for one workspace, never shared .git state.
pub const WORKSPACE_GIT_IDENTITY_CONFIG_PATH: &str = ".cowshed/git-identity.inc";

/// The workspace's private identity config, when the controller has published one.
///
/// A sandboxed child loads exactly this file as `GIT_CONFIG_GLOBAL`, so the answer is what
/// decides whether `git commit` in a workspace has an author at all. A workspace minted before
/// identity capture existed simply has none and keeps the empty device.
///
/// Anything at that path which is not a regular file is refused by name instead of loaded. The
/// controller publishes one regular 0600 file there and the child's profile denies writing it,
/// so a symlink or device at that name is tampering, and pointing Git's global configuration at
/// it would be the one place the workspace boundary leaks.
pub fn workspace_git_identity_config(mount: &Path) -> Result<Option<PathBuf>> {
    let path = mount.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(Some(path)),
        Ok(_) => Err(CowshedError::integrity(
            format!(
                "workspace Git identity config is not a regular file: {}",
                path.display()
            ),
            format!("remove {WORKSPACE_GIT_IDENTITY_CONFIG_PATH} in the workspace, then reattach"),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::internal(format!(
            "inspect workspace Git identity config at {}: {error}",
            path.display()
        ))),
    }
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

/// A temporary bare repository belongs to the verification future, including when its caller
/// drops that future. Async cleanup alone cannot run after cancellation.
struct BundleVerificationScratch {
    path: PathBuf,
    removed: bool,
}

impl BundleVerificationScratch {
    fn create(parent: &Path) -> Result<Self> {
        let path = parent.join(format!(".bundle-verify-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir(&path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot create abandonment bundle verification repository {}: {error}",
                    path.display()
                ),
                "repair the cowshed store and retry removal",
            )
        })?;
        Ok(Self {
            path,
            removed: false,
        })
    }

    async fn cleanup(mut self) -> Result<()> {
        tokio::fs::remove_dir_all(&self.path)
            .await
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot remove abandonment bundle verification repository {}: {error}",
                        self.path.display()
                    ),
                    "repair the cowshed store and retry removal",
                )
            })?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for BundleVerificationScratch {
    fn drop(&mut self) {
        if !self.removed
            && let Err(error) = fs::remove_dir_all(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "cowshed: could not remove interrupted bundle verification repository {}: {error}",
                self.path.display()
            );
        }
    }
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

    /// Snapshot only effective Git identity from the source checkout into a private global config.
    ///
    /// Git resolves includes and repository/worktree overrides in the source context. A fork uses
    /// its source's inherited global identity instead of the controller's global config. The
    /// destination later loads this as GIT_CONFIG_GLOBAL, below its own repository-local config;
    /// nothing is written to either repository's .git, including a linked worktree's shared config.
    pub async fn inherit_identity_from(&self, source_root: &Path) -> Result<()> {
        let root = self.root.clone();
        let source_root = source_root.to_owned();
        tokio::task::spawn_blocking(move || {
            let io_error = |operation: &str, path: &Path, error: std::io::Error| {
                CowshedError::internal(format!("{operation} at {}: {error}", path.display()))
            };
            let source_root = fs::canonicalize(&source_root)
                .map_err(|error| io_error("resolve Git identity source", &source_root, error))?;
            let inherited = source_root.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
            let inherited = match fs::symlink_metadata(&inherited) {
                Ok(_) => Some(fs::canonicalize(&inherited).map_err(|error| {
                    io_error("resolve source Git identity config", &inherited, error)
                })?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(io_error(
                        "inspect source Git identity config",
                        &inherited,
                        error,
                    ));
                }
            };
            let mut identity = Vec::with_capacity(6);
            let caller_global = std::env::var_os("GIT_CONFIG_GLOBAL");
            for key in [
                "user.name",
                "user.email",
                "author.name",
                "author.email",
                "committer.name",
                "committer.email",
            ] {
                let mut command = git_command_at(&source_root);
                command.args(["config", "--includes", "--null", "--get", key]);
                if let Some(inherited) = &inherited {
                    command.env("GIT_CONFIG_GLOBAL", inherited);
                } else if let Some(global) = &caller_global {
                    command.env("GIT_CONFIG_GLOBAL", global);
                } else {
                    // The adopted checkout inherits the operator's Git identity once;
                    // ordinary controller Git must not load the operator's config.
                    command.env_remove("GIT_CONFIG_GLOBAL");
                }
                let output = command
                    .output_locked()
                    .map_err(|error| git_spawn_error(&error))?;
                if output.status.code() == Some(1) {
                    continue; // git-config's documented absent-key status, not an empty value.
                }
                if !output.status.success() {
                    return Err(git_internal(
                        &format!("read source Git identity {key}"),
                        &output,
                    ));
                }
                let mut value = output.stdout;
                if value.pop() != Some(0) || value.contains(&0) {
                    return Err(CowshedError::internal(format!(
                        "invalid NUL-delimited source Git identity value for {key}"
                    )));
                }
                identity.push((key, OsString::from_vec(value)));
            }

            let root = fs::canonicalize(&root)
                .map_err(|error| io_error("resolve Git identity destination", &root, error))?;
            let path = root.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
            let directory = path.parent().expect("identity config has a parent");
            match fs::symlink_metadata(directory) {
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(_) => {
                    return Err(CowshedError::integrity(
                        format!(
                            "Git identity directory is not a real directory: {}",
                            directory.display()
                        ),
                        "replace the workspace's .cowshed symlink or non-directory, then retry",
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::DirBuilder::new()
                        .mode(0o700)
                        .create(directory)
                        .map_err(|error| {
                            io_error("create Git identity directory", directory, error)
                        })?;
                    cowshed_os::fsio::sync_directory(&root)
                        .map_err(|error| io_error("sync Git identity parent", &root, error))?;
                }
                Err(error) => {
                    return Err(io_error("inspect Git identity directory", directory, error));
                }
            }

            // A newly created private directory cannot contain inherited links or Git lockfiles.
            // Let Git quote values, then use the maintained private-file publisher for the final
            // rename; replacing a copied final-component symlink never writes through its target.
            let staging = directory.join(cowshed_os::fsio::temp_name(
                OsStr::new("git-identity.inc"),
                uuid::Uuid::new_v4().simple(),
            ));
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&staging)
                .map_err(|error| {
                    io_error("create Git identity staging directory", &staging, error)
                })?;
            let result = (|| -> Result<()> {
                let draft = staging.join("config");
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&draft)
                    .map_err(|error| io_error("create Git identity draft", &draft, error))?;
                for (key, value) in identity {
                    let output = git_command_at(&source_root)
                        .args([
                            OsStr::new("config"),
                            OsStr::new("--file"),
                            draft.as_os_str(),
                            OsStr::new(key),
                            value.as_os_str(),
                        ])
                        .output_locked()
                        .map_err(|error| git_spawn_error(&error))?;
                    if !output.status.success() {
                        return Err(git_internal("encode inherited Git identity", &output));
                    }
                }
                let bytes = fs::read(&draft)
                    .map_err(|error| io_error("read Git identity draft", &draft, error))?;
                crate::metadata::write_atomic_bytes(&path, &bytes).map_err(|error| {
                    CowshedError::internal(format!(
                        "publish Git identity at {}: {error}",
                        path.display()
                    ))
                })
            })();
            let cleanup = fs::remove_dir_all(&staging).map_err(|error| {
                io_error("remove Git identity staging directory", &staging, error)
            });
            match (result, cleanup) {
                (Ok(()), result) | (result, Ok(())) => result,
                (Err(error), Err(cleanup)) => {
                    Err(CowshedError::internal(format!("{error}; {cleanup}")))
                }
            }
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("inherit Git identity task failed: {error}"))
        })?
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

    #[cfg(any(target_os = "macos", test))]
    /// Read-only digest of a workspace's working state for rename preservation. Without optional
    /// locks it covers the porcelain status with its branch header, the binary index-against-HEAD
    /// diff, the binary working-against-index diff, and the path, mode and bytes of every
    /// untracked non-ignored file; a linked worktree also digests its index file. Inherited links
    /// restored by the clone compare by path and symlink presence only, and only those Git sees as
    /// tracked or untracked non-ignored; ignored links are never part of it. It grants nothing and
    /// copies nothing; callers compare it before and after a rename.
    pub(crate) async fn rename_work_state(
        &self,
        linked: bool,
        restored_links: &[PathBuf],
    ) -> Result<RenameWorkState> {
        // The preserved index is digested before any Git read below: a read that refreshes
        // stat data in a cloned worktree's index would otherwise digest the refresh, not the
        // preservation. The same refresh would rewrite the preserved file itself, so its
        // bytes are held here and put back if a read below changed them.
        let preserved_index = if linked {
            let path = self.linked_index_path().await?;
            let bytes = fs::read(&path)
                .map_err(|error| io_failure("read linked worktree index", &path, error))?;
            Some((path, bytes))
        } else {
            None
        };
        let index = match &preserved_index {
            Some((_, bytes)) => Some(Self::digest_index_bytes(bytes.clone()).await?),
            None => None,
        };
        let restored = self.git_visible_restored_links(restored_links).await?;
        let mut pathspecs = vec![OsString::from("--"), OsString::from(".")];
        pathspecs.extend(restored.iter().map(|path| {
            let mut spec = OsString::from(":(top,literal,exclude)");
            spec.push(path.as_os_str());
            spec
        }));
        let mut status_args: Vec<OsString> = [
            "--no-optional-locks",
            "status",
            "--porcelain=v1",
            "-z",
            "--branch",
            "--untracked-files=all",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        status_args.extend(pathspecs.iter().cloned());
        let status = self.run(status_args).await?;
        if !status.status.success() {
            return Err(git_internal("read rename work state", &status));
        }
        let cached = self.run(diff_args(true, &pathspecs)).await?;
        if !cached.status.success() {
            return Err(git_internal("diff rename index against HEAD", &cached));
        }
        let working = self.run(diff_args(false, &pathspecs)).await?;
        if !working.status.success() {
            return Err(git_internal(
                "diff rename working tree against index",
                &working,
            ));
        }
        let untracked: Vec<PathBuf> = porcelain_records(&status.stdout)
            .filter(|(code, _)| *code == b"??".as_slice())
            .map(|(_, path)| PathBuf::from(OsStr::from_bytes(path)))
            .collect();
        let root = self.root.clone();
        let extras =
            tokio::task::spawn_blocking(move || hash_work_extras(&root, &untracked, &restored))
                .await
                .map_err(|error| {
                    CowshedError::internal(format!("hash rename work extras: {error}"))
                })??;

        use sha2::{Digest as _, Sha256};
        let mut hasher = Sha256::new();
        for section in [
            status.stdout.as_slice(),
            cached.stdout.as_slice(),
            working.stdout.as_slice(),
            extras.as_slice(),
        ] {
            let length = u64::try_from(section.len())
                .expect("a digest section length fits in u64 on every supported platform");
            hasher.update(length.to_le_bytes());
            hasher.update(section);
        }
        let work_digest = Sha256Digest::from_bytes(hasher.finalize().into());
        if let Some((path, bytes)) = &preserved_index
            && fs::read(path).is_ok_and(|current| current != *bytes)
        {
            fs::write(path, bytes)
                .map_err(|error| io_failure("restore linked worktree index", path, error))?;
        }
        Ok(RenameWorkState {
            work: *work_digest.as_bytes(),
            index: index.map(|digest| *digest.as_bytes()),
        })
    }

    #[cfg(any(target_os = "macos", test))]
    /// The index file of a linked worktree, resolved without reading it.
    async fn linked_index_path(&self) -> Result<PathBuf> {
        let output = self
            .run(["rev-parse", "--path-format=absolute", "--git-path", "index"])
            .await?;
        if !output.status.success() {
            return Err(git_internal("resolve linked worktree index", &output));
        }
        parse_one_path(&output.stdout, "linked worktree index")
    }

    #[cfg(any(target_os = "macos", test))]
    /// SHA-256 of index bytes already read, hashed off the runtime's threads.
    async fn digest_index_bytes(bytes: Vec<u8>) -> Result<Sha256Digest> {
        tokio::task::spawn_blocking(move || {
            use sha2::{Digest as _, Sha256};
            Sha256Digest::from_bytes(Sha256::digest(&bytes).into())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("hash linked worktree index: {error}")))
    }

    #[cfg(any(target_os = "macos", test))]
    /// The restored links Git itself reports as tracked or untracked non-ignored, sorted. Ignored
    /// links are not Git's work, so they are neither exempted nor hashed.
    async fn git_visible_restored_links(&self, restored: &[PathBuf]) -> Result<Vec<PathBuf>> {
        if restored.is_empty() {
            return Ok(Vec::new());
        }
        let mut args: Vec<OsString> = [
            "--no-optional-locks",
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        for path in restored {
            let mut spec = OsString::from(":(literal)");
            spec.push(path.as_os_str());
            args.push(spec);
        }
        let output = self.run(args).await?;
        if !output.status.success() {
            return Err(git_internal("list inherited links Git sees", &output));
        }
        let visible: std::collections::BTreeSet<PathBuf> = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|field| !field.is_empty())
            .map(|field| PathBuf::from(OsStr::from_bytes(field)))
            .collect();
        let mut sorted: Vec<PathBuf> = restored
            .iter()
            .filter(|path| visible.contains(*path))
            .cloned()
            .collect();
        sorted.sort();
        sorted.dedup();
        Ok(sorted)
    }

    #[cfg(target_os = "macos")]
    /// The plan that restores this tree's inherited links, as the paths it would rewrite, in order.
    /// Refuses, naming each link, when any link cannot be restored.
    pub(crate) async fn rename_link_paths(&self) -> Result<Vec<PathBuf>> {
        let root = self.root.clone();
        let plan = tokio::task::spawn_blocking(move || crate::inherited_links::plan(&root, &root))
            .await
            .map_err(|error| CowshedError::internal(format!("plan inherited links: {error}")))??;
        if !plan.refusals.is_empty() {
            return Err(CowshedError::integrity(
                format!(
                    "{} inherited link(s) cannot be restored: {}",
                    plan.refusals.len(),
                    plan.refusal_report()
                ),
                "repair the link in the source checkout — for a package manager `link:` dependency, re-run its install there — then retry",
            ));
        }
        Ok(plan
            .rewrites
            .into_iter()
            .map(|rewrite| rewrite.at)
            .collect())
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
        self.is_dirty_by(None).await
    }

    /// [`Self::is_dirty`] judged against `checkout` (the adopted main), which adds two readings
    /// of an untracked path that is not work.
    ///
    /// One is a path the PROJECT's own gitignore rules ignore. A clone carries the ignore file
    /// of the commit it was cut from, so bench output main learned to ignore an hour later still
    /// reads as work in every older clone unless the current rules decide.
    ///
    /// The other is a path main's checkout holds with the same content. An untracked file left
    /// in main arrives in every clone, so reading it as work would make every workspace of that
    /// project unremovable and fail every `land` at its retire step. Main still holds every
    /// byte of such a path, so retiring the workspace loses nothing.
    pub async fn is_dirty_by(&self, checkout: Option<&Path>) -> Result<bool> {
        Ok(!self.dirty_paths_by(checkout).await?.is_empty())
    }

    /// The paths [`Self::is_dirty_by`] reads as work, in `git status` order: what a refusal names.
    /// A clean tree costs what `is_dirty_by` always cost; only a dirty one reads past its first
    /// path.
    pub async fn dirty_paths_by(&self, checkout: Option<&Path>) -> Result<Vec<PathBuf>> {
        let output = self.porcelain_status("read repository status").await?;
        let mut work: Vec<PathBuf> = Vec::new();
        let mut untracked: Vec<&[u8]> = Vec::new();
        for (status, path) in porcelain_records(&output.stdout) {
            if is_untracked_junk(status, path) {
                continue;
            }
            if status == b"??" {
                untracked.push(path);
            } else {
                work.push(PathBuf::from(OsStr::from_bytes(path)));
            }
        }
        let Some(checkout) = checkout.filter(|_| !untracked.is_empty()) else {
            work.extend(
                untracked
                    .into_iter()
                    .map(|path| PathBuf::from(OsStr::from_bytes(path))),
            );
            return Ok(work);
        };
        // One check-ignore for every untracked path, not one process per path. It answers in
        // input order, so the ignored paths are a subsequence of `untracked` and one pass
        // separates them.
        let ignored = ignored_by(checkout, &untracked).await;
        let mut ignored = ignored.iter().map(Vec::as_slice).peekable();
        let unignored: Vec<PathBuf> = untracked
            .into_iter()
            .filter(|path| {
                let is_ignored = ignored.peek() == Some(path);
                if is_ignored {
                    ignored.next();
                }
                !is_ignored
            })
            .map(|path| PathBuf::from(OsStr::from_bytes(path)))
            .collect();
        if unignored.is_empty() {
            return Ok(work);
        }
        let root = self.root.clone();
        let checkout = checkout.to_path_buf();
        let unheld = tokio::task::spawn_blocking(move || {
            unignored
                .into_iter()
                .filter(|relative| !held_by_checkout(&root, &checkout, relative))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|source| {
            CowshedError::internal(format!(
                "comparing untracked files with main's checkout panicked: {source}"
            ))
        })?;
        work.extend(unheld);
        Ok(work)
    }

    /// The tracked paths with staged or unstaged changes: exactly what makes `git rebase` refuse
    /// to start ("cannot rebase: You have unstaged changes"). Untracked files never block it.
    pub async fn tracked_change_paths(&self) -> Result<Vec<PathBuf>> {
        let output = self
            .run(["status", "--porcelain=v1", "-z", "--untracked-files=no"])
            .await?;
        if !output.status.success() {
            return Err(git_internal("read tracked changes", &output));
        }
        Ok(porcelain_records(&output.stdout)
            .map(|(_, path)| PathBuf::from(OsStr::from_bytes(path)))
            .collect())
    }

    /// Exclude cowshed's runtime metadata, the volume's own bookkeeping
    /// ([`cowshed_apfs::VOLUME_METADATA`]) and adopted main's Spotlight opt-out at the
    /// volume root ([`cowshed_apfs::SPOTLIGHT_OPT_OUT`]) in `.git/info/exclude`: a checkout is its
    /// volume's root, so none of it is the repository's work, for a dirty check or for a clean.
    /// Directories are excluded as `name/`; the marker is a file, so it is `/name`, anchored at
    /// the root, where a trailing slash would match only a directory.
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
            let directories = std::iter::once(".cowshed")
                .chain(cowshed_apfs::VOLUME_METADATA)
                .map(|name| format!("{name}/"));
            for pattern in directories.chain([format!("/{}", cowshed_apfs::SPOTLIGHT_OPT_OUT)]) {
                let pattern = pattern.as_bytes();
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

    /// Record that this repository's working tree lives on a case-sensitive volume.
    ///
    /// Git probes the filesystem for `core.ignorecase` once, at `git init`, and trusts the
    /// recorded value from then on. A repository copied off a case-insensitive volume carries
    /// `true` onto the image's case-sensitive one, where git would then fold names the filesystem
    /// keeps apart; `false` is what `git init` would have recorded there.
    pub async fn record_case_sensitive_filesystem(&self) -> Result<()> {
        let output = self
            .run(["config", "--local", "core.ignorecase", "false"])
            .await?;
        ensure_git_success("record the case-sensitive filesystem", output)
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
    ///
    /// A retried removal finds the bundle its earlier attempt already wrote for this exact range
    /// and keeps it: the artifact is written once. A new bundle is written beside its final name
    /// and renamed into place once verified, so no attempt shares git's lock file with another
    /// writer of the same name, and the final name never holds a partial bundle.
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
        let existing = tokio::fs::try_exists(destination).await.map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot inspect abandonment bundle {}: {error}",
                    destination.display()
                ),
                "repair the cowshed store and retry removal",
            )
        })?;
        if existing {
            match self
                .verify_bundle(destination, &expected_head, target, commit_count)
                .await
            {
                Ok(()) => return Ok(commit_count),
                Err(error) => eprintln!(
                    "cowshed: replacing {}, which does not hold this range: {}",
                    destination.display(),
                    error.message
                ),
            }
        }
        let file_name = destination.file_name().ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "abandonment bundle has no file name: {}",
                    destination.display()
                ),
                "repair the cowshed store and retry removal",
            )
        })?;
        let mut staged_name = OsString::from(".");
        staged_name.push(file_name);
        staged_name.push(format!(".{}.partial", uuid::Uuid::new_v4().simple()));
        let staged = destination.with_file_name(staged_name);
        let mut args = vec![
            OsString::from("bundle"),
            OsString::from("create"),
            staged.as_os_str().to_owned(),
            OsString::from(head),
        ];
        if let Some(target) = target {
            args.push(OsString::from(target));
        }
        let written = async {
            let output =
                run_git_bundle_at(&self.root, self.alternate_objects.as_deref(), args).await?;
            ensure_git_success("write commit bundle", output)?;
            self.verify_bundle(&staged, &expected_head, target, commit_count)
                .await?;
            tokio::fs::rename(&staged, destination)
                .await
                .map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot publish abandonment bundle {}: {error}",
                            destination.display()
                        ),
                        "repair the cowshed store and retry removal",
                    )
                })
        }
        .await;
        if let Err(error) = written {
            return Err(match tokio::fs::remove_file(&staged).await {
                Ok(()) => error,
                Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => error,
                Err(cleanup) => CowshedError::new(
                    error.code,
                    format!(
                        "{}; the partial bundle {} could not be removed either: {cleanup}",
                        error.message,
                        staged.display()
                    ),
                    error.hint,
                ),
            });
        }
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
        let scratch = BundleVerificationScratch::create(parent)?;

        let verification = async {
            let output = run_git_at(&scratch.path, ["init", "--bare", "."]).await?;
            if !output.status.success() {
                return Err(bundle_artifact_error(
                    "initialize abandonment bundle verification repository",
                    &output,
                ));
            }
            let recovery = Self::from_root(&scratch.path);
            let output = run_git_bundle_at(
                &scratch.path,
                None,
                [
                    OsStr::new("bundle"),
                    OsStr::new("verify"),
                    bundle.as_os_str(),
                ],
            )
            .await?;
            if !output.status.success() {
                return Err(bundle_artifact_error(
                    "verify abandonment bundle",
                    &output,
                ));
            }
            let output = run_git_bundle_at(
                &scratch.path,
                None,
                [
                    OsStr::new("fetch"),
                    OsStr::new("--quiet"),
                    OsStr::new("--no-tags"),
                    bundle.as_os_str(),
                    OsStr::new("HEAD"),
                ],
            )
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

        let cleanup = scratch.cleanup().await;
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
            .run(["status", "--porcelain=v1", "-z", "--untracked-files=all"])
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
    /// An empty `--show-prefix` proves this is the repository root in the same lookup;
    /// otherwise a missing checkout could silently resolve an enclosing repository.
    pub async fn object_directory(&self) -> Result<PathBuf> {
        let output = self
            .run([
                "rev-parse",
                "--show-prefix",
                "--path-format=absolute",
                "--git-path",
                "objects",
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal("locate the git object store", &output));
        }
        let root_output = output.stdout.strip_prefix(b"\n").ok_or_else(|| {
            CowshedError::integrity(
                format!("{} is not a git repository root", self.root.display()),
                "restore the expected repository at this path, then retry",
            )
        })?;
        parse_one_path(root_output, "git object store")
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

    async fn ensure_workspace_branch_checked_out(
        &self,
        branch: &str,
        branch_ref: &str,
    ) -> Result<()> {
        let current = self.run(["symbolic-ref", "--quiet", "HEAD"]).await?;
        if current.status.success() {
            let current = parse_one_string(&current.stdout, "resumed workspace branch")?;
            if current == branch_ref {
                return Ok(());
            }
        } else if current.status.code() != Some(1) {
            return Err(git_internal("resolve resumed workspace branch", &current));
        }
        let switched = self.run(["switch", branch]).await?;
        ensure_git_success("resume workspace branch", switched)
    }

    async fn workspace_branch_for_initialization(
        &self,
        name: &str,
        holder: &str,
        retry: String,
        resuming: bool,
    ) -> Result<(String, bool)> {
        let (branch, branch_ref) = workspace_branch(name);
        if !resuming {
            return self
                .ensure_workspace_branch_absent(name, holder, retry)
                .await
                .map(|branch| (branch, false));
        }
        let exists = self
            .run(["show-ref", "--verify", "--quiet", branch_ref.as_str()])
            .await?;
        match exists.status.code() {
            Some(1) => Ok((branch, false)),
            Some(0) => Ok((branch, true)),
            _ => Err(git_internal("check resumed workspace branch", &exists)),
        }
    }

    /// Turn a fresh clone into a workspace: restore what its inherited symlinks meant in the tree
    /// that produced them, give it Git that answers to main, and capture the identity it commits
    /// as. Replayable when `resuming`, which only the storage lifecycle fence may grant.
    pub async fn mint_workspace(
        &self,
        name: &str,
        origin: CloneOrigin<'_>,
        repository: WorkspaceRepository,
        start: Option<&str>,
        clean: bool,
        resuming: bool,
    ) -> Result<()> {
        if self.root.join(".git").is_dir() {
            timed_async("fork", "excludes", self.ensure_cowshed_excludes()).await?;
            if clean {
                timed_async("fork", "head", self.discard_uncommitted()).await?;
            }
        }
        // Before any branch is checked out, so only what the clone inherited is judged. On resume
        // every link still holds either the source's bytes or the absolute target an earlier pass
        // derived from those same bytes, so the source remains the producer to resolve against.
        timed_async("fork", "links", self.restore_inherited_links(origin.source)).await?;
        match repository {
            WorkspaceRepository::LinkedWorktree => {
                // Registration may have replaced the cloned `.git` directory with a pointer
                // before a kill. Resume that state machine before preparing workspace Git.
                let source_head = Self::from_root(origin.source).head_oid().await?;
                self.adopt_as_linked_worktree_resumable(
                    name,
                    origin.main,
                    Some(source_head.as_str()),
                    start,
                    clean,
                    resuming,
                )
                .await?;
            }
            WorkspaceRepository::Standalone => {
                self.prepare_workspace(name, origin.main, start, resuming)
                    .await?;
            }
        }
        // A sandboxed child reads this file as its whole global Git configuration. Capturing at
        // mint is what makes `git commit` in a fresh workspace author as the operator instead of
        // failing for want of an identity. Only the source's context resolves the operator's
        // `includeIf gitdir:` rules, and a fork of a workspace that already inherited one no
        // longer needs the operator's global file at all.
        timed_async(
            "fork",
            "identity",
            self.inherit_identity_from(origin.source),
        )
        .await
    }

    /// Opt-in clean discards tracked edits and nonignored untracked paths, keeping ignored warmth.
    /// Only call through clone-owned Git: a linked source's inherited pointer must first be
    /// replaced by the clone's registration/index, never reset through the source registration.
    async fn discard_uncommitted(&self) -> Result<()> {
        // No `.git`-shape guard: the mint call site checks for a real directory itself, and
        // the linked-adopt call site runs after registration replaced the inherited pointer
        // with the clone's own, where `.git` is a file and the discard must still run.
        // A directory clone preserves bytes and mtimes, not inode identity. Refresh HEAD's
        // index against those bytes before the hard reset: otherwise Git rewrites unchanged
        // files whose copied stat cache names the source's inodes, invalidating warm builds.
        let refresh = self.run(["reset", "-q", "--mixed", "HEAD"]).await?;
        ensure_git_success(
            "refresh the clone's index without rewriting its worktree",
            refresh,
        )?;
        let reset = self.run(["reset", "-q", "--hard", "HEAD"]).await?;
        ensure_git_success("reset source edits in the clone", reset)?;
        let clean = self.run(["clean", "-q", "-f", "-d"]).await?;
        ensure_git_success("delete nonignored untracked files in the clone", clean)
    }

    /// Configure local-only workspace Git. A fresh preparation refuses an existing branch; it
    /// continues this clone's branch only when `resuming` proves the canonical image came from
    /// the pending lifecycle record.
    async fn prepare_workspace(
        &self,
        name: &str,
        main_mount: &Path,
        start: Option<&str>,
        resuming: bool,
    ) -> Result<MainRemote> {
        if !main_mount.is_absolute() {
            return Err(CowshedError::usage(
                "workspace main remote must be an absolute local path",
                "retry from a resolved repository root",
            ));
        }
        // Before any branch is judged: an inherited registration may hold `cowshed/<name>`.
        timed_async(
            "fork",
            "worktrees",
            self.strip_inherited_worktree_registrations(),
        )
        .await?;

        let retry = format!("remove or rename cowshed/{name}, then retry: cowshed fork {name}");
        let (branch, branch_ready) = self
            .workspace_branch_for_initialization(name, "the cloned workspace", retry, resuming)
            .await?;

        // The `.git` directory arrived by CoW carrying every remote main had, including network
        // URLs. Replaying this loop is safe: an earlier pass leaves either no remotes or the local
        // cowshed-owned `main`, which configure_main_remote recreates below.
        timed_async("fork", "remotes", async {
            for remote in self.remote_names().await? {
                let output = self.run(["remote", "remove", remote.as_str()]).await?;
                ensure_git_success("remove inherited remote", output)?;
            }
            Ok::<_, CowshedError>(())
        })
        .await?;
        let main_remote = timed_async(
            "fork",
            "main-remote",
            self.configure_main_remote(main_mount),
        )
        .await?;

        timed_async("fork", "branch", async {
            if branch_ready {
                let (_, branch_ref) = workspace_branch(name);
                self.ensure_workspace_branch_checked_out(&branch, &branch_ref)
                    .await
            } else {
                self.switch_to_workspace_branch(&branch, start).await
            }
        })
        .await?;
        Ok(main_remote)
    }

    /// Drop the linked-worktree registrations this standalone clone inherited from main, before any
    /// branch is judged against them (02_workspaces.md, fork mint step 6).
    async fn strip_inherited_worktree_registrations(&self) -> Result<()> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || strip_inherited_registrations(&root))
            .await
            .map_err(|error| {
                CowshedError::internal(format!("stripping inherited worktrees panicked: {error}"))
            })?
    }

    /// Re-resolve this tree's symlinks that point outside it or into the tree they came from,
    /// against that tree.
    ///
    /// Runs at mint, where the workspace is still cowshed's: a link that escapes the root is
    /// generated state carrying its source tree's depth, in the same way an inherited remote
    /// carries main's URLs, and neither is the user's yet.
    ///
    /// Only escaping links and absolute links into the source tree are rewritten; the latter
    /// become relative links to the same path in this tree. That predicate keeps the step
    /// affordable on a tree whose whole value is being ready in seconds — the walk reads
    /// directory entries and rewrites the handful of links that need it, rather than reinstalling
    /// anything.
    ///
    /// An escaping link whose source-tree target does not exist is refused by name rather than
    /// repointed at a guess, and the refusal writes nothing. It is broken in the source tree
    /// too, and a workspace that silently resolves it to whatever happens to sit at that path in
    /// the new tree is the failure being fixed, not an acceptable outcome.
    pub(crate) async fn restore_inherited_links(
        &self,
        source_root: &Path,
    ) -> Result<crate::inherited_links::LinkPlan> {
        let root = self.root.clone();
        let source = source_root.to_path_buf();
        let plan = tokio::task::spawn_blocking(move || {
            crate::inherited_links::restore(
                &root,
                &source,
                crate::inherited_links::RestoreScope::Inherited,
            )
        })
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
    /// `main_mount`, then create the session branch — `cowshed fork --git-worktree`.
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
        self.adopt_as_linked_worktree_resumable(name, main_mount, None, start, false, false)
            .await
    }

    /// Continue a pending fork's exact registration, then initialize its index and branch.
    /// `resuming` is granted only by the storage lifecycle fence.
    pub async fn adopt_as_linked_worktree_resumable(
        &self,
        name: &str,
        main_mount: &Path,
        source_head: Option<&str>,
        start: Option<&str>,
        clean: bool,
        resuming: bool,
    ) -> Result<()> {
        if !main_mount.is_absolute() {
            return Err(CowshedError::usage(
                "linked worktree repository must be an absolute local path",
                "retry from a resolved repository root",
            ));
        }
        let main = Self::from_root(main_mount);
        let retry = format!(
            "remove or rename cowshed/{name}, then retry: cowshed fork {name} --git-worktree"
        );
        let (branch, branch_ref) = workspace_branch(name);
        let branch_exists = main
            .run(["show-ref", "--verify", "--quiet", branch_ref.as_str()])
            .await?;
        let branch_ready = match branch_exists.status.code() {
            Some(0) if resuming => true,
            Some(0) => {
                return Err(CowshedError::conflict(
                    format!("branch {branch} already exists in main's repository"),
                    retry,
                ));
            }
            Some(1) => false,
            _ => return Err(git_internal("check linked-worktree branch", &branch_exists)),
        };
        let admin = main.worktree_admin_dir(name).await?;
        if branch_ready && !admin.is_dir() {
            return Err(CowshedError::integrity(
                format!(
                    "resumed branch {branch} exists without linked-worktree registration {name}"
                ),
                "cowshed doctor --json",
            ));
        }
        let fallback_head;
        let head = match source_head.or(start) {
            Some(head) => head,
            None => {
                fallback_head = main.head_oid().await?;
                fallback_head.as_str()
            }
        };
        self.register_linked_worktree(name, &main, &admin, head, resuming)
            .await?;
        let reset = self.run(["reset", "-q"]).await?;
        ensure_git_success("populate linked worktree index", reset)?;
        main.ensure_cowshed_excludes().await?;
        if clean {
            self.discard_uncommitted().await?;
        }
        if branch_ready {
            self.ensure_workspace_branch_checked_out(&branch, &branch_ref)
                .await?;
        } else {
            self.switch_to_workspace_branch(&branch, start).await?;
        }
        Ok(())
    }

    /// Rename preserves the linked source's Git administration, including staged-only bytes.
    /// Only its registration id and counterpart path change; no reset or switch runs.
    pub async fn preserve_linked_worktree_for_rename(
        &self,
        source_name: &str,
        name: &str,
        source_mount: &Path,
        main_mount: &Path,
        resuming: bool,
    ) -> Result<()> {
        let main = Self::from_root(main_mount);
        let source = Self::from_root(source_mount);
        let source_admin = main.worktree_admin_dir(source_name).await?;
        let actual = source.run(["rev-parse", "--absolute-git-dir"]).await?;
        if !actual.status.success() {
            return Err(git_internal("read rename source Git directory", &actual));
        }
        if parse_one_path(&actual.stdout, "rename source Git directory")? != source_admin {
            return Err(CowshedError::integrity(
                "rename source does not own its recorded linked-worktree registration",
                "leave the foreign registration intact; cowshed doctor --json",
            ));
        }
        let head = source.head_oid().await?;
        let admin = main.worktree_admin_dir(name).await?;
        self.register_linked_worktree(name, &main, &admin, head.as_str(), resuming)
            .await?;
        tokio::task::spawn_blocking(move || {
            copy_worktree_administration(&source_admin, &admin, true)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("preserve renamed worktree Git state: {error}"))
        })?
    }
    /// Establish clone-owned Git, without resetting its index or selecting a branch.
    async fn register_linked_worktree(
        &self,
        name: &str,
        main: &Self,
        admin: &Path,
        head: &str,
        resuming: bool,
    ) -> Result<()> {
        let mount = fs::canonicalize(&self.root)
            .map_err(|error| io_failure("canonicalize linked worktree mount", &self.root, error))?;
        let staging = mount.join(WORKTREE_STAGING);
        let staged = staging.join(name);
        let staged_dot_git = staged.join(".git");
        let dot_git = mount.join(".git");
        let admin_present =
            inspect_path(admin, "inspect linked-worktree administration")?.is_some();
        if admin_present && !resuming {
            return Err(CowshedError::conflict(
                format!("main already registers a linked worktree named {name}"),
                format!("cowshed rm {name}"),
            ));
        }
        if !admin_present {
            tokio::task::spawn_blocking({
                let dot_git = dot_git.clone();
                let staging = staging.clone();
                move || -> Result<()> {
                    match inspect_path(&dot_git, "inspect clone .git before registration")? {
                        Some(metadata) if metadata.is_dir() => fs::remove_dir_all(&dot_git)
                            .map_err(|error| {
                                io_failure("remove clone .git directory", &dot_git, error)
                            })?,
                        Some(_) => fs::remove_file(&dot_git).map_err(|error| {
                            io_failure("remove clone .git pointer", &dot_git, error)
                        })?,
                        None => {}
                    }
                    if inspect_path(&staging, "inspect worktree staging")?.is_some() {
                        fs::remove_dir_all(&staging).map_err(|error| {
                            io_failure("remove worktree staging", &staging, error)
                        })?;
                    }
                    fs::create_dir_all(&staging)
                        .map_err(|error| io_failure("create worktree staging", &staging, error))
                }
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("prepare worktree staging: {error}"))
            })??;
            let output = run_git_at_with_objects(
                &main.root,
                main.alternate_objects.as_deref(),
                #[cfg(target_os = "macos")]
                Some(&staging),
                [
                    OsStr::new("worktree"),
                    OsStr::new("add"),
                    OsStr::new("--no-checkout"),
                    OsStr::new("--detach"),
                    staged.as_os_str(),
                    OsStr::new(head),
                ],
            )
            .await?;
            ensure_git_success("register linked worktree", output)?;
        }
        match inspect_path(admin, "inspect linked-worktree administration")? {
            Some(metadata) if metadata.file_type().is_dir() => {}
            Some(_) => {
                return Err(CowshedError::integrity(
                    format!(
                        "linked-worktree administration {} is not a real directory",
                        admin.display()
                    ),
                    "leave the foreign registration intact; cowshed doctor --json",
                ));
            }
            None => {
                return Err(CowshedError::integrity(
                    format!("git registered the linked worktree for {name} under another id"),
                    "cowshed doctor --json",
                ));
            }
        }
        let gitdir_file = admin.join("gitdir");
        let registered = fs::read(&gitdir_file).map_err(|error| {
            CowshedError::integrity(
                format!(
                    "read linked-worktree registration {name} at {}: {error}",
                    gitdir_file.display()
                ),
                "cowshed doctor --json",
            )
        })?;
        let registered = parse_one_path(&registered, "linked worktree gitdir")?;
        if registered != dot_git && registered != staged_dot_git {
            return Err(CowshedError::integrity(
                format!(
                    "linked-worktree registration {name} belongs to {}",
                    registered.display()
                ),
                "leave the foreign registration intact; cowshed doctor --json",
            ));
        }
        if inspect_path(&dot_git, "inspect clone .git")?.is_some_and(|metadata| metadata.is_dir()) {
            return Err(CowshedError::integrity(
                format!("linked-worktree registration {name} exists beside a full .git directory"),
                "cowshed doctor --json",
            ));
        }
        match inspect_path(&staged_dot_git, "inspect staged .git pointer")? {
            Some(metadata) if metadata.is_file() => {
                tokio::task::spawn_blocking({
                    let from = staged_dot_git.clone();
                    let to = dot_git.clone();
                    move || -> Result<()> {
                        fs::rename(&from, &to).map_err(|error| {
                            io_failure("relocate staged worktree pointer", &from, error)
                        })
                    }
                })
                .await
                .map_err(|error| {
                    CowshedError::internal(format!("relocate worktree pointer: {error}"))
                })??;
            }
            Some(_) => {
                return Err(CowshedError::integrity(
                    format!("staged .git for {name} is not a pointer file"),
                    "cowshed doctor --json",
                ));
            }
            None => match inspect_path(&dot_git, "inspect clone .git pointer")? {
                Some(metadata) if metadata.is_file() => {}
                _ => {
                    return Err(CowshedError::integrity(
                        format!(
                            "linked-worktree registration {name} has neither canonical nor staged .git pointer"
                        ),
                        "cowshed doctor --json",
                    ));
                }
            },
        }
        if inspect_path(&staging, "inspect worktree staging")?.is_some() {
            tokio::task::spawn_blocking({
                let staging = staging.clone();
                move || -> Result<()> {
                    fs::remove_dir_all(&staging)
                        .map_err(|error| io_failure("remove worktree staging", &staging, error))
                }
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("remove worktree staging: {error}"))
            })??;
        }
        main.repair_linked_worktree(&mount).await
    }

    /// Point this repository's registration for `mount` back at the mount, both directions.
    ///
    /// The primitive git provides for exactly this two-way pointer fixup, and the one `cowshed mv`
    /// runs after moving either end.
    pub async fn repair_linked_worktree(&self, mount: &Path) -> Result<()> {
        let output = run_git_at_with_objects(
            &self.root,
            self.alternate_objects.as_deref(),
            #[cfg(target_os = "macos")]
            Some(mount),
            [
                OsStr::new("worktree"),
                OsStr::new("repair"),
                mount.as_os_str(),
            ],
        )
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
        run_git_at_with_objects(
            &self.root,
            self.alternate_objects.as_deref(),
            #[cfg(target_os = "macos")]
            None,
            args,
        )
        .await
    }
}

async fn run_git_at<I, S>(root: &Path, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_git_at_with_objects(
        root,
        None,
        #[cfg(target_os = "macos")]
        None,
        args,
    )
    .await
}

/// Bundle object transport is the narrow exception to sandboxed Git: the
/// controller writes and verifies bundles in its denied store, which no
/// workspace profile can read. These object-only verbs do not check out files,
/// run merge/filter drivers, use configured remotes, or invoke hooks (disabled here).
async fn run_git_bundle_at<I, S>(
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
    command.kill_on_drop(true);
    command
        .output_locked()
        .await
        .map_err(|error| git_spawn_error(&error))
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

/// Point git at a checkout without allowing repository metadata to run hooks or
/// filesystem monitors in the controller's process. Other executable config
/// (merge drivers, filters and transports) is confined by the controller's
/// workspace Git sandbox when it runs a workspace operation.
pub fn git_command_at(root: &Path) -> std::process::Command {
    let mut command = std::process::Command::new("git");
    command
        .arg("-C")
        .arg(root)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "-c",
            "core.pager=cat",
            "-c",
            "core.editor=false",
            "-c",
            "sequence.editor=false",
            "-c",
            "diff.external=false",
        ])
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_CONFIG_COUNT", "0")
        .env_remove("GIT_CONFIG_PARAMETERS")
        .env_remove("GIT_SSH_COMMAND")
        .env_remove("GIT_EXTERNAL_DIFF")
        .env_remove("GIT_SEQUENCE_EDITOR")
        .env_remove("GIT_EDITOR");
    command
}

/// A job can rewrite a linked worktree's `.git` pointer. Its claimed common
/// repository gains write authority only when that repository's own worktree
/// registration points back to this exact checkout.
#[cfg(target_os = "macos")]
fn registered_worktree_git_dir(root: &Path) -> Result<Option<PathBuf>> {
    let pointer = root.join(".git");
    let metadata = match fs::symlink_metadata(&pointer) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CowshedError::integrity(
                format!(
                    "cannot inspect Git worktree pointer {}: {error}",
                    pointer.display()
                ),
                "repair the Git worktree",
            ));
        }
    };
    if !metadata.file_type().is_file() {
        return Ok(None);
    }
    let content = fs::read_to_string(&pointer).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot read Git worktree pointer {}: {error}",
                pointer.display()
            ),
            "repair the Git worktree",
        )
    })?;
    let admin = content.trim_end().strip_prefix("gitdir: ").ok_or_else(|| {
        CowshedError::integrity("invalid Git worktree pointer", "repair the Git worktree")
    })?;
    let admin = fs::canonicalize(admin).map_err(|error| {
        CowshedError::integrity(
            format!("cannot resolve Git worktree administration: {error}"),
            "repair the Git worktree",
        )
    })?;
    let worktrees = admin.parent().ok_or_else(|| {
        CowshedError::integrity(
            "Git worktree administration has no parent",
            "repair the Git worktree",
        )
    })?;
    if worktrees.file_name() != Some(OsStr::new("worktrees")) {
        return Err(CowshedError::integrity(
            "Git worktree administration is not under worktrees",
            "repair the Git worktree",
        ));
    }
    let common = worktrees.parent().ok_or_else(|| {
        CowshedError::integrity(
            "Git worktree has no common repository",
            "repair the Git worktree",
        )
    })?;
    let reverse = fs::read_to_string(admin.join("gitdir")).map_err(|error| {
        CowshedError::integrity(
            format!("cannot read reverse Git worktree registration: {error}"),
            "repair the Git worktree",
        )
    })?;
    if fs::canonicalize(reverse.trim_end()).ok().as_deref() != Some(pointer.as_path()) {
        return Err(CowshedError::integrity(
            "Git worktree registration does not point back to this workspace",
            "repair the Git worktree",
        ));
    }
    Ok(Some(common.to_owned()))
}

/// Git stops following includes this deep (`MAX_INCLUDE_DEPTH` in Git's `config.c`).
#[cfg(target_os = "macos")]
const MAX_GIT_INCLUDE_DEPTH: usize = 10;

/// The most config files one linked worktree's common config may bring in through includes.
#[cfg(target_os = "macos")]
const MAX_GIT_INCLUDED_FILES: usize = 64;

/// The largest Git config file the controller reads to resolve includes.
#[cfg(target_os = "macos")]
const MAX_GIT_CONFIG_BYTES: u64 = 1024 * 1024;

/// The files main's tree holds that a linked worktree's common config includes, transitively:
/// the read grants its controller Git needs (02_workspaces.md, "Git-worktree workspaces").
///
/// The common config lives in main's `.git`, and an include it names by a path relative to it
/// (`include.path = ../tooling/workspace.gitconfig`) lands in main's working tree, which the
/// worktree's sandboxed Git cannot otherwise read, so Git fails before it reads `HEAD`. Every
/// job of the workspace can write that config, so what it names is held to the narrowest
/// reading: a target inside the workspace or the common directory is already readable and is
/// followed without a grant; a regular file inside main's tree is granted, by itself; any other
/// target is refused by name. `~/` resolves against `git_home`, the HOME the sandboxed Git runs
/// with. An include that does not exist is skipped, as Git skips it. A conditional include is
/// held to the same bounds whatever its condition, which is Git's to decide. Every file is read
/// without following a final symlink or blocking on a FIFO, only when it is a regular file, and
/// only up to [`MAX_GIT_CONFIG_BYTES`].
#[cfg(target_os = "macos")]
fn included_config_files(common: &Path, workspace: &Path, git_home: &Path) -> Result<Vec<PathBuf>> {
    let main_tree = common
        .parent()
        .filter(|_| common.file_name() == Some(OsStr::new(".git")));
    let refuse = |file: &Path, target: &Path, reason: &str| {
        CowshedError::integrity(
            format!(
                "Git config {} includes {}, which {reason}; a git-worktree workspace's controller Git reads only regular config files inside main's checkout, the common Git directory or the workspace",
                file.display(),
                target.display()
            ),
            "move the included file into main's checkout or remove the include from main's .git/config",
        )
    };
    let mut granted: Vec<PathBuf> = Vec::new();
    let mut seen: Vec<PathBuf> = Vec::new();
    let mut pending = vec![(common.join("config"), 0)];
    while let Some((file, depth)) = pending.pop() {
        let text = match read_git_config_bounded(&file)? {
            Some(text) => text,
            None => continue,
        };
        let directory = file.parent().unwrap_or(Path::new("/"));
        for path in config_include_paths(&String::from_utf8_lossy(&text)) {
            let target = match path.strip_prefix("~/") {
                Some(relative) => git_home.join(relative),
                None => directory.join(path),
            };
            let target = match fs::canonicalize(&target) {
                Ok(target) => target,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(refuse(
                        &file,
                        &target,
                        &format!("cannot be resolved: {error}"),
                    ));
                }
            };
            if seen.contains(&target) {
                continue;
            }
            if target.starts_with(workspace) || target.starts_with(common) {
                // Readable already; nothing to grant.
            } else if main_tree.is_some_and(|tree| target.starts_with(tree)) {
                granted.push(target.clone());
            } else {
                return Err(refuse(&file, &target, "is outside main's checkout"));
            }
            if seen.len() == MAX_GIT_INCLUDED_FILES {
                return Err(refuse(
                    &file,
                    &target,
                    &format!("is past the {MAX_GIT_INCLUDED_FILES} included files allowed"),
                ));
            }
            seen.push(target.clone());
            if depth + 1 < MAX_GIT_INCLUDE_DEPTH {
                pending.push((target, depth + 1));
            }
        }
    }
    Ok(granted)
}

/// One Git config file's bytes, or `None` when it does not exist. Refused unless it is a
/// regular file within [`MAX_GIT_CONFIG_BYTES`]: its path comes from config a job can write, so
/// it may name a FIFO, a device or a symlink to one.
#[cfg(target_os = "macos")]
fn read_git_config_bounded(path: &Path) -> Result<Option<Vec<u8>>> {
    use std::io::Read as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let refuse = |reason: String| {
        CowshedError::integrity(
            format!("Git config {} {reason}", path.display()),
            "repair the repository's Git config",
        )
    };
    let file = match fs::OpenOptions::new()
        .read(true)
        // Regular files ignore O_NONBLOCK; a FIFO must not block the open before fstat refuses it.
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(refuse(format!("cannot be opened: {error}"))),
    };
    let metadata = file
        .metadata()
        .map_err(|error| refuse(format!("cannot be inspected: {error}")))?;
    if !metadata.file_type().is_file() {
        return Err(refuse("is not a regular file".to_owned()));
    }
    if metadata.len() > MAX_GIT_CONFIG_BYTES {
        return Err(refuse(format!(
            "is larger than {MAX_GIT_CONFIG_BYTES} bytes"
        )));
    }
    let mut bytes = Vec::new();
    file.take(MAX_GIT_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| refuse(format!("cannot be read: {error}")))?;
    if bytes.len() as u64 > MAX_GIT_CONFIG_BYTES {
        return Err(refuse(format!(
            "is larger than {MAX_GIT_CONFIG_BYTES} bytes"
        )));
    }
    Ok(Some(bytes))
}

/// The `-c` overrides that switch off every filter, merge and diff driver a linked worktree's
/// Git config names, given `git config -z --name-only --list`. Its controller Git reads files in
/// main's checkout that the workspace's own jobs cannot, and those jobs write the config, so no
/// command the config selects may run with that read: filters pass content through unchanged,
/// merge and external diff drivers fail as `diff.external` already does, and a textconv is the
/// identity. A key that cannot be spelled as a `-c` override refuses the command.
#[cfg(target_os = "macos")]
fn driver_overrides(listing: &[u8]) -> Result<Vec<String>> {
    let mut drivers = std::collections::BTreeSet::new();
    for key in listing
        .split(|byte| *byte == 0)
        .filter(|key| !key.is_empty())
    {
        let key = std::str::from_utf8(key).map_err(|_| {
            CowshedError::integrity(
                "a Git config key is not UTF-8",
                "repair the repository's Git config",
            )
        })?;
        let Some((section, rest)) = key.split_once('.') else {
            continue;
        };
        let Some((name, variable)) = rest.rsplit_once('.') else {
            continue;
        };
        let section = section.to_ascii_lowercase();
        let variable = variable.to_ascii_lowercase();
        let driver = match (section.as_str(), variable.as_str()) {
            ("filter", "clean" | "smudge" | "process" | "required") => "filter",
            ("merge", "driver") => "merge",
            ("diff", "textconv" | "command") => "diff",
            _ => continue,
        };
        if name.contains('=') {
            return Err(CowshedError::integrity(
                format!("Git config names a {driver} driver {name:?} that cannot be switched off"),
                "rename the driver in the repository's Git config",
            ));
        }
        drivers.insert((driver, name.to_owned()));
    }
    let mut overrides = Vec::new();
    for (driver, name) in drivers {
        match driver {
            "filter" => overrides.extend([
                format!("filter.{name}.clean="),
                format!("filter.{name}.smudge="),
                format!("filter.{name}.process="),
                format!("filter.{name}.required=false"),
            ]),
            "merge" => overrides.push(format!("merge.{name}.driver=false")),
            _ => overrides.extend([
                format!("diff.{name}.textconv=cat"),
                format!("diff.{name}.command=false"),
            ]),
        }
    }
    Ok(overrides)
}

/// The values of `include.path` and every `includeIf.<condition>.path` in one Git config file,
/// in file order, read with Git's syntax: section and key names are case-insensitive, a
/// `key = value` may follow a section header on its line, a value may be double-quoted with
/// backslash escapes, and an unquoted `#` or `;` starts a comment.
#[cfg(target_os = "macos")]
fn config_include_paths(text: &str) -> Vec<String> {
    let mut including = false;
    let mut paths = Vec::new();
    for line in text.lines() {
        let mut rest = line.trim_start();
        if let Some(header) = rest.strip_prefix('[') {
            let mut quoted = false;
            let Some(end) = header.char_indices().find_map(|(index, character)| {
                match character {
                    '"' => quoted = !quoted,
                    ']' if !quoted => return Some(index),
                    _ => {}
                }
                None
            }) else {
                including = false;
                continue;
            };
            let name = header[..end]
                .split(|character: char| character.is_whitespace() || character == '"')
                .next()
                .unwrap_or_default();
            including =
                name.eq_ignore_ascii_case("include") || name.eq_ignore_ascii_case("includeif");
            rest = header[end + 1..].trim_start();
        }
        if !including {
            continue;
        }
        if let Some((key, value)) = rest.split_once('=')
            && key.trim().eq_ignore_ascii_case("path")
        {
            paths.push(config_value(value));
        }
    }
    paths
}

/// One Git config value as Git reads it: quotes removed, escapes applied, a trailing comment
/// and the unquoted whitespace around the value dropped.
#[cfg(target_os = "macos")]
fn config_value(raw: &str) -> String {
    let mut value = String::new();
    let mut quoted = false;
    let mut spaces = String::new();
    let mut characters = raw.trim_start().chars();
    while let Some(character) = characters.next() {
        match character {
            '"' => {
                value.push_str(&spaces);
                spaces.clear();
                quoted = !quoted;
            }
            '\\' => {
                let escaped = match characters.next() {
                    Some('n') => '\n',
                    Some('t') => '\t',
                    Some('b') => '\u{8}',
                    Some(other) => other,
                    None => break,
                };
                value.push_str(&spaces);
                spaces.clear();
                value.push(escaped);
            }
            '#' | ';' if !quoted => break,
            character if character.is_whitespace() && !quoted => spaces.push(character),
            character => {
                value.push_str(&spaces);
                spaces.clear();
                value.push(character);
            }
        }
    }
    value
}

/// Controller Git can consume repository-selected filters, merge drivers and
/// transports. Run it with the GitDiscovery policy: writable only in this
/// checkout; other trees require controller-supplied, explicit roots.
#[cfg(target_os = "macos")]
pub fn sandboxed_git_command_at(root: &Path) -> Result<std::process::Command> {
    sandboxed_git_command_with_paths(root, None, None)
}

/// A tree outside the checkout that controller Git reads, as the controller names it: never a
/// path the repository's own config or remote URL chose.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
enum ExtraRead<'a> {
    /// A checkout fetched from by local path. Git runs `upload-pack` for it through a shell.
    FetchSource(&'a Path),
    /// Another repository's object store, read in place through `GIT_ALTERNATE_OBJECT_DIRECTORIES`.
    AlternateObjects(&'a Path),
}

#[cfg(target_os = "macos")]
impl<'a> ExtraRead<'a> {
    fn kind(self) -> &'static str {
        match self {
            Self::FetchSource(_) => "Git source checkout",
            Self::AlternateObjects(_) => "Git object store",
        }
    }

    fn path(self) -> &'a Path {
        match self {
            Self::FetchSource(path) | Self::AlternateObjects(path) => path,
        }
    }
}

/// Git for a workspace fetch from `source`, a checkout the controller resolved: main's mount,
/// a lane base, a unit. The repository's own remote URL never authorizes a host read.
#[cfg(target_os = "macos")]
pub(crate) fn sandboxed_git_fetch_command(
    root: &Path,
    source: &Path,
) -> Result<std::process::Command> {
    sandboxed_git_command_with_paths(root, Some(ExtraRead::FetchSource(source)), None)
}

#[cfg(target_os = "macos")]
fn sandboxed_git_command_with_paths(
    root: &Path,
    extra: Option<ExtraRead<'_>>,
    writable: Option<&Path>,
) -> Result<std::process::Command> {
    use crate::sandbox::{
        RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, seatbelt_profile,
    };
    let parent = root
        .parent()
        .ok_or_else(|| CowshedError::internal("git checkout has no parent"))?;
    // Seatbelt filters the resolved spelling. Temporary checkouts often arrive
    // through /var -> /private/var; granting only their lexical name denies cwd.
    let canonical = match fs::canonicalize(root) {
        Ok(canonical) => canonical,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::canonicalize(parent)
            .map_err(|error| {
                CowshedError::integrity(
                    format!(
                        "cannot resolve git checkout parent {}: {error}",
                        parent.display()
                    ),
                    "repair the checkout path",
                )
            })?
            .join(
                root.file_name()
                    .ok_or_else(|| CowshedError::internal("git checkout has no name"))?,
            ),
        Err(error) => {
            return Err(CowshedError::integrity(
                format!("cannot resolve git checkout {}: {error}", root.display()),
                "repair the checkout path",
            ));
        }
    };
    let parent = canonical
        .parent()
        .ok_or_else(|| CowshedError::internal("git checkout has no parent"))?;
    let home = std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
        CowshedError::environment_missing(
            "HOME is unset",
            "set HOME to the operator's home directory",
        )
    })?;
    let common = registered_worktree_git_dir(&canonical)?;
    let git_home = canonical.join(".cowshed/home");
    let mut read = match &common {
        Some(common) => included_config_files(common, &canonical, &git_home)?,
        None => Vec::new(),
    };
    let linked = common.is_some();
    // Reads of main's checkout that the workspace's jobs cannot make: no program the config
    // selects may run beside them, whatever name it is selected by, or whenever it is added.
    // A controller-named tree (`extra`) is not among them: it is one the controller chose. An
    // object store is read in place and runs nothing. A local fetch runs Git's upload-pack through
    // a shell, which a Git that may execute only Git cannot start, so the two never meet: such a
    // fetch is refused here, by name, before Git runs (02_workspaces.md, "Git-worktree workspaces").
    if let Some(ExtraRead::FetchSource(source)) = extra
        && !read.is_empty()
    {
        return Err(CowshedError::sandbox_denied(
            format!(
                "a linked worktree whose main config includes main-tree files cannot fetch: {} reads {} from main's checkout through those includes, so its Git may execute only Git, and fetching from {} runs upload-pack through a shell",
                canonical.display(),
                read.iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                source.display()
            ),
            "move each included file into main's .git or remove the include from main's .git/config, or rebase and land through a standalone lane",
        ));
    }
    let role = if read.is_empty() {
        SandboxProfileRole::GitDiscovery
    } else {
        SandboxProfileRole::GitDiscoveryGitOnly
    };
    if let Some(extra) = extra {
        read.push(fs::canonicalize(extra.path()).map_err(|error| {
            CowshedError::integrity(
                format!(
                    "cannot resolve the {} {}: {error}",
                    extra.kind(),
                    extra.path().display()
                ),
                "repair the path the controller named",
            )
        })?);
    }
    let config = SandboxConfig {
        home,
        mount_root: PathBuf::from(cowshed_gateway_types::STORE_ROOT),
        workspace_mount: canonical.clone(),
        shed_links: Vec::new(),
        exec_temp_dir: canonical.clone(),
        // Controller Git serves the host store, the one it already names as its mount root.
        profile_directory: crate::exec::profile_directory(Path::new(
            cowshed_gateway_types::STORE_ROOT,
        )),
        port_block: crate::metadata::PortBlock::new(40_960, 16).expect("static macOS port block"),
        retained_port_blocks: Vec::new(),
        cpu_pool_directory: None,
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants {
            read,
            write: writable
                .map(|path| {
                    fs::canonicalize(path).map_err(|error| {
                        CowshedError::integrity(
                            format!(
                                "cannot resolve Git worktree staging {}: {error}",
                                path.display()
                            ),
                            "repair the linked worktree staging directory",
                        )
                    })
                })
                .transpose()?
                .into_iter()
                .chain(common)
                .collect(),
            ..SandboxGrants::default()
        },
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        git_worktree_repository: None,
        build_volume_mount: None,
        repository_caches: Vec::new(),
        capabilities: Default::default(),
    };
    let profile = seatbelt_profile(&config, role).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot sandbox controller git in {}: {error}",
                root.display()
            ),
            "repair the workspace Git path",
        )
    })?;
    let profile = crate::exec::ProfileLaunch::write(&config, &profile).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot write controller git's sandbox profile into {}: {error}",
                config.profile_directory.display()
            ),
            "run `cowshed setup` so the store's run directory exists and is yours",
        )
    })?;
    let build = || -> Result<std::process::Command> {
        let git = git_command_at(&canonical);
        let mut command = std::process::Command::new(crate::exec::SANDBOX_EXEC);
        command
            .args(profile.args())
            .arg(format!("{}/usr/bin/git", crate::sandbox::GIT_DEVELOPER_DIR));
        command.args(git.get_args());
        for (name, value) in git.get_envs() {
            match value {
                Some(value) => {
                    command.env(name, value);
                }
                None => {
                    command.env_remove(name);
                }
            }
        }
        command
            .current_dir(if canonical.is_dir() {
                &canonical
            } else {
                parent
            })
            .env("HOME", &git_home);
        if let Some(identity) = workspace_git_identity_config(&canonical)? {
            command.env("GIT_CONFIG_GLOBAL", identity);
        }
        Ok(command)
    };
    let mut command = build()?;
    if linked {
        // Git's own reading of its config, under the same profile, names every driver it could
        // select; each is switched off for the command the caller runs.
        let listing = build()?
            .args(["config", "-z", "--name-only", "--list"])
            .output_locked()
            .map_err(|error| git_spawn_error(&error))?;
        if !listing.status.success() {
            return Err(CowshedError::integrity(
                format!(
                    "cannot read the Git config of {}: {}",
                    root.display(),
                    git_ended(&listing)
                ),
                "repair the repository's Git config",
            ));
        }
        for setting in driver_overrides(&listing.stdout)? {
            command.arg("-c").arg(setting);
        }
    }
    Ok(command)
}

/// The one spelling of "git itself could not run", so the same failure never carries two
/// different instructions depending on which runner hit it.
pub fn git_spawn_error(error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot execute git: {error}"),
        "install the macOS command line developer tools, then retry",
    )
}

/// How a failed Git command ended, followed by anything it wrote to stderr.
/// A signal-killed command can leave stderr empty, so its status must still be reported.
pub(crate) fn git_ended(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    match stderr.trim_end() {
        "" => format!("git {}", output.status),
        said => format!("git {}: {said}", output.status),
    }
}

async fn run_git_at_with_objects<I, S>(
    root: &Path,
    alternate_objects: Option<&Path>,
    #[cfg(target_os = "macos")] writable: Option<&Path>,
    args: I,
) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    #[cfg(target_os = "macos")]
    let git = sandboxed_git_command_with_paths(
        root,
        alternate_objects.map(ExtraRead::AlternateObjects),
        writable,
    )?;
    #[cfg(not(target_os = "macos"))]
    let git = git_command_at(root);
    let mut command = Command::from(git);
    command.args(args);
    if let Some(objects) = alternate_objects {
        command.env("GIT_ALTERNATE_OBJECT_DIRECTORIES", objects);
    }
    command.kill_on_drop(true);
    command
        .output_locked()
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

/// The subset of `relative` paths that `checkout`'s gitignore rules ignore - one
/// `git check-ignore --stdin -z` over all of them, index-blind, each path judged as a fresh
/// entry in that tree would be. Returns the ignored paths in git's order.
pub async fn ignored_by(checkout: &Path, relative: &[&[u8]]) -> Vec<Vec<u8>> {
    use tokio::io::AsyncWriteExt;
    let mut child = match Command::from(git_command_at(checkout))
        .args(CHECK_IGNORE_BATCH)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn_locked()
    {
        Ok(child) => child,
        Err(_) => return Vec::new(),
    };
    let input = nul_joined(relative);
    let stdin = child.stdin.take();
    // Written while `wait_with_output` drains stdout, never before it: see `git_batch_blocking`.
    let write = async move {
        if let Some(mut stdin) = stdin {
            let _ = stdin.write_all(&input).await;
        }
    };
    let ((), output) = tokio::join!(write, child.wait_with_output());
    match output {
        Ok(output) => nul_fields(&output.stdout),
        Err(_) => Vec::new(),
    }
}

/// [`ignored_by`] for a caller that is not async.
pub fn ignored_by_blocking(checkout: &Path, relative: &[&[u8]]) -> Vec<Vec<u8>> {
    nul_fields(&git_batch_blocking(
        checkout,
        CHECK_IGNORE_BATCH,
        nul_joined(relative),
    ))
}

/// The regular `files` whose exact bytes are already a blob in `checkout`'s repository: content
/// git can reproduce, so deleting the file loses nothing. `--no-filters` hashes the bytes as they
/// are, so a file only a clean filter would turn into a stored blob is never judged held; a path
/// that cannot travel on one line of `--stdin-paths` (it contains a newline) is never judged held.
/// Any disagreement between what was asked and what git answered judges nothing held.
pub fn held_by_repository_blocking(checkout: &Path, files: &[&Path]) -> Vec<PathBuf> {
    let asked: Vec<&Path> = files
        .iter()
        .copied()
        .filter(|path| !path.as_os_str().as_bytes().contains(&b'\n'))
        .collect();
    if asked.is_empty() {
        return Vec::new();
    }
    let mut paths = Vec::new();
    for path in &asked {
        paths.extend_from_slice(path.as_os_str().as_bytes());
        paths.push(b'\n');
    }
    let hashes = git_batch_blocking(
        checkout,
        &["hash-object", "--no-filters", "--stdin-paths"],
        paths,
    );
    let oids: Vec<&[u8]> = hashes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    if oids.len() != asked.len() {
        return Vec::new();
    }
    let mut query = Vec::new();
    for oid in &oids {
        query.extend_from_slice(oid);
        query.push(b'\n');
    }
    let answers = git_batch_blocking(checkout, &["cat-file", "--batch-check"], query);
    let answers: Vec<&[u8]> = answers
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    if answers.len() != oids.len() {
        return Vec::new();
    }
    asked
        .into_iter()
        .zip(oids)
        .zip(answers)
        .filter(|((_, oid), answer)| {
            answer.starts_with(oid)
                && answer
                    .get(oid.len()..)
                    .is_some_and(|rest| rest.starts_with(b" blob "))
        })
        .map(|((path, _), _)| path.to_path_buf())
        .collect()
}

const CHECK_IGNORE_BATCH: &[&str] = &["check-ignore", "--stdin", "-z", "--no-index"];

/// Runs one batch git verb in `checkout` with `input` on stdin and returns its stdout, empty when
/// git cannot be started. The input is written from its own thread while this one drains stdout.
/// Batch verbs answer as they read, so writing all of the input before reading any answer
/// deadlocks as soon as the answers fill the stdout pipe: git blocks writing its stdout, the
/// caller blocks writing git's stdin, and neither returns.
fn git_batch_blocking(checkout: &Path, args: &[&str], input: Vec<u8>) -> Vec<u8> {
    let mut child = match git_command_at(checkout)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn_locked()
    {
        Ok(child) => child,
        Err(_) => return Vec::new(),
    };
    let writer = child.stdin.take().map(|mut stdin| {
        std::thread::spawn(move || {
            let _ = stdin.write_all(&input);
        })
    });
    let output = child.wait_with_output();
    if let Some(writer) = writer {
        let _ = writer.join();
    }
    output.map(|output| output.stdout).unwrap_or_default()
}

fn nul_joined(relative: &[&[u8]]) -> Vec<u8> {
    let mut input = Vec::with_capacity(relative.iter().map(|path| path.len() + 1).sum());
    for path in relative {
        input.extend_from_slice(path);
        input.push(0);
    }
    input
}

fn nul_fields(stdout: &[u8]) -> Vec<Vec<u8>> {
    stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
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

#[cfg(any(target_os = "macos", test))]
/// `git diff` arguments for the rename work digest: no optional locks, no external or textconv
/// filters, binary bytes, and the pathspecs the caller built (`--`, `.`, and literal exclusions).
fn diff_args(cached: bool, pathspecs: &[OsString]) -> Vec<OsString> {
    let mut args: Vec<OsString> = ["--no-optional-locks", "diff"]
        .into_iter()
        .map(OsString::from)
        .collect();
    if cached {
        args.push(OsString::from("--cached"));
    }
    args.extend(
        ["--binary", "--no-ext-diff", "--no-textconv", "--no-color"]
            .into_iter()
            .map(OsString::from),
    );
    args.extend(pathspecs.iter().cloned());
    args
}

#[cfg(any(target_os = "macos", test))]
/// Hash the bytes of untracked non-ignored paths and the presence of restored links. Each path
/// contributes its raw bytes, mode and content (a symlink contributes its target; a regular file
/// is streamed without following links). Any other entry type, or any read failure, refuses.
/// `restored` must be sorted and holds only links Git reports, whose targets are not digested.
fn hash_work_extras(root: &Path, untracked: &[PathBuf], restored: &[PathBuf]) -> Result<[u8; 32]> {
    use sha2::{Digest as _, Sha256};
    use std::os::unix::fs::MetadataExt as _;
    fn absorb(hasher: &mut Sha256, bytes: &[u8]) {
        let length = u64::try_from(bytes.len())
            .expect("a byte slice length fits in u64 on every supported platform");
        hasher.update(length.to_le_bytes());
        hasher.update(bytes);
    }
    let mut hasher = Sha256::new();
    for relative in untracked {
        let path = root.join(relative);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| io_failure("inspect untracked path", &path, error))?;
        absorb(&mut hasher, relative.as_os_str().as_bytes());
        hasher.update(metadata.mode().to_le_bytes());
        let kind = metadata.file_type();
        if kind.is_symlink() {
            hasher.update(b"link");
            let target = fs::read_link(&path)
                .map_err(|error| io_failure("read untracked link target", &path, error))?;
            absorb(&mut hasher, target.as_os_str().as_bytes());
        } else if kind.is_file() {
            hasher.update(b"file");
            let length = metadata.len();
            hasher.update(length.to_le_bytes());
            let mut file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)
                .map_err(|error| io_failure("open untracked file", &path, error))?;
            let mut buffer = [0_u8; 64 * 1024];
            let mut total = 0_u64;
            loop {
                let read = file
                    .read(&mut buffer)
                    .map_err(|error| io_failure("read untracked file", &path, error))?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                total +=
                    u64::try_from(read).expect("a read of at most the 64 KiB buffer fits in u64");
            }
            if total != length {
                return Err(CowshedError::conflict(
                    format!(
                        "untracked file changed while the rename digest read it: {}",
                        path.display()
                    ),
                    "stop writing to the file and retry the rename",
                ));
            }
        } else if kind.is_dir() {
            return Err(CowshedError::conflict(
                format!(
                    "untracked nested repository {} cannot be carried by a rename",
                    relative.display()
                ),
                format!(
                    "commit it, move it out of the workspace, or exclude it, then retry the rename of {}",
                    relative.display()
                ),
            ));
        } else {
            return Err(CowshedError::integrity(
                format!("untracked path has an unsupported type: {}", path.display()),
                "move it out of the workspace and retry the rename",
            ));
        }
    }
    for relative in restored {
        let path = root.join(relative);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(CowshedError::integrity(
                    format!("inherited link is no longer a symlink: {}", path.display()),
                    "restore the inherited links and retry",
                ));
            }
            Err(error) => return Err(io_failure("inspect inherited link", &path, error)),
        }
        absorb(&mut hasher, relative.as_os_str().as_bytes());
        hasher.update(b"restored-link");
    }
    Ok(hasher.finalize().into())
}

/// An untracked path with a hidden component: junk, never work.
/// Whether main's `checkout` holds exactly what this workspace's `root` holds at the untracked
/// `relative` path: a regular file with the same bytes, or a symlink with the same target.
///
/// Anything that cannot be read on either side, or any other kind of entry, answers no, which
/// can only cost a refusal and never a lost file.
fn held_by_checkout(root: &Path, checkout: &Path, relative: &Path) -> bool {
    let ours = root.join(relative);
    let theirs = checkout.join(relative);
    let (Ok(our_kind), Ok(their_kind)) =
        (fs::symlink_metadata(&ours), fs::symlink_metadata(&theirs))
    else {
        return false;
    };
    if our_kind.file_type().is_symlink() && their_kind.file_type().is_symlink() {
        return matches!(
            (fs::read_link(&ours), fs::read_link(&theirs)),
            (Ok(our_target), Ok(their_target)) if our_target == their_target
        );
    }
    our_kind.is_file()
        && their_kind.is_file()
        && our_kind.len() == their_kind.len()
        && same_bytes(&ours, &theirs).unwrap_or(false)
}

/// Byte-for-byte equality of two files already known to be the same length.
fn same_bytes(left: &Path, right: &Path) -> std::io::Result<bool> {
    const CHUNK: usize = 64 * 1024;
    let mut left = File::open(left)?;
    let mut right = File::open(right)?;
    let mut left_chunk = vec![0_u8; CHUNK];
    let mut right_chunk = vec![0_u8; CHUNK];
    loop {
        let read = left.read(&mut left_chunk)?;
        if read == 0 {
            return Ok(right.read(&mut right_chunk)? == 0);
        }
        right.read_exact(&mut right_chunk[..read])?;
        if left_chunk[..read] != right_chunk[..read] {
            return Ok(false);
        }
    }
}

fn is_untracked_junk(status: &[u8], path: &[u8]) -> bool {
    status == b"??" && is_hidden_path(path)
}

/// True when any component of a relative path starts with `.`.
pub fn is_hidden_path(path: &[u8]) -> bool {
    path.split(|byte| *byte == b'/')
        .any(|component| component.first() == Some(&b'.'))
}

/// Remove the linked-worktree registrations a standalone clone inherited from its source
/// (02_workspaces.md, fork mint step 6). Each `.git/worktrees/<id>` directory describes a
/// worktree of the source repository, so removing that whole directory is the unregistration and
/// nothing else. Every entry is judged before the first removal, so a refusal leaves the clone as
/// it was. An absent `worktrees` directory is success, which keeps a resumed mint idempotent.
fn strip_inherited_registrations(root: &Path) -> Result<()> {
    fn inspect(path: &Path) -> Result<fs::Metadata> {
        fs::symlink_metadata(path).map_err(|error| {
            CowshedError::integrity(
                format!("cannot inspect {}: {error}", path.display()),
                "repair the cloned workspace before retrying",
            )
        })
    }
    let git = root.join(".git");
    if !inspect(&git)?.file_type().is_dir() {
        return Err(CowshedError::integrity(
            format!("cloned .git is not a real directory: {}", git.display()),
            "repair the cloned workspace before retrying; the source repository is untouched",
        ));
    }
    let worktrees = git.join("worktrees");
    let metadata = match fs::symlink_metadata(&worktrees) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(CowshedError::integrity(
                format!("cannot inspect {}: {error}", worktrees.display()),
                "repair the cloned workspace before retrying",
            ));
        }
    };
    if !metadata.file_type().is_dir() {
        return Err(CowshedError::integrity(
            format!(
                "cloned worktree registry is not a real directory: {}",
                worktrees.display()
            ),
            "repair the cloned workspace before retrying; cowshed will not follow or delete it",
        ));
    }
    let listing = fs::read_dir(&worktrees).map_err(|error| {
        CowshedError::integrity(
            format!("cannot list {}: {error}", worktrees.display()),
            "repair the cloned workspace before retrying",
        )
    })?;
    let mut admins = Vec::new();
    for entry in listing {
        let entry = entry.map_err(|error| {
            CowshedError::integrity(
                format!("cannot list {}: {error}", worktrees.display()),
                "repair the cloned workspace before retrying",
            )
        })?;
        let admin = entry.path();
        if !inspect(&admin)?.file_type().is_dir() {
            return Err(CowshedError::integrity(
                format!(
                    "cloned worktree registration is not a real directory: {}",
                    admin.display()
                ),
                "remove or repair that entry by hand; cowshed will not delete it",
            ));
        }
        admins.push(admin);
    }
    for admin in admins {
        fs::remove_dir_all(&admin).map_err(|error| {
            CowshedError::integrity(
                format!(
                    "cannot remove inherited worktree registration {}: {error}",
                    admin.display()
                ),
                "retry the workspace; removal is idempotent and the source repository is untouched",
            )
        })?;
    }
    Ok(())
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

/// A registration-step filesystem failure, named by the operation that failed.
fn io_failure(operation: &str, path: &Path, error: std::io::Error) -> CowshedError {
    CowshedError::internal(format!("{operation} at {}: {error}", path.display()))
}

/// Inspect `path` without following links. Absence is `None`; any other failure is an error.
fn inspect_path(path: &Path, operation: &str) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::integrity(
            format!("{operation} at {}: {error}", path.display()),
            "cowshed doctor --json",
        )),
    }
}

/// Copy a linked worktree's administration tree into its clone's, skipping only the root `gitdir`
/// counterpart, so HEAD, index, config and logs travel unchanged. Links are refused rather than
/// followed, and both ends must be real directories.
fn copy_worktree_administration(source: &Path, destination: &Path, root: bool) -> Result<()> {
    const HINT: &str = "finish the journaled cowshed mv; the source is retained";
    let fail = |operation: &str, path: &Path, error: std::io::Error| {
        CowshedError::integrity(format!("{operation} at {}: {error}", path.display()), HINT)
    };
    for path in [source, destination] {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| fail("inspect worktree administration", path, error))?;
        if !metadata.file_type().is_dir() {
            return Err(CowshedError::integrity(
                format!(
                    "worktree administration at {} is not a real directory",
                    path.display()
                ),
                HINT,
            ));
        }
    }
    let entries = fs::read_dir(source)
        .map_err(|error| fail("read worktree administration", source, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| fail("read worktree administration entry", source, error))?;
        let file_name = entry.file_name();
        if root && file_name == "gitdir" {
            continue;
        }
        let path = entry.path();
        let target = destination.join(&file_name);
        let kind = entry
            .file_type()
            .map_err(|error| fail("inspect worktree administration entry", &path, error))?;
        if kind.is_dir() {
            fs::create_dir_all(&target).map_err(|error| {
                fail("create worktree administration directory", &target, error)
            })?;
            copy_worktree_administration(&path, &target, false)?;
        } else if kind.is_file() {
            match fs::symlink_metadata(&target) {
                Ok(existing) if !existing.file_type().is_file() => {
                    return Err(CowshedError::integrity(
                        format!(
                            "worktree administration target {} is not a regular file",
                            target.display()
                        ),
                        HINT,
                    ));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(fail(
                        "inspect worktree administration target",
                        &target,
                        error,
                    ));
                }
            }
            fs::copy(&path, &target)
                .map_err(|error| fail("copy worktree administration file", &path, error))?;
            fs::File::open(&target)
                .and_then(|file| file.sync_all())
                .map_err(|error| fail("sync worktree administration file", &target, error))?;
        } else {
            return Err(CowshedError::integrity(
                format!(
                    "worktree administration entry {} is not a regular file or directory",
                    path.display()
                ),
                HINT,
            ));
        }
    }
    fs::File::open(destination)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| fail("sync worktree administration", destination, error))
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
    use cowshed_os::fork_lock::Run as _;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::symlink;
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus, Output};

    use super::{
        BundleVerificationScratch, CloneOrigin, CowshedUpstream, FALLBACK_MAIN_REMOTE,
        GitRepository, MAIN_REMOTE, MainRemote, RemoteUrl, WorkspaceRepository, ensure_git_success,
        git_ended, git_message, held_by_repository_blocking, ignored_by, ignored_by_blocking,
        is_git_repository, parse_lines, workspace_remote_name,
    };

    #[cfg(target_os = "macos")]
    #[test]
    fn a_worktree_pointer_cannot_grant_a_foreign_repository_write_access() {
        let main = repository();
        let workspace = main.with_extension("candidate");
        fs::create_dir_all(&workspace).expect("workspace");
        let admin = main.join(".git/worktrees/candidate");
        fs::create_dir_all(&admin).expect("registration");
        fs::write(
            workspace.join(".git"),
            format!("gitdir: {}\n", admin.display()),
        )
        .expect("writable pointer");
        fs::write(
            admin.join("gitdir"),
            main.join(".git").display().to_string(),
        )
        .expect("foreign registration");
        let workspace = workspace.canonicalize().expect("canonical workspace");
        assert!(super::registered_worktree_git_dir(&workspace).is_err());
        fs::write(
            admin.join("gitdir"),
            workspace.join(".git").display().to_string(),
        )
        .expect("real registration");
        assert_eq!(
            super::registered_worktree_git_dir(&workspace).expect("registered"),
            Some(
                main.join(".git")
                    .canonicalize()
                    .expect("common Git directory")
            ),
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn fetching_main_outside_the_workspace_tree_needs_an_explicit_read_root() {
        let main = repository();
        let parent = main.join("units");
        fs::create_dir_all(&parent).expect("units");
        let workspace = parent.join("raven");
        let cloned = Command::new("git")
            .args(["clone", "-q"])
            .arg(&main)
            .arg(&workspace)
            .status_locked()
            .expect("clone workspace");
        assert!(cloned.success());
        let main = main.canonicalize().expect("canonical main");
        let workspace = workspace.canonicalize().expect("canonical workspace");
        let source = main.to_str().expect("UTF-8 fixture");
        let denied = super::sandboxed_git_command_at(&workspace)
            .expect("narrow Git")
            .args(["fetch", "--no-tags", source])
            .output_locked()
            .expect("sandboxed fetch");
        assert!(
            !denied.status.success(),
            "a repository URL cannot grant its own read"
        );
        let admitted = super::sandboxed_git_fetch_command(&workspace, &main)
            .expect("trusted read grant")
            .args(["fetch", "--no-tags", source])
            .output_locked()
            .expect("sandboxed fetch");
        assert!(
            admitted.status.success(),
            "trusted source fetch: {}",
            String::from_utf8_lossy(&admitted.stderr)
        );
    }

    /// Main's config is a linked worktree's config, and a file it includes by a path relative to
    /// main's `.git` is in main's working tree, which the worktree's sandboxed Git cannot
    /// otherwise read: every controller verb on a git-worktree workspace failed at its first Git
    /// command with "unable to access …/.git/../tooling/workspace.gitconfig".
    #[cfg(target_os = "macos")]
    #[test]
    fn a_linked_worktree_reads_the_files_its_common_config_includes() {
        let main = repository();
        fs::create_dir_all(main.join("tooling")).expect("tooling");
        fs::write(
            main.join("tooling/workspace.gitconfig"),
            "[core]\n\tabbrev = 12\n",
        )
        .expect("included config");
        git(
            &main,
            &["config", "include.path", "../tooling/workspace.gitconfig"],
        );
        let workspace = main.with_extension("linked");
        let added = Command::new("git")
            .arg("-C")
            .arg(&*main)
            .args(["worktree", "add", "-q", "--detach"])
            .arg(&workspace)
            .status_locked()
            .expect("add linked worktree");
        assert!(added.success());
        let workspace = workspace.canonicalize().expect("canonical workspace");
        let output = super::sandboxed_git_command_at(&workspace)
            .expect("narrow Git")
            .args(["config", "core.abbrev"])
            .output_locked()
            .expect("sandboxed Git");
        assert!(
            output.status.success(),
            "the include is readable: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"12\n");
        let head = super::sandboxed_git_command_at(&workspace)
            .expect("narrow Git")
            .args(["rev-parse", "HEAD"])
            .output_locked()
            .expect("sandboxed Git");
        assert!(
            head.status.success(),
            "HEAD reads: {}",
            String::from_utf8_lossy(&head.stderr)
        );
    }

    /// Include paths are read with Git's own syntax, and only from include sections.
    #[cfg(target_os = "macos")]
    #[test]
    fn config_include_paths_follow_git_syntax() {
        let config = "[core]\n\tpath = not-an-include\n[include]\n\tpath = ../tooling/a.gitconfig ; comment\n[Include] PATH = \"b c.gitconfig\"\n[includeIf \"gitdir:~/Dev/]x\"]\n\tpath = ~/work.gitconfig\n\tother = d\n[user]\n\tname = x\n";
        assert_eq!(
            super::config_include_paths(config),
            [
                "../tooling/a.gitconfig",
                "b c.gitconfig",
                "~/work.gitconfig"
            ]
        );
    }

    /// A git-worktree workspace whose main config includes each of `includes`, and main.
    #[cfg(target_os = "macos")]
    fn linked_worktree(includes: &[&str]) -> (Repository, PathBuf) {
        let main = repository();
        for include in includes {
            git(&main, &["config", "--add", "include.path", include]);
        }
        let workspace = main.with_extension("linked");
        let added = Command::new("git")
            .arg("-C")
            .arg(&*main)
            .args(["worktree", "add", "-q", "--detach"])
            .arg(&workspace)
            .status_locked()
            .expect("add linked worktree");
        assert!(added.success());
        let workspace = workspace.canonicalize().expect("canonical workspace");
        (main, workspace)
    }

    #[cfg(target_os = "macos")]
    fn sandboxed_config(workspace: &Path, key: &str) -> String {
        let output = super::sandboxed_git_command_at(workspace)
            .expect("narrow Git")
            .args(["config", key])
            .output_locked()
            .expect("sandboxed Git");
        assert!(
            output.status.success(),
            "{key}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("UTF-8 value")
    }

    /// `~/` is the HOME the sandboxed Git runs with, inside the workspace; a conditional include
    /// inside main's checkout is followed and its own relative include is granted too.
    #[cfg(target_os = "macos")]
    #[test]
    fn linked_worktree_includes_resolve_home_conditions_and_nesting() {
        let (main, workspace) = linked_worktree(&["~/home.gitconfig"]);
        git(
            &main,
            &[
                "config",
                "includeIf.gitdir:/.path",
                "../tooling/outer.gitconfig",
            ],
        );
        fs::create_dir_all(main.join("tooling")).expect("tooling");
        fs::write(
            main.join("tooling/outer.gitconfig"),
            "[include]\n\tpath = inner.gitconfig\n",
        )
        .expect("outer");
        fs::write(
            main.join("tooling/inner.gitconfig"),
            "[core]\n\tabbrev = 11\n",
        )
        .expect("inner");
        fs::create_dir_all(workspace.join(".cowshed/home")).expect("git home");
        fs::write(
            workspace.join(".cowshed/home/home.gitconfig"),
            "[user]\n\tname = Home Include\n",
        )
        .expect("home include");
        assert_eq!(sandboxed_config(&workspace, "core.abbrev"), "11\n");
        assert_eq!(sandboxed_config(&workspace, "user.name"), "Home Include\n");
    }

    /// An include outside main's checkout stays denied: the controller refuses it by name rather
    /// than granting it, and never opens a device or a FIFO a job named.
    #[cfg(target_os = "macos")]
    #[test]
    fn linked_worktree_includes_outside_main_or_not_regular_are_refused() {
        let (main, workspace) = linked_worktree(&[]);
        // Written by file, from outside the repository: Git opening main would read the include.
        let config = main.join(".git/config");
        let include = |target: &str| {
            let status = Command::new("git")
                .args(["config", "--file"])
                .arg(&config)
                .args(["include.path", target])
                .current_dir("/")
                .status_locked()
                .expect("git config --file");
            assert!(status.success());
        };
        let outside = main.with_extension("outside.gitconfig");
        fs::write(&outside, "[core]\n\tabbrev = 13\n").expect("outside include");
        for target in [outside.to_str().expect("UTF-8 path"), "/dev/zero"] {
            include(target);
            let refused = super::sandboxed_git_command_at(&workspace)
                .expect_err("an include outside main is refused");
            assert!(
                refused.message.contains("outside main's checkout"),
                "{}",
                refused.message
            );
        }
        let fifo = main.join("tooling.fifo");
        let made = Command::new("mkfifo")
            .arg(&fifo)
            .status_locked()
            .expect("mkfifo");
        assert!(made.success());
        include("../tooling.fifo");
        let refused = super::sandboxed_git_command_at(&workspace)
            .expect_err("a FIFO include is refused without blocking");
        assert!(
            refused.message.contains("is not a regular file"),
            "{}",
            refused.message
        );
    }

    /// The config a workspace's jobs write cannot run a filter under the controller's read of
    /// main's checkout: the same `git add` that runs the clean filter unconfined passes the file
    /// through unchanged under the controller's Git.
    #[cfg(target_os = "macos")]
    #[test]
    fn linked_worktree_controller_git_runs_no_repository_filter() {
        let (main, workspace) = linked_worktree(&[]);
        git(
            &main,
            &[
                "config",
                "filter.marker.clean",
                "sh -c 'echo ran >> .filter-ran; cat'",
            ],
        );
        fs::write(workspace.join(".gitattributes"), "README filter=marker\n").expect("attributes");
        fs::write(workspace.join("README"), "changed\n").expect("change README");
        let added = super::sandboxed_git_command_at(&workspace)
            .expect("narrow Git")
            .args(["add", "README"])
            .output_locked()
            .expect("sandboxed add");
        assert!(
            added.status.success(),
            "{}",
            String::from_utf8_lossy(&added.stderr)
        );
        assert!(
            !workspace.join(".filter-ran").exists(),
            "the controller's Git ran the repository's clean filter"
        );
        // New content, so the unconfined add cleans the file rather than trusting the index.
        fs::write(workspace.join("README"), "changed again\n").expect("change README again");
        git(&workspace, &["add", "README"]);
        assert!(
            workspace.join(".filter-ran").exists(),
            "unconfined Git runs the filter, so the fixture would have shown it"
        );
    }

    /// A git-worktree workspace whose main config includes a file of main's checkout, so its
    /// controller Git holds that read, with a forged signed commit in main and an executable in
    /// the workspace that records it ran: what a job could leave behind for `rm`'s `git log` or
    /// a land's `git merge` to run.
    #[cfg(target_os = "macos")]
    fn worktree_with_a_planted_program() -> (Repository, PathBuf, String, PathBuf) {
        use cowshed_os::fork_lock::Spawn as _;
        let (main, workspace) = linked_worktree(&["../tooling/workspace.gitconfig"]);
        fs::create_dir_all(main.join("tooling")).expect("tooling");
        fs::write(
            main.join("tooling/workspace.gitconfig"),
            "[core]\n\tabbrev = 12\n",
        )
        .expect("included config");
        let marker = workspace.join(".program-ran");
        let program = workspace.join("planted-gpg");
        fs::write(
            &program,
            format!("#!/bin/sh\necho ran >> '{}'\nexit 1\n", marker.display()),
        )
        .expect("planted program");
        fs::set_permissions(
            &program,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("executable");
        let rev = |spec: &str| {
            let output = Command::new("git")
                .arg("-C")
                .arg(&*main)
                .args(["rev-parse", spec])
                .output_locked()
                .expect("rev-parse");
            String::from_utf8(output.stdout)
                .expect("oid")
                .trim()
                .to_owned()
        };
        let body = format!(
            "tree {}\nparent {}\nauthor Forger <f@example.invalid> 0 +0000\ncommitter Forger <f@example.invalid> 0 +0000\ngpgsig -----BEGIN PGP SIGNATURE-----\n \n forged\n -----END PGP SIGNATURE-----\n\nforged\n",
            rev("HEAD^{tree}"),
            rev("HEAD")
        );
        let mut hash = Command::new("git")
            .arg("-C")
            .arg(&*main)
            .args(["hash-object", "-t", "commit", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .expect("hash-object");
        std::io::Write::write_all(&mut hash.stdin.take().expect("stdin"), body.as_bytes())
            .expect("write commit");
        let output = hash.wait_with_output().expect("hash-object output");
        assert!(output.status.success());
        let forged = String::from_utf8(output.stdout)
            .expect("oid")
            .trim()
            .to_owned();
        git(
            &main,
            &["config", "gpg.program", program.to_str().expect("UTF-8")],
        );
        (main, workspace, forged, marker)
    }

    /// `log.showSignature` makes `git log` run `gpg.program` on a signed commit. The controller's
    /// Git for a workspace holding main-tree reads cannot run it: only Git's own executables run.
    #[cfg(target_os = "macos")]
    #[test]
    fn linked_worktree_controller_git_runs_no_gpg_program_for_log() {
        let (main, workspace, forged, marker) = worktree_with_a_planted_program();
        git(&main, &["config", "log.showSignature", "true"]);
        let _ = super::sandboxed_git_command_at(&workspace)
            .expect("narrow Git")
            // Git writes the signature to a temporary file before it runs gpg.program; a TMPDIR
            // the sandbox can write isolates the exec rule as the only thing that stops it.
            .env("TMPDIR", &workspace)
            .args(["log", "-1", "--pretty=format:%H", &forged])
            .output_locked()
            .expect("sandboxed log");
        assert!(!marker.exists(), "the controller's Git ran gpg.program");
        let _ = Command::new("git")
            .arg("-C")
            .arg(&workspace)
            .args(["log", "-1", "--pretty=format:%H", &forged])
            .output_locked()
            .expect("unconfined log");
        assert!(
            marker.exists(),
            "unconfined Git runs it, so the fixture would have shown it"
        );
    }

    /// `merge.verifySignatures` makes `git merge` run `gpg.program` on the commit it would take.
    #[cfg(target_os = "macos")]
    #[test]
    fn linked_worktree_controller_git_runs_no_gpg_program_for_merge() {
        let (main, workspace, forged, marker) = worktree_with_a_planted_program();
        git(&main, &["config", "merge.verifySignatures", "true"]);
        let merged = super::sandboxed_git_command_at(&workspace)
            .expect("narrow Git")
            .env("TMPDIR", &workspace)
            .args(["merge", "--ff-only", &forged])
            .output_locked()
            .expect("sandboxed merge");
        assert!(
            !merged.status.success(),
            "an unverifiable commit is not merged"
        );
        assert!(!marker.exists(), "the controller's Git ran gpg.program");
    }

    /// `rm`'s unlanded-work proof and `ls --landing` read a linked worktree through another
    /// repository's object store (`with_alternate_objects`). That is a read, not a fetch: no shell
    /// runs, so it works under the Git-only profile that main-tree include grants impose.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn an_alternate_object_read_works_under_main_tree_include_grants() {
        let (main, workspace) = linked_worktree(&["../tooling/workspace.gitconfig"]);
        fs::create_dir_all(main.join("tooling")).expect("tooling");
        fs::write(
            main.join("tooling/workspace.gitconfig"),
            "[core]\n\tabbrev = 12\n",
        )
        .expect("included config");
        let other = repository();
        let objects = other.join(".git/objects").canonicalize().expect("objects");
        let held = GitRepository::from_root(&workspace)
            .with_alternate_objects(objects)
            .expect("alternate objects")
            .commits_already_held("HEAD", "HEAD")
            .await
            .expect("an alternate-object read is not a fetch");
        assert_eq!(held, 0);
    }

    /// A driver added after the command was built — by a job of this or a sibling workspace,
    /// between construction and spawn — is named by no override, and still cannot run.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_driver_added_after_construction_cannot_run() {
        let (main, workspace, _, marker) = worktree_with_a_planted_program();
        let mut command = super::sandboxed_git_command_at(&workspace).expect("narrow Git");
        git(
            &main,
            &[
                "config",
                "filter.late.clean",
                workspace.join("planted-gpg").to_str().expect("UTF-8"),
            ],
        );
        fs::write(workspace.join(".gitattributes"), "README filter=late\n").expect("attributes");
        fs::write(workspace.join("README"), "changed\n").expect("change README");
        let _ = command
            .args(["add", "README"])
            .output_locked()
            .expect("sandboxed add");
        assert!(
            !marker.exists(),
            "a late driver ran under the controller's Git"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn driver_overrides_switch_off_every_named_driver() {
        let listing = b"core.abbrev\0filter.lfs.clean\0filter.lfs.smudge\0filter.a.b.process\0merge.ours.driver\0diff.img.textconv\0diff.ext.command\0merge.ours.name\0";
        assert_eq!(
            super::driver_overrides(listing).expect("overrides"),
            [
                "diff.ext.textconv=cat",
                "diff.ext.command=false",
                "diff.img.textconv=cat",
                "diff.img.command=false",
                "filter.a.b.clean=",
                "filter.a.b.smudge=",
                "filter.a.b.process=",
                "filter.a.b.required=false",
                "filter.lfs.clean=",
                "filter.lfs.smudge=",
                "filter.lfs.process=",
                "filter.lfs.required=false",
                "merge.ours.driver=false",
            ]
        );
        assert!(super::driver_overrides(b"filter.a=b.clean\0").is_err());
    }

    /// A committed Git repository inside a private scratch directory. Every tree a test derives
    /// beside it (`with_extension`) lands inside that scratch too, and dropping the fixture
    /// removes the scratch on every exit path: a failed assertion unwinds through `TempRoot`'s
    /// `Drop` exactly as a passing test does.
    struct Repository {
        root: PathBuf,
        _scratch: crate::temp_root::TempRoot,
    }

    impl std::ops::Deref for Repository {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.root
        }
    }

    impl AsRef<Path> for Repository {
        fn as_ref(&self) -> &Path {
            &self.root
        }
    }

    impl AsRef<std::ffi::OsStr> for Repository {
        fn as_ref(&self) -> &std::ffi::OsStr {
            self.root.as_os_str()
        }
    }

    fn repository() -> Repository {
        let scratch = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "cowshed-git-test",
        );
        let fixture = Repository {
            root: scratch.join("repository"),
            _scratch: scratch,
        };
        fs::create_dir_all(&fixture.root).expect("create test repository");
        let root = &fixture.root;
        let status = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(root)
            .status_locked()
            .expect("run git init");
        assert!(status.success());
        fs::write(root.join("README"), "test\n").expect("write fixture");
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
                "add",
                ".",
            ])
            .status_locked()
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
                "initial",
            ])
            .status_locked()
            .expect("run git commit");
        assert!(status.success());
        fixture
    }

    /// What `git status --porcelain=v2 --branch` reports for `root`: the checked-out branch, HEAD's
    /// object ID and every changed or untracked entry, read in one process. `--no-optional-locks`
    /// keeps the read from refreshing, and so rewriting, the index it is judging.
    #[derive(Debug, Eq, PartialEq)]
    struct TreeState {
        branch: String,
        head: String,
        changes: Vec<String>,
    }

    fn tree_state(root: &Path) -> TreeState {
        let report = git_stdout(
            root,
            &[
                "--no-optional-locks",
                "status",
                "--porcelain=v2",
                "--branch",
            ],
        );
        let (mut branch, mut head, mut changes) = (None, None, Vec::new());
        for line in report.lines() {
            if let Some(oid) = line.strip_prefix("# branch.oid ") {
                head = Some(oid.to_owned());
            } else if let Some(name) = line.strip_prefix("# branch.head ") {
                branch = Some(name.to_owned());
            } else if !line.starts_with('#') {
                changes.push(line.to_owned());
            }
        }
        TreeState {
            branch: branch
                .unwrap_or_else(|| panic!("no branch.head for {}: {report}", root.display())),
            head: head.unwrap_or_else(|| panic!("no branch.oid for {}: {report}", root.display())),
            changes,
        }
    }

    #[tokio::test]
    async fn fork_carry_clean_and_ref_preserve_source_bytes_and_index() {
        for linked in [false, true] {
            for session in [false, true] {
                let main = repository();
                fs::write(main.join(".gitignore"), "/ignored/\n").unwrap();
                fs::write(main.join("staged.txt"), "base\n").unwrap();
                git(&main, &["add", "."]);
                git(&main, &["commit", "-qm", "warm baseline"]);
                let baseline = git_stdout(&main, &["rev-parse", "HEAD"]).trim().to_owned();
                // Main moves past the baseline, so `--ref <baseline>` names a commit that is not the
                // source's HEAD in any fixture: a mint that ignored the ref would otherwise pass
                // whenever the two are the same commit.
                fs::write(main.join("README"), "main moved on\n").unwrap();
                git(&main, &["commit", "-qam", "main moves past the baseline"]);
                // What `repository()` committed, and what the source's HEAD holds.
                let baseline_readme = "test\n";
                let committed_readme = if session {
                    "session committed\n"
                } else {
                    "main moved on\n"
                };
                let sibling = main.with_extension("source");
                let source = if session {
                    if linked {
                        git(
                            &main,
                            &[
                                "worktree",
                                "add",
                                "-q",
                                "-b",
                                "source",
                                sibling.to_str().unwrap(),
                            ],
                        );
                    } else {
                        copy_tree(&main, &sibling);
                        git(&sibling, &["switch", "-qc", "source"]);
                    }
                    fs::write(sibling.join("README"), "session committed\n").unwrap();
                    git(&sibling, &["add", "README"]);
                    git(
                        &sibling,
                        &["commit", "-qm", "session head differs from main"],
                    );
                    sibling.as_path()
                } else {
                    &*main
                };
                fs::write(source.join("README"), "source working edit\n").unwrap();
                fs::write(source.join("staged.txt"), "index-only edit\n").unwrap();
                git(source, &["add", "staged.txt"]);
                fs::write(source.join("staged.txt"), "base\n").unwrap();
                fs::write(source.join("stray.txt"), "untracked\n").unwrap();
                fs::create_dir_all(source.join("ignored")).unwrap();
                fs::write(source.join("ignored/warm"), "installed build state\n").unwrap();
                let before = tree_state(source);
                assert_ne!(
                    before.head, baseline,
                    "the ref must differ from the source's HEAD for the ref cells to discriminate"
                );
                let index_path = git_stdout(source, &["rev-parse", "--git-path", "index"]);
                let index_path = source.join(index_path.trim());
                let index = fs::read(&index_path).unwrap();
                let every = [
                    ("carry", false, None),
                    ("clean", true, None),
                    ("ref", true, Some(baseline.as_str())),
                ];
                // A standalone mint reads its source only for its links and identity and never
                // compares it with main, so from main the clean and ref cells repeat the session
                // fixture's. The carry cell keeps the from-main scenario.
                let choices = if !linked && !session {
                    &every[..1]
                } else {
                    &every[..]
                };
                for &(choice, clean, start) in choices {
                    let name = format!("fork-{linked}-{session}-{choice}");
                    let clone = main.with_extension(&name);
                    copy_tree(source, &clone);
                    GitRepository::from_root(&clone)
                        .mint_workspace(
                            &name,
                            CloneOrigin {
                                source,
                                main: &main,
                            },
                            if linked {
                                WorkspaceRepository::LinkedWorktree
                            } else {
                                WorkspaceRepository::Standalone
                            },
                            start,
                            clean,
                            false,
                        )
                        .await
                        .unwrap_or_else(|error| panic!("{name}: {error}"));
                    let forked = tree_state(&clone);
                    assert_eq!(forked.branch, format!("cowshed/{name}"));
                    assert_eq!(
                        forked.head,
                        start.unwrap_or(before.head.as_str()),
                        "{name}: HEAD is the ref, or the source's head when none is given"
                    );
                    assert_eq!(
                        fs::read_to_string(clone.join("ignored/warm")).unwrap(),
                        "installed build state\n"
                    );
                    assert_eq!(
                        clone.join("stray.txt").exists(),
                        !clean,
                        "{name}: the untracked file survives exactly when the fork is not clean"
                    );
                    if !clean {
                        assert_eq!(
                            fs::read_to_string(clone.join("README")).unwrap(),
                            "source working edit\n"
                        );
                        let staged = git_stdout(&clone, &["show", ":staged.txt"]);
                        assert_eq!(
                            staged,
                            if linked {
                                "base\n"
                            } else {
                                "index-only edit\n"
                            }
                        );
                    } else {
                        assert!(
                            forked.changes.is_empty(),
                            "{name}: clean leaves nothing to commit: {:?}",
                            forked.changes
                        );
                        // The files are the selected commit's: `ref` reverts them to the baseline's,
                        // `clean` keeps the source's head.
                        assert_eq!(
                            fs::read_to_string(clone.join("README")).unwrap(),
                            if start.is_some() {
                                baseline_readme
                            } else {
                                committed_readme
                            },
                            "{name}: README is the selected commit's"
                        );
                    }
                    assert_eq!(
                        tree_state(source),
                        before,
                        "{name}: the source's branch, HEAD and status never change"
                    );
                    assert_eq!(
                        fs::read(&index_path).unwrap(),
                        index,
                        "source index must never be reset"
                    );
                    assert_eq!(
                        fs::read_to_string(source.join("README")).unwrap(),
                        "source working edit\n"
                    );
                }
            }
        }
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
        fs::write(root.join("README"), "test\n").expect("restore tracked");

        // Bench output main learned to ignore after this clone was cut: junk by main's rules,
        // work by the clone's own frozen ignore file.
        let main = repository();
        fs::write(main.join(".gitignore"), "/bench/results/\n").expect("main ignore");
        fs::create_dir_all(root.join("bench/results/run-1")).expect("bench dir");
        fs::write(root.join("bench/results/run-1/out.txt"), "1\n").expect("bench output");
        assert!(git.is_dirty().await.expect("own rules"));
        assert!(!git.is_dirty_by(Some(&main)).await.expect("main's rules"));
    }

    #[tokio::test]
    async fn an_untracked_path_main_holds_identically_was_inherited_not_written() {
        let root = repository();
        let main = repository();
        let git = GitRepository::from_root(&root);
        for tree in [&root, &main] {
            fs::create_dir_all(tree.join("tooling")).expect("tooling dir");
            fs::write(tree.join("tooling/prune.ts"), "export {};\n").expect("script");
            symlink("../README", tree.join("tooling/readme")).expect("link");
        }
        assert!(
            git.is_dirty().await.expect("own reading"),
            "with no main to compare against, an untracked file is work"
        );
        assert!(
            !git.is_dirty_by(Some(&main)).await.expect("main holds both"),
            "a file and a link main holds identically came with the clone"
        );

        // Same length, different bytes: written in the workspace.
        fs::write(root.join("tooling/prune.ts"), "export {}\n;").expect("edit");
        assert!(git.is_dirty_by(Some(&main)).await.expect("edited file"));
        fs::write(root.join("tooling/prune.ts"), "export {};\n").expect("restore");

        // A link repointed in the workspace.
        fs::remove_file(root.join("tooling/readme")).expect("unlink");
        symlink("../notes", root.join("tooling/readme")).expect("relink");
        assert!(git.is_dirty_by(Some(&main)).await.expect("repointed link"));
        fs::remove_file(root.join("tooling/readme")).expect("unlink");
        symlink("../README", root.join("tooling/readme")).expect("restore link");

        // A file main does not have.
        fs::write(root.join("notes.rs"), "fn main() {}\n").expect("new file");
        assert!(git.is_dirty_by(Some(&main)).await.expect("new file"));
        // What land's refusal names: the tracked change and the file main lacks, never the paths
        // main holds identically.
        fs::write(root.join("README"), "edited\n").expect("tracked edit");
        assert_eq!(
            git.dirty_paths_by(Some(&main)).await.expect("dirty paths"),
            [PathBuf::from("README"), PathBuf::from("notes.rs")]
        );
        assert_eq!(
            git.tracked_change_paths().await.expect("tracked changes"),
            [PathBuf::from("README")],
            "what blocks a rebase is the tracked change alone"
        );
    }

    /// Enough ignored paths that git's answers overflow the stdout pipe many times over.
    fn pipe_overflowing_paths() -> Vec<Vec<u8>> {
        (0..40_000)
            .map(|index| format!("node_modules/pkg-{index}/lib/index.js").into_bytes())
            .collect()
    }

    /// Writing every path before reading any answer deadlocked once git's answers filled the
    /// stdout pipe (a retired worktree's `node_modules` was enough): both sides blocked writing.
    /// A regression must fail this test, not hang it, so the batch runs under a deadline.
    #[test]
    fn a_check_ignore_batch_larger_than_the_pipe_finishes() {
        let root = repository();
        fs::write(root.join(".gitignore"), "node_modules/\n").expect("ignore rules");
        let paths = pipe_overflowing_paths();
        let (sender, receiver) = std::sync::mpsc::channel();
        let checkout = root.to_path_buf();
        let asked = paths.clone();
        std::thread::spawn(move || {
            let borrowed: Vec<&[u8]> = asked.iter().map(Vec::as_slice).collect();
            let _ = sender.send(ignored_by_blocking(&checkout, &borrowed));
        });
        let ignored = receiver
            .recv_timeout(std::time::Duration::from_secs(120))
            .expect("check-ignore batch deadlocked");
        assert_eq!(ignored, paths);
    }

    #[tokio::test]
    async fn an_async_check_ignore_batch_larger_than_the_pipe_finishes() {
        let root = repository();
        fs::write(root.join(".gitignore"), "node_modules/\n").expect("ignore rules");
        let paths = pipe_overflowing_paths();
        let borrowed: Vec<&[u8]> = paths.iter().map(Vec::as_slice).collect();
        let ignored = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            ignored_by(&root, &borrowed),
        )
        .await
        .expect("check-ignore batch deadlocked");
        assert_eq!(ignored, paths);
    }

    /// Only a regular file whose exact bytes the repository already stores is held; a
    /// same-named file with other bytes, and a path that cannot travel on one line, are not.
    #[test]
    fn held_files_are_exactly_the_ones_git_already_stores() {
        let root = repository();
        let outside = repository();
        let copy = outside.join("copy-of-readme");
        fs::write(&copy, "test\n").expect("held copy");
        let edited = outside.join("README.edited");
        fs::write(&edited, "test, edited\n").expect("unheld edit");
        let newline = outside.join("two\nlines");
        fs::write(&newline, "test\n").expect("held bytes, unsendable name");
        let held = held_by_repository_blocking(&root, &[&copy, &edited, &newline]);
        assert_eq!(held, vec![copy]);
        assert!(held_by_repository_blocking(&root, &[]).is_empty());
    }

    #[tokio::test]
    async fn minting_without_direnv_leaves_repository_hooks_absent() {
        let root = repository();
        let workspace = root.with_extension("plain-workspace");
        copy_tree(&root, &workspace);
        let repository = GitRepository::from_root(&workspace);
        repository
            .ensure_cowshed_excludes()
            .await
            .expect("private metadata exclusion");
        repository
            .mint_workspace(
                "plain",
                CloneOrigin {
                    source: &root,
                    main: &root,
                },
                WorkspaceRepository::Standalone,
                None,
                false,
                false,
            )
            .await
            .expect("a plain Git repository requires no shell configuration");
        for directory in [&*root, workspace.as_path()] {
            assert!(!directory.join(".envrc").exists());
            assert!(!directory.join(".envrc-local").exists());
            assert!(!directory.join(".gitignore").exists());
            assert_eq!(git_stdout(directory, &["status", "--porcelain"]), "");
        }
    }

    /// A clone of main starts at main's committed `HEAD`: main's tracked edits, deletions and
    /// untracked files stay in main and never reach the workspace, while what Git ignores (build
    /// outputs, installed dependencies) arrives warm. A workspace that inherited main's dirt
    /// gated a tree that was not its commit, and its land then refused it as dirty.
    #[tokio::test]
    async fn a_clone_of_main_starts_at_mains_committed_head() {
        let root = repository();
        fs::write(root.join(".gitignore"), "/ignored/\n").expect("ignore rules");
        fs::write(root.join("kept.txt"), "committed\n").expect("tracked file");
        fs::write(root.join("warm.rs"), "pub const WARM: bool = true;\n")
            .expect("unchanged source");
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "ignore build output"]);
        fs::write(root.join("README"), "edited in main\n").expect("tracked edit");
        fs::remove_file(root.join("kept.txt")).expect("tracked deletion");
        fs::write(root.join("stray.ts"), "export const stray = 1;\n").expect("untracked file");
        fs::create_dir_all(root.join("notes/deep")).expect("untracked directory");
        fs::write(root.join("notes/deep/draft.md"), "draft\n").expect("untracked nested file");
        fs::create_dir_all(root.join("ignored")).expect("ignored directory");
        fs::write(root.join("ignored/build.out"), "warm\n").expect("ignored output");
        let main_status = git_stdout(&root, &["status", "--porcelain"]);
        let workspace = root.with_extension("clean-workspace");
        copy_tree(&root, &workspace);
        let warm_time = fs::metadata(root.join("warm.rs"))
            .unwrap()
            .modified()
            .unwrap();
        fs::File::open(workspace.join("warm.rs"))
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(warm_time))
            .unwrap();
        let repository = GitRepository::from_root(&workspace);
        repository
            .ensure_cowshed_excludes()
            .await
            .expect("private metadata exclusion");
        repository
            .mint_workspace(
                "clean",
                CloneOrigin {
                    source: &root,
                    main: &root,
                },
                WorkspaceRepository::Standalone,
                None,
                true,
                false,
            )
            .await
            .expect("mint from a dirty main");
        assert_eq!(git_stdout(&workspace, &["status", "--porcelain"]), "");
        assert_eq!(
            fs::read_to_string(workspace.join("README")).unwrap(),
            "test\n"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("kept.txt")).unwrap(),
            "committed\n"
        );
        assert_eq!(
            fs::metadata(workspace.join("warm.rs"))
                .unwrap()
                .modified()
                .unwrap(),
            warm_time,
            "discarding main's edits must not rewrite an unchanged file with copied index stat data"
        );
        assert!(!workspace.join("stray.ts").exists());
        assert!(!workspace.join("notes").exists());
        assert_eq!(
            fs::read_to_string(workspace.join("ignored/build.out")).unwrap(),
            "warm\n",
            "ignored build output stays warm"
        );
        assert_eq!(
            git_stdout(&root, &["status", "--porcelain"]),
            main_status,
            "main keeps its own edits"
        );
    }

    /// macOS keeps its own bookkeeping at every volume's root, and a clone's checkout is its
    /// volume's root: a fresh mount grew a root-owned `.Trashes` nobody else may empty, and the
    /// clone's cleanup of main's untracked files failed on it ('failed to delete main's untracked
    /// files in the clone: .Trashes/ Permission denied'), leaving the create unfinished. The
    /// cleanup leaves the volume's bookkeeping alone, even under an exclude file main wrote
    /// before it named all of it, and the clone's dirty checks ignore it.
    #[tokio::test]
    async fn a_clone_of_main_leaves_the_volumes_own_bookkeeping_alone() {
        use std::os::unix::fs::PermissionsExt;
        let root = repository();
        let workspace = root.with_extension("bookkeeping-workspace");
        copy_tree(&root, &workspace);
        fs::write(
            workspace.join(".git/info/exclude"),
            ".cowshed/\n.fseventsd/\n",
        )
        .expect("main's earlier excludes");
        let trashes = workspace.join(".Trashes");
        fs::create_dir(&trashes).expect("the volume's trash");
        fs::write(trashes.join("501"), "").expect("an entry in the volume's trash");
        /// The trash made this user's to empty again however the test ends, so its fixture
        /// can be removed.
        struct Emptiable<'a>(&'a Path);
        impl Drop for Emptiable<'_> {
            fn drop(&mut self) {
                let _ = fs::set_permissions(self.0, fs::Permissions::from_mode(0o700));
            }
        }
        // Not this user's to empty, as root's is not.
        fs::set_permissions(&trashes, fs::Permissions::from_mode(0o500)).expect("read-only trash");
        let _emptiable = Emptiable(&trashes);
        let repository = GitRepository::from_root(&workspace);
        repository
            .mint_workspace(
                "bookkeeping",
                CloneOrigin {
                    source: &root,
                    main: &root,
                },
                WorkspaceRepository::Standalone,
                None,
                false,
                false,
            )
            .await
            .expect("mint beside the volume's bookkeeping");
        assert_eq!(git_stdout(&workspace, &["status", "--porcelain"]), "");
        assert!(
            trashes.join("501").exists(),
            "the volume's trash stays as macOS made it"
        );
    }

    #[tokio::test]
    async fn minting_preserves_project_shell_hooks_without_a_managed_loader() {
        for linked in [false, true] {
            let root = repository();
            let hook = if linked {
                fs::create_dir(root.join("shell")).expect("shell directory");
                let hook = root.join("shell/envrc");
                symlink("shell/envrc", root.join(".envrc")).expect("relative shell hook");
                hook
            } else {
                root.join(".envrc")
            };
            let contents = b"export PROJECT_VALUE=project-owned\n";
            fs::write(&hook, contents).expect("project hook");
            git(&root, &["add", "."]);
            git(&root, &["commit", "-qm", "project shell hook"]);
            let workspace = root.with_extension("hook-workspace");
            copy_tree(&root, &workspace);
            let repository = GitRepository::from_root(&workspace);
            repository
                .ensure_cowshed_excludes()
                .await
                .expect("private metadata exclusion");
            repository
                .mint_workspace(
                    "hook",
                    CloneOrigin {
                        source: &root,
                        main: &root,
                    },
                    WorkspaceRepository::Standalone,
                    None,
                    false,
                    false,
                )
                .await
                .expect("a project hook needs no cowshed loader");
            assert_eq!(fs::read(workspace.join(".envrc")).unwrap(), contents);
            if linked {
                assert_eq!(
                    fs::read_link(workspace.join(".envrc")).unwrap(),
                    Path::new("shell/envrc")
                );
            }
            assert!(!workspace.join(".envrc-local").exists());
            assert_eq!(git_stdout(&workspace, &["status", "--porcelain"]), "");
        }
    }

    #[tokio::test]
    async fn detached_head_has_no_current_branch() {
        let root = repository();
        let status = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["switch", "--detach", "--quiet", "HEAD"])
            .status_locked()
            .expect("detach HEAD");
        assert!(status.success());

        let branch = GitRepository::from_root(&root)
            .current_branch()
            .await
            .expect("detached HEAD is not an error");
        assert_eq!(branch, None);
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

    #[test]
    fn a_failed_git_is_named_by_how_it_ended() {
        let signalled = Output {
            status: ExitStatus::from_raw(libc::SIGTERM),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        assert_eq!(git_ended(&signalled), "git signal: 15 (SIGTERM)");
        assert_eq!(git_ended(&command_output(9, b"")), "git exit status: 9");
        assert_eq!(
            git_ended(&command_output(9, b" \n\t")),
            "git exit status: 9"
        );
        assert_eq!(
            git_ended(&command_output(128, b"fatal: index file corrupt\n")),
            "git exit status: 128: fatal: index file corrupt"
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
            b"user-pattern\n.cowshed/\n.DocumentRevisions-V100/\n.Spotlight-V100/\n.TemporaryItems/\n.Trashes/\n.fseventsd/\n/.metadata_never_index\n"
        );
        fs::create_dir(root.join(".cowshed")).expect("runtime metadata");
        fs::write(root.join(".cowshed/state"), b"").expect("runtime state");
        for metadata in cowshed_apfs::VOLUME_METADATA {
            fs::create_dir(root.join(metadata)).expect("APFS metadata");
            fs::write(root.join(metadata).join("entry"), b"").expect("APFS metadata entry");
        }
        fs::write(root.join(cowshed_apfs::SPOTLIGHT_OPT_OUT), b"").expect("Spotlight opt-out");
        assert!(!repo.is_dirty().await.expect("runtime state is ignored"));
        // Ignored by the exclude itself, not merely as hidden junk, so `git add -A` leaves it,
        // and anchored at the root: a same-named file deeper in the tree is the repository's.
        fs::create_dir(root.join("nested")).expect("nested directory");
        fs::write(
            root.join("nested").join(cowshed_apfs::SPOTLIGHT_OPT_OUT),
            b"",
        )
        .expect("nested");
        let ignored = |path: &str| {
            Command::new("git")
                .arg("-C")
                .arg(&root)
                .args(["check-ignore", "-q", "--no-index", path])
                .status_locked()
                .expect("git check-ignore")
                .success()
        };
        assert!(ignored(cowshed_apfs::SPOTLIGHT_OPT_OUT));
        assert!(!ignored(&format!(
            "nested/{}",
            cowshed_apfs::SPOTLIGHT_OPT_OUT
        )));
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
    }

    /// A registered linked worktree's `.git` is a pointer, not the repository's authority, so the
    /// standalone exclude writer must refuse it even though git itself accepts the registration.
    /// Main's exclude file has to read exactly as it did before the refusal.
    #[tokio::test]
    async fn cowshed_exclude_wiring_refuses_a_linked_pointer_root_and_leaves_main_untouched() {
        let main = repository();
        let scratch = main.parent().expect("scratch root").to_path_buf();
        let pointer_root = scratch.join("raven");
        copy_tree(&main, &pointer_root);
        GitRepository::from_root(&pointer_root)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect("adopt raven as a linked worktree");
        assert!(
            pointer_root.join(".git").is_file(),
            "fixture root must be a pointer"
        );

        let main_exclude = main.join(".git/info/exclude");
        let before = fs::read(&main_exclude).expect("main exclude bytes");

        let error = GitRepository::from_root(&pointer_root)
            .ensure_cowshed_excludes()
            .await
            .expect_err("a pointer root is not the exclude authority");
        assert_eq!(error.code.as_str(), "integrity");
        assert_eq!(
            fs::read(&main_exclude).expect("main exclude after refusal"),
            before
        );
    }

    /// Cached and untracked bytes are workspace work a rename must preserve. A digest of the
    /// porcelain status alone reads the same before and after both change, so it cannot prove
    /// the preserved state is the one that was captured.
    #[tokio::test]
    async fn rename_work_digest_changes_when_dirty_and_untracked_bytes_change() {
        let root = repository();
        let repo = GitRepository::from_root(&root);

        fs::write(root.join("README"), "stage1\n").expect("stage README");
        git(&root, &["add", "README"]);
        fs::write(root.join("README"), "two\n").expect("unstaged README");
        fs::write(root.join("u"), "u1\n").expect("untracked file");
        let state1 = repo
            .rename_work_state(false, &[])
            .await
            .expect("first work state");

        fs::write(root.join("README"), "stage2\n").expect("restage README");
        git(&root, &["add", "README"]);
        fs::write(root.join("README"), "five\n").expect("unstaged README again");
        fs::write(root.join("u"), "u2\n").expect("untracked file rewritten");
        let state2 = repo
            .rename_work_state(false, &[])
            .await
            .expect("second work state");

        assert_eq!(
            git_stdout(&root, &["status", "--porcelain=v1"]),
            "MM README\n?? u\n",
            "fixture keeps the same status codes"
        );
        assert_ne!(
            state2, state1,
            "a rename work digest must change when cached or untracked bytes change"
        );
    }

    /// An untracked nested repository is Git's `??dir/` entry. Its work is not something a rename
    /// can carry by path, so the digest refuses it before any capture, naming it and the way out.
    #[tokio::test]
    async fn rename_work_refuses_an_untracked_nested_repository() {
        let root = repository();
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("nested directory");
        git(&nested, &["init", "-q"]);
        fs::write(nested.join("tracked.txt"), "nested\n").expect("nested file");
        git(&nested, &["add", "tracked.txt"]);
        git(&nested, &["commit", "-qm", "nested"]);

        let status = git_stdout(
            &root,
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        );
        assert!(
            status.contains("?? nested/\0"),
            "premise: Git lists the nested repository as an untracked directory: {status:?}"
        );
        let before_index = fs::read(root.join(".git/index")).expect("source index");
        let before_nested = fs::read(nested.join("tracked.txt")).expect("nested bytes");

        let error = GitRepository::from_root(&root)
            .rename_work_state(false, &[])
            .await
            .expect_err("an untracked nested repository must refuse before capture");
        assert_eq!(error.code.as_str(), "conflict", "{}", error.message);
        assert!(error.message.contains("nested"), "{}", error.message);
        for word in ["commit", "move", "exclude", "retry"] {
            assert!(
                error.hint.contains(word),
                "hint names {word}: {}",
                error.hint
            );
        }
        assert_eq!(
            fs::read(root.join(".git/index")).expect("source index"),
            before_index
        );
        assert_eq!(
            fs::read(nested.join("tracked.txt")).expect("nested bytes after refusal"),
            before_nested,
            "nested work is untouched"
        );
    }

    /// An untracked symlink's target bytes are work, so retargeting it changes the digest.
    #[tokio::test]
    async fn rename_work_digest_covers_untracked_symlink_targets() {
        let root = repository();
        let repo = GitRepository::from_root(&root);
        symlink("a", root.join("ln")).expect("untracked link");
        let first = repo
            .rename_work_state(false, &[])
            .await
            .expect("first state");
        fs::remove_file(root.join("ln")).expect("replace link");
        symlink("b", root.join("ln")).expect("retargeted link");
        let second = repo
            .rename_work_state(false, &[])
            .await
            .expect("second state");
        assert_ne!(
            second, first,
            "an untracked symlink's target is work a rename must preserve"
        );
    }

    /// The branch header is part of the digest: a switch at the same HEAD changes it.
    #[tokio::test]
    async fn rename_work_digest_covers_the_branch_header() {
        let root = repository();
        let repo = GitRepository::from_root(&root);
        let first = repo
            .rename_work_state(false, &[])
            .await
            .expect("main state");
        git(&root, &["checkout", "-q", "-b", "other"]);
        let second = repo
            .rename_work_state(false, &[])
            .await
            .expect("other state");
        assert_ne!(
            second, first,
            "a branch switch at the same HEAD must change the digest"
        );
    }

    /// Restored inherited links compare by path and presence, not target, on two copied trees at
    /// different depths; the branch survives restoration.
    #[tokio::test]
    async fn restored_inherited_links_compare_by_path_and_branch_survives_restoration() {
        let source = repository();
        let scratch = source.parent().expect("scratch root").to_path_buf();
        fs::create_dir_all(source.join("deps")).expect("deps directory");
        fs::write(scratch.join("shared"), "shared\n").expect("source-side target");
        symlink("../../shared", source.join("deps/tracked")).expect("tracked link");
        git(&source, &["add", "deps/tracked"]);
        git(&source, &["commit", "-qm", "link"]);
        symlink("../../shared", source.join("deps/untracked")).expect("untracked link");

        let clone = scratch.join("nested").join("deep").join("clone");
        fs::create_dir_all(clone.parent().expect("clone parent")).expect("clone parent");
        copy_tree(&source, &clone);
        let repo = GitRepository::from_root(&clone);
        let branch = repo.current_branch().await.expect("clone branch");
        repo.restore_inherited_links(&source)
            .await
            .expect("restore inherited links");
        assert_eq!(
            repo.current_branch().await.expect("branch after restore"),
            branch
        );
        for link in ["deps/tracked", "deps/untracked"] {
            assert!(
                fs::read_link(clone.join(link))
                    .expect("restored link")
                    .is_absolute(),
                "{link} must be restored to an absolute source-tree target"
            );
        }
        let restored = vec![
            PathBuf::from("deps/tracked"),
            PathBuf::from("deps/untracked"),
        ];
        let source_state = GitRepository::from_root(&source)
            .rename_work_state(false, &restored)
            .await
            .expect("source state");
        let clone_state = repo
            .rename_work_state(false, &restored)
            .await
            .expect("clone state right after restoration");
        assert_eq!(
            clone_state, source_state,
            "a restored clone digests as its source"
        );

        let restored = vec![
            PathBuf::from("deps/tracked"),
            PathBuf::from("deps/untracked"),
        ];
        let before = repo
            .rename_work_state(false, &restored)
            .await
            .expect("state before retarget");
        for link in ["deps/tracked", "deps/untracked"] {
            fs::remove_file(clone.join(link)).expect("drop restored link");
            symlink(scratch.join("elsewhere"), clone.join(link)).expect("retarget link");
        }
        let after = repo
            .rename_work_state(false, &restored)
            .await
            .expect("state after retarget");
        assert_eq!(
            after, before,
            "restored links compare by path and presence only"
        );
    }

    #[tokio::test]
    async fn prepares_standalone_workspace_branch_and_only_the_local_main_remote() {
        let main = repository();
        let status = Command::new("git")
            .arg("-C")
            .arg(&main)
            .args([
                "remote",
                "add",
                "origin",
                "https://example.invalid/private.git",
            ])
            .status_locked()
            .expect("add inherited network remote");
        assert!(status.success());
        // A linked worktree of main: the registration a standalone clone inherits from it.
        let linked = main.with_extension("inherited-worktree");
        let status = Command::new("git")
            .arg("-C")
            .arg(&main)
            .args(["worktree", "add", "-q", "--detach"])
            .arg(&linked)
            .status_locked()
            .expect("register inherited worktree");
        assert!(status.success());
        let source_registrations = registrations(&main);
        let source_worktrees = worktree_lines(&main);
        assert_eq!(source_registrations.len(), 1);

        let mount = clone_image(&main);
        assert_eq!(
            worktree_lines(&mount).len(),
            2,
            "the clone inherits main's registration"
        );
        let repo = GitRepository::from_root(&mount);
        assert_eq!(
            repo.prepare_workspace("raven", &main, Some("main"), false)
                .await
                .expect("prepare workspace"),
            MainRemote::Canonical
        );
        assert_eq!(
            repo.current_branch().await.expect("read branch").as_deref(),
            Some("cowshed/raven")
        );
        // The clone lists only itself and holds no registration; the source keeps its own.
        assert!(registrations(&mount).is_empty());
        assert_eq!(
            worktree_lines(&mount),
            vec![format!(
                "worktree {}",
                fs::canonicalize(&mount).expect("canonical clone").display()
            )]
        );
        assert_eq!(registrations(&main), source_registrations);
        assert_eq!(worktree_lines(&main), source_worktrees);
        // The inherited network remote is gone and exactly one local remote stands in its place.
        let remotes = repo.remotes().await.expect("read remotes");
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].name, MAIN_REMOTE);
        assert_eq!(Path::new(&remotes[0].url), &*main);
    }

    #[tokio::test]
    async fn standalone_workspace_resume_accepts_only_its_current_branch() {
        let root = repository();
        let repo = GitRepository::from_root(&root);
        repo.prepare_workspace("raven", &root, Some("main"), false)
            .await
            .expect("initial preparation");

        let switched = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["switch", "--quiet", "main"])
            .status_locked()
            .expect("move HEAD away after branch creation");
        assert!(switched.success());
        repo.prepare_workspace("raven", &root, Some("main"), true)
            .await
            .expect("resume current workspace branch");
        assert_eq!(
            repo.current_branch()
                .await
                .expect("current branch")
                .as_deref(),
            Some("cowshed/raven")
        );
        let error = repo
            .prepare_workspace("raven", &root, Some("main"), false)
            .await
            .expect_err("fresh operation still refuses the existing branch");
        assert_eq!(error.code.as_str(), "conflict");
    }

    /// Clone main's tree the way the substrate does — files and all, `.git` included — so the
    /// mint step under test starts from exactly what a CoW clone hands it.
    fn clone_image(main: &Path) -> PathBuf {
        let mount = main.with_extension("workspace");
        let status = Command::new("cp")
            .args(["-R".as_ref(), main.as_os_str(), mount.as_os_str()])
            .status_locked()
            .expect("clone image");
        assert!(status.success());
        mount
    }

    fn git_stdout(root: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output_locked()
            .expect("run git");
        assert!(output.status.success(), "{args:?}");
        String::from_utf8(output.stdout).expect("git output is utf-8")
    }

    /// `worktree <path>` lines from `git worktree list --porcelain`: the checkout first, then each
    /// linked worktree git knows about.
    fn worktree_lines(root: &Path) -> Vec<String> {
        git_stdout(root, &["worktree", "list", "--porcelain"])
            .lines()
            .filter(|line| line.starts_with("worktree "))
            .map(str::to_owned)
            .collect()
    }

    /// Every `.git/worktrees/<id>` entry as its name and `gitdir` bytes, sorted; empty when none.
    fn registrations(root: &Path) -> Vec<(OsString, Vec<u8>)> {
        let Ok(entries) = fs::read_dir(root.join(".git/worktrees")) else {
            return Vec::new();
        };
        let mut found: Vec<(OsString, Vec<u8>)> = entries
            .map(|entry| {
                let entry = entry.expect("registration entry");
                let gitdir = fs::read(entry.path().join("gitdir")).expect("registration gitdir");
                (entry.file_name(), gitdir)
            })
            .collect();
        found.sort();
        found
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
    }

    /// Rename carries a linked source's Git state across. A crash before the administration
    /// handoff and a crash after it both resume to the same bytes, and the source is never touched.
    #[tokio::test]
    async fn rename_preserves_linked_source_index_wip_and_branch_across_resumed_handoff() {
        let main = repository();
        let scratch = main.parent().expect("scratch root").to_path_buf();
        let source = scratch.join("raven");
        let destination = scratch.join("cedar");
        fs::create_dir_all(main.join(".git/info")).expect("info directory");
        fs::write(main.join(".git/info/exclude"), "/warm/\n").expect("ignore warm directory");
        copy_tree(&main, &source);
        GitRepository::from_root(&source)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect("adopt source as linked worktree");

        fs::write(source.join("README"), "test\nstaged\n").expect("stage README edit");
        git(&source, &["add", "README"]);
        fs::write(source.join("README"), "test\nstaged\nunstaged\n").expect("unstaged README edit");
        fs::write(source.join("untracked.txt"), "untracked\n").expect("untracked file");
        fs::create_dir_all(source.join("warm")).expect("warm directory");
        fs::write(source.join("warm/cache.bin"), [0u8, 1, 2, 255]).expect("ignored warm file");

        type Snapshot = (String, String, Vec<u8>, [Vec<u8>; 3], String);
        fn snapshot(root: &Path, admin: &Path) -> Snapshot {
            (
                git_stdout(root, &["branch", "--show-current"])
                    .trim()
                    .to_owned(),
                git_stdout(root, &["ls-files", "--stage"]),
                fs::read(admin.join("index")).expect("administration index"),
                ["README", "untracked.txt", "warm/cache.bin"]
                    .map(|path| fs::read(root.join(path)).expect("workspace file")),
                git_stdout(
                    root,
                    &[
                        "--no-optional-locks",
                        "status",
                        "--porcelain=v1",
                        "--ignored",
                    ],
                ),
            )
        }

        let source_repo = GitRepository::from_root(&source);
        let head = source_repo.head_oid().await.expect("source head");
        let source_admin = main.join(".git/worktrees/raven");
        let cedar_admin = main.join(".git/worktrees/cedar");
        let original = snapshot(&source, &source_admin);
        assert_eq!(original.0, "cowshed/raven");

        copy_tree(&source, &destination);
        let main_repo = GitRepository::from_root(&main);
        let destination_repo = GitRepository::from_root(&destination);
        destination_repo
            .register_linked_worktree("cedar", &main_repo, &cedar_admin, head.as_str(), false)
            .await
            .expect("register clone immediately before handoff");
        destination_repo
            .preserve_linked_worktree_for_rename("raven", "cedar", &source, &main, true)
            .await
            .expect("resume preservation after registration");
        assert_eq!(snapshot(&destination, &cedar_admin), original);
        assert_eq!(snapshot(&source, &source_admin), original);
        assert_eq!(
            fs::read_to_string(cedar_admin.join("gitdir")).expect("destination gitdir"),
            format!(
                "{}\n",
                fs::canonicalize(&destination)
                    .expect("canonical destination")
                    .join(".git")
                    .display()
            ),
        );

        destination_repo
            .preserve_linked_worktree_for_rename("raven", "cedar", &source, &main, true)
            .await
            .expect("repeat preservation after completed handoff");
        assert_eq!(snapshot(&destination, &cedar_admin), original);
        assert_eq!(snapshot(&source, &source_admin), original);
        let names: Vec<OsString> = registrations(&main)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, [OsString::from("cedar"), OsString::from("raven")]);
    }

    #[tokio::test]
    async fn linked_worktree_resume_reuses_completed_registration() {
        let main = repository();
        let mount = clone_image(&main);
        let workspace = GitRepository::from_root(&mount);
        workspace
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect("initial linked worktree");

        workspace
            .adopt_as_linked_worktree_resumable("raven", &main, None, None, false, true)
            .await
            .expect("resume linked worktree");
        assert_eq!(
            workspace
                .current_branch()
                .await
                .expect("current branch")
                .as_deref(),
            Some("cowshed/raven")
        );
        assert!(main.join(".git/worktrees/raven").is_dir());
        assert!(mount.join(".git").is_file());
        assert!(!mount.join(".cowshed/worktree-staging").exists());
    }

    /// Where the source's `link:` dependency sits in every tree, relative to the tree root.
    const LINKED_PACKAGE: &str = "node_modules/@scope/pkg";

    /// Main, a session source and the new workspace at three different depths. The source holds
    /// what `bun install` leaves for a `link:` dependency — a relative link that climbs out of the
    /// tree into an immutable global install — beside an ordinary in-tree relative link.
    struct ThreeDepths {
        /// Owns every tree below: main is the seed repository moved inside it.
        _scratch: Repository,
        main: PathBuf,
        source: PathBuf,
        destination: PathBuf,
        /// The bytes the source recorded for its escaping link, valid only at the source's depth.
        inherited: PathBuf,
        /// What those bytes named in the source: outside every tree.
        external: PathBuf,
    }

    impl ThreeDepths {
        fn new(external_installed: bool) -> Self {
            let seed = repository();
            let base = seed.with_extension("depths");
            fs::create_dir_all(&base).expect("fixture base");
            let base = base.canonicalize().expect("canonical fixture base");
            let main = base.join("checkouts/home/user/dev/main");
            let source = base.join("shed/a/fixture");
            let destination = base.join("shed/b/c/raven");
            fs::create_dir_all(main.parent().expect("main parent")).expect("main parent");
            fs::rename(&seed, &main).expect("place main");
            fs::create_dir_all(source.parent().expect("source parent")).expect("source parent");
            copy_tree(&main, &source);

            let external = base.join("bun-links/pkg-0123abcd/global/node_modules/@scope/pkg");
            if external_installed {
                install(&external);
            }
            let package = source.join(LINKED_PACKAGE);
            let package_parent = package.parent().expect("package parent");
            fs::create_dir_all(package_parent).expect("source node_modules");
            let climb = package_parent
                .strip_prefix(&base)
                .expect("source under fixture")
                .components()
                .count();
            let inherited = PathBuf::from("../".repeat(climb)).join(
                external
                    .strip_prefix(&base)
                    .expect("external under fixture"),
            );
            symlink(&inherited, &package).expect("source link: dependency");
            fs::create_dir_all(source.join("vendor/lib")).expect("in-tree target");
            fs::create_dir_all(source.join("packages/app")).expect("in-tree link parent");
            symlink("../../vendor/lib", source.join("packages/app/lib")).expect("in-tree link");
            symlink(source.join("vendor/lib"), source.join("packages/app/own"))
                .expect("absolute link into the source");

            // Main sits deeper than the source, so the same bytes resolved from main land on a
            // real directory: the silent wrong answer, not merely a dangling one.
            let decoy = crate::inherited_links::resolve_in_source(
                &main,
                Path::new(LINKED_PACKAGE).parent().expect("package parent"),
                &inherited,
            );
            assert!(
                decoy.starts_with(&base) && decoy != external,
                "main's reading must be a distinct path inside the fixture: {}",
                decoy.display()
            );
            fs::create_dir_all(decoy.join("decoy")).expect("decoy");

            fs::create_dir_all(destination.parent().expect("destination parent"))
                .expect("destination parent");
            copy_tree(&source, &destination);
            assert!(
                !destination.join(LINKED_PACKAGE).exists(),
                "the inherited bytes must miss at the clone's depth, or this proves nothing"
            );
            Self {
                _scratch: seed,
                main,
                source,
                destination,
                inherited,
                external,
            }
        }

        fn origin(&self) -> CloneOrigin<'_> {
            CloneOrigin {
                source: &self.source,
                main: &self.main,
            }
        }

        fn assert_links_follow_the_source(&self) {
            let package = self.destination.join(LINKED_PACKAGE);
            assert_eq!(
                fs::read_link(&package).expect("restored link"),
                self.external,
                "an escaping link names what it named in the source, never what it names from main"
            );
            assert!(
                package.join("installed").is_file(),
                "the restored link resolves to the source's install"
            );
            let lib = self.destination.join("packages/app/lib");
            assert_eq!(
                fs::read_link(&lib).expect("in-tree link"),
                Path::new("../../vendor/lib"),
                "an in-tree link keeps its relative bytes"
            );
            assert_eq!(
                lib.canonicalize().expect("in-tree link resolves"),
                self.destination.join("vendor/lib"),
                "an in-tree link resolves inside the clone"
            );
            let own = self.destination.join("packages/app/own");
            assert_eq!(
                fs::read_link(&own).expect("absolute link into the source"),
                Path::new("../../vendor/lib"),
                "an absolute link into the source is re-spelled relative to the clone's own copy"
            );
            assert_eq!(
                own.canonicalize().expect("re-spelled link resolves"),
                self.destination.join("vendor/lib"),
                "never the source's"
            );
        }

        async fn assert_standalone_main_remote(&self, workspace: &GitRepository) {
            assert_eq!(
                workspace.current_branch().await.expect("branch").as_deref(),
                Some("cowshed/raven")
            );
            assert_eq!(
                workspace.remotes().await.expect("remotes"),
                vec![RemoteUrl {
                    name: MAIN_REMOTE.to_owned(),
                    url: self.main.clone(),
                }],
                "the main remote names main's canonical mount, not the tree the image came from"
            );
        }
    }

    /// Copy a tree the way the substrate clones an image: links as links, `.git` included.
    fn copy_tree(from: &Path, to: &Path) {
        let status = Command::new("cp")
            .args(["-RP".as_ref(), from.as_os_str(), to.as_os_str()])
            .status_locked()
            .expect("copy tree");
        assert!(status.success());
    }

    fn install(package: &Path) {
        fs::create_dir_all(package).expect("install package");
        fs::write(package.join("installed"), "").expect("install marker");
    }

    #[tokio::test]
    async fn a_fork_repairs_links_from_its_producer_without_changing_git_identity() {
        let trees = ThreeDepths::new(true);
        let workspace = GitRepository::from_root(&trees.destination);
        let branch = workspace.current_branch().await.expect("inherited branch");
        let remotes = workspace.remotes().await.expect("inherited remotes");
        workspace
            .restore_inherited_links(&trees.source)
            .await
            .expect("repair fork links");
        trees.assert_links_follow_the_source();
        assert_eq!(
            workspace.current_branch().await.expect("fork branch"),
            branch
        );
        assert_eq!(workspace.remotes().await.expect("fork remotes"), remotes);
    }

    #[tokio::test]
    async fn a_standalone_mint_resolves_inherited_links_against_its_source_and_remotes_against_main()
     {
        let trees = ThreeDepths::new(true);
        let workspace = GitRepository::from_root(&trees.destination);
        workspace
            .mint_workspace(
                "raven",
                trees.origin(),
                WorkspaceRepository::Standalone,
                None,
                false,
                false,
            )
            .await
            .expect("mint standalone workspace");
        trees.assert_links_follow_the_source();
        trees.assert_standalone_main_remote(&workspace).await;
    }

    #[tokio::test]
    async fn a_linked_worktree_mint_resolves_inherited_links_against_its_source_and_registers_in_main()
     {
        let trees = ThreeDepths::new(true);
        let workspace = GitRepository::from_root(&trees.destination);
        workspace
            .mint_workspace(
                "raven",
                trees.origin(),
                WorkspaceRepository::LinkedWorktree,
                None,
                false,
                false,
            )
            .await
            .expect("mint linked worktree");
        trees.assert_links_follow_the_source();
        assert!(trees.destination.join(".git").is_file());
        assert_eq!(
            PathBuf::from(
                fs::read_to_string(trees.main.join(".git/worktrees/raven/gitdir"))
                    .expect("admin gitdir")
                    .trim()
            ),
            trees.destination.join(".git"),
            "the registration lives in main, whichever tree the image came from"
        );
        assert!(
            git_stdout(&trees.main, &["branch", "--list", "cowshed/raven"])
                .contains("cowshed/raven")
        );
        assert!(
            workspace.remotes().await.expect("remotes").is_empty(),
            "a linked worktree shares main's object store and has nothing to fetch"
        );
    }

    #[tokio::test]
    async fn a_refused_inherited_link_writes_nothing_and_a_resumed_mint_repairs_it_from_the_source()
    {
        let trees = ThreeDepths::new(false);
        let workspace = GitRepository::from_root(&trees.destination);
        let error = workspace
            .mint_workspace(
                "raven",
                trees.origin(),
                WorkspaceRepository::Standalone,
                None,
                false,
                false,
            )
            .await
            .expect_err("an escaping link its source cannot resolve fails closed");
        assert_eq!(error.code.as_str(), "integrity");
        assert!(error.message.contains(LINKED_PACKAGE), "{}", error.message);
        assert_eq!(
            fs::read_link(trees.destination.join(LINKED_PACKAGE)).expect("refused link"),
            trees.inherited,
            "a refused link keeps the bytes it inherited"
        );
        assert_eq!(
            workspace.current_branch().await.expect("branch").as_deref(),
            Some("main"),
            "a refusal stops the mint before any Git preparation"
        );

        // Repaired in the producer, as an operator would: the install target appears and the
        // source's own link is normalized to it. The interrupted clone still holds the bytes it
        // inherited, and only the tree that wrote them can say what they meant.
        install(&trees.external);
        let source_package = trees.source.join(LINKED_PACKAGE);
        fs::remove_file(&source_package).expect("unlink source package");
        symlink(&trees.external, &source_package).expect("normalize source package");
        workspace
            .mint_workspace(
                "raven",
                trees.origin(),
                WorkspaceRepository::Standalone,
                None,
                false,
                true,
            )
            .await
            .expect("resume mint");
        trees.assert_links_follow_the_source();
        trees.assert_standalone_main_remote(&workspace).await;
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
            .status_locked()
            .expect("create colliding branch");
        assert!(status.success());

        let error = GitRepository::from_root(&mount)
            .adopt_as_linked_worktree("raven", &main, None)
            .await
            .expect_err("colliding branch must refuse");
        assert_eq!(error.code.as_str(), "conflict");
        // Refused before anything was discarded: the image is still a repository.
        assert!(mount.join(".git").is_dir());
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
    }

    /// `host` is an ordinary remote name: configuration adds cowshed's `main` beside a remote a
    /// workspace's user named `host` and never removes it.
    #[tokio::test]
    async fn a_remote_named_host_is_the_users_and_survives_configuration() {
        let root = repository();
        let theirs = PathBuf::from("/tmp/cowshed-users-host-remote");
        let mount = PathBuf::from("/tmp/cowshed-canonical-mount");
        let repo = GitRepository::from_root(&root);
        repo.set_remote("host", &theirs).await.expect("user remote");

        assert_eq!(
            repo.configure_main_remote(&mount)
                .await
                .expect("configure beside a user remote"),
            MainRemote::Canonical
        );
        assert_eq!(
            repo.remote_url("host").await.expect("read host"),
            Some(theirs)
        );
        assert_eq!(
            repo.remote_url(MAIN_REMOTE).await.expect("read main"),
            Some(mount)
        );
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
    }

    #[tokio::test]
    async fn preserves_non_utf8_main_remote_argument() {
        let root = repository();
        let main_mount = PathBuf::from(OsString::from_vec(b"/tmp/cowshed-main-\xff".to_vec()));
        let repo = GitRepository::from_root(&root);
        repo.prepare_workspace("raven", &main_mount, None, false)
            .await
            .expect("prepare workspace");

        let output = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["remote", "get-url", MAIN_REMOTE])
            .output_locked()
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
            .status_locked()
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
            .status_locked()
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
            .status_locked()
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
            .status_locked()
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
            .status_locked()
            .expect("publish preservation ref");
        assert!(status.success());
        assert!(
            host_repo
                .commit_is_preserved(session_head.as_str())
                .await
                .expect("preservation ref contains session commit")
        );
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
    }

    #[tokio::test]
    async fn stale_auto_merge_is_not_an_in_progress_operation() {
        let root = repository();
        fs::write(root.join(".git/AUTO_MERGE"), "deadbeef\n").expect("write stale merge tree");
        let git = GitRepository::from_root(&root);
        assert_eq!(
            git.in_progress_operation()
                .await
                .expect("read operation state"),
            None
        );
        assert!(!git.is_dirty_by(None).await.expect("read repository status"));
        git.ensure_adoptable()
            .await
            .expect("a leftover merge tree is not a running operation");
        fs::write(root.join(".git/CHERRY_PICK_HEAD"), "deadbeef\n")
            .expect("mark an active cherry-pick");
        assert_eq!(
            git.in_progress_operation()
                .await
                .expect("read operation state"),
            Some("CHERRY_PICK_HEAD".to_owned())
        );
    }

    #[tokio::test]
    async fn cancelled_bundle_verification_removes_its_bare_repository() {
        let root = repository();
        let scratch = BundleVerificationScratch::create(&root).expect("verification repository");
        let path = scratch.path.clone();
        let work = tokio::spawn(async move {
            let _scratch = scratch;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert!(
            path.is_dir(),
            "verification scratch exists before cancellation"
        );
        work.abort();
        assert!(
            work.await
                .expect_err("cancelled verification")
                .is_cancelled()
        );
        assert!(
            !path.exists(),
            "dropping the cancelled future must remove its scratch"
        );
    }

    /// Commit `label` on top of whatever is checked out in `root`, and answer the new head.
    fn commit_on(root: &Path, label: &str) -> String {
        fs::write(root.join(label), format!("{label}\n")).expect("write fixture change");
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["add", "."])
            .status_locked()
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
            .status_locked()
            .expect("run git commit");
        assert!(status.success());
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(["rev-parse", "HEAD"])
            .output_locked()
            .expect("read head");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("utf-8 oid")
            .trim_end()
            .to_owned()
    }

    /// `git <args>` in `root`, committing as the fixture's author: a CI runner has no global identity.
    fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "-c",
                "user.name=Cowshed Test",
                "-c",
                "user.email=test@example.invalid",
            ])
            .args(args)
            .status_locked()
            .expect("run git");
        assert!(status.success(), "git {args:?}");
    }

    #[tokio::test]
    async fn a_repository_copied_off_a_case_insensitive_volume_reads_case_sensitive_once_recorded()
    {
        let root = repository();
        git(&root, &["config", "core.ignorecase", "true"]);
        GitRepository::from_root(&root)
            .record_case_sensitive_filesystem()
            .await
            .expect("record case sensitivity");
        let output = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["config", "--local", "--get", "core.ignorecase"])
            .output_locked()
            .expect("read core.ignorecase");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"false\n");
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
    }

    #[tokio::test]
    async fn rewritten_main_bundle_matches_the_live_range_and_fetches_into_an_empty_repository() {
        let main = repository();
        let main_repo = GitRepository::from_root(&main);
        commit_on(&main, "old-one");
        let clone_time_main = commit_on(&main, "old-two");

        let workspace = main.with_extension("history-diverged");
        let status = Command::new("git")
            .args(["clone", "-q"])
            .arg(&main)
            .arg(&workspace)
            .status_locked()
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

        let recovery = main.with_extension("empty-recovery");
        fs::create_dir_all(&recovery).expect("create empty recovery");
        git(&recovery, &["init", "-q", "--bare", "."]);
        let verify = Command::new("git")
            .arg("-C")
            .arg(&recovery)
            .args(["bundle", "verify"])
            .arg(&bundle)
            .output_locked()
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
    }

    #[tokio::test]
    async fn a_retried_abandonment_keeps_its_verified_bundle_and_ignores_a_lock_at_its_name() {
        use std::os::unix::fs::MetadataExt as _;

        let workspace = repository();
        let repo = GitRepository::from_root(&workspace);
        let base = repo.head_oid().await.expect("read base");
        git(&workspace, &["switch", "-qc", "cowshed/retried"]);
        commit_on(&workspace, "one");
        commit_on(&workspace, "two");
        let bundle = workspace.join("retried.bundle");
        // An earlier writer of this bundle — a removal killed mid-`git bundle create`, or one a
        // stale binary is still running — holds git's lock file at the bundle's own name.
        fs::write(workspace.join("retried.bundle.lock"), b"another writer\n")
            .expect("plant the other writer's lock");

        let written = repo
            .bundle_commits(&bundle, Some(base.as_str()), "HEAD")
            .await
            .expect("a lock at the bundle's name cannot fail the removal");
        let first = fs::metadata(&bundle).expect("bundle written").ino();
        let retried = repo
            .bundle_commits(&bundle, Some(base.as_str()), "HEAD")
            .await
            .expect("the retry reports the same range");
        assert_eq!(written, 2);
        assert_eq!(retried, written);
        assert_eq!(
            fs::metadata(&bundle).expect("bundle kept").ino(),
            first,
            "a retry keeps the verified bundle rather than writing a second one over it"
        );
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
        assert_eq!(after.url.as_deref(), Some(&*main_mount));
        assert!(
            after.repository,
            "configure_main_remote must retarget cowshed's remote"
        );
    }
}
