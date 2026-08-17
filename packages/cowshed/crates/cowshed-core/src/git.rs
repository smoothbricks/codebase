use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;

use tokio::process::Command;

use crate::error::{CowshedError, Result};

#[cfg(target_os = "macos")]
const GIT: &str = "/usr/bin/git";
#[cfg(not(target_os = "macos"))]
const GIT: &str = "git";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteUrl {
    pub name: String,
    pub url: String,
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

/// Where a linked-worktree registration is taken out before its pointer is moved to the mount
/// root. Inside the image, so it is on the workspace's own volume and gone before the mount is
/// published; under `.cowshed/` because that subtree is already cowshed's and not the repository's.
const WORKTREE_STAGING: &str = ".cowshed/worktree-staging";

/// The name of the remote main registers for a workspace under `--register`.
pub fn workspace_remote_name(workspace: &str) -> String {
    format!("cowshed/{workspace}")
}

#[derive(Clone, Debug)]
pub struct GitRepository {
    root: PathBuf,
}

impl GitRepository {
    /// Resolve the standalone repository containing `path`.
    pub async fn discover(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let output = run_git_at(path, ["rev-parse", "--show-toplevel"]).await?;
        if output.status.success() {
            return Ok(Self {
                root: parse_one_path(&output.stdout, "git root")?,
            });
        }

        let git_dir_output =
            run_git_at(path, ["rev-parse", "--path-format=absolute", "--git-dir"]).await?;
        if git_dir_output.status.success() {
            let git_dir = parse_one_path(&git_dir_output.stdout, "git directory")?;
            if git_dir.file_name() == Some(OsStr::new(".git"))
                && let Some(root) = git_dir.parent()
            {
                return Ok(Self {
                    root: root.to_path_buf(),
                });
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
        Self { root: root.into() }
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
        let names_output = self.run(["remote"]).await?;
        if !names_output.status.success() {
            return Err(git_internal("list git remotes", &names_output));
        }

        let names = parse_lines(&names_output.stdout, "remote name")?;
        let mut remotes = Vec::new();
        for name in names {
            let output = self
                .run(["remote", "get-url", "--all", name.as_str()])
                .await?;
            if !output.status.success() {
                return Err(git_internal("read git remote", &output));
            }
            for url in parse_lines(&output.stdout, "remote URL")? {
                remotes.push(RemoteUrl {
                    name: name.clone(),
                    url,
                });
            }
        }
        remotes.sort_by(|left, right| (&left.name, &left.url).cmp(&(&right.name, &right.url)));
        remotes.dedup();
        Ok(remotes)
    }

    pub async fn head_oid(&self) -> Result<String> {
        self.read_one(["rev-parse", "HEAD"], "read HEAD").await
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

    pub async fn is_dirty(&self) -> Result<bool> {
        let output = self
            .run(["status", "--porcelain=v1", "-z", "--untracked-files=normal"])
            .await?;
        if !output.status.success() {
            return Err(git_internal("read repository status", &output));
        }
        Ok(!output.stdout.is_empty())
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
    async fn resolve_commit(&self, revision: &str) -> Result<Option<String>> {
        let peeled = format!("{revision}^{{commit}}");
        let output = self
            .run(["rev-parse", "--verify", "--quiet", peeled.as_str()])
            .await?;
        match output.status.code() {
            Some(0) => parse_one_string(&output.stdout, "commit revision").map(Some),
            Some(1) => Ok(None),
            _ => Err(git_internal("resolve commit revision", &output)),
        }
    }

    async fn has_commit(&self, commit: &str) -> Result<bool> {
        Ok(self.resolve_commit(commit).await?.is_some())
    }

    /// Whether `commit` is contained by a host branch or a Cowshed preservation ref.
    pub async fn commit_is_preserved(&self, commit: &str) -> Result<bool> {
        if !self.has_commit(commit).await? {
            return Ok(false);
        }
        let output = self
            .run([
                "for-each-ref",
                "--format=%(refname)",
                "--contains",
                commit,
                "refs/heads",
                "refs/cowshed",
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal("check host commit preservation refs", &output));
        }
        Ok(!output.stdout.is_empty())
    }

    /// Whether `commit` is contained by a remote-tracking ref in this repository.
    ///
    /// This is the conservative, offline proof used before deleting an adopted main image:
    /// local heads disappear with that image, while a remote-tracking ref records a push/fetch
    /// boundary whose remote retains the commit.
    pub async fn commit_is_remote_preserved(&self, commit: &str) -> Result<bool> {
        if !self.has_commit(commit).await? {
            return Ok(false);
        }
        let output = self
            .run([
                "for-each-ref",
                "--format=%(refname)",
                "--contains",
                commit,
                "refs/remotes",
            ])
            .await?;
        if !output.status.success() {
            return Err(git_internal(
                "check remote commit preservation refs",
                &output,
            ));
        }
        Ok(!output.stdout.is_empty())
    }

    /// The tip of local branch `branch`, or `None` when this repository has no such branch.
    pub async fn branch_tip(&self, branch: &str) -> Result<Option<String>> {
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
        let count = self
            .read_one(["rev-list", "--count", range.as_str()], "count commits")
            .await?;
        count.parse().map_err(|_| {
            CowshedError::integrity(
                format!("git reported an unparseable commit count: {count}"),
                "repair the git repository",
            )
        })
    }

    /// Write a Git bundle of the commits `head` has and `exclude` does not.
    ///
    /// The range form produces a *thin* bundle whose one prerequisite is `exclude`. That is the
    /// right trade here: the bundle is written beside a workspace's retired image, and the
    /// repository it would ever be restored into is main's, which contains `exclude` by
    /// construction — a workspace image is a copy of main's, so its local `main` is an ancestor of
    /// main's own. When there is no usable range (no such branch, or nothing ahead of it) the
    /// bundle carries every ref instead, because a bundle that cannot stand alone is not a belt.
    ///
    /// `head` MUST be a ref spelling — `HEAD` or a branch — never a raw oid. `git bundle create`
    /// names a bundle's contents after the refs in its rev range, so an oid tip yields a bundle
    /// with no refs, which git rejects outright as empty.
    pub async fn bundle_commits(
        &self,
        destination: &Path,
        exclude: Option<&str>,
        head: &str,
    ) -> Result<()> {
        let range = match self.usable_exclude(exclude).await? {
            Some(exclude) if self.commits_ahead(Some(&exclude), head).await? > 0 => {
                Some(format!("{exclude}..{head}"))
            }
            _ => None,
        };
        let mut args = vec![
            OsString::from("bundle"),
            OsString::from("create"),
            destination.as_os_str().to_owned(),
        ];
        args.push(OsString::from(range.unwrap_or_else(|| "--all".to_owned())));
        let output = self.run(args).await?;
        ensure_git_success("write commit bundle", output)
    }

    /// The `exclude` endpoint of a range, reduced to an oid this repository holds.
    async fn usable_exclude(&self, exclude: Option<&str>) -> Result<Option<String>> {
        match exclude {
            Some(exclude) => self.resolve_commit(exclude).await,
            None => Ok(None),
        }
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
        let branch = format!("cowshed/{name}");
        let branch_ref = format!("refs/heads/{branch}");
        let exists = self
            .run(["show-ref", "--verify", "--quiet", branch_ref.as_str()])
            .await?;
        if exists.status.success() {
            return Err(CowshedError::conflict(
                format!("branch {branch} already exists in the cloned workspace"),
                format!("remove or rename {branch}, then retry: cowshed new {name}"),
            ));
        }
        if exists.status.code() != Some(1) {
            return Err(git_internal("check workspace branch", &exists));
        }

        // The `.git` directory arrived by CoW carrying every remote main had, including network
        // URLs. Sandboxed git speaks only local paths, so a fresh mint drops the lot before
        // configuring its own upstream — this is the "no remote URL ever exists inside a sandbox"
        // invariant, not a clobber of user intent: nothing in this repository is the user's yet.
        for remote in self.remote_names().await? {
            let output = self.run(["remote", "remove", remote.as_str()]).await?;
            ensure_git_success("remove inherited remote", output)?;
        }
        let main_remote = self.configure_main_remote(main_mount).await?;

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
        ensure_git_success("create workspace branch", output)?;
        Ok(main_remote)
    }

    /// Point this workspace at main's canonical mount, without ever clobbering a remote cowshed
    /// did not create.
    ///
    /// Idempotent, and that is the point: it runs at mint against a repository whose remotes were
    /// just stripped, and again on any later reconciliation against one an agent has been working
    /// in — where `cowshed repo` mirrors and hand-added remotes are the user's, and a remote named
    /// `main` may well be one of them.
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
            // Already ours and already correct: the idempotent re-run.
            Some(url) if Path::new(&url) == main_mount => Ok(MainRemote::Canonical),
            // Someone else holds the name. Leave it exactly as it is and stand beside it.
            Some(_) => {
                self.set_remote(FALLBACK_MAIN_REMOTE, main_mount).await?;
                Ok(MainRemote::Displaced)
            }
            None => {
                self.set_remote(MAIN_REMOTE, main_mount).await?;
                Ok(MainRemote::Canonical)
            }
        }
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
        let branch = format!("cowshed/{name}");
        let branch_ref = format!("refs/heads/{branch}");
        // The branch is created in main's ref namespace, so the collision to check is main's, not
        // this image's — the image is about to stop having a ref namespace of its own.
        let exists = main
            .run(["show-ref", "--verify", "--quiet", branch_ref.as_str()])
            .await?;
        if exists.status.success() {
            return Err(CowshedError::conflict(
                format!("branch {branch} already exists in main's repository"),
                format!("remove or rename {branch}, then retry: cowshed new {name} --git-worktree"),
            ));
        }
        if exists.status.code() != Some(1) {
            return Err(git_internal("check workspace branch", &exists));
        }
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
        ensure_git_success("create workspace branch", output)?;
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
        run_git_at(&self.root, args).await
    }
}

async fn run_git_at<I, S>(root: &Path, args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(GIT)
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot execute git: {error}"),
                "install the macOS command line developer tools, then retry",
            )
        })
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

fn git_internal(operation: &str, output: &Output) -> CowshedError {
    CowshedError::internal(git_message(operation, output))
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
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus, Output};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        FALLBACK_MAIN_REMOTE, GIT, GitRepository, MAIN_REMOTE, MainRemote, ensure_git_success,
        git_message, workspace_remote_name,
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
        let status = Command::new(GIT)
            .args(["init", "-q", "-b", "main"])
            .arg(&root)
            .status()
            .expect("run git init");
        assert!(status.success());
        fs::write(root.join("README"), "test\n").expect("write fixture");
        let status = Command::new(GIT)
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
        let status = Command::new(GIT)
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
    async fn detached_head_has_no_current_branch() {
        let root = repository();
        let status = Command::new(GIT)
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
        assert_eq!(repo.head_oid().await.expect("read head").len(), 40);
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
        let status = Command::new(GIT)
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
        let status = Command::new("/bin/cp")
            .args(["-R".as_ref(), main.as_os_str(), mount.as_os_str()])
            .status()
            .expect("clone image");
        assert!(status.success());
        mount
    }

    fn git_stdout(root: &Path, args: &[&str]) -> String {
        let output = Command::new(GIT)
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
        let status = Command::new(GIT)
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
        // upstream stands beside it under the fallback name.
        let foreign = PathBuf::from("/tmp/somewhere-the-user-chose");
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
            Some(mount)
        );
        assert_eq!(MainRemote::Displaced.remote_name(), FALLBACK_MAIN_REMOTE);
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

        let output = Command::new(GIT)
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
    async fn preservation_requires_a_host_branch_or_cowshed_ref_containing_the_commit() {
        let host = repository();
        let host_repo = GitRepository::from_root(&host);
        let host_head = host_repo.head_oid().await.expect("read host head");
        assert!(
            host_repo
                .commit_is_preserved(&host_head)
                .await
                .expect("main preserves its head")
        );
        assert!(
            !host_repo
                .commit_is_remote_preserved(&host_head)
                .await
                .expect("local head is not remotely preserved")
        );
        let status = Command::new(GIT)
            .arg("-C")
            .arg(&host)
            .args(["update-ref", "refs/remotes/origin/main", &host_head])
            .status()
            .expect("write remote-tracking ref");
        assert!(status.success());
        assert!(
            host_repo
                .commit_is_remote_preserved(&host_head)
                .await
                .expect("remote-tracking ref preserves head")
        );

        let session = host.with_extension("session");
        let status = Command::new(GIT)
            .args(["clone", "-q"])
            .arg(&host)
            .arg(&session)
            .status()
            .expect("clone session");
        assert!(status.success());
        fs::write(session.join("session-only"), "unpublished\n").expect("write session change");
        let status = Command::new(GIT)
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
        let status = Command::new(GIT)
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
                .commit_is_preserved(&session_head)
                .await
                .expect("absent session object is not preserved")
        );

        let status = Command::new(GIT)
            .arg("-C")
            .arg(&session)
            .args(["push", "-q", "origin", "HEAD:refs/cowshed/raven/heads/main"])
            .status()
            .expect("publish preservation ref");
        assert!(status.success());
        assert!(
            host_repo
                .commit_is_preserved(&session_head)
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
        let status = Command::new(GIT)
            .arg("-C")
            .arg(root)
            .args(["add", "."])
            .status()
            .expect("run git add");
        assert!(status.success());
        let status = Command::new(GIT)
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
        let output = Command::new(GIT)
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
        let status = Command::new(GIT)
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
                .commit_is_ancestor(&work, &base)
                .await
                .expect("compare ancestry")
        );
        // The base is landed by definition, and a commit is its own ancestor.
        assert!(
            repo.commit_is_ancestor(&base, &work)
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
        assert_eq!(landed_tip, work);
        assert!(
            repo.commit_is_ancestor(&work, &landed_tip)
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
            repo.commits_ahead(Some(&base), "HEAD")
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
    async fn bundled_unlanded_commits_restore_into_a_repository_holding_the_prerequisite() {
        let root = repository();
        let repo = GitRepository::from_root(&root);
        let base = repo.head_oid().await.expect("read base");
        git(&root, &["switch", "-qc", "cowshed/raven"]);
        commit_on(&root, "one");
        let tip = commit_on(&root, "two");

        let bundle = root.join("raven.bundle");
        repo.bundle_commits(&bundle, Some("main"), "HEAD")
            .await
            .expect("bundle the unlanded range");
        assert!(bundle.is_file());

        // A raw-oid tip names no ref, so git refuses the bundle as empty. This is the shape a live
        // abandonment hit: the caller had the fenced oid in hand and the range looked right.
        let refless = repo
            .bundle_commits(&root.join("refless.bundle"), Some("main"), &tip)
            .await
            .expect_err("an oid tip produces a ref-less bundle git will not write");
        assert!(refless.message.contains("empty bundle"), "{refless:?}");

        // The recovery repository is main's: it holds the prerequisite and nothing of the session.
        let recovery = repository();
        git(
            &recovery,
            &[
                "fetch",
                "-q",
                root.to_str().expect("utf-8 root"),
                "+refs/heads/main:refs/heads/mint",
            ],
        );
        let recovery_repo = GitRepository::from_root(&recovery);
        assert!(
            !recovery_repo
                .has_commit(&tip)
                .await
                .expect("recovery repository has no session object yet")
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
        assert!(
            recovery_repo
                .has_commit(&tip)
                .await
                .expect("bundle restores the abandoned tip")
        );
        assert_eq!(
            recovery_repo
                .commits_ahead(Some(&base), "refs/heads/recovered")
                .await
                .expect("count restored commits"),
            2
        );

        // No usable range: the bundle has to stand alone, so it carries every ref.
        let standalone = root.join("standalone.bundle");
        repo.bundle_commits(&standalone, Some("no-such-branch"), "HEAD")
            .await
            .expect("bundle every ref");
        let empty_recovery = std::env::temp_dir().join(format!(
            "cowshed-git-test-empty-{}-{}",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&empty_recovery).expect("create empty recovery");
        git(&empty_recovery, &["init", "-q", "-b", "main"]);
        git(
            &empty_recovery,
            &[
                "fetch",
                "-q",
                standalone.to_str().expect("utf-8 bundle"),
                "+refs/heads/cowshed/raven:refs/heads/recovered",
            ],
        );
        assert!(
            GitRepository::from_root(&empty_recovery)
                .has_commit(&tip)
                .await
                .expect("all-refs bundle stands alone")
        );

        fs::remove_dir_all(empty_recovery).expect("remove empty recovery");
        fs::remove_dir_all(recovery).expect("remove recovery fixture");
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
