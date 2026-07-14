use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::process::Output;

use tokio::process::Command;

use crate::error::{CowshedError, Result};

const GIT: &str = "/usr/bin/git";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteUrl {
    pub name: String,
    pub url: String,
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

    /// Whether `commit` is contained by a host branch or a Cowshed preservation ref.
    ///
    /// A commit absent from this repository is an ordinary negative result: session repositories
    /// can contain unpublished objects that the controller-side host repository has never seen.
    pub async fn commit_is_preserved(&self, commit: &str) -> Result<bool> {
        let object = format!("{commit}^{{commit}}");
        let exists = self.run(["cat-file", "-e", object.as_str()]).await?;
        if !exists.status.success() {
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
        let object = format!("{commit}^{{commit}}");
        let exists = self.run(["cat-file", "-e", object.as_str()]).await?;
        if !exists.status.success() {
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

    /// Configure local-only workspace Git and create its session branch.
    pub async fn prepare_workspace(
        &self,
        name: &str,
        host_path: &Path,
        start: Option<&str>,
    ) -> Result<()> {
        if !host_path.is_absolute() {
            return Err(CowshedError::usage(
                "workspace host remote must be an absolute local path",
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

        let remotes = self.run(["remote"]).await?;
        if !remotes.status.success() {
            return Err(git_internal("list workspace remotes", &remotes));
        }
        for remote in parse_lines(&remotes.stdout, "remote name")? {
            let output = self.run(["remote", "remove", remote.as_str()]).await?;
            ensure_git_success("remove inherited remote", output)?;
        }
        let output = self
            .run([
                OsStr::new("remote"),
                OsStr::new("add"),
                OsStr::new("host"),
                host_path.as_os_str(),
            ])
            .await?;
        ensure_git_success("add host remote", output)?;

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

    use super::{GitRepository, ensure_git_success, git_message};
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
        let status = Command::new("/usr/bin/git")
            .args(["init", "-q", "-b", "main"])
            .arg(&root)
            .status()
            .expect("run git init");
        assert!(status.success());
        fs::write(root.join("README"), "test\n").expect("write fixture");
        let status = Command::new("/usr/bin/git")
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
        let status = Command::new("/usr/bin/git")
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
        let status = Command::new("/usr/bin/git")
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
    async fn prepares_standalone_workspace_branch_and_only_local_host_remote() {
        let root = repository();
        let status = Command::new("/usr/bin/git")
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
        repo.prepare_workspace("raven", &root, Some("main"))
            .await
            .expect("prepare workspace");
        assert_eq!(
            repo.current_branch().await.expect("read branch").as_deref(),
            Some("cowshed/raven")
        );
        let remotes = repo.remotes().await.expect("read remotes");
        assert_eq!(remotes.len(), 1);
        assert_eq!(remotes[0].name, "host");
        assert_eq!(Path::new(&remotes[0].url), root);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn preserves_non_utf8_host_remote_argument() {
        let root = repository();
        let host_path = PathBuf::from(OsString::from_vec(b"/tmp/cowshed-host-\xff".to_vec()));
        let repo = GitRepository::from_root(&root);
        repo.prepare_workspace("raven", &host_path, None)
            .await
            .expect("prepare workspace");

        let output = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(&root)
            .args(["remote", "get-url", "host"])
            .output()
            .expect("read raw host remote");
        assert!(output.status.success());
        assert_eq!(
            output
                .stdout
                .strip_suffix(b"\n")
                .expect("git output newline"),
            host_path.as_os_str().as_bytes()
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
        let status = Command::new("/usr/bin/git")
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
        let status = Command::new("/usr/bin/git")
            .args(["clone", "-q"])
            .arg(&host)
            .arg(&session)
            .status()
            .expect("clone session");
        assert!(status.success());
        fs::write(session.join("session-only"), "unpublished\n").expect("write session change");
        let status = Command::new("/usr/bin/git")
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
        let status = Command::new("/usr/bin/git")
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

        let status = Command::new("/usr/bin/git")
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
}
