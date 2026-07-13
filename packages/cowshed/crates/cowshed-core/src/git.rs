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

        let git_dir_output = run_git_at(
            path,
            ["rev-parse", "--path-format=absolute", "--git-dir"],
        )
        .await?;
        if git_dir_output.status.success() {
            let git_dir = parse_one_path(&git_dir_output.stdout, "git directory")?;
            if git_dir.file_name() == Some(OsStr::new(".git")) {
                if let Some(root) = git_dir.parent() {
                    return Ok(Self {
                        root: root.to_path_buf(),
                    });
                }
            }
        }

        Err(CowshedError::environment_missing(
            format!("{} is not inside a standalone git repository", path.display()),
            "cowshed adopt <git-root>",
        ))
    }

    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Reject in-progress repository operations; ordinary dirty work is intentionally allowed.
    pub async fn ensure_adoptable(&self) -> Result<()> {
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
                return Err(CowshedError::conflict(
                    format!("repository has an in-progress {state} operation"),
                    format!(
                        "finish or abort the git operation, then run: cowshed adopt {}",
                        self.root.display()
                    ),
                ));
            }
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
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::GitRepository;
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
    async fn rejects_in_progress_operation() {
        let root = repository();
        fs::write(root.join(".git/MERGE_HEAD"), "deadbeef\n").expect("write merge marker");
        let error = GitRepository::from_root(&root)
            .ensure_adoptable()
            .await
            .expect_err("must reject merge");
        assert_eq!(error.code.as_str(), "conflict");
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
