//! Source identity capture, then a plain git commit in the destination.
//!
//! New cowshed workspaces used to exec with `GIT_CONFIG_GLOBAL=/dev/null`, so an identity that
//! lived only in the operator's global config or an `includeIf gitdir:` rule never reached `git
//! commit`. These cases drive the public capture helper and then commit the way a workspace child
//! will: isolated `HOME`, `GIT_CONFIG_GLOBAL` pointing at the generated identity file, and no
//! author flags or `GIT_AUTHOR_*` / `GIT_COMMITTER_*` overrides. The recorded author and committer
//! come from `git log`, not from echoing config.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::git::{
    GitRepository, WORKSPACE_GIT_IDENTITY_CONFIG_PATH, workspace_git_identity_config,
};

const INCLUDED_NAME: &str = "Included Identity";
const INCLUDED_EMAIL: &str = "included@example.invalid";
const LOCAL_NAME: &str = "Destination Local";

static CAPTURE_ENV: Mutex<()> = Mutex::new(());

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-git-identity-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("fixture root");
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct CaptureEnv {
    _guard: MutexGuard<'static, ()>,
    previous: Vec<(&'static str, Option<OsString>)>,
}

impl CaptureEnv {
    fn lock(pairs: &[(&str, Option<&OsStr>)]) -> Self {
        let guard = CAPTURE_ENV.lock().expect("capture env lock");
        const KEYS: [&str; 11] = [
            "HOME",
            "GIT_CONFIG_GLOBAL",
            "GIT_CONFIG_SYSTEM",
            "GIT_CONFIG_NOSYSTEM",
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_PARAMETERS",
            "GIT_ATTR_NOSYSTEM",
            "GIT_AUTHOR_NAME",
            "GIT_AUTHOR_EMAIL",
            "GIT_COMMITTER_NAME",
            "GIT_COMMITTER_EMAIL",
        ];
        let previous = KEYS
            .iter()
            .map(|key| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        unsafe {
            for key in KEYS {
                std::env::remove_var(key);
            }
            for (key, value) in pairs {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                }
            }
        }
        Self {
            _guard: guard,
            previous,
        }
    }
}

impl Drop for CaptureEnv {
    fn drop(&mut self) {
        unsafe {
            for (key, value) in &self.previous {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

fn init_repo(path: &Path) -> PathBuf {
    fs::create_dir_all(path).expect("repository directory");
    git(
        path,
        ["init", "-q", "-b", "main", "."],
        IsolatedGit::setup(),
    );
    fs::canonicalize(path).expect("canonical repository")
}

fn git<I, S>(root: impl AsRef<Path>, args: I, isolated: IsolatedGit<'_>) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = git_output(root, args, isolated);
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("git output is UTF-8")
}

fn git_output<I, S>(root: impl AsRef<Path>, args: I, isolated: IsolatedGit<'_>) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new("git");
    command.env_clear();
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    command
        .arg("-C")
        .arg(root.as_ref())
        .args(args)
        .env("HOME", isolated.home)
        .env("GIT_CONFIG_GLOBAL", isolated.global)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    command.output().expect("run git")
}

#[derive(Clone, Copy)]
struct IsolatedGit<'a> {
    home: &'a Path,
    global: &'a Path,
}

impl IsolatedGit<'static> {
    fn setup() -> Self {
        Self {
            home: Path::new("/var/empty"),
            global: Path::new("/dev/null"),
        }
    }
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parent directory");
    }
    fs::write(path, contents).expect("write file");
}

fn source_local_config(source: &Path) -> Vec<u8> {
    fs::read(source.join(".git/config")).expect("read source local config")
}

fn recorded_ident(root: &Path, home: &Path, global: &Path) -> (String, String, String, String) {
    let log = git(
        root,
        ["log", "-1", "--format=%an%n%ae%n%cn%n%ce"],
        IsolatedGit { home, global },
    );
    let mut lines = log.lines();
    let author_name = lines.next().expect("author name").to_owned();
    let author_email = lines.next().expect("author email").to_owned();
    let committer_name = lines.next().expect("committer name").to_owned();
    let committer_email = lines.next().expect("committer email").to_owned();
    (author_name, author_email, committer_name, committer_email)
}

fn plain_commit(root: &Path, home: &Path, identity: &Path, file: &str, message: &str) {
    write_file(&root.join(file), message);
    git(
        root,
        ["add", "--", file],
        IsolatedGit {
            home,
            global: identity,
        },
    );
    let output = git_output(
        root,
        ["commit", "-q", "-m", message],
        IsolatedGit {
            home,
            global: identity,
        },
    );
    assert!(
        output.status.success(),
        "plain commit must succeed from captured identity; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// includeIf identity is captured, a destination local override still wins, and forking from a
/// workspace that already inherited identity does not need the original global file. Source config
/// is a snapshot, not a mutation.
#[tokio::test]
async fn captured_includeif_identity_authors_a_plain_commit_and_preserves_local_overrides() {
    let fixture = Fixture::new("includeif");
    let source = init_repo(&fixture.path("source"));
    let dest = init_repo(&fixture.path("dest"));
    let forked = init_repo(&fixture.path("forked"));
    let operator_home = fixture.path("operator-home");
    let dest_home = fixture.path("dest-home");
    let fork_home = fixture.path("fork-home");
    fs::create_dir_all(&operator_home).expect("operator home");
    fs::create_dir_all(&dest_home).expect("destination home");
    fs::create_dir_all(&fork_home).expect("fork home");

    let included = fixture.path("included.gitconfig");
    let global = fixture.path("operator.gitconfig");
    write_file(
        &included,
        &format!("[user]\n\tname = {INCLUDED_NAME}\n\temail = {INCLUDED_EMAIL}\n"),
    );
    write_file(
        &global,
        &format!(
            "[includeIf \"gitdir:{}/\"]\n\tpath = {}\n",
            source.display(),
            included.display()
        ),
    );

    let source_config_before = source_local_config(&source);
    let global_before = fs::read(&global).expect("read operator global");
    let included_before = fs::read(&included).expect("read included identity");

    {
        let _capture = CaptureEnv::lock(&[
            ("HOME", Some(operator_home.as_os_str())),
            ("GIT_CONFIG_GLOBAL", Some(global.as_os_str())),
            ("GIT_CONFIG_NOSYSTEM", Some(OsStr::new("1"))),
            ("GIT_ATTR_NOSYSTEM", Some(OsStr::new("1"))),
        ]);
        GitRepository::from_root(&dest)
            .inherit_identity_from(&source)
            .await
            .expect("capture includeIf identity from source");
    }

    let identity = dest.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
    assert!(
        identity.exists(),
        "capture must publish {}",
        WORKSPACE_GIT_IDENTITY_CONFIG_PATH
    );

    plain_commit(
        &dest,
        &dest_home,
        &identity,
        "inherited.txt",
        "from includeIf",
    );
    let (author_name, author_email, committer_name, committer_email) =
        recorded_ident(&dest, &dest_home, &identity);
    assert_eq!(author_name, INCLUDED_NAME);
    assert_eq!(author_email, INCLUDED_EMAIL);
    assert_eq!(committer_name, INCLUDED_NAME);
    assert_eq!(committer_email, INCLUDED_EMAIL);

    {
        let empty_home = fixture.path("empty-home");
        fs::create_dir_all(&empty_home).expect("empty home");
        let _capture = CaptureEnv::lock(&[
            ("HOME", Some(empty_home.as_os_str())),
            ("GIT_CONFIG_GLOBAL", Some(OsStr::new("/dev/null"))),
            ("GIT_CONFIG_NOSYSTEM", Some(OsStr::new("1"))),
            ("GIT_ATTR_NOSYSTEM", Some(OsStr::new("1"))),
        ]);
        GitRepository::from_root(&forked)
            .inherit_identity_from(&dest)
            .await
            .expect("capture already-inherited private identity");
    }

    let forked_identity = forked.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
    plain_commit(
        &forked,
        &fork_home,
        &forked_identity,
        "forked.txt",
        "from inherited private identity",
    );
    let (author_name, author_email, committer_name, committer_email) =
        recorded_ident(&forked, &fork_home, &forked_identity);
    assert_eq!(author_name, INCLUDED_NAME);
    assert_eq!(author_email, INCLUDED_EMAIL);
    assert_eq!(committer_name, INCLUDED_NAME);
    assert_eq!(committer_email, INCLUDED_EMAIL);

    git(
        &dest,
        ["config", "--local", "user.name", LOCAL_NAME],
        IsolatedGit::setup(),
    );
    plain_commit(&dest, &dest_home, &identity, "local.txt", "local override");
    let (author_name, author_email, committer_name, committer_email) =
        recorded_ident(&dest, &dest_home, &identity);
    assert_eq!(author_name, LOCAL_NAME);
    assert_eq!(author_email, INCLUDED_EMAIL);
    assert_eq!(committer_name, LOCAL_NAME);
    assert_eq!(committer_email, INCLUDED_EMAIL);

    assert_eq!(
        source_local_config(&source),
        source_config_before,
        "source local git config must be unchanged"
    );
    assert_eq!(
        fs::read(&global).expect("re-read operator global"),
        global_before,
        "operator global config must be unchanged"
    );
    assert_eq!(
        fs::read(&included).expect("re-read included identity"),
        included_before,
        "included identity file must be unchanged"
    );
}

/// Missing settings stay missing. An empty capture must not invent a name or email that would let
/// a destination commit succeed without an identity.
#[tokio::test]
async fn absent_source_identity_is_not_invented_for_a_destination_commit() {
    let fixture = Fixture::new("absent");
    let source = init_repo(&fixture.path("source"));
    let dest = init_repo(&fixture.path("dest"));
    let operator_home = fixture.path("operator-home");
    let dest_home = fixture.path("dest-home");
    fs::create_dir_all(&operator_home).expect("operator home");
    fs::create_dir_all(&dest_home).expect("destination home");
    let source_config_before = source_local_config(&source);

    {
        let _capture = CaptureEnv::lock(&[
            ("HOME", Some(operator_home.as_os_str())),
            ("GIT_CONFIG_GLOBAL", Some(OsStr::new("/dev/null"))),
            ("GIT_CONFIG_NOSYSTEM", Some(OsStr::new("1"))),
            ("GIT_ATTR_NOSYSTEM", Some(OsStr::new("1"))),
        ]);
        GitRepository::from_root(&dest)
            .inherit_identity_from(&source)
            .await
            .expect("absent identity is still a valid empty capture");
    }

    let identity = dest.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
    write_file(&dest.join("empty.txt"), "no identity\n");
    git(
        &dest,
        ["add", "--", "empty.txt"],
        IsolatedGit {
            home: &dest_home,
            global: &identity,
        },
    );
    // Without `user.useConfigOnly` git invents `<user>@<hostname>` wherever the
    // passwd gecos and a resolvable hostname allow it (hosted macOS runners), so
    // the refusal would depend on the host, not on the captured identity.
    let output = git_output(
        &dest,
        [
            "-c",
            "user.useConfigOnly=true",
            "commit",
            "-q",
            "-m",
            "should not invent identity",
        ],
        IsolatedGit {
            home: &dest_home,
            global: &identity,
        },
    );
    assert!(
        !output.status.success(),
        "a destination with no captured identity must not produce a commit"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("tell me who you are")
            || stderr.contains("empty ident")
            || stderr.contains("user.email")
            || stderr.contains("can't guess")
            || stderr.contains("auto-detection is disabled"),
        "git must refuse for missing identity, not another reason: {stderr}"
    );
    assert_eq!(
        source_local_config(&source),
        source_config_before,
        "source local git config must be unchanged"
    );
}

/// What the supervisor asks before it points a child's `GIT_CONFIG_GLOBAL` anywhere. A captured
/// file is offered, a workspace that never captured one is absent rather than an error, and a
/// planted symlink is refused by name — loading it as global configuration is the one way the
/// workspace boundary would leak into every child's Git.
#[test]
fn only_a_real_captured_identity_file_is_offered_to_a_child() {
    let fixture = Fixture::new("resolve");
    let workspace = fixture.path("workspace");
    fs::create_dir_all(workspace.join(".cowshed")).expect("workspace metadata");
    assert_eq!(
        workspace_git_identity_config(&workspace).expect("absent identity is not a failure"),
        None
    );

    let identity = workspace.join(WORKSPACE_GIT_IDENTITY_CONFIG_PATH);
    write_file(&identity, "[user]\n\tname = Captured\n");
    assert_eq!(
        workspace_git_identity_config(&workspace).expect("published identity"),
        Some(identity.clone())
    );

    let elsewhere = fixture.path("elsewhere");
    write_file(&elsewhere, "[user]\n\tname = Planted\n");
    fs::remove_file(&identity).expect("remove captured identity");
    std::os::unix::fs::symlink(&elsewhere, &identity).expect("plant identity symlink");
    let error = workspace_git_identity_config(&workspace)
        .expect_err("a symlinked identity config must be refused");
    assert!(
        error.message.contains("not a regular file"),
        "refusal must name what is wrong: {}",
        error.message
    );
}
