//! Controller-generated, ignored Git rewrites. Portable URLs come exclusively from the
//! adopted repository binding, never from mutable checkout config. Git directory discovery
//! runs under a narrowed child profile without ambient root-wide data reads. Gitdir,
//! common-dir and config/include indirection cannot widen the controller's discovery reads.
//! The final fetch still runs under the normal child boundary: routing is not a grant.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::{CowshedError, Result};
use crate::repository::RepositoryBinding;
use crate::sandbox::SandboxConfig;

pub const WORKSPACE_GIT_FETCH_CONFIG_PATH: &str = ".cowshed/git-fetch.inc";

#[derive(Clone, Debug, Eq, PartialEq)]
struct GitFetchMapping {
    portable_url: String,
    local_path: PathBuf,
}

fn failure(message: impl Into<String>) -> CowshedError {
    CowshedError::integrity(
        message,
        "repair the adopted repository binding or filesystem grants and retry",
    )
}

/// Paths are resolved before this predicate, against canonical controller grants. Do not
/// canonicalize a grant here: retargeting a granted symlink must not retarget its authority.
fn is_read_authorized(path: &Path, sandbox: &SandboxConfig) -> bool {
    crate::repository::is_lexically_canonical(path)
        && sandbox
            .grants
            .read
            .iter()
            .chain(&sandbox.grants.write)
            .map(PathBuf::as_path)
            .chain(sandbox.git_worktree_repository.as_deref())
            .any(|base| path.starts_with(base))
}

fn authorized_path(path: &Path, sandbox: &SandboxConfig) -> Option<PathBuf> {
    let resolved = std::fs::canonicalize(path).ok()?;
    is_read_authorized(&resolved, sandbox).then_some(resolved)
}

fn safe_url(url: &str) -> bool {
    if url.len() > 2048
        || crate::repository::normalize_remote_url(url).is_err()
        || url
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b < 0x20 || b == 0x7f || matches!(b, b'"' | b'\\'))
    {
        return false;
    }
    if let Some((scheme, rest)) = url.split_once("://") {
        let authority = rest.split('/').next().unwrap_or_default();
        if let Some((userinfo, _)) = authority.split_once('@') {
            // HTTPS usernames can themselves be tokens. Only SSH login names are addressing.
            return scheme == "ssh" && !userinfo.is_empty() && !userinfo.contains(':');
        }
    }
    true
}

fn safe_path(path: &Path) -> bool {
    path.is_absolute()
        && path.to_str().is_some_and(|text| {
            !text
                .bytes()
                .any(|b| b < 0x20 || b == 0x7f || matches!(b, b'"' | b'\\'))
        })
}

fn stripped_ssh_url(url: &str) -> Option<String> {
    let rest = url.strip_prefix("ssh://")?;
    let (authority, path) = rest.split_once('/')?;
    let (_, host) = authority.split_once('@')?;
    Some(format!("ssh://{host}/{path}"))
}

/// Every URL a bound identity's recorded URL routes: the URL itself, and for an SSH URL carrying a
/// login the same URL without it, which is how uv spells it. Git matches `insteadOf` values as
/// literal prefixes and derives neither spelling from the other.
pub(crate) fn fetch_route_keys(url: &str) -> impl Iterator<Item = std::borrow::Cow<'_, str>> {
    std::iter::once(std::borrow::Cow::Borrowed(url))
        .chain(stripped_ssh_url(url).map(std::borrow::Cow::Owned))
}

/// A URL (including uv's userinfo-stripped alias) must have exactly one target. Git's
/// equal-length insteadOf tie breaking is not repository selection policy.
fn render_git_fetch_config(mappings: &[GitFetchMapping]) -> Result<String> {
    let mut routes = BTreeMap::new();
    for mapping in mappings {
        if !safe_url(&mapping.portable_url) || !safe_path(&mapping.local_path) {
            return Err(failure("unsafe local Git fetch URL or path"));
        }
        for url in fetch_route_keys(&mapping.portable_url) {
            match routes.entry(url) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(&mapping.local_path);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if *entry.get() != &mapping.local_path {
                        return Err(failure(format!(
                            "ambiguous local Git fetch mapping for {}",
                            entry.key()
                        )));
                    }
                }
            }
        }
    }
    if routes.is_empty() {
        return Ok(String::new());
    }
    let mut output =
        String::from("# cowshed: generated local Git fetch mapping; never commit this file.\n");
    for (url, path) in routes {
        // Quote values as well as section names: '#' and ';' are Git config comments.
        output.push_str(&format!(
            "[url \"{}\"]\n\tinsteadOf = \"{url}\"\n",
            path.display()
        ));
    }
    Ok(output)
}

pub fn git_fetch_config_path(mount: &Path) -> PathBuf {
    mount.join(WORKSPACE_GIT_FETCH_CONFIG_PATH)
}

pub fn git_fetch_include_env(path: &Path) -> [(&'static str, &std::ffi::OsStr); 3] {
    [
        ("GIT_CONFIG_COUNT", std::ffi::OsStr::new("1")),
        ("GIT_CONFIG_KEY_0", std::ffi::OsStr::new("include.path")),
        ("GIT_CONFIG_VALUE_0", path.as_os_str()),
    ]
}

/// Filter the entire config injection family, not just COUNT. GIT_CONFIG_PARAMETERS is
/// independently interpreted by Git, and KEY/VALUE entries can survive a later count change.
pub(crate) fn caller_git_environment_allowed(name: &str) -> bool {
    name != "GIT_CONFIG"
        && !name.starts_with("GIT_CONFIG_")
        && !matches!(
            name,
            "GIT_DIR"
                | "GIT_COMMON_DIR"
                | "GIT_WORK_TREE"
                | "GIT_OBJECT_DIRECTORY"
                | "GIT_ALTERNATE_OBJECT_DIRECTORIES"
        )
}

const MAX_GIT_CONFIG_BYTES: u64 = 1024 * 1024;

fn publish_git_fetch_config(mount: &Path, mappings: &[GitFetchMapping]) -> Result<Option<PathBuf>> {
    let directory = mount.join(".cowshed");
    crate::storage::verify_no_symlinks(mount, &directory)
        .map_err(|error| failure(error.to_string()))?;
    let path = git_fetch_config_path(mount);
    let output = render_git_fetch_config(mappings)?;
    if output.is_empty() {
        match std::fs::remove_file(&path) {
            Ok(()) => cowshed_os::fsio::sync_directory(&directory)
                .map_err(|error| failure(format!("cannot sync Git mapping revocation: {error}")))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(failure(format!(
                    "cannot revoke Git mapping {}: {error}",
                    path.display()
                )));
            }
        }
        return Ok(None);
    }
    if output.len() as u64 > MAX_GIT_CONFIG_BYTES {
        return Err(failure("generated Git mapping exceeds its size bound"));
    }
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            let existing =
                crate::gateway_inventory::read_bytes_nofollow(&path, MAX_GIT_CONFIG_BYTES)
                    .map_err(failure)?;
            if existing == output.as_bytes() {
                return Ok(Some(path));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(failure(format!(
                "cannot inspect Git mapping {}: {error}",
                path.display()
            )));
        }
    }
    crate::metadata::write_atomic_bytes(&path, output.as_bytes())
        .map_err(|error| failure(error.to_string()))?;
    Ok(Some(path))
}

/// This probe, including every config Git might consult, is *already sandboxed*. No shell,
/// PATH lookup, user HOME, inherited Git variables, network operation, or credential copying.
/// Parsing only directory names also means a remote in an included config is never identity.
#[cfg(target_os = "macos")]
async fn probe_git(root: &Path, args: &[&str], sandbox: &SandboxConfig) -> Result<Vec<u8>> {
    use cowshed_os::fork_lock::Spawn as _;

    let profile =
        crate::sandbox::seatbelt_profile(sandbox, crate::sandbox::SandboxProfileRole::GitDiscovery)
            .map_err(|error| failure(error.to_string()))?;
    let profile = crate::exec::ProfileLaunch::write(&sandbox.profile_directory(), &profile)
        .map_err(|error| {
            failure(format!(
                "cannot write the Git probe's sandbox profile into {}: {error}",
                sandbox.profile_directory().display()
            ))
        })?;
    let mut command = tokio::process::Command::new(crate::exec::SANDBOX_EXEC);
    // The system selector names the active developer tools without consulting PATH.
    // /usr/bin/git is an xcrun shim that writes host caches even for read-only probes.
    command
        .args(profile.args())
        .arg(format!("{}/usr/bin/git", crate::sandbox::GIT_DEVELOPER_DIR))
        .arg("-C")
        .arg(root)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ])
        .args(args)
        .current_dir(&sandbox.workspace_mount)
        .env_clear()
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("HOME", sandbox.workspace_mount.join(".cowshed/home"))
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_COUNT", "0")
        .env("GIT_TERMINAL_PROMPT", "0");
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    crate::exec::prepare_child_descriptors(command.as_std_mut())
        .map_err(|error| failure(error.source.to_string()))?;
    let mut child = command
        .spawn_locked()
        .map_err(|error| failure(format!("cannot inspect local Git object store: {error}")))?;
    let (stdout, stderr, status) = collect_probe_output(&mut child).await.map_err(|error| {
        failure(format!(
            "cannot inspect local Git object store {}: {error}",
            root.display()
        ))
    })?;
    if !status.success() || !stderr.is_empty() {
        return Err(failure(format!(
            "cannot inspect local Git object store {}: {status}: {}",
            root.display(),
            String::from_utf8_lossy(&stderr)
        )));
    }
    Ok(stdout)
}

#[cfg(not(target_os = "macos"))]
async fn probe_git(root: &Path, args: &[&str], sandbox: &SandboxConfig) -> Result<Vec<u8>> {
    let _ = (root, args, sandbox);
    Err(CowshedError::environment_missing(
        "local Git discovery requires the native macOS sandbox runtime",
        "run the native cowshed controller on macOS",
    ))
}

#[cfg(any(target_os = "macos", test))]
const MAX_PROBE_PIPE_BYTES: u64 = 64 * 1024;

#[cfg(any(target_os = "macos", test))]
async fn read_probe_pipe(reader: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    reader
        .take(MAX_PROBE_PIPE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() as u64 > MAX_PROBE_PIPE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Git discovery output exceeds its byte bound",
        ));
    }
    Ok(bytes)
}

/// Both pipes are drained concurrently. A limit/error/deadline drops the other read,
/// kills the child, and reaps it before returning; partial output is never accepted.
#[cfg(any(target_os = "macos", test))]
async fn collect_probe_output(
    child: &mut tokio::process::Child,
) -> std::io::Result<(Vec<u8>, Vec<u8>, std::process::ExitStatus)> {
    let stdout = child
        .stdout
        .take()
        .expect("Git probe configured a stdout pipe");
    let stderr = child
        .stderr
        .take()
        .expect("Git probe configured a stderr pipe");
    // WHY unbounded: the 10 s timeout around it ends it.
    #[allow(clippy::disallowed_methods)]
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::try_join!(
            read_probe_pipe(stdout),
            read_probe_pipe(stderr),
            child.wait()
        )
    })
    .await
    .unwrap_or_else(|_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Git discovery timed out",
        ))
    });
    match result {
        Ok(output) => Ok(output),
        Err(error) => {
            let killed = child.start_kill();
            let reaped = cowshed_os::deadline::reap_async(
                "git discovery probe",
                child,
                &cowshed_os::deadline::start(
                    "reap git discovery probe",
                    cowshed_os::deadline::REAP,
                ),
                cowshed_os::deadline::Progress::Signalled {
                    signal: libc::SIGKILL,
                },
            )
            .await;
            match (killed, reaped) {
                (Ok(()), Ok(_)) => Err(error),
                (kill, reap) => Err(std::io::Error::other(format!(
                    "{error}; kill result: {kill:?}; reap result: {reap:?}"
                ))),
            }
        }
    }
}

/// The object store of the checkout at `root`, when every directory its Git reaches is granted
/// to the sandbox. Every spawn of a workspace that holds read or write grants asks it of every
/// project's checkout (04_sandbox.md, "Exec pipeline" step 4), so its probes are a kept reading
/// (11_shell.md, "Kept readings"): with a `.git` directory of its own and no alternates, they
/// read the checkout's configuration ([`crate::capabilities::local_config_inputs`]), object
/// directory and alternates file and nothing that can change while this process runs. Git work in
/// that checkout (an `index.lock`, a new object directory) moves `.git` or `.git/objects`, so an
/// actively used checkout is probed again for a few seconds after it; an idle sibling is kept.
/// Measured 2026-10-30: in a workspace whose grants name two other projects' checkouts, every
/// warm exec paid two sandboxed `git rev-parse` processes.
async fn repository_object_store(root: &Path, sandbox: &SandboxConfig) -> Result<Option<PathBuf>> {
    type Key = (PathBuf, Vec<PathBuf>, Vec<PathBuf>, Option<PathBuf>);
    static KEPT: std::sync::LazyLock<crate::input_memo::InputMemo<Key, Option<PathBuf>>> =
        std::sync::LazyLock::new(crate::input_memo::InputMemo::new);
    let Some(root) = authorized_path(root, sandbox) else {
        return Ok(None);
    };
    if std::fs::canonicalize(&sandbox.workspace_mount)
        .ok()
        .as_ref()
        == Some(&root)
    {
        return Ok(None);
    }
    let Some(inputs) = probed_inputs(&root) else {
        return probed_object_store(root, sandbox).await;
    };
    let key = (
        root.clone(),
        sandbox.grants.read.clone(),
        sandbox.grants.write.clone(),
        sandbox.git_worktree_repository.clone(),
    );
    KEPT.read_async(&key, &inputs, || probed_object_store(root, sandbox))
        .await
}

/// What [`probed_object_store`] reads of the checkout at `root` beyond the grants, when that is
/// its own `.git` directory's configuration ([`crate::capabilities::local_config_inputs`]),
/// object directory and alternates file alone: `None` for a `.git` file (a linked worktree), an
/// alternates file (whose stores may name more), or a configuration not kept, each probed every
/// time.
fn probed_inputs(root: &Path) -> Option<Vec<PathBuf>> {
    let git = root.join(".git");
    if !std::fs::symlink_metadata(&git).ok()?.is_dir() {
        return None;
    }
    let alternates = git.join("objects/info/alternates");
    if alternates.symlink_metadata().is_ok() {
        return None;
    }
    let mut inputs = crate::capabilities::local_config_inputs(&git)?;
    inputs.extend([git.join("objects"), alternates]);
    Some(inputs)
}

/// [`repository_object_store`] asked of Git: its own paths, each granted, and every alternate
/// store it traverses granted too.
async fn probed_object_store(root: PathBuf, sandbox: &SandboxConfig) -> Result<Option<PathBuf>> {
    let output = probe_git(
        &root,
        &[
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--absolute-git-dir",
            "--git-common-dir",
            "--git-path",
            "objects",
        ],
        sandbox,
    )
    .await?;
    let Ok(text) = std::str::from_utf8(&output) else {
        return Ok(None);
    };
    let mut lines = text.lines();
    let mut resolved: [PathBuf; 4] = std::array::from_fn(|_| PathBuf::new());
    for slot in &mut resolved {
        let Some(path) = lines
            .next()
            .and_then(|line| authorized_path(Path::new(line), sandbox))
        else {
            return Ok(None);
        };
        *slot = path;
    }
    if lines.next().is_some() || resolved[0] != root {
        return Ok(None);
    }
    // Ask Git to traverse its own alternate-store graph, but only inside the discovery profile.
    // Every reported store needs its own grant; warnings (e.g. unreadable alternates) fail
    // the probe instead of turning partial discovery into authorization.
    if resolved[3]
        .join("info/alternates")
        .try_exists()
        .map_err(|error| failure(error.to_string()))?
    {
        let output = probe_git(&root, &["count-objects", "-v"], sandbox).await?;
        let Ok(text) = std::str::from_utf8(&output) else {
            return Ok(None);
        };
        for alternate in text
            .lines()
            .filter_map(|line| line.strip_prefix("alternate: "))
        {
            if authorized_path(Path::new(alternate), sandbox).is_none() {
                return Ok(None);
            }
        }
    }
    Ok(Some(root))
}

/// Called host-side immediately before child spawn. The registry, binding and main image
/// sidecar are controller authority, read from the store the sandbox's launcher serves and from
/// no other (04_sandbox.md, "Exec pipeline" step 4). A checkout's mutable remotes cannot invent
/// routes.
pub async fn refresh_git_fetch_config(sandbox: &SandboxConfig) -> Result<Option<PathBuf>> {
    let store = sandbox.store_root.as_path();
    crate::sandbox::validate_sandbox_config(sandbox).map_err(|error| failure(error.to_string()))?;
    let mut mappings = Vec::new();
    // No external read capability means no discovery and no dependency on store availability.
    if !sandbox.grants.read.is_empty() || !sandbox.grants.write.is_empty() {
        for repo in crate::gateway_inventory::discover_repositories(store)
            .map_err(|error| failure(error.to_string()))?
        {
            let layout =
                crate::storage::StorageLayout::with_mount_root(store, &sandbox.mount_root, &repo)
                    .map_err(|error| failure(error.to_string()))?;
            let Some(checkout) =
                crate::gateway_inventory::authoritative_checkout_path(&layout, &repo)
                    .map_err(|error| failure(error.to_string()))?
            else {
                continue;
            };
            let Some(root) = repository_object_store(&checkout, sandbox).await? else {
                continue;
            };
            crate::storage::verify_no_symlinks(store, &layout.project().project_root)
                .map_err(|error| failure(error.to_string()))?;
            let binding: RepositoryBinding = crate::gateway_inventory::read_typed_json_nofollow(
                &layout.project().repository_binding,
                crate::gateway_inventory::MAX_BINDING_BYTES,
            )
            .map_err(failure)?;
            if binding
                .primary()
                .map_err(|error| failure(error.to_string()))?
                .repo_id
                != repo
            {
                return Err(failure(
                    "local Git fetch binding changed identity during discovery",
                ));
            }
            for identity in binding.identities {
                if let Some(url) = identity.remote_url {
                    mappings.push(GitFetchMapping {
                        portable_url: url,
                        local_path: root.clone(),
                    });
                }
            }
        }
    }
    publish_git_fetch_config(&sandbox.workspace_mount, &mappings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Spawn as _;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::CommandBackstop as _;

    fn mapping(url: &str, path: &str) -> GitFetchMapping {
        GitFetchMapping {
            portable_url: url.into(),
            local_path: path.into(),
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_uv_alias_and_duplicate_routes_have_one_target() {
        let root = scratch();
        std::fs::create_dir(root.join(".cowshed")).expect("workspace metadata");
        let repository = root.join("repository");
        std::fs::create_dir(&repository).expect("repository");
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ],
        );
        let exact = "ssh://git@example.invalid/team/repo.git";
        let bare = "ssh://example.invalid/team/repo.git";
        let route = mapping(exact, repository.to_str().expect("path"));
        let path = publish_git_fetch_config(&root, &[route.clone(), route])
            .expect("publish")
            .expect("path");
        let expected = remote_head(&root, &path, repository.to_str().expect("path"))
            .await
            .expect("local HEAD");
        assert_eq!(
            remote_head(&root, &path, exact)
                .await
                .expect("exact SSH URL"),
            expected
        );
        assert_eq!(
            remote_head(&root, &path, bare).await.expect("uv SSH alias"),
            expected
        );
        assert!(
            render_git_fetch_config(&[mapping(exact, "/srv/one"), mapping(bare, "/srv/two"),])
                .is_err()
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn credentials_and_config_injection_are_rejected() {
        for url in [
            "https://user:secret@example.invalid/team/repo.git",
            "https://token@example.invalid/team/repo.git",
            "ssh://git@example.invalid/team/repo.git\n[include]",
        ] {
            assert!(render_git_fetch_config(&[mapping(url, "/srv/repo")]).is_err());
        }
        for key in [
            "GIT_CONFIG_PARAMETERS",
            "GIT_CONFIG",
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_KEY_99",
            "GIT_CONFIG_VALUE_99",
            "GIT_DIR",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        ] {
            assert!(!caller_git_environment_allowed(key));
        }
    }

    fn scratch() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::fs::canonicalize(std::env::temp_dir())
            .expect("temp")
            .join(format!(
                "cowshed-git-fetch-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
        std::fs::create_dir(&root).expect("root");
        root
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_relocation_revocation_and_unchanged_publication_are_observable() {
        use std::os::unix::fs::MetadataExt;
        let root = scratch();
        std::fs::create_dir(root.join(".cowshed")).expect("workspace metadata");
        let old = root.join("old");
        let new = root.join("new");
        for (repository, message) in [(&old, "old"), (&new, "new")] {
            std::fs::create_dir(repository).expect("repository");
            git(repository, &["init", "-q"]);
            git(
                repository,
                &[
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    message,
                ],
            );
        }
        let url = "https://example.invalid/team/repo.git";
        let routes = [mapping(url, old.to_str().expect("old path"))];
        let path = publish_git_fetch_config(&root, &routes)
            .expect("publish")
            .expect("path");
        let expected_old = remote_head(&root, &path, old.to_str().expect("old path"))
            .await
            .expect("old HEAD");
        assert_eq!(
            remote_head(&root, &path, url).await.expect("portable URL"),
            expected_old
        );
        let before = std::fs::metadata(&path).expect("published metadata");
        publish_git_fetch_config(&root, &routes).expect("unchanged publish");
        let after = std::fs::metadata(&path).expect("unchanged metadata");
        assert_eq!(
            before.ino(),
            after.ino(),
            "unchanged mapping must not be atomically replaced"
        );
        assert_eq!(
            (before.mtime(), before.mtime_nsec()),
            (after.mtime(), after.mtime_nsec())
        );
        publish_git_fetch_config(&root, &[mapping(url, new.to_str().expect("new path"))])
            .expect("relocate");
        let expected_new = remote_head(&root, &path, new.to_str().expect("new path"))
            .await
            .expect("new HEAD");
        assert_ne!(
            expected_old, expected_new,
            "relocation must change the served commit"
        );
        assert_eq!(
            remote_head(&root, &path, url).await.expect("relocated URL"),
            expected_new
        );
        assert_eq!(publish_git_fetch_config(&root, &[]).expect("revoke"), None);
        assert!(
            remote_head(&root, &path, url).await.is_err(),
            "revoked route must not fall back to network"
        );
        std::fs::create_dir(&path).expect("block revocation");
        assert!(publish_git_fetch_config(&root, &[]).is_err());
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn publication_does_not_follow_metadata_directory_symlink() {
        let root = scratch();
        let outside = scratch();
        std::os::unix::fs::symlink(&outside, root.join(".cowshed")).expect("link");
        assert!(
            publish_git_fetch_config(
                &root,
                &[mapping(
                    "https://example.invalid/team/repo.git",
                    "/srv/repo"
                )]
            )
            .is_err()
        );
        assert!(!outside.join("git-fetch.inc").exists());
        std::fs::remove_dir_all(root).expect("cleanup");
        std::fs::remove_dir_all(outside).expect("cleanup");
    }

    /// Routes come from the store the sandbox's launcher serves, never the host's: a fixture
    /// whose own store is not there is refused by that store's path, where reading the host
    /// store would have answered from another installation's bindings (04_sandbox.md, "Exec
    /// pipeline" step 4).
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn routes_are_read_from_the_store_the_sandbox_serves() {
        // Under the canonical /tmp parent: a grant inside the cowshed store is refused before any
        // store is read (08_testing.md, "Fixtures").
        let root = crate::temp_root::TempRoot::new("cowshed-git-fetch-store");
        let checkout = root.join("checkout");
        std::fs::create_dir(&checkout).expect("checkout");
        let config = sandbox(&root, &checkout);
        std::fs::create_dir_all(config.workspace_mount.join(".cowshed"))
            .expect("workspace metadata");
        let error = refresh_git_fetch_config(&config)
            .await
            .expect_err("a store that is not there is not read around");
        assert!(
            error
                .message
                .contains(&config.store_root.display().to_string()),
            "{}",
            error.message
        );
    }

    #[cfg(target_os = "macos")]
    fn sandbox(root: &Path, checkout: &Path) -> SandboxConfig {
        SandboxConfig {
            home: root.join("home"),
            mount_root: root.join("mounts"),
            workspace_mount: root.join("mounts/workspace"),
            shed_links: Vec::new(),
            exec_temp_dir: root.join("temp"),
            store_root: root.join("store"),
            port_block: crate::metadata::PortBlock::new(49_136, 16).expect("ports"),
            retained_port_blocks: Vec::new(),
            cpu_pool_directory: None,
            mode: crate::sandbox::RunSandboxMode::ReadWrite,
            grants: crate::sandbox::SandboxGrants {
                read: vec![checkout.to_owned()],
                write: Vec::new(),
                deny_write: Vec::new(),
                deny: Vec::new(),
                egress: Vec::new(),
                loopback_any: false,
            },
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            git_worktree_repository: None,
            main_mount: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            program_directories: Vec::new(),
            capabilities: Default::default(),
        }
    }

    #[cfg(target_os = "macos")]
    fn git(root: &Path, args: &[&str]) {
        let output = std::process::Command::new("/usr/bin/git")
            .arg("-C")
            .arg(root)
            .args(args)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output_bounded()
            .expect("git");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Exercise Git's actual rewrite parser and local upload-pack. Non-file protocols are
    /// disabled so an unusable include can never accidentally pass via a network fetch.
    #[cfg(target_os = "macos")]
    async fn remote_head(cwd: &Path, config: &Path, url: &str) -> Result<Vec<u8>> {
        let mut child = tokio::process::Command::new("/usr/bin/git")
            .args([
                "-c",
                "protocol.allow=never",
                "-c",
                "protocol.file.allow=always",
                "ls-remote",
                url,
                "HEAD",
            ])
            .current_dir(cwd)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .envs(git_fetch_include_env(config))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("Git ls-remote");
        let (stdout, stderr, status) = collect_probe_output(&mut child)
            .await
            .map_err(|error| failure(error.to_string()))?;
        if !status.success() {
            return Err(failure(format!(
                "local ref lookup failed: {}",
                String::from_utf8_lossy(&stderr)
            )));
        }
        Ok(stdout)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn excessive_probe_output_is_refused_and_the_process_is_reaped() {
        // `yes` through the shell's PATH: NixOS runners have no /usr/bin/yes.
        for script in ["exec yes", "exec yes >&2"] {
            let mut child = tokio::process::Command::new("/bin/sh")
                .args(["-c", script])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn_locked()
                .expect("unbounded output process");
            assert!(
                collect_probe_output(&mut child).await.is_err(),
                "partial output must be refused"
            );
            assert!(
                child.try_wait().expect("reaped child").is_some(),
                "collector must not leave the writer running"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_gitdir_symlink_and_worktree_common_store_require_real_grants() {
        let root = scratch();
        let checkout = root.join("checkout");
        std::fs::create_dir(&checkout).expect("checkout");
        git(&checkout, &["init", "-q"]);
        git(
            &checkout,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ],
        );
        let mut config = sandbox(&root, &checkout);
        std::fs::create_dir_all(&config.workspace_mount).expect("mount");
        assert_eq!(
            repository_object_store(&checkout, &config)
                .await
                .expect("probe"),
            Some(checkout.clone())
        );
        let outside = root.join("outside");
        std::fs::rename(checkout.join(".git"), &outside).expect("move gitdir");
        std::os::unix::fs::symlink(&outside, checkout.join(".git")).expect("gitdir link");
        assert!(!matches!(
            repository_object_store(&checkout, &config).await,
            Ok(Some(_))
        ));
        config.grants.read.push(outside.clone());
        assert_eq!(
            repository_object_store(&checkout, &config)
                .await
                .expect("granted store"),
            Some(checkout.clone())
        );
        let worktree = root.join("worktree");
        git(
            &checkout,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                worktree.to_str().expect("path"),
            ],
        );
        config.grants.read = vec![worktree.clone()];
        // Git may fail reading the ungranted gitdir before it can report paths. Either refusal
        // is closed; it must never publish a route from a worktree-only grant.
        assert!(!matches!(
            repository_object_store(&worktree, &config).await,
            Ok(Some(_))
        ));
        config.grants.read.push(outside);
        assert_eq!(
            repository_object_store(&worktree, &config)
                .await
                .expect("common store grant"),
            Some(worktree)
        );
        config.grants.read = vec![checkout.join("subdir")];
        assert_eq!(
            repository_object_store(&checkout, &config)
                .await
                .expect("subdir grant"),
            None
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_denied_included_config_is_not_read_by_privileged_discovery() {
        let root = scratch();
        let checkout = root.join("checkout");
        std::fs::create_dir(&checkout).expect("checkout");
        git(&checkout, &["init", "-q"]);
        let mut config = sandbox(&root, &checkout);
        std::fs::create_dir_all(&config.workspace_mount).expect("mount");
        let denied = root.join("secret");
        std::fs::create_dir(&denied).expect("secret dir");
        let included = denied.join("gitconfig");
        // An unsandboxed probe would succeed. Only the actual denied-path boundary can
        // make the first probe fail; granting the same file must restore the route.
        std::fs::write(&included, "[core]\n\tbare = false\n").expect("include");
        git(
            &checkout,
            &["config", "include.path", included.to_str().expect("path")],
        );
        assert!(repository_object_store(&checkout, &config).await.is_err());
        config.additional_denies.push(denied.clone());
        assert!(repository_object_store(&checkout, &config).await.is_err());
        config.additional_denies.clear();
        config.grants.read.push(denied);
        assert_eq!(
            repository_object_store(&checkout, &config)
                .await
                .expect("granted include"),
            Some(checkout)
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_unadopted_mutable_remote_claims_cannot_create_routes() {
        let root = scratch();
        let checkout = root.join("checkout");
        let store = root.join("store");
        std::fs::create_dir(&checkout).expect("checkout");
        std::fs::create_dir(&store).expect("store");
        git(&checkout, &["init", "-q"]);
        git(
            &checkout,
            &[
                "remote",
                "add",
                "origin",
                "https://example.invalid/team/claimed.git",
            ],
        );
        let config = sandbox(&root, &checkout);
        std::fs::create_dir_all(config.workspace_mount.join(".cowshed"))
            .expect("workspace metadata");
        // A previous route must be revoked, not retained because discovery found nothing.
        publish_git_fetch_config(
            &config.workspace_mount,
            &[mapping(
                "https://example.invalid/team/claimed.git",
                checkout.to_str().expect("path"),
            )],
        )
        .expect("stale route");
        assert_eq!(
            refresh_git_fetch_config(&config).await.expect("refresh"),
            None
        );
        assert!(!git_fetch_config_path(&config.workspace_mount).exists());
        // Mutable Git config is not consulted even when the checkout holds a read grant.
        std::fs::write(checkout.join(".git/config"), "[broken\n").expect("poison config");
        assert_eq!(
            refresh_git_fetch_config(&config)
                .await
                .expect("no Git config discovery"),
            None
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    async fn host_controller_alternate_object_stores_need_their_own_grants() {
        let root = scratch();
        let source = root.join("source");
        let checkout = root.join("checkout");
        std::fs::create_dir(&source).expect("source");
        git(&source, &["init", "-q"]);
        git(
            &source,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ],
        );
        git(
            &root,
            &[
                "clone",
                "-q",
                "--shared",
                source.to_str().expect("source"),
                checkout.to_str().expect("checkout"),
            ],
        );
        let mut config = sandbox(&root, &checkout);
        std::fs::create_dir_all(&config.workspace_mount).expect("mount");
        assert!(!matches!(
            repository_object_store(&checkout, &config).await,
            Ok(Some(_))
        ));
        config.grants.read.push(source);
        assert_eq!(
            repository_object_store(&checkout, &config)
                .await
                .expect("alternate granted"),
            Some(checkout)
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    /// A checkout's object-store probes are kept only while what they read is its own `.git`
    /// directory's configuration, the files it includes unconditionally among its inputs, and
    /// its object directory and alternates file: a linked worktree's `.git` file, a `commondir`,
    /// a per-worktree configuration, a conditional or nested include, or an alternates file is
    /// probed every time.
    #[test]
    fn only_a_plain_git_directory_keeps_its_probe() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-git-probe-inputs-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let git = root.join(".git");
        std::fs::create_dir_all(git.join("objects/info")).unwrap();
        std::fs::write(git.join("config"), "[core]\n\tbare = false\n").unwrap();
        assert_eq!(
            probed_inputs(&root),
            Some(vec![
                git.join("config"),
                git.join("commondir"),
                git.join("objects"),
                git.join("objects/info/alternates")
            ])
        );
        std::fs::write(
            git.join("config"),
            "[core]\n[Include]\n\tpath = ../tooling/shared.gitconfig ; shared\n",
        )
        .unwrap();
        assert_eq!(
            probed_inputs(&root),
            Some(vec![
                git.join("config"),
                git.join("../tooling/shared.gitconfig"),
                git.join("commondir"),
                git.join("objects"),
                git.join("objects/info/alternates")
            ]),
            "an included file, absent, is an input"
        );
        std::fs::create_dir_all(root.join("tooling")).unwrap();
        std::fs::write(
            root.join("tooling/shared.gitconfig"),
            "[include]\n\tpath = more\n",
        )
        .unwrap();
        assert_eq!(probed_inputs(&root), None, "a nested include");
        for (config, case) in [
            (
                "\u{feff}[extensions]\n\tworktreeConfig = true\n",
                "a per-worktree configuration behind a byte-order mark",
            ),
            (
                "[include]\n\tpath = %(prefix)/etc/gitconfig\n",
                "an install-prefix include",
            ),
            (
                "[includeIf \"gitdir:/x/\"]\n\tpath = other\n",
                "a conditional include",
            ),
            (
                "[extensions]\n\tworktreeConfig = true\n",
                "a per-worktree configuration",
            ),
            (
                "[include]\n\tpath = ~/shared\n",
                "an include through a home directory",
            ),
            (
                "[include] path = other\n",
                "a variable on its section's header line",
            ),
        ] {
            std::fs::write(git.join("config"), config).unwrap();
            assert_eq!(probed_inputs(&root), None, "{case}");
        }
        std::fs::write(git.join("config"), "[core]\n").unwrap();
        std::fs::write(git.join("commondir"), "../shared\n").unwrap();
        assert_eq!(probed_inputs(&root), None, "a commondir");
        std::fs::remove_file(git.join("commondir")).unwrap();
        std::fs::write(git.join("objects/info/alternates"), "/elsewhere/objects\n").unwrap();
        assert_eq!(probed_inputs(&root), None, "alternates");
        std::fs::remove_dir_all(&git).unwrap();
        std::fs::write(&git, "gitdir: /elsewhere/.git/worktrees/w\n").unwrap();
        assert_eq!(probed_inputs(&root), None, "a linked worktree");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
