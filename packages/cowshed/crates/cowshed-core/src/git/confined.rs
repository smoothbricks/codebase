//! Controller Git's confinement, decided once for every platform: the trees a command run in a
//! checkout reads and writes and whether it may execute anything but Git, as the sandbox
//! configuration each platform's enforcer renders (04_sandbox.md, "Controller Git is separately
//! confined"). The filters, merge drivers and transports a repository selects write only inside
//! that checkout and read only the trees the controller names.
//!
//! [`controller_git_policy`] is the one owner of the grant list and reads nothing. The platform
//! supplies the Git program a launch starts and nothing else: the launcher, its profile and the
//! profile's file are [`crate::sandbox`]'s and [`crate::exec`]'s, the same for every caller.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use cowshed_os::fork_lock::Run as _;

use super::{
    driver_overrides, git_command_at, git_ended, git_spawn_error, platform,
    workspace_git_identity_config,
};
use crate::error::{CowshedError, Result};
use crate::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole};

/// Controller Git at `root` for the repository's own operations, before its verb and before
/// `alternate_objects` is attached to its environment: confined to the checkout, reading the
/// object store in place and writing `writable` besides.
pub(super) fn controller_git_command(
    store_root: &Path,
    root: &Path,
    alternate_objects: Option<&Path>,
    writable: Option<&Path>,
) -> Result<std::process::Command> {
    confined_git_command(
        store_root,
        root,
        alternate_objects.map(ExtraRead::AlternateObjects),
        writable,
    )
}

/// Controller Git at `root` for an operation that consumes repository-selected filters, merge
/// drivers and transports: confined to the checkout, with no tree outside it granted.
pub(super) fn sandboxed_git_command(
    store_root: &Path,
    root: &Path,
) -> Result<std::process::Command> {
    confined_git_command(store_root, root, None, None)
}

/// [`sandboxed_git_command`] for a fetch from `source`, the one tree outside the checkout it reads.
pub(super) fn sandboxed_git_fetch_command(
    store_root: &Path,
    root: &Path,
    source: &Path,
) -> Result<std::process::Command> {
    confined_git_command(store_root, root, Some(ExtraRead::FetchSource(source)), None)
}

/// A tree outside the checkout that controller Git reads, as the controller names it: never a
/// path the repository's own config or remote URL chose.
#[derive(Clone, Copy, Debug)]
pub(super) enum ExtraRead<'a> {
    /// A checkout fetched from by local path. Git runs `upload-pack` for it through a shell.
    FetchSource(&'a Path),
    /// Another repository's object store, read in place through `GIT_ALTERNATE_OBJECT_DIRECTORIES`.
    AlternateObjects(&'a Path),
}

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

/// A checkout as the controller read it from disk, before any grant is decided.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Checkout {
    /// Its resolved path. Seatbelt filters the resolved spelling and Landlock the inode it
    /// names, so a grant on a lexical name that leads through a link admits nothing.
    pub(super) root: PathBuf,
    /// A linked worktree's common Git directory, when the registration there points back at
    /// `root` ([`registered_worktree_git_dir`]).
    pub(super) common: Option<PathBuf>,
    /// The files of main's checkout that `common`'s config includes
    /// ([`included_config_files`]).
    pub(super) includes: Vec<PathBuf>,
}

/// What one confined controller Git reaches beyond the system's programs: every platform's
/// enforcer is rendered from this one value ([`Self::sandbox_config`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ControllerGitPolicy {
    /// The checkout: read and written whole, the command's working directory.
    root: PathBuf,
    /// Whether Git may execute what it reads, or only Git's own executables.
    role: SandboxProfileRole,
    /// The files and trees outside the checkout it reads and never writes.
    read: Vec<PathBuf>,
    /// The trees outside the checkout it reads and writes.
    write: Vec<PathBuf>,
    /// Whether the checkout is a linked worktree: every driver its config names is switched off.
    linked: bool,
}

/// Decide what controller Git in `checkout` reaches. `extra` and `writable` are paths the
/// controller named, each granted as `resolve` spells it; nothing else is read.
///
/// - The checkout is read and written whole.
/// - A linked worktree writes its common Git directory, the object store it commits into, and
///   reads the files of main's checkout that directory's config includes.
/// - `extra` is read; `writable` is read and written.
/// - Reads of main's checkout are ones the workspace's jobs cannot make, so no program the
///   config selects may run beside them, whatever name it is selected by, or whenever it is
///   added: the role is then the one that executes only Git. A controller-named tree (`extra`)
///   is not among them: it is one the controller chose. An object store is read in place and
///   runs nothing. A local fetch runs Git's upload-pack through a shell, which a Git that may
///   execute only Git cannot start, so the two never meet: such a fetch is refused here, by
///   name, before Git runs (02_workspaces.md, "Git-worktree workspaces").
pub(super) fn controller_git_policy(
    checkout: Checkout,
    extra: Option<ExtraRead<'_>>,
    writable: Option<&Path>,
    resolve: impl Fn(&Path) -> std::io::Result<PathBuf>,
) -> Result<ControllerGitPolicy> {
    let Checkout {
        root,
        common,
        includes: mut read,
    } = checkout;
    if let Some(ExtraRead::FetchSource(source)) = extra
        && !read.is_empty()
    {
        return Err(CowshedError::sandbox_denied(
            format!(
                "a linked worktree whose main config includes main-tree files cannot fetch: {} reads {} from main's checkout through those includes, so its Git may execute only Git, and fetching from {} runs upload-pack through a shell",
                root.display(),
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
        read.push(resolve(extra.path()).map_err(|error| {
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
    let linked = common.is_some();
    let write = writable
        .map(|path| {
            resolve(path).map_err(|error| {
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
        .collect();
    Ok(ControllerGitPolicy {
        root,
        role,
        read,
        write,
        linked,
    })
}

impl ControllerGitPolicy {
    /// The authority tier the profile is rendered for.
    pub(super) fn role(&self) -> SandboxProfileRole {
        self.role
    }

    /// The sandbox configuration every platform renders this policy from: the checkout as the
    /// workspace mount and the exec temp dir, the grants, and nothing a workspace's jobs get
    /// besides (no capability, socket, cache, CPU pool or build volume). `store_root` is the
    /// store this controller serves, where the profile's file lives: the host's in production, a
    /// fixture's own in a test, never one it does not serve (04_sandbox.md, "Exec pipeline" step
    /// 4; 08_testing.md, "Fixtures"). `home` is the operator's, whose secrets stay closed.
    pub(super) fn sandbox_config(&self, store_root: &Path, home: PathBuf) -> SandboxConfig {
        SandboxConfig {
            home,
            mount_root: PathBuf::from(cowshed_gateway_types::STORE_ROOT),
            workspace_mount: self.root.clone(),
            shed_links: Vec::new(),
            exec_temp_dir: self.root.clone(),
            store_root: store_root.to_path_buf(),
            // The loopback ports a Seatbelt profile names. A Linux launch has a loopback of its
            // own and compiles no port.
            port_block: Some(
                crate::metadata::PortBlock::new(40_960, 16).expect("a static, aligned port block"),
            ),
            retained_port_blocks: Vec::new(),
            cpu_pool_directory: None,
            mode: RunSandboxMode::ReadWrite,
            grants: SandboxGrants {
                read: self.read.clone(),
                write: self.write.clone(),
                ..SandboxGrants::default()
            },
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            git_worktree_repository: None,
            main_mount: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            // Controller Git detects no capability, so it resolves no program.
            program_directories: Vec::new(),
            capabilities: Default::default(),
        }
    }
}

/// The HOME controller Git in the checkout `root` runs with: inside the checkout, never the
/// operator's.
fn git_home(root: &Path) -> PathBuf {
    root.join(".cowshed/home")
}

/// `root` as it resolves. A checkout that does not exist yet resolves through its parent, so
/// the command Git then fails is still built for the name it will have.
fn resolved_checkout(root: &Path) -> Result<PathBuf> {
    let parent = root
        .parent()
        .ok_or_else(|| CowshedError::internal("git checkout has no parent"))?;
    // Temporary checkouts often arrive through a link (/var -> /private/var on macOS);
    // granting only their lexical name denies cwd.
    match fs::canonicalize(root) {
        Ok(canonical) => Ok(canonical),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(fs::canonicalize(parent)
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
            )),
        Err(error) => Err(CowshedError::integrity(
            format!("cannot resolve git checkout {}: {error}", root.display()),
            "repair the checkout path",
        )),
    }
}

/// Git in the checkout `root` under the platform's sandbox launcher, before its verb: the
/// checkout read from disk, [`controller_git_policy`] rendered into a profile in `store_root`,
/// and [`git_command_at`]'s arguments and environment behind the launcher's.
fn confined_git_command(
    store_root: &Path,
    root: &Path,
    extra: Option<ExtraRead<'_>>,
    writable: Option<&Path>,
) -> Result<std::process::Command> {
    let canonical = resolved_checkout(root)?;
    let parent = canonical
        .parent()
        .ok_or_else(|| CowshedError::internal("git checkout has no parent"))?
        .to_owned();
    let home = std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
        CowshedError::environment_missing(
            "HOME is unset",
            "set HOME to the operator's home directory",
        )
    })?;
    let common = registered_worktree_git_dir(&canonical)?;
    let git_home = git_home(&canonical);
    let includes = match &common {
        Some(common) => included_config_files(common, &canonical, &git_home)?,
        None => Vec::new(),
    };
    let policy = controller_git_policy(
        Checkout {
            root: canonical.clone(),
            common,
            includes,
        },
        extra,
        writable,
        |path| fs::canonicalize(path),
    )?;
    let config = policy.sandbox_config(store_root, home);
    let profile = crate::sandbox::seatbelt_profile(&config, policy.role()).map_err(|error| {
        CowshedError::integrity(
            format!(
                "cannot sandbox controller git in {}: {error}",
                root.display()
            ),
            "repair the workspace Git path",
        )
    })?;
    let profile = crate::exec::ProfileLaunch::write(&config.profile_directory(), &profile)
        .map_err(|error| {
            let message = format!(
                "cannot write controller git's sandbox profile into {}: {error}",
                config.profile_directory().display()
            );
            if error.raw_os_error() == Some(libc::EPERM) {
                CowshedError::sandbox_denied(
                    format!("{message}: the process running cowshed is sandboxed away from cowshed's store"),
                    "rerun the command from a shell whose sandbox allows writing the cowshed store",
                )
            } else {
                CowshedError::environment_missing(
                    message,
                    "run `cowshed setup` so the store's run directory exists and is yours",
                )
            }
        })?;
    let launcher = crate::sandbox::launcher().map_err(|error| error.report())?;
    let build = || -> Result<std::process::Command> {
        let git = git_command_at(&canonical);
        let mut command = std::process::Command::new(launcher.program());
        command
            .args(profile.args(launcher))
            .arg(platform::git_program());
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
                &parent
            })
            .env("HOME", &git_home);
        if let Some(identity) = workspace_git_identity_config(&canonical)? {
            command.env("GIT_CONFIG_GLOBAL", identity);
        }
        Ok(command)
    };
    let mut command = build()?;
    if policy.linked {
        // Git's own reading of its config, under the same profile, names every driver it could
        // select; each is switched off for the command the caller runs.
        // WHY unbounded: a workspace git with hooks off and drivers off or confined by the workspace
        // Git sandbox (`git_command_with_objects`): it waits on no other process. Git creates its lock
        // files exclusively: a held index lock refuses at once, a held ref lock after the pinned timeout
        // (core.filesRefLockTimeout 100 ms, core.packedRefsTimeout 1 s), never waited out.
        #[allow(clippy::disallowed_methods)]
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

/// A job can rewrite a linked worktree's `.git` pointer. Its claimed common
/// repository gains write authority only when that repository's own worktree
/// registration points back to this exact checkout.
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
    let admin = super::read_worktree_pointer(&pointer)?;
    let admin = fs::canonicalize(&admin).map_err(|error| {
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
    let reverse =
        super::read_gitdir_file(&admin.join("gitdir"), "reverse Git worktree registration")?;
    if fs::canonicalize(&reverse).ok().as_deref() != Some(pointer.as_path()) {
        return Err(CowshedError::integrity(
            "Git worktree registration does not point back to this workspace",
            "repair the Git worktree",
        ));
    }
    Ok(Some(common.to_owned()))
}

/// Git stops following includes this deep (`MAX_INCLUDE_DEPTH` in Git's `config.c`).
const MAX_GIT_INCLUDE_DEPTH: usize = 10;

/// The most config files one linked worktree's common config may bring in through includes.
const MAX_GIT_INCLUDED_FILES: usize = 64;

/// The largest Git config file the controller reads to resolve includes.
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

/// The values of `include.path` and every `includeIf.<condition>.path` in one Git config file,
/// in file order, read with Git's syntax: section and key names are case-insensitive, a
/// `key = value` may follow a section header on its line, a value may be double-quoted with
/// backslash escapes, and an unquoted `#` or `;` starts a comment.
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::{Checkout, ControllerGitPolicy, ExtraRead, controller_git_policy};
    use crate::error::ErrorCode;
    use crate::git::tests::repository;
    use crate::sandbox::{RunSandboxMode, SandboxGrants, SandboxProfileRole};

    const WORKSPACE: &str = "/home/operator/.cowshed/mnt/acme/widget/workspaces/raven/mount";
    const COMMON: &str = "/home/operator/src/widget/.git";
    const INCLUDE: &str = "/home/operator/src/widget/tooling/workspace.gitconfig";
    const SOURCE: &str = "/home/operator/src/widget";
    const OBJECTS: &str =
        "/home/operator/.cowshed/mnt/acme/widget/workspaces/wren/mount/.git/objects";
    const STAGING: &str =
        "/home/operator/.cowshed/mnt/acme/widget/workspaces/wren/mount/.cowshed/worktree-staging";

    fn checkout(common: Option<&str>, includes: &[&str]) -> Checkout {
        Checkout {
            root: PathBuf::from(WORKSPACE),
            common: common.map(PathBuf::from),
            includes: includes.iter().map(PathBuf::from).collect(),
        }
    }

    /// Every path as it is named: a layout with no links.
    fn as_named(path: &Path) -> std::io::Result<PathBuf> {
        Ok(path.to_owned())
    }

    /// The whole grant list of controller Git, layout by layout. Both platforms render this one
    /// value, so what a row grants is what Seatbelt and Landlock are each told.
    #[test]
    fn controller_git_reaches_its_checkout_and_exactly_the_named_trees() {
        use SandboxProfileRole::{GitDiscovery, GitDiscoveryGitOnly};
        let fetch = ExtraRead::FetchSource(Path::new(SOURCE));
        let objects = ExtraRead::AlternateObjects(Path::new(OBJECTS));
        let staging = Path::new(STAGING);
        type Row<'a> = (
            &'a str,
            Checkout,
            Option<ExtraRead<'a>>,
            Option<&'a Path>,
            SandboxProfileRole,
            &'a [&'a str],
            &'a [&'a str],
            bool,
        );
        #[rustfmt::skip]
        let rows: [Row<'_>; 8] = [
            ("a standalone checkout", checkout(None, &[]), None, None, GitDiscovery, &[], &[], false),
            ("a fetch reads its source", checkout(None, &[]), Some(fetch), None, GitDiscovery, &[SOURCE], &[], false),
            ("an object store is read in place", checkout(None, &[]), Some(objects), None, GitDiscovery, &[OBJECTS], &[], false),
            ("a staging directory is written", checkout(None, &[]), None, Some(staging), GitDiscovery, &[], &[STAGING], false),
            ("a linked worktree writes its common directory", checkout(Some(COMMON), &[]), None, None, GitDiscovery, &[], &[COMMON], true),
            ("a linked worktree may fetch while main includes nothing", checkout(Some(COMMON), &[]), Some(fetch), None, GitDiscovery, &[SOURCE], &[COMMON], true),
            ("a main-tree include is read and only Git executes", checkout(Some(COMMON), &[INCLUDE]), None, None, GitDiscoveryGitOnly, &[INCLUDE], &[COMMON], true),
            ("every grant at once, in order", checkout(Some(COMMON), &[INCLUDE]), Some(objects), Some(staging), GitDiscoveryGitOnly, &[INCLUDE, OBJECTS], &[STAGING, COMMON], true),
        ];
        for (name, checkout, extra, writable, role, read, write, linked) in rows {
            let policy = controller_git_policy(checkout, extra, writable, as_named)
                .unwrap_or_else(|error| panic!("{name}: {}", error.message));
            assert_eq!(
                policy,
                ControllerGitPolicy {
                    root: PathBuf::from(WORKSPACE),
                    role,
                    read: read.iter().map(PathBuf::from).collect(),
                    write: write.iter().map(PathBuf::from).collect(),
                    linked,
                },
                "{name}"
            );
        }
    }

    /// A grant names what its path resolves to, never the spelling the controller was handed.
    #[test]
    fn controller_named_trees_are_granted_as_they_resolve() {
        let policy = controller_git_policy(
            checkout(None, &[]),
            Some(ExtraRead::AlternateObjects(Path::new("/var/objects"))),
            Some(Path::new("/var/staging")),
            |path| Ok(Path::new("/private").join(path.strip_prefix("/").unwrap())),
        )
        .unwrap();
        assert_eq!(policy.read, [Path::new("/private/var/objects")]);
        assert_eq!(policy.write, [Path::new("/private/var/staging")]);
    }

    /// What the decision refuses, and as what: the fetch that would need a shell beside
    /// main-tree reads before anything is resolved, then a controller-named path that does not
    /// resolve, by its kind and name.
    #[test]
    fn controller_git_refuses_a_shell_beside_main_reads_and_unresolvable_trees() {
        let absent = |_: &Path| -> std::io::Result<PathBuf> {
            Err(std::io::Error::from(std::io::ErrorKind::NotFound))
        };
        let fetch = ExtraRead::FetchSource(Path::new(SOURCE));
        let objects = ExtraRead::AlternateObjects(Path::new(OBJECTS));
        let staging = Path::new(STAGING);
        type Refused<'a> = (
            Checkout,
            Option<ExtraRead<'a>>,
            Option<&'a Path>,
            ErrorCode,
            &'a [&'a str],
        );
        let rows: [Refused<'_>; 4] = [
            (
                checkout(Some(COMMON), &[INCLUDE]),
                Some(fetch),
                None,
                ErrorCode::SandboxDenied,
                &["cannot fetch", WORKSPACE, INCLUDE, SOURCE],
            ),
            (
                checkout(None, &[]),
                Some(fetch),
                None,
                ErrorCode::Integrity,
                &["cannot resolve the Git source checkout", SOURCE],
            ),
            (
                checkout(None, &[]),
                Some(objects),
                None,
                ErrorCode::Integrity,
                &["cannot resolve the Git object store", OBJECTS],
            ),
            (
                checkout(None, &[]),
                None,
                Some(staging),
                ErrorCode::Integrity,
                &["cannot resolve Git worktree staging", STAGING],
            ),
        ];
        for (checkout, extra, writable, code, said) in rows {
            let refused = controller_git_policy(checkout, extra, writable, absent)
                .expect_err("the layout is refused");
            assert_eq!(refused.code, code, "{}", refused.message);
            for part in said {
                assert!(refused.message.contains(part), "{}", refused.message);
            }
        }
    }

    /// The configuration both enforcers render: the checkout is the mount and the temp dir, the
    /// grants are the policy's, the profile's file goes to the store the controller serves, and
    /// nothing a workspace's jobs are given comes with it.
    #[test]
    fn the_sandbox_configuration_carries_the_policy_and_nothing_else() {
        let policy = controller_git_policy(
            checkout(Some(COMMON), &[INCLUDE]),
            Some(ExtraRead::AlternateObjects(Path::new(OBJECTS))),
            Some(Path::new(STAGING)),
            as_named,
        )
        .unwrap();
        let config = policy.sandbox_config(
            Path::new("/private/cowshed/store"),
            PathBuf::from("/home/operator"),
        );
        assert_eq!(config.workspace_mount, Path::new(WORKSPACE));
        assert_eq!(config.exec_temp_dir, Path::new(WORKSPACE));
        assert_eq!(config.home, Path::new("/home/operator"));
        assert_eq!(config.store_root, Path::new("/private/cowshed/store"));
        assert_eq!(
            config.profile_directory(),
            Path::new("/private/cowshed/store/run/profiles")
        );
        assert_eq!(config.mode, RunSandboxMode::ReadWrite);
        assert_eq!(
            config.grants,
            SandboxGrants {
                read: vec![PathBuf::from(INCLUDE), PathBuf::from(OBJECTS)],
                write: vec![PathBuf::from(STAGING), PathBuf::from(COMMON)],
                ..SandboxGrants::default()
            }
        );
        assert!(config.allowed_unix_sockets.is_empty());
        assert!(config.additional_denies.is_empty());
        assert!(config.shed_links.is_empty());
        assert!(config.repository_caches.is_empty());
        assert!(config.program_directories.is_empty());
        assert_eq!(config.cpu_pool_directory, None);
        assert_eq!(config.git_worktree_repository, None);
        assert_eq!(config.main_mount, None);
        assert_eq!(config.build_volume_mount, None);
        assert_eq!(config.capabilities, Default::default());
    }

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

    /// Include paths are read with Git's own syntax, and only from include sections.
    #[test]
    fn config_include_paths_follow_git_syntax() {
        let config = "[core]\n\tpath = not-an-include\n[include]\n\tpath = ../tooling/a.gitconfig ; comment\n[Include] PATH = \"b c.gitconfig\"\n[includeIf \"gitdir:~/src/]x\"]\n\tpath = ~/work.gitconfig\n\tother = d\n[user]\n\tname = x\n";
        assert_eq!(
            super::config_include_paths(config),
            [
                "../tooling/a.gitconfig",
                "b c.gitconfig",
                "~/work.gitconfig"
            ]
        );
    }
}
