//! Seatbelt enforcement: a [`ProfilePolicy`] rendered as the SBPL program `sandbox-exec` reads
//! (04_sandbox.md, "Baseline (closed) profile"). SBPL is last-match-wins, so the order of the
//! rules below is the policy as much as the rules are.

use std::path::{Path, PathBuf};

use super::{
    BUILD_VOLUME_PARAM, GIT_DEVELOPER_DIR, Launcher, LauncherError, MainSnapshotStore,
    ProfilePolicy, RunSandboxMode, SCRATCH_DIR_PARAM, SCRATCH_PARENT_PARAM, SandboxConfig,
    SandboxError, SandboxProfileRole, SeatbeltProfile, cowshed_root, held_port_blocks,
    main_snapshot_store, paths_intersect, sandbox_runtime_link, shared_daemon_runtime_dir,
    shared_daemon_runtime_link, validate_path,
};
use crate::capabilities::{CapabilityGrant, GrantAccess, GrantScope};

/// Seatbelt enforces every rule a policy states, so nothing a validated policy carries is
/// refused here.
pub(super) fn validate(_policy: &ProfilePolicy<'_>) -> Result<(), SandboxError> {
    Ok(())
}

/// `sandbox-exec` is the launcher on macOS, so no invocation of this binary is one.
pub(super) fn dispatch_launcher() -> Option<i32> {
    None
}

/// The system's Seatbelt launcher.
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// `sandbox-exec`, which takes a launch's profile arguments first: the system's, the same for
/// every process, so no program names one.
static LAUNCHER: std::sync::LazyLock<Launcher> = std::sync::LazyLock::new(|| Launcher {
    program: PathBuf::from(SANDBOX_EXEC),
    verb: None,
});

pub(super) fn launcher() -> Result<&'static Launcher, LauncherError> {
    Ok(&LAUNCHER)
}

/// Nothing to name: every launch starts the system's `sandbox-exec`.
pub(super) fn name_launcher(_executable: &Path) -> Result<(), LauncherError> {
    Ok(())
}

/// `EX_USAGE`: this program launches nothing here.
pub(super) fn launcher_main() -> i32 {
    cowshed_os::eprintln!(
        "cowshed: {} is the Linux sandbox launcher; macOS launches through {SANDBOX_EXEC}",
        super::SANDBOX_EXEC_NAME
    );
    64
}

/// The SBPL text one profile is built into, and whether its role starts only what it reads.
pub(super) struct ProfileText {
    pub(super) text: String,
    /// Exec follows read: each rule that decides `file-read-data` is followed by the same rule for
    /// `process-exec`, so last-match-wins starts a program exactly where it reads one. macOS checks
    /// only `process-exec` when it starts a file, so with exec allowed everywhere a program the job
    /// could not read still started: a 755 `#!/usr/bin/env bun` entry Bun then failed to read ran
    /// again through Bun's command fallback, one nested process per level (04_sandbox.md,
    /// "Baseline (closed) profile"). Git's executes-only-Git role names its programs instead.
    pub(super) exec_follows_read: bool,
}

/// The one program a workspace command runs outside the sandbox. macOS refuses a sandboxed exec
/// of a set-user-ID program whatever the profile allows, and `/bin/ps` is one, so without this a
/// job cannot list its own processes or read their start environment (04_sandbox.md, "Process
/// inspection"). `ps` only reads the process table and prints it, and shows the environment of
/// this user's processes alone, which every job already reads through `kern.procargs2`.
pub(super) const PROCESS_STATUS_EXEC: &str =
    "(allow process-exec (literal \"/bin/ps\") (with no-sandbox))";

/// Where a fixture's short root lies: directly under the world-shared `/tmp`, spelled canonically
/// because Seatbelt matches the resolved path and `/tmp` is a symlink to `/private/tmp`.
const FIXTURE_PARENT: &str = "/private/tmp";

/// What starts a fixture's names beneath [`FIXTURE_PARENT`]: the root `cx<pid>-<2 alphanumerics>`
/// its process creates and removes with everything beneath it, and the `cxsweep.lock` that
/// serializes reclaiming a dead run's roots (04_sandbox.md, "Fixture roots under /tmp"). It holds
/// no regex metacharacter.
pub(super) const FIXTURE_PREFIX: &str = "cx";

/// The one place a job writes in `/tmp`: a fixture root and what lies beneath it, the sweep lock,
/// and the Unix sockets a daemon binds inside a root. Only these spellings match, so a lookalike
/// name and every other entry of `/tmp` stay denied. A regex is the one SBPL filter that names a
/// pattern, and its engine does not honor a `{n}` interval (measured: `[A-Za-z0-9]{2}` compiles
/// and does not match `ab`), so the two random characters are two classes. The sweep reads
/// `/tmp`'s names with the `file-read-data` the broad allow already grants; no other entry gains
/// metadata.
fn push_fixture_root_rules(profile: &mut ProfileText) {
    let root = format!("^{FIXTURE_PARENT}/{FIXTURE_PREFIX}[0-9]+-[A-Za-z0-9][A-Za-z0-9]");
    let lock = format!("{FIXTURE_PARENT}/{FIXTURE_PREFIX}sweep.lock");
    push_line(
        profile,
        &format!("(allow file-read* file-write* (regex #\"{root}(/|$)\") (literal \"{lock}\"))"),
    );
    push_line(
        profile,
        &format!(
            "(allow network-bind network-inbound network-outbound (local unix-socket (path-regex #\"{root}/\")) (remote unix-socket (path-regex #\"{root}/\")))"
        ),
    );
}

/// Generate a complete, deterministic SBPL profile for one authority tier.
///
/// Paths must already be canonical controller data. Child argv, environment,
/// output, and repository-controlled grants are deliberately absent from the
/// role selection and therefore cannot remove the executed-child narrowing.
pub(super) fn render(
    policy: ProfilePolicy<'_>,
    scratch: bool,
) -> Result<SeatbeltProfile, SandboxError> {
    let ProfilePolicy {
        config,
        role,
        hard_denies,
        read_grants,
        write_grants,
        sockets,
    } = policy;

    let cowshed = cowshed_root();
    let mut profile = ProfileText {
        text: String::new(),
        exec_follows_read: role != SandboxProfileRole::GitDiscoveryGitOnly,
    };

    push_line(&mut profile, "(version 1)");
    push_line(&mut profile, "(deny default)");
    // Hard-link creation is a separate SBPL operation from file-write*.
    // Keep aliases unavailable to both authority tiers.
    push_line(&mut profile, "(deny file-link)");
    if !matches!(
        role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    ) {
        push_line(&mut profile, "(allow file-read-data (subpath \"/\"))");
        // The broad allow is for system paths. Nothing under HOME is readable unless a later
        // rule names it: an explicit grant, the workspace's own mount, the exec temp dir, an
        // allowed socket, or a detected capability's grant. Every one of those follows here,
        // so last-match-wins carves each back.
        push_subpath_rule(&mut profile, "deny file-read*", &config.home)?;
    } else {
        // Git's isolated global configuration is the empty device, not user HOME.
        push_literal_rule(&mut profile, "allow file-read*", Path::new("/dev/null"))?;
    }
    // Every runtime seeds itself from the kernel's entropy before it runs a line; Python reads
    // `/dev/urandom` for it, and without it dies in `_Py_HashRandomization_Init`. Git's own
    // profile may run a standalone repository's merge drivers too, so every role reads both devices.
    for device in ["/dev/random", "/dev/urandom"] {
        push_literal_rule(&mut profile, "allow file-read*", Path::new(device))?;
    }
    // Directory metadata is distinct from file-read-data in Seatbelt. Toolchain
    // launchers (notably /usr/bin/git -> xcrun) must traverse their immutable
    // system roots without gaining metadata access to the user's home.
    // Nix tools load libraries from other immutable store entries even without a Nix
    // capability: dyld needs their metadata as well as the baseline data reads.
    for root in [
        "/Applications",
        "/Library",
        "/System",
        "/bin",
        "/nix/store",
        "/opt",
        "/private/var/select",
        "/sbin",
        "/usr",
        "/var/select",
    ] {
        push_exact_and_subpath_rule(&mut profile, "allow file-read*", Path::new(root))?;
        push_readable_ancestors(&mut profile, Path::new(root))?;
    }
    if role == SandboxProfileRole::GitDiscoveryGitOnly {
        push_line(&mut profile, "(allow process-fork)");
        // The developer-dir `git` the sandbox launches and the helpers it re-executes from its
        // exec path, in both the spelling Git is launched by and the one the link resolves to.
        // Nothing else runs: not a shell, so not any command a config key names.
        let developer = Path::new(GIT_DEVELOPER_DIR);
        // Seatbelt matches the resolved spelling. A developer directory that does not resolve
        // would leave only a rule that never matches, and Git would die with no cause named.
        let resolved = std::fs::canonicalize(developer).map_err(|_| SandboxError::InvalidPath {
            path: developer.to_owned(),
            reason: "the active developer directory does not resolve",
        })?;
        for root in [developer.to_owned(), resolved] {
            push_literal_rule(
                &mut profile,
                "allow process-exec",
                &root.join("usr/bin/git"),
            )?;
            push_subpath_rule(
                &mut profile,
                "allow process-exec",
                &root.join("usr/libexec/git-core"),
            )?;
        }
    } else {
        // Exec comes from each read rule's exec twin (ProfileText::exec_follows_read).
        push_line(&mut profile, "(allow process-fork)");
        // After the `/bin` carve-back's exec twin, which would otherwise match `/bin/ps` last and
        // keep it sandboxed.
        push_line(&mut profile, PROCESS_STATUS_EXEC);
    }
    push_line(&mut profile, "(allow file-map-executable)");
    push_line(&mut profile, "(allow sysctl-read)");
    push_line(&mut profile, "(allow pseudo-tty)");
    push_line(&mut profile, "(allow process-info* (target same-sandbox))");
    push_line(&mut profile, "(allow signal (target same-sandbox))");
    push_line(
        &mut profile,
        "(allow mach-priv-task-port (target same-sandbox))",
    );
    // Filesystem watchers need this service port or they silently receive no
    // events. FSEvents still filters notifications through the file-read
    // policy: the real watcher regression covers an allowed update and a
    // denied descendant. This service adds no filesystem authority.
    push_line(
        &mut profile,
        "(allow mach-lookup (global-name \"com.apple.FSEvents\"))",
    );
    // pwd.h/grp.h use OpenDirectory's read-only libinfo lookup service, as
    // Apple's opendirectory.sb documents. Stock id and ssh-keygen need it to
    // resolve the effective uid; record (.api) and membership services stay denied.
    push_line(
        &mut profile,
        "(allow mach-lookup (global-name \"com.apple.system.opendirectoryd.libinfo\"))",
    );
    // The per-user directory helper (`com.apple.bsd.dirhelper`) stays denied, so `confstr(3)`'s
    // `DARWIN_USER_TEMP_DIR` and `DARWIN_USER_CACHE_DIR` answer EIO and tools fall back to
    // `TMPDIR` or `/tmp`. Answered, they name the host user's own `/var/folders` directories,
    // which no job may write (every host process's temp files live there): `mktemp` under a
    // devenv shell then picked one and failed with EPERM instead of using the job's temp dir.

    for socket in &sockets {
        push_line(
            &mut profile,
            &format!(
                "(allow network-outbound (remote unix-socket (path-literal \"{}\")))",
                sbpl_path(socket)?
            ),
        );
    }
    push_line(
        &mut profile,
        "(allow network-bind network-inbound (local tcp \"localhost:*\"))",
    );
    // Unix sockets the workspace's own processes rendezvous over — devenv's and nx's in the
    // runtime dir, a test's in its temp dir, a tool's under the checkout — are admitted in the
    // workspace's own tree and nowhere else: exec temp, and the whole source mount only for a
    // read-write job. Generic rendezvous state stays private to the job mode; the contributed
    // shared daemon leaf is granted separately below. `path-prefix` is a string prefix, so the
    // trailing slash excludes sibling names.
    let runtime_link = sandbox_runtime_link(config);
    let private_runtime_link =
        PathBuf::from("/private").join(runtime_link.strip_prefix("/").unwrap_or(&runtime_link));
    let own_mount = (config.mode == RunSandboxMode::ReadWrite).then_some(&config.workspace_mount);
    for tree in
        own_mount
            .into_iter()
            .chain([&config.exec_temp_dir, &runtime_link, &private_runtime_link])
    {
        push_line(
            &mut profile,
            &format!(
                "(allow network-bind network-inbound network-outbound (local unix-socket (path-prefix \"{0}/\")) (remote unix-socket (path-prefix \"{0}/\")))",
                sbpl_path(tree)?
            ),
        );
    }
    if scratch {
        push_line(
            &mut profile,
            &format!(
                "(allow network-bind network-inbound network-outbound (local unix-socket (path-prefix (string-append (param \"{SCRATCH_DIR_PARAM}\") \"/\"))) (remote unix-socket (path-prefix (string-append (param \"{SCRATCH_DIR_PARAM}\") \"/\"))))"
            ),
        );
    }
    // Every port of every block the workspace holds, current and retained, one literal each; a
    // loopback-any grant admits every loopback port instead (04_sandbox.md).
    if config.grants.loopback_any {
        push_line(
            &mut profile,
            "(allow network-outbound (remote tcp \"localhost:*\"))",
        );
    } else {
        for block in held_port_blocks(config) {
            for port in block.ports().map_err(|_| SandboxError::InvalidPortBlock {
                base: block.base,
                size: block.size,
            })? {
                push_line(
                    &mut profile,
                    &format!("(allow network-outbound (remote tcp \"localhost:{port}\"))"),
                );
            }
        }
    }

    if !matches!(
        role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    ) {
        let directory = crate::host_dirs::installed_binary_directory(&config.home);
        let stable = directory.join("cowshed");
        let installed = match std::fs::canonicalize(&stable) {
            Ok(path) => Some(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(SandboxError::InstalledCowshed {
                    path: stable.clone(),
                    cause: error.to_string(),
                });
            }
        };
        if let Some(installed) = installed {
            let metadata =
                std::fs::metadata(&installed).map_err(|error| SandboxError::InstalledCowshed {
                    path: installed.clone(),
                    cause: error.to_string(),
                })?;
            if !metadata.is_file() {
                return Err(SandboxError::InvalidPath {
                    path: installed,
                    reason: "the installed cowshed must resolve to one regular executable",
                });
            }
            // Only the installed copy is readable. Lookup of the stable name and its ancestors
            // needs metadata, never directory contents.
            for name in [&stable, &installed] {
                for ancestor in name.ancestors() {
                    push_literal_rule(&mut profile, "allow file-read-metadata", ancestor)?;
                }
            }
            push_literal_rule(&mut profile, "allow file-read*", &installed)?;
        }
    }

    for path in &read_grants {
        push_subpath_rule(&mut profile, "allow file-read*", path)?;
    }
    for path in &write_grants {
        push_subpath_rule(&mut profile, "allow file-read* file-write*", path)?;
    }
    // The machine-global evidence layer and the fixed per-user mount tree are separate protected
    // roots. The mount tree needs its own deny, outside the `/private/cowshed` boundary.
    push_subpath_rule(&mut profile, "deny file-read* file-write*", cowshed)?;
    push_subpath_rule(
        &mut profile,
        "deny file-read* file-write*",
        &config.mount_root,
    )?;
    // `getcwd(2)` and path resolution need read access to every exact ancestor. Literal rules
    // reveal no sibling subtree and are emitted after both broad denies, while the own-workspace
    // subpath grant below carves back only this workspace.
    push_readable_ancestors(&mut profile, &config.workspace_mount)?;
    for path in read_grants.iter().chain(write_grants.iter()) {
        push_readable_ancestors(&mut profile, path)?;
    }
    // A shed link is the spelling a workspace reaches a sibling repository by (`../<name>`), and
    // it sits inside the mount-root deny above. Only a link whose target a grant covers is carved
    // back, as a literal: the link stays unreadable until its target is granted, and reading it
    // reveals nothing a granted subtree does not already show.
    for link in &config.shed_links {
        if read_grants
            .iter()
            .chain(write_grants.iter())
            .any(|grant| paths_intersect(&link.target, grant))
        {
            push_readable_ancestors(&mut profile, &link.link)?;
        }
    }
    // Connecting to an allowed socket resolves its path, so every ancestor must
    // be traversable. Socket directories are not under the immutable roots
    // granted above — the nix daemon socket resolves into `/private/var/run`,
    // while a socket under the store is itself behind the `/private/cowshed` deny — so
    // the literals must land here, after the store-wide deny, or last-match-wins
    // re-denies them and the connect fails on path resolution before the
    // outbound rule is ever consulted.
    for socket in &sockets {
        push_readable_ancestors(&mut profile, socket)?;
    }
    // The workspace's CPU pool directory, `<store>/run/cpu/<incarnation>` (05_gateway.md
    // "Parallelism inside a job"), holds its jobs' jobserver fifos (05_gateway.md "Cargo reads
    // its holder's jobserver"). It sits behind the `/private/cowshed` deny, so its rules land
    // here for the reason the socket ancestors above do: emitted before that deny, last-match-wins
    // re-denied the jobserver FIFO's open with EPERM, and Cargo ran without its jobserver. The
    // profile is rendered per workspace, not per job, so no job's fifo can be a literal here: the
    // scope is this workspace's own pool directory, and inside it only FIFOs open. Its ancestors
    // and entries resolve by metadata, as the installed cowshed's do: neither the directory nor
    // `run/cpu`, where sibling workspaces' directories sit, is listable.
    if let Some(directory) = &config.cpu_pool_directory {
        for ancestor in directory.ancestors().skip(1) {
            push_literal_rule(&mut profile, "allow file-read-metadata", ancestor)?;
        }
        let directory = sbpl_path(directory)?;
        push_line(
            &mut profile,
            &format!("(allow file-read-metadata (subpath \"{directory}\"))"),
        );
        // Its jobserver fifo's tokens are read and written back (05_gateway.md "Cargo reads its
        // holder's jobserver"), and nothing else there is.
        push_line(
            &mut profile,
            &format!(
                "(allow file-read-data file-write-data (require-all (subpath \"{directory}\") (vnode-type FIFO)))"
            ),
        );
    }
    // Every tool-specific authority is a detected capability's contribution (15_capabilities.md):
    // its shared caches with their access where the host keeps them, their ancestors beneath HOME
    // metadata only, and its exact grants. Configuration, credentials and binaries a tool keeps
    // beside them stay hard denies, which follow every grant below.
    let contribution = &config.capabilities.contribution;
    for cache in &contribution.shared_caches {
        push_capability_grant(
            &mut profile,
            &config.home,
            &CapabilityGrant {
                path: cache.path.clone(),
                scope: GrantScope::Subtree,
                access: cache.access,
            },
        )?;
    }
    for grant in &contribution.grants {
        push_capability_grant(&mut profile, &config.home, grant)?;
    }
    // Build state is not source. Grant only the controller-selected physical volume, after
    // the store and mount-tree denies; sibling volumes and images remain inaccessible. The volume
    // is the profile's one parameter (`BUILD_VOLUME_PARAM`), so a build-volume pivot binds a new
    // path to the same profile rather than rendering another.
    if let Some(volume) = &config.build_volume_mount {
        if let Some(parent) = volume.parent() {
            push_readable_ancestors(&mut profile, parent)?;
        }
        push_volume_exact_and_subpath_rule(
            &mut profile,
            "allow file-read* file-write*",
            volume,
            volume,
        )?;
    }
    push_subpath_rule(&mut profile, "allow file-read*", &config.workspace_mount)?;
    if config.mode == RunSandboxMode::ReadWrite {
        push_subpath_rule(&mut profile, "allow file-write*", &config.workspace_mount)?;
    }
    // Last-match-wins: a workspace-relative deny must follow every mount and grant allow.
    // Deny rename/unlink on ancestors too, or renaming `.git` would move the protected
    // children out of the denied spelling before rewriting them.
    for (relatives, operations) in [
        (&config.grants.deny_write, "deny file-write*"),
        (&config.grants.deny, "deny file-read* file-write*"),
    ] {
        for relative in relatives {
            let mut parent = config.workspace_mount.clone();
            for component in relative.components() {
                parent.push(component);
                push_literal_rule(&mut profile, "deny file-write-unlink", &parent)?;
            }
            push_exact_and_subpath_rule(
                &mut profile,
                operations,
                &config.workspace_mount.join(relative),
            )?;
            if let Some(volume) = &config.build_volume_mount {
                let resolved = resolve_deny(&config.workspace_mount.join(relative))?;
                if let Ok(relative) = resolved.strip_prefix(volume) {
                    let mut parent = volume.clone();
                    for component in relative.components() {
                        parent.push(component);
                        push_line(
                            &mut profile,
                            &format!(
                                "(deny file-write-unlink (literal {}))",
                                volume_expression(volume, &parent)?
                            ),
                        );
                    }
                    push_volume_exact_and_subpath_rule(
                        &mut profile,
                        operations,
                        volume,
                        &resolved,
                    )?;
                }
            }
        }
    }
    let workspace_metadata = config.workspace_mount.join(".cowshed");
    let job_artifacts = workspace_metadata.join("job");

    // SBPL is last-match-wins. Policy denies (project roots the host configured) come first,
    // then the two carve-backs they would otherwise close, then the immutable secret denies as
    // the profile's last word on the shared tree: no grant may follow a secret's deny.
    let (policy, secrets): (Vec<_>, Vec<_>) = hard_denies
        .into_iter()
        .filter(|path| path.as_ref() != cowshed && path.as_ref() != config.mount_root.as_path())
        .partition(|path| {
            config
                .additional_denies
                .iter()
                .any(|extra| extra == path.as_ref())
        });
    for deny in policy {
        push_exact_and_subpath_rule(&mut profile, "deny file-read* file-write*", deny.as_ref())?;
    }

    // After every deny that would otherwise close it: the mount-root deny covers
    // main under the symlink layout, and policy denies the project root under direct mount. The
    // carve-back is narrowed to `.git`, never main's working tree, which stays as unreachable as
    // any sibling workspace.
    if let Some(repository) = &config.git_worktree_repository {
        push_readable_ancestors(&mut profile, repository)?;
        push_exact_and_subpath_rule(&mut profile, "allow file-read* file-write*", repository)?;
    }

    // The consumer imports sealed pins from main, not from a live provider checkout. This is a
    // controller-derived read exception, after main's deny and before every immutable secret deny.
    if let Some(main) = &config.main_mount
        && let Some(warning) = push_main_snapshot_store(&mut profile, config, main)?
    {
        cowshed_os::eprintln!("{warning}");
    }

    // The exec temp dir is exported as TMPDIR and lives in the project's store directory, so its
    // grant must follow every deny that covers it - the store-wide one and the project root
    // above - or last-match-wins re-denies it: every `mktemp` in a child then fails on a path
    // the child never chose. Read and write both - a child reads back what it wrote there, and
    // metadata on the directory is what a `realpath(os.tmpdir())` needs.
    push_subpath_rule(&mut profile, "allow file-read*", &config.exec_temp_dir)?;
    push_line(
        &mut profile,
        &format!(
            "(allow file-write* (subpath \"{}\") (literal \"/dev/null\"))",
            sbpl_path(&config.exec_temp_dir)?
        ),
    );
    // A child writes the null device through a descriptor its parent opened write-only (a
    // shell's `>/dev/null`, a detached job's stdio), and runtimes fstat their stdio before
    // running a line: Node aborts in its process setup, with no message, when that fstat
    // fails. fstat on a write-only descriptor is file-read-metadata, which neither the
    // write grant above nor file-read-data on `/` includes.
    push_literal_rule(
        &mut profile,
        "allow file-read-metadata",
        Path::new("/dev/null"),
    )?;
    // A child opens its stdio by name too (usdt's proc macro runs `dtrace -h -s /dev/stdin -o
    // /dev/stdout`). The names are links into /dev/fd, and Seatbelt matches the resolved
    // spelling: a rule on a link name never admits an open, only its lookup, which is
    // file-read-metadata. The open lands on /dev/fd/N, which duplicates a descriptor this
    // process already holds and refuses an access mode wider than that descriptor's, so the
    // subtree reaches nothing the child could not already read or write.
    for link in ["/dev/stdin", "/dev/stdout", "/dev/stderr"] {
        push_literal_rule(&mut profile, "allow file-read-metadata", Path::new(link))?;
    }
    push_subpath_rule(
        &mut profile,
        "allow file-read* file-write*",
        Path::new("/dev/fd"),
    )?;
    push_readable_ancestors(&mut profile, &config.exec_temp_dir)?;
    if scratch {
        // Scratch provider directory for this host, bound via `-D SCRATCH_DIR` and
        // `-D SCRATCH_PARENT`. Deny the parent subtree first so sibling directories stay out of
        // reach even outside HOME/store, then carve back only this host's directory and allow
        // metadata-only lookup (no listing) on the parent literal.
        push_line(
            &mut profile,
            &format!("(deny file-read* file-write* (subpath (param \"{SCRATCH_PARENT_PARAM}\")))"),
        );
        push_line(
            &mut profile,
            &format!("(allow file-read* file-write* (subpath (param \"{SCRATCH_DIR_PARAM}\")))"),
        );
        push_line(
            &mut profile,
            &format!("(allow file-read-metadata (literal (param \"{SCRATCH_PARENT_PARAM}\")))"),
        );
    }
    // A contributed daemon leaf (Nx's run/nx) is shared across modes, never the surrounding
    // generic runtime directory. The rest of a read-only job's rendezvous state stays private.
    let shared_alias = shared_daemon_runtime_link(config);
    let shared_runtime = shared_daemon_runtime_dir(config);
    for alias in &config
        .capabilities
        .contribution
        .daemon_isolation
        .directories
    {
        let Ok(relative) = alias.strip_prefix(&shared_alias) else {
            continue;
        };
        validate_path(alias)?;
        if relative.as_os_str().is_empty() {
            return Err(SandboxError::InvalidPath {
                path: alias.clone(),
                reason: "a shared daemon grant must name a leaf below its runtime root",
            });
        }
        let directory = shared_runtime.join(relative);
        push_readable_ancestors(&mut profile, &directory)?;
        push_subpath_rule(&mut profile, "allow file-read* file-write*", &directory)?;
        let canonical_alias = Path::new("/private").join(alias.strip_prefix("/").unwrap_or(alias));
        for socket_tree in [&directory, alias, &canonical_alias] {
            push_line(
                &mut profile,
                &format!(
                    "(allow network-bind network-inbound network-outbound (local unix-socket (path-prefix \"{0}/\")) (remote unix-socket (path-prefix \"{0}/\")))",
                    sbpl_path(socket_tree)?
                ),
            );
        }
        for name in [
            &shared_alias,
            &Path::new("/private").join(shared_alias.strip_prefix("/").unwrap_or(&shared_alias)),
        ] {
            push_literal_rule(&mut profile, "allow file-read-metadata", name)?;
        }
    }
    // Generic short runtime links resolve to the mode's own runtime directory. Only metadata
    // on their exact names is allowed, never metadata for the rest of /tmp.
    let runtime_name = runtime_link
        .file_name()
        .expect("runtime link has a file name");
    for tmp in ["/tmp", "/private/tmp"] {
        push_literal_rule(&mut profile, "allow file-read-metadata", Path::new(tmp))?;
        push_literal_rule(
            &mut profile,
            "allow file-read-metadata",
            &Path::new(tmp).join(runtime_name),
        )?;
    }

    // The one world-shared `/tmp` area a job writes: a fixture's short root and its sweep lock
    // (04_sandbox.md, "Fixture roots under /tmp"). Controller Git keeps its narrower write
    // boundary.
    if !matches!(
        role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    ) {
        push_fixture_root_rules(&mut profile);
    }

    for deny in secrets {
        push_exact_and_subpath_rule(&mut profile, "deny file-read* file-write*", deny.as_ref())?;
    }

    for protected in [
        crate::storage::WORKSPACE_MARKER_PATH,
        crate::workspace_credentials::CA_CERTIFICATE_PATH,
        crate::workspace_credentials::WORKSPACE_TOKEN_PATH,
        crate::workspace_environment::WORKSPACE_ENVIRONMENT_PATH,
        crate::workspace_git_fetch::WORKSPACE_GIT_FETCH_CONFIG_PATH,
        crate::git::WORKSPACE_GIT_IDENTITY_CONFIG_PATH,
    ] {
        push_literal_rule(
            &mut profile,
            "deny file-write*",
            &config.workspace_mount.join(protected),
        )?;
    }
    // The staged exec host every warm shell runs as. A job that could replace it would run its
    // own program as every later command's parent, able to forge their wait status.
    push_exact_and_subpath_rule(
        &mut profile,
        "deny file-write*",
        &config
            .workspace_mount
            .join(crate::runtime::shell_host::SHELL_HOST_DIRECTORY),
    )?;

    match role {
        SandboxProfileRole::TrustedSupervisor => {
            // The trusted writer's reserved authority is the final narrow
            // carve-back, including when the repository itself is read-only.
            push_exact_and_subpath_rule(&mut profile, "allow file-write*", &job_artifacts)?;
        }
        SandboxProfileRole::ExecutedChild
        | SandboxProfileRole::GitDiscovery
        | SandboxProfileRole::GitDiscoveryGitOnly => {
            // These terminal rules are emitted after every configurable or broad
            // allow. Denying create/unlink at the metadata directory itself
            // prevents replacing or renaming that ancestor without blocking
            // writes to unrelated metadata children.
            // file-write* covers create, data write/truncate, rename, unlink, and
            // symlink creation. Hard links are separately denied for both tiers.
            push_literal_rule(
                &mut profile,
                "deny file-write-create file-write-unlink",
                &workspace_metadata,
            )?;
            push_exact_and_subpath_rule(&mut profile, "deny file-write*", &job_artifacts)?;
            let tool_root = match config.mode {
                RunSandboxMode::ReadOnly => config.exec_temp_dir.join("tools"),
                RunSandboxMode::ReadWrite => workspace_metadata.join("tools"),
            };
            push_exact_and_subpath_rule(&mut profile, "deny file-write*", &tool_root)?;
        }
    }
    Ok(SeatbeltProfile {
        text: profile.text,
        build_volume: config.build_volume_mount.clone(),
        scratch,
    })
}

/// Resolve a deny even when its leaf has not been created. This adds no authority: only
/// deny rules beneath an already controller-selected physical build volume use the result.
fn resolve_deny(path: &Path) -> Result<PathBuf, SandboxError> {
    let mut existing = path;
    let mut suffix = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut resolved) => {
                for part in suffix.into_iter().rev() {
                    resolved.push(part);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = existing.file_name() else {
                    return Err(SandboxError::DenyResolution {
                        path: path.to_owned(),
                        cause: error.to_string(),
                    });
                };
                suffix.push(name);
                existing = existing
                    .parent()
                    .expect("an absolute named path has a parent");
            }
            Err(error) => {
                return Err(SandboxError::DenyResolution {
                    path: path.to_owned(),
                    cause: error.to_string(),
                });
            }
        }
    }
}

fn sbpl_path(path: &Path) -> Result<String, SandboxError> {
    validate_path(path)?;
    Ok(path
        .as_os_str()
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\""))
}

/// One capability grant. Every grant makes its path and ancestors resolvable; a read-write
/// literal is a file the tool itself writes, and a subtree extends the grant to everything
/// beneath its root. An ancestor beneath HOME gets metadata only: enough to resolve and
/// `realpath` through it, never to list `~` or `~/Library/Application Support`, which the
/// HOME-wide read deny keeps closed.
fn push_capability_grant(
    profile: &mut ProfileText,
    home: &Path,
    grant: &CapabilityGrant,
) -> Result<(), SandboxError> {
    for ancestor in grant.path.ancestors().skip(1) {
        let operation = if ancestor.starts_with(home) {
            "allow file-read-metadata"
        } else {
            "allow file-read*"
        };
        push_literal_rule(profile, operation, ancestor)?;
    }
    let operation = match grant.access {
        GrantAccess::ReadMetadata => "allow file-read-metadata",
        GrantAccess::Read => "allow file-read*",
        GrantAccess::ReadWrite => "allow file-read* file-write*",
    };
    match grant.scope {
        GrantScope::Literal => push_literal_rule(profile, operation, &grant.path),
        GrantScope::Subtree => push_subpath_rule(profile, operation, &grant.path),
    }
}

/// Read the one controller-named store, including its fixed build-volume link, never main's cache.
pub(super) fn push_main_snapshot_store(
    profile: &mut ProfileText,
    config: &SandboxConfig,
    main: &Path,
) -> Result<Option<String>, SandboxError> {
    let (store, resolved) = match main_snapshot_store(config, main) {
        MainSnapshotStore::Absent => return Ok(None),
        MainSnapshotStore::Refused(warning) => return Ok(Some(warning)),
        MainSnapshotStore::Readable { store, resolved } => (store, resolved),
    };
    let build_link = main.join(".cowshed/build");
    // A symlink in the fixed store path passes through main/.cowshed/build. Only lookup is
    // admitted at those hops and at every ancestor, including the otherwise-denied build volume.
    for path in [&store, &resolved, &build_link] {
        for ancestor in path.ancestors() {
            push_literal_rule(profile, "allow file-read-metadata", ancestor)?;
        }
    }
    for path in [&store, &resolved] {
        push_subpath_rule(profile, "allow file-read*", path)?;
    }
    Ok(None)
}

fn push_readable_ancestors(profile: &mut ProfileText, path: &Path) -> Result<(), SandboxError> {
    for ancestor in path.ancestors() {
        push_literal_rule(profile, "allow file-read*", ancestor)?;
    }
    Ok(())
}

fn push_subpath_rule(
    profile: &mut ProfileText,
    operation: &str,
    path: &Path,
) -> Result<(), SandboxError> {
    push_line(
        profile,
        &format!("({operation} (subpath \"{}\"))", sbpl_path(path)?),
    );
    Ok(())
}

fn push_literal_rule(
    profile: &mut ProfileText,
    operation: &str,
    path: &Path,
) -> Result<(), SandboxError> {
    push_line(
        profile,
        &format!("({operation} (literal \"{}\"))", sbpl_path(path)?),
    );
    Ok(())
}

fn push_exact_and_subpath_rule(
    profile: &mut ProfileText,
    operation: &str,
    path: &Path,
) -> Result<(), SandboxError> {
    let path = sbpl_path(path)?;
    push_line(
        profile,
        &format!("({operation} (literal \"{path}\") (subpath \"{path}\"))"),
    );
    Ok(())
}

/// `path`, inside the build volume `volume`, as an SBPL string expression over the
/// [`BUILD_VOLUME_PARAM`] parameter.
fn volume_expression(volume: &Path, path: &Path) -> Result<String, SandboxError> {
    sbpl_path(path)?;
    let parameter = format!("(param \"{BUILD_VOLUME_PARAM}\")");
    let relative = path
        .strip_prefix(volume)
        .map_err(|_| SandboxError::InvalidPath {
            path: path.to_owned(),
            reason: "a build volume rule must name a path inside the build volume",
        })?;
    if relative.as_os_str().is_empty() {
        return Ok(parameter);
    }
    let relative = relative
        .as_os_str()
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    Ok(format!("(string-append {parameter} \"/{relative}\")"))
}

fn push_volume_exact_and_subpath_rule(
    profile: &mut ProfileText,
    operation: &str,
    volume: &Path,
    path: &Path,
) -> Result<(), SandboxError> {
    let path = volume_expression(volume, path)?;
    push_line(
        profile,
        &format!("({operation} (literal {path}) (subpath {path}))"),
    );
    Ok(())
}

fn push_line(profile: &mut ProfileText, line: &str) {
    profile.text.push_str(line);
    profile.text.push('\n');
    let Some((operations, filters)) = line.split_once(" (") else {
        return;
    };
    let Some((effect, names)) = operations.split_once(' ') else {
        return;
    };
    let wildcard = names.split_whitespace().any(|name| name == "file-read*");
    // An explicit file-read-data rule takes precedence over file-read*, even
    // when the wildcard comes later. Give every read rule the same specificity
    // so ordered denies and carve-backs govern actual reads as well as metadata.
    if wildcard {
        profile.text.push_str(effect);
        profile.text.push_str(" file-read-data (");
        profile.text.push_str(filters);
        profile.text.push('\n');
    }
    if profile.exec_follows_read && (wildcard || names == "file-read-data") {
        profile.text.push_str(effect);
        profile.text.push_str(" process-exec (");
        profile.text.push_str(filters);
        profile.text.push('\n');
    }
}
