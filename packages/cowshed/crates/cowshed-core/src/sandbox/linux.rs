//! Linux enforcement: a [`ProfilePolicy`] compiled into the profile a process's launcher
//! enforces (04_sandbox.md, "Linux enforcement").
//!
//! The policy is the one Seatbelt renders, and the enforcers differ in shape. Seatbelt is an
//! ordered list of allows and denies over spellings; Landlock is a set of allows tied to inodes,
//! with no deny. So nothing here starts from a broad read of `/`: a rule exists for each tree the
//! policy admits, and what the policy denies has no rule. Three consequences are decided here
//! rather than left to surprise a launch:
//!
//! - A rule at or beneath a path a later deny covers is dropped, as last-match-wins would deny
//!   it. A subtree rule *above* such a path cannot be honoured, because Landlock cannot carve
//!   the deny back out, so the configuration is refused as a grant that intersects a protected
//!   path.
//! - A workspace-relative deny (`deny`, `deny_write`) names a path inside the workspace mount,
//!   which is admitted whole. It is refused, never dropped.
//! - Lookup and `stat` are not Landlock's to decide, so the metadata-only rules Seatbelt needs
//!   for ancestors, links and literal directories have no counterpart and need none.
//!
//! [`compile`] is the whole decision and reads nothing. The launcher ([`launcher`]) is its shell:
//! namespaces, the loopback connector, the hidden directories, then the ruleset. Linux has no
//! system launcher, so a process launches through the one its own program named ([`NAMED`]).

mod connector;
mod failure;
mod landlock;
mod launcher;
mod mounts;
mod profile;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use super::{
    BUILD_VOLUME_PARAM, Launcher, LauncherError, MainSnapshotStore, ProfilePolicy, RunSandboxMode,
    SandboxConfig, SandboxError, SandboxProfileRole, SeatbeltProfile, cowshed_root,
    main_snapshot_store, shared_daemon_runtime_dir, shared_daemon_runtime_link, validate_path,
};
use crate::capabilities::{GrantAccess, GrantScope};
use cowshed_gateway_types::WorkspaceEndpoint;
use profile::{Access, ExecAuthority, Profile, Rule, Scope};

/// The kernel's name for the running executable: the image itself, whatever its path names now.
const THIS_EXECUTABLE: &str = "/proc/self/exe";

/// The launcher this process's program named, once: itself, by answering the launcher verb
/// ([`dispatch_launcher`]), or the `cowshed-sandbox-exec` built with it ([`name_launcher`]).
/// Nothing else is ever started as a launcher, and nothing is searched for.
static NAMED: OnceLock<Launcher> = OnceLock::new();

/// A binary whose `main` answers the launcher verb is its own launcher: a launch of
/// [`THIS_EXECUTABLE`] runs the image that rendered the profile, whatever is installed or on
/// `PATH` since. Only [`dispatch_launcher`] makes one, so no process that does not answer the
/// verb ever launches its own image.
fn own_launcher() -> Launcher {
    Launcher {
        program: PathBuf::from(THIS_EXECUTABLE),
        verb: Some(launcher::VERB),
    }
}

/// The launcher `executable` is when a program names it: an absolute path to a file. `is_file`
/// answers for the host.
fn named_launcher(
    executable: &Path,
    is_file: impl Fn(&Path) -> bool,
) -> Result<Launcher, LauncherError> {
    if !executable.is_absolute() {
        return Err(LauncherError::NotAbsolute {
            path: executable.to_owned(),
        });
    }
    if !is_file(executable) {
        return Err(LauncherError::NotAFile {
            path: executable.to_owned(),
        });
    }
    Ok(Launcher {
        program: executable.to_owned(),
        verb: None,
    })
}

/// What naming `proposed` answers in a process whose launcher is `standing`: a process has one,
/// and naming it again is nothing.
fn naming(standing: &Launcher, proposed: Launcher) -> Result<(), LauncherError> {
    if *standing == proposed {
        return Ok(());
    }
    Err(LauncherError::AlreadyNamed {
        named: standing.program.clone(),
        refused: proposed.program,
    })
}

pub(super) fn name_launcher(executable: &Path) -> Result<(), LauncherError> {
    let proposed = named_launcher(executable, Path::is_file)?;
    naming(NAMED.get_or_init(|| proposed.clone()), proposed)
}

pub(super) fn launcher() -> Result<&'static Launcher, LauncherError> {
    NAMED.get().ok_or(LauncherError::Unnamed)
}

/// The trees every workspace command reads and starts programs from: the system's programs,
/// libraries and configuration, the Nix store, and the kernel's process and device tables. A
/// root that does not exist on a host has no rule there.
const SYSTEM_ROOTS: &[&str] = &[
    "/bin",
    "/etc",
    "/lib",
    "/lib32",
    "/lib64",
    "/nix/store",
    "/opt",
    "/proc",
    "/sbin",
    "/sys",
    "/usr",
];

/// The system trees controller Git reads: programs and libraries, never the host's
/// configuration or its process table.
const GIT_ROOTS: &[&str] = &[
    "/bin",
    "/lib",
    "/lib32",
    "/lib64",
    "/nix/store",
    "/opt",
    "/sbin",
    "/usr",
];

/// Devices every role reads: entropy and zeroes.
const READ_DEVICES: &[&str] = &["/dev/random", "/dev/urandom", "/dev/zero"];

/// Devices every role reads, writes and controls: the null device and the terminal.
const CONTROL_DEVICES: &[&str] = &["/dev/null", "/dev/ptmx", "/dev/tty"];

/// Where pseudo-terminal slaves appear.
const TERMINALS: &str = "/dev/pts";

/// Why a workspace-relative deny refuses the configuration.
const UNENFORCEABLE_DENY: &str = "a workspace-relative deny cannot be enforced on Linux: Landlock admits the workspace mount whole and has no deny";

/// The rules of a profile as they are admitted, each against the denies that still apply to it.
#[derive(Default)]
struct Rules(Vec<Rule>);

impl Rules {
    /// Admit `access` at `path` unless a path of `denies` covers it. A subtree that would
    /// enclose one of them is refused: nothing could deny it again.
    fn admit(
        &mut self,
        path: &Path,
        scope: Scope,
        access: Access,
        denies: &[&Path],
    ) -> Result<(), SandboxError> {
        if denies.iter().any(|deny| path.starts_with(deny)) {
            return Ok(());
        }
        if scope == Scope::Subtree
            && let Some(deny) = denies.iter().find(|deny| deny.starts_with(path))
        {
            return Err(SandboxError::GrantIntersectsDeny {
                grant: path.to_owned(),
                deny: (*deny).to_owned(),
            });
        }
        self.0.push(Rule {
            path: utf8(path)?,
            scope,
            access,
        });
        Ok(())
    }
}

/// `path` as the profile spells it. The profile is JSON, which holds text.
fn utf8(path: &Path) -> Result<String, SandboxError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| SandboxError::InvalidPath {
            path: path.to_owned(),
            reason: "path is not UTF-8",
        })
}

/// The workspace's gateway socket ([`WorkspaceEndpoint::linux_data_socket`]): the loopback
/// connector's one upstream. A sandbox names its workspace's incarnation only through its CPU
/// pool directory (`<store>/run/cpu/<incarnation as base64url>`), so the incarnation is read
/// back from there; a sandbox with no pool directory has no gateway socket and runs no
/// connector.
fn gateway_socket(config: &SandboxConfig) -> Option<PathBuf> {
    use base64::Engine as _;
    let pool = config.cpu_pool_directory.as_ref()?;
    let identity = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(pool.file_name()?.as_encoded_bytes())
        .ok()
        .filter(|identity| identity.len() == 16)?;
    match WorkspaceEndpoint::linux_data_socket(
        &config.store_root,
        &crate::api::dto::hex_lower(&identity),
    ) {
        WorkspaceEndpoint::Unix(socket) => Some(socket),
        WorkspaceEndpoint::Tcp { .. } => None,
    }
}

/// Compile one authority tier's policy into its Linux profile. `snapshot` is main's
/// linked-snapshot store as the caller resolved it.
fn compile(
    policy: &ProfilePolicy<'_>,
    snapshot: &MainSnapshotStore,
) -> Result<Profile, SandboxError> {
    let config = policy.config;
    if let Some(relative) = config
        .grants
        .deny
        .iter()
        .chain(&config.grants.deny_write)
        .next()
    {
        return Err(SandboxError::InvalidPath {
            path: relative.clone(),
            reason: UNENFORCEABLE_DENY,
        });
    }
    let git = matches!(
        policy.role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    );

    // Seatbelt emits its denies in three places, and an allow is subject to the denies after
    // it: the store and mount root first, then the host's policy denies (project and telemetry
    // roots, main's checkout), then the secrets, last.
    let every_deny: Vec<&Path> = policy.hard_denies.iter().map(AsRef::as_ref).collect();
    let policy_and_secrets: Vec<&Path> = every_deny
        .iter()
        .copied()
        .filter(|deny| *deny != cowshed_root() && *deny != config.mount_root)
        .collect();
    let secrets: Vec<&Path> = policy_and_secrets
        .iter()
        .copied()
        .filter(|deny| !config.additional_denies.iter().any(|extra| extra == deny))
        .collect();

    let mut rules = Rules::default();

    // Before every deny: the system, the installed cowshed, and the explicit grants.
    for root in if git { GIT_ROOTS } else { SYSTEM_ROOTS } {
        rules.admit(Path::new(root), Scope::Subtree, Access::Read, &every_deny)?;
    }
    if !git {
        // The stable name; the rule ties to the installed copy it resolves to at launch.
        rules.admit(
            &crate::host_dirs::installed_binary_directory(&config.home).join("cowshed"),
            Scope::Literal,
            Access::Read,
            &every_deny,
        )?;
    }
    for grant in &policy.read_grants {
        rules.admit(grant, Scope::Subtree, Access::Read, &every_deny)?;
    }
    for grant in &policy.write_grants {
        rules.admit(grant, Scope::Subtree, Access::ReadWrite, &every_deny)?;
    }

    // After the store and mount-root denies: what lies behind them and is this workspace's.
    if let Some(directory) = &config.cpu_pool_directory {
        // Its jobs' jobserver fifos open; nothing there is listed, made or removed.
        rules.admit(
            directory,
            Scope::Subtree,
            Access::Files,
            &policy_and_secrets,
        )?;
    }
    let contribution = &config.capabilities.contribution;
    for cache in &contribution.shared_caches {
        if let Some(access) = granted(cache.access) {
            rules.admit(&cache.path, Scope::Subtree, access, &policy_and_secrets)?;
        }
    }
    for grant in &contribution.grants {
        if let Some(access) = granted(grant.access) {
            let scope = match grant.scope {
                GrantScope::Literal => Scope::Literal,
                GrantScope::Subtree => Scope::Subtree,
            };
            rules.admit(&grant.path, scope, access, &policy_and_secrets)?;
        }
    }
    rules.admit(
        &config.workspace_mount,
        Scope::Subtree,
        match config.mode {
            RunSandboxMode::ReadOnly => Access::Read,
            RunSandboxMode::ReadWrite => Access::ReadWrite,
        },
        &policy_and_secrets,
    )?;

    // After the policy denies: the controller's own carve-backs, which only a secret closes.
    if let Some(repository) = &config.git_worktree_repository {
        rules.admit(repository, Scope::Subtree, Access::ReadWrite, &secrets)?;
    }
    if let MainSnapshotStore::Readable { store, resolved } = snapshot {
        for path in [store, resolved] {
            rules.admit(path, Scope::Subtree, Access::Read, &secrets)?;
        }
    }
    rules.admit(
        &config.exec_temp_dir,
        Scope::Subtree,
        Access::ReadWrite,
        &secrets,
    )?;
    // A contributed daemon leaf is shared across modes, never the runtime directory around it:
    // a read-only job writes it and nothing else of the checkout.
    let shared_alias = shared_daemon_runtime_link(config);
    let shared_runtime = shared_daemon_runtime_dir(config);
    for alias in &contribution.daemon_isolation.directories {
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
        rules.admit(
            &shared_runtime.join(relative),
            Scope::Subtree,
            Access::ReadWrite,
            &secrets,
        )?;
    }
    for device in READ_DEVICES {
        rules.admit(Path::new(device), Scope::Literal, Access::Read, &secrets)?;
    }
    for device in CONTROL_DEVICES {
        rules.admit(Path::new(device), Scope::Literal, Access::Device, &secrets)?;
    }
    rules.admit(
        Path::new(TERMINALS),
        Scope::Subtree,
        Access::Device,
        &secrets,
    )?;

    let Rules(mut rules) = rules;
    rules.sort();
    rules.dedup();
    Ok(Profile {
        build: profile::BUILD,
        exec: match policy.role {
            SandboxProfileRole::GitDiscoveryGitOnly => ExecAuthority::LaunchedProgram,
            SandboxProfileRole::TrustedSupervisor
            | SandboxProfileRole::ExecutedChild
            | SandboxProfileRole::GitDiscovery => ExecAuthority::FollowsRead,
        },
        rules,
        build_volume: config.build_volume_mount.is_some(),
        // Where the store's supervisors and their manager listen.
        masks: vec![utf8(&config.store_root.join("run"))?],
        sockets: policy
            .sockets
            .iter()
            .map(|socket| utf8(socket))
            .collect::<Result<_, _>>()?,
        gateway: gateway_socket(config)
            .map(|socket| utf8(&socket))
            .transpose()?,
    })
}

/// What a capability's access admits here. Metadata is not Landlock's to decide.
fn granted(access: GrantAccess) -> Option<Access> {
    match access {
        GrantAccess::ReadMetadata => None,
        GrantAccess::Read => Some(Access::Read),
        GrantAccess::ReadWrite => Some(Access::ReadWrite),
    }
}

/// Refuse what [`compile`] refuses, before a grant snapshot is published. Main's snapshot store
/// only ever adds reads, so it is left unresolved here.
pub(super) fn validate(policy: &ProfilePolicy<'_>) -> Result<(), SandboxError> {
    compile(policy, &MainSnapshotStore::Absent).map(drop)
}

pub(super) fn render(policy: ProfilePolicy<'_>) -> Result<SeatbeltProfile, SandboxError> {
    let config = policy.config;
    let snapshot = config
        .main_mount
        .as_ref()
        .map_or(MainSnapshotStore::Absent, |main| {
            main_snapshot_store(config, main)
        });
    if let MainSnapshotStore::Refused(warning) = &snapshot {
        cowshed_os::eprintln!("{warning}");
    }
    let text = compile(&policy, &snapshot)?
        .to_text()
        .map_err(|_| SandboxError::InvalidPath {
            path: config.workspace_mount.clone(),
            reason: "its sandbox profile cannot be written as JSON",
        })?;
    Ok(SeatbeltProfile {
        text,
        build_volume: config.build_volume_mount.clone(),
    })
}

pub(super) fn dispatch_launcher() -> Option<i32> {
    let code = launcher::dispatch(BUILD_VOLUME_PARAM);
    if code.is_none() {
        // The first naming stands: a program that named another launcher before this keeps it.
        NAMED.get_or_init(own_launcher);
    }
    code
}

pub(super) fn launcher_main() -> i32 {
    launcher::main(BUILD_VOLUME_PARAM)
}

#[cfg(test)]
mod escape_tests;

#[cfg(test)]
mod tests {
    use super::super::tests::{config, grant, with_contribution};
    use super::super::{profile_policy, validate_sandbox_config};
    use super::*;

    fn compiled(config: &SandboxConfig, role: SandboxProfileRole) -> Profile {
        compile(
            &profile_policy(config, role).unwrap(),
            &MainSnapshotStore::Absent,
        )
        .unwrap()
    }

    fn refusal(config: &SandboxConfig) -> SandboxError {
        compile(
            &profile_policy(config, SandboxProfileRole::ExecutedChild).unwrap(),
            &MainSnapshotStore::Absent,
        )
        .unwrap_err()
    }

    /// A program names its launcher by an absolute path to a file, a process has one launcher,
    /// and only a binary that answers the launcher verb launches its own image.
    #[test]
    fn a_named_launcher_is_an_absolute_file_and_a_process_has_one() {
        let built = Path::new("/w/target/debug/cowshed-sandbox-exec");
        let present = |path: &Path| path == built;
        for (executable, expected) in [
            (
                "/w/target/debug/cowshed-sandbox-exec",
                Ok(Launcher {
                    program: built.to_owned(),
                    verb: None,
                }),
            ),
            (
                "target/debug/cowshed-sandbox-exec",
                Err(LauncherError::NotAbsolute {
                    path: PathBuf::from("target/debug/cowshed-sandbox-exec"),
                }),
            ),
            (
                "cowshed-sandbox-exec",
                Err(LauncherError::NotAbsolute {
                    path: PathBuf::from("cowshed-sandbox-exec"),
                }),
            ),
            (
                "/w/target/release/cowshed-sandbox-exec",
                Err(LauncherError::NotAFile {
                    path: PathBuf::from("/w/target/release/cowshed-sandbox-exec"),
                }),
            ),
            (
                "/w/target/debug",
                Err(LauncherError::NotAFile {
                    path: PathBuf::from("/w/target/debug"),
                }),
            ),
        ] {
            assert_eq!(
                named_launcher(Path::new(executable), present),
                expected,
                "{executable}"
            );
        }

        let named = named_launcher(built, present).unwrap();
        let other = Launcher {
            program: PathBuf::from("/elsewhere/cowshed-sandbox-exec"),
            verb: None,
        };
        let own = own_launcher();
        assert_eq!(naming(&named, named.clone()), Ok(()), "named again");
        for (standing, proposed) in [(&named, &other), (&own, &named)] {
            assert_eq!(
                naming(standing, proposed.clone()),
                Err(LauncherError::AlreadyNamed {
                    named: standing.program.clone(),
                    refused: proposed.program.clone(),
                }),
                "{standing:?}"
            );
        }

        // The two launchers there are: an image that answers the verb, and a program that is
        // nothing but a launcher.
        assert_eq!(own.program(), Path::new("/proc/self/exe"));
        let file = Path::new("/s/run/profiles/a.sb");
        assert_eq!(
            own.launch_arguments(file, None),
            ["__sandbox-exec", "-f", "/s/run/profiles/a.sb", "--"]
        );
        assert_eq!(
            named.launch_arguments(file, None),
            ["-f", "/s/run/profiles/a.sb", "--"]
        );
    }

    /// The access a profile holds at exactly `path`, in rule order.
    fn at(profile: &Profile, path: &str) -> Vec<(Scope, Access)> {
        profile
            .rules
            .iter()
            .filter(|rule| rule.path == path)
            .map(|rule| (rule.scope, rule.access))
            .collect()
    }

    /// The stable name of the installed cowshed, as a rule spells it.
    fn installed_cowshed(config: &SandboxConfig) -> &'static str {
        crate::host_dirs::installed_binary_directory(&config.home)
            .join("cowshed")
            .into_os_string()
            .into_string()
            .unwrap()
            .leak()
    }

    /// Whether any subtree rule of `profile` covers `path`.
    fn covered(profile: &Profile, path: &str) -> bool {
        profile.rules.iter().any(|rule| {
            rule.scope == Scope::Subtree && Path::new(path).starts_with(Path::new(&rule.path))
        })
    }

    const WORKSPACE: &str = "/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount";

    #[test]
    fn the_baseline_is_closed_and_opens_the_system_the_workspace_and_its_temp_dir() {
        let profile = compiled(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        );
        assert_eq!(profile.build, profile::BUILD);
        assert_eq!(profile.exec, ExecAuthority::FollowsRead);
        for root in SYSTEM_ROOTS {
            assert_eq!(
                at(&profile, root),
                [(Scope::Subtree, Access::Read)],
                "{root}"
            );
        }
        assert_eq!(
            at(&profile, WORKSPACE),
            [(Scope::Subtree, Access::ReadWrite)]
        );
        assert_eq!(
            at(&profile, "/private/tmp/cowshed-raven"),
            [(Scope::Subtree, Access::ReadWrite)]
        );
        assert_eq!(
            at(&profile, "/dev/null"),
            [(Scope::Literal, Access::Device)]
        );
        assert_eq!(
            at(&profile, "/dev/urandom"),
            [(Scope::Literal, Access::Read)]
        );
        assert_eq!(at(&profile, "/dev/pts"), [(Scope::Subtree, Access::Device)]);
        assert_eq!(
            at(
                &profile,
                installed_cowshed(&config(RunSandboxMode::ReadWrite))
            ),
            [(Scope::Literal, Access::Read)],
            "the installed cowshed alone, never its directory"
        );
        // Closed: nothing opens the filesystem root, HOME, the store, the mount root, a
        // sibling workspace, a secret, or the world-shared temp directory.
        for closed in [
            "/",
            "/Users/tester",
            "/Users/tester/.ssh/id_ed25519",
            "/Users/tester/.cargo/credentials.toml",
            "/Users/tester/.cowshed/mnt",
            "/Users/tester/.cowshed/mnt/acme/widget/workspaces/wren/mount",
            "/private/cowshed/store",
            "/private/tmp/cowshed-raven-store/run/profiles",
            "/tmp",
            "/run/user/1000/bus",
            "/var",
        ] {
            assert!(!covered(&profile, closed), "{closed} must have no rule");
        }
        assert_eq!(
            profile.masks,
            ["/private/tmp/cowshed-raven-store/run"],
            "the store's supervisor sockets are hidden"
        );
        assert_eq!(profile.sockets, ["/var/run/nix/daemon-socket/socket"]);
        assert_eq!(profile.gateway, None, "no pool directory, no incarnation");
        assert!(!profile.build_volume);
        let mut sorted = profile.rules.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(profile.rules, sorted, "one authority is one text");
    }

    #[test]
    fn a_grant_opens_its_subtree_read_or_read_write_and_nothing_beside_it() {
        let profile = compiled(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        );
        assert_eq!(
            at(&profile, "/opt/shared"),
            [(Scope::Subtree, Access::Read)],
            "a read grant, named twice, is one rule"
        );
        assert_eq!(
            at(&profile, "/opt/output"),
            [(Scope::Subtree, Access::ReadWrite)]
        );

        let mut granted = config(RunSandboxMode::ReadWrite);
        granted.grants.read = vec![PathBuf::from("/srv/data")];
        granted.grants.write = vec![PathBuf::from("/srv/out")];
        let profile = compiled(&granted, SandboxProfileRole::ExecutedChild);
        assert_eq!(at(&profile, "/srv/data"), [(Scope::Subtree, Access::Read)]);
        assert_eq!(
            at(&profile, "/srv/out"),
            [(Scope::Subtree, Access::ReadWrite)]
        );
        assert!(!covered(&profile, "/srv/other"));
        assert!(!covered(&profile, "/srv"));
    }

    #[test]
    fn a_grant_that_reaches_a_protected_path_from_either_side_is_refused() {
        for (grant, deny) in [
            // Above a secret, at one, beneath one; above and inside the mount root and store.
            ("/Users/tester", "/Users/tester/.aws"),
            ("/Users/tester/.ssh", "/Users/tester/.ssh"),
            ("/Users/tester/.ssh/id_ed25519", "/Users/tester/.ssh"),
            ("/Users/tester/.cowshed", "/Users/tester/.cowshed/mnt"),
            (
                "/Users/tester/.cowshed/mnt/acme/widget/workspaces/wren/mount",
                "/Users/tester/.cowshed/mnt",
            ),
            ("/private", "/private/cowshed"),
            ("/private/cowshed/store/run", "/private/cowshed"),
        ] {
            for write in [false, true] {
                let mut config = config(RunSandboxMode::ReadWrite);
                let grants = if write {
                    &mut config.grants.write
                } else {
                    &mut config.grants.read
                };
                *grants = vec![PathBuf::from(grant)];
                let expected = Err(SandboxError::GrantIntersectsDeny {
                    grant: PathBuf::from(grant),
                    deny: PathBuf::from(deny),
                });
                assert_eq!(validate_sandbox_config(&config), expected, "{grant}");
                assert_eq!(
                    super::super::seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
                        .map(drop),
                    expected,
                    "{grant}"
                );
            }
        }
    }

    /// Seatbelt lets a capability's subtree enclose a secret, because its deny comes last.
    /// Landlock has no last word: the subtree would open the secret, so it is refused. A
    /// literal beside the secret and a subtree clear of it are admitted.
    #[test]
    fn a_capability_subtree_that_encloses_a_protected_path_is_refused() {
        let enclosing = with_contribution(
            vec![grant(
                "/Users/tester/.cargo",
                GrantScope::Subtree,
                GrantAccess::Read,
            )],
            &[],
        );
        let expected = SandboxError::GrantIntersectsDeny {
            grant: PathBuf::from("/Users/tester/.cargo"),
            deny: PathBuf::from("/Users/tester/.cargo/config"),
        };
        assert_eq!(refusal(&enclosing), expected);
        assert_eq!(validate_sandbox_config(&enclosing), Err(expected));

        let beside = with_contribution(
            vec![
                grant(
                    "/Users/tester/.cargo",
                    GrantScope::Literal,
                    GrantAccess::Read,
                ),
                grant(
                    "/Users/tester/.cargo/bin",
                    GrantScope::Subtree,
                    GrantAccess::Read,
                ),
                grant(
                    "/Users/tester/.rustup",
                    GrantScope::Subtree,
                    GrantAccess::ReadMetadata,
                ),
            ],
            &["/Users/tester/.cargo/registry"],
        );
        validate_sandbox_config(&beside).unwrap();
        let profile = compiled(&beside, SandboxProfileRole::ExecutedChild);
        assert_eq!(
            at(&profile, "/Users/tester/.cargo"),
            [(Scope::Literal, Access::Read)]
        );
        assert_eq!(
            at(&profile, "/Users/tester/.cargo/bin"),
            [(Scope::Subtree, Access::Read)]
        );
        assert_eq!(
            at(&profile, "/Users/tester/.cargo/registry"),
            [(Scope::Subtree, Access::ReadWrite)]
        );
        assert!(
            at(&profile, "/Users/tester/.rustup").is_empty(),
            "lookup needs no rule"
        );
        assert!(!covered(&profile, "/Users/tester/.cargo/credentials.toml"));
    }

    #[test]
    fn read_only_drops_write_on_the_mount_and_keeps_every_other_write() {
        let mut read_write = config(RunSandboxMode::ReadWrite);
        read_write.build_volume_mount = Some(
            read_write
                .mount_root
                .join(".build/acme/widget/0123456789abcdef0123456789abcdef"),
        );
        let mut read_only = read_write.clone();
        read_only.mode = RunSandboxMode::ReadOnly;
        let writable = compiled(&read_write, SandboxProfileRole::ExecutedChild);
        let narrowed = compiled(&read_only, SandboxProfileRole::ExecutedChild);

        assert_eq!(
            at(&writable, WORKSPACE),
            [(Scope::Subtree, Access::ReadWrite)]
        );
        assert_eq!(at(&narrowed, WORKSPACE), [(Scope::Subtree, Access::Read)]);
        let elsewhere = |profile: &Profile| -> Vec<Rule> {
            profile
                .rules
                .iter()
                .filter(|rule| rule.path != WORKSPACE)
                .cloned()
                .collect()
        };
        assert_eq!(
            elsewhere(&writable),
            elsewhere(&narrowed),
            "only the mount's rule differs"
        );
        assert!(
            narrowed.build_volume && writable.build_volume,
            "build state stays writable beside read-only source"
        );
        assert!(
            !narrowed
                .to_text()
                .unwrap()
                .contains("0123456789abcdef0123456789abcdef"),
            "the volume is bound at launch, so a pivot launches the same text"
        );
    }

    #[test]
    fn a_read_only_job_still_writes_its_shared_daemon_leaf() {
        let mut sandbox = config(RunSandboxMode::ReadOnly);
        let alias = shared_daemon_runtime_link(&sandbox);
        sandbox
            .capabilities
            .contribution
            .daemon_isolation
            .directories = vec![alias.join("nx"), PathBuf::from("/elsewhere/run")];
        let profile = compiled(&sandbox, SandboxProfileRole::ExecutedChild);
        assert_eq!(at(&profile, WORKSPACE), [(Scope::Subtree, Access::Read)]);
        assert_eq!(
            at(&profile, &format!("{WORKSPACE}/.cowshed/run/nx")),
            [(Scope::Subtree, Access::ReadWrite)]
        );
        assert!(!covered(&profile, "/elsewhere/run"));

        sandbox
            .capabilities
            .contribution
            .daemon_isolation
            .directories = vec![alias.clone()];
        assert_eq!(
            refusal(&sandbox),
            SandboxError::InvalidPath {
                path: alias,
                reason: "a shared daemon grant must name a leaf below its runtime root",
            }
        );
    }

    #[test]
    fn a_workspace_relative_deny_is_refused_rather_than_left_unenforced() {
        for deny_write in [false, true] {
            let mut sandbox = config(RunSandboxMode::ReadWrite);
            let relative = PathBuf::from("secrets/production.env");
            if deny_write {
                sandbox.grants.deny_write = vec![relative.clone()];
            } else {
                sandbox.grants.deny = vec![relative.clone()];
            }
            let expected = SandboxError::InvalidPath {
                path: relative,
                reason: UNENFORCEABLE_DENY,
            };
            assert_eq!(refusal(&sandbox), expected);
            assert_eq!(validate_sandbox_config(&sandbox), Err(expected));
        }
    }

    /// A policy deny (the project root, main's checkout) closes what was admitted before it and
    /// leaves the controller's carve-backs, which follow it; a secret closes those too. A tree
    /// admitted before a deny that it would enclose is refused.
    #[test]
    fn a_rule_beneath_a_later_deny_is_dropped_and_a_carve_back_after_it_stays() {
        let main = "/opt/src/widget";
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        sandbox.grants.read.clear();
        sandbox.grants.write.clear();
        sandbox.additional_denies = vec![PathBuf::from("/opt")];
        sandbox.git_worktree_repository = Some(PathBuf::from(main).join(".git"));
        let profile = compiled(&sandbox, SandboxProfileRole::ExecutedChild);
        assert!(
            at(&profile, "/opt").is_empty(),
            "the policy deny follows the system root"
        );
        assert_eq!(at(&profile, "/usr"), [(Scope::Subtree, Access::Read)]);
        assert_eq!(
            at(&profile, "/opt/src/widget/.git"),
            [(Scope::Subtree, Access::ReadWrite)],
            "the worktree repository is carved back after the policy deny"
        );
        assert!(!covered(&profile, "/opt/src/widget/src/lib.rs"));

        // A secret is the last word: it closes even a controller carve-back.
        sandbox.git_worktree_repository = Some(PathBuf::from("/Users/tester/.ssh/repository"));
        assert!(
            at(
                &compiled(&sandbox, SandboxProfileRole::ExecutedChild),
                "/Users/tester/.ssh/repository"
            )
            .is_empty()
        );

        sandbox.additional_denies = vec![PathBuf::from("/usr/lib/protected")];
        assert_eq!(
            refusal(&sandbox),
            SandboxError::GrantIntersectsDeny {
                grant: PathBuf::from("/usr"),
                deny: PathBuf::from("/usr/lib/protected"),
            },
            "Landlock cannot open /usr and close a tree inside it"
        );
    }

    #[test]
    fn mains_snapshot_store_is_read_where_it_resolves_and_nowhere_when_refused() {
        let sandbox = config(RunSandboxMode::ReadWrite);
        let policy = profile_policy(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        let readable = compile(
            &policy,
            &MainSnapshotStore::Readable {
                store: PathBuf::from("/Users/tester/src/widget/.cache/linked-snapshots"),
                resolved: PathBuf::from("/volumes/build/declared/.cache/linked-snapshots"),
            },
        )
        .unwrap();
        for path in [
            "/Users/tester/src/widget/.cache/linked-snapshots",
            "/volumes/build/declared/.cache/linked-snapshots",
        ] {
            assert_eq!(at(&readable, path), [(Scope::Subtree, Access::Read)]);
        }
        assert!(!covered(&readable, "/Users/tester/src/widget/.cache/nx"));
        let refused = compile(&policy, &MainSnapshotStore::Refused("warned".into())).unwrap();
        assert!(!covered(
            &refused,
            "/Users/tester/src/widget/.cache/linked-snapshots"
        ));
    }

    #[test]
    fn controller_git_reads_programs_only_and_the_git_only_role_starts_only_git() {
        let sandbox = config(RunSandboxMode::ReadOnly);
        for (role, exec) in [
            (SandboxProfileRole::GitDiscovery, ExecAuthority::FollowsRead),
            (
                SandboxProfileRole::GitDiscoveryGitOnly,
                ExecAuthority::LaunchedProgram,
            ),
        ] {
            let profile = compiled(&sandbox, role);
            assert_eq!(profile.exec, exec, "{role:?}");
            for root in GIT_ROOTS {
                assert_eq!(at(&profile, root), [(Scope::Subtree, Access::Read)]);
            }
            for closed in ["/etc/passwd", "/proc/1/cmdline", "/sys/kernel"] {
                assert!(!covered(&profile, closed), "{role:?} reads {closed}");
            }
            assert!(
                at(&profile, installed_cowshed(&sandbox)).is_empty(),
                "{role:?}"
            );
            assert_eq!(
                at(&profile, "/dev/null"),
                [(Scope::Literal, Access::Device)]
            );
        }
    }

    /// Landlock cannot narrow one tier inside the workspace mount, so the trusted supervisor
    /// and the executed child compile to one profile: the child's narrowing is not enforced.
    #[test]
    fn the_supervisor_and_child_tiers_compile_to_one_profile() {
        let sandbox = config(RunSandboxMode::ReadWrite);
        assert_eq!(
            compiled(&sandbox, SandboxProfileRole::TrustedSupervisor),
            compiled(&sandbox, SandboxProfileRole::ExecutedChild)
        );
    }

    #[test]
    fn the_pool_directory_opens_for_its_fifos_and_names_the_gateway_socket() {
        let incarnation =
            crate::metadata::WorkspaceIncarnation::new("00112233445566778899aabbccddeeff").unwrap();
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        let pool = crate::runtime::job_cpu::cpu_pool_directory(&sandbox.store_root, &incarnation);
        sandbox.cpu_pool_directory = Some(pool.clone());
        let profile = compiled(&sandbox, SandboxProfileRole::ExecutedChild);
        assert_eq!(
            at(&profile, pool.to_str().unwrap()),
            [(Scope::Subtree, Access::Files)]
        );
        assert_eq!(
            profile.gateway.as_deref(),
            Some(
                "/private/tmp/cowshed-raven-store/run/gateway/00112233445566778899aabbccddeeff.sock"
            )
        );

        sandbox.cpu_pool_directory = Some(sandbox.store_root.join("run/cpu/not-an-incarnation"));
        assert_eq!(
            compiled(&sandbox, SandboxProfileRole::ExecutedChild).gateway,
            None
        );
    }

    /// A workspace that records no port block (every Linux workspace) launches as any other:
    /// its sandbox compiles to a launch plan whose connector relays to the workspace's own
    /// gateway socket, and its jobs' environment names that connector's one port in every proxy
    /// variable, exports no block and withholds a caller's.
    #[test]
    fn a_workspace_with_no_port_block_launches_behind_its_connector() {
        use crate::workspace_environment::{
            PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, job_gateway_environment,
        };
        let incarnation =
            crate::metadata::WorkspaceIncarnation::new("00112233445566778899aabbccddeeff").unwrap();
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        sandbox.port_block = None;
        sandbox.cpu_pool_directory = Some(crate::runtime::job_cpu::cpu_pool_directory(
            &sandbox.store_root,
            &incarnation,
        ));

        validate_sandbox_config(&sandbox).unwrap();
        let rendered =
            super::super::seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        let plan = Profile::parse(rendered.text()).unwrap();
        assert_eq!(
            plan.gateway.map(PathBuf::from).map(WorkspaceEndpoint::Unix),
            Some(WorkspaceEndpoint::linux_data_socket(
                &sandbox.store_root,
                incarnation.as_str()
            )),
            "the connector's upstream is the socket the gateway serves the workspace on"
        );

        let token = cowshed_gateway_types::WorkspaceToken::from_bytes(
            [7; cowshed_gateway_types::TOKEN_BYTES],
        );
        let environment = job_gateway_environment(sandbox.port_block, &token);
        let proxy = format!("http://cowshed:{}@{}", token.encode(), connector::ADDRESS);
        assert!(proxy.ends_with("@127.0.0.1:7644"), "{proxy}");
        for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
            assert!(environment.owned.contains(&(name, proxy.clone())), "{name}");
        }
        assert_eq!(environment.withheld, [PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV]);
        assert!(
            environment
                .owned
                .iter()
                .all(|(name, _)| ![PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV].contains(name)),
            "no block is exported"
        );
    }

    #[test]
    fn a_path_that_is_not_text_is_refused_by_name() {
        use std::os::unix::ffi::OsStringExt as _;
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        let path = PathBuf::from(std::ffi::OsString::from_vec(b"/srv/\xff".to_vec()));
        sandbox.grants.read = vec![path.clone()];
        assert_eq!(
            refusal(&sandbox),
            SandboxError::InvalidPath {
                path,
                reason: "path is not UTF-8",
            }
        );
    }

    #[test]
    fn a_rendered_profile_is_the_compiled_one_and_carries_its_build_volume() {
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        let volume = sandbox
            .mount_root
            .join(".build/acme/widget/0123456789abcdef0123456789abcdef");
        sandbox.build_volume_mount = Some(volume.clone());
        let rendered =
            super::super::seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        assert_eq!(rendered.build_volume(), Some(volume.as_path()));
        assert_eq!(
            Profile::parse(rendered.text()).unwrap(),
            compiled(&sandbox, SandboxProfileRole::ExecutedChild)
        );
    }
}
