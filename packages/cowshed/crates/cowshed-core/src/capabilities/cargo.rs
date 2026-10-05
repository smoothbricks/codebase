//! Cargo (`Cargo.toml`): one literal `$CARGO_HOME` shared with the host, git fetches through the
//! Git CLI, and the workspace trust bundle.
//!
//! Cargo fingerprints a registry or git dependency by the absolute path of its source under
//! `$CARGO_HOME`, so a sandbox reaches the host's own `~/.cargo` once host setup has relocated its
//! `registry` and `git` onto the caches volume; any other spelling of the same bytes dirties every
//! dependency a clone's copied `target/` holds. Until then cargo keeps its private default under
//! the sandbox HOME. Configuration, credentials and the rest of `~/.cargo` stay host-owned.
//!
//! A host whose toolchain is rustup's gets it read-only: the proxies in `~/.cargo/bin`, and the
//! settings and toolchains under `~/.rustup`, reached through an owned `RUSTUP_HOME` because the
//! sandbox HOME is private. Nothing a sandbox runs can install or update a toolchain there.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityGrant, CapabilityId, DetectionContext, DetectionScope,
    Detector, EnvAction, GrantAccess, GrantScope,
};
use crate::fork_lock::Fenced;
use crate::{CowshedError, Result};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Cargo,
    marker_kind: super::MarkerKind::File,
    all: &[],
    any: &["Cargo.toml"],
    scope: DetectionScope::Project,
    host_cache_homes: &[&HOME],
    reached_from: Some(super::build_state_discovery::tracked_cargo_convention),
    contribute,
};

/// What cargo itself writes at the root of `$CARGO_HOME` while it resolves and builds: the
/// package-cache locks, and the global-cache usage database with its rollback journal.
pub const STATE_FILES: [&str; 4] = [
    ".package-cache",
    ".package-cache-mutate",
    ".global-cache",
    ".global-cache-journal",
];

/// Cargo's `registry` (index, fetched `.crate` archives, unpacked sources) and `git` (bare
/// databases and checkouts of git dependencies): read-at-build caches cargo writes once per crate
/// or revision (03_caches.md, third layer). The rest of `~/.cargo` stays on the host.
pub static HOME: SharedToolHome = SharedToolHome {
    variable: Some("CARGO_HOME"),
    home: ".cargo",
    layout: SharedLayout::Split {
        links: &[("registry", "cargo/registry"), ("git", "cargo/git")],
        state_files: &STATE_FILES,
    },
    linked_from_checkouts: false,
};

/// Cargo honors `url.insteadOf` only through the Git CLI, which is where the workspace's fetch
/// include (02_workspaces.md "Remote code ingress") applies.
pub const GIT_FETCH_WITH_CLI_ENV: &str = "CARGO_NET_GIT_FETCH_WITH_CLI";
/// Cargo's documented environment spelling of `http.cainfo`.
pub const CA_ENV: &str = "CARGO_HTTP_CAINFO";
const RUSTUP_HOME_ENV: &str = "RUSTUP_HOME";

/// rustup's proxies, which dispatch on the name they are invoked by.
const RUSTUP_PROXIES: &str = ".cargo/bin";
const RUSTUP_HOME: &str = ".rustup";
/// rustup writes its settings file at install; its presence is the rustup convention.
const RUSTUP_SETTINGS: &str = "settings.toml";
const RUSTUP_TOOLCHAINS: &str = "toolchains";
/// The directories a rustup proxy ensures exist on every dispatch.
const RUSTUP_STATE_DIRECTORIES: [&str; 3] = ["update-hashes", "downloads", "tmp"];
/// The commands a cargo project runs: rustup's proxies first, then the host's own installations.
const PROGRAMS: [&str; 4] = ["cargo", "rustc", "rustdoc", "rustup"];

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = super::shared_tool_contribution(context, &HOME)?;
    // Build state follows the tracked tree's Cargo config, never a caller shell's target dir.
    contribution
        .env
        .insert("CARGO_TARGET_DIR", EnvAction::Unset);
    contribution.env.insert(
        GIT_FETCH_WITH_CLI_ENV,
        EnvAction::Own(OsString::from("true")),
    );
    if let Some(bundle) = context.trust_bundle {
        contribution
            .env
            .insert(CA_ENV, EnvAction::Default(bundle.as_os_str().to_owned()));
    }
    let rustup_home = context.home.join(RUSTUP_HOME);
    let proxies = context.home.join(RUSTUP_PROXIES);
    let settings = rustup_home.join(RUSTUP_SETTINGS);
    // The sandboxed supervisor repeats this probe beneath the HOME-wide read deny: the settings
    // file is granted whether or not it exists, so its absence reads as absence there too.
    contribution
        .grants
        .push(read(settings.clone(), GrantScope::Literal));
    if is_file(&settings)? {
        contribution.env.insert(
            RUSTUP_HOME_ENV,
            EnvAction::Own(rustup_home.clone().into_os_string()),
        );
        contribution.grants.extend([
            read(rustup_home.clone(), GrantScope::Literal),
            read(rustup_home.join(RUSTUP_TOOLCHAINS), GrantScope::Subtree),
            read(proxies.clone(), GrantScope::Subtree),
        ]);
        // A proxy ensures rustup's state directories exist before it dispatches, and reads a
        // directory it cannot stat as missing, then fails creating it ("File exists"). Their
        // metadata is granted; their contents and every write stay denied.
        contribution.grants.extend(
            RUSTUP_STATE_DIRECTORIES
                .map(|directory| read(rustup_home.join(directory), GrantScope::Literal)),
        );
    }
    let mut directories = vec![proxies];
    directories.extend(super::host_program_directories(context));
    for program in PROGRAMS {
        super::add_bootstrap(&mut contribution, context, program, &directories)?;
    }
    Ok(contribution)
}

fn read(path: PathBuf, scope: GrantScope) -> CapabilityGrant {
    CapabilityGrant {
        path,
        scope,
        access: GrantAccess::Read,
    }
}

fn is_file(path: &Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(CowshedError::environment_missing(
            format!("cannot inspect rustup at {}: {error}", path.display()),
            "repair the host rustup installation and retry",
        )),
    }
}

/// Cargo's own package-cache locks, held exclusively while host setup moves its caches.
///
/// Cargo takes these same `flock`s before it reads or writes `registry` or `git`, so holding
/// them keeps every cargo process on the host out of the caches for the duration. Fenced: cargo
/// gets them back as soon as this is dropped, whatever this process is spawning (`fork_lock`).
pub struct CacheLock {
    _files: Vec<Fenced<fs::File>>,
}

/// `Ok(None)` when a cargo process holds a lock right now.
pub fn try_lock_caches(cargo_home: &Path) -> io::Result<Option<CacheLock>> {
    let mut files = Vec::with_capacity(2);
    for name in &STATE_FILES[..2] {
        let file = Fenced::new(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(cargo_home.join(name))?,
        );
        // SAFETY: the descriptor is owned by `file`, which outlives the call.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(None);
            }
            return Err(error);
        }
        files.push(file);
    }
    Ok(Some(CacheLock { _files: files }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::capabilities::CacheMount;
    use crate::capabilities::test_support::{Fixture, assert_switch};

    /// The grants a contribution makes in the fixture's host home: host toolchains a test
    /// machine happens to have installed elsewhere are not this detector's subject.
    fn home_grants(contribution: &CapabilityContribution, home: &Path) -> Vec<CapabilityGrant> {
        contribution
            .grants
            .iter()
            .filter(|grant| grant.path.starts_with(home))
            .cloned()
            .collect()
    }

    /// What the sandboxed supervisor's repeat of the search probes beneath HOME: rustup's
    /// settings file and each program's literal candidate path, nothing listable.
    fn probes(home: &Path) -> Vec<CapabilityGrant> {
        let mut probes = vec![read(
            home.join(RUSTUP_HOME).join(RUSTUP_SETTINGS),
            GrantScope::Literal,
        )];
        for program in PROGRAMS {
            for directory in [
                RUSTUP_PROXIES,
                ".nix-profile/bin",
                ".local/state/nix/profile/bin",
            ] {
                probes.push(read(
                    home.join(directory).join(program),
                    GrantScope::Literal,
                ));
            }
        }
        probes
    }

    #[test]
    fn cargo_toml_switches_cargo_on_and_off() {
        assert_switch(&DETECTOR, &["Cargo.toml"]);
    }

    /// Without relocated caches cargo keeps its private default, so `CARGO_HOME` is the
    /// sandbox's to leave unset and no part of the host's `~/.cargo` is granted beyond the exact
    /// program paths its search probes. A plain host (no rustup) adds no toolchain authority.
    #[test]
    fn an_unshared_cargo_home_and_no_rustup_grant_nothing_on_the_host() {
        let fixture = Fixture::new();
        fixture.files(&["Cargo.toml"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("cargo detected");
        assert_eq!(
            contribution.env,
            BTreeMap::from([
                ("CARGO_HOME", EnvAction::Unset),
                ("CARGO_TARGET_DIR", EnvAction::Unset),
                (GIT_FETCH_WITH_CLI_ENV, EnvAction::Own("true".into())),
            ])
        );
        assert_eq!(
            home_grants(&contribution, &fixture.home),
            probes(&fixture.home)
        );
        // The caches volume exists, so the shared directories are prepared and granted for
        // when setup relocates the host links; nothing names the host path.
        assert_eq!(
            contribution.cache_mounts,
            vec![
                CacheMount {
                    source: fixture.caches.join("cargo/registry"),
                    private_target: None,
                },
                CacheMount {
                    source: fixture.caches.join("cargo/git"),
                    private_target: None,
                },
            ]
        );
    }

    /// Once both host links resolve to the caches volume, every child builds against the host's
    /// literal `~/.cargo`, reading its links and writing only cargo's own root state files; the
    /// workspace trust bundle is a default a caller may replace.
    #[test]
    fn a_shared_cargo_home_is_the_host_path_with_exact_state_file_grants() {
        let fixture = Fixture::new();
        fixture.files(&["Cargo.toml"]);
        let cargo_home = fixture.home.join(".cargo");
        fs::create_dir_all(&cargo_home).expect("host cargo home");
        for (link, shared) in [("registry", "cargo/registry"), ("git", "cargo/git")] {
            let target = fixture.caches.join(shared);
            fs::create_dir_all(&target).expect("shared cache");
            std::os::unix::fs::symlink(&target, cargo_home.join(link)).expect("host link");
        }
        let bundle = fixture.root.join(".cowshed/ca-bundle.pem");
        let context = DetectionContext {
            trust_bundle: Some(&bundle),
            ..fixture.context()
        };
        let contribution = DETECTOR
            .detect(&context)
            .expect("detection")
            .expect("cargo detected");
        assert_eq!(
            contribution.env,
            BTreeMap::from([
                ("CARGO_HOME", EnvAction::Own(cargo_home.clone().into())),
                ("CARGO_TARGET_DIR", EnvAction::Unset),
                (CA_ENV, EnvAction::Default(bundle.clone().into())),
                (GIT_FETCH_WITH_CLI_ENV, EnvAction::Own("true".into())),
            ])
        );
        let mut expected = vec![
            read(cargo_home.clone(), GrantScope::Literal),
            read(cargo_home.join("registry"), GrantScope::Literal),
            read(cargo_home.join("git"), GrantScope::Literal),
        ];
        expected.extend(STATE_FILES.map(|file| CapabilityGrant {
            path: cargo_home.join(file),
            scope: GrantScope::Literal,
            access: GrantAccess::ReadWrite,
        }));
        expected.extend(probes(&fixture.home));
        assert_eq!(home_grants(&contribution, &fixture.home), expected);
    }

    /// A rustup host lends its installed toolchains read-only: the proxies, settings and
    /// toolchains, found through an owned `RUSTUP_HOME`, and `cargo` resolves to rustup's proxy
    /// before any host installation. Nothing grants write to either home.
    #[test]
    fn a_rustup_host_lends_its_toolchains_read_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let fixture = Fixture::new();
        fixture.files(&["Cargo.toml"]);
        let rustup = fixture.home.join(".rustup");
        let proxies = fixture.home.join(".cargo/bin");
        fs::create_dir_all(rustup.join("toolchains/stable-aarch64-apple-darwin/bin"))
            .expect("toolchain");
        fs::write(
            rustup.join("settings.toml"),
            "default_toolchain = \"stable\"\n",
        )
        .expect("rustup settings");
        fs::create_dir_all(&proxies).expect("proxy directory");
        let cargo = proxies.join("cargo");
        fs::write(&cargo, "").expect("cargo proxy");
        fs::set_permissions(&cargo, fs::Permissions::from_mode(0o755)).expect("executable");

        let contribution = DETECTOR
            .detect(&fixture.context())
            .expect("detection")
            .expect("cargo detected");
        assert_eq!(
            contribution.env.get(RUSTUP_HOME_ENV),
            Some(&EnvAction::Own(rustup.clone().into()))
        );
        let grants = home_grants(&contribution, &fixture.home);
        for expected in [
            read(rustup.clone(), GrantScope::Literal),
            read(rustup.join("settings.toml"), GrantScope::Literal),
            read(rustup.join("toolchains"), GrantScope::Subtree),
            read(proxies.clone(), GrantScope::Subtree),
            read(rustup.join("update-hashes"), GrantScope::Literal),
            read(rustup.join("downloads"), GrantScope::Literal),
            read(rustup.join("tmp"), GrantScope::Literal),
        ] {
            assert!(
                grants.contains(&expected),
                "{expected:?} missing from {grants:?}"
            );
        }
        assert!(grants.iter().all(|grant| grant.access == GrantAccess::Read));
        assert!(
            contribution
                .bootstrap_programs
                .contains(&crate::capabilities::BootstrapProgram {
                    name: "cargo",
                    target: cargo.clone(),
                }),
            "{:?}",
            contribution.bootstrap_programs
        );
    }

    #[test]
    fn a_held_cargo_lock_is_reported_busy_and_released_on_drop() {
        let fixture = Fixture::new();
        let first = try_lock_caches(&fixture.home).unwrap().expect("unlocked");
        assert!(try_lock_caches(&fixture.home).unwrap().is_none());
        drop(first);
        assert!(try_lock_caches(&fixture.home).unwrap().is_some());
    }
}
