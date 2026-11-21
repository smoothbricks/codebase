//! The mbx cowshed ships (14_nix.md, "mbx"): `cowshed setup` builds the pinned flake this package
//! carries at `nix/mbx`, roots the build with an out-link under cowshed's support directory, and
//! links mbx beside the installed cowshed to the rooted executable. Its own setup command
//! generates the Cargo route in a private scratch home; only the generated cargo launcher and
//! mbx-target sidecar are published in cowshed's support directory.
//!
//! The out-link is the GC root: without it the freshly built store path is garbage the next
//! `nix store gc` may delete from under the link. A host without nix installs no mbx, and the
//! non-mbx scheduling applies unchanged (05_gateway.md, "Host scheduling").

use cowshed_core::capabilities::mbx::{self, LinkPlan};
use cowshed_core::{CowshedError, Result};
use cowshed_os::fork_lock::Run as _;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The flake directory, relative to the package root that ships it.
const FLAKE_DIRECTORY: [&str; 2] = ["nix", "mbx"];

/// The file that identifies [`FLAKE_DIRECTORY`] as the real thing rather than an empty namesake.
const FLAKE_MANIFEST: &str = "flake.nix";

/// The flake output setup installs.
const FLAKE_ATTRIBUTE: &str = "mbx";

/// The `nix` this host would run, by PATH.
const NIX_EXECUTABLE: &str = "nix";

/// What setup did about the host's mbx.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MbxInstall {
    /// Built and rooted, and `link` names `program`; `changed` when this run made or moved it.
    Installed {
        program: PathBuf,
        link: PathBuf,
        changed: bool,
    },
    /// No `nix` on PATH and no `mbx` there either: nothing was built or linked, and the host
    /// keeps no mbx. `fork` is the source `flake` pins, for the operator to install by Cargo.
    NixMissing {
        flake: PathBuf,
        fork: Option<ForkPin>,
    },
    /// `nix build` ran and failed; `stderr` is nix's, verbatim.
    BuildFailed {
        flake: PathBuf,
        status: Option<i32>,
        stderr: String,
    },
    /// Something that is not a link is at the host's `mbx`: cowshed removes nothing it did not make.
    Occupied { link: PathBuf },
}

impl MbxInstall {
    /// The one line setup prints.
    pub fn phrase(&self) -> String {
        match self {
            Self::Installed {
                program,
                link,
                changed: true,
            } => format!("mbx: linked {} to {}", link.display(), program.display()),
            Self::Installed {
                program,
                link,
                changed: false,
            } => format!(
                "mbx: {} already names {}",
                link.display(),
                program.display()
            ),
            Self::NixMissing { flake, fork } => {
                let install = match fork {
                    Some(pin) => format!(
                        "; to use it, install cowshed's mbx fork with `{}` and rerun cowshed setup, which links the mbx on PATH",
                        pin.cargo_install()
                    ),
                    None => String::new(),
                };
                format!(
                    "mbx: nix is not on PATH, so {} could not be built; this host keeps no mbx and schedules Cargo without it{install}",
                    flake.display()
                )
            }
            Self::BuildFailed {
                flake,
                status,
                stderr,
            } => format!(
                "mbx: nix build of {} failed ({}); no mbx was installed\n{}",
                flake.display(),
                exit_phrase(*status),
                stderr.trim_end()
            ),
            Self::Occupied { link } => format!(
                "mbx: {} is not a link cowshed made; it was left as it is and no mbx was installed",
                link.display()
            ),
        }
    }

    /// The failure a run must not exit 0 over. A host without nix is not one: it has no mbx, and
    /// the non-mbx path is the specified behavior there.
    pub fn failure(&self) -> Option<CowshedError> {
        match self {
            Self::Installed { .. } | Self::NixMissing { .. } => None,
            Self::BuildFailed { flake, status, .. } => Some(CowshedError::environment_missing(
                format!(
                    "nix build of {} failed ({})",
                    flake.display(),
                    exit_phrase(*status)
                ),
                format!(
                    "run nix build --no-update-lock-file path:{}#{FLAKE_ATTRIBUTE} to see the failure in full",
                    flake.display()
                ),
            )),
            Self::Occupied { link } => Some(CowshedError::conflict(
                format!(
                    "{} is not the mbx link cowshed installs, and cowshed removes nothing it did not make",
                    link.display()
                ),
                "move it out of that directory and rerun cowshed setup",
            )),
        }
    }
}

/// The mbx source the shipped flake pins: `inputs.mbx.url = "github:<repository>/<revision>"`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkPin {
    /// `<owner>/<repo>` on GitHub.
    pub repository: String,
    pub revision: String,
}

impl ForkPin {
    /// The pin `flake_nix` (the text of the flake's `flake.nix`) names, when it names one.
    pub fn parse(flake_nix: &str) -> Option<Self> {
        let (_, url) = flake_nix.split_once("inputs.mbx.url")?;
        let (_, quoted) = url.split_once('"')?;
        let (url, _) = quoted.split_once('"')?;
        let (repository, revision) = url.strip_prefix("github:")?.rsplit_once('/')?;
        let hex = !revision.is_empty() && revision.bytes().all(|byte| byte.is_ascii_hexdigit());
        (hex && repository.split('/').count() == 2).then(|| Self {
            repository: repository.to_owned(),
            revision: revision.to_owned(),
        })
    }

    /// The command that builds this pin's `mbx` without nix.
    pub fn cargo_install(&self) -> String {
        format!(
            "cargo install --locked --git https://github.com/{} --rev {} mbx",
            self.repository, self.revision
        )
    }
}

/// The first `mbx` a `search` path (a `PATH` value) holds, other than cowshed's own `link`.
fn mbx_on(search: &std::ffi::OsStr, link: &Path) -> Option<PathBuf> {
    std::env::split_paths(search)
        .filter(|directory| directory.is_absolute())
        .map(|directory| directory.join("mbx"))
        .find(|candidate| candidate != link && candidate.is_file())
}

/// A process outcome in words; `None` is a signal, which has no exit code.
fn exit_phrase(status: Option<i32>) -> String {
    match status {
        Some(code) => format!("exit {code}"),
        None => "killed by a signal".to_owned(),
    }
}

/// Build the shipped flake, root it, and link the host's `mbx` to the rooted executable.
///
/// The flake is built as a `path:` flake, so a checkout whose `nix/mbx` is not yet tracked builds
/// the same lock a published package does. With `--no-update-lock-file` the build reads that lock and
/// never rewrites it: a lock that no longer matches the revision `flake.nix` names fails the build
/// instead of relocking.
pub fn install(home: &Path, flake: &Path) -> Result<MbxInstall> {
    let root = mbx::gc_root(home);
    let parent = root
        .parent()
        .expect("the gc root is always derived with a parent");
    std::fs::create_dir_all(parent).map_err(|error| {
        CowshedError::internal(format!("could not create {}: {error}", parent.display()))
    })?;
    // WHY unbounded: the nix build is setup's own work, network-bound on a cold host; the
    // operator's interrupt ends it.
    #[allow(clippy::disallowed_methods)]
    let output = match Command::new(NIX_EXECUTABLE)
        .arg("build")
        .arg("--no-update-lock-file")
        .arg(format!("path:{}#{FLAKE_ATTRIBUTE}", flake.display()))
        .arg("--out-link")
        .arg(&root)
        .output_locked()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A host without nix runs the mbx its operator installed from the same pin.
            let found =
                std::env::var_os("PATH").and_then(|search| mbx_on(&search, &mbx::host_link(home)));
            let Some(program) = found else {
                return Ok(MbxInstall::NixMissing {
                    flake: flake.to_owned(),
                    fork: std::fs::read_to_string(flake.join(FLAKE_MANIFEST))
                        .ok()
                        .as_deref()
                        .and_then(ForkPin::parse),
                });
            };
            let installed = link(home, &program)?;
            if matches!(installed, MbxInstall::Installed { .. }) {
                install_cargo_route(home)?;
            }
            return Ok(installed);
        }
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not run {NIX_EXECUTABLE} build: {error}"
            )));
        }
    };
    if !output.status.success() {
        return Ok(MbxInstall::BuildFailed {
            flake: flake.to_owned(),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    let installed = link(home, &rooted_program(&root)?)?;
    if matches!(installed, MbxInstall::Installed { .. }) {
        install_cargo_route(home)?;
    }
    Ok(installed)
}

/// Generate the upstream route without touching the operator's shell, editor or mbx settings.
fn install_cargo_route(home: &Path) -> Result<()> {
    let route = mbx::cargo_route(home);
    let support = route
        .parent()
        .expect("the Cargo route has a support directory");
    let scratch = support.join(format!(".mbx-setup-{}", std::process::id()));
    std::fs::create_dir(&scratch).map_err(|error| {
        CowshedError::internal(format!("could not create {}: {error}", scratch.display()))
    })?;
    let result = (|| {
        let private_home = scratch.join("home");
        // dirs uses this location on Darwin; XDG_DATA_HOME selects the same path on Linux.
        let data = private_home.join("Library/Application Support");
        std::fs::create_dir_all(&data).map_err(|error| {
            CowshedError::internal(format!("could not create {}: {error}", data.display()))
        })?;
        let executable = mbx::host_link(home);
        // WHY unbounded: setup writes its own local configuration and shim; the operator's
        // interrupt ends the enclosing setup command.
        #[allow(clippy::disallowed_methods)]
        let output = Command::new(&executable)
            .args(["setup", "--yes"])
            .env_clear()
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("HOME", &private_home)
            .env("XDG_DATA_HOME", &data)
            .env("XDG_CONFIG_HOME", scratch.join("config"))
            .env("XDG_CACHE_HOME", scratch.join("cache"))
            .env("XDG_STATE_HOME", scratch.join("state"))
            .env("MBX_CACHE_DIR", scratch.join("cache/mbx"))
            .env("TMPDIR", &scratch)
            .current_dir(&scratch)
            .output_locked()
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!("could not run {} setup: {error}", executable.display()),
                    "rerun cowshed setup",
                )
            })?;
        if !output.status.success() {
            return Err(CowshedError::environment_missing(
                format!(
                    "{} setup failed ({}): {}{}",
                    executable.display(),
                    exit_phrase(output.status.code()),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ),
                "repair the installed mbx and rerun cowshed setup",
            ));
        }
        std::fs::create_dir_all(&route).map_err(|error| {
            CowshedError::internal(format!("could not create {}: {error}", route.display()))
        })?;
        let installed = std::fs::canonicalize(&executable).map_err(|error| {
            CowshedError::internal(format!(
                "could not resolve installed mbx {}: {error}",
                executable.display()
            ))
        })?;
        let generated = data.join("mbx/bin");
        let staged_mbx = generated.join(mbx::BINARY_NAME);
        std::os::unix::fs::symlink(&installed, &staged_mbx).map_err(|error| {
            CowshedError::internal(format!(
                "could not link {} to installed mbx {}: {error}",
                staged_mbx.display(),
                installed.display()
            ))
        })?;
        // Scratch and route share a filesystem. Never truncate a launcher a job may execute;
        // the executable-identity sidecar is published after both executable routes.
        for name in ["cargo", mbx::BINARY_NAME, "mbx-target"] {
            let source = generated.join(name);
            let destination = route.join(name);
            std::fs::rename(&source, &destination).map_err(|error| {
                CowshedError::internal(format!(
                    "could not install generated mbx route {} at {}: {error}",
                    source.display(),
                    destination.display(),
                ))
            })?;
        }
        Ok(())
    })();
    match (result, std::fs::remove_dir_all(&scratch)) {
        (result, Ok(())) => result,
        (result, Err(error)) => Err(CowshedError::internal(format!(
            "{}could not remove mbx setup scratch {}: {error}",
            result
                .err()
                .map_or_else(String::new, |cause| format!("{cause}; ")),
            scratch.display(),
        ))),
    }
}

/// The executable the GC root pins, read through the root rather than from nix's stdout, so the
/// link and the root name one artifact.
fn rooted_program(root: &Path) -> Result<PathBuf> {
    let store = std::fs::read_link(root).map_err(|error| {
        CowshedError::internal(format!(
            "nix build left no out-link at {}: {error}",
            root.display()
        ))
    })?;
    let program = store.join("bin").join(mbx::BINARY_NAME);
    if !program.is_file() {
        return Err(CowshedError::integrity(
            format!(
                "the mbx nix GC root {} names {}, which has no bin/{}",
                root.display(),
                store.display(),
                mbx::BINARY_NAME
            ),
            "rerun cowshed setup",
        ));
    }
    Ok(program)
}

/// Point the host's `mbx` at `program`, atomically: a fresh link beside it renamed over it.
fn link(home: &Path, program: &Path) -> Result<MbxInstall> {
    let link = mbx::host_link(home);
    let found = mbx::observe_link(&link).map_err(|error| {
        CowshedError::internal(format!("could not inspect {}: {error}", link.display()))
    })?;
    let changed = match mbx::plan_link(&found, program) {
        LinkPlan::Current => false,
        LinkPlan::Create | LinkPlan::Retarget { .. } => {
            let directory = link.parent().expect("the link always has a parent");
            std::fs::create_dir_all(directory).map_err(|error| {
                CowshedError::internal(format!("could not create {}: {error}", directory.display()))
            })?;
            let staged = directory.join(format!(".mbx.link.{}", std::process::id()));
            let _ = std::fs::remove_file(&staged);
            std::os::unix::fs::symlink(program, &staged)
                .and_then(|()| std::fs::rename(&staged, &link))
                .map_err(|error| {
                    let _ = std::fs::remove_file(&staged);
                    CowshedError::internal(format!(
                        "could not link {} to {}: {error}",
                        link.display(),
                        program.display()
                    ))
                })?;
            true
        }
        LinkPlan::Refuse => return Ok(MbxInstall::Occupied { link }),
    };
    Ok(MbxInstall::Installed {
        program: program.to_owned(),
        link,
        changed,
    })
}

/// The file Cargo keeps in every profile directory it builds into (`target/<profile>`, and
/// `target/<triple>/<profile>`): an executable beside one is a build run from where Cargo left it.
const CARGO_PROFILE_LOCK: &str = ".cargo-lock";

/// The flake directory this build ships (14_nix.md, "mbx"): the first `nix/mbx` that holds a
/// `flake.nix` above the running executable, which is the package root for a packaged build
/// (`dist/bin/<platform>/cowshed`). A build run from its Cargo profile directory has no package
/// above it, because the target directory is the workspace's, or a link to one outside the
/// checkout, so it looks next above the crate sources it was compiled from: the flake of the
/// checkout that built it. An installed package never takes that second walk.
pub fn flake_directory() -> Result<PathBuf> {
    let executable = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("could not identify the cowshed executable: {error}"),
                "reinstall cowshed",
            )
        })?;
    flake_directory_from(&executable, Path::new(env!("CARGO_MANIFEST_DIR")))
}

/// [`flake_directory`] for `executable`, a build of the crate at `manifest_directory`. The
/// filesystem is read and nothing else: every directory above `executable`, then, only when
/// `executable` lies in a Cargo profile directory, `manifest_directory` and every directory above
/// it. The error names every candidate of the walks it took.
fn flake_directory_from(executable: &Path, manifest_directory: &Path) -> Result<PathBuf> {
    let mut searched = Vec::new();
    if let Some(flake) = flake_above(executable.ancestors().skip(1), &mut searched) {
        return Ok(flake);
    }
    let cargo_built = executable
        .parent()
        .is_some_and(|profile| profile.join(CARGO_PROFILE_LOCK).is_file());
    if cargo_built && let Some(flake) = flake_above(manifest_directory.ancestors(), &mut searched) {
        return Ok(flake);
    }
    let sources = if cargo_built {
        format!(
            " or above the sources it was built from, {}",
            manifest_directory.display()
        )
    } else {
        String::new()
    };
    Err(CowshedError::environment_missing(
        format!(
            "could not find the mbx flake ({}/{FLAKE_MANIFEST}) above {}{sources}; looked in {}",
            FLAKE_DIRECTORY.join("/"),
            executable.display(),
            searched
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "reinstall cowshed; the npm package ships nix/mbx and this install is missing it",
    ))
}

/// The first of `roots` whose [`FLAKE_DIRECTORY`] holds a [`FLAKE_MANIFEST`]; every candidate
/// that does not is added to `searched`.
fn flake_above<'a>(
    roots: impl Iterator<Item = &'a Path>,
    searched: &mut Vec<PathBuf>,
) -> Option<PathBuf> {
    for root in roots {
        let candidate = FLAKE_DIRECTORY
            .iter()
            .fold(root.to_path_buf(), |path, part| path.join(part));
        if candidate.join(FLAKE_MANIFEST).is_file() {
            return Some(candidate);
        }
        searched.push(candidate);
    }
    None
}

/// What setup or adopt added to mbx's two target-collection lists (03_caches.md, "mbx"): the
/// mains now in `target.keep` and the shed mount trees now in `target.evict_first`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TargetCollection {
    pub kept: Vec<PathBuf>,
    pub evicted_first: Vec<PathBuf>,
}

impl TargetCollection {
    /// One line per path this run added, `target.keep` first.
    pub fn phrases(&self) -> impl Iterator<Item = String> + '_ {
        let named = |key: &'static str| {
            move |path: &PathBuf| format!("mbx: {key} names {}", path.display())
        };
        self.kept.iter().map(named(mbx::TARGET_KEEP)).chain(
            self.evicted_first
                .iter()
                .map(named(mbx::TARGET_EVICT_FIRST)),
        )
    }
}

/// Name `mains` in the host mbx's `target.keep` and `shed_trees` in its `target.evict_first`,
/// keeping every entry the operator wrote (03_caches.md, "mbx"). `None` on a host that keeps no
/// cowshed mbx. Run with a cleared environment, as every read of the host's settings is, so a
/// caller's `MBX_*` cannot redirect the write.
pub fn configure_target_collection(
    home: &Path,
    mains: &[PathBuf],
    shed_trees: &[PathBuf],
) -> Result<Option<TargetCollection>> {
    let executable = mbx::host_link(home);
    if std::fs::symlink_metadata(&executable).is_err() {
        return Ok(None);
    }
    Ok(Some(TargetCollection {
        kept: name_in(&executable, home, mbx::TARGET_KEEP, mains)?,
        evicted_first: name_in(&executable, home, mbx::TARGET_EVICT_FIRST, shed_trees)?,
    }))
}

/// Add `required` to the list setting `key`; the paths it added.
fn name_in(
    executable: &Path,
    home: &Path,
    key: &str,
    required: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let printed = settings(executable, home, &["get", key])?;
    let current = mbx::parse_setting_list(&printed).map_err(|text| {
        CowshedError::environment_missing(
            format!("mbx's {key} reads {text:?}, which is no list of paths"),
            format!("repair it with mbx settings set {key} <paths>"),
        )
    })?;
    let Some(list) = mbx::with_required(&current, required).map_err(|path| {
        CowshedError::usage(
            format!("{path} cannot be named in mbx's {key}: mbx takes its items comma-separated"),
            "move the checkout to a path without a comma",
        )
    })?
    else {
        return Ok(Vec::new());
    };
    settings(executable, home, &["set", key, &list.join(",")])?;
    Ok(required
        .iter()
        .filter(|path| {
            !current
                .iter()
                .any(|entry| Path::new(entry) == path.as_path())
        })
        .cloned()
        .collect())
}

/// `mbx settings <args>` with a cleared environment; its stdout.
fn settings(executable: &Path, home: &Path, args: &[&str]) -> Result<String> {
    // WHY unbounded: `mbx settings` reads or writes one TOML file and prints; it waits on nothing.
    #[allow(clippy::disallowed_methods)]
    let output = Command::new(executable)
        .env_clear()
        .env("HOME", home)
        .arg("settings")
        .args(args)
        .output_locked()
        .map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "the mbx installed beside cowshed, {}, cannot be executed: {error}",
                    executable.display()
                ),
                "cowshed setup",
            )
        })?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "mbx settings {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            "repair mbx's global configuration with mbx settings",
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Remove the host's mbx link and release its GC root, for `setup --uninstall`: a link only, never
/// a file cowshed did not make, and nothing under the nix store, which is nix's to collect.
pub fn uninstall(home: &Path) -> Result<[crate::launchd::RemovalOutcome; 2]> {
    let link = mbx::host_link(home);
    let link_outcome = match mbx::observe_link(&link) {
        Ok(mbx::LinkFound::Absent) => crate::launchd::RemovalOutcome::AlreadyAbsent,
        Ok(mbx::LinkFound::Symlink(_)) => {
            std::fs::remove_file(&link).map_err(|error| {
                CowshedError::internal(format!("could not remove {}: {error}", link.display()))
            })?;
            crate::launchd::RemovalOutcome::Removed
        }
        Ok(mbx::LinkFound::Other) => {
            return Err(CowshedError::conflict(
                format!(
                    "{} is not the mbx link cowshed installs, and cowshed removes nothing it did not make",
                    link.display()
                ),
                "move it out of that directory and rerun cowshed setup --uninstall",
            ));
        }
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not inspect {}: {error}",
                link.display()
            )));
        }
    };
    let root = mbx::gc_root(home);
    let root_outcome = if std::fs::symlink_metadata(&root).is_err() {
        crate::launchd::RemovalOutcome::AlreadyAbsent
    } else {
        std::fs::remove_file(&root).map_err(|error| {
            CowshedError::internal(format!("could not remove {}: {error}", root.display()))
        })?;
        crate::launchd::RemovalOutcome::Removed
    };
    let route = mbx::cargo_route(home);
    match std::fs::remove_dir_all(&route) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not remove mbx Cargo route {}: {error}",
                route.display()
            )));
        }
    }
    Ok([link_outcome, root_outcome])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowshed-mbx-install-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("scratch root");
        root
    }

    /// A host without nix is told the Cargo command for the pin the shipped flake itself names,
    /// and its own `mbx` on PATH is the one linked: never cowshed's link naming itself.
    #[test]
    fn a_host_without_nix_installs_the_flakes_pin_by_cargo() {
        let shipped = include_str!("../../../../nix/mbx/flake.nix");
        let pin = ForkPin::parse(shipped).expect("the shipped flake names its pin");
        assert_eq!(pin.repository, "smoothbricks/mr-boxington");
        assert!(shipped.contains(&format!("github:{}/{}", pin.repository, pin.revision)));
        assert_eq!(
            pin.cargo_install(),
            format!(
                "cargo install --locked --git https://github.com/smoothbricks/mr-boxington --rev {} mbx",
                pin.revision
            )
        );
        for unpinned in [
            "",
            "inputs.mbx.url = \"github:smoothbricks/mr-boxington\";",
            "inputs.mbx.url = \"github:smoothbricks/mr-boxington/main\";",
            "inputs.mbx.url = \"path:/src/mbx\";",
        ] {
            assert_eq!(ForkPin::parse(unpinned), None, "{unpinned}");
        }

        let home = scratch("path-mbx");
        let (own, cargo) = (home.join("own"), home.join("cargo"));
        for directory in [&own, &cargo] {
            std::fs::create_dir_all(directory).unwrap();
            std::fs::write(directory.join("mbx"), b"#!/bin/sh\n").unwrap();
        }
        let search =
            std::env::join_paths([home.join("empty"), own.clone(), cargo.clone()]).unwrap();
        assert_eq!(mbx_on(&search, &own.join("mbx")), Some(cargo.join("mbx")));
        let search = std::env::join_paths([own.clone()]).unwrap();
        assert_eq!(mbx_on(&search, &own.join("mbx")), None);
        std::fs::remove_dir_all(&home).unwrap();
    }

    /// The host's `mbx` is a link to the rooted executable, never a copied inode, retargeted to a
    /// new pin, never put over a file cowshed did not make; uninstall removes only what install
    /// made, and nothing else is written beside it.
    #[test]
    fn the_host_mbx_is_one_link_to_the_rooted_executable() {
        let home = scratch("link");
        let store = home.join("store/pin-mbx-1.28.0");
        std::fs::create_dir_all(store.join("bin")).unwrap();
        std::fs::write(store.join("bin/mbx"), b"#!/bin/sh\n").unwrap();
        let root = mbx::gc_root(&home);
        std::fs::create_dir_all(root.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&store, &root).unwrap();
        let program = rooted_program(&root).unwrap();

        let first = link(&home, &program).unwrap();
        assert_eq!(
            first,
            MbxInstall::Installed {
                program: program.clone(),
                link: mbx::host_link(&home),
                changed: true
            }
        );
        assert_eq!(std::fs::read_link(mbx::host_link(&home)).unwrap(), program);
        assert_eq!(
            std::fs::canonicalize(mbx::host_link(&home)).unwrap(),
            std::fs::canonicalize(&program).unwrap()
        );
        assert!(matches!(
            link(&home, &program).unwrap(),
            MbxInstall::Installed { changed: false, .. }
        ));
        let bin = mbx::host_link(&home).parent().unwrap().to_owned();
        assert_eq!(
            std::fs::read_dir(&bin).unwrap().count(),
            1,
            "install writes the link and nothing else beside it"
        );

        assert_eq!(
            uninstall(&home).unwrap(),
            [
                crate::launchd::RemovalOutcome::Removed,
                crate::launchd::RemovalOutcome::Removed
            ]
        );
        assert!(store.join("bin/mbx").is_file(), "the store path is nix's");

        std::fs::write(mbx::host_link(&home), b"a copied inode").unwrap();
        assert_eq!(
            link(&home, &program).unwrap(),
            MbxInstall::Occupied {
                link: mbx::host_link(&home)
            }
        );
        assert_eq!(
            std::fs::read(mbx::host_link(&home)).unwrap(),
            b"a copied inode"
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    /// Where setup finds its flake, by the layout the executable runs from (14_nix.md, "mbx"):
    /// above the executable wherever one is there, above the sources the build was compiled from
    /// only for an executable in a Cargo profile directory, and nowhere else.
    #[test]
    fn the_flake_is_found_above_the_executable_or_above_a_cargo_builds_sources() {
        /// Which walk answers, or that none does.
        enum Found {
            AbovePackage,
            AboveSources,
            Nowhere { names_the_source_walk: bool },
        }
        struct Case {
            name: &'static str,
            /// The executable, below the package root.
            executable: &'static [&'static str],
            /// Whether Cargo's lock lies beside the executable.
            cargo_profile: bool,
            /// Whether the package root above the executable carries the flake.
            package_flake: bool,
            /// Whether the checkout above the manifest directory carries the flake.
            source_flake: bool,
            found: Found,
        }
        let cases = [
            Case {
                name: "packaged",
                executable: &["dist", "bin", "linux-x64", "cowshed"],
                cargo_profile: false,
                package_flake: true,
                source_flake: false,
                found: Found::AbovePackage,
            },
            Case {
                name: "package-target",
                executable: &["target", "release", "cowshed"],
                cargo_profile: true,
                package_flake: true,
                source_flake: true,
                found: Found::AbovePackage,
            },
            Case {
                name: "cargo-profile",
                executable: &["target", "debug", "cowshed"],
                cargo_profile: true,
                package_flake: false,
                source_flake: true,
                found: Found::AboveSources,
            },
            Case {
                name: "cargo-triple-profile",
                executable: &["target", "x86_64-unknown-linux-gnu", "release", "cowshed"],
                cargo_profile: true,
                package_flake: false,
                source_flake: true,
                found: Found::AboveSources,
            },
            Case {
                name: "cargo-profile-no-flake",
                executable: &["target", "debug", "cowshed"],
                cargo_profile: true,
                package_flake: false,
                source_flake: false,
                found: Found::Nowhere {
                    names_the_source_walk: true,
                },
            },
            Case {
                name: "installed-beside-sources",
                executable: &["dist", "bin", "linux-x64", "cowshed"],
                cargo_profile: false,
                package_flake: false,
                source_flake: true,
                found: Found::Nowhere {
                    names_the_source_walk: false,
                },
            },
        ];
        let flake_in = |root: &Path, present: bool| {
            let flake = FLAKE_DIRECTORY
                .iter()
                .fold(root.to_path_buf(), |path, part| path.join(part));
            if present {
                std::fs::create_dir_all(&flake).unwrap();
                std::fs::write(flake.join(FLAKE_MANIFEST), b"{}").unwrap();
            }
            flake
        };
        for case in cases {
            let name = case.name;
            let root = scratch(&format!("flake-{name}"));
            // The executable's tree and the checkout share no ancestor that could carry a flake.
            let package = root.join("built");
            let checkout = root.join("checkout");
            let manifest = checkout.join("crates").join("cowshed-cli");
            std::fs::create_dir_all(&manifest).unwrap();
            let executable = case
                .executable
                .iter()
                .fold(package.clone(), |path, part| path.join(part));
            let profile = executable.parent().unwrap();
            std::fs::create_dir_all(profile).unwrap();
            if case.cargo_profile {
                std::fs::write(profile.join(CARGO_PROFILE_LOCK), b"").unwrap();
            }
            let package_flake = flake_in(&package, case.package_flake);
            let source_flake = flake_in(&checkout, case.source_flake);
            let nearest_to_sources = flake_in(&manifest, false);

            let found = flake_directory_from(&executable, &manifest);
            match case.found {
                Found::AbovePackage => assert_eq!(found.unwrap(), package_flake, "{name}"),
                Found::AboveSources => assert_eq!(found.unwrap(), source_flake, "{name}"),
                Found::Nowhere {
                    names_the_source_walk,
                } => {
                    let error = found.expect_err(name);
                    assert_eq!(
                        error.code,
                        cowshed_core::ErrorCode::EnvironmentMissing,
                        "{name}"
                    );
                    let message = &error.message;
                    let looked_in = |candidate: &Path| {
                        message
                            .split_once("; looked in ")
                            .expect("the candidates")
                            .1
                            .split(", ")
                            .any(|listed| Path::new(listed) == candidate)
                    };
                    assert!(
                        message.starts_with(&format!(
                            "could not find the mbx flake (nix/mbx/flake.nix) above {}",
                            executable.display()
                        )),
                        "{name}: {message}"
                    );
                    assert!(looked_in(&package_flake), "{name}: {message}");
                    assert!(
                        looked_in(&flake_in(profile, false)),
                        "{name}: the walk starts at the executable's directory: {message}"
                    );
                    for candidate in [&nearest_to_sources, &source_flake] {
                        assert_eq!(
                            looked_in(candidate),
                            names_the_source_walk,
                            "{name}: {message}"
                        );
                    }
                    assert_eq!(
                        message.contains(&manifest.display().to_string()),
                        names_the_source_walk,
                        "{name}: {message}"
                    );
                }
            }
            let _ = std::fs::remove_dir_all(&root);
        }
    }

    #[test]
    fn setup_publishes_generated_cargo_bytes_and_preserves_executable_mode() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = crate::temp_root::TempRoot::new_in(
            &std::env::temp_dir(),
            "cowshed-mbx-generated-route",
        );
        let physical_home = root.join("physical-home");
        std::fs::create_dir(&physical_home).unwrap();
        let home = root.join("home");
        std::os::unix::fs::symlink(&physical_home, &home).unwrap();
        let executable = mbx::host_link(&home);
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        let generated = b"#!/bin/sh\nprintf 'generated cargo\\n'\n";
        let script = format!(
            r#"#!/bin/sh
set -eu
export PATH='{path}'
[ "$1" = setup ] && [ "$2" = --yes ] || {{
  printf 'fake mbx: expected setup --yes, got %s %s\n' "$1" "$2" >&2
  exit 97
}}
[ "$(pwd -P)" = "$(cd "$TMPDIR" && pwd -P)" ] || {{
  printf 'fake mbx: cwd %s is not TMPDIR %s\n' "$(pwd -P)" "$TMPDIR" >&2
  exit 97
}}
[ "$MBX_CACHE_DIR" = "$TMPDIR/cache/mbx" ] || {{
  printf 'fake mbx: MBX_CACHE_DIR %s is not %s/cache/mbx\n' "$MBX_CACHE_DIR" "$TMPDIR" >&2
  exit 97
}}
[ -z "${{MISE_CONFIG_FILE-}}" ] || {{
  printf 'fake mbx: inherited MISE_CONFIG_FILE %s\n' "$MISE_CONFIG_FILE" >&2
  exit 97
}}
mkdir -p "$XDG_DATA_HOME/mbx/bin"
printf '#!/bin/sh\nprintf '\''generated cargo\\n'\''\n' > "$XDG_DATA_HOME/mbx/bin/cargo"
chmod 755 "$XDG_DATA_HOME/mbx/bin/cargo"
printf '%s\n' "$0" > "$XDG_DATA_HOME/mbx/bin/mbx-target"
"#,
            path = std::env::var("PATH")
                .expect("the test PATH supplies mkdir and chmod")
                .replace('\'', "'\\''"),
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        install_cargo_route(&home).unwrap();
        let route = mbx::cargo_route(&home);
        let cargo = std::fs::read(route.join("cargo")).unwrap();
        let mode = std::fs::metadata(route.join("cargo"))
            .unwrap()
            .permissions()
            .mode();
        let target = std::fs::read_to_string(route.join("mbx-target")).unwrap();
        let routed_mbx = std::fs::read_link(route.join(mbx::BINARY_NAME)).unwrap();
        let installed_mbx = std::fs::canonicalize(&executable).unwrap();
        let scratch_left = route
            .parent()
            .unwrap()
            .join(format!(".mbx-setup-{}", std::process::id()))
            .exists();
        std::fs::remove_file(&executable).unwrap();
        uninstall(&home).unwrap();
        let route_left = route.exists();
        assert_eq!(cargo, generated);
        assert_ne!(mode & 0o111, 0, "the generated launcher remains executable");
        assert_eq!(target, format!("{}\n", executable.display()));
        assert_eq!(
            routed_mbx, installed_mbx,
            "explicit mbx uses the installed executable"
        );
        assert!(
            !scratch_left,
            "setup's home and configuration are disposable"
        );
        assert!(!route_left, "uninstall removes the generated route");
    }

    #[test]
    fn failed_mbx_setup_reports_status_and_diagnostics_without_publishing_a_route() {
        use std::os::unix::fs::PermissionsExt as _;
        let home = scratch("failed-route");
        let executable = mbx::host_link(&home);
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        std::fs::write(
            &executable,
            "#!/bin/sh\necho 'setup fixture refused' >&2\nexit 23\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = install_cargo_route(&home).unwrap_err().to_string();
        let route = mbx::cargo_route(&home);
        let route_left = route.exists();
        let scratch_left = route
            .parent()
            .unwrap()
            .join(format!(".mbx-setup-{}", std::process::id()))
            .exists();
        std::fs::remove_dir_all(&home).unwrap();
        assert!(error.contains("exit 23"), "{error}");
        assert!(error.contains("setup fixture refused"), "{error}");
        assert!(!route_left, "failed setup publishes nothing");
        assert!(!scratch_left, "failed setup still removes its private home");
    }
}
