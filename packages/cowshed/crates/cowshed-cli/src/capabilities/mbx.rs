//! The mbx cowshed ships (14_nix.md, "mbx"): `cowshed setup` builds the pinned flake this package
//! carries at `nix/mbx`, roots the build with an out-link under cowshed's support directory, and
//! links `mbx` beside the installed cowshed to the rooted executable. No agent, no socket and no
//! copy: the link's realpath is the pinned build's store path, the identity a project's own mbx is
//! proven against (05_gateway.md, "Pool selection and revision").
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
    /// No `nix` on PATH: nothing was built, and the host keeps no mbx.
    NixMissing { flake: PathBuf },
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
            Self::NixMissing { flake } => format!(
                "mbx: nix is not on PATH, so {} could not be built; this host keeps no mbx and schedules Cargo without it",
                flake.display()
            ),
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
                    "run nix build path:{}#{FLAKE_ATTRIBUTE} to see the failure in full",
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
/// the same lock a published package does.
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
        .arg(format!("path:{}#{FLAKE_ATTRIBUTE}", flake.display()))
        .arg("--out-link")
        .arg(&root)
        .output_locked()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MbxInstall::NixMissing {
                flake: flake.to_owned(),
            });
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
    link(home, &rooted_program(&root)?)
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

/// The flake directory this build ships, found by walking up from the running executable:
/// `dist/bin/<platform>/cowshed` in a package and `target/<profile>/cowshed` in a checkout are
/// both below the package root that carries `nix/mbx`.
pub fn flake_directory() -> Result<PathBuf> {
    let executable = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("could not identify the cowshed executable: {error}"),
                "reinstall cowshed",
            )
        })?;
    flake_directory_from(&executable)
}

fn flake_directory_from(search_from: &Path) -> Result<PathBuf> {
    let mut searched = Vec::new();
    for ancestor in search_from.ancestors().skip(1) {
        let candidate = FLAKE_DIRECTORY
            .iter()
            .fold(ancestor.to_path_buf(), |path, part| path.join(part));
        if candidate.join(FLAKE_MANIFEST).is_file() {
            return Ok(candidate);
        }
        searched.push(candidate);
    }
    Err(CowshedError::environment_missing(
        format!(
            "could not find the mbx flake ({}/{FLAKE_MANIFEST}) above {}; looked in {}",
            FLAKE_DIRECTORY.join("/"),
            search_from.display(),
            searched
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "reinstall cowshed; the npm package ships nix/mbx and this install is missing it",
    ))
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

    #[test]
    fn the_flake_is_found_from_both_shipped_binary_layouts() {
        let package = scratch("layouts");
        let flake = package.join("nix").join("mbx");
        std::fs::create_dir_all(&flake).unwrap();
        std::fs::write(flake.join(FLAKE_MANIFEST), b"{}").unwrap();
        for relative in [
            ["dist", "bin", "darwin-arm64", "cowshed"].as_slice(),
            ["target", "release", "cowshed"].as_slice(),
        ] {
            let executable = relative
                .iter()
                .fold(package.clone(), |path, part| path.join(part));
            assert_eq!(flake_directory_from(&executable).unwrap(), flake);
        }
        let _ = std::fs::remove_dir_all(&package);
    }
}
