//! Proof that the installed root helper can act for a ZFS store, and the root command that makes
//! it so (specs/cowshed/09_substrates.md, "Privileged helper").
//!
//! An install is this user's: it writes `cowshed-helper` beside the installed cowshed, in a
//! directory this user can write, where whatever replaced the file would run as root. So the
//! helper cowshed runs is a copy root owns, read-only, in a root-owned directory
//! ([`cowshed_helper::host::SYSTEM_DIRECTORY`]), reached through `sudo -n`: a sudoers entry
//! admits it without a password, since the gateway daemon has no terminal to type one at, and
//! pins its SHA-256. `cowshed setup` proves that by the helper's `check` verb and, when the
//! proof fails or the copy is not this build's, installs it. On Linux, at a terminal it runs the
//! root command itself ([`mend`]), which `sudo` authorizes, and anywhere else it refuses with
//! that command. On macOS the install is an action of setup's one administrator authorization
//! session (01c_macos.md, "Privileged steps run in one authorization session"), planned with the
//! host's storage, so this only proves it and, unproven, names setup as the next step.

use std::path::Path;

use cowshed_core::storage::directory::{
    HelperProof, SUDOERS_DROP_INS, prove_store_helper, sudoers_entry,
};
use cowshed_core::{CowshedError, Result};

use crate::declared_land::shell_word;
use crate::gateway_service::{HELPER_NAME, executable_digest, launchd_error};
use crate::launchd::HostStableExecutable;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// Whether setup runs the root remedy of an unproven helper itself, at a terminal.
pub use platform::TERMINAL_REMEDY;

/// What setup says on a host with no `zfs`, where the platform has something to say.
pub use platform::WITHOUT_ZFS;

/// What setup says on a host with `zfs` where no root names the user and none was named: how to
/// name one, since cowshed never picks a pool.
pub const NAME_A_PARENT: &str = "no ZFS root names you, so projects stay on APFS images or directories; name the pool or dataset to hold it with `cowshed setup --zfs-parent <pool|dataset>` or a `.cowshed.toml` `[substrate] kind = \"zfs\"` and `parent = \"<pool|dataset>\"`, and setup sets up `<parent>/cowshed`";
/// What makes a helper that did not act as root do so: why this is the step, and the command.
#[derive(Debug, Eq, PartialEq)]
struct Remedy {
    why: String,
    command: String,
}

/// The remedy for `helper`, the helper cowshed reaches, not being this build's helper acting as
/// root for `uid` from `system`, the root-owned path. `installed` is the helper this install
/// wrote, whose bytes hash to `sha256`.
///
/// One root command copies the installed helper to `system` as root's, read-only, and writes
/// its sudoers entry as `uid`'s drop-in: staged under a name `sudo` passes over (it holds a
/// `.`), checked by `visudo` and only then given the name `sudo` reads, so a malformed entry
/// never reaches the sudoers in force. A helper reached anywhere else was reached because it is
/// root's and setuid, and is replaced by this build's.
fn remedy(uid: u32, installed: &Path, helper: &Path, system: &Path, sha256: &str) -> Remedy {
    let installed_word = shell_word(&installed.to_string_lossy());
    if helper != installed && helper != system {
        return Remedy {
            why: format!(
                "it is root's and setuid, so cowshed runs it before {}; the next command replaces it with this build's helper",
                system.display()
            ),
            command: format!(
                "sudo install -o 0 -g 0 -m 4755 {installed_word} {}",
                shell_word(&helper.to_string_lossy())
            ),
        };
    }
    let entry = sudoers_entry(uid, sha256, system)
        .expect("the system helper path holds no character sudoers cannot spell");
    let drop_in = format!("{SUDOERS_DROP_INS}/{HELPER_NAME}-{uid}");
    let directory = system
        .parent()
        .expect("the system helper lies in a directory");
    let script = format!(
        "install -d -o 0 -g 0 -m 755 {directory} && install -o 0 -g 0 -m 555 {installed_word} {system} && umask 227 && printf '%s\\n' {entry} >{drop_in}.new && visudo -cf {drop_in}.new && mv {drop_in}.new {drop_in}",
        directory = shell_word(&directory.to_string_lossy()),
        system = shell_word(&system.to_string_lossy()),
        entry = shell_word(&entry),
    );
    Remedy {
        why: format!(
            "cowshed runs the helper root owns at {}, which no user can replace, through sudo, which admits it without a password only by a sudoers entry; the next command, run once per helper build, installs this build's helper there and writes the entry that admits exactly those bytes for uid {uid} (a host that reads no {SUDOERS_DROP_INS} takes the entry through its own sudoers configuration: {entry})",
            system.display()
        ),
        command: format!("sudo sh -c {}", shell_word(&script)),
    }
}

/// A helper that did not act as root: the refusal `cowshed setup` ends with, whose hint is the
/// root command and the rerun, and that root command alone, for a setup that runs it itself.
#[derive(Debug)]
pub struct Unproven {
    pub refusal: CowshedError,
    pub command: String,
}

/// Prove that the cowshed installed under `home` can change every ZFS root it serves from the
/// store at `store_root` ([`prove_store_helper`]): the helper it reaches answers `check` as root
/// for `uid`, with no credentials a terminal cached. `None` is a proven helper, or a host with
/// no root to serve. `setup` is the executable running this setup: the refusal's rerun is its.
pub fn prove(store_root: &Path, home: &Path, uid: u32, setup: &Path) -> Result<Option<Unproven>> {
    let installed = HostStableExecutable::new(home, HELPER_NAME).map_err(launchd_error)?;
    let proof = prove_store_helper(store_root, installed.directory()).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "the root helper of the store {} could not be proven: {error}",
                store_root.display()
            ),
            rerun(setup),
        )
    })?;
    let system = Path::new(cowshed_helper::host::SYSTEM_DIRECTORY).join(HELPER_NAME);
    let sha256 = executable_digest(installed.path())?.to_string();
    let (root, helper, cause) = match proof {
        HelperProof::NoDatasets => return Ok(None),
        HelperProof::Refused {
            root,
            helper,
            cause,
        } => (root, helper, cause),
        // The helper that acted is the installed one, which this user can replace.
        HelperProof::Proven { root, helper } if helper == installed.path() => (
            root,
            helper,
            String::from("it lies in a directory this user can write"),
        ),
        HelperProof::Proven { root, helper } if helper == system => {
            if executable_digest(&system)?.to_string() == sha256 {
                return Ok(None);
            }
            (root, helper, String::from("it is another build's helper"))
        }
        // An administrator's setuid helper acted.
        HelperProof::Proven { .. } => return Ok(None),
    };
    let Remedy { why, command } = remedy(uid, installed.path(), &helper, &system, &sha256);
    let why = platform::why(why);
    Ok(Some(Unproven {
        refusal: CowshedError::environment_missing(
            format!(
                "{} is not this build's helper acting as root from a root-owned place for the ZFS root {root}: {cause}; {why}",
                helper.display()
            ),
            platform::next(&command, rerun(setup)),
        ),
        command,
    }))
}

/// The rerun a refusal names: `setup` of the build that ran this one. The installed copy has no
/// package around it to find its mbx flake in (14_nix.md, "mbx"), so a bare `cowshed setup`,
/// which runs that copy, is refused before it reaches the helper.
fn rerun(setup: &Path) -> String {
    format!("{} setup", shell_word(&setup.to_string_lossy()))
}

/// Run the root command of an [`Unproven`] helper from this terminal; `sudo` asks for what its
/// policy wants. Whether it succeeded: the proof, run again, is the verdict.
// WHY unbounded: `sudo` waits for the operator's password at the terminal.
#[allow(clippy::disallowed_methods)]
pub fn mend(command: &str) -> std::io::Result<bool> {
    use cowshed_os::fork_lock::Run as _;
    Ok(std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .status_locked()?
        .success())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA256: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    #[test]
    fn the_remedy_is_the_one_root_command_for_the_helper_that_is_reached() {
        let installed =
            Path::new("/home/u/Library/Application Support/dev.cowshed/bin/cowshed-helper");
        let system = Path::new("/usr/local/libexec/cowshed/cowshed-helper");
        let entry = format!(
            "#1000 ALL=(root) NOPASSWD: sha256:{SHA256} /usr/local/libexec/cowshed/cowshed-helper"
        );
        assert_eq!(
            sudoers_entry(1000, SHA256, system).as_deref(),
            Some(entry.as_str())
        );
        // The helper goes to the root-owned path as root's, read-only, and its entry is checked
        // before it is in force, under a drop-in name that is this uid's.
        let install = format!(
            "sudo sh -c 'install -d -o 0 -g 0 -m 755 /usr/local/libexec/cowshed && install -o 0 -g 0 -m 555 '\\''/home/u/Library/Application Support/dev.cowshed/bin/cowshed-helper'\\'' /usr/local/libexec/cowshed/cowshed-helper && umask 227 && printf '\\''%s\\n'\\'' '\\''{entry}'\\'' >/etc/sudoers.d/cowshed-helper-1000.new && visudo -cf /etc/sudoers.d/cowshed-helper-1000.new && mv /etc/sudoers.d/cowshed-helper-1000.new /etc/sudoers.d/cowshed-helper-1000'"
        );
        let cases = [
            (installed, install.clone(), "which no user can replace"),
            (system, install, "which no user can replace"),
            // A setuid helper elsewhere is reached first, so no entry would be consulted: it is
            // the one replaced.
            (
                Path::new("/usr/local/bin/cowshed-helper"),
                "sudo install -o 0 -g 0 -m 4755 '/home/u/Library/Application Support/dev.cowshed/bin/cowshed-helper' /usr/local/bin/cowshed-helper".to_owned(),
                "replaces it with this build's helper",
            ),
        ];
        for (helper, command, why) in cases {
            let remedy = remedy(1000, installed, helper, system, SHA256);
            assert_eq!(remedy.command, command, "reached as {}", helper.display());
            assert!(remedy.why.contains(why), "{}", remedy.why);
        }
        // The entry itself is in the reason, for a host that keeps its sudoers elsewhere.
        assert!(
            remedy(1000, installed, installed, system, SHA256)
                .why
                .contains(&entry)
        );
    }

    #[test]
    fn the_rerun_is_setup_of_the_build_that_ran_it() {
        assert_eq!(
            rerun(Path::new(
                "/h/sb/.cache/cowshed-release/c0/dist/bin/darwin-arm64/cowshed"
            )),
            "/h/sb/.cache/cowshed-release/c0/dist/bin/darwin-arm64/cowshed setup"
        );
        assert_eq!(
            rerun(Path::new(
                "/h/My Checkout/packages/cowshed/dist/bin/linux-x64/cowshed"
            )),
            "'/h/My Checkout/packages/cowshed/dist/bin/linux-x64/cowshed' setup"
        );
    }
}
