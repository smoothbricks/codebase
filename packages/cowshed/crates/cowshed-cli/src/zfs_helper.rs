//! Proof that the installed root helper can act for a ZFS store, and the root command that makes
//! it so (specs/cowshed/09_substrates.md, "Privileged helper").
//!
//! An install is this user's: it writes `cowshed-helper` beside the installed cowshed and cannot
//! make it root's. cowshed therefore reaches it through `sudo -n`, which a sudoers entry must
//! admit without a password, since the gateway daemon has no terminal to type one at. The entry
//! names the installed path and pins the helper's SHA-256, because that path lies in a directory
//! this user can write: an entry naming the path alone would make root of whatever file is put
//! there. `cowshed setup` proves the entry by the helper's `check` verb and, when the proof
//! fails, writes it: at a terminal it runs the root command itself ([`mend`]), which `sudo`
//! authorizes, and anywhere else it refuses with that command.

use std::path::Path;

use cowshed_core::storage::directory::{HelperProof, prove_store_helper};
use cowshed_core::{CowshedError, Result};

use crate::declared_land::shell_word;
use crate::gateway_service::{HELPER_NAME, executable_digest, launchd_error};
use crate::launchd::HostStableExecutable;

/// Where a host's sudoers reads drop-in files from.
const SUDOERS_DROP_INS: &str = "/etc/sudoers.d";

/// `path` as sudoers spells a command name, or `None` when it holds a character this spelling
/// does not cover. Whitespace ends a command name and `,`, `:`, `=` and `#` have a meaning of
/// their own; a backslash takes each away. A backslash, a quote, `!`, the wildcards `*`, `?`, `[`
/// and `]`, and every control character are refused rather than guessed at.
fn sudoers_command(path: &Path) -> Option<String> {
    let text = path.to_str()?;
    let mut spelled = String::with_capacity(text.len() + 8);
    for character in text.chars() {
        match character {
            ' ' | ',' | ':' | '=' | '#' => {
                spelled.push('\\');
                spelled.push(character);
            }
            '\\' | '"' | '!' | '*' | '?' | '[' | ']' => return None,
            other if other.is_control() => return None,
            other => spelled.push(other),
        }
    }
    Some(spelled)
}

/// The sudoers entry that admits `uid` to run as root, without a password, the helper at
/// `helper` while its bytes hash to `sha256`.
fn sudoers_entry(uid: u32, sha256: &str, helper: &Path) -> Option<String> {
    Some(format!(
        "#{uid} ALL=(root) NOPASSWD: sha256:{sha256} {}",
        sudoers_command(helper)?
    ))
}

/// What makes a helper that did not act as root do so: why this is the step, and the command.
#[derive(Debug, Eq, PartialEq)]
struct Remedy {
    why: String,
    command: String,
}

/// The remedy for `helper`, the helper cowshed reaches, not acting as root for `uid`.
/// `installed` is the helper this install wrote, whose bytes hash to `sha256`.
///
/// The installed helper is reached through `sudo`, so it needs its sudoers entry: written as
/// `uid`'s drop-in, staged under a name `sudo` passes over (it holds a `.`), checked by `visudo`
/// and only then given the name `sudo` reads, so a malformed entry never reaches the sudoers in
/// force. Any other helper was reached because it is root's and setuid, and is replaced by this
/// build's. A path sudoers cannot spell gets the setuid install instead of an entry.
fn remedy(uid: u32, installed: &Path, helper: &Path, sha256: &str) -> Remedy {
    let setuid_install = |destination: &Path| {
        format!(
            "sudo install -o 0 -g 0 -m 4755 {} {}",
            shell_word(&installed.to_string_lossy()),
            shell_word(&destination.to_string_lossy())
        )
    };
    if helper != installed {
        return Remedy {
            why: format!(
                "it is root's and setuid, so cowshed runs it before the installed {}; the next command replaces it with this build's helper",
                installed.display()
            ),
            command: setuid_install(helper),
        };
    }
    match sudoers_entry(uid, sha256, installed) {
        Some(entry) => {
            let drop_in = format!("{SUDOERS_DROP_INS}/{HELPER_NAME}-{uid}");
            Remedy {
                why: format!(
                    "it is reached through sudo, which admits it without a password only by a sudoers entry; the next command, run once per installed helper build, writes the entry that admits exactly these bytes for uid {uid} (a host that reads no {SUDOERS_DROP_INS} takes the entry through its own sudoers configuration: {entry})"
                ),
                command: format!(
                    "printf '%s\\n' {} | sudo sh -c 'umask 227 && cat >{drop_in}.new && visudo -cf {drop_in}.new && mv {drop_in}.new {drop_in}'",
                    shell_word(&entry)
                ),
            }
        }
        None => Remedy {
            why: String::from(
                "it is reached through sudo, and sudoers cannot spell its path; the next command installs it setuid root in /usr/local/bin, where cowshed runs it before the installed one once that directory is on PATH",
            ),
            command: setuid_install(&Path::new("/usr/local/bin").join(HELPER_NAME)),
        },
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
/// no root to serve.
pub fn prove(store_root: &Path, home: &Path, uid: u32) -> Result<Option<Unproven>> {
    let installed = HostStableExecutable::new(home, HELPER_NAME).map_err(launchd_error)?;
    let proof = prove_store_helper(store_root, installed.directory()).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "the root helper of the store {} could not be proven: {error}",
                store_root.display()
            ),
            "cowshed setup",
        )
    })?;
    let HelperProof::Refused {
        root,
        helper,
        cause,
    } = proof
    else {
        return Ok(None);
    };
    let sha256 = executable_digest(installed.path())?.to_string();
    let Remedy { why, command } = remedy(uid, installed.path(), &helper, &sha256);
    Ok(Some(Unproven {
        refusal: CowshedError::environment_missing(
            format!(
                "{} did not act as root for the ZFS root {root}, so nothing can change its datasets: {cause}; {why}",
                helper.display()
            ),
            format!("{command} && cowshed setup"),
        ),
        command,
    }))
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
    fn a_command_name_is_spelled_for_sudoers_or_refused() {
        for (path, expected) in [
            ("/usr/bin/cowshed-helper", Some("/usr/bin/cowshed-helper")),
            (
                "/home/u/Library/Application Support/dev.cowshed/bin/cowshed-helper",
                Some("/home/u/Library/Application\\ Support/dev.cowshed/bin/cowshed-helper"),
            ),
            ("/h/a,b:c=d#e/x", Some("/h/a\\,b\\:c\\=d\\#e/x")),
            ("/h/ü/x", Some("/h/ü/x")),
            ("/h/a\\b/x", None),
            ("/h/a\"b/x", None),
            ("/h/a!b/x", None),
            ("/h/a*b/x", None),
            ("/h/a?b/x", None),
            ("/h/a[b]/x", None),
            ("/h/a\tb/x", None),
            ("/h/a\nb/x", None),
        ] {
            assert_eq!(
                sudoers_command(Path::new(path)).as_deref(),
                expected,
                "{path:?}"
            );
        }
    }

    #[test]
    fn the_remedy_is_the_one_root_command_for_the_helper_that_is_reached() {
        let installed =
            Path::new("/home/u/Library/Application Support/dev.cowshed/bin/cowshed-helper");
        let entry = format!(
            "#1000 ALL=(root) NOPASSWD: sha256:{SHA256} /home/u/Library/Application\\ Support/dev.cowshed/bin/cowshed-helper"
        );
        assert_eq!(
            sudoers_entry(1000, SHA256, installed).as_deref(),
            Some(entry.as_str())
        );
        let unspellable = Path::new("/home/a*b/bin/cowshed-helper");
        let cases = [
            // The installed helper, reached through sudo: its entry, checked before it is in
            // force, under a drop-in name that is this uid's.
            (
                installed,
                installed,
                format!(
                    "printf '%s\\n' '{entry}' | sudo sh -c 'umask 227 && cat >/etc/sudoers.d/cowshed-helper-1000.new && visudo -cf /etc/sudoers.d/cowshed-helper-1000.new && mv /etc/sudoers.d/cowshed-helper-1000.new /etc/sudoers.d/cowshed-helper-1000' && cowshed setup"
                ),
                "sudoers entry",
            ),
            // A setuid helper elsewhere is reached first, so no entry would be consulted: it is
            // the one replaced.
            (
                installed,
                Path::new("/usr/local/bin/cowshed-helper"),
                "sudo install -o 0 -g 0 -m 4755 '/home/u/Library/Application Support/dev.cowshed/bin/cowshed-helper' /usr/local/bin/cowshed-helper && cowshed setup".to_owned(),
                "replaces it with this build's helper",
            ),
            // A path sudoers cannot spell: the helper goes where no entry is needed.
            (
                unspellable,
                unspellable,
                "sudo install -o 0 -g 0 -m 4755 '/home/a*b/bin/cowshed-helper' /usr/local/bin/cowshed-helper && cowshed setup".to_owned(),
                "sudoers cannot spell its path",
            ),
        ];
        for (installed, helper, command, why) in cases {
            let remedy = remedy(1000, installed, helper, SHA256);
            let reached = format!("{} reached as {}", installed.display(), helper.display());
            assert_eq!(
                format!("{} && cowshed setup", remedy.command),
                command,
                "{reached}"
            );
            assert!(remedy.why.contains(why), "{reached}: {}", remedy.why);
        }
        // The entry itself is in the reason, for a host that keeps its sudoers elsewhere.
        assert!(
            remedy(1000, installed, installed, SHA256)
                .why
                .contains(&entry)
        );
    }
}
