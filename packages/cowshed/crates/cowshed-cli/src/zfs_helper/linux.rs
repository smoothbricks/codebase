//! Linux: setup runs the one root command at a terminal, which `sudo` authorizes
//! (09_substrates.md, "Install and privilege").

/// Setup runs the root command itself when it has a terminal.
pub const TERMINAL_REMEDY: bool = true;

/// Why the root command is the step, as [`super::remedy`] says it.
pub(super) fn why(why: String) -> String {
    why
}

/// The `next:` line of an unproven helper: the root command, then setup again.
pub(super) fn next(command: &str, rerun: String) -> String {
    format!("{command} && {rerun}")
}

/// A Linux store is its own substrate: a host with no `zfs` has a directory store, and setup has
/// nothing to say of ZFS.
pub const WITHOUT_ZFS: Option<&str> = None;
