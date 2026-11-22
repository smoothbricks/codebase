//! macOS: setup installs the helper and its sudoers entry in its one administrator authorization
//! session, as a host action it plans and announces with the host's storage (01c_macos.md,
//! "Privileged steps run in one authorization session"). No root command is run at a terminal.

/// Setup never runs a root command at a terminal: its authorization dialog is the remedy.
pub const TERMINAL_REMEDY: bool = false;

/// Why setup is the step: it installs the helper itself, in its authorization.
pub(super) fn why(_terminal: String) -> String {
    String::from(
        "setup installs this build's helper there, with the sudoers entry that admits exactly its bytes, in its one administrator authorization",
    )
}

/// The `next:` line of an unproven helper: setup again, which raises that authorization.
pub(super) fn next(_command: &str, rerun: String) -> String {
    rerun
}

/// What setup says on a host with no `zfs`: ZFS datasets, the preferred substrate, need OpenZFS
/// for macOS, which the user installs; until then every project stays on APFS images.
pub const WITHOUT_ZFS: Option<&str> = Some(
    "ZFS datasets, the substrate that replaces APFS disk images, need OpenZFS for macOS, which you install (https://openzfsonosx.org); until then this host keeps its projects on APFS images",
);
