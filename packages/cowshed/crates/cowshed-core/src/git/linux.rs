//! Controller Git on Linux: the `git` on `PATH`, which cowshed's own launcher starts under the
//! Landlock profile [`super::confined`] renders for its checkout (04_sandbox.md, "Linux
//! enforcement").

use std::ffi::OsString;
use std::path::PathBuf;

/// The Git a confined launch starts, by name: the launcher resolves it on the `PATH` the
/// controller hands the launch, as the controller's own `git` is resolved.
pub(super) fn git_program() -> PathBuf {
    PathBuf::from("git")
}

/// The `PATH` a launch of [`git_program`] that inherits no environment is handed: the
/// controller's own, where the launcher resolves the name, so that launch starts the Git the
/// controller's own `git` is. `None` when the controller has none; the launcher's exec then
/// searches the system's default directories, as the controller's would.
pub(super) fn git_search_path() -> Option<OsString> {
    std::env::var_os("PATH")
}

/// What to do when Git cannot be started ([`super::git_spawn_error`]).
pub(super) const INSTALL_GIT: &str = "install git with the system's package manager so `git` is on the controller's PATH, then retry";

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::super::confined::{Checkout, ExtraRead, controller_git_policy};
    use crate::sandbox::seatbelt_profile;

    const HOME: &str = "/home/operator";
    const STORE: &str = "/private/cowshed/store";
    const WORKSPACE: &str = "/home/operator/.cowshed/mnt/acme/widget/workspaces/raven/mount";
    const MAIN: &str = "/home/operator/src/widget";
    const COMMON: &str = "/home/operator/src/widget/.git";
    const INCLUDE: &str = "/home/operator/src/widget/tooling/workspace.gitconfig";
    const OBJECTS: &str =
        "/home/operator/.cowshed/mnt/acme/widget/workspaces/wren/mount/.git/objects";
    const STAGING: &str =
        "/home/operator/.cowshed/mnt/acme/widget/workspaces/wren/mount/.cowshed/worktree-staging";

    /// The profile a launch of controller Git in `checkout` reads, as its launcher parses it.
    fn launched_profile(
        checkout: Checkout,
        extra: Option<ExtraRead<'_>>,
        writable: Option<&Path>,
    ) -> serde_json::Value {
        let policy = controller_git_policy(checkout, extra, writable, |path| Ok(path.to_owned()))
            .expect("a layout the policy admits");
        let config = policy.sandbox_config(Path::new(STORE), PathBuf::from(HOME));
        let profile =
            seatbelt_profile(&config, policy.role()).expect("a profile Landlock enforces");
        serde_json::from_str(profile.text()).expect("the launcher's profile is JSON")
    }

    /// Every rule of `profile` outside the system's own trees, as `(path, scope, access)`, and
    /// the rest.
    fn rules(profile: &serde_json::Value) -> (Vec<(String, String, String)>, Vec<String>) {
        let mut granted = Vec::new();
        let mut system = Vec::new();
        for rule in profile["rules"].as_array().expect("rules") {
            let text = |key: &str| rule[key].as_str().expect("a rule's text").to_owned();
            let path = text("path");
            if Path::new(&path).starts_with(HOME) || Path::new(&path).starts_with("/private") {
                granted.push((path, text("scope"), text("access")));
            } else {
                assert!(
                    matches!(text("access").as_str(), "read" | "device"),
                    "a system rule that writes: {rule}"
                );
                system.push(path);
            }
        }
        (granted, system)
    }

    fn rule(path: &str, access: &str) -> (String, String, String) {
        (path.to_owned(), "subtree".to_owned(), access.to_owned())
    }

    /// A git-worktree workspace whose main config includes a main-tree file, read through
    /// another checkout's object store while a staging directory is written: every grant the
    /// policy holds is one Landlock rule with that grant's access, nothing else beneath HOME or
    /// the store has a rule, and only the launched Git starts.
    #[test]
    fn every_controller_git_grant_is_one_landlock_rule_and_nothing_else_is() {
        let profile = launched_profile(
            Checkout {
                root: PathBuf::from(WORKSPACE),
                common: Some(PathBuf::from(COMMON)),
                includes: vec![PathBuf::from(INCLUDE)],
            },
            Some(ExtraRead::AlternateObjects(Path::new(OBJECTS))),
            Some(Path::new(STAGING)),
        );
        let (granted, system) = rules(&profile);
        assert_eq!(
            granted,
            [
                rule(WORKSPACE, "read-write"),
                rule(STAGING, "read-write"),
                rule(OBJECTS, "read"),
                rule(COMMON, "read-write"),
                rule(INCLUDE, "read"),
            ]
        );
        assert_eq!(profile["exec"], "launched-program");
        // Programs and libraries, the devices Git opens; never the host's configuration, its
        // process table, a temp directory or a runtime directory.
        assert!(system.iter().any(|path| path == "/usr"), "{system:?}");
        assert!(system.iter().any(|path| path == "/dev/null"), "{system:?}");
        for closed in [
            "/", "/etc", "/proc", "/sys", "/tmp", "/run", "/var", "/home",
        ] {
            assert!(!system.iter().any(|path| path == closed), "{closed}");
        }
        // No pool directory, so no gateway socket: the launch's network namespace holds its own
        // loopback and no connector. The store's sockets and profiles are hidden.
        assert_eq!(profile["gateway"], serde_json::Value::Null);
        assert_eq!(profile["sockets"], serde_json::json!([]));
        assert_eq!(
            profile["masks"],
            serde_json::json!([format!("{STORE}/run")])
        );
        assert_eq!(profile["build_volume"], false);
    }

    /// A standalone checkout fetching from main reads main and writes itself alone; it may run
    /// the shell Git starts `upload-pack` through, and the drivers its repository selects.
    #[test]
    fn a_fetch_reads_its_source_and_keeps_exec() {
        let profile = launched_profile(
            Checkout {
                root: PathBuf::from(WORKSPACE),
                common: None,
                includes: Vec::new(),
            },
            Some(ExtraRead::FetchSource(Path::new(MAIN))),
            None,
        );
        let (granted, _) = rules(&profile);
        assert_eq!(granted, [rule(WORKSPACE, "read-write"), rule(MAIN, "read")]);
        assert_eq!(profile["exec"], "follows-read");
        assert_eq!(profile["gateway"], serde_json::Value::Null);
    }

    /// Git that cannot start is a missing tool of this host, said with the cause and with what
    /// installs it here, never another platform's developer tools.
    #[test]
    fn a_git_that_cannot_start_names_its_cause_and_this_platforms_install() {
        let error = crate::git::git_spawn_error(&std::io::Error::from_raw_os_error(libc::ENOENT));
        assert_eq!(error.code, crate::error::ErrorCode::EnvironmentMissing);
        assert!(
            error.message.starts_with("cannot execute git: ")
                && error.message.contains("No such file or directory"),
            "{}",
            error.message
        );
        assert_eq!(error.hint, super::INSTALL_GIT);
        assert!(error.hint.contains("PATH"), "{}", error.hint);
        assert!(!error.hint.contains("macOS"), "{}", error.hint);
    }
}
