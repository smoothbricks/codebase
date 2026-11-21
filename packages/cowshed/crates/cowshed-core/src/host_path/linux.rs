//! Linux: the user manager's `PATH`, as NixOS composes it and an FHS distribution completes it.
//!
//! The setuid wrappers, the user's and the default Nix profiles and the system profile, in the
//! order NixOS's own `environment.profiles` gives a session, then the FHS directories. A host
//! has some of these and not the others: an FHS distribution without Nix has only the last
//! three, and NixOS keeps nothing in them but `/bin/sh` and `/usr/bin/env`.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};

/// Where a directory is, for one user.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Place {
    /// The same path for every user.
    Fixed(&'static str),
    /// Beneath the user's home.
    Home(&'static str),
    /// `<before>/<user name>/<after>`.
    PerUser {
        before: &'static str,
        after: &'static str,
    },
}

/// Where the programs a directory names are files, which is what a sandbox must admit for them
/// to start: Landlock decides the open of the file a name resolves to, never the lookup or the
/// links on the way (04_sandbox.md, "Linux enforcement").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Programs {
    /// In the directory itself.
    Own,
    /// In `/nix/store`: the directory is a Nix profile's, reached and filled by links into the
    /// store.
    Store,
    /// In the directory itself, each a set-user-ID wrapper of a program the system profile also
    /// names. `no_new_privs` leaves a wrapper without the privilege it exists for and no rule
    /// admits its directory, so on a job's `PATH` it would only hide the program behind it
    /// (measured 2026-11-21: `mount --version` is `Permission denied` with the wrappers ahead of
    /// the system profile, and runs without them).
    SetUserId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Directory {
    place: Place,
    programs: Programs,
}

const fn profile(place: Place) -> Directory {
    Directory {
        place,
        programs: Programs::Store,
    }
}

const fn system(path: &'static str) -> Directory {
    Directory {
        place: Place::Fixed(path),
        programs: Programs::Own,
    }
}

/// The system `PATH`, first directory first.
const DIRECTORIES: [Directory; 10] = [
    Directory {
        place: Place::Fixed("/run/wrappers/bin"),
        programs: Programs::SetUserId,
    },
    profile(Place::Home(".nix-profile/bin")),
    profile(Place::Fixed("/nix/profile/bin")),
    profile(Place::Home(".local/state/nix/profile/bin")),
    profile(Place::PerUser {
        before: "/etc/profiles/per-user",
        after: "bin",
    }),
    profile(Place::Fixed("/nix/var/nix/profiles/default/bin")),
    profile(Place::Fixed("/run/current-system/sw/bin")),
    system("/usr/local/bin"),
    system("/usr/bin"),
    system("/bin"),
];

/// The user manager's specifiers for the home and the name of the user it runs as
/// (systemd.unit(5), "Specifiers"): a unit states them and the manager spells them out.
const MANAGER_HOME: &str = "%h";
const MANAGER_USER: &str = "%u";

impl Place {
    /// The directory for the user named `user` whose home is `home`: the one place the home and
    /// the name are written into a directory. A user with no name has no per-user directory.
    fn spelt(self, home: &Path, user: Option<&OsStr>) -> Option<PathBuf> {
        match self {
            Self::Fixed(path) => Some(PathBuf::from(path)),
            Self::Home(relative) => Some(home.join(relative)),
            Self::PerUser { before, after } => {
                user.map(|user| Path::new(before).join(user).join(after))
            }
        }
    }
}

pub fn service_path() -> String {
    use std::fmt::Write as _;

    let mut path = String::new();
    for directory in DIRECTORIES.iter().filter_map(|directory| {
        directory
            .place
            .spelt(Path::new(MANAGER_HOME), Some(OsStr::new(MANAGER_USER)))
    }) {
        if !path.is_empty() {
            path.push(':');
        }
        write!(path, "{}", directory.display()).expect("writing into a String cannot fail");
    }
    path
}

pub fn job_directories(home: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(job_directories_of(home, super::user_name()?.as_deref()))
}

/// [`job_directories`] for the user named `user`, when the account database names one.
fn job_directories_of(home: &Path, user: Option<&OsStr>) -> Vec<PathBuf> {
    DIRECTORIES
        .iter()
        .filter(|directory| directory.programs != Programs::SetUserId)
        .filter_map(|directory| directory.place.spelt(home, user))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{RunSandboxMode, SandboxProfileRole, seatbelt_profile};

    const NIX_STORE: &str = "/nix/store";

    #[test]
    fn the_service_path_is_the_user_managers_with_its_specifiers() {
        assert_eq!(
            service_path(),
            "/run/wrappers/bin:%h/.nix-profile/bin:/nix/profile/bin:%h/.local/state/nix/profile/bin:/etc/profiles/per-user/%u/bin:/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/local/bin:/usr/bin:/bin"
        );
    }

    #[test]
    fn a_job_path_is_the_service_path_spelt_out_without_the_setuid_wrappers() {
        const PROFILES: [&str; 4] = [
            "/home/operator/.nix-profile/bin",
            "/nix/profile/bin",
            "/home/operator/.local/state/nix/profile/bin",
            "/etc/profiles/per-user/operator/bin",
        ];
        const SHARED: [&str; 5] = [
            "/nix/var/nix/profiles/default/bin",
            "/run/current-system/sw/bin",
            "/usr/local/bin",
            "/usr/bin",
            "/bin",
        ];
        for (case, user, expected) in [
            (
                "a named user",
                Some("operator"),
                [&PROFILES[..], &SHARED[..]].concat(),
            ),
            (
                "a uid the account database does not name has no per-user profile",
                None,
                [&PROFILES[..3], &SHARED[..]].concat(),
            ),
        ] {
            assert_eq!(
                job_directories_of(Path::new("/home/operator"), user.map(OsStr::new)),
                expected.into_iter().map(PathBuf::from).collect::<Vec<_>>(),
                "{case}"
            );
        }
    }

    /// The job path is the service path: the same directories in the same order, the manager's
    /// specifiers spelt out, and only the setuid wrappers left behind.
    #[test]
    fn a_job_path_keeps_the_order_of_the_service_path() {
        let spelt: Vec<PathBuf> = service_path()
            .replace(MANAGER_HOME, "/home/operator")
            .replace(MANAGER_USER, "operator")
            .split(':')
            .map(PathBuf::from)
            .collect();
        let job = job_directories_of(Path::new("/home/operator"), Some(OsStr::new("operator")));
        assert_eq!(spelt[0], Path::new("/run/wrappers/bin"));
        assert_eq!(job, spelt[1..]);
    }

    /// A workspace with no grant of its own reads and starts the programs of every directory on
    /// its jobs' `PATH`, in both modes and both job tiers: a rule covers the tree those programs
    /// are files of. The wrappers left off that `PATH` have none.
    #[test]
    fn a_default_workspace_reads_and_starts_the_programs_of_every_job_path_directory() {
        for mode in [RunSandboxMode::ReadWrite, RunSandboxMode::ReadOnly] {
            for role in [
                SandboxProfileRole::TrustedSupervisor,
                SandboxProfileRole::ExecutedChild,
            ] {
                let config = crate::sandbox::tests::config(mode);
                let profile = seatbelt_profile(&config, role).expect("a default workspace");
                let profile: serde_json::Value =
                    serde_json::from_str(profile.text()).expect("the launcher's profile is JSON");
                assert_eq!(profile["exec"], "follows-read", "{mode:?} {role:?}");
                let rules = profile["rules"].as_array().expect("rules");
                for directory in &DIRECTORIES {
                    let spelt = directory
                        .place
                        .spelt(&config.home, Some(OsStr::new("tester")))
                        .expect("a named user has every directory");
                    let programs = match directory.programs {
                        Programs::Store => PathBuf::from(NIX_STORE),
                        Programs::Own | Programs::SetUserId => spelt.clone(),
                    };
                    let admitted = rules.iter().any(|rule| {
                        rule["scope"] == "subtree"
                            && rule["access"] == "read"
                            && programs.starts_with(rule["path"].as_str().expect("a rule's path"))
                    });
                    assert_eq!(
                        admitted,
                        directory.programs != Programs::SetUserId,
                        "{mode:?} {role:?}: {} keeps its programs in {}",
                        spelt.display(),
                        programs.display()
                    );
                }
            }
        }
    }
}
