//! A program a job names by its bare name, found as `execvp` would find it on the job's PATH.
//! A shell-less job's PATH is its bootstrap PATH (11_shell.md, "Shell activation and process
//! reuse"), so the supervisor can look along it before admitting a job; a shell's PATH is the
//! activated one, which only the job sees. [`Lookup::along`] is the one lookup (stats only);
//! what its answer means for a caller is decided by pure functions of it.

use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;

use crate::api::dto::CommandArg;

/// Where a job's PATH comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum JobPath<'a> {
    /// The shed activates a shell, which declares its own toolchain (15_capabilities.md,
    /// "Bootstrap executables"): a bare name is whatever the activated PATH finds, which only the
    /// job can tell.
    Shell,
    /// The shed activates none: the job's PATH is this bootstrap PATH and nothing else.
    Bootstrap(&'a OsStr),
}

/// What a lookup of one name along a PATH proved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Lookup {
    /// The first executable regular file of the name, past ones that cannot execute.
    Found(PathBuf),
    /// None found, and a directory the lookup may not read could hold one.
    Undecided,
    /// None executable, and one that cannot execute: `execvp`'s `EACCES`.
    Unexecutable,
    /// None anywhere along the PATH: `execvp`'s `ENOENT`.
    Absent,
}

impl Lookup {
    /// Look `name` up along `path` as `execvp` would, by `stat` alone.
    pub(crate) fn along(path: &OsStr, name: &str) -> Self {
        let mut unexecutable = false;
        let mut undecided = false;
        for candidate in std::env::split_paths(path).map(|directory| directory.join(name)) {
            match std::fs::metadata(&candidate) {
                Ok(metadata)
                    if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 =>
                {
                    return Self::Found(candidate);
                }
                Ok(_) => unexecutable = true,
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    unexecutable = true
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                    ) => {}
                Err(_) => undecided = true,
            }
        }
        if undecided {
            Self::Undecided
        } else if unexecutable {
            Self::Unexecutable
        } else {
            Self::Absent
        }
    }

    /// What a job runs for `name`: the found file by its absolute path, or the bare name when the
    /// lookup cannot tell, leaving the answer to the kernel. A lookup that proved none can run is
    /// `execvp`'s errno for it.
    pub(crate) fn runnable(self, name: &'static str) -> Result<CommandArg, io::Error> {
        match self {
            Self::Found(program) => Ok(CommandArg::new(program)),
            Self::Undecided => Ok(CommandArg::from(name)),
            Self::Unexecutable => Err(io::Error::from_raw_os_error(libc::EACCES)),
            Self::Absent => Err(io::Error::from_raw_os_error(libc::ENOENT)),
        }
    }
}

/// The runtime a land's JavaScript helper runs (16_build_volumes.md, Land 5.3 and step 7;
/// 05_gateway.md, "Publication"): the same as the workspace's other jobs reach. A shed with a
/// shell runs `node`, the one its shell declares, as the Nx daemon does (04_sandbox.md). A
/// shell-less shed runs the `node` on its bootstrap PATH, or else its `bun`: a shell-less Bun
/// workspace on a host whose conventional directories hold no Node has only Bun, which builds the
/// workspace's Nx graph for its other jobs too.
pub(crate) fn script_runtime(job: JobPath<'_>) -> Result<CommandArg, String> {
    match job {
        JobPath::Shell => Ok(CommandArg::from("node")),
        JobPath::Bootstrap(path) => choose_script_runtime(
            path,
            Lookup::along(path, "node"),
            Lookup::along(path, "bun"),
        ),
    }
}

/// [`script_runtime`] on a bootstrap `path` whose lookups found `node` and `bun`: Node first,
/// as every helper ran before, then Bun; neither names both errnos and the PATH.
fn choose_script_runtime(path: &OsStr, node: Lookup, bun: Lookup) -> Result<CommandArg, String> {
    let node = match node.runnable("node") {
        Ok(node) => return Ok(node),
        Err(errno) => errno,
    };
    bun.runnable("bun").map_err(|bun| {
        format!(
            "neither an executable `node` ({node}) nor `bun` ({bun}) on its PATH {}",
            path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn found(program: &str) -> Lookup {
        Lookup::Found(PathBuf::from(program))
    }

    const PATH: &str = "/w/.cowshed/tools/manager-bin:/w/.cowshed/tools/bin:/usr/bin:/bin";

    fn choose(node: Lookup, bun: Lookup) -> Result<CommandArg, String> {
        choose_script_runtime(OsStr::new(PATH), node, bun)
    }

    //#region smoo/cowshed!n/land-helper-runtime
    #[test]
    fn a_shell_runs_the_node_it_declares() {
        assert_eq!(script_runtime(JobPath::Shell), Ok(CommandArg::from("node")));
    }

    #[test]
    fn a_node_on_the_path_runs_ahead_of_bun() {
        assert_eq!(
            choose(
                found("/usr/bin/node"),
                found("/w/.cowshed/tools/manager-bin/bun")
            ),
            Ok(CommandArg::from("/usr/bin/node"))
        );
    }

    #[test]
    fn a_shell_less_bun_workspace_without_node_runs_bun_by_its_path() {
        for node in [Lookup::Absent, Lookup::Unexecutable] {
            assert_eq!(
                choose(node, found("/w/.cowshed/tools/manager-bin/bun")),
                Ok(CommandArg::from("/w/.cowshed/tools/manager-bin/bun"))
            );
        }
    }

    #[test]
    fn a_node_lookup_that_cannot_tell_leaves_node_to_the_kernel() {
        assert_eq!(
            choose(
                Lookup::Undecided,
                found("/w/.cowshed/tools/manager-bin/bun")
            ),
            Ok(CommandArg::from("node"))
        );
    }

    #[test]
    fn a_bun_lookup_that_cannot_tell_leaves_bun_to_the_kernel() {
        assert_eq!(
            choose(Lookup::Absent, Lookup::Undecided),
            Ok(CommandArg::from("bun"))
        );
    }

    #[test]
    fn neither_runtime_names_both_errnos_and_the_path() {
        let why = choose(Lookup::Absent, Lookup::Unexecutable).unwrap_err();
        let enoent = io::Error::from_raw_os_error(libc::ENOENT).to_string();
        let eacces = io::Error::from_raw_os_error(libc::EACCES).to_string();
        assert_eq!(
            why,
            format!(
                "neither an executable `node` ({enoent}) nor `bun` ({eacces}) on its PATH {PATH}"
            )
        );
    }

    #[test]
    fn the_lookup_finds_the_first_executable_past_one_that_cannot_run() {
        use std::os::unix::fs::PermissionsExt as _;

        let root =
            crate::scratch_directory::ScratchRoot::new("job-program-lookup").expect("scratch root");
        let [first, second, empty] =
            ["first", "second", "empty"].map(|name| root.path().join(name));
        for directory in [&first, &second, &empty] {
            std::fs::create_dir_all(directory).unwrap();
        }
        std::fs::write(first.join("bun"), b"").unwrap();
        std::fs::write(second.join("bun"), b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(second.join("bun"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        let path = std::env::join_paths([&empty, &first, &second]).unwrap();
        assert_eq!(Lookup::along(&path, "node"), Lookup::Absent);
        assert_eq!(
            Lookup::along(&path, "bun"),
            Lookup::Found(second.join("bun"))
        );
        let only_first = std::env::join_paths([&empty, &first]).unwrap();
        assert_eq!(Lookup::along(&only_first, "bun"), Lookup::Unexecutable);
        assert_eq!(
            script_runtime(JobPath::Bootstrap(&path)),
            Ok(CommandArg::new(second.join("bun")))
        );
    }
    //#endregion smoo/cowshed!n/land-helper-runtime
}
