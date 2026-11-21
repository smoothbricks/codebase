//! macOS: the `PATH` launchd gives an agent that states none, which is the system's own
//! directories and names no user.

use std::io;
use std::path::{Path, PathBuf};

/// The system `PATH`, first directory first.
const DIRECTORIES: [&str; 4] = ["/usr/bin", "/bin", "/usr/sbin", "/sbin"];

/// Where a developer directory is installed, the preferred one first: a fixed system install,
/// never a value from the environment that started this process.
const DEVELOPER_DIRECTORIES: [&str; 2] = [
    "/Applications/Xcode.app/Contents/Developer",
    "/Library/Developer/CommandLineTools",
];

/// Where a developer directory keeps its programs.
const DEVELOPER_PROGRAMS: &str = "usr/bin";

pub fn service_path() -> String {
    DIRECTORIES.join(":")
}

/// The installed developer directory's programs, then the system's. No directory is spelt for
/// a user.
pub fn job_directories(_home: &Path) -> io::Result<Vec<PathBuf>> {
    let developer = DEVELOPER_DIRECTORIES
        .into_iter()
        .map(Path::new)
        .find(|directory| directory.is_dir());
    Ok(job_directories_behind(developer))
}

/// [`job_directories`] on a host whose developer directory is `developer`.
fn job_directories_behind(developer: Option<&Path>) -> Vec<PathBuf> {
    developer
        .map(|directory| directory.join(DEVELOPER_PROGRAMS))
        .into_iter()
        .chain(DIRECTORIES.map(PathBuf::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_service_path_is_launchds_default() {
        assert_eq!(service_path(), "/usr/bin:/bin:/usr/sbin:/sbin");
    }

    #[test]
    fn a_job_path_is_the_developer_directorys_programs_then_the_service_path() {
        const SYSTEM: [&str; 4] = ["/usr/bin", "/bin", "/usr/sbin", "/sbin"];
        for (case, developer, expected) in [
            ("no developer directory", None, &SYSTEM[..]),
            (
                "the command line tools",
                Some("/Library/Developer/CommandLineTools"),
                &[
                    "/Library/Developer/CommandLineTools/usr/bin",
                    "/usr/bin",
                    "/bin",
                    "/usr/sbin",
                    "/sbin",
                ][..],
            ),
        ] {
            assert_eq!(
                job_directories_behind(developer.map(Path::new)),
                expected.iter().map(PathBuf::from).collect::<Vec<_>>(),
                "{case}"
            );
        }
    }
}
