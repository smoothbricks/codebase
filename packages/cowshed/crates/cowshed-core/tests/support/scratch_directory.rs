//! Real filesystem roots for lifecycle tests that own no disk images: a short directory of the
//! test's own. Its teardown ends every process still working in it, then removes its tree,
//! several threads unlinking at once, whatever ends the test.
//!
//! Where a root lives, and where a tree its teardown cannot remove is left for the run's sweep,
//! is the platform's (`scratch_directory/linux.rs`, `scratch_directory/macos.rs`).

use std::fs;
use std::path::Path;

// The macOS lane's exact-tree reclamation, shared with its dead-run sweep. It reaches the
// includer's `scratch_namespace` from this module's depth, so it is declared here.
#[cfg(target_os = "macos")]
#[path = "../../../cowshed-apfs/tests/support/scratch_directory_cleanup.rs"]
pub(crate) mod cleanup;
#[cfg(target_os = "linux")]
#[path = "scratch_directory/linux.rs"]
mod linux;
#[cfg(target_os = "macos")]
#[path = "scratch_directory/macos.rs"]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// One root: `new(label)` makes it, `path()` names it, and dropping it reclaims it.
pub use platform::ScratchRoot;

/// Every process but this one whose working directory is the given root or below it, with that
/// directory: what a root's teardown ends. Not every test binary that includes this fixture
/// lists them.
#[allow(unused_imports)]
pub(crate) use platform::processes_in;

/// The file in each root that names the test which made it. The name stays out of the path: the
/// store's Unix sockets (`run/<32 hex>.sock`, the CPU pools' `run/cpu/<22>/<22>`) must fit
/// `sun_path` (104 bytes on macOS, 108 on Linux), so a root's length never grows with its label.
pub(crate) const LABEL_FILE: &str = "label";

#[cfg(test)]
thread_local! {
    static RETIREMENT_RENAMES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn retirement_rename_attempts() -> usize {
    RETIREMENT_RENAMES.with(std::cell::Cell::get)
}

/// Missing source is already retired; a missing destination must not hide a surviving source.
fn retirement_result(
    renamed: std::io::Result<()>,
    source_exists: impl FnOnce() -> std::io::Result<bool>,
) -> std::io::Result<()> {
    match renamed {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match source_exists() {
            Ok(false) => Ok(()),
            Ok(true) => Err(error),
            Err(probe) => Err(std::io::Error::new(
                error.kind(),
                format!("rename fixture: {error}; inspect its source: {probe}"),
            )),
        },
        result => result,
    }
}

pub(crate) fn retire_directory_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    RETIREMENT_RENAMES.with(|count| count.set(count.get() + 1));
    retirement_result(
        fs::rename(source, destination),
        || match fs::symlink_metadata(source) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        },
    )
}

#[cfg(test)]
mod retirement_result_tests {
    use super::retirement_result;
    use std::cell::Cell;
    use std::io::{Error, ErrorKind};

    #[test]
    fn rename_completion_preserves_source_absence_and_real_failures() {
        for (rename_error, source, expected, reads) in [
            (None, Err(ErrorKind::Other), None, 0),
            (
                Some(ErrorKind::PermissionDenied),
                Err(ErrorKind::Other),
                Some(ErrorKind::PermissionDenied),
                0,
            ),
            (Some(ErrorKind::NotFound), Ok(false), None, 1),
            (
                Some(ErrorKind::NotFound),
                Ok(true),
                Some(ErrorKind::NotFound),
                1,
            ),
            (
                Some(ErrorKind::NotFound),
                Err(ErrorKind::PermissionDenied),
                Some(ErrorKind::NotFound),
                1,
            ),
        ] {
            let probes = Cell::new(0);
            let result = retirement_result(
                rename_error.map_or(Ok(()), |kind| Err(Error::from(kind))),
                || {
                    probes.set(probes.get() + 1);
                    source.map_err(Error::from)
                },
            );
            assert_eq!(result.as_ref().err().map(Error::kind), expected);
            assert_eq!(probes.get(), reads);
            if source.is_err() && reads == 1 {
                let message = result.unwrap_err().to_string();
                assert!(
                    message.contains("rename fixture") && message.contains("inspect its source")
                );
            }
        }
    }
}
