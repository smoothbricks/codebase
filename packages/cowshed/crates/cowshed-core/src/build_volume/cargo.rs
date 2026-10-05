//! Cargo's state inside a build volume: the lock a running Cargo holds on each profile directory
//! of a target directory for its whole build (`<target>/debug/.cargo-lock`, and
//! `<target>/<triple>/debug/.cargo-lock` for a cross build). Taking the same lock is how cowshed
//! knows no Cargo writes the volume, and how it keeps one from starting while it clones it
//! (16_build_volumes.md, "Targets and seeds").

use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use super::BuildVolumeState;

/// The file Cargo locks in each profile directory it builds into.
pub const BUILD_LOCK: &str = ".cargo-lock";
/// How deep under a target directory a profile directory sits: `<profile>`,
/// `<triple>/<profile>`, or a nested target directory's `<triple>/<profile>`.
const PROFILE_DEPTH: usize = 3;

/// A volume's Cargo build locks, held by this process until dropped. While they are held no
/// Cargo build of the volume runs, and one that starts waits for them ("Blocking waiting for
/// file lock on build directory").
#[must_use]
pub struct Held {
    _locks: Vec<File>,
}

/// Take every Cargo build lock in the volume rooted at `volume` without waiting. Answers the
/// held locks, or the first lock a running Cargo holds, after letting go of the rest.
pub fn hold(volume: &Path, state: &BuildVolumeState) -> io::Result<Result<Held, PathBuf>> {
    let mut held = Vec::new();
    for target in state.cargo_targets() {
        for lock in locks_in(&volume.join(target))? {
            let file = match File::open(&lock) {
                Ok(file) => file,
                // A profile directory deleted since it was listed holds no build.
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            match file.try_lock() {
                Ok(()) => held.push(file),
                Err(TryLockError::WouldBlock) => return Ok(Err(lock)),
                Err(TryLockError::Error(error)) => return Err(error),
            }
        }
    }
    Ok(Ok(Held { _locks: held }))
}

/// Every build lock under the target directory `target`: each profile directory's, found
/// through the directories that are not profile directories, never inside one (a profile
/// directory holds the build's hundreds of thousands of files).
fn locks_in(target: &Path) -> io::Result<Vec<PathBuf>> {
    let mut locks = Vec::new();
    let mut pending = vec![(target.to_path_buf(), 1)];
    while let Some((directory, depth)) = pending.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let lock = entry.path().join(BUILD_LOCK);
            if lock.is_file() {
                locks.push(lock);
            } else if depth < PROFILE_DEPTH {
                pending.push((entry.path(), depth + 1));
            }
        }
    }
    locks.sort();
    Ok(locks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::BuildStatePath;

    fn scratch(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-cargo-locks-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn state() -> BuildVolumeState {
        BuildVolumeState {
            paths: vec![
                BuildStatePath::new("target", "target").unwrap(),
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
            ],
            fingerprint: None,
        }
    }

    fn lock_at(root: &Path, relative: &str) -> PathBuf {
        let lock = root.join(relative).join(BUILD_LOCK);
        fs::create_dir_all(lock.parent().unwrap()).unwrap();
        fs::write(&lock, b"").unwrap();
        lock
    }

    #[test]
    fn every_profile_lock_is_found_and_none_inside_a_profile() {
        let root = scratch("found");
        let mut expected = vec![
            lock_at(&root, "target/debug"),
            lock_at(&root, "target/wasm32-unknown-unknown/debug"),
            lock_at(&root, "target/cargo-lint/aarch64-apple-darwin/debug"),
        ];
        // Inside a profile directory: Cargo never locks there, and nothing reads it.
        lock_at(&root, "target/debug/build");
        // Not Cargo's: Nx's cache is never searched.
        lock_at(&root, "nx/cache/debug");
        expected.sort();
        assert_eq!(locks_in(&root.join("target")).unwrap(), expected);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_lock_a_build_holds_is_named_and_no_other_lock_stays_held() {
        let root = scratch("held");
        let debug = lock_at(&root, "target/debug");
        let release = lock_at(&root, "target/release");
        let build = File::open(&release).unwrap();
        build.lock().unwrap();
        assert!(matches!(hold(&root, &state()).unwrap(), Err(lock) if lock == release));
        // The debug lock taken before the refusal was let go.
        let other = File::open(&debug).unwrap();
        other.try_lock().unwrap();
        drop(other);
        drop(build);
        let held = hold(&root, &state()).unwrap().unwrap();
        assert!(matches!(
            File::open(&release).unwrap().try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        drop(held);
        File::open(&release).unwrap().try_lock().unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
