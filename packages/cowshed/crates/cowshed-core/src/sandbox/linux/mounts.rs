//! The mount namespace of a launch: the directories it hides (04_sandbox.md, "Linux
//! enforcement").
//!
//! Landlock decides opens, never `connect(2)` on a pathname Unix socket (measured on kernel 6.18,
//! Landlock ABI 7: a job under a full ruleset reached the user's D-Bus socket and started a
//! process outside its sandbox through the user service manager). What a launch must not connect
//! to is therefore hidden by name: each such directory gets an empty read-only tmpfs mounted
//! over it in the launch's own mount namespace, and the paths beneath it the policy does admit
//! are bound back into that tmpfs from descriptors opened before it covered them.
//!
//! Every mount of the namespace is made a slave of the host's first, so nothing mounted here
//! reaches the host and a host-side unmount still propagates in: a workspace dataset mounted
//! before the launch unmounts from the host while the launch runs, unless the launch itself
//! holds it busy, as it would without a namespace (measured with `zfs unmount`).

use std::ffi::{CStr, CString};
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use super::failure::Failure;

/// One directory hidden behind an empty tmpfs, and the paths beneath it that stay reachable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Mask {
    pub(super) directory: PathBuf,
    /// Sorted, none beneath another: a kept directory is bound with everything in it.
    pub(super) kept: Vec<PathBuf>,
}

/// The user's runtime directory, where the user service manager, the session bus and the user's
/// agents listen.
pub(super) fn user_runtime_directory(uid: u32) -> PathBuf {
    PathBuf::from(format!("/run/user/{uid}"))
}

/// What to hide and what to keep: each of `directories` not beneath another of them, with every
/// path of `reachable` strictly beneath it. A reachable path that is a hidden directory itself,
/// or above one, keeps nothing of it: the mask is the narrower decision.
pub(super) fn masks(directories: &[PathBuf], reachable: &[PathBuf]) -> Vec<Mask> {
    outermost(directories)
        .into_iter()
        .map(|directory| {
            let beneath: Vec<&PathBuf> = reachable
                .iter()
                .filter(|path| *path != directory && path.starts_with(directory))
                .collect();
            Mask {
                directory: directory.clone(),
                kept: outermost(beneath).into_iter().cloned().collect(),
            }
        })
        .collect()
}

/// `paths` sorted, without duplicates and without any path beneath another.
fn outermost<'a>(paths: impl IntoIterator<Item = &'a PathBuf>) -> Vec<&'a PathBuf> {
    let mut paths: Vec<&PathBuf> = paths.into_iter().collect();
    paths.sort();
    paths.dedup();
    let mut kept: Vec<&PathBuf> = Vec::with_capacity(paths.len());
    for path in paths {
        // Sorted, so an ancestor precedes everything beneath it.
        if !kept.iter().any(|ancestor| path.starts_with(ancestor)) {
            kept.push(path);
        }
    }
    kept
}

/// Whether `error` says the path names nothing.
pub(super) fn is_absent(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

/// Enter a mount namespace of this process's own and hide `masks` in it. The caller holds
/// `CAP_SYS_ADMIN` over its user namespace, and is one thread.
pub(super) fn enter(masks: &[Mask]) -> Result<(), Failure> {
    // SAFETY: unshare takes flags only.
    if unsafe { libc::unshare(libc::CLONE_NEWNS) } != 0 {
        return Err(Failure::last("enter", "a mount namespace"));
    }
    let root = Path::new("/");
    mount(None, root, None, libc::MS_REC | libc::MS_SLAVE, None).map_err(|cause| {
        Failure::new("make every mount a slave of the host's beneath", "/", cause)
    })?;
    for mask in masks {
        hide(mask)?;
    }
    Ok(())
}

fn hide(mask: &Mask) -> Result<(), Failure> {
    let directory = &mask.directory;
    match fs::metadata(directory) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(Failure::new(
                "hide",
                directory.display(),
                io::Error::from_raw_os_error(libc::ENOTDIR),
            ));
        }
        // Nothing there is nothing to connect to.
        Err(error) if is_absent(&error) => return Ok(()),
        Err(error) => return Err(Failure::new("inspect", directory.display(), error)),
    }
    let mut kept = Vec::with_capacity(mask.kept.len());
    for path in &mask.kept {
        let held = match cowshed_os::fsio::open_path(path) {
            Ok(held) => File::from(held),
            Err(error) if is_absent(&error) => continue,
            Err(error) => return Err(Failure::new("open", path.display(), error)),
        };
        let is_directory = held
            .metadata()
            .map_err(|error| Failure::new("inspect", path.display(), error))?
            .is_dir();
        kept.push((path, held, is_directory));
    }
    mount(
        Some(Path::new("tmpfs")),
        directory,
        Some(c"tmpfs"),
        libc::MS_NOSUID | libc::MS_NODEV,
        Some(c"mode=0755,size=1m"),
    )
    .map_err(|cause| Failure::new("mount an empty tmpfs over", directory.display(), cause))?;
    for (path, held, is_directory) in &kept {
        mount_point(path, *is_directory)
            .map_err(|cause| Failure::new("make a mount point at", path.display(), cause))?;
        let held = PathBuf::from(format!("/proc/self/fd/{}", held.as_raw_fd()));
        mount(Some(&held), path, None, libc::MS_BIND | libc::MS_REC, None)
            .map_err(|cause| Failure::new("bind back", path.display(), cause))?;
    }
    mount(
        None,
        directory,
        None,
        libc::MS_REMOUNT | libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV,
        None,
    )
    .map_err(|cause| Failure::new("make read-only the tmpfs over", directory.display(), cause))
}

/// Something to mount `path`'s original on, in the fresh tmpfs: a directory for a directory, an
/// empty file for anything else.
fn mount_point(path: &Path, is_directory: bool) -> io::Result<()> {
    let mut directories = DirBuilder::new();
    directories.recursive(true).mode(0o755);
    if is_directory {
        return directories.create(path);
    }
    if let Some(parent) = path.parent() {
        directories.create(parent)?;
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map(drop)
}

fn mount(
    source: Option<&Path>,
    target: &Path,
    filesystem: Option<&CStr>,
    flags: libc::c_ulong,
    data: Option<&CStr>,
) -> io::Result<()> {
    let c_path = |path: &Path| {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    };
    let source = source.map(c_path).transpose()?;
    let target = c_path(target)?;
    // SAFETY: every pointer is null or a NUL-terminated string that lives across the call.
    let mounted = unsafe {
        libc::mount(
            source.as_deref().map_or(std::ptr::null(), CStr::as_ptr),
            target.as_ptr(),
            filesystem.map_or(std::ptr::null(), CStr::as_ptr),
            flags,
            data.map_or(std::ptr::null(), |data| data.as_ptr().cast()),
        )
    };
    if mounted == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(paths: &[&str]) -> Vec<PathBuf> {
        paths.iter().map(PathBuf::from).collect()
    }

    #[test]
    fn a_mask_keeps_exactly_the_reachable_paths_strictly_beneath_it() {
        /// The masks, the reachable paths, and what each mask keeps.
        type Case = (
            &'static [&'static str],
            &'static [&'static str],
            Vec<(&'static str, &'static [&'static str])>,
        );
        let cases: [Case; 5] = [
            // The store's run directory: its pool directory and an admitted socket stay.
            (
                &["/store/run", "/run/user/1000"],
                &[
                    "/store/run/cpu/pool",
                    "/store/run/supervisor.sock",
                    "/work",
                    "/nix/store",
                ],
                vec![
                    ("/run/user/1000", &[]),
                    (
                        "/store/run",
                        &["/store/run/cpu/pool", "/store/run/supervisor.sock"],
                    ),
                ],
            ),
            // A kept directory brings what is beneath it; a path is kept once.
            (
                &["/store/run"],
                &["/store/run/a/b", "/store/run/a", "/store/run/a"],
                vec![("/store/run", &["/store/run/a"])],
            ),
            // A reachable path at or above the hidden directory keeps nothing of it.
            (
                &["/store/run"],
                &["/store/run", "/store", "/"],
                vec![("/store/run", &[])],
            ),
            // A name that merely extends the directory's is not beneath it.
            (
                &["/store/run"],
                &["/store/run-other/x"],
                vec![("/store/run", &[])],
            ),
            // A hidden directory beneath another is the outer one's to hide.
            (
                &["/run/user/1000/cowshed", "/run/user/1000", "/run/user/1000"],
                &["/run/user/1000/cowshed/work"],
                vec![("/run/user/1000", &["/run/user/1000/cowshed/work"])],
            ),
        ];
        for (directories, reachable, expected) in cases {
            let expected: Vec<Mask> = expected
                .into_iter()
                .map(|(directory, kept)| Mask {
                    directory: PathBuf::from(directory),
                    kept: paths(kept),
                })
                .collect();
            assert_eq!(
                masks(&paths(directories), &paths(reachable)),
                expected,
                "{directories:?} {reachable:?}"
            );
        }
    }

    #[test]
    fn the_user_runtime_directory_is_named_by_uid() {
        assert_eq!(
            user_runtime_directory(1000),
            PathBuf::from("/run/user/1000")
        );
    }
}
