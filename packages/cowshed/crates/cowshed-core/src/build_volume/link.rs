//! The checkout side of a build volume (16_build_volumes.md, "One link per checkout").
//!
//! A checkout reaches its build volume through exactly one name, [`BUILD_LINK`], whose target is
//! the volume's mountpoint. Every build-state path capability detection names is a fixed relative
//! link through it — `target -> .cowshed/build/target`, `.nx/cache -> ../.cowshed/build/nx/cache` —
//! made once and never changed. Those links are in the tree, so every image clone carries them
//! verbatim and they mean the same thing at every mount.
//!
//! A land moves a checkout to another volume by renaming one new symlink over [`BUILD_LINK`],
//! which `rename(2)` does atomically: every path a tool opens afterwards resolves into the new
//! volume, and no tool ever names a volume directly. `.cowshed/` is in every checkout's
//! `.git/info/exclude`, so linking never makes a tree dirty.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use crate::capabilities::BuildStatePath;
use crate::error::{CowshedError, Result};

/// The one name a checkout reaches its build volume through, relative to the checkout root.
pub const BUILD_LINK: &str = ".cowshed/build";

/// What a stray directory found where a build-state link belongs is renamed to, beside it.
const DISPLACED_INFIX: &str = ".displaced-";

/// The mountpoint `checkout`'s build link names, or `None` for a checkout never linked.
pub fn linked(checkout: &Path) -> Result<Option<PathBuf>> {
    let link = checkout.join(BUILD_LINK);
    match fs::symlink_metadata(&link) {
        Ok(metadata) if metadata.file_type().is_symlink() => fs::read_link(&link)
            .map(Some)
            .map_err(|error| link_error(&link, &error)),
        Ok(_) => Err(not_a_link(&link)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(link_error(&link, &error)),
    }
}

/// Move `checkout` to the volume mounted at `mount` by renaming one new symlink over its build
/// link. Atomic: no process ever resolves the link missing or half-switched. The build-state
/// links are not touched; they reach whatever the build link names.
pub fn point(checkout: &Path, mount: &Path) -> Result<()> {
    if !mount.is_absolute() {
        return Err(CowshedError::internal(format!(
            "build volume mountpoint {} must be an absolute path",
            mount.display()
        )));
    }
    let link = checkout.join(BUILD_LINK);
    let (parent, name) = contained_parent(checkout, Path::new(BUILD_LINK))
        .map_err(|error| link_error(&link, &error))?;
    match fs::symlink_metadata(&link) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            if fs::read_link(&link).map_err(|error| link_error(&link, &error))? == mount {
                return Ok(());
            }
        }
        Ok(_) => return Err(not_a_link(&link)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(link_error(&link, &error)),
    }
    let staged = parent.join(sibling(name, ".next-", &unique()));
    std::os::unix::fs::symlink(mount, &staged)
        .and_then(|()| fs::rename(&staged, &link))
        .map_err(|error| {
            let _ = fs::remove_file(&staged);
            link_error(&link, &error)
        })
}

/// Make every build-state path of `checkout` the fixed relative link through its build link,
/// with the directory it names present in the volume mounted at `volume`.
///
/// A real directory where a build-state link belongs is honoured only in a checkout that is
/// already linked: there a tool removed the link (`cargo clean` deletes `target` itself) and
/// rebuilt into the source image, so that directory is rebuildable private state, displaced and
/// removed in the background. In a checkout never linked it is the checkout's whole build
/// history, which only the setup migration may move, so linking refuses it.
pub fn link_paths(checkout: &Path, volume: &Path, paths: &[BuildStatePath]) -> Result<()> {
    let linked = linked(checkout)?.is_some();
    for state in paths {
        let path = state.checkout.as_path();
        let (parent, name) = contained_parent(checkout, path)
            .map_err(|error| link_error(&checkout.join(path), &error))?;
        let at = parent.join(name);
        if !linked
            && let Ok(metadata) = fs::symlink_metadata(&at)
            && !metadata.file_type().is_symlink()
        {
            return Err(CowshedError::integrity(
                format!(
                    "{} holds the checkout's own build state and no build volume is linked yet",
                    at.display()
                ),
                "run `cowshed setup`, which moves a checkout's build state into its first build volume",
            ));
        }
        let directory = volume.join(state.volume.as_path());
        fs::create_dir_all(&directory).map_err(|error| link_error(&directory, &error))?;
    }
    for state in paths {
        let path = state.checkout.as_path();
        let (parent, name) = contained_parent(checkout, path)
            .map_err(|error| link_error(&checkout.join(path), &error))?;
        remove_displaced(&parent, name);
        through_build_link(
            &parent,
            name,
            &relative_target(path, state.volume.as_path()),
        )
        .map_err(|error| link_error(&parent.join(name), &error))?;
    }
    Ok(())
}

/// `../` once per directory above `checkout_path`, then the build link, then the volume path:
/// the in-tree target of `checkout_path`'s link through the build link.
pub fn relative_target(checkout_path: &Path, volume_path: &Path) -> PathBuf {
    let depth = checkout_path.components().count().saturating_sub(1);
    let mut target = PathBuf::new();
    for _ in 0..depth {
        target.push("..");
    }
    target.push(BUILD_LINK);
    target.push(volume_path);
    target
}

/// The real parent directory of `relative` inside `checkout`, created where missing, and its
/// file name. No component between the checkout and the parent may be a symlink: a link planted
/// there would aim the rename and the removal at a directory outside the checkout.
fn contained_parent<'a>(checkout: &Path, relative: &'a Path) -> io::Result<(PathBuf, &'a OsStr)> {
    let mut components = relative.components();
    let Some(Component::Normal(name)) = components.next_back() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a plain checkout path", relative.display()),
        ));
    };
    let mut parent = checkout.to_path_buf();
    for component in components {
        let Component::Normal(part) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not a plain checkout path", relative.display()),
            ));
        };
        parent.push(part);
        match fs::symlink_metadata(&parent) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} is not a real directory", parent.display()),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(&parent)?,
            Err(error) => return Err(error),
        }
    }
    Ok((parent, name))
}

/// `parent/name` becomes the symlink to `target`: created, left, retargeted, or put in place of
/// a stray directory, which is renamed aside in one step and removed in the background.
fn through_build_link(parent: &Path, name: &OsStr, target: &Path) -> io::Result<()> {
    let path = parent.join(name);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::os::unix::fs::symlink(target, &path)
        }
        Err(error) => Err(error),
        Ok(metadata) if metadata.file_type().is_symlink() => {
            if fs::read_link(&path)? == target {
                return Ok(());
            }
            let staged = parent.join(sibling(name, ".next-", &unique()));
            std::os::unix::fs::symlink(target, &staged)?;
            fs::rename(&staged, &path)
        }
        Ok(metadata) if metadata.is_dir() => {
            let displaced = parent.join(sibling(name, DISPLACED_INFIX, &unique()));
            fs::rename(&path, &displaced)?;
            std::os::unix::fs::symlink(target, &path)?;
            remove_in_background(displaced);
            Ok(())
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} is a file where a build-state link belongs",
                path.display()
            ),
        )),
    }
}

/// Finish removing the directories an earlier link displaced beside `name`.
fn remove_displaced(parent: &Path, name: &OsStr) {
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    let mut prefix = name.as_bytes().to_vec();
    prefix.extend_from_slice(DISPLACED_INFIX.as_bytes());
    for entry in entries.flatten() {
        if entry.file_name().as_bytes().starts_with(&prefix) {
            remove_in_background(entry.path());
        }
    }
}

fn remove_in_background(directory: PathBuf) {
    std::thread::spawn(move || {
        if let Err(error) = fs::remove_dir_all(&directory)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!(
                "cowshed: cannot remove displaced build state {}: {error}; the next link retries it",
                directory.display()
            );
        }
    });
}

fn sibling(name: &OsStr, infix: &str, suffix: &str) -> OsString {
    let mut sibling = name.to_os_string();
    sibling.push(infix);
    sibling.push(suffix);
    sibling
}

fn unique() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn not_a_link(link: &Path) -> CowshedError {
    CowshedError::integrity(
        format!("{} is not a symlink", link.display()),
        "move it aside; cowshed owns this name and links it to the checkout's build volume",
    )
}

fn link_error(path: &Path, error: &io::Error) -> CowshedError {
    CowshedError::integrity(
        format!("cannot link build state at {}: {error}", path.display()),
        "repair the path and retry; `cowshed doctor` reports each checkout's build link",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowshed-build-link-{label}-{}",
                uuid::Uuid::new_v4().simple()
            ));
            fs::create_dir_all(root.join("checkout/.cowshed")).unwrap();
            fs::create_dir_all(root.join("volume-a")).unwrap();
            fs::create_dir_all(root.join("volume-b")).unwrap();
            Self(root)
        }

        fn checkout(&self) -> PathBuf {
            self.0.join("checkout")
        }

        fn volume(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn paths() -> Vec<BuildStatePath> {
        vec![
            BuildStatePath::new("target", "target").unwrap(),
            BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
            BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
            BuildStatePath::new("packages/engine/target", "packages/engine/target").unwrap(),
        ]
    }

    #[test]
    fn build_state_paths_are_fixed_relative_links_through_the_one_build_link() {
        let scratch = Scratch::new("fixed");
        let checkout = scratch.checkout();
        let volume = scratch.volume("volume-a");
        point(&checkout, &volume).unwrap();
        link_paths(&checkout, &volume, &paths()).unwrap();
        assert_eq!(linked(&checkout).unwrap(), Some(volume.clone()));
        for (path, target) in [
            ("target", ".cowshed/build/target"),
            (".nx/cache", "../.cowshed/build/nx/cache"),
            (".nx/workspace-data", "../.cowshed/build/nx/workspace-data"),
            (
                "packages/engine/target",
                "../../.cowshed/build/packages/engine/target",
            ),
        ] {
            assert_eq!(
                fs::read_link(checkout.join(path)).unwrap(),
                Path::new(target),
                "{path}"
            );
        }
        fs::write(checkout.join("target/unit"), b"a").unwrap();
        assert_eq!(fs::read(volume.join("target/unit")).unwrap(), b"a");
        fs::write(checkout.join(".nx/cache/run.json"), b"{}").unwrap();
        assert!(volume.join("nx/cache/run.json").is_file());
    }

    #[test]
    fn one_rename_moves_every_path_to_another_volume() {
        let scratch = Scratch::new("flip");
        let checkout = scratch.checkout();
        let (a, b) = (scratch.volume("volume-a"), scratch.volume("volume-b"));
        point(&checkout, &a).unwrap();
        link_paths(&checkout, &a, &paths()).unwrap();
        fs::create_dir_all(b.join("target")).unwrap();
        fs::write(b.join("target/unit"), b"b").unwrap();
        let before = fs::read_link(checkout.join("target")).unwrap();
        point(&checkout, &b).unwrap();
        assert_eq!(fs::read_link(checkout.join("target")).unwrap(), before);
        assert_eq!(fs::read(checkout.join("target/unit")).unwrap(), b"b");
        assert_eq!(linked(&checkout).unwrap(), Some(b));
        // No staged sibling is left beside the link.
        let leftovers = fs::read_dir(checkout.join(".cowshed"))
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().file_name() != "build")
            .count();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn an_unlinked_checkout_with_its_own_build_state_is_refused() {
        let scratch = Scratch::new("unlinked");
        let checkout = scratch.checkout();
        fs::create_dir_all(checkout.join("target/debug")).unwrap();
        let error = link_paths(&checkout, &scratch.volume("volume-a"), &paths()).unwrap_err();
        assert!(
            error.message.contains("no build volume is linked yet"),
            "{error}"
        );
        assert!(
            checkout.join("target/debug").is_dir(),
            "the build state is untouched"
        );
    }

    #[test]
    fn a_linked_checkout_displaces_a_directory_a_tool_put_in_place_of_its_link() {
        let scratch = Scratch::new("displace");
        let checkout = scratch.checkout();
        let volume = scratch.volume("volume-a");
        point(&checkout, &volume).unwrap();
        link_paths(&checkout, &volume, &paths()).unwrap();
        // `cargo clean` removes the link; the next build makes a real directory there.
        fs::remove_file(checkout.join("target")).unwrap();
        fs::create_dir_all(checkout.join("target/debug")).unwrap();
        link_paths(&checkout, &volume, &paths()).unwrap();
        assert_eq!(
            fs::read_link(checkout.join("target")).unwrap(),
            Path::new(".cowshed/build/target")
        );
    }

    #[test]
    fn a_symlinked_parent_never_aims_a_link_outside_the_checkout() {
        let scratch = Scratch::new("escape");
        let checkout = scratch.checkout();
        let volume = scratch.volume("volume-a");
        point(&checkout, &volume).unwrap();
        std::os::unix::fs::symlink(scratch.volume("volume-b"), checkout.join("packages")).unwrap();
        let error = link_paths(&checkout, &volume, &paths()).unwrap_err();
        assert!(error.message.contains("not a real directory"), "{error}");
        assert!(!scratch.volume("volume-b").join("engine").exists());
    }

    #[test]
    fn a_build_link_that_is_not_a_symlink_is_refused() {
        let scratch = Scratch::new("not-link");
        let checkout = scratch.checkout();
        fs::create_dir_all(checkout.join(BUILD_LINK)).unwrap();
        assert!(linked(&checkout).is_err());
        assert!(point(&checkout, &scratch.volume("volume-a")).is_err());
    }
}
