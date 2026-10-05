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
use std::path::{Component, Path, PathBuf};

use crate::capabilities::BuildStatePath;
use crate::error::{CowshedError, Result};

/// The one name a checkout reaches its build volume through, relative to the checkout root.
pub const BUILD_LINK: &str = ".cowshed/build";

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
/// The links are made once and never changed (16_build_volumes.md, "One link per checkout"): a
/// path already holding its exact link is left alone, an absent one is created, and anything
/// else there — a real directory (a checkout's own build state, or what a tool rebuilt after
/// removing the link), a file, a link aimed elsewhere — refuses before anything changes, naming
/// the path. Cowshed never moves or deletes build state to make a link fit.
pub fn link_paths(checkout: &Path, volume: &Path, paths: &[BuildStatePath]) -> Result<()> {
    let mut missing = Vec::new();
    for state in paths {
        let path = state.checkout.as_path();
        let (parent, name) = contained_parent(checkout, path)
            .map_err(|error| link_error(&checkout.join(path), &error))?;
        let at = parent.join(name);
        let target = relative_target(path, state.volume.as_path());
        match fs::symlink_metadata(&at) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => missing.push((at, target)),
            Err(error) => return Err(link_error(&at, &error)),
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    && fs::read_link(&at).map_err(|error| link_error(&at, &error))? == target => {}
            Ok(_) => {
                return Err(CowshedError::integrity(
                    format!(
                        "{} is where the build-state link to {} belongs, but something else is there",
                        at.display(),
                        target.display()
                    ),
                    "move it aside (a checkout's first build state is moved by `cowshed setup`; a \
                     directory a tool made after removing the link is rebuildable) and retry",
                ));
            }
        }
        let directory = volume.join(state.volume.as_path());
        fs::create_dir_all(&directory).map_err(|error| link_error(&directory, &error))?;
    }
    for (at, target) in missing {
        std::os::unix::fs::symlink(&target, &at).map_err(|error| link_error(&at, &error))?;
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
    fn build_state_already_at_a_link_path_is_refused_and_left_untouched() {
        let scratch = Scratch::new("occupied");
        let checkout = scratch.checkout();
        let volume = scratch.volume("volume-a");
        // A checkout's own build state before the migration moved it.
        fs::create_dir_all(checkout.join("target/debug")).unwrap();
        let error = link_paths(&checkout, &volume, &paths()).unwrap_err();
        assert!(error.message.contains("something else is there"), "{error}");
        assert!(
            checkout.join("target/debug").is_dir(),
            "the build state is untouched"
        );
        assert!(
            !checkout.join(".nx").exists(),
            "nothing was linked before the refusal"
        );
        // `cargo clean` removed the link and the next build made a real directory there.
        fs::remove_dir_all(checkout.join("target")).unwrap();
        point(&checkout, &volume).unwrap();
        link_paths(&checkout, &volume, &paths()).unwrap();
        fs::remove_file(checkout.join("target")).unwrap();
        fs::create_dir_all(checkout.join("target/debug")).unwrap();
        assert!(link_paths(&checkout, &volume, &paths()).is_err());
        assert!(checkout.join("target/debug").is_dir());
        // A link aimed elsewhere is never retargeted either.
        fs::remove_dir_all(checkout.join("target")).unwrap();
        std::os::unix::fs::symlink("elsewhere", checkout.join("target")).unwrap();
        assert!(link_paths(&checkout, &volume, &paths()).is_err());
        assert_eq!(
            fs::read_link(checkout.join("target")).unwrap(),
            Path::new("elsewhere")
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
