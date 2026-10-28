//! mbx, the Cargo compiler cache (03_caches.md, "mbx"): where the host's mbx lives once
//! `cowshed setup` installs cowshed's pin beside the installed cowshed (14_nix.md, "mbx"), and the
//! one decision over that link. The host's `mbx` is a symlink to the rooted executable, never a
//! copied inode, so its realpath is the pinned build's store path.

use std::io;
use std::path::{Path, PathBuf};

/// The executable's name, in the pinned package's `bin` and beside the installed cowshed.
pub const BINARY_NAME: &str = "mbx";

/// The nix GC root `cowshed setup` builds the pinned mbx into (`nix build --out-link`): the
/// indirect root that keeps the store path the host's link names.
pub fn gc_root(home: &Path) -> PathBuf {
    home.join("Library/Application Support/dev.cowshed/nix/mbx")
}

/// The host's `mbx`, beside the installed cowshed: a symlink to the rooted executable.
pub fn host_link(home: &Path) -> PathBuf {
    home.join("Library/Application Support/dev.cowshed/bin")
        .join(BINARY_NAME)
}

/// What the host has at [`host_link`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkFound {
    Absent,
    /// A symlink, and the path it names, as written.
    Symlink(PathBuf),
    /// A file, a directory or anything else that is not a link: not cowshed's.
    Other,
}

/// What an install does to [`host_link`] for the rooted `program`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkPlan {
    /// The link already names the program.
    Current,
    Create,
    /// The link names another build (an earlier pin, or a store path since collected).
    Retarget {
        from: PathBuf,
    },
    /// Something that is not a link is there; cowshed removes nothing it did not make.
    Refuse,
}

pub fn plan_link(found: &LinkFound, program: &Path) -> LinkPlan {
    match found {
        LinkFound::Absent => LinkPlan::Create,
        LinkFound::Symlink(target) if target == program => LinkPlan::Current,
        LinkFound::Symlink(target) => LinkPlan::Retarget {
            from: target.clone(),
        },
        LinkFound::Other => LinkPlan::Refuse,
    }
}

/// The host's link as it is, never following it: a link to a collected store path is still a link.
pub fn observe_link(path: &Path) -> io::Result<LinkFound> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            std::fs::read_link(path).map(LinkFound::Symlink)
        }
        Ok(_) => Ok(LinkFound::Other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(LinkFound::Absent),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_host_link_is_made_once_retargeted_to_a_new_pin_and_never_replaces_a_foreign_file() {
        let program = Path::new("/pinned/mbx-1.28.0/bin/mbx");
        let earlier = PathBuf::from("/pinned/mbx-1.27.0/bin/mbx");
        for (found, expected) in [
            (LinkFound::Absent, LinkPlan::Create),
            (LinkFound::Symlink(program.to_owned()), LinkPlan::Current),
            (
                LinkFound::Symlink(earlier.clone()),
                LinkPlan::Retarget { from: earlier },
            ),
            (LinkFound::Other, LinkPlan::Refuse),
        ] {
            assert_eq!(plan_link(&found, program), expected, "{found:?}");
        }
    }

    #[test]
    fn the_link_is_observed_without_following_it() {
        let root = std::env::temp_dir().join(format!("cowshed-mbx-link-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let link = root.join(BINARY_NAME);
        assert_eq!(observe_link(&link).unwrap(), LinkFound::Absent);
        let collected = root.join("collected/bin/mbx");
        std::os::unix::fs::symlink(&collected, &link).unwrap();
        assert_eq!(observe_link(&link).unwrap(), LinkFound::Symlink(collected));
        std::fs::remove_file(&link).unwrap();
        std::fs::write(&link, b"a copied inode").unwrap();
        assert_eq!(observe_link(&link).unwrap(), LinkFound::Other);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
