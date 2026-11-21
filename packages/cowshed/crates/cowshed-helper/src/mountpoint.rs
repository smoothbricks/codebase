//! Where a caller may have a dataset mounted.
//!
//! The helper mounts as root, so a mountpoint is as much an authority as a dataset name: a
//! dataset mounted over a directory the caller could not write would replace it. A mountpoint is
//! therefore one of the fixed host paths, or lies in a tree the caller already owns.

use std::fmt;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// The host mountpoints that lie in no user's tree (09_substrates.md, "Linux host paths").
pub const HOST_MOUNTPOINTS: [&str; 2] = ["/private/cowshed/store", "/private/cowshed/build-cache"];

#[derive(Debug)]
pub enum Denied {
    Symlink(PathBuf),
    Owner {
        path: PathBuf,
        owner: u32,
        caller: u32,
    },
    Read {
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Symlink(path) => write!(f, "{} is a symlink", path.display()),
            Self::Owner {
                path,
                owner,
                caller,
            } => write!(
                f,
                "{} belongs to uid {owner}, not to the caller, uid {caller}",
                path.display()
            ),
            Self::Read { path, source } => write!(f, "cannot read {}: {source}", path.display()),
        }
    }
}

/// Permit `mountpoint` (absolute, normal components only) for `caller`: every existing component
/// is a real directory entry, never a symlink, and the deepest one that exists belongs to the
/// caller. Root is permitted everywhere it could mount anyway.
pub fn permit(mountpoint: &Path, caller: u32) -> Result<(), Denied> {
    if caller == 0
        || HOST_MOUNTPOINTS
            .iter()
            .any(|host| Path::new(host) == mountpoint)
    {
        return Ok(());
    }
    let mut deepest = (PathBuf::from("/"), 0);
    let mut walked = PathBuf::new();
    for component in mountpoint.components() {
        walked.push(component);
        match std::fs::symlink_metadata(&walked) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Denied::Symlink(walked));
            }
            Ok(metadata) => deepest = (walked.clone(), metadata.uid()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(source) => {
                return Err(Denied::Read {
                    path: walked,
                    source,
                });
            }
        }
    }
    let (path, owner) = deepest;
    if owner == caller {
        Ok(())
    } else {
        Err(Denied::Owner {
            path,
            owner,
            caller,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir()
                .canonicalize()
                .unwrap()
                .join(format!("cowshed-helper-{name}-{}", std::process::id()));
            std::fs::create_dir(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn a_mountpoint_is_permitted_only_in_the_callers_own_tree() {
        let scratch = Scratch::new("permit");
        // The owner of a directory this process just made is its effective uid.
        let caller = std::fs::metadata(&scratch.0).unwrap().uid();
        let stranger = caller + 1;
        std::fs::create_dir(scratch.0.join("real")).unwrap();
        std::os::unix::fs::symlink("/etc", scratch.0.join("link")).unwrap();

        // Existing, not yet existing, and nested below a not yet existing directory.
        for inside in ["real", "absent", "absent/deeper/still"] {
            permit(&scratch.0.join(inside), caller).unwrap();
        }
        assert!(matches!(
            permit(&scratch.0.join("link/passwd.d"), caller),
            Err(Denied::Symlink(path)) if path == scratch.0.join("link")
        ));
        assert!(matches!(
            permit(&scratch.0.join("link"), caller),
            Err(Denied::Symlink(_))
        ));
        assert!(matches!(
            permit(&scratch.0.join("real"), stranger),
            Err(Denied::Owner { owner, caller: refused, .. }) if owner == caller && refused == stranger
        ));
        // A path whose deepest existing ancestor is root's.
        if caller != 0 {
            assert!(matches!(
                permit(Path::new("/cowshed-helper-absent/x"), caller),
                Err(Denied::Owner { owner: 0, .. })
            ));
        }
        for host in HOST_MOUNTPOINTS {
            permit(Path::new(host), stranger).unwrap();
        }
        permit(Path::new("/etc"), 0).unwrap();
    }
}
