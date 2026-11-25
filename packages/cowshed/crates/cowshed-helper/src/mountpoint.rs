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

/// What a walk finds at one existing component of a mountpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    pub symlink: bool,
    pub owner: u32,
}

/// Permit `mountpoint` (absolute, normal components only) for `caller`: [`judge`] over the
/// entries the host's directory tree holds, read without following a symlink.
pub fn permit(mountpoint: &Path, caller: u32) -> Result<(), Denied> {
    judge(
        mountpoint,
        caller,
        |walked| match std::fs::symlink_metadata(walked) {
            Ok(metadata) => Ok(Some(Entry {
                symlink: metadata.file_type().is_symlink(),
                owner: metadata.uid(),
            })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Denied::Read {
                path: walked.to_owned(),
                source,
            }),
        },
    )
}

/// The mountpoint rule (09_substrates.md, "Privileged helper"): every existing component is a
/// real directory entry, never a symlink, and the deepest one that exists belongs to the caller.
/// Root is permitted everywhere it could mount anyway, and so is every fixed host path. `look`
/// answers what is at one component, `None` when nothing is; the walk asks for each component
/// from the root down, and stops at the first that does not exist or is a symlink.
pub fn judge(
    mountpoint: &Path,
    caller: u32,
    mut look: impl FnMut(&Path) -> Result<Option<Entry>, Denied>,
) -> Result<(), Denied> {
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
        match look(&walked)? {
            Some(Entry { symlink: true, .. }) => return Err(Denied::Symlink(walked)),
            Some(Entry { owner, .. }) => deepest = (walked.clone(), owner),
            None => break,
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
        // A path whose deepest existing ancestor is `/`. Inside a Linux user namespace host root
        // may map to the overflow uid rather than 0, so read `/`'s actual owner.
        let root_owner = std::fs::metadata("/").unwrap().uid();
        if caller != root_owner {
            assert!(matches!(
                permit(Path::new("/cowshed-helper-absent/x"), caller),
                Err(Denied::Owner {
                    path,
                    owner,
                    caller: refused,
                }) if path == Path::new("/") && owner == root_owner && refused == caller
            ));
        }
        for host in HOST_MOUNTPOINTS {
            permit(Path::new(host), stranger).unwrap();
        }
        permit(Path::new("/etc"), 0).unwrap();
    }

    /// The rule over a walk's findings, without a filesystem: a table of what each path holds.
    #[test]
    fn a_mountpoint_is_judged_by_what_its_walk_finds() {
        let dir = |owner| {
            Some(Entry {
                symlink: false,
                owner,
            })
        };
        let link = Some(Entry {
            symlink: true,
            owner: 501,
        });
        let tree = [
            ("/", dir(0)),
            ("/srv", dir(0)),
            ("/srv/u", dir(501)),
            ("/srv/u/link", link),
            ("/srv/u/w", dir(501)),
            ("/srv/v", dir(502)),
        ];
        let mut asked = Vec::new();
        let mut verdict = |mountpoint: &str, caller| {
            asked.clear();
            judge(Path::new(mountpoint), caller, |path| {
                asked.push(path.to_owned());
                Ok(tree
                    .iter()
                    .find(|(at, _)| Path::new(at) == path)
                    .and_then(|(_, entry)| *entry))
            })
            .map_err(|denied| denied.to_string())
        };
        assert_eq!(verdict("/srv/u/w/x/y", 501), Ok(()));
        assert_eq!(verdict("/srv/u", 501), Ok(()));
        assert_eq!(
            verdict("/srv/u/link/x", 501),
            Err("/srv/u/link is a symlink".to_owned())
        );
        assert_eq!(
            verdict("/srv/v/x", 501),
            Err("/srv/v belongs to uid 502, not to the caller, uid 501".to_owned())
        );
        assert_eq!(
            verdict("/absent/x", 501),
            Err("/ belongs to uid 0, not to the caller, uid 501".to_owned())
        );
        assert_eq!(verdict("/srv/v/x", 0), Ok(()));
        assert_eq!(verdict(HOST_MOUNTPOINTS[0], 501), Ok(()));
        // The walk stops at the first component that does not exist: nothing below it is read.
        verdict("/srv/u/absent/deeper", 501).unwrap();
        assert_eq!(asked.last(), Some(&PathBuf::from("/srv/u/absent")));
    }
}
