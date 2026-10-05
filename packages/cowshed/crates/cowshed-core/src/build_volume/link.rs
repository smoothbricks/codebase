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
//! volume, and no tool ever names a volume directly. Every link cowshed makes has an exact
//! pattern in the repository's `info/exclude` before it exists, so linking never makes a tree
//! dirty, whatever the repository's own ignore rules say.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
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
/// the path. Rebuild-only migration and displaced-directory recovery belong to `migrate`;
/// this strict primitive never removes a caller's entries.
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
                    "run cowshed setup or retry job admission to recover a contributed real \
                     directory; move foreign files or links aside first",
                ));
            }
        }
        let directory = volume.join(state.volume.as_path());
        fs::create_dir_all(&directory).map_err(|error| link_error(&directory, &error))?;
    }
    // Excluded before any link exists, so no link is ever an untracked file.
    exclude_links(checkout, paths)?;
    for (at, target) in missing {
        std::os::unix::fs::symlink(&target, &at).map_err(|error| link_error(&at, &error))?;
    }
    Ok(())
}

/// Opens the block of `info/exclude` lines cowshed owns. Every entry in it is one of cowshed's
/// own links, anchored at the checkout root.
const EXCLUDE_BEGIN: &str =
    "# cowshed build-state links (managed by cowshed; entries are only ever added)";
const EXCLUDE_END: &str = "# end cowshed build-state links";

/// Keep the build link and every build-state link out of `git status`. To Git a symlink is a
/// file, so a repository's own directory pattern (`target/`) never matches the link that
/// replaced its directory. The patterns belong to cowshed, not to the repository, so they go in
/// the repository's `info/exclude` (its common directory's, for a linked worktree), in one
/// managed block. Entries are only ever added: linked worktrees share the file, and an entry
/// naming a path nothing occupies matches nothing. A checkout outside Git has no status to keep
/// clean.
pub(crate) fn exclude_links(checkout: &Path, paths: &[BuildStatePath]) -> Result<()> {
    use crate::fork_lock::Run as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    match fs::symlink_metadata(checkout.join(".git")) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(exclude_error(&checkout.join(".git"), &error)),
        Ok(_) => {}
    }
    let output = crate::git::git_command_at(checkout)
        .args([
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "info/exclude",
        ])
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error))?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "cannot locate {}'s Git exclude file: {}",
                checkout.display(),
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            "repair the checkout's Git metadata and retry",
        ));
    }
    let exclude = PathBuf::from(OsStr::from_bytes(
        output.stdout.strip_suffix(b"\n").unwrap_or(&output.stdout),
    ));
    let existing = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&exclude)
    {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            io::Read::read_to_end(&mut file, &mut bytes)
                .map_err(|error| exclude_error(&exclude, &error))?;
            String::from_utf8(bytes).map_err(|_| {
                CowshedError::integrity(
                    format!("{} is not UTF-8", exclude.display()),
                    "repair .git/info/exclude and retry",
                )
            })?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(exclude_error(&exclude, &error)),
    };
    // `/.cowshed/` covers the build link and the discard directory a migration moves old build
    // state into (`discard`): cowshed's namespace, never the source tree's.
    let wanted = std::iter::once("/.cowshed/".to_owned()).chain(
        std::iter::once(Path::new(BUILD_LINK))
            .chain(paths.iter().map(|state| state.checkout.as_path()))
            .map(exclude_pattern),
    );
    let Some(updated) = with_excluded(&existing, wanted) else {
        return Ok(());
    };
    let directory = exclude
        .parent()
        .expect("Git names info/exclude inside a directory");
    fs::create_dir_all(directory).map_err(|error| exclude_error(directory, &error))?;
    let staged = directory.join(format!(".exclude.cowshed-{}", unique()));
    let written = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&staged)?;
        io::Write::write_all(&mut file, updated.as_bytes())?;
        file.sync_all()?;
        fs::rename(&staged, &exclude)?;
        fs::File::open(directory)?.sync_all()
    })();
    written.map_err(|error| {
        let _ = fs::remove_file(&staged);
        exclude_error(&exclude, &error)
    })
}

/// `existing` with every pattern of `wanted` in cowshed's block, or `None` when it already has
/// them all. Lines outside the block are kept byte for byte; a missing block is appended.
fn with_excluded(existing: &str, wanted: impl Iterator<Item = String>) -> Option<String> {
    let lines: Vec<&str> = existing.lines().collect();
    let begin = lines.iter().position(|line| *line == EXCLUDE_BEGIN);
    let end = begin.and_then(|begin| {
        lines[begin..]
            .iter()
            .position(|line| *line == EXCLUDE_END)
            .map(|offset| begin + offset)
    });
    let (before, mut block, after) = match (begin, end) {
        (Some(begin), Some(end)) => (
            &lines[..begin],
            lines[begin + 1..end]
                .iter()
                .map(|line| (*line).to_owned())
                .collect::<std::collections::BTreeSet<_>>(),
            &lines[end + 1..],
        ),
        _ => (&lines[..], std::collections::BTreeSet::new(), &[][..]),
    };
    let known = block.len();
    block.extend(wanted);
    if block.len() == known && begin.is_some() && end.is_some() {
        return None;
    }
    let mut updated = String::with_capacity(existing.len() + 64 * block.len());
    for line in before {
        updated.push_str(line);
        updated.push('\n');
    }
    updated.push_str(EXCLUDE_BEGIN);
    updated.push('\n');
    for pattern in &block {
        updated.push_str(pattern);
        updated.push('\n');
    }
    updated.push_str(EXCLUDE_END);
    updated.push('\n');
    for line in after {
        updated.push_str(line);
        updated.push('\n');
    }
    Some(updated)
}

/// The gitignore pattern matching exactly the checkout-relative `path`: anchored at the root,
/// with no trailing slash so it matches a link, and with every glob or escape character quoted.
fn exclude_pattern(path: &Path) -> String {
    let path = path.to_string_lossy();
    let mut pattern = String::with_capacity(path.len() + 1);
    pattern.push('/');
    for character in path.chars() {
        if matches!(character, '*' | '?' | '[' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    if pattern.ends_with(' ') {
        pattern.insert(pattern.len() - 1, '\\');
    }
    pattern
}

fn exclude_error(path: &Path, error: &io::Error) -> CowshedError {
    CowshedError::integrity(
        format!(
            "cannot keep build-state links out of Git status via {}: {error}",
            path.display()
        ),
        "repair .git/info/exclude and retry",
    )
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
pub(crate) fn contained_parent<'a>(
    checkout: &Path,
    relative: &'a Path,
) -> io::Result<(PathBuf, &'a OsStr)> {
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

    /// A repository ignores its build directories with directory patterns (`target/`), which
    /// never match the symlink a migration puts there: every migrated checkout showed its links
    /// as untracked. Cowshed excludes its own links, and leaves the repository's lines alone.
    #[test]
    fn linked_checkouts_show_no_build_links_in_git_status() {
        use crate::fork_lock::Run as _;
        let scratch = Scratch::new("status");
        let checkout = scratch.checkout();
        let volume = scratch.volume("volume-a");
        let git = |args: &[&str]| {
            let output = crate::git::git_command_at(&checkout)
                .args(args)
                .output_locked()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        git(&["init", "--quiet"]);
        fs::write(checkout.join(".gitignore"), "target/\n.nx/\n").unwrap();
        let exclude = checkout.join(".git/info/exclude");
        fs::write(&exclude, "# the user's own\nlocal-notes\n").unwrap();
        git(&["add", ".gitignore"]);
        point(&checkout, &volume).unwrap();
        let mut weird = paths();
        weird.push(BuildStatePath::new("odd[1]*dir ", "odd").unwrap());
        link_paths(&checkout, &volume, &weird).unwrap();
        let status = git(&["status", "--porcelain", "--untracked-files=all"]);
        assert_eq!(status, "A  .gitignore\n", "{status}");
        fs::write(checkout.join("local-notes"), "mine").unwrap();
        fs::write(checkout.join("odd[1]x"), "not a link").unwrap();
        let status = git(&["status", "--porcelain", "--untracked-files=all"]);
        assert_eq!(
            status, "A  .gitignore\n?? odd[1]x\n",
            "only cowshed's exact link paths are excluded: {status}"
        );
        let written = fs::read_to_string(&exclude).unwrap();
        assert!(
            written.starts_with("# the user's own\nlocal-notes\n"),
            "{written}"
        );
        // Linking again, or with fewer paths, changes nothing.
        link_paths(&checkout, &volume, &paths()).unwrap();
        assert_eq!(fs::read_to_string(&exclude).unwrap(), written);
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
