//! The git side of `build.rs`: the source → commit map span provenance reads, and the files that
//! map depends on. Shared with the tests that hold it to cargo's `rerun-if-changed` contract.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Git's repository-local variables (`git rev-parse --local-env-vars` as of git 2.55), which
/// git exports to hooks. A build started from a hook would otherwise let them override the
/// `cwd`-based repository discovery these git calls rely on; githooks(5) says to clear them.
const GIT_REPOSITORY_ENV: &[&str] = &[
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CONFIG",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_OBJECT_DIRECTORY",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_IMPLICIT_WORK_TREE",
    "GIT_GRAFT_FILE",
    "GIT_INDEX_FILE",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_REPLACE_REF_BASE",
    "GIT_PREFIX",
    "GIT_SHALLOW_FILE",
    "GIT_COMMON_DIR",
];

pub struct SourceGit {
    /// The commit the map describes; `None` when no repository holds these sources.
    pub revision: Option<String>,
    /// Every spelling `file!()` can take for a source → the commit that last touched it.
    pub commits: BTreeMap<String, String>,
    /// What the map depends on, as `rerun-if-changed` paths; see [`revision_inputs`].
    pub inputs: Vec<PathBuf>,
}

impl SourceGit {
    /// No repository holds these sources: nothing to map and nothing to watch.
    const UNTRACKED: Self = Self {
        revision: None,
        commits: BTreeMap::new(),
        inputs: Vec::new(),
    };
}

/// Maps every Rust source under `package_root/crates` to the commit that last touched it at
/// `requested`, or at `HEAD` when nothing is requested.
///
/// The map is a function of that revision alone. A file's last-touch commit moves only when a
/// commit does, and that commit moves HEAD, which the inputs watch; an uncommitted edit changes no
/// row, so no source file is an input — cargo recompiles lmao-core and every crate above it
/// whenever this build script reruns.
pub fn source_git(
    package_root: &Path,
    manifest_dir: &Path,
    requested: Option<&str>,
) -> Result<SourceGit, String> {
    // Where the package sits in its repository, as git reports it. Composing the repository
    // spelling from this — instead of stripping `--show-toplevel` off the package path — keeps
    // the map whole when the checkout is reached through a symlink (a cargo home linked
    // elsewhere): git reports the resolved path, cargo hands the build the one it was given.
    let Some(prefix) = git_output(package_root, &["rev-parse", "--show-prefix"]) else {
        return Ok(SourceGit::UNTRACKED);
    };
    let revision = match requested {
        Some(requested) => git_output(
            package_root,
            &["rev-parse", "--verify", &format!("{requested}^{{commit}}")],
        )
        .ok_or_else(|| {
            format!(
                "does not name a commit in the repository holding {}: {requested}",
                package_root.display()
            )
        })?,
        None => match git_output(package_root, &["rev-parse", "HEAD"]) {
            Some(head) => head,
            None => return Ok(SourceGit::UNTRACKED),
        },
    };

    let mut sources = Vec::new();
    collect_rust_files(&package_root.join("crates"), &mut sources);
    let mut commits = BTreeMap::new();
    for source in &sources {
        let Some(in_package) = relative_utf8(source, package_root) else {
            continue;
        };
        let Some(commit) = git_output(
            package_root,
            &["rev-list", "-1", &revision, "--", in_package],
        )
        .filter(|commit| !commit.is_empty()) else {
            continue;
        };
        if let Some(in_crate) = relative_utf8(source, manifest_dir) {
            commits.insert(in_crate.to_owned(), commit.clone());
        }
        commits.insert(format!("{prefix}{in_package}"), commit.clone());
        commits.insert(in_package.to_owned(), commit);
    }
    if commits.is_empty() {
        // The enclosing repository does not hold these sources — the npm tarball in a consumer's
        // `node_modules`. Its history says nothing about them, and watching its HEAD would rerun
        // this build script on every consumer commit for a map that stays empty.
        return Ok(SourceGit::UNTRACKED);
    }
    Ok(SourceGit {
        revision: Some(revision),
        commits,
        inputs: revision_inputs(manifest_dir),
    })
}

/// The files whose content decides which commit HEAD names, as git spells them from
/// `manifest_dir`: relative wherever git can, which is how cargo resolves a `rerun-if-changed`
/// path, so the fingerprint is the same wherever the checkout is mounted.
///
/// Every path returned exists. Cargo treats a watched path that is missing as changed, so one
/// absent file — the `packed-refs` a fresh clone or a cargo git checkout never wrote — reran the
/// build script, and recompiled lmao-core and every crate above it, on every build.
fn revision_inputs(manifest_dir: &Path) -> Vec<PathBuf> {
    let exists = |path: &Path| manifest_dir.join(path).exists();
    let Some([shallow, reftable, head, packed_refs]) = git_paths(
        manifest_dir,
        ["shallow", "reftable/tables.list", "HEAD", "packed-refs"],
    ) else {
        return Vec::new();
    };
    // A shallow clone's boundary grafts history, so deepening it moves last-touch commits.
    let mut inputs: Vec<PathBuf> = Some(shallow)
        .filter(|path| exists(path))
        .into_iter()
        .collect();
    if exists(&reftable) {
        // A reftable repository rewrites its table list on every ref update, HEAD's included;
        // its `HEAD` file is a fixed stub.
        inputs.push(reftable);
        return inputs;
    }
    let branch = fs::read_to_string(manifest_dir.join(&head))
        .ok()
        .and_then(|head| head.trim().strip_prefix("ref: ").map(str::to_owned));
    inputs.push(head);
    // Detached, HEAD names the commit itself.
    let Some([branch]) = branch.and_then(|branch| git_paths(manifest_dir, [branch.as_str()]))
    else {
        return inputs;
    };
    if exists(&branch) {
        // A loose ref shadows its packed entry; packing deletes the file, which cargo sees.
        inputs.push(branch);
        return inputs;
    }
    // A packed or unborn branch: `packed-refs` holds it, and the commit that next moves it writes
    // the loose file. Cargo compares a watched directory by the newest mtime beneath it, so
    // watching the nearest existing directory above that file sees it appear.
    if exists(&packed_refs) {
        inputs.push(packed_refs);
    }
    if let Some(directory) = branch
        .ancestors()
        .skip(1)
        .find(|directory| manifest_dir.join(directory).is_dir())
    {
        inputs.push(directory.to_path_buf());
    }
    inputs
}

/// `git rev-parse --git-path` for every name, in one invocation.
fn git_paths<const N: usize>(cwd: &Path, names: [&str; N]) -> Option<[PathBuf; N]> {
    let mut args = Vec::with_capacity(1 + 2 * N);
    args.push("rev-parse");
    for name in names {
        args.extend(["--git-path", name]);
    }
    let paths: Vec<PathBuf> = git_output(cwd, &args)?.lines().map(PathBuf::from).collect();
    paths.try_into().ok()
}

fn collect_rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("read {}: {error}", dir.display()))
    {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", dir.display()))
            .path();
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("target") {
                continue;
            }
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn relative_utf8<'a>(path: &'a Path, base: &Path) -> Option<&'a str> {
    path.strip_prefix(base).ok()?.to_str()
}

pub fn git_output(cwd: &Path, args: &[&str]) -> Option<String> {
    let mut command = Command::new("git");
    command.current_dir(cwd).args(args);
    for variable in GIT_REPOSITORY_ENV {
        command.env_remove(variable);
    }
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
}
