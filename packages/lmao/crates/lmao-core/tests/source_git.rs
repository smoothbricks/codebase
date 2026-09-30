//! The build-time source → commit map that span provenance reads, and the files `build.rs`
//! tells cargo that map depends on.

#[path = "../git_state.rs"]
mod git_state;

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use git_state::{SourceGit, git_output, source_git};

/// Guards the empty-map failure: git answered a revision, yet every per-file lookup came back
/// empty. Skipped where the build had no git at all (e.g. a source archive), where the map is
/// legitimately empty.
#[test]
fn resolves_committed_sources_when_git_answered_a_revision() {
    if !env!("LMAO_GIT_REVISION").is_empty() {
        assert!(lmao_core::source_git_sha("src/lib.rs").is_some());
    }
}

/// A cargo git checkout: the branch is a loose ref and no `packed-refs` was ever written.
#[test]
fn a_loose_branch_is_watched_through_its_ref_file() {
    let repository = Repository::with_package("loose", "files", Tracking::Tracked);
    let inputs = repository.fresh_inputs();
    repository.assert_watched(&inputs, "HEAD");
    repository.assert_watched(&inputs, "refs/heads/main");
}

#[test]
fn a_packed_branch_is_watched_where_its_next_commit_writes() {
    let repository = Repository::with_package("packed", "files", Tracking::Tracked);
    repository.git(&["pack-refs", "--all"]);
    let inputs = repository.fresh_inputs();
    repository.commit();
    repository.assert_watched(&inputs, "refs/heads/main");
}

#[test]
fn a_detached_head_is_watched_through_head() {
    let repository = Repository::with_package("detached", "files", Tracking::Tracked);
    repository.git(&["checkout", "--quiet", "--detach"]);
    let inputs = repository.fresh_inputs();
    repository.commit();
    repository.assert_watched(&inputs, "HEAD");
}

#[test]
fn a_reftable_repository_is_watched_through_its_table_list() {
    let repository = Repository::with_package("reftable", "reftable", Tracking::Tracked);
    let inputs = repository.fresh_inputs();
    repository.commit();
    repository.assert_watched(&inputs, "reftable/tables.list");
}

/// A file's last-touch commit moves only with a commit, which the ref inputs already see; an
/// uncommitted edit changes no row of the map, so it must not rerun the build script — cargo
/// recompiles lmao-core and every crate above it whenever that script reruns.
#[test]
fn a_source_edit_is_not_an_input() {
    let repository = Repository::with_package("edit", "files", Tracking::Tracked);
    let manifest_dir = repository.manifest_dir();
    let source = manifest_dir.join("src/lib.rs");
    let inputs = repository.source_git(&repository.package_root()).inputs;
    assert!(
        !inputs
            .iter()
            .any(|input| source.starts_with(manifest_dir.join(input))),
        "{} is watched through {inputs:?}",
        source.display()
    );
}

/// A cargo home linked elsewhere puts the manifest behind a symlink while git reports the
/// resolved repository path; the map and its inputs must not depend on which spelling the build
/// was handed.
#[test]
fn a_symlinked_checkout_resolves_the_same_map_and_inputs() {
    let repository = Repository::with_package("symlinked", "files", Tracking::Tracked);
    let direct = repository.source_git(&repository.package_root());
    let link = repository.scratch.join("link");
    symlink(&repository.dir, &link).expect("link the scratch repository");
    let linked = repository.source_git(&link.join(PACKAGE));
    assert_eq!(
        direct.commits.get("src/lib.rs"),
        Some(&repository.head()),
        "the direct spelling resolves the committed source"
    );
    assert_eq!(linked.commits, direct.commits);
    assert_eq!(linked.inputs, direct.inputs);
}

/// A package installed under an enclosing repository that does not track it — the npm tarball in
/// a consumer's `node_modules`: that repository's history says nothing about these sources, so
/// the map is empty and none of its state is an input, and a consumer commit reruns nothing.
#[test]
fn sources_their_repository_does_not_track_name_no_revision_and_no_inputs() {
    let repository = Repository::with_package("untracked", "files", Tracking::Ignored);
    let resolved = repository.source_git(&repository.package_root());
    assert_eq!(resolved.revision, None);
    assert!(resolved.commits.is_empty(), "{:?}", resolved.commits);
    assert!(resolved.inputs.is_empty(), "{:?}", resolved.inputs);
}

/// Where the package sits in the scratch repository, and the crate within it — this crate's own
/// layout.
const PACKAGE: &str = "packages/lmao";
const CRATE: &str = "crates/lmao-core";

enum Tracking {
    Tracked,
    /// `.gitignore` keeps the package out of the repository.
    Ignored,
}

/// A scratch repository holding a package laid out like this one, removed on drop.
struct Repository {
    scratch: PathBuf,
    dir: PathBuf,
}

impl Repository {
    fn with_package(name: &str, ref_format: &str, tracking: Tracking) -> Self {
        let scratch = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("source-git-{name}-{}", std::process::id()));
        if scratch.exists() {
            fs::remove_dir_all(&scratch).expect("clear the previous scratch repository");
        }
        let dir = scratch.join("repository");
        let repository = Self { scratch, dir };
        let source_dir = repository.manifest_dir().join("src");
        fs::create_dir_all(&source_dir).expect("create the scratch package");
        fs::write(source_dir.join("lib.rs"), "pub fn f() {}\n").expect("write a source");
        let ignored = match tracking {
            Tracking::Tracked => "",
            Tracking::Ignored => "/packages/\n",
        };
        fs::write(repository.dir.join(".gitignore"), ignored).expect("write .gitignore");
        repository.git(&[
            "init",
            "--quiet",
            "--initial-branch=main",
            &format!("--ref-format={ref_format}"),
        ]);
        repository.git(&["add", "--all"]);
        repository.commit();
        repository
    }

    fn package_root(&self) -> PathBuf {
        self.dir.join(PACKAGE)
    }

    fn manifest_dir(&self) -> PathBuf {
        self.package_root().join(CRATE)
    }

    fn source_git(&self, package_root: &Path) -> SourceGit {
        source_git(package_root, &package_root.join(CRATE), None).expect("resolve the source map")
    }

    /// The inputs as cargo resolves them — against the manifest directory — after asserting each
    /// exists: cargo treats a watched path that is missing as changed, so one absent input reruns
    /// the build script, and recompiles lmao-core and everything above it, on every build.
    fn fresh_inputs(&self) -> Vec<PathBuf> {
        let manifest_dir = self.manifest_dir();
        let inputs: Vec<PathBuf> = self
            .source_git(&self.package_root())
            .inputs
            .iter()
            .map(|input| manifest_dir.join(input))
            .collect();
        let missing: Vec<&PathBuf> = inputs.iter().filter(|input| !input.exists()).collect();
        assert!(
            missing.is_empty(),
            "watched paths that do not exist: {missing:?}"
        );
        inputs
            .iter()
            .map(|input| fs::canonicalize(input).expect("canonicalize an existing input"))
            .collect()
    }

    /// The git file `name` — which git just wrote to move HEAD — lies under a watched path, so the
    /// move reruns the build script.
    fn assert_watched(&self, inputs: &[PathBuf], name: &str) {
        let manifest_dir = self.manifest_dir();
        let written = manifest_dir.join(
            git_output(&manifest_dir, &["rev-parse", "--git-path", name])
                .unwrap_or_else(|| panic!("git rev-parse --git-path {name}")),
        );
        let written = fs::canonicalize(&written)
            .unwrap_or_else(|error| panic!("{} was not written: {error}", written.display()));
        assert!(
            inputs.iter().any(|input| written.starts_with(input)),
            "{} lies under none of {inputs:?}",
            written.display()
        );
    }

    fn commit(&self) {
        self.git(&["commit", "--quiet", "--allow-empty", "--message=move HEAD"]);
    }

    fn head(&self) -> String {
        self.git(&["rev-parse", "HEAD"])
    }

    fn git(&self, args: &[&str]) -> String {
        let mut command = vec![
            "-c",
            "user.name=lmao",
            "-c",
            "user.email=lmao@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
        ];
        command.extend_from_slice(args);
        git_output(&self.dir, &command)
            .unwrap_or_else(|| panic!("git {args:?} failed in {}", self.dir.display()))
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.scratch) {
            eprintln!("leaving {}: {error}", self.scratch.display());
        }
    }
}
