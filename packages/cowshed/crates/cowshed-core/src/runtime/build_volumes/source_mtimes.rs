//! Land source harmonization (16_build_volumes.md, "Land", step 5.3): before the landing volume is
//! published as the target's seed, the target's sources take the landing checkout's view of time,
//! so Cargo's mtime freshness judges every adopted unit as the landing checkout would.
//!
//! A fast-forward rewrites only the files it changes. A file the landing checkout rewrote on a
//! rebase holds the same bytes in the target but an older mtime, older than a unit the landing
//! volume built from the bytes it held before; a declared output can even hold other bytes. The
//! pass reads both trees through held directories and never follows a link, so a planted link can
//! neither redirect a read nor a timestamp write outside either checkout.

use std::cmp::Ordering;
use std::ffi::CStr;
use std::fs::{File, FileTimes};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use cowshed_os::fsio::AnchoredDirectory;

use super::io as io_error;
use crate::copy::FileComparison;
use crate::{CowshedError, Result};

/// The paths a land reconciles, by provenance. Both are relative to the checkout root.
#[derive(Debug, Default)]
pub(crate) struct SourcePaths {
    /// The index's tracked files: the fast-forward made their bytes equal, so the newer mtime wins.
    pub tracked: Vec<PathBuf>,
    /// Every declared Nx output file (packages/nx-plugin README, "Cowshed Build-State Output
    /// Policy"): generated and ignored, so its bytes can differ. An output that is also tracked is
    /// reconciled as an output.
    pub outputs: Vec<PathBuf>,
}

/// What one pass did, for the land's report.
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct Harmonized {
    /// Paths that are regular files in both checkouts.
    pub examined: usize,
    /// Files given the landing checkout's newer mtime.
    pub advanced: usize,
    /// Outputs whose timestamps differed, so their bytes were compared.
    pub compared: usize,
    /// Outputs whose bytes differ, given the time of this pass.
    pub republished: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Provenance {
    Tracked,
    Output,
}

/// Give each file under `target` the landing (`source`) checkout's mtime where that is newer, and
/// a declared output whose bytes differ the time of this pass (never moving a newer one back).
/// Equal timestamps are skipped without reading a byte; a path that is not a regular file in both
/// checkouts (absent, a link, a directory, beneath a link) is left alone.
pub(super) fn harmonize(source: &Path, target: &Path, paths: &SourcePaths) -> Result<Harmonized> {
    let published = SystemTime::now();
    let mut entries: Vec<(&Path, Provenance)> =
        Vec::with_capacity(paths.tracked.len() + paths.outputs.len());
    entries.extend(
        paths
            .tracked
            .iter()
            .map(|path| (path.as_path(), Provenance::Tracked)),
    );
    entries.extend(
        paths
            .outputs
            .iter()
            .map(|path| (path.as_path(), Provenance::Output)),
    );
    // Paths in order, each path's Output first, so dedup keeps the output.
    entries.sort_unstable_by(|left, right| match left.0.cmp(right.0) {
        Ordering::Equal => right.1.cmp(&left.1),
        unequal => unequal,
    });
    entries.dedup_by(|later, kept| later.0 == kept.0);

    let source_root = AnchoredDirectory::open(source)
        .map_err(|error| io_error("open the landing checkout", source, &error))?;
    let target_root = AnchoredDirectory::open(target)
        .map_err(|error| io_error("open the target checkout", target, &error))?;
    let mut parents = Parents::default();
    let mut leaf = Vec::new();
    let mut comparison = FileComparison::new();
    let mut report = Harmonized::default();
    for (relative, provenance) in entries {
        let (parent, name) = split(relative)?;
        let Some((source_parent, target_parent)) =
            parents.held(parent, &source_root, &target_root, source, target)?
        else {
            continue;
        };
        leaf.clear();
        leaf.extend_from_slice(name);
        leaf.push(0);
        let name = CStr::from_bytes_with_nul(&leaf).map_err(|_| invalid(relative))?;
        let open = |directory: &AnchoredDirectory, root: &Path| {
            directory
                .open_regular_file(name)
                .map_err(|error| io_error("open source file", &root.join(relative), &error))
        };
        let (Some(landing), Some(file)) =
            (open(source_parent, source)?, open(target_parent, target)?)
        else {
            continue;
        };
        report.examined += 1;
        let times = |file: &File, root: &Path| {
            file.metadata()
                .and_then(|metadata| Ok((metadata.modified()?, metadata.len())))
                .map_err(|error| io_error("read source file times", &root.join(relative), &error))
        };
        let (landing_mtime, landing_len) = times(&landing, source)?;
        let (target_mtime, target_len) = times(&file, target)?;
        if landing_mtime == target_mtime {
            continue;
        }
        let equal = match provenance {
            Provenance::Tracked => true,
            Provenance::Output => {
                report.compared += 1;
                landing_len == target_len
                    && comparison
                        .equal(&landing, &file, landing_len)
                        .map_err(|error| {
                            io_error("compare declared output", &target.join(relative), &error)
                        })?
            }
        };
        let stamp = if equal {
            report.advanced += usize::from(landing_mtime > target_mtime);
            landing_mtime
        } else {
            report.republished += usize::from(published > target_mtime);
            published
        };
        if stamp > target_mtime {
            file.set_times(FileTimes::new().set_modified(stamp))
                .map_err(|error| io_error("set source mtime", &target.join(relative), &error))?;
        }
    }
    Ok(report)
}

/// The held parent directories of the last path, in both checkouts. Sorted paths visit one
/// directory's files together, so each directory chain is opened once per run of its files.
#[derive(Default)]
struct Parents<'a> {
    path: Option<&'a Path>,
    /// `None` inside: the checkout root itself.
    held: Option<(Option<AnchoredDirectory>, Option<AnchoredDirectory>)>,
}

impl<'a> Parents<'a> {
    fn held<'b>(
        &'b mut self,
        parent: &'a Path,
        source_root: &'b AnchoredDirectory,
        target_root: &'b AnchoredDirectory,
        source: &Path,
        target: &Path,
    ) -> Result<Option<(&'b AnchoredDirectory, &'b AnchoredDirectory)>> {
        if self.path != Some(parent) {
            self.path = Some(parent);
            self.held = match descend(source_root, parent, source)? {
                Located::Missing => None,
                Located::At(landing) => match descend(target_root, parent, target)? {
                    Located::Missing => None,
                    Located::At(file) => Some((landing, file)),
                },
            };
        }
        Ok(self.held.as_ref().map(|(landing, file)| {
            (
                landing.as_ref().unwrap_or(source_root),
                file.as_ref().unwrap_or(target_root),
            )
        }))
    }
}

enum Located {
    Missing,
    /// `None`: the root itself.
    At(Option<AnchoredDirectory>),
}

/// `parent` beneath `root`, one held component at a time; missing when a component is absent, not
/// a directory or a link.
fn descend(root: &AnchoredDirectory, parent: &Path, path: &Path) -> Result<Located> {
    let mut name = Vec::new();
    let mut held: Option<AnchoredDirectory> = None;
    for component in parent.components() {
        let Component::Normal(component) = component else {
            return Err(invalid(parent));
        };
        name.clear();
        name.extend_from_slice(component.as_bytes());
        name.push(0);
        let leaf = CStr::from_bytes_with_nul(&name).map_err(|_| invalid(parent))?;
        match held.as_ref().unwrap_or(root).existing_child(leaf) {
            Ok(child) => held = Some(child),
            Err(error) if absent(&error) => return Ok(Located::Missing),
            Err(error) => {
                return Err(io_error(
                    "open source directory",
                    &path.join(parent),
                    &error,
                ));
            }
        }
    }
    Ok(Located::At(held))
}

fn absent(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP)
    )
}

/// `relative` as its parent directory and leaf name; only plain relative components are accepted.
fn split(relative: &Path) -> Result<(&Path, &[u8])> {
    let mut components = relative.components();
    let Some(Component::Normal(name)) = components.next_back() else {
        return Err(invalid(relative));
    };
    let parent = components.as_path();
    if parent
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid(relative));
    }
    Ok((parent, name.as_bytes()))
}

fn invalid(relative: &Path) -> CowshedError {
    CowshedError::integrity(
        format!(
            "source path {} is not a plain path relative to the checkout",
            relative.display()
        ),
        "the land's tracked and declared-output lists name checkout-relative files only",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp_parent::canonical_parent;
    use crate::temp_root::TempRoot;
    use cowshed_os::test_backstop::CommandBackstop as _;
    use std::fs;
    use std::time::{Duration, Instant};

    const HOUR: Duration = Duration::from_secs(3600);

    fn checkouts(prefix: &str) -> (TempRoot, PathBuf, PathBuf) {
        let scratch = TempRoot::new_in(&canonical_parent(), prefix);
        let source = scratch.join("landing");
        let target = scratch.join("target");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&target).unwrap();
        (scratch, source, target)
    }

    fn put(path: &Path, bytes: &[u8], modified: SystemTime) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
    }

    fn mtime(path: &Path) -> SystemTime {
        fs::symlink_metadata(path).unwrap().modified().unwrap()
    }

    fn tracked(paths: &[&str]) -> SourcePaths {
        SourcePaths {
            tracked: paths.iter().map(PathBuf::from).collect(),
            outputs: Vec::new(),
        }
    }

    #[test]
    fn source_mtimes_take_only_a_newer_landing_mtime_for_tracked_files() {
        let (_scratch, source, target) = checkouts("source-mtimes-tracked");
        let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let new = old + HOUR;
        for (name, landing, held) in [
            ("older", new, old),
            ("equal", old, old),
            ("newer", old, new),
        ] {
            put(&source.join("src").join(name), b"same", landing);
            put(&target.join("src").join(name), b"same", held);
        }
        let report = harmonize(
            &source,
            &target,
            &tracked(&["src/older", "src/equal", "src/newer"]),
        )
        .unwrap();
        assert_eq!(
            report,
            Harmonized {
                examined: 3,
                advanced: 1,
                compared: 0,
                republished: 0
            }
        );
        assert_eq!(mtime(&target.join("src/older")), new);
        assert_eq!(mtime(&target.join("src/equal")), old);
        assert_eq!(mtime(&target.join("src/newer")), new);
        for name in ["older", "equal", "newer"] {
            assert_eq!(fs::read(target.join("src").join(name)).unwrap(), b"same");
        }
    }

    #[test]
    fn source_mtimes_republish_a_differing_output_and_advance_an_equal_one() {
        let (_scratch, source, target) = checkouts("source-mtimes-outputs");
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let t2 = t0 + 2 * HOUR;
        // The landing checkout's generated X is old; the target holds Y from a later land. A unit
        // the landing volume built from X after t2 would stay fresh beside Y under the max alone.
        put(&source.join("gen/codec.rs"), b"X", t0);
        put(&target.join("gen/codec.rs"), b"Y", t2);
        put(&source.join("gen/same.rs"), b"S", t2);
        put(&target.join("gen/same.rs"), b"S", t0);
        put(&source.join("gen/kept.rs"), b"K", t0);
        put(&target.join("gen/kept.rs"), b"K", t2);
        let before = SystemTime::now();
        let report = harmonize(
            &source,
            &target,
            &SourcePaths {
                // An output that is also tracked is reconciled as an output.
                tracked: vec![PathBuf::from("gen/codec.rs")],
                outputs: ["gen/codec.rs", "gen/same.rs", "gen/kept.rs"]
                    .into_iter()
                    .map(PathBuf::from)
                    .collect(),
            },
        )
        .unwrap();
        assert_eq!(
            report,
            Harmonized {
                examined: 3,
                advanced: 1,
                compared: 3,
                republished: 1
            }
        );
        assert!(mtime(&target.join("gen/codec.rs")) >= before);
        assert_eq!(fs::read(target.join("gen/codec.rs")).unwrap(), b"Y");
        assert_eq!(mtime(&target.join("gen/same.rs")), t2);
        assert_eq!(mtime(&target.join("gen/kept.rs")), t2);
    }

    #[test]
    fn source_mtimes_skip_absent_files_and_never_follow_a_link() {
        let (scratch, source, target) = checkouts("source-mtimes-links");
        let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let new = old + HOUR;
        let foreign = scratch.join("foreign");
        put(&foreign.join("leaf.rs"), b"foreign", old);
        put(&foreign.join("dir/inner.rs"), b"foreign", old);
        for name in ["leaf.rs", "dir/inner.rs", "only-landing.rs"] {
            put(&source.join(name), b"landing", new);
        }
        // A link leaf and a link ancestor in the target, and a file the target lacks.
        std::os::unix::fs::symlink(foreign.join("leaf.rs"), target.join("leaf.rs")).unwrap();
        std::os::unix::fs::symlink(foreign.join("dir"), target.join("dir")).unwrap();
        let report = harmonize(
            &source,
            &target,
            &tracked(&["leaf.rs", "dir/inner.rs", "only-landing.rs", "nowhere/x.rs"]),
        )
        .unwrap();
        assert_eq!(report, Harmonized::default());
        assert_eq!(mtime(&foreign.join("leaf.rs")), old);
        assert_eq!(mtime(&foreign.join("dir/inner.rs")), old);
        assert!(!target.join("only-landing.rs").exists());
    }

    #[test]
    fn source_mtimes_refuse_a_path_that_is_not_plainly_relative() {
        let (_scratch, source, target) = checkouts("source-mtimes-invalid");
        for path in ["../escape.rs", "/etc/hosts", "src/../escape.rs", ""] {
            let error = harmonize(&source, &target, &tracked(&[path])).unwrap_err();
            assert!(
                error.to_string().contains("not a plain path"),
                "{path}: {error}"
            );
        }
    }

    fn cargo(checkout: &Path, volume: &Path, args: &[&str]) -> std::process::Output {
        let output =
            std::process::Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
                .args(args)
                .args(["--quiet", "--offline"])
                .current_dir(checkout)
                .env("CARGO_TARGET_DIR", volume)
                .output_bounded()
                .unwrap();
        assert!(
            output.status.success(),
            "cargo {args:?} in {}: {}",
            checkout.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    /// The land hazard with real Cargo in its default mtime freshness: the landing volume holds a
    /// unit built from B before the landing checkout's rebase rewrote its source to A, and the
    /// target got A from an earlier land, older than that unit. Paired with the volume, the target
    /// reuses B; after harmonization it rebuilds A.
    #[test]
    fn source_mtimes_make_cargo_rebuild_a_unit_the_fast_forward_left_stale() {
        let (scratch, landing, target) = checkouts("source-mtimes-cargo");
        let volume = scratch.join("volume");
        let manifest = b"[package]\nname = \"land_probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n";
        let program = |text: &str| format!("fn main() {{ print!(\"{text}\") }}\n");
        let long_ago = SystemTime::now() - HOUR;
        put(&landing.join("Cargo.toml"), manifest, long_ago);
        put(
            &landing.join("src/main.rs"),
            program("B").as_bytes(),
            long_ago,
        );
        cargo(&landing, &volume, &["build"]);
        // The rebase rewrites the landing source to A after its unit was built.
        fs::write(landing.join("src/main.rs"), program("A")).unwrap();
        put(&target.join("Cargo.toml"), manifest, long_ago);
        put(
            &target.join("src/main.rs"),
            program("A").as_bytes(),
            long_ago,
        );
        put(
            &target.join("Cargo.lock"),
            &fs::read(landing.join("Cargo.lock")).unwrap(),
            mtime(&landing.join("Cargo.lock")),
        );

        let run = |checkout: &Path| cargo(checkout, &volume, &["run"]).stdout;
        assert_eq!(
            run(&target),
            b"B",
            "the planted pairing reuses the stale unit"
        );
        let report = harmonize(
            &landing,
            &target,
            &tracked(&["Cargo.lock", "Cargo.toml", "src/main.rs"]),
        )
        .unwrap();
        assert_eq!(report.advanced, 1);
        assert_eq!(run(&target), b"A");
    }

    /// Run by hand against a real checkout to price the pass: the root and a JSON
    /// `{"tracked":[...],"outputs":[...]}` file of checkout-relative paths. The same checkout on
    /// both sides opens and stats every file twice and writes nothing.
    #[test]
    #[ignore = "a measurement over a real checkout: COWSHED_MTIME_BENCH_ROOT and COWSHED_MTIME_BENCH_PATHS"]
    fn source_mtime_stat_cost() {
        let root = PathBuf::from(
            std::env::var_os("COWSHED_MTIME_BENCH_ROOT").expect("COWSHED_MTIME_BENCH_ROOT"),
        );
        let listing: serde_json::Value = serde_json::from_slice(
            &fs::read(
                std::env::var_os("COWSHED_MTIME_BENCH_PATHS").expect("COWSHED_MTIME_BENCH_PATHS"),
            )
            .unwrap(),
        )
        .unwrap();
        let list = |key: &str| -> Vec<PathBuf> {
            listing[key]
                .as_array()
                .unwrap_or_else(|| panic!("{key} is an array"))
                .iter()
                .map(|path| PathBuf::from(path.as_str().expect("a path string")))
                .collect()
        };
        let paths = SourcePaths {
            tracked: list("tracked"),
            outputs: list("outputs"),
        };
        let started = Instant::now();
        let report = harmonize(&root, &root, &paths).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(report.advanced + report.republished + report.compared, 0);
        println!(
            "source_mtime_stat_cost: same-checkout pass (open+fstat both sides, no compare, no write) \
             over {} tracked + {} output paths: {} regular files examined in {elapsed:?}",
            paths.tracked.len(),
            paths.outputs.len(),
            report.examined,
        );
    }
}
