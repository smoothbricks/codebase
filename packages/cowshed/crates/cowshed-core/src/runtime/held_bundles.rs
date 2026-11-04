//! The plain `gc`'s held-bundle sweep (02_workspaces.md, `rm` step 1), run beside the project's
//! router actor instead of inside one of its routes.
//!
//! The router is serial (05_gateway.md, "A controller connection heartbeats"): a call waits
//! behind whatever op it serves. Measuring an abandon bundle may fetch it whole, seconds per
//! self-contained bundle, and every retirement a coordinator makes ends in a `gc`, so a trash of
//! them held every exec, info and fork behind each retirement for minutes. The actor now only
//! reads what a sweep needs (the trash listing and main's tip) and hands it here; the `gc` answer
//! waits for the sweep after the actor has moved on.
//!
//! Requests that arrive while a sweep runs are answered together by the one sweep that follows
//! it, so retirements in a burst cost one more sweep, not one each. A bundle a sweep found main
//! does not hold is not measured again until its file or main's tip changes: its verdict is
//! recorded by both.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::api::dto::{GcCandidate, GcReason, GcReport, Sha256Digest};
use crate::error::{CowshedError, Result};
use crate::git::BundleTip;

/// What one sweep measures: the bundles the actor listed in the trash, main's tip they are
/// measured against, main's repository, and the project whose deletion log records each drop.
#[derive(Clone, Debug)]
pub(crate) struct Sweep {
    pub(crate) bundles: Vec<PathBuf>,
    pub(crate) tip: String,
    pub(crate) main: crate::git::GitRepository,
    pub(crate) project_root: PathBuf,
}

/// One measurement of `bundle` against the sweep's tip.
pub(crate) type Measured = Pin<Box<dyn Future<Output = Result<Vec<BundleTip>>> + Send>>;
pub(crate) type Measure = Arc<dyn Fn(&Sweep, &Path) -> Measured + Send + Sync>;

/// The measurement a controller runs: [`crate::git::GitRepository::measure_bundle`] on main.
pub(crate) fn measure_against_main() -> Measure {
    Arc::new(|sweep: &Sweep, bundle: &Path| {
        let (main, tip, bundle) = (sweep.main.clone(), sweep.tip.clone(), bundle.to_owned());
        Box::pin(async move { main.measure_bundle(&bundle, &tip).await })
    })
}

/// The file a verdict was measured from. A bundle is written once and renamed into place, so a
/// rewrite under the same name changes at least its inode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BundleFile {
    device: u64,
    inode: u64,
    len: u64,
    modified_ns: i64,
}

impl BundleFile {
    fn of(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            len: metadata.len(),
            modified_ns: metadata
                .mtime()
                .saturating_mul(1_000_000_000)
                .saturating_add(metadata.mtime_nsec()),
        }
    }
}

/// A bundle main did not hold: the file and main's tip it was measured at.
#[derive(Debug, Eq, PartialEq)]
struct Unheld {
    file: BundleFile,
    tip: String,
}

struct Request {
    sweep: Sweep,
    dry_run: bool,
    answer: oneshot::Sender<Result<Vec<GcCandidate>>>,
}

/// The project's one held-bundle sweeper. Dropping it ends its task once the sweep in hand is
/// answered.
pub(crate) struct HeldBundles {
    requests: mpsc::UnboundedSender<Request>,
}

impl HeldBundles {
    pub(crate) fn spawn(measure: Measure) -> Self {
        let (requests, received) = mpsc::unbounded_channel();
        tokio::spawn(serve(received, measure));
        Self { requests }
    }

    /// The bundles of `sweep` main holds, deleted unless `dry_run`. The answer comes from the
    /// next sweep to start, which serves every request queued before it.
    pub(crate) fn sweep(
        &self,
        sweep: Sweep,
        dry_run: bool,
    ) -> impl Future<Output = Result<Vec<GcCandidate>>> + Send + 'static {
        let (answer, answered) = oneshot::channel();
        let sent = self.requests.send(Request {
            sweep,
            dry_run,
            answer,
        });
        async move {
            sent.map_err(|_| stopped())?;
            answered.await.map_err(|_| stopped())?
        }
    }
}

fn stopped() -> CowshedError {
    CowshedError::internal("the held-bundle sweeper stopped before answering")
}

async fn serve(mut received: mpsc::UnboundedReceiver<Request>, measure: Measure) {
    let mut unheld = HashMap::<PathBuf, Unheld>::new();
    while let Some(first) = received.recv().await {
        let mut batch = vec![first];
        while let Ok(next) = received.try_recv() {
            batch.push(next);
        }
        // The newest request carries the listing and tip the actor read last.
        let sweep = batch
            .last()
            .map(|request| request.sweep.clone())
            .expect("one request");
        let delete = batch.iter().any(|request| !request.dry_run);
        let result = sweep_once(&sweep, delete, &measure, &mut unheld).await;
        for request in batch {
            let _ = request.answer.send(result.clone());
        }
    }
}

/// One pass over `sweep.bundles`: each one main holds is named, and deleted when `delete`, with
/// its patch-id sidecar. A bundle another sweep or `gc --abandoned` deleted meanwhile is gone,
/// not an error; one that cannot be measured is kept and said on stderr.
async fn sweep_once(
    sweep: &Sweep,
    delete: bool,
    measure: &Measure,
    unheld: &mut HashMap<PathBuf, Unheld>,
) -> Result<Vec<GcCandidate>> {
    use sha2::Digest as _;
    use std::os::unix::ffi::OsStrExt as _;

    unheld.retain(|path, _| sweep.bundles.binary_search(path).is_ok());
    let mut held = Vec::new();
    for bundle in &sweep.bundles {
        let metadata = match tokio::fs::symlink_metadata(bundle).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(inspect_error(bundle, &error)),
        };
        let file = BundleFile::of(&metadata);
        if unheld
            .get(bundle)
            .is_some_and(|verdict| verdict.file == file && verdict.tip == sweep.tip)
        {
            continue;
        }
        let tips = match measure(sweep, bundle).await {
            Ok(tips) => tips,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: kept abandon bundle {}: cannot measure it against main: {}",
                    bundle.display(),
                    error.message
                );
                continue;
            }
        };
        if !crate::git::bundle_is_held(&tips) {
            unheld.insert(
                bundle.clone(),
                Unheld {
                    file,
                    tip: sweep.tip.clone(),
                },
            );
            continue;
        }
        unheld.remove(bundle);
        let mut identity = sha2::Sha256::new();
        identity.update(bundle.as_os_str().as_bytes());
        identity.update(metadata.len().to_le_bytes());
        if delete {
            match tokio::fs::remove_file(bundle).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(delete_error(bundle, &error)),
            }
            let sidecar = crate::storage::recovery::abandon_bundle_sidecar(bundle);
            match tokio::fs::remove_file(&sidecar).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(delete_error(&sidecar, &error)),
            }
            let workspace = bundle
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.rsplit_once('-'))
                .map_or("", |(workspace, _)| workspace);
            crate::storage::deletion_log::log_deletion(
                &sweep.project_root,
                crate::storage::deletion_log::DeletionOp::RemoveAbandonedBundle,
                crate::storage::deletion_log::DeletionKind::Other,
                workspace,
                None,
                bundle,
            );
        }
        held.push(GcCandidate {
            identity: Sha256Digest::from_bytes(identity.finalize().into()),
            path: bundle.clone(),
            bytes: metadata.blocks().saturating_mul(512),
            reason: GcReason::HeldBundle,
        });
    }
    Ok(held)
}

/// `report` with a sweep's answer folded in: each bundle main holds is a candidate at `at`, after
/// the substrate's own, counted examined and, outside a dry run, reclaimed, its bytes released.
/// Then the abandon bundles `kept` (an `--abandoned` preview) names and the sweep left are said,
/// so they are never out of sight.
pub(crate) async fn folded(
    mut report: GcReport,
    at: usize,
    held: Option<impl Future<Output = Result<Vec<GcCandidate>>>>,
    kept: Option<crate::storage::lifecycle::StorageGcPlan>,
) -> Result<GcReport> {
    let held = match held {
        Some(sweep) => sweep.await?,
        None => Vec::new(),
    };
    let count =
        u64::try_from(held.len()).map_err(|_| CowshedError::internal("GC count overflow"))?;
    let bytes = held
        .iter()
        .try_fold(0_u64, |sum, bundle| sum.checked_add(bundle.bytes))
        .ok_or_else(|| CowshedError::internal("GC candidate byte accounting overflow"))?;
    report.examined += count;
    if !report.dry_run {
        report.reclaimed += count;
    }
    report.released_bytes = report.released_bytes.saturating_add(bytes);
    if let Some(kept) = kept {
        let kept = kept
            .candidates()
            .iter()
            .filter(|candidate| !held.iter().any(|bundle| bundle.path == candidate.path()));
        if let Some(kept) = crate::storage::recovery::KeptAbandonBundles::named_by(kept)? {
            cowshed_os::eprintln!(
                "cowshed: {kept}; {}",
                crate::storage::recovery::KeptAbandonBundles::HINT
            );
        }
    }
    report.candidates.splice(at..at, held);
    Ok(report)
}

fn inspect_error(bundle: &Path, error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "cannot inspect the abandon bundle {}: {error}",
            bundle.display()
        ),
        "check controller storage permissions, then cowshed gc",
    )
}

fn delete_error(path: &Path, error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "cannot delete the abandon bundle {}: {error}",
            path.display()
        ),
        "check controller storage permissions, then cowshed gc",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Trash {
        _root: crate::temp_root::TempRoot,
        dir: PathBuf,
        store: crate::temp_root::FixtureStore,
    }

    fn trash(bundles: usize) -> (Trash, Vec<PathBuf>) {
        let root = crate::temp_root::TempRoot::new("cowshed-held-bundles");
        let dir = root.join("trash");
        std::fs::create_dir_all(&dir).expect("trash");
        let mut paths: Vec<PathBuf> = (0..bundles)
            .map(|index| {
                let path = dir.join(format!("ws{index}-{index:040}.bundle"));
                std::fs::write(&path, b"# v2 git bundle\n").expect("bundle");
                path
            })
            .collect();
        paths.sort();
        let store = crate::temp_root::FixtureStore::new();
        (
            Trash {
                _root: root,
                dir,
                store,
            },
            paths,
        )
    }

    fn sweep(trash: &Trash, bundles: &[PathBuf], tip: &str) -> Sweep {
        Sweep {
            bundles: bundles.to_vec(),
            tip: tip.to_owned(),
            main: crate::git::GitRepository::from_root(trash.store.shared(), &trash.dir),
            project_root: trash.dir.clone(),
        }
    }

    /// A measurement that counts its calls, answers `held` for every bundle, and first waits for
    /// `gate` when one is given, so requests can pile up behind a running sweep.
    fn counting(
        held: bool,
        gate: Option<Arc<tokio::sync::Semaphore>>,
    ) -> (Measure, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let measure: Measure = Arc::new(move |_: &Sweep, _: &Path| {
            counted.fetch_add(1, Ordering::SeqCst);
            let gate = gate.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.acquire().await.expect("gate").forget();
                }
                let commit = "a".repeat(40);
                Ok(vec![BundleTip {
                    commits: vec![commit.clone()],
                    held: if held {
                        [commit].into_iter().collect()
                    } else {
                        Default::default()
                    },
                }])
            })
        });
        (measure, calls)
    }

    /// Ten retirements' gcs over a trash of 19 bundles main does not hold measure each bundle
    /// once per main tip: the burst behind a running sweep shares one more sweep, and that one
    /// and every later one at the same tip read the recorded verdicts. Main moving re-measures.
    #[tokio::test]
    async fn retirements_measure_each_unheld_bundle_once_per_main_tip() {
        let (trash, bundles) = trash(19);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (measure, calls) = counting(false, Some(Arc::clone(&gate)));
        let sweeper = HeldBundles::spawn(measure);
        let tip = "1".repeat(40);
        let answers: Vec<_> = (0..10)
            .map(|_| sweeper.sweep(sweep(&trash, &bundles, &tip), false))
            .collect();
        gate.add_permits(19 * 10);
        for answer in answers {
            assert_eq!(answer.await.expect("sweep"), Vec::new());
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            19,
            "one measurement per bundle"
        );
        for _ in 0..10 {
            sweeper
                .sweep(sweep(&trash, &bundles, &tip), false)
                .await
                .expect("sweep");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            19,
            "verdicts recorded at this tip"
        );

        let moved = "2".repeat(40);
        sweeper
            .sweep(sweep(&trash, &bundles, &moved), false)
            .await
            .expect("sweep");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            38,
            "main moved: measured again"
        );
        assert!(bundles.iter().all(|bundle| bundle.exists()));
    }

    /// A rewritten bundle under the same name is measured again at an unchanged tip.
    #[tokio::test]
    async fn a_rewritten_bundle_is_measured_again() {
        let (trash, bundles) = trash(1);
        let (measure, calls) = counting(false, None);
        let sweeper = HeldBundles::spawn(measure);
        let tip = "1".repeat(40);
        sweeper
            .sweep(sweep(&trash, &bundles, &tip), false)
            .await
            .expect("sweep");
        let staged = trash.dir.join("staged");
        std::fs::write(&staged, b"# v2 git bundle\nrewritten\n").expect("rewrite");
        std::fs::rename(&staged, &bundles[0]).expect("rename into place");
        sweeper
            .sweep(sweep(&trash, &bundles, &tip), false)
            .await
            .expect("sweep");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// A held bundle is named; a dry run deletes nothing, a sweep deletes it with its sidecar.
    #[tokio::test]
    async fn a_held_bundle_is_dropped_with_its_sidecar_only_outside_a_dry_run() {
        let (trash, bundles) = trash(1);
        let sidecar = crate::storage::recovery::abandon_bundle_sidecar(&bundles[0]);
        std::fs::write(&sidecar, b"sidecar\n").expect("sidecar");
        let (measure, _) = counting(true, None);
        let sweeper = HeldBundles::spawn(measure);
        let tip = "1".repeat(40);
        let named = sweeper
            .sweep(sweep(&trash, &bundles, &tip), true)
            .await
            .expect("dry run");
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].reason, GcReason::HeldBundle);
        assert!(bundles[0].exists() && sidecar.exists());
        let dropped = sweeper
            .sweep(sweep(&trash, &bundles, &tip), false)
            .await
            .expect("sweep");
        assert_eq!(dropped, named);
        assert!(!bundles[0].exists() && !sidecar.exists());
    }
}
