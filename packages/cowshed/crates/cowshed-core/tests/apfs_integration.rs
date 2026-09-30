#![cfg(target_os = "macos")]

use std::error::Error;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cowshed_core::apfs::{ApfsCaseSensitivity, SystemCommandRunner, volume_name};
use cowshed_core::metadata::{
    GrantSet, ImageCapacity, ImageFormat, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, PortBlock,
    WorkspaceName,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::apfs::extents::{count_extents, rewrite_sibling};
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::apfs::{
    ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig, CheckoutLayout, IncarnationSource,
    TokioApfsBlockingLane, volume_label,
};
use cowshed_core::storage::lifecycle::{
    AdoptRequest, Destination, LifecyclePlanner, MountIntent, MountState, OperationIdentity, Pin,
    RestoreMode, Revision, Substrate,
};
use cowshed_core::storage::{CheckpointLabel, StorageLayout, WORKSPACE_MARKER_PATH};

struct IntegrationRoot {
    path: PathBuf,
}

/// Every integration root lives directly under this prefix and spells out the pid of the run
/// that owns it. The pid is the whole cleanup protocol: a later run can tell a live root from an
/// abandoned one without any lock file or shared state of its own.
const ROOT_PREFIX: &str = "/private/tmp/cowshed-itest-";

impl IntegrationRoot {
    fn new(format: ImageFormat) -> Result<Self, Box<dyn Error>> {
        sweep_dead_runs();
        let path = PathBuf::from(format!(
            "{ROOT_PREFIX}{}-{}",
            std::process::id(),
            format.extension()
        ));
        if path.exists() {
            detach_images(|image| image.starts_with(&*path.to_string_lossy()));
            fs::remove_dir_all(&path)?;
        }
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for IntegrationRoot {
    fn drop(&mut self) {
        // A failed test unwinds with its volumes still attached; removing the tree without
        // detaching first strands kernel attachments pointing into a half-deleted root, and
        // leaves their volumes showing up in Finder and `mount` until the machine reboots.
        detach_images(|image| image.starts_with(&*self.path.to_string_lossy()));
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Reclaim what runs that can no longer clean up after themselves left behind. Nothing runs after
/// a SIGKILL — a harness timeout, a bounded-exec force-kill, a `cargo test` killed mid-mount — so
/// the only protocol that always converges is that every run sweeps its dead predecessors.
///
/// The sweep is driven off the attachment table rather than the directory listing, because the
/// two residues outlive each other independently: a root directory can be deleted (by hand, or by
/// a tmp reaper) while its images stay attached, and an attached image keeps working from a
/// deleted backing file. Detaching therefore selects on the image path `hdiutil` still reports,
/// not on what is on disk now.
fn sweep_dead_runs() {
    detach_images(|image| owner_pid(image).is_some_and(process_is_gone));
    let Ok(entries) = fs::read_dir("/private/tmp") else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if owner_pid(&path.to_string_lossy()).is_some_and(process_is_gone) {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

/// The pid of the run that owns an integration root, given the root itself or any path below it.
fn owner_pid(path: &str) -> Option<i32> {
    path.strip_prefix(ROOT_PREFIX)?
        .split(['-', '/'])
        .next()?
        .parse()
        .ok()
}

/// Whether no process holds `pid` any more. Signal 0 runs the existence and permission checks
/// without delivering anything, and `ESRCH` is the single answer that proves the owner is gone —
/// `EPERM` means it is alive under another user, so a run must not reclaim its root.
fn process_is_gone(pid: i32) -> bool {
    // SAFETY: `kill` with signal 0 only probes; it delivers nothing and touches no memory.
    let probe = unsafe { libc::kill(pid, 0) };
    probe != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// Detach every attached disk image whose backing path `select` accepts, using each image's first
/// (whole-device) `/dev/diskN` line — detaching the whole device takes its synthesized APFS
/// container and every mounted volume down with it. Text parsing is deliberate: this is
/// best-effort hygiene, and a parse miss only means the residue waits for the next sweep.
fn detach_images(select: impl Fn(&str) -> bool) {
    let Ok(output) = std::process::Command::new("hdiutil").arg("info").output() else {
        return;
    };
    let info = String::from_utf8_lossy(&output.stdout);
    let mut in_matching_image = false;
    for line in info.lines() {
        if let Some(image_path) = line.strip_prefix("image-path") {
            in_matching_image = select(image_path.trim_start_matches([' ', ':']));
            continue;
        }
        if !in_matching_image {
            continue;
        }
        if let Some(device) = line.split_whitespace().next().filter(|token| {
            token.starts_with("/dev/disk") && !token.trim_start_matches("/dev/disk").contains('s')
        }) {
            let _ = std::process::Command::new("hdiutil")
                .args(["detach", device, "-force"])
                .output();
            in_matching_image = false;
        }
    }
}

struct DeterministicIncarnations(AtomicU64);

impl IncarnationSource for DeterministicIncarnations {
    fn mint(&self) -> Result<cowshed_core::metadata::WorkspaceIncarnation, ApfsStorageError> {
        let value = self.0.fetch_add(1, Ordering::Relaxed);
        cowshed_core::metadata::WorkspaceIncarnation::new(format!("{value:032x}"))
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }
}

struct AttachmentCleanup<'a> {
    host: &'a MacOsApfsExecutionHost<SystemCommandRunner>,
    armed: bool,
}

impl AttachmentCleanup<'_> {
    fn finish(mut self) -> Result<(), ApfsStorageError> {
        let result = self.host.detach_all_reverse();
        if result.is_ok() {
            self.armed = false;
        }
        result
    }
}

impl Drop for AttachmentCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.host.detach_all_reverse();
        }
    }
}

struct ChurnGuard {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Result<(), std::io::Error>>>,
}

impl ChurnGuard {
    fn finish(mut self) -> Result<(), Box<dyn Error>> {
        self.stop.store(true, Ordering::Release);
        let result = self
            .handle
            .take()
            .expect("churn handle is present")
            .join()
            .map_err(|_| "writer churn thread panicked")?;
        result?;
        Ok(())
    }
}

impl Drop for ChurnGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// One `#[test]` per image format, deliberately not a loop over both.
///
/// Each format drives a complete, independent substrate lifecycle — adopt, mount, a stream
/// write, clone under writer churn, checkpoint, restore, stats, defragment (a
/// busy refusal, then the rewrite of main's image fragmented under a clone), retire, reclaim, GC,
/// detach — and each is bounded by the harness's PER-TEST deadline. The defragment phase lives
/// here, not in a test of its own, because its own test pays adopt's attaches again: one did, and
/// under host contention stalled past the 30s default inside adopt. Running both inside one test
/// spent 27.4s of a 30s budget on an idle host (Sparse 14.8s + Asif 10.6s), leaving 8.6% headroom,
/// so ordinary host variance read as a test failure. Split, each scenario answers for its own wall
/// time against the whole budget, and a format that regresses names itself instead of being one
/// of two suspects behind a single timeout.
///
/// The data work is sized to what the assertions need, not to the image: on a hosted CI runner
/// the disk is the slow part. There the Asif lifecycle's APFS phases took 20s of a run that passed
/// 60s, the rest spent writing a 128 MiB stream and moving every other page of the whole image.
/// The stream is [`STREAM_MEBIBYTES`] MiB, and [`FRAGMENTED_PAGES`] pages move.
///
/// These two share the host's APFS driver and Disk Arbitration, so they are serialized against
/// each other by the `real-apfs` nextest test group rather than by living in one test body.
/// Neither format is optional: a missing capability is a failure, never a skip.
#[test]
fn real_apfs_sparse_substrate_lifecycle() {
    run_lifecycle(ImageFormat::Sparse);
}

#[test]
fn real_apfs_asif_substrate_lifecycle() {
    run_lifecycle(ImageFormat::Asif);
}

/// The stream written before the clone: data the clone shares and the defragment rewrite moves.
const STREAM_MEBIBYTES: usize = 32;

/// Pages [`fragment_under_clone`] moves. Each moved page, and each page left between two moved
/// ones, is an extent, so this leaves about twice the 1000 extents the lifecycle asserts.
const FRAGMENTED_PAGES: u64 = 1_200;

fn run_lifecycle(format: ImageFormat) {
    match run_format(format) {
        Ok(evidence) => eprintln!("APFS {format:?}: {evidence}"),
        Err(error) => panic!("required APFS {format:?} capability failed: {error}"),
    }
}

fn run_format(format: ImageFormat) -> Result<String, Box<dyn Error>> {
    let root = IntegrationRoot::new(format)?;
    let store = root.path.join("store");
    let caches = store.join("caches");
    let checkout_path = root.path.join("main-mount");
    fs::create_dir_all(&store)?;
    fs::create_dir_all(&caches)?;
    fs::create_dir_all(&checkout_path)?;
    let config = ApfsSubstrateConfig::new(
        &store,
        &caches,
        &checkout_path,
        CheckoutLayout::Symlink,
        ApfsCaseSensitivity::Insensitive,
    )
    .with_capacity(ImageCapacity::from_gibibytes(1));
    let identity = || -> Result<OperationIdentity, Box<dyn Error>> {
        Ok(OperationIdentity {
            project_root: checkout_path.clone(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            branch: Some("main".to_owned()),
            forked_from: None,
            created_trace: format!("apfs-integration-{}", format.extension()),
            git_worktree: false,
            grants: GrantSet::closed_baseline(Some(PortBlock::new(
                MACOS_PORT_MIN,
                NEW_PORT_BLOCK_SIZE,
            )?))?,
        })
    };
    let host = MacOsApfsExecutionHost::new(SystemCommandRunner, config.clone())?;
    let substrate = ApfsSubstrate::with_lane_and_incarnations(
        config,
        host,
        TokioApfsBlockingLane,
        DeterministicIncarnations(AtomicU64::new(0)),
    );
    let cleanup = AttachmentCleanup {
        host: substrate.host(),
        armed: true,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let started = Instant::now();
    let result: Result<String, Box<dyn Error>> = runtime.block_on(async {
        // Format-distinct, matching the per-format `IntegrationRoot`: the two lifecycle tests
        // share a pid, so a pid-only identity would give both the same StorageLayout keys and
        // the same (human-facing) volume label, leaving per-format evidence ambiguous to read.
        let repo = RepoId::parse(&format!(
            "cowshed/itest-{}-{}",
            std::process::id(),
            format.extension()
        ))?;
        let adopt = substrate.plan_adopt(AdoptRequest {
            repo: repo.clone(),
            format,
            capacity: ImageCapacity::from_gibibytes(1),
            topology_revision: Revision::new(0),
            source_checkout: checkout_path.clone(),
            pre_cowshed_checkout: PathBuf::from(format!("{}.pre-cowshed", checkout_path.display())),
            identity: identity()?,
        })?;
        let main = substrate
            .execute_adopt_staged(adopt, |_| async { Ok::<(), std::io::Error>(()) })
            .await
            .map_err(|error| std::io::Error::other(format!("adopt: {error}")))?
            .workspace;
        if main.format() != format {
            return Err(std::io::Error::other(format!(
                "requested {format:?}, native capability selected {:?}",
                main.format()
            ))
            .into());
        }
        // Main mounts under the store's mount root like every other workspace, and the adopted
        // checkout path is a symlink into it.
        let canonical_mount =
            StorageLayout::new(&store, &repo)?.workspace_mount(&WorkspaceName::new("main")?)?;
        assert_eq!(
            substrate
                .ensure_mounted(&main, MountIntent { browse: false })
                .await
                .map_err(|error| std::io::Error::other(format!("ensure main: {error}")))?,
            canonical_mount
        );
        assert_eq!(fs::read_link(&checkout_path)?, canonical_mount);
        assert_eq!(
            fs::canonicalize(&checkout_path)?,
            fs::canonicalize(&canonical_mount)?,
            "the familiar path resolves to the canonical mount"
        );
        assert!(
            PathBuf::from(format!("{}.pre-cowshed", checkout_path.display())).is_dir(),
            "the original tree is retained beside the checkout"
        );
        // Everything below reaches the workspace through the symlink, exactly as the user does.
        let mounted_root = fs::metadata(&checkout_path)?;
        assert_eq!(mounted_root.uid(), unsafe { libc::getuid() });
        assert_eq!(mounted_root.gid(), unsafe { libc::getgid() });
        // The label adoption gave main's volume, read from the kernel rather than from Disk
        // Arbitration's cache.
        let main_label = volume_label(&repo, &WorkspaceName::new("main")?);
        assert_eq!(volume_name(&canonical_mount)?, main_label.as_str());

        let payload = checkout_path.join("payload.txt");
        fs::write(&payload, b"checkpoint baseline\n")?;
        let stream = checkout_path.join("stream.bin");
        write_stream(&stream, STREAM_MEBIBYTES)?;
        let churn_stop = Arc::new(AtomicBool::new(false));
        let churn = ChurnGuard {
            handle: Some(spawn_churn(
                checkout_path.join("churn.bin"),
                Arc::clone(&churn_stop),
            )),
            stop: churn_stop,
        };
        let destination = WorkspaceName::session("clone-under-write")?;
        let fork_plan = substrate.plan_create(
            &main,
            Destination {
                repo: repo.clone(),
                name: destination,
                topology_revision: Revision::new(1),
                identity: identity()?,
            },
        )?;
        let fork_started = Instant::now();
        let fork = substrate
            .execute_create_staged(fork_plan, |_| async { Ok::<(), &'static str>(()) })
            .await
            .map_err(|error| std::io::Error::other(format!("live clone: {error}")))?
            .workspace;
        let fork_elapsed = fork_started.elapsed();
        churn.finish()?;
        // Defends CoW-cheap fork lifecycle, not a benchmark: ~1s idle, but
        // 5s and 10s budgets both flaked under real host contention (5.04s
        // at load ~20, 12.8s at load ~59). The bound only needs to catch
        // pathology — a copy-instead-of-clone or sync stall regression sits
        // far beyond 30s — so it is deliberately generous.
        assert!(
            fork_elapsed < Duration::from_secs(30),
            "fork lifecycle exceeded 30 seconds: {fork_elapsed:?}"
        );
        let fork_mount = substrate
            .ensure_mounted(&fork, MountIntent { browse: false })
            .await
            .map_err(|error| std::io::Error::other(format!("ensure fork: {error}")))?;
        assert_eq!(
            fs::read(fork_mount.join("payload.txt"))?,
            b"checkpoint baseline\n"
        );
        assert_eq!(
            fs::metadata(fork_mount.join("stream.bin"))?.len(),
            u64::try_from(STREAM_MEBIBYTES * 1024 * 1024)?
        );
        // Provisioning leaves the label the clone inherited: relabelling is a Disk Arbitration
        // round trip the workspace's supervisor makes later, off the path that creates it.
        assert_eq!(volume_name(&fork_mount)?, main_label.as_str());

        let checkpoint_plan = substrate.plan_checkpoint(
            &main,
            CheckpointLabel::new("before-mutation")?,
            Pin::Pinned,
        )?;
        let checkpoint = substrate
            .execute_checkpoint_staged(checkpoint_plan, |_| async { Ok::<(), &'static str>(()) })
            .await
            .map_err(|error| std::io::Error::other(format!("checkpoint: {error}")))?;
        fs::write(&payload, b"mutated after checkpoint\n")?;
        let restore_plan =
            substrate.plan_restore(&main, &checkpoint, RestoreMode::Replace, identity()?)?;
        let restored = substrate
            .execute_restore_staged(
                restore_plan,
                |_| async { Ok::<(), &'static str>(()) },
                |_| async { Ok::<(), &'static str>(()) },
            )
            .await
            .map_err(|error| std::io::Error::other(format!("restore: {error}")))?
            .workspace;
        assert_ne!(restored.incarnation(), main.incarnation());
        assert_eq!(fs::read(&payload)?, b"checkpoint baseline\n");

        let stats = substrate
            .stats(&restored)
            .await
            .map_err(|error| std::io::Error::other(format!("stats: {error}")))?;
        assert!(stats.logical_bytes > 0);
        assert!(stats.allocated_bytes > 0);
        assert!(stats.allocated_bytes <= stats.logical_bytes);
        assert!(
            stats.checkpoint_count >= 2,
            "source + pre-restore undo checkpoints"
        );

        // Defragment: main fragments by taking writes while a clone shares its blocks. Rewrite
        // every other page of the detached image under a clone, then the verb, busy and idle.
        // Lifecycle verbs take the workspace as the store derives it now, revision included.
        let defragment_phase = Instant::now();
        let restored = substrate
            .list(&repo)
            .await
            .map_err(|error| std::io::Error::other(format!("list: {error}")))?
            .into_iter()
            .map(|derived| derived.workspace)
            .find(|workspace| workspace.name().is_main())
            .ok_or("restored main is not listed")?;
        substrate
            .unmount(&restored)
            .await
            .map_err(|error| std::io::Error::other(format!("unmount main: {error}")))?;
        let image = StorageLayout::new(&store, &repo)?
            .main_image(format)?
            .image()
            .to_owned();
        let rewritten_pages = fragment_under_clone(&image)?;
        let fragmented = count_extents(&image)?;
        assert!(
            fragmented.get() >= 1_000,
            "rewriting {rewritten_pages} pages under a clone left only {fragmented} extents"
        );
        substrate
            .ensure_mounted(&restored, MountIntent { browse: false })
            .await
            .map_err(|error| std::io::Error::other(format!("remount main: {error}")))?;
        // An open file is work in flight: the rewrite refuses before it touches the image.
        let held = File::open(&payload)?;
        let inode = fs::metadata(&image)?.ino();
        let refused = substrate
            .defragment(&restored)
            .await
            .expect_err("a busy main refuses the rewrite");
        assert!(
            matches!(refused, ApfsStorageError::Apfs(_))
                && refused.to_string().contains("detach image failed"),
            "unexpected refusal: {refused}"
        );
        assert_eq!(
            fs::metadata(&image)?.ino(),
            inode,
            "a refused rewrite keeps the image"
        );
        assert!(!fs::exists(rewrite_sibling(&image))?);
        drop(held);
        let defragment_started = Instant::now();
        let defragmented = substrate
            .defragment(&restored)
            .await
            .map_err(|error| std::io::Error::other(format!("defragment: {error}")))?;
        let defragment_elapsed = defragment_started.elapsed();
        assert!(
            defragmented.previous >= fragmented
                && defragmented.extents.get().saturating_mul(10) <= defragmented.previous.get(),
            "the rewrite lays the data out in at most a tenth of the extents: {defragmented:?}"
        );
        assert_ne!(fs::metadata(&image)?.ino(), inode, "the image is a new file");
        assert!(!fs::exists(rewrite_sibling(&image))?);
        assert!(matches!(
            substrate
                .mount_state(&restored)
                .await
                .map_err(|error| std::io::Error::other(format!("mount state: {error}")))?,
            MountState::Mounted { .. }
        ));
        assert_eq!(fs::read(&payload)?, b"checkpoint baseline\n");
        let streamed = fs::read(&stream)?;
        assert_eq!(streamed.len(), STREAM_MEBIBYTES * 1024 * 1024);
        assert!(streamed.iter().all(|byte| *byte == 0x5a));
        let marker = cowshed_core::metadata::WorkspaceMarker::read_from(
            &checkout_path.join(WORKSPACE_MARKER_PATH),
        )?;
        assert_eq!(&marker.workspace_incarnation, restored.incarnation());
        let defragment_phase = defragment_phase.elapsed();

        let retire = substrate.plan_retire(&fork)?;
        let retired = substrate
            .execute_retire(retire)
            .await
            .map_err(|error| std::io::Error::other(format!("retire: {error}")))?;
        substrate
            .reclaim(retired)
            .await
            .map_err(|error| std::io::Error::other(format!("reclaim: {error}")))?;
        let gc_plan = substrate
            .preview_gc(&repo)
            .await
            .map_err(|error| std::io::Error::other(format!("preview gc: {error}")))?;
        substrate
            .execute_gc(gc_plan)
            .await
            .map_err(|error| std::io::Error::other(format!("execute gc: {error}")))?;

        Ok(format!(
            "lifecycle={:?}, fork={fork_elapsed:?}, logical={}, allocated={}, checkpoints={}, defragment phase={defragment_phase:?} verb={defragment_elapsed:?} ({rewritten_pages} pages rewritten under a clone, extents {} -> {})",
            started.elapsed(),
            stats.logical_bytes,
            stats.allocated_bytes,
            stats.checkpoint_count,
            defragmented.previous,
            defragmented.extents
        ))
    });

    let teardown = cleanup.finish();
    match (result, teardown) {
        (Ok(evidence), Ok(())) => Ok(evidence),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(Box::new(error)),
        (Err(primary), Err(cleanup)) => Err(format!(
            "lifecycle failed: {primary}; reverse-order teardown failed: {cleanup}"
        )
        .into()),
    }
}

fn write_stream(path: &Path, mebibytes: usize) -> Result<(), Box<dyn Error>> {
    let block = vec![0x5a; 1024 * 1024];
    let mut file = File::create(path)?;
    for _ in 0..mebibytes {
        file.write_all(&block)?;
    }
    file.sync_all()?;
    Ok(())
}

fn spawn_churn(
    path: PathBuf,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Result<(), std::io::Error>> {
    std::thread::spawn(move || {
        let block = vec![0xa5; 1024 * 1024];
        let mut file = File::create(path)?;
        let mut offset = 0_u64;
        while !stop.load(Ordering::Acquire) {
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&block)?;
            file.sync_data()?;
            offset = (offset + block.len() as u64) % (16 * 1024 * 1024);
        }
        Ok(())
    })
}

/// Fragment `image` the way a main fragments: rewrite every other 16 KiB page from the start,
/// [`FRAGMENTED_PAGES`] of them, contents unchanged, while a clone shares its blocks, so each
/// rewritten page moves and splits the map. 16 KiB is the Apple silicon VM page; a smaller write
/// would dirty, and move, a whole page anyway.
fn fragment_under_clone(image: &Path) -> Result<u64, Box<dyn Error>> {
    use std::os::unix::fs::FileExt;

    let holder = image.with_file_name("fragment-holder");
    let cloned = std::process::Command::new("/bin/cp")
        .arg("-c")
        .arg(image)
        .arg(&holder)
        .status()?;
    if !cloned.success() {
        return Err(format!("cp -c {} failed: {cloned}", image.display()).into());
    }
    let file = fs::OpenOptions::new().read(true).write(true).open(image)?;
    let length = file.metadata()?.len();
    let mut page = vec![0_u8; 16 * 1024];
    let page_bytes = u64::try_from(page.len())?;
    let mut rewritten = 0;
    let mut offset = 0;
    while rewritten < FRAGMENTED_PAGES && offset + page_bytes <= length {
        file.read_exact_at(&mut page, offset)?;
        file.write_all_at(&page, offset)?;
        rewritten += 1;
        offset += 2 * page_bytes;
    }
    file.sync_all()?;
    fs::remove_file(&holder)?;
    Ok(rewritten)
}
