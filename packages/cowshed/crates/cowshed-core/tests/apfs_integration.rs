#![cfg(target_os = "macos")]

use std::error::Error;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cowshed_core::apfs::{ApfsCaseSensitivity, SystemCommandRunner};
use cowshed_core::metadata::{GrantSet, ImageFormat, PortBlock, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::CheckpointLabel;
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::apfs::{
    ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig, IncarnationSource, TokioApfsBlockingLane,
};
use cowshed_core::storage::lifecycle::{
    AdoptRequest, Destination, LifecyclePlanner, MountIntent, OperationIdentity, Pin, RestoreMode,
    Revision, Substrate,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequiredFormats {
    Auto,
    Asif,
    Sparse,
    Both,
}

impl RequiredFormats {
    fn from_env() -> Result<Self, String> {
        match std::env::var("COWSHED_APFS_REQUIRED")
            .unwrap_or_else(|_| "auto".to_owned())
            .as_str()
        {
            "auto" => Ok(Self::Auto),
            "asif" => Ok(Self::Asif),
            "sparse" => Ok(Self::Sparse),
            "both" => Ok(Self::Both),
            value => Err(format!(
                "COWSHED_APFS_REQUIRED must be auto|asif|sparse|both, got {value}"
            )),
        }
    }

    fn requires(self, format: ImageFormat) -> bool {
        matches!(
            (self, format),
            (Self::Asif, ImageFormat::Asif) | (Self::Sparse, ImageFormat::Sparse) | (Self::Both, _)
        )
    }

    fn formats(self) -> &'static [ImageFormat] {
        match self {
            Self::Asif => &[ImageFormat::Asif],
            Self::Sparse => &[ImageFormat::Sparse],
            Self::Auto | Self::Both => &[ImageFormat::Sparse, ImageFormat::Asif],
        }
    }
}

struct IntegrationRoot {
    path: PathBuf,
}

impl IntegrationRoot {
    fn new(format: ImageFormat) -> Result<Self, Box<dyn Error>> {
        let path = PathBuf::from(format!(
            "/private/tmp/cowshed-itest-{}-{}",
            std::process::id(),
            format.extension()
        ));
        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for IntegrationRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
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
        self.armed = false;
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

#[test]
#[ignore = "explicit real APFS target: nx integration-apfs cowshed"]
fn real_apfs_substrate_lifecycle() {
    assert_eq!(
        std::env::var("COWSHED_INTEGRATION").as_deref(),
        Ok("1"),
        "invoke the explicit integration-apfs Nx target"
    );
    let required = RequiredFormats::from_env().expect("capability selection");
    let mut completed = Vec::new();
    for &format in required.formats() {
        match run_format(format) {
            Ok(evidence) => {
                eprintln!("APFS {format:?}: {evidence}");
                completed.push(format);
            }
            Err(error) if !required.requires(format) => {
                eprintln!("APFS {format:?} unavailable in auto mode: {error}");
            }
            Err(error) => panic!("required APFS {format:?} capability failed: {error}"),
        }
    }
    assert!(
        !completed.is_empty(),
        "auto mode requires at least one working APFS image format"
    );
    if required == RequiredFormats::Both {
        assert_eq!(completed.len(), 2, "both selected formats must complete");
    }
}

fn run_format(format: ImageFormat) -> Result<String, Box<dyn Error>> {
    let root = IntegrationRoot::new(format)?;
    let store = root.path.join("store");
    let caches = store.join("caches");
    let main_mount = root.path.join("main-mount");
    fs::create_dir_all(&store)?;
    fs::create_dir_all(&caches)?;
    fs::create_dir_all(&main_mount)?;
    let config = ApfsSubstrateConfig::new(
        &store,
        &caches,
        &main_mount,
        ApfsCaseSensitivity::Insensitive,
    )
    .with_capacity("1g")?;
    let identity = || -> Result<OperationIdentity, Box<dyn Error>> {
        Ok(OperationIdentity {
            project_root: main_mount.clone(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            branch: Some("main".to_owned()),
            forked_from: None,
            created_trace: format!("apfs-integration-{}", format.extension()),
            grants: GrantSet::closed_baseline(Some(PortBlock::new(30000, 16)?))?,
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
        let repo = RepoId::parse(&format!("cowshed/itest-{}", std::process::id()))?;
        let adopt = substrate.plan_adopt(AdoptRequest {
            repo: repo.clone(),
            format,
            topology_revision: Revision::new(0),
            source_checkout: main_mount.clone(),
            pre_cowshed_checkout: PathBuf::from(format!("{}.pre-cowshed", main_mount.display())),
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
        assert_eq!(
            substrate
                .ensure_mounted(&main, MountIntent { browse: false })
                .await
                .map_err(|error| std::io::Error::other(format!("ensure main: {error}")))?,
            main_mount
        );
        let mounted_root = fs::metadata(&main_mount)?;
        assert_eq!(mounted_root.uid(), unsafe { libc::getuid() });
        assert_eq!(mounted_root.gid(), unsafe { libc::getgid() });

        let payload = main_mount.join("payload.txt");
        fs::write(&payload, b"checkpoint baseline\n")?;
        let stream = main_mount.join("stream.bin");
        write_stream(&stream, 128)?;
        let churn_stop = Arc::new(AtomicBool::new(false));
        let churn = ChurnGuard {
            handle: Some(spawn_churn(
                main_mount.join("churn.bin"),
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
        assert!(
            fork_elapsed < Duration::from_secs(5),
            "fork lifecycle exceeded 5 seconds: {fork_elapsed:?}"
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
            128 * 1024 * 1024
        );

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
            "lifecycle={:?}, fork={fork_elapsed:?}, logical={}, allocated={}, checkpoints={}",
            started.elapsed(),
            stats.logical_bytes,
            stats.allocated_bytes,
            stats.checkpoint_count
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
