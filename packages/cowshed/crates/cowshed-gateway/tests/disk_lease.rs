#![cfg(target_os = "macos")]
//! The disk-lifecycle lease over a real control socket, asked by the client every disk command
//! runs inside (`cowshed_core::disk_lease`).

use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cowshed_core::apfs::{
    ApfsBackend, AttachedImage, CreateImageRequest, DetachIntent, MacOsApfsBackend,
    SystemCommandRunner,
};
use cowshed_core::disk_lease::leased_at;
use cowshed_core::fork_lock::Run as _;
use cowshed_core::metadata::{IMAGE_EXTENSION, ImageCapacity};
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, DiskLeaseLimits, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_gateway_types::DiskClass;
use uuid::Uuid;

struct NoCredentials;

#[async_trait]
impl CredentialProvider for NoCredentials {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(None)
    }
}

struct NoConnector;

#[async_trait]
impl UpstreamConnector for NoConnector {
    async fn health(&self, _target: &cowshed_gateway::CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Unknown
    }

    async fn connect(
        &self,
        _target: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        Err(ConnectError::NoAddresses)
    }
}

struct DiscardAudit;

#[async_trait]
impl AuditSink for DiscardAudit {
    async fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Ok(())
    }
}

/// A gateway serving leases with `limits` on a socket under `/tmp` (a bind path is capped at
/// `sun_path`'s 104 bytes), and the root to remove once it has drained.
async fn gateway(limits: DiskLeaseLimits) -> (Gateway, PathBuf, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let root = PathBuf::from(format!("/tmp/csdl-{}", &id[..8]));
    std::fs::create_dir(&root).expect("create fixture root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure fixture root");
    let cache = root.join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            disk_lease: limits,
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");
    (gateway, root, socket)
}

/// A client thread that takes a `class` lease on `socket`, says when its command starts, and
/// holds the lease until told to finish.
fn holder(socket: &Path, class: DiskClass, label: &'static str) -> (Receiver<()>, Sender<()>) {
    let (entered, entered_rx) = channel();
    let (finish, finish_rx) = channel::<()>();
    let socket = socket.to_path_buf();
    std::thread::spawn(move || {
        leased_at(&socket, class, label, || {
            let _ = entered.send(());
            let _ = finish_rx.recv();
            Ok::<(), String>(())
        })
    });
    (entered_rx, finish)
}

/// Whether `signal` fires within `within`, waited off the runtime the gateway serves on.
async fn fires(signal: Receiver<()>, within: Duration) -> (Receiver<()>, bool) {
    tokio::task::spawn_blocking(move || {
        let fired = signal.recv_timeout(within).is_ok();
        (signal, fired)
    })
    .await
    .expect("wait")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_class_shares_its_phase_and_the_other_runs_only_once_it_drains() {
    let (gateway, root, socket) = gateway(DiskLeaseLimits {
        per_class: NonZeroUsize::new(2).unwrap(),
        phase_bound: Duration::from_secs(60),
    })
    .await;

    let (first, finish_first) = holder(&socket, DiskClass::Storage, "/usr/sbin/diskutil a");
    let (_, entered) = fires(first, Duration::from_secs(10)).await;
    assert!(entered, "an idle lease grants at once");
    let (second, finish_second) = holder(&socket, DiskClass::Storage, "/usr/sbin/diskutil b");
    let (_, entered) = fires(second, Duration::from_secs(10)).await;
    assert!(entered, "a second storage command shares the phase");

    let (mount, finish_mount) = holder(&socket, DiskClass::Namespace, "/sbin/mount_apfs x");
    let (mount, entered) = fires(mount, Duration::from_millis(500)).await;
    assert!(!entered, "a mount never runs beside storage commands");
    let (third, finish_third) = holder(&socket, DiskClass::Storage, "/usr/sbin/diskutil c");
    let (third, entered) = fires(third, Duration::from_millis(500)).await;
    assert!(
        !entered,
        "room in the phase, but namespace waits: storage admits nobody new"
    );

    finish_first.send(()).unwrap();
    let (mount, entered) = fires(mount, Duration::from_millis(500)).await;
    assert!(!entered, "storage has not drained");
    finish_second.send(()).unwrap();
    let (_, entered) = fires(mount, Duration::from_secs(10)).await;
    assert!(entered, "the drained phase goes to namespace");
    let (third, entered) = fires(third, Duration::from_millis(300)).await;
    assert!(!entered, "storage waits for the namespace phase");
    finish_mount.send(()).unwrap();
    let (_, entered) = fires(third, Duration::from_secs(10)).await;
    assert!(entered, "and gets the phase back");
    finish_third.send(()).unwrap();

    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_stuck_past_the_bound_stops_holding_the_other_class_back() {
    let bound = Duration::from_millis(300);
    let (gateway, root, socket) = gateway(DiskLeaseLimits {
        per_class: NonZeroUsize::new(8).unwrap(),
        phase_bound: bound,
    })
    .await;

    let (stuck, _never_finished) = holder(&socket, DiskClass::Namespace, "/sbin/umount /hung");
    let (_, entered) = fires(stuck, Duration::from_secs(10)).await;
    assert!(entered);
    let asked = Instant::now();
    let (attach, finish_attach) = holder(
        &socket,
        DiskClass::Storage,
        "/usr/sbin/diskutil image attach",
    );
    let (_, entered) = fires(attach, Duration::from_secs(10)).await;
    assert!(entered, "the stuck unmount is evicted and storage proceeds");
    assert!(
        asked.elapsed() >= bound - Duration::from_millis(50),
        "not before the bound: {:?}",
        asked.elapsed()
    );
    finish_attach.send(()).unwrap();

    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

/// How the contention regression runs one arm: free for all, or every command under the lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Arm {
    Unleased,
    Leased,
}

/// One disk command, timed, through the lease in the leased arm: how long the command itself
/// took, how long its lease wait added, and what it printed.
fn timed(
    arm: Arm,
    socket: &Path,
    class: DiskClass,
    argv: &[&str],
) -> (Duration, Duration, std::process::Output) {
    let asked = Instant::now();
    let mut ran = Duration::ZERO;
    let mut run = || {
        let started = Instant::now();
        let output = Command::new(argv[0]).args(&argv[1..]).output_locked();
        ran = started.elapsed();
        output.map_err(|error| format!("{}: {error}", argv[0]))
    };
    let output = match arm {
        Arm::Unleased => run(),
        Arm::Leased => leased_at(socket, class, &argv.join(" "), run),
    }
    .expect("run a disk command");
    (ran, asked.elapsed() - ran, output)
}

/// The whole device an `--noMount --plist` attach answered with: its shortest `dev-entry`, which
/// the attach spells without `/dev/`.
fn whole_device(plist: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(plist);
    text.split("<string>")
        .filter_map(|piece| piece.split('<').next())
        .filter(|entry| entry.starts_with("disk"))
        .min_by_key(|entry| entry.len())
        .map(|entry| format!("/dev/{entry}"))
}

/// The `fraction` percentile of `sorted`, nearest rank; `Duration::MAX` when nothing finished.
fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::MAX;
    }
    let rank = (sorted.len() as f64 * fraction).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// What the regression measured in one arm.
struct Measured {
    /// Each attach's own run time, and its lease wait plus run time.
    attaches: Vec<(Duration, Duration)>,
    mount_cycles: usize,
    wall: Duration,
    /// Why a loop stopped early. A loop never panics: one that did would leave its image attached.
    failures: Vec<String>,
}

/// One attach/detach loop until `stop`, or until a command fails, which stops every loop.
fn attach_loop(
    arm: Arm,
    socket: &Path,
    image: &str,
    stop: &AtomicBool,
) -> Result<Vec<(Duration, Duration)>, String> {
    let mut attaches = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let (ran, waited, output) = timed(
            arm,
            socket,
            DiskClass::Storage,
            &[
                "/usr/sbin/diskutil",
                "image",
                "attach",
                "--nobrowse",
                "--noMount",
                "--plist",
                image,
            ],
        );
        if !output.status.success() {
            return Err(format!("attach {image}: {output:?}"));
        }
        attaches.push((ran, ran + waited));
        let device = whole_device(&output.stdout)
            .ok_or_else(|| format!("attach {image} named no device: {output:?}"))?;
        let (_, _, detached) = timed(
            arm,
            socket,
            DiskClass::Storage,
            &["/usr/bin/hdiutil", "detach", "-force", &device],
        );
        if !detached.status.success() {
            return Err(format!("detach {device}: {detached:?}"));
        }
    }
    Ok(attaches)
}

/// One mount/unmount loop on an attached volume until `stop` or a failure.
fn mount_loop(
    arm: Arm,
    socket: &Path,
    volume: &str,
    mount_point: &str,
    stop: &AtomicBool,
) -> Result<usize, String> {
    let mut cycles = 0;
    while !stop.load(Ordering::Relaxed) {
        let (_, _, mounted) = timed(
            arm,
            socket,
            DiskClass::Namespace,
            &["/sbin/mount_apfs", "-o", "nobrowse", volume, mount_point],
        );
        if !mounted.status.success() {
            return Err(format!("mount {volume}: {mounted:?}"));
        }
        let (_, _, unmounted) = timed(
            arm,
            socket,
            DiskClass::Namespace,
            &["/sbin/umount", mount_point],
        );
        if !unmounted.status.success() {
            return Err(format!("unmount {volume}: {unmounted:?}"));
        }
        cycles += 1;
    }
    Ok(cycles)
}

/// One attach/detach loop per image beside one mount/unmount loop per volume, for `period`.
fn contend(
    arm: Arm,
    socket: &Path,
    attach_images: &[PathBuf],
    mounted: &[(String, PathBuf)],
    period: Duration,
) -> Measured {
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let attachers: Vec<_> = attach_images
        .iter()
        .map(|image| {
            let (stop, socket, image) = (
                Arc::clone(&stop),
                socket.to_path_buf(),
                image.to_string_lossy().into_owned(),
            );
            std::thread::spawn(move || {
                let result = attach_loop(arm, &socket, &image, &stop);
                if result.is_err() {
                    stop.store(true, Ordering::Relaxed);
                }
                result
            })
        })
        .collect();
    let mounters: Vec<_> = mounted
        .iter()
        .map(|(volume, mount_point)| {
            let (stop, socket, volume, mount_point) = (
                Arc::clone(&stop),
                socket.to_path_buf(),
                volume.clone(),
                mount_point.to_string_lossy().into_owned(),
            );
            std::thread::spawn(move || {
                let result = mount_loop(arm, &socket, &volume, &mount_point, &stop);
                if result.is_err() {
                    stop.store(true, Ordering::Relaxed);
                }
                result
            })
        })
        .collect();
    let deadline = started + period;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Relaxed);
    let mut measured = Measured {
        attaches: Vec::new(),
        mount_cycles: 0,
        wall: Duration::ZERO,
        failures: Vec::new(),
    };
    for attacher in attachers {
        match attacher.join().expect("an attach loop returns") {
            Ok(attaches) => measured.attaches.extend(attaches),
            Err(failure) => measured.failures.push(failure),
        }
    }
    for mounter in mounters {
        match mounter.join().expect("a mount loop returns") {
            Ok(cycles) => measured.mount_cycles += cycles,
            Err(failure) => measured.failures.push(failure),
        }
    }
    measured.wall = started.elapsed();
    measured
}

fn report(arm: Arm, measured: &Measured) -> (Duration, Duration) {
    let mut ran: Vec<Duration> = measured.attaches.iter().map(|(ran, _)| *ran).collect();
    let mut total: Vec<Duration> = measured.attaches.iter().map(|(_, total)| *total).collect();
    ran.sort_unstable();
    total.sort_unstable();
    let mut load = [0.0_f64; 3];
    // SAFETY: `getloadavg` writes at most the three samples the buffer holds.
    unsafe { libc::getloadavg(load.as_mut_ptr(), 3) };
    let p95 = percentile(&ran, 0.95);
    eprintln!(
        "disk-lease contention {arm:?}: load {:.0}, {} attaches in {:.1?} ({:.2}/s), attach p50 \
         {:.2?} p95 {p95:.2?}, with its lease wait p50 {:.2?} p95 {:.2?}, {} mount cycles",
        load[0],
        ran.len(),
        measured.wall,
        ran.len() as f64 / measured.wall.as_secs_f64(),
        percentile(&ran, 0.5),
        percentile(&total, 0.5),
        percentile(&total, 0.95),
        measured.mount_cycles,
    );
    (p95, percentile(&total, 0.95))
}

/// The regression for the lease itself: eight attach/detach loops beside two mount/unmount loops
/// on real images, first through a real gateway's lease and then free for all. Leased, the two
/// never overlap and an attach stays under two seconds at p95; free, mount churn starves
/// `storagekitd`'s `syncAllDisks` and an attach takes tens of seconds. The leased arm goes first
/// because the free arm leaves `storagekitd` a backlog the next minute's attaches inherit. It
/// loads the host's disk services on purpose, so it runs only when asked:
/// `cargo test -p cowshed-gateway --test disk_lease -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "loads the host's disk services for a minute on purpose; run it by name"]
async fn contention_attaches_stay_fast_beside_mount_churn_only_under_the_lease() {
    const ATTACHERS: usize = 8;
    const MOUNTERS: usize = 2;
    const PERIOD: Duration = Duration::from_secs(40);
    let (gateway, root, socket) = gateway(DiskLeaseLimits::default()).await;
    let images = root.join("images");
    std::fs::create_dir(&images).expect("image directory");
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    let source = backend
        .create_staged_image(&CreateImageRequest {
            staged_stem: images.join("source"),
            capacity: ImageCapacity::from_gibibytes(1),
            volume_name: "cowshed.contention".to_owned(),
            // SAFETY: `getuid`/`getgid` read this process's credentials and cannot fail.
            owner_uid: unsafe { libc::getuid() },
            owner_gid: unsafe { libc::getgid() },
        })
        .expect("a formatted image");
    let clone = |name: String| {
        let image = images.join(name).with_extension(IMAGE_EXTENSION);
        backend.clone_image(&source, &image).expect("clone");
        image
    };
    let attach_images: Vec<PathBuf> = (0..ATTACHERS).map(|i| clone(format!("a{i}"))).collect();
    let attached: Vec<AttachedImage> = (0..MOUNTERS)
        .map(|i| {
            backend
                .attach_verified(&clone(format!("m{i}")))
                .expect("attach a mount-loop image")
        })
        .collect();
    let mounted: Vec<(String, PathBuf)> = attached
        .iter()
        .enumerate()
        .map(|(i, attachment)| {
            let mount_point = root.join(format!("mnt{i}"));
            std::fs::create_dir(&mount_point).expect("mount point");
            (attachment.volume_device().to_owned(), mount_point)
        })
        .collect();

    let socket_for = socket.clone();
    let (unleased, leased) = tokio::task::spawn_blocking(move || {
        let leased = contend(Arm::Leased, &socket_for, &attach_images, &mounted, PERIOD);
        let unleased = contend(Arm::Unleased, &socket_for, &attach_images, &mounted, PERIOD);
        (unleased, leased)
    })
    .await
    .expect("both arms");
    for attachment in &attached {
        backend
            .detach(attachment, DetachIntent::Release)
            .expect("detach a mount-loop image");
    }
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(&root);

    let (unleased_p95, _) = report(Arm::Unleased, &unleased);
    let (leased_p95, leased_total_p95) = report(Arm::Leased, &leased);
    assert!(
        unleased.failures.is_empty() && leased.failures.is_empty(),
        "a contending loop failed: {:?} {:?}",
        unleased.failures,
        leased.failures
    );
    assert!(
        leased_p95 < Duration::from_secs(2),
        "a leased attach's p95 is {leased_p95:?} (with its wait {leased_total_p95:?}); free for \
         all it was {unleased_p95:?}"
    );
}
