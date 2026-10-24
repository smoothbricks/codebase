#![cfg(target_os = "macos")]
//! A RAM disk the gateway attaches belongs to no job (05_gateway.md, "RAM disks").
//!
//! A job asks a fixture gateway for a RAM disk and concludes; the process monitor that owns the
//! job retires everything it retains, and that retirement signals no `diskimages-helper`: the
//! disk stays registered. The same attach forked inside a job is retired with the job, its helper
//! among the members signalled. Every disk here is the fixture's own: a size no other process
//! uses, attached under the fixture gateway's lease on its own socket. The fixture records each
//! disk's registry identity right after its attach and detaches only those identities, each while a
//! fresh registry read still lists it unchanged. The user's gateway is never asked.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cowshed_apfs::disk_lease::leased_at;
use cowshed_apfs::ram_disk::SystemRamDisks;
use cowshed_apfs::{CommandRequest, SystemCommandRunner};
use cowshed_core::api::dto::JobId;
use cowshed_core::api::process::JobProcessRetirement;
use cowshed_core::runtime::process_monitor::ProcessMonitor;
use cowshed_core::runtime::process_tree::TreeOwner;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_gateway_types::{DiskClass, RamDiskSectors};
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

const HELPER: &str = "diskimages-helper";

/// The largest pid macOS issues (`PID_MAX`, `bsd/sys/proc_internal.h`).
const PID_MAX: u64 = 99_999;
/// This file's band of sizes: 512 MiB plus 4 sectors per pid, so no two processes share one.
const FIXTURE_BASE: u64 = 1_048_576;
const FIXTURE_TOP: u64 = FIXTURE_BASE + PID_MAX * 4 + 4;
/// The nx-plugin host proofs' band (`scratchVolume` in
/// `packages/nx-plugin/src/executors/bounded-exec/ram-temp-fixtures.ts`): 64 MiB plus 4 sectors per pid.
const NX_PLUGIN_FIXTURE_TOP: u64 = 131_072 + PID_MAX * 4 + 4;
/// The user's RAM temp volume, in sectors: `RAM_TEMP_CAPACITY_BYTES` (8 GiB plus its revision, 3, in sectors) of
/// `packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts` over 512. Only that TypeScript defines it.
const RAM_TEMP_SECTORS: u64 = 16_777_216 + 3;

/// A size no other process attaches: [`FIXTURE_BASE`] plus sectors this process's pid and `index`
/// pick. Its band lies above the nx-plugin host proofs' and below the RAM temp volume's own size.
fn fixture_sectors(index: u64) -> RamDiskSectors {
    let pid = u64::from(std::process::id());
    assert!(pid <= PID_MAX, "pid {pid} is past macOS's PID_MAX");
    assert!(index < 4, "index {index} would reach another pid's sizes");
    let sectors = FIXTURE_BASE + pid * 4 + index + 1;
    assert!(
        NX_PLUGIN_FIXTURE_TOP < FIXTURE_BASE
            && sectors <= FIXTURE_TOP
            && FIXTURE_TOP < RAM_TEMP_SECTORS,
        "fixture size {sectors} must lie in ({FIXTURE_BASE}, {FIXTURE_TOP}], above the nx-plugin band \
         (top {NX_PLUGIN_FIXTURE_TOP}) and below the RAM temp volume's {RAM_TEMP_SECTORS} sectors"
    );
    RamDiskSectors::new(NonZeroU64::new(sectors).expect("a positive size"))
}

/// A gateway serving `ram-attach` and the disk lease on a socket of its own, its RAM disks
/// attached under its own lease, and the guard that removes its root on every exit path.
async fn gateway() -> (Gateway, fixture_dir::FixtureDir, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("csrd-{}", &id[..8]));
    let cache = root.path().join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.path().join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            ram_disks: Some(Arc::new(SystemRamDisks::new(socket.clone()))),
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

/// One RAM disk as the I/O Registry lists it: the kernel's record, which leaves out no attached
/// disk. `device` is absent while the disk has published no media yet.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RegisteredRamDisk {
    entry: String,
    device: Option<String>,
    sectors: u64,
    uid: u32,
}

/// Every RAM disk in the I/O Registry. A read only; it takes no lease.
fn ram_disks() -> Vec<RegisteredRamDisk> {
    let output = Command::new("/usr/sbin/ioreg")
        .args(["-r", "-c", "IOHDIXHDDriveOutKernel", "-l", "-w0"])
        .output()
        .expect("read the I/O Registry");
    assert!(output.status.success(), "ioreg: {output:?}");
    let registry = String::from_utf8(output.stdout).expect("ioreg prints UTF-8");
    let after = |entry: &str, key: &str, end: char| -> Option<String> {
        entry
            .split_once(key)
            .and_then(|(_, rest)| rest.split_once(end).map(|(value, _)| value.to_owned()))
    };
    registry
        // The class name prefixes its user client's, whose line precedes the entry's media: split
        // on the drive's own class line so the BSD name below that client stays with its entry.
        .split("<class IOHDIXHDDriveOutKernel,")
        .skip(1)
        .filter_map(|entry| {
            let sectors = after(entry, "\"image-path\" = <\"ram://", '"')?
                .parse()
                .expect("a RAM disk's image path names its sectors");
            Some(RegisteredRamDisk {
                entry: after(entry, " id ", ',').expect("an entry names its registry id"),
                device: after(entry, "\"BSD Name\" = \"", '"').map(|disk| format!("/dev/{disk}")),
                sectors,
                uid: after(entry, "\"owner-uid\" = ", '\n')
                    .and_then(|uid| uid.trim().parse().ok())
                    .expect("a RAM disk names its owner"),
            })
        })
        .collect()
}

fn euid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

/// A disk this fixture attached, by the identity the registry gave it right after the attach.
#[derive(Clone, Debug)]
struct OwnedDisk {
    entry: String,
    device: String,
    sectors: RamDiskSectors,
}

/// The fixture's disks: only those it recorded right after attaching them, detached when it drops,
/// on every way out of the test, before the gateway whose lease the detach takes.
struct FixtureDisks {
    socket: PathBuf,
    owned: Vec<OwnedDisk>,
}

impl FixtureDisks {
    /// Record `device`, the disk of `sectors` an attach of this fixture's just answered, by the one
    /// registry entry that lists it with that size and this user. Any other listing is no ownership
    /// and is never recorded, so never detached.
    fn own(&mut self, device: &str, sectors: RamDiskSectors) -> OwnedDisk {
        let listed: Vec<RegisteredRamDisk> = ram_disks()
            .into_iter()
            .filter(|disk| disk.device.as_deref() == Some(device))
            .collect();
        let owned = match listed.as_slice() {
            [disk] if disk.sectors == sectors.get() && disk.uid == euid() => OwnedDisk {
                entry: disk.entry.clone(),
                device: device.to_owned(),
                sectors,
            },
            other => panic!(
                "the fixture's attach answered {device} of {sectors}, but the registry lists {other:?}: \
                 not recorded, so never detached"
            ),
        };
        self.owned.push(owned.clone());
        owned
    }

    /// Detach each recorded disk while a fresh registry read still lists its entry with the same
    /// whole disk, size and owner. A disk already gone needs nothing; any other listing is refused
    /// and reported, never detached. Answers the disks detached and the refusals.
    fn detach(&mut self) -> (Vec<String>, Vec<String>) {
        let registry = ram_disks();
        let mut detached = Vec::new();
        let mut refused = Vec::new();
        for owned in self.owned.drain(..) {
            let Some(now) = registry.iter().find(|disk| disk.entry == owned.entry) else {
                continue;
            };
            if now.device.as_deref() != Some(owned.device.as_str())
                || now.sectors != owned.sectors.get()
                || now.uid != euid()
            {
                refused.push(format!(
                    "registry entry {} now lists {now:?}, not the {} of {} this fixture attached: not detached",
                    owned.entry, owned.device, owned.sectors
                ));
                continue;
            }
            cowshed_os::eprintln!("fixture exact-owned detach: {owned:?}");
            let request =
                CommandRequest::new("/usr/bin/hdiutil", ["detach", owned.device.as_str()]);
            let command = format!("hdiutil detach {}", owned.device);
            let entry = u64::from_str_radix(
                owned.entry.strip_prefix("0x").expect("hex registry entry"),
                16,
            )
            .expect("registry entry id");
            match SystemCommandRunner.run_detach_of_entry(&request, entry, Some(&self.socket)) {
                Ok(output) if output.status.succeeded() => detached.push(owned.device),
                Ok(output) => refused.push(format!(
                    "{command} ended {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
                Err(error) => refused.push(format!("{command}: {error}")),
            }
        }
        // Said on stderr, outside the fixture's root, before anything removes it.
        for refusal in &refused {
            cowshed_os::eprintln!("fixture teardown: {refusal}");
        }
        (detached, refused)
    }
}

impl Drop for FixtureDisks {
    fn drop(&mut self) {
        self.detach();
    }
}

/// Start `command` as a job's root under `monitor` and wait for it to exit.
fn run_job(
    monitor: &ProcessMonitor,
    mut command: Command,
) -> (
    std::process::Output,
    cowshed_core::runtime::process_tree::ProcessIdentity,
) {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let (child, root) = monitor.spawn_root(command).expect("spawn the job's root");
    let output = child.wait_with_output().expect("wait for the job's root");
    (output, root)
}

/// Retire what the job retains: what a job's conclusion does to the members that outlive its
/// leader.
fn conclude(
    monitor: &ProcessMonitor,
    root: cowshed_core::runtime::process_tree::ProcessIdentity,
) -> JobProcessRetirement {
    monitor
        .retire_root(root, Duration::from_secs(1))
        .expect("retire the job")
}

fn helpers(retired: &JobProcessRetirement) -> Vec<u32> {
    retired
        .outlived
        .iter()
        .filter(|member| {
            member
                .image
                .as_deref()
                .is_some_and(|image| image.ends_with(HELPER))
        })
        .map(|member| member.pid)
        .collect()
}

/// The job's root: ask the gateway at `socket` for a RAM disk of `sectors`, one line and EOF, and
/// print the one answer line.
fn asks_the_gateway(socket: &Path, sectors: RamDiskSectors) -> Command {
    let mut command = Command::new("python3");
    command.args([
        "-c",
        "import socket, sys\n\
         s = socket.socket(socket.AF_UNIX)\n\
         s.connect(sys.argv[1])\n\
         s.sendall(('{\"op\":\"ram-attach\",\"sectors\":%s}\\n' % sys.argv[2]).encode())\n\
         s.shutdown(socket.SHUT_WR)\n\
         answer = b''\n\
         while chunk := s.recv(65536):\n    answer += chunk\n\
         sys.stdout.write(answer.decode())\n",
    ]);
    command.arg(socket).arg(sectors.get().to_string());
    command
}

fn monitor(job: u64) -> ProcessMonitor {
    ProcessMonitor::prepare(TreeOwner::Job(JobId::new(job).expect("a job id")))
        .expect("prepare the job's process monitor")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ram_disk_the_gateway_attaches_outlives_the_job_that_asked_for_it() {
    let production_before: Vec<RegisteredRamDisk> = ram_disks()
        .into_iter()
        .filter(|disk| disk.sectors == RAM_TEMP_SECTORS && disk.uid == euid())
        .collect();
    cowshed_os::eprintln!(
        "read-only production registry before fixture mutation: {production_before:?}"
    );
    let (gateway, root, socket) = gateway().await;
    assert!(
        socket.starts_with(root.path()),
        "the fixture socket {} lies under its root {}",
        socket.display(),
        root.path().display()
    );
    assert_ne!(
        socket,
        cowshed_gateway_types::control_socket_path(),
        "the fixture never serves on the user's gateway socket"
    );
    let served = fixture_sectors(0);
    let forked = fixture_sectors(1);
    // A guard, never authority: teardown detaches only what the fixture records below.
    for sectors in [served, forked] {
        assert!(
            !ram_disks()
                .iter()
                .any(|disk| disk.sectors == sectors.get() && disk.uid == euid()),
            "no disk of the fixture's size {sectors} before the test"
        );
    }
    let disks = Arc::new(std::sync::Mutex::new(FixtureDisks {
        socket: socket.clone(),
        owned: Vec::new(),
    }));

    // The job asks the gateway; its disk is recorded right after the answer, then the job concludes.
    let job_socket = socket.clone();
    let job_disks = Arc::clone(&disks);
    let (output, owned, retired) = tokio::task::spawn_blocking(move || {
        let monitor = monitor(1);
        let (output, root) = run_job(&monitor, asks_the_gateway(&job_socket, served));
        let answer: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("the gateway's answer ({error}): {output:?}"));
        assert_eq!(
            answer["ok"], true,
            "the gateway attached the disk: {answer}"
        );
        let device = answer["ramDisk"]["device"]
            .as_str()
            .expect("the answer names the whole disk")
            .to_owned();
        let owned = job_disks
            .lock()
            .expect("fixture disks")
            .own(&device, served);
        (output, owned, conclude(&monitor, root))
    })
    .await
    .expect("the gateway job");
    cowshed_os::eprintln!("gateway-attached job: {output:?}; {owned:?}; retirement {retired:?}");
    assert!(
        helpers(&retired).is_empty(),
        "the job's retirement signalled no helper: the gateway forked it ({retired:?})"
    );
    let still: Vec<RegisteredRamDisk> = ram_disks()
        .into_iter()
        .filter(|disk| disk.entry == owned.entry)
        .collect();
    assert!(
        matches!(still.as_slice(), [disk] if disk.device.as_deref() == Some(owned.device.as_str())),
        "the disk outlived the job that asked for it: {still:?}"
    );

    // A second gateway and disk are the unrelated live owner. The first fixture's teardown
    // must not affect it, even though both devices have the same user.
    let (other_gateway, _other_root, other_socket) = self::gateway().await;
    let other_sectors = fixture_sectors(2);
    let other_disks = Arc::new(std::sync::Mutex::new(FixtureDisks {
        socket: other_socket.clone(),
        owned: Vec::new(),
    }));
    let other_owned = Arc::clone(&other_disks);
    let canary = tokio::task::spawn_blocking(move || {
        let monitor = monitor(3);
        let (output, root) = run_job(&monitor, asks_the_gateway(&other_socket, other_sectors));
        let answer: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("other gateway answer");
        assert_eq!(answer["ok"], true, "other gateway attached: {answer}");
        let device = answer["ramDisk"]["device"].as_str().expect("other disk");
        let owned = other_owned
            .lock()
            .expect("other fixture disks")
            .own(device, other_sectors);
        let retired = conclude(&monitor, root);
        assert!(
            helpers(&retired).is_empty(),
            "other creator retired no helper: {retired:?}"
        );
        cowshed_os::eprintln!("independent gateway disk: {owned:?}; retirement {retired:?}");
        owned
    })
    .await
    .expect("other creator job");

    // The same attach forked inside the job is retired with it: the creating task's own attach
    // before the gateway owned it. Its disk is recorded right after the attach, before the job
    // concludes. The fixture gateway's lease spaces it out, as it would any disk command.
    let lease_socket = socket.clone();
    let forked_disks = Arc::clone(&disks);
    let (output, forked_disk, retired) = tokio::task::spawn_blocking(move || {
        leased_at(
            &lease_socket,
            DiskClass::Storage,
            "hdiutil attach -nomount (a job's own)",
            None,
            || {
                let mut attach = Command::new("/usr/bin/hdiutil");
                attach.args(["attach", "-nomount", &format!("ram://{}", forked.get())]);
                let monitor = monitor(2);
                let (output, root) = run_job(&monitor, attach);
                assert!(output.status.success(), "the job's own attach: {output:?}");
                let device = String::from_utf8_lossy(&output.stdout)
                    .split_whitespace()
                    .next()
                    .expect("hdiutil printed the whole disk")
                    .to_owned();
                let owned = forked_disks
                    .lock()
                    .expect("fixture disks")
                    .own(&device, forked);
                Ok::<_, String>((output, owned, conclude(&monitor, root)))
            },
        )
    })
    .await
    .expect("the forking job")
    .expect("the forking job's lease");
    cowshed_os::eprintln!("job-forked attach: {output:?}; {forked_disk:?}; retirement {retired:?}");
    assert!(
        !helpers(&retired).is_empty(),
        "a helper the job forked is the job's member and its retirement signals it ({retired:?})"
    );

    assert!(
        ram_disks()
            .iter()
            .all(|disk| disk.entry != forked_disk.entry),
        "the job-owned control disk left the registry after its helper was retired: {forked_disk:?}"
    );
    // Teardown detaches only the identities recorded above, each revalidated against the registry.
    let (detached, refused) = disks.lock().expect("fixture disks").detach();
    assert!(refused.is_empty(), "teardown refused: {refused:?}");
    assert!(
        detached.contains(&owned.device),
        "the fixture detached its gateway-attached disk {}: {detached:?}",
        owned.device
    );
    assert!(
        ram_disks().iter().all(|disk| disk.entry != owned.entry),
        "the fixture's gateway-attached disk is gone after teardown"
    );
    assert!(
        ram_disks().iter().any(|disk| disk.entry == canary.entry
            && disk.device.as_deref() == Some(canary.device.as_str())),
        "exact-owned teardown preserved the independently owned disk {canary:?}"
    );
    cowshed_os::eprintln!(
        "fixture teardown detached {detached:?}; independent disk {} still registered",
        canary.entry
    );
    let (other_detached, other_refused) = other_disks.lock().expect("other fixture disks").detach();
    assert!(
        other_refused.is_empty(),
        "other teardown: {other_refused:?}"
    );
    assert_eq!(
        other_detached.as_slice(),
        std::slice::from_ref(&canary.device)
    );
    assert!(ram_disks().iter().all(|disk| disk.entry != canary.entry));
    drop(other_disks);
    other_gateway.drain().await.expect("drain other gateway");
    let production_after: Vec<RegisteredRamDisk> = ram_disks()
        .into_iter()
        .filter(|disk| disk.sectors == RAM_TEMP_SECTORS && disk.uid == euid())
        .collect();
    cowshed_os::eprintln!(
        "read-only production registry after owned teardown: {production_after:?}"
    );
    assert_eq!(
        production_before, production_after,
        "production registry changed during isolated fixture boundary; inspect exact operation/retirement receipts before attributing an external change"
    );
    drop(disks);
    gateway.drain().await.expect("drain");
}
