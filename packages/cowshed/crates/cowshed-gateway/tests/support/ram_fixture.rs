//! A fixture gateway serving `ram-lease`, `ram-detach` and the disk lease on a socket of its own,
//! and the I/O Registry's RAM disks, for the RAM disk tests (05_gateway.md, "RAM disks").
//!
//! Every fixture owns a test namespace and derives everything from it (01_storage.md, "Host
//! namespace"): its root, the namespace's directory below the per-user root; the gateway's socket, at
//! that root, where the namespace's disk tools take their lease; and the runner that detaches. No
//! fixture path can be the user's gateway's, so none is compared with it.
//!
//! Every disk a test attaches is its own: a size no other process uses, attached under its fixture
//! gateway's lease. The fixture records each disk's registry identity right after its attach and
//! its teardown detaches only those identities, each while a fresh registry read still lists it
//! unchanged, through the runner's registry-bound detach, never through the gateway under test.
//! Size and user tell the fixture's disks apart; they never authorize a detach. The user's gateway
//! is never asked.
#![allow(dead_code)]

use std::io::{BufRead as _, Read as _, Write as _};
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use cowshed_apfs::disk_lease::lease_socket;
use cowshed_apfs::ram_disk::SystemRamDisks;
use cowshed_apfs::{CommandRequest, SystemCommandRunner};
use cowshed_core::api::dto::JobId;
use cowshed_core::runtime::process_monitor::ProcessMonitor;
use cowshed_core::runtime::process_tree::TreeOwner;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, DiskLeaseLimits, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_gateway_types::{
    RamDiskDevice, RamDiskEntry, RamDiskSectors, RamLeaseEnd, RamLeaseEndLine, RamLeaseRequest,
};
use cowshed_os::namespace::{HostNamespace, TestNamespace};

#[path = "namespace_root.rs"]
mod namespace_root;
use namespace_root::NamespaceRoot;

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

/// The largest pid macOS issues (`PID_MAX`, `bsd/sys/proc_internal.h`).
const PID_MAX: u64 = 99_999;
/// The RAM disk tests' band of sizes: 512 MiB plus 4 sectors per pid, so no two processes share one.
const FIXTURE_BASE: u64 = 1_048_576;
const FIXTURE_TOP: u64 = FIXTURE_BASE + PID_MAX * 4 + 4;
/// The nx-plugin host proofs' band (`scratchVolume` in
/// `packages/nx-plugin/src/executors/bounded-exec/ram-temp-fixtures.ts`): 64 MiB plus 4 sectors per pid.
const NX_PLUGIN_FIXTURE_TOP: u64 = 131_072 + PID_MAX * 4 + 4;
/// The user's RAM temp volume, in sectors: `RAM_TEMP_CAPACITY_BYTES` (8 GiB plus its revision, 3,
/// in sectors) of `packages/nx-plugin/src/executors/bounded-exec/ram-temp.ts` over 512.
pub const RAM_TEMP_SECTORS: u64 = 16_777_216 + 3;

/// A size no other process attaches: [`FIXTURE_BASE`] plus sectors this process's pid and `index`
/// pick. Its band lies above the nx-plugin host proofs' and below the RAM temp volume's own size.
pub fn fixture_sectors(index: u64) -> RamDiskSectors {
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

/// A gateway serving the RAM disk operations and the disk lease on the socket of a test namespace of
/// its own under `disk_lease`, its RAM disks attached and detached under its own lease, and the
/// guard that removes the namespace's root on every exit path. That socket is where the
/// namespace's disk tools lease from (`lease_socket`), so the runners that attach and detach ask
/// this gateway and no other.
pub async fn gateway(disk_lease: DiskLeaseLimits) -> (Gateway, NamespaceRoot, PathBuf) {
    let root = NamespaceRoot::mint();
    let (gateway, socket) = gateway_on(&root, disk_lease).await;
    (gateway, root, socket)
}

/// A gateway on `root`'s namespace, as [`gateway`] starts one: a second one on the same root is
/// the next gateway of that namespace, which finds what the first left recorded.
pub async fn gateway_on(root: &NamespaceRoot, disk_lease: DiskLeaseLimits) -> (Gateway, PathBuf) {
    let host = HostNamespace::Test(root.namespace());
    let cache = root.path().join("cache");
    std::fs::create_dir_all(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = lease_socket(host);
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            ram_disks: Some(Arc::new(SystemRamDisks::new(host))),
            disk_lease,
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");
    (gateway, socket)
}

/// Take a `ram-lease` of `sectors` from the gateway at `socket`: its one answer line and the
/// connection that holds the lease, still open.
pub fn lease(socket: &Path, sectors: RamDiskSectors) -> (UnixStream, serde_json::Value) {
    let mut stream = UnixStream::connect(socket).expect("connect to the fixture gateway");
    let request = serde_json::to_string(&RamLeaseRequest::new(sectors)).expect("encode the lease");
    stream
        .write_all(format!("{request}\n").as_bytes())
        .expect("write the request");
    let mut answer = String::new();
    std::io::BufReader::new(&stream)
        .read_line(&mut answer)
        .expect("read the answer");
    let answer = serde_json::from_str(&answer)
        .unwrap_or_else(|error| panic!("the gateway's answer to {request} ({error}): {answer:?}"));
    (stream, answer)
}

/// End a held lease with `end`, or with no line at all, half-close, and read what the gateway
/// answers until it closes: it closes once it has settled the lease, a detach included.
pub fn end_lease(mut stream: UnixStream, end: Option<RamLeaseEnd>) -> String {
    if let Some(end) = end {
        let line = serde_json::to_string(&RamLeaseEndLine { end }).expect("encode the end");
        stream
            .write_all(format!("{line}\n").as_bytes())
            .expect("write the end");
    }
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close");
    let mut after = String::new();
    stream
        .read_to_string(&mut after)
        .expect("read to the close");
    after
}

/// Write one request line to the gateway at `socket`, half-close, and read its one answer line.
pub fn ask(socket: &Path, request: &str) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket).expect("connect to the fixture gateway");
    stream
        .write_all(format!("{request}\n").as_bytes())
        .expect("write the request");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close");
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).expect("read the answer");
    serde_json::from_slice(&answer).unwrap_or_else(|error| {
        panic!(
            "the gateway's answer to {request} ({error}): {:?}",
            String::from_utf8_lossy(&answer)
        )
    })
}

/// One RAM disk as the I/O Registry lists it: the kernel's record, which leaves out no attached
/// disk. `device` is absent while the disk has published no media yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredRamDisk {
    pub entry: RamDiskEntry,
    pub device: Option<RamDiskDevice>,
    pub sectors: u64,
    pub uid: u32,
}

/// Every RAM disk in the I/O Registry. A read only; it takes no lease.
pub fn ram_disks() -> Vec<RegisteredRamDisk> {
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
            let id = after(entry, " id ", ',').expect("an entry names its registry id");
            Some(RegisteredRamDisk {
                entry: RamDiskEntry::parse(&id)
                    .unwrap_or_else(|error| panic!("ioreg's registry id: {error}")),
                device: after(entry, "\"BSD Name\" = \"", '"').map(|disk| {
                    RamDiskDevice::parse(&format!("/dev/{disk}"))
                        .unwrap_or_else(|error| panic!("a RAM disk's whole disk: {error}"))
                }),
                sectors,
                uid: after(entry, "\"owner-uid\" = ", '\n')
                    .and_then(|uid| uid.trim().parse().ok())
                    .expect("a RAM disk names its owner"),
            })
        })
        .collect()
}

/// The registry's listing of `entry`, if it still lists it.
pub fn registered(entry: RamDiskEntry) -> Option<RegisteredRamDisk> {
    ram_disks().into_iter().find(|disk| disk.entry == entry)
}

pub fn euid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

/// A disk this fixture attached, by the identity the registry gave it right after the attach.
#[derive(Clone, Debug)]
pub struct OwnedDisk {
    pub entry: RamDiskEntry,
    pub device: RamDiskDevice,
    pub sectors: RamDiskSectors,
}

impl OwnedDisk {
    /// Whether the registry still lists this exact disk: its entry, with the same whole disk, size
    /// and owner.
    pub fn is_registered(&self) -> bool {
        registered(self.entry).is_some_and(|now| {
            now.device.as_ref() == Some(&self.device)
                && now.sectors == self.sectors.get()
                && now.uid == euid()
        })
    }
}

/// The fixture's disks: only those it recorded right after attaching them, detached when it drops,
/// on every way out of the test, before the gateway whose lease the detach takes.
pub struct FixtureDisks {
    namespace: TestNamespace,
    owned: Vec<OwnedDisk>,
}

impl FixtureDisks {
    /// The disks of the fixture gateway of `namespace`, none recorded yet.
    pub fn new(namespace: TestNamespace) -> Self {
        Self {
            namespace,
            owned: Vec::new(),
        }
    }

    fn host(&self) -> HostNamespace {
        HostNamespace::Test(self.namespace)
    }

    /// Attach a disk of `sectors` through the fixture gateway's `ram-lease`, keep it, and record it by the
    /// one registry entry that lists its whole disk with that size and this user. Any other
    /// listing is no ownership and is never recorded, so never detached.
    pub fn attach(&mut self, sectors: RamDiskSectors) -> OwnedDisk {
        assert!(
            !ram_disks()
                .iter()
                .any(|disk| disk.sectors == sectors.get() && disk.uid == euid()),
            "no disk of the fixture's size {sectors} before its attach"
        );
        let (stream, answer) = lease(&lease_socket(self.host()), sectors);
        assert_eq!(
            answer["ok"], true,
            "the gateway attached the disk: {answer}"
        );
        let device = RamDiskDevice::parse(
            answer["ramDisk"]["device"]
                .as_str()
                .expect("the answer names the whole disk"),
        )
        .expect("a whole disk");
        let owned = self.own(device, sectors);
        let kept = end_lease(stream, Some(RamLeaseEnd::Keep));
        assert_eq!(
            kept.trim(),
            r#"{"ok":true}"#,
            "the gateway let the fixture keep its disk"
        );
        owned
    }

    /// Record `device`, the disk of `sectors` an attach of this fixture's gateway just answered, by
    /// the one registry entry that lists it with that size and this user. Any other listing is no
    /// ownership and is never recorded, so never detached.
    pub fn own(&mut self, device: RamDiskDevice, sectors: RamDiskSectors) -> OwnedDisk {
        let listed: Vec<RegisteredRamDisk> = ram_disks()
            .into_iter()
            .filter(|disk| disk.device.as_ref() == Some(&device))
            .collect();
        let owned = match listed.as_slice() {
            [disk] if disk.sectors == sectors.get() && disk.uid == euid() => OwnedDisk {
                entry: disk.entry,
                device,
                sectors,
            },
            other => panic!(
                "the fixture's attach answered {device} of {sectors}, but the registry lists {other:?}: not recorded, so never detached"
            ),
        };
        self.owned.push(owned.clone());
        owned
    }

    /// Stop recording `disk` once the test proved it detached.
    pub fn forget(&mut self, disk: &OwnedDisk) {
        self.owned.retain(|owned| owned.entry != disk.entry);
    }

    /// Detach each recorded disk while a fresh registry read still lists it unchanged. A disk
    /// already gone needs nothing; any other listing is refused and reported, never detached.
    /// Answers the disks detached and the refusals.
    pub fn detach(&mut self) -> (Vec<RamDiskDevice>, Vec<String>) {
        let mut detached = Vec::new();
        let mut refused = Vec::new();
        let runner = SystemCommandRunner::new(self.host());
        for owned in self.owned.drain(..) {
            let Some(now) = registered(owned.entry) else {
                continue;
            };
            if now.device.as_ref() != Some(&owned.device)
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
            match runner.run_detach_of_entry(&request, owned.entry.get(), None) {
                Ok(output) if output.status.succeeded() => detached.push(owned.device),
                Ok(output) => refused.push(format!(
                    "hdiutil detach {} ended {}: {}",
                    owned.device,
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
                Err(error) => refused.push(format!("hdiutil detach {}: {error}", owned.device)),
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

pub fn monitor(job: u64) -> ProcessMonitor {
    ProcessMonitor::prepare(TreeOwner::Job(JobId::new(job).expect("a job id")))
        .expect("prepare the job's process monitor")
}
