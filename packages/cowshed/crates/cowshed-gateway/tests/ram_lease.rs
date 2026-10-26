#![cfg(target_os = "macos")]
//! A RAM disk lease leaves no disk behind its owner (05_gateway.md, "RAM disks").
//!
//! The decisions are `ram_lease`'s table tests; these are the actual runs of the shell on real
//! disks: an owner that ends its connection with no end line, and a gateway stopped while it holds
//! a lease, whose namespace's next gateway detaches it at startup. Every disk is the fixture's own,
//! a size no other process uses, recorded by its registry identity for a teardown that detaches
//! only that identity if the gateway did not. The user's gateway is never asked.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;
#[path = "support/ram_fixture.rs"]
mod ram_fixture;

use std::time::Duration;

use cowshed_gateway::DiskLeaseLimits;
use cowshed_gateway_types::RamDiskDevice;

use ram_fixture::{FixtureDisks, end_lease, fixture_sectors, gateway, gateway_on, lease};

fn leased_device(answer: &serde_json::Value) -> RamDiskDevice {
    assert_eq!(
        answer["ok"], true,
        "the gateway attached the disk: {answer}"
    );
    RamDiskDevice::parse(answer["ramDisk"]["device"].as_str().expect("a whole disk"))
        .expect("a whole disk")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ram_lease_whose_owner_ends_without_keeping_it_leaves_the_registry() {
    let (gateway, root, socket) = gateway(DiskLeaseLimits::default()).await;
    let namespace = root.namespace();
    let sectors = fixture_sectors(0);
    let held = socket.clone();
    let (owned, after, disks) = tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(namespace);
        let (stream, answer) = lease(&held, sectors);
        let owned = disks.own(leased_device(&answer), sectors);
        // No end line: the owner's connection just ends. The gateway closes it once settled.
        (owned, end_lease(stream, None), disks)
    })
    .await
    .expect("the lease");
    assert_eq!(after, "", "nobody is left to answer");
    assert!(
        !owned.is_registered(),
        "the gateway detached the disk its owner left: {owned:?}"
    );
    drop(disks);
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_next_gateway_detaches_a_lease_a_stopped_gateway_held() {
    let (first, root, socket) = gateway(DiskLeaseLimits::default()).await;
    let sectors = fixture_sectors(1);
    let held = socket.clone();
    let namespace = root.namespace();
    let (stream, owned, disks) = tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(namespace);
        let (stream, answer) = lease(&held, sectors);
        let owned = disks.own(leased_device(&answer), sectors);
        (stream, owned, disks)
    })
    .await
    .expect("the lease");
    // The gateway stops while it holds the lease: its connection goes with it, its record stays.
    first.drain().await.expect("drain the first gateway");
    drop(stream);
    assert!(
        owned.is_registered(),
        "a stopped gateway detaches nothing: {owned:?}"
    );
    let (mut next, _) = gateway_on(&root, DiskLeaseLimits::default()).await;
    next.detached(owned.entry).await;
    assert!(
        !owned.is_registered(),
        "the next gateway's startup scan detached its predecessor's lease: {owned:?}"
    );
    drop(disks);
    next.drain().await.expect("drain the next gateway");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_next_gateway_detaches_a_disk_a_stopped_gateway_attached_but_never_recorded() {
    let (first, root, socket) = gateway(DiskLeaseLimits::default()).await;
    let sectors = fixture_sectors(2);
    let held = socket.clone();
    let namespace = root.namespace();
    let (stream, owned, disks) = tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(namespace);
        let (stream, answer) = lease(&held, sectors);
        let owned = disks.own(leased_device(&answer), sectors);
        (stream, owned, disks)
    })
    .await
    .expect("the lease");
    first.drain().await.expect("drain the first gateway");
    drop(stream);
    // What a gateway stopped mid-attach leaves: the attach's intent and no lease record.
    let records = root.path().join("ram-leases");
    std::fs::remove_file(records.join(owned.entry.to_string())).expect("unrecord the lease");
    std::fs::write(records.join("1-0.intent"), sectors.get().to_string()).expect("the intent");
    assert!(
        owned.is_registered(),
        "a stopped gateway detaches nothing: {owned:?}"
    );
    let (mut next, _) = gateway_on(&root, DiskLeaseLimits::default()).await;
    next.detached(owned.entry).await;
    assert!(
        !owned.is_registered(),
        "the next gateway's startup scan detached the unrecorded attach's disk: {owned:?}"
    );
    assert!(
        !records.join("1-0.intent").exists(),
        "and removed its intent"
    );
    drop(disks);
    next.drain().await.expect("drain the next gateway");
}

/// The parent owns this process even when a readiness assertion unwinds.
struct FixtureProcess(std::process::Child);

impl Drop for FixtureProcess {
    fn drop(&mut self) {
        use cowshed_os::test_backstop::ChildBackstop as _;
        match self.0.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                if let Err(error) = self.0.kill() {
                    cowshed_os::eprintln!("end RAM fixture process {}: {error}", self.0.id());
                }
            }
            Err(error) => {
                cowshed_os::eprintln!("inspect RAM fixture process {}: {error}", self.0.id())
            }
        }
        if let Err(error) = self.0.wait_bounded() {
            cowshed_os::eprintln!("reap RAM fixture process {}: {error}", self.0.id());
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RamOwnerReport {
    root: std::path::PathBuf,
    disk: cowshed_gateway_types::RamDiskAttached,
}

fn fixture_process(mode: &str) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().expect("test executable"));
    command
        .args([
            "--exact",
            "ram_namespace_owner_process",
            "--ignored",
            "--nocapture",
            "--quiet",
        ])
        .env("COWSHED_TEST_RAM_NAMESPACE_PROCESS", mode)
        // This is the next fixture run, not another process of the earlier nextest run.
        .env("NEXTEST_RUN_ID", uuid::Uuid::new_v4().to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    command
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_next_fixture_start_reclaims_a_killed_owners_kept_ram_and_keeps_a_live_sibling() {
    use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
    use std::io::BufRead as _;
    let (live, root, socket) = gateway(DiskLeaseLimits::default()).await;
    let namespace = root.namespace();
    let (live_disk, live_disks) = tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(namespace);
        let disk = disks.attach(fixture_sectors(3));
        (disk, disks)
    })
    .await
    .expect("live sibling disk");
    let mut owner = FixtureProcess(fixture_process("owner").spawn().expect("RAM owner process"));
    let mut output = std::io::BufReader::new(owner.0.stdout.take().expect("owner stdout"));
    let dead: RamOwnerReport = loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).expect("owner readiness"),
            0,
            "owner exited before publishing its disk"
        );
        if let Some(report) = line.strip_prefix("ram-owner:") {
            break serde_json::from_str(report.trim()).expect("typed owner report");
        }
    };
    assert!(ram_fixture::registered(dead.disk.registry_entry).is_some());
    // The parent owns this exact child fixture too. Retain a recovery receipt in
    // its live root before killing, so even a deliberately broken sweep cannot
    // strand the disk when an assertion unwinds after the dead root disappears.
    ram_fixture::namespace_root::scratch_apfs::ram::record(root.namespace(), &dead.disk)
        .expect("retain parent custody of its child's exact disk");
    owner.0.kill().expect("kill the owner without Drop");
    owner.0.wait_bounded().expect("reap killed owner");
    assert!(
        dead.root.is_dir(),
        "killed owner's root remains for the next start"
    );
    let recovered = fixture_process("sweep")
        .output_bounded()
        .expect("next fixture startup");
    assert!(
        std::str::from_utf8(&recovered.stdout)
            .expect("recovery stdout")
            .lines()
            .any(|line| line == "ram-sweep-complete"),
        "the recovery child actually ran and joined its sweep"
    );
    assert!(
        recovered.status.success(),
        "next fixture startup: {recovered:?}"
    );
    assert!(
        !dead.root.exists(),
        "next start removed the dead namespace: {}",
        dead.root.display()
    );
    assert!(
        ram_fixture::registered(dead.disk.registry_entry).is_none(),
        "kept RAM was released before its receipt root disappeared"
    );
    assert!(
        root.path().is_dir() && live_disk.is_registered(),
        "live sibling namespace and exact disk survive reclaim"
    );
    drop(live_disks);
    live.drain().await.expect("drain live sibling");
    drop(socket);
}

/// A process fixture selected only by the parent above, as in the APFS killed-owner witness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned RAM namespace child selected by its parent"]
async fn ram_namespace_owner_process() {
    use std::io::{Read as _, Write as _};
    let Some(mode) = std::env::var_os("COWSHED_TEST_RAM_NAMESPACE_PROCESS") else {
        return;
    };
    if mode == "sweep" {
        // Dropping the starter joins its startup sweep before this process returns.
        drop(ram_fixture::namespace_root::NamespaceRoot::mint());
        println!("ram-sweep-complete");
        return;
    }
    assert_eq!(mode, "owner");
    let (gateway, root, socket) = gateway(DiskLeaseLimits::default()).await;
    let namespace = root.namespace();
    let root_path = root.path().to_owned();
    tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(namespace);
        let disk = disks.attach(fixture_sectors(0));
        let report = RamOwnerReport {
            root: root_path,
            disk: cowshed_gateway_types::RamDiskAttached {
                device: disk.device,
                registry_entry: disk.entry,
            },
        };
        println!(
            "ram-owner:{}",
            serde_json::to_string(&report).expect("owner report")
        );
        std::io::stdout().flush().expect("publish owner readiness");
        std::io::stdin()
            .read_to_end(&mut Vec::new())
            .expect("parent input gate");
        drop(disks);
    })
    .await
    .expect("owner held by its parent");
    gateway.drain().await.expect("drain owner");
}
