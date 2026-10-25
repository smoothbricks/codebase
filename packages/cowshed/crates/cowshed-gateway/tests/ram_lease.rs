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

use std::time::{Duration, Instant};

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
    let (next, _) = gateway_on(&root, DiskLeaseLimits::default()).await;
    // The startup scan runs on in the background: the registry is its only witness.
    let deadline = Instant::now() + Duration::from_secs(60);
    while owned.is_registered() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !owned.is_registered(),
        "the next gateway's startup scan detached its predecessor's lease: {owned:?}"
    );
    drop(disks);
    next.drain().await.expect("drain the next gateway");
}
