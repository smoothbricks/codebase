#![cfg(target_os = "macos")]
//! A RAM disk is detached by the gateway that attached it, bound to its exact registry entry
//! (05_gateway.md, "RAM disks").
//!
//! A job's requester asks a fixture gateway to detach a disk and dies, its job retired, while the
//! detach waits behind a namespace phase the test holds: the gateway's detach runs once the phase
//! drains, with no requester left to hear it. A detach whose entry no longer registers the named
//! whole disk is refused before any spawn, and a detach by one disk's entry leaves another disk of
//! the same user registered. Every disk here is the fixture's own: a size no other process uses,
//! attached under the fixture gateway's lease on its own socket, recorded by its registry entry
//! right after the attach. The user's gateway is never asked.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;
#[path = "support/ram_fixture.rs"]
mod ram_fixture;

use std::io::{BufRead as _, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use cowshed_apfs::disk_lease::leased_at;
use cowshed_gateway::DiskLeaseLimits;
use cowshed_gateway_types::{DiskClass, RamDetachRequest, RamDiskEntry};

use ram_fixture::{FixtureDisks, OwnedDisk, ask, fixture_sectors, gateway, monitor, registered};

/// The line that asks to detach `disk`'s whole disk as registry entry `entry`.
fn detach_line(disk: &OwnedDisk, entry: RamDiskEntry) -> String {
    serde_json::to_string(&RamDetachRequest::new(disk.device.clone(), entry))
        .expect("encode the detach")
}

/// A job's root that writes `line` to the gateway at `socket`, half-closes, prints `sent`, and
/// waits for an answer it is killed before reading.
fn requester(socket: &Path, line: &str) -> Command {
    let mut command = Command::new("python3");
    command.args([
        "-c",
        "import socket, sys\n\
         s = socket.socket(socket.AF_UNIX)\n\
         s.connect(sys.argv[1])\n\
         s.sendall((sys.argv[2] + '\\n').encode())\n\
         s.shutdown(socket.SHUT_WR)\n\
         print('sent', flush=True)\n\
         s.recv(1)\n",
    ]);
    command
        .arg(socket)
        .arg(line)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    command
}

/// Wait until the registry no longer lists `disk`'s entry. Nothing tells the test when a detach no
/// one waits for has ended, so the registry, which lists every attached disk, is read until it
/// does; a detach that never ends is the test runner's hang to report.
fn await_detached(disk: &OwnedDisk) {
    while registered(disk.entry).is_some() {
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ram_detach_outlives_the_requester_that_asked_for_it() {
    // The test holds a namespace phase for as long as the requester lives; the gateway must not
    // evict it to the waiting detach meanwhile.
    let (gateway, _root, socket) = gateway(DiskLeaseLimits {
        phase_bound: Duration::from_secs(600),
        ..DiskLeaseLimits::default()
    })
    .await;
    tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(socket.clone());
        let disk = disks.attach(fixture_sectors(0));
        let line = detach_line(&disk, disk.entry);
        let monitor = monitor(1);
        let retired = leased_at(
            &socket,
            DiskClass::Namespace,
            "ram-detach test holds the namespace phase",
            None,
            || {
                let (mut child, root) = monitor
                    .spawn_root(requester(&socket, &line))
                    .expect("spawn the requester's job");
                let mut said = String::new();
                BufReader::new(child.stdout.take().expect("the requester's stdout"))
                    .read_line(&mut said)
                    .expect("read the requester");
                assert_eq!(said, "sent\n", "the requester wrote its detach");
                child.kill().expect("kill the requester");
                child.wait().expect("reap the requester");
                let retired = monitor
                    .retire_root(root, Duration::from_secs(1))
                    .expect("retire the requester's job");
                assert!(
                    disk.is_registered(),
                    "the detach waits behind the held namespace phase: nothing detached {disk:?} \
                     while its requester lived"
                );
                Ok::<_, String>(retired)
            },
        )
        .expect("the namespace phase");
        cowshed_os::eprintln!("requester's job retired: {retired:?}");
        assert!(
            retired.outlived.iter().all(|member| {
                !member.image.as_deref().is_some_and(|image| {
                    image.ends_with("/hdiutil") || image.ends_with("/diskimages-helper")
                })
            }),
            "the requester's job held no disk-image process to retire: {retired:?}"
        );
        // The phase drained with the requester gone: the gateway's detach runs to its end.
        await_detached(&disk);
        disks.forget(&disk);
    })
    .await
    .expect("the requester proof");
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ram_detach_refuses_an_entry_that_does_not_register_the_device() {
    let (gateway, _root, socket) = gateway(DiskLeaseLimits::default()).await;
    tokio::task::spawn_blocking(move || {
        let mut disks = FixtureDisks::new(socket.clone());
        let first = disks.attach(fixture_sectors(1));
        let second = disks.attach(fixture_sectors(2));

        // Each disk's whole disk named with the other's entry: both pairs are swapped.
        for (named, foreign) in [(&first, &second), (&second, &first)] {
            let answer = ask(&socket, &detach_line(named, foreign.entry));
            assert_eq!(answer["ok"], true, "a typed answer: {answer}");
            assert_eq!(
                answer["ramDetach"]["outcome"], "not-started",
                "an entry that registers another disk is refused before any spawn: {answer}"
            );
            assert!(
                first.is_registered() && second.is_registered(),
                "a refused detach detached nothing: {answer}"
            );
        }

        // The exact pair detaches its own disk, and only it.
        let detached = ask(&socket, &detach_line(&first, first.entry));
        assert_eq!(
            detached["ramDetach"]["outcome"], "terminal",
            "the exact pair detached: {detached}"
        );
        assert_eq!(
            detached["ramDetach"]["status"],
            serde_json::json!({"exit": 0}),
            "hdiutil detached the disk: {detached}"
        );
        assert!(
            registered(first.entry).is_none(),
            "the detached disk left the registry: {first:?}"
        );
        disks.forget(&first);
        assert!(
            second.is_registered(),
            "the independent disk stays registered under its same whole disk: {second:?}"
        );

        // The entry is gone for good: its whole disk's name, whoever has it now, is never detached.
        let again = ask(&socket, &detach_line(&first, first.entry));
        assert_eq!(
            again["ramDetach"]["outcome"], "not-started",
            "an entry that left the registry detaches nothing: {again}"
        );
        assert!(second.is_registered(), "still registered: {second:?}");

        let (detached, refused) = disks.detach();
        assert!(refused.is_empty(), "teardown refused: {refused:?}");
        assert_eq!(detached.as_slice(), std::slice::from_ref(&second.device));
    })
    .await
    .expect("the refusal proof");
    gateway.drain().await.expect("drain");
}
