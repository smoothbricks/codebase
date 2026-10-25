#![cfg(target_os = "macos")]
//! A RAM disk the gateway attaches belongs to no job (05_gateway.md, "RAM disks").
//!
//! A job asks a fixture gateway for a RAM disk and concludes; the process monitor that owns the
//! job retires everything it retains, and that retirement signals no `diskimages-helper`: the
//! disk stays registered. Every disk here is the fixture's own: a size no other process
//! uses, attached under the fixture gateway's lease on its own socket. The fixture records each
//! disk's registry identity right after its attach and detaches only those identities, each while a
//! fresh registry read still lists it unchanged. The user's gateway is never asked.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;
#[path = "support/ram_fixture.rs"]
mod ram_fixture;

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use cowshed_core::api::process::JobProcessRetirement;
use cowshed_core::runtime::process_monitor::ProcessMonitor;
use cowshed_gateway::DiskLeaseLimits;
use cowshed_gateway_types::{RamDiskDevice, RamDiskSectors};

use ram_fixture::{
    FixtureDisks, RAM_TEMP_SECTORS, RegisteredRamDisk, euid, fixture_sectors, monitor, ram_disks,
};

const HELPER: &str = "diskimages-helper";

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

/// The job's root: take a `ram-lease` of `sectors` from the gateway at `socket`, keep it, and
/// print the one attach answer line (the keep's answer goes to stderr).
fn asks_the_gateway(socket: &Path, sectors: RamDiskSectors) -> Command {
    let mut command = Command::new("python3");
    command.args([
        "-c",
        "import socket, sys\n\
         s = socket.socket(socket.AF_UNIX)\n\
         s.connect(sys.argv[1])\n\
         s.sendall(('{\"op\":\"ram-lease\",\"sectors\":%s}\\n' % sys.argv[2]).encode())\n\
         f = s.makefile('rb')\n\
         answer = f.readline()\n\
         s.sendall(b'{\"end\":\"keep\"}\\n')\n\
         s.shutdown(socket.SHUT_WR)\n\
         sys.stderr.write(f.read().decode())\n\
         sys.stdout.write(answer.decode())\n",
    ]);
    command.arg(socket).arg(sectors.get().to_string());
    command
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
    let (gateway, root, socket) = ram_fixture::gateway(DiskLeaseLimits::default()).await;
    assert!(
        socket.starts_with(root.path()),
        "the fixture socket {} lies under its root {}",
        socket.display(),
        root.path().display()
    );
    let served = fixture_sectors(0);
    // A guard, never authority: teardown detaches only what the fixture records below.
    assert!(
        !ram_disks()
            .iter()
            .any(|disk| disk.sectors == served.get() && disk.uid == euid()),
        "no disk of the fixture's size {served} before the test"
    );
    let disks = Arc::new(std::sync::Mutex::new(FixtureDisks::new(root.namespace())));

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
        let device = RamDiskDevice::parse(
            answer["ramDisk"]["device"]
                .as_str()
                .expect("the answer names the whole disk"),
        )
        .expect("a whole disk");
        let owned = job_disks.lock().expect("fixture disks").own(device, served);
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
        matches!(still.as_slice(), [disk] if disk.device.as_ref() == Some(&owned.device)),
        "the disk outlived the job that asked for it: {still:?}"
    );

    // A second gateway and disk are the unrelated live owner. The first fixture's teardown
    // must not affect it, even though both devices have the same user.
    let (other_gateway, other_root, other_socket) =
        ram_fixture::gateway(DiskLeaseLimits::default()).await;
    let other_sectors = fixture_sectors(2);
    let other_disks = Arc::new(std::sync::Mutex::new(FixtureDisks::new(
        other_root.namespace(),
    )));
    let other_owned = Arc::clone(&other_disks);
    let canary = tokio::task::spawn_blocking(move || {
        let monitor = monitor(3);
        let (output, root) = run_job(&monitor, asks_the_gateway(&other_socket, other_sectors));
        let answer: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("other gateway answer");
        assert_eq!(answer["ok"], true, "other gateway attached: {answer}");
        let device =
            RamDiskDevice::parse(answer["ramDisk"]["device"].as_str().expect("other disk"))
                .expect("a whole disk");
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
        ram_disks()
            .iter()
            .any(|disk| disk.entry == canary.entry && disk.device.as_ref() == Some(&canary.device)),
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
