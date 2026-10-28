//! `<cowshed> __name-volume <device> <mount-point> <name>`: the one step of `cowshed setup` that
//! runs this binary as root, inside setup's own authorization session, to name the build-cache
//! volume it adopts under another label (01a_apfs.md, "Dedicated build-cache volume"). A
//! root-mounted volume is named only by root, and never through Disk Arbitration, so setup hands
//! this verb the device and mountpoint it just attested and it names that device through its mount
//! root held open (`cowshed_apfs::name_host_volume`). Not a user verb and not in the command map:
//! `main` answers it before anything else runs, so a root process starts no runtime, no warm
//! host and no telemetry, and writes nothing but its one line of stderr.

use std::ffi::OsString;
use std::path::Path;

use cowshed_core::storage::bootstrap::VOLUME_NAME_VERB;

/// The exit code when the first argument is [`VOLUME_NAME_VERB`], else `None`.
pub fn dispatch() -> Option<i32> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next()? != VOLUME_NAME_VERB {
        return None;
    }
    Some(run(&arguments.collect::<Vec<_>>()))
}

fn run(arguments: &[OsString]) -> i32 {
    let [device, mount_point, name] = arguments else {
        cowshed_os::eprintln!("cowshed: {VOLUME_NAME_VERB} takes <device> <mount-point> <name>");
        return 2;
    };
    let (Some(device), Some(name)) = (device.to_str(), name.to_str()) else {
        cowshed_os::eprintln!("cowshed: {VOLUME_NAME_VERB} takes a UTF-8 device and name");
        return 2;
    };
    match cowshed_apfs::name_host_volume(Path::new(mount_point), device, name) {
        Ok(()) => 0,
        Err(error) => {
            cowshed_os::eprintln!("cowshed: {VOLUME_NAME_VERB}: {error}");
            1
        }
    }
}
