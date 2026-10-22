//! `attach-budget`: the host's disk-image device classes as one JSON line (08_testing.md, "Host
//! limit"), read without attaching anything. Cowshed runs no DiskImages2 attach, so nothing is
//! gated on them; the line is evidence for the leaked-device delta across a lane run. Classes
//! that cannot be read print `unread` and exit 1.

use std::process::ExitCode;

use cowshed_apfs::attach_budget::{self, ClassCount, KernelReading};
use serde_json::{Value, json};

const USAGE: &str = "usage: attach-budget";

fn main() -> ExitCode {
    if std::env::args().nth(1).is_some() {
        cowshed_os::eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    match attach_budget::read_kernel() {
        Ok(reading) => {
            println!("{}", line(&reading));
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!("{}", json!({ "unread": error.to_string() }));
            cowshed_os::eprintln!(
                "attach-budget: the disk-image device classes are unreadable: {error}"
            );
            ExitCode::from(1)
        }
    }
}

fn line(reading: &KernelReading) -> Value {
    let class = |count: ClassCount| {
        json!({
            "instances": count.instances,
            "registered": count.registered,
            "leaked": count.leaked(),
        })
    };
    json!({
        "classes": {
            "AppleDiskImageDevice": class(reading.image_devices),
            "DIDeviceIOUserClient": class(reading.user_clients),
            "AppleAPFSVolume": class(reading.apfs_volumes),
            "IOMedia": class(reading.media),
        },
    })
}
