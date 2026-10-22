//! `attach-budget`: the host's DiskImages2 attach pool as one JSON line (08_testing.md, "Host
//! limit"), read without attaching anything. Cowshed attaches its images in the kernel, so the
//! pool gates nothing; the line is evidence for the leaked-device delta across a lane run. A pool
//! that cannot be read prints `unread` and exits 1. `--clear` ends this boot's recorded
//! exhaustion once the operator has freed what spent it (killed the orphaned or stalled helpers
//! `cowshed doctor` names).

use std::process::ExitCode;

use cowshed_apfs::attach_budget::{self, Budget, ClassCount};
use serde_json::{Value, json};

const USAGE: &str = "usage: attach-budget | attach-budget --clear";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (None, None) => report(),
        (Some("--clear"), None) => clear(),
        _ => {
            cowshed_os::eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn report() -> ExitCode {
    match attach_budget::observe() {
        Ok(budget) => {
            println!("{}", line(&budget));
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!("{}", json!({ "unread": error.to_string() }));
            cowshed_os::eprintln!(
                "attach-budget: the disk-image attach pool is unreadable: {error}"
            );
            ExitCode::from(1)
        }
    }
}

fn clear() -> ExitCode {
    match attach_budget::clear() {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => {
            cowshed_os::eprintln!("attach-budget: clear the recorded exhaustion: {error}");
            ExitCode::from(1)
        }
    }
}

fn line(budget: &Budget) -> Value {
    let class = |count: ClassCount| {
        json!({
            "instances": count.instances,
            "registered": count.registered,
            "leaked": count.leaked(),
        })
    };
    let reading = &budget.reading;
    json!({
        "classes": {
            "AppleDiskImageDevice": class(reading.image_devices),
            "DIDeviceIOUserClient": class(reading.user_clients),
            "AppleAPFSVolume": class(reading.apfs_volumes),
            "IOMedia": class(reading.media),
        },
        "spentThisBoot": budget.spent.is_some(),
        "probableLeaks": budget.probable_leaks,
    })
}
