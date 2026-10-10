//! `attach-budget --need <N>`: the real-APFS lane's gate (08_testing.md, "Host limit").
//!
//! Prints the host's disk-image attach pool as one JSON line and refuses, exit 3 with
//! `AttachPoolExhausted` and a `next:` hint on stderr, when this boot's pool is spent or its
//! headroom is below `N`, the lane's measured attach count. A pool that cannot be read proves
//! nothing either way: the line says `unread`, stderr says why, and the exit is 0, so the lane
//! runs with the warning visible. `--clear` ends this boot's recorded exhaustion once the
//! operator has freed what spent it (killed the orphaned helpers `cowshed doctor` names).

use std::process::ExitCode;

use cowshed_core::attach_budget::{self, Budget, Ceiling, ClassCount};
use serde_json::{Value, json};

const USAGE: &str = "usage: attach-budget --need <attaches> | attach-budget --clear";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (first, second, rest) = (args.next(), args.next(), args.next());
    match (first.as_deref(), second.as_deref(), rest) {
        (Some("--need"), Some(need), None) => match need.parse::<u64>() {
            Ok(need) => gate(need),
            Err(_) => usage(),
        },
        (Some("--clear"), None, None) => clear(),
        _ => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

fn gate(need: u64) -> ExitCode {
    let budget = match attach_budget::observe() {
        Ok(budget) => budget,
        Err(error) => {
            println!("{}", json!({ "need": need, "unread": error.to_string() }));
            eprintln!(
                "attach-budget: warning: the disk-image attach pool is unreadable ({error}); \
                 nothing proves there is headroom or that there is not, so the lane runs"
            );
            return ExitCode::SUCCESS;
        }
    };
    println!("{}", report(&budget, need));
    match budget.admits(need) {
        Ok(()) => ExitCode::SUCCESS,
        Err(refused) => {
            eprintln!("{refused}");
            ExitCode::from(3)
        }
    }
}

fn clear() -> ExitCode {
    let cleared = attach_budget::boot().and_then(|boot| {
        attach_budget::record_attach_at(&attach_budget::record_path()?, &boot.session)
    });
    match cleared {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("attach-budget: clear the recorded exhaustion: {error}");
            ExitCode::from(1)
        }
    }
}

fn report(budget: &Budget, need: u64) -> Value {
    let class = |count: ClassCount| {
        json!({
            "instances": count.instances,
            "registered": count.registered,
            "leaked": count.leaked(),
        })
    };
    let reading = &budget.reading;
    json!({
        "need": need,
        "classes": {
            "AppleDiskImageDevice": class(reading.image_devices),
            "DIDeviceIOUserClient": class(reading.user_clients),
            "AppleAPFSVolume": class(reading.apfs_volumes),
            "IOMedia": class(reading.media),
        },
        "ceiling": {
            "leaked": budget.ceiling.leaked(),
            "source": match budget.ceiling {
                Ceiling::Default => "default",
                Ceiling::Observed { .. } => "observed",
            },
        },
        "headroom": budget.headroom(),
        "spentThisBoot": budget.spent.is_some(),
        "probableLeaks": budget.probable_leaks,
    })
}
