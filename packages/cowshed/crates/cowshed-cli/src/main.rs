use std::ffi::OsString;
use std::time::Duration;

use cowshed_cli::run::{Ending, run};

/// How long an interrupted invocation waits for blocking work already underway before it exits.
const INTERRUPTED_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

fn main() {
    let arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cowshed: cannot start the async runtime: {error}");
            std::process::exit(1);
        }
    };
    let ending = runtime.block_on(run(arguments));
    if let Ending::Interrupted { .. } = ending {
        // Shutting the runtime down drops every task it still holds. A workspace supervisor is
        // one, and dropping it ends the process group of each job it still runs: once this
        // process is gone nothing could observe or cancel them. A finished command skips this on
        // purpose, so the jobs it backgrounded keep running after it exits.
        runtime.shutdown_timeout(INTERRUPTED_SHUTDOWN_GRACE);
    }
    std::process::exit(ending.exit_code());
}
