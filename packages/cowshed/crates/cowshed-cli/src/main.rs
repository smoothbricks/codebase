use std::ffi::OsString;
use std::time::Duration;

use cowshed_cli::run::{Ending, run_interruptible};

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
    let ending = runtime.block_on(run_interruptible(arguments));
    if let Ending::Interrupted { signal } = ending {
        // Shutting the runtime down drops every task it still holds. A workspace supervisor is
        // one, and dropping it ends the process group of each job it still runs: once this
        // process is gone nothing could observe or cancel them. A finished command skips this on
        // purpose, so the jobs it backgrounded keep running after it exits.
        runtime.shutdown_timeout(INTERRUPTED_SHUTDOWN_GRACE);
        die_by(signal);
    }
    std::process::exit(ending.exit_code());
}

/// End this process by `signal` itself, so a parent shell sees the interrupt and stops too; a
/// plain exit status would let a loop around this command carry on after Ctrl-C.
fn die_by(signal: i32) {
    // SAFETY: SIG_DFL installs no handler code, and `raise` delivers the signal to this thread,
    // whose default action for every signal passed here terminates the process.
    let reset = unsafe { libc::signal(signal, libc::SIG_DFL) };
    if reset == libc::SIG_ERR {
        eprintln!(
            "cowshed: cannot restore the default action for signal {signal}: {}",
            std::io::Error::last_os_error()
        );
        return;
    }
    // SAFETY: see above; `raise` takes no memory operands.
    if unsafe { libc::raise(signal) } != 0 {
        eprintln!(
            "cowshed: cannot raise signal {signal}: {}",
            std::io::Error::last_os_error()
        );
    }
}
