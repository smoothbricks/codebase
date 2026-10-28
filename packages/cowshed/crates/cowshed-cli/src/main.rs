use std::ffi::OsString;
use std::time::Duration;

use cowshed_cli::run::{Ending, prepare, run_interruptible};

/// How long an interrupted invocation waits for blocking work already underway before it exits.
const INTERRUPTED_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

fn main() {
    // Setup runs this binary as root for one thing, naming the volume it adopts: answered before
    // anything else, so the root process starts nothing (`volume_name`).
    if let Some(code) = cowshed_cli::volume_name::dispatch() {
        std::process::exit(code);
    }
    // The gateway host launchd runs (05_gateway.md "The gateway host") does nothing but run its
    // child, so it runs before anything else: one thread, no warm-host registration.
    let mut host = std::env::args_os().skip(1);
    if host
        .next()
        .is_some_and(|verb| verb == cowshed_cli::gateway_host::VERB)
    {
        cowshed_cli::gateway_host::run(&host.collect::<Vec<_>>());
    }
    // A declared land's after-land child pushes main and runs the repository's after commands,
    // detached from the land that started it (02_workspaces.md "A declared land" step 6).
    if let Some(code) = cowshed_cli::land_after::dispatch() {
        std::process::exit(code);
    }
    // Before the runtime starts any thread: this binary is also every workspace's warm
    // exec host, and in that role it never returns from here.
    if let Err(error) = cowshed_shell::dispatch() {
        cowshed_os::eprintln!(
            "cowshed: cannot locate this executable for warm workspace shells: {error}"
        );
    }
    cowshed_os::timing::start_clock();
    let arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
    let invocation = match prepare(arguments) {
        Ok(invocation) => invocation,
        Err(code) => std::process::exit(code),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let code = invocation.fail(cowshed_core::CowshedError::internal(format!(
                "cannot start the async runtime: {error}"
            )));
            std::process::exit(code);
        }
    };
    let ending = runtime.block_on(run_interruptible(invocation));
    if let Ending::Interrupted { signal } = ending {
        // Shutting the runtime down drops every task it still holds. A workspace supervisor is
        // one, and dropping it ends the process group of each job it still runs: once this
        // process is gone nothing could observe or cancel them. A finished command skips this on
        // purpose, so the jobs it backgrounded keep running after it exits.
        runtime.shutdown_timeout(INTERRUPTED_SHUTDOWN_GRACE);
        cowshed_os::stderr::flush();
        die_by(signal);
    }
    // A daemon's stderr lines still queued are written before it goes.
    cowshed_os::stderr::flush();
    std::process::exit(ending.exit_code());
}

/// End this process by `signal` itself, so a parent shell sees the interrupt and stops too; a
/// plain exit status would let a loop around this command carry on after Ctrl-C.
fn die_by(signal: i32) {
    // SAFETY: SIG_DFL installs no handler code, and `raise` delivers the signal to this thread,
    // whose default action for every signal passed here terminates the process.
    let reset = unsafe { libc::signal(signal, libc::SIG_DFL) };
    if reset == libc::SIG_ERR {
        cowshed_os::eprintln!(
            "cowshed: cannot restore the default action for signal {signal}: {}",
            std::io::Error::last_os_error()
        );
        return;
    }
    // SAFETY: see above; `raise` takes no memory operands.
    if unsafe { libc::raise(signal) } != 0 {
        cowshed_os::eprintln!(
            "cowshed: cannot raise signal {signal}: {}",
            std::io::Error::last_os_error()
        );
    }
}
