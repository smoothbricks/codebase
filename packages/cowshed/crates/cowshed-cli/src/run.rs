//! The CLI entrypoint, expressed as a library function.
//!
//! The `cowshed` binary drives the CLI through [`run_interruptible`], and a process that hosts
//! it in-process through [`run`]. Keeping dispatch here — rather than in `main.rs` — is what lets
//! the npm package ship a CLI without a second, separately cross-compiled executable: the addon
//! already builds for every supported target, so the CLI rides along in that same artifact.

use std::ffi::{OsStr, OsString};
use std::io;

use tokio::signal::unix::{Signal, SignalKind, signal};

use cowshed_core::CowshedError;
use cowshed_gateway::{GATEWAY_GIT_FETCH_HELPER_ARG, run_gateway_git_fetch_helper};

use crate::capabilities::sccache;
use crate::{
    args, controller_service, credential_service, gateway_service, help, identity_service, output,
    runtime, setup_service, skill,
};

/// How one invocation ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ending {
    /// The command ran to its end. Jobs it backgrounded outlive the process by design.
    Finished(i32),
    /// A signal cut the command short. Nothing will observe or cancel the jobs it started once
    /// this process is gone, so the process must end them before it exits.
    Interrupted { signal: i32 },
}

impl Ending {
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Finished(code) => code,
            Self::Interrupted { signal } => 128 + signal,
        }
    }
}

/// Who answers the process's termination signals while a project or host command runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InterruptPolicy {
    /// An in-process host owns its signals; cowshed installs no handler it could never remove.
    LeaveToHost,
    /// The `cowshed` process itself: an interrupt ends the command and the jobs it started.
    EndJobs,
}

/// Run one CLI invocation inside a host process. `arguments` excludes argv[0].
///
/// Returns the process exit code and never touches signal dispositions, because an in-process
/// host must keep its own and be allowed to flush and unwind normally.
pub async fn run(arguments: Vec<OsString>) -> i32 {
    run_with(arguments, InterruptPolicy::LeaveToHost)
        .await
        .exit_code()
}

/// Run one CLI invocation as the `cowshed` process. `arguments` excludes argv[0].
///
/// A project or host command watches SIGINT, SIGHUP and SIGTERM — each one the process did not
/// inherit as ignored — and answers [`Ending::Interrupted`] when one arrives. The caller owns
/// the async runtime, and shutting it down is what ends the interrupted command's jobs.
pub async fn run_interruptible(arguments: Vec<OsString>) -> Ending {
    run_with(arguments, InterruptPolicy::EndJobs).await
}

async fn run_with(arguments: Vec<OsString>, interrupts: InterruptPolicy) -> Ending {
    if arguments
        .first()
        .is_some_and(|argument| argument == OsStr::new(GATEWAY_GIT_FETCH_HELPER_ARG))
    {
        if arguments.len() != 1 {
            eprintln!("cowshed: the internal gateway git helper accepts no arguments");
            return Ending::Finished(2);
        }
        return Ending::Finished(match run_gateway_git_fetch_helper() {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("cowshed: gateway git helper failed: {error}");
                1
            }
        });
    }
    match parse_then_invoke_service(&arguments, |parsed| run_parsed(parsed, interrupts)).await {
        Ok(ending) => ending,
        Err(error) => {
            let command_map = error.command_map();
            // Only a refused line needs the argv walked: an accepted one answers from
            // `parsed.global`, so scanning it up front was three passes nothing read.
            let globals = args::globals_before_child_argv(&arguments);
            let error = CowshedError::usage(error.message, error.hint);
            Ending::Finished(emit_error(error, command_map, globals.json, globals.quiet))
        }
    }
}

async fn parse_then_invoke_service<F, Fut, T>(
    arguments: &[OsString],
    invoke: F,
) -> Result<T, args::UsageError>
where
    F: FnOnce(args::Cli) -> Fut,
    Fut: Future<Output = T>,
{
    let parsed = args::parse_args(arguments)?;
    Ok(invoke(parsed).await)
}

async fn run_parsed(parsed: args::Cli, interrupts: InterruptPolicy) -> Ending {
    Ending::Finished(match run_command(parsed, interrupts).await {
        Ok(code) => code,
        Err(signal) => return Ending::Interrupted { signal },
    })
}

/// Runs one parsed command to its exit code, or answers the signal that interrupted it.
async fn run_command(parsed: args::Cli, interrupts: InterruptPolicy) -> Result<i32, i32> {
    let json = parsed.global.json;
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut output = output::Output::new(stdout, stderr, parsed.global.quiet);
    // Help is an answer, not a diagnostic: it goes to stdout, exits 0, and is never suppressed by
    // --quiet, because it is the output the caller asked for.
    if let args::Command::Help(topic) = &parsed.command {
        let page = match topic {
            Some(spec) => spec.page(),
            None => help::overview(),
        };
        return Ok(match output.bare(page.as_bytes()) {
            Ok(()) => 0,
            Err(write_error) => {
                eprintln!("cowshed: failed to write command result: {write_error}");
                1
            }
        });
    }

    if matches!(parsed.command, args::Command::Version) {
        let version = format!("cowshed {}\n", args::package_version());
        return Ok(match output.bare(version.as_bytes()) {
            Ok(()) => 0,
            Err(write_error) => {
                eprintln!("cowshed: failed to write command result: {write_error}");
                1
            }
        });
    }
    if let args::Command::Skill(skill_args) = &parsed.command {
        let outcome = skill::dispatch(skill_args, &parsed.global, &mut output);
        return Ok(finish(outcome, &mut output, json));
    }
    if matches!(parsed.command, args::Command::BuildState) {
        let outcome = match runtime::resolve_project_root(&parsed).await {
            Ok(root) => crate::build_state::dispatch(&root, json, &mut output),
            Err(error) => Err(error),
        };
        return Ok(finish(outcome, &mut output, json));
    }
    if let args::Command::Gateway(action) = &parsed.command {
        let outcome = gateway_service::dispatch(*action, parsed.global.json, &mut output).await;
        return Ok(finish(outcome, &mut output, json));
    }
    // An embedding process's controller: a service over the socket it was handed that lasts as long
    // as that process keeps its end open, so like the host services above it sits outside the
    // interruptible section below. The socket is taken first, so a terminal on stdin is refused
    // before the project is resolved or opened.
    if matches!(parsed.command, args::Command::Controller) {
        let outcome = async {
            let socket = controller_service::take_inherited_socket()?;
            let root = runtime::resolve_project_root(&parsed).await?;
            controller_service::serve(&root, socket).await
        }
        .await
        .map(|()| 0);
        return Ok(finish(outcome, &mut output, json));
    }
    // Enrolment is a host operation with a project subject: it needs the repository identity a
    // credential record binds to, and nothing else the project bridge provides.
    if let args::Command::Credential(action) = parsed.command.clone() {
        let outcome = match runtime::resolve_project_root(&parsed).await {
            Ok(root) => {
                credential_service::dispatch(action, &root, parsed.global.json, &mut output).await
            }
            Err(error) => Err(error),
        };
        return Ok(finish(outcome, &mut output, json));
    }
    // Binding an identity is the same shape: a project subject, the binding record, and the main
    // checkout's remotes — nothing the project bridge adds.
    if let args::Command::Identity(action) = parsed.command.clone() {
        let outcome = match runtime::resolve_project_root(&parsed).await {
            Ok(root) => {
                identity_service::dispatch(action, &root, parsed.global.json, &mut output).await
            }
            Err(error) => Err(error),
        };
        return Ok(finish(outcome, &mut output, json));
    }
    if let args::Command::Sccache(action) = &parsed.command {
        let outcome =
            sccache::service::dispatch(action.clone(), parsed.global.json, &mut output).await;
        return Ok(finish(outcome, &mut output, json));
    }
    // `setup` has no project and no workspace: its subject is the host, so it dispatches here
    // beside the other host services rather than through the project runtime bridge.
    if let args::Command::Setup(setup_args) = &parsed.command {
        let outcome =
            setup_service::dispatch_native(setup_args, parsed.global.json, &mut output).await;
        return Ok(finish(outcome, &mut output, json));
    }
    // Project and host commands run the controller in this process, and its workspace supervisors
    // run jobs as children in their own process groups, out of reach of the terminal's signals.
    // The long-running services above own their signals, so only this section is interruptible.
    let mut interrupts = match interrupts {
        InterruptPolicy::LeaveToHost => Interrupts::none(),
        InterruptPolicy::EndJobs => Interrupts::install(),
    };
    let outcome = {
        let command = async {
            match runtime_dispatch(&parsed.command) {
                RuntimeDispatch::Host => runtime::run_host_command(parsed, &mut output).await,
                RuntimeDispatch::Project => {
                    runtime::run_bridge_command(parsed, tokio::io::stdin(), &mut output).await
                }
            }
            .map(|exit| exit.code)
        };
        tokio::select! {
            outcome = command => outcome,
            signal = interrupts.received() => return Err(signal),
        }
    };
    Ok(finish(outcome, &mut output, json))
}

/// The signals that end an interactive command early: Ctrl-C, a closed terminal, a polite kill.
///
/// A signal the process inherited as ignored stays ignored: `nohup`, or a background job of a
/// non-interactive shell, asked for exactly that, and its jobs are meant to outlive the caller.
struct Interrupts {
    interrupt: Option<Signal>,
    hangup: Option<Signal>,
    terminate: Option<Signal>,
}

impl Interrupts {
    fn none() -> Self {
        Self {
            interrupt: None,
            hangup: None,
            terminate: None,
        }
    }

    fn install() -> Self {
        Self {
            interrupt: watch(SignalKind::interrupt(), libc::SIGINT, "SIGINT"),
            hangup: watch(SignalKind::hangup(), libc::SIGHUP, "SIGHUP"),
            terminate: watch(SignalKind::terminate(), libc::SIGTERM, "SIGTERM"),
        }
    }

    /// The number of the first watched signal to arrive; never resolves when none is watched.
    async fn received(&mut self) -> i32 {
        tokio::select! {
            () = arrival(&mut self.interrupt) => libc::SIGINT,
            () = arrival(&mut self.hangup) => libc::SIGHUP,
            () = arrival(&mut self.terminate) => libc::SIGTERM,
        }
    }
}

/// Watches `kind` unless the process inherited it as ignored. A disposition that cannot be read
/// or a handler that cannot be installed is said out loud, and that signal keeps its inherited
/// action.
fn watch(kind: SignalKind, number: libc::c_int, name: &str) -> Option<Signal> {
    match inherited_as_ignored(number) {
        Ok(true) => return None,
        Ok(false) => {}
        Err(error) => {
            eprintln!(
                "cowshed: cannot read the {name} disposition ({error}); a {name} will leave the \
                 jobs this command started running"
            );
            return None;
        }
    }
    match signal(kind) {
        Ok(watched) => Some(watched),
        Err(error) => {
            eprintln!(
                "cowshed: cannot watch for {name} ({error}); a {name} will leave the jobs this \
                 command started running"
            );
            None
        }
    }
}

fn inherited_as_ignored(number: libc::c_int) -> io::Result<bool> {
    let mut current = std::mem::MaybeUninit::<libc::sigaction>::uninit();
    // SAFETY: with a null new action `sigaction` only reads: it writes the current disposition of
    // `number` into `current`, which is valid for one `sigaction` struct, and changes nothing.
    if unsafe { libc::sigaction(number, std::ptr::null(), current.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `sigaction` returned 0, so it initialised every field of `current`.
    let current = unsafe { current.assume_init() };
    Ok(current.sa_sigaction == libc::SIG_IGN)
}

async fn arrival(signal: &mut Option<Signal>) {
    if let Some(signal) = signal
        && signal.recv().await.is_some()
    {
        return;
    }
    std::future::pending().await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeDispatch {
    Host,
    Project,
}

/// Select host commands before anything is allowed to discover or open a project.
///
/// The store-wide forms are entirely described by their parsed flags. Routing them here keeps a
/// broken cwd checkout (including one whose recorded Git remote no longer exists) out of the
/// execution path instead of asking project discovery to fail before the host operation starts.
fn runtime_dispatch(command: &args::Command) -> RuntimeDispatch {
    match command {
        args::Command::Attach(args) if args.all => RuntimeDispatch::Host,
        // `mount main --repo-id` resolves from store records, never from a
        // live checkout, so it must survive a broken cwd like the other host
        // commands rather than ask project discovery to fail first.
        args::Command::Mount(_) => RuntimeDispatch::Host,
        args::Command::Detach(args) if args.all => RuntimeDispatch::Host,
        args::Command::List(args) if args.all => RuntimeDispatch::Host,
        _ => RuntimeDispatch::Project,
    }
}

/// Turn one command's outcome into this process's exit code, reporting a failure in whichever
/// format the caller asked for. Every dispatch path ends here so none of them can grow its own
/// idea of how an error is written.
fn finish<W: io::Write, E: io::Write>(
    outcome: Result<i32, CowshedError>,
    output: &mut output::Output<W, E>,
    json: bool,
) -> i32 {
    match outcome {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let exit_code = i32::from(error.exit_code());
            if let Err(write_error) = write_error(output, error, json, None) {
                eprintln!("cowshed: failed to write command result: {write_error}");
                1
            } else {
                exit_code
            }
        }
    }
}

fn emit_error(error: CowshedError, command_map: Option<&str>, json: bool, quiet: bool) -> i32 {
    let exit_code = i32::from(error.exit_code());
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut output = output::Output::new(stdout, stderr, quiet);
    if let Err(write_error) = write_error(&mut output, error, json, command_map) {
        eprintln!("cowshed: failed to write command result: {write_error}");
        1
    } else {
        exit_code
    }
}

fn write_error<W: io::Write, E: io::Write>(
    output: &mut output::Output<W, E>,
    error: CowshedError,
    json: bool,
    command_map: Option<&str>,
) -> io::Result<()> {
    if json {
        return output.json_error(error);
    }
    output.error(&error.message)?;
    if let Some(command_map) = command_map {
        output.error(command_map)?;
    }
    output.hint(&error.hint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn parser_invalid_invocations_never_invoke_a_service() {
        let invocations = [
            vec!["--json", "exec", "raven", "--unknown"],
            Vec::new(),
            vec![
                "exec",
                "raven",
                "--stdin",
                "--stdin-file",
                "input",
                "--",
                "--json",
            ],
        ];

        for invocation in invocations {
            let service_invoked = Cell::new(false);
            let argv: Vec<OsString> = invocation.into_iter().map(OsString::from).collect();
            let result = parse_then_invoke_service(&argv, |_| {
                service_invoked.set(true);
                async { 0 }
            })
            .await;

            assert!(result.is_err());
            assert!(!service_invoked.get());
        }
    }

    /// `nohup` hands its command SIGHUP ignored. Watching it anyway would let a closed terminal
    /// end a command, and the jobs it started, that the caller asked to outlive the terminal.
    /// SIGINT starts at its default action whatever this test process inherited, so the watched
    /// case is exercised too, and SIGHUP is put back afterwards; the SIGINT and SIGTERM watches
    /// stay installed, which only matters where tests share a process (nextest gives each its own).
    #[tokio::test]
    async fn a_signal_inherited_as_ignored_stays_ignored() {
        // SAFETY: SIG_DFL and SIG_IGN install no handler code; the calls only change this
        // process's dispositions for SIGINT and SIGHUP, and SIGHUP's is restored below.
        let interrupt = unsafe { libc::signal(libc::SIGINT, libc::SIG_DFL) };
        assert_ne!(interrupt, libc::SIG_ERR, "set SIGINT to its default");
        // SAFETY: see above.
        let hangup = unsafe { libc::signal(libc::SIGHUP, libc::SIG_IGN) };
        assert_ne!(hangup, libc::SIG_ERR, "set SIGHUP ignored");

        let interrupts = Interrupts::install();
        let hangup_still_ignored = inherited_as_ignored(libc::SIGHUP);
        // SAFETY: restores the disposition this test found, a value `signal` itself returned.
        let restored = unsafe { libc::signal(libc::SIGHUP, hangup) };
        assert_ne!(restored, libc::SIG_ERR, "restore SIGHUP");

        assert!(interrupts.hangup.is_none(), "an ignored SIGHUP was watched");
        assert!(interrupts.interrupt.is_some(), "SIGINT was not watched");
        assert!(
            hangup_still_ignored.expect("read SIGHUP disposition"),
            "installing the watch replaced the inherited SIG_IGN"
        );
    }

    #[test]
    fn store_wide_dispatch_is_selected_before_any_project_open() {
        let parsed = args::parse_args([
            "--project",
            "/adopted/checkout/whose-recorded-remote-is-stale",
            "attach",
            "--all",
        ])
        .expect("attach --all parses without opening the project");
        assert_eq!(runtime_dispatch(&parsed.command), RuntimeDispatch::Host);
        assert_eq!(
            parsed.command.project_discovery(),
            args::ProjectDiscovery::NotUsed
        );

        let bare = args::parse_args(["attach"]).expect("bare attach parses");
        assert_eq!(runtime_dispatch(&bare.command), RuntimeDispatch::Project);
        assert_eq!(
            bare.command.project_discovery(),
            args::ProjectDiscovery::Required
        );

        let named = args::parse_args(["attach", "raven"]).expect("named attach parses");
        assert_eq!(runtime_dispatch(&named.command), RuntimeDispatch::Project);
        assert_eq!(
            named.command.project_discovery(),
            args::ProjectDiscovery::Required
        );

        let detach_all = args::parse_args(["detach", "--all"]).expect("detach --all parses");
        assert_eq!(runtime_dispatch(&detach_all.command), RuntimeDispatch::Host);
    }
}
