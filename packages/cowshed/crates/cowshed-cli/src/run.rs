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
use cowshed_core::runtime::wait_spans::WaitRecording;
use cowshed_gateway::{GATEWAY_GIT_FETCH_HELPER_ARG, run_gateway_git_fetch_helper};

use crate::capabilities::sccache;
use crate::{
    args, controller_service, credential_service, gateway_service, help, identity_service,
    install_basis, output, runtime, setup_service, skill,
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
pub(crate) enum InterruptPolicy {
    /// An in-process host owns its signals; cowshed installs no handler it could never remove.
    LeaveToHost,
    /// The `cowshed` process itself: an interrupt ends the command and the jobs it started.
    EndJobs,
}

/// The clause a signal's warning ends with when cowshed cannot watch for it, for a command whose
/// jobs the signal ends.
const JOBS_FATE: &str = "will leave the jobs this command started running";
/// The same clause for a command a signal is forwarded to. Cowshed is the process that holds the
/// turn, so when it cannot watch for the signal the signal ends cowshed and the turn with it.
const FORWARDED_FATE: &str =
    "will end cowshed and give up the land turn while its command may keep running";
/// The same clause for a held command: cowshed answers for its group, so when it cannot watch for
/// the signal the signal ends cowshed and leaves the group running unheld.
const HELD_FATE: &str = "will end cowshed and leave its held command running unheld";

impl InterruptPolicy {
    /// The signal watch this policy calls for, for a command whose jobs a signal ends. The caller
    /// arms it where the command's own signal handling begins.
    pub(crate) fn arm(self) -> Interrupts {
        self.arm_with(JOBS_FATE)
    }

    /// The signal watch this policy calls for, for a command a signal is forwarded to: it keeps
    /// running and holds its turn until it ends.
    pub(crate) fn arm_forwarding(self) -> Interrupts {
        self.arm_with(FORWARDED_FATE)
    }

    /// The signal watch this policy calls for, for a held command: each signal is sent to its
    /// group through the fence, and cowshed holds the leader until it is released.
    pub(crate) fn arm_holding(self) -> Interrupts {
        self.arm_with(HELD_FATE)
    }

    fn arm_with(self, fate: &'static str) -> Interrupts {
        match self {
            Self::LeaveToHost => Interrupts::none(),
            Self::EndJobs => Interrupts::install(fate),
        }
    }
}

/// One parsed invocation, including ownership of any descriptor the caller handed to it.
/// Construct with [`prepare`] before allocating an async runtime, then consume exactly once.
pub struct Invocation(Prepared);

enum Prepared {
    Command(args::Cli),
    Lock {
        args: args::LockArgs,
        state: crate::lock::State<cowshed_core::api::held_lock::LockWaiterEvent>,
        quiet: bool,
    },
    Hold {
        args: args::HoldArgs,
        state: crate::lock::State<cowshed_core::api::hold::HoldEvent>,
        control: crate::hold::Control,
        quiet: bool,
    },
    GatewayGitFetch,
    WorkspaceSupervisor(Vec<OsString>),

    SeedCompaction(Vec<OsString>),
}

impl Invocation {
    /// Report a failure to start the runtime on the invocation's owned state channel too.
    pub fn fail(mut self, error: CowshedError) -> i32 {
        let quiet = match &mut self.0 {
            Prepared::Lock { state, quiet, .. } => {
                state.failed(&error);
                *quiet
            }
            Prepared::Hold { state, quiet, .. } => {
                state.failed(&error);
                *quiet
            }
            Prepared::Command(parsed) => parsed.global.quiet,
            Prepared::GatewayGitFetch
            | Prepared::WorkspaceSupervisor(_)
            | Prepared::SeedCompaction(_) => false,
        };
        let mut output = output::Output::stdio(quiet);
        crate::lock::report(&mut output, &error)
    }
}

/// Parse argv (excluding argv[0]) and adopt caller-owned descriptors synchronously. A refused
/// invocation has already reported its error and returns its exit code. No runtime may be
/// allocated between receiving argv and this call: its descriptors are not caller resources.
pub fn prepare(arguments: Vec<OsString>) -> Result<Invocation, i32> {
    if arguments
        .first()
        .is_some_and(|argument| argument == crate::workspace_supervisor::VERB)
    {
        return Ok(Invocation(Prepared::WorkspaceSupervisor(arguments)));
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == cowshed_core::runtime::SEED_COMPACTION_VERB)
    {
        return Ok(Invocation(Prepared::SeedCompaction(arguments)));
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == OsStr::new(GATEWAY_GIT_FETCH_HELPER_ARG))
    {
        if arguments.len() != 1 {
            cowshed_os::eprintln!("cowshed: the internal gateway git helper accepts no arguments");
            return Err(2);
        }
        return Ok(Invocation(Prepared::GatewayGitFetch));
    }
    match parse_then_prepare(&arguments, |parsed| {
        if let args::Command::Lock(mut lock) = parsed.command {
            let state = crate::lock::State::adopt(lock.state_fd.take()).map_err(|error| {
                let mut output = output::Output::stdio(parsed.global.quiet);
                crate::lock::report(&mut output, &error)
            })?;
            Ok(Invocation(Prepared::Lock {
                args: lock,
                state,
                quiet: parsed.global.quiet,
            }))
        } else if let args::Command::Hold(hold) = parsed.command {
            let report = |error: &CowshedError| {
                let mut output = output::Output::stdio(parsed.global.quiet);
                crate::lock::report(&mut output, error)
            };
            let mut state =
                crate::lock::State::adopt(Some(hold.state_fd)).map_err(|error| report(&error))?;
            let control = crate::hold::Control::adopt(hold.control_fd).map_err(|error| {
                state.failed(&error);
                report(&error)
            })?;
            Ok(Invocation(Prepared::Hold {
                args: hold,
                state,
                control,
                quiet: parsed.global.quiet,
            }))
        } else {
            Ok(Invocation(Prepared::Command(parsed)))
        }
    }) {
        Ok(invocation) => invocation,
        Err(error) => {
            let command_map = error.command_map();
            // Only a refused line needs argv walked; accepted commands use parsed globals.
            let globals = args::globals_before_child_argv(&arguments);
            let error = CowshedError::usage(error.message, error.hint);
            Err(emit_error(error, command_map, globals.json, globals.quiet))
        }
    }
}

fn parse_then_prepare<F, T>(arguments: &[OsString], prepare: F) -> Result<T, args::UsageError>
where
    F: FnOnce(args::Cli) -> T,
{
    let parsed = args::parse_args(arguments)?;
    Ok(prepare(parsed))
}

/// Run a prepared invocation inside a host process, leaving signal dispositions to the host,
/// which must keep its own and be allowed to flush and unwind normally. Exception: `subtree-root`
/// explicitly takes ownership of its named inherited descriptor and execs, replacing the entire
/// host process after publishing admission. It does not return on successful exec.
pub async fn run(invocation: Invocation) -> i32 {
    run_with(invocation, InterruptPolicy::LeaveToHost)
        .await
        .exit_code()
}

/// Run a prepared invocation as the `cowshed` process. Project and host commands watch SIGINT,
/// SIGHUP and SIGTERM unless inherited ignored. The caller owns runtime shutdown on interruption.
pub async fn run_interruptible(invocation: Invocation) -> Ending {
    run_with(invocation, InterruptPolicy::EndJobs).await
}

async fn run_with(invocation: Invocation, interrupts: InterruptPolicy) -> Ending {
    match invocation.0 {
        Prepared::Command(parsed) => run_parsed(parsed, interrupts).await,
        Prepared::Lock { args, state, quiet } => {
            let mut output = output::Output::stdio(quiet);
            Ending::Finished(crate::lock::dispatch(&args, state, &mut output, interrupts).await)
        }
        Prepared::Hold {
            args,
            state,
            control,
            quiet,
        } => {
            let mut output = output::Output::stdio(quiet);
            Ending::Finished(
                crate::hold::dispatch(&args, state, control, &mut output, interrupts).await,
            )
        }
        Prepared::WorkspaceSupervisor(arguments) => {
            Ending::Finished(crate::workspace_supervisor::run(&arguments[1..]).await)
        }

        Prepared::SeedCompaction(arguments) => {
            Ending::Finished(crate::seed_compaction::run(&arguments[1..]).await)
        }
        Prepared::GatewayGitFetch => Ending::Finished(match run_gateway_git_fetch_helper() {
            Ok(()) => 0,
            Err(error) => {
                cowshed_os::eprintln!("cowshed: gateway git helper failed: {error}");
                1
            }
        }),
    }
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
    let mut output = output::Output::stdio(parsed.global.quiet);
    // A wait that expires under this command is sealed as its span in the host's telemetry, in the
    // trace its caller exported (13_telemetry.md, "Wait spans").
    let waits = wait_spans();
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
                cowshed_os::eprintln!("cowshed: failed to write command result: {write_error}");
                1
            }
        });
    }

    if matches!(parsed.command, args::Command::Version) {
        let version = format!("cowshed {}\n", args::package_version());
        return Ok(match output.bare(version.as_bytes()) {
            Ok(()) => 0,
            Err(write_error) => {
                cowshed_os::eprintln!("cowshed: failed to write command result: {write_error}");
                1
            }
        });
    }
    // A disk tool for a caller that is not cowshed: no project and no daemon, so it is answered
    // before anything asks the daemon and works on a host whose daemon runs another build or
    // none at all. It writes the tool's bytes itself; only the runner's own failures are
    // cowshed's to report.
    if let args::Command::DiskCommand(disk) = &parsed.command {
        return Ok(crate::disk_command::dispatch(disk, &mut output).await);
    }
    // A land turn queues behind the repository's other lands and may wait a long time, so like
    // disk-command it is answered before anything asks the daemon.
    if let args::Command::LandTurn(land) = &parsed.command {
        return Ok(crate::land_turn::dispatch(land, &mut output, interrupts).await);
    }
    // A doc commit edits main under its ref turn alone: no daemon, no gateway, no project open,
    // so a hung gateway never holds it (02_workspaces.md "`cowshed doc-commit`").
    if let args::Command::DocCommit(doc) = &parsed.command {
        let outcome =
            crate::doc_commit::dispatch(doc, parsed.global.project.as_deref(), json, &mut output)
                .await;
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    // These verbs address the already-running job supervisor directly. In particular, admission
    // authenticates this wrapper before exec replaces its image without changing its life.
    if let args::Command::SubtreeRoot(root) = &parsed.command {
        let outcome = crate::subtree::root(root).await;
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    if let args::Command::RetireDescendants(retire) = &parsed.command {
        let outcome = crate::subtree::retire(retire, &mut output).await;
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    // Lock invocations were separated during synchronous preparation to own their state channel.
    // Observations read only the kernel, before anything asks the daemon.
    if let args::Command::LockHolders(holders) = &parsed.command {
        return Ok(crate::lock::dispatch_holders(holders, &mut output));
    }
    if let args::Command::TreeHolders(tree) = &parsed.command {
        return Ok(crate::tree_holders::dispatch(tree, &mut output));
    }
    if let args::Command::FdHolders(request) = &parsed.command {
        return Ok(crate::fd_holders::dispatch(request, &mut output));
    }
    if let args::Command::Skill(skill_args) = &parsed.command {
        let outcome = skill::dispatch(skill_args, &parsed.global, &mut output);
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    if matches!(parsed.command, args::Command::BuildState) {
        let outcome = match parsed.global.project.as_deref() {
            Some(start) => crate::build_state::dispatch(start, json, &mut output),
            None => std::env::current_dir()
                .map_err(|error| {
                    CowshedError::environment_missing(
                        format!("could not determine the current directory: {error}"),
                        "use --project <git-root>",
                    )
                })
                .and_then(|start| crate::build_state::dispatch(&start, json, &mut output)),
        };
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    if let args::Command::Gateway(action) = &parsed.command {
        let outcome = gateway_service::dispatch(*action, parsed.global.json, &mut output).await;
        return Ok(finish(outcome, &mut output, json, &waits));
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
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    // An embedding process's host connection: like the controller it lasts as long as that
    // process keeps its end open. It names no project, so nothing is resolved before it serves.
    if let args::Command::Host(role) = parsed.command {
        let outcome = async {
            let socket = controller_service::take_inherited_socket()?;
            crate::host_service::serve(socket, role).await
        }
        .await
        .map(|()| 0);
        return Ok(finish(outcome, &mut output, json, &waits));
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
        return Ok(finish(outcome, &mut output, json, &waits));
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
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    if let args::Command::Sccache(action) = &parsed.command {
        let outcome =
            sccache::service::dispatch(action.clone(), parsed.global.json, &mut output).await;
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    // `setup` has no project and no workspace: its subject is the host, so it dispatches here
    // beside the other host services rather than through the project runtime bridge.
    if let args::Command::Setup(setup_args) = &parsed.command {
        let outcome =
            setup_service::dispatch_native(setup_args, parsed.global.json, &mut output).await;
        return Ok(finish(outcome, &mut output, json, &waits));
    }
    let prepared = match runtime::prepare_project_before_control(&parsed).await {
        Ok(prepared) => prepared,
        Err(error) => return Ok(finish(Err(error), &mut output, json, &waits)),
    };
    // A land step that starts while an install holds the slot waits for that install, and then
    // holds nothing (`install_basis::wait_for_install`): no install ever waits on a land step.
    // Waited for before the build check, so the step judges the daemon the install left.
    let mut waited_in = None;
    if is_land_step(&parsed.command) {
        let waited = async {
            let home = cowshed_core::gateway_sessions::canonical_home()?;
            let support = cowshed_core::host_dirs::support_directory(&home);
            let waited = install_basis::wait_for_install(&support).await?;
            Ok::<_, CowshedError>(waited.then_some(support))
        }
        .await;
        match waited {
            Ok(support) => waited_in = support,
            Err(error) => return Ok(finish(Err(error), &mut output, json, &waits)),
        }
    }
    // The daemon refuses another build's cowshed at the first supervisor a verb asks for, which
    // is after the verb's first step. Ask it now, before anything is changed.
    if let Err(error) = runtime::require_daemon_of_this_build(&parsed.command).await {
        // A step the install it waited for replaced goes on as the installed build, in place.
        if let (Some(support), Some(_)) = (&waited_in, error.other_build_source()) {
            return Ok(finish(
                Err(continue_as_installed(support)),
                &mut output,
                json,
                &waits,
            ));
        }
        return Ok(finish(Err(error), &mut output, json, &waits));
    }
    // Project and host commands run the controller in this process, and its workspace supervisors
    // run jobs as children in their own process groups, out of reach of the terminal's signals.
    // The long-running services above own their signals, so only this section is interruptible.
    let mut interrupts = interrupts.arm();
    let outcome = {
        let command = async {
            match runtime_dispatch(&parsed.command) {
                RuntimeDispatch::Host => runtime::run_host_command(parsed, &mut output).await,
                RuntimeDispatch::Project => {
                    runtime::run_bridge_command(parsed, prepared, tokio::io::stdin(), &mut output)
                        .await
                }
            }
            .map(|exit| exit.code)
        };
        tokio::select! {
            outcome = command => outcome,
            signal = interrupts.received() => return Err(signal),
        }
    };
    Ok(finish(outcome, &mut output, json, &waits))
}

/// Replace this process with the installed cowshed, run with this process's own arguments
/// ([`install_basis::continue_as_installed`]). Returns only when that fails.
fn continue_as_installed(support: &std::path::Path) -> CowshedError {
    use std::os::unix::process::CommandExt as _;
    let mut command = match install_basis::continue_as_installed(support) {
        Ok(command) => command,
        Err(error) => return error,
    };
    cowshed_os::eprintln!(
        "cowshed: the install this land step waited for replaced its build; it goes on as the \
         installed cowshed {}",
        command.get_program().to_string_lossy()
    );
    let error = command.exec();
    CowshedError::environment_missing(
        format!(
            "an install replaced this cowshed while the land step waited, and the installed \
             cowshed {} could not be run in its place: {error}",
            command.get_program().to_string_lossy()
        ),
        "rerun the land step with the installed cowshed",
    )
}

/// The signals that end an interactive command early: Ctrl-C, a closed terminal, a polite kill.
///
/// A signal the process inherited as ignored stays ignored: `nohup`, or a background job of a
/// non-interactive shell, asked for exactly that, and its jobs are meant to outlive the caller.
pub(crate) struct Interrupts {
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

    fn install(fate: &'static str) -> Self {
        Self {
            interrupt: watch(SignalKind::interrupt(), libc::SIGINT, "SIGINT", fate),
            hangup: watch(SignalKind::hangup(), libc::SIGHUP, "SIGHUP", fate),
            terminate: watch(SignalKind::terminate(), libc::SIGTERM, "SIGTERM", fate),
        }
    }

    /// The number of the first watched signal to arrive; never resolves when none is watched.
    pub(crate) async fn received(&mut self) -> i32 {
        tokio::select! {
            () = arrival(&mut self.interrupt) => libc::SIGINT,
            () = arrival(&mut self.hangup) => libc::SIGHUP,
            () = arrival(&mut self.terminate) => libc::SIGTERM,
        }
    }
}

/// Watches `kind` unless the process inherited it as ignored. A disposition that cannot be read
/// or a handler that cannot be installed is said out loud, with `fate` telling what the signal
/// then does to the command, and that signal keeps its inherited action.
fn watch(kind: SignalKind, number: libc::c_int, name: &str, fate: &str) -> Option<Signal> {
    match inherited_as_ignored(number) {
        Ok(true) => return None,
        Ok(false) => {}
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: cannot read the {name} disposition ({error}); a {name} {fate}"
            );
            return None;
        }
    }
    match signal(kind) {
        Ok(watched) => Some(watched),
        Err(error) => {
            cowshed_os::eprintln!("cowshed: cannot watch for {name} ({error}); a {name} {fate}");
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
        // `forget` names its project by `<owner/repo>` and exists for the one whose checkout is
        // gone, so project discovery must never run before it.
        args::Command::Forget(_) => RuntimeDispatch::Host,
        args::Command::Detach(args) if args.all => RuntimeDispatch::Host,
        args::Command::List(args) if args.all => RuntimeDispatch::Host,
        _ => RuntimeDispatch::Project,
    }
}

/// The commands a land runs, each of which waits for an install in progress before it starts:
/// its gates (`exec --class gate`), its rebase and its mutating land. Read-only preflight
/// and ordinary work do not wait for an install.
fn is_land_step(command: &args::Command) -> bool {
    match command {
        args::Command::Exec(exec) => exec.class == cowshed_core::api::dto::ExecClass::Gate,
        args::Command::Rebase(_) => true,
        args::Command::Land(args) => !args.preflight,
        _ => false,
    }
}

/// Turn one command's outcome into this process's exit code, reporting a failure in whichever
/// format the caller asked for. Every dispatch path ends here so none of them can grow its own
/// idea of how an error is written. A failure that is an expired wait is also sealed as its span
/// through `waits` (05_gateway.md, "Deadlines", Spans).
fn finish<W: io::Write, E: io::Write>(
    outcome: Result<i32, CowshedError>,
    output: &mut output::Output<W, E>,
    json: bool,
    waits: &WaitRecording,
) -> i32 {
    match outcome {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let error = waits.record(error);
            let exit_code = i32::from(error.exit_code());
            if let Err(write_error) = write_error(output, error, json, None) {
                cowshed_os::eprintln!("cowshed: failed to write command result: {write_error}");
                1
            } else {
                exit_code
            }
        }
    }
}

/// The command's wait recording: the host's telemetry root and the trace its caller exported, also
/// used before a relay releases its hold.
pub(crate) fn wait_spans() -> WaitRecording {
    WaitRecording::host()
}
fn emit_error(error: CowshedError, command_map: Option<&str>, json: bool, quiet: bool) -> i32 {
    let exit_code = i32::from(error.exit_code());
    let mut output = output::Output::stdio(quiet);
    if let Err(write_error) = write_error(&mut output, error, json, command_map) {
        cowshed_os::eprintln!("cowshed: failed to write command result: {write_error}");
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

    /// One real shell wiring check: our SILENT value reaches the unprivileged sampler and one
    /// private retained receipt; the final error boundary consumes that same receipt.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_silent_supervisor_retains_one_stack_sample_and_one_wait_receipt() {
        use cowshed_core::runtime::wait_spans::WaitFailure;
        use cowshed_os::deadline::{Progress, Silent};
        use std::os::unix::fs::PermissionsExt as _;
        use std::time::Duration;
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::scratch_parent(),
            "cswait-sample",
        );
        let waits = WaitRecording::new(
            cowshed_core::runtime::wait_spans::WaitSpans::adopting(root.to_path_buf(), None),
            cowshed_core::runtime::stack_sample::capture,
        );
        let pid = i32::try_from(std::process::id()).unwrap();
        let silent = Silent {
            op: "wait",
            holder: "workspace supervisor",
            idle: Duration::from_secs(15),
            silent_for: Duration::from_secs(15),
            elapsed: Duration::from_secs(265),
            last: Progress::Received { bytes: 3841 },
        };
        let failure = WaitFailure::silent(&silent, Some(pid), "workspace supervisor");
        let error = CowshedError::environment_missing(
            failure.message.clone(),
            "inspect the retained stacks",
        )
        .with_wait(Some(failure));
        let exit_code = i32::from(error.exit_code());
        // The host exec bridge uses this real socketpair path, not the resident direct client.
        let expected = error.wait_source().cloned();
        let local_waits = cowshed_core::api::server::LocalWaitCauses::default();
        let (router, mut commands) =
            cowshed_core::api::server::RouterHandle::channel(std::num::NonZeroUsize::MIN);
        let (client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let served = tokio::spawn(
            cowshed_core::api::server::serve_controller_connection_in_process(
                server.into(),
                cowshed_core::api::server::ConnectionAuthority::Coordinator {
                    repo_id: cowshed_core::repository::RepoId::parse("acme/widget").unwrap(),
                },
                router,
                local_waits.clone(),
            ),
        );
        let (controller, token) =
            cowshed_core::Cowshed::connect_in_process(client.into(), local_waits)
                .await
                .unwrap();
        drop(token);
        let routed = tokio::spawn(async move {
            let (_, reply) = commands.recv().await.unwrap().into_parts();
            assert!(reply.send(Err(error)).is_ok());
        });
        let error = match controller.open(&*root).await {
            Err(error) => error,
            Ok(_) => panic!("the injected route failure was lost"),
        };
        assert_eq!(
            error.wait_source(),
            expected.as_ref(),
            "host loopback retains the typed wait"
        );
        routed.await.unwrap();
        drop(controller);
        served.abort();
        match served.await {
            Ok(Ok(())) => {}
            Err(error) if error.is_cancelled() => {}
            other => panic!("owned controller teardown: {other:?}"),
        }
        let error = waits.record(error);
        let receipt = error.wait_source().unwrap().receipt.clone().unwrap();
        let span = receipt.span.as_ref().unwrap();
        let sample = receipt.sample.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(span.parent(), sample.parent());
        assert_eq!(
            span.file_stem().unwrap().to_str().unwrap(),
            sample
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_suffix(".sample.txt")
                .unwrap()
        );
        assert_eq!(
            std::fs::metadata(sample).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(!std::fs::read(sample).unwrap().is_empty());
        assert!(
            error.message.contains(&sample.display().to_string()),
            "{}",
            error.message
        );
        let mut output = output::Output::new(Vec::new(), Vec::new(), false);
        assert_eq!(finish(Err(error), &mut output, false, &waits), exit_code);
        let entries: Vec<_> = std::fs::read_dir(span.parent().unwrap()).unwrap().collect();
        assert_eq!(
            entries.len(),
            2,
            "only one span and sample, no second recording attempt"
        );
    }

    #[test]
    fn a_lands_gates_rebase_and_land_wait_for_an_install_and_ordinary_work_does_not() {
        let step = |argv: &[&str]| {
            is_land_step(
                &args::parse_args(argv.iter().map(OsString::from))
                    .expect("valid command")
                    .command,
            )
        };
        assert!(step(&[
            "exec", "raven", "--class", "gate", "--host", "--", "gate"
        ]));
        assert!(step(&["rebase", "raven"]));
        assert!(step(&["land", "raven"]));
        assert!(!step(&["land", "raven", "--preflight"]));
        assert!(!step(&[
            "land",
            "raven",
            "--preflight",
            "--into",
            "lane",
            "--ungated"
        ]));
        assert!(!step(&[
            "--json",
            "land",
            "raven",
            "--preflight",
            "--target",
            "main",
            "--push-only"
        ]));
        assert!(!step(&["exec", "raven", "--", "build"]));
        assert!(!step(&["exec", "raven", "--host", "--", "build"]));
        assert!(!step(&[
            "exec", "raven", "--class", "resident", "--", "serve"
        ]));
        assert!(!step(&["ls"]));
    }

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
            let result = parse_then_prepare(&argv, |_| {
                service_invoked.set(true);
                0
            });

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

        let interrupts = Interrupts::install(JOBS_FATE);
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

        // The project a `forget` retires is the one whose checkout is gone: the verb parses with
        // no project, is routed to the host entrypoint, and refuses a checkout it could be steered
        // by, so nothing can make its dispatch depend on one.
        let forget = args::parse_args(["forget", "acme/widget", "--abandon"])
            .expect("forget parses without a project");
        assert_eq!(runtime_dispatch(&forget.command), RuntimeDispatch::Host);
        assert_eq!(
            forget.command.project_discovery(),
            args::ProjectDiscovery::NotUsed
        );
        let steered = [
            "--project",
            "/vanished/checkout",
            "forget",
            "acme/widget",
            "--abandon",
        ];
        assert!(args::parse_args(steered).is_err());
    }

    /// A supervisor that took the hello and never answers (05_gateway.md, "Deadlines"): the hello
    /// expires `TIMEOUT` within the handshake bound, and the command's boundary seals the expiry
    /// as a one-span `cowshed.wait` segment under the trace the CLI adopted from `TRACEPARENT`
    /// (13_telemetry.md, "Wait spans"). Time is paused: the listener never sends a byte, so no
    /// readiness arrives from outside the runtime while the bound runs.
    #[tokio::test(start_paused = true)]
    async fn an_expired_wait_reaches_the_command_boundary_as_a_sealed_span() {
        use arrow_array::cast::AsArray as _;
        use arrow_array::types::{UInt8Type, UInt16Type, UInt32Type, UInt64Type};
        use arrow_array::{Array as _, ArrayAccessor as _};

        let root =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cswait");
        let socket = root.join("s.sock");
        // Bound and never accepted: the kernel completes the connect and takes the hello.
        let _silent = std::os::unix::net::UnixListener::bind(&socket).expect("silent supervisor");
        let telemetry = root.join("telemetry");
        std::fs::create_dir(&telemetry).expect("telemetry root");
        let waits = WaitRecording::new(
            cowshed_core::runtime::wait_spans::WaitSpans::adopting(
                telemetry.clone(),
                Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            ),
            cowshed_core::runtime::stack_sample::capture,
        );

        let outcome = cowshed_core::runtime::supervisor_socket::hello(&socket)
            .await
            .map(|_| 0);
        let mut output = output::Output::new(Vec::new(), Vec::new(), false);
        let code = finish(outcome, &mut output, false, &waits);
        let (_, stderr) = output.into_inner();
        let stderr = String::from_utf8(stderr).expect("utf-8 stderr");
        assert_ne!(code, 0);
        assert!(
            stderr.contains("TIMEOUT: hello on workspace supervisor"),
            "{stderr}"
        );

        let segments: Vec<_> = std::fs::read_dir(&telemetry)
            .expect("telemetry root")
            .flat_map(|partition| {
                std::fs::read_dir(partition.expect("partition").path()).expect("partition")
            })
            .map(|entry| entry.expect("segment").path())
            .collect();
        assert_eq!(segments.len(), 1, "{segments:?}");
        let name = segments[0]
            .file_name()
            .and_then(|name| name.to_str())
            .expect("name");
        assert!(
            name.starts_with("wait-") && name.ends_with(".arrow"),
            "{name}"
        );
        let mut reader = arrow_ipc::reader::StreamReader::try_new(
            std::fs::File::open(&segments[0]).expect("open segment"),
            None,
        )
        .expect("an Arrow stream");
        let batch = reader.next().expect("one batch").expect("a readable batch");
        assert!(reader.next().is_none(), "one batch per segment");
        assert_eq!(batch.num_rows(), 2);
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("{name}"))
        };
        let entry = column("entry_type").as_dictionary::<UInt8Type>();
        let entry = entry
            .downcast_dict::<arrow_array::StringArray>()
            .expect("names");
        assert_eq!((entry.value(0), entry.value(1)), ("span-start", "span-err"));
        let message = column("message").as_dictionary::<UInt32Type>();
        let message = message
            .downcast_dict::<arrow_array::StringArray>()
            .expect("messages");
        assert_eq!(message.value(0), "cowshed.wait");
        assert!(
            message.value(1).starts_with(&format!(
                "TIMEOUT: hello on workspace supervisor at {}",
                socket.display()
            )),
            "{}",
            message.value(1)
        );
        let trace = column("trace_id").as_dictionary::<UInt32Type>();
        let trace = trace
            .downcast_dict::<arrow_array::StringArray>()
            .expect("traces");
        for row in 0..2 {
            assert_eq!(trace.value(row), "4bf92f3577b34da6a3ce929d0e0e4736");
        }
        let threads = column("thread_id").as_primitive::<UInt64Type>();
        assert_eq!(threads.value(0), threads.value(1));
        let spans = column("span_id").as_primitive::<UInt32Type>();
        assert_eq!((spans.value(0), spans.value(1)), (1, 1));
        assert_eq!(column("parent_span_id").null_count(), 2);
        let parent = column("w3c_parent_span_id").as_primitive::<UInt64Type>();
        assert_eq!(parent.value(0), 0x00f0_67aa_0ba9_02b7);

        let utf8 = |name: &str, row: usize| {
            let values = column(name).as_string::<i32>();
            (!values.is_null(row)).then(|| values.value(row).to_owned())
        };
        let code = column("wait_code").as_dictionary::<UInt16Type>();
        let code = code
            .downcast_dict::<arrow_array::StringArray>()
            .expect("codes");
        assert!(
            code.is_null(0),
            "the start row does not know how the wait ends"
        );
        assert_eq!(code.value(1), "TIMEOUT");
        assert_eq!(utf8("wait_op", 0).as_deref(), Some("hello"));
        assert_eq!(utf8("wait_class", 0).as_deref(), Some("handshake"));
        assert!(
            utf8("wait_peer", 0).is_some_and(|peer| peer.contains(&socket.display().to_string())),
            "{:?}",
            utf8("wait_peer", 0)
        );
        let pid = column("wait_peer_pid").as_primitive::<UInt32Type>();
        assert_eq!(
            pid.value(0),
            std::process::id(),
            "the listener is this process"
        );
        let bound = column("wait_bound_us").as_primitive::<UInt64Type>();
        assert_eq!(bound.value(0), 10_000_000);
        let elapsed = column("wait_elapsed_us").as_primitive::<UInt64Type>();
        assert!(elapsed.is_null(0));
        assert!(elapsed.value(1) >= 10_000_000, "{}", elapsed.value(1));
        assert!(
            utf8("wait_last", 1).is_some_and(|last| last.starts_with("sent ")),
            "{:?}",
            utf8("wait_last", 1)
        );
        let process = column("process_id").as_primitive::<UInt32Type>();
        assert_eq!(process.value(1), std::process::id());
        let at = column("timestamp").as_primitive::<arrow_array::types::TimestampNanosecondType>();
        assert_eq!(
            u64::try_from(at.value(1) - at.value(0)).expect("the span ends after it starts")
                / 1_000,
            elapsed.value(1),
            "the span covers exactly the wait"
        );
    }
}
