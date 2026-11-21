//! systemd: a user unit under `~/.config/systemd/user`, `systemctl --user`'s argv for each
//! lifecycle step, and the reading of `systemctl show`'s properties (05_gateway.md "The installed
//! cowshed", "On Linux").
//!
//! The steps, as launchd names them and the user manager takes them:
//!
//! | step      | `systemctl --user`     | what it guarantees                                          |
//! | --------- | ---------------------- | ----------------------------------------------------------- |
//! | bootstrap | `enable --now <unit>`  | the manager reloads, so it reads the unit file just written, starts the unit, and starts it again at every login |
//! | bootout   | `disable --now <unit>` | the unit is no longer started at login; the call returns once its stop job has ended |
//! | kickstart | `restart <unit>`       | the definition the manager loaded is stopped and started again |
//! | print     | `show <unit> --property=…` | exit 0 whatever the unit's state, which the properties say |
//!
//! Every answer here was read from systemd 260's user manager.

use super::{
    BunDirectory, CommandStatus, ControlAction, ControlExecutionError, LaunchAgentSpec,
    LaunchAgentTarget, LaunchctlOutput, LaunchdServiceStatus, Mutation, PRIVATE_DIRECTORY_MODE,
    RequiredDirectory, ServiceLifecycle, is_unsafe_xml_control,
};
use cowshed_core::runtime::job_cgroup::{CONTROLLER_LEAF, Controller};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// `systemctl`, found on `PATH`: a distribution keeps it wherever it likes.
pub const MANAGER_EXECUTABLE: &str = "systemctl";

/// The service manager, its control tool, a service it runs and that service's definition, as
/// messages name them.
pub const MANAGER: &str = "systemd";
pub const TOOL: &str = "systemctl";
pub const AGENT: &str = "user unit";
pub const DEFINITION: &str = "unit file";
pub const AGENT_DEFINITION: &str = "user unit file";

/// The extension of a definition's file name: every cowshed unit is a service.
pub(super) const DEFINITION_EXTENSION: &str = "service";

/// Why a `/nix/store` path is no [`BunDirectory`].
pub(super) const STORE_PATH_REFUSAL: &str =
    "must not be a /nix/store path, which nothing roots once a unit names it";

/// Where a user keeps their own units, under the home (systemd.unit(5), "User Unit Search Path").
const UNIT_DIRECTORY: &str = ".config/systemd/user";

/// The mode `~/.config/systemd/user` is made with. It is the user's, shared with every unit they
/// keep, so one that exists is left at the mode it has.
const UNIT_DIRECTORY_MODE: u32 = 0o755;

/// The variable the gateway reads its credential records' directory from
/// (`cowshed_core::host_dirs::credentials_directory`).
const CREDENTIALS_DIRECTORY: &str = "CREDENTIALS_DIRECTORY";

/// The properties a status read asks for: whether the manager knows the unit, runs it and
/// starts it at login, its main process, and the stop timeout of the definition it loaded.
const SHOWN_PROPERTIES: &str =
    "--property=LoadState,ActiveState,SubState,UnitFileState,MainPID,TimeoutStopUSec";

/// `~/.config/systemd/user/<label>.service`.
pub(super) fn definition_path(home: &Path, label: &str) -> PathBuf {
    home.join(UNIT_DIRECTORY).join(unit_name(label))
}

/// `~/.config/systemd/user` is made, with its parents, when it is missing, and never changed.
pub(super) fn plan_definition_directory(path: &Path, mode: Option<u32>) -> Option<Mutation> {
    match mode {
        None => Some(Mutation::EnsureDirectory {
            path: path.to_path_buf(),
            mode: UNIT_DIRECTORY_MODE,
        }),
        Some(_) => None,
    }
}

/// The directories the manager does not make: the parent of the stderr log (`append:` opens the
/// file and creates nothing above it, and the unit then fails every start with exit 222), and
/// the credentials directory a definition names, which the gateway refuses to start without.
/// Both are private to the user.
pub(super) fn required_directories(spec: &LaunchAgentSpec) -> Vec<RequiredDirectory> {
    let logs = spec.standard_error_path.parent().map(Path::to_path_buf);
    let credentials = spec
        .environment
        .iter()
        .filter(|(name, _)| name == CREDENTIALS_DIRECTORY)
        .map(|(_, directory)| PathBuf::from(directory));
    logs.into_iter()
        .chain(credentials)
        .map(|path| RequiredDirectory {
            path,
            mode: PRIVATE_DIRECTORY_MODE,
        })
        .collect()
}

/// The gateway unit's environment: `PATH`, which the unit writes ahead of the user manager's
/// own (`definition_bytes`), is the directory of the `bun` setup found, and
/// `CREDENTIALS_DIRECTORY` is where the gateway's credential provider reads its records.
pub(super) fn gateway_environment(home: &Path, bun: &BunDirectory) -> Vec<(String, String)> {
    vec![
        (
            "PATH".to_owned(),
            bun.path()
                .to_str()
                .expect("validated paths are UTF-8")
                .to_owned(),
        ),
        (
            CREDENTIALS_DIRECTORY.to_owned(),
            cowshed_core::host_dirs::credentials_directory(home)
                .to_str()
                .expect("validated home paths are UTF-8")
                .to_owned(),
        ),
    ]
}

/// A path the unit names outside a quoted word (`StandardError=append:`) runs to the end of its
/// line, so a line break in it cannot be written.
pub(super) fn is_unsafe_in_path(character: char) -> bool {
    is_unsafe_xml_control(character) || matches!(character, '\n' | '\r')
}

/// The unit file.
///
/// - `Type=exec`: a start has succeeded once the program runs, so a bootstrap reports a program
///   that cannot be executed.
/// - `Restart=always` with `RestartSec=1` and no start limit is launchd's `KeepAlive`: the manager
///   starts the service again whenever it exits, however often.
/// - `TimeoutStopSec` is launchd's `ExitTimeOut`: the manager SIGKILLs the main process that long
///   after a stop's SIGTERM.
/// - `KillMode=process`: a stop signals the main process only, as launchd signals the agent's own
///   process group. Under `control-group` or `mixed` the manager SIGKILLs everything left in the
///   unit's cgroup the moment the main process has exited, and the workspace supervisors and
///   their jobs live there and outlast a gateway restart (05_gateway.md "An install refuses while
///   a land runs").
/// - `Delegate=` hands the unit's cgroup subtree and those controllers to the service, and
///   `DelegateSubgroup=` starts it in the leaf the job cgroups' controller keeps
///   (`cowshed_core::runtime::job_cgroup`, "Authority"): the unit's own cgroup then holds no
///   process, which is what lets it distribute controllers to the class cgroups beside the leaf.
/// - `LimitNOFILE=<soft>:infinity` asks for the soft limit and whatever hard limit the manager
///   may give. A user manager cannot raise its own hard limit and gives the closest it has
///   instead of refusing the start (probed: `245760:infinity` under a manager at 524288 runs the
///   service at 245760 and 524288).
/// - `StandardError=append:` is launchd's `StandardErrorPath`.
pub(super) fn definition_bytes(spec: &LaunchAgentSpec) -> Vec<u8> {
    use std::fmt::Write as _;

    let mut unit = String::from("[Unit]\nDescription=cowshed ");
    push_escaped(&mut unit, spec.label(), Word::Verbatim);
    unit.push('\n');
    if spec.lifecycle == ServiceLifecycle::KeepAlive {
        unit.push_str("StartLimitIntervalSec=0\n");
    }
    unit.push_str("\n[Service]\nType=exec\nExecStart=");
    for (index, argument) in spec.program_arguments().enumerate() {
        if index > 0 {
            unit.push(' ');
        }
        unit.push('"');
        push_escaped(&mut unit, argument, Word::Command);
        unit.push('"');
    }
    unit.push('\n');
    for (name, value) in &spec.environment {
        unit.push_str("Environment=\"");
        push_escaped(&mut unit, name, Word::Quoted);
        unit.push('=');
        push_escaped(&mut unit, value, Word::Quoted);
        if name == "PATH" {
            // A unit that states `PATH` replaces the manager's, so the unit restates it: the
            // system `PATH`, its per-user directories spelt with the manager's specifiers
            // (`%h` the home, `%u` the user name), which it resolves when it loads the unit
            // and which are therefore written as they are.
            unit.push(':');
            unit.push_str(&cowshed_core::host_path::service_path());
        }
        unit.push_str("\"\n");
    }
    match spec.lifecycle {
        ServiceLifecycle::KeepAlive => unit.push_str("Restart=always\nRestartSec=1\n"),
        ServiceLifecycle::RunAtLoad => unit.push_str("Restart=no\n"),
    }
    if let Some(exit_timeout) = spec.exit_timeout {
        writeln!(unit, "TimeoutStopSec={}", exit_timeout.as_secs())
            .expect("writing into a String cannot fail");
    }
    unit.push_str("KillMode=process\nDelegate=");
    for (index, controller) in Controller::ENABLED.iter().enumerate() {
        if index > 0 {
            unit.push(' ');
        }
        unit.push_str(controller.name());
    }
    unit.push_str("\nDelegateSubgroup=");
    unit.push_str(CONTROLLER_LEAF);
    unit.push('\n');
    if let Some(soft) = spec.soft_number_of_files {
        writeln!(unit, "LimitNOFILE={soft}:infinity").expect("writing into a String cannot fail");
    }
    unit.push_str("StandardError=append:");
    push_escaped(
        &mut unit,
        spec.standard_error_path
            .to_str()
            .expect("validated home paths are UTF-8"),
        Word::Verbatim,
    );
    unit.push_str("\n\n[Install]\nWantedBy=default.target\n");
    unit.into_bytes()
}

/// Where a value is written in a unit file, which decides what the manager would read into it
/// (systemd.unit(5) "Specifiers", systemd.service(5) "Command lines").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Word {
    /// Outside quotes, to the end of its line: only `%` specifiers are expanded.
    Verbatim,
    /// Inside a double-quoted word: C escapes are read as well, so `\` and `"` are escaped and a
    /// tab or line break is written as its escape.
    Quoted,
    /// Inside a double-quoted word of `ExecStart=`: `$` starts a variable there too.
    Command,
}

/// Write `value` so the manager reads back exactly `value`.
fn push_escaped(unit: &mut String, value: &str, word: Word) {
    for character in value.chars() {
        match (character, word) {
            ('%', _) => unit.push_str("%%"),
            ('$', Word::Command) => unit.push_str("$$"),
            ('\\', Word::Quoted | Word::Command) => unit.push_str("\\\\"),
            ('"', Word::Quoted | Word::Command) => unit.push_str("\\\""),
            ('\n', Word::Quoted | Word::Command) => unit.push_str("\\n"),
            ('\r', Word::Quoted | Word::Command) => unit.push_str("\\r"),
            ('\t', Word::Quoted | Word::Command) => unit.push_str("\\t"),
            _ => unit.push(character),
        }
    }
}

/// The op a call of `action` is bounded and named under (05_gateway.md "Deadlines").
pub(super) const fn op(action: ControlAction) -> &'static str {
    match action {
        ControlAction::Bootstrap => "systemctl enable",
        ControlAction::Bootout => "systemctl disable",
        ControlAction::Kickstart => "systemctl restart",
        ControlAction::Print => "systemctl show",
    }
}

/// `systemctl`'s arguments for `action` on `target` in the calling user's manager, which
/// `--user` names: the uid names nothing.
pub(super) fn control_arguments(
    action: ControlAction,
    _uid: u32,
    target: &LaunchAgentTarget,
) -> Vec<OsString> {
    let unit = unit_name(target.label());
    let unit = unit.as_str();
    let words: &[&str] = match action {
        ControlAction::Bootstrap => &["--user", "enable", "--now", unit],
        ControlAction::Bootout => &["--user", "disable", "--now", unit],
        ControlAction::Kickstart => &["--user", "restart", unit],
        ControlAction::Print => &["--user", "show", unit, SHOWN_PROPERTIES],
    };
    words.iter().copied().map(OsString::from).collect()
}

/// The loaded state `systemctl show` answered. It exits 0 for a unit the manager has never
/// heard of, so the properties decide; any other exit is the manager not answering, which never
/// reads as "not loaded".
pub(super) fn service_status(
    output: LaunchctlOutput,
) -> Result<LaunchdServiceStatus, ControlExecutionError> {
    let LaunchctlOutput {
        status,
        stdout,
        stderr,
    } = output;
    if status != CommandStatus::Success {
        return Err(ControlExecutionError::Rejected {
            action: ControlAction::Print,
            status,
            stdout,
            stderr,
        });
    }
    let loaded = held(&String::from_utf8_lossy(&stdout));
    match loaded {
        Some(true) => Ok(LaunchdServiceStatus::Loaded { stdout, stderr }),
        Some(false) => Ok(LaunchdServiceStatus::NotLoaded {
            exit_code: 0,
            stdout,
            stderr,
        }),
        None => Err(ControlExecutionError::UnreadableStatus { stdout, stderr }),
    }
}

/// Whether the manager holds the unit `show` describes, as launchd holds a bootstrapped agent:
/// it runs it or is between two of its states (`ActiveState`), or it starts it at login
/// (`UnitFileState`). A unit that is enabled and stopped is held, so a stop still disables it
/// and its `default.target.wants` link never outlives its file. `None` when `show` states no
/// `ActiveState`. A unit with no file states an empty `UnitFileState`.
fn held(show: &str) -> Option<bool> {
    let mut running = None;
    let mut enabled = false;
    for line in show.lines() {
        match line.split_once('=') {
            Some(("ActiveState", state)) => {
                running = Some(matches!(
                    state,
                    "active" | "activating" | "deactivating" | "reloading" | "refreshing"
                ));
            }
            Some(("UnitFileState", state)) => {
                enabled = matches!(state, "enabled" | "enabled-runtime");
            }
            _ => {}
        }
    }
    running.map(|running| running || enabled)
}

fn unit_name(label: &str) -> String {
    format!("{label}.{DEFINITION_EXTENSION}")
}

/// The unit a label's service is, as `systemctl --user` takes it. Spelt with its type: a label
/// is free to end in a word `systemctl` would read as another unit type.
pub fn service_target(_uid: u32, label: &str) -> String {
    unit_name(label)
}

/// User-facing guidance that restarts a unit the manager holds.
pub fn kickstart_hint(uid: u32, label: &str) -> String {
    format!("systemctl --user restart {}", service_target(uid, label))
}

/// User-facing guidance that starts a unit the manager does not hold, now and at every login.
pub fn bootstrap_hint(uid: u32, target: &LaunchAgentTarget) -> String {
    format!(
        "systemctl --user enable --now {}",
        service_target(uid, target.label())
    )
}

/// User-facing guidance that shows what the manager holds of a unit.
pub fn print_hint(uid: u32, label: &str) -> String {
    format!("systemctl --user show {}", service_target(uid, label))
}

/// What a test's recording runner reads of a `systemctl` argv and answers a status read with.
#[cfg(test)]
pub(crate) mod recorded {
    use super::{CommandStatus, LaunchctlOutput};
    use std::ffi::OsString;

    /// The lifecycle step `arguments` ask for, by launchd's name for it: `print`, `bootstrap`,
    /// `bootout` or `kickstart`.
    pub(crate) fn verb(arguments: &[OsString]) -> &str {
        assert_eq!(arguments[0], "--user", "{arguments:?}");
        match arguments[1].to_str() {
            Some("show") => "print",
            Some("enable") => "bootstrap",
            Some("disable") => "bootout",
            Some("restart") => "kickstart",
            _ => panic!("no lifecycle step: {arguments:?}"),
        }
    }

    /// `systemctl --user show` of a unit the manager has never heard of (captured).
    pub(crate) fn not_loaded() -> LaunchctlOutput {
        LaunchctlOutput {
            status: CommandStatus::Success,
            stdout: b"LoadState=not-found\nActiveState=inactive\nSubState=dead\nUnitFileState=\nTimeoutStopUSec=1min 30s\nMainPID=0\n".to_vec(),
            stderr: Vec::new(),
        }
    }

    /// `systemctl --user show` of a unit the manager holds and does not run, whose loaded
    /// definition states `exit_timeout` seconds.
    pub(crate) fn loaded(exit_timeout: u64) -> LaunchctlOutput {
        LaunchctlOutput {
            status: CommandStatus::Success,
            stdout: format!(
                "LoadState=loaded\nActiveState=inactive\nSubState=dead\nUnitFileState=enabled\nTimeoutStopUSec={exit_timeout}s\nMainPID=0\n"
            )
            .into_bytes(),
            stderr: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::launchd::{COWSHED_BINARY_NAME, HostStableExecutable, plan_install};
    use cowshed_os::namespace::HostNamespace;

    const HOME: &str = "/host/home";

    fn gateway(home: &str, bun: &str) -> LaunchAgentSpec {
        let executable = HostStableExecutable::new(Path::new(home), COWSHED_BINARY_NAME).unwrap();
        let bun = BunDirectory::new(PathBuf::from(bun)).unwrap();
        LaunchAgentSpec::gateway(HostNamespace::Production, &executable, &bun).unwrap()
    }

    /// The production gateway's unit, byte for byte.
    #[test]
    fn the_gateway_unit_is_exactly_this() {
        let spec = gateway(HOME, "/etc/profiles/per-user/operator/bin");
        assert_eq!(
            spec.plist_path(),
            Path::new("/host/home/.config/systemd/user/dev.cowshed.gateway.service")
        );
        let expected = concat!(
            "[Unit]\n",
            "Description=cowshed dev.cowshed.gateway\n",
            "StartLimitIntervalSec=0\n",
            "\n",
            "[Service]\n",
            "Type=exec\n",
            "ExecStart=\"/host/home/Library/Application Support/dev.cowshed/bin/cowshed-gateway-host\" \"__gateway-host\" \"/host/home/Library/Application Support/dev.cowshed/bin/cowshed\" \"gateway\" \"run\"\n",
            "Environment=\"PATH=/etc/profiles/per-user/operator/bin:/run/wrappers/bin:%h/.nix-profile/bin:/nix/profile/bin:%h/.local/state/nix/profile/bin:/etc/profiles/per-user/%u/bin:/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/local/bin:/usr/bin:/bin\"\n",
            "Environment=\"CREDENTIALS_DIRECTORY=/host/home/Library/Application Support/dev.cowshed/credentials\"\n",
            "Restart=always\n",
            "RestartSec=1\n",
            "TimeoutStopSec=30\n",
            "KillMode=process\n",
            "Delegate=cpu memory io pids\n",
            "DelegateSubgroup=controller\n",
            "LimitNOFILE=245760:infinity\n",
            "StandardError=append:/host/home/Library/Logs/cowshed/daemon-stderr.log\n",
            "\n",
            "[Install]\n",
            "WantedBy=default.target\n",
        );
        assert_eq!(String::from_utf8(spec.plist_bytes()).unwrap(), expected);
    }

    /// A service that runs once is not restarted, states no stop timeout or descriptor limit of
    /// its own, and keeps the manager's start limit.
    #[test]
    fn a_run_once_unit_is_not_restarted() {
        let executable = HostStableExecutable::new(Path::new(HOME), "probe").unwrap();
        let spec = LaunchAgentSpec::new_user(
            &executable,
            "dev.cowshed.probe",
            vec!["once".to_owned()],
            ServiceLifecycle::RunAtLoad,
        )
        .unwrap();
        let unit = String::from_utf8(spec.plist_bytes()).unwrap();
        assert_eq!(
            unit,
            concat!(
                "[Unit]\n",
                "Description=cowshed dev.cowshed.probe\n",
                "\n",
                "[Service]\n",
                "Type=exec\n",
                "ExecStart=\"/host/home/Library/Application Support/dev.cowshed/bin/probe\" \"once\"\n",
                "Restart=no\n",
                "KillMode=process\n",
                "Delegate=cpu memory io pids\n",
                "DelegateSubgroup=controller\n",
                "StandardError=append:/host/home/Library/Logs/cowshed/daemon-stderr.log\n",
                "\n",
                "[Install]\n",
                "WantedBy=default.target\n",
            )
        );
    }

    /// Each place a value is written escapes exactly what the manager reads there. The command
    /// and quoted rows were read back from a started unit: `$` is a variable only in `ExecStart=`,
    /// and `%` a specifier everywhere.
    #[test]
    fn a_value_is_escaped_for_the_place_it_is_written() {
        for (value, verbatim, quoted, command) in [
            ("plain", "plain", "plain", "plain"),
            ("two words", "two words", "two words", "two words"),
            ("100%", "100%%", "100%%", "100%%"),
            ("%h", "%%h", "%%h", "%%h"),
            ("$HOME", "$HOME", "$HOME", "$$HOME"),
            ("${x}$$", "${x}$$", "${x}$$", "$${x}$$$$"),
            (r"back\slash", r"back\slash", r"back\\slash", r"back\\slash"),
            (r#"quo"te"#, r#"quo"te"#, r#"quo\"te"#, r#"quo\"te"#),
            ("it's", "it's", "it's", "it's"),
            (";", ";", ";", ";"),
            ("tab\there", "tab\there", r"tab\there", r"tab\there"),
            (
                "line\nbreak\r",
                "line\nbreak\r",
                r"line\nbreak\r",
                r"line\nbreak\r",
            ),
            ("é ünï", "é ünï", "é ünï", "é ünï"),
        ] {
            for (word, expected) in [
                (Word::Verbatim, verbatim),
                (Word::Quoted, quoted),
                (Word::Command, command),
            ] {
                let mut written = String::new();
                push_escaped(&mut written, value, word);
                assert_eq!(written, expected, "{value:?} as {word:?}");
            }
        }
    }

    /// A home and a bun directory are written so the manager reads them back: quoted where a
    /// space would split them, `%` doubled everywhere, and the default `PATH`'s specifiers left
    /// for the manager.
    #[test]
    fn a_unit_escapes_the_paths_it_names() {
        let spec = gateway("/host/50% $home", "/host/50% $home/.devenv/profile/bin");
        let unit = String::from_utf8(spec.plist_bytes()).unwrap();
        for line in [
            "ExecStart=\"/host/50%% $$home/Library/Application Support/dev.cowshed/bin/cowshed-gateway-host\" \"__gateway-host\" \"/host/50%% $$home/Library/Application Support/dev.cowshed/bin/cowshed\" \"gateway\" \"run\"\n",
            "Environment=\"PATH=/host/50%% $home/.devenv/profile/bin:/run/wrappers/bin:%h/.nix-profile/bin:",
            "Environment=\"CREDENTIALS_DIRECTORY=/host/50%% $home/Library/Application Support/dev.cowshed/credentials\"\n",
            "StandardError=append:/host/50%% $home/Library/Logs/cowshed/daemon-stderr.log\n",
        ] {
            assert!(unit.contains(line), "{line:?} in\n{unit}");
        }
    }

    /// A home with a line break cannot be written after `StandardError=append:`, so it is no
    /// home a definition takes.
    #[test]
    fn a_home_with_a_line_break_is_refused() {
        for home in ["/host/ho\nme", "/host/ho\rme"] {
            assert!(
                HostStableExecutable::new(Path::new(home), COWSHED_BINARY_NAME).is_err(),
                "{home:?}"
            );
        }
        assert!(HostStableExecutable::new(Path::new("/host/ho\tme"), COWSHED_BINARY_NAME).is_ok());
    }

    /// A test namespace's unit is its own label's, beside production's.
    #[test]
    fn a_unit_is_named_for_its_label() {
        for (label, path) in [
            (
                "dev.cowshed.gateway",
                "/host/home/.config/systemd/user/dev.cowshed.gateway.service",
            ),
            (
                "dev.cowshed-test-a1b2.gateway",
                "/host/home/.config/systemd/user/dev.cowshed-test-a1b2.gateway.service",
            ),
        ] {
            let target = LaunchAgentTarget::new(Path::new(HOME), label).unwrap();
            assert_eq!(target.plist_path(), Path::new(path));
            assert_eq!(
                target.launch_agents_directory(),
                Path::new("/host/home/.config/systemd/user")
            );
        }
    }

    /// The unit directory is made when missing and left at whatever mode it has.
    #[test]
    fn the_unit_directory_is_made_and_never_changed() {
        let directory = Path::new("/host/home/.config/systemd/user");
        for (mode, planned) in [
            (
                None,
                Some(Mutation::EnsureDirectory {
                    path: directory.to_path_buf(),
                    mode: 0o755,
                }),
            ),
            (Some(0o755), None),
            (Some(0o700), None),
            (Some(0o775), None),
        ] {
            assert_eq!(
                plan_definition_directory(directory, mode),
                planned,
                "{mode:?}"
            );
        }
    }

    /// An install on a home with no unit directory makes it, then publishes the unit at 0600
    /// through a temporary file beside it.
    #[test]
    fn an_install_writes_the_unit_under_the_user_unit_directory() {
        let spec = gateway(HOME, "/etc/profiles/per-user/operator/bin");
        let directory = PathBuf::from("/host/home/.config/systemd/user");
        let plan = plan_install(&spec, crate::launchd::InstallState::default());
        assert_eq!(
            plan.operations(),
            [
                Mutation::EnsureDirectory {
                    path: directory.clone(),
                    mode: 0o755,
                },
                Mutation::CreateExclusiveTemporaryFile {
                    directory: directory.clone(),
                    name_prefix: ".dev.cowshed.gateway.service.".to_owned(),
                    bytes: spec.plist_bytes(),
                    mode: 0o600,
                },
                Mutation::SyncTemporaryFile,
                Mutation::RenameTemporaryFile {
                    destination: directory.join("dev.cowshed.gateway.service"),
                },
                Mutation::SyncDirectory { path: directory },
            ]
        );
    }

    /// The gateway needs its log's directory and its credentials directory before it can start;
    /// a service that names no credentials needs only the log's.
    #[test]
    fn a_unit_requires_the_directories_the_manager_does_not_make() {
        let private = |path: &str| RequiredDirectory {
            path: PathBuf::from(path),
            mode: 0o700,
        };
        assert_eq!(
            gateway(HOME, "/etc/profiles/per-user/operator/bin").required_directories(),
            [
                private("/host/home/Library/Logs/cowshed"),
                private("/host/home/Library/Application Support/dev.cowshed/credentials"),
            ]
        );
        let executable = HostStableExecutable::new(Path::new(HOME), "probe").unwrap();
        let plain = LaunchAgentSpec::new_user(
            &executable,
            "dev.cowshed.probe",
            vec!["once".to_owned()],
            ServiceLifecycle::RunAtLoad,
        )
        .unwrap();
        assert_eq!(
            plain.required_directories(),
            [private("/host/home/Library/Logs/cowshed")]
        );
    }

    /// Each lifecycle step is one `systemctl --user` call on the label's unit.
    #[test]
    fn each_step_is_one_systemctl_call() {
        let target = LaunchAgentTarget::new(Path::new(HOME), "dev.cowshed.gateway").unwrap();
        for (action, named, argv) in [
            (
                ControlAction::Bootstrap,
                "systemctl enable",
                &["--user", "enable", "--now", "dev.cowshed.gateway.service"][..],
            ),
            (
                ControlAction::Bootout,
                "systemctl disable",
                &["--user", "disable", "--now", "dev.cowshed.gateway.service"][..],
            ),
            (
                ControlAction::Kickstart,
                "systemctl restart",
                &["--user", "restart", "dev.cowshed.gateway.service"][..],
            ),
            (
                ControlAction::Print,
                "systemctl show",
                &[
                    "--user",
                    "show",
                    "dev.cowshed.gateway.service",
                    "--property=LoadState,ActiveState,SubState,UnitFileState,MainPID,TimeoutStopUSec",
                ][..],
            ),
        ] {
            // The uid names nothing: `--user` is the calling user's manager.
            for uid in [0, 1000] {
                assert_eq!(control_arguments(action, uid, &target), argv, "{action:?}");
            }
            assert_eq!(op(action), named, "{action:?}");
            assert_eq!(
                recorded::verb(&control_arguments(action, 0, &target)),
                step(action)
            );
        }
    }

    fn step(action: ControlAction) -> &'static str {
        match action {
            ControlAction::Bootstrap => "bootstrap",
            ControlAction::Bootout => "bootout",
            ControlAction::Kickstart => "kickstart",
            ControlAction::Print => "print",
        }
    }

    fn show(text: &str) -> LaunchctlOutput {
        LaunchctlOutput {
            status: CommandStatus::Success,
            stdout: text.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    /// `systemctl --user show` as systemd 260 printed it for a probe unit in each state. The
    /// manager holds a unit it runs, one between two states, and one it starts at login.
    #[test]
    fn a_unit_is_loaded_while_the_manager_runs_it_or_starts_it_at_login() {
        for (state, text, loaded) in [
            (
                "never heard of",
                "LoadState=not-found\nActiveState=inactive\nSubState=dead\nUnitFileState=\nTimeoutStopUSec=1min 30s\nMainPID=0\n",
                false,
            ),
            (
                "running",
                "LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\nTimeoutStopUSec=3s\nMainPID=31807\n",
                true,
            ),
            (
                "between two runs",
                "LoadState=loaded\nActiveState=activating\nSubState=auto-restart\nUnitFileState=enabled\nTimeoutStopUSec=3s\nMainPID=0\n",
                true,
            ),
            (
                "stopping after a disable",
                "LoadState=loaded\nActiveState=deactivating\nSubState=stop-sigterm\nUnitFileState=disabled\nTimeoutStopUSec=3s\nMainPID=31815\n",
                true,
            ),
            (
                "disabled and stopped",
                "LoadState=loaded\nActiveState=inactive\nSubState=dead\nUnitFileState=disabled\nTimeoutStopUSec=3s\nMainPID=0\n",
                false,
            ),
            (
                "disabled and killed at its stop timeout",
                "LoadState=loaded\nActiveState=failed\nSubState=failed\nUnitFileState=disabled\nTimeoutStopUSec=3s\nMainPID=0\n",
                false,
            ),
            (
                "enabled and stopped",
                "LoadState=loaded\nActiveState=failed\nSubState=failed\nUnitFileState=enabled\nTimeoutStopUSec=3s\nMainPID=0\n",
                true,
            ),
            (
                "reloading",
                "LoadState=loaded\nActiveState=reloading\nSubState=reload\nUnitFileState=disabled\nTimeoutStopUSec=3s\nMainPID=7\n",
                true,
            ),
        ] {
            assert_eq!(held(text), Some(loaded), "{state}");
            let status = service_status(show(text)).expect(state);
            assert_eq!(
                matches!(status, LaunchdServiceStatus::Loaded { .. }),
                loaded,
                "{state}: {status:?}"
            );
        }
    }

    /// An answer that states no `ActiveState`, or a `systemctl` that failed or was killed, is
    /// the manager not answering: an error, never "not loaded".
    #[test]
    fn a_manager_that_does_not_answer_is_never_not_loaded() {
        assert_eq!(held(""), None);
        assert_eq!(held("LoadState=loaded\nMainPID=0\n"), None);
        assert!(matches!(
            service_status(show("")),
            Err(ControlExecutionError::UnreadableStatus { .. })
        ));
        for status in [CommandStatus::ExitCode(1), CommandStatus::Terminated] {
            let answered = service_status(LaunchctlOutput {
                status,
                stdout: Vec::new(),
                stderr: b"Failed to connect to user scope bus via local transport".to_vec(),
            });
            assert!(
                matches!(
                    answered,
                    Err(ControlExecutionError::Rejected {
                        action: ControlAction::Print,
                        ..
                    })
                ),
                "{status:?}: {answered:?}"
            );
        }
    }

    /// The recording runner's answers read as the states they stand for.
    #[test]
    fn the_recorded_answers_read_as_their_states() {
        assert!(matches!(
            service_status(recorded::not_loaded()),
            Ok(LaunchdServiceStatus::NotLoaded { .. })
        ));
        assert!(matches!(
            service_status(recorded::loaded(0)),
            Ok(LaunchdServiceStatus::Loaded { .. })
        ));
    }

    /// The hints name the unit with its type, as the calls do.
    #[test]
    fn the_hints_are_the_commands_to_type() {
        let target = LaunchAgentTarget::new(Path::new(HOME), "dev.cowshed.gateway").unwrap();
        assert_eq!(
            kickstart_hint(1000, "dev.cowshed.gateway"),
            "systemctl --user restart dev.cowshed.gateway.service"
        );
        assert_eq!(
            bootstrap_hint(1000, &target),
            "systemctl --user enable --now dev.cowshed.gateway.service"
        );
        assert_eq!(
            print_hint(1000, "dev.cowshed.gateway"),
            "systemctl --user show dev.cowshed.gateway.service"
        );
    }
}
