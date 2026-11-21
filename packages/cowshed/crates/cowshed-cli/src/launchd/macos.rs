//! launchd: a LaunchAgent's property list under `~/Library/LaunchAgents`, `launchctl`'s argv for
//! each lifecycle step, and the reading of `launchctl print`'s exit status.

use super::{
    BunDirectory, CommandStatus, ControlAction, ControlExecutionError, LIBRARY_DIRECTORY,
    LaunchAgentSpec, LaunchAgentTarget, LaunchctlOutput, LaunchdServiceStatus, Mutation,
    RequiredDirectory, ServiceLifecycle, is_unsafe_xml_control, plan_private_directory,
};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub const MANAGER_EXECUTABLE: &str = "/bin/launchctl";

/// The service manager, its control tool, a service it runs and that service's definition, as
/// messages name them.
pub const MANAGER: &str = "launchd";
pub const TOOL: &str = "launchctl";
pub const AGENT: &str = "LaunchAgent";
pub const DEFINITION: &str = "plist";
pub const AGENT_DEFINITION: &str = "LaunchAgent plist";

/// The extension of a definition's file name.
pub(super) const DEFINITION_EXTENSION: &str = "plist";

/// Why a `/nix/store` path is no [`BunDirectory`].
pub(super) const STORE_PATH_REFUSAL: &str =
    "must not be a /nix/store path, which nothing roots once a plist names it";

const LAUNCH_AGENTS_DIRECTORY: &str = "LaunchAgents";

/// A path is written into the plist as XML text, which carries every character but these.
pub(super) fn is_unsafe_in_path(character: char) -> bool {
    is_unsafe_xml_control(character)
}

/// launchd `ProcessType` of every cowshed agent: Interactive.
///
/// The type is the QoS band the agent *and every descendant* runs in, and a descendant cannot
/// leave it: launchd applies Background as the process's apptype, so `setpriority(
/// PRIO_DARWIN_PROCESS, 0, 0)` in a child reports success and changes nothing (probed on macOS 26
/// with a Background agent: PRI 4 before and after, in the agent and in its children).
///
/// Only Interactive runs at parity with the host's own processes. The gateway's supervisor
/// manager starts every workspace supervisor, which starts every shell host and every
/// `cowshed exec` job. Probed with a test
/// agent on macOS 26: Background runs it and its children at PRI 4, Standard at PRI 20 (the
/// utility band, `taskpolicy -c utility`), Interactive at PRI 31 like a terminal's processes.
/// Both clamped bands throttle IO behind the host's: under Background on a host at load 80 a
/// shell loop that takes 0.7 s took 1–30 s, a fresh workspace's supervisor took 115 s to answer,
/// and its first `cowshed exec` spent 537 s inside `direnv export json`; under the utility clamp
/// at load 10–47 the first `codegraph status` over a fresh workspace's cold 1 GB SQLite index
/// took 34.5 s and 54.5 s where the same cold run unclamped took 2.4 s and 2.7 s.
const PROCESS_TYPE: &str = "Interactive";

/// `~/Library/LaunchAgents/<label>.plist`.
pub(super) fn definition_path(home: &Path, label: &str) -> PathBuf {
    home.join(LIBRARY_DIRECTORY)
        .join(LAUNCH_AGENTS_DIRECTORY)
        .join(format!("{label}.{DEFINITION_EXTENSION}"))
}

/// `~/Library/LaunchAgents` is kept private to the user.
pub(super) fn plan_definition_directory(path: &Path, mode: Option<u32>) -> Option<Mutation> {
    plan_private_directory(path, mode)
}

/// launchd creates `StandardErrorPath` and what leads to it itself.
pub(super) fn required_directories(_spec: &LaunchAgentSpec) -> Vec<RequiredDirectory> {
    Vec::new()
}

/// The gateway agent's `PATH`: the directory of the `bun` setup found ahead of the one launchd
/// gives an agent that states none, so the daemon's other children resolve as they always did.
pub(super) fn gateway_environment(_home: &Path, bun: &BunDirectory) -> Vec<(String, String)> {
    let path = format!(
        "{}:{}",
        bun.path().to_str().expect("validated paths are UTF-8"),
        cowshed_core::host_path::service_path()
    );
    vec![("PATH".to_owned(), path)]
}

pub(super) fn definition_bytes(spec: &LaunchAgentSpec) -> Vec<u8> {
    let mut plist = String::from(concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
        "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
        "<plist version=\"1.0\">\n",
        "<dict>\n",
        "  <key>Label</key>\n",
        "  ",
    ));
    push_xml_string(&mut plist, spec.label());
    plist.push_str("  <key>ProgramArguments</key>\n  <array>\n");
    for argument in spec.program_arguments() {
        plist.push_str("    ");
        push_xml_string(&mut plist, argument);
    }
    plist.push_str("  </array>\n  <key>RunAtLoad</key>\n  <true/>\n  <key>KeepAlive</key>\n");
    match spec.lifecycle {
        ServiceLifecycle::KeepAlive => plist.push_str("  <true/>\n"),
        ServiceLifecycle::RunAtLoad => plist.push_str("  <false/>\n"),
    }
    plist.push_str("  <key>ProcessType</key>\n  <string>");
    plist.push_str(PROCESS_TYPE);
    plist.push_str("</string>\n  <key>StandardErrorPath</key>\n  ");
    push_xml_string(
        &mut plist,
        spec.standard_error_path
            .to_str()
            .expect("validated home paths are UTF-8"),
    );
    if !spec.environment.is_empty() {
        plist.push_str("  <key>EnvironmentVariables</key>\n  <dict>\n");
        for (name, value) in &spec.environment {
            plist.push_str("    ");
            push_xml_key(&mut plist, name);
            plist.push_str("    ");
            push_xml_string(&mut plist, value);
        }
        plist.push_str("  </dict>\n");
    }
    if let Some(soft) = spec.soft_number_of_files {
        use std::fmt::Write as _;
        for (key, limit) in [
            ("SoftResourceLimits", soft),
            ("HardResourceLimits", 9_223_372_036_854_775_807),
        ] {
            writeln!(
                plist,
                "  <key>{key}</key>\n  <dict>\n    <key>NumberOfFiles</key>\n    <integer>{limit}</integer>\n  </dict>"
            )
            .expect("writing into a String cannot fail");
        }
    }
    if let Some(exit_timeout) = spec.exit_timeout {
        use std::fmt::Write as _;
        writeln!(
            plist,
            "  <key>ExitTimeOut</key>\n  <integer>{}</integer>",
            exit_timeout.as_secs()
        )
        .expect("writing into a String cannot fail");
    }
    plist.push_str("</dict>\n</plist>\n");
    plist.into_bytes()
}

/// The op a call of `action` is bounded and named under (05_gateway.md "Deadlines").
pub(super) const fn op(action: ControlAction) -> &'static str {
    match action {
        ControlAction::Bootstrap => "launchctl bootstrap",
        ControlAction::Bootout => "launchctl bootout",
        ControlAction::Kickstart => "launchctl kickstart",
        ControlAction::Print => "launchctl print",
    }
}

/// `launchctl`'s arguments for `action` on `target` in `uid`'s per-user domain. A bootstrap
/// names the plist; every other step names the service launchd already holds.
pub(super) fn control_arguments(
    action: ControlAction,
    uid: u32,
    target: &LaunchAgentTarget,
) -> Vec<OsString> {
    match action {
        ControlAction::Bootstrap => vec![
            OsString::from("bootstrap"),
            OsString::from(gui_domain(uid)),
            target.plist_path().as_os_str().to_owned(),
        ],
        ControlAction::Bootout => vec![
            OsString::from("bootout"),
            OsString::from(service_target(uid, target.label())),
        ],
        ControlAction::Kickstart => vec![
            OsString::from("kickstart"),
            OsString::from("-k"),
            OsString::from(service_target(uid, target.label())),
        ],
        ControlAction::Print => vec![
            OsString::from("print"),
            OsString::from(service_target(uid, target.label())),
        ],
    }
}

/// The loaded state `launchctl print` answered. A normal non-zero exit means the service is
/// absent; signal termination remains an operational error.
pub(super) fn service_status(
    output: LaunchctlOutput,
) -> Result<LaunchdServiceStatus, ControlExecutionError> {
    match output.status {
        CommandStatus::Success => Ok(LaunchdServiceStatus::Loaded {
            stdout: output.stdout,
            stderr: output.stderr,
        }),
        CommandStatus::ExitCode(exit_code) => Ok(LaunchdServiceStatus::NotLoaded {
            exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        }),
        CommandStatus::Terminated => Err(ControlExecutionError::Rejected {
            action: ControlAction::Print,
            status: CommandStatus::Terminated,
            stdout: output.stdout,
            stderr: output.stderr,
        }),
    }
}

fn gui_domain(uid: u32) -> String {
    format!("gui/{uid}")
}

pub fn service_target(uid: u32, label: &str) -> String {
    format!("{}/{label}", gui_domain(uid))
}

/// User-facing `launchctl kickstart -k` guidance for a per-user agent that is loaded.
pub fn kickstart_hint(uid: u32, label: &str) -> String {
    format!("launchctl kickstart -k {}", service_target(uid, label))
}

/// User-facing guidance that loads a per-user agent launchd does not hold: `kickstart` answers
/// such an agent "Could not find service" (exit 113), so only a bootstrap of its plist brings it
/// back.
pub fn bootstrap_hint(uid: u32, target: &LaunchAgentTarget) -> String {
    format!(
        "launchctl bootstrap {} '{}'",
        gui_domain(uid),
        target.plist_path().display()
    )
}

/// User-facing guidance that shows what launchd holds of a per-user agent.
pub fn print_hint(uid: u32, label: &str) -> String {
    format!("launchctl print {}", service_target(uid, label))
}

fn write_escaped(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '\'' => output.push_str("&apos;"),
            '"' => output.push_str("&quot;"),
            _ => output.push(character),
        }
    }
}

fn push_xml_text(output: &mut String, tag: &str, value: &str) {
    output.push('<');
    output.push_str(tag);
    output.push('>');
    write_escaped(output, value);
    output.push_str("</");
    output.push_str(tag);
    output.push_str(">\n");
}

fn push_xml_string(output: &mut String, value: &str) {
    push_xml_text(output, "string", value);
}

fn push_xml_key(output: &mut String, value: &str) {
    push_xml_text(output, "key", value);
}

/// What a test's recording runner reads of a `launchctl` argv and answers a status read with.
#[cfg(test)]
pub(crate) mod recorded {
    use super::{CommandStatus, LaunchctlOutput};
    use std::ffi::OsString;

    /// The lifecycle step `arguments` ask for: `print`, `bootstrap`, `bootout` or `kickstart`.
    pub(crate) fn verb(arguments: &[OsString]) -> &str {
        arguments[0].to_str().expect("launchctl verbs are UTF-8")
    }

    /// `launchctl print` of a service launchd does not hold: "Could not find service".
    pub(crate) fn not_loaded() -> LaunchctlOutput {
        LaunchctlOutput {
            status: CommandStatus::ExitCode(113),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    /// `launchctl print` of a service launchd holds and does not run, whose loaded definition
    /// states `exit_timeout` seconds.
    pub(crate) fn loaded(exit_timeout: u64) -> LaunchctlOutput {
        LaunchctlOutput {
            status: CommandStatus::Success,
            stdout: format!(
                "gui/501/dev.cowshed.gateway = {{\n\tstate = not running\n\texit timeout = {exit_timeout}\n}}\n"
            )
            .into_bytes(),
            stderr: Vec::new(),
        }
    }
}
