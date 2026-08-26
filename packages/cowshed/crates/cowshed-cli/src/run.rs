//! The CLI entrypoint, expressed as a library function.
//!
//! Both the `cowshed` binary and the Node-API addon's `runCli` export drive the
//! CLI through [`run`]. Keeping dispatch here — rather than in `main.rs` — is
//! what lets the npm package ship a CLI without a second, separately
//! cross-compiled executable: the addon already builds for every supported
//! target, so the CLI rides along in that same artifact.

use std::ffi::{OsStr, OsString};
use std::io;

use cowshed_core::CowshedError;
use cowshed_gateway::{GATEWAY_GIT_FETCH_HELPER_ARG, run_gateway_git_fetch_helper};

use crate::{
    args, gateway_service, help, output, runtime, sccache_service, setup_service, skill,
};

/// Run one CLI invocation. `arguments` excludes argv[0].
///
/// Returns the process exit code; it never exits the process itself, because an
/// in-process host (the addon) must be allowed to flush and unwind normally.
pub async fn run(arguments: Vec<OsString>) -> i32 {
    if arguments
        .first()
        .is_some_and(|argument| argument == OsStr::new(GATEWAY_GIT_FETCH_HELPER_ARG))
    {
        if arguments.len() != 1 {
            eprintln!("cowshed: the internal gateway git helper accepts no arguments");
            return 2;
        }
        return match run_gateway_git_fetch_helper() {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("cowshed: gateway git helper failed: {error}");
                1
            }
        };
    }
    let json = option_before_child_argv(&arguments, "--json");
    let quiet = option_before_child_argv(&arguments, "--quiet")
        || option_before_child_argv(&arguments, "-q");
    match parse_then_invoke_service(arguments, |parsed| run_parsed(parsed, json)).await {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let command_map = error.command_map();
            let error = CowshedError::usage(error.message, error.hint);
            emit_error(error, command_map, json, quiet)
        }
    }
}

async fn parse_then_invoke_service<F, Fut>(
    arguments: Vec<OsString>,
    invoke: F,
) -> Result<i32, args::UsageError>
where
    F: FnOnce(args::Cli) -> Fut,
    Fut: Future<Output = i32>,
{
    let parsed = args::parse_args(arguments)?;
    Ok(invoke(parsed).await)
}

async fn run_parsed(parsed: args::Cli, json: bool) -> i32 {
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
        return match output.bare(page.as_bytes()) {
            Ok(()) => 0,
            Err(write_error) => {
                eprintln!("cowshed: failed to write command result: {write_error}");
                1
            }
        };
    }
    if let args::Command::Skill(skill_args) = &parsed.command {
        let outcome = skill::dispatch(skill_args, &parsed.global, &mut output);
        return finish(outcome, &mut output, json);
    }
    if let args::Command::Gateway(action) = &parsed.command {
        let outcome = gateway_service::dispatch(*action, parsed.global.json, &mut output).await;
        return finish(outcome, &mut output, json);
    }
    if let args::Command::Sccache(action) = &parsed.command {
        let outcome =
            sccache_service::dispatch(action.clone(), parsed.global.json, &mut output).await;
        return finish(outcome, &mut output, json);
    }
    // `setup` has no project and no workspace: its subject is the host, so it dispatches here
    // beside the other host services rather than through the project runtime bridge.
    if let args::Command::Setup(setup_args) = &parsed.command {
        let outcome =
            setup_service::dispatch_native(setup_args, parsed.global.json, &mut output).await;
        return finish(outcome, &mut output, json);
    }
    let outcome = runtime::run_bridge_command(parsed, tokio::io::stdin(), &mut output)
        .await
        .map(|exit| exit.code);
    finish(outcome, &mut output, json)
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

fn option_before_child_argv(args: &[OsString], option: &str) -> bool {
    args.iter()
        .take_while(|argument| argument.as_os_str() != OsStr::new("--"))
        .any(|argument| argument == option)
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
            let result = parse_then_invoke_service(
                invocation.into_iter().map(OsString::from).collect(),
                |_| {
                    service_invoked.set(true);
                    async { 0 }
                },
            )
            .await;

            assert!(result.is_err());
            assert!(!service_invoked.get());
        }
    }
}
