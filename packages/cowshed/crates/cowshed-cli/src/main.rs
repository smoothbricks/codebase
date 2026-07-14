use std::ffi::{OsStr, OsString};
use std::io;

use cowshed_cli::{args, gateway_service, output, runtime};
use cowshed_core::CowshedError;
use cowshed_gateway::{GATEWAY_GIT_FETCH_HELPER_ARG, run_gateway_git_fetch_helper};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    std::process::exit(run().await);
}

async fn run() -> i32 {
    let arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
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
    if let args::Command::Gateway(action) = &parsed.command {
        return match gateway_service::dispatch(*action, parsed.global.json, &mut output).await {
            Ok(exit_code) => exit_code,
            Err(error) => {
                let exit_code = i32::from(error.exit_code());
                if let Err(write_error) = write_error(&mut output, error, json, None) {
                    eprintln!("cowshed: failed to write command result: {write_error}");
                    1
                } else {
                    exit_code
                }
            }
        };
    }
    match runtime::run_bridge_command(parsed, tokio::io::stdin(), &mut output).await {
        Ok(exit) => exit.code,
        Err(error) => {
            let exit_code = i32::from(error.exit_code());
            if let Err(write_error) = write_error(&mut output, error, json, None) {
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
