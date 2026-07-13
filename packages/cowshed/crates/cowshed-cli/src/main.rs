use std::ffi::{OsStr, OsString};
use std::io;

use cowshed_cli::{args, output};
use cowshed_core::CowshedError;

fn main() {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let json = option_before_child_argv(&args, "--json");
    let quiet = option_before_child_argv(&args, "--quiet") || option_before_child_argv(&args, "-q");
    let (error, command_map) = match args::parse_args(args) {
        Ok(cli) => {
            let _validated_command = cli.command;
            (
                CowshedError::environment_missing(
                    "cowshed command dispatch is not available in this Phase 1 build",
                    "use the cowshed-core API until Phase 2 adapter wiring is installed",
                ),
                None,
            )
        }
        Err(error) => {
            let command_map = error.command_map();
            (CowshedError::usage(error.message, error.hint), command_map)
        }
    };
    let exit_code = error.exit_code();
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut output = output::Output::new(stdout.lock(), stderr.lock(), quiet);
    let result = if json {
        output.json_error(error)
    } else {
        output
            .error(&error.message)
            .and_then(|()| {
                if let Some(command_map) = command_map {
                    output.error(command_map)
                } else {
                    Ok(())
                }
            })
            .and_then(|()| output.hint(&error.hint))
    };
    if let Err(write_error) = result {
        eprintln!("cowshed: failed to write command result: {write_error}");
        std::process::exit(1);
    }
    std::process::exit(exit_code.into());
}

fn option_before_child_argv(args: &[OsString], option: &str) -> bool {
    args.iter()
        .take_while(|argument| argument.as_os_str() != OsStr::new("--"))
        .any(|argument| argument == option)
}
