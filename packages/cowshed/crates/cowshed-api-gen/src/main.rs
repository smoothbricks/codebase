use std::path::PathBuf;
use std::process::ExitCode;

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os().skip(1);
    let mode = arguments
        .next()
        .ok_or("usage: cowshed-api-gen <write|check> <cowshed-project-path>")?;
    let project = PathBuf::from(arguments.next().ok_or("missing cowshed project path")?);
    if arguments.next().is_some() {
        return Err(
            "unexpected argument; usage: cowshed-api-gen <write|check> <cowshed-project-path>"
                .to_owned(),
        );
    }
    let files = cowshed_api_gen::generate(&project)?;
    if mode == "write" {
        cowshed_api_gen::write(&files)
    } else if mode == "check" {
        cowshed_api_gen::check(&files)
    } else {
        Err("mode must be write or check".to_owned())
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cowshed-api-gen: {error}");
            ExitCode::FAILURE
        }
    }
}
