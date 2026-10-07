use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};

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
    let mut files = cowshed_api_gen::generate(&project)?;
    for file in &mut files {
        format_generated(file)?;
    }
    if mode == "write" {
        cowshed_api_gen::write(&files)
    } else if mode == "check" {
        cowshed_api_gen::check(&files)
    } else {
        Err("mode must be write or check".to_owned())
    }
}

// Write and check compare the same formatted bytes, not a raw emitter against formatter output.
fn format_generated(file: &mut cowshed_api_gen::GeneratedFile) -> Result<(), String> {
    let mut command = match file
        .path
        .extension()
        .and_then(|extension| extension.to_str())
    {
        Some("ts") => {
            let mut command = Command::new("biome");
            command
                .args(["format", "--stdin-file-path"])
                .arg(&file.path);
            command
        }
        Some("rs") => {
            let mut command = Command::new("rustfmt");
            command.args(["--edition", "2024", "--emit", "stdout"]);
            command
        }
        _ => return Ok(()),
    };
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("format {}: {error}", file.path.display()))?;
    child
        .stdin
        .take()
        .ok_or("formatter did not open stdin")?
        .write_all(file.contents.as_bytes())
        .map_err(|error| format!("send {} to formatter: {error}", file.path.display()))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("format {}: {error}", file.path.display()))?;
    if !output.status.success() {
        return Err(format!(
            "format {} failed ({}): {}",
            file.path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if !output.stderr.is_empty() {
        std::io::stderr()
            .write_all(&output.stderr)
            .map_err(|error| format!("report formatter diagnostics: {error}"))?;
    }
    file.contents = String::from_utf8(output.stdout).map_err(|error| {
        format!(
            "formatter emitted non-UTF-8 for {}: {error}",
            file.path.display()
        )
    })?;
    Ok(())
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
