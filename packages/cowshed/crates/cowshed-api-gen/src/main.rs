use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os().skip(1);
    let mode = arguments
        .next()
        .ok_or("usage: cowshed-api-gen <write|check|ir-model> <cowshed-project-path> or <ir-types|ir-validators> <types-module>")?;
    if mode == "ir-types" || mode == "ir-validators" {
        let module = arguments
            .next()
            .ok_or("missing IR types module specifier")?
            .into_string()
            .map_err(|_| "IR types module specifier is not UTF-8")?;
        if arguments.next().is_some() {
            return Err("IR type emission takes one module specifier".into());
        }
        let output = cowshed_api_gen::ir_types(&module)?;
        let text = if mode == "ir-types" {
            output.types
        } else {
            output.validators
        };
        return std::io::stdout()
            .lock()
            .write_all(text.as_bytes())
            .map_err(|error| format!("writing public IR declarations: {error}"));
    }
    let project = PathBuf::from(arguments.next().ok_or("missing cowshed project path")?);
    if arguments.next().is_some() {
        return Err(
            "unexpected argument; usage: cowshed-api-gen <write|check|ir-model> <cowshed-project-path>"
                .to_owned(),
        );
    }
    if mode == "ir-model" {
        let model = cowshed_api_gen::collect(&project)?;
        return serde_json::to_writer(std::io::stdout().lock(), &model)
            .map_err(|error| format!("writing public declaration IR: {error}"));
    }
    if mode == "columns" {
        let mut files = cowshed_api_gen::column_files(&project.join("crates/cowshed-core/src"))?;
        for file in &mut files {
            format_generated(file)?;
        }
        return cowshed_api_gen::write(&files);
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
        Err("mode must be write, check, ir-model, ir-types or ir-validators".to_owned())
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
