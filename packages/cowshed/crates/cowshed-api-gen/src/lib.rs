pub mod ir;
pub mod napi;
pub mod operations;
pub mod records;
pub mod typescript;

use std::path::{Path, PathBuf};

pub struct GeneratedFile {
    pub path: PathBuf,
    pub contents: String,
}

pub fn generate(project: &Path) -> Result<Vec<GeneratedFile>, String> {
    let core = project.join("crates/cowshed-core/src");
    let mut api = ir::Api::default();
    let directory = core.join("api");
    let mut declarations = std::fs::read_dir(&directory)
        .map_err(|error| format!("{}: {error}", directory.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("{}: {error}", directory.display()))?;
    declarations.retain(|path| path.extension().is_some_and(|extension| extension == "rs"));
    declarations.sort();
    for path in declarations {
        let source = read_source(&path)?;
        records::parse(&source, &mut api)
            .map_err(|error| format!("{}: {error}", path.display()))?;
    }
    let error_path = core.join("error.rs");
    records::parse(&read_source(&error_path)?, &mut api)
        .map_err(|error| format!("{}: {error}", error_path.display()))?;
    for name in [
        "metadata.rs",
        "repository.rs",
        "project_policy.rs",
        "build_volume/mod.rs",
        "runtime/supervisor_socket.rs",
    ] {
        let path = core.join(name);
        let source = read_source(&path)?;
        records::parse_support(&source, &mut api)
            .map_err(|error| format!("{}: {error}", path.display()))?;
    }
    let gateway_status = project.join("crates/cowshed-gateway-types/src/status.rs");
    records::parse_support(&read_source(&gateway_status)?, &mut api)
        .map_err(|error| format!("{}: {error}", gateway_status.display()))?;
    let table = core.join("api/operations.rs");
    let operations = operations::parse(&read_source(&table)?)
        .map_err(|error| format!("{}: {error}", table.display()))?;
    napi::export_records(&operations, &mut api)?;
    let output = typescript::emit(&api)?;
    let projection = napi::emit(&operations, &api)?;
    Ok(vec![
        GeneratedFile {
            path: project.join("src/api.generated.ts"),
            contents: output.types,
        },
        GeneratedFile {
            path: project.join("src/validators.generated.ts"),
            contents: output.validators,
        },
        GeneratedFile {
            path: core.join("api/served.generated.rs"),
            contents: projection.served,
        },
        GeneratedFile {
            path: project.join("crates/cowshed-napi/src/operations.generated.rs"),
            contents: projection.rust,
        },
        GeneratedFile {
            path: project.join("src/native.generated.ts"),
            contents: projection.typescript,
        },
    ])
}

fn read_source(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))
}

pub fn write(files: &[GeneratedFile]) -> Result<(), String> {
    for file in files {
        if std::fs::read_to_string(&file.path).is_ok_and(|previous| previous == file.contents) {
            continue;
        }
        std::fs::write(&file.path, &file.contents)
            .map_err(|error| format!("{}: {error}", file.path.display()))?;
    }
    Ok(())
}

pub fn check(files: &[GeneratedFile]) -> Result<(), String> {
    for file in files {
        let current = read_source(&file.path)?;
        if current != file.contents {
            return Err(format!(
                "{} is stale; run cowshed:api-generate",
                file.path.display()
            ));
        }
    }
    Ok(())
}
