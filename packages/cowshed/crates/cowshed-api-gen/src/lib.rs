pub mod capabilities;
pub mod errors;
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

/// Collect the actual owner declarations and their Rust module bindings.
pub fn collect(project: &Path) -> Result<ir::DeclarationModel, String> {
    let core = project.join("crates/cowshed-core/src");
    let mut model = ir::DeclarationModel {
        api: ir::Api::default(),
        modules: Default::default(),
    };
    let directory = core.join("api");
    let mut declarations = std::fs::read_dir(&directory)
        .map_err(|error| format!("{}: {error}", directory.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("{}: {error}", directory.display()))?;
    declarations.retain(|path| path.extension().is_some_and(|extension| extension == "rs"));
    declarations.sort();
    for path in declarations {
        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("{}: invalid source module name", path.display()))?;
        parse_module(
            &path,
            &format!("cowshed_core::api::{stem}"),
            false,
            &mut model,
        )?;
    }
    let error_path = core.join("error.rs");
    parse_module(&error_path, "cowshed_core::error", false, &mut model)?;
    for name in [
        "metadata.rs",
        "repository.rs",
        "project_policy.rs",
        "build_volume/mod.rs",
        "runtime/supervisor_socket.rs",
    ] {
        let path = core.join(name);
        let relative = name
            .strip_suffix(".rs")
            .ok_or_else(|| format!("{name}: Rust source module has no .rs suffix"))?;
        let relative = relative
            .strip_suffix("/mod")
            .unwrap_or(relative)
            .replace('/', "::");
        parse_module(
            &path,
            &format!("cowshed_core::{relative}"),
            true,
            &mut model,
        )?;
    }
    let gateway_status = project.join("crates/cowshed-gateway-types/src/status.rs");
    parse_module(
        &gateway_status,
        "cowshed_gateway_types::status",
        true,
        &mut model,
    )?;
    Ok(model)
}

fn parse_module(
    path: &Path,
    module: &str,
    support: bool,
    model: &mut ir::DeclarationModel,
) -> Result<(), String> {
    let source = read_source(path)?;
    if support {
        records::parse_support(&source, &mut model.api)
    } else {
        records::parse(&source, &mut model.api)
    }
    .map_err(|error| format!("{}: {error}", path.display()))?;
    for name in model.api.records.keys() {
        if !model.modules.contains_key(name) {
            model.modules.insert(name.clone(), module.to_owned());
        }
    }
    Ok(())
}

/// Generate the TypeScript metadata model from these exact public IR declarations.
pub fn ir_types(types_module: &str) -> Result<typescript::Output, String> {
    let mut api = ir::Api::default();
    records::parse(include_str!("ir.rs"), &mut api)
        .map_err(|error| format!("public declaration IR: {error}"))?;
    typescript::emit(&api, "cowshed-api-gen public declaration IR", types_module)
}

pub fn generate(project: &Path) -> Result<Vec<GeneratedFile>, String> {
    let core = project.join("crates/cowshed-core/src");
    let mut api = collect(project)?.api;
    let table = core.join("api/operations.rs");
    let operations = operations::parse(&read_source(&table)?)
        .map_err(|error| format!("{}: {error}", table.display()))?;
    napi::export_records(&operations, &mut api)?;
    let capabilities_path = core.join("api/capability.rs");
    let capabilities = capabilities::parse(&read_source(&capabilities_path)?)
        .map_err(|error| format!("{}: {error}", capabilities_path.display()))?;
    let output = typescript::emit(
        &api,
        "cowshed-core declarations and public support types",
        "./api.generated.js",
    )?;
    let projection = napi::emit(&operations, &api, &capabilities)?;
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
