pub mod arrow;
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

/// Collect the actual owner declarations and their Rust module bindings. Each binding is the
/// module its record carries, so the map cannot disagree with the declarations.
pub fn collect(project: &Path) -> Result<ir::DeclarationModel, String> {
    let api = collect_api(project)?;
    let modules = api
        .records
        .iter()
        .map(|(name, record)| (name.clone(), record.module.clone()))
        .collect();
    Ok(ir::DeclarationModel { api, modules })
}

/// The owner declarations the generator reads. Each record keeps the module its source defines:
/// an API source by the module declarations that reach it, every other source by its file path.
pub fn collect_api(project: &Path) -> Result<ir::Api, String> {
    let core = project.join("crates/cowshed-core/src");
    let mut api = ir::Api::default();
    for source in api_sources(&core)? {
        parse_api_source(source, &mut api)?;
    }
    let error_path = core.join("error.rs");
    let module = module_of("cowshed_core", "error.rs")?;
    parse_module(&error_path, &module, false, &mut api)?;
    for name in [
        "metadata.rs",
        "repository.rs",
        "project_policy.rs",
        "build_volume/mod.rs",
        "runtime/supervisor_socket.rs",
    ] {
        let module = module_of("cowshed_core", name)?;
        parse_module(&core.join(name), &module, true, &mut api)?;
    }
    let gateway_status = project.join("crates/cowshed-gateway-types/src/status.rs");
    let module = module_of("cowshed_gateway_types", "status.rs")?;
    parse_module(&gateway_status, &module, true, &mut api)?;
    Ok(api)
}

/// A source file the `api` module declares: the Rust module its declaration gives it, and whether
/// every module on the way to it is public.
struct ApiSource {
    file: PathBuf,
    module: String,
    exported: bool,
    syntax: syn::File,
}

/// Every declared API source, children before its owner. The resulting declaration map is name-ordered.
/// The walk starts at `api/mod.rs` and follows each out-of-line `mod` as written, `#[path]` included, so a file no
/// declaration reaches is not part of the crate and is not read. `cowshed-core` declares its `api`
/// module `pub`, so the root is exported.
fn api_sources(core: &Path) -> Result<Vec<ApiSource>, String> {
    let root = core.join("api/mod.rs");
    let mut sources = Vec::new();
    walk_api_file(&root, "cowshed_core::api", true, &mut sources)?;
    Ok(sources)
}

/// Where one module's declarations resolve. `children` is the directory its out-of-line `mod`s live
/// in; `path_base` is the directory its `#[path]` attributes resolve against: the file's directory
/// at top level, and the inline module's own directory inside an inline module.
struct Scope<'a> {
    file: &'a Path,
    module: &'a str,
    exported: bool,
    children: &'a Path,
    path_base: &'a Path,
}

/// Reads one module file and walks its declarations. `exported` is whether every module on the way
/// to this file is public.
fn walk_api_file(
    file: &Path,
    module: &str,
    exported: bool,
    sources: &mut Vec<ApiSource>,
) -> Result<(), String> {
    let source = read_source(file)?;
    let syntax =
        syn::parse_file(&source).map_err(|error| format!("{}: {error}", file.display()))?;
    let file_dir = file
        .parent()
        .ok_or_else(|| format!("{}: has no directory", file.display()))?;
    let children = module_directory(file, file_dir)?;
    let scope = Scope {
        file,
        module,
        exported,
        children: &children,
        path_base: file_dir,
    };
    walk_api_items(&syntax.items, scope, sources)?;
    sources.push(ApiSource {
        file: file.to_owned(),
        module: module.to_owned(),
        exported,
        syntax,
    });
    Ok(())
}

/// The directory a module file's own out-of-line `mod`s resolve in. By the Rust reference, a mod-rs
/// file (`mod.rs`, `lib.rs`, `main.rs`) owns the directory it is in; any other module file owns a
/// directory named after its stem beside it.
fn module_directory(file: &Path, file_dir: &Path) -> Result<PathBuf, String> {
    if matches!(
        file.file_name().and_then(|name| name.to_str()),
        Some("mod.rs" | "lib.rs" | "main.rs")
    ) {
        return Ok(file_dir.to_owned());
    }
    let stem = file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| format!("{}: has no module name", file.display()))?;
    Ok(file_dir.join(stem))
}

/// Walks the module declarations of one source. An inline `mod` recurses into its own items,
/// extending both directories by its name. An out-of-line `mod` recurses into its file. A test-only
/// declaration is skipped, as `records` skips an inline one.
fn walk_api_items(
    items: &[syn::Item],
    scope: Scope<'_>,
    sources: &mut Vec<ApiSource>,
) -> Result<(), String> {
    for item in items {
        let syn::Item::Mod(item) = item else {
            continue;
        };
        if records::is_test_only(&item.attrs)
            .map_err(|error| format!("{}: {error}", scope.file.display()))?
        {
            continue;
        }
        let name = item.ident.to_string();
        let module = format!("{}::{name}", scope.module);
        let exported = scope.exported && records::is_public(&item.vis);
        if let Some((_, inline)) = &item.content {
            let children = scope.children.join(&name);
            let inline_scope = Scope {
                file: scope.file,
                module: &module,
                exported,
                children: &children,
                path_base: &children,
            };
            walk_api_items(inline, inline_scope, sources)?;
            continue;
        }
        let target = match path_attribute(&item.attrs)
            .map_err(|error| format!("{}: {error}", scope.file.display()))?
        {
            Some(path) => scope.path_base.join(path),
            None => module_file(scope.children, &name, scope.file)?,
        };
        walk_api_file(&target, &module, exported, sources)?;
    }
    Ok(())
}

/// The file an out-of-line `mod name;` in `file` names: `dir/name.rs`, or `dir/name/mod.rs`. Both
/// existing is refused as ambiguous; neither is refused as missing, naming both candidates.
fn module_file(dir: &Path, name: &str, file: &Path) -> Result<PathBuf, String> {
    let flat = dir.join(format!("{name}.rs"));
    let nested = dir.join(name).join("mod.rs");
    match (flat.is_file(), nested.is_file()) {
        (true, false) => Ok(flat),
        (false, true) => Ok(nested),
        (true, true) => Err(format!(
            "{}: mod {name} is ambiguous: {} and {} both exist",
            file.display(),
            flat.display(),
            nested.display()
        )),
        (false, false) => Err(format!(
            "{}: mod {name} has no source file: looked for {} and {}",
            file.display(),
            flat.display(),
            nested.display()
        )),
    }
}

/// Consumes the syntax the module walk already parsed, with its exact visibility basis.
fn parse_api_source(source: ApiSource, api: &mut ir::Api) -> Result<(), String> {
    records::parse_file(source.syntax, api, &source.module, source.exported)
        .map_err(|error| format!("{}: {error}", source.file.display()))
}

/// The path a `#[path = "..."]` attribute names, if the declaration carries one.
fn path_attribute(attrs: &[syn::Attribute]) -> syn::Result<Option<String>> {
    let Some(attr) = attrs.iter().find(|attr| attr.path().is_ident("path")) else {
        return Ok(None);
    };
    let syn::Meta::NameValue(meta) = &attr.meta else {
        return Err(syn::Error::new_spanned(
            attr,
            "#[path] takes `= \"file.rs\"`",
        ));
    };
    let syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(path),
        ..
    }) = &meta.value
    else {
        return Err(syn::Error::new_spanned(
            &meta.value,
            "#[path] takes a string literal",
        ));
    };
    Ok(Some(path.value()))
}

/// The Rust module a fixed source file defines, from its path under the crate's `src` directory:
/// `build_volume/mod.rs` defines `build_volume`, and `runtime/supervisor_socket.rs` defines
/// `runtime::supervisor_socket`.
fn module_of(crate_name: &str, source: &str) -> Result<String, String> {
    let path = source
        .strip_suffix(".rs")
        .ok_or_else(|| format!("{source}: Rust source module has no .rs suffix"))?;
    let path = path.strip_suffix("/mod").unwrap_or(path);
    Ok(std::iter::once(crate_name)
        .chain(path.split('/'))
        .collect::<Vec<_>>()
        .join("::"))
}

fn parse_module(path: &Path, module: &str, support: bool, api: &mut ir::Api) -> Result<(), String> {
    let source = read_source(path)?;
    let parsed = if support {
        records::parse_support(&source, api, module)
    } else {
        records::parse(&source, api, module)
    };
    parsed.map_err(|error| format!("{}: {error}", path.display()))
}

/// Generate the TypeScript metadata model from these exact public IR declarations.
pub fn ir_types(types_module: &str) -> Result<typescript::Output, String> {
    let mut api = ir::Api::default();
    records::parse(include_str!("ir.rs"), &mut api, "cowshed_api_gen::ir")
        .map_err(|error| format!("public declaration IR: {error}"))?;
    typescript::emit(&api, "cowshed-api-gen public declaration IR", types_module)
}

pub fn generate(project: &Path) -> Result<Vec<GeneratedFile>, String> {
    let core = project.join("crates/cowshed-core/src");
    let mut api = collect_api(project)?;
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
    let declaration_ir = ir_types("./declaration-ir.generated.js")?;
    let mut files = vec![
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
        GeneratedFile {
            path: project.join("src/declaration-ir.generated.ts"),
            contents: declaration_ir.types,
        },
    ];
    files.extend(column_files(&core)?);
    Ok(files)
}

pub fn column_files(core: &Path) -> Result<Vec<GeneratedFile>, String> {
    let owners = [
        (
            "runtime/process_spans.rs",
            "runtime/process_span_columns.generated.rs",
        ),
        (
            "runtime/job_unattributed.rs",
            "runtime/job_unattributed_columns.generated.rs",
        ),
        (
            "runtime/job_boundary.rs",
            "runtime/job_boundary_columns.generated.rs",
        ),
    ];
    owners
        .into_iter()
        .map(|(source, generated)| {
            let source = core.join(source);
            let record = arrow::parse_arrow_record(&read_source(&source)?)
                .map_err(|error| format!("{}: {error}", source.display()))?;
            Ok(GeneratedFile {
                path: core.join(generated),
                contents: arrow::emit_columns(&record),
            })
        })
        .collect()
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
