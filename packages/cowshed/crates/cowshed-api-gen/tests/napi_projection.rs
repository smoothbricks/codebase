//! The N-API projection moves with the operation table: a request field renamed in a scratch copy
//! of the declarations changes the fields the job handle binds for the addon's `kill` adapter, the
//! request its generated construction builds, and the TypeScript its `.d.ts` is compiled from.
//! B1b's own mutation — renaming the field that names the job — is refused.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Distinguishes the scratch projects of tests running at once in one process.
static SCRATCHES: AtomicUsize = AtomicUsize::new(0);

const ADAPTERS: &str = "crates/cowshed-napi/src/operations.generated.rs";
const SERVED: &str = "crates/cowshed-core/src/api/served.generated.rs";
const DECLARATIONS: &str = "src/native.generated.ts";

/// A scratch project holding copies of the declarations the generator reads, removed on every
/// exit path.
struct Scratch(PathBuf);

impl Scratch {
    fn copy_of(project: &Path) -> Self {
        let root = Path::new("/tmp").join(format!(
            "cowshed-api-gen-napi-{}-{}",
            std::process::id(),
            SCRATCHES.fetch_add(1, Ordering::Relaxed)
        ));
        for crate_source in [
            "crates/cowshed-core/src",
            "crates/cowshed-gateway-types/src",
        ] {
            copy_tree(&project.join(crate_source), &root.join(crate_source));
        }
        Self(root)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!(
                "scratch project {} was not removed: {error}",
                self.0.display()
            );
        }
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("scratch directory");
    for entry in fs::read_dir(source).expect("declaration directory") {
        let entry = entry.expect("declaration entry");
        let path = entry.path();
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).expect("copy declaration");
        }
    }
}

fn generated(project: &Path, file: &str) -> String {
    cowshed_api_gen::generate(project)
        .expect("generate")
        .into_iter()
        .find(|generated| generated.path == project.join(file))
        .unwrap_or_else(|| panic!("the generator emits {file}"))
        .contents
}

/// A scratch copy of the declarations with `record`'s field `from` renamed to `to`.
fn renamed_field(project: &Path, record: &str, from: &str, to: &str) -> Scratch {
    let scratch = Scratch::copy_of(project);
    let table = scratch.0.join("crates/cowshed-core/src/api/operations.rs");
    let source = fs::read_to_string(&table).expect("operation table");
    let start = source
        .find(&format!("pub struct {record} {{"))
        .unwrap_or_else(|| panic!("{record} is declared"));
    let field = start
        + source[start..]
            .find(from)
            .unwrap_or_else(|| panic!("{record} declares {from}"));
    let mutated = format!("{}{to}{}", &source[..field], &source[field + from.len()..]);
    fs::write(&table, mutated).expect("mutate the scratch declaration");
    scratch
}

/// The one adapter method `name` of `class` in the generated Rust.
fn adapter<'a>(rust: &'a str, class: &str, name: &str) -> &'a str {
    let class = &rust[rust
        .find(&format!("impl {class} {{"))
        .unwrap_or_else(|| panic!("an impl block for {class}"))..];
    let method = &class[class
        .find(&format!("pub fn {name}("))
        .unwrap_or_else(|| panic!("{class} has an adapter {name}"))..];
    &method[..method.find("\n    }").expect("the adapter's end")]
}

/// The generated `Serves<operation>` impl for `class`, with rustfmt's line breaks folded away.
fn serves(served: &str, operation: &str, class: &str) -> String {
    let header = format!("impl Serves<{operation}> for {class} {{");
    let block = &served[served
        .find(&header)
        .unwrap_or_else(|| panic!("{class} serves {operation}"))..];
    let block = &block[..block.find("\n}").expect("the impl's end")];
    block.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The request fields `class` binds for `operation`, as its generated `Serves` impl spells them.
fn bound(served: &str, operation: &str, class: &str) -> String {
    let block = serves(served, operation, class);
    let list = &block[block.find("&[").expect("a BOUND list")..];
    list[..=list.find(';').expect("the BOUND list's end")].to_owned()
}

#[test]
fn a_mutated_request_field_moves_the_napi_construction_and_its_declarations() {
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let scratch = renamed_field(
        &project,
        "LogsRequest",
        "pub follow: bool,",
        "pub tail: bool,",
    );

    let (rust, served, typescript) = (
        generated(&project, ADAPTERS),
        generated(&project, SERVED),
        generated(&project, DECLARATIONS),
    );
    let (mutated_served, mutated_typescript) = (
        generated(&scratch.0, SERVED),
        generated(&scratch.0, DECLARATIONS),
    );

    let logs = adapter(&rust, "JobHandle", "logs");
    assert!(
        logs.contains("download_call::<operations::JobLogs, _>"),
        "the addon's logs adapter calls job.logs through the job handle: {logs}"
    );
    for served in [&served, &mutated_served] {
        assert_eq!(
            bound(served, "JobLogs", "JobHandle"),
            r#"&["repoId", "workspace", "workspaceIncarnation", "jobId"];"#,
            "the job handle binds its whole fence whatever the caller's fields are named"
        );
    }
    let logs = serves(&served, "JobLogs", "JobHandle");
    assert!(
        logs.contains("let Caller { stream, follow, offset }"),
        "{logs}"
    );
    let mutated_logs = serves(&mutated_served, "JobLogs", "JobHandle");
    assert!(
        mutated_logs.contains("let Caller { stream, tail, offset }"),
        "the request's construction now decodes the renamed field from the caller: {mutated_logs}"
    );
    assert!(typescript.contains(
        "export type JobLogsArguments = Pick<Api.LogsRequest, 'stream' | 'follow' | 'offset'>;"
    ));
    assert!(
        mutated_typescript.contains(
            "export type JobLogsArguments = Pick<Api.LogsRequest, 'stream' | 'tail' | 'offset'>;"
        ),
        "the caller now names the renamed field"
    );
    assert!(
        typescript.contains("export type JobKillArguments = Readonly<Record<string, never>>;"),
        "a job handle that binds every field takes exactly an empty object"
    );
}

/// A renamed fence field would leave it to the caller: a worker could name another incarnation.
/// The projection refuses the table instead.
#[test]
fn a_request_that_stops_naming_its_incarnation_is_refused() {
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let scratch = renamed_field(
        &project,
        "JobRequest",
        "pub workspace_incarnation: WorkspaceIncarnation,",
        "pub incarnation: WorkspaceIncarnation,",
    );
    let Err(error) = cowshed_api_gen::generate(&scratch.0) else {
        panic!("a job request that does not name its incarnation is projected");
    };
    assert!(
        error.contains("serves it but its request does not name workspaceIncarnation"),
        "{error}"
    );
}

/// B1b's mutation renames the field that names the job: the job handle could no longer bind its
/// own job, so the projection refuses the table rather than let a caller name another job.
#[test]
fn a_request_that_stops_naming_its_job_is_refused() {
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let scratch = renamed_field(
        &project,
        "JobRequest",
        "pub job_id: JobId,",
        "pub job: JobId,",
    );

    let Err(error) = cowshed_api_gen::generate(&scratch.0) else {
        panic!("a job request that does not name its job is projected");
    };
    assert!(
        error.contains("JobHandle serves it but its request does not name jobId"),
        "{error}"
    );
}
