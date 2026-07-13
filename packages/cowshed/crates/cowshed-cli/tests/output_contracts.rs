#[path = "../src/output.rs"]
mod output;

use cowshed_core::api::{EmptyResult, MountResult};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::{CowshedError, ErrorCode};
use output::{Output, write_error_envelope, write_success_envelope};
use serde_json::json;
use std::path::PathBuf;

#[test]
fn success_and_failure_are_exact_core_envelopes() {
    let mut success = Vec::new();
    write_success_envelope(
        &mut success,
        MountResult {
            workspace: WorkspaceName::new("raven").unwrap(),
            mount: PathBuf::from("/mnt/raven"),
            base_commit: None,
        },
    )
    .unwrap();
    assert_eq!(
        success,
        b"{\"ok\":true,\"result\":{\"workspace\":\"raven\",\"mount\":\"/mnt/raven\"}}\n"
    );

    let mut failure = Vec::new();
    write_error_envelope(
        &mut failure,
        CowshedError::new(
            ErrorCode::SandboxDenied,
            "egress host is not granted",
            "cowshed grant raven --egress registry.example",
        ),
    )
    .unwrap();
    assert_eq!(
        failure,
        b"{\"ok\":false,\"error\":{\"code\":\"sandbox-denied\",\"message\":\"egress host is not granted\",\"hint\":\"cowshed grant raven --egress registry.example\"}}\n"
    );
}

#[test]
fn empty_success_is_object_and_guidance_is_stderr_only() {
    let mut stdout = Vec::new();
    write_success_envelope(&mut stdout, EmptyResult {}).unwrap();
    assert_eq!(stdout, b"{\"ok\":true,\"result\":{}}\n");

    let mut output = Output::new(Vec::new(), Vec::new(), false);
    output.guidance("attached raven").unwrap();
    output.hint("cowshed path raven").unwrap();
    let (stdout, stderr) = output.into_inner();
    assert!(stdout.is_empty());
    assert_eq!(
        stderr,
        b"cowshed: attached raven\nnext: cowshed path raven\n"
    );
}

#[test]
fn bare_streams_and_records_preserve_machine_bytes() {
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    output.bare(b"\0raw\n").unwrap();
    output
        .bare_record(&json!({"jobId":7,"state":"running"}))
        .unwrap();
    let (stdout, stderr) = output.into_inner();
    assert_eq!(stdout, b"\0raw\n{\"jobId\":7,\"state\":\"running\"}\n");
    assert!(stderr.is_empty());
}
