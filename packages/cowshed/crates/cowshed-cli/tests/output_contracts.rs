use cowshed_cli::output::{Output, write_error_envelope, write_success_envelope};

use cowshed_core::api::{EmptyResult, MountResult};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::{CowshedError, ErrorCode};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;

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

#[test]
fn binary_entrypoint_returns_typed_json_usage_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args(["--json", "exec", "raven", "--unknown"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["ok"], false);
    assert_eq!(envelope["error"]["code"], "usage");
}

#[test]
fn binary_entrypoint_returns_usage_and_command_map() {
    let output = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("a command is required"));
    assert!(stderr.contains("commands:"));
}

#[test]
fn child_argv_cannot_enable_cli_json_mode() {
    let output = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args([
            "exec",
            "raven",
            "--stdin",
            "--stdin-file",
            "input",
            "--",
            "--json",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("conflict")
    );
}

/// A scratch HOME per test, following the crate's temp-directory convention.
fn scratch_home(label: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "cowshed-cli-skill-home-{label}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

#[test]
fn skill_install_splits_tsv_stdout_from_guidance_and_hints_exactly_once() {
    let home = scratch_home("streams");

    let output = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args(["skill", "install"])
        .env("HOME", &home)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let installed = home.join(".claude/skills/cowshed/SKILL.md");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!("claude-code\twritten\t{}\n", installed.display()),
        "stdout carries only the machine answer"
    );

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cowshed: installed the cowshed skill"));
    assert_eq!(
        stderr.matches("next: ").count(),
        1,
        "exactly one next: prefix, never a doubled one"
    );
    assert!(!stderr.contains("next: next:"));

    assert!(
        std::fs::read_to_string(&installed)
            .unwrap()
            .starts_with("---\nname: cowshed\n"),
        "the installed file is the shipped skill, frontmatter first"
    );
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn skill_install_is_idempotent_and_reports_unchanged() {
    let home = scratch_home("idempotent");

    let first = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args(["skill", "install"])
        .env("HOME", &home)
        .output()
        .unwrap();
    let second = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args(["--json", "skill", "install"])
        .env("HOME", &home)
        .output()
        .unwrap();

    assert!(String::from_utf8(first.stdout).unwrap().contains("written"));
    assert_eq!(second.status.code(), Some(0));
    let envelope: serde_json::Value =
        serde_json::from_slice(&second.stdout).expect("one JSON envelope on stdout");
    assert_eq!(
        envelope,
        json!({
            "ok": true,
            "result": {
                "skill": "cowshed",
                "installs": [{
                    "harness": "claude-code",
                    "path": home.join(".claude/skills/cowshed/SKILL.md").to_str().unwrap(),
                    "status": "unchanged",
                }],
            },
        })
    );
    assert!(
        String::from_utf8(second.stderr).unwrap().is_empty(),
        "--json keeps stderr clear of guidance"
    );
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn skill_install_rejects_an_unknown_harness_before_writing_anything() {
    let home = scratch_home("unknown-harness");

    let output = Command::new(env!("CARGO_BIN_EXE_cowshed"))
        .args(["skill", "install", "--harness", "nonesuch"])
        .env("HOME", &home)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unknown harness `nonesuch`")
    );
    assert!(
        !home.join(".claude").exists(),
        "a usage error must not create harness directories"
    );
    std::fs::remove_dir_all(&home).unwrap();
}
