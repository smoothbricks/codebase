//! The CLI/N-API parity check refuses, by name, a CLI word without a typed counterpart, a parity
//! row for a word the CLI no longer parses, and a wire field its request does not carry
//! (07_api.md, "CLI and N-API option parity").

use std::path::{Path, PathBuf};

use cowshed_api_gen::cli_parity::{self, Counterpart, Facade, PARITY, Row, read_cli};
use cowshed_api_gen::{collect_api, ir::Api, operations::Operation};

fn project() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    Path::new(&manifest).join("../..")
}

fn declarations() -> (Vec<Operation>, Api) {
    let project = project();
    let table = std::fs::read_to_string(project.join("crates/cowshed-core/src/api/operations.rs"))
        .expect("the operation table");
    let operations =
        cowshed_api_gen::operations::parse(&table).expect("the operation table parses");
    let api = collect_api(&project).expect("the declarations collect");
    (operations, api)
}

fn grammar() -> String {
    std::fs::read_to_string(project().join("crates/cowshed-cli/src/args.rs")).expect("args.rs")
}

/// Rewrite one exact, unique span of the CLI grammar.
fn edited(source: &str, from: &str, to: &str) -> String {
    assert_eq!(
        source.matches(from).count(),
        1,
        "{from:?} occurs once in args.rs"
    );
    source.replacen(from, to, 1)
}

fn refusal(source: &str) -> String {
    let (operations, api) = declarations();
    match cli_parity::check(source, &operations, &api) {
        Ok(_) => panic!("the parity check accepted the grammar"),
        Err(error) => error,
    }
}

#[test]
fn every_word_of_the_cli_has_a_counterpart() {
    let (operations, api) = declarations();
    let checked =
        cli_parity::check(&grammar(), &operations, &api).expect("the real grammar is covered");
    assert!(
        checked.summary.starts_with("cli parity: "),
        "{}",
        checked.summary
    );
}

#[test]
fn a_cli_option_without_a_counterpart_is_refused() {
    let source = edited(
        &grammar(),
        "flag(\"quarantine\"),",
        "flag(\"quarantine\"),\n            flag(\"napi-less\"),",
    );
    let error = refusal(&source);
    assert!(
        error.contains("cowshed adopt --napi-less has no N-API counterpart"),
        "{error}"
    );
}

#[test]
fn a_verb_without_a_counterpart_is_refused() {
    let source = edited(
        &grammar(),
        ".subcommand(leaf(\"controller\"))",
        ".subcommand(leaf(\"controller\"))\n        .subcommand(leaf(\"napi-less\"))",
    );
    let error = refusal(&source);
    assert!(
        error.contains("cowshed napi-less has no N-API counterpart"),
        "{error}"
    );
}

#[test]
fn a_row_for_a_word_the_cli_dropped_is_refused() {
    let source = edited(&grammar(), "            flag(\"quarantine\"),\n", "");
    let error = refusal(&source);
    assert!(
        error.contains("cli parity row `adopt quarantine` names nothing the CLI parses"),
        "{error}"
    );
}

#[test]
fn a_wire_field_the_request_does_not_carry_is_refused() {
    let (operations, api) = declarations();
    let cli = read_cli(&grammar()).expect("args.rs reads");
    let rows: Vec<Row> = PARITY
        .iter()
        .map(|row| match (row.command, row.argument) {
            ("fork", "clean") => Row {
                command: "fork",
                argument: "clean",
                counterpart: Counterpart::Field {
                    wire: &[("coordinator.fork", "options.scrub")],
                    facade: &[Facade {
                        member: "Coordinator.fork",
                        argument: Some(1),
                        path: "clean",
                    }],
                },
            },
            _ => Row {
                command: row.command,
                argument: row.argument,
                counterpart: row.counterpart,
            },
        })
        .collect();
    let Err(error) = cli_parity::check_rows(&cli, &rows, &operations, &api) else {
        panic!("a row naming a missing field was accepted");
    };
    assert!(
        error.contains("cowshed fork --clean: names coordinator.fork `options.scrub`, but the request field `options` carries no `scrub`"),
        "{error}"
    );
}

#[test]
fn an_unknown_builder_call_is_refused_by_name() {
    let source = edited(
        &grammar(),
        ".subcommand(leaf(\"controller\"))",
        ".subcommand(leaf(\"controller\").alias(\"ctl\"))",
    );
    let Err(error) = read_cli(&source) else {
        panic!("the reader skipped a builder call it does not know");
    };
    assert!(
        error.contains("command method `.alias` is unknown to the parity reader"),
        "{error}"
    );
}

#[test]
fn an_unknown_argument_builder_call_is_refused_by_name() {
    let source = edited(
        &grammar(),
        r#"flag("quarantine"),"#,
        r#"flag("quarantine").long_alias("isolate"),"#,
    );
    let Err(error) = read_cli(&source) else {
        panic!("the reader skipped an argument builder call it does not know");
    };
    assert!(
        error.contains("argument method `.long_alias` is unknown to the parity reader"),
        "{error}"
    );
}
