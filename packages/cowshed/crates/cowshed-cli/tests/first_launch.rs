//! The first-launch probe on the real host (05_gateway.md "The gateway host"): a fresh copy of
//! cowshed is minted, launched twice and removed.

#![cfg(target_os = "macos")]

use std::path::{Path, PathBuf};

use cowshed_cli::first_launch::measure;

fn probes() -> Vec<PathBuf> {
    let prefix = format!("cowshed-first-launch-{}-", std::process::id());
    let mut found: Vec<PathBuf> = std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .collect();
    found.sort();
    found
}

#[test]
fn a_probe_is_launched_twice_and_leaves_nothing_behind() {
    let before = probes();
    let measured = measure(Path::new(
        &std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    ))
    .unwrap();
    cowshed_os::eprintln!(
        "first launch {} us, second {} us",
        measured.first_micros,
        measured.second_micros
    );
    assert!(
        measured.first_micros > 0 && measured.second_micros > 0,
        "{measured:?}"
    );
    assert_eq!(probes(), before);
}

#[test]
fn a_template_that_is_not_there_is_an_error_naming_it() {
    let error = measure(Path::new("/nonexistent/cowshed")).unwrap_err();
    assert!(
        error.to_string().contains("/nonexistent/cowshed"),
        "{error}"
    );
}
