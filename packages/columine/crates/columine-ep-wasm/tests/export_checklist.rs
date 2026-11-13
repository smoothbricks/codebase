//! Audits the `event_processor.wasm` export surface against
//! `columine_types::wasm_abi::COLUMINE_EP_EXPORTS`: the seven `ep_*`
//! functions (`ep_compact`, `ep_create_log_entry`, `ep_create_with_schema`,
//! `ep_create_with_schema_and_dictionaries`, `ep_create_with_schema_and_names`,
//! `ep_destroy`, `ep_version`). That table is the one list: the built artifact
//! and the TypeScript host are each checked against it.
//!
//! `built_wasm_matches_the_export_table` needs the compiled artifact and says
//! so out loud when it is missing rather than reporting success: the nx
//! `cargo-test` target builds it first (`dependsOn: cargo-wasm`).

use columine_types::wasm_abi::{COLUMINE_EP_EXPORTS, EXPORTED_MEMORY, parse_exports};
use std::collections::BTreeSet;

/// This crate's directory joined with `relative`, read at run time: a workspace runs the test
/// binary it inherited from the checkout that compiled it, and must read its own tree.
fn manifest_path(relative: &str) -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs");
    format!("{manifest}{relative}")
}

/// The checkout's dev build, `.cache/wasm-dev/event_processor.wasm`: the build a
/// checkout's source authority names (packages/nx-plugin/README.md, "Cargo profiles"),
/// which the default configuration of the nx `cargo-wasm` target writes and
/// `cargo-test` depends on. The shipped `dist/event_processor.wasm` is its production
/// configuration's.
fn artifact() -> String {
    manifest_path("/../../.cache/wasm-dev/event_processor.wasm")
}

/// The host's backend source, compiled in: Nx keys the test on a file a literal `include_str!` names
/// (packages/nx-plugin/README.md), not on one the test opens at run time.
const PARSE_BACKEND_TS: &str = include_str!("../../../src/parse-backend.ts");

/// The host declares these as members of `EventProcessorWasmExports` rather
/// than as a name array, so the audit looks for each member declaration.
#[test]
fn typescript_host_declares_every_ep_export() {
    let source = PARSE_BACKEND_TS;
    let missing: Vec<&str> = COLUMINE_EP_EXPORTS
        .iter()
        .copied()
        .filter(|name| !source.contains(&format!("{name}:")))
        .collect();
    assert!(
        missing.is_empty(),
        "exports in columine_types::wasm_abi::COLUMINE_EP_EXPORTS with no \
         EventProcessorWasmExports member: {missing:?}"
    );
}

#[test]
fn built_wasm_matches_the_export_table() {
    let path = artifact();
    let bytes = std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "read {path}: {error}\n\
             This test audits the compiled artifact, so it cannot pass without \
             one. Build it with `nx run columine:cargo-wasm` (or run this through \
             `nx run columine:cargo-test`, which depends on cargo-wasm)."
        )
    });
    let exports = parse_exports(&bytes).expect("built artifact is a readable wasm module");
    let functions: BTreeSet<&str> = exports
        .iter()
        .filter(|export| export.kind == 0 && !export.name.starts_with("__"))
        .map(|export| export.name.as_str())
        .collect();
    let expected: BTreeSet<&str> = COLUMINE_EP_EXPORTS.iter().copied().collect();
    assert_eq!(
        expected, functions,
        "built event_processor.wasm function exports diverged from \
         columine_types::wasm_abi::COLUMINE_EP_EXPORTS"
    );
    assert!(
        exports
            .iter()
            .any(|export| export.name == EXPORTED_MEMORY && export.kind == 2),
        "{EXPORTED_MEMORY} must be exported (the TS host reads instance.exports.memory)"
    );
}
