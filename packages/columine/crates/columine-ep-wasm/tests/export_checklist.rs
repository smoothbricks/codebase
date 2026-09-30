//! Audits the `event_processor.wasm` export surface against
//! `columine_types::wasm_abi::COLUMINE_EP_EXPORTS`.
//!
//! There used to be two lists here — a five-name "baseline" and the real
//! six-name surface — plus a third in the TypeScript host. A list that omits a
//! shipped export (`ep_compact`) cannot audit anything, so there is now one
//! table and both sides are checked against it.
//!
//! `built_wasm_matches_the_export_table` needs the compiled artifact and says
//! so out loud when it is missing rather than reporting success: the nx
//! `cargo-test` target builds it first (`dependsOn: cargo-wasm`), and `just
//! wasm-ep` runs this file after deploying it.

use columine_types::wasm_abi::{COLUMINE_EP_EXPORTS, EXPORTED_MEMORY, parse_exports};
use std::collections::BTreeSet;

/// This crate's directory joined with `relative`, read at run time: a workspace runs the test
/// binary it inherited from the checkout that compiled it, and must read its own tree.
fn manifest_path(relative: &str) -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs");
    format!("{manifest}{relative}")
}

/// The deployed artifact, `dist/event_processor.wasm`: the file that ships,
/// whichever cargo profile `just wasm-ep` and the nx `cargo-wasm` target
/// compiled it with.
fn artifact() -> String {
    manifest_path("/../../dist/event_processor.wasm")
}

fn parse_backend_ts() -> String {
    manifest_path("/../../src/parse-backend.ts")
}

/// The host declares these as members of `EventProcessorWasmExports` rather
/// than as a name array, so the audit looks for each member declaration.
#[test]
fn typescript_host_declares_every_ep_export() {
    let path = parse_backend_ts();
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path}: {error}"));
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
             one. Build it with `just wasm-ep` (or run this through \
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
