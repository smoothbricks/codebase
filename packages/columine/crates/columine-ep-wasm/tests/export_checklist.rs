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
//! wasm-ep` runs this file directly after linking.

use columine_types::wasm_abi::{COLUMINE_EP_EXPORTS, EXPORTED_MEMORY, parse_exports};
use std::collections::BTreeSet;

const ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../target/wasm32-unknown-unknown/wasm-release/columine_ep_wasm.wasm"
);

const PARSE_BACKEND_TS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../src/parse-backend.ts");

/// The host declares these as members of `EventProcessorWasmExports` rather
/// than as a name array, so the audit looks for each member declaration.
#[test]
fn typescript_host_declares_every_ep_export() {
    let source = std::fs::read_to_string(PARSE_BACKEND_TS)
        .unwrap_or_else(|error| panic!("read {PARSE_BACKEND_TS}: {error}"));
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
    let bytes = std::fs::read(ARTIFACT).unwrap_or_else(|error| {
        panic!(
            "read {ARTIFACT}: {error}\n\
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
