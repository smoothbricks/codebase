//! Audits the `columine.wasm` export surface against
//! `columine_types::wasm_abi::COLUMINE_VM_EXPORTS` — from both sides.
//!
//! The rust artifact and the TypeScript host each used to carry their own copy
//! of this list, and they disagreed by 32 names. Both are now checked against
//! the one table, so an export added on one side and forgotten on the other
//! fails here instead of failing a caller.
//!
//! `built_wasm_matches_the_export_table` needs the compiled artifact and says
//! so out loud when it is missing rather than reporting success: the nx
//! `cargo-test` target builds it first (`dependsOn: cargo-wasm`), and `just
//! wasm` runs this file directly after linking.

use columine_types::wasm_abi::{COLUMINE_VM_EXPORTS, EXPORTED_MEMORY, parse_exports};
use std::collections::BTreeSet;

/// Built artifact, relative to this crate. `just wasm` and the nx `cargo-wasm`
/// target both produce it at this path.
const ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../target/wasm32-unknown-unknown/wasm-release/columine_wasm.wasm"
);

/// The TypeScript host, read as source. A generator would be a build-order
/// edge from rust into the TS package for a list that changes when someone
/// edits an export by hand; harvesting the declaration catches the same drift
/// in both directions with no build step.
const WASM_BACKEND_TS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../src/wasm-backend.ts");

fn table() -> BTreeSet<&'static str> {
    COLUMINE_VM_EXPORTS.iter().copied().collect()
}

/// Harvest the quoted names of a `const <name> = [ ... ] as const;` array.
fn ts_string_array(source: &str, declaration: &str) -> BTreeSet<String> {
    let start = source
        .find(declaration)
        .unwrap_or_else(|| panic!("{declaration} not found in wasm-backend.ts"));
    let body = &source[start..];
    let end = body
        .find("] as const")
        .unwrap_or_else(|| panic!("{declaration} is not a `[...] as const` array"));
    body[..end]
        .split('\'')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

#[test]
fn typescript_host_binds_exactly_the_export_table() {
    let source = std::fs::read_to_string(WASM_BACKEND_TS)
        .unwrap_or_else(|error| panic!("read {WASM_BACKEND_TS}: {error}"));
    let bound = ts_string_array(&source, "const VM_EXPORT_NAMES");
    let expected: BTreeSet<String> = COLUMINE_VM_EXPORTS
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    assert_eq!(
        expected, bound,
        "wasm-backend.ts VM_EXPORT_NAMES diverged from \
         columine_types::wasm_abi::COLUMINE_VM_EXPORTS"
    );
}

/// The `VmExports` interface is what the rest of the host calls through, so a
/// name present in `VM_EXPORT_NAMES` but absent from the type is still an
/// unbound export. Members are declared as method signatures, so the audit
/// looks for `name(`.
#[test]
fn typescript_vm_exports_type_covers_the_export_table() {
    let source = std::fs::read_to_string(WASM_BACKEND_TS)
        .unwrap_or_else(|error| panic!("read {WASM_BACKEND_TS}: {error}"));
    let missing: Vec<&str> = COLUMINE_VM_EXPORTS
        .iter()
        .copied()
        .filter(|name| !source.contains(&format!("{name}(")))
        .collect();
    assert!(
        missing.is_empty(),
        "exports in the table with no VmExports member: {missing:?}"
    );
}

#[test]
fn built_wasm_matches_the_export_table() {
    let bytes = std::fs::read(ARTIFACT).unwrap_or_else(|error| {
        panic!(
            "read {ARTIFACT}: {error}\n\
             This test audits the compiled artifact, so it cannot pass without \
             one. Build it with `just wasm` (or run this through \
             `nx run columine:cargo-test`, which depends on cargo-wasm)."
        )
    });
    let exports = parse_exports(&bytes).expect("built artifact is a readable wasm module");
    // `__`-prefixed symbols are toolchain internals (`__heap_base`,
    // `__data_end`), not ABI.
    let functions: BTreeSet<&str> = exports
        .iter()
        .filter(|export| export.kind == 0 && !export.name.starts_with("__"))
        .map(|export| export.name.as_str())
        .collect();
    assert_eq!(
        table(),
        functions,
        "built columine.wasm function exports diverged from \
         columine_types::wasm_abi::COLUMINE_VM_EXPORTS"
    );
    assert!(
        exports
            .iter()
            .any(|export| export.name == EXPORTED_MEMORY && export.kind == 2),
        "{EXPORTED_MEMORY} must be exported (the TS host reads instance.exports.memory)"
    );
}
