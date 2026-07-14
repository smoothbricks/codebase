//! Pins the wasm export surface against the COLUMINE Zig
//! `event_processor.wasm` ground truth: five function exports plus the
//! EXPORTED memory (packages/columine build.zig: `export_memory = true`;
//! the host writes schema/input at offsets it computes in
//! wasm-memory-contract.ts — there is no reserve/alloc protocol).

/// Every function export of the Zig event_processor.wasm (export section,
/// parsed mechanically from the columine package's src/event_processor.zig export set (all five `export fn`s)).
pub const ZIG_EP_EXPORTS: [&str; 5] = [
    "ep_version",
    "ep_create_with_schema",
    "ep_create_with_schema_and_names",
    "ep_destroy",
    "ep_create_log_entry",
];

/// Minimal wasm export-section reader (section id 7): (name, kind) pairs.
fn wasm_exports(bytes: &[u8]) -> Vec<(String, u8)> {
    assert_eq!(&bytes[..4], b"\0asm", "not a wasm module");
    let mut i = 8;
    let uleb = |i: &mut usize| -> u64 {
        let mut r = 0u64;
        let mut s = 0;
        loop {
            let b = bytes[*i];
            *i += 1;
            r |= u64::from(b & 0x7f) << s;
            if b & 0x80 == 0 {
                return r;
            }
            s += 7;
        }
    };
    let mut out = Vec::new();
    while i < bytes.len() {
        let sid = bytes[i];
        i += 1;
        let size = uleb(&mut i) as usize;
        let end = i + size;
        if sid == 7 {
            let n = uleb(&mut i);
            for _ in 0..n {
                let len = uleb(&mut i) as usize;
                let name = String::from_utf8(bytes[i..i + len].to_vec()).unwrap();
                i += len;
                let kind = bytes[i];
                i += 1;
                let _idx = uleb(&mut i);
                out.push((name, kind));
            }
        }
        i = end;
    }
    out
}

#[test]
fn zig_export_list_is_complete_and_deduped() {
    let mut names: Vec<&str> = ZIG_EP_EXPORTS.to_vec();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 5, "duplicate names in the checklist");
}

/// `just wasm-ep` runs this against the built artifact.
#[test]
#[ignore = "needs target/wasm32-unknown-unknown/wasm-release/columine_ep_wasm.wasm (run `just wasm-ep`)"]
fn built_wasm_exports_every_zig_symbol_and_memory() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../target/wasm32-unknown-unknown/wasm-release/columine_ep_wasm.wasm"
    );
    let bytes = std::fs::read(path)
        .unwrap_or_else(|e| panic!("read {path}: {e} — run `just wasm-ep` first"));
    let exports = wasm_exports(&bytes);
    let fn_names: std::collections::HashSet<&str> = exports
        .iter()
        .filter(|(_, k)| *k == 0)
        .map(|(n, _)| n.as_str())
        .collect();
    let missing: Vec<&&str> = ZIG_EP_EXPORTS
        .iter()
        .filter(|n| !fn_names.contains(**n))
        .collect();
    assert!(
        missing.is_empty(),
        "exports missing vs Zig event_processor.wasm: {missing:?}"
    );
    // build.zig: export_memory = true — JS reads instance.exports.memory.
    assert!(
        exports.iter().any(|(n, k)| n == "memory" && *k == 2),
        "memory must be exported (build.zig ep_wasm.export_memory)"
    );
}
