//! Pins the wasm export surface against the Zig `columine.wasm` ground
//! truth: 56 vm_* function exports plus the EXPORTED memory (frozen from the
//! Zig artifact's export section at cutover — the Zig build is deleted; this
//! list is its tombstone). The surface is the axe superset minus the
//! RETE/ax_eval/condition-tree families.

/// Every function export of the Zig columine.wasm.
pub const ZIG_COLUMINE_EXPORTS: [&str; 61] = [
    "vm_calculate_grown_state_size",
    "vm_calculate_state_size",
    "vm_delta_apply_rollback_segment",
    "vm_delta_apply_rollforward_segment",
    "vm_delta_export_entry_size",
    "vm_delta_export_len_bytes",
    "vm_delta_export_overflow",
    "vm_delta_export_redo_ptr",
    "vm_delta_export_segment",
    "vm_delta_export_undo_ptr",
    "vm_evict_all_expired",
    "vm_execute_batch",
    "vm_execute_batch_delta",
    "vm_get_needs_growth_slot",
    "vm_get_rbmp_last_error",
    "vm_get_rbmp_scratch_len",
    "vm_get_rbmp_scratch_ptr",
    "vm_grow_state",
    "vm_init_state",
    "vm_map_get",
    "vm_map_iter_get",
    "vm_map_iter_next",
    "vm_map_iter_start",
    "vm_rbmp_algebra_result_len",
    "vm_rbmp_algebra_result_ptr",
    "vm_rbmp_and",
    "vm_rbmp_andnot",
    "vm_rbmp_cardinality_serialized",
    "vm_rbmp_contains_serialized",
    "vm_rbmp_export_copy",
    "vm_rbmp_export_len",
    "vm_rbmp_extract_serialized",
    "vm_rbmp_import_copy",
    "vm_rbmp_intersect_any_serialized",
    "vm_rbmp_intersect_any_slots",
    "vm_rbmp_intersect_count_serialized",
    "vm_rbmp_intersect_count_slots",
    "vm_rbmp_or",
    "vm_rbmp_slot_data_len",
    "vm_rbmp_slot_data_ptr",
    "vm_rbmp_xor",
    "vm_reset_state",
    "vm_set_contains",
    "vm_set_iter_get",
    "vm_set_iter_next",
    "vm_set_iter_start",
    "vm_set_rbmp_scratch",
    "vm_struct_map2_get_row_ptr",
    "vm_struct_map2_iter_key1",
    "vm_struct_map2_iter_key2",
    "vm_struct_map2_iter_next",
    "vm_struct_map2_iter_start",
    "vm_struct_map_get_row_ptr",
    "vm_struct_map_iter_key",
    "vm_struct_map_iter_next",
    "vm_struct_map_iter_start",
    "vm_undo_checkpoint",
    "vm_undo_commit",
    "vm_undo_enable",
    "vm_undo_has_overflow",
    "vm_undo_rollback",
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
fn export_list_is_complete_and_deduped() {
    let mut names: Vec<&str> = ZIG_COLUMINE_EXPORTS.to_vec();
    names.sort_unstable();
    names.dedup();
    assert_eq!(
        names.len(),
        ZIG_COLUMINE_EXPORTS.len(),
        "duplicate names in the checklist"
    );
}

/// `just wasm` (columine justfile) runs this against the built artifact.
#[test]
#[ignore = "needs target/wasm32-unknown-unknown/wasm-release/columine_wasm.wasm (run `just wasm`)"]
fn built_wasm_exports_every_zig_symbol_and_memory() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../target/wasm32-unknown-unknown/wasm-release/columine_wasm.wasm"
    );
    let bytes =
        std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e} — run `just wasm` first"));
    let exports = wasm_exports(&bytes);
    let fn_names: std::collections::HashSet<&str> = exports
        .iter()
        .filter(|(_, k)| *k == 0)
        .map(|(n, _)| n.as_str())
        .collect();
    let missing: Vec<&&str> = ZIG_COLUMINE_EXPORTS
        .iter()
        .filter(|n| !fn_names.contains(**n))
        .collect();
    assert!(
        missing.is_empty(),
        "exports missing from the Columine ABI checklist: {missing:?}"
    );
    let extra: Vec<&str> = fn_names
        .iter()
        .filter(|n| !ZIG_COLUMINE_EXPORTS.contains(*n) && !n.starts_with("__"))
        .copied()
        .collect();
    assert!(
        extra.is_empty(),
        "exports beyond the Columine ABI checklist: {extra:?}"
    );
    assert!(
        exports.iter().any(|(n, k)| n == "memory" && *k == 2),
        "memory must be exported (columine TS reads instance.exports.memory)"
    );
}
