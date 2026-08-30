//! The wasm export surface, named once.
//!
//! Three places used to restate this list — the crate rustdoc of
//! `columine-wasm`, its `tests/export_checklist.rs`, and the TypeScript host's
//! `VM_EXPORT_NAMES` — and they disagreed: the rustdoc said 56, the checklist
//! pinned 62, the host bound 30. A count that appears three times is not a
//! contract.
//!
//! These tables are the contract. `columine-wasm` and `columine-ep-wasm` audit
//! their built artifacts against them, and the same tests audit the TypeScript
//! host's binding lists against them, so an export added on one side and
//! forgotten on the other fails a test instead of failing a caller. Every name
//! here is bound by the published `@smoothbricks/columine` host; an export
//! nothing can reach does not belong in a shipped artifact.
//!
//! This crate has no dependencies, so both wasm crates and the audit can share
//! the table without a build-order edge.

/// Function exports of `columine.wasm`, in the order the ABI documents them.
/// Sorted, so the checklist audit compares sets without re-sorting.
pub const COLUMINE_VM_EXPORTS: &[&str] = &[
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
    "vm_get_evicted_count",
    "vm_get_needs_growth_slot",
    "vm_grow_state",
    "vm_init_state",
    "vm_map_get",
    "vm_reset_state",
    "vm_set_contains",
    "vm_struct_map2_get_row_ptr",
    "vm_struct_map2_iter_key1",
    "vm_struct_map2_iter_key2",
    "vm_struct_map2_iter_next",
    "vm_struct_map2_iter_start",
    "vm_undo_checkpoint",
    "vm_undo_commit",
    "vm_undo_enable",
    "vm_undo_has_overflow",
    "vm_undo_rollback",
];

/// Function exports of `event_processor.wasm`. `ep_compact` is the CPB1
/// extension; there is no shorter "baseline" list, because a list that omits a
/// shipped export cannot audit anything.
pub const COLUMINE_EP_EXPORTS: &[&str] = &[
    "ep_compact",
    "ep_create_log_entry",
    "ep_create_with_schema",
    "ep_create_with_schema_and_names",
    "ep_destroy",
    "ep_version",
];

/// Name of the exported linear memory both artifacts publish. The host reads
/// state, columns, and IPC output through it.
pub const EXPORTED_MEMORY: &str = "memory";

/// One export entry of a wasm module: name and external kind (0 = function,
/// 2 = memory).
#[cfg(feature = "audit")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WasmExport {
    pub name: String,
    pub kind: u8,
}

/// Read section id 7 of a wasm module.
///
/// Both wasm crates audit their built artifact against the tables above, and
/// this reader was copy-pasted into both test files. `Err` names the byte
/// position so a truncated or non-wasm input is reported rather than indexed
/// past its end.
#[cfg(feature = "audit")]
pub fn parse_exports(bytes: &[u8]) -> Result<Vec<WasmExport>, String> {
    if bytes.len() < 8 || &bytes[..4] != b"\0asm" {
        return Err("not a wasm module: missing \\0asm magic".to_owned());
    }
    let mut cursor = 8;
    let mut exports = Vec::new();
    while cursor < bytes.len() {
        let section_id = bytes[cursor];
        cursor += 1;
        let size = usize::try_from(uleb(bytes, &mut cursor)?)
            .map_err(|_| format!("section size at {cursor} exceeds this platform's usize"))?;
        let section_end = cursor
            .checked_add(size)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| format!("section at {cursor} declares {size} bytes past end of input"))?;
        if section_id == 7 {
            let count = uleb(bytes, &mut cursor)?;
            for _ in 0..count {
                let len = usize::try_from(uleb(bytes, &mut cursor)?)
                    .map_err(|_| format!("export name length at {cursor} exceeds usize"))?;
                let raw = bytes
                    .get(cursor..cursor + len)
                    .ok_or_else(|| format!("export name at {cursor} runs past end of input"))?;
                let name = String::from_utf8(raw.to_vec())
                    .map_err(|_| format!("export name at {cursor} is not UTF-8"))?;
                cursor += len;
                let kind = *bytes
                    .get(cursor)
                    .ok_or_else(|| format!("export kind at {cursor} runs past end of input"))?;
                cursor += 1;
                uleb(bytes, &mut cursor)?;
                exports.push(WasmExport { name, kind });
            }
        }
        cursor = section_end;
    }
    Ok(exports)
}

#[cfg(feature = "audit")]
fn uleb(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| format!("LEB128 at {cursor} runs past end of input"))?;
        *cursor += 1;
        if shift >= 64 {
            return Err(format!("LEB128 at {cursor} is wider than u64"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tables are the audit input, so a duplicate or an unsorted entry
    /// would make a set comparison silently weaker.
    #[test]
    fn tables_are_sorted_and_unique() {
        for (what, table) in [
            ("COLUMINE_VM_EXPORTS", COLUMINE_VM_EXPORTS),
            ("COLUMINE_EP_EXPORTS", COLUMINE_EP_EXPORTS),
        ] {
            assert!(
                table.windows(2).all(|pair| pair[0] < pair[1]),
                "{what} must be sorted and duplicate-free"
            );
        }
    }
}
