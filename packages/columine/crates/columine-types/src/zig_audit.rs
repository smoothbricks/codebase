//! Test-support parsers for the Zig↔Rust opcode-registry audit tripwire.
//!
//! WHY this lives in the library (doc-hidden) instead of a test module: the
//! audit runs in three crates (columine-types, columine-vm, axe-rete) against
//! the in-repo Zig sources, and integration tests cannot share code across
//! crates any other way without duplicating the parser. The module is
//! `#[doc(hidden)]`, compiled only when referenced, and has zero runtime
//! callers.
//!
//! These are TRIPWIRES, not compilers: line-oriented scans over the Zig and
//! Rust sources. Every consumer must pair a harvest with a sanity FLOOR
//! (assert the harvested count >= the count known today) so that silent
//! parser rot — a formatting change that makes a scan return nothing — fails
//! a test instead of quietly weakening the audit (the 0x81 incident was
//! exactly a silent-skip class; the audit must not reproduce it).

use std::collections::BTreeSet;
use std::path::Path;

/// Read a source file relative to a crate's `CARGO_MANIFEST_DIR`, panicking
/// with the resolved path on failure so a moved Zig file fails loud.
pub fn read_source(manifest_dir: &str, rel: &str) -> String {
    let path = Path::new(manifest_dir).join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("audit source missing: {} ({e})", path.display()))
}

/// Parse a numeric literal that is either `0xNN` hex or plain decimal.
fn parse_value(tok: &str) -> Option<u8> {
    let tok = tok.trim();
    if let Some(hex) = tok.strip_prefix("0x").or_else(|| tok.strip_prefix("0X")) {
        u8::from_str_radix(hex, 16).ok()
    } else {
        tok.parse::<u8>().ok()
    }
}

/// Harvest `NAME = <byte>,` declarations from the enum block that starts at
/// the first line containing `header`. The block ends at the first following
/// line whose trimmed form starts with `}`. Commented-out declarations
/// (`// NAME = ...`, e.g. Zig "planned" opcodes) are skipped — they are
/// intentionally NOT part of the declared set.
pub fn enum_decls(src: &str, header: &str) -> Vec<(String, u8)> {
    let mut out = Vec::new();
    let mut in_block = false;
    for line in src.lines() {
        if !in_block {
            if line.contains(header) {
                in_block = true;
            }
            continue;
        }
        let t = line.trim();
        if t.starts_with('}') {
            break;
        }
        if t.starts_with("//") || t.starts_with("#") {
            continue;
        }
        // NAME = VALUE,
        let Some(eq) = t.find('=') else { continue };
        let name = t[..eq].trim();
        if name.is_empty()
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || !name
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            continue;
        }
        let rest = &t[eq + 1..];
        let value_tok: String = rest
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if let Some(v) = parse_value(&value_tok) {
            out.push((name.to_string(), v));
        }
    }
    out
}

/// Harvest the bytes of raw-hex match-arm labels: lines shaped like
/// `0xNN => …`, `0xNN | 0xMM => …`, `0xNN..=0xMM => …` (Rust) or
/// `0xNN...0xMM => …`, `0xNN, 0xMM => …` (Zig). Ranges expand inclusively.
/// Decimal-only labels are deliberately ignored (they belong to non-opcode
/// matches like undo-op decode); a label containing any character outside
/// the hex-literal/label-punctuation set is rejected wholesale.
pub fn arm_bytes(src: &str) -> BTreeSet<u8> {
    let mut out = BTreeSet::new();
    for line in src.lines() {
        let Some(idx) = line.find("=>") else { continue };
        let label = line[..idx].trim();
        if label.is_empty()
            || !label
                .chars()
                .all(|c| c.is_ascii_hexdigit() || "xX.,|= \t".contains(c))
        {
            continue;
        }
        // normalize both range spellings to one marker
        let norm = label.replace("..=", "§").replace("...", "§");
        for item in norm.split([',', '|']) {
            let item = item.trim();
            if let Some((lo, hi)) = item.split_once('§') {
                if let (Some(lo), Some(hi)) = (parse_value(lo), parse_value(hi)) {
                    for b in lo..=hi {
                        out.insert(b);
                    }
                }
            } else if (item.starts_with("0x") || item.starts_with("0X"))
                && let Some(b) = parse_value(item)
            {
                out.insert(b);
            }
        }
    }
    out
}

/// Harvest `<prefix>NAME` tokens from lines that contain a match arrow.
/// For Zig pass `prefix = "."` (only SCREAMING_CASE names are taken, so
/// `.slot_ref` field inits don't pollute); for Rust pass e.g. `"Opcode::"`.
pub fn arm_names(src: &str, prefix: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in src.lines() {
        if !line.contains("=>") {
            continue;
        }
        collect_prefixed(line, prefix, &mut out);
    }
    out
}

/// Harvest `<prefix>NAME` tokens from EVERY line (no arrow requirement) —
/// for handler-table registrations like `h[@intFromEnum(ReteOpcode.NAME)]`.
pub fn all_prefixed_names(src: &str, prefix: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in src.lines() {
        collect_prefixed(line, prefix, &mut out);
    }
    out
}

fn collect_prefixed(line: &str, prefix: &str, out: &mut BTreeSet<String>) {
    let mut rest = line;
    while let Some(pos) = rest.find(prefix) {
        let after = &rest[pos + prefix.len()..];
        let name: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        // A Zig `.name` harvest must not pick up lowercase field access.
        let uppercase_ok =
            prefix != "." || name.chars().next().is_some_and(|c| c.is_ascii_uppercase());
        if !name.is_empty() && uppercase_ok {
            out.insert(name);
        }
        rest = &rest[pos + prefix.len()..];
    }
}

/// Case/underscore-insensitive name normalization so Zig `BATCH_BITMAP_ANDNOT`
/// equals Rust `BatchBitmapAndNot`: lowercase alphanumerics only.
pub fn norm(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}
