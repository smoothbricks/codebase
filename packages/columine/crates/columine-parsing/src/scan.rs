//! Byte-classifier kernels for the JSON hot loops (`parse_string`,
//! `skip_whitespace`).
//!
//! These are deliberately scalar. A hand-rolled wasm32 simd128 variant
//! (16-byte `u8x16_splat`/`i8x16_eq`/bitmask stride) was built, proven
//! equivalent in-wasm on adversarial lane-edge inputs, and paired-benched
//! against this scalar shape at `columine-parsing` `opt-level = 3`: the
//! v128 path measured ~23% SLOWER on both a realistic 250-event mix and a
//! long-clean-strings batch (tools/ep-ingest-bench.ts) — short JSON tokens
//! pay the per-call vector setup without amortizing it, while LLVM compiles
//! this scalar shape (bulk-copy runs via `extend_from_slice`) to faster code.
//! Re-measure only if the EP call envelope grows substantially (the value
//! column budget bounds a call to ~80KB input today).

/// First index `>= from` whose byte ends a clean string run — `"`, `\`, or a
/// control byte (`< 0x20`) — or `input.len()` if the run reaches the end.
#[inline]
pub fn find_string_special(input: &[u8], from: usize) -> usize {
    match input[from.min(input.len())..]
        .iter()
        .position(|&b| b == b'"' || b == b'\\' || b < 0x20)
    {
        Some(offset) => from + offset,
        None => input.len(),
    }
}

/// Zig std.json's whitespace predicate — space, `\t`, `\r`, `\n` only
/// (Scanner.zig:1283). `u8::is_ascii_whitespace` includes `\x0C` and would
/// accept `[1,\x0C2]` where the Zig backends reject it.
#[inline]
pub fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

/// First index `>= from` whose byte is not JSON whitespace, or `input.len()`.
#[inline]
pub fn skip_whitespace(input: &[u8], from: usize) -> usize {
    match input[from.min(input.len())..]
        .iter()
        .position(|&b| !is_json_whitespace(b))
    {
        Some(offset) => from + offset,
        None => input.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn naive_find(input: &[u8], from: usize) -> usize {
        let mut idx = from;
        while idx < input.len() {
            let b = input[idx];
            if b == b'"' || b == b'\\' || b < 0x20 {
                return idx;
            }
            idx += 1;
        }
        input.len()
    }

    fn naive_skip(input: &[u8], from: usize) -> usize {
        let mut idx = from;
        while idx < input.len() && matches!(input[idx], b' ' | b'\t' | b'\r' | b'\n') {
            idx += 1;
        }
        idx
    }

    #[test]
    fn boundary_positions_pin_the_classifier() {
        for special in [b'"', b'\\', 0x1f_u8, 0x00] {
            for pos in [0usize, 1, 14, 15, 16, 17, 31, 32, 33] {
                let mut input = vec![b'a'; 48];
                input[pos] = special;
                assert_eq!(
                    find_string_special(&input, 0),
                    pos,
                    "special {special:#x} at {pos}"
                );
            }
        }
    }

    #[test]
    fn from_offsets_cover_every_phase() {
        let mut input = vec![b'x'; 40];
        input[35] = b'"';
        for from in 0..=35 {
            assert_eq!(find_string_special(&input, from), 35);
        }
        assert_eq!(find_string_special(&input, 36), input.len());
    }

    #[test]
    fn empty_and_past_end_are_len() {
        assert_eq!(find_string_special(&[], 0), 0);
        assert_eq!(find_string_special(b"abc", 7), 3);
        assert_eq!(skip_whitespace(&[], 0), 0);
        assert_eq!(skip_whitespace(b"  ", 9), 2);
    }

    #[test]
    fn whitespace_set_is_zig_std_json_exactly() {
        // Scanner.zig:1283 — ' ', '\t', '\r', '\n' and nothing else.
        // In particular \x0C (form feed, in u8::is_ascii_whitespace) is NOT
        // whitespace to the Zig backends.
        for b in 0u8..=255 {
            let input = [b, b'x'];
            let expected = usize::from(matches!(b, b' ' | b'\t' | b'\r' | b'\n'));
            assert_eq!(skip_whitespace(&input, 0), expected, "byte {b:#x}");
        }
        assert!(!is_json_whitespace(0x0c));
    }

    proptest! {
        #[test]
        fn find_matches_naive_reference(
            input in proptest::collection::vec(any::<u8>(), 0..200),
            from in 0usize..220,
        ) {
            prop_assert_eq!(find_string_special(&input, from), naive_find(&input, from.min(input.len())));
        }

        #[test]
        fn skip_matches_naive_reference(
            input in proptest::collection::vec(
                prop_oneof![Just(b' '), Just(b'\t'), Just(b'\n'), Just(b'\r'), Just(0x0c_u8), any::<u8>()],
                0..200,
            ),
            from in 0usize..220,
        ) {
            prop_assert_eq!(skip_whitespace(&input, from), naive_skip(&input, from.min(input.len())));
        }
    }
}
