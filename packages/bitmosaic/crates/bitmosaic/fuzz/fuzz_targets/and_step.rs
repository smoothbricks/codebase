//! Two sets built by an operation list, intersected after every operation on
//! every skew arm, owned, borrowed and through the u64 tier.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| bitmosaic::oracle::and_step(data));
