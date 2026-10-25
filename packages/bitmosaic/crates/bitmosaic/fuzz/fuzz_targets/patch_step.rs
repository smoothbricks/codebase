//! An operation list patched into one image, checked against a `BTreeSet`
//! after every operation: bytes, report, witness and reads.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| bitmosaic::oracle::patch_step(data));
