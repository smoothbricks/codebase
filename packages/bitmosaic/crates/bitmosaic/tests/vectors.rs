//! The checked-in conformance vectors are what this crate writes, admits and
//! refuses today.

#[path = "vectors/generate.rs"]
mod generate;

//#region smoo/bitmosaic!n/conformance-vectors
#[test]
fn the_checked_in_vectors_are_this_crates_images_and_refusals() {
    let checked_in = include_str!("vectors/native-v2.json");
    let generated = generate::document();
    assert!(
        checked_in == generated,
        "tests/vectors/native-v2.json differs from the crate: regenerate it with the \
         conformance_vectors example and port the change"
    );
}
//#endregion smoo/bitmosaic!n/conformance-vectors
