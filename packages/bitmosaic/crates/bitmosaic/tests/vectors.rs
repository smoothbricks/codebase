//! The checked-in conformance vectors are what this crate writes, admits and
//! refuses today.

#[path = "vectors/generate.rs"]
mod generate;

/// The document without its layout: the repository formatter owns the
/// checked-in file's line breaks and spacing, and no value holds whitespace
/// that matters.
fn unspaced(document: &str) -> String {
    document.chars().filter(|c| !c.is_whitespace()).collect()
}

//#region smoo/bitmosaic!n/conformance-vectors
#[test]
fn the_checked_in_vectors_are_this_crates_images_and_refusals() {
    let checked_in = unspaced(include_str!("vectors/native-v2.json"));
    let generated = unspaced(&generate::document());
    assert!(
        checked_in == generated,
        "tests/vectors/native-v2.json differs from the crate: regenerate it with the \
         conformance_vectors example and port the change"
    );
}
//#endregion smoo/bitmosaic!n/conformance-vectors
