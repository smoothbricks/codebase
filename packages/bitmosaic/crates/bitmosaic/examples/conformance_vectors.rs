//! Prints the conformance vectors of the native format:
//!
//! ```sh
//! cargo run --locked -p bitmosaic --example conformance_vectors \
//!     > packages/bitmosaic/crates/bitmosaic/tests/vectors/native-v2.json
//! ```
//!
//! `tests/vectors.rs` holds the checked-in file to this output.

#[path = "../tests/vectors/generate.rs"]
mod generate;

fn main() {
    print!("{}", generate::document());
}
