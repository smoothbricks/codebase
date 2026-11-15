//! One set a line on stdin (ascending u32). Prints the `sealed forest` lengths of the repository's crate; with
//! `hex` as the first argument, the two images in hex instead. The size model and the reference writer read it.
use std::io::BufRead;
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
fn main() {
    let as_hex = std::env::args().nth(1).as_deref() == Some("hex");
    for line in std::io::stdin().lock().lines() {
        let members: Vec<u32> = line
            .unwrap()
            .split_whitespace()
            .map(|x| x.parse().unwrap())
            .collect();
        let set = v1::Bitmosaic::from_sorted(members);
        if as_hex {
            println!("{} {}", hex(&set.to_bytes()), hex(&set.to_forest_bytes()));
        } else {
            println!("{} {}", set.to_bytes().len(), set.to_forest_bytes().len());
        }
    }
}
