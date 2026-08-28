fn main() {
    // Reducer VM wasm ABI: memory is exported for JS, with 64 initial pages
    // (4 MiB) and 4096 maximum pages (256 MiB). The layout is shared with
    // wasm-backend.ts and wasm-loader.ts: stack [0, 1 MiB), JS state at 64 KiB
    // inside its lower band, module data/BSS from 1 MiB, and JS input/output
    // regions from 8 MiB (MIN_INPUT_REGION_OFFSET). The Rust heap must stay
    // below 8 MiB; growth beyond that is a documented cutover hazard, as with
    // unusually deep call stacks.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--initial-memory=4194304");
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
