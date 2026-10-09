fn main() {
    // Reducer VM wasm ABI: memory is exported for JS. Initial memory is
    // linker-derived from the shadow stack and static data; only the 4096-page
    // (256 MiB) maximum is pinned. The JS host owns pages above the module's
    // initial memory and skips pages native allocation appends between calls
    // (`WasmHostRegion` in src/wasm-memory-contract.ts); no host address is
    // fixed inside the module's own layout.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
