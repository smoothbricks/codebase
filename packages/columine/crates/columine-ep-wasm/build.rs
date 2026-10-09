fn main() {
    // Event processor wasm ABI: memory is exported for the TypeScript
    // parse-backend. Initial memory is linker-derived from the shadow stack and
    // static data; only the 4096-page (256 MiB) maximum is pinned. The host
    // stages request and result bytes only in a `WasmHostRegion` above the
    // module's initial memory (src/wasm-memory-contract.ts), past pages native
    // allocation appends.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
