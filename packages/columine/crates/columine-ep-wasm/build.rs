fn main() {
    // Event processor wasm ABI: memory is exported for the TypeScript
    // parse-backend, which writes request bytes and reads results at
    // caller-chosen offsets. This artifact uses the same initial/max memory
    // pinning as the consumer configuration.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--initial-memory=4194304");
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
