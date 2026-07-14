fn main() {
    // columine's event_processor.wasm ABI (columine build.zig): memory is
    // EXPORTED (the artifact owns its allocator); the TS parse-backend writes
    // request bytes and reads results at caller-chosen offsets in that
    // memory. Same initial/max pinning as the axe EP artifact.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo::rustc-link-arg=--initial-memory=4194304");
        println!("cargo::rustc-link-arg=--max-memory=268435456");
    }
}
